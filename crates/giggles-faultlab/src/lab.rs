//! A bounded closed-loop local rehearsal of one actual three-voter range.
use crate::{
    Result,
    process::{OwnedNode, fresh_directory, write_new},
    proxy::{Direction, Fault, Proxy, ProxyLimits},
    report::*,
};
use giggles_consensus::{
    authority::Binding,
    native,
    transport::{Configuration, Peer, fingerprint},
};
use giggles_core::*;
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose,
};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::Write,
    net::{SocketAddr, TcpListener},
    path::{Path, PathBuf},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const SCENARIO_EVENTS: usize = 16384;
struct ReportBuffer(Vec<u8>);
impl Write for ReportBuffer {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > (32 * 1024 * 1024usize).saturating_sub(self.0.len()) {
            return Err(std::io::ErrorKind::FileTooLarge.into());
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn ca() -> Result<(rcgen::Certificate, KeyPair)> {
    let mut p = CertificateParams::new(Vec::<String>::new())?;
    p.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    p.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    let key = KeyPair::generate()?;
    Ok((p.self_signed(&key)?, key))
}
fn leaf(
    ca: &rcgen::Certificate,
    key: &KeyPair,
    root: &Path,
    name: &str,
) -> Result<(PathBuf, PathBuf, String)> {
    let mut p = CertificateParams::new(vec!["localhost".into()])?;
    p.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    p.extended_key_usages = vec![
        ExtendedKeyUsagePurpose::ServerAuth,
        ExtendedKeyUsagePurpose::ClientAuth,
    ];
    let own = KeyPair::generate()?;
    let cert = p.signed_by(&own, ca, key)?;
    let certificate = root.join(format!("{name}.pem"));
    let path = root.join(format!("{name}.key.pem"));
    write_new(&certificate, cert.pem().as_bytes())?;
    write_new(&path, own.serialize_pem().as_bytes())?;
    Ok((certificate, path, fingerprint(cert.der())))
}
fn ensure(v: &Value) -> Result<()> {
    if v["reply"]["code"] == 0 {
        Ok(())
    } else {
        Err(format!("bootstrap/control failed: {v}").into())
    }
}
fn applied(v: &Value) -> Result<u64> {
    v["diagnostics"]
        .as_str()
        .and_then(|s| {
            s.split_whitespace()
                .find_map(|f| f.strip_prefix("applied=")?.parse().ok())
        })
        .ok_or_else(|| "missing applied diagnostic".into())
}
async fn leader(nodes: &mut BTreeMap<u64, OwnedNode>, excluded: Option<u64>) -> Result<u64> {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        for (id, node) in nodes.iter_mut() {
            if Some(*id) == excluded {
                continue;
            }
            if node.diagnostics()?["authority_ready"] == true {
                return Ok(*id);
            }
        }
        if Instant::now() > deadline {
            return Err("authority election deadline".into());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
fn request(business: u64, row: u64) -> Request {
    Request {
        operation: OperationId(business),
        selection: Selection::Keys(vec![Key {
            tenant: TenantId(42),
            row: RowId(row),
        }]),
        column: Column::Status,
        after: Status::Queued,
    }
}
fn client(config: &giggles_client::Config, proxy: &Proxy) -> Result<giggles_client::Client> {
    let mut c = config.clone();
    c.endpoints = vec![proxy.address()];
    Ok(giggles_client::Client::new(c)?)
}
struct Recorder {
    start: Instant,
    invocations: Vec<Invocation>,
    log: std::fs::File,
}
impl Recorder {
    async fn send(
        &mut self,
        c: &giggles_client::Client,
        op: &giggles_client::Operation,
        phase: Phase,
        action: Action,
    ) -> Result<Observation> {
        let request: Value = serde_json::from_slice(op.wire_bytes())?;
        let start_micros = self.start.elapsed().as_micros() as u64;
        let outcome = Observation::from_outcome(
            c.send(op).await,
            request["id"].as_u64().ok_or("request ID")?,
        );
        let i = Invocation {
            phase,
            action,
            correlation_id: request["id"].as_u64().ok_or("request ID")?,
            business_id: op.operation_id().0,
            start_micros,
            end_micros: self.start.elapsed().as_micros() as u64,
            request,
            outcome: outcome.clone(),
        };
        serde_json::to_writer(&mut self.log, &i)?;
        self.log.write_all(b"\n")?;
        self.log.flush()?;
        self.invocations.push(i);
        Ok(outcome)
    }
}
/// Run actual node processes. Artifacts are retained even on failure.
pub async fn run(binary: &Path, samples: usize) -> Result<PathBuf> {
    run_with_loss(binary, samples, true).await
}
/// Run with leader loss (CLI default) or follower loss, using synthetic local domains.
pub async fn run_with_loss(binary: &Path, samples: usize, lose_leader: bool) -> Result<PathBuf> {
    if !(1..=8).contains(&samples) {
        return Err("samples must be 1..=8".into());
    }
    let binary = std::fs::canonicalize(binary)?;
    let root = fresh_directory("run")?;
    eprintln!("retained faultlab fixture: {}", root.display());
    let result = tokio::time::timeout(
        Duration::from_secs(180),
        scenario(&binary, samples, &root, lose_leader),
    )
    .await;
    match result {
        Ok(Ok(report)) => {
            let path = root.join("report.json");
            let mut buffer = ReportBuffer(Vec::new());
            serde_json::to_writer_pretty(&mut buffer, &report)?;
            write_new(&path, &buffer.0)?;
            report.validate()?;
            Ok(path)
        }
        other => {
            let error = match other {
                Ok(Err(e)) => e.to_string(),
                Err(_) => "scenario exceeded 180 seconds".into(),
                _ => unreachable!(),
            };
            write_new(
                &root.join("failure.json"),
                &serde_json::to_vec(&json!({"error":error,"scope":LOCAL_SCOPE}))?,
            )?;
            Err(format!("{error}; retained {}", root.display()).into())
        }
    }
}
async fn scenario(binary: &Path, samples: usize, root: &Path, lose_leader: bool) -> Result<Report> {
    let binary_hash = crate::process::file_sha256(binary)?;
    let (peer_ca, peer_key) = ca()?;
    let (task_ca, task_key) = ca()?;
    let peer_ca_path = root.join("peer-ca.pem");
    let task_ca_path = root.join("task-ca.pem");
    write_new(&peer_ca_path, peer_ca.pem().as_bytes())?;
    write_new(&task_ca_path, task_ca.pem().as_bytes())?;
    let mut reservations = Vec::new();
    let mut peers = BTreeMap::new();
    let mut task_addresses = BTreeMap::<u64, SocketAddr>::new();
    let mut peer_certs = BTreeMap::new();
    let mut task_certs = BTreeMap::new();
    for id in 1..=3 {
        let p = TcpListener::bind("127.0.0.1:0")?;
        let t = TcpListener::bind("127.0.0.1:0")?;
        let cert = leaf(&peer_ca, &peer_key, root, &format!("peer-{id}"))?;
        peers.insert(
            id,
            Peer {
                address: p.local_addr()?.to_string(),
                fingerprint: cert.2.clone(),
            },
        );
        peer_certs.insert(id, cert);
        task_addresses.insert(id, t.local_addr()?);
        task_certs.insert(id, leaf(&task_ca, &task_key, root, &format!("task-{id}"))?);
        reservations.extend([p, t]);
    }
    let mut peer_proxies = BTreeMap::new();
    let mut sdk_proxies = BTreeMap::new();
    for from in 1..=3 {
        for to in 1..=3 {
            if from != to {
                peer_proxies.insert(
                    (from, to),
                    Proxy::start_retained(
                        peers[&to].address.parse()?,
                        ProxyLimits {
                            events: SCENARIO_EVENTS,
                            ..Default::default()
                        },
                        &root.join(format!("proxy-peer-{from}-{to}.jsonl")),
                    )
                    .await?,
                );
            }
        }
        sdk_proxies.insert(
            from,
            Proxy::start_retained(
                task_addresses[&from],
                ProxyLimits {
                    events: SCENARIO_EVENTS,
                    ..Default::default()
                },
                &root.join(format!("proxy-sdk-{from}.jsonl")),
            )
            .await?,
        );
    }
    let (cert, key, fp) = leaf(&task_ca, &task_key, root, "sdk-client")?;
    let mut sdk_config = giggles_client::Config {
        endpoints: vec![sdk_proxies[&1].address()],
        ca: task_ca_path.clone(),
        certificate: cert,
        key,
        server_name: "localhost".into(),
        limits: giggles_client::Limits::default(),
    };
    sdk_config.limits.operation = Duration::from_millis(1800);
    sdk_config.limits.request = Duration::from_millis(1500);
    sdk_config.limits.connect = Duration::from_millis(500);
    sdk_config.limits.max_attempts = 1;
    write_new(
        &root.join("sdk-config.json"),
        &serde_json::to_vec_pretty(&sdk_config)?,
    )?;
    let mut configs = BTreeMap::new();
    for id in 1..=3 {
        let mut routes = peers.clone();
        for to in 1..=3 {
            if id != to {
                routes.get_mut(&to).expect("peer").address =
                    peer_proxies[&(id, to)].address().to_string();
            }
        }
        let c = Configuration {
            id,
            domain: 42,
            database: root.join(format!("node-{id}.redb")),
            listen: peers[&id].address.clone(),
            certificate: peer_certs[&id].0.clone(),
            key: peer_certs[&id].1.clone(),
            ca: peer_ca_path.clone(),
            peers: routes,
            task: Some(native::Configuration {
                listen: task_addresses[&id].to_string(),
                certificate: task_certs[&id].0.clone(),
                key: task_certs[&id].1.clone(),
                ca: task_ca_path.clone(),
            }),
            pgwire: None,
        };
        let path = root.join(format!("node-{id}.json"));
        write_new(&path, &serde_json::to_vec_pretty(&c)?)?;
        configs.insert(id, path);
    }
    drop(reservations);
    let mut nodes = BTreeMap::new();
    for id in 1..=3 {
        nodes.insert(id, OwnedNode::start(binary, &configs[&id], root, id)?);
    }
    write_new(
        &root.join("owned-pids.json"),
        &serde_json::to_vec(
            &nodes
                .iter()
                .map(|(id, n)| (*id, n.pid()))
                .collect::<BTreeMap<_, _>>(),
        )?,
    )?;
    nodes
        .get_mut(&1)
        .expect("node")
        .send(json!({"op":"campaign"}))?;
    let initial = leader(&mut nodes, None).await?;
    let isolated = if lose_leader {
        initial
    } else if initial == 3 {
        2
    } else {
        3
    };
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let grant = Grant {
        task: TaskId(1),
        issuer: Principal(1),
        executor: Principal(2),
        approver: Principal(3),
        policy: Policy {
            tenant: TenantId(42),
            columns: BTreeSet::from([Column::Status]),
            transitions: BTreeSet::from([(Status::Failed, Status::Queued)]),
            incident: (100, 107),
            distinct_limit: 32,
            transition_limit: 32,
            approval_above: 32,
            valid_from: now - 10,
            expires: now + 3600,
        },
    };
    let node = nodes.get_mut(&initial).expect("leader");
    for row in 0..samples * 4 {
        ensure(&node.admin(100+row as u64,json!({"command":"seed","key":Key{tenant:TenantId(42),row:RowId(row as u64)},"row":Row{status:Status::Failed,failed_at:103,attempt_count:0,version:1}}))?)?;
    }
    ensure(&node.admin(200, json!({"command":"grant","grant":grant}))?)?;
    ensure(&node.admin(201,json!({"command":"provision_task_resources","task":1,"limits":{"read_rows":100000,"disclosed_rows":10000,"encoded_bytes":10000000}}))?)?;
    let binding = Binding {
        fingerprint: fp,
        domain: 42,
        task: TaskId(1),
        principal: Principal(2),
    };
    ensure(&node.control(
        202,
        json!({"op":"reload_credentials","expected":0,"bindings":[binding]}),
    )?)?;
    let mut rec = Recorder {
        start: Instant::now(),
        invocations: Vec::new(),
        log: std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(root.join("sdk-invocations.jsonl"))?,
    };
    let mut operations = Vec::new();
    let mut correlation = 1000;
    let mut active = initial;
    let mut phase_nodes = BTreeMap::new();
    for phase in [
        Phase::Healthy,
        Phase::DomainLoss,
        Phase::NoQuorum,
        Phase::Restored,
    ] {
        match phase {
            Phase::Healthy => {}
            Phase::DomainLoss => {
                for (&(from, to), p) in &peer_proxies {
                    if from == isolated || to == isolated {
                        for d in [Direction::Upstream, Direction::Downstream] {
                            p.set(d, Fault::Disconnect)?;
                        }
                    }
                }
                for d in [Direction::Upstream, Direction::Downstream] {
                    sdk_proxies[&isolated].set(d, Fault::Disconnect)?;
                }
            }
            Phase::NoQuorum => {
                for p in peer_proxies.values() {
                    for d in [Direction::Upstream, Direction::Downstream] {
                        p.set(d, Fault::Disconnect)?;
                    }
                }
                tokio::time::sleep(Duration::from_millis(1200)).await;
            }
            Phase::Restored => {
                for p in peer_proxies.values().chain(sdk_proxies.values()) {
                    for d in [Direction::Upstream, Direction::Downstream] {
                        p.set(d, Fault::Pass)?;
                    }
                }
                // Authority-ready alone is not used as a quorum proof: SDK release is.
                tokio::time::sleep(Duration::from_millis(1200)).await;
            }
        }
        let selected = if phase == Phase::NoQuorum {
            active
        } else {
            leader(
                &mut nodes,
                if phase == Phase::DomainLoss {
                    Some(isolated)
                } else {
                    None
                },
            )
            .await?
        };
        active = selected;
        phase_nodes.insert(phase, selected);
        let c = client(&sdk_config, &sdk_proxies[&selected])?;
        if phase == Phase::Healthy {
            sdk_proxies[&selected].set(Direction::Upstream, Fault::Delay { millis: 15 })?;
        }
        for _ in 0..samples {
            let row = operations.len() as u64;
            let business = 10000 + row;
            let op = giggles_client::Operation::execute(
                correlation,
                request(business, row),
                None,
                1500,
            )?;
            correlation += 1;
            write_new(
                &root.join(format!("operation-{business}.json")),
                op.wire_bytes(),
            )?;
            rec.send(&c, &op, phase, Action::Execute).await?;
            operations.push(op);
        }
        if phase == Phase::Healthy {
            sdk_proxies[&selected].set(Direction::Upstream, Fault::Pass)?;
        }
    }
    let selected = leader(&mut nodes, None).await?;
    let c = client(&sdk_config, &sdk_proxies[&selected])?;
    for op in &operations {
        let status = giggles_client::Operation::status(correlation, op.operation_id(), 1500)?;
        correlation += 1;
        rec.send(&c, &status, Phase::Restored, Action::Status)
            .await?;
        // Replay preserves the complete action, with a new transport correlation ID.
        let mut replay: native::Request = serde_json::from_slice(op.wire_bytes())?;
        replay.id = correlation;
        correlation += 1;
        let replay = giggles_client::Operation::from_request(replay)?;
        rec.send(&c, &replay, Phase::Restored, Action::Replay)
            .await?;
    }
    let final_inspection = nodes
        .get_mut(&selected)
        .expect("leader")
        .admin(9000, json!({"command":"inspect"}))?;
    ensure(&final_inspection)?;
    let barrier = final_inspection["applied"]
        .as_u64()
        .ok_or("inspection missing apply index")?;
    let end = Instant::now() + Duration::from_secs(12);
    let mut catch_up;
    loop {
        catch_up = Vec::new();
        for (id, node) in &mut nodes {
            let d = node.diagnostics()?;
            catch_up.push(CatchUp {
                node: *id,
                applied: applied(&d)?,
                diagnostics: d,
            });
        }
        if catch_up.iter().all(|c| c.applied >= barrier) {
            break;
        }
        if Instant::now() > end {
            return Err("replica catch-up deadline".into());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // Teardown only the exact handles we spawned. Stop proxies after children.
    for node in nodes.values_mut() {
        node.terminate()?;
    }
    let mut traces = BTreeMap::new();
    for ((from, to), p) in peer_proxies {
        traces.insert(format!("peer-{from}-{to}"), p.stop().await);
    }
    for (id, p) in sdk_proxies {
        traces.insert(format!("sdk-{id}"), p.stop().await);
    }
    if crate::process::file_sha256(binary)? != binary_hash {
        return Err("node binary changed during run".into());
    }
    Ok(Report {
        version: 1,
        scope: LOCAL_SCOPE.into(),
        node_binary_sha256: binary_hash,
        host_os: std::env::consts::OS.into(),
        host_arch: std::env::consts::ARCH.into(),
        samples_per_phase: samples,
        domains: (1..=3)
            .map(|id| (id, format!("LOCAL-provider-{id}")))
            .collect(),
        isolated_node: isolated,
        initial_leader: initial,
        phase_nodes,
        summary: summarize(&rec.invocations),
        invocations: rec.invocations,
        traces,
        final_inspection,
        catch_up,
        barrier,
    })
}

/// Reject oversized inputs before parsing; retained reports are at most 32 MiB.
pub fn check(path: &Path) -> Result<Report> {
    use std::io::Read;
    let f = std::fs::File::open(path)?;
    let mut bytes = Vec::new();
    f.take(32 * 1024 * 1024 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > 32 * 1024 * 1024 {
        return Err("report exceeds 32 MiB".into());
    }
    let report: Report = serde_json::from_slice(&bytes)?;
    report.validate()?;
    Ok(report)
}
