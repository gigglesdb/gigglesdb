mod common;
use common::*;
use giggles_consensus::{
    transport::{Configuration, Peer, fingerprint, read_frame, write_frame},
    *,
};
use protobuf::Message as Pb;
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose,
};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    process::{Child, ChildStdin, Command as Process, Stdio},
    sync::mpsc,
    time::{Duration, Instant},
};

struct Running {
    child: Child,
    input: ChildStdin,
    output: mpsc::Receiver<Value>,
    history: std::fs::File,
}
impl Running {
    fn start(path: &Path, root: &Path, round: u64, id: u64) -> Self {
        let err =
            std::fs::File::create(root.join(format!("node-{id}-{round}.stderr.txt"))).unwrap();
        let mut child = Process::new(env!("CARGO_BIN_EXE_giggles-node"))
            .arg(path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(err)
            .spawn()
            .unwrap();
        let input = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let (tx, output) = mpsc::channel();
        let mut log =
            std::fs::File::create(root.join(format!("node-{id}-{round}.stdout.jsonl"))).unwrap();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else {
                    break;
                };
                writeln!(log, "{line}").unwrap();
                if let Ok(v) = serde_json::from_str(&line) {
                    let _ = tx.send(v);
                }
            }
        });
        let s = Self {
            child,
            input,
            output,
            history: std::fs::File::create(
                root.join(format!("node-{id}-{round}.invocations.jsonl")),
            )
            .unwrap(),
        };
        s.until(|v| v["ready"] == true, Duration::from_secs(5));
        s
    }
    fn send(&mut self, v: Value) {
        writeln!(self.history, "{v}").unwrap();
        writeln!(self.input, "{v}").unwrap();
        self.input.flush().unwrap();
    }
    fn until(&self, p: impl Fn(&Value) -> bool, d: Duration) -> Value {
        let start = Instant::now();
        loop {
            let remain = d.checked_sub(start.elapsed()).expect("process timeout");
            let v = self
                .output
                .recv_timeout(remain)
                .expect("process output timeout");
            if p(&v) {
                return v;
            }
        }
    }
    fn request(&mut self, id: u64, command: crate::Command) -> Value {
        self.send(json!({"op":"propose","id":id,"command":command}));
        self.until(|v| v["id"] == id, Duration::from_secs(10))
    }
    fn diag(&mut self) -> Value {
        self.send(json!({"op":"diagnostics"}));
        self.until(|v| v.get("is_leader").is_some(), Duration::from_secs(3))
    }
    fn kill(&mut self) {
        self.child.kill().unwrap();
        self.child.wait().unwrap();
    }
}
impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
struct Setup {
    root: PathBuf,
    configs: BTreeMap<u64, PathBuf>,
    config: Configuration,
    ca: rcgen::Certificate,
    ca_key: KeyPair,
}
fn setup() -> Setup {
    let root = fresh("real-mtls");
    let mut cp = CertificateParams::new(Vec::<String>::new()).unwrap();
    cp.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    cp.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    let ca_key = KeyPair::generate().unwrap();
    let ca = cp.self_signed(&ca_key).unwrap();
    std::fs::write(root.join("ca.pem"), ca.pem()).unwrap();
    let mut peers = BTreeMap::new();
    let mut leaves = BTreeMap::new();
    let listeners = (1..=3)
        .map(|id| (id, std::net::TcpListener::bind("127.0.0.1:0").unwrap()))
        .collect::<BTreeMap<_, _>>();
    for id in VOTERS {
        let (cert, key) = leaf(&ca, &ca_key);
        let cert_path = root.join(format!("node-{id}.pem"));
        let key_path = root.join(format!("node-{id}.key.pem"));
        std::fs::write(&cert_path, cert.pem()).unwrap();
        std::fs::write(&key_path, key.serialize_pem()).unwrap();
        peers.insert(
            id,
            Peer {
                address: listeners[&id].local_addr().unwrap().to_string(),
                fingerprint: fingerprint(cert.der()),
            },
        );
        leaves.insert(id, (cert_path, key_path));
    }
    drop(listeners);
    let mut configs = BTreeMap::new();
    let mut first = None;
    for id in VOTERS {
        let config = Configuration {
            id,
            domain: 42,
            database: root.join(format!("node-{id}.redb")),
            listen: peers[&id].address.clone(),
            certificate: leaves[&id].0.clone(),
            key: leaves[&id].1.clone(),
            ca: root.join("ca.pem"),
            peers: peers.clone(),
        };
        let path = root.join(format!("node-{id}.json"));
        std::fs::write(&path, serde_json::to_vec_pretty(&config).unwrap()).unwrap();
        configs.insert(id, path);
        if id == 1 {
            first = Some(config);
        }
    }
    Setup {
        root,
        configs,
        config: first.unwrap(),
        ca,
        ca_key,
    }
}
fn leaf(ca: &rcgen::Certificate, key: &KeyPair) -> (rcgen::Certificate, KeyPair) {
    let mut p = CertificateParams::new(vec!["localhost".into()]).unwrap();
    p.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    p.extended_key_usages = vec![
        ExtendedKeyUsagePurpose::ServerAuth,
        ExtendedKeyUsagePurpose::ClientAuth,
    ];
    let k = KeyPair::generate().unwrap();
    (p.signed_by(&k, ca, key).unwrap(), k)
}
fn find_leader(nodes: &mut BTreeMap<u64, Running>) -> u64 {
    let start = Instant::now();
    loop {
        for (id, n) in nodes.iter_mut() {
            if n.diag()["authority_ready"] == true {
                return *id;
            }
        }
        assert!(
            start.elapsed() < Duration::from_secs(12),
            "no elected leader"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn native_command(action: TaskAction) -> Command {
    Command::NativeTask {
        task: giggles_core::TaskId(1),
        principal: giggles_core::Principal(2),
        lower: 10,
        upper: 10,
        action,
    }
}

fn control(n: &mut Running, id: u64, mut value: Value) -> Value {
    value["id"] = json!(id);
    n.send(value);
    n.until(|v| v["id"] == id, Duration::from_secs(10))
}
fn authenticate(n: &mut Running, fingerprint: &str) -> u64 {
    n.send(json!({"op":"authenticate","fingerprint":fingerprint}));
    n.until(
        |v| v.get("session").is_some() || v.get("error").is_some(),
        Duration::from_secs(5),
    )["session"]
        .as_u64()
        .expect("authentication failed")
}
fn native_roundtrip(n: &mut Running, id: u64, session: u64, action: TaskAction) -> Value {
    let held = control(
        n,
        id,
        json!({"op":"native_decide","session":session,"action":action}),
    );
    assert!(held["held"].is_u64(), "{held}");
    control(n, id + 1, json!({"op":"native_release","decision":id}))
}

#[test]
fn real_three_process_credential_epochs_held_output_restart_failover_and_automatic_time() {
    use giggles_consensus::authority::Binding;
    use giggles_core::{OperationId, Principal, TaskId};
    let s = setup();
    let mut nodes = BTreeMap::new();
    for id in VOTERS {
        nodes.insert(id, Running::start(&s.configs[&id], &s.root, 0, id));
    }
    nodes.get_mut(&1).unwrap().send(json!({"op":"campaign"}));
    let leader = find_leader(&mut nodes);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let mut g = grant(10, 10);
    g.policy.valid_from = now - 10;
    g.policy.expires = now + 3600;
    for (i,command) in [admin(json!({"command":"seed","key":key(0),"row":row()})),admin(json!({"command":"grant","grant":g})),admin(json!({"command":"provision_task_resources","task":1,"limits":{"read_rows":10000,"disclosed_rows":20,"encoded_bytes":100000}}))].into_iter().enumerate(){assert_eq!(nodes.get_mut(&leader).unwrap().request(3000+i as u64,command)["reply"]["code"],0);}
    let b = Binding {
        fingerprint: format!("{:064x}", 1),
        domain: 42,
        task: TaskId(1),
        principal: Principal(2),
    };
    let b2 = Binding {
        fingerprint: format!("{:064x}", 2),
        ..b.clone()
    };
    let n = nodes.get_mut(&leader).unwrap();
    assert_eq!(
        control(
            n,
            3010,
            json!({"op":"reload_credentials","expected":0,"bindings":[b,b2]})
        )["reply"]["data"]["epoch"],
        1
    );
    let session = authenticate(n, &b.fingerprint);
    // An idle but ticking actor must not confuse lack of requests with a host
    // pause. Exceed the default ten-second pause bound without sending control.
    std::thread::sleep(Duration::from_secs(11));
    let idle = native_roundtrip(n, 3090, session, TaskAction::Status(OperationId(3090)));
    assert_eq!(idle["reply"]["data"]["code"], "absent", "{idle}");
    let bad = control(
        n,
        3011,
        json!({"op":"reload_credentials","expected":1,"bindings":[b,b]}),
    );
    assert!(bad["error"].is_string());
    // Successful old session proves invalid reload did not mutate committed map.
    let held = control(
        n,
        3012,
        json!({"op":"native_decide","session":session,"action":TaskAction::Execute(request(3012,&[0]),None)}),
    );
    assert!(held["held"].is_u64());
    assert_eq!(
        control(
            n,
            3013,
            json!({"op":"reload_credentials","expected":1,"bindings":[b2]})
        )["reply"]["data"]["epoch"],
        2
    );
    assert!(control(n, 3014, json!({"op":"native_release","decision":3012}))["error"].is_string());
    assert_eq!(
        control(
            n,
            3015,
            json!({"op":"reload_credentials","expected":2,"bindings":[b,b2]})
        )["reply"]["data"]["epoch"],
        3
    );
    assert!(control(n,3016,json!({"op":"native_decide","session":session,"action":TaskAction::Status(OperationId(3012))}))["error"].is_string());
    let session = authenticate(n, &b.fingerprint);
    let recovered = native_roundtrip(n, 3020, session, TaskAction::Status(OperationId(3012)));
    assert_eq!(recovered["reply"]["data"]["code"], "committed");
    // Persist snapshots with binding metadata, then restart every executable.
    for n in nodes.values_mut() {
        n.send(json!({"op":"checkpoint"}));
        n.until(|v| v.get("checkpoint").is_some(), Duration::from_secs(5));
        n.kill();
    }
    drop(nodes);
    let mut nodes = BTreeMap::new();
    for id in VOTERS {
        nodes.insert(id, Running::start(&s.configs[&id], &s.root, 1, id));
    }
    let leader = find_leader(&mut nodes);
    let n = nodes.get_mut(&leader).unwrap();
    let session = authenticate(n, &b.fingerprint);
    assert_eq!(
        native_roundtrip(n, 3030, session, TaskAction::Status(OperationId(3012)))["reply"]["data"]
            ["code"],
        "committed"
    );
    // Hold charged output, kill leader, recover receipt through new term's clock.
    assert!(control(n,3032,json!({"op":"native_decide","session":session,"action":TaskAction::Status(OperationId(3012))}))["held"].is_u64());
    n.kill();
    drop(nodes.remove(&leader));
    let successor = find_leader(&mut nodes);
    let n = nodes.get_mut(&successor).unwrap();
    let session = authenticate(n, &b2.fingerprint);
    assert_eq!(
        native_roundtrip(n, 3040, session, TaskAction::Status(OperationId(3012)))["reply"]["data"]
            ["code"],
        "committed"
    );
    std::fs::write(s.root.join("authority-checker.txt"),"PASS: atomic invalid reload; overlap; revoke/readd ABA; denied release preserves mutation/receipt; snapshot full restart; held leader loss; automatic OS time on each leader. Trusted stdin only, no external listener qualification.\n").unwrap();
}

#[test]
fn real_three_process_redacted_native_lost_ack_failover_and_exhaustion() {
    let s = setup();
    let mut nodes = BTreeMap::new();
    for id in VOTERS {
        nodes.insert(id, Running::start(&s.configs[&id], &s.root, 0, id));
    }
    nodes.get_mut(&1).unwrap().send(json!({"op":"campaign"}));
    let leader = find_leader(&mut nodes);
    let mut index = 0;
    for (id,command) in [
        admin(json!({"command":"seed","key":key(0),"row":row()})),
        admin(json!({"command":"grant","grant":grant(1,5)})),
        admin(json!({"command":"provision_task_resources","task":1,"limits":{"read_rows":10000,"disclosed_rows":3,"encoded_bytes":100000}})),
    ].into_iter().enumerate() {
        let r=nodes.get_mut(&leader).unwrap().request(2000+id as u64,command);
        assert_eq!(r["reply"]["code"],0); index=r["applied"].as_u64().unwrap();
    }
    let follower = VOTERS.into_iter().find(|id| *id != leader).unwrap();
    let rejected = nodes.get_mut(&follower).unwrap().request(
        2003,
        native_command(TaskAction::Status(giggles_core::OperationId(77))),
    );
    assert_eq!(rejected["error"], "not leader");
    // Do not read the proposing leader's outcome. Observe commitment at a
    // follower, then kill that leader with its response still unread by caller.
    nodes.get_mut(&leader).unwrap().send(json!({"op":"propose","id":2004,"command":native_command(TaskAction::Execute(request(77,&[0]),None))}));
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let d = nodes.get_mut(&follower).unwrap().diag();
        let applied = d["diagnostics"]
            .as_str()
            .unwrap()
            .split("applied=")
            .nth(1)
            .unwrap()
            .parse::<u64>()
            .unwrap();
        if applied > index {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "follower never observed committed native command"
        );
    }
    nodes.get_mut(&leader).unwrap().kill();
    drop(nodes.remove(&leader));
    let successor = find_leader(&mut nodes);
    let retry = nodes.get_mut(&successor).unwrap().request(
        2005,
        native_command(TaskAction::Execute(request(77, &[0]), None)),
    );
    assert_eq!(retry["reply"]["data"]["code"], "committed");
    assert_eq!(retry["reply"]["data"]["count"], 1);
    assert_eq!(retry["reply"]["data"].as_object().unwrap().len(), 6);
    let status = nodes.get_mut(&successor).unwrap().request(
        2006,
        native_command(TaskAction::Status(giggles_core::OperationId(77))),
    );
    assert_eq!(status["reply"], retry["reply"]);
    let exhausted = nodes.get_mut(&successor).unwrap().request(
        2007,
        native_command(TaskAction::Status(giggles_core::OperationId(77))),
    );
    let fixed =
        json!({"version":1,"code":"42501","operation":null,"count":null,"digest":null,"sql":null});
    assert_eq!(exhausted["reply"]["data"], fixed);
    nodes.insert(
        leader,
        Running::start(&s.configs[&leader], &s.root, 1, leader),
    );
    // A current barrier confirms original business mutation exactly once.
    nodes
        .get_mut(&successor)
        .unwrap()
        .send(json!({"op":"read","id":2008}));
    let inspection = nodes[&successor].until(|v| v["id"] == 2008, Duration::from_secs(5));
    assert_eq!(
        inspection["reply"]["data"]["operations"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(inspection["reply"]["data"]["tasks"][0][1]["transitions"], 1);
    for n in nodes.values_mut() {
        n.kill();
    }
    nodes.clear();
    for id in VOTERS {
        nodes.insert(id, Running::start(&s.configs[&id], &s.root, 2, id));
    }
    let restarted = find_leader(&mut nodes);
    let after = nodes.get_mut(&restarted).unwrap().request(
        2009,
        native_command(TaskAction::Status(giggles_core::OperationId(77))),
    );
    assert_eq!(after["reply"]["data"], fixed);
    std::fs::write(s.root.join("native-checker.txt"),"PASS trusted local native command; actual three mTLS peer processes; follower refuses; unread leader reply; failover receipt/status; exactly one transition; exhausted disclosure persists across full restart. NOT external task-client mTLS.\n").unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_three_process_mtls_failover_snapshot_and_lost_ack() {
    let s = setup();
    let mut nodes = BTreeMap::new();
    for id in VOTERS {
        nodes.insert(id, Running::start(&s.configs[&id], &s.root, 0, id));
    }
    nodes.get_mut(&1).unwrap().send(json!({"op":"campaign"}));
    let leader = find_leader(&mut nodes);
    let mut seq = 100;
    let mut submit = |nodes: &mut BTreeMap<u64, Running>, id, c| {
        seq += 1;
        let reply = nodes.get_mut(&id).unwrap().request(seq, c);
        assert!(reply.get("error").is_none(), "{reply}");
        reply
    };
    for i in 0..3 {
        assert_eq!(
            submit(
                &mut nodes,
                leader,
                admin(json!({"command":"seed","key":key(i),"row":row()}))
            )["reply"]["code"],
            0
        );
    }
    assert_eq!(
        submit(
            &mut nodes,
            leader,
            admin(json!({"command":"grant","grant":grant(2,5)}))
        )["reply"]["code"],
        0
    );
    assert_eq!(
        submit(
            &mut nodes,
            leader,
            admin(
                json!({"command":"provision_task_resources","task":1,"limits":{"read_rows":10000,"disclosed_rows":1000,"encoded_bytes":1000000}})
            )
        )["reply"]["code"],
        0
    );
    let absent = VOTERS.into_iter().find(|id| *id != leader).unwrap();
    nodes.get_mut(&absent).unwrap().kill();
    drop(nodes.remove(&absent));
    let original = submit(
        &mut nodes,
        leader,
        task(TaskAction::Execute(request(77, &[0]), None)),
    );
    assert_eq!(original["reply"]["code"], 0);
    nodes
        .get_mut(&leader)
        .unwrap()
        .send(json!({"op":"checkpoint"}));
    nodes[&leader].until(|v| v.get("checkpoint").is_some(), Duration::from_secs(3));
    submit(
        &mut nodes,
        leader,
        task(TaskAction::Execute(request(78, &[1]), None)),
    );
    nodes.insert(
        absent,
        Running::start(&s.configs[&absent], &s.root, 1, absent),
    );
    // A restarted stale full-data voter must catch up through the actual snapshot RPC.
    let start = Instant::now();
    loop {
        let d = nodes.get_mut(&absent).unwrap().diag();
        let text = d["diagnostics"].as_str().unwrap();
        let applied = text
            .split("applied=")
            .nth(1)
            .unwrap()
            .parse::<u64>()
            .unwrap();
        if applied > original["applied"].as_u64().unwrap() {
            break;
        }
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "snapshot catchup timeout: {d}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    // Valid CA and valid TLS are insufficient without the exact member fingerprint.
    let (rogue, key) = leaf(&s.ca, &s.ca_key);
    let mut roots = rustls::RootCertStore::empty();
    roots.add(s.ca.der().clone()).unwrap();
    let client = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_client_auth_cert(
            vec![rogue.der().clone()],
            rustls::pki_types::PrivatePkcs8KeyDer::from(key.serialize_der()).into(),
        )
        .unwrap();
    let connector = tokio_rustls::TlsConnector::from(std::sync::Arc::new(client));
    let tcp = tokio::net::TcpStream::connect(&s.config.peers[&leader].address)
        .await
        .unwrap();
    let mut tls = connector
        .connect(
            rustls::pki_types::ServerName::try_from("localhost").unwrap(),
            tcp,
        )
        .await
        .unwrap();
    let mut m = raft::eraftpb::Message {
        from: absent,
        to: leader,
        ..Default::default()
    };
    m.set_msg_type(raft::eraftpb::MessageType::MsgHeartbeat);
    let sent = write_frame(&mut tls, &m.write_to_bytes().unwrap()).await;
    if sent.is_ok() {
        assert!(read_frame(&mut tls).await.is_err());
    }
    nodes.get_mut(&leader).unwrap().kill();
    drop(nodes.remove(&leader));
    let successor = find_leader(&mut nodes);
    let replay = submit(
        &mut nodes,
        successor,
        task(TaskAction::Execute(request(77, &[0]), None)),
    );
    assert_eq!(replay["reply"], original["reply"]);
    let deny = submit(
        &mut nodes,
        successor,
        task(TaskAction::Execute(request(79, &[2]), None)),
    );
    assert_eq!(deny["reply"]["code"], 112);
    nodes
        .get_mut(&successor)
        .unwrap()
        .send(json!({"op":"read","id":9000}));
    let read = nodes[&successor].until(|v| v["id"] == 9000, Duration::from_secs(5));
    assert!(read["applied"].as_u64().unwrap() >= read["barrier"].as_u64().unwrap());
    assert_eq!(
        read["reply"]["data"]["operations"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
    let survivor = nodes.keys().copied().find(|id| *id != successor).unwrap();
    nodes.get_mut(&survivor).unwrap().kill();
    drop(nodes.remove(&survivor));
    // Isolated last process cannot complete a new current read.
    nodes
        .get_mut(&successor)
        .unwrap()
        .send(json!({"op":"read","id":9999}));
    let start = Instant::now();
    while start.elapsed() < Duration::from_millis(700) {
        if let Ok(v) = nodes[&successor]
            .output
            .recv_timeout(Duration::from_millis(30))
        {
            assert!(
                v["id"] != 9999 || v.get("error").is_some(),
                "minority returned current state"
            );
        }
    }
    std::fs::write(s.root.join("checker.txt"),"PASS: actual three processes; mTLS fixed identities; missing replica snapshot+tail; one process loss; original receipt retry; budget; quorum read; no majority read. Local host only.\n").unwrap();
}

#[test]
fn subprocess_crash_before_and_after_atomic_image_index_commit() {
    for after_commit in [false, true] {
        let s = setup();
        let mut nodes = BTreeMap::new();
        for id in VOTERS {
            nodes.insert(id, Running::start(&s.configs[&id], &s.root, 0, id));
        }
        nodes.get_mut(&1).unwrap().send(json!({"op":"campaign"}));
        let leader = find_leader(&mut nodes);
        let mut seq = 1000;
        for c in [
            admin(json!({"command":"seed","key":key(0),"row":row()})),
            admin(json!({"command":"grant","grant":grant(1,5)})),
            admin(
                json!({"command":"provision_task_resources","task":1,"limits":{"read_rows":10000,"disclosed_rows":1000,"encoded_bytes":1000000}}),
            ),
        ] {
            seq += 1;
            let r = nodes.get_mut(&leader).unwrap().request(seq, c);
            assert_eq!(r["reply"]["code"], 0);
        }
        nodes
            .get_mut(&leader)
            .unwrap()
            .send(json!({"op":"crash_next_apply","after_commit":after_commit}));
        nodes[&leader].until(|v| v["crash_armed"] == true, Duration::from_secs(3));
        nodes.get_mut(&leader).unwrap().send(json!({"op":"propose","id":9990,"command":task(TaskAction::Execute(request(90,&[0]),None))}));
        let start = Instant::now();
        loop {
            if let Some(status) = nodes.get_mut(&leader).unwrap().child.try_wait().unwrap() {
                assert_eq!(status.code(), Some(86));
                break;
            }
            assert!(
                start.elapsed() < Duration::from_secs(8),
                "fault not reached"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        drop(nodes.remove(&leader));
        let successor = find_leader(&mut nodes);
        let retry = nodes
            .get_mut(&successor)
            .unwrap()
            .request(9991, task(TaskAction::Execute(request(90, &[0]), None)));
        assert_eq!(retry["reply"]["code"], 0);
        nodes.insert(
            leader,
            Running::start(&s.configs[&leader], &s.root, 1, leader),
        );
        std::thread::sleep(Duration::from_millis(400));
        nodes
            .get_mut(&successor)
            .unwrap()
            .send(json!({"op":"read","id":9992}));
        let read = nodes[&successor].until(|v| v["id"] == 9992, Duration::from_secs(5));
        assert_eq!(
            read["reply"]["data"]["operations"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        let tasks = read["reply"]["data"]["tasks"].as_array().unwrap();
        assert_eq!(tasks[0][1]["transitions"], 1);
        std::fs::write(s.root.join("crash-checker.txt"),format!("PASS process exit86 after_commit={after_commit}; original operation90 exactly once; task transitions1 after failover/restart\n")).unwrap();
    }
}
