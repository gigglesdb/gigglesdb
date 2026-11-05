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
            .env("GIGGLES_NATIVE_TEST_HOOK_ROOT", root.join("native-hooks"))
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
            task: None,
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

// GD17: these clients use the actual external TASK socket, never stdin output.
struct NativeSetup {
    s: Setup,
    addresses: BTreeMap<u64, String>,
    client: native::ClientConfiguration,
    binding: authority::Binding,
}
fn native_setup() -> NativeSetup {
    let s = setup();
    std::fs::create_dir(s.root.join("native-hooks")).unwrap();
    let mut p = CertificateParams::new(Vec::<String>::new()).unwrap();
    p.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    p.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    let ca_key = KeyPair::generate().unwrap();
    let ca = p.self_signed(&ca_key).unwrap();
    let ca_path = s.root.join("task-ca.pem");
    std::fs::write(&ca_path, ca.pem()).unwrap();
    let (cert, key) = leaf(&ca, &ca_key);
    let certificate = s.root.join("task-client.pem");
    let key_path = s.root.join("task-client.key.pem");
    std::fs::write(&certificate, cert.pem()).unwrap();
    std::fs::write(&key_path, key.serialize_pem()).unwrap();
    let binding = authority::Binding {
        fingerprint: fingerprint(cert.der()),
        domain: 42,
        task: giggles_core::TaskId(1),
        principal: giggles_core::Principal(2),
    };
    let mut addresses = BTreeMap::new();
    for id in VOTERS {
        let mut config: Configuration =
            serde_json::from_slice(&std::fs::read(&s.configs[&id]).unwrap()).unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let (cert, key) = leaf(&ca, &ca_key);
        let certificate = s.root.join(format!("task-server-{id}.pem"));
        let key_path = s.root.join(format!("task-server-{id}.key.pem"));
        std::fs::write(&certificate, cert.pem()).unwrap();
        std::fs::write(&key_path, key.serialize_pem()).unwrap();
        config.task = Some(native::Configuration {
            listen: address.clone(),
            certificate,
            key: key_path,
            ca: ca_path.clone(),
        });
        std::fs::write(&s.configs[&id], serde_json::to_vec_pretty(&config).unwrap()).unwrap();
        addresses.insert(id, address);
    }
    let client = native::ClientConfiguration {
        addresses: addresses.values().cloned().collect(),
        certificate,
        key: key_path,
        ca: ca_path,
        server_name: "localhost".into(),
    };
    std::fs::write(
        s.root.join("task-client.json"),
        serde_json::to_vec_pretty(&client).unwrap(),
    )
    .unwrap();
    NativeSetup {
        s,
        addresses,
        client,
        binding,
    }
}
fn native_start(n: &NativeSetup, limits: Value, transitions: u64) -> (BTreeMap<u64, Running>, u64) {
    let mut nodes = VOTERS
        .into_iter()
        .map(|id| (id, Running::start(&n.s.configs[&id], &n.s.root, 0, id)))
        .collect::<BTreeMap<_, _>>();
    nodes.get_mut(&1).unwrap().send(json!({"op":"campaign"}));
    let leader = find_leader(&mut nodes);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let mut g = grant(transitions, 10);
    g.policy.valid_from = now - 10;
    g.policy.expires = now + 3600;
    for id in 0..4 {
        assert_eq!(
            nodes.get_mut(&leader).unwrap().request(
                8000 + id,
                admin(json!({"command":"seed","key":key(id),"row":row()}))
            )["reply"]["code"],
            0
        );
    }
    for (id, c) in [
        admin(json!({"command":"grant","grant":g})),
        admin(json!({"command":"provision_task_resources","task":1,"limits":limits})),
    ]
    .into_iter()
    .enumerate()
    {
        assert_eq!(
            nodes.get_mut(&leader).unwrap().request(8010 + id as u64, c)["reply"]["code"],
            0
        );
    }
    assert_eq!(
        control(
            nodes.get_mut(&leader).unwrap(),
            8020,
            json!({"op":"reload_credentials","expected":0,"bindings":[n.binding]})
        )["reply"]["data"]["epoch"],
        1
    );
    (nodes, leader)
}
fn nr(id: u64, action: TaskAction) -> native::Request {
    native::Request {
        version: 1,
        id,
        deadline_ms: 5000,
        action,
    }
}
async fn nc(n: &NativeSetup, node: u64, r: &native::Request) -> native::Response {
    let mut config = n.client.clone();
    config.addresses = vec![n.addresses[&node].clone()];
    let result = recorded_call(&config, r).await.unwrap();
    let path = n.s.root.join("external-results.jsonl");
    let mut log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap();
    writeln!(
        log,
        "{}",
        json!({"node":node,"request":r,"response":result})
    )
    .unwrap();
    result
}
fn resources(nodes: &mut BTreeMap<u64, Running>, leader: u64) -> Value {
    nodes
        .get_mut(&leader)
        .unwrap()
        .request(8099, admin(json!({"command":"task_resources","task":1})))["reply"]["data"]
        .clone()
}
fn inspect(nodes: &mut BTreeMap<u64, Running>, leader: u64) -> Value {
    let n = nodes.get_mut(&leader).unwrap();
    n.send(json!({"op":"read","id":8098}));
    n.until(|v| v["id"] == 8098, Duration::from_secs(5))["reply"]["data"].clone()
}
fn hook(n: &NativeSetup, stage: &str, id: u64) {
    std::fs::write(
        n.s.root
            .join("native-hooks")
            .join(format!("pause-{stage}-{id}")),
        b"pause",
    )
    .unwrap();
}
async fn reached(n: &NativeSetup, stage: &str, id: u64) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(4);
    while !n
        .s
        .root
        .join("native-hooks")
        .join(format!("reached-{stage}-{id}"))
        .exists()
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "hook not reached {stage} {id}"
        );
        tokio::task::yield_now().await;
    }
}
fn resume(n: &NativeSetup, stage: &str, id: u64) {
    std::fs::write(
        n.s.root
            .join("native-hooks")
            .join(format!("resume-{stage}-{id}")),
        b"resume",
    )
    .unwrap();
}
async fn socket(
    c: &native::ClientConfiguration,
    address: &str,
) -> giggles_consensus::Result<tokio_rustls::client::TlsStream<tokio::net::TcpStream>> {
    let certs = |p: &PathBuf| -> Vec<rustls::pki_types::CertificateDer<'static>> {
        rustls_pemfile::certs(&mut BufReader::new(std::fs::File::open(p).unwrap()))
            .collect::<std::io::Result<_>>()
            .unwrap()
    };
    let mut roots = rustls::RootCertStore::empty();
    for c in certs(&c.ca) {
        roots.add(c)?;
    }
    let key =
        rustls_pemfile::private_key(&mut BufReader::new(std::fs::File::open(&c.key)?))?.unwrap();
    let mut tls = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_client_auth_cert(certs(&c.certificate), key)?;
    tls.alpn_protocols = vec![b"giggles-native/1".to_vec()];
    Ok(tokio_rustls::TlsConnector::from(std::sync::Arc::new(tls))
        .connect(
            rustls::pki_types::ServerName::try_from(c.server_name.clone())?,
            tokio::net::TcpStream::connect(address).await?,
        )
        .await?)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn external_native_three_process_preview_execute_status_accounting_and_cli() {
    let n = native_setup();
    let (mut nodes, leader) = native_start(
        &n,
        json!({"read_rows":10000,"disclosed_rows":4,"encoded_bytes":100000}),
        4,
    );
    let preview = nc(&n, leader, &nr(9, TaskAction::Preview(request(90, &[0])))).await;
    let data = &preview.reply.as_ref().unwrap().data;
    assert_eq!(data["code"], "preview");
    assert_eq!(data["count"], 1);
    assert_eq!(data.as_object().unwrap().len(), 6);
    assert!(!serde_json::to_string(&preview).unwrap().contains("effects"));
    assert!(!serde_json::to_string(&preview).unwrap().contains("token"));
    let execute = nc(
        &n,
        leader,
        &nr(9, TaskAction::Execute(request(90, &[0]), None)),
    )
    .await;
    assert_eq!(execute.reply.as_ref().unwrap().data["code"], "committed");
    let status = nc(
        &n,
        leader,
        &nr(9, TaskAction::Status(giggles_core::OperationId(90))),
    )
    .await;
    assert_eq!(
        status.reply.as_ref().unwrap().data,
        execute.reply.as_ref().unwrap().data
    );
    // Run the installed executable entrypoint too, using a single known leader.
    let mut c = n.client.clone();
    c.addresses = vec![n.addresses[&leader].clone()];
    let cp = n.s.root.join("cli-client.json");
    let rp = n.s.root.join("cli-request.json");
    std::fs::write(&cp, serde_json::to_vec(&c).unwrap()).unwrap();
    std::fs::write(
        &rp,
        serde_json::to_vec(&nr(9, TaskAction::Status(giggles_core::OperationId(90)))).unwrap(),
    )
    .unwrap();
    let cli = Process::new(env!("CARGO_BIN_EXE_giggles-task"))
        .args([cp, rp])
        .output()
        .unwrap();
    std::fs::write(n.s.root.join("client.stdout.json"), &cli.stdout).unwrap();
    std::fs::write(n.s.root.join("client.stderr.txt"), &cli.stderr).unwrap();
    assert!(cli.status.success(), "{:?}", cli);
    let cli: native::Response = serde_json::from_slice(&cli.stdout).unwrap();
    assert_eq!(
        cli.reply.as_ref().unwrap().data,
        status.reply.as_ref().unwrap().data
    );
    let r = resources(&mut nodes, leader);
    assert_eq!(r["used"]["disclosed_rows"], 4);
    let charge = [&preview, &execute, &status, &cli]
        .iter()
        .map(|r| {
            serde_json::to_vec(&r.reply.as_ref().unwrap().data)
                .unwrap()
                .len() as u64
                + 1024
        })
        .sum::<u64>();
    assert_eq!(r["used"]["encoded_bytes"], charge);
    let exhausted = nc(
        &n,
        leader,
        &nr(9, TaskAction::Status(giggles_core::OperationId(90))),
    )
    .await;
    assert_eq!(exhausted.reply.unwrap().data["code"], "42501");
    let after = resources(&mut nodes, leader);
    assert_eq!(after["used"]["disclosed_rows"], r["used"]["disclosed_rows"]);
    assert_eq!(after["used"]["encoded_bytes"], r["used"]["encoded_bytes"]);
    assert_eq!(
        after["used"]["read_rows"].as_u64().unwrap(),
        r["used"]["read_rows"].as_u64().unwrap() + 2
    );
    assert_eq!(inspect(&mut nodes, leader)["tasks"][0][1]["transitions"], 1);
    std::fs::write(n.s.root.join("external-checker.txt"),"PASS external mTLS Preview/Execute/Status/CLI; duplicate caller ids; exact debit; disclosure exhaustion; redacted response; exactly one transition. Local only.\n").unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn external_native_lost_response_failover_status_old_leader_and_no_quorum() {
    let n = native_setup();
    let (mut nodes, leader) = native_start(
        &n,
        json!({"read_rows":10000,"disclosed_rows":100,"encoded_bytes":1000000}),
        4,
    );
    let follower = VOTERS.into_iter().find(|i| *i != leader).unwrap();
    assert_eq!(
        nc(
            &n,
            follower,
            &nr(1, TaskAction::Status(giggles_core::OperationId(777)))
        )
        .await
        .error
        .as_deref(),
        Some("unavailable")
    );
    hook(&n, "write", 2);
    let c = {
        let mut c = n.client.clone();
        c.addresses = vec![n.addresses[&leader].clone()];
        c
    };
    let task = tokio::spawn(async move {
        recorded_call(&c, &nr(2, TaskAction::Execute(request(777, &[0]), None))).await
    });
    reached(&n, "write", 2).await;
    assert_eq!(inspect(&mut nodes, leader)["tasks"][0][1]["transitions"], 1);
    nodes.get_mut(&leader).unwrap().kill();
    drop(nodes.remove(&leader));
    assert!(task.await.unwrap().is_err());
    let successor = find_leader(&mut nodes);
    resume(&n, "write", 2);
    let retry = nc(
        &n,
        successor,
        &nr(2, TaskAction::Execute(request(777, &[0]), None)),
    )
    .await; // write gate belongs to id2: resume it before retry
    assert_eq!(retry.reply.as_ref().unwrap().data["code"], "committed");
    let status = nc(
        &n,
        successor,
        &nr(2, TaskAction::Status(giggles_core::OperationId(777))),
    )
    .await;
    assert_eq!(retry.reply.unwrap().data, status.reply.unwrap().data);
    assert_eq!(
        inspect(&mut nodes, successor)["tasks"][0][1]["transitions"],
        1
    );
    // Remove the other voter: surviving former leader cannot acknowledge current data.
    let other = *nodes.keys().find(|i| **i != successor).unwrap();
    nodes.get_mut(&other).unwrap().kill();
    drop(nodes.remove(&other));
    let mut c = n.client.clone();
    c.addresses = vec![n.addresses[&successor].clone()];
    let mut req = nr(3, TaskAction::Status(giggles_core::OperationId(777)));
    req.deadline_ms = 700;
    let result = recorded_call(&c, &req).await;
    assert!(
        result.as_ref().map_or(true, |r| r.reply.is_none()),
        "no quorum returned data: {result:?}"
    );
    std::fs::write(n.s.root.join("external-failover-checker.txt"),"PASS actual external lost response after final authority check; durable original operation; leader death; retry/status; follower refusal; surviving old leader no quorum withheld data.\n").unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn external_native_reload_held_epoch_aba_deadlines_and_session_reclamation() {
    let n = native_setup();
    let (mut nodes, leader) = native_start(
        &n,
        json!({"read_rows":10000,"disclosed_rows":200,"encoded_bytes":1000000}),
        4,
    );
    let config = || {
        let mut c = n.client.clone();
        c.addresses = vec![n.addresses[&leader].clone()];
        c
    };
    // Hold a committed decision/release, revoke binding before final local check.
    hook(&n, "held", 20);
    let c = config();
    let pending = tokio::spawn(async move {
        recorded_call(&c, &nr(20, TaskAction::Execute(request(200, &[0]), None))).await
    });
    reached(&n, "held", 20).await;
    assert_eq!(
        control(
            nodes.get_mut(&leader).unwrap(),
            8100,
            json!({"op":"reload_credentials","expected":1,"bindings":[]})
        )["reply"]["data"]["epoch"],
        2
    );
    resume(&n, "held", 20);
    let denied = pending.await.unwrap().unwrap();
    assert_eq!(denied.error.as_deref(), Some("indeterminate"));
    assert!(denied.reply.is_none());
    // Re-add same credential: old session (paused before proposal) remains fenced
    // even when the binding's payload is byte-for-byte identical.
    assert_eq!(
        control(
            nodes.get_mut(&leader).unwrap(),
            8101,
            json!({"op":"reload_credentials","expected":2,"bindings":[n.binding]})
        )["reply"]["data"]["epoch"],
        3
    );
    hook(&n, "before", 21);
    let c = config();
    let pending = tokio::spawn(async move {
        recorded_call(
            &c,
            &nr(21, TaskAction::Status(giggles_core::OperationId(200))),
        )
        .await
    });
    reached(&n, "before", 21).await;
    assert_eq!(
        control(
            nodes.get_mut(&leader).unwrap(),
            8102,
            json!({"op":"reload_credentials","expected":3,"bindings":[n.binding]})
        )["reply"]["data"]["epoch"],
        4
    );
    resume(&n, "before", 21);
    assert_eq!(
        pending.await.unwrap().unwrap().error.as_deref(),
        Some("unavailable")
    );
    let recovered = nc(
        &n,
        leader,
        &nr(22, TaskAction::Status(giggles_core::OperationId(200))),
    )
    .await;
    assert_eq!(recovered.reply.unwrap().data["code"], "committed");
    // Deadline while still before proposal: operation remains absent.
    hook(&n, "before", 23);
    let c = config();
    let pending = tokio::spawn(async move {
        let mut r = nr(23, TaskAction::Execute(request(201, &[1]), None));
        r.deadline_ms = 400;
        recorded_call(&c, &r).await
    });
    reached(&n, "before", 23).await;
    assert!(pending.await.unwrap().is_err());
    resume(&n, "before", 23);
    assert_eq!(
        nc(
            &n,
            leader,
            &nr(24, TaskAction::Status(giggles_core::OperationId(201)))
        )
        .await
        .reply
        .unwrap()
        .data["code"],
        "absent"
    );
    // Deadline after committed business decision: no rollback and no refund.
    hook(&n, "held", 25);
    let c = config();
    let pending = tokio::spawn(async move {
        let mut r = nr(25, TaskAction::Execute(request(202, &[1]), None));
        r.deadline_ms = 700;
        recorded_call(&c, &r).await
    });
    reached(&n, "held", 25).await;
    assert!(pending.await.unwrap().is_err());
    assert_eq!(
        nc(
            &n,
            leader,
            &nr(26, TaskAction::Status(giggles_core::OperationId(202)))
        )
        .await
        .reply
        .unwrap()
        .data["code"],
        "committed"
    );
    // More than32 independent connections are reclaimed; no trusted-stdin
    // lifetime session map is used by this listener.
    for id in 100..140 {
        assert!(
            nc(
                &n,
                leader,
                &nr(id, TaskAction::Status(giggles_core::OperationId(200)))
            )
            .await
            .reply
            .is_some()
        );
    }
    std::thread::sleep(Duration::from_secs(11)); // Deliberate idle test, not state-transition evidence.
    assert_eq!(
        nc(
            &n,
            leader,
            &nr(150, TaskAction::Status(giggles_core::OperationId(200)))
        )
        .await
        .reply
        .unwrap()
        .data["code"],
        "committed"
    );
    assert_eq!(inspect(&mut nodes, leader)["tasks"][0][1]["transitions"], 2);
    std::fs::write(n.s.root.join("external-lifecycle-checker.txt"),"PASS held revoke; same binding re-add/overlap epoch fences; reconnect; deadline before proposal absent; deadline after commit retained;40 reclaimed connections;11second idle actor clock.\n").unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn external_native_untrusted_grammar_certificates_frames_caps_and_stalls() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let n = native_setup();
    let (mut nodes, leader) = native_start(
        &n,
        json!({"read_rows":10000,"disclosed_rows":200,"encoded_bytes":1000000}),
        4,
    );
    let addr = &n.addresses[&leader];
    let before = resources(&mut nodes, leader);
    let mut plain = tokio::net::TcpStream::connect(addr).await.unwrap();
    plain.write_all(b"{\"op\":\"propose\"}\n").await.unwrap();
    plain.shutdown().await.unwrap();
    let mut b = vec![];
    let _ = tokio::time::timeout(Duration::from_secs(3), plain.read_to_end(&mut b)).await;
    assert!(!b.windows(5).any(|w| w == b"reply"));
    // Valid chain but no committed identity binding (task server leaf).
    let config: Configuration =
        serde_json::from_slice(&std::fs::read(&n.s.configs[&leader]).unwrap()).unwrap();
    let task = config.task.unwrap();
    let mut unbound = n.client.clone();
    unbound.certificate = task.certificate;
    unbound.key = task.key;
    if let Ok(mut tls) = socket(&unbound, addr).await {
        let _ = native::write_frame(
            &mut tls,
            &serde_json::to_vec(&nr(1, TaskAction::Status(giggles_core::OperationId(1)))).unwrap(),
        )
        .await;
        assert!(native::read_frame(&mut tls).await.is_err());
    }
    // Peer leaf chains to a separate CA and must not enter task authority.
    let mut peer = n.client.clone();
    peer.certificate = n.s.config.certificate.clone();
    peer.key = n.s.config.key.clone();
    if let Ok(mut tls) = socket(&peer, addr).await {
        let _ = native::write_frame(&mut tls, b"{}").await;
        assert!(native::read_frame(&mut tls).await.is_err());
    }
    let mut wrong_name = n.client.clone();
    wrong_name.server_name = "wrong.invalid".into();
    assert!(socket(&wrong_name, addr).await.is_err());
    let base =
        serde_json::to_value(nr(1, TaskAction::Status(giggles_core::OperationId(1)))).unwrap();
    let mut bads = vec![
        json!({"version":1,"id":1,"deadline_ms":5000,"action":{"Admin":[]}}),
        json!({"version":2,"id":1,"deadline_ms":5000,"action":{"Status":1}}),
    ];
    for field in [
        "principal",
        "task",
        "domain",
        "lower",
        "upper",
        "epoch",
        "session",
    ] {
        let mut v = base.clone();
        v[field] = json!(1);
        bads.push(v);
    }
    let mut nested = serde_json::to_value(nr(1, TaskAction::Preview(request(11, &[0])))).unwrap();
    nested["action"]["Preview"]["principal"] = json!(1);
    bads.push(nested);
    std::fs::write(
        n.s.root.join("untrusted-requests.json"),
        serde_json::to_vec_pretty(&bads).unwrap(),
    )
    .unwrap();
    for bad in bads {
        let mut tls = socket(&n.client, addr).await.unwrap();
        native::write_frame(&mut tls, &serde_json::to_vec(&bad).unwrap())
            .await
            .unwrap();
        let r: native::Response =
            serde_json::from_slice(&native::read_frame(&mut tls).await.unwrap()).unwrap();
        assert_eq!(r.error.as_deref(), Some("protocol"));
        assert!(r.reply.is_none());
    }
    for size in [0, native::MAX_FRAME as u32 + 1, u32::MAX] {
        let mut tls = socket(&n.client, addr).await.unwrap();
        tls.write_u32(size).await.unwrap();
        tls.flush().await.unwrap();
        assert!(native::read_frame(&mut tls).await.is_err());
    }
    assert_eq!(
        resources(&mut nodes, leader),
        before,
        "untrusted requests changed task accounting"
    );
    // Actual accepted TLS sessions occupy16 slots, including stalled readers.
    let mut held = Vec::new();
    for _ in 0..native::MAX_SESSIONS {
        held.push(socket(&n.client, addr).await.unwrap());
    }
    let extra = socket(&n.client, addr).await;
    assert!(extra.is_err(), "connection cap admitted another session");
    drop(held);
    // Observe recovery by a successful socket request, not a timed assumption.
    let deadline = Instant::now() + Duration::from_secs(4);
    loop {
        let mut c = n.client.clone();
        c.addresses = vec![addr.clone()];
        if let Ok(r) = recorded_call(
            &c,
            &nr(60, TaskAction::Status(giggles_core::OperationId(60))),
        )
        .await
            && r.reply.is_some()
        {
            break;
        }
        assert!(Instant::now() < deadline);
    }
    // Stalled body is forcibly reclaimed by connection lifetime.
    let mut stalled = socket(&n.client, addr).await.unwrap();
    stalled.write_u32(100).await.unwrap();
    stalled.write_all(b"{").await.unwrap();
    stalled.flush().await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(6), native::read_frame(&mut stalled))
            .await
            .unwrap()
            .is_err()
    );
    // A post-check stalled write remains bounded and retains charged output.
    hook(&n, "write", 61);
    let mut c = n.client.clone();
    c.addresses = vec![addr.clone()];
    let pending = tokio::spawn(async move {
        let mut r = nr(61, TaskAction::Status(giggles_core::OperationId(61)));
        r.deadline_ms = 500;
        recorded_call(&c, &r).await
    });
    reached(&n, "write", 61).await;
    assert!(pending.await.unwrap().is_err());
    assert_eq!(
        nc(
            &n,
            leader,
            &nr(62, TaskAction::Status(giggles_core::OperationId(62)))
        )
        .await
        .reply
        .unwrap()
        .data["code"],
        "absent"
    );
    std::fs::write(n.s.root.join("external-adversarial-checker.txt"),"PASS plaintext, unbound and peer cert, wrong server name, authority/command injection, nested unknown field, version, zero/oversize frame,16-session cap/recovery, stalled frame deadline, bounded post-check write stall.\n").unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn external_native_concurrent_last_allowance_exact_byte_boundary_and_read_exhaustion() {
    let n = native_setup();
    let (mut nodes, leader) = native_start(
        &n,
        json!({"read_rows":10000,"disclosed_rows":3,"encoded_bytes":1000000}),
        1,
    );
    let mut tasks = Vec::new();
    for op in 0..8 {
        let mut c = n.client.clone();
        c.addresses = vec![n.addresses[&leader].clone()];
        tasks.push(tokio::spawn(async move {
            recorded_call(
                &c,
                &nr(7, TaskAction::Execute(request(300 + op, &[op % 4]), None)),
            )
            .await
        }));
    }
    let mut successful = 0;
    for (op, t) in tasks.into_iter().enumerate() {
        let r = t.await.unwrap().unwrap();
        assert_eq!(r.id, 7);
        if let Some(reply) = r.reply {
            assert_eq!(reply.data.as_object().unwrap().len(), 6);
            if !reply.data["operation"].is_null() {
                assert_eq!(reply.data["operation"], 300 + op as u64);
            }
            if reply.data["code"] == "committed" {
                successful += reply.data["count"].as_u64().unwrap();
            }
        }
    }
    assert!(successful <= 1);
    assert_eq!(inspect(&mut nodes, leader)["tasks"][0][1]["transitions"], 1);
    let r = resources(&mut nodes, leader);
    assert_eq!(r["used"]["disclosed_rows"], 3);
    // Separate actual clusters fix the exact wire-data debit boundary. An absent
    // status performs one read unit; the release fence adds no disclosure debit.
    let absent =
        json!({"version":1,"code":"absent","operation":123,"count":null,"digest":null,"sql":null});
    let charge = serde_json::to_vec(&absent).unwrap().len() as u64 + 1024;
    for (bytes, allowed) in [(charge - 1, false), (charge, true)] {
        let n = native_setup();
        let (mut ns, l) = native_start(
            &n,
            json!({"read_rows":2,"disclosed_rows":10,"encoded_bytes":bytes}),
            1,
        );
        let r = nc(
            &n,
            l,
            &nr(1, TaskAction::Status(giggles_core::OperationId(123))),
        )
        .await;
        assert_eq!(
            r.reply.unwrap().data["code"],
            if allowed { "absent" } else { "42501" }
        );
        let used = resources(&mut ns, l);
        assert_eq!(
            used["used"]["encoded_bytes"],
            if allowed { charge } else { 0 }
        );
        assert_eq!(used["used"]["read_rows"], 1);
    }
    let read_n = native_setup();
    let (mut ns, l) = native_start(
        &read_n,
        json!({"read_rows":1,"disclosed_rows":10,"encoded_bytes":100000}),
        1,
    );
    assert_eq!(
        nc(
            &read_n,
            l,
            &nr(1, TaskAction::Status(giggles_core::OperationId(123)))
        )
        .await
        .reply
        .unwrap()
        .data["code"],
        "absent"
    );
    assert_eq!(
        nc(&read_n, l, &nr(2, TaskAction::Preview(request(124, &[0]))))
            .await
            .reply
            .unwrap()
            .data["code"],
        "54000"
    );
    assert_eq!(resources(&mut ns, l)["used"]["read_rows"], 1);
    std::fs::write(n.s.root.join("external-concurrency-checker.txt"),"PASS8 concurrent external TLS calls sharing caller id; one lifetime transition;3 disclosure slots; exact encoded-byte boundary and read exhaustion in additional actual clusters.\n").unwrap();
}

async fn recorded_call(
    c: &native::ClientConfiguration,
    r: &native::Request,
) -> giggles_consensus::Result<native::Response> {
    let result = native::call(c, r).await;
    static LOG: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let _guard = LOG.lock().unwrap();
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(
            c.certificate
                .parent()
                .unwrap()
                .join("external-client-attempts.jsonl"),
        )
        .unwrap();
    writeln!(file,"{}",json!({"addresses":c.addresses,"certificate":c.certificate,"request":r,"response":result.as_ref().ok(),"transport_error":result.as_ref().err().map(|e|e.to_string())})).unwrap();
    result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn external_native_certificate_expiry_disconnect_and_task_revoke_at_release() {
    let n = native_setup();
    let (mut nodes, _leader) = native_start(
        &n,
        json!({"read_rows":10000,"disclosed_rows":100,"encoded_bytes":1000000}),
        4,
    );
    // CA signs short-lived client. Hold output until an observed wall-clock time
    // after notAfter, then release the gate; established TLS must be revalidated.
    // A separate task CA key is retained only in this fixture's memory, so issue
    // another fixture CA and configure the task CA bundle before process start
    // is unnecessary: use the peer CA as additional trust via local restart.
    for node in nodes.values_mut() {
        node.kill();
    }
    nodes.clear();
    for id in VOTERS {
        let config: Configuration =
            serde_json::from_slice(&std::fs::read(&n.s.configs[&id]).unwrap()).unwrap();
        let path = config.task.unwrap().ca;
        let mut pem = std::fs::read_to_string(&path).unwrap();
        if !pem.contains(&n.s.ca.pem()) {
            pem.push_str(&n.s.ca.pem());
            std::fs::write(&path, pem).unwrap();
        }
    }
    for id in VOTERS {
        nodes.insert(id, Running::start(&n.s.configs[&id], &n.s.root, 1, id));
    }
    let leader = find_leader(&mut nodes);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let mut params = CertificateParams::new(vec!["localhost".into()]).unwrap();
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    params.not_before = rcgen::date_time_ymd(1970, 1, 1) + Duration::from_secs(now - 5);
    params.not_after = rcgen::date_time_ymd(1970, 1, 1) + Duration::from_secs(now + 2);
    let key = KeyPair::generate().unwrap();
    let cert = params.signed_by(&key, &n.s.ca, &n.s.ca_key).unwrap();
    let cp = n.s.root.join("short-client.pem");
    let kp = n.s.root.join("short-client.key.pem");
    std::fs::write(&cp, cert.pem()).unwrap();
    std::fs::write(&kp, key.serialize_pem()).unwrap();
    let peer_binding = authority::Binding {
        fingerprint: n.s.config.peers[&1].fingerprint.clone(),
        ..n.binding.clone()
    };
    let short = authority::Binding {
        fingerprint: fingerprint(cert.der()),
        ..n.binding.clone()
    };
    assert_eq!(
        control(
            nodes.get_mut(&leader).unwrap(),
            8200,
            json!({"op":"reload_credentials","expected":1,"bindings":[n.binding,short,peer_binding]})
        )["reply"]["data"]["epoch"],
        2
    );
    let mut peer_client = n.client.clone();
    peer_client.addresses = vec![n.addresses[&leader].clone()];
    peer_client.certificate = n.s.config.certificate.clone();
    peer_client.key = n.s.config.key.clone();
    assert!(
        recorded_call(
            &peer_client,
            &nr(69, TaskAction::Status(giggles_core::OperationId(700)))
        )
        .await
        .is_err(),
        "even CA-valid committed peer binding must not gain task role"
    );
    hook(&n, "held", 70);
    let mut c = n.client.clone();
    c.addresses = vec![n.addresses[&leader].clone()];
    c.certificate = cp;
    c.key = kp;
    let exp_c = c.clone();
    let pending = tokio::spawn(async move {
        recorded_call(&c, &nr(70, TaskAction::Execute(request(700, &[0]), None))).await
    });
    reached(&n, "held", 70).await;
    while std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        <= now + 2
    {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    resume(&n, "held", 70);
    assert!(pending.await.unwrap().is_err());
    // Expired leaf also fails new authentication.
    assert!(
        recorded_call(
            &exp_c,
            &nr(71, TaskAction::Status(giggles_core::OperationId(700)))
        )
        .await
        .is_err()
    );
    assert_eq!(
        nc(
            &n,
            leader,
            &nr(72, TaskAction::Status(giggles_core::OperationId(700)))
        )
        .await
        .reply
        .unwrap()
        .data["code"],
        "committed"
    );
    // Before-proposal disconnect cancellation prevents submission.
    hook(&n, "before", 73);
    let mut tls = socket(&n.client, &n.addresses[&leader]).await.unwrap();
    native::write_frame(
        &mut tls,
        &serde_json::to_vec(&nr(73, TaskAction::Execute(request(701, &[1]), None))).unwrap(),
    )
    .await
    .unwrap();
    reached(&n, "before", 73).await;
    drop(tls);
    resume(&n, "before", 73);
    assert_eq!(
        nc(
            &n,
            leader,
            &nr(74, TaskAction::Status(giggles_core::OperationId(701)))
        )
        .await
        .reply
        .unwrap()
        .data["code"],
        "absent"
    );
    // After-commit disconnect does not roll back business or refund disclosure.
    hook(&n, "held", 75);
    let mut tls = socket(&n.client, &n.addresses[&leader]).await.unwrap();
    native::write_frame(
        &mut tls,
        &serde_json::to_vec(&nr(75, TaskAction::Execute(request(702, &[1]), None))).unwrap(),
    )
    .await
    .unwrap();
    reached(&n, "held", 75).await;
    let before = resources(&mut nodes, leader);
    drop(tls);
    resume(&n, "held", 75);
    assert_eq!(
        nc(
            &n,
            leader,
            &nr(76, TaskAction::Status(giggles_core::OperationId(702)))
        )
        .await
        .reply
        .unwrap()
        .data["code"],
        "committed"
    );
    assert_eq!(
        resources(&mut nodes, leader)["used"]["disclosed_rows"]
            .as_u64()
            .unwrap(),
        before["used"]["disclosed_rows"].as_u64().unwrap() + 1
    );
    // Current task revoke after release proof is also checked at final write.
    hook(&n, "held", 77);
    let mut c = n.client.clone();
    c.addresses = vec![n.addresses[&leader].clone()];
    let pending = tokio::spawn(async move {
        recorded_call(
            &c,
            &nr(77, TaskAction::Status(giggles_core::OperationId(700))),
        )
        .await
    });
    reached(&n, "held", 77).await;
    assert_eq!(
        nodes
            .get_mut(&leader)
            .unwrap()
            .request(8201, admin(json!({"command":"revoke","task":1})))["reply"]["code"],
        0
    );
    resume(&n, "held", 77);
    assert_eq!(
        pending.await.unwrap().unwrap().error.as_deref(),
        Some("indeterminate")
    );
    std::fs::write(n.s.root.join("external-expiry-checker.txt"),"PASS actual held TLS client expiry and expired reconnect; before-proposal disconnect absent; after-commit disconnect survives without refund; task revoke at final release.\n").unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn external_native_isolated_old_leader_held_term_and_configured_node_retry() {
    let n = native_setup();
    let (mut nodes, leader) = native_start(
        &n,
        json!({"read_rows":10000,"disclosed_rows":100,"encoded_bytes":1000000}),
        4,
    );
    hook(&n, "held", 80);
    let mut c = n.client.clone();
    c.addresses = vec![n.addresses[&leader].clone()];
    let pending = tokio::spawn(async move {
        recorded_call(&c, &nr(80, TaskAction::Execute(request(800, &[0]), None))).await
    });
    reached(&n, "held", 80).await;
    let old = nodes.get_mut(&leader).unwrap();
    old.send(json!({"op":"pause_peers","paused":true}));
    old.until(|v| v["peers_paused"] == true, Duration::from_secs(2));
    // Observe the remaining majority's actual election, never sleep as proof.
    let start = Instant::now();
    let successor = loop {
        let mut found = None;
        for (id, node) in &mut nodes {
            if *id != leader && node.diag()["authority_ready"] == true {
                found = Some(*id);
                break;
            }
        }
        if let Some(id) = found {
            break id;
        }
        assert!(
            start.elapsed() < Duration::from_secs(4),
            "majority failed election"
        );
    };
    let mut old_c = n.client.clone();
    old_c.addresses = vec![n.addresses[&leader].clone()];
    let mut r = nr(81, TaskAction::Status(giggles_core::OperationId(800)));
    r.deadline_ms = 400;
    let rejected = recorded_call(&old_c, &r).await;
    assert!(rejected.as_ref().map_or(true, |r| r.reply.is_none()));
    resume(&n, "held", 80);
    let result = pending.await.unwrap();
    assert!(result.as_ref().map_or(true, |r| r.reply.is_none()));
    // Follower -> live leader routing with the identical canonical business op.
    let mut route = n.client.clone();
    let follower = *nodes
        .keys()
        .find(|id| **id != leader && **id != successor)
        .unwrap();
    route.addresses = vec![
        n.addresses[&follower].clone(),
        n.addresses[&successor].clone(),
    ];
    let retry = recorded_call(
        &route,
        &nr(80, TaskAction::Execute(request(800, &[0]), None)),
    )
    .await
    .unwrap();
    assert_eq!(retry.reply.unwrap().data["code"], "committed");
    assert_eq!(
        nc(
            &n,
            successor,
            &nr(80, TaskAction::Status(giggles_core::OperationId(800)))
        )
        .await
        .reply
        .unwrap()
        .data["code"],
        "committed"
    );
    assert_eq!(
        inspect(&mut nodes, successor)["tasks"][0][1]["transitions"],
        1
    );
    std::fs::write(n.s.root.join("external-isolated-leader-checker.txt"),"PASS actual peer-isolated old leader withheld current data and held output; remaining majority elected; same caller ID on new origin; configured follower->leader retry original business operation exactly once.\n").unwrap();
}
