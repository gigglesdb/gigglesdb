#![allow(dead_code)]
use giggles_consensus::{
    transport::{Configuration, Peer, fingerprint},
    *,
};
use giggles_core::*;
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose,
};
use serde_json::{Value, json};
use std::collections::BTreeSet;
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
        let mut child = Process::new(node_binary())
            .arg(path)
            .env("GIGGLES_NATIVE_TEST_HOOK_ROOT", root.join("native-hooks"))
            .env("GIGGLES_PGWIRE_TEST_HOOK_ROOT", root.join("pgwire-hooks"))
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
    fn request(&mut self, id: u64, command: giggles_consensus::Command) -> Value {
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
            pgwire: None,
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

pub fn key(n: u64) -> Key {
    Key {
        tenant: TenantId(42),
        row: RowId(n),
    }
}
pub fn row() -> Row {
    Row {
        status: Status::Failed,
        failed_at: 103,
        attempt_count: 0,
        version: 999,
    }
}
pub fn grant(limit: u64, threshold: u64) -> Grant {
    Grant {
        task: TaskId(1),
        issuer: Principal(1),
        executor: Principal(2),
        approver: Principal(3),
        policy: Policy {
            tenant: TenantId(42),
            columns: BTreeSet::from([Column::Status]),
            transitions: BTreeSet::from([(Status::Failed, Status::Queued)]),
            incident: (100, 107),
            distinct_limit: limit,
            transition_limit: limit,
            approval_above: threshold,
            valid_from: 1,
            expires: 1000,
        },
    }
}
pub fn request(op: u64, ids: &[u64]) -> Request {
    Request {
        operation: OperationId(op),
        selection: Selection::Keys(ids.iter().map(|i| key(*i)).collect()),
        column: Column::Status,
        after: Status::Queued,
    }
}
pub fn admin(v: serde_json::Value) -> Command {
    Command::Admin(serde_json::to_vec(&v).unwrap())
}
pub fn task(action: TaskAction) -> Command {
    Command::Task {
        task: TaskId(1),
        principal: Principal(2),
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

fn node_binary() -> PathBuf {
    std::env::var_os("GIGGLES_NODE_EXE")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/debug/giggles-node.exe")
        })
}
fn fresh(name: &str) -> PathBuf {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    for _ in 0..32 {
        let p = std::env::temp_dir().join(format!(
            "giggles-client-{name}-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        match std::fs::create_dir(&p) {
            Ok(()) => {
                eprintln!("retained client fixture: {}", p.display());
                return p;
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => panic!("fixture creation failed: {e}"),
        }
    }
    panic!("fixture unique name attempts exhausted")
}

#[test]
fn concurrent_fixture_names_are_exclusive() {
    let threads = (0..16)
        .map(|_| std::thread::spawn(|| fresh("uniqueness")))
        .collect::<Vec<_>>();
    let paths = threads
        .into_iter()
        .map(|t| t.join().unwrap())
        .collect::<BTreeSet<_>>();
    assert_eq!(paths.len(), 16);
}
fn sdk_config(n: &NativeSetup, ids: &[u64]) -> giggles_client::Config {
    giggles_client::Config {
        endpoints: ids
            .iter()
            .map(|id| n.addresses[id].parse().unwrap())
            .collect(),
        ca: n.client.ca.clone(),
        certificate: n.client.certificate.clone(),
        key: n.client.key.clone(),
        server_name: n.client.server_name.clone(),
        limits: giggles_client::Limits::default(),
    }
}
fn sdk(n: &NativeSetup, ids: &[u64]) -> giggles_client::Client {
    giggles_client::Client::new(sdk_config(n, ids)).unwrap()
}
fn data(o: giggles_client::Outcome) -> Value {
    match o {
        giggles_client::Outcome::Recorded(r) => r.into_reply().data,
        other => panic!("expected recorded: {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_execute_repeat_status_failover_noquorum_and_invalid_certificate() {
    use giggles_client::{Operation, Outcome};
    let n = native_setup();
    let (mut nodes, leader) = native_start(
        &n,
        json!({"read_rows":10000,"disclosed_rows":100,"encoded_bytes":1000000}),
        4,
    );
    let c = sdk(&n, &[leader]);
    // The fixture persists the business request before any application send.
    let op = Operation::execute(100, request(777, &[0]), None, 5000).unwrap();
    std::fs::write(n.s.root.join("persisted-operation.json"), op.wire_bytes()).unwrap();
    let preview = data(c.preview(99, request(777, &[0]), 5000).await.unwrap());
    assert_eq!(preview["code"], "preview");
    let result = data(c.send(&op).await);
    assert_eq!(result["code"], "committed");
    assert_eq!(data(c.send(&op).await), result);
    assert_eq!(
        data(c.status(101, OperationId(777), 5000).await.unwrap()),
        result
    );
    let client_path = n.s.root.join("sdk-config.json");
    std::fs::write(
        &client_path,
        serde_json::to_vec(&sdk_config(&n, &[leader])).unwrap(),
    )
    .unwrap();
    let cli = Process::new(env!("CARGO_BIN_EXE_giggles-client"))
        .arg(client_path)
        .arg(n.s.root.join("persisted-operation.json"))
        .output()
        .unwrap();
    assert!(
        cli.status.success(),
        "CLI failed: {}",
        String::from_utf8_lossy(&cli.stderr)
    );
    let cli_reply: giggles_core::local::Reply = serde_json::from_slice(&cli.stdout).unwrap();
    assert_eq!(cli_reply.data, result);
    assert_eq!(
        data(
            c.execute(107, request(777, &[2]), None, 5000)
                .await
                .unwrap()
        )["code"],
        "GD002"
    );
    // Distinct task and peer CA: a peer certificate cannot authenticate as task.
    let mut bad = sdk_config(&n, &[leader]);
    bad.certificate = n.s.config.certificate.clone();
    bad.key = n.s.config.key.clone();
    let outcome = giggles_client::Client::new(bad)
        .unwrap()
        .status(102, OperationId(777), 500)
        .await
        .unwrap();
    assert!(
        !matches!(outcome, Outcome::Recorded(_)),
        "invalid mTLS released data"
    );
    // Real committed reply loss: server gate after final release proof before write.
    let id = 103;
    std::fs::write(
        n.s.root
            .join("native-hooks")
            .join(format!("pause-write-{id}")),
        b"pause",
    )
    .unwrap();
    let op = Operation::execute(id, request(778, &[1]), None, 5000).unwrap();
    let task = tokio::spawn({
        let c = c.clone();
        async move { c.send(&op).await }
    });
    let end = tokio::time::Instant::now() + Duration::from_secs(4);
    while !n
        .s
        .root
        .join("native-hooks")
        .join(format!("reached-write-{id}"))
        .exists()
    {
        assert!(tokio::time::Instant::now() < end, "write gate not reached");
        tokio::task::yield_now().await;
    }
    nodes.get_mut(&leader).unwrap().kill();
    nodes.retain(|id, _| *id != leader);
    assert!(matches!(
        task.await.unwrap(),
        Outcome::Unknown {
            operation_id: OperationId(778),
            ..
        }
    ));
    let successor = find_leader(&mut nodes);
    // Synchronize fixture provisioning visibility before testing native delivery.
    let ready = nodes.get_mut(&successor).unwrap();
    ready.send(json!({"op":"authenticate","fingerprint":n.binding.fingerprint}));
    let auth = ready.until(
        |v| v.get("session").is_some() || v.get("error").is_some(),
        Duration::from_secs(3),
    );
    assert!(
        auth.get("session").is_some(),
        "successor binding not ready: {auth}"
    );
    let c = sdk(&n, &[successor]);
    let recovered = data(c.status(104, OperationId(778), 5000).await.unwrap());
    assert_eq!(recovered["code"], "committed");
    let replay = data(
        c.execute(105, request(778, &[1]), None, 5000)
            .await
            .unwrap(),
    );
    assert_eq!(replay, recovered);
    let state = nodes.get_mut(&successor).unwrap();
    let snapshot = state.request(9900, admin(json!({"command":"inspect"})));
    assert_eq!(snapshot["reply"]["data"]["tasks"][0][1]["transitions"], 2);
    let other = *nodes.keys().find(|id| **id != successor).unwrap();
    nodes.get_mut(&other).unwrap().kill();
    nodes.retain(|id, _| *id != other);
    let mut cfg = sdk_config(&n, &[successor]);
    cfg.limits.operation = Duration::from_millis(700);
    cfg.limits.request = Duration::from_millis(600);
    assert!(!matches!(
        giggles_client::Client::new(cfg)
            .unwrap()
            .status(106, OperationId(777), 600)
            .await
            .unwrap(),
        Outcome::Recorded(_)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_exhausted_disclosure_and_revoked_task_preserve_authority() {
    use giggles_client::Outcome;
    let n = native_setup();
    let (mut nodes, leader) = native_start(
        &n,
        json!({"read_rows":10000,"disclosed_rows":0,"encoded_bytes":1000000}),
        4,
    );
    let c = sdk(&n, &[leader]);
    assert!(matches!(
        c.execute(200, request(880, &[0]), None, 5000)
            .await
            .unwrap(),
        Outcome::Unknown { .. }
    ));
    assert!(matches!(
        c.status(201, OperationId(880), 5000).await.unwrap(),
        Outcome::Denied(_)
    ));
    // Revoke through the trusted fixture control, never through the SDK.
    assert_eq!(
        nodes
            .get_mut(&leader)
            .unwrap()
            .request(9020, admin(json!({"command":"revoke","task":1})))["reply"]["code"],
        0
    );
    assert!(!matches!(
        c.status(202, OperationId(880), 5000).await.unwrap(),
        Outcome::Recorded(_)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_revocation_with_remaining_disclosure_has_negative_control() {
    use giggles_client::Outcome;
    let n = native_setup();
    let (mut nodes, leader) = native_start(
        &n,
        json!({"read_rows":10000,"disclosed_rows":100,"encoded_bytes":1000000}),
        4,
    );
    let c = sdk(&n, &[leader]);
    // Establish that this exact credential and disclosure pool work before revoke.
    assert_eq!(
        data(c.status(301, OperationId(991), 5000).await.unwrap())["code"],
        "absent"
    );
    assert_eq!(
        nodes
            .get_mut(&leader)
            .unwrap()
            .request(9030, admin(json!({"command":"revoke","task":1})))["reply"]["code"],
        0
    );
    let revoked_status = c.status(302, OperationId(991), 5000).await.unwrap();
    eprintln!("revoked status: {revoked_status:?}");
    assert!(matches!(
        revoked_status,
        Outcome::Denied(_) | Outcome::Unknown { .. }
    ));
    assert!(matches!(
        c.execute(303, request(991, &[0]), None, 5000)
            .await
            .unwrap(),
        Outcome::Unknown {
            operation_id: OperationId(991),
            ..
        }
    ));
    let snapshot = nodes
        .get_mut(&leader)
        .unwrap()
        .request(9031, admin(json!({"command":"inspect"})));
    assert_eq!(snapshot["reply"]["data"]["tasks"][0][1]["transitions"], 0);
}
