#![cfg(feature = "engine-rocksdb")]

use rcgen::{BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    io::{BufRead, BufReader},
    net::{SocketAddr, TcpListener},
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::mpsc::{self, Receiver, Sender},
    thread::JoinHandle,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio_postgres::{Client, Config, NoTls, config::SslMode};

const WAIT: Duration = Duration::from_secs(20);
const POLL: Duration = Duration::from_millis(50);

struct Node {
    dir: PathBuf,
    pg: SocketAddr,
    fingerprint: String,
    reservations: Vec<TcpListener>,
    child: Option<Child>,
    reader: Option<JoinHandle<()>>,
    latest: String,
}
struct Cluster {
    nodes: Vec<Node>,
    tx: Sender<(usize, String)>,
    rx: Receiver<(usize, String)>,
    root: PathBuf,
}
impl Cluster {
    fn new() -> Self {
        // Concurrent tests can receive the same coarse Windows system timestamp.
        static NEXT_CLUSTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let root = std::env::temp_dir().join(format!(
            "replication-replace-process-{}-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos(),
            NEXT_CLUSTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&root).unwrap();
        let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let ca_key = KeyPair::generate().unwrap();
        let ca_cert = ca_params.self_signed(&ca_key).unwrap();
        std::fs::write(root.join("ca.pem"), ca_cert.pem()).unwrap();
        let (tx, rx) = mpsc::channel();
        let nodes: Vec<_> = (0..4)
            .map(|i| {
                let dir = root.join(format!("node-{}", i + 1));
                std::fs::create_dir_all(&dir).unwrap();
                let key = KeyPair::generate().unwrap();
                let mut params = CertificateParams::new(vec!["localhost".into()]).unwrap();
                params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth, ExtendedKeyUsagePurpose::ClientAuth];
                let cert = params.signed_by(&key, &ca_cert, &ca_key).unwrap();
                let fingerprint = hex::encode(Sha256::digest(cert.der()));
                std::fs::write(dir.join("server.pem"), cert.pem()).unwrap();
                std::fs::write(dir.join("server.key.TEST-ONLY"), key.serialize_pem()).unwrap();
                let reservations: Vec<_> = (0..2).map(|_| TcpListener::bind("127.0.0.1:0").unwrap()).collect();
                Node {
                    dir,
                    pg: reservations[0].local_addr().unwrap(),
                    fingerprint,
                    reservations,
                    child: None,
                    reader: None,
                    latest: String::new(),
                }
            })
            .collect();
        for (i, node) in nodes.iter().enumerate() {
            let peers: Vec<_> = nodes
                .iter()
                .enumerate()
                .filter(|(j, _)| *j != i && *j < 3 && (i != 3 || *j != 0))
                .map(|(j, n)| {
                    json!({"id": j + 1, "address": n.reservations[1].local_addr().unwrap(), "fingerprint": n.fingerprint})
                })
                .collect();
            let config = json!({
                "database": node.dir.join("db"), "native": "127.0.0.1:0", "postgres": "127.0.0.1:0",
                "certificate": node.dir.join("server.pem"), "private_key": node.dir.join("server.key.TEST-ONLY"),
                "client_ca": root.join("ca.pem"), "bindings": [], "connections": 32,
                "skew_seconds": 0, "max_pause_seconds": 3600,
                "postgresql": {"listen": node.pg, "tls": "disabled", "loopback_trust": true},
                "replication": {"node_id": i + 1, "join": i == 3, "cluster": "0123456789abcdef0123456789abcdef",
                    "listen": node.reservations[1].local_addr().unwrap(), "log_dir": node.dir.join("raftlog"),
                    "tick_ms": 20, "owner": "owner", "peers": peers}
            });
            std::fs::write(node.dir.join("config.json"), serde_json::to_vec_pretty(&config).unwrap()).unwrap();
        }
        let mut cluster = Self { nodes, tx, rx, root };
        for i in 0..3 {
            cluster.start(i);
        }
        cluster
    }
    fn start(&mut self, i: usize) {
        let node = &mut self.nodes[i];
        assert!(node.child.is_none());
        node.latest.clear();
        node.reservations.clear();
        let mut child = Command::new(env!("CARGO_BIN_EXE_giggles-server"))
            .arg(node.dir.join("config.json"))
            .env("GIGGLES_OWNER_PASSWORD", "replication-test-only")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(std::fs::OpenOptions::new().create(true).append(true).open(node.dir.join("stderr.log")).unwrap())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        node.child = Some(child);
        let tx = self.tx.clone();
        node.reader = Some(std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                if tx.send((i, line)).is_err() {
                    break;
                }
            }
        }));
    }
    fn refresh(&mut self) {
        while let Ok((i, line)) = self.rx.try_recv() {
            if token(&line, "preview") == Some("replicated") && token(&line, "role").is_some() {
                assert_eq!(token(&line, "node"), Some((i + 1).to_string().as_str()));
                assert_eq!(token(&line, "postgresql"), Some(self.nodes[i].pg.to_string().as_str()));
                self.nodes[i].latest = line;
            }
        }
    }
    fn kill(&mut self, i: usize) {
        if let Some(mut child) = self.nodes[i].child.take() {
            let _ = child.kill();
            child.wait().unwrap();
        }
        if let Some(reader) = self.nodes[i].reader.take() {
            reader.join().unwrap();
        }
        self.refresh();
        self.nodes[i].latest.clear();
    }
    async fn leader(&mut self) -> usize {
        let deadline = Instant::now() + WAIT;
        loop {
            self.refresh();
            let leaders: Vec<_> = self
                .nodes
                .iter()
                .enumerate()
                .filter(|(_, n)| n.child.is_some() && token(&n.latest, "role") == Some("leader"))
                .map(|(i, _)| i)
                .collect();
            // Wait for every live node's initial role announcement too.
            if leaders.len() == 1 && self.nodes.iter().all(|n| n.child.is_none() || !n.latest.is_empty()) {
                return leaders[0];
            }
            assert!(
                Instant::now() < deadline,
                "no unique leader: {:?}",
                self.nodes.iter().map(|n| &n.latest).collect::<Vec<_>>()
            );
            tokio::time::sleep(POLL).await;
        }
    }
    fn config(&self, order: &[usize]) -> Config {
        let mut config = Config::new();
        config.user("owner").dbname("postgres").ssl_mode(SslMode::Disable).connect_timeout(Duration::from_secs(2));
        for &i in order {
            config.host("127.0.0.1").port(self.nodes[i].pg.port());
        }
        config
    }
    fn stop_all(&mut self) {
        for i in 0..self.nodes.len() {
            self.kill(i);
        }
    }
}
impl Drop for Cluster {
    fn drop(&mut self) {
        self.stop_all();
        let _ = std::fs::remove_dir_all(&self.root);
    }
}
fn token<'a>(line: &'a str, key: &str) -> Option<&'a str> {
    line.split_whitespace().filter_map(|s| s.split_once('=')).find_map(|(k, v)| (k == key).then_some(v))
}
async fn connect(config: &Config) -> Client {
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        match tokio::time::timeout_at(deadline, config.connect(NoTls)).await {
            Ok(Ok((client, connection))) => {
                tokio::spawn(async move {
                    let _ = connection.await;
                });
                return client;
            }
            Ok(Err(error)) => assert!(tokio::time::Instant::now() < deadline, "could not connect: {error}"),
            Err(error) => panic!("connection deadline expired: {error}"),
        }
        tokio::time::sleep(POLL).await;
    }
}

async fn admin(cluster: &Cluster, survivor: usize, args: &[String]) -> std::process::Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_giggles-server"))
        .args(["replica", &args[0]])
        .arg(cluster.nodes[survivor].dir.join("config.json"))
        .args(&args[1..]).stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().unwrap();
    let deadline = Instant::now() + Duration::from_secs(95);
    loop {
        if child.try_wait().unwrap().is_some() { return child.wait_with_output().unwrap(); }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let output = child.wait_with_output().unwrap();
            panic!("admin deadline: {output:?}");
        }
        tokio::time::sleep(POLL).await;
    }
}

#[tokio::test]
async fn replace_lost_voter_and_fail_over() {
    let mut cluster = Cluster::new();
    let leader = cluster.leader().await;
    let client = connect(&cluster.config(&[0, 1, 2])).await;
    tokio::time::timeout(WAIT, client.batch_execute("CREATE TABLE t (id BIGINT PRIMARY KEY); INSERT INTO t VALUES (1), (2)")).await.unwrap().unwrap();
    let lost = (0..3).find(|&i| i != leader).unwrap();
    cluster.kill(lost);
    // The joining node lists exactly the two surviving voters.
    let path = cluster.nodes[3].dir.join("config.json");
    let mut config: serde_json::Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    config["replication"]["peers"] = serde_json::Value::Array((0..3).filter(|&i| i != lost).map(|i| {
        let c: serde_json::Value = serde_json::from_slice(&std::fs::read(cluster.nodes[i].dir.join("config.json")).unwrap()).unwrap();
        json!({"id": i+1, "address": c["replication"]["listen"], "fingerprint": cluster.nodes[i].fingerprint})
    }).collect());
    std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
    cluster.start(3);
    let survivor = (0..3).find(|&i| i != lost && i != leader).unwrap();
    let output = admin(&cluster, survivor, &[
        "replace".into(), "--lost".into(), (lost+1).to_string(), "--new-id".into(), "4".into(),
        "--new-address".into(), config["replication"]["listen"].as_str().unwrap().into(),
        "--new-fingerprint".into(), cluster.nodes[3].fingerprint.clone(), "--timeout-secs".into(), "60".into(),
    ]).await;
    assert!(output.status.success(), "{output:?}");
    let text = String::from_utf8(output.stdout).unwrap();
    let voters: Vec<_> = (1..=4).filter(|&id| id != lost+1).collect();
    assert!(text.contains(&format!("voters: {voters:?}")), "{text}");
    assert!(text.contains(&format!("retired: [{}]", lost+1)), "{text}");
    let status = admin(&cluster, survivor, &["status".into()]).await;
    assert!(status.status.success(), "{status:?}");
    assert!(String::from_utf8(status.stdout).unwrap().contains("step: idle"));
    tokio::time::timeout(WAIT, client.batch_execute("INSERT INTO t VALUES (3), (4)")).await.unwrap().unwrap();
    let rows = tokio::time::timeout(WAIT, client.query("SELECT id FROM t ORDER BY id", &[])).await.unwrap().unwrap();
    assert_eq!(rows.iter().map(|r| r.get::<_,i64>(0)).collect::<Vec<_>>(), [1,2,3,4]);
    let current = cluster.leader().await;
    cluster.kill(current);
    let elected = cluster.leader().await;
    assert_ne!(elected, current);
    let order: Vec<_> = (0..4).filter(|&i| i != lost && i != current).collect();
    let client = connect(&cluster.config(&order)).await;
    let rows = tokio::time::timeout(WAIT, client.query("SELECT id FROM t ORDER BY id", &[])).await.unwrap().unwrap();
    assert_eq!(rows.iter().map(|r| r.get::<_,i64>(0)).collect::<Vec<_>>(), [1,2,3,4]);
    tokio::time::timeout(WAIT, client.batch_execute("INSERT INTO t VALUES (5)")).await.unwrap().unwrap();
}
