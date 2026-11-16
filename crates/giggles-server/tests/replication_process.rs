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
use tokio_postgres::{Client, Config, NoTls, config::SslMode, error::SqlState};

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
            "replication-process-{}-{}-{}",
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
        let nodes: Vec<_> = (0..3)
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
                .filter(|(j, _)| *j != i)
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
                "replication": {"node_id": i + 1, "cluster": "0123456789abcdef0123456789abcdef",
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
    async fn follower_until(&mut self, i: usize, deadline: Instant) {
        loop {
            self.refresh();
            if token(&self.nodes[i].latest, "role") == Some("follower") {
                return;
            }
            assert!(Instant::now() < deadline, "node {} did not become follower: {}", i + 1, self.nodes[i].latest);
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
async fn refused(config: &Config) {
    match tokio::time::timeout(WAIT, config.connect(NoTls)).await.unwrap() {
        Err(error) => assert_eq!(error.code(), Some(&SqlState::CANNOT_CONNECT_NOW), "{error}"),
        Ok(_) => panic!("follower accepted startup"),
    }
}

#[tokio::test]
async fn follower_setup_retries_after_occupied_port_is_released() {
    let mut cluster = Cluster::new();
    let leader = cluster.leader().await;
    let follower = (0..3).find(|&i| i != leader).unwrap();
    cluster.kill(follower);
    let occupied = TcpListener::bind(cluster.nodes[follower].pg).unwrap();
    let stderr = cluster.nodes[follower].dir.join("stderr.log");
    let prior = std::fs::metadata(&stderr).unwrap().len() as usize;
    cluster.start(follower);
    let deadline = Instant::now() + WAIT;
    loop {
        assert!(cluster.nodes[follower].child.as_mut().unwrap().try_wait().unwrap().is_none(),
            "follower exited after setup failure");
        let log = std::fs::read_to_string(&stderr).unwrap();
        if log[prior..].contains("replication: follower setup failed:") {
            break;
        }
        assert!(Instant::now() < deadline, "no follower setup failure logged");
        tokio::time::sleep(POLL).await;
    }
    cluster.refresh();
    assert!(cluster.nodes[follower].latest.is_empty(), "failed setup announced a role");
    drop(occupied);
    cluster.follower_until(follower, Instant::now() + WAIT).await;
    refused(&cluster.config(&[follower])).await;
}

#[tokio::test]
async fn three_process_failover() {
    let mut cluster = Cluster::new();
    let leader = cluster.leader().await;
    let mut order: Vec<_> = (0..3).filter(|&i| i != leader).collect();
    for &i in &order {
        refused(&cluster.config(&[i])).await;
    }
    order.push(leader);
    let config = cluster.config(&order);
    let client = connect(&config).await;
    // Every preceding host refuses startup; a successful query therefore reaches the leader.
    client.batch_execute("CREATE TABLE t (id BIGINT PRIMARY KEY, v TEXT)").await.unwrap();
    for id in 0..100_i64 {
        client.execute("INSERT INTO t (id, v) VALUES ($1, $2)", &[&id, &format!("row-{id}")]).await.unwrap();
    }
    cluster.kill(leader);
    let replacement = cluster.leader().await;
    assert_ne!(replacement, leader);
    let client = connect(&config).await;
    let rows = tokio::time::timeout(WAIT, client.query("SELECT id, v FROM t ORDER BY id", &[])).await.unwrap().unwrap();
    assert_eq!(rows.len(), 100);
    for (id, row) in rows.iter().enumerate() {
        assert_eq!(row.get::<_, i64>(0), id as i64);
        assert_eq!(row.get::<_, String>(1), format!("row-{id}"));
    }
    for id in 100..110_i64 {
        client.execute("INSERT INTO t (id, v) VALUES ($1, $2)", &[&id, &format!("row-{id}")]).await.unwrap();
    }
    cluster.start(leader);
    cluster.follower_until(leader, Instant::now() + WAIT).await;
    refused(&cluster.config(&[leader])).await;
}

#[tokio::test]
async fn old_leader_session_is_terminated() {
    let mut cluster = Cluster::new();
    let leader = cluster.leader().await;
    let client = connect(&cluster.config(&[leader])).await;
    client.simple_query("SELECT 1").await.unwrap();
    let followers: Vec<_> = (0..3).filter(|&i| i != leader).collect();
    for &i in &followers {
        cluster.kill(i);
    }
    let deadline = Instant::now() + Duration::from_secs(15);
    // Observe demotion before submitting the next statement on the old session.
    cluster.follower_until(leader, deadline).await;
    let result =
        tokio::time::timeout(deadline.saturating_duration_since(Instant::now()), client.simple_query("SELECT 1"))
            .await
            .expect("old leader session did not terminate within 15 seconds");
    match result {
        Err(error) => assert!(error.is_closed() || error.code() == Some(&SqlState::ADMIN_SHUTDOWN), "{error}"),
        Ok(rows) => panic!("old leader returned rows after losing its majority: {rows:?}"),
    }
    for i in followers {
        cluster.start(i);
    }
    cluster.leader().await;
}
