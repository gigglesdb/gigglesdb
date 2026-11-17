#![cfg(feature = "engine-rocksdb")]
//! N-node cluster process harness (GD-0075 R5-63): 3..=6 real `giggles-server` processes in
//! cluster mode, every node a SQL gateway over pre-split range groups. Every directed peer link
//! (node A dialling node B) runs through its own giggles-faultlab proxy, so a test can cut a
//! node off the network while its process keeps running.

use giggles_core::range_api::{RangeSpace, range_key};
use giggles_faultlab::proxy::{Direction, Fault, MAX_LINK_MILLIS, Proxy, ProxyLimits};
use rcgen::{BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    io::{BufRead, BufReader},
    net::{SocketAddr, TcpListener},
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::mpsc::{self, Receiver, Sender},
    thread::JoinHandle,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio_postgres::{Client, Config, NoTls, config::SslMode};

/// A starting node waits for the directory and every range leader; generous on a loaded host.
const START_WAIT: Duration = Duration::from_secs(90);
/// The owner role reaches non-bootstrap gateways shortly after the bootstrap node creates it.
const CONNECT_WAIT: Duration = Duration::from_secs(30);
const POLL: Duration = Duration::from_millis(50);
const STDERR_TAIL: usize = 40;

/// Rows and index entries of table ids 1..=8 each in their own range (as giggles-core's gateway
/// tests split), so two tables' rows sit in different range groups.
pub(crate) fn split_keys() -> Vec<String> {
    let mut keys: Vec<Vec<u8>> = [RangeSpace::Rows, RangeSpace::Index]
        .into_iter()
        .flat_map(|space| (1..=8_u64).map(move |id| range_key(space, &id.to_be_bytes())))
        .collect();
    keys.sort();
    keys.into_iter().map(hex::encode).collect()
}

struct Node {
    dir: PathBuf,
    pg: SocketAddr,
    listen: SocketAddr,
    reservations: Vec<TcpListener>,
    child: Option<Child>,
    reader: Option<JoinHandle<()>>,
    /// The stdout line announcing a serving gateway, since the last start.
    ready: Option<String>,
    /// Extra environment for the node's next starts.
    env: Vec<(String, String)>,
}

/// `n` cluster-mode server processes (node ids 1..=n; node 1 bootstraps) with persistent node
/// directories, peers wired through per-directed-link fault proxies.
pub(crate) struct ClusterProcs {
    nodes: Vec<Node>,
    links: BTreeMap<(usize, usize), Proxy>,
    tx: Sender<(usize, String)>,
    rx: Receiver<(usize, String)>,
    root: PathBuf,
}

impl ClusterProcs {
    /// Writes certificates and configurations, starts every node and waits until each serves.
    pub(crate) async fn new(n: usize) -> Self {
        Self::build(n, false).await
    }

    /// Three original members and a CA-signed spare, absent from original peer lists.
    pub(crate) async fn with_spare() -> Self {
        Self::build(4, true).await
    }

    async fn build(n: usize, spare: bool) -> Self {
        assert!((3..=6).contains(&n), "3..=6 nodes, not {n}");
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let root = std::env::temp_dir().join(format!(
            "cluster-process-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&root).unwrap();
        let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let ca_key = KeyPair::generate().unwrap();
        let ca_cert = ca_params.self_signed(&ca_key).unwrap();
        std::fs::write(root.join("ca.pem"), ca_cert.pem()).unwrap();
        let mut fingerprints = Vec::new();
        let mut nodes = Vec::new();
        for i in 0..n {
            let dir = root.join(format!("node-{}", i + 1));
            std::fs::create_dir_all(&dir).unwrap();
            let key = KeyPair::generate().unwrap();
            let mut params = CertificateParams::new(vec!["localhost".into()]).unwrap();
            params.extended_key_usages = vec![
                ExtendedKeyUsagePurpose::ServerAuth,
                ExtendedKeyUsagePurpose::ClientAuth,
            ];
            let cert = params.signed_by(&key, &ca_cert, &ca_key).unwrap();
            fingerprints.push(hex::encode(Sha256::digest(cert.der())));
            std::fs::write(dir.join("server.pem"), cert.pem()).unwrap();
            std::fs::write(dir.join("server.key.TEST-ONLY"), key.serialize_pem()).unwrap();
            let reservations: Vec<_> = (0..2)
                .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
                .collect();
            nodes.push(Node {
                dir,
                pg: reservations[0].local_addr().unwrap(),
                listen: reservations[1].local_addr().unwrap(),
                reservations,
                child: None,
                env: Vec::new(),
                reader: None,
                ready: None,
            });
        }
        // Peer streams are long-lived; the proxy must not expire them mid-run.
        let limits = ProxyLimits {
            connections: 64,
            buffer_bytes: 65536,
            events: 65536,
            connect_millis: 1000,
            lifetime_millis: MAX_LINK_MILLIS,
            idle_millis: MAX_LINK_MILLIS,
        };
        let mut links = BTreeMap::new();
        for from in 0..n {
            for to in (0..n).filter(|&to| to != from) {
                let proxy = Proxy::start(nodes[to].listen, limits.clone()).await;
                links.insert((from, to), proxy.unwrap());
            }
        }
        let split_keys = split_keys();
        for (i, node) in nodes.iter().enumerate() {
            let peers: Vec<_> = (0..n)
                .filter(|&j| j != i && (!spare || i == n - 1 || j != n - 1))
                .map(|j| json!({"id": j + 1, "address": links[&(i, j)].address(), "fingerprint": fingerprints[j]}))
                .collect();
            let config = json!({
                "database": node.dir.join("db"), "native": "127.0.0.1:0", "postgres": "127.0.0.1:0",
                "certificate": node.dir.join("server.pem"), "private_key": node.dir.join("server.key.TEST-ONLY"),
                "client_ca": root.join("ca.pem"), "bindings": [], "connections": 32,
                "skew_seconds": 0, "max_pause_seconds": 3600,
                "postgresql": {"listen": node.pg, "tls": "disabled", "loopback_trust": true},
                "cluster": {"node_id": i + 1, "cluster": "0123456789abcdef0123456789abcdef",
                    "listen": node.listen, "log_dir": node.dir.join("raftlog"),
                    "tick_ms": 50, "owner": "owner", "peers": peers, "split_keys": split_keys,
                    "initial_members": (1..=if spare { n - 1 } else { n }).collect::<Vec<_>>()}
            });
            std::fs::write(
                node.dir.join("config.json"),
                serde_json::to_vec_pretty(&config).unwrap(),
            )
            .unwrap();
        }
        let (tx, rx) = mpsc::channel();
        let mut cluster = Self {
            nodes,
            links,
            tx,
            rx,
            root,
        };
        for i in 0..if spare { n - 1 } else { n } {
            cluster.start(i);
        }
        for i in 0..if spare { n - 1 } else { n } {
            cluster.ready(i).await;
        }
        cluster
    }

    /// Enrolment arguments for the spare, using its actual listening address.
    pub(crate) fn spare_identity(&self) -> (String, String) {
        let node = self.nodes.last().unwrap();
        let pem = std::fs::read(node.dir.join("server.pem")).unwrap();
        let cert = rustls_pemfile::certs(&mut &pem[..])
            .next()
            .unwrap()
            .unwrap();
        (node.listen.to_string(), hex::encode(Sha256::digest(cert)))
    }

    pub(crate) fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Borrow a live child for control-pipe and process-exit assertions.
    pub(crate) fn child_mut(&mut self, i: usize) -> &mut Child {
        self.nodes[i].child.as_mut().expect("node not started")
    }

    /// Spawns node i's process (stderr appended to `stderr.log` in its directory).
    pub(crate) fn start(&mut self, i: usize) {
        let node = &mut self.nodes[i];
        assert!(node.child.is_none(), "node {} is running", i + 1);
        node.ready = None;
        node.reservations.clear();
        let stderr = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(node.dir.join("stderr.log"))
            .unwrap();
        let mut child = Command::new(env!("CARGO_BIN_EXE_giggles-server"))
            .arg(node.dir.join("config.json"))
            .env("GIGGLES_OWNER_PASSWORD", "cluster-test-only")
            .envs(node.env.iter().map(|(k, v)| (k.as_str(), v.as_str())))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(stderr)
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
            if token(&line, "preview") == Some("cluster") {
                assert_eq!(token(&line, "node"), Some((i + 1).to_string().as_str()));
                assert_eq!(
                    token(&line, "postgresql"),
                    Some(self.nodes[i].pg.to_string().as_str()),
                    "{line}"
                );
                self.nodes[i].ready = Some(line);
            }
        }
    }

    /// Waits for node i's readiness line; panics if the process exits or the wait expires.
    pub(crate) async fn ready(&mut self, i: usize) {
        let deadline = Instant::now() + START_WAIT;
        loop {
            self.refresh();
            if self.nodes[i].ready.is_some() {
                return;
            }
            let child = self.nodes[i].child.as_mut().expect("node not started");
            if let Some(status) = child.try_wait().unwrap() {
                panic!("node {} exited before serving: {status}", i + 1);
            }
            assert!(
                Instant::now() < deadline,
                "node {} not serving within {START_WAIT:?}",
                i + 1
            );
            tokio::time::sleep(POLL).await;
        }
    }

    /// Hard-kills node i's process (no shutdown command).
    pub(crate) fn kill(&mut self, i: usize) {
        if let Some(mut child) = self.nodes[i].child.take() {
            let _ = child.kill();
            child.wait().unwrap();
        }
        if let Some(reader) = self.nodes[i].reader.take() {
            reader.join().unwrap();
        }
        self.refresh();
        self.nodes[i].ready = None;
    }

    /// Sets an environment variable for node i's later starts (for example a crash hook).
    pub(crate) fn set_env(&mut self, i: usize, key: &str, value: &str) {
        self.nodes[i].env.push((key.into(), value.into()));
    }

    /// Waits until node i's process exits on its own (a crash hook); panics after `wait`.
    pub(crate) async fn exited(&mut self, i: usize, wait: Duration) {
        let deadline = Instant::now() + wait;
        loop {
            let child = self.nodes[i].child.as_mut().expect("node not started");
            if child.try_wait().unwrap().is_some() {
                self.kill(i);
                return;
            }
            assert!(
                Instant::now() < deadline,
                "node {} still runs after {wait:?}",
                i + 1
            );
            tokio::time::sleep(POLL).await;
        }
    }

    /// Node i's stderr log, across its starts.
    pub(crate) fn stderr(&self, i: usize) -> String {
        std::fs::read_to_string(self.nodes[i].dir.join("stderr.log")).unwrap_or_default()
    }

    /// Kills node i if it runs, starts it again over its directories and waits until it serves.
    pub(crate) async fn restart(&mut self, i: usize) {
        self.kill(i);
        self.start(i);
        self.ready(i).await;
    }

    /// Node i's PostgreSQL listener (fixed across restarts).
    /// Node i's server configuration file (for operator commands run against the cluster).
    pub(crate) fn config_path(&self, i: usize) -> PathBuf {
        self.nodes[i].dir.join("config.json")
    }
    pub(crate) fn pg_addr(&self, i: usize) -> SocketAddr {
        self.nodes[i].pg
    }

    /// A session to node i as `owner` (loopback trust), retried while the role propagates.
    pub(crate) async fn client(&self, i: usize) -> Client {
        connect(self.nodes[i].pg).await
    }

    fn link(&self, from: usize, to: usize, fault: Fault) {
        let proxy = &self.links[&(from, to)];
        proxy.set(Direction::Upstream, fault).unwrap();
        proxy.set(Direction::Downstream, fault).unwrap();
    }

    /// Cuts every peer link to and from node i (silent partition: bytes held, no resets).
    pub(crate) fn isolate(&self, i: usize) {
        for o in (0..self.len()).filter(|&o| o != i) {
            self.link(i, o, Fault::Partition);
            self.link(o, i, Fault::Partition);
        }
    }

    /// Restores every peer link.
    pub(crate) fn heal(&self) {
        for &(from, to) in self.links.keys() {
            self.link(from, to, Fault::Pass);
        }
    }

    /// No proxy dropped trace events (an overflowing proxy also refuses new connections).
    pub(crate) fn assert_proxies_unsaturated(&self) {
        for (link, proxy) in &self.links {
            assert!(!proxy.trace().overflow, "proxy {link:?} trace overflowed");
        }
    }

    /// Lines of node i's `stderr.log` that contain `needle` (statement errors are logged
    /// there with their SQLSTATE).
    pub(crate) fn stderr_lines(&self, i: usize, needle: &str) -> usize {
        std::fs::read_to_string(self.nodes[i].dir.join("stderr.log"))
            .unwrap_or_default()
            .lines()
            .filter(|line| line.contains(needle))
            .count()
    }

    fn print_stderr_tails(&self) {
        for (i, node) in self.nodes.iter().enumerate() {
            let text = std::fs::read_to_string(node.dir.join("stderr.log")).unwrap_or_default();
            let lines: Vec<_> = text.lines().collect();
            let tail = &lines[lines.len().saturating_sub(STDERR_TAIL)..];
            eprintln!("--- node {} stderr (last {} lines) ---", i + 1, tail.len());
            for line in tail {
                eprintln!("{line}");
            }
        }
    }
}

impl Drop for ClusterProcs {
    fn drop(&mut self) {
        for node in &mut self.nodes {
            if let Some(mut child) = node.child.take() {
                let _ = child.kill();
                let _ = child.wait();
            }
            if let Some(reader) = node.reader.take() {
                let _ = reader.join();
            }
        }
        if std::thread::panicking() {
            self.print_stderr_tails();
            eprintln!("cluster directories kept at {}", self.root.display());
        } else {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }
}

/// A session as `owner` (loopback trust) to a PostgreSQL listener or a relay in front of one,
/// retried while the role propagates to a non-bootstrap gateway.
pub(crate) async fn connect(address: SocketAddr) -> Client {
    let mut config = Config::new();
    config
        .user("owner")
        .dbname("postgres")
        .ssl_mode(SslMode::Disable)
        .connect_timeout(Duration::from_secs(5))
        .host("127.0.0.1")
        .port(address.port());
    let deadline = tokio::time::Instant::now() + CONNECT_WAIT;
    loop {
        match tokio::time::timeout_at(deadline, config.connect(NoTls)).await {
            Ok(Ok((client, connection))) => {
                tokio::spawn(async move {
                    let _ = connection.await;
                });
                return client;
            }
            Ok(Err(error)) => assert!(
                tokio::time::Instant::now() < deadline,
                "could not connect to {address}: {error}"
            ),
            Err(_) => panic!("connecting to {address} timed out"),
        }
        tokio::time::sleep(POLL).await;
    }
}

pub(crate) fn token<'a>(line: &'a str, key: &str) -> Option<&'a str> {
    line.split_whitespace()
        .filter_map(|s| s.split_once('='))
        .find_map(|(k, v)| (k == key).then_some(v))
}
