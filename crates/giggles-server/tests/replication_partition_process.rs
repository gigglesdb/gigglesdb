#![cfg(feature = "engine-rocksdb")]
//! R4-30: cut a live leader off the network between three real server processes.
//!
//! Every directed peer link (node A dialling node B) runs through its own
//! giggles-faultlab TCP proxy. Replication peer streams are one-way (A's Raft
//! messages to B travel only on the connection A dials), so a proxy per
//! directed link can cut one direction of a pair without the other. The
//! processes keep running throughout; only the network changes.

use crate::replication_harness::{connect, token};
use giggles_core::{
    Limits,
    relational::Value,
    replicated::{AppliedPosition, BatchSink, ReplicaStore, ReplicateError},
};
use giggles_faultlab::proxy::{Direction, Fault, MAX_LINK_MILLIS, Proxy, ProxyLimits};
use rcgen::{BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::{BufRead, BufReader},
    net::{SocketAddr, TcpListener},
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::{
        Arc,
        mpsc::{self, Receiver, Sender},
    },
    thread::JoinHandle,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio_postgres::{Client, Config, NoTls, config::SslMode, error::SqlState};

const WAIT: Duration = Duration::from_secs(20);
const POLL: Duration = Duration::from_millis(50);

struct Node {
    dir: PathBuf,
    pg: SocketAddr,
    listen: SocketAddr,
    reservations: Vec<TcpListener>,
    child: Option<Child>,
    reader: Option<JoinHandle<()>>,
    /// Every role announcement since the cluster was created, in order.
    roles: Vec<String>,
}

/// Three server processes whose peer links all pass through fault proxies.
struct Cluster {
    nodes: Vec<Node>,
    links: BTreeMap<(usize, usize), Proxy>,
    tx: Sender<(usize, String)>,
    rx: Receiver<(usize, String)>,
    root: PathBuf,
}

impl Cluster {
    async fn new() -> Self {
        static NEXT_CLUSTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let root = std::env::temp_dir().join(format!(
            "replication-partition-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT_CLUSTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&root).unwrap();
        let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let ca_key = KeyPair::generate().unwrap();
        let ca_cert = ca_params.self_signed(&ca_key).unwrap();
        std::fs::write(root.join("ca.pem"), ca_cert.pem()).unwrap();
        let mut fingerprints = Vec::new();
        let mut nodes = Vec::new();
        for i in 0..3 {
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
                reader: None,
                roles: Vec::new(),
            });
        }
        // Peer streams are long-lived and carry nothing back to the dialler, so
        // the proxy must not expire them by lifetime or idleness mid-run.
        let limits = ProxyLimits {
            connections: 64,
            buffer_bytes: 65536,
            events: 65536,
            connect_millis: 1000,
            lifetime_millis: MAX_LINK_MILLIS,
            idle_millis: MAX_LINK_MILLIS,
        };
        let mut links = BTreeMap::new();
        for from in 0..3 {
            for to in (0..3).filter(|&to| to != from) {
                links.insert(
                    (from, to),
                    Proxy::start(nodes[to].listen, limits.clone())
                        .await
                        .unwrap(),
                );
            }
        }
        for (i, node) in nodes.iter().enumerate() {
            let peers: Vec<_> = (0..3)
                .filter(|&j| j != i)
                .map(|j| json!({"id": j + 1, "address": links[&(i, j)].address(), "fingerprint": fingerprints[j]}))
                .collect();
            let config = json!({
                "database": node.dir.join("db"), "native": "127.0.0.1:0", "postgres": "127.0.0.1:0",
                "certificate": node.dir.join("server.pem"), "private_key": node.dir.join("server.key.TEST-ONLY"),
                "client_ca": root.join("ca.pem"), "bindings": [], "connections": 32,
                "skew_seconds": 0, "max_pause_seconds": 3600,
                "postgresql": {"listen": node.pg, "tls": "disabled", "loopback_trust": true},
                "replication": {"node_id": i + 1, "cluster": "0123456789abcdef0123456789abcdef",
                    "listen": node.listen, "log_dir": node.dir.join("raftlog"),
                    "tick_ms": 20, "owner": "owner", "peers": peers}
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
        for i in 0..3 {
            cluster.start(i);
        }
        cluster
    }

    fn start(&mut self, i: usize) {
        let node = &mut self.nodes[i];
        assert!(node.child.is_none());
        node.reservations.clear();
        let mut child = Command::new(env!("CARGO_BIN_EXE_giggles-server"))
            .arg(node.dir.join("config.json"))
            .env("GIGGLES_OWNER_PASSWORD", "replication-test-only")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(
                std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(node.dir.join("stderr.log"))
                    .unwrap(),
            )
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
                self.nodes[i].roles.push(line);
            }
        }
    }

    /// The role node i announced most recently while running, if any.
    fn role(&self, i: usize) -> Option<&str> {
        self.nodes[i].child.as_ref()?;
        self.nodes[i]
            .roles
            .last()
            .and_then(|line| token(line, "role"))
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
        // A restarted process announces its role afresh.
        self.nodes[i].roles.push(String::new());
    }

    /// The single running node announcing leadership, once every running node has announced.
    async fn leader(&mut self) -> usize {
        let deadline = Instant::now() + WAIT;
        loop {
            self.refresh();
            let leaders: Vec<_> = (0..3).filter(|&i| self.role(i) == Some("leader")).collect();
            if leaders.len() == 1
                && (0..3).all(|i| self.nodes[i].child.is_none() || self.role(i).is_some())
            {
                return leaders[0];
            }
            assert!(
                Instant::now() < deadline,
                "no unique leader: {:?}",
                self.latest()
            );
            tokio::time::sleep(POLL).await;
        }
    }

    async fn until_follower(&mut self, i: usize) {
        let deadline = Instant::now() + WAIT;
        loop {
            self.refresh();
            if self.role(i) == Some("follower") {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "node {} did not step down: {:?}",
                i + 1,
                self.latest()
            );
            tokio::time::sleep(POLL).await;
        }
    }

    fn latest(&self) -> Vec<Option<&str>> {
        (0..3).map(|i| self.role(i)).collect()
    }

    /// Apply `fault` to both directions of the connections `from` dials to `to`,
    /// which carry every Raft message from `from` to `to`.
    fn link(&self, from: usize, to: usize, fault: Fault) {
        let proxy = &self.links[&(from, to)];
        proxy.set(Direction::Upstream, fault).unwrap();
        proxy.set(Direction::Downstream, fault).unwrap();
    }

    fn isolate(&self, n: usize, fault: Fault) {
        for o in (0..3).filter(|&o| o != n) {
            self.link(n, o, fault);
            self.link(o, n, fault);
        }
    }

    fn heal(&self) {
        for &(from, to) in self.links.keys() {
            self.link(from, to, Fault::Pass);
        }
    }

    fn config(&self, order: &[usize]) -> Config {
        let mut config = Config::new();
        config
            .user("owner")
            .dbname("postgres")
            .ssl_mode(SslMode::Disable)
            .connect_timeout(Duration::from_secs(2));
        for &i in order {
            config.host("127.0.0.1").port(self.nodes[i].pg.port());
        }
        config
    }

    /// Stop every process and read each node's store directly: applied Raft
    /// position and the ids in table t. Retried after a restart when a node
    /// had not yet learned the final commit index when it was stopped.
    async fn converged(&mut self) -> Vec<i64> {
        let mut seen = Vec::new();
        for _ in 0..4 {
            // Outbound peer streams reconnect with up to 2 s of backoff after a cut.
            tokio::time::sleep(Duration::from_secs(3)).await;
            self.refresh();
            let mut order: Vec<_> = (0..3).filter(|&i| self.role(i) != Some("leader")).collect();
            order.extend((0..3).filter(|&i| self.role(i) == Some("leader")));
            for i in order {
                self.kill(i);
            }
            seen = (0..3).map(|i| read_store(&self.nodes[i].dir)).collect();
            if seen[0].0.is_some() && seen.iter().all(|s| *s == seen[0]) {
                return seen[0].1.clone();
            }
            for i in 0..3 {
                self.start(i);
            }
            self.leader().await;
        }
        panic!("stores did not converge: {seen:?}");
    }

    /// No proxy dropped trace events (an overflowing proxy also refuses new connections).
    fn assert_proxies_unsaturated(&self) {
        for (link, proxy) in &self.links {
            assert!(!proxy.trace().overflow, "proxy {link:?} trace overflowed");
        }
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for i in 0..self.nodes.len() {
            if let Some(mut child) = self.nodes[i].child.take() {
                let _ = child.kill();
                let _ = child.wait();
            }
            if let Some(reader) = self.nodes[i].reader.take() {
                let _ = reader.join();
            }
        }
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

struct ReadOnly;
impl BatchSink for ReadOnly {
    fn replicate(&self, _: Vec<u8>) -> Result<(), ReplicateError> {
        Err(ReplicateError::NotLeader)
    }
}

fn read_store(dir: &std::path::Path) -> (Option<AppliedPosition>, Vec<i64>) {
    let store = ReplicaStore::open(dir.join("db")).unwrap();
    let applied = store.applied().unwrap();
    let admin = store
        .open_admin(Arc::new(ReadOnly), Limits::default(), 64 << 20)
        .unwrap();
    let result = admin
        .sql_session()
        .execute("SELECT id FROM t ORDER BY id", &[], None)
        .unwrap();
    let ids = result
        .rows
        .iter()
        .map(|row| match row.values[..] {
            [Value::Int(id)] => id,
            ref other => panic!("unexpected row {other:?}"),
        })
        .collect();
    (applied, ids)
}

async fn insert(client: &Client, id: i64) -> Result<u64, tokio_postgres::Error> {
    tokio::time::timeout(
        WAIT,
        client.execute("INSERT INTO t (id) VALUES ($1)", &[&id]),
    )
    .await
    .unwrap_or_else(|_| panic!("INSERT {id} neither committed nor failed within {WAIT:?}"))
}

async fn ids(client: &Client) -> Vec<i64> {
    let rows = tokio::time::timeout(WAIT, client.query("SELECT id FROM t ORDER BY id", &[]))
        .await
        .unwrap()
        .unwrap();
    rows.iter().map(|r| r.get(0)).collect()
}

async fn refused(config: &Config) {
    match tokio::time::timeout(WAIT, config.connect(NoTls))
        .await
        .unwrap()
    {
        Err(error) => assert_eq!(error.code(), Some(&SqlState::CANNOT_CONNECT_NOW), "{error}"),
        Ok(_) => panic!("a node that is not the leader accepted startup"),
    }
}

/// The session ended without a definite answer: the server terminated it
/// (FATAL 57P01) or the connection closed. Either way the outcome is unknown.
fn ended(error: &tokio_postgres::Error) -> bool {
    error.is_closed() || error.code() == Some(&SqlState::ADMIN_SHUTDOWN)
}

/// An outstanding write can also lose quorum mid-commit (ERROR 08007), leaving
/// its outcome unknown and its session open until the next statement is refused.
fn write_indeterminate(error: &tokio_postgres::Error) -> bool {
    ended(error) || error.code() == Some(&SqlState::TRANSACTION_RESOLUTION_UNKNOWN)
}

/// Seed table t through `client` with ids in `range`, returning the acknowledged set.
async fn seed(client: &Client, range: std::ops::Range<i64>) -> BTreeSet<i64> {
    client
        .batch_execute("CREATE TABLE t (id BIGINT PRIMARY KEY)")
        .await
        .unwrap();
    let mut acked = BTreeSet::new();
    for id in range {
        insert(client, id).await.unwrap();
        acked.insert(id);
    }
    acked
}

/// Which peer links around the leader a test cuts.
#[derive(Clone, Copy, Debug)]
enum Cut {
    /// Every link to and from the leader: a silent partition (bytes held, no resets).
    Isolate,
    /// Only the leader's own links: it hears the followers but cannot reach them.
    Outbound,
    /// Only the followers' links to the leader: it reaches them but hears nothing back.
    Inbound,
}

/// Cut a live leader off while a writer streams inserts through it.
///
/// The old leader's sessions must end (FATAL 57P01 or a closed connection)
/// rather than commit or serve a read; the majority elects a new leader that a
/// multi-host client reaches; after the cut heals the old leader rejoins as a
/// follower, and all three stores converge to the same applied position and
/// rows, with every acknowledged write present. The single write the stream
/// had outstanding when its session ended is the only one whose outcome is
/// unknown; it may or may not be present, and nothing else may be.
/// That write may return ERROR 08007 before the session ends, in which case
/// the next statement on the same writer session must end it.
async fn cut_live_leader(cut: Cut) {
    let mut cluster = Cluster::new().await;
    let old = cluster.leader().await;
    let others: Vec<_> = (0..3).filter(|&i| i != old).collect();
    let writer = Arc::new(connect(&cluster.config(&[old])).await);
    let reader = connect(&cluster.config(&[old])).await;
    let mut acked = seed(&writer, 0..50).await;
    assert_eq!(
        ids(&reader).await,
        acked.iter().copied().collect::<Vec<_>>()
    );

    let stream = {
        let writer = writer.clone();
        tokio::spawn(async move {
            let mut acked = BTreeSet::new();
            for id in 1000.. {
                match insert(&writer, id).await {
                    Ok(_) => acked.insert(id),
                    Err(error) => return (acked, id, error),
                };
            }
            unreachable!()
        })
    };
    // Let the stream run so the cut lands between, or during, its commits.
    tokio::time::sleep(Duration::from_millis(300)).await;
    match cut {
        Cut::Isolate => cluster.isolate(old, Fault::Partition),
        Cut::Outbound => others
            .iter()
            .for_each(|&o| cluster.link(old, o, Fault::Partition)),
        Cut::Inbound => others
            .iter()
            .for_each(|&o| cluster.link(o, old, Fault::Partition)),
    }
    let read = tokio::time::timeout(WAIT, reader.query("SELECT id FROM t", &[]))
        .await
        .expect("read on the cut-off leader neither answered nor failed");
    match read {
        Ok(rows) => panic!(
            "cut-off leader served a read after the cut: {} rows",
            rows.len()
        ),
        Err(error) => assert!(ended(&error), "read on the cut-off leader: {error:?}"),
    }
    let (streamed, unknown, error) = tokio::time::timeout(WAIT, stream).await.unwrap().unwrap();
    assert!(
        write_indeterminate(&error),
        "write {unknown} on the cut-off leader: {error:?}"
    );
    if error.code() == Some(&SqlState::TRANSACTION_RESOLUTION_UNKNOWN) {
        // Use simple protocol: the gate checks Q/E; extended Parse on a fenced ex-leader currently answers 08007 (separate follow-up).
        let error = tokio::time::timeout(WAIT, writer.simple_query("SELECT id FROM t"))
            .await
            .expect("writer on the cut-off leader neither answered nor failed")
            .expect_err("cut-off leader served a read on the writer session after 08007");
        assert!(ended(&error), "writer after 08007: {error:?}");
    }
    assert!(
        !streamed.is_empty(),
        "the stream committed nothing before the cut"
    );
    acked.extend(&streamed);

    cluster.until_follower(old).await;
    let new = cluster.leader().await;
    assert!(others.contains(&new));
    refused(&cluster.config(&[old])).await;
    // A multi-host client listing the cut-off node first reaches the new leader.
    let client = connect(&cluster.config(&[old, others[0], others[1]])).await;
    for id in 50..100 {
        insert(&client, id).await.unwrap();
        acked.insert(id);
    }

    cluster.heal();
    for id in 100..110 {
        insert(&client, id).await.unwrap();
        acked.insert(id);
    }
    // Pre-vote keeps the healed node from disrupting the new leader.
    assert_eq!(cluster.leader().await, new);
    cluster.until_follower(old).await;
    refused(&cluster.config(&[old])).await;
    cluster.assert_proxies_unsaturated();
    let stored = cluster.converged().await;
    let mut expected: Vec<_> = acked.into_iter().collect();
    if stored.contains(&unknown) {
        expected.push(unknown);
        expected.sort();
    }
    assert_eq!(
        stored, expected,
        "{cut:?}: write {unknown} ended with {error:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn isolated_leader_steps_down_and_rejoins() {
    cut_live_leader(Cut::Isolate).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn leader_that_cannot_send_is_replaced() {
    cut_live_leader(Cut::Outbound).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn leader_that_cannot_hear_is_replaced() {
    cut_live_leader(Cut::Inbound).await;
}

/// A leader cut from only one follower (connection resets both ways) keeps
/// its majority with the other follower and serves without interruption;
/// pre-vote stops the cut-off follower from forcing an election.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn leader_cut_from_one_follower_keeps_serving() {
    let mut cluster = Cluster::new().await;
    let leader = cluster.leader().await;
    let cut = (0..3).find(|&i| i != leader).unwrap();
    let client = connect(&cluster.config(&[leader])).await;
    let mut acked = seed(&client, 0..20).await;
    cluster.refresh();
    let marks: Vec<_> = cluster.nodes.iter().map(|n| n.roles.len()).collect();

    cluster.link(leader, cut, Fault::Disconnect);
    cluster.link(cut, leader, Fault::Disconnect);
    // Span many election timeouts (10 ticks of 20 ms) on the same session.
    let until = Instant::now() + Duration::from_secs(3);
    let mut id = 20;
    while Instant::now() < until {
        insert(&client, id).await.unwrap();
        acked.insert(id);
        id += 1;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    cluster.refresh();
    for (i, node) in cluster.nodes.iter().enumerate() {
        let announced: Vec<_> = node.roles[marks[i]..]
            .iter()
            .filter_map(|l| token(l, "role"))
            .collect();
        // The leader never stepped down, and nobody else took over.
        let changed = if i == leader {
            !announced.is_empty()
        } else {
            announced.contains(&"leader")
        };
        assert!(
            !changed,
            "node {} announced {announced:?} during the cut",
            i + 1
        );
    }
    assert_eq!(cluster.role(leader), Some("leader"));

    cluster.heal();
    for _ in 0..10 {
        insert(&client, id).await.unwrap();
        acked.insert(id);
        id += 1;
    }
    assert_eq!(
        ids(&client).await,
        acked.iter().copied().collect::<Vec<_>>()
    );
    assert_eq!(cluster.leader().await, leader);
    cluster.assert_proxies_unsaturated();
    assert_eq!(
        cluster.converged().await,
        acked.into_iter().collect::<Vec<_>>()
    );
}
