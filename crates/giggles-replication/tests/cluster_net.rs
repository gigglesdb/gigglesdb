use super::common::certs::{config_for, make_pki};
use giggles_core::{
    Limits,
    relational::Value,
    replicated::{BatchSink, ReplicateError},
};
use giggles_replication::{
    cluster::{Node, NodeConfig},
    core_store::ReplicatedEngine,
    log_store::LogStore,
    role::Serving,
    sink::SnapshotPolicy,
};
use std::{
    collections::BTreeMap,
    fs,
    net::TcpListener,
    path::PathBuf,
    sync::Arc,
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

struct Cluster {
    nodes: Vec<Option<Node>>,
    configs: Vec<NodeConfig>,
    root: PathBuf,
}
impl Cluster {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "cluster-net-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let pki = make_pki(&root.join("pki"), &[1, 2, 3]);
        let listeners: Vec<_> = (0..3)
            .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        let addresses: BTreeMap<_, _> = listeners
            .iter()
            .enumerate()
            .map(|(i, l)| (i as u64 + 1, l.local_addr().unwrap().to_string()))
            .collect();
        let configs: Vec<_> = (1..=3)
            .map(|id| NodeConfig {
                transport: config_for(&pki, id, [74; 16], &addresses),
                data_dir: root.join(format!("data-{id}")),
                log_dir: root.join(format!("log-{id}")),
                snap_dir: root.join(format!("snap-{id}")),
                voters: vec![1, 2, 3],
                tick: Duration::from_millis(20),
                policy: SnapshotPolicy {
                    every_entries: 20,
                    margin: 5,
                },
                limits: Limits::default(),
                write_set_bytes: 64 << 20,
            })
            .collect();
        drop(listeners);
        let mut c = Self {
            nodes: vec![],
            configs,
            root,
        };
        for config in &c.configs {
            c.nodes.push(Some(Node::start(config.clone()).unwrap()));
        }
        c
    }

    fn leader(&self) -> (usize, Arc<ReplicatedEngine>) {
        let mut found = None;
        until(|| {
            found = self.nodes.iter().enumerate().find_map(|(i, node)| {
                let node = node.as_ref()?;
                if let Serving::Leader { engine, .. } = node.roles.current()
                    && node.handle.read_barrier().is_ok()
                {
                    Some((i, engine))
                } else {
                    None
                }
            });
            found.is_some()
        });
        found.unwrap()
    }

    // The statements are idempotent so a lost acknowledgement at an election
    // cannot duplicate a row. Rediscover the published engine on every retry.
    fn execute(&self, sql: &str) {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let (_, engine) = self.leader();
            match engine.admin().sql_session().execute(sql, &[], None) {
                Ok(_) => return,
                Err(error) => {
                    assert!(Instant::now() < deadline, "SQL deadline: {sql}: {error}");
                }
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn converged(&self) {
        until(|| {
            let positions: Vec<_> = self
                .nodes
                .iter()
                .flatten()
                .map(|n| n.slot.current().unwrap().applied().unwrap())
                .collect();
            positions.len() == 3
                && positions[0].is_some()
                && positions.iter().all(|p| *p == positions[0])
        });
    }

    fn stop(&mut self, i: usize) {
        self.nodes[i].take().unwrap().stop();
    }

    fn start(&mut self, i: usize) {
        self.nodes[i] = Some(Node::start(self.configs[i].clone()).unwrap());
    }

    fn compaction_index(&self, i: usize) -> u64 {
        let config = &self.configs[i];
        LogStore::open(
            &config.log_dir,
            config.transport.cluster,
            config.transport.node_id,
        )
        .unwrap()
        .snapshot_meta()
        .unwrap()
        .0
    }
}
impl Drop for Cluster {
    fn drop(&mut self) {
        for node in &mut self.nodes {
            if let Some(node) = node.take() {
                node.stop();
            }
        }
        let result = fs::remove_dir_all(&self.root);
        if !thread::panicking() {
            result.unwrap();
        }
    }
}
fn until(mut ready: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !ready() {
        assert!(
            Instant::now() < deadline,
            "cluster network deadline exceeded"
        );
        thread::sleep(Duration::from_millis(10));
    }
}
struct ReadOnly;
impl BatchSink for ReadOnly {
    fn replicate(&self, _: Vec<u8>) -> Result<(), ReplicateError> {
        Err(ReplicateError::NotLeader)
    }
}

#[test]
fn sql_over_mtls_and_wiped_follower_rejoins_by_snapshot() {
    let mut c = Cluster::new();
    c.execute("CREATE TABLE IF NOT EXISTS t(id int PRIMARY KEY)");
    for id in 0..60 {
        c.execute(&format!(
            "INSERT INTO t VALUES ({id}) ON CONFLICT DO NOTHING"
        ));
    }
    c.converged();
    let (leader, engine) = c.leader();
    drop(engine);
    // Observe publication without opening another SnapshotStore, whose startup
    // recovery would remove the running builder's staging directories.
    until(|| {
        fs::read_dir(&c.configs[leader].snap_dir)
            .unwrap()
            .flatten()
            .any(|e| {
                !e.file_name().to_string_lossy().starts_with("tmp-")
                    && e.path().join("MANIFEST").is_file()
            })
    });
    // Even a NotLeader reply after an election lets the driver finish its
    // policy iteration before the subsequently queued shutdown.
    let _ = c.nodes[leader].as_ref().unwrap().handle.read_barrier();
    // The log is exclusively locked while running. Stop briefly to inspect
    // persisted compaction, then rejoin with all durable state intact.
    c.stop(leader);
    assert!(c.compaction_index(leader) > 0);
    c.start(leader);
    c.converged();

    let (leader, engine) = c.leader();
    drop(engine);
    let follower = (leader + 1) % 3;
    c.stop(follower);
    let config = &c.configs[follower];
    for dir in [&config.data_dir, &config.log_dir, &config.snap_dir] {
        fs::remove_dir_all(dir).unwrap();
        assert!(!dir.exists());
    }
    c.start(follower);
    for id in 60..70 {
        c.execute(&format!(
            "INSERT INTO t VALUES ({id}) ON CONFLICT DO NOTHING"
        ));
    }
    c.converged();
    // Keep every Admin off the rejoining slot until installation has finished.
    {
        let store = c.nodes[follower].as_ref().unwrap().slot.current().unwrap();
        let admin = store
            .open_admin(Arc::new(ReadOnly), Limits::default(), 64 << 20)
            .unwrap();
        let result = admin
            .sql_session()
            .execute("SELECT count(*) FROM t", &[], None)
            .unwrap();
        assert_eq!(result.rows[0].values, vec![Value::Int(70)]);
    }
    c.stop(follower);
    assert!(
        c.compaction_index(follower) > 0,
        "wiped follower installed a snapshot"
    );
}

#[test]
fn heartbeat_ahead_of_wiped_log_waits_for_recovery() {
    use giggles_replication::{node::Replica, state::MemStateMachine};
    use raft::eraftpb::{Message, MessageType};
    let root = std::env::temp_dir().join(format!(
        "wiped-heartbeat-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let log = LogStore::open(&root, [74; 16], 2).unwrap();
    let mut replica = Replica::new(log, Arc::new(MemStateMachine::new()), &[1, 2, 3]).unwrap();
    replica
        .step(
            1,
            Message {
                from: 1,
                to: 2,
                term: 1,
                commit: 65,
                msg_type: MessageType::MsgHeartbeat,
                ..Default::default()
            },
        )
        .unwrap();
    let output = replica.drive().unwrap();
    assert_eq!(replica.applied_index(), 0);
    assert!(
        output
            .messages
            .iter()
            .any(|m| m.to == 1 && m.get_msg_type() == MessageType::MsgHeartbeatResponse)
    );
    drop(replica);
    fs::remove_dir_all(root).unwrap();
}
