use super::common::certs::{config_for, make_pki};
use giggles_core::{Limits, relational::Value};
use giggles_replication::{
    cluster::{Node, NodeConfig},
    core_store::ReplicatedEngine,
    membership::{AdminCommand, PeerRecord},
    replace::{self, ReplaceError, ReplaceRequest},
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

static NEXT_CLUSTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

struct Cluster {
    nodes: Vec<Option<Node>>,
    configs: Vec<NodeConfig>,
    root: PathBuf,
    // Each stopped node's listen port stays bound here until the node starts,
    // so a concurrent test process cannot take it (os error 10048).
    reserved: Vec<Option<TcpListener>>,
}
impl Cluster {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "replace-net-{}-{}-{}",
            std::process::id(),
            NEXT_CLUSTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let pki = make_pki(&root.join("pki"), &[1, 2, 3, 4]);
        let listeners: Vec<_> = (0..4)
            .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        let addresses: BTreeMap<_, _> = listeners
            .iter()
            .enumerate()
            .map(|(i, l)| (i as u64 + 1, l.local_addr().unwrap().to_string()))
            .collect();
        let configs: Vec<_> = (1..=4)
            .map(|id| NodeConfig {
                transport: {
                    let mut config = config_for(&pki, id, [74; 16], &addresses);
                    config
                        .peers
                        .retain(|peer, _| if id == 4 { *peer != 1 } else { *peer != 4 });
                    config
                },
                data_dir: root.join(format!("data-{id}")),
                log_dir: root.join(format!("log-{id}")),
                snap_dir: root.join(format!("snap-{id}")),
                voters: if id == 4 { vec![] } else { vec![1, 2, 3] },
                tick: Duration::from_millis(20),
                policy: SnapshotPolicy {
                    every_entries: 20,
                    margin: 5,
                },
                limits: Limits::default(),
                write_set_bytes: 64 << 20,
            })
            .collect();
        let mut c = Self {
            nodes: (0..4).map(|_| None).collect(),
            configs,
            root,
            reserved: listeners.into_iter().map(Some).collect(),
        };
        for i in 0..3 {
            c.start(i);
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
        self.reserved[i] = TcpListener::bind(&self.configs[i].transport.listen).ok();
    }

    fn start(&mut self, i: usize) {
        drop(self.reserved[i].take());
        self.nodes[i] = Some(Node::start(self.configs[i].clone()).unwrap());
    }

    fn request(&self) -> ReplaceRequest {
        let peer = &self.configs[3].transport.peers[&4];
        ReplaceRequest {
            lost: 1,
            new: PeerRecord {
                id: 4,
                address: peer.address.clone(),
                fingerprint: peer.fingerprint.clone(),
            },
        }
    }
    fn lose_one(&mut self) -> usize {
        drop(self.leader());
        self.stop(0);
        self.leader().0
    }
    fn complete(&self) {
        // A leader change (likely on a busy machine) interrupts the run. Resume it
        // on the new leader, as an operator would: replacement is resumable.
        let resume_until = Instant::now() + Duration::from_secs(120);
        loop {
            let (leader, engine) = self.leader();
            drop(engine);
            let handle = &self.nodes[leader].as_ref().unwrap().handle;
            until(|| {
                let _ = handle.read_barrier();
                let s = handle.membership_status().unwrap();
                s.role.state != raft::StateRole::Leader
                    || s.reachable.iter().filter(|id| **id != 1).count() >= 2
            });
            match replace::run(handle, &self.request(), Duration::from_secs(60)) {
                Ok(_) => break,
                Err(ReplaceError::NotLeader { .. }) if Instant::now() < resume_until => {}
                Err(e) => panic!("replace failed: {e}"),
            }
        }
        let deadline = Instant::now() + Duration::from_secs(30);
        until(|| {
            let statuses: Vec<_> = self
                .nodes
                .iter()
                .skip(1)
                .flatten()
                .map(|node| node.handle.membership_status().unwrap())
                .collect();
            assert!(Instant::now() < deadline, "final membership: {statuses:?}");
            statuses.into_iter().all(|mut s| {
                s.conf.voters.sort_unstable();
                s.conf.voters == [2, 3, 4]
                    && s.conf.voters_outgoing.is_empty()
                    && s.membership.is_retired(1)
            })
        });
    }
    fn seed(&self) {
        self.execute("CREATE TABLE IF NOT EXISTS t(id int PRIMARY KEY)");
        for id in 0..25 {
            self.execute(&format!(
                "INSERT INTO t VALUES ({id}) ON CONFLICT DO NOTHING"
            ));
        }
    }
    fn check_rows(&self, count: i64) {
        self.converged();
        let (_, engine) = self.leader();
        let result = engine
            .admin()
            .sql_session()
            .execute("SELECT count(*) FROM t", &[], None)
            .unwrap();
        assert_eq!(result.rows[0].values, vec![Value::Int(count)]);
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

#[test]
fn replaces_lost_voter_over_tls() {
    let mut c = Cluster::new();
    c.seed();
    c.lose_one();
    c.start(3);
    c.complete();
    for id in 25..35 {
        c.execute(&format!(
            "INSERT INTO t VALUES ({id}) ON CONFLICT DO NOTHING"
        ));
    }
    c.check_rows(35);
    // Inspect the new node only after snapshot installation is finished.
    let store = c.nodes[3].as_ref().unwrap().slot.current().unwrap();
    struct ReadOnly;
    impl giggles_core::replicated::BatchSink for ReadOnly {
        fn replicate(&self, _: Vec<u8>) -> Result<(), giggles_core::replicated::ReplicateError> {
            Err(giggles_core::replicated::ReplicateError::NotLeader)
        }
    }
    let admin = store
        .open_admin(Arc::new(ReadOnly), Limits::default(), 64 << 20)
        .unwrap();
    let result = admin
        .sql_session()
        .execute("SELECT count(*) FROM t", &[], None)
        .unwrap();
    assert_eq!(result.rows[0].values, vec![Value::Int(35)]);
}
#[test]
fn refuses_with_fewer_than_two_reachable_voters() {
    let mut c = Cluster::new();
    drop(c.leader());
    c.stop(0);
    c.stop(2);
    let handle = &c.nodes[1].as_ref().unwrap().handle;
    let result = replace::run(handle, &c.request(), Duration::from_secs(5));
    assert!(
        matches!(
            result,
            Err(ReplaceError::Refused(_)) | Err(ReplaceError::NotLeader { .. })
        ),
        "{result:?}"
    );
    assert!(
        handle
            .membership_status()
            .unwrap()
            .membership
            .active
            .is_none()
    );
}
#[test]
fn resumes_after_leader_change() {
    let mut c = Cluster::new();
    c.seed();
    let leader = c.lose_one();
    c.start(3);
    let handle = &c.nodes[leader].as_ref().unwrap().handle;
    let req = c.request();
    handle.read_barrier().unwrap();
    handle
        .propose_admin(AdminCommand::Enrol {
            lost: req.lost,
            peer: req.new.clone(),
        })
        .unwrap();
    let s = handle.membership_status().unwrap();
    handle
        .propose_conf_change(s.membership.expected_conf_change().unwrap())
        .unwrap();
    handle.build_snapshot().unwrap();
    assert!(matches!(
        handle.membership_status().unwrap().membership.next(),
        giggles_replication::membership::Next::CatchUp { .. }
    ));
    c.stop(leader);
    // The learner cannot vote yet: restart restores the old majority.
    // Give the survivor an election head start and slow the restarted node's ticks.
    thread::sleep(Duration::from_secs(1));
    c.configs[leader].tick = Duration::from_millis(100);
    c.start(leader);
    assert_ne!(c.leader().0, leader, "a different leader must resume");
    c.complete();
    c.check_rows(25);
}
#[test]
fn retired_node_cannot_rejoin() {
    let mut c = Cluster::new();
    c.seed();
    c.lose_one();
    c.start(3);
    c.complete();
    c.start(0);
    for id in 25..35 {
        c.execute(&format!(
            "INSERT INTO t VALUES ({id}) ON CONFLICT DO NOTHING"
        ));
    }
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        assert_ne!(
            c.nodes[0].as_ref().unwrap().handle.role().state,
            raft::StateRole::Leader
        );
        for i in [1, 2] {
            let s = c.nodes[i]
                .as_ref()
                .unwrap()
                .handle
                .membership_status()
                .unwrap();
            assert!(s.membership.is_retired(1));
            assert!(!s.conf.voters.contains(&1));
        }
        thread::sleep(Duration::from_millis(20));
    }
    c.stop(0);
    c.check_rows(35);
}
