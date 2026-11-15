use super::common::certs::{config_for, make_pki};
use giggles_core::Limits;
use giggles_replication::{
    admin::{self, AdminRequest, AdminResponse, StatusReport},
    cluster::{Node, NodeConfig},
    core_store::ReplicatedEngine,
    membership::PeerRecord,
    replace::ReplaceRequest,
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
}
impl Cluster {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "admin-net-{}-{}-{}",
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
        drop(listeners);
        let mut c = Self {
            nodes: vec![],
            configs,
            root,
        };
        for config in &c.configs[..3] {
            c.nodes.push(Some(Node::start(config.clone()).unwrap()));
        }
        c.nodes.push(None);
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

    fn stop(&mut self, i: usize) {
        self.nodes[i].take().unwrap().stop();
    }

    fn start(&mut self, i: usize) {
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
    fn seed(&self) {
        self.execute("CREATE TABLE IF NOT EXISTS t(id int PRIMARY KEY)");
        for id in 0..25 {
            self.execute(&format!(
                "INSERT INTO t VALUES ({id}) ON CONFLICT DO NOTHING"
            ));
        }
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
fn status_and_replace_via_admin_call() {
    let mut c = Cluster::new();
    c.seed();
    // Stop node 1 only once another node has won an election.
    // If it was elected first, restart it with slower ticks to yield leadership.
    if c.leader().0 == 0 {
        c.stop(0);
        c.leader();
        c.configs[0].tick = Duration::from_millis(100);
        c.start(0);
    }
    assert_ne!(c.leader().0, 0);
    c.stop(0);
    c.start(3);
    let leader = c.leader().0;
    let follower = if leader == 1 { 3 } else { 2 };
    let config = &c.configs[1].transport;
    let status = admin::call(
        config,
        follower,
        &AdminRequest::Status,
        Duration::from_secs(10),
    )
    .unwrap();
    let AdminResponse::Status(report) = status else {
        panic!("{status:?}")
    };
    assert_eq!(report.voters, [1, 2, 3]);
    assert_eq!(report.step, "idle");
    until(|| {
        c.nodes[leader]
            .as_ref()
            .unwrap()
            .handle
            .membership_status()
            .unwrap()
            .reachable
            .iter()
            .filter(|id| **id != 1)
            .count()
            >= 2
    });
    let req = c.request();
    let response = admin::call(
        config,
        follower,
        &AdminRequest::Replace {
            lost: req.lost,
            new: req.new,
            timeout_ms: 60_000,
        },
        Duration::from_secs(70),
    )
    .unwrap();
    let AdminResponse::Done(report) = response else {
        panic!("{response:?}")
    };
    assert_eq!(report.voters, [2, 3, 4]);
    assert_eq!(report.retired, [1]);
    assert_eq!(report.step, "idle");
}
#[test]
fn unknown_certificate_is_refused() {
    let c = Cluster::new();
    let leader = c.leader().0;
    let config = &c.configs[3].transport;
    let target = if leader == 0 { 2 } else { leader as u64 + 1 };
    assert!(
        admin::call(
            config,
            target,
            &AdminRequest::Status,
            Duration::from_secs(5)
        )
        .is_err()
    );
    for node in c.nodes.iter().flatten() {
        let s = node.handle.membership_status().unwrap();
        let mut voters = s.conf.voters;
        voters.sort_unstable();
        assert_eq!(voters, [1, 2, 3]);
        assert!(s.membership.active.is_none());
        assert!(s.membership.retired.is_empty());
    }
}
#[test]
fn codec_roundtrips_and_rejects_invalid_input() {
    let report = StatusReport {
        voters: vec![1, 2, 3],
        voters_outgoing: vec![1, 2],
        learners: vec![4],
        step: "learner".into(),
        lost: Some(1),
        new: Some(4),
        learner_lag: Some(12),
        retired: vec![5],
        leader: 2,
        term: 3,
    };
    let requests = [
        AdminRequest::Status,
        AdminRequest::Replace {
            lost: 1,
            new: PeerRecord {
                id: 4,
                address: "localhost:9000".into(),
                fingerprint: "a".repeat(64),
            },
            timeout_ms: 60_000,
        },
    ];
    for req in requests {
        let bytes = req.encode().unwrap();
        assert_eq!(AdminRequest::decode(&bytes).unwrap(), req);
        for n in 0..bytes.len() {
            assert!(AdminRequest::decode(&bytes[..n]).is_err());
        }
        let mut extra = bytes;
        extra.push(0);
        assert!(AdminRequest::decode(&extra).is_err());
    }
    for response in [
        AdminResponse::Status(report.clone()),
        AdminResponse::Done(report),
        AdminResponse::NotLeader { leader: 2 },
        AdminResponse::Refused("no".into()),
        AdminResponse::Failed("timeout at learner".into()),
    ] {
        let bytes = response.encode().unwrap();
        assert_eq!(AdminResponse::decode(&bytes).unwrap(), response);
        for n in 0..bytes.len() {
            assert!(AdminResponse::decode(&bytes[..n]).is_err());
        }
        let mut extra = bytes;
        extra.push(0);
        assert!(AdminResponse::decode(&extra).is_err());
    }
    for bad in [vec![2, 0], vec![1, 255], vec![0; (64 << 10) + 1]] {
        assert!(AdminRequest::decode(&bad).is_err());
        assert!(AdminResponse::decode(&bad).is_err());
    }
    assert!(
        AdminResponse::Failed("x".repeat(64 << 10))
            .encode()
            .is_err()
    );
}
