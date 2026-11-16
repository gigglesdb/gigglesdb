//! Leadership confirmation before reads and timestamps (GD-0075 R5-33,
//! design.md 4a O1/O6): an old leader cut off from its group serves no reads
//! and issues no timestamps or leases, while the new leader serves them.
use giggles_core::{
    range_api::{
        ClusterTxnId, Epoch, HomeOp, HomeReply, LeaseKind, LockGrant, LockRequest, Outcome,
        RangeDescriptor, RangeEngineOpener, RangeError, RangeId, RangeService, RangeSpace,
        RangeWrite, ReadDirection, ReadPage, ReadRequest, ReplicaDesc, ReplicaRole,
        SYSTEM_RANGE, SystemService, Timestamp, TsOracle, first_user_key,
    },
    replicated::{BatchSink, RangeStore},
};
use giggles_replication::{
    client::LocalRanges,
    host::{GroupNetwork, RangeHost, RangeHostConfig},
    sink::Outbox,
    transport::PeerSink,
};
use raft::eraftpb::Message;
use std::{
    collections::{BTreeSet, HashMap},
    ops::Bound,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const CLUSTER: [u8; 16] = [80; 16];
const NODES: [u64; 3] = [1, 2, 3];
const USER: RangeId = RangeId(2);

type Sinks = Arc<Mutex<HashMap<(u64, RangeId), Arc<dyn PeerSink>>>>;
type Blocked = Arc<Mutex<BTreeSet<u64>>>;

struct MemNet {
    node: u64,
    sinks: Sinks,
    blocked: Blocked,
}
struct MemOutbox {
    group: RangeId,
    sinks: Sinks,
    blocked: Blocked,
}
impl Outbox for MemOutbox {
    fn send(&mut self, m: Message) {
        {
            let blocked = self.blocked.lock().unwrap();
            if blocked.contains(&m.from) || blocked.contains(&m.to) {
                return;
            }
        }
        let sink = self.sinks.lock().unwrap().get(&(m.to, self.group)).cloned();
        if let Some(sink) = sink {
            sink.deliver(m.from, m);
        }
    }
}
impl GroupNetwork for MemNet {
    fn outbox(&self, group: RangeId) -> Box<dyn Outbox> {
        Box::new(MemOutbox {
            group,
            sinks: self.sinks.clone(),
            blocked: self.blocked.clone(),
        })
    }
    fn register(&self, group: RangeId, sink: Arc<dyn PeerSink>) {
        self.sinks.lock().unwrap().insert((self.node, group), sink);
    }
    fn unregister(&self, group: RangeId) {
        self.sinks.lock().unwrap().remove(&(self.node, group));
    }
}

fn unavailable<T>() -> Result<T, RangeError> {
    Err(RangeError::Unavailable("stub".into()))
}

/// Answers every read with an empty page (an engine with stale state would too).
struct Reads(RangeDescriptor);
impl RangeService for Reads {
    fn descriptor(&self) -> RangeDescriptor {
        self.0.clone()
    }
    fn read(&self, _: ReadRequest) -> Result<ReadPage, RangeError> {
        Ok(ReadPage {
            pairs: Vec::new(),
            resume: None,
            range_end: self.0.end.clone(),
        })
    }
    fn lock(&self, _: LockRequest) -> Result<LockGrant, RangeError> {
        Ok(LockGrant { latest: None })
    }
    fn blockers(&self, _: ClusterTxnId) -> Result<Vec<ClusterTxnId>, RangeError> {
        unavailable()
    }
    fn cancel_wait(&self, _: ClusterTxnId) -> Result<(), RangeError> {
        unavailable()
    }
    fn release(&self, _: ClusterTxnId) -> Result<(), RangeError> {
        unavailable()
    }
    fn heartbeat(&self, _: ClusterTxnId) -> Result<(), RangeError> {
        unavailable()
    }
    fn commit(
        &self,
        _: Epoch,
        _: ClusterTxnId,
        _: Vec<RangeWrite>,
        _: Option<u64>,
    ) -> Result<Timestamp, RangeError> {
        unavailable()
    }
    fn prepare(
        &self,
        _: Epoch,
        _: ClusterTxnId,
        _: RangeId,
        _: Vec<RangeWrite>,
    ) -> Result<Timestamp, RangeError> {
        unavailable()
    }
    fn finalize(&self, _: ClusterTxnId, _: Outcome) -> Result<(), RangeError> {
        unavailable()
    }
    fn home(&self, _: Epoch, _: HomeOp) -> Result<HomeReply, RangeError> {
        unavailable()
    }
}

struct Opener;
impl RangeEngineOpener for Opener {
    fn open(
        &self,
        descriptor: RangeDescriptor,
        _sink: Arc<dyn BatchSink>,
    ) -> Result<Arc<dyn RangeService>, RangeError> {
        Ok(Arc::new(Reads(descriptor)))
    }
    fn close(&self, _: RangeId) {}
}

/// An oracle that would happily keep issuing from its window if asked.
struct Window(AtomicU64);
impl TsOracle for Window {
    fn next(&self, n: u32) -> Result<Timestamp, RangeError> {
        Ok(Timestamp(self.0.fetch_add(u64::from(n), Ordering::SeqCst)))
    }
}
impl SystemService for Window {
    fn oracle(&self) -> &dyn TsOracle {
        self
    }
    fn lease(&self, _: LeaseKind, _: u64, count: u64) -> Result<std::ops::Range<u64>, RangeError> {
        Ok(0..count)
    }
}

fn descriptors() -> Vec<RangeDescriptor> {
    let replicas: Vec<ReplicaDesc> = NODES
        .iter()
        .map(|&node| ReplicaDesc {
            node,
            role: ReplicaRole::Voter,
        })
        .collect();
    vec![
        RangeDescriptor {
            id: SYSTEM_RANGE,
            epoch: Epoch(1),
            start: Vec::new(),
            end: Some(first_user_key()),
            replicas: replicas.clone(),
        },
        RangeDescriptor {
            id: USER,
            epoch: Epoch(1),
            start: first_user_key(),
            end: None,
            replicas,
        },
    ]
}

fn read_request() -> ReadRequest {
    ReadRequest {
        range: USER,
        epoch: Epoch(1),
        space: RangeSpace::Rows,
        start: Bound::Unbounded,
        end: Bound::Unbounded,
        dir: ReadDirection::Forward,
        ts: Timestamp(5),
        page_bytes: 1024,
    }
}

fn wait<T>(what: &str, mut found: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(t) = found() {
            return t;
        }
        assert!(Instant::now() < deadline, "deadline: {what}");
        thread::sleep(Duration::from_millis(10));
    }
}

/// The node serving `range` (system: the node whose system() is Some), but not `except`.
fn serving(hosts: &[RangeHost], range: RangeId, except: Option<u64>) -> Option<usize> {
    hosts.iter().position(|h| {
        Some(h.node_id()) != except
            && if range == SYSTEM_RANGE {
                h.system().is_some()
            } else {
                h.service(range).is_some()
            }
    })
}

#[test]
fn a_cut_off_old_leader_serves_no_reads_and_issues_no_timestamps() {
    let dir = std::env::temp_dir().join(format!(
        "leader-reads-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let sinks: Sinks = Arc::default();
    let blocked: Blocked = Arc::default();
    let hosts: Vec<RangeHost> = NODES
        .iter()
        .map(|&node| {
            let store =
                Arc::new(RangeStore::create(dir.join(format!("n{node}/data")), CLUSTER).unwrap());
            let config = RangeHostConfig {
                node_id: node,
                cluster: CLUSTER,
                log_dir: dir.join(format!("n{node}/log")),
                snap_dir: dir.join(format!("n{node}/snap")),
                tick: Duration::from_millis(10),
                policy: None,
                liveness: None,
            };
            let network = Arc::new(MemNet {
                node,
                sinks: sinks.clone(),
                blocked: blocked.clone(),
            });
            let host = RangeHost::open(config, network, store, Some(Arc::new(Opener))).unwrap();
            for d in descriptors() {
                host.create_range(&d).unwrap();
            }
            host.set_system(Some(Arc::new(Window(AtomicU64::new(100)))));
            host
        })
        .collect();

    // Healthy: only the system range's leader issues timestamps; only the user
    // range's leader serves reads.
    let s = wait("system leader", || serving(&hosts, SYSTEM_RANGE, None));
    assert!(hosts[s].system().unwrap().oracle().next(1).is_ok());
    let u = wait("user range leader", || serving(&hosts, USER, None));
    let old_reads = hosts[u].service(USER).unwrap();
    assert!(old_reads.read(read_request()).is_ok());
    let old_system = hosts[s].system().unwrap();

    // Cut off the system leader: it must issue no timestamp or lease, however long
    // it still believes it leads; the others elect a leader that does.
    let cut = hosts[s].node_id();
    blocked.lock().unwrap().insert(cut);
    let start = Instant::now();
    let issued = old_system.oracle().next(1);
    assert!(
        matches!(issued, Err(RangeError::NotLeader { .. })),
        "{issued:?}"
    );
    assert!(old_system.lease(LeaseKind::Xid, 1, 10).is_err());
    eprintln!("cut-off system leader refused after {:?}", start.elapsed());
    let s2 = wait("new system leader", || serving(&hosts, SYSTEM_RANGE, Some(cut)));
    assert!(hosts[s2].system().unwrap().oracle().next(1).is_ok());
    blocked.lock().unwrap().clear();

    // Cut off the user range's leader once one serves reads again after the heal
    // (leadership may still move while the healed node rejoins).
    let (u, old_reads) = wait("user range leader serving reads", || {
        let u = serving(&hosts, USER, None)?;
        let reads = hosts[u].service(USER)?;
        reads.read(read_request()).ok().map(|_| (u, reads))
    });
    let cut = hosts[u].node_id();
    blocked.lock().unwrap().insert(cut);
    let read = old_reads.read(read_request());
    assert!(matches!(read, Err(RangeError::NotLeader { .. })), "{read:?}");
    let lock = old_reads.lock(LockRequest {
        range: USER,
        epoch: Epoch(1),
        txn: ClusterTxnId(9),
        priority: Timestamp(1),
        target: giggles_core::range_api::LockTarget::TableSentinel { table: 7 },
        mode: giggles_core::range_api::RangeLockMode::Shared,
        timeout_ms: None,
    });
    assert!(lock.is_err(), "{lock:?}");
    let u2 = wait("new user range leader", || serving(&hosts, USER, Some(cut)));
    assert!(hosts[u2].service(USER).unwrap().read(read_request()).is_ok());
    blocked.lock().unwrap().clear();

    drop(hosts);
    let _ = std::fs::remove_dir_all(dir);
}
