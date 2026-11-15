use giggles_replication::{
    log_store::LogStore,
    node::Replica,
    sink::{self, Outbox, ReplicaHandle, SnapshotPolicy},
    snapshot::SnapshotStore,
    state::MemStateMachine,
};
use raft::{StateRole, eraftpb::Message};
use std::{
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

type Peers = Arc<Mutex<Vec<ReplicaHandle>>>;
struct Network(Peers);
impl Outbox for Network {
    fn send(&mut self, m: Message) {
        if let Some(peer) = self.0.lock().unwrap().get(m.to as usize - 1) {
            peer.step(m.from, m);
        }
    }
}
struct Cluster {
    root: PathBuf,
    peers: Peers,
    handles: Vec<ReplicaHandle>,
    joins: Vec<thread::JoinHandle<()>>,
    states: Vec<Arc<MemStateMachine>>,
    snapshots: Vec<Arc<SnapshotStore>>,
}
impl Drop for Cluster {
    fn drop(&mut self) {
        for handle in &self.handles {
            handle.shutdown();
        }
        for join in self.joins.drain(..) {
            let result = join.join();
            if !thread::panicking() {
                result.unwrap();
            }
        }
        self.peers.lock().unwrap().clear();
        let result = std::fs::remove_dir_all(&self.root);
        if !thread::panicking() {
            result.unwrap();
        }
    }
}
fn until(mut predicate: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !predicate() {
        assert!(Instant::now() < deadline, "policy poll timed out");
        thread::sleep(Duration::from_millis(5));
    }
}
#[test]
fn snapshots_build_and_compact_while_proposals_continue() {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let root = std::env::temp_dir().join(format!(
        "snapshot-policy-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let mut c = Cluster {
        root,
        peers: Arc::new(Mutex::new(vec![])),
        handles: vec![],
        joins: vec![],
        states: vec![],
        snapshots: vec![],
    };
    let mut replicas = vec![];
    for id in 1..=3 {
        let mut log = LogStore::open(&c.root.join(format!("log-{id}")), [74; 16], id).unwrap();
        let snapshots =
            Arc::new(SnapshotStore::open(&c.root.join(format!("snap-{id}")), [74; 16]).unwrap());
        log.attach_snapshots(snapshots.clone());
        let state = Arc::new(MemStateMachine::new());
        let mut replica = Replica::new(log, state.clone(), &[1, 2, 3]).unwrap();
        replica.set_election_timeout(if id == 1 { 10 } else { 19 });
        replicas.push(replica);
        c.states.push(state);
        c.snapshots.push(snapshots);
    }
    for replica in replicas {
        let (handle, join) = sink::spawn_with_policy(
            replica,
            Box::new(Network(c.peers.clone())),
            Duration::from_millis(10),
            Some(SnapshotPolicy {
                every_entries: 20,
                margin: 5,
            }),
        );
        c.handles.push(handle);
        c.joins.push(join);
    }
    *c.peers.lock().unwrap() = c.handles.clone();
    until(|| {
        c.handles
            .iter()
            .any(|h| h.role().state == StateRole::Leader)
    });
    let leader = c
        .handles
        .iter()
        .position(|h| h.role().state == StateRole::Leader)
        .unwrap();
    for n in 0u64..100 {
        // Every proposal must succeed, including those after policy builds start.
        c.handles[leader]
            .replicate(n.to_be_bytes().to_vec())
            .unwrap();
    }
    until(|| c.snapshots[leader].latest().is_some_and(|m| m.index >= 80));
    until(|| {
        c.states
            .iter()
            .all(|s| s.applied_batches() == c.states[leader].applied_batches())
    });
    assert_eq!(c.states[leader].applied_batches().len(), 100);
    // Stop the drivers before reopening the log to inspect its persisted compaction point.
    for handle in &c.handles {
        handle.shutdown();
    }
    for join in c.joins.drain(..) {
        join.join().unwrap();
    }
    let log = LogStore::open(
        &c.root.join(format!("log-{}", leader + 1)),
        [74; 16],
        leader as u64 + 1,
    )
    .unwrap();
    assert!(log.snapshot_meta().unwrap().0 > 0);
    drop(log);
}
