use giggles_replication::{
    log_store::LogStore,
    node::Replica,
    sink::{self, Outbox, ReplicaHandle, ReplicateError, SnapshotPolicy},
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
/// The leader with the highest term, waiting until one exists.
fn current_leader(handles: &[ReplicaHandle]) -> usize {
    let mut found = None;
    until(|| {
        found = handles
            .iter()
            .enumerate()
            .filter(|(_, h)| h.role().state == StateRole::Leader)
            .max_by_key(|(_, h)| h.role().term)
            .map(|(i, _)| i);
        found.is_some()
    });
    found.unwrap()
}
/// Replicate `payload` exactly once, the way a client must across a leader change.
///
/// `NotLeader` means the proposal was never appended, so it is retried. `Unknown` means
/// the proposer was deposed with the proposal in flight: it may or may not commit. That
/// is resolved on the current leader: a read barrier there returns only once the leader
/// has committed an entry of its own term and applied up to its commit index, so every
/// entry of an earlier term is by then either applied there or discarded for good. The
/// payload is proposed again only if it was discarded. Both errors must coincide with a
/// leadership change, so a proposal that fails for any other reason (for example a
/// snapshot build) still fails the test. Returns the number of leadership changes met.
fn replicate_once(c: &Cluster, payload: &[u8]) -> u32 {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut changes = 0;
    loop {
        assert!(Instant::now() < deadline, "replicate deadline");
        let leader = current_leader(&c.handles);
        let role = c.handles[leader].role();
        match c.handles[leader].replicate(payload.to_vec()) {
            Ok(()) => return changes,
            Err(ReplicateError::NotLeader) => {}
            Err(ReplicateError::Unknown(reason)) => {
                // Unknown is only legitimate when the proposer lost this leadership term.
                until(|| c.handles[leader].role() != role);
                changes += 1;
                loop {
                    assert!(Instant::now() < deadline, "unresolved proposal: {reason}");
                    let resolver = current_leader(&c.handles);
                    match c.handles[resolver].read_barrier() {
                        Ok(_) => {
                            let applied = c.states[resolver]
                                .applied_batches()
                                .iter()
                                .any(|(_, batch)| batch == payload);
                            eprintln!("in-flight proposal resolved: applied={applied} ({reason})");
                            if applied {
                                return changes;
                            }
                            break;
                        }
                        Err(ReplicateError::NotLeader) => {
                            thread::sleep(Duration::from_millis(5));
                        }
                        Err(e) => panic!("read barrier failed: {e:?}"),
                    }
                }
                continue;
            }
            Err(e) => panic!("proposal failed outside a leadership change: {e:?}"),
        }
        changes += 1;
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
    let mut changes = 0;
    for n in 0u64..100 {
        // Every proposal must succeed, including those after policy builds start. Under
        // machine load the leader can change mid-run; a proposal caught by that is resolved
        // (applied once, never twice) rather than failing the test.
        changes += replicate_once(&c, &n.to_be_bytes());
    }
    let leader = current_leader(&c.handles);
    if changes > 0 {
        eprintln!("resolved {changes} proposals across leadership changes");
    }
    until(|| c.snapshots[leader].latest().is_some_and(|m| m.index >= 80));
    until(|| {
        c.states
            .iter()
            .all(|s| s.applied_batches() == c.states[leader].applied_batches())
    });
    // Exactly once and in order: a resolved proposal is never applied twice.
    let applied: Vec<Vec<u8>> = c.states[leader]
        .applied_batches()
        .into_iter()
        .map(|(_, batch)| batch)
        .collect();
    let expected: Vec<Vec<u8>> = (0u64..100).map(|n| n.to_be_bytes().to_vec()).collect();
    assert_eq!(applied, expected);
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
