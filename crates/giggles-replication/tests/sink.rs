use giggles_replication::{
    log_store::LogStore,
    node::Replica,
    sink::{self, Outbox, ReplicaHandle, ReplicateError},
    state::MemStateMachine,
};
use raft::{StateRole, eraftpb::Message};
use std::{
    path::PathBuf,
    sync::{
        Arc, Barrier, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

type Peers = Arc<Mutex<Vec<ReplicaHandle>>>;
struct Network {
    peers: Peers,
    isolated: Arc<Vec<AtomicBool>>,
}
impl Outbox for Network {
    fn send(&mut self, m: Message) {
        if self.isolated[m.from as usize - 1].load(Ordering::SeqCst)
            || self.isolated[m.to as usize - 1].load(Ordering::SeqCst)
        {
            return;
        }
        if let Some(peer) = self.peers.lock().unwrap().get(m.to as usize - 1) {
            peer.step(m.from, m);
        }
    }
}
struct Cluster {
    handles: Vec<ReplicaHandle>,
    joins: Vec<thread::JoinHandle<()>>,
    states: Vec<Arc<MemStateMachine>>,
    isolated: Arc<Vec<AtomicBool>>,
    peers: Peers,
    dir: PathBuf,
}
impl Cluster {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let sequence = NEXT.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "sink-{}-{sequence}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let peers = Arc::new(Mutex::new(Vec::new()));
        let isolated = Arc::new((0..3).map(|_| AtomicBool::new(false)).collect::<Vec<_>>());
        let mut c = Self {
            handles: vec![],
            joins: vec![],
            states: vec![],
            isolated,
            peers,
            dir,
        };
        let mut replicas = Vec::new();
        for id in 1..=3 {
            let state = Arc::new(MemStateMachine::new());
            let mut replica = Replica::new(
                LogStore::open(&c.dir.join(id.to_string()), [14; 16], id).unwrap(),
                state.clone(),
                &[1, 2, 3],
            )
            .unwrap();
            replica.set_election_timeout(if id == 1 { 10 } else { 19 });
            replicas.push(replica);
            c.states.push(state);
        }
        for replica in replicas {
            let (h, j) = sink::spawn(
                replica,
                Box::new(Network {
                    peers: c.peers.clone(),
                    isolated: c.isolated.clone(),
                }),
                Duration::from_millis(10),
            );
            c.handles.push(h);
            c.joins.push(j);
        }
        *c.peers.lock().unwrap() = c.handles.clone();
        until(|| c.handles[0].role().state == StateRole::Leader);
        c
    }
}
impl Drop for Cluster {
    fn drop(&mut self) {
        for h in &self.handles {
            h.shutdown();
        }
        for j in self.joins.drain(..) {
            j.join().unwrap();
        }
        self.peers.lock().unwrap().clear();
        std::fs::remove_dir_all(&self.dir).unwrap();
    }
}
fn until(mut predicate: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !predicate() {
        assert!(Instant::now() < deadline, "poll timed out");
        thread::sleep(Duration::from_millis(5));
    }
}
fn bounded<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let _ = tx.send(f());
    });
    rx.recv_timeout(Duration::from_secs(5))
        .expect("blocking operation timed out")
}
#[test]
fn replicate_barrier_isolation_and_shutdown() {
    let c = Cluster::new();
    let h = c.handles[0].clone();
    assert_eq!(bounded(move || h.replicate(b"batch".to_vec())), Ok(()));
    until(|| c.states.iter().all(|s| s.applied_batches().len() == 1));
    for state in &c.states {
        assert_eq!(state.applied_batches()[0].1, b"batch");
    }
    let h = c.handles[1].clone();
    assert_eq!(
        bounded(move || h.replicate(vec![0])),
        Err(ReplicateError::NotLeader)
    );
    let before = c.handles[0].read_rounds();
    // Hold quorum responses while all callers queue, then allow a retried round through.
    c.isolated[0].store(true, Ordering::SeqCst);
    let start = Arc::new(Barrier::new(101));
    let calls: Vec<_> = (0..100)
        .map(|_| {
            let h = c.handles[0].clone();
            let start = start.clone();
            thread::spawn(move || {
                start.wait();
                h.read_barrier()
            })
        })
        .collect();
    start.wait();
    thread::sleep(Duration::from_millis(30));
    c.isolated[0].store(false, Ordering::SeqCst);
    for call in calls {
        assert!(bounded(move || call.join().unwrap()).unwrap() >= 2);
    }
    let rounds = c.handles[0].read_rounds() - before;
    assert!((1..=10).contains(&rounds), "issued {rounds} rounds");
    c.isolated[0].store(true, Ordering::SeqCst);
    let h = c.handles[0].clone();
    let read = thread::spawn(move || h.read_barrier());
    let h = c.handles[0].clone();
    let write = thread::spawn(move || h.replicate(b"isolated".to_vec()));
    assert_eq!(
        bounded(move || read.join().unwrap()),
        Err(ReplicateError::NotLeader)
    );
    assert!(matches!(
        bounded(move || write.join().unwrap()),
        Err(ReplicateError::Unknown(_) | ReplicateError::NotLeader)
    ));
    for h in &c.handles {
        h.shutdown();
    }
    until(|| c.joins.iter().all(|j| j.is_finished()));
    for h in &c.handles {
        assert!(h.is_stopped() && h.role().state == StateRole::Follower && h.role().leader == 0);
        assert_eq!(h.replicate(vec![]), Err(ReplicateError::Stopped));
        assert_eq!(h.read_barrier(), Err(ReplicateError::Stopped));
    }
}
#[test]
fn fatal_apply_failure_stops_waiters() {
    let c = Cluster::new();
    c.states[0].fail_next_apply();
    let h = c.handles[0].clone();
    assert_eq!(
        bounded(move || h.replicate(vec![1])),
        Err(ReplicateError::Stopped)
    );
    assert_eq!(c.handles[0].read_barrier(), Err(ReplicateError::Stopped));
}
