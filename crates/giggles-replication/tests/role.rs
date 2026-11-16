use giggles_replication::{
    log_store::LogStore,
    node::Replica,
    role::{Engine, RoleManager, Serving},
    sink::{self, Outbox, ReplicaHandle},
    state::MemStateMachine,
};
use raft::{StateRole, eraftpb::Message};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
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
struct FakeEngine {
    term: u64,
    fenced: AtomicBool,
    fence_calls: AtomicU64,
}
impl Engine for FakeEngine {
    fn fence(&self) {
        self.fence_calls.fetch_add(1, Ordering::SeqCst);
        self.fenced.store(true, Ordering::SeqCst);
    }
    fn is_fenced(&self) -> bool {
        self.fenced.load(Ordering::SeqCst)
    }
}
struct Cluster {
    handles: Vec<ReplicaHandle>,
    joins: Vec<thread::JoinHandle<()>>,
    managers: Vec<RoleManager<FakeEngine>>,
    opens: Arc<Mutex<BTreeMap<(usize, u64), usize>>>,
    isolated: Arc<Vec<AtomicBool>>,
    peers: Peers,
    dir: PathBuf,
}
impl Cluster {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let sequence = NEXT.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "role-{}-{sequence}-{}",
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
            managers: vec![],
            opens: Arc::default(),
            isolated,
            peers,
            dir,
        };
        for id in 1..=3 {
            let mut replica = Replica::new(
                LogStore::open(&c.dir.join(id.to_string()), [16; 16], id).unwrap(),
                Arc::new(MemStateMachine::new()),
                &[1, 2, 3],
            )
            .unwrap();
            replica.set_election_timeout(if id == 1 { 10 } else { 19 });
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
        for (node, h) in c.handles.iter().enumerate() {
            let opens = c.opens.clone();
            c.managers.push(RoleManager::start(
                h.clone(),
                Box::new(move |term| {
                    *opens.lock().unwrap().entry((node, term)).or_default() += 1;
                    Ok(FakeEngine {
                        term,
                        fenced: AtomicBool::new(false),
                        fence_calls: AtomicU64::new(0),
                    })
                }),
                Duration::from_millis(5),
            ));
        }
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
        for m in self.managers.drain(..) {
            m.stop();
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
#[test]
fn leadership_loss_heal_and_stop_fence_engines() {
    let mut c = Cluster::new();
    until(|| {
        c.managers
            .iter()
            .any(|m| matches!(m.current(), Serving::Leader { .. }))
    });
    let leader = c
        .managers
        .iter()
        .position(|m| matches!(m.current(), Serving::Leader { .. }))
        .unwrap();
    let Serving::Leader { term, engine } = c.managers[leader].current() else {
        panic!("lost leader")
    };
    assert_eq!(engine.term, term);
    assert!(!engine.fenced.load(Ordering::SeqCst));
    for (i, manager) in c.managers.iter().enumerate() {
        if i != leader {
            assert!(manager.wait_for(|s| matches!(s, Serving::Follower { leader: id, term: t } if *id == leader as u64 + 1 && *t == term), Duration::from_secs(5)).is_some());
        }
    }
    assert_eq!(c.opens.lock().unwrap().get(&(leader, term)), Some(&1));
    c.isolated[leader].store(true, Ordering::SeqCst);
    until(|| {
        c.managers.iter().enumerate().any(|(i, m)| {
            i != leader && matches!(m.current(), Serving::Leader { term: t, .. } if t > term)
        })
    });
    assert!(
        c.managers[leader]
            .wait_for(
                |s| matches!(s, Serving::Follower { .. }),
                Duration::from_secs(5)
            )
            .is_some()
    );
    assert!(engine.fenced.load(Ordering::SeqCst));
    c.isolated[leader].store(false, Ordering::SeqCst);
    until(|| {
        let roles: Vec<_> = c.handles.iter().map(ReplicaHandle::role).collect();
        let Some(r) = roles.iter().find(|r| r.state == StateRole::Leader) else {
            return false;
        };
        roles
            .iter()
            .all(|other| other.term == r.term && other.leader == r.leader)
            && c.managers
                .iter()
                .filter(|m| matches!(m.current(), Serving::Leader { .. }))
                .count()
                == 1
    });
    for m in c.managers.drain(..) {
        let serving = m.current();
        m.stop();
        if let Serving::Leader { engine, .. } = serving {
            assert!(engine.fenced.load(Ordering::SeqCst));
        }
    }
    assert!(c.opens.lock().unwrap().values().all(|count| *count == 1));
}
#[test]
fn stopped_replica_never_opens_and_wait_times_out() {
    let c = Cluster::new();
    until(|| {
        c.handles
            .iter()
            .any(|h| h.role().state == StateRole::Leader)
    });
    let h = c
        .handles
        .iter()
        .find(|h| h.role().state == StateRole::Leader)
        .unwrap()
        .clone();
    h.shutdown();
    until(|| h.is_stopped());
    let m = RoleManager::<FakeEngine>::start(
        h,
        Box::new(|_| panic!("opened stopped replica")),
        Duration::from_millis(5),
    );
    assert!(
        m.wait_for(
            |s| matches!(s, Serving::Leader { .. }),
            Duration::from_millis(30)
        )
        .is_none()
    );
    m.stop();
}

#[test]
fn self_fenced_leader_reopens_in_the_same_term() {
    let c = Cluster::new();
    until(|| {
        c.managers
            .iter()
            .any(|m| matches!(m.current(), Serving::Leader { .. }))
    });
    let leader = c
        .managers
        .iter()
        .position(|m| matches!(m.current(), Serving::Leader { .. }))
        .unwrap();
    let Serving::Leader { term, engine } = c.managers[leader].current() else {
        panic!("lost leader")
    };
    assert_eq!(engine.fence_calls.load(Ordering::SeqCst), 0);
    engine.fenced.store(true, Ordering::SeqCst);
    let reopened = c.managers[leader].wait_for(
        |s| {
            matches!(s, Serving::Leader { term: t, engine: e }
            if *t == term && !Arc::ptr_eq(e, &engine))
        },
        Duration::from_millis(250),
    );
    let Some(Serving::Leader { engine: fresh, .. }) = reopened else {
        panic!("fenced leader did not reopen in the same term")
    };
    assert_eq!(fresh.term, term);
    assert!(!fresh.is_fenced());
    assert_eq!(engine.fence_calls.load(Ordering::SeqCst), 1);
    assert_eq!(c.opens.lock().unwrap().get(&(leader, term)), Some(&2));
    let role = c.handles[leader].role();
    assert_eq!(role.state, StateRole::Leader);
    assert_eq!(role.term, term);
}
