use giggles_core::{Limits, relational::Value, replicated::ReplicaStore};
use giggles_replication::{
    core_store::{ReplicatedEngine, bootstrap, engine_opener},
    node::Replica,
    role::{RoleManager, Serving},
    sink::{self, Outbox, ReplicaHandle},
};
use raft::eraftpb::Message;
use std::{
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

type Peers = Arc<Mutex<Vec<Option<ReplicaHandle>>>>;
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
        if let Some(Some(peer)) = self.peers.lock().unwrap().get(m.to as usize - 1) {
            peer.step(m.from, m);
        }
    }
}
struct Node {
    handle: ReplicaHandle,
    join: thread::JoinHandle<()>,
    manager: RoleManager<ReplicatedEngine>,
    store: Arc<ReplicaStore>,
    opened_leader: Arc<AtomicBool>,
}
impl Node {
    fn stop(self) {
        self.handle.shutdown();
        until(|| self.join.is_finished());
        self.manager.stop();
        self.join.join().expect("replica thread panicked");
    }
}
struct Cluster {
    nodes: Vec<Option<Node>>,
    peers: Peers,
    isolated: Arc<Vec<AtomicBool>>,
    dir: PathBuf,
}
impl Cluster {
    fn new() -> Self {
        // Parallel tests can read the same Windows clock tick; a timestamp alone
        // lets their live clusters collide and hold each other's writer lock.
        static NEXT_CLUSTER: AtomicU64 = AtomicU64::new(0);
        let sequence = NEXT_CLUSTER.fetch_add(1, Ordering::Relaxed);
        let mut c = Self {
            nodes: (0..3).map(|_| None).collect(),
            peers: Arc::new(Mutex::new(vec![None; 3])),
            isolated: Arc::new((0..3).map(|_| AtomicBool::new(false)).collect()),
            dir: std::env::temp_dir().join(format!(
                "failover-{}-{sequence}-{}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            )),
        };
        for i in 0..3 {
            c.start(i);
        }
        c.leader(0);
        c
    }
    fn node(&self, i: usize) -> &Node {
        self.nodes[i].as_ref().unwrap()
    }
    fn start(&mut self, i: usize) {
        let id = i as u64 + 1;
        let (store, log) = bootstrap(
            &self.dir.join(format!("data-{id}")),
            &self.dir.join(format!("log-{id}")),
            [14; 16],
            id,
        )
        .unwrap();
        let mut replica = Replica::new(log, store.clone(), &[1, 2, 3]).unwrap();
        replica.set_election_timeout(if i == 0 { 10 } else { 19 });
        let (handle, join) = sink::spawn(
            replica,
            Box::new(Network {
                peers: self.peers.clone(),
                isolated: self.isolated.clone(),
            }),
            Duration::from_millis(10),
        );
        self.peers.lock().unwrap()[i] = Some(handle.clone());
        let opened_leader = Arc::new(AtomicBool::new(false));
        let opened = opened_leader.clone();
        let mut open = engine_opener(store.clone(), handle.clone(), Limits::default(), 64 << 20);
        let manager = RoleManager::start(
            handle.clone(),
            Box::new(move |term| {
                let engine = open(term)?;
                opened.store(true, Ordering::SeqCst);
                Ok(engine)
            }),
            Duration::from_millis(20),
        );
        self.nodes[i] = Some(Node {
            handle,
            join,
            manager,
            store,
            opened_leader,
        });
    }
    fn leader(&self, after: u64) -> (usize, Arc<ReplicatedEngine>) {
        let mut result = None;
        until(|| {
            let leaders: Vec<_> = self
                .nodes
                .iter()
                .enumerate()
                .filter_map(|(i, n)| match n.as_ref()?.manager.current() {
                    Serving::Leader { term, engine } if term > after => Some((i, engine)),
                    _ => None,
                })
                .collect();
            if leaders.len() == 1 {
                result = leaders.into_iter().next();
                true
            } else {
                false
            }
        });
        result.unwrap()
    }
    fn converged(&self) {
        until(|| {
            for n in self.nodes.iter().flatten() {
                assert!(!n.handle.is_stopped(), "replica stopped unexpectedly");
            }
            let applied = self.node(0).store.applied().unwrap();
            applied.is_some()
                && self
                    .nodes
                    .iter()
                    .flatten()
                    .all(|n| n.store.applied().unwrap() == applied)
        });
    }
}
impl Drop for Cluster {
    fn drop(&mut self) {
        for n in self.nodes.iter().flatten() {
            n.handle.shutdown();
        }
        for n in &mut self.nodes {
            if let Some(n) = n.take() {
                n.stop();
            }
        }
        self.peers.lock().unwrap().clear();
        let cleanup = std::fs::remove_dir_all(&self.dir);
        if !thread::panicking() {
            cleanup.unwrap();
        }
    }
}
fn until(mut predicate: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while !predicate() {
        assert!(Instant::now() < deadline, "poll timed out");
        thread::sleep(Duration::from_millis(5));
    }
}
fn sql(engine: &Arc<ReplicatedEngine>, statement: &str) -> Result<Vec<Vec<Value>>, String> {
    let (tx, rx) = mpsc::channel();
    let engine = engine.clone();
    let statement = statement.to_owned();
    let join = thread::spawn(move || {
        let result = engine
            .admin()
            .sql_session()
            .execute(&statement, &[], None)
            .map(|r| r.rows.into_iter().map(|r| r.values).collect())
            .map_err(|e| e.to_string());
        let _ = tx.send(result);
    });
    let result = rx
        .recv_timeout(Duration::from_secs(20))
        .expect("SQL timed out");
    join.join().unwrap();
    result
}
fn seed(engine: &Arc<ReplicatedEngine>) {
    sql(engine, "CREATE TABLE t(id int PRIMARY KEY, v text)").unwrap();
    sql(
        engine,
        "INSERT INTO t VALUES (1, 'one'), (2, 'two'), (3, 'three')",
    )
    .unwrap();
}
fn rows(engine: &Arc<ReplicatedEngine>, fourth: bool) {
    let mut expected = vec![
        vec![Value::Int(1), Value::Text("one".into())],
        vec![Value::Int(2), Value::Text("two".into())],
        vec![Value::Int(3), Value::Text("three".into())],
    ];
    if fourth {
        expected.push(vec![Value::Int(4), Value::Text("four".into())]);
    }
    assert_eq!(
        sql(engine, "SELECT id, v FROM t ORDER BY id").unwrap(),
        expected
    );
}
#[test]
fn leader_failover_serves_committed_rows_and_fences_old_engine() {
    let c = Cluster::new();
    let (leader, old) = c.leader(0);
    seed(&old);
    c.converged();
    c.isolated[leader].store(true, Ordering::SeqCst);
    let (next, engine) = c.leader(old.term());
    assert_ne!(leader, next);
    assert!(engine.term() > old.term());
    rows(&engine, false);
    sql(&engine, "INSERT INTO t VALUES (4, 'four')").unwrap();
    assert!(sql(&old, "INSERT INTO t VALUES (5, 'stale')").is_err());
    c.isolated[leader].store(false, Ordering::SeqCst);
    c.converged();
    rows(&c.leader(0).1, true);
}
#[test]
fn restarted_follower_applies_exactly_once() {
    let mut c = Cluster::new();
    let (leader, engine) = c.leader(0);
    seed(&engine);
    c.converged();
    let follower = (leader + 1) % 3;
    let before = c.node(follower).store.applied().unwrap().unwrap();
    c.peers.lock().unwrap()[follower] = None;
    let stopped = c.nodes[follower].take().unwrap();
    let old_store = stopped.store.clone();
    let opened_leader = stopped.opened_leader.clone();
    stopped.stop();
    let opened_leader = opened_leader.load(Ordering::SeqCst);
    eprintln!(
        "restart follower {}: store strong_count={}, opened leader engine={opened_leader}",
        follower + 1,
        Arc::strong_count(&old_store)
    );
    assert_eq!(
        Arc::strong_count(&old_store),
        1,
        "stopped follower retains its store"
    );
    drop(old_store);
    c.start(follower);
    assert!(c.node(follower).store.applied().unwrap().unwrap().index >= before.index);
    sql(&engine, "INSERT INTO t VALUES (4, 'four')").unwrap();
    c.converged();
    let applied = c.node(follower).store.applied().unwrap().unwrap();
    assert!(applied.index > before.index);
    assert_eq!(applied, c.node(leader).store.applied().unwrap().unwrap());
    rows(&engine, true);
}
