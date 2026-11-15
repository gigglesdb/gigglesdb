use giggles_core::{
    Limits,
    relational::Value,
    replicated::{self as gr, BatchSink},
};
use giggles_replication::{
    core_store::{TermSink, bootstrap},
    error::Error,
    node::Replica,
    sink::{self, Outbox, ReplicaHandle},
    state::StateMachine,
};
use raft::{StateRole, eraftpb::Message};
use std::{
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
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
struct Dir(PathBuf);
impl Dir {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let sequence = NEXT.fetch_add(1, Ordering::Relaxed);
        Self(std::env::temp_dir().join(format!(
                "core-store-{}-{sequence}-{}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            )))
    }
}
impl Drop for Dir {
    fn drop(&mut self) {
        let result = std::fs::remove_dir_all(&self.0);
        if !std::thread::panicking() {
            result.unwrap();
        }
    }
}
struct Cluster {
    handles: Vec<ReplicaHandle>,
    joins: Vec<thread::JoinHandle<()>>,
    stores: Vec<Arc<gr::ReplicaStore>>,
    peers: Peers,
    _dir: Dir,
}
impl Cluster {
    fn new() -> Self {
        let dir = Dir::new();
        let peers = Arc::new(Mutex::new(Vec::new()));
        let isolated = Arc::new((0..3).map(|_| AtomicBool::new(false)).collect::<Vec<_>>());
        let mut c = Self {
            handles: vec![],
            joins: vec![],
            stores: vec![],
            peers,
            _dir: dir,
        };
        let mut replicas = Vec::new();
        for id in 1..=3 {
            let (store, log) = bootstrap(
                &c._dir.0.join(format!("data-{id}")),
                &c._dir.0.join(format!("log-{id}")),
                [14; 16],
                id,
            )
            .unwrap();
            let mut replica =
                Replica::new(log, store.clone() as Arc<dyn StateMachine>, &[1, 2, 3]).unwrap();
            replica.set_election_timeout(if id == 1 { 10 } else { 19 });
            replicas.push(replica);
            c.stores.push(store);
        }
        for replica in replicas {
            let (h, j) = sink::spawn(
                replica,
                Box::new(Network {
                    peers: c.peers.clone(),
                    isolated: isolated.clone(),
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
    fn leader_sink(&self) -> Arc<TermSink> {
        Arc::new(TermSink::new(
            self.handles[0].clone(),
            self.handles[0].role().term,
        ))
    }
    fn converged(&self) {
        until(|| {
            self.stores
                .iter()
                .all(|s| s.applied().unwrap() == self.stores[0].applied().unwrap())
        });
    }
}
impl Drop for Cluster {
    fn drop(&mut self) {
        for h in &self.handles {
            h.shutdown();
        }
        for j in self.joins.drain(..) {
            let result = j.join();
            if !std::thread::panicking() {
                result.unwrap();
            }
        }
        self.peers.lock().unwrap_or_else(|e| e.into_inner()).clear();
        self.stores.clear();
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
fn sql_write_on_leader_reaches_all_three_stores() {
    let c = Cluster::new();
    let admin = c.stores[0]
        .open_admin(c.leader_sink(), Limits::default(), 64 << 20)
        .unwrap();
    let mut session = admin.sql_session();
    session
        .execute("CREATE TABLE t(id int PRIMARY KEY, v text)", &[], None)
        .unwrap();
    session
        .execute(
            "INSERT INTO t VALUES (1, 'one'), (2, 'two'), (3, 'three')",
            &[],
            None,
        )
        .unwrap();
    let result = session
        .execute("SELECT id, v FROM t ORDER BY id", &[], None)
        .unwrap();
    assert_eq!(
        result
            .rows
            .iter()
            .map(|r| r.values.clone())
            .collect::<Vec<_>>(),
        vec![
            vec![Value::Int(1), Value::Text("one".into())],
            vec![Value::Int(2), Value::Text("two".into())],
            vec![Value::Int(3), Value::Text("three".into())],
        ]
    );
    c.converged();
    assert!(c.stores[0].applied().unwrap().is_some());
}
#[test]
fn follower_and_fenced_sinks_refuse() {
    let c = Cluster::new();
    let follower = TermSink::new(c.handles[1].clone(), c.handles[1].role().term);
    assert_eq!(
        follower.replicate(vec![]),
        Err(gr::ReplicateError::NotLeader)
    );
    assert!(follower.is_fenced());
    let sink = c.leader_sink();
    let admin = c.stores[0]
        .open_admin(sink.clone(), Limits::default(), 64 << 20)
        .unwrap();
    let mut session = admin.sql_session();
    session
        .execute("CREATE TABLE t(id int PRIMARY KEY)", &[], None)
        .unwrap();
    c.converged();
    let before: Vec<_> = c.stores.iter().map(|s| s.applied().unwrap()).collect();
    let wrong_term = TermSink::new(c.handles[0].clone(), c.handles[0].role().term + 1);
    assert_eq!(
        wrong_term.replicate(vec![]),
        Err(gr::ReplicateError::NotLeader)
    );
    assert!(wrong_term.is_fenced());
    sink.fence();
    assert!(sink.is_fenced());
    assert!(
        session
            .execute("INSERT INTO t VALUES (1)", &[], None)
            .is_err()
    );
    assert!(
        session
            .execute("INSERT INTO t VALUES (2)", &[], None)
            .is_err()
    );
    thread::sleep(Duration::from_millis(50));
    assert_eq!(
        c.stores
            .iter()
            .map(|s| s.applied().unwrap())
            .collect::<Vec<_>>(),
        before
    );
    c.handles[0].shutdown();
    until(|| c.joins[0].is_finished());
    assert!(c.handles[0].is_stopped());
    assert_eq!(c.handles[0].role().state, StateRole::Follower);
    let stopped = c.leader_sink();
    assert_eq!(
        stopped.replicate(vec![]),
        Err(gr::ReplicateError::NotLeader)
    );
    assert!(stopped.is_fenced());
    assert_eq!(
        stopped.replicate(vec![]),
        Err(gr::ReplicateError::NotLeader)
    );
}
#[test]
fn bootstrap_refuses_wrong_domain_and_reopens() {
    let dir = Dir::new();
    let data = dir.0.join("data");
    let log = dir.0.join("log");
    std::fs::create_dir_all(&data).unwrap(); // Existing empty directory also creates.
    let (store, raft_log) = bootstrap(&data, &log, [14; 16], 1).unwrap();
    store
        .apply_noop(gr::AppliedPosition { index: 1, term: 1 })
        .unwrap();
    store.flush().unwrap();
    let applied = store.applied().unwrap();
    drop((store, raft_log));
    let (store, raft_log) = bootstrap(&data, &log, [14; 16], 1).unwrap();
    assert_eq!(store.applied().unwrap(), applied);
    assert_eq!(
        StateMachine::applied(store.as_ref())
            .unwrap()
            .unwrap()
            .index,
        1
    );
    assert_eq!(
        StateMachine::apply_noop(
            store.as_ref(),
            giggles_replication::state::AppliedPosition { index: 1, term: 1 }
        )
        .unwrap(),
        giggles_replication::state::ApplyOutcome::Skipped
    );
    drop((store, raft_log));
    assert!(matches!(
        bootstrap(&data, &log, [15; 16], 1),
        Err(Error::Identity(_))
    ));
    assert!(matches!(
        bootstrap(&data, &log, [14; 16], 2),
        Err(Error::Identity(_))
    ));
    let other_data = dir.0.join("other-data");
    assert!(matches!(
        bootstrap(&other_data, &log, [15; 16], 1),
        Err(Error::Identity(_))
    ));
}
