//! A leader engine that fences itself in the same term must not be replaced
//! while one of its proposals is still in the Raft log uncommitted.
use giggles_core::{Limits, relational::Value, replicated::ReplicaStore};
use giggles_replication::{
    core_store::{ReplicatedEngine, bootstrap, engine_opener},
    node::Replica,
    role::{Engine, RoleManager, Serving},
    sink::{self, Outbox, ReplicaHandle},
};
use raft::{
    StateRole,
    eraftpb::{Message, MessageType},
};
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

type Peers = Arc<Mutex<Vec<ReplicaHandle>>>;
/// Delivers everything except, while `hold` is set, log appends.
struct Network {
    peers: Peers,
    hold: Arc<AtomicBool>,
}
impl Outbox for Network {
    fn send(&mut self, m: Message) {
        if self.hold.load(Ordering::SeqCst) && m.get_msg_type() == MessageType::MsgAppend {
            return;
        }
        if let Some(peer) = self.peers.lock().unwrap().get(m.to as usize - 1) {
            peer.step(m.from, m);
        }
    }
}
/// (term, store applied index at the moment the engine was opened)
type Opens = Arc<Mutex<Vec<(u64, u64)>>>;
struct Cluster {
    handles: Vec<ReplicaHandle>,
    joins: Vec<thread::JoinHandle<()>>,
    managers: Vec<RoleManager<ReplicatedEngine>>,
    stores: Vec<Arc<ReplicaStore>>,
    opens: Vec<Opens>,
    peers: Peers,
    hold: Arc<AtomicBool>,
    dir: PathBuf,
}
impl Cluster {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let sequence = NEXT.fetch_add(1, Ordering::Relaxed);
        let mut c = Self {
            handles: vec![],
            joins: vec![],
            managers: vec![],
            stores: vec![],
            opens: vec![],
            peers: Arc::default(),
            hold: Arc::default(),
            dir: std::env::temp_dir().join(format!(
                "reopen-inflight-{}-{sequence}-{}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            )),
        };
        for id in 1..=3u64 {
            let (store, log) = bootstrap(
                &c.dir.join(format!("data-{id}")),
                &c.dir.join(format!("log-{id}")),
                [14; 16],
                id,
            )
            .unwrap();
            let mut replica = Replica::new(log, store.clone(), &[1, 2, 3]).unwrap();
            replica.set_election_timeout(if id == 1 { 10 } else { 19 });
            let (handle, join) = sink::spawn(
                replica,
                Box::new(Network {
                    peers: c.peers.clone(),
                    hold: c.hold.clone(),
                }),
                Duration::from_millis(10),
            );
            c.handles.push(handle);
            c.joins.push(join);
            c.stores.push(store);
        }
        *c.peers.lock().unwrap() = c.handles.clone();
        for (handle, store) in c.handles.iter().zip(&c.stores) {
            let opens: Opens = Arc::default();
            let record = opens.clone();
            let observed = store.clone();
            let mut open =
                engine_opener(store.clone(), handle.clone(), Limits::default(), 64 << 20);
            c.managers.push(RoleManager::start(
                handle.clone(),
                Box::new(move |term| {
                    let applied = observed.applied().unwrap().map_or(0, |p| p.index);
                    let engine = open(term)?;
                    record.lock().unwrap().push((term, applied));
                    Ok(engine)
                }),
                Duration::from_millis(20),
            ));
            c.opens.push(opens);
        }
        c
    }
    fn leader(&self) -> (usize, Arc<ReplicatedEngine>) {
        let mut found = None;
        until(|| {
            found = self
                .managers
                .iter()
                .enumerate()
                .find_map(|(i, m)| match m.current() {
                    Serving::Leader { engine, .. } => Some((i, engine)),
                    Serving::Follower { .. } => None,
                });
            found.is_some()
        });
        found.unwrap()
    }
}
impl Drop for Cluster {
    fn drop(&mut self) {
        self.hold.store(false, Ordering::SeqCst);
        for h in &self.handles {
            h.shutdown();
        }
        for j in self.joins.drain(..) {
            let result = j.join();
            if !thread::panicking() {
                result.unwrap();
            }
        }
        for m in self.managers.drain(..) {
            m.stop();
        }
        self.peers.lock().unwrap_or_else(|e| e.into_inner()).clear();
        self.stores.clear();
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
type SqlResult = Result<Vec<Vec<Value>>, String>;
fn sql_async(engine: &Arc<ReplicatedEngine>, statement: &str) -> mpsc::Receiver<SqlResult> {
    let (tx, rx) = mpsc::channel();
    let engine = engine.clone();
    let statement = statement.to_owned();
    thread::spawn(move || {
        let result = engine
            .admin()
            .sql_session()
            .execute(&statement, &[], None)
            .map(|r| r.rows.into_iter().map(|r| r.values).collect())
            .map_err(|e| e.to_string());
        let _ = tx.send(result);
    });
    rx
}
fn sql(engine: &Arc<ReplicatedEngine>, statement: &str) -> SqlResult {
    sql_async(engine, statement)
        .recv_timeout(Duration::from_secs(20))
        .expect("SQL timed out")
}

#[test]
fn self_fenced_engine_is_not_replaced_until_its_inflight_write_applies() {
    let c = Cluster::new();
    let (leader, old) = c.leader();
    let term = old.term();
    let handle = &c.handles[leader];
    let store = &c.stores[leader];
    sql(&old, "CREATE TABLE t(id int PRIMARY KEY)").unwrap();
    let before = handle.last_index().unwrap();

    // The write reaches the leader's log, but no follower receives it.
    c.hold.store(true, Ordering::SeqCst);
    let insert = sql_async(&old, "INSERT INTO t VALUES (1)");
    until(|| handle.last_index().unwrap() > before);
    thread::sleep(Duration::from_millis(50));
    let inflight = handle.last_index().unwrap();

    // Same effect as a failed replicate() on this engine: its sink fences itself.
    old.fence();
    assert!(old.is_fenced());

    // While the entry cannot commit, no replacement engine may be published.
    let replaced = c.managers[leader].wait_for(
        |s| matches!(s, Serving::Leader { engine, .. } if !Arc::ptr_eq(engine, &old)),
        Duration::from_millis(500),
    );
    assert!(
        replaced.is_none(),
        "a new engine was published while entry {inflight} was still in flight (applied {:?})",
        store.applied().unwrap()
    );
    let role = handle.role();
    assert_eq!((role.state, role.term), (StateRole::Leader, term));
    assert!(store.applied().unwrap().unwrap().index < inflight);
    assert!(matches!(insert.try_recv(), Err(mpsc::TryRecvError::Empty)));

    // Once appends flow, the in-flight entry commits, then the engine reopens.
    // An INSERT first reserves a durable sequence block, so the held entry is
    // usually that reservation and the fenced engine then refuses the data
    // batch; either way the statement must finish rather than hang.
    c.hold.store(false, Ordering::SeqCst);
    let inserted = insert.recv_timeout(Duration::from_secs(20)).unwrap();
    let Some(Serving::Leader {
        term: reopened,
        engine: fresh,
    }) = c.managers[leader].wait_for(
        |s| matches!(s, Serving::Leader { engine, .. } if !Arc::ptr_eq(engine, &old)),
        Duration::from_secs(20),
    )
    else {
        panic!("fenced leader did not reopen")
    };
    assert_eq!(reopened, term);
    let opens = c.opens[leader].lock().unwrap().clone();
    let (open_term, applied_at_open) = *opens.last().unwrap();
    assert_eq!(open_term, term);
    assert!(
        applied_at_open >= inflight,
        "engine opened at applied {applied_at_open}, before in-flight entry {inflight}"
    );
    // The fenced engine proposes nothing after it is fenced.
    assert!(sql(&old, "INSERT INTO t VALUES (3)").is_err());
    if inserted.is_err() {
        sql(&fresh, "INSERT INTO t VALUES (1)").unwrap();
    }
    sql(&fresh, "INSERT INTO t VALUES (2)").unwrap();
    assert_eq!(
        sql(&fresh, "SELECT id FROM t ORDER BY id").unwrap(),
        vec![vec![Value::Int(1)], vec![Value::Int(2)]]
    );
}
