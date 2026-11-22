//! R3-607 (C4): a protected run takes the table's Shared SQL sentinel, so it
//! neither blocks point writers nor queues them behind its own wait.
use super::*;
use crate::task::effects::Effects;
use relational::Value::Int;
use std::sync::mpsc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

struct Fixture { admin: Option<Admin>, path: std::path::PathBuf }
impl Fixture {
    fn new() -> Self {
        let mut nonce = [0; 16];
        getrandom::fill(&mut nonce).unwrap();
        let path = std::env::temp_dir().join(format!("r3sentinel-{:032x}", u128::from_be_bytes(nonce)));
        let admin = Some(Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap());
        Self { admin, path }
    }
    fn admin(&self) -> &Admin { self.admin.as_ref().unwrap() }
    fn sql(&self) -> SqlSession { self.admin().sql_session() }
    fn exec(&self, sql: &str) { run(&mut self.sql(), sql); }
    fn db(&self) -> crate::storage::EngineLease {
        match &self.admin().inner.backend { Backend::V2(_) => self.admin().test_v2(), _ => panic!("v2 only") }
    }
    fn run(&self) -> ProtectedRun {
        ProtectedRun { txn: self.db().begin(Isolation::Serializable), effects: Effects::default() }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) { self.admin.take(); let _ = fs::remove_dir_all(&self.path); }
}
fn run(s: &mut SqlSession, sql: &str) -> relational::SqlResult {
    s.execute_at(sql, &[], None, &clock()).unwrap_or_else(|e| panic!("{sql}: {e:?}"))
}
fn clock() -> relational::StatementClock {
    relational::StatementClock { unavailable: None, transaction_us: 1, statement_us: 1, zone: None, wall: None }
}
fn protected(f: &Fixture, run: ProtectedRun, sql: &str) -> (ProtectedRun, Result<relational::SqlResult, CoreError>) {
    let mut s = f.sql();
    let p = s.prepare(sql).unwrap();
    s.execute_protected_v2(&f.db(), run, &p, &[], None)
}
fn commit(f: &Fixture, run: ProtectedRun) -> Result<(), CoreError> {
    cross_check_effects(&run.effects, &run.txn)?;
    f.db().catalog().commit_ddl(run.txn).map(drop).map_err(CoreError::from)
}
fn values(f: &Fixture) -> Vec<Vec<relational::Value>> {
    run(&mut f.sql(), "SELECT id,n FROM t ORDER BY id").rows.into_iter().map(|r| r.values).collect()
}
fn setup() -> Fixture {
    let f = Fixture::new();
    f.exec("CREATE TABLE t(id bigint PRIMARY KEY,n bigint)");
    f.exec("INSERT INTO t VALUES(1,0),(2,0),(3,0)");
    f
}
/// Work on its own thread (and so its own session).
struct Pending<T> { done: mpsc::Receiver<T>, worker: JoinHandle<()> }
impl<T: Send + 'static> Pending<T> {
    fn spawn(work: impl FnOnce() -> T + Send + 'static) -> Self {
        let (send, done) = mpsc::channel();
        let worker = thread::spawn(move || { let _ = send.send(work()); });
        Self { done, worker }
    }
    /// Waits until some transaction is queued on a lock while this one has
    /// not finished.
    fn wait_blocked(&self, waiting: impl Fn() -> usize) {
        let until = Instant::now() + Duration::from_secs(10);
        while waiting() == 0 {
            assert!(matches!(self.done.try_recv(), Err(mpsc::TryRecvError::Empty)), "finished instead of waiting");
            assert!(Instant::now() < until, "never entered a lock wait");
            thread::sleep(Duration::from_millis(1));
        }
    }
    fn finish(self) -> T {
        let result = self.done.recv_timeout(Duration::from_secs(20)).expect("did not finish");
        self.worker.join().unwrap();
        result
    }
}

/// An autocommit statement on its own session and thread.
fn spawn_sql(f: &Fixture, sql: &str) -> Pending<Result<relational::SqlResult, CoreError>> {
    let admin = f.admin().clone();
    let sql = sql.to_owned();
    Pending::spawn(move || admin.sql_session().execute_at(&sql, &[], None, &clock()))
}

#[test]
fn point_writer_proceeds_during_protected_run() {
    let f = setup();
    let (r, result) = protected(&f, f.run(), "UPDATE t SET n=5 WHERE id=1");
    assert_eq!(result.unwrap().affected, 1);
    // The parked run holds the Shared sentinel; a point writer of another row
    // completes without waiting for it.
    let writer = spawn_sql(&f, "UPDATE t SET n=7 WHERE id=2");
    assert_eq!(writer.finish().unwrap().affected, 1);
    assert_eq!(values(&f), vec![vec![Int(1), Int(0)], vec![Int(2), Int(7)], vec![Int(3), Int(0)]]);
    // It wrote no row the run read, so the run still commits.
    commit(&f, r).unwrap();
    assert_eq!(values(&f), vec![vec![Int(1), Int(5)], vec![Int(2), Int(7)], vec![Int(3), Int(0)]]);
}

#[test]
fn waiting_protected_run_does_not_block_later_point_writers() {
    let f = setup();
    // A long application transaction holds the Shared sentinel and row 1.
    let mut app = f.sql();
    run(&mut app, "BEGIN");
    run(&mut app, "UPDATE t SET n=1 WHERE id=1");
    let (admin, db) = (f.admin().clone(), f.db());
    let agent = Pending::spawn(move || {
        let mut s = admin.sql_session();
        let run = ProtectedRun { txn: db.begin(Isolation::Serializable), effects: Effects::default() };
        let p = s.prepare("UPDATE t SET n=9 WHERE id=1").unwrap();
        s.execute_protected_v2(&db, run, &p, &[], None).1.map(|r| r.affected)
    });
    agent.wait_blocked(|| app.v2_transaction.as_ref().unwrap().sql_waiting_count());
    // With an Exclusive sentinel request queued (FIFO), this writer would wait
    // behind the agent. With Shared it proceeds while the agent still waits.
    let later = spawn_sql(&f, "UPDATE t SET n=2 WHERE id=2");
    assert_eq!(later.finish().unwrap().affected, 1);
    assert!(matches!(agent.done.try_recv(), Err(mpsc::TryRecvError::Empty)), "agent finished before the app committed");
    run(&mut app, "COMMIT");
    // Row 1 changed after the run's snapshot.
    assert_eq!(agent.finish(), Err(CoreError::Sql(SqlError::Conflict)));
    assert_eq!(values(&f), vec![vec![Int(1), Int(1)], vec![Int(2), Int(2)], vec![Int(3), Int(0)]]);
}

#[test]
fn scan_writer_waits_for_protected_run() {
    let f = setup();
    let (r, result) = protected(&f, f.run(), "UPDATE t SET n=5 WHERE id=1");
    assert_eq!(result.unwrap().affected, 1);
    // A searched writer takes the Exclusive sentinel and so waits for the run.
    let scan = spawn_sql(&f, "UPDATE t SET n=n+7 WHERE n>=0");
    scan.wait_blocked(|| r.txn.sql_waiting_count());
    commit(&f, r).unwrap();
    assert_eq!(scan.finish().unwrap().affected, 3);
    // The scan ran after the run committed and saw its write.
    assert_eq!(values(&f), vec![vec![Int(1), Int(12)], vec![Int(2), Int(7)], vec![Int(3), Int(7)]]);
}

#[test]
fn row_changed_after_snapshot_gives_conflict() {
    let f = setup();
    let mut app = f.sql();
    run(&mut app, "BEGIN");
    run(&mut app, "UPDATE t SET n=3 WHERE id=2");
    let (admin, db) = (f.admin().clone(), f.db());
    let agent = Pending::spawn(move || {
        let mut s = admin.sql_session();
        let run = ProtectedRun { txn: db.begin(Isolation::Serializable), effects: Effects::default() };
        let p = s.prepare("UPDATE t SET n=n+10 WHERE id=2").unwrap();
        let (r, result) = s.execute_protected_v2(&db, run, &p, &[], None);
        (result.map(|x| x.affected), r.txn.written_keys(Keyspace::Rows).count(), r.effects.len())
    });
    // The run took its snapshot, then waits for row 2's lock.
    agent.wait_blocked(|| app.v2_transaction.as_ref().unwrap().sql_waiting_count());
    run(&mut app, "COMMIT");
    let (result, written, effects) = agent.finish();
    let error = result.unwrap_err();
    assert_eq!(error, CoreError::Sql(SqlError::Conflict));
    assert_eq!(crate::sqlstate::sqlstate(&error), "40001");
    assert_eq!((written, effects), (0, 0));
    assert_eq!(values(&f)[1], vec![Int(2), Int(3)]);
}
