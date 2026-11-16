//! R3-606: the protected-statement seam (C1) runs one DML statement inside a
//! caller-owned transaction and never commits it.
use super::*;
use crate::task::effects::Effects;
use relational::Value::Int;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

struct Fixture { admin: Option<Admin>, path: std::path::PathBuf }
impl Fixture {
    fn new() -> Self {
        let mut nonce = [0; 16];
        getrandom::fill(&mut nonce).unwrap();
        let path = std::env::temp_dir().join(format!("r3seam-{:032x}", u128::from_be_bytes(nonce)));
        let admin = Some(Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap());
        Self { admin, path }
    }
    fn admin(&self) -> &Admin { self.admin.as_ref().unwrap() }
    fn sql(&self) -> SqlSession { self.admin().sql_session() }
    fn exec(&self, sql: &str) { run(&mut self.sql(), sql); }
    fn db(&self) -> Arc<v2::V2Db> {
        match &self.admin().inner.backend { Backend::V2(db) => db.clone(), _ => panic!("v2 only") }
    }
    fn run(&self) -> ProtectedRun {
        ProtectedRun { txn: self.db().begin(Isolation::Serializable), effects: Effects::default() }
    }
    fn slots(&self) -> usize { self.admin().inner.sql_transactions.load(Ordering::Acquire) }
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
fn protected(f: &Fixture, s: &mut SqlSession, run: ProtectedRun, sql: &str)
    -> (ProtectedRun, Result<relational::SqlResult, CoreError>)
{
    let p = s.prepare(sql).unwrap();
    s.execute_protected_v2(&f.db(), run, &p, &[], None)
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

#[test]
fn returns_uncommitted_with_writes_in_txn() {
    let f = setup();
    let mut s = f.sql();
    let (r, result) = protected(&f, &mut s, f.run(), "UPDATE t SET n=n+1 WHERE id<=2");
    assert_eq!(result.unwrap().affected, 2);
    assert_eq!(r.txn.written_keys(Keyspace::Rows).count(), 2);
    assert_eq!(r.effects.len(), 2);
    cross_check_effects(&r.effects, &r.txn).unwrap();
    // Not committed: another session still sees the old values.
    assert_eq!(values(&f), vec![vec![Int(1), Int(0)], vec![Int(2), Int(0)], vec![Int(3), Int(0)]]);
}

#[test]
fn drop_of_run_leaves_table_unchanged() {
    let f = setup();
    let mut s = f.sql();
    let (r, result) = protected(&f, &mut s, f.run(), "UPDATE t SET n=9");
    assert_eq!(result.unwrap().affected, 3);
    drop(r);
    assert_eq!(values(&f), vec![vec![Int(1), Int(0)], vec![Int(2), Int(0)], vec![Int(3), Int(0)]]);
    // The session is usable afterwards and the ordinary path still commits.
    run(&mut s, "UPDATE t SET n=5 WHERE id=1");
    assert_eq!(values(&f)[0], vec![Int(1), Int(5)]);
}

#[test]
fn no_sql_transaction_slot_is_taken() {
    let f = setup();
    let mut s = f.sql();
    assert_eq!(f.slots(), 0);
    let (r, result) = protected(&f, &mut s, f.run(), "UPDATE t SET n=1 WHERE id=3");
    result.unwrap();
    assert_eq!(f.slots(), 0);
    assert!(s.protected.is_none());
    drop(r);
    s.rollback();
    assert_eq!(f.slots(), 0);
}

#[test]
fn refused_inside_explicit_transaction() {
    let f = setup();
    let mut s = f.sql();
    run(&mut s, "BEGIN");
    let (r, result) = protected(&f, &mut s, f.run(), "UPDATE t SET n=1");
    assert_eq!(result.unwrap_err(), CoreError::Sql(SqlError::Transaction));
    assert!(r.effects.is_empty());
    run(&mut s, "ROLLBACK");
    // A non-DML statement is refused too.
    let (_, result) = protected(&f, &mut s, f.run(), "SELECT 1");
    assert_eq!(result.unwrap_err(), CoreError::Sql(SqlError::Transaction));
}

#[test]
fn statement_error_leaves_empty_write_set() {
    let f = setup();
    let mut s = f.sql();
    let (r, result) = protected(&f, &mut s, f.run(), "UPDATE t SET n=1/(id-2)");
    assert!(result.is_err());
    assert_eq!(r.txn.written_keys(Keyspace::Rows).count(), 0);
    assert!(s.protected.is_none());
}

#[test]
fn snapshot_is_taken_after_table_lock_wait() {
    let f = setup();
    let mut blocker = f.sql();
    run(&mut blocker, "BEGIN");
    // A searched (non-candidate) READ COMMITTED writer holds the table's
    // Exclusive SQL sentinel to commit, so the run's Shared sentinel waits.
    run(&mut blocker, "UPDATE t SET n=n+7 WHERE n>=0");
    let admin = f.admin().clone();
    let db = f.db();
    let (send, done) = mpsc::channel();
    let worker = thread::spawn(move || {
        let mut s = admin.sql_session();
        let run = ProtectedRun { txn: db.begin(Isolation::Serializable), effects: Effects::default() };
        let p = s.prepare("UPDATE t SET n=n+1 WHERE id=1").unwrap();
        let (r, result) = s.execute_protected_v2(&db, run, &p, &[], None);
        let new = r.effects.iter().next().and_then(|e| e.new.clone()).map(|row| row.values);
        send.send((result.map(|x| x.affected), new)).unwrap();
    });
    let until = Instant::now() + Duration::from_secs(10);
    while blocker.v2_transaction.as_ref().unwrap().sql_waiting_count() == 0 {
        assert!(done.try_recv().is_err(), "protected run did not wait for the table lock");
        assert!(Instant::now() < until, "protected run never entered a lock wait");
        thread::sleep(Duration::from_millis(1));
    }
    run(&mut blocker, "COMMIT");
    let (affected, new) = done.recv_timeout(Duration::from_secs(20)).unwrap();
    worker.join().unwrap();
    assert_eq!(affected.unwrap(), 1);
    // The run's snapshot was taken after the blocker committed: 7 + 1.
    assert_eq!(new, Some(vec![Int(1), Int(8)]));
}

#[test]
fn paged_update_of_500_rows_collects_all_through_seam() {
    let f = Fixture::new();
    f.exec("CREATE TABLE t(id bigint PRIMARY KEY,n bigint)");
    let rows = (1..=500).map(|g| format!("({g},0)")).collect::<Vec<_>>().join(",");
    f.exec(&format!("INSERT INTO t VALUES{rows}"));
    let mut s = f.sql();
    let (r, result) = protected(&f, &mut s, f.run(), "UPDATE t SET n=1");
    assert_eq!(result.unwrap().affected, 500);
    assert_eq!(r.effects.len(), 500);
    cross_check_effects(&r.effects, &r.txn).unwrap();
}
