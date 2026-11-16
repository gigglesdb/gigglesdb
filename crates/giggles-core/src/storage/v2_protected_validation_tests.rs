//! R3-610 (C7): SQL-level validation of a protected run against other
//! writers. Each test parks a run (one protected DML statement, uncommitted),
//! performs the concurrent write on a second session, then commits the run
//! with `db.catalog().commit_ddl`. `shipments` stands in for the brief's
//! `deliveries`, which is a reserved identifier.
use super::*;
use crate::storage::roles::{NewRole, TablePrivilege};
use crate::task::effects::Effects;
use relational::Value::{Int, Text};
use std::sync::mpsc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

struct Fixture { admin: Option<Admin>, path: std::path::PathBuf }
impl Fixture {
    fn new() -> Self { Self::with_recent_log(None) }
    /// `Some(0)` keeps no recent commit log, so every SERIALIZABLE commit
    /// validates through the durable fallback scan.
    fn with_recent_log(bytes: Option<usize>) -> Self {
        let mut nonce = [0; 16];
        getrandom::fill(&mut nonce).unwrap();
        let path = std::env::temp_dir().join(format!("r3valid-{:032x}", u128::from_be_bytes(nonce)));
        v2::TEST_RECENT_LOG_BYTES.set(bytes);
        let admin = Admin::create_engine(&path, Limits::default(), Engine::V2);
        v2::TEST_RECENT_LOG_BYTES.set(None);
        let f = Self { admin: Some(admin.unwrap()), path };
        f.exec("CREATE TABLE shipments(id bigint PRIMARY KEY,merchant_id bigint,status text,customer_email text)");
        f.exec("INSERT INTO shipments VALUES(1,7,'new','a@x'),(2,8,'new','b@x'),(3,7,'new','c@x'),(4,8,'new','d@x')");
        f
    }
    fn admin(&self) -> &Admin { self.admin.as_ref().unwrap() }
    fn sql(&self) -> SqlSession { self.admin().sql_session() }
    fn exec(&self, sql: &str) -> relational::SqlResult { run(&mut self.sql(), sql) }
    fn db(&self) -> Arc<v2::V2Db> {
        match &self.admin().inner.backend { Backend::V2(db) => db.clone(), _ => panic!("v2 only") }
    }
    /// Parks a run: one protected statement executed, nothing committed.
    fn park(&self, sql: &str) -> ProtectedRun {
        let run = ProtectedRun { txn: self.db().begin(Isolation::Serializable), effects: Effects::default() };
        let mut s = self.sql();
        let p = s.prepare(sql).unwrap();
        let (run, result) = s.execute_protected_v2(&self.db(), run, &p, &[], None);
        result.unwrap_or_else(|e| panic!("{sql}: {e:?}"));
        run
    }
    fn commit(&self, run: ProtectedRun) -> Result<(), CoreError> {
        cross_check_effects(&run.effects, &run.txn)?;
        self.db().catalog().commit_ddl(run.txn).map(drop).map_err(CoreError::from)
    }
    fn statuses(&self) -> Vec<(i64, String)> {
        self.exec("SELECT id,status FROM shipments ORDER BY id").rows.into_iter().map(|r| match &r.values[..] {
            [Int(id), Text(status)] => (*id, status.to_string()),
            other => panic!("{other:?}"),
        }).collect()
    }
    fn create_role(&self, name: &str) {
        self.admin().roles().create(NewRole { name: name.into(), login: true, ..NewRole::default() }).unwrap();
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
/// The run reads every row (no index on merchant_id) and writes rows 1 and 3.
const SCAN_RUN: &str = "UPDATE shipments SET status='held' WHERE merchant_id=7";
fn unchanged() -> Vec<(i64, String)> { (1..=4).map(|id| (id, "new".to_string())).collect() }

/// Parks `SCAN_RUN`, commits `write` on a second session, then commits the run.
fn scan_run_against(f: &Fixture, write: &str) -> Result<(), CoreError> {
    let r = f.park(SCAN_RUN);
    f.exec(write);
    f.commit(r)
}
fn insert_into_scanned_range(f: &Fixture) -> CoreError {
    scan_run_against(f, "INSERT INTO shipments VALUES(5,7,'new','e@x')").unwrap_err()
}
fn update_of_read_row(f: &Fixture) -> CoreError {
    // Row 2 was read by the run's scan but not written by it.
    scan_run_against(f, "UPDATE shipments SET customer_email='z@x' WHERE id=2").unwrap_err()
}
fn delete_in_range(f: &Fixture) -> CoreError {
    scan_run_against(f, "DELETE FROM shipments WHERE id=4").unwrap_err()
}
fn grant_during_run(f: &Fixture) -> CoreError {
    f.create_role("clerk");
    let r = f.park(SCAN_RUN);
    f.admin().roles().grant_table("shipments", "clerk", &[TablePrivilege::Select]).unwrap();
    f.commit(r).unwrap_err()
}
fn revoke_during_run(f: &Fixture) -> CoreError {
    f.create_role("clerk");
    f.admin().roles().grant_table("shipments", "clerk", &[TablePrivilege::Select, TablePrivilege::Update]).unwrap();
    let r = f.park(SCAN_RUN);
    f.admin().roles().revoke_table("shipments", "clerk", &[TablePrivilege::Update]).unwrap();
    f.commit(r).unwrap_err()
}
fn overflow_insert(f: &Fixture) -> CoreError {
    let r = f.park(SCAN_RUN);
    f.exec("INSERT INTO shipments VALUES(5,7,'new','e@x')");
    // Further commits push any recent-log entries out.
    for n in 0..3 {
        f.exec(&format!("INSERT INTO shipments_churn VALUES({n})"));
    }
    f.commit(r).unwrap_err()
}
fn overflow_fixture() -> Fixture {
    let f = Fixture::with_recent_log(Some(0));
    f.exec("CREATE TABLE shipments_churn(n bigint PRIMARY KEY)");
    f
}
/// The run waits for a row lock held by an application transaction that then
/// commits a change to that row, so the row changed after the run's snapshot.
fn write_conflict(f: &Fixture) -> CoreError {
    let mut app = f.sql();
    run(&mut app, "BEGIN");
    run(&mut app, "UPDATE shipments SET status='picked' WHERE id=1");
    let (admin, db) = (f.admin().clone(), f.db());
    let (send, done) = mpsc::channel();
    let agent: JoinHandle<()> = thread::spawn(move || {
        let mut s = admin.sql_session();
        let run = ProtectedRun { txn: db.begin(Isolation::Serializable), effects: Effects::default() };
        let p = s.prepare(SCAN_RUN).unwrap();
        send.send(s.execute_protected_v2(&db, run, &p, &[], None).1.map(drop)).unwrap();
    });
    let until = Instant::now() + Duration::from_secs(10);
    while app.v2_transaction.as_ref().unwrap().sql_waiting_count() == 0 {
        assert!(done.try_recv().is_err(), "run did not wait for the row lock");
        assert!(Instant::now() < until, "run never entered a lock wait");
        thread::sleep(Duration::from_millis(1));
    }
    run(&mut app, "COMMIT");
    let result = done.recv_timeout(Duration::from_secs(20)).unwrap();
    agent.join().unwrap();
    result.unwrap_err()
}
fn assert_retry(error: &CoreError) {
    assert_eq!(*error, CoreError::Sql(SqlError::Conflict));
    assert_eq!(crate::sqlstate::sqlstate(error), "40001");
}

#[test]
fn rc_insert_into_scanned_range_aborts_commit() {
    let f = Fixture::new();
    assert_retry(&insert_into_scanned_range(&f));
    let mut expected = unchanged();
    expected.push((5, "new".into()));
    assert_eq!(f.statuses(), expected);
}

#[test]
fn rc_update_of_read_row_aborts_commit() {
    let f = Fixture::new();
    assert_retry(&update_of_read_row(&f));
    assert_eq!(f.statuses(), unchanged());
}

#[test]
fn rc_delete_in_range_aborts_commit() {
    let f = Fixture::new();
    assert_retry(&delete_in_range(&f));
    assert_eq!(f.statuses(), unchanged()[..3].to_vec());
}

#[test]
fn write_outside_scanned_pk_range_does_not_abort() {
    let f = Fixture::new();
    // A primary-key IN list reads two points. (A PK range predicate such as
    // `id>=1 AND id<=2` is not pushed down by v2 DML: it scans and tracks the
    // whole table, so any write to the table would abort the run.)
    let r = f.park("UPDATE shipments SET status='held' WHERE id IN (1,2)");
    assert_eq!(r.effects.len(), 2);
    assert!(r.txn.read_set().ranges.iter().all(|range| range.ks == Keyspace::Catalog));
    // Outside the run's primary keys: a new key above them and another row.
    f.exec("INSERT INTO shipments VALUES(10,7,'new','j@x')");
    f.exec("UPDATE shipments SET status='lost' WHERE id=4");
    f.commit(r).unwrap();
    assert_eq!(f.statuses(), vec![
        (1, "held".into()), (2, "held".into()), (3, "new".into()), (4, "lost".into()), (10, "new".into()),
    ]);
}

#[test]
fn grant_during_run_aborts_commit() {
    let f = Fixture::new();
    assert_retry(&grant_during_run(&f));
    assert_eq!(f.statuses(), unchanged());
    assert!(f.admin().roles().table_privileges("shipments", "clerk").unwrap().contains(&TablePrivilege::Select));
}

#[test]
fn revoke_during_run_aborts_commit() {
    let f = Fixture::new();
    assert_retry(&revoke_during_run(&f));
    assert_eq!(f.statuses(), unchanged());
    assert_eq!(
        f.admin().roles().table_privileges("shipments", "clerk").unwrap().into_iter().collect::<Vec<_>>(),
        vec![TablePrivilege::Select]
    );
}

#[test]
fn ddl_waits_for_run_and_run_commits() {
    let f = Fixture::new();
    let r = f.park(SCAN_RUN);
    let admin = f.admin().clone();
    let (send, done) = mpsc::channel();
    let ddl = thread::spawn(move || {
        send.send(admin.sql_session().execute_at("ALTER TABLE shipments ADD COLUMN note text", &[], None, &clock()).map(drop)).unwrap();
    });
    // The run holds the DML side of the catalog barrier until it ends.
    let until = Instant::now() + Duration::from_secs(10);
    while r.txn.sql_waiting_count() == 0 {
        assert!(done.try_recv().is_err(), "DDL did not wait for the run");
        assert!(Instant::now() < until, "DDL never entered a lock wait");
        thread::sleep(Duration::from_millis(1));
    }
    f.commit(r).unwrap();
    done.recv_timeout(Duration::from_secs(20)).unwrap().unwrap();
    ddl.join().unwrap();
    assert_eq!(f.statuses(), vec![(1, "held".into()), (2, "new".into()), (3, "held".into()), (4, "new".into())]);
    assert_eq!(f.exec("SELECT note FROM shipments WHERE id=1").rows[0].values, vec![relational::Value::Null]);
}

#[test]
fn ddl_committed_before_snapshot_is_seen() {
    let f = Fixture::new();
    // The run's transaction exists before the DDL commits, but its snapshot
    // is chosen after the statement's locks, so it sees the new column.
    let run = ProtectedRun { txn: f.db().begin(Isolation::Serializable), effects: Effects::default() };
    f.exec("ALTER TABLE shipments ADD COLUMN note text");
    let mut s = f.sql();
    let p = s.prepare("UPDATE shipments SET note='fragile' WHERE id=2").unwrap();
    let (run, result) = s.execute_protected_v2(&f.db(), run, &p, &[], None);
    assert_eq!(result.unwrap().affected, 1);
    // The DDL committed before the snapshot, so it is no conflict either.
    f.commit(run).unwrap();
    assert_eq!(
        f.exec("SELECT id,note FROM shipments WHERE note IS NOT NULL").rows[0].values,
        vec![Int(2), Text("fragile".into())]
    );
}

#[test]
fn log_overflow_uses_durable_fallback() {
    // Conflicting insert: the recent log holds nothing, the fallback finds it.
    let f = overflow_fixture();
    assert_retry(&overflow_insert(&f));
    // Writes elsewhere (another table) pass the same fallback validation.
    let f = overflow_fixture();
    let r = f.park(SCAN_RUN);
    for n in 0..3 {
        f.exec(&format!("INSERT INTO shipments_churn VALUES({n})"));
    }
    f.commit(r).unwrap();
    assert_eq!(f.statuses(), vec![(1, "held".into()), (2, "new".into()), (3, "held".into()), (4, "new".into())]);
}

#[test]
fn all_failures_map_to_sqlstate_40001() {
    let errors = [
        insert_into_scanned_range(&Fixture::new()),
        update_of_read_row(&Fixture::new()),
        delete_in_range(&Fixture::new()),
        grant_during_run(&Fixture::new()),
        revoke_during_run(&Fixture::new()),
        overflow_insert(&overflow_fixture()),
        write_conflict(&Fixture::new()),
    ];
    for error in &errors {
        assert_retry(error);
    }
    // The engine reasons behind them, and the task layer's retry error.
    for reason in [engine::txn::SerializationReason::ReadWriteConflict, engine::txn::SerializationReason::WriteConflict] {
        let error = engine::txn::TxnError::Serialization(reason);
        assert_eq!(error.sqlstate(), "40001");
        assert_eq!(crate::sqlstate::sqlstate(&CoreError::from(error)), "40001");
    }
    assert_eq!(crate::task::error::TaskError::Retry.sqlstate(), "40001");
}
