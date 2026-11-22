//! R3-608 (GD-0061 C3): the FK snapshot fence passes for a protected run,
//! whose snapshot is restarted after the Share locks on every FK-connected
//! table; a plain non-RC transaction keeps the RI crosscheck path.
use super::*;
use crate::task::effects::Effects;
use relational::Value::{Int, Text};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

struct Fixture { admin: Option<Admin>, path: std::path::PathBuf }
impl Fixture {
    fn new() -> Self {
        let mut nonce = [0; 16];
        getrandom::fill(&mut nonce).unwrap();
        let path = std::env::temp_dir().join(format!("r3fk-{:032x}", u128::from_be_bytes(nonce)));
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
    fn table_id(&self, name: &str) -> u64 {
        let db = self.db();
        let mut txn = db.begin(Isolation::ReadCommitted);
        db.catalog().snapshot(&mut txn).unwrap().parts.tables[name].id
    }
    fn rows(&self, sql: &str) -> Vec<Vec<relational::Value>> {
        run(&mut self.sql(), sql).rows.into_iter().map(|r| r.values).collect()
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
fn protected(f: &Fixture, s: &mut SqlSession, run: ProtectedRun, sql: &str)
    -> (ProtectedRun, Result<relational::SqlResult, CoreError>)
{
    let p = s.prepare(sql).unwrap();
    s.execute_protected_v2(&f.db(), run, &p, &[], None)
}
/// `deliveries` is reserved, so the brief's schema uses `shipments`.
fn setup() -> Fixture {
    let f = Fixture::new();
    f.exec("CREATE TABLE merchants(id bigint PRIMARY KEY,name text)");
    f.exec("CREATE TABLE shipments(id bigint PRIMARY KEY,merchant_id bigint REFERENCES merchants(id),n bigint)");
    f.exec("CREATE TABLE shipment_events(id bigint PRIMARY KEY,\
            shipment_id bigint REFERENCES shipments(id) ON UPDATE CASCADE,note text)");
    f.exec("INSERT INTO merchants VALUES(1,'a'),(2,'b')");
    f.exec("INSERT INTO shipments VALUES(1,1,0),(2,1,0)");
    f.exec("INSERT INTO shipment_events VALUES(501,1,'x'),(502,1,'y'),(503,2,'z')");
    f
}
fn wait_for_waiter(txn: &Transaction, done: &mpsc::Receiver<impl std::fmt::Debug>, what: &str) {
    let until = Instant::now() + Duration::from_secs(10);
    while txn.sql_waiting_count() == 0 {
        assert!(done.try_recv().is_err(), "{what} did not wait for the table lock");
        assert!(Instant::now() < until, "{what} never entered a lock wait");
        thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn protected_update_on_table_with_parent_fk_succeeds() {
    let f = setup();
    let mut s = f.sql();
    let (r, result) = protected(&f, &mut s, f.run(), "UPDATE shipments SET n=n+1, merchant_id=2 WHERE id=1");
    assert_eq!(result.unwrap().affected, 1);
    assert!(r.txn.snapshot_restarted());
    assert_eq!(r.effects.len(), 1);
    let e = r.effects.iter().next().unwrap();
    assert_eq!(e.table, f.table_id("shipments"));
    assert_eq!(e.new.as_ref().unwrap().values, vec![Int(1), Int(2), Int(1)]);
    cross_check_effects(&r.effects, &r.txn).unwrap();
    // Not committed: another session still sees the old row.
    assert_eq!(f.rows("SELECT * FROM shipments WHERE id=1"), vec![vec![Int(1), Int(1), Int(0)]]);
    // Inserting a child of an existing parent passes the fence too.
    let (r, result) = protected(&f, &mut s, f.run(), "INSERT INTO shipments VALUES(3,2,0)");
    assert_eq!(result.unwrap().affected, 1);
    cross_check_effects(&r.effects, &r.txn).unwrap();
}

#[test]
fn fk_violation_still_reported() {
    let f = setup();
    let mut s = f.sql();
    for sql in [
        "UPDATE shipments SET merchant_id=99 WHERE id=1",
        "INSERT INTO shipment_events VALUES(9,77,'q')",
        // NO ACTION parent delete with live children.
        "DELETE FROM merchants WHERE id=1",
    ] {
        let (r, result) = protected(&f, &mut s, f.run(), sql);
        assert_eq!(result.unwrap_err(), CoreError::Sql(SqlError::ForeignKeyViolation), "{sql}");
        assert_eq!(r.txn.written_keys(Keyspace::Rows).count(), 0, "{sql}");
        assert!(r.effects.is_empty(), "{sql}");
    }
    assert_eq!(f.rows("SELECT count(*) FROM merchants"), vec![vec![Int(2)]]);
}

#[test]
fn cascade_child_rows_appear_in_effects() {
    let f = setup();
    let mut s = f.sql();
    let (r, result) = protected(&f, &mut s, f.run(), "UPDATE shipments SET id=101 WHERE id=1");
    assert_eq!(result.unwrap().affected, 1);
    cross_check_effects(&r.effects, &r.txn).unwrap();
    let events = f.table_id("shipment_events");
    let mut cascaded = r.effects.iter().filter(|e| e.table == events)
        .map(|e| (e.old.as_ref().unwrap().values.clone(), e.new.as_ref().unwrap().values.clone()))
        .collect::<Vec<_>>();
    cascaded.sort_by_key(|(old, _)| format!("{old:?}"));
    assert_eq!(cascaded, vec![
        (vec![Int(501), Int(1), Text("x".into())], vec![Int(501), Int(101), Text("x".into())]),
        (vec![Int(502), Int(1), Text("y".into())], vec![Int(502), Int(101), Text("y".into())]),
    ]);
    // The parent's key change is in the effects too (old key gone, new key present).
    let shipments = f.table_id("shipments");
    let parent = r.effects.iter().filter(|e| e.table == shipments).collect::<Vec<_>>();
    assert!(parent.iter().any(|e| e.new.as_ref().is_some_and(|n| n.values[0] == Int(101))));
    assert!(parent.iter().all(|e| e.new.as_ref().is_none_or(|n| n.values[0] != Int(1))));
    // Nothing committed.
    assert_eq!(f.rows("SELECT shipment_id FROM shipment_events WHERE id=501"), vec![vec![Int(1)]]);
}

#[test]
fn parent_deleted_before_snapshot_is_seen() {
    let f = setup();
    let mut blocker = f.sql();
    run(&mut blocker, "BEGIN");
    run(&mut blocker, "DELETE FROM merchants WHERE id=2");
    // The run's first snapshot is older than the delete's commit: it still
    // sees merchant 2. Only the restarted snapshot can reject the reference.
    let mut early = f.run();
    early.txn.snapshot_ts();
    let admin = f.admin().clone();
    let db = f.db();
    let (send, done) = mpsc::channel();
    let worker = thread::spawn(move || {
        let mut s = admin.sql_session();
        let p = s.prepare("UPDATE shipments SET merchant_id=2 WHERE id=1").unwrap();
        let (r, result) = s.execute_protected_v2(&db, early, &p, &[], None);
        send.send((result.map(|x| x.affected), r.txn.written_keys(Keyspace::Rows).count(),
            r.effects.len())).unwrap();
    });
    wait_for_waiter(blocker.v2_transaction.as_ref().unwrap(), &done, "protected run");
    run(&mut blocker, "COMMIT");
    let (result, written, effects) = done.recv_timeout(Duration::from_secs(20)).unwrap();
    worker.join().unwrap();
    assert_eq!(result.unwrap_err(), CoreError::Sql(SqlError::ForeignKeyViolation));
    assert_eq!((written, effects), (0, 0));
    assert_eq!(f.rows("SELECT merchant_id FROM shipments WHERE id=1"), vec![vec![Int(1)]]);
}

#[test]
fn writer_of_connected_table_waits_for_share_lock() {
    let f = setup();
    let mut s = f.sql();
    let (r, result) = protected(&f, &mut s, f.run(), "UPDATE shipments SET n=5 WHERE id=2");
    assert_eq!(result.unwrap().affected, 1);
    // The run keeps Share on merchants (parent) and shipment_events (child)
    // until it ends, so a writer of either waits for it.
    let (send, done) = mpsc::channel();
    let writers = ["INSERT INTO merchants VALUES(3,'c')", "INSERT INTO shipment_events VALUES(504,2,'w')"]
        .map(|sql| {
            let (admin, send) = (f.admin().clone(), send.clone());
            thread::spawn(move || {
                let mut w = admin.sql_session();
                send.send(w.execute_at(sql, &[], None, &clock()).map(|x| x.affected)).unwrap();
            })
        });
    let until = Instant::now() + Duration::from_secs(10);
    while r.txn.sql_waiting_count() < 2 {
        assert!(done.try_recv().is_err(), "a connected-table writer did not wait for Share");
        assert!(Instant::now() < until, "connected-table writers never entered a lock wait");
        thread::sleep(Duration::from_millis(1));
    }
    thread::sleep(Duration::from_millis(50));
    assert!(done.try_recv().is_err(), "a writer ran while the protected run held Share");
    // Ending the run releases both writers; the run itself committed nothing.
    drop(r);
    for _ in 0..2 {
        assert_eq!(done.recv_timeout(Duration::from_secs(20)).unwrap().unwrap(), 1);
    }
    for w in writers {
        w.join().unwrap();
    }
    assert_eq!(f.rows("SELECT n FROM shipments WHERE id=2"), vec![vec![Int(0)]]);
    assert_eq!(f.rows("SELECT count(*) FROM shipment_events"), vec![vec![Int(4)]]);
}

#[test]
fn plain_serializable_without_restart_uses_snapshot_crosscheck() {
    // A plain REPEATABLE READ / SERIALIZABLE transaction never restarts its
    // snapshot. Since GD-0075 R5-76 its FK checks no longer fail with 40001
    // outright: they take PostgreSQL's RI crosscheck path (fk_lookup), which
    // fails only when a witness changed after the snapshot.
    let f = setup();
    for isolation in [Isolation::RepeatableRead, Isolation::Serializable] {
        let mut s = f.sql();
        run(&mut s, "BEGIN");
        s.v2_transaction = Some(f.db().begin(isolation));
        assert_eq!(
            s.execute_at("UPDATE shipments SET n=1 WHERE id=1", &[], None, &clock()).unwrap().affected,
            1
        );
        assert!(!s.v2_transaction.as_ref().unwrap().snapshot_restarted());
        run(&mut s, "ROLLBACK");
    }
    assert_eq!(f.rows("SELECT n FROM shipments WHERE id=1"), vec![vec![Int(0)]]);
}
