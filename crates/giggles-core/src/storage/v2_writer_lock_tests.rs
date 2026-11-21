//! F6: transaction-held writer locks and READ COMMITTED wait/re-read behavior.
use super::*;
use relational::Value::{Int, Text};
use std::sync::{Barrier, mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

struct Fixture { admin: Option<Admin>, path: std::path::PathBuf }
impl Fixture {
    fn new() -> Self {
        let mut nonce = [0; 16];
        getrandom::fill(&mut nonce).unwrap();
        let path = std::env::temp_dir().join(format!("f6-{:032x}", u128::from_be_bytes(nonce)));
        let admin = Some(Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap());
        Self { admin, path }
    }
    fn admin(&self) -> &Admin { self.admin.as_ref().unwrap() }
    fn sql(&self) -> SqlSession { self.admin().sql_session() }
    fn exec(&self, sql: &str) { run(&mut self.sql(), sql); }
}
impl Drop for Fixture {
    fn drop(&mut self) { self.admin.take(); fs::remove_dir_all(&self.path).unwrap(); }
}
fn run(s: &mut SqlSession, sql: &str) -> relational::SqlResult {
    s.execute_at(sql, &[], None, &clock()).unwrap_or_else(|e| panic!("{sql}: {e:?}"))
}
fn clock() -> relational::StatementClock {
    relational::StatementClock { unavailable: None, transaction_us: 1, statement_us: 1, zone: None, wall: None }
}
struct Pending { done: mpsc::Receiver<Result<relational::SqlResult, CoreError>>, worker: JoinHandle<()> }
impl Pending {
    fn start(f: &Fixture, sql: &str) -> Self {
        Self::start_expected(f, sql, None, "")
    }
    fn start_expected(f: &Fixture, sql: &str, expected: Option<u64>, begin: &str) -> Self {
        let admin = f.admin().clone();
        let begin = begin.to_owned();
        let sql = sql.to_owned();
        let (send, done) = mpsc::channel();
        let worker = thread::spawn(move || { let mut s = admin.sql_session();
            if !begin.is_empty() { begin_isolation(&mut s, &begin); }
            let result = s.execute_at(&sql, &[], expected, &clock());
            if !begin.is_empty() && result.is_ok() { run(&mut s, "COMMIT"); }
            send.send(result).unwrap(); });
        Self { done, worker }
    }
    fn blocked(&self, holder: &SqlSession) {
        let until = Instant::now() + Duration::from_secs(10);
        loop {
            assert!(matches!(self.done.try_recv(), Err(mpsc::TryRecvError::Empty)), "writer completed before holder ended");
            if holder.v2_transaction.as_ref().unwrap().sql_waiting_count() > 0 { break; }
            assert!(Instant::now() < until, "writer never entered a lock wait");
            thread::sleep(Duration::from_millis(1));
        }
    }
    fn finish(self) -> Result<relational::SqlResult, CoreError> {
        let result = self.done.recv_timeout(Duration::from_secs(20)).unwrap();
        self.worker.join().unwrap();
        result
    }
}

#[test]
fn distinct_primary_keys_proceed_with_first_transaction_open() {
    let f = Fixture::new();
    f.exec("CREATE TABLE t(id bigint PRIMARY KEY,n bigint)");
    f.exec("INSERT INTO t VALUES(1,0),(2,0)");
    let mut a = f.sql();
    run(&mut a, "BEGIN");
    run(&mut a, "UPDATE t SET n=n+1 WHERE id=1");
    let second = Pending::start(&f, "UPDATE t SET n=n+1 WHERE id=2 RETURNING n").finish().unwrap();
    assert_eq!(second.rows[0].values, vec![Int(1)]);
    run(&mut a, "COMMIT");
}

#[test]
fn same_primary_key_waits_then_rereads_commit_or_rollback() {
    for commit in [false, true] {
        let f = Fixture::new();
        f.exec("CREATE TABLE t(id bigint PRIMARY KEY,n bigint)");
        f.exec("INSERT INTO t VALUES(1,0)");
        let mut a = f.sql();
        run(&mut a, "BEGIN");
        run(&mut a, "UPDATE t SET n=10 WHERE id=1");
        let second = Pending::start(&f, "UPDATE t SET n=n+1 WHERE id=1 RETURNING n");
        second.blocked(&a);
        run(&mut a, if commit { "COMMIT" } else { "ROLLBACK" });
        assert_eq!(second.finish().unwrap().rows[0].values, vec![Int(if commit { 11 } else { 1 })]);
    }
}

#[test]
fn point_and_scan_writers_exclude_each_other_in_both_orders() {
    for point_first in [false, true] {
        let f = Fixture::new();
        f.exec("CREATE TABLE t(id bigint PRIMARY KEY,n bigint)");
        f.exec("INSERT INTO t VALUES(1,0),(2,0)");
        let point = "UPDATE t SET n=n+1 WHERE id=1";
        let scan = "UPDATE t SET n=n+1 WHERE n>=0";
        let mut a = f.sql();
        run(&mut a, "BEGIN");
        run(&mut a, if point_first { point } else { scan });
        let second = Pending::start(&f, if point_first { scan } else { point });
        second.blocked(&a);
        run(&mut a, "COMMIT");
        second.finish().unwrap();
        assert_eq!(run(&mut f.sql(), "SELECT n FROM t ORDER BY id").rows.into_iter().map(|r| r.values).collect::<Vec<_>>(), vec![vec![Int(2)], vec![Int(1)]]);
    }
}

#[test]
fn concurrent_insert_pk_and_unique_wait_then_duplicate_or_success() {
    for unique in [false, true] {
        for commit in [false, true] {
            let f = Fixture::new();
            f.exec("CREATE TABLE t(id bigint PRIMARY KEY,u bigint UNIQUE)");
            let mut a = f.sql();
            run(&mut a, "BEGIN");
            run(&mut a, "INSERT INTO t VALUES(1,10)");
            let second = Pending::start(&f, if unique { "INSERT INTO t VALUES(2,10)" } else { "INSERT INTO t VALUES(1,20)" });
            second.blocked(&a);
            run(&mut a, if commit { "COMMIT" } else { "ROLLBACK" });
            let result = second.finish();
            if commit { assert_eq!(result, Err(CoreError::Sql(SqlError::Duplicate))); }
            else { assert_eq!(result.unwrap().affected, 1); }
        }
    }
}

#[test]
fn plain_values_defaults_allow_distinct_inserts_without_waiting() {
    let f = Fixture::new();
    f.exec("CREATE TABLE t(id bigserial PRIMARY KEY,u bigint UNIQUE)");
    let mut a = f.sql();
    run(&mut a, "BEGIN");
    run(&mut a, "INSERT INTO t(u) VALUES(10)");
    assert_eq!(Pending::start(&f, "INSERT INTO t(u) VALUES(20) RETURNING u").finish().unwrap().rows[0].values, vec![Int(20)]);
    let same_unique = Pending::start(&f, "INSERT INTO t(u) VALUES(10)");
    same_unique.blocked(&a);
    run(&mut a, "COMMIT");
    assert_eq!(same_unique.finish(), Err(CoreError::Sql(SqlError::Duplicate)));
}

#[test]
fn insert_waits_for_point_writer_to_release_pk_or_unique_value() {
    for unique in [false, true] {
        for commit in [false, true] {
            let f = Fixture::new();
            f.exec("CREATE TABLE t(id bigint PRIMARY KEY,u bigint UNIQUE)");
            f.exec("INSERT INTO t VALUES(1,10)");
            let mut a = f.sql();
            run(&mut a, "BEGIN");
            run(&mut a, if unique { "UPDATE t SET u=20 WHERE id=1" } else { "DELETE FROM t WHERE id=1" });
            let second = Pending::start(&f, if unique { "INSERT INTO t VALUES(2,10)" } else { "INSERT INTO t VALUES(1,20)" });
            second.blocked(&a);
            run(&mut a, if commit { "COMMIT" } else { "ROLLBACK" });
            let result = second.finish();
            if commit { assert_eq!(result.unwrap().affected, 1); }
            else { assert_eq!(result, Err(CoreError::Sql(SqlError::Duplicate))); }
        }
    }
}

#[test]
fn fk_parent_delete_child_insert_baseline() {
    for commit in [false, true] {
        let f = Fixture::new();
        f.exec("CREATE TABLE p(id bigint PRIMARY KEY)");
        f.exec("CREATE TABLE c(id bigint PRIMARY KEY,p bigint REFERENCES p(id))");
        f.exec("INSERT INTO p VALUES(1)");
        let mut a = f.sql();
        run(&mut a, "BEGIN");
        run(&mut a, "DELETE FROM p WHERE id=1");
        let child = Pending::start(&f, "INSERT INTO c VALUES(1,1)");
        child.blocked(&a);
        run(&mut a, if commit { "COMMIT" } else { "ROLLBACK" });
        let result = child.finish();
        if commit { assert_eq!(result, Err(CoreError::Sql(SqlError::ForeignKeyViolation))); }
        else { assert_eq!(result.unwrap().affected, 1); }
        println!("F6 FK baseline parent_commit={commit}: child waited; outcome={}", if commit { "23503" } else { "inserted" });
    }
}

#[test]
fn wide_point_fallback_and_returning_multi_keys_keep_exclusive() {
    let f = Fixture::new();
    f.exec("CREATE TABLE t(id bigint PRIMARY KEY,n bigint,payload text)");
    f.sql().execute("INSERT INTO t VALUES(1,0,$1),(2,0,$1),(3,0,$1),(4,0,$1),(5,0,$1),(6,0,$1),(7,0,'small')", &[Text("x".repeat(50_000))], None).unwrap();
    for sql in ["UPDATE t SET n=n+1 WHERE id IN(1,2,3,4,5,6)", "UPDATE t SET n=n+1 WHERE id IN(1,2) RETURNING n"] {
        let mut a = f.sql();
        run(&mut a, "BEGIN");
        run(&mut a, sql);
        let second = Pending::start(&f, "UPDATE t SET n=n+1 WHERE id=7");
        second.blocked(&a);
        run(&mut a, "ROLLBACK");
        second.finish().unwrap();
    }
}

#[test]
fn sentinel_upgrades_and_reverse_order_inside_one_transaction() {
    let f = Fixture::new();
    f.exec("CREATE TABLE t(id bigint PRIMARY KEY,n bigint)");
    f.exec("INSERT INTO t VALUES(1,0),(2,0)");
    for point_first in [false, true] {
        let mut a = f.sql();
        run(&mut a, "BEGIN");
        let point = "UPDATE t SET n=n+1 WHERE id=1";
        let scan = "UPDATE t SET n=n+1 WHERE n>=0";
        run(&mut a, if point_first { point } else { scan });
        run(&mut a, if point_first { scan } else { point });
        run(&mut a, "COMMIT");
    }
}

#[test]
fn eight_threads_distinct_updates_and_collisions_have_exact_totals() {
    let f = Fixture::new();
    f.exec("CREATE TABLE t(id bigint PRIMARY KEY,n bigint)");
    f.exec("INSERT INTO t VALUES(0,0),(1,0),(2,0),(3,0),(4,0),(5,0),(6,0),(7,0),(8,0)");
    let start = Arc::new(Barrier::new(8));
    let workers = (1..=8).map(|id| {
        let admin = f.admin().clone();
        let start = start.clone();
        thread::spawn(move || {
            let mut s = admin.sql_session();
            let p = s.prepare("UPDATE t SET n=n+1 WHERE id=$1").unwrap();
            start.wait();
            for i in 0..200 {
                s.execute_prepared(&p, &[Int(id)], None).unwrap();
                if i % 10 == 0 { s.execute_prepared(&p, &[Int(0)], None).unwrap(); }
            }
        })
    }).collect::<Vec<_>>();
    for worker in workers { worker.join().unwrap(); }
    let rows = run(&mut f.sql(), "SELECT id,n FROM t ORDER BY id").rows;
    assert_eq!(rows[0].values, vec![Int(0), Int(160)]);
    for (i, row) in rows.iter().enumerate().skip(1) { assert_eq!(row.values, vec![Int(i as i64), Int(200)]); }
}


fn begin_isolation(s: &mut SqlSession, level: &str) {
    s.set_isolation(match level {
        "BEGIN" | "READ COMMITTED" => crate::SqlIsolation::ReadCommitted,
        "REPEATABLE READ" => crate::SqlIsolation::RepeatableRead,
        "SERIALIZABLE" => crate::SqlIsolation::Serializable,
        _ => panic!("unexpected isolation: {level}"),
    });
    run(s, "BEGIN");
}
fn version(f: &Fixture, id: i64) -> u64 {
    run(&mut f.sql(), &format!("SELECT * FROM t WHERE id={id}")).rows[0].version
}
fn expected(s: &mut SqlSession, sql: &str, v: u64) -> Result<relational::SqlResult, CoreError> {
    s.execute_at(sql, &[], Some(v), &clock())
}

#[test]
fn expected_distinct_rc_writers_complete_while_holder_open() {
    for delete in [false, true] {
        let f = Fixture::new();
        f.exec("CREATE TABLE t(id bigint PRIMARY KEY,n bigint)");
        f.exec("INSERT INTO t VALUES(1,0),(2,0)");
        let (v1, v2) = (version(&f, 1), version(&f, 2));
        let mut a = f.sql();
        run(&mut a, "BEGIN");
        expected(&mut a, "UPDATE t SET n=10 WHERE id=1", v1).unwrap();
        let sql = if delete { "DELETE FROM t WHERE id=2 RETURNING n" }
            else { "UPDATE t SET n=20 WHERE id=2 RETURNING n" };
        let b = Pending::start_expected(&f, sql, Some(v2), "BEGIN").finish().unwrap();
        assert_eq!(b.affected, 1);
        assert_eq!(b.rows[0].values, vec![Int(if delete { 0 } else { 20 })]);
        run(&mut a, "COMMIT");
        let rows = run(&mut f.sql(), "SELECT * FROM t ORDER BY id").rows;
        assert_eq!(rows.iter().map(|r| r.values.clone()).collect::<Vec<_>>(),
            if delete { vec![vec![Int(1), Int(10)]] }
            else { vec![vec![Int(1), Int(10)], vec![Int(2), Int(20)]] });
    }
}

#[test]
fn expected_same_row_rc_waits_then_checks_commit_or_rollback() {
    for delete in [false, true] {
        for commit in [false, true] {
            let f = Fixture::new();
            f.exec("CREATE TABLE t(id bigint PRIMARY KEY,n bigint)");
            f.exec("INSERT INTO t VALUES(1,0)");
            let v = version(&f, 1);
            let mut a = f.sql();
            run(&mut a, "BEGIN");
            expected(&mut a, "UPDATE t SET n=10 WHERE id=1", v).unwrap();
            let sql = if delete { "DELETE FROM t WHERE id=1 RETURNING n" }
                else { "UPDATE t SET n=n+1 WHERE id=1 RETURNING n" };
            let b = Pending::start_expected(&f, sql, Some(v), "BEGIN");
            b.blocked(&a);
            run(&mut a, if commit { "COMMIT" } else { "ROLLBACK" });
            let result = b.finish();
            if commit { assert_eq!(result, Err(CoreError::Sql(SqlError::Version))); }
            else {
                let result = result.unwrap();
                assert_eq!(result.affected, 1);
                assert_eq!(result.rows[0].values, vec![Int(if delete { 0 } else { 1 })]);
            }
        }
    }
}

#[test]
fn expected_mismatch_and_missing_all_isolations() {
    for isolation in ["READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"] {
        for delete in [false, true] {
            for id in [1, 5] {
                let f = Fixture::new();
                f.exec("CREATE TABLE t(id bigint PRIMARY KEY,n bigint)");
                f.exec("INSERT INTO t VALUES(1,0)");
                let v = version(&f, 1) + 999;
                let mut s = f.sql();
                begin_isolation(&mut s, isolation);
                let sql = if delete { format!("DELETE FROM t WHERE id={id}") }
                    else { format!("UPDATE t SET n=1 WHERE id={id}") };
                assert_eq!(expected(&mut s, &sql, v), Err(CoreError::Sql(SqlError::Version)));
                run(&mut s, "ROLLBACK");
            }
        }
    }
}

#[test]
fn expected_missing_key_waits_for_insert_commit_or_rollback() {
    for delete in [false, true] {
        for commit in [false, true] {
            let f = Fixture::new();
            f.exec("CREATE TABLE t(id bigint PRIMARY KEY,n bigint)");
            let mut a = f.sql();
            run(&mut a, "BEGIN");
            run(&mut a, "INSERT INTO t VALUES(5,0)");
            let sql = if delete { "DELETE FROM t WHERE id=5" } else { "UPDATE t SET n=1 WHERE id=5" };
            let b = Pending::start_expected(&f, sql, Some(0), "BEGIN");
            b.blocked(&a);
            run(&mut a, if commit { "COMMIT" } else { "ROLLBACK" });
            assert_eq!(b.finish(), Err(CoreError::Sql(SqlError::Version)));
        }
    }
}

#[test]
fn expected_parent_cascade_serializes_child_insert() {
    for commit in [false, true] {
        let f = Fixture::new();
        f.exec("CREATE TABLE t(id bigint PRIMARY KEY)");
        f.exec("CREATE TABLE c(id bigint PRIMARY KEY,p bigint REFERENCES t(id) ON DELETE CASCADE)");
        f.exec("INSERT INTO t VALUES(1)");
        f.exec("INSERT INTO c VALUES(1,1)");
        let v = version(&f, 1);
        let mut a = f.sql();
        run(&mut a, "BEGIN");
        let before = FK_DELETE_SHARED.get();
        expected(&mut a, "DELETE FROM t WHERE id=1", v).unwrap();
        assert_eq!(FK_DELETE_SHARED.get(), before + 1);
        assert!(run(&mut a, "SELECT * FROM c").rows.is_empty());
        let b = Pending::start(&f, "INSERT INTO c VALUES(2,1)");
        b.blocked(&a);
        run(&mut a, if commit { "COMMIT" } else { "ROLLBACK" });
        if commit {
            assert_eq!(b.finish(), Err(CoreError::Sql(SqlError::ForeignKeyViolation)));
            assert!(run(&mut f.sql(), "SELECT * FROM c").rows.is_empty());
        } else {
            assert_eq!(b.finish().unwrap().affected, 1);
            assert_eq!(run(&mut f.sql(), "SELECT * FROM c ORDER BY id").rows.iter().map(|r| r.values.clone()).collect::<Vec<_>>(),
                vec![vec![Int(1), Int(1)], vec![Int(2), Int(1)]]);
        }
    }
}

#[test]
fn expected_rekey_and_nondeterministic_keys_keep_exclusive() {
    for collated in [false, true] {
        let f = Fixture::new();
        if collated {
            f.exec("CREATE COLLATION ci (provider=icu,locale='und-u-ks-level2',deterministic=false)");
            f.exec("CREATE TABLE t(id text COLLATE ci PRIMARY KEY,n bigint)");
            f.exec("INSERT INTO t VALUES('one',0),('two',0)");
        } else {
            f.exec("CREATE TABLE t(id bigint PRIMARY KEY,n bigint)");
            f.exec("INSERT INTO t VALUES(1,0),(2,0)");
        }
        let v = run(&mut f.sql(), "SELECT * FROM t ORDER BY id").rows[0].version;
        let mut a = f.sql();
        run(&mut a, "BEGIN");
        expected(&mut a, if collated { "UPDATE t SET n=1 WHERE id='one'" }
            else { "UPDATE t SET id=3 WHERE id=1" }, v).unwrap();
        let b = Pending::start(&f, if collated { "UPDATE t SET n=2 WHERE id='two'" }
            else { "UPDATE t SET n=2 WHERE id=2" });
        b.blocked(&a);
        run(&mut a, "ROLLBACK");
        b.finish().unwrap();
    }
}

#[test]
fn expected_fixed_snapshot_same_row_race_remains_conflict() {
    for isolation in ["REPEATABLE READ", "SERIALIZABLE"] {
        for delete in [false, true] {
            let f = Fixture::new();
            f.exec("CREATE TABLE t(id bigint PRIMARY KEY,n bigint)");
            f.exec("INSERT INTO t VALUES(1,0)");
            let v = version(&f, 1);
            let mut a = f.sql();
            run(&mut a, "BEGIN");
            run(&mut a, "UPDATE t SET n=10 WHERE id=1");
            let sql = if delete { "DELETE FROM t WHERE id=1" } else { "UPDATE t SET n=1 WHERE id=1" };
            let b = Pending::start_expected(&f, sql, Some(v), isolation);
            b.blocked(&a);
            run(&mut a, "COMMIT");
            assert_eq!(b.finish(), Err(CoreError::Sql(SqlError::Conflict)));
        }
    }
}

#[test]
fn expected_sixteen_threads_distinct_updates_have_exact_totals() {
    let f = Fixture::new();
    f.exec("CREATE TABLE t(id bigint PRIMARY KEY,n bigint)");
    for id in 1..=16 { f.exec(&format!("INSERT INTO t VALUES({id},0)")); }
    let start = Arc::new(Barrier::new(16));
    let workers = (1..=16).map(|id| {
        let admin = f.admin().clone();
        let start = start.clone();
        thread::spawn(move || {
            let mut s = admin.sql_session();
            start.wait();
            for _ in 0..50 {
                let v = run(&mut s, &format!("SELECT * FROM t WHERE id={id}")).rows[0].version;
                assert_eq!(expected(&mut s, &format!("UPDATE t SET n=n+1 WHERE id={id}"), v).unwrap().affected, 1);
            }
        })
    }).collect::<Vec<_>>();
    for worker in workers { worker.join().unwrap(); }
    let rows = run(&mut f.sql(), "SELECT * FROM t ORDER BY id").rows;
    assert_eq!(rows.len(), 16);
    for (i, row) in rows.iter().enumerate() { assert_eq!(row.values, vec![Int(i as i64 + 1), Int(50)]); }
}

// Exercise the exact Prepared::*_engine statements used by native operations.
fn native(s: &mut SqlSession, table: &TableDef, id: i64, version: u64, delete: bool)
    -> Result<relational::SqlResult, CoreError> {
    if delete { s.delete_engine(table, vec![Int(id)], version) }
    else { s.update_engine(table, vec![Int(id)], vec![(1, Int(10))], version) }
}
impl Pending {
    fn native(f: &Fixture, table: TableDef, id: i64, version: u64, delete: bool, level: &str) -> Self {
        let admin = f.admin().clone();
        let disabled = DISABLE_MUTATION_POINTS.get();
        let level = level.to_owned();
        let (send, done) = mpsc::channel();
        let worker = thread::spawn(move || {
            DISABLE_MUTATION_POINTS.set(disabled);
            let mut s = admin.sql_session();
            begin_isolation(&mut s, &level);
            let result = native(&mut s, &table, id, version, delete);
            if result.is_ok() { run(&mut s, "COMMIT"); }
            else { run(&mut s, "ROLLBACK"); }
            send.send(result).unwrap();
        });
        Self { done, worker }
    }
}
#[test]
fn native_distinct_update_and_delete_hold_locks_concurrently() {
    for first_delete in [false, true] {
        for second_delete in [false, true] {
            let f = Fixture::new();
            f.exec("CREATE TABLE t(id bigint PRIMARY KEY,n bigint)");
            f.exec("INSERT INTO t VALUES(1,0),(2,0)");
            let table = f.sql().catalog_engine().unwrap().1.remove(0);
            let (v1, v2) = (version(&f, 1), version(&f, 2));
            let mut a = f.sql(); run(&mut a, "BEGIN");
            native(&mut a, &table, 1, v1, first_delete).unwrap();
            assert_eq!(Pending::native(&f, table, 2, v2, second_delete, "READ COMMITTED")
                .finish().unwrap().affected, 1);
            run(&mut a, "ROLLBACK");
        }
    }
}
#[test]
fn native_same_row_and_missing_versions_match_exclusive_per_isolation() {
    for level in ["READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"] {
        for delete in [false, true] {
            // Update, delete, and delete/reinsert are separate version histories.
            for history in [0, 1, 2, 3] {
                for commit in [false, true] {
                    let mut outcomes = Vec::new();
                    for disabled in [true, false] {
                        DISABLE_MUTATION_POINTS.set(disabled);
                        let f = Fixture::new();
                        f.exec("CREATE TABLE t(id bigint PRIMARY KEY,n bigint)");
                        if history != 3 { f.exec("INSERT INTO t VALUES(1,0)"); }
                        let table = f.sql().catalog_engine().unwrap().1.remove(0);
                        let v = if history == 3 { 1 } else { version(&f, 1) };
                        let mut a = f.sql(); run(&mut a, "BEGIN");
                        if history == 0 { native(&mut a, &table, 1, v, false).unwrap(); }
                        else if history != 3 {
                            native(&mut a, &table, 1, v, true).unwrap();
                            if history == 2 { run(&mut a, "INSERT INTO t VALUES(1,20)"); }
                        }
                        let b = Pending::native(&f, table, 1, v, delete, level);
                        if history != 3 { b.blocked(&a); }
                        run(&mut a, if commit { "COMMIT" } else { "ROLLBACK" });
                        let result = b.finish();
                        if history == 3 || commit && level == "READ COMMITTED" {
                            assert_eq!(result, Err(CoreError::Sql(SqlError::Version)));
                        }
                        outcomes.push(result.map(|r| r.affected));
                    }
                    assert_eq!(outcomes[0], outcomes[1], "{level}, delete={delete}, history={history}, commit={commit}");
                }
            }
        }
    }
    DISABLE_MUTATION_POINTS.set(false);
}
#[test]
fn native_reverse_order_deadlock_rolls_back_victim() {
    let f = Fixture::new();
    f.exec("CREATE TABLE t(id bigint PRIMARY KEY,n bigint)");
    f.exec("INSERT INTO t VALUES(1,0),(2,0)");
    let table = f.sql().catalog_engine().unwrap().1.remove(0);
    let versions = [version(&f, 1), version(&f, 2)];
    let barrier = std::sync::Arc::new(Barrier::new(2));
    let workers = [false, true].map(|reverse| {
        let admin = f.admin().clone(); let table = table.clone(); let barrier = barrier.clone();
        thread::spawn(move || {
            let mut s = admin.sql_session(); run(&mut s, "BEGIN");
            let ids = if reverse { [2,1] } else { [1,2] };
            s.update_engine(&table, vec![Int(ids[0])], vec![(1, Int(if reverse {20} else {10}))], versions[ids[0] as usize-1]).unwrap();
            barrier.wait();
            let v = s.row_version_engine(&table, &[Int(ids[1])]).unwrap();
            let result = s.update_engine(&table, vec![Int(ids[1])], vec![(1, Int(if reverse {20} else {10}))], v);
            if result.is_ok() { run(&mut s, "COMMIT"); } else { run(&mut s, "ROLLBACK"); }
            result.map(|_| if reverse {20} else {10})
        })
    });
    let results = workers.map(|w| w.join().unwrap());
    assert_eq!(results.iter().filter(|r| **r == Err(CoreError::Deadlock)).count(), 1);
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(run(&mut f.sql(), "SELECT n FROM t ORDER BY id").rows.into_iter()
        .map(|r| r.values).collect::<Vec<_>>(), vec![vec![Int(*results.iter().find_map(|r| r.as_ref().ok()).unwrap())];2]);
}

#[test]
fn native_unique_delete_reuse_matches_exclusive() {
    for commit in [false,true] {
        let mut outcomes = Vec::new();
        for disabled in [true,false] {
            DISABLE_MUTATION_POINTS.set(disabled);
            let f=Fixture::new(); f.exec("CREATE TABLE t(id bigint PRIMARY KEY,n bigint UNIQUE)");
            f.exec("INSERT INTO t VALUES(1,10)");
            let table=f.sql().catalog_engine().unwrap().1.remove(0);
            let mut a=f.sql(); run(&mut a,"BEGIN");
            native(&mut a,&table,1,version(&f,1),true).unwrap();
            let b=Pending::start(&f,"INSERT INTO t VALUES(2,10)"); b.blocked(&a);
            run(&mut a,if commit {"COMMIT"} else {"ROLLBACK"});
            outcomes.push(b.finish().map(|r|r.affected));
        }
        assert_eq!(outcomes[0],outcomes[1]);
    }
    DISABLE_MUTATION_POINTS.set(false);
}
#[test]
fn native_fk_races_match_exclusive() {
    fn scenario(action: &str, child_first: bool, commit: bool, disabled: bool) -> String {
        DISABLE_MUTATION_POINTS.set(disabled);
        let f=Fixture::new();
        f.exec("CREATE TABLE p(id bigint PRIMARY KEY,n bigint)");
        f.exec(&format!("CREATE TABLE c(id bigint PRIMARY KEY,p bigint REFERENCES p(id) ON DELETE {action})"));
        f.exec("INSERT INTO p VALUES(1,0),(2,0)"); f.exec("INSERT INTO c VALUES(1,2)");
        let tables=f.sql().catalog_engine().unwrap().1;
        let parent=tables.iter().find(|t|t.name=="p").unwrap().clone();
        let child=tables.iter().find(|t|t.name=="c").unwrap().clone();
        let pv=run(&mut f.sql(),"SELECT * FROM p WHERE id=1").rows[0].version;
        let cv=run(&mut f.sql(),"SELECT * FROM c WHERE id=1").rows[0].version;
        let mut a=f.sql(); run(&mut a,"BEGIN");
        // Both native operations use their exact engine-built predicate shape.
        if child_first { a.update_engine(&child,vec![Int(1)],vec![(1,Int(1))],cv).unwrap(); }
        else { native(&mut a,&parent,1,pv,true).unwrap(); }
        let admin=f.admin().clone();
        let (send,done)=mpsc::channel();
        let worker=thread::spawn(move || {
            DISABLE_MUTATION_POINTS.set(disabled);
            let mut s=admin.sql_session(); run(&mut s,"BEGIN");
            let result=if child_first { native(&mut s,&parent,1,pv,true) }
                else { s.update_engine(&child,vec![Int(1)],vec![(1,Int(1))],cv) };
            if result.is_ok() {run(&mut s,"COMMIT");} else {run(&mut s,"ROLLBACK");}
            send.send(result).unwrap();
        });
        let b=Pending{done,worker}; b.blocked(&a);
        run(&mut a,if commit {"COMMIT"}else{"ROLLBACK"});
        let outcome=b.finish().map(|r|r.affected);
        let parents=run(&mut f.sql(),"SELECT * FROM p ORDER BY id").rows.into_iter().map(|r|r.values).collect::<Vec<_>>();
        let children=run(&mut f.sql(),"SELECT * FROM c ORDER BY id").rows.into_iter().map(|r|r.values).collect::<Vec<_>>();
        format!("{outcome:?};{parents:?};{children:?}")
    }
    for action in ["RESTRICT","NO ACTION","CASCADE","SET NULL"] {
        for child_first in [false,true] { for commit in [false,true] {
            assert_eq!(scenario(action,child_first,commit,false),scenario(action,child_first,commit,true),
                "{action}, child_first={child_first}, commit={commit}");
        }}
    }
    DISABLE_MUTATION_POINTS.set(false);
}

#[test]
fn native_late_failures_roll_back_insert_many_mutation_and_mixed_upsert() {
    use crate::operations::{self as o, Request, ValueExpr, Expr, Intent, Bindings, ConflictAction};
    let lit = |v| ValueExpr::Literal(Int(v));
    let expr = |v| Expr::Value(lit(v));
    for unique in [false,true] {
        let f=Fixture::new(); f.exec("CREATE TABLE t(id bigint PRIMARY KEY,n bigint UNIQUE)");
        f.exec("INSERT INTO t VALUES(1,10),(2,20),(9,90)");
        let engine=o::Engine::new(f.admin().clone());
        let table=f.sql().catalog_engine().unwrap().1.remove(0);
        let mut a=f.sql(); run(&mut a,"BEGIN");
        a.update_engine(&table,vec![Int(9)],vec![(1,Int(91))],version(&f,9)).unwrap();
        let request=Request::InsertMany {table:"t".into(),columns:vec!["id".into(),"n".into()],
            rows:vec![vec![expr(3),expr(30)],vec![expr(if unique {4}else{1}),expr(if unique {10}else{40})]],cap:lit(2)};
        let admin=f.admin().clone();
        let worker=thread::spawn(move || o::Engine::new(admin).execute(&request,&Bindings::new()));
        run(&mut a,"ROLLBACK");
        assert_eq!(worker.join().unwrap().unwrap_err(), o::Error::Duplicate);
        assert_eq!(run(&mut f.sql(),"SELECT * FROM t WHERE id=3").rows.len(),0);
        let request=Request::Update {table:"t".into(),filter:Expr::Value(ValueExpr::Literal(relational::Value::Bool(true))),
            guard:None,values:vec![("n".into(),lit(30))],intent:Intent::Many{cap:lit(3)}};
        assert_eq!(engine.execute(&request,&Bindings::new()).unwrap_err(), o::Error::Duplicate);
        let request=Request::OnConflict {request:Box::new(Request::InsertMany {table:"t".into(),
            columns:vec!["id".into(),"n".into()],rows:vec![vec![expr(3),expr(30)],vec![expr(1),expr(11)],vec![expr(4),expr(20)]],cap:lit(3)}),
            target:vec!["id".into()],action:ConflictAction::Update {guard:None,values:vec![("n".into(),expr(11))]}};
        assert_eq!(engine.execute(&request,&Bindings::new()).unwrap_err(), o::Error::Duplicate);
        assert_eq!(run(&mut f.sql(),"SELECT id,n FROM t ORDER BY id").rows.into_iter().map(|r|r.values).collect::<Vec<_>>(),
            vec![vec![Int(1),Int(10)],vec![Int(2),Int(20)],vec![Int(9),Int(90)]]);
    }
}

#[test]
fn native_many_distinct_keys_remain_unblocked() {
    use crate::operations::{Bindings, Expr, Request, ValueExpr};
    let literal = |n| Expr::Value(ValueExpr::Literal(Int(n)));
    for insert in [false, true] {
        let f = Fixture::new(); f.exec("CREATE TABLE t(id bigint PRIMARY KEY,n bigint)");
        f.exec("INSERT INTO t VALUES(9999,0)");
        if !insert { f.exec("INSERT INTO t VALUES(1,0),(2,0)"); }
        let mut a = f.sql(); run(&mut a,"BEGIN");
        let request = if insert {
            Request::InsertMany { table: "t".into(), columns: vec!["id".into(),"n".into()],
                rows: (1..=201).rev().map(|id|vec![literal(id),literal(0)]).collect(),
                cap: ValueExpr::Literal(Int(201)) }
        } else {
            Request::Delete { table: "t".into(), filter: Expr::Compare(
                Box::new(Expr::Field("id".into())), crate::operations::Compare::Lt, Box::new(literal(3))),
                guard: None, cap: Some(ValueExpr::Literal(Int(2))) }
        };
        assert_eq!(a.execute_native(&request,&Bindings::new()).unwrap().affected,if insert {201} else {2});
        assert_eq!(Pending::start(&f,"UPDATE t SET n=1 WHERE id=9999").finish().unwrap().affected,1);
        let blocked = Pending::start(&f,"INSERT INTO t VALUES(1,10)");
        blocked.blocked(&a);
        run(&mut a,"ROLLBACK");
        let result = blocked.finish();
        if insert { assert!(result.is_ok()); } else { assert!(result.is_err()); }
    }
}

#[test]
fn native_many_stale_version_aborts_all_targets() {
    for missing in [false,true] {
        let f = Fixture::new(); f.exec("CREATE TABLE t(id bigint PRIMARY KEY,n bigint)");
        f.exec("CREATE TABLE c(id bigint PRIMARY KEY,p bigint REFERENCES t(id) ON DELETE RESTRICT)");
        f.exec("INSERT INTO t VALUES(1,0),(2,0)"); f.exec("INSERT INTO c VALUES(10,1)");
        let table = f.sql().catalog_engine().unwrap().1.into_iter().find(|t|t.name=="t").unwrap();
        let targets = vec![(vec![Int(1)],version(&f,1)),(vec![Int(2)],version(&f,2))];
        f.exec(if missing { "DELETE FROM t WHERE id=2" } else { "UPDATE t SET n=1 WHERE id=2" });
        let mut a=f.sql(); run(&mut a,"BEGIN");
        assert_eq!(a.delete_many_engine(&table,targets),Err(CoreError::Sql(SqlError::Version)));
        run(&mut a,"ROLLBACK");
        assert_eq!(run(&mut f.sql(),"SELECT * FROM t WHERE id=1").rows.len(),1);
        assert_eq!(run(&mut f.sql(),"SELECT * FROM c").rows.len(),1);
    }
}

#[test]
fn native_many_fixed_snapshot_conflicts_match_sql() {
    use crate::operations::{Bindings,Expr,Request,ValueExpr};
    for level in ["REPEATABLE READ","SERIALIZABLE"] {
        let mut outcomes = Vec::new();
        for native in [false,true] {
            let f=Fixture::new(); f.exec("CREATE TABLE t(id bigint PRIMARY KEY,n bigint)");
            f.exec("INSERT INTO t VALUES(1,0),(2,0)");
            let mut a=f.sql(); begin_isolation(&mut a,level);
            run(&mut a,"SELECT * FROM t");
            f.exec("UPDATE t SET n=1 WHERE id=2");
            let state=if native {
                let request=Request::Delete { table:"t".into(),filter:Expr::Value(ValueExpr::Literal(relational::Value::Bool(true))),
                    guard:None,cap:Some(ValueExpr::Literal(Int(2))) };
                a.execute_native(&request,&Bindings::new()).unwrap_err()
            } else {
                match a.execute("DELETE FROM t WHERE id IN (1,2)",&[],None).unwrap_err() {
                    CoreError::Sql(SqlError::Conflict) => crate::operations::Error::Conflict,
                    error => crate::operations::Error::Sql { sqlstate:crate::sqlstate(&error).into() },
                }
            };
            outcomes.push(state);
            run(&mut a,"ROLLBACK");
            assert_eq!(run(&mut f.sql(),"SELECT * FROM t").rows.len(),2);
        }
        assert_eq!(outcomes[0], outcomes[1], "{level}");
        assert_eq!(outcomes[0], crate::operations::Error::Conflict);
    }
}

#[test]
fn native_many_delete_waits_then_rechecks_all_versions() {
    use crate::operations::{self as o, Bindings, Expr, Request, ValueExpr};
    for level in ["READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"] {
        for history in 0..4 {
            for commit in [false, true] {
                let mut outcomes = Vec::new();
                for batch in [false, true] {
                    let f = Fixture::new();
                    f.exec("CREATE TABLE t(id bigint PRIMARY KEY,n bigint)");
                    f.exec("INSERT INTO t VALUES(1,0),(2,0)");
                    let table = f.sql().catalog_engine().unwrap().1.remove(0);
                    let targets = [(1,version(&f,1)),(2,version(&f,2))];
                    let (ready_send, ready) = mpsc::channel();
                    let (go, proceed) = mpsc::channel();
                    let (send, done) = mpsc::channel();
                    let admin = f.admin().clone();
                    let level_owned = level.to_owned();
                    let worker = thread::spawn(move || {
                        let mut s = admin.sql_session();
                        begin_isolation(&mut s, &level_owned);
                        run(&mut s,"SELECT * FROM t");
                        let result = if batch {
                            o::NATIVE_DELETE_BEFORE_WRITE.with_borrow_mut(|h| *h = Some(Box::new(move || {
                                ready_send.send(()).unwrap(); proceed.recv().unwrap();
                            })));
                            let r = s.execute_native(&Request::Delete { table:"t".into(),
                                filter:Expr::Value(ValueExpr::Literal(relational::Value::Bool(true))),
                                guard:None,cap:Some(ValueExpr::Literal(Int(2))) }, &Bindings::new());
                            o::NATIVE_DELETE_BEFORE_WRITE.with_borrow_mut(|h| *h = None);
                            r.map(|r| relational::SqlResult { affected:r.affected as u64,..Default::default() })
                                .map_err(|e| match e {
                                    o::Error::Sql { sqlstate } if sqlstate == "40001" => CoreError::Sql(SqlError::Version),
                                    o::Error::Conflict => CoreError::Sql(SqlError::Conflict),
                                    e => panic!("unexpected native error: {e:?}"),
                                })
                        } else {
                            ready_send.send(()).unwrap(); proceed.recv().unwrap();
                            targets.into_iter().try_fold(relational::SqlResult::default(), |mut out,(id,v)| {
                                out.affected += s.delete_engine(&table,vec![Int(id)],v)?.affected;
                                Ok(out)
                            })
                        };
                        run(&mut s, if result.is_ok() { "COMMIT" } else { "ROLLBACK" });
                        send.send(result).unwrap();
                    });
                    ready.recv_timeout(Duration::from_secs(20)).unwrap();
                    let mut holder = f.sql();
                    if history == 3 { f.exec("DELETE FROM t WHERE id=2"); }
                    run(&mut holder,"BEGIN");
                    match history {
                        0 => { run(&mut holder,"UPDATE t SET n=10 WHERE id=2"); }
                        1 => { run(&mut holder,"DELETE FROM t WHERE id=2"); }
                        2 => { run(&mut holder,"DELETE FROM t WHERE id=2"); run(&mut holder,"INSERT INTO t VALUES(2,20)"); }
                        _ => { run(&mut holder,"INSERT INTO t VALUES(2,20)"); }
                    }
                    go.send(()).unwrap();
                    let pending = Pending { done, worker };
                    pending.blocked(&holder);
                    run(&mut holder,if commit {"COMMIT"} else {"ROLLBACK"});
                    let result = pending.finish().map(|r|r.affected);
                    if result.is_err() {
                        assert_eq!(run(&mut f.sql(),"SELECT * FROM t WHERE id=1").rows.len(),1);
                    }
                    outcomes.push((result,run(&mut f.sql(),"SELECT * FROM t ORDER BY id").rows.into_iter().map(|r|r.values).collect::<Vec<_>>()));
                }
                assert_eq!(outcomes[0],outcomes[1],"{level}, history={history}, commit={commit}");
            }
        }
    }
}

#[test]
fn native_many_incoming_fk_delete_above_sql_threshold_keeps_row_locks() {
    use crate::operations::{Bindings, Expr, Request, ValueExpr};
    for count in [200,201] {
      for per_row_baseline in [false,true] {
        let f = Fixture::new();
        f.exec("CREATE TABLE t(id bigint PRIMARY KEY,n bigint)");
        f.exec("CREATE TABLE c(id bigint PRIMARY KEY,p bigint REFERENCES t(id) ON DELETE CASCADE)");
        f.exec(&format!("INSERT INTO t VALUES {}",(1..=count).map(|id|format!("({id},0)")).collect::<Vec<_>>().join(",")));
        f.exec("INSERT INTO t VALUES(9999,0)");
        f.exec("INSERT INTO c VALUES(10,1),(20,9999)");
        let mut s = f.sql(); run(&mut s,"BEGIN");
        if per_row_baseline {
            let table=f.sql().catalog_engine().unwrap().1.into_iter().find(|t|t.name=="t").unwrap();
            let targets=run(&mut s,"SELECT * FROM t WHERE id<9999 ORDER BY id").rows;
            for row in targets { s.delete_engine(&table,vec![row.values[0].clone()],row.version).unwrap(); }
        } else {
        s.execute_native(&Request::Delete { table:"t".into(),
            filter:Expr::Compare(Box::new(Expr::Field("id".into())),crate::operations::Compare::Lt,
                Box::new(Expr::Value(ValueExpr::Literal(Int(9999))))),
            guard:None,cap:Some(ValueExpr::Literal(Int(count))) },&Bindings::new()).unwrap();
        }
        let other = Pending::start(&f,"UPDATE t SET n=1 WHERE id=9999");
        other.finish().unwrap(); run(&mut s,"ROLLBACK");
      }
    }
}

#[test]
fn native_many_fixed_snapshot_insert_conflicts_equal_sql() {
    use crate::operations::{self as o, Bindings, Expr, Request, ValueExpr};
    for level in ["REPEATABLE READ","SERIALIZABLE"] {
        for unique in [false,true] {
            let mut outcomes=Vec::new();
            for native in [false,true] {
                let f=Fixture::new(); f.exec("CREATE TABLE t(id bigint PRIMARY KEY,n bigint UNIQUE)");
                let mut s=f.sql(); begin_isolation(&mut s,level); run(&mut s,"SELECT * FROM t");
                f.exec(if unique {"INSERT INTO t VALUES(9,10)"} else {"INSERT INTO t VALUES(1,10)"});
                let result = if native {
                    let lit=|n| Expr::Value(ValueExpr::Literal(Int(n)));
                    s.execute_native(&Request::InsertMany {table:"t".into(),columns:vec!["id".into(),"n".into()],
                        rows:vec![vec![lit(1),lit(10)],vec![lit(2),lit(20)]],cap:ValueExpr::Literal(Int(2))},&Bindings::new())
                        .map(|r|r.affected as u64).map_err(|e|match e {o::Error::Conflict=>"40001".into(),o::Error::Duplicate=>"23505".into(), e=>e.sqlstate().unwrap().to_owned()})
                } else { s.execute("INSERT INTO t VALUES(1,10),(2,20)",&[],None).map(|r|r.affected).map_err(|e|crate::sqlstate(&e).to_owned()) };
                run(&mut s,"ROLLBACK");
                assert!(result.is_err());
                outcomes.push((result,run(&mut f.sql(),"SELECT * FROM t ORDER BY id").rows.into_iter().map(|r|r.values).collect::<Vec<_>>()));
            }
            assert_eq!(outcomes[0],outcomes[1],"{level}, unique={unique}");
        }
    }
}
