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
        let admin = f.admin().clone();
        let sql = sql.to_owned();
        let (send, done) = mpsc::channel();
        let worker = thread::spawn(move || { let result = admin.sql_session().execute_at(&sql, &[], None, &clock()); send.send(result).unwrap(); });
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
