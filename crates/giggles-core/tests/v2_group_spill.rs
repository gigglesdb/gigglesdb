#![cfg(feature = "engine-rocksdb")]
//! R2-PGLIMITS-4: v2 GROUP BY, DISTINCT, DISTINCT aggregates, derived tables
//! and IN subqueries spill past work_mem instead of raising 54000, and v2
//! joins have no cumulative visit cap (cancellation still stops them).
//! Legacy keeps its limits.
mod common;
use giggles_core::{
    Admin, Cancellation, CoreError, Engine, Limits, SqlSession,
    relational::{SqlError, SqlRow, Value, spill},
};
use std::path::PathBuf;
use std::time::{Duration, Instant};

struct SpillDir(PathBuf);
impl SpillDir {
    fn new(name: &str) -> Self {
        let dir = common::fresh(&format!("{name}-spill"));
        std::fs::create_dir_all(&dir).unwrap();
        spill::set_thread_dir(Some(dir.clone()));
        Self(dir)
    }
    fn files(&self) -> usize {
        std::fs::read_dir(&self.0).unwrap().count()
    }
}
impl Drop for SpillDir {
    fn drop(&mut self) {
        spill::set_thread_dir(None);
    }
}

fn insert(s: &mut SqlSession, table: &str, rows: i64, row: impl Fn(i64) -> String) {
    for start in (0..rows).step_by(10_000) {
        let values = (start..(start + 10_000).min(rows)).map(&row).collect::<Vec<_>>().join(",");
        s.execute(&format!("INSERT INTO {table} VALUES {values}"), &[], None).unwrap();
    }
}
fn rows(s: &mut SqlSession, sql: &str) -> Vec<SqlRow> {
    s.execute(sql, &[], None).unwrap_or_else(|e| panic!("{sql}: {e:?}")).rows
}
fn ints(row: &SqlRow) -> Vec<i64> {
    row.values.iter().map(|v| match v {
        Value::Int(n) => *n,
        other => panic!("{other:?}"),
    }).collect()
}

const ROWS: i64 = 200_000;
const GROUPS: i64 = 100_000;
/// 150,000 distinct values over 200,000 rows (7919 is coprime to 150,000).
fn x(id: i64) -> i64 {
    id * 7919 % 150_000
}

#[test]
fn v2_group_by_distinct_and_distinct_aggregates_spill() {
    let dir = SpillDir::new("v2-group-spill");
    let admin = Admin::create_engine(common::fresh("v2-group-spill"), Limits::default(), Engine::V2).unwrap();
    let mut s = admin.sql_session();
    s.execute("CREATE TABLE t (id int PRIMARY KEY, g int, x int)", &[], None).unwrap();
    insert(&mut s, "t", ROWS, |i| format!("({i},{},{})", i % GROUPS, x(i)));

    // 100,000 groups of two rows, ordered on the group key, with a per-group
    // DISTINCT aggregate (both rows' x differ).
    let r = rows(&mut s, "SELECT g, count(*), sum(id), count(DISTINCT x) FROM t GROUP BY g ORDER BY g DESC");
    assert_eq!(r.len(), GROUPS as usize);
    for (row, g) in r.iter().zip((0..GROUPS).rev()) {
        assert_eq!(ints(row), [g, 2, 2 * g + GROUPS, 2]);
    }
    assert_eq!(dir.files(), 0);

    // Hash aggregation (no DISTINCT aggregate): groups past work_mem spill and
    // merge back with the in-memory ones in group-key order; few groups stay
    // in memory.
    let r = rows(&mut s, "SELECT g, count(*), sum(id) FROM t GROUP BY g");
    assert_eq!(r.len(), GROUPS as usize);
    for (row, g) in r.iter().zip(0..GROUPS) {
        assert_eq!(ints(row), [g, 2, 2 * g + GROUPS]);
    }
    let r = rows(&mut s, "SELECT g % 3, count(*) FROM t GROUP BY g % 3");
    assert_eq!(r.iter().map(ints).collect::<Vec<_>>(), [[0, 66_668], [1, 66_666], [2, 66_666]]);
    assert_eq!(dir.files(), 0);

    // Non-aggregate DISTINCT keeps first occurrences in input order.
    let r = rows(&mut s, "SELECT DISTINCT g FROM t");
    assert_eq!(r.iter().map(|row| ints(row)[0]).collect::<Vec<_>>(), (0..GROUPS).collect::<Vec<_>>());

    // Global DISTINCT aggregates over every row.
    let r = rows(&mut s, "SELECT count(DISTINCT x), count(DISTINCT g), count(*) FROM t");
    assert_eq!(ints(&r[0]), [150_000, GROUPS, ROWS]);
    assert_eq!(dir.files(), 0);

    // Keyless grouped shapes (no GROUP BY keys, non-plain projection) also
    // apply their deferred DISTINCT inputs (review B1).
    let r = rows(&mut s, "SELECT 'n', count(DISTINCT x) FROM t");
    assert_eq!(r[0].values, [Value::Text("n".into()), Value::Int(150_000)]);
    s.execute("CREATE TABLE small (id int PRIMARY KEY, x int)", &[], None).unwrap();
    insert(&mut s, "small", 1_000, |i| format!("({i},{})", i % 300));
    for sql in [
        "SELECT count(DISTINCT x) FROM small HAVING count(*) > 0",
        "SELECT count(DISTINCT x) + 0 FROM small",
        "SELECT count(DISTINCT x) FROM small ORDER BY 1",
    ] {
        assert_eq!(ints(&rows(&mut s, sql)[0]), [300], "{sql}");
    }
    let r = rows(&mut s, "SELECT sum(DISTINCT x), max(x), count(DISTINCT id) FROM small HAVING count(DISTINCT x) > 1");
    assert_eq!(ints(&r[0]), [299 * 300 / 2, 299, 1_000]);
    assert_eq!(dir.files(), 0);

    // An error after the group sort has spilled removes every run.
    let failed = s.execute("SELECT 10 / (id - 199999), count(*) FROM t GROUP BY 10 / (id - 199999)", &[], None);
    assert!(matches!(failed, Err(CoreError::Sql(SqlError::DivisionByZero))), "{failed:?}");
    assert_eq!(dir.files(), 0);
}

#[test]
fn v2_derived_table_in_subquery_and_join_cancellation() {
    const N: i64 = 100_000;
    let dir = SpillDir::new("v2-derived-spill");
    let admin = Admin::create_engine(common::fresh("v2-derived-spill"), Limits::default(), Engine::V2).unwrap();
    let mut s = admin.sql_session();
    s.execute("CREATE TABLE u (id int PRIMARY KEY, v int)", &[], None).unwrap();
    insert(&mut s, "u", N, |i| format!("({i},{})", i * 7919 % N));

    let r = rows(&mut s, "SELECT count(*), sum(v), max(v) FROM (SELECT id, v FROM u) AS d");
    assert_eq!(ints(&r[0]), [N, N * (N - 1) / 2, N - 1]);
    let r = rows(&mut s, "SELECT count(*) FROM u WHERE id IN (SELECT v FROM u)");
    assert_eq!(ints(&r[0]), [N]);
    let r = rows(&mut s, "SELECT count(*) FROM u WHERE id + 50000 NOT IN (SELECT id FROM u)");
    assert_eq!(ints(&r[0]), [N / 2]);
    assert_eq!(dir.files(), 0);

    // 10^10 candidate visits: far past the legacy 2^24 cap, so only
    // cancellation ends it.
    let cancellation = Cancellation::default();
    let (worker, context) = (admin.clone(), cancellation.clone());
    let job = std::thread::spawn(move || {
        context.scope(|| worker.sql_session().execute("SELECT count(*) FROM u a JOIN u b ON a.v + b.v < 0", &[], None))
    });
    std::thread::sleep(Duration::from_millis(500));
    assert!(!job.is_finished());
    let cancelled = Instant::now();
    assert!(cancellation.cancel());
    let result = job.join().unwrap();
    assert!(cancelled.elapsed() < Duration::from_secs(10));
    assert!(
        matches!(result, Err(CoreError::Cancelled | CoreError::Sql(SqlError::Aborted))),
        "{:?}",
        result.map(|r| r.rows.len())
    );
}

#[test]
fn legacy_group_by_and_distinct_keep_their_limits() {
    let admin = Admin::create_engine(common::fresh("legacy-group-limit"), Limits::default(), Engine::Legacy).unwrap();
    let mut s = admin.sql_session();
    s.execute("CREATE TABLE t (id int PRIMARY KEY, w text)", &[], None).unwrap();
    for start in (0..20_000).step_by(500) {
        let values = (start..start + 500).map(|i| format!("({i},'{i:0>60}')")).collect::<Vec<_>>().join(",");
        s.execute(&format!("INSERT INTO t VALUES {values}"), &[], None).unwrap();
    }
    // 20,000 distinct 60-byte groups exceed the legacy 1 MiB result cap.
    let limit = Err(CoreError::Sql(SqlError::Limit));
    assert_eq!(s.execute("SELECT w, count(*) FROM t GROUP BY w", &[], None), limit);
    assert_eq!(s.execute("SELECT DISTINCT w FROM t", &[], None), limit);
    assert_eq!(ints(&rows(&mut s, "SELECT count(DISTINCT id) FROM t")[0]), [20_000]);
}
