#![cfg(feature = "engine-rocksdb")]
//! R2-PGLIMITS-3: v2 ORDER BY past work_mem spills sorted runs and merges
//! them (PostgreSQL's external sort); the statement output spills too, so
//! no row or byte ceiling applies. Legacy keeps its limits.
mod common;
use giggles_core::{
    Admin, Cancellation, CoreError, Engine, Limits, SqlSession,
    relational::{SqlError, SqlRow, Value, spill},
};
use std::path::PathBuf;
use std::time::{Duration, Instant};

const ROWS: i64 = 200_000;

/// `k` is a permutation of 0..ROWS with every 1000th row NULL; `w` repeats
/// every 50,000 keys so text ties exercise stability.
fn key(id: i64) -> Option<i64> {
    (id % 1000 != 0).then_some(id * 7919 % ROWS)
}
fn text(id: i64) -> String {
    format!("w-{:06}", id * 7919 % 50_000)
}

fn load(s: &mut SqlSession, rows: i64) {
    s.execute("CREATE TABLE t (id int PRIMARY KEY, k int, w text)", &[], None).unwrap();
    for start in (0..rows).step_by(10_000) {
        let values = (start..start + 10_000)
            .map(|i| format!("({i},{},'{}')", key(i).map_or("NULL".into(), |k| k.to_string()), text(i)))
            .collect::<Vec<_>>()
            .join(",");
        s.execute(&format!("INSERT INTO t VALUES {values}"), &[], None).unwrap();
    }
}

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

fn int(value: &Value) -> Option<i64> {
    match value {
        Value::Int(n) => Some(*n),
        Value::Null => None,
        other => panic!("{other:?}"),
    }
}
fn ids(rows: &[SqlRow]) -> Vec<i64> {
    rows.iter().map(|row| int(&row.values[0]).unwrap()).collect()
}
/// Expected ids for ORDER BY k, ties (NULLs) in primary key order.
fn by_key(desc: bool, nulls_first: bool) -> Vec<i64> {
    let mut ids = (0..ROWS).collect::<Vec<_>>();
    ids.sort_by(|a, b| match (key(*a), key(*b)) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, _) => if nulls_first { std::cmp::Ordering::Less } else { std::cmp::Ordering::Greater },
        (_, None) => if nulls_first { std::cmp::Ordering::Greater } else { std::cmp::Ordering::Less },
        (Some(x), Some(y)) => if desc { y.cmp(&x) } else { x.cmp(&y) },
    });
    ids
}

#[test]
fn v2_order_by_spills_and_returns_every_row_in_order() {
    let dir = SpillDir::new("v2-sort-spill");
    let admin = Admin::create_engine(common::fresh("v2-sort-spill"), Limits::default(), Engine::V2).unwrap();
    let mut s = admin.sql_session();
    load(&mut s, ROWS);

    // Materialised: every row, int key ASC NULLS LAST. DESC NULLS FIRST and
    // ASC NULLS FIRST are read by the cursors below; the server test covers DESC NULLS LAST.
    let r = s.execute("SELECT id, k FROM t ORDER BY k", &[], None).unwrap();
    assert_eq!(ids(&r.rows), by_key(false, false));
    assert!(r.rows.iter().all(|row| int(&row.values[1]) == key(int(&row.values[0]).unwrap())));
    assert_eq!(dir.files(), 0);

    // Streamed text key DESC: ties keep primary key order (ASC in the server test).
    let (result, spilled) = spill::streaming(|| s.execute("SELECT id, w FROM t ORDER BY w DESC", &[], None));
    assert!(result.unwrap().rows.is_empty());
    let spilled = spilled.expect("SELECT output spills");
    assert!(spilled.path().starts_with(&dir.0));
    let rows = spilled.collect::<Result<Vec<_>, _>>().unwrap();
    let mut expected = (0..ROWS).collect::<Vec<_>>();
    expected.sort_by_key(|id| std::cmp::Reverse(text(*id)));
    assert_eq!(ids(&rows), expected);
    assert_eq!(rows[0].values[1], Value::Text(text(expected[0])));
    assert_eq!(dir.files(), 0);

    // Top-N stays in memory and is unchanged.
    let r = s.execute("SELECT id FROM t ORDER BY k DESC NULLS LAST LIMIT 10", &[], None).unwrap();
    assert_eq!(ids(&r.rows), by_key(true, false)[..10]);

    // SqlSession cursors read the spilled result in chunks; dropping one early removes it.
    let p = s.prepare("SELECT id FROM t ORDER BY k DESC").unwrap();
    let mut cursor = s.open_cursor(&p, &[]).unwrap();
    let mut read = Vec::new();
    while !cursor.is_exhausted() {
        read.extend(ids(&cursor.fetch(7_000, 1 << 20).unwrap()));
    }
    assert_eq!(read, by_key(true, true));
    drop(cursor);
    let p = s.prepare("SELECT id FROM t ORDER BY k ASC NULLS FIRST").unwrap();
    let mut cursor = s.open_cursor(&p, &[]).unwrap();
    assert_eq!(ids(&cursor.fetch(10, 1 << 20).unwrap()), by_key(false, true)[..10]);
    assert!(dir.files() > 0);
    drop(cursor);
    assert_eq!(dir.files(), 0);

    // An error after the sort has spilled removes every run and output file.
    let failed = s.execute("SELECT id, 10 / (id - 199999) FROM t ORDER BY w", &[], None);
    assert!(matches!(failed, Err(CoreError::Sql(SqlError::DivisionByZero))), "{failed:?}");
    assert_eq!(dir.files(), 0);
}

#[test]
fn v2_order_by_cancel_removes_spilled_runs() {
    let dir = common::fresh("v2-sort-cancel-spill");
    std::fs::create_dir_all(&dir).unwrap();
    let admin = Admin::create_engine(common::fresh("v2-sort-cancel"), Limits::default(), Engine::V2).unwrap();
    // 40,000 rows charge ~10 MiB of sort input: several runs past work_mem.
    load(&mut admin.sql_session(), 40_000);
    let cancellation = Cancellation::default();
    let (worker, context, spill_dir) = (admin.clone(), cancellation.clone(), dir.clone());
    let job = std::thread::spawn(move || {
        spill::set_thread_dir(Some(spill_dir));
        context.scope(|| worker.sql_session().execute("SELECT id, w FROM t ORDER BY w DESC, k", &[], None))
    });
    let deadline = Instant::now() + Duration::from_secs(30);
    while std::fs::read_dir(&dir).unwrap().count() == 0 && !job.is_finished() {
        assert!(Instant::now() < deadline, "no sort run appeared");
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(cancellation.cancel());
    let result = job.join().unwrap();
    assert!(
        matches!(result, Err(CoreError::Cancelled | CoreError::Sql(SqlError::Aborted))),
        "{:?}",
        result.map(|r| r.rows.len())
    );
    assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 0);
}

#[test]
fn legacy_order_by_keeps_its_limits() {
    let admin = Admin::create_engine(common::fresh("legacy-sort-limit"), Limits::default(), Engine::Legacy).unwrap();
    let mut s = admin.sql_session();
    s.execute("CREATE TABLE t (id int PRIMARY KEY, w text)", &[], None).unwrap();
    for start in (0..20_000).step_by(500) {
        let values = (start..start + 500).map(|i| format!("({i},'{i:0>60}')")).collect::<Vec<_>>().join(",");
        s.execute(&format!("INSERT INTO t VALUES {values}"), &[], None).unwrap();
    }
    assert_eq!(s.execute("SELECT id, w FROM t ORDER BY w DESC", &[], None), Err(CoreError::Sql(SqlError::Limit)));
    assert_eq!(s.execute("SELECT id FROM t ORDER BY w DESC LIMIT 3", &[], None).unwrap().rows.len(), 3);
}
