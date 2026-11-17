#![cfg(feature = "engine-rocksdb")]
//! R2-PGLIMITS-2: v2 DML RETURNING past 1 MiB spills to a private temp file
//! (PostgreSQL's tuplestore) and returns every row; legacy keeps 54000.
mod common;
use giggles_core::{
    Admin, CoreError, Engine, Limits, SqlSession,
    relational::{SqlError, SqlRow, Value, spill},
};
use std::path::{Path, PathBuf};

const ROWS: i64 = 50_000;

fn load(s: &mut SqlSession, table: &str, rows: i64, batch: i64) {
    s.execute(&format!("CREATE TABLE {table} (id int PRIMARY KEY, v bigint, w text)"), &[], None).unwrap();
    for start in (0..rows).step_by(batch as usize) {
        let values = (start..(start + batch).min(rows))
            .map(|i| format!("({i},{},'row-{i}')", i * 10))
            .collect::<Vec<_>>()
            .join(",");
        s.execute(&format!("INSERT INTO {table} VALUES {values}"), &[], None).unwrap();
    }
}

/// A private spill directory for this test thread; asserts it ends empty.
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

fn ids(rows: impl IntoIterator<Item = SqlRow>) -> Vec<i64> {
    rows.into_iter()
        .map(|row| match row.values[0] { Value::Int(id) => id, ref other => panic!("{other:?}") })
        .collect()
}

fn streamed(s: &mut SqlSession, sql: &str, dir: &Path) -> (u64, Vec<SqlRow>) {
    let (result, spilled) = spill::streaming(|| s.execute(sql, &[], None));
    let result = result.unwrap();
    assert!(result.rows.is_empty(), "{sql}: spilled rows are not also in memory");
    let spilled = spilled.unwrap_or_else(|| panic!("{sql}: expected a spill"));
    assert!(spilled.path().starts_with(dir) && spilled.path().exists());
    let rows = spilled.collect::<Result<Vec<_>, _>>().unwrap();
    (result.affected, rows)
}

#[test]
fn update_delete_insert_select_returning_spill_every_row_in_order() {
    let dir = SpillDir::new("v2-returning-spill");
    let admin = Admin::create_engine(common::fresh("v2-returning-spill"), Limits::default(), Engine::V2).unwrap();
    let mut s = admin.sql_session();
    load(&mut s, "t", ROWS, 10_000);
    let expected = (0..ROWS).collect::<Vec<_>>();

    // Materialising callers get every row; the spill file is gone afterwards.
    let r = s.execute("UPDATE t SET v = v + 1 RETURNING id, v, w", &[], None).unwrap();
    assert_eq!((r.affected, r.rows.len()), (ROWS as u64, ROWS as usize));
    assert_eq!(r.rows[49_999].values, [Value::Int(49_999), Value::Int(499_991), Value::Text("row-49999".into())]);
    assert_eq!(ids(r.rows), expected);
    assert_eq!(dir.files(), 0);

    // Streaming callers read the spill sequentially.
    let (affected, rows) = streamed(&mut s, "UPDATE t SET v = v + 1 RETURNING *", &dir.0);
    assert_eq!((affected, rows.len()), (ROWS as u64, ROWS as usize));
    assert_eq!(rows[7].values[1], Value::Int(72));
    assert_eq!(ids(rows), expected);
    assert_eq!(dir.files(), 0);

    s.execute("CREATE TABLE u (id int PRIMARY KEY, v bigint, w text)", &[], None).unwrap();
    let (affected, rows) = streamed(&mut s, "INSERT INTO u SELECT id, v, w FROM t RETURNING id, w", &dir.0);
    assert_eq!(affected, ROWS as u64);
    assert_eq!(rows[123].values[1], Value::Text("row-123".into()));
    assert_eq!(ids(rows), expected);

    let (affected, rows) = streamed(&mut s, "DELETE FROM u RETURNING id", &dir.0);
    assert_eq!(affected, ROWS as u64);
    assert_eq!(ids(rows), expected);
    assert_eq!(s.execute("SELECT count(*) FROM u", &[], None).unwrap().rows[0].values, [Value::Int(0)]);
    assert_eq!(dir.files(), 0);
}

#[test]
fn spill_is_removed_after_error_and_rollback() {
    let dir = SpillDir::new("v2-returning-spill-cleanup");
    let admin = Admin::create_engine(common::fresh("v2-returning-spill-cleanup"), Limits::default(), Engine::V2).unwrap();
    let mut s = admin.sql_session();
    load(&mut s, "t", ROWS, 10_000);
    // The CHECK fails on a late input page, after RETURNING has spilled.
    s.execute("CREATE TABLE u (id int PRIMARY KEY CHECK (id < 45000), v bigint, w text)", &[], None).unwrap();
    let insert = "INSERT INTO u SELECT id, v, w FROM t RETURNING *";
    let (result, spilled) = spill::streaming(|| s.execute(insert, &[], None));
    assert!(matches!(result, Err(CoreError::Sql(SqlError::CheckViolation))), "{result:?}");
    assert!(spilled.is_none());
    assert_eq!(dir.files(), 0);
    let result = s.execute(insert, &[], None);
    assert!(matches!(result, Err(CoreError::Sql(SqlError::CheckViolation))), "{result:?}");
    assert_eq!(dir.files(), 0);
    assert_eq!(s.execute("SELECT count(*) FROM u", &[], None).unwrap().rows[0].values, [Value::Int(0)]);

    // ROLLBACK discards the writes; a held handle stays readable until dropped.
    s.execute("BEGIN", &[], None).unwrap();
    let (result, spilled) = spill::streaming(|| s.execute("UPDATE t SET v = -1 RETURNING id, v", &[], None));
    assert_eq!(result.unwrap().affected, ROWS as u64);
    let spilled = spilled.unwrap();
    let r = s.execute("DELETE FROM t RETURNING id", &[], None).unwrap();
    assert_eq!(r.rows.len(), ROWS as usize);
    assert_eq!(dir.files(), 1);
    s.execute("ROLLBACK", &[], None).unwrap();
    assert_eq!(spilled.len(), ROWS as u64);
    assert_eq!(ids(spilled.map(Result::unwrap)), (0..ROWS).collect::<Vec<_>>());
    assert_eq!(dir.files(), 0);
    let r = s.execute("SELECT v FROM t WHERE id = 7", &[], None).unwrap();
    assert_eq!(r.rows[0].values, [Value::Int(70)]);
}

#[test]
fn legacy_returning_keeps_the_one_mebibyte_limit() {
    let admin = Admin::create_engine(common::fresh("legacy-returning-limit"), Limits::default(), Engine::Legacy).unwrap();
    let mut s = admin.sql_session();
    load(&mut s, "t", 20_000, 500);
    assert_eq!(
        s.execute("UPDATE t SET v = v + 1 RETURNING id, v, w", &[], None),
        Err(CoreError::Sql(SqlError::Limit))
    );
}

const WIDE_ROWS: i64 = 400;

fn insert_values(start: i64, end: i64) -> String {
    // Keep input below its separate budget; RETURNING widens each output row.
    (start..end).map(|i| format!("({i},{},'x')", i * 10))
        .collect::<Vec<_>>().join(",")
}

fn assert_wide(affected: u64, rows: Vec<SqlRow>, start: i64, end: i64, dir: &SpillDir) {
    assert_eq!((affected, rows.len()), ((end - start) as u64, (end - start) as usize));
    for row in [&rows[0], &rows[rows.len() / 2], &rows[rows.len() - 1]] {
        assert_eq!(row.values[1], Value::Text("x".repeat(8000)));
    }
    assert_eq!(ids(rows), (start..end).collect::<Vec<_>>());
    assert_eq!(dir.files(), 0);
}

#[test]
fn wide_returning_covers_paged_dml_and_cascade_paths() {
    let dir = SpillDir::new("v2-wide-dml");
    let admin = Admin::create_engine(common::fresh("v2-wide-dml"), Limits::default(), Engine::V2).unwrap();
    let mut s = admin.sql_session();
    load(&mut s, "t", WIDE_ROWS, 200);
    let result = s.execute("UPDATE t SET w = 'x' RETURNING id, repeat(w,8000), v", &[], None).unwrap();
    assert_eq!(result.rows[123].values[2], Value::Int(1230));
    assert_wide(result.affected, result.rows, 0, WIDE_ROWS, &dir);

    // A key move selects the cascade executor, even with no referencing table.
    let result = s.execute("UPDATE t SET id = id + 1000 RETURNING id, repeat(w,8000)", &[], None).unwrap();
    assert_wide(result.affected, result.rows, 1000, 1000 + WIDE_ROWS, &dir);
    let (affected, rows) = streamed(&mut s, "DELETE FROM t RETURNING id, repeat(w,8000)", &dir.0);
    assert_wide(affected, rows, 1000, 1000 + WIDE_ROWS, &dir);

    s.execute(&format!("INSERT INTO t VALUES {}", insert_values(0, WIDE_ROWS)), &[], None).unwrap();
    s.execute("CREATE TABLE child (id int PRIMARY KEY, parent int REFERENCES t(id) ON DELETE CASCADE)", &[], None).unwrap();
    s.execute("INSERT INTO child SELECT id, id FROM t", &[], None).unwrap();
    let result = s.execute("DELETE FROM t RETURNING id, repeat(w,8000)", &[], None).unwrap();
    assert_wide(result.affected, result.rows, 0, WIDE_ROWS, &dir);
    assert_eq!(s.execute("SELECT count(*) FROM child", &[], None).unwrap().rows[0].values, [Value::Int(0)]);
}

#[test]
fn wide_returning_covers_insert_pages_conflicts_and_complete_image() {
    let dir = SpillDir::new("v2-wide-insert");
    let admin = Admin::create_engine(common::fresh("v2-wide-insert"), Limits::default(), Engine::V2).unwrap();
    let mut s = admin.sql_session();
    s.execute("CREATE TABLE t (id int PRIMARY KEY, v bigint, w text)", &[], None).unwrap();
    // Each nested 200-row VALUES page projects more than 1 MiB. Its rows must
    // stay available to the outer executor instead of publishing a spill slot.
    let result = s.execute(&format!("INSERT INTO t VALUES {} RETURNING id, repeat(w,8000)", insert_values(0, WIDE_ROWS)), &[], None).unwrap();
    assert_wide(result.affected, result.rows, 0, WIDE_ROWS, &dir);
    s.execute("CREATE TABLE u (id int PRIMARY KEY, v bigint, w text)", &[], None).unwrap();
    let (affected, rows) = streamed(&mut s, "INSERT INTO u SELECT * FROM t RETURNING id, repeat(w,8000)", &dir.0);
    assert_wide(affected, rows, 0, WIDE_ROWS, &dir);

    // 350 conflicts and 50 new rows; check both the row order and updated value.
    let result = s.execute(&format!("INSERT INTO t VALUES {} ON CONFLICT (id) DO UPDATE SET v = excluded.v + 1 RETURNING id, repeat(w,8000), v", insert_values(50, 450)), &[], None).unwrap();
    assert_eq!(result.rows[0].values[2], Value::Int(501));
    assert_eq!(result.rows[399].values[2], Value::Int(4490));
    assert_wide(result.affected, result.rows, 50, 450, &dir);
    let result = s.execute(&format!("INSERT INTO t VALUES {} ON CONFLICT DO NOTHING RETURNING id, repeat(w,8000)", insert_values(50, 850)), &[], None).unwrap();
    assert_wide(result.affected, result.rows, 450, 850, &dir);

    // Self-referencing FKs reject the paged insert path, even when NULL.
    s.execute("CREATE TABLE complete (id int PRIMARY KEY, parent int REFERENCES complete(id), w text)", &[], None).unwrap();
    let (affected, rows) = streamed(&mut s, "INSERT INTO complete SELECT id, NULL::integer, w FROM u RETURNING id, repeat(w,8000)", &dir.0);
    assert_wide(affected, rows, 0, WIDE_ROWS, &dir);
}

#[test]
fn wide_returning_keeps_bounded_and_legacy_limits_and_cleans_up() {
    let dir = SpillDir::new("wide-returning-limits");
    for engine in [Engine::V2, Engine::Legacy] {
        let admin = Admin::create_engine(common::fresh("wide-returning-limits"), Limits::default(), engine).unwrap();
        let mut s = admin.sql_session();
        load(&mut s, "t", WIDE_ROWS, 200);
        let sql = "UPDATE t SET v = v + 1 RETURNING id, repeat('x',8000)";
        let result = if engine == Engine::V2 {
            spill::bounded(|| s.execute(sql, &[], None))
        } else {
            s.execute(sql, &[], None)
        };
        assert_eq!(result, Err(CoreError::Sql(SqlError::Limit)));
        assert_eq!(dir.files(), 0);
        assert_eq!(s.execute("SELECT v FROM t WHERE id = 7", &[], None).unwrap().rows[0].values, [Value::Int(70)]);
    }
}
