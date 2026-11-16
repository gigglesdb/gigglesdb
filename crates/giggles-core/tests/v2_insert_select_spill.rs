#![cfg(feature = "engine-rocksdb")]
//! R2-PGLIMITS-7: on v2, an INSERT ... SELECT source with ORDER BY, DISTINCT
//! or a join, or one that reads the target itself, is read completely into a
//! spill store before the first row is inserted (PostgreSQL's statement
//! snapshot), so it has no 65,536-row source cap. Legacy keeps 54000.
mod common;
use giggles_core::{
    Admin, CoreError, Engine, Limits, SqlSession,
    relational::{SqlError, Value, spill},
};
use std::path::PathBuf;

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
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn load(s: &mut SqlSession, rows: i64) {
    s.execute("CREATE TABLE t (id int PRIMARY KEY, x int, w text)", &[], None).unwrap();
    for start in (0..rows).step_by(10_000) {
        let values = (start..(start + 10_000).min(rows))
            .map(|i| format!("({i},{},'w{i:0>20}')", x(i)))
            .collect::<Vec<_>>()
            .join(",");
        s.execute(&format!("INSERT INTO t VALUES {values}"), &[], None).unwrap();
    }
}
fn ints(s: &mut SqlSession, sql: &str) -> Vec<i64> {
    let r = s.execute(sql, &[], None).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    assert_eq!(r.rows.len(), 1, "{sql}");
    r.rows[0].values.iter().map(|v| match v {
        Value::Int(n) => *n,
        Value::Numeric(n) => n.to_string().parse().unwrap(),
        other => panic!("{other:?}"),
    }).collect()
}
fn affected(s: &mut SqlSession, sql: &str) -> u64 {
    s.execute(sql, &[], None).unwrap_or_else(|e| panic!("{sql}: {e:?}")).affected
}

const ROWS: i64 = 150_000;
/// 70,000 distinct values over 150,000 rows (7919 is coprime to 70,000).
fn x(id: i64) -> i64 {
    id * 7919 % 70_000
}

#[test]
fn v2_insert_select_sources_spill_past_the_row_cap() {
    let dir = SpillDir::new("v2-insert-select");
    let path = common::fresh("v2-insert-select");
    {
        let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
        let mut s = admin.sql_session();
        load(&mut s, ROWS);

        // ORDER BY source with RETURNING. PostgreSQL does not promise RETURNING
        // order, so only the set is checked.
        s.execute("CREATE TABLE o (id int PRIMARY KEY, w text)", &[], None).unwrap();
        let r = s.execute("INSERT INTO o SELECT id, w FROM t ORDER BY id DESC RETURNING id", &[], None).unwrap();
        assert_eq!(r.affected, ROWS as u64);
        let mut ids = r.rows.iter().map(|row| match row.values[0] {
            Value::Int(id) => id,
            ref other => panic!("{other:?}"),
        }).collect::<Vec<_>>();
        ids.sort_unstable();
        assert!(ids.iter().copied().eq(0..ROWS));
        assert_eq!(ints(&mut s, "SELECT count(*), min(id), max(id) FROM o"), [ROWS, 0, ROWS - 1]);

        // DISTINCT source; a second run fails on the first duplicate key and
        // leaves the table unchanged.
        s.execute("CREATE TABLE d (x int PRIMARY KEY)", &[], None).unwrap();
        assert_eq!(affected(&mut s, "INSERT INTO d SELECT DISTINCT x FROM t"), 70_000);
        assert_eq!(
            s.execute("INSERT INTO d SELECT DISTINCT x FROM t ORDER BY x DESC", &[], None).map(|r| r.affected),
            Err(CoreError::Sql(SqlError::Duplicate))
        );
        assert_eq!(ints(&mut s, "SELECT count(*), sum(x) FROM d"), [70_000, 2_449_965_000]);

        // Join of two 150,000-row inputs; 75,000 rows out.
        s.execute("CREATE TABLE j (id int PRIMARY KEY, s int)", &[], None).unwrap();
        assert_eq!(
            affected(&mut s, "INSERT INTO j SELECT a.id, a.x + b.x FROM t a JOIN t b ON a.id = b.id WHERE a.id % 2 = 0"),
            75_000
        );
        let sum_x = (0..ROWS).step_by(2).map(x).sum::<i64>();
        assert_eq!(ints(&mut s, "SELECT count(*), sum(s) FROM j"), [75_000, 2 * sum_x]);

        // Self-insert reads the snapshot taken before its first row: exactly double.
        assert_eq!(affected(&mut s, "INSERT INTO d SELECT x + 70000 FROM d"), 70_000);
        assert_eq!(ints(&mut s, "SELECT count(*), min(x), max(x) FROM d"), [140_000, 0, 139_999]);

        assert_eq!(dir.files(), 0, "spill files removed");
    }
    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn legacy_insert_select_source_keeps_the_row_cap() {
    let path = common::fresh("legacy-insert-select");
    {
        let admin = Admin::create_engine(&path, Limits::default(), Engine::Legacy).unwrap();
        let mut s = admin.sql_session();
        // Legacy tables stay under the cap; a 300 x 300 join source passes it.
        load(&mut s, 300);
        s.execute("CREATE TABLE o (id int PRIMARY KEY)", &[], None).unwrap();
        assert_eq!(
            s.execute("INSERT INTO o SELECT a.id * 1000 + b.id FROM t a JOIN t b ON b.id >= 0 ORDER BY 1", &[], None)
                .map(|r| r.affected),
            Err(CoreError::Sql(SqlError::Limit))
        );
    }
    let _ = std::fs::remove_dir_all(&path);
}
