#![cfg(feature = "engine-rocksdb")]
//! R2-PGLIMITS-6 (DD-0068 Q4, "per-value limits on v2"): on v2, computed
//! aggregate arguments, DISTINCT aggregate inputs and INSERT ... SELECT computed
//! columns are limited per value, not by one statement-wide scalar work total,
//! and keyless DISTINCT aggregates with HAVING or an expression spill instead
//! of raising 54000 past 65,536 rows. Legacy keeps the shared budget.
mod common;
use giggles_core::{
    Admin, CoreError, Engine, Limits, SqlSession,
    relational::{SqlError, SqlRow, Value, spill},
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
}
impl Drop for SpillDir {
    fn drop(&mut self) {
        spill::set_thread_dir(None);
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn insert(s: &mut SqlSession, table: &str, rows: i64, step: i64, row: impl Fn(i64) -> String) {
    for start in (0..rows).step_by(step as usize) {
        let values = (start..(start + step).min(rows)).map(&row).collect::<Vec<_>>().join(",");
        s.execute(&format!("INSERT INTO {table} VALUES {values}"), &[], None).unwrap();
    }
}
fn rows(s: &mut SqlSession, sql: &str) -> Vec<SqlRow> {
    s.execute(sql, &[], None).unwrap_or_else(|e| panic!("{sql}: {e:?}")).rows
}
fn text(v: &Value) -> String {
    match v {
        Value::Int(n) => n.to_string(),
        Value::Numeric(n) => n.to_string(),
        Value::Text(t) => t.clone(),
        Value::Bool(b) => b.to_string(),
        other => panic!("{other:?}"),
    }
}
fn row(s: &mut SqlSession, sql: &str) -> Vec<String> {
    let r = rows(s, sql);
    assert_eq!(r.len(), 1, "{sql}");
    r[0].values.iter().map(text).collect()
}

const ROWS: i64 = 200_000;
/// 150,000 distinct values over 200,000 rows (7919 is coprime to 150,000).
fn x(id: i64) -> i64 {
    id * 7919 % 150_000
}

#[test]
fn v2_computed_and_distinct_aggregate_inputs_are_limited_per_value() {
    let _dir = SpillDir::new("v2-per-value");
    let path = common::fresh("v2-per-value");
    {
        let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
        let mut s = admin.sql_session();
        s.execute("CREATE TABLE t (id int PRIMARY KEY, x int, w text)", &[], None).unwrap();
        // w is 21 bytes; 150,000 distinct values.
        insert(&mut s, "t", ROWS, 10_000, |i| format!("({i},{},'w{:0>20}')", x(i), x(i)));
        // Distinct text input of 200k rows: 4.2 MB of values, PG never fails.
        assert_eq!(row(&mut s, "SELECT count(DISTINCT w) FROM t"), ["150000"]);
        // Computed arguments: one length() per row, no shared total.
        assert_eq!(row(&mut s, "SELECT sum(length(w)), count(upper(w)) FROM t"), ["4200000", "200000"]);
        // Each value is limited on its own: 2,000 values of 43 KB (86 MB in all)
        // pass. A 2.1 MB value passes at default, then fails at a lowered budget.
        assert_eq!(row(&mut s, "SELECT max(length(repeat(w, 2048))) FROM t WHERE id < 2000"), ["43008"]);
        assert_eq!(row(&mut s, "SELECT max(length(repeat(w, 100000))) FROM t"), ["2100000"]);
        s.set_max_value_bytes(65536);
        assert_eq!(
            s.execute("SELECT max(length(repeat(w, 100000))) FROM t", &[], None),
            Err(CoreError::Sql(SqlError::Limit))
        );
        let grouped = rows(&mut s, "SELECT id % 2, sum(length(w || 'x')) FROM t GROUP BY 1 ORDER BY 1");
        let grouped = grouped.iter().map(|r| r.values.iter().map(text).collect::<Vec<_>>()).collect::<Vec<_>>();
        assert_eq!(grouped, [["0", "2200000"], ["1", "2200000"]]);
        // Keyless DISTINCT aggregates with HAVING or an expression (pglimits5 item 1).
        assert_eq!(row(&mut s, "SELECT count(DISTINCT x) FROM t HAVING count(*) > 0"), ["150000"]);
        assert_eq!(row(&mut s, "SELECT count(DISTINCT x) + 0 FROM t"), ["150000"]);
        assert_eq!(row(&mut s, "SELECT 'n', count(DISTINCT x) FROM t ORDER BY 1"), ["n", "150000"]);
        assert_eq!(
            row(&mut s, "SELECT sum(DISTINCT x), max(x), count(DISTINCT id) FROM t HAVING count(DISTINCT x) > 1"),
            ["11249925000", "149999", "200000"]
        );
        assert_eq!(row(&mut s, "SELECT count(DISTINCT w) > 1 FROM t HAVING sum(length(w)) > 0"), ["true"]);
        // INSERT ... SELECT with computed columns over 150k rows.
        s.execute("CREATE TABLE u (id int PRIMARY KEY, w text)", &[], None).unwrap();
        let r = s
            .execute("INSERT INTO u SELECT id + 100000, w || '!' FROM t WHERE id < 150000", &[], None)
            .unwrap();
        assert_eq!(r.affected, 150_000);
        assert_eq!(
            row(&mut s, "SELECT count(*), min(id), max(id), sum(length(w)) FROM u"),
            ["150000", "100000", "249999", "3300000"]
        );
    }
    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn legacy_keeps_the_shared_scalar_work_budget() {
    let path = common::fresh("legacy-per-value");
    {
        let admin = Admin::create_engine(&path, Limits::default(), Engine::Legacy).unwrap();
        let mut s = admin.sql_session();
        s.execute("CREATE TABLE t (id int PRIMARY KEY, w text)", &[], None).unwrap();
        insert(&mut s, "t", 2_000, 500, |i| format!("({i},'w{i:0>20}')"));
        // 43 KB per value passes alone and fails cumulatively past 1 MiB.
        assert_eq!(row(&mut s, "SELECT length(repeat(w, 2048)) FROM t WHERE id = 1"), ["43008"]);
        let limit = Err(CoreError::Sql(SqlError::Limit));
        assert_eq!(s.execute("SELECT max(length(repeat(w, 2048))) FROM t", &[], None), limit);
        assert_eq!(s.execute("SELECT id % 2, sum(length(repeat(w, 2048))) FROM t GROUP BY 1", &[], None), limit);
    }
    let _ = std::fs::remove_dir_all(&path);
}
