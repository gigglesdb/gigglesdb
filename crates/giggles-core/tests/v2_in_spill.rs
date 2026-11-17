#![cfg(feature = "engine-rocksdb")]
//! R2-PGLIMITS-8: indexed, bounded-memory membership past the IN hash budget.
mod common;
use giggles_core::{Admin, Engine, Limits, SqlSession, relational::{Value, spill}};
use std::path::PathBuf;
use std::time::{Duration, Instant};

struct SpillDir(PathBuf);
impl SpillDir {
    fn new(name: &str) -> Self {
        let dir = common::fresh(&format!("{name}-spill"));
        std::fs::create_dir_all(&dir).unwrap();
        spill::set_thread_dir(Some(dir.clone()));
        spill::set_in_hash_bytes(Some(64 * 1024));
        Self(dir)
    }
    fn count(&self, s: &mut SqlSession, predicate: &str, expected: i64) {
        let sql = format!("SELECT count(*) FROM u WHERE {predicate}");
        let result = s.execute(&sql, &[], None).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
        assert_eq!(result.rows[0].values, [Value::Int(expected)], "{sql}");
        assert_eq!(std::fs::read_dir(&self.0).unwrap().count(), 0, "{sql}");
    }
}
impl Drop for SpillDir {
    fn drop(&mut self) {
        spill::set_in_hash_bytes(None);
        spill::set_thread_dir(None);
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn insert(s: &mut SqlSession, rows: i64, row: impl Fn(i64) -> String) {
    for start in (0..rows).step_by(10_000) {
        let values = (start..(start + 10_000).min(rows)).map(&row).collect::<Vec<_>>().join(",");
        s.execute(&format!("INSERT INTO u VALUES {values}"), &[], None).unwrap();
    }
}

#[test]
fn indexed_int_membership_nulls_and_200k_probe_speed() {
    const N: i64 = 200_000;
    let dir = SpillDir::new("v2-in-int");
    let path = common::fresh("v2-in-int");
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    let mut s = admin.sql_session();
    s.execute("CREATE TABLE u (id int PRIMARY KEY, v int)", &[], None).unwrap();
    insert(&mut s, N, |i| format!("({i},{i})"));
    let start = Instant::now();
    dir.count(&mut s, "id IN (SELECT v FROM u)", N);
    let elapsed = start.elapsed();
    eprintln!("200k outer x 200k distinct IN keys: {elapsed:?}");
    assert!(elapsed < Duration::from_secs(60), "indexed membership took {elapsed:?}");
    dir.count(&mut s, "v + 100000 IN (SELECT v FROM u)", N / 2);
    dir.count(&mut s, "v + 100000 NOT IN (SELECT v FROM u)", N / 2);

    s.execute("INSERT INTO u VALUES (200000,NULL),(200001,NULL)", &[], None).unwrap();
    dir.count(&mut s, "v + 100000 IN (SELECT v FROM u)", N / 2);
    dir.count(&mut s, "v + 100000 NOT IN (SELECT v FROM u)", 0);
    dir.count(&mut s, "(v + 100000 NOT IN (SELECT v FROM u)) IS NULL", N / 2 + 2);
    dir.count(&mut s, "v + 100000 NOT IN (SELECT v FROM u WHERE v IS NOT NULL)", N / 2);
    dir.count(&mut s, "v IN (SELECT v FROM u WHERE false)", 0);
    dir.count(&mut s, "v NOT IN (SELECT v FROM u WHERE false)", N + 2);

    // The production threshold keeps this small materialization in its hash set.
    spill::set_in_hash_bytes(None);
    dir.count(&mut s, "v IN (SELECT v FROM u WHERE id < 10)", 10);
    drop(s);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn indexed_text_membership_preserves_exact_keys() {
    const N: i64 = 100_000;
    let dir = SpillDir::new("v2-in-text");
    let path = common::fresh("v2-in-text");
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    let mut s = admin.sql_session();
    s.execute("CREATE TABLE u (id int PRIMARY KEY, v text)", &[], None).unwrap();
    insert(&mut s, N, |i| format!("({i},'key-{i:06}   ' )"));
    dir.count(&mut s, "v IN (SELECT v FROM u WHERE id < 50000)", N / 2);
    dir.count(&mut s, "v NOT IN (SELECT v FROM u WHERE id < 50000)", N / 2);
    s.execute("INSERT INTO u VALUES (100000,'key-000000'),(100001,NULL)", &[], None).unwrap();
    dir.count(&mut s, "v IN (SELECT v FROM u WHERE id < 50000)", N / 2);
    dir.count(&mut s, "v NOT IN (SELECT v FROM u WHERE id < 50000)", N / 2 + 1);
    drop(s);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}
