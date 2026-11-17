#![cfg(feature = "engine-rocksdb")]
//! R2-PGLIMITS-9: on v2, INSERT ... SELECT from a view, into a self-referencing
//! FK table with forward references, and ON CONFLICT over a view source have
//! no 65,536-row cap. FKs are checked at statement end like PG18 (23503,
//! nothing inserted); a second affect raises 21000. Legacy keeps 54000 (it has no views).
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
            .map(|i| format!("({i},{},'w{i:0>20}')", i % 70_000))
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

#[test]
fn view_selfref_and_conflicts() {
    let dir = SpillDir::new("view-selfref");
    let path = common::fresh("view-selfref");
    {
        let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
        let mut s = admin.sql_session();
        load(&mut s, 150_000);
        s.execute("CREATE VIEW v AS SELECT id, x FROM t WHERE id >= 0", &[], None).unwrap();
        s.execute("CREATE TABLE tree (id int PRIMARY KEY, parent int REFERENCES tree(id))", &[], None).unwrap();
        assert_eq!(affected(&mut s, "INSERT INTO tree SELECT id, CASE WHEN id < 149999 THEN id + 1 END FROM v"), 150_000);
        assert_eq!(ints(&mut s, "SELECT count(*), count(parent) FROM tree"), [150_000, 149_999]);
        s.execute("CREATE TABLE tree2 (id int PRIMARY KEY, parent int REFERENCES tree2(id))", &[], None).unwrap();
        let err = s.execute("INSERT INTO tree2 SELECT id, CASE WHEN id = 60000 THEN 999999 WHEN id < 69999 THEN id + 1 END FROM v WHERE id < 70000", &[], None).unwrap_err();
        assert_eq!(giggles_core::sqlstate(&err), "23503");
        assert_eq!(ints(&mut s, "SELECT count(*) FROM tree2"), [0]);
        for name in ["c", "c2"] {
            s.execute(&format!("CREATE TABLE {name} (id int PRIMARY KEY, n int)"), &[], None).unwrap();
        }
        assert_eq!(affected(&mut s, "INSERT INTO c SELECT id % 60000, id FROM v WHERE id < 70000 ON CONFLICT (id) DO NOTHING"), 60_000);
        let err = s.execute("INSERT INTO c2 SELECT id % 60000, id FROM v WHERE id < 70000 ON CONFLICT (id) DO UPDATE SET n = excluded.n", &[], None).unwrap_err();
        assert_eq!(giggles_core::sqlstate(&err), "21000");
        assert_eq!(ints(&mut s, "SELECT count(*) FROM c2"), [0]);
        assert_eq!(dir.files(), 0);
    }
    let _ = std::fs::remove_dir_all(path);
}

#[test]
fn legacy_self_fk_keeps_cap() {
    let path = common::fresh("legacy-self-fk-cap");
    {
        let admin = Admin::create_engine(&path, Limits::default(), Engine::Legacy).unwrap();
        let mut s = admin.sql_session();
        load(&mut s, 300);
        s.execute("CREATE TABLE tree (id int PRIMARY KEY, parent int REFERENCES tree(id))", &[], None).unwrap();
        assert!(matches!(s.execute("INSERT INTO tree SELECT a.id * 1000 + b.id, NULL FROM t a JOIN t b ON b.id >= 0 ORDER BY 1", &[], None), Err(CoreError::Sql(SqlError::Limit))));
        assert_eq!(ints(&mut s, "SELECT count(*) FROM tree"), [0]);
    }
    let _ = std::fs::remove_dir_all(path);
}
