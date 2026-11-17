#![cfg(feature = "engine-rocksdb")]
//! Heap arbiters and referenced-key updates retain statement-wide semantics
//! across v2 insert pages; legacy retains its complete-image admission limit.
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
    s.execute("CREATE TABLE t (id int PRIMARY KEY)", &[], None)
        .unwrap();
    for start in (1..=rows).step_by(10_000) {
        let values = (start..(start + 10_000).min(rows + 1))
            .map(|i| format!("({i})"))
            .collect::<Vec<_>>()
            .join(",");
        affected(s, &format!("INSERT INTO t VALUES {values}"));
    }
}
fn ints(s: &mut SqlSession, sql: &str) -> Vec<i64> {
    let r = s
        .execute(sql, &[], None)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    assert_eq!(r.rows.len(), 1, "{sql}");
    r.rows[0]
        .values
        .iter()
        .map(|v| match v {
            Value::Int(n) => *n,
            Value::Numeric(n) => n.to_string().parse().unwrap(),
            other => panic!("{other:?}"),
        })
        .collect()
}
fn affected(s: &mut SqlSession, sql: &str) -> u64 {
    s.execute(sql, &[], None)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
        .affected
}

#[test]
fn heap_conflicts_and_legacy_cap() {
    let dir = SpillDir::new("on-conflict-heap");
    let path = common::fresh("on-conflict-heap");
    {
        let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
        let mut s = admin.sql_session();
        load(&mut s, 150_000);
        affected(&mut s, "CREATE TABLE u (k int UNIQUE, v int)");
        assert_eq!(
            affected(&mut s, "INSERT INTO u SELECT id, id FROM t"),
            150_000
        );
        assert_eq!(
            affected(
                &mut s,
                "INSERT INTO u SELECT id, id FROM t ON CONFLICT (k) DO UPDATE SET v = excluded.v + u.v"
            ),
            150_000
        );
        let before = ints(&mut s, "SELECT count(*), sum(v) FROM u");
        assert_eq!(before, [150_000, 22_500_150_000]);
        let err = s.execute("INSERT INTO u SELECT CASE WHEN id = 100001 THEN 1 ELSE id END, id FROM t ON CONFLICT (k) DO UPDATE SET v = excluded.v + u.v", &[], None).unwrap_err();
        assert_eq!(giggles_core::sqlstate(&err), "21000");
        assert_eq!(ints(&mut s, "SELECT count(*), sum(v) FROM u"), before);
        assert_eq!(
            affected(
                &mut s,
                "INSERT INTO u SELECT id + 150000, id FROM t ON CONFLICT ON CONSTRAINT u_k_key DO NOTHING"
            ),
            150_000
        );
        assert_eq!(
            ints(&mut s, "SELECT count(*), sum(v) FROM u"),
            [300_000, 33_750_225_000]
        );
        // NULL arbiters do not witness a conflict, and fresh rowids must also
        // avoid existing heap rows omitted from the witness image.
        assert_eq!(
            affected(
                &mut s,
                "INSERT INTO u VALUES (NULL,1),(NULL,2),(300001,3) ON CONFLICT (k) DO UPDATE SET v = excluded.v"
            ),
            3
        );
        assert_eq!(
            ints(&mut s, "SELECT count(*), count(k) FROM u"),
            [300_003, 300_001]
        );
        assert_eq!(dir.files(), 0);
    }
    let _ = std::fs::remove_dir_all(path);
    let path = common::fresh("on-conflict-heap-legacy");
    {
        let admin = Admin::create_engine(&path, Limits::default(), Engine::Legacy).unwrap();
        let mut s = admin.sql_session();
        load(&mut s, 280);
        affected(&mut s, "CREATE TABLE u (k int UNIQUE, v int)");
        let err = s.execute("INSERT INTO u SELECT a.id * 1000 + b.id, a.id FROM t a JOIN t b ON b.id >= 0 ON CONFLICT (k) DO UPDATE SET v = excluded.v", &[], None).unwrap_err();
        assert!(matches!(err, CoreError::Sql(SqlError::Limit)), "{err:?}");
        assert_eq!(giggles_core::sqlstate(&err), "54000");
        assert_eq!(ints(&mut s, "SELECT count(*) FROM u"), [0]);
        assert_eq!(dir.files(), 0);
    }
    let _ = std::fs::remove_dir_all(path);
}

#[test]
fn referenced_key_actions_across_pages() {
    let dir = SpillDir::new("on-conflict-parent");
    let path = common::fresh("on-conflict-parent");
    {
        let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
        let mut s = admin.sql_session();
        load(&mut s, 150_000);
        affected(&mut s, "CREATE TABLE p (id int PRIMARY KEY, k int UNIQUE)");
        affected(&mut s, "INSERT INTO p SELECT id, id FROM t");
        affected(
            &mut s,
            "CREATE TABLE c_cas (pk int REFERENCES p(k) ON UPDATE CASCADE)",
        );
        affected(&mut s, "CREATE INDEX c_cas_pk ON c_cas(pk)");
        // Small fixture batches avoid the pre-existing quadratic certification
        // of a large outgoing-FK append, while retaining the original FK schema.
        affected(&mut s, "BEGIN");
        for start in (1..=150_000).step_by(8) {
            let values = (start..start + 8)
                .map(|id| format!("({id})"))
                .collect::<Vec<_>>()
                .join(",");
            s.execute(&format!("INSERT INTO c_cas VALUES {values}"), &[], None)
                .unwrap();
        }
        affected(&mut s, "COMMIT");
        assert_eq!(
            affected(
                &mut s,
                "INSERT INTO p SELECT id, id FROM t ON CONFLICT (id) DO UPDATE SET k = p.k + 1000000"
            ),
            150_000
        );
        assert_eq!(
            ints(&mut s, "SELECT count(*), min(pk), max(pk) FROM c_cas"),
            [150_000, 1_000_001, 1_150_000]
        );
        affected(&mut s, "CREATE TABLE c_na (pk int REFERENCES p(k))");
        affected(&mut s, "INSERT INTO c_na VALUES (1000005)");
        let before = ints(&mut s, "SELECT count(*), sum(k) FROM p");
        let err = s.execute("INSERT INTO p SELECT id, id FROM t WHERE id <= 70000 ON CONFLICT (id) DO UPDATE SET k = CASE WHEN p.id = 5 THEN p.k + 1000000 ELSE p.k END", &[], None).unwrap_err();
        assert_eq!(giggles_core::sqlstate(&err), "23503");
        assert_eq!(ints(&mut s, "SELECT count(*), sum(k) FROM p"), before);
        assert_eq!(
            ints(&mut s, "SELECT count(*), min(pk), max(pk) FROM c_cas"),
            [150_000, 1_000_001, 1_150_000]
        );
        assert_eq!(
            affected(
                &mut s,
                "INSERT INTO p SELECT id, CASE WHEN id = 5 THEN 20000005 WHEN id = 70000 THEN 1000005 ELSE id + 1000000 END FROM t WHERE id <= 70000 ON CONFLICT (id) DO UPDATE SET k = excluded.k"
            ),
            70_000
        );
        assert_eq!(ints(&mut s, "SELECT k FROM p WHERE id = 5"), [20_000_005]);
        assert_eq!(
            ints(&mut s, "SELECT k FROM p WHERE id = 70000"),
            [1_000_005]
        );
        assert_eq!(ints(&mut s, "SELECT pk FROM c_na"), [1_000_005]);
        assert_eq!(
            ints(&mut s, "SELECT count(*) FROM c_cas WHERE pk = 20000005"),
            [1]
        );
        for (parent, child, action) in [("rp", "rc", "RESTRICT"), ("np", "nc", "SET NULL")] {
            affected(
                &mut s,
                &format!("CREATE TABLE {parent} (id int PRIMARY KEY, k int UNIQUE)"),
            );
            affected(&mut s, &format!("INSERT INTO {parent} VALUES (1,1),(2,2)"));
            affected(
                &mut s,
                &format!("CREATE TABLE {child} (pk int REFERENCES {parent}(k) ON UPDATE {action})"),
            );
            affected(&mut s, &format!("INSERT INTO {child} VALUES (1)"));
        }
        let err = s
            .execute(
                "INSERT INTO rp SELECT id, CASE WHEN id = 1 THEN 3 ELSE 1 END FROM t WHERE id <= 2 ON CONFLICT (id) DO UPDATE SET k = excluded.k",
                &[],
                None,
            )
            .unwrap_err();
        assert!(
            matches!(err, CoreError::Sql(SqlError::RestrictViolation)),
            "{err:?}"
        );
        assert_eq!(giggles_core::sqlstate(&err), "23001");
        assert_eq!(ints(&mut s, "SELECT sum(k) FROM rp"), [3]);
        affected(
            &mut s,
            "INSERT INTO np SELECT id, CASE WHEN id = 1 THEN 3 ELSE 1 END FROM t WHERE id <= 2 ON CONFLICT (id) DO UPDATE SET k = excluded.k",
        );
        assert_eq!(ints(&mut s, "SELECT count(*), count(pk) FROM nc"), [1, 0]);
        affected(&mut s, "CREATE TABLE dp (id int PRIMARY KEY, k int UNIQUE)");
        affected(
            &mut s,
            "INSERT INTO dp SELECT id, id FROM t WHERE id <= 201",
        );
        affected(
            &mut s,
            "CREATE TABLE dc (pk int REFERENCES dp(k) DEFERRABLE INITIALLY DEFERRED)",
        );
        affected(&mut s, "INSERT INTO dc VALUES (1)");
        affected(&mut s, "BEGIN");
        assert_eq!(
            affected(
                &mut s,
                "INSERT INTO dp SELECT id, id + 1000000 FROM t WHERE id <= 201 ON CONFLICT (id) DO UPDATE SET k = excluded.k"
            ),
            201
        );
        affected(&mut s, "UPDATE dc SET pk = 1000001");
        affected(&mut s, "COMMIT");
        assert_eq!(ints(&mut s, "SELECT pk FROM dc"), [1_000_001]);
        affected(&mut s, "BEGIN");
        affected(
            &mut s,
            "INSERT INTO dp SELECT id, id FROM t WHERE id <= 201 ON CONFLICT (id) DO UPDATE SET k = excluded.k",
        );
        let err = s
            .execute("SET CONSTRAINTS ALL IMMEDIATE", &[], None)
            .unwrap_err();
        assert_eq!(giggles_core::sqlstate(&err), "23503");
        affected(&mut s, "ROLLBACK");
        assert_eq!(ints(&mut s, "SELECT k FROM dp WHERE id = 1"), [1_000_001]);
        assert_eq!(dir.files(), 0);
    }
    let _ = std::fs::remove_dir_all(path);
}
