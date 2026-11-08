//! PostgreSQL 18.6 oracle: e3-query/wo-in-subquery/pg-probes2.txt.
use giggles_core::{
    relational::{SqlError as E, SqlResult, Value as V},
    *,
};
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Db {
    session: Option<SqlSession>,
    admin: Option<Admin>,
    path: PathBuf,
}
impl Db {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "gd-in-subquery-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let admin = Admin::create(&path, Limits::default()).unwrap();
        let session = admin.sql_session();
        Self {
            session: Some(session),
            admin: Some(admin),
            path,
        }
    }
    fn session(&mut self) -> &mut SqlSession {
        self.session.as_mut().unwrap()
    }
}
impl Drop for Db {
    fn drop(&mut self) {
        drop(self.session.take());
        drop(self.admin.take());
        std::fs::remove_dir_all(&self.path).unwrap();
    }
}
fn run(s: &mut SqlSession, sql: &str) -> SqlResult {
    s.execute(sql, &[], None)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
}
fn unsupported(s: &mut SqlSession, sql: &str) {
    assert_eq!(
        s.execute(sql, &[], None),
        Err(CoreError::Sql(E::Unsupported)),
        "{sql}"
    );
}
#[test]
fn schema_subquery_rejection_leaves_no_tables_or_checks() {
    let mut db = Db::new();
    let s = db.session();
    // PG: "ERROR:  0A000: cannot use subquery in check constraint"; c1_exists=0.
    unsupported(s, "CREATE TABLE c1(a int CHECK(a IN(SELECT 1)))");
    assert_eq!(
        s.execute("SELECT a FROM c1", &[], None),
        Err(CoreError::Sql(E::UnknownTable))
    );
    run(s, "CREATE TABLE c2(a int)");
    // PG: "ERROR:  0A000: cannot use subquery in check constraint"; c2_checks=0.
    unsupported(s, "ALTER TABLE c2 ADD CHECK(a IN(SELECT 1))");
    run(s, "INSERT INTO c2 VALUES(5)");
    assert_eq!(run(s, "SELECT a FROM c2").rows[0].values, vec![V::Int(5)]);
    // A second disallowed value also proves no rejected CHECK became active.
    run(s, "INSERT INTO c2 VALUES(6)");
    // Unchanged from main 0ec7c954 (42601). PG18.6: ERROR 0A000 cannot use subquery in DEFAULT expression; not in this slice.
    assert_eq!(
        s.execute(
            "CREATE TABLE d1(a int DEFAULT(1 IN(SELECT 1))::int)",
            &[],
            None
        ),
        Err(CoreError::Sql(E::Syntax))
    );
    assert_eq!(
        s.execute("SELECT a FROM d1", &[], None),
        Err(CoreError::Sql(E::UnknownTable))
    );
}
#[test]
fn insert_value_subquery_remains_unsupported_and_preserves_rows() {
    let mut db = Db::new();
    let s = db.session();
    run(s, "CREATE TABLE u(id int PRIMARY KEY)");
    run(s, "INSERT INTO u VALUES(2)");
    run(s, "CREATE TABLE v(id int PRIMARY KEY,b boolean)");
    let before = run(s, "SELECT id,b FROM v ORDER BY id");
    // Unchanged from main 0ec7c954 (42601). PG18.6: INSERT 0 1 and (1,t); not in this slice.
    assert_eq!(
        s.execute("INSERT INTO v VALUES(1,2 IN(SELECT id FROM u))", &[], None),
        Err(CoreError::Sql(E::Syntax))
    );
    assert_eq!(run(s, "SELECT id,b FROM v ORDER BY id"), before);
}
#[test]
fn conflict_subquery_contexts_remain_unsupported_and_preserve_versions() {
    let mut db = Db::new();
    let s = db.session();
    run(s, "CREATE TABLE u(id int PRIMARY KEY)");
    run(s, "INSERT INTO u VALUES(2)");
    run(s, "CREATE TABLE v(id int PRIMARY KEY,b boolean)");
    run(s, "INSERT INTO v VALUES(1,true),(2,true)");
    let before = run(s, "SELECT id,b FROM v ORDER BY id");
    // PG: "INSERT 0 1"; rows after conflict WHERE are (1,f),(2,t).
    unsupported(
        s,
        "INSERT INTO v VALUES(1,false) ON CONFLICT(id) DO UPDATE SET b=NOT v.b WHERE v.id IN(SELECT 1)",
    );
    assert_eq!(run(s, "SELECT id,b FROM v ORDER BY id"), before);
    // PG: "INSERT 0 1"; rows after conflict SET are (1,f),(2,t).
    unsupported(
        s,
        "INSERT INTO v VALUES(1,false) ON CONFLICT(id) DO UPDATE SET b=v.id IN(SELECT id FROM u)",
    );
    assert_eq!(run(s, "SELECT id,b FROM v ORDER BY id"), before);
}
