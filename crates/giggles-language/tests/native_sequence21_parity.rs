//! Profile 0.21 sequences: native text vs the same SQL on paired fresh databases.
use giggles_core::{Admin, Engine as Storage, Limits};
use giggles_core::relational::with_sql_parser_forbidden as trap;
use giggles_language::*;
use std::{path::PathBuf, sync::atomic::{AtomicU64, Ordering}};
const V: LanguageVersion = LanguageVersion::Embedded021;
struct Db(PathBuf);
impl Db {
    fn fresh(storage: Storage) -> (Self, Admin) {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!("sequence21-{}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
        let admin = Admin::create_engine(&path, Limits::default(), storage).unwrap();
        (Self(path), admin)
    }
}
impl Drop for Db { fn drop(&mut self) { std::fs::remove_dir_all(&self.0).unwrap(); } }
fn storages() -> Vec<Storage> { vec![Storage::Legacy, #[cfg(feature = "engine-rocksdb")] Storage::V2] }
fn at(s: &mut giggles_core::SqlSession, q: &str) -> std::result::Result<giggles_core::relational::SqlResult, giggles_core::CoreError> {
    // Volatile functions (nextval, defaults) need a statement clock in repo SQL.
    let clock = giggles_core::relational::StatementClock { unavailable: None, transaction_us: 100, statement_us: 100, zone: None, wall: None };
    s.execute_at(q, &[], None, &clock)
}
fn snapshot(a: &Admin) -> String {
    let mut m = a.sql_session().catalog_metadata().unwrap(); m.presentation.epoch = 0;
    format!("{:?}", (&m.tables, &m.indexes, &m.presentation))
}
fn rows(a: &Admin, q: &str) -> Vec<Vec<Value>> {
    at(&mut a.sql_session(), q).unwrap().rows.into_iter().map(|r| r.values).collect()
}
/// One SQL statement on a fresh session (a new connection, like a standalone native call).
fn sql_value(a: &Admin, q: &str) -> i64 {
    match rows(a, q).as_slice() { [row] => match row.as_slice() { [Value::Int(v)] => *v, other => panic!("{other:?}") }, other => panic!("{other:?}") }
}
fn sql_state(a: &Admin, q: &str) -> String {
    giggles_core::sqlstate(&at(&mut a.sql_session(), q).unwrap_err()).to_owned()
}
fn sql_ok(a: &Admin, q: &str) { at(&mut a.sql_session(), q).unwrap(); }
fn state(err: Error) -> String { err.sqlstate().unwrap_or_else(|| panic!("{err:?}")).to_owned() }
fn emitted(out: &TransactionOutcome, i: usize) -> Value {
    let EmittedValue::Rows { rows, .. } = &out.emitted[i] else { panic!("{:?}", out.emitted) };
    rows[0].values[0].clone()
}
fn setup(storage: Storage) -> (Db, Admin, Db, Admin, Engine) {
    let (l, left) = Db::fresh(storage); let (r, right) = Db::fresh(storage);
    for a in [&left, &right] { sql_ok(a, "CREATE SCHEMA app"); }
    let engine = Engine::with_version(right.clone(), V);
    (l, left, r, right, engine)
}
#[test]
fn sequences_match_sql() {
    for storage in storages() {
        let (_l, left, _r, right, engine) = setup(storage);
        let none = Bindings::new();
        let ddl = |text: &str| trap(|| engine.execute_sequence_catalog_text(text));
        let call = |text: &str, b: &Bindings| trap(|| engine.execute_sequence_text(text, b));
        assert!(ddl("create sequence s").unwrap().is_some()); sql_ok(&left, "CREATE SEQUENCE s");
        assert!(ddl("create sequence app.s").unwrap().is_some()); sql_ok(&left, "CREATE SEQUENCE app.s");
        assert_eq!(snapshot(&left), snapshot(&right));
        trap(|| engine.execute_catalog_text("create table t (id int64 default nextval(s), v text, primary key (id))")).unwrap();
        sql_ok(&left, "CREATE TABLE t (id BIGINT NOT NULL DEFAULT nextval('s'), v TEXT NOT NULL, PRIMARY KEY(id))");
        assert_eq!(snapshot(&left), snapshot(&right));
        for _ in 0..3 { assert_eq!(call("nextval(s)", &none).unwrap(), sql_value(&left, "SELECT nextval('s')")); }
        assert_eq!(call("nextval(app.s)", &none).unwrap(), 1); assert_eq!(sql_value(&left, "SELECT nextval('app.s')"), 1);
        assert_eq!(call("setval(s, 10)", &none).unwrap(), sql_value(&left, "SELECT setval('s', 10)"));
        assert_eq!(call("nextval(s)", &none).unwrap(), 11); assert_eq!(sql_value(&left, "SELECT nextval('s')"), 11);
        assert_eq!(call("setval(s, 20, false)", &none).unwrap(), sql_value(&left, "SELECT setval('s', 20, false)"));
        assert_eq!(call("nextval(s)", &none).unwrap(), 20); assert_eq!(sql_value(&left, "SELECT nextval('s')"), 20);
        let mut b = Bindings::new(); b.insert("v".into(), Value::Int(30)); b.insert("c".into(), Value::Bool(true));
        assert_eq!(call("setval(s, $v, $c)", &b).unwrap(), 30); assert_eq!(sql_value(&left, "SELECT setval('s', 30, true)"), 30);
        // Standalone calls are fresh sessions, like a new SQL connection.
        assert_eq!(state(call("currval(s)", &none).unwrap_err()), "55000"); assert_eq!(sql_state(&left, "SELECT currval('s')"), "55000");
        let n = trap(|| engine.execute_text("insert into t(v) values(\"a\")", &none)).unwrap();
        assert_eq!(n.affected as u64, at(&mut left.sql_session(), "INSERT INTO t(v) VALUES('a')").unwrap().affected);
        assert_eq!(rows(&left, "SELECT id, v FROM t ORDER BY id"), rows(&right, "SELECT id, v FROM t ORDER BY id"));
        assert_eq!(rows(&right, "SELECT id FROM t"), vec![vec![Value::Int(31)]]);
        assert_eq!(state(ddl("drop sequence s").unwrap_err()), "2BP01"); assert_eq!(sql_state(&left, "DROP SEQUENCE s"), "2BP01");
        assert_eq!(ddl("drop sequence if exists nope").unwrap(), None); sql_ok(&left, "DROP SEQUENCE IF EXISTS nope");
        // PG18.6 skips a missing schema under IF EXISTS; repo SQL reports 42P01 (pinned).
        assert_eq!(ddl("drop sequence if exists nope.s").unwrap(), None); assert_eq!(sql_state(&left, "DROP SEQUENCE IF EXISTS nope.s"), "42P01");
        assert_eq!(ddl("create sequence if not exists app.s").unwrap(), None); sql_ok(&left, "CREATE SEQUENCE IF NOT EXISTS app.s");
        assert_eq!(state(ddl("create sequence s").unwrap_err()), "42P07"); assert_eq!(sql_state(&left, "CREATE SEQUENCE s"), "42P07");
        assert_eq!(state(call("nextval(nope)", &none).unwrap_err()), "42P01"); assert_eq!(sql_state(&left, "SELECT nextval('nope')"), "42P01");
        assert_eq!(state(call("setval(s, 0)", &none).unwrap_err()), sql_state(&left, "SELECT setval('s', 0)"));
        assert_eq!(snapshot(&left), snapshot(&right));
        assert!(ddl("drop sequence s cascade").unwrap().is_some()); sql_ok(&left, "DROP SEQUENCE s CASCADE");
        assert!(ddl("drop sequence app.s restrict").unwrap().is_some()); sql_ok(&left, "DROP SEQUENCE app.s RESTRICT");
        assert_eq!(snapshot(&left), snapshot(&right));
        assert_eq!(state(call("nextval(s)", &none).unwrap_err()), "42P01");
    }
}
#[test]
fn sequence_profile_is_opt_in() {
    let old = LanguageVersion::Embedded020;
    assert!(matches!(parse_catalog_with_version("create sequence s", old), Err(Error::Syntax { .. })));
    assert!(matches!(parse_catalog_with_version("drop sequence s", old), Err(Error::Syntax { .. })));
    assert_eq!(parse_sequence_with_version("nextval(s)", old), Err(Error::Unsupported));
    assert!(parse_transaction_with_version("transaction { nextval(s) }", old).is_err());
    assert_eq!(parse_catalog_with_version("drop sequence if exists app.s cascade", V).unwrap(),
        CatalogStatement::DropSequence { name: "app.s".into(), if_exists: true, cascade: true });
    assert_eq!(parse_sequence_with_version("setval(s, $v, false)", V).unwrap(), SequenceStatement::Setval {
        name: "s".into(), value: ValueExpr::Parameter("v".into()), is_called: Some(ValueExpr::Literal(Value::Bool(false))) });
    for storage in storages() {
        let (_r, right) = Db::fresh(storage);
        let engine = Engine::with_version(right.clone(), V);
        let none = Bindings::new();
        trap(|| engine.execute_sequence_catalog_text("create sequence s")).unwrap();
        // Sequence DDL has its own entry point; the table catalog APIs refuse it.
        assert_eq!(trap(|| engine.execute_catalog_text("create sequence t")).unwrap_err(), Error::Unsupported);
        assert_eq!(trap(|| engine.execute_catalog_optional_text("drop sequence if exists s")).unwrap_err(), Error::Unsupported);
        assert!(matches!(trap(|| engine.prepare_catalog_bound_text("drop sequence s")), Err(Error::Unsupported)));
        assert_eq!(trap(|| engine.execute_sequence_catalog_text("create table x (id int64, primary key (id))")), Err(Error::Unsupported));
        assert_eq!(trap(|| engine.execute_sequence_text("setval(s, \"x\")", &none)), Err(Error::Type));
        assert_eq!(trap(|| engine.execute_sequence_text("setval(s, 1, 2)", &none)), Err(Error::Type));
        assert!(matches!(trap(|| engine.execute_sequence_text("setval(s, $missing)", &none)), Err(Error::Bind(_))));
        assert!(matches!(trap(|| engine.execute_sequence_text("nextval(s) x", &none)), Err(Error::Syntax { .. })));
        assert_eq!(trap(|| Engine::with_version(right.clone(), old).execute_sequence_text("nextval(s)", &none)), Err(Error::Unsupported));
        assert!(matches!(trap(|| engine.prepare_transaction_bound_text("transaction { nextval(s) }", &none)), Err(Error::Unsupported)));
    }
}
#[test]
fn block_sequence_calls_match_sql() {
    for storage in storages() {
        let (_l, left, _r, right, engine) = setup(storage);
        let none = Bindings::new();
        trap(|| engine.execute_sequence_catalog_text("create sequence s")).unwrap(); sql_ok(&left, "CREATE SEQUENCE s");
        trap(|| engine.execute_catalog_text("create table t (id int64 default nextval(s), v text, primary key (id))")).unwrap();
        sql_ok(&left, "CREATE TABLE t (id BIGINT NOT NULL DEFAULT nextval('s'), v TEXT NOT NULL, PRIMARY KEY(id))");
        let block = |text: &str| trap(|| engine.execute_transaction_text(text, &none));
        // Session-scoped currval inside one block, like one SQL session.
        let out = block("transaction { let a = nextval(s); let b = currval(s); insert into t(v) values(\"x\"); emit @a.rows, @b.rows }").unwrap();
        let mut sql = left.sql_session();
        at(&mut sql, "BEGIN").unwrap();
        let a = at(&mut sql, "SELECT nextval('s')").unwrap().rows[0].values[0].clone();
        let b = at(&mut sql, "SELECT currval('s')").unwrap().rows[0].values[0].clone();
        at(&mut sql, "INSERT INTO t(v) VALUES('x')").unwrap();
        at(&mut sql, "COMMIT").unwrap();
        assert_eq!((emitted(&out, 0), emitted(&out, 1)), (a, b));
        assert_eq!(emitted(&out, 0), Value::Int(1));
        assert_eq!(out.summaries.iter().map(|s| s.item_index).collect::<Vec<_>>(), vec![0, 1, 2]);
        assert_eq!(rows(&left, "SELECT id, v FROM t ORDER BY id"), rows(&right, "SELECT id, v FROM t ORDER BY id"));
        // A failing sequence call recovers through ROLLBACK TO.
        let out = block("transaction { nextval(s); savepoint p; currval(nope); rollback to p; let c = currval(s); emit @c.rows }").unwrap();
        at(&mut sql, "BEGIN").unwrap();
        at(&mut sql, "SELECT nextval('s')").unwrap();
        at(&mut sql, "SAVEPOINT p").unwrap();
        assert_eq!(giggles_core::sqlstate(&at(&mut sql, "SELECT currval('nope')").unwrap_err()), "42P01");
        at(&mut sql, "ROLLBACK TO p").unwrap();
        let c = at(&mut sql, "SELECT currval('s')").unwrap().rows[0].values[0].clone();
        at(&mut sql, "COMMIT").unwrap();
        assert_eq!(emitted(&out, 0), c); assert_eq!(c, Value::Int(3));
        assert_eq!(out.summaries.iter().map(|s| s.item_index).collect::<Vec<_>>(), vec![0, 4]);
        // A failed block rolls back its rows but not nextval, in both.
        let err = block("transaction { nextval(s); insert into t(id, v) values(2, \"dup\") }").unwrap_err();
        assert_eq!(err, Error::Duplicate);
        at(&mut sql, "BEGIN").unwrap();
        at(&mut sql, "SELECT nextval('s')").unwrap();
        assert_eq!(giggles_core::sqlstate(&at(&mut sql, "INSERT INTO t(id, v) VALUES(2, 'dup')").unwrap_err()), "23505");
        at(&mut sql, "ROLLBACK").unwrap();
        assert_eq!(trap(|| engine.execute_sequence_text("nextval(s)", &none)).unwrap(), sql_value(&left, "SELECT nextval('s')"));
        // Read-only blocks: native refuses nextval up front (PG18.6 gives 25006 at the
        // call); repo SQL has no BEGIN READ ONLY to compare with (pinned).
        assert_eq!(block("transaction read only { let a = nextval(s); emit @a.rows }"), Err(Error::Unsupported));
        assert!(at(&mut sql, "BEGIN READ ONLY").is_err());
        assert_eq!(rows(&left, "SELECT id, v FROM t ORDER BY id"), rows(&right, "SELECT id, v FROM t ORDER BY id"));
        assert_eq!(snapshot(&left), snapshot(&right));
    }
}
