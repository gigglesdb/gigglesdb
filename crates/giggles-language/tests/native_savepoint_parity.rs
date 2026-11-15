//! Paired savepoint blocks; native execution forbids the SQL parser.
use giggles_core::{Admin, Engine as Storage, Limits, SqlSession};
use giggles_core::relational::with_sql_parser_forbidden;
use giggles_language::*;
use std::{path::PathBuf, sync::atomic::{AtomicU64, Ordering}};
const VERSION: LanguageVersion = LanguageVersion::Embedded019;
struct Database(PathBuf);
impl Database {
    fn fresh(storage: Storage) -> (Self, Admin) {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!("native-savepoint-{}-{}",
            std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
        let admin = Admin::create_engine(&path, Limits::default(), storage).unwrap();
        admin.sql_session().execute("CREATE TABLE t(id BIGINT PRIMARY KEY)", &[], None).unwrap();
        (Self(path), admin)
    }
}
impl Drop for Database { fn drop(&mut self) { std::fs::remove_dir_all(&self.0).unwrap(); } }
fn storages() -> Vec<Storage> {
    vec![Storage::Legacy, #[cfg(feature = "engine-rocksdb")] Storage::V2]
}
fn rows(admin: &Admin) -> Vec<Vec<Value>> {
    admin.sql_session().execute("SELECT id FROM t ORDER BY id", &[], None).unwrap().rows.into_iter().map(|r| r.values).collect()
}
fn snapshot(session: &mut SqlSession) -> String {
    let mut m = session.catalog_metadata().unwrap();
    m.presentation.epoch = 0;
    let rows = session.execute("SELECT id FROM t ORDER BY id", &[], None).unwrap().rows;
    format!("{:?}{rows:?}", (&m.tables, &m.indexes, &m.presentation))
}
fn equal(left: &Admin, right: &Admin) -> String {
    let expected = snapshot(&mut left.sql_session());
    assert_eq!(snapshot(&mut right.sql_session()), expected);
    expected
}
fn sql(admin: &Admin, items: &[&str]) {
    let mut s = admin.sql_session();
    for text in items { s.execute(text, &[], None).unwrap_or_else(|e| panic!("{text}: {e:?}")); }
}
fn native(engine: &Engine, text: &str) -> Result<TransactionOutcome> {
    with_sql_parser_forbidden(|| engine.execute_transaction_text(text, &Bindings::new()))
}
#[test]
fn savepoint_blocks_match_sql() {
    for storage in storages() {
        let (_ld, left) = Database::fresh(storage);
        let (_rd, right) = Database::fresh(storage);
        let engine = Engine::with_version(right.clone(), VERSION);
        sql(&left, &["BEGIN", "INSERT INTO t VALUES(1)", "SAVEPOINT a", "INSERT INTO t VALUES(2)",
            "ROLLBACK TO a", "INSERT INTO t VALUES(3)", "RELEASE a", "COMMIT"]);
        let out = native(&engine, "transaction { insert into t(id) values(1); savepoint a; insert into t(id) values(2); rollback to a; insert into t(id) values(3); release a }").unwrap();
        assert!(out.emitted.is_empty());
        assert_eq!(out.summaries.iter().map(|s| (s.item_index,s.affected)).collect::<Vec<_>>(), [(0,1),(2,1),(4,1)]);
        assert_eq!(left.sql_session().execute("SELECT id FROM t ORDER BY id", &[], None).unwrap().rows
            .into_iter().map(|r| r.values).collect::<Vec<_>>(), [vec![Value::Int(1)],vec![Value::Int(3)]]);
        equal(&left, &right);
        sql(&left, &["BEGIN", "SAVEPOINT a", "INSERT INTO t VALUES(4)", "SAVEPOINT a",
            "INSERT INTO t VALUES(5)", "ROLLBACK TO SAVEPOINT a", "RELEASE SAVEPOINT a", "COMMIT"]);
        native(&engine, "transaction { savepoint a; insert into t(id) values(4); savepoint a; insert into t(id) values(5); rollback to savepoint a; release savepoint a }").unwrap();
        assert_eq!(rows(&right), [vec![Value::Int(1)], vec![Value::Int(3)], vec![Value::Int(4)]]);
        let before = equal(&left, &right);
        // Native read_only is a rollback policy; SQL currently rejects this spelling.
        let err = left.sql_session().execute("BEGIN READ ONLY", &[], None).unwrap_err();
        assert_eq!(giggles_core::sqlstate(&err), "0A000");
        sql(&left, &["BEGIN", "SAVEPOINT a", "SELECT id FROM t", "ROLLBACK TO a", "ROLLBACK"]);
        native(&engine, "transaction read only { savepoint a; t select id; rollback to a }").unwrap();
        assert_eq!(equal(&left, &right), before);
    }
}
#[test]
fn savepoint_errors_match_sql() {
    for storage in storages() {
        let (_ld, left) = Database::fresh(storage);
        let (_rd, right) = Database::fresh(storage);
        let engine = Engine::with_version(right.clone(), VERSION);
        for (prefix, target) in [(vec![], "missing"), (vec!["SAVEPOINT a", "RELEASE a"], "a")] {
            let before = equal(&left, &right);
            let mut s = left.sql_session();
            s.execute("BEGIN", &[], None).unwrap();
            for text in &prefix { s.execute(text, &[], None).unwrap(); }
            let sql_err = s.execute(&format!("ROLLBACK TO {target}"), &[], None).unwrap_err();
            assert_eq!(giggles_core::sqlstate(&sql_err), "3B001");
            s.execute("ROLLBACK", &[], None).unwrap();
            let items = if prefix.is_empty() { "" } else { "savepoint a; release a; " };
            let err = native(&engine, &format!("transaction {{ {items}rollback to {target} }}")).unwrap_err();
            assert!(matches!(&err, Error::Sql { sqlstate } if sqlstate == "3B001"));
            assert_eq!(err.sqlstate(), Some(giggles_core::sqlstate(&sql_err)));
            assert_eq!(equal(&left, &right), before);
        }
        let err = native(&engine, "transaction { insert into t(id) values(1); savepoint a; insert into t(id) values(1) }").unwrap_err();
        assert_eq!(err.sqlstate(), Some("23505"));
        equal(&left, &right); // Whole native block rolled back, including the first INSERT.
        let mut s = left.sql_session();
        for text in ["BEGIN", "INSERT INTO t VALUES(1)", "SAVEPOINT a"] { s.execute(text, &[], None).unwrap(); }
        let err = s.execute("INSERT INTO t VALUES(1)", &[], None).unwrap_err();
        assert_eq!(giggles_core::sqlstate(&err), "23505");
        for text in ["ROLLBACK TO a", "COMMIT"] { s.execute(text, &[], None).unwrap(); }
        assert_ne!(snapshot(&mut s), snapshot(&mut right.sql_session()));
        assert_eq!(rows(&left), [vec![Value::Int(1)]]);
        assert!(rows(&right).is_empty());
    }
}
#[test]
fn savepoint_blocks_are_opt_in() {
    for storage in storages() {
        let (_db, admin) = Database::fresh(storage);
        let engine = Engine::with_version(admin.clone(), VERSION);
        let before = snapshot(&mut admin.sql_session());
        let block = "transaction { insert into t(id) values(1); savepoint a }";
        assert!(native(&Engine::with_version(admin.clone(), LanguageVersion::Embedded018), block).is_err());
        for text in ["transaction { savepoint a; let x=(t select id) }",
            "transaction { savepoint a; require true }", "transaction { savepoint a; emit 1 }"] {
            assert!(matches!(native(&engine, text), Err(Error::Unsupported)));
        }
        with_sql_parser_forbidden(|| {
            assert!(matches!(engine.prepare_transaction_bound_text(block, &Bindings::new()), Err(Error::Unsupported)));
            assert!(matches!(lower_text(block, &Bindings::new(), VERSION), Err(Error::Unsupported)));
            assert!(matches!(parse_input_with_version("transaction once $key for 1m { savepoint a }", VERSION), Err(Error::Unsupported)));
            let invalid = Transaction { read_only: false, steps: vec![TransactionStep::Savepoint("bad.name".into())], emit: None };
            assert!(matches!(engine.execute_transaction(&invalid, &Bindings::new()), Err(Error::Bind(name)) if name == "identifier"));
            assert!(matches!(Engine::with_version(admin.clone(), LanguageVersion::Embedded018).execute_transaction(&invalid, &Bindings::new()), Err(Error::Unsupported)));
            let ordinary = engine.prepare_transaction_bound_text("transaction { t select id }", &Bindings::new()).unwrap();
            engine.execute_transaction_bound(&ordinary).unwrap();
            let mut bindings = Bindings::new(); bindings.insert("id".into(), Value::Int(7));
            engine.execute_transaction_text("transaction { savepoint a; insert into t(id) values($id); rollback to a }", &bindings).unwrap();
        });
        assert_eq!(snapshot(&mut admin.sql_session()), before);
        for name in ["select", "`a`", "\"a\""] {
            assert!(parse_transaction_with_version(&format!("transaction {{ savepoint {name} }}"), VERSION).is_err());
        }
        assert!(parse_transaction_with_version("transaction { `rollback` select id }", VERSION).is_ok());
        for table in ["savepoint", "rollback", "release"] {
            let text = format!("transaction {{ {table} select id }}");
            assert_eq!(parse_transaction_with_version(&text, VERSION).is_ok(),
                parse_transaction_with_version(&text, LanguageVersion::Embedded018).is_ok());
        }
    }
}
