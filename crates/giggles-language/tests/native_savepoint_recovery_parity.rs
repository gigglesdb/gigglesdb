//! Paired savepoint blocks; native execution forbids the SQL parser.
use giggles_core::{Admin, Engine as Storage, Limits, SqlSession};
use giggles_core::relational::with_sql_parser_forbidden;
use giggles_language::*;
use std::{path::PathBuf, sync::atomic::{AtomicU64, Ordering}};
const VERSION: LanguageVersion = LanguageVersion::Embedded020;
struct Database(PathBuf);
impl Database {
    fn fresh(storage: Storage) -> (Self, Admin) {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!("native-savepoint-recovery-{}-{}",
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
fn native(engine: &Engine, text: &str) -> Result<TransactionOutcome> {
    with_sql_parser_forbidden(|| engine.execute_transaction_text(text, &Bindings::new()))
}
fn sql_error(s: &mut SqlSession, text: &str, state: &str) -> String {
    let err = s.execute(text, &[], None).unwrap_err();
    assert_eq!(giggles_core::sqlstate(&err), state);
    giggles_core::sqlstate(&err).to_owned()
}
fn native_error(engine: &Engine, text: &str, expected: &str) {
    let err = native(engine, text).unwrap_err();
    if expected == "23505" { assert!(matches!(err, Error::Duplicate)); }
    else { assert!(matches!(&err, Error::Sql { sqlstate } if sqlstate == expected), "{text}: {err:?}, expected {expected}"); }
    assert_eq!(err.sqlstate(), Some(expected));
}
#[test]
fn rollback_to_recovers_like_sql() {
    for storage in storages() {
        for (sql_steps, block, indices) in [
            (vec!["BEGIN", "INSERT INTO t VALUES(1)", "SAVEPOINT a", "!INSERT INTO t VALUES(1)", "ROLLBACK TO a", "INSERT INTO t VALUES(2)", "COMMIT"],
             "transaction { insert into t(id) values(1); savepoint a; insert into t(id) values(1); rollback to a; insert into t(id) values(2) }", vec![0,4]),
            (vec!["BEGIN", "INSERT INTO t VALUES(1)", "SAVEPOINT a", "SAVEPOINT b", "!INSERT INTO t VALUES(1)", "ROLLBACK TO b", "INSERT INTO t VALUES(2)", "RELEASE a", "COMMIT"],
             "transaction { insert into t(id) values(1); savepoint a; savepoint b; insert into t(id) values(1); rollback to b; insert into t(id) values(2); release a }", vec![0,5]),
            (vec!["BEGIN", "INSERT INTO t VALUES(1)", "SAVEPOINT a", "!INSERT INTO t VALUES(1)", "ROLLBACK TO a", "INSERT INTO t VALUES(2)", "SAVEPOINT b", "!INSERT INTO t VALUES(2)", "ROLLBACK TO b", "COMMIT"],
             "transaction { insert into t(id) values(1); savepoint a; insert into t(id) values(1); rollback to a; insert into t(id) values(2); savepoint b; insert into t(id) values(2); rollback to b }", vec![0,4]),
        ] {
            let (_ld, left) = Database::fresh(storage);
            let (_rd, right) = Database::fresh(storage);
            let mut s = left.sql_session();
            for text in sql_steps {
                if let Some(text) = text.strip_prefix('!') { sql_error(&mut s, text, "23505"); }
                else {
                    let out = s.execute(text, &[], None).unwrap();
                    if text.starts_with("INSERT") { assert_eq!(out.affected, 1); }
                }
            }
            let out = native(&Engine::with_version(right.clone(), VERSION), block).unwrap();
            assert!(out.emitted.is_empty());
            assert_eq!(out.summaries.iter().map(|s| s.item_index).collect::<Vec<_>>(), indices);
            assert!(out.summaries.iter().all(|s| s.affected == 1));
            assert_eq!(rows(&right), [vec![Value::Int(1)],vec![Value::Int(2)]]);
            equal(&left, &right);
        }
    }
}
#[test]
fn recovery_limits() {
    for storage in storages() {
        for case in 0..4 {
            let (_ld, left) = Database::fresh(storage);
            let (_rd, right) = Database::fresh(storage);
            let version = if case == 3 { LanguageVersion::Embedded019 } else { VERSION };
            let engine = Engine::with_version(right.clone(), version);
            let before = equal(&left, &right);
            let mut s = left.sql_session();
            for text in ["BEGIN", "INSERT INTO t VALUES(1)", "SAVEPOINT a"] { s.execute(text, &[], None).unwrap(); }
            let duplicate = sql_error(&mut s, "INSERT INTO t VALUES(1)", "23505");
            let (tail, expected) = match case {
                0 => {
                    s.execute("ROLLBACK TO a", &[], None).unwrap();
                    s.execute("INSERT INTO t VALUES(2)", &[], None).unwrap();
                    let missing = sql_error(&mut s, "INSERT INTO missing VALUES(3)", "42P01");
                    s.execute("ROLLBACK", &[], None).unwrap();
                    ("rollback to a; insert into t(id) values(2); insert into missing(id) values(3)", missing)
                }
                1 => {
                    sql_error(&mut s, "INSERT INTO t VALUES(2)", "25P02");
                    s.execute("ROLLBACK TO a", &[], None).unwrap();
                    s.execute("COMMIT", &[], None).unwrap();
                    ("insert into t(id) values(2); rollback to a", duplicate)
                }
                2 => {
                    sql_error(&mut s, "ROLLBACK TO nope", "3B001");
                    s.execute("ROLLBACK", &[], None).unwrap();
                    ("rollback to nope", duplicate)
                }
                _ => {
                    s.execute("ROLLBACK TO a", &[], None).unwrap();
                    s.execute("INSERT INTO t VALUES(2)", &[], None).unwrap();
                    s.execute("COMMIT", &[], None).unwrap();
                    ("rollback to a; insert into t(id) values(2)", duplicate)
                }
            };
            native_error(&engine, &format!("transaction {{ insert into t(id) values(1); savepoint a; insert into t(id) values(1); {tail} }}"), &expected);
            assert!(rows(&right).is_empty());
            assert_eq!(snapshot(&mut right.sql_session()), before);
            if case == 1 || case == 3 {
                assert_eq!(rows(&left), if case == 1 { vec![vec![Value::Int(1)]] } else { vec![vec![Value::Int(1)],vec![Value::Int(2)]] });
                assert_ne!(snapshot(&mut s), snapshot(&mut right.sql_session()));
                let mut l = s.catalog_metadata().unwrap();
                let mut r = right.sql_session().catalog_metadata().unwrap();
                l.presentation.epoch = 0; r.presentation.epoch = 0;
                assert_eq!(format!("{:?}", (&l.tables,&l.indexes,&l.presentation)), format!("{:?}", (&r.tables,&r.indexes,&r.presentation)));
            } else { equal(&left, &right); }
        }
    }
}
