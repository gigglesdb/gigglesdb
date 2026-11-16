//! Paired savepoint blocks; native execution forbids the SQL parser.
use giggles_core::{Admin, Engine as Storage, Limits, SqlSession};
use giggles_core::relational::with_sql_parser_forbidden;
use giggles_language::*;
use std::{path::PathBuf, sync::atomic::{AtomicU64, Ordering}};
const VERSION: LanguageVersion = LanguageVersion::Embedded021;
struct Database(PathBuf);
impl Database {
    fn fresh(storage: Storage) -> (Self, Admin) {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!("native-recovery21-{}-{}",
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
#[test]
fn recovery_profile_is_opt_in_and_inherits_twenty() {
    let old = LanguageVersion::Embedded020;
    assert_eq!(VERSION.as_str(), "0.21");
    assert_eq!(profile_for(VERSION).name, "embedded-trusted-native-0.21");
    assert_eq!(serde_json::to_string(&VERSION).unwrap(), "\"0.21\"");
    assert_eq!(serde_json::from_str::<LanguageVersion>("\"0.21\"").unwrap(), VERSION);
    assert_eq!(profile_for(VERSION).unsupported, profile_for(old).unsupported);
    let manifest = capability_manifest_for(VERSION);
    for prior in capability_manifest_for(old).capabilities {
        let entry = manifest.capabilities.iter().find(|c| c.interface == prior.interface && c.feature == prior.feature).unwrap();
        assert_eq!(entry.state, prior.state);
        if prior.feature != "block_savepoints" { assert_eq!(entry.detail, prior.detail); }
    }
    for text in ["create view v as t select id", "drop view v"] {
        assert_eq!(parse_catalog_with_version(text, old).unwrap(), parse_catalog_with_version(text, VERSION).unwrap());
    }
    for storage in storages() {
        let (_db, admin) = Database::fresh(storage);
        let old_engine = Engine::with_version(admin.clone(), old);
        let block = "transaction { insert into t(id) values(1); savepoint a; insert into t(id) values(1); insert into t(id) values(3); rollback to a; insert into t(id) values(2) }";
        let err = native(&old_engine, block).unwrap_err();
        assert_eq!(err, Error::Duplicate); assert_eq!(err.sqlstate(), Some("23505"));
        assert!(rows(&admin).is_empty());
        let engine = Engine::with_version(admin.clone(), VERSION);
        for version in [old, VERSION] {
            let page = with_sql_parser_forbidden(|| Engine::with_version(admin.clone(), version).read_page_text("page t order by id limit 2 select id", &Bindings::new())).unwrap();
            assert!(page.outcome.rows.is_empty());
        }
        let result = with_sql_parser_forbidden(|| engine.execute_catalog_text("create view v as t select id"));
        if storage == Storage::Legacy { assert_eq!(result, Err(Error::Sql { sqlstate: "0A000".into() })); }
        else { result.unwrap(); with_sql_parser_forbidden(|| engine.execute_catalog_text("drop view v")).unwrap(); }
    }
}
#[test]
fn full_recovery_matches_sql() {
    for storage in storages() {
        for (sql_steps, block, indices, expected_rows) in [
            (vec![("BEGIN", ""), ("INSERT INTO t VALUES(1)", ""), ("SAVEPOINT a", ""), ("INSERT INTO t VALUES(1)", "23505"), ("INSERT INTO t VALUES(3)", "25P02"), ("ROLLBACK TO a", ""), ("INSERT INTO t VALUES(2)", ""), ("COMMIT", "")],
             "transaction { insert into t(id) values(1); savepoint a; insert into t(id) values(1); insert into t(id) values(3); rollback to a; insert into t(id) values(2) }", vec![0,5], vec![vec![Value::Int(1)],vec![Value::Int(2)]]),
            (vec![("BEGIN", ""), ("INSERT INTO t VALUES(1)", ""), ("SAVEPOINT a", ""), ("INSERT INTO t VALUES(1)", "23505"), ("SAVEPOINT b", "25P02"), ("ROLLBACK TO b", "3B001"), ("RELEASE a", "25P02"), ("ROLLBACK TO a", ""), ("INSERT INTO t VALUES(2)", ""), ("COMMIT", "")],
             "transaction { insert into t(id) values(1); savepoint a; insert into t(id) values(1); savepoint b; rollback to b; release a; rollback to a; insert into t(id) values(2) }", vec![0,7], vec![vec![Value::Int(1)],vec![Value::Int(2)]]),
            (vec![("BEGIN", ""), ("INSERT INTO t VALUES(1)", ""), ("SAVEPOINT a", ""), ("SELECT count(*) FROM nope", "42P01"), ("INSERT INTO t VALUES(3)", "25P02"), ("ROLLBACK TO a", ""), ("INSERT INTO t VALUES(2)", ""), ("COMMIT", "")],
             "transaction { insert into t(id) values(1); savepoint a; nope select count(*); insert into t(id) values(3); rollback to a; insert into t(id) values(2) }", vec![0,5], vec![vec![Value::Int(1)],vec![Value::Int(2)]])
        ] {
            let (_ld, left) = Database::fresh(storage); let (_rd, right) = Database::fresh(storage);
            let mut sql = left.sql_session();
            for (text, state) in sql_steps {
                if state.is_empty() {
                    let out = sql.execute(text, &[], None).unwrap();
                    if text.starts_with("INSERT") { assert_eq!(out.affected, 1); }
                } else { sql_error(&mut sql, text, state); }
            }
            let out = native(&Engine::with_version(right.clone(), VERSION), block).unwrap();
            assert!(out.emitted.is_empty());
            assert_eq!(out.summaries.iter().map(|s| s.item_index).collect::<Vec<_>>(), indices);
            assert!(out.summaries.iter().all(|s| s.affected == 1 && s.matched == 1 && s.changed == 1));
            assert_eq!(rows(&right), expected_rows); equal(&left, &right);
        }
        // Ending aborted preserves the first error even after an unknown rollback target.
        for typed in [false, true] {
            let (_ld, left) = Database::fresh(storage); let (_rd, right) = Database::fresh(storage);
            let before = equal(&left, &right); let mut sql = left.sql_session();
            for text in ["BEGIN", "INSERT INTO t VALUES(1)", "SAVEPOINT a"] { sql.execute(text, &[], None).unwrap(); }
            let (bad_sql, bad_native, state) = if typed { ("SELECT count(*) FROM nope", "nope select count(*)", "42P01") }
                else { ("INSERT INTO t VALUES(1)", "insert into t(id) values(1)", "23505") };
            let original = sql.execute(bad_sql, &[], None).unwrap_err();
            assert_eq!(giggles_core::sqlstate(&original), state);
            sql_error(&mut sql, "INSERT INTO t VALUES(3)", "25P02");
            sql_error(&mut sql, "ROLLBACK TO zz", "3B001");
            // PG18.6 COMMIT reports ROLLBACK; repo SQL instead remains aborted with 25P02.
            // Native returns the original error and rolls back; explicitly roll SQL back to compare.
            sql_error(&mut sql, "COMMIT", "25P02");
            sql.execute("ROLLBACK", &[], None).unwrap();
            let err = native(&Engine::with_version(right.clone(), VERSION), &format!("transaction {{ insert into t(id) values(1); savepoint a; {bad_native}; insert into t(id) values(3); rollback to zz }}")).unwrap_err();
            assert_eq!(err.sqlstate(), Some(giggles_core::sqlstate(&original)));
            if typed { assert_eq!(err, Error::Sql { sqlstate: giggles_core::sqlstate(&original).into() }); }
            else { assert_eq!(err, Error::Duplicate); }
            assert!(rows(&right).is_empty()); assert_eq!(equal(&left, &right), before);
        }
        let (_ld, left) = Database::fresh(storage); let (_rd, right) = Database::fresh(storage);
        let before = equal(&left, &right); let mut sql = left.sql_session();
        let engine = Engine::with_version(right.clone(), VERSION);
        let bound = native(&engine, "transaction { savepoint a; let g=(t select id); rollback to a; emit @g.rows }").unwrap();
        assert_eq!(bound.summaries.len(), 1); assert_eq!(bound.emitted.len(), 1);
        for text in ["BEGIN", "INSERT INTO t VALUES(1)", "SAVEPOINT a"] { sql.execute(text, &[], None).unwrap(); }
        sql_error(&mut sql, "INSERT INTO t VALUES(1)", "23505");
        sql_error(&mut sql, "SELECT id FROM t", "25P02");
        sql.execute("ROLLBACK TO a", &[], None).unwrap();
        // SQL has no emit binding: its recovered transaction could commit, while native rejects @g.
        assert_eq!(sql.execute("SELECT id FROM t", &[], None).unwrap().rows.len(), 1);
        sql.execute("ROLLBACK", &[], None).unwrap();
        assert_eq!(native(&Engine::with_version(right.clone(), VERSION), "transaction { insert into t(id) values(1); savepoint a; insert into t(id) values(1); let g=(t select id); rollback to a; emit @g.rows }"), Err(Error::Unsupported));
        assert!(rows(&right).is_empty()); assert_eq!(equal(&left, &right), before);
    }
}
