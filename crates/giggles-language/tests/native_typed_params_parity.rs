//! Native typed values share SQL coercion without entering its parser.
use giggles_core::{Admin, Engine as Storage, Limits, SqlSession};
use giggles_core::relational::{Numeric, with_sql_parser_forbidden};
use giggles_language::{Bindings, Engine, Error, LanguageVersion, Value, ValueExpr, Statement,
    lower_text, parse_with_version, parse_transaction_with_version};
use std::{path::PathBuf, sync::atomic::{AtomicU64, Ordering}};
const VERSION: LanguageVersion = LanguageVersion::Embedded019;
struct Database(PathBuf);
impl Database {
    fn fresh(storage: Storage) -> (Self, Admin) {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!("native-typed-{}-{}",
            std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
        let admin = Admin::create_engine(&path, Limits::default(), storage).unwrap();
        (Self(path), admin)
    }
}
impl Drop for Database {
    fn drop(&mut self) { std::fs::remove_dir_all(&self.0).unwrap(); }
}
fn storages() -> Vec<Storage> {
    vec![Storage::Legacy, #[cfg(feature = "engine-rocksdb")] Storage::V2]
}
fn snapshot(session: &mut SqlSession) -> String {
    let mut m = session.catalog_metadata().unwrap();
    m.presentation.epoch = 0; // V2 epochs are per-database HLCs, not logical catalog state.
    // Fixture IDs are deterministic on both paths; include FK/view definitions and indexes.
    let mut out = format!("{:?}", (&m.tables, &m.indexes, &m.presentation));
    for table in m.tables {
        let rows = session.execute(&format!("select * from {} order by id", table.name), &[], None)
            .unwrap_or_else(|e| panic!("snapshot table {}: {e:?}", table.name)).rows.into_iter().map(|r| r.values).collect::<Vec<_>>();
        out.push_str(&format!("{rows:?}"));
    }
    out
}

fn sql(admin: &Admin, text: &str) -> Vec<Vec<Value>> {
    admin.sql_session().execute(text, &[], None).unwrap().rows.into_iter().map(|r| r.values).collect()
}
fn equal(left: &Admin, right: &Admin) {
    assert_eq!(snapshot(&mut left.sql_session()), snapshot(&mut right.sql_session()));
}
#[test]
fn typed_parameters_match_sql() {
    for storage in storages() {
        for (ty, input) in [
            ("date", "'2024-02-29'::date"),
            ("time(3)", "'12:34:56.7896'::time"),
            ("timetz(3)", "'12:34:56.7896+02'::timetz"),
            ("timestamp(3)", "'2024-02-29 12:34:56.7896'::timestamp"),
            ("timestamptz(3)", "'2024-02-29 12:34:56.7896+02'::timestamptz"),
            ("interval", "'1 mon 2 days 03:04:05.6789'::interval"),
            ("real", "'0.1'::real"), ("double precision", "'0.1'::double precision"),
            ("real", "'NaN'::real"), ("double precision", "'Infinity'::double precision"),
            ("uuid", "'{550E8400E29B41D4A716446655440000}'::uuid"),
            ("jsonb", "'{\"b\":2,\"a\":1}'::jsonb"),
        ] {
            let (_left_db, left) = Database::fresh(storage);
            let (_right_db, right) = Database::fresh(storage);
            for admin in [&left, &right] { sql(admin, &format!("create table t(id int primary key,v {ty})")); }
            let value = match ty {
                "uuid" => Value::Text("{550E8400E29B41D4A716446655440000}".into()),
                "jsonb" => Value::Text("{ \"b\": 2, \"a\": 1 }".into()),
                _ => sql(&left, &format!("select {input}"))[0][0].clone(),
            };
            let bindings = Bindings::from([("v".into(), value.clone())]);
            let engine = Engine::with_version(right.clone(), VERSION);
            let expected = left.sql_session().execute(&format!("insert into t values(1,{input})"), &[], None).unwrap();
            let actual = with_sql_parser_forbidden(|| engine.execute_text("insert into t(id,v) values(1,$v)", &bindings)).unwrap();
            assert_eq!(actual.affected as u64, expected.affected, "{ty}");
            equal(&left, &right);
            let expected = left.sql_session().execute(&format!("select id from t where v={input}"), &[], None).unwrap();
            let actual = with_sql_parser_forbidden(|| engine.execute_text("t where v=$v select id", &bindings)).unwrap();
            assert_eq!(actual.rows.iter().map(|r| &r.values).collect::<Vec<_>>(), expected.rows.iter().map(|r| &r.values).collect::<Vec<_>>(), "{ty}");
            assert_eq!(actual.rows.len(), usize::from(!ty.ends_with("(3)")), "unrounded predicate {ty}");
            // Both tree literals and every parameter-bearing public entry point admit the type.
            let mut literal = parse_with_version("t where v=$v select v", VERSION).unwrap();
            if let Statement::Read { filter: Some(giggles_language::Expr::Compare(_, _, rhs)), .. } = &mut literal {
                **rhs = giggles_language::Expr::Value(ValueExpr::Literal(value));
            } else { panic!("read shape"); }
            with_sql_parser_forbidden(|| engine.execute(&literal, &Bindings::new())).unwrap();
            with_sql_parser_forbidden(|| engine.prepare_bound(&literal, &Bindings::new())).unwrap();
            let read = parse_with_version("t where v=$v select v", VERSION).unwrap();
            with_sql_parser_forbidden(|| engine.prepare_bound(&read, &bindings)).unwrap();
            with_sql_parser_forbidden(|| lower_text("t where v=$v select v", &bindings, VERSION)).unwrap();
            for text in ["update t where v=$v when v=$v set v=coalesce($v,v)",
                "insert many into t(id,v) values(2,$v) at most 1 return $v as value",
                "t select $v as value"] {
                with_sql_parser_forbidden(|| lower_text(text, &bindings, VERSION)).unwrap();
            }
            let block = "transaction read only { t where v=$v select v; require $v=$v; emit $v }";
            let transaction = parse_transaction_with_version(block, VERSION).unwrap();
            with_sql_parser_forbidden(|| engine.execute_transaction(&transaction, &bindings)).unwrap();
            with_sql_parser_forbidden(|| engine.prepare_transaction_bound(&transaction, &bindings)).unwrap();
            with_sql_parser_forbidden(|| lower_text(block, &bindings, VERSION)).unwrap();
            if ty == "date" {
                let out = with_sql_parser_forbidden(|| engine.execute_transaction_text("transaction { update t where id=1 set v=$v; }", &bindings)).unwrap();
                assert_eq!(out.summaries[0].affected, 1);
                left.sql_session().execute(&format!("update t set v={input} where id=1"), &[], None).unwrap();
            }
            if ty == "timestamp(3)" {
                let bound = with_sql_parser_forbidden(|| engine.prepare_bound_text("update t where id=1 set v=$v return v", &bindings)).unwrap();
                let actual = with_sql_parser_forbidden(|| engine.execute_bound(&bound)).unwrap();
                let direct = with_sql_parser_forbidden(|| engine.execute_text("update t where id=1 set v=$v return v", &bindings)).unwrap();
                assert_eq!((actual.matched, actual.affected), (direct.matched, direct.affected));
                assert_eq!(actual.rows.len(), 1);
                assert_eq!(actual.rows, direct.rows);
                left.sql_session().execute(&format!("update t set v={input} where id=1"), &[], None).unwrap();
            }
            equal(&left, &right);
        }
    }
}
#[test]
fn typed_parameter_errors_match_sql() {
    for storage in storages() {
        let (_db, admin) = Database::fresh(storage);
        let (_native_db, native) = Database::fresh(storage);
        let engine = Engine::with_version(native.clone(), VERSION);
        for (ty, input, value, state) in [
            ("bigint", "'2024-02-29'::date", sql(&admin, "select '2024-02-29'::date")[0][0].clone(), "42804"),
            ("uuid", "'nope'", Value::Text("nope".into()), "22P02"),
        ] {
            for db in [&admin, &native] { sql(db, &format!("create table t(id int primary key,v {ty})")); }
            let expected = admin.sql_session().execute(&format!("insert into t values(1,{input})"), &[], None).unwrap_err();
            let actual = with_sql_parser_forbidden(|| engine.execute_text("insert t set id=1,v=$v", &Bindings::from([("v".into(), value)]))).unwrap_err();
            assert_eq!(giggles_core::sqlstate(&expected), state);
            if ty == "bigint" {
                // Shared native binding exposes Type; SQL preserves its 42804 diagnostic.
                assert_eq!(actual, Error::Type);
                assert_eq!(actual.sqlstate(), None);
            } else { assert_eq!(actual.sqlstate(), Some(state), "{ty}: {actual:?}"); }
            assert_eq!(sql(&admin, "select count(*) from t"), [vec![Value::Int(0)]]);
            equal(&admin, &native);
            for db in [&admin, &native] { sql(db, "drop table t"); }
        }
    }
}
#[test]
fn typed_parameters_are_opt_in() {
    for storage in storages() {
        let (_db, admin) = Database::fresh(storage);
        sql(&admin, "create table t(id int primary key,v date)");
        let before = snapshot(&mut admin.sql_session());
        let date = sql(&admin, "select '2024-02-29'::date")[0][0].clone();
        let old = Engine::with_version(admin.clone(), LanguageVersion::Embedded018);
        assert!(matches!(with_sql_parser_forbidden(|| old.execute_text("insert into t(id,v) values(1,$v)", &Bindings::from([("v".into(), date)]))), Err(Error::Unsupported)));
        let engine = Engine::with_version(admin.clone(), VERSION);
        assert!(matches!(with_sql_parser_forbidden(|| engine.execute_text("t where id=$v", &Bindings::from([("v".into(), Value::Numeric(Numeric::parse("1").unwrap()))]))), Err(Error::Unsupported)));
        assert_eq!(snapshot(&mut admin.sql_session()), before);
    }
}
