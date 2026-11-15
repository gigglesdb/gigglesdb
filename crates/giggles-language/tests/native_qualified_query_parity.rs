//! Qualified typed sources and targets match SQL through native schema resolution.
use giggles_core::{Admin, Engine as Storage, Limits, relational::with_sql_parser_forbidden};
use giggles_language::*;
use std::{path::PathBuf, sync::atomic::{AtomicU64, Ordering}};
const V: LanguageVersion = LanguageVersion::Embedded020;
struct Database(PathBuf);
impl Database {
    fn fresh(storage: Storage) -> (Self, Admin) {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!("native-qualified-query-{}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
        let admin = Admin::create_engine(&path, Limits::default(), storage).unwrap();
        for sql in ["create schema app", "create table app.orders(id bigint primary key,item bigint,qty bigint)",
            "create table app.dst(id bigint primary key,item bigint,qty bigint)",
            "create table orders(id bigint primary key,item bigint,qty bigint)",
            "insert into app.orders values(1,7,2),(2,7,4)", "insert into orders values(9,8,3)"] {
            admin.sql_session().execute(sql, &[], None).unwrap();
        }
        (Self(path), admin)
    }
}
impl Drop for Database { fn drop(&mut self) { std::fs::remove_dir_all(&self.0).unwrap(); } }
fn storages() -> Vec<Storage> { vec![Storage::Legacy, #[cfg(feature = "engine-rocksdb")] Storage::V2] }
fn snapshot(admin: &Admin) -> String {
    let mut m = admin.sql_session().catalog_metadata().unwrap(); m.presentation.epoch = 0; m.schemas.epoch = 0;
    let mut out = format!("{:?}", (&m.tables, &m.indexes, &m.schemas, &m.presentation));
    for table in ["app.orders", "app.dst", "orders"] { out.push_str(&format!("{:?}", admin.sql_session().execute(&format!("select * from {table} order by id"), &[], None).unwrap().rows.into_iter().map(|r| r.values).collect::<Vec<_>>())); }
    out
}
#[test]
fn qualified_targets_match_sql_and_old_profile_stays_frozen() {
    for storage in storages() { for columns in ["(id,item,qty)", ""] {
        let (_ld, left) = Database::fresh(storage); let (_rd, right) = Database::fresh(storage);
        let engine = Engine::with_version(right.clone(), V); let b = Bindings::new();
        let text = format!("insert into app.dst{columns} from orders where qty > 1 select id,item,qty");
        let expected = left.sql_session().execute(&format!("insert into app.dst{columns} select id,item,qty from orders where qty > 1"), &[], None).unwrap();
        let actual = with_sql_parser_forbidden(|| engine.execute_text(&text, &b)).unwrap();
        assert_eq!((actual.matched, actual.affected, actual.changed), (expected.affected as usize, expected.affected as usize, expected.affected as usize));
        assert_eq!(snapshot(&left), snapshot(&right));
        let missing = format!("insert into nope.dst{columns} from orders select id,item,qty");
        assert_eq!(giggles_core::sqlstate(&left.sql_session().execute(&format!("insert into nope.dst{columns} select id,item,qty from orders"), &[], None).unwrap_err()), "42P01");
        assert_eq!(with_sql_parser_forbidden(|| engine.execute_text(&missing, &b)), Err(Error::Sql { sqlstate: "42P01".into() }));
        let old = Engine::with_version(right.clone(), LanguageVersion::Embedded019);
        assert_eq!(with_sql_parser_forbidden(|| old.execute_text(&text, &b)), Err(Error::Unsupported));
        assert_eq!(snapshot(&left), snapshot(&right));
    } }
}
#[test]
fn qualified_sources_match_sql() {
    for storage in storages() {
        let (_ld, left) = Database::fresh(storage); let (_rd, right) = Database::fresh(storage);
        let engine = Engine::with_version(right.clone(), V); let old = Engine::with_version(right.clone(), LanguageVersion::Embedded019);
        for (native, sql) in [
            ("app.orders group by item order by item select item,count(*) as n", "select item,count(*) as n from app.orders group by item order by item"),
            ("app.orders o join app.orders p on o.id=p.id select o.id,p.qty", "select o.id,p.qty from app.orders o join app.orders p on o.id=p.id"),
            ("orders o join app.orders p on o.id=o.id select o.id,p.qty", "select o.id,p.qty from orders o join app.orders p on o.id=o.id"),
            ("orders where item in (app.orders select item) select id", "select id from orders where item in (select item from app.orders)"),
            ("insert into app.dst from app.orders select id,item,qty", "insert into app.dst select id,item,qty from app.orders"),
        ] {
            assert!(with_sql_parser_forbidden(|| parse_input_with_version(native, V)).is_ok(), "{native}");
            let expected = left.sql_session().execute(sql, &[], None).unwrap();
            if native.starts_with("insert") { assert_eq!(expected.affected, 2); }
            else if native.contains(" in ") { assert!(expected.rows.is_empty()); }
            else { assert!(!expected.rows.is_empty()); }
            let actual = with_sql_parser_forbidden(|| engine.execute_text(native, &Bindings::new())).unwrap();
            if native.starts_with("insert") {
                assert_eq!((actual.matched, actual.affected, actual.changed, actual.complete),
                    (expected.affected as usize, expected.affected as usize, expected.affected as usize, true), "{native}");
            } else {
                assert_eq!((actual.matched, actual.affected, actual.changed, actual.complete),
                    (expected.rows.len(), 0, 0, true), "{native}");
                assert_eq!((actual.columns, actual.rows), (expected.columns, expected.rows), "{native}");
            }
            assert_eq!(with_sql_parser_forbidden(|| old.execute_text(native, &Bindings::new())), Err(Error::Unsupported), "{native}");
            assert_eq!(snapshot(&left), snapshot(&right));
        }
        for (native, sql) in [
            ("nope.orders select id", "select id from nope.orders"),
            ("app.nope select id", "select id from app.nope"),
        ] {
            assert_eq!(giggles_core::sqlstate(&left.sql_session().execute(sql, &[], None).unwrap_err()), "42P01");
            assert_eq!(with_sql_parser_forbidden(|| engine.execute_text(native, &Bindings::new())),
                Err(Error::Sql { sqlstate: "42P01".into() }), "{native}");
            assert_eq!(snapshot(&left), snapshot(&right));
        }
        assert!(with_sql_parser_forbidden(|| parse_input_with_version("app.orders select app.orders.id", V)).is_err());
    }
}
#[test]
fn qualified_target_handles_match_sql() {
    for storage in storages() {
        let (_ld, left) = Database::fresh(storage); let (_rd, right) = Database::fresh(storage);
        let engine = Engine::with_version(right.clone(), V); let b = Bindings::new();
        let handle = with_sql_parser_forbidden(|| engine.prepare_bound_text("insert into app.dst from orders select id,item,qty", &b)).unwrap();
        assert_eq!(handle.description().table.name, "dst");
        let expected = left.sql_session().execute("insert into app.dst select id,item,qty from orders", &[], None).unwrap();
        assert_eq!(with_sql_parser_forbidden(|| engine.execute_bound(&handle)).unwrap().affected, expected.affected as usize);
        assert_eq!(snapshot(&left), snapshot(&right));
        let handle = with_sql_parser_forbidden(|| engine.prepare_bound_text("app.orders select count(*)", &b)).unwrap();
        let expected = left.sql_session().execute("select count(*) from app.orders", &[], None).unwrap();
        let actual = with_sql_parser_forbidden(|| engine.execute_bound(&handle)).unwrap();
        assert_eq!((actual.matched, actual.affected, actual.changed, actual.complete), (expected.rows.len(), 0, 0, true));
        assert_eq!((actual.columns, actual.rows), (expected.columns, expected.rows));
        assert_eq!(snapshot(&left), snapshot(&right));
        #[cfg(feature = "engine-rocksdb")]
        if storage == Storage::V2 {
            for a in [&left, &right] { a.sql_session().execute("create view v as select id from orders", &[], None).unwrap(); }
            let handle = with_sql_parser_forbidden(|| engine.prepare_bound_text("v select id", &b)).unwrap();
            let expected = left.sql_session().execute("select id from v", &[], None).unwrap();
            assert_eq!(with_sql_parser_forbidden(|| engine.execute_bound(&handle)).unwrap().rows, expected.rows);
            assert_eq!(handle.description().table.name, "v");
        }
    }
}
