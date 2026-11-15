//! Plain native view reads retry through typed execution without SQL parsing.
#![cfg(feature = "engine-rocksdb")]
use giggles_core::{Admin, Engine as Storage, Limits, relational::with_sql_parser_forbidden};
use giggles_language::*;
use std::{path::PathBuf, sync::atomic::{AtomicU64, Ordering}};
const V: LanguageVersion = LanguageVersion::Embedded020;
struct Database(PathBuf);
impl Database {
    fn fresh(storage: Storage) -> (Self, Admin) {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!("native-view-read-{}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
        let admin = Admin::create_engine(&path, Limits::default(), storage).unwrap();
        (Self(path), admin)
    }
}
impl Drop for Database { fn drop(&mut self) { std::fs::remove_dir_all(&self.0).unwrap(); } }
fn snapshot(admin: &Admin) -> String {
    let mut m = admin.sql_session().catalog_metadata().unwrap(); m.presentation.epoch = 0;
    m.schemas.epoch = 0; // Database-local HLCs differ across otherwise equal catalogs.
    let mut out = format!("{:?}", (&m.tables, &m.indexes, &m.schemas, &m.presentation));
    for table in m.tables {
        let schema = m.schemas.tables.get(&table.id).map(String::as_str).unwrap_or("public");
        let rows = admin.sql_session().execute(&format!("select * from {schema}.{} order by id", table.name), &[], None).unwrap().rows.into_iter().map(|r| r.values).collect::<Vec<_>>();
        out.push_str(&format!("{rows:?}"));
    }
    out
}

fn databases() -> (Database, Admin, Database, Admin) {
    let (ldb, left) = Database::fresh(Storage::V2);
    let (rdb, right) = Database::fresh(Storage::V2);
    for admin in [&left, &right] {
        for sql in ["create table orders(id bigint primary key,item bigint,qty bigint)",
            "insert into orders values(1,1,2),(2,2,4),(3,2,null),(4,2,8),(5,null,10)",
            "create schema app"] {
            admin.sql_session().execute(sql, &[], None).unwrap();
        }
    }
    for (native, sql) in [
        ("create view v as orders select id,item,qty", "create view v as select id,item,qty from orders"),
        ("create view app.v3 as orders select id", "create view app.v3 as select id from orders")]
    {
        left.sql_session().execute(sql, &[], None).unwrap();
        with_sql_parser_forbidden(|| Engine::with_version(right.clone(), V).execute_catalog_text(native)).unwrap();
    }
    assert_eq!(snapshot(&left), snapshot(&right));
    (ldb, left, rdb, right)
}
#[test]
fn view_reads_match_sql() {
    let (_ldb, left, _rdb, right) = databases();
    let before = snapshot(&left);
    let engine = Engine::with_version(right.clone(), V);
    for (native, sql, parameter) in [
        ("v select id,qty", "select id,qty from v", false),
        ("v where item=2 order by id desc limit 2 select id", "select id from v where item=2 order by id desc limit 2", false),
        ("v where qty is not null and not(id=3) select id as k", "select id as k from v where qty is not null and not(id=3)", false),
        ("v where item=$p order by id select id", "select id from v where item=$1 order by id", true),
        ("v where id=1 or qty is null order by id select id", "select id from v where id=1 or qty is null order by id", false),
        ("v where 1=1 select id", "select id from v where 1=1", false),
    ] {
        let bindings = if parameter { Bindings::from([("p".into(), Value::Int(2))]) } else { Bindings::new() };
        let params = if parameter { vec![Value::Int(2)] } else { vec![] };
        let expected = left.sql_session().execute(sql, &params, None).unwrap();
        let actual = with_sql_parser_forbidden(|| engine.execute_text(native, &bindings)).unwrap();
        assert_eq!(actual.columns, expected.columns, "{native}");
        assert_eq!(actual.rows, expected.rows, "{native}");
        assert_eq!((actual.matched, actual.affected, actual.changed, actual.complete),
            (expected.rows.len(), 0, 0, true), "{native}");
        assert_eq!(snapshot(&left), before);
        assert_eq!(snapshot(&right), before);
    }
    // Qualified view sources use the same typed read semantics as SQL.
    let expected = left.sql_session().execute("select id from app.v3", &[], None).unwrap();
    assert_eq!(expected.rows.len(), 5);
    let actual = with_sql_parser_forbidden(|| engine.execute_text("app.v3 select id", &Bindings::new())).unwrap();
    assert_eq!((actual.matched, actual.affected, actual.changed, actual.complete), (expected.rows.len(), 0, 0, true));
    assert_eq!((actual.columns, actual.rows), (expected.columns, expected.rows));
    assert_eq!(snapshot(&left), before);
    assert_eq!(snapshot(&right), before);
}
#[test]
fn view_reads_need_twenty() {
    let (_ldb, left, _rdb, right) = databases();
    let before = snapshot(&left);
    let old = Engine::with_version(right.clone(), LanguageVersion::Embedded019);
    let engine = Engine::with_version(right.clone(), V);
    let missing = Error::Sql { sqlstate: "42P01".into() };
    assert_eq!(with_sql_parser_forbidden(|| old.execute_text("v select id,qty", &Bindings::new())), Err(missing.clone()));
    let sql_error = left.sql_session().execute("select id from nope", &[], None).unwrap_err();
    assert_eq!(giggles_core::sqlstate(&sql_error), "42P01");
    assert_eq!(with_sql_parser_forbidden(|| engine.execute_text("nope select id", &Bindings::new())), Err(missing.clone()));
    // Arithmetic projections and omitted projections have no equivalent Query expression.
    for (native, sql) in [("v select qty*2 as d", "select qty*2 as d from v"), ("v", "select * from v")] {
        assert_eq!(left.sql_session().execute(sql, &[], None).unwrap().rows.len(), 5);
        assert_eq!(with_sql_parser_forbidden(|| engine.execute_text(native, &Bindings::new())), Err(missing.clone()));
    }
    assert_eq!(snapshot(&left), before);
    assert_eq!(snapshot(&right), before);
}
