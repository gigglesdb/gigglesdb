use giggles_core::{Admin, Engine as Storage, Limits, SqlSession, relational::with_sql_parser_forbidden};
use giggles_language::{Bindings, Engine, Error, LanguageVersion, Value};
use std::{path::PathBuf, sync::atomic::{AtomicU64, Ordering}};
const VERSION: LanguageVersion = LanguageVersion::Embedded020;
struct Database(PathBuf);
impl Database {
    fn fresh(storage: Storage) -> (Self, Admin) {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!("native-bound-query-{}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
        let admin = Admin::create_engine(&path, Limits::default(), storage).unwrap();
        for sql in ["create table orders(id bigint primary key,item bigint,qty bigint)",
            "create table dst(id bigint primary key,item bigint,qty bigint)"] { admin.sql_session().execute(sql, &[], None).unwrap(); }
        (Self(path), admin)
    }
}
impl Drop for Database { fn drop(&mut self) { std::fs::remove_dir_all(&self.0).unwrap(); } }
fn storages() -> Vec<Storage> { vec![Storage::Legacy, #[cfg(feature = "engine-rocksdb")] Storage::V2] }
fn snapshot(session: &mut SqlSession) -> String {
    let mut m = session.catalog_metadata().unwrap(); m.presentation.epoch = 0;
    let mut out = format!("{:?}", (&m.tables, &m.indexes, &m.presentation));
    // Physical row versions differ between native batches and SQL; parity compares logical values.
    for t in ["orders", "dst"] { out.push_str(&format!("{:?}", session.execute(&format!("select * from {t} order by id"), &[], None).unwrap().rows.into_iter().map(|r| r.values).collect::<Vec<_>>())); }
    out
}
fn sql_shape(admin: &Admin, sql: &str) -> Vec<giggles_core::relational::ColumnDef> {
    let mut session = admin.sql_session(); let prepared = session.prepare(sql).unwrap(); session.describe(&prepared).unwrap().columns
}
#[test]
fn bound_typed_reads_match_sql() {
    for storage in storages() {
        let (_ldb, left) = Database::fresh(storage); let (_rdb, right) = Database::fresh(storage);
        let engine = Engine::with_version(right.clone(), VERSION);
        let mut bindings = Bindings::from([("min".into(), Value::Int(1))]);
        let aggregate = "orders where qty > $min group by item order by item select item,count(*) as n";
        let join = "orders o join orders p on o.id=p.id order by o.id select o.id,p.qty";
        let insert = "insert into dst(id,item,qty) from orders where qty > $min select id,item,qty";
        let (read, joined, write) = with_sql_parser_forbidden(|| (engine.prepare_bound_text(aggregate, &bindings).unwrap(),
            engine.prepare_bound_text(join, &Bindings::new()).unwrap(), engine.prepare_bound_text(insert, &bindings).unwrap()));
        let simple = with_sql_parser_forbidden(|| engine.prepare_bound_text("orders select id", &Bindings::new())).unwrap();
        assert_eq!((read.description().contract_version, read.description().database_domain, read.description().catalog_epoch),
            (simple.description().contract_version, simple.description().database_domain, simple.description().catalog_epoch));
        assert_eq!(read.description().table, simple.description().table);
        assert_eq!(read.description().columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(), ["item", "n"]);
        assert_eq!(read.description().columns, sql_shape(&left, "select item,count(*) as n from orders where qty>1 group by item order by item"));
        assert_eq!(joined.description().columns, sql_shape(&left, "select o.id,p.qty from orders o join orders p on o.id=p.id order by o.id"));
        assert!(write.description().columns.is_empty()); assert_eq!(write.description().table.name, "dst");
        bindings.insert("min".into(), Value::Int(99));
        left.sql_session().execute("insert into orders values(1,1,2),(2,1,4),(3,2,0)", &[], None).unwrap();
        with_sql_parser_forbidden(|| engine.execute_text("insert many into orders(id,item,qty) values(1,1,2),(2,1,4),(3,2,0) at most 3", &Bindings::new())).unwrap();
        for (handle, sql, params) in [(&read, "select item,count(*) as n from orders where qty > $1 group by item order by item", vec![Value::Int(1)]),
            (&joined, "select o.id,p.qty from orders o join orders p on o.id=p.id order by o.id", vec![])] {
            let expected = left.sql_session().execute(sql, &params, None).unwrap();
            let actual = with_sql_parser_forbidden(|| engine.execute_bound(handle)).unwrap();
            assert_eq!((actual.columns, actual.rows, actual.matched), (expected.columns, expected.rows.clone(), expected.rows.len()));
        }
        let sql = "insert into dst(id,item,qty) select id,item,qty from orders where qty > 1";
        let written = with_sql_parser_forbidden(|| engine.execute_bound(&write)).unwrap();
        let affected = left.sql_session().execute(sql, &[], None).unwrap().affected as usize;
        assert_eq!((written.matched, written.affected, written.changed), (affected, affected, affected));
        let duplicate = left.sql_session().execute(sql, &[], None).unwrap_err();
        assert_eq!(giggles_core::sqlstate(&duplicate), "23505");
        assert_eq!(with_sql_parser_forbidden(|| engine.execute_bound(&write)), Err(Error::Sql { sqlstate: giggles_core::sqlstate(&duplicate).into() }));
        assert_eq!(snapshot(&mut left.sql_session()), snapshot(&mut right.sql_session()));
        assert_eq!(with_sql_parser_forbidden(|| Engine::with_version(right.clone(), LanguageVersion::Embedded019).execute_bound(&read)), Err(Error::Unsupported));
        let foreign = Engine::with_version(left.clone(), VERSION);
        assert_eq!(with_sql_parser_forbidden(|| foreign.execute_bound(&read)), Err(Error::Sql { sqlstate: "0A000".into() }));
        right.sql_session().execute("alter table orders add z bigint", &[], None).unwrap();
        let stale = with_sql_parser_forbidden(|| engine.execute_bound(&simple)).unwrap_err();
        assert_eq!(with_sql_parser_forbidden(|| engine.execute_bound(&read)), Err(stale.clone()));
        assert_eq!(with_sql_parser_forbidden(|| engine.execute_bound(&write)), Err(stale));
    }
}
#[test]
fn explain_typed_reads() {
    for storage in storages() {
        let (_ldb, left) = Database::fresh(storage); let (_rdb, admin) = Database::fresh(storage);
        let engine = Engine::with_version(admin.clone(), VERSION);
        let before = snapshot(&mut admin.sql_session()); let empty = Bindings::new();
        for (text, sql, sources) in [("explain orders group by item select item,count(*) as n", "select item,count(*) as n from orders group by item", vec!["orders"]),
            ("explain orders o join dst d on o.id=d.id select o.id,d.qty", "select o.id,d.qty from orders o join dst d on o.id=d.id", vec!["orders", "dst"])] {
            let result = with_sql_parser_forbidden(|| engine.explain_query_text(text, &empty)).unwrap();
            assert_eq!(result.columns, sql_shape(&left, sql));
            assert_eq!(result.columns.iter().map(|c| c.name.clone()).collect::<Vec<_>>(), left.sql_session().execute(sql, &[], None).unwrap().columns);
            assert_eq!(result.sources, sources);
            assert_eq!(with_sql_parser_forbidden(|| engine.explain_text(text, &empty)), Err(Error::Unsupported));
            assert_eq!(with_sql_parser_forbidden(|| Engine::with_version(admin.clone(), LanguageVersion::Embedded019).explain_query_text(text, &empty)), Err(Error::Unsupported));
        }
        assert_eq!(with_sql_parser_forbidden(|| engine.explain_query_text("explain insert into dst from orders select id,item,qty", &empty)), Err(Error::Unsupported));
        assert_eq!(with_sql_parser_forbidden(|| engine.explain_query_text("explain missing select count(*)", &empty)).unwrap_err().sqlstate(), Some("42P01"));
        assert_eq!(giggles_core::sqlstate(&left.sql_session().execute("select count(*) from missing", &[], None).unwrap_err()), "42P01");
        assert_eq!(snapshot(&mut admin.sql_session()), before);
        assert_eq!(snapshot(&mut left.sql_session()), before);
    }
}
