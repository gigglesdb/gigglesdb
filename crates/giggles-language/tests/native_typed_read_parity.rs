//! Typed native reads share SQL execution without invoking its text parser.
use giggles_core::{Admin, Engine as Storage, Limits, SqlSession};
use giggles_core::relational::{Numeric, with_sql_parser_forbidden};
use giggles_language::{Bindings, Engine, Error, LanguageVersion, Value};
use std::{path::PathBuf, sync::atomic::{AtomicU64, Ordering}};
const VERSION: LanguageVersion = LanguageVersion::Embedded019;
struct Database(PathBuf);
impl Database {
    fn fresh(storage: Storage) -> (Self, Admin) {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!("native-typed-read-{}-{}",
            std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
        let admin = Admin::create_engine(&path, Limits::default(), storage).unwrap();
        for sql in ["create table items(id bigint primary key,name text)",
            "create table orders(id bigint primary key,item bigint,qty bigint)",
            "insert into items values(1,'one'),(2,'two'),(3,'three')",
            "insert into orders values(1,1,2),(2,1,4),(3,2,null),(4,null,8)"] {
            admin.sql_session().execute(sql, &[], None).unwrap();
        }
        (Self(path), admin)
    }
}
impl Drop for Database { fn drop(&mut self) { std::fs::remove_dir_all(&self.0).unwrap(); } }
fn storages() -> Vec<Storage> { vec![Storage::Legacy, #[cfg(feature = "engine-rocksdb")] Storage::V2] }
fn snapshot(session: &mut SqlSession) -> String {
    let metadata = session.catalog_metadata().unwrap();
    let mut out = format!("{:?}", (&metadata.tables, &metadata.indexes, &metadata.presentation));
    for table in ["items", "orders"] {
        out.push_str(&format!("{:?}", session.execute(&format!("select * from {table} order by id"), &[], None).unwrap().rows));
    }
    out
}
#[test]
fn typed_reads_match_sql() {
    for storage in storages() {
        let (_db, admin) = Database::fresh(storage);
        let native = Engine::with_version(admin.clone(), VERSION);
        let mut sql = admin.sql_session();
        let before = snapshot(&mut sql);
        let bindings = Bindings::from([("n".into(), Value::Int(1))]);
        for (text, expected, parameter) in [
            ("orders group by item order by item select item,count(*) as n,sum(qty) as total,min(qty),max(qty),avg(qty)",
             "select item,count(*) as n,sum(qty) as total,min(qty),max(qty),avg(qty) from orders group by item order by item", false),
            ("orders group by item having count(*) > $n order by item select item,count(*)",
             "select item,count(*) from orders group by item having count(*) > $1 order by item", true),
            ("orders having count(*) > 1 select count(*)", "select count(*) from orders having count(*) > 1", false),
            ("orders select count(*),count(qty)", "select count(*),count(qty) from orders", false),
            ("orders group by orders.item order by orders.item select orders.item,count(*)",
             "select orders.item,count(*) from orders group by orders.item order by orders.item", false),
            ("orders o group by o.item order by o.item select o.item,sum(o.qty)",
             "select o.item,sum(o.qty) from orders o group by o.item order by o.item", false),
            ("items i inner join orders o on i.id=o.item order by i.id,o.qty limit 2 select i.id,o.qty",
             "select i.id,o.qty from items i inner join orders o on i.id=o.item order by i.id,o.qty limit 2", false),
            ("items i left join orders o on i.id=o.item order by i.id,o.qty limit 4 select i.id,o.qty",
             "select i.id,o.qty from items i left join orders o on i.id=o.item order by i.id,o.qty limit 4", false),
            ("items i join orders o on i.id=o.item group by o.item order by o.item select o.item,count(o.qty)",
             "select o.item,count(o.qty) from items i join orders o on i.id=o.item group by o.item order by o.item", false),
        ] {
            let empty = Bindings::new();
            let actual = with_sql_parser_forbidden(|| native.execute_text(text, if parameter { &bindings } else { &empty })).unwrap();
            let params = if parameter { vec![bindings["n"].clone()] } else { vec![] };
            let expected = sql.execute(expected, &params, None).unwrap_or_else(|e| panic!("{text}: {e:?}"));
            assert_eq!(actual.columns, expected.columns, "{text}");
            assert_eq!(actual.rows, expected.rows, "{text}");
            assert_eq!((actual.matched, actual.affected, actual.changed, actual.complete), (expected.rows.len(), 0, 0, true));
            assert_eq!(snapshot(&mut sql), before);
        }
        let numeric = Value::Numeric(Numeric::parse("1").unwrap());
        let actual = with_sql_parser_forbidden(|| native.execute_text(
            "orders group by item having count(*) > $n order by item select item,count(*)",
            &Bindings::from([("n".into(), numeric.clone())]))).unwrap();
        let expected = sql.execute("select item,count(*) from orders group by item having count(*) > 1.0 order by item", &[], None).unwrap();
        assert_eq!((actual.columns, actual.rows), (expected.columns, expected.rows));
        // SQL infers bigint for an uncast parameter; typed native bindings become literals.
        let error = sql.execute("select item,count(*) from orders group by item having count(*) > $1 order by item", &[numeric], None).unwrap_err();
        assert_eq!(giggles_core::sqlstate(&error), "42804");
        assert_eq!(snapshot(&mut sql), before);
    }
}
#[test]
fn typed_read_errors_match_sql() {
    for storage in storages() {
        let (_db, admin) = Database::fresh(storage);
        let native = Engine::with_version(admin.clone(), VERSION);
        let before = snapshot(&mut admin.sql_session());
        for (text, sql, state) in [
            ("items i join orders o on i.id=o.item select o.nope", "select o.nope from items i join orders o on i.id=o.item", "42703"),
            ("orders group by item select qty", "select qty from orders group by item", "42803"),
            ("missing select count(*)", "select count(*) from missing", "42P01"),
        ] {
            let error = with_sql_parser_forbidden(|| native.execute_text(text, &Bindings::new())).unwrap_err();
            assert_eq!(error, Error::Sql { sqlstate: state.into() });
            assert_eq!(giggles_core::sqlstate(&admin.sql_session().execute(sql, &[], None).unwrap_err()), state);
            assert_eq!(snapshot(&mut admin.sql_session()), before);
        }
    }
}
#[test]
fn typed_read_limits_are_asserted() {
    for storage in storages() {
    let (_db, admin) = Database::fresh(storage);
    let native = Engine::with_version(admin.clone(), VERSION);
    for text in ["app.items group by id select id", "items i join orders o on i.id=o.item"] {
        assert_eq!(native.execute_text(text, &Bindings::new()), Err(Error::Unsupported));
    }
    assert!(matches!(native.prepare_bound_text("orders select count(*)", &Bindings::new()), Err(Error::Unsupported)));
    assert!(Engine::with_version(admin, LanguageVersion::Embedded018).execute_text("orders select count(*)", &Bindings::new()).is_err());
    assert_eq!(native.execute_text("orders having count(*) > $n select count(*)", &Bindings::new()), Err(Error::Bind("parameter missing".into())));
    assert_eq!(native.execute_text("orders limit $n select count(*)", &Bindings::from([("n".into(), Value::Text("bad".into()))])), Err(Error::Type));
    }
}
