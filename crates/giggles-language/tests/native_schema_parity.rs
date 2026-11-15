use giggles_core::{Admin, Engine as Storage, Limits, relational::with_sql_parser_forbidden};
use giggles_language::{Bindings, Engine, LanguageVersion, Value, capability_manifest_for,
    parse_catalog_with_version, parse_input_with_version, profile_for};
use std::{path::PathBuf, sync::atomic::{AtomicU64, Ordering}};
const VERSION: LanguageVersion = LanguageVersion::Embedded018;
struct Database(PathBuf);
impl Database {
    fn fresh(storage: Storage) -> (Self, Admin) {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!("native-schema-{}-{}",
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
fn equal(left: &Admin, right: &Admin) {
    for table in ["public.items", "app.items"] {
        let query = format!("select * from {table} order by id");
        let rows = |a: &Admin| a.sql_session().execute(&query, &[], None).unwrap().rows
            .into_iter().map(|r| r.values).collect::<Vec<_>>();
        assert_eq!(rows(left), rows(right), "{table}");
    }
    let metadata = |a: &Admin| {
        let mut m = a.sql_session().catalog_metadata().unwrap();
        m.presentation.epoch = 0;
        format!("{:?}", (&m.tables, &m.indexes, &m.presentation))
    };
    assert_eq!(metadata(left), metadata(right));
}
fn step(left: &Admin, right: &Admin, native: &str, sql: &str) {
    let expected = left.sql_session().execute(sql, &[], None).unwrap();
    let actual = with_sql_parser_forbidden(|| Engine::with_version(right.clone(), VERSION)
        .execute_text(native, &Bindings::new())).unwrap_or_else(|e| panic!("{native}: {e:?}"));
    assert_eq!(actual.affected as u64, expected.affected, "{native}");
    assert_eq!(actual.rows.iter().map(|r| &r.values).collect::<Vec<_>>(),
        expected.rows.iter().map(|r| &r.values).collect::<Vec<_>>(), "{native}");
    equal(left, right);
}
#[test]
fn schema_dml_catalog_and_reopen_match_sql() {
    for storage in storages() {
        let (left_db, left) = Database::fresh(storage);
        let (right_db, right) = Database::fresh(storage);
        for a in [&left, &right] { a.sql_session().execute("create schema app", &[], None).unwrap(); }
        let engine = Engine::with_version(right.clone(), VERSION);
        // Integer-to-NUMERIC native assignment parity is tracked by the separate native value-types task.
        for table in ["app.items", "items"] {
            left.sql_session().execute(&format!("create table {table}(id bigint primary key,n bigint not null)"), &[], None).unwrap();
            with_sql_parser_forbidden(|| engine.execute_catalog_text(&format!("create table {table}(id int64,n int64,primary key(id))"))).unwrap();
        }
        equal(&left, &right);
        for table in ["app.items", "public.items"] {
            left.sql_session().execute(&format!("create index same_idx on {table}(id)"), &[], None).unwrap();
            with_sql_parser_forbidden(|| engine.execute_catalog_text(&format!("create index same_idx on {table}(id)"))).unwrap();
            equal(&left, &right);
        }
        for name in ["app.same_idx", "public.same_idx"] {
            let sql = format!("drop index {name}");
            let expected = left.sql_session().execute(&sql, &[], None);
            let actual = with_sql_parser_forbidden(|| engine.execute_catalog_text(&sql));
            assert!(expected.is_ok() && actual.is_ok(), "{storage:?} {sql}: SQL={expected:?}; native={actual:?}");
            equal(&left, &right);
        }
        for (native, sql) in [
            ("insert `app`.`items` set id=1,n=11 return *", "insert into app.items values(1,11) returning *"),
            ("insert items(id,n) values(1,22) return *", "insert into items values(1,22) returning *"),
            ("insert app.items(id,n) values(1,99) on conflict(id) do update set n=incoming.n return *", "insert into app.items values(1,99) on conflict(id) do update set n=excluded.n returning *"),
            ("update public.items where id=1 set n=23 return *", "update public.items set n=23 where id=1 returning *"),
            ("app.items select id,n", "select id,n from app.items"),
            ("items select id,n", "select id,n from public.items"),
            ("public.items select n as value", "select n as value from public.items"),
            ("insert many into app.items(id,n) values(2,2),(3,3) at most 2", "insert into app.items values(2,2),(3,3)"),
            ("delete app.items where id=3 return *", "delete from app.items where id=3 returning *"),
        ] { step(&left, &right, native, sql); }
        let bindings = Bindings::from([("n".into(), Value::Int(125))]);
        // Use one retained SQL session for the paired atomic block.
        let mut sql = left.sql_session();
        sql.execute("begin", &[], None).unwrap();
        for text in ["update app.items set n=125 where id=1", "update public.items set n=125 where id=1"] {
            sql.execute(text, &[], None).unwrap();
        }
        sql.execute("commit", &[], None).unwrap();
        with_sql_parser_forbidden(|| engine.execute_transaction_text("transaction { update app.items where id=1 set n=$n; update items where id=1 set n=$n; }", &bindings)).unwrap();
        equal(&left, &right);
        drop(sql); drop(engine); drop(left); drop(right);
        let left = Admin::open(&left_db.0, Limits::default()).unwrap();
        let right = Admin::open(&right_db.0, Limits::default()).unwrap();
        equal(&left, &right);
        step(&left, &right, "delete many app.items where id>0 at most 10", "delete from app.items");
        step(&left, &right, "delete items where id=1", "delete from items where id=1");
        for table in ["app.items", "public.items"] {
            let text = format!("drop table {table}");
            left.sql_session().execute(&text, &[], None).unwrap();
            with_sql_parser_forbidden(|| Engine::with_version(right.clone(), VERSION).execute_catalog_text(&text)).unwrap();
        }
        assert_eq!(left.catalog().unwrap().1, right.catalog().unwrap().1);
    }
}
#[test]
fn schema_profile_is_opt_in_and_inherits_seventeen() {
    let old = LanguageVersion::Embedded017;
    for storage in storages() {
    let (_db, admin) = Database::fresh(storage);
    let engine = Engine::with_version(admin.clone(), VERSION);
    for (native, sql, state) in [
        ("create table absent.items(id int64,primary key(id))", "create table absent.items(id bigint primary key)", "3F000"),
        ("absent.items", "select * from absent.items", "42P01"),
        ("insert absent.items set id=1", "insert into absent.items values(1)", "42P01"),
        ("update absent.items where id=1 set id=2", "update absent.items set id=2 where id=1", "42P01"),
        ("delete absent.items where id=1", "delete from absent.items where id=1", "42P01"),
    ] {
        assert!(parse_input_with_version(native, old).is_err());
        let expected = admin.sql_session().execute(sql, &[], None).unwrap_err();
        let actual = with_sql_parser_forbidden(|| if native.starts_with("create") {
            engine.execute_catalog_text(native).map(|_| ())
        } else { engine.execute_text(native, &Bindings::new()).map(|_| ()) }).unwrap_err();
        assert_eq!(giggles_core::sqlstate(&expected), state);
        assert_eq!(actual.sqlstate(), Some(giggles_core::sqlstate(&expected)));
    }
    }
    for text in ["app..items", "app.items.extra", "`App`.items", "app.`items.extra`", "app."] {
        assert!(parse_input_with_version(text, VERSION).is_err(), "{text}");
    }
    for text in ["drop table app.items", "drop index app.idx", "create index idx on app.items(id)"] {
        assert!(parse_catalog_with_version(text, VERSION).is_ok());
        assert!(parse_catalog_with_version(text, old).is_err());
    }
    assert_eq!(profile_for(VERSION).scalar_types, profile_for(old).scalar_types);
    let manifest = capability_manifest_for(VERSION);
    for c in capability_manifest_for(old).capabilities.into_iter().filter(|c| format!("{:?}", c.state) == "Implemented") {
        assert!(manifest.capabilities.iter().any(|n| n.feature == c.feature && n.interface == c.interface && n.state == c.state));
    }
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_giggles-language"))
        .args(["capability-manifest", "--language-version", "0.18"]).output().unwrap();
    assert!(output.status.success());
    assert_eq!(serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()["language_version"], "0.18");
}
