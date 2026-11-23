use giggles_core::{Admin, Engine as Storage, Limits, SqlSession, relational::with_sql_parser_forbidden};
use giggles_language::{AlterTableAction, CatalogStatement, Engine, Error, LanguageVersion,
    capability_manifest_for, parse_catalog_with_version, profile_for};
use std::{path::PathBuf, sync::atomic::{AtomicU64, Ordering}};
const VERSION: LanguageVersion = LanguageVersion::Embedded019;
struct Database(PathBuf);
impl Database {
    fn fresh(storage: Storage) -> (Self, Admin) {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!("native-alter-{}-{}",
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
        let rows = session.execute(&format!("select * from {}.{}", m.schemas.tables.get(&table.id).map(String::as_str).unwrap_or("public"), table.name), &[], None)
            .unwrap_or_else(|e| panic!("snapshot table {}: {e:?}", table.name)).rows.into_iter().map(|r| r.values).collect::<Vec<_>>();
        out.push_str(&format!("{rows:?}"));
    }
    out
}
fn equal(left: &Admin, right: &Admin) -> String {
    let expected = snapshot(&mut left.sql_session());
    assert_eq!(snapshot(&mut right.sql_session()), expected);
    expected
}
fn step(left: &Admin, right: &Admin, native: &str, sql: &str, state: Option<&str>) {
    let before = equal(left, right);
    let expected = left.sql_session().execute(sql, &[], None);
    let actual = with_sql_parser_forbidden(|| Engine::with_version(right.clone(), VERSION).execute_catalog_text(native));
    if let Some(state) = state {
        assert_eq!(giggles_core::sqlstate(&expected.unwrap_err()), state);
        assert_eq!(actual.unwrap_err().sqlstate(), Some(state));
        assert_eq!(equal(left, right), before);
    } else { assert_eq!(expected.unwrap().affected, 0); actual.unwrap(); }
    equal(left, right);
}
#[test]
fn alter_matrix_matches_sql() {
    for storage in storages() {
        let (_ldb, left) = Database::fresh(storage); let (_rdb, right) = Database::fresh(storage);
        for a in [&left, &right] { a.sql_session().execute("create table t(id bigint primary key,c bigint,keep bigint)", &[], None).unwrap(); }
        for (native, sql, state) in [
            ("alter table t add extra nullable int64", "alter table t add extra bigint", None),
            ("alter table t add column req int64", "alter table t add column req bigint not null", None),
            ("alter table t add extra nullable int64", "alter table t add extra bigint", Some("42701")),
            ("alter table t drop missing", "alter table t drop missing", Some("42703")),
            ("alter table t rename column extra to renamed", "alter table t rename column extra to renamed", None),
            ("alter table t drop column renamed", "alter table t drop column renamed", None),
            ("alter table t rename to t2", "alter table t rename to t2", None)] { step(&left, &right, native, sql, state); }
        for a in [&left, &right] { a.sql_session().execute("insert into t2 values(1,2,3,4)", &[], None).unwrap(); }
        if storage == Storage::V2 {
            for a in [&left, &right] { a.sql_session().execute("create view v as select c from t2", &[], None).unwrap(); }
            step(&left, &right, "alter table t2 drop c", "alter table t2 drop c", Some("2BP01"));
            step(&left, &right, "alter table t2 drop c cascade", "alter table t2 drop c cascade", None);
            assert!(right.sql_session().catalog_metadata().unwrap().presentation.views.is_empty());
        }
        // Populated Legacy and v2 both follow SQL (formerly native PlanRequired on Legacy).
        step(&left, &right, "alter table t2 add x nullable int64", "alter table t2 add x bigint", None);
        step(&left, &right, "alter table t2 add y int64", "alter table t2 add y bigint not null", Some("23502"));
        step(&left, &right, "alter table t2 rename column x to x2", "alter table t2 rename column x to x2", None);
        step(&left, &right, "alter table t2 drop x2", "alter table t2 drop x2", None);
        step(&left, &right, "alter table t2 rename to t3", "alter table t2 rename to t3", None);
    }
}
#[test]
fn native_alter_reserves_new_column_names_and_native_dml_can_use_sql_columns() {
    use giggles_language::{Bindings, Value};

    for storage in storages() {
        let (db, admin) = Database::fresh(storage);
        let mut sql = admin.sql_session();
        for statement in [
            "CREATE TABLE settings (id bigint PRIMARY KEY, approvals boolean)",
            "ALTER TABLE settings ADD COLUMN pg_x integer",
            "CREATE INDEX settings_pg_x ON settings (pg_x)",
            "INSERT INTO settings (id, approvals, pg_x) VALUES (1, false, 7)",
        ] {
            sql.execute(statement, &[], None).unwrap();
        }
        drop(sql);
        drop(admin);

        let admin = Admin::open(&db.0, Limits::default()).unwrap();
        let mut sql = admin.sql_session();
        let engine = Engine::with_version(admin.clone(), VERSION);
        let before = admin.catalog().unwrap();
        for statement in [
            "alter table settings add column deliveries nullable text",
            "alter table settings rename column pg_x to pg_new",
        ] {
            assert_eq!(
                with_sql_parser_forbidden(|| engine.execute_catalog_text(statement)),
                Err(Error::Sql { sqlstate: "0A000".into() }),
                "{statement}"
            );
        }
        assert_eq!(admin.catalog().unwrap(), before);

        let native = |statement| {
            with_sql_parser_forbidden(|| engine.execute_text(statement, &Bindings::new())).unwrap()
        };
        assert_eq!(
            native("settings where id=1 select approvals,pg_x").rows[0].values,
            [Value::Bool(false), Value::Int(7)]
        );
        native("insert into settings (id,approvals,pg_x) values (2,false,8)");
        native("update settings where id=2 set approvals=true,pg_x=9");
        let rows = sql
            .execute(
                "SELECT approvals, pg_x FROM settings WHERE pg_x=9",
                &[],
                None,
            )
            .unwrap()
            .rows;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values, [Value::Bool(true), Value::Int(9)]);

        // Renaming an existing reserved source to an ordinary name is valid.
        with_sql_parser_forbidden(|| {
            engine.execute_catalog_text("alter table settings rename column pg_x to score")
        })
        .unwrap();
        assert_eq!(
            native("settings where id=2 select score").rows[0].values,
            [Value::Int(9)]
        );
    }
}

#[test]
fn alter_schema_qualified_matches_sql() {
    for storage in storages() {
        let (_ldb, left) = Database::fresh(storage); let (_rdb, right) = Database::fresh(storage);
        for a in [&left, &right] { for sql in ["create schema app", "create table app.t(id bigint primary key)"] { a.sql_session().execute(sql, &[], None).unwrap(); } }
        step(&left, &right, "alter table app.t add z nullable int64", "alter table app.t add z bigint", None);
    }
}
#[test]
fn alter_profile_is_opt_in_and_inherits_eighteen() {
    assert_eq!(parse_catalog_with_version("alter table app.t rename to u", VERSION).unwrap(), CatalogStatement::AlterTable { table: "app.t".into(), action: AlterTableAction::RenameTable { to: "u".into() } });
    assert!(parse_catalog_with_version("alter table app.t rename to app.u", VERSION).is_err());
    assert!(parse_catalog_with_version("alter table app.t rename to u", LanguageVersion::Embedded018).is_err());
    let text = "alter table app.t drop column c restrict";
    let tree = parse_catalog_with_version(text, VERSION).unwrap();
    assert_eq!(tree, CatalogStatement::AlterTable { table: "app.t".into(), action: AlterTableAction::DropColumn { name: "c".into(), cascade: false } });
    let (_db, a) = Database::fresh(Storage::Legacy);
    assert!(parse_catalog_with_version(text, LanguageVersion::Embedded018).is_err());
    assert_eq!(Engine::with_version(a, LanguageVersion::Embedded018).execute_catalog(&tree), Err(Error::Unsupported));
    for text in ["drop table app.t", "create table app.t(id int64,n numeric,primary key(id))"] { parse_catalog_with_version(text, VERSION).unwrap(); }
    assert_eq!(profile_for(VERSION).name, "embedded-trusted-native-0.19");
    assert_eq!(serde_json::to_string(&VERSION).unwrap(), "\"0.19\"");
    assert_eq!(serde_json::from_str::<LanguageVersion>("\"0.19\"").unwrap(), VERSION);
    assert_eq!(profile_for(VERSION).scalar_types, [profile_for(LanguageVersion::Embedded018).scalar_types,
        &["real parameters", "double precision parameters", "date parameters", "time parameters",
            "timetz parameters", "timestamp parameters", "timestamptz parameters", "interval parameters"]].concat());
    for v in [LanguageVersion::Embedded018, VERSION] { assert_eq!(capability_manifest_for(v).capabilities.iter().filter(|c| c.feature == "catalog_alter").count(), if v == VERSION {3} else {0}); }
}
