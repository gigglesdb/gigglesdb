//! SQL/native view lifecycle parity; native calls forbid the SQL parser.
use giggles_core::{Admin, Engine as Storage, Limits, relational::with_sql_parser_forbidden};
use giggles_language::*;
use std::{path::PathBuf, sync::atomic::{AtomicU64, Ordering}};
const V: LanguageVersion = LanguageVersion::Embedded020;
struct Database(PathBuf);
impl Database {
    fn fresh(storage: Storage) -> (Self, Admin) {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!("native-view-{}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
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
fn step(left: &Admin, right: &Admin, native: &str, sql: &str, state: Option<&str>, missing: bool) {
    let before = snapshot(left); assert_eq!(snapshot(right), before);
    let expected = left.sql_session().execute(sql, &[], None);
    let actual = with_sql_parser_forbidden(|| Engine::with_version(right.clone(), V).execute_catalog_optional_text(native));
    if let Some(state) = state {
        let sql_error = expected.unwrap_err();
        assert_eq!(giggles_core::sqlstate(&sql_error), state);
        assert_eq!(actual.unwrap_err(), Error::Sql { sqlstate: giggles_core::sqlstate(&sql_error).into() });
        assert_eq!(snapshot(left), before);
    } else { assert_eq!(expected.unwrap().affected, 0); assert_eq!(actual.unwrap().is_none(), missing); }
    assert_eq!(snapshot(right), snapshot(left));
}
#[test]
fn view_profile_is_opt_in_and_inherits_nineteen() {
    let old = LanguageVersion::Embedded019;
    assert_eq!(V.as_str(), "0.20"); assert_eq!(profile_for(V).name, "embedded-trusted-native-0.20");
    assert_eq!(serde_json::to_string(&V).unwrap(), "\"0.20\"");
    assert_eq!(serde_json::from_str::<LanguageVersion>("\"0.20\"").unwrap(), V);
    assert_eq!(profile_for(V).scalar_types, profile_for(old).scalar_types);
    assert_eq!(profile_for(V).unsupported, profile_for(old).unsupported);
    for text in ["t select id, count(*)", "t a join t b on a.id = b.id select a.id"] {
        assert_eq!(parse_with_version(text, old).unwrap(), parse_with_version(text, V).unwrap());
    }
    assert_eq!(parse_catalog_with_version("alter table t rename to u", old).unwrap(), parse_catalog_with_version("alter table t rename to u", V).unwrap());
    let (_db, admin) = Database::fresh(Storage::Legacy);
    let engine = Engine::with_version(admin.clone(), V);
    for text in ["create view app.v as t select id", "drop view app.v restrict"] {
        assert_eq!(parse_catalog_with_version(text, old), Err(Error::Syntax { offset: if text.starts_with("create") { 7 } else { 5 } }));
        let tree = parse_catalog_with_version(text, V).unwrap();
        assert_eq!(with_sql_parser_forbidden(|| Engine::with_version(admin.clone(), old).execute_catalog(&tree)), Err(Error::Unsupported));
    }
    for text in ["create view v as t where id = $p select id", "create view v as t limit $p select id", "create view v as t select $p", "create view v as t where exists (t where id = $p select id) select id"] {
        assert_eq!(with_sql_parser_forbidden(|| engine.execute_catalog_text(text)), Err(Error::Unsupported));
    }
    let tree = parse_catalog_with_version("drop view if exists v", V).unwrap();
    assert_eq!(with_sql_parser_forbidden(|| engine.execute_catalog(&tree)), Err(Error::Unsupported));
    assert!(matches!(with_sql_parser_forbidden(|| engine.prepare_catalog_bound(&tree)), Err(Error::Unsupported)));
    let manifest = capability_manifest_for(V);
    for prior in capability_manifest_for(old).capabilities {
        assert!(manifest.capabilities.iter().any(|c| c.interface == prior.interface && c.feature == prior.feature && c.state == prior.state));
    }
    assert_eq!(manifest.capabilities.iter().filter(|c| c.feature == "catalog_views" && c.state == CapabilityState::Implemented).count(), 3);
    assert!(!capability_manifest_for(old).capabilities.iter().any(|c| c.feature == "catalog_views"));
}
#[test]
fn view_matrix_matches_sql() {
    for storage in [Storage::Legacy, #[cfg(feature = "engine-rocksdb")] Storage::V2] {
        let (_ldb, left) = Database::fresh(storage); let (_rdb, right) = Database::fresh(storage);
        for admin in [&left, &right] { admin.sql_session().execute("create table orders(id bigint primary key,item bigint,qty bigint)", &[], None).unwrap(); }
        if storage == Storage::Legacy {
            step(&left, &right, "create view v as orders select id", "create view v as select id from orders", Some("0A000"), false);
            step(&left, &right, "drop view if exists missing", "drop view if exists missing", Some("0A000"), false);
            continue;
        }
        for admin in [&left, &right] { for sql in ["insert into orders values(1,1,1),(2,1,3)", "create table items(id bigint primary key)", "insert into items values(1)", "create schema app"] { admin.sql_session().execute(sql, &[], None).unwrap(); } }
        // PostgreSQL accepts filtered views; GigglesDB SQL and native both reject them (shared R1 gap)
        step(&left, &right, "create view vf as orders where qty > 1 select id,qty", "create view vf as select id,qty from orders where qty > 1", Some("0A000"), false);
        for (native, sql) in [
            ("create view v as orders select id,qty", "create view v as select id,qty from orders"),
            ("create view app.v2 as orders select id", "create view app.v2 as select id from orders"),
            ("create view w as v select id", "create view w as select id from v")] { step(&left, &right, native, sql, None, false); }
        for query in ["select * from v order by id", "select * from app.v2 order by id", "select * from w order by id"] {
            let rows = |admin: &Admin| admin.sql_session().execute(query, &[], None).unwrap().rows.into_iter().map(|r| r.values).collect::<Vec<_>>();
            assert_eq!(rows(&left), rows(&right));
        }
        for (native, sql, state, missing) in [
            ("drop view v", "drop view v", Some("2BP01"), false),
            ("drop view v cascade", "drop view v cascade", None, false),
            ("drop view if exists v", "drop view if exists v", None, true),
            ("drop view if exists nope.v", "drop view if exists nope.v", None, true),
            ("drop view v", "drop view v", Some("42P01"), false),
            ("drop view nope.v", "drop view nope.v", Some("3F000"), false),
            ("drop view orders", "drop view orders", Some("42809"), false),
            ("drop view if exists orders", "drop view if exists orders", Some("42809"), false),
            // PostgreSQL accepts joins in views; R1 SQL and native both reject them.
            ("create view j as items i join orders o on i.id = o.item select i.id,o.qty", "create view j as select i.id,o.qty from items i join orders o on i.id = o.item", Some("0A000"), false)
        ] { step(&left, &right, native, sql, state, missing); }
        assert_eq!(right.sql_session().catalog_metadata().unwrap().presentation.views.len(), 1);
    }
}
