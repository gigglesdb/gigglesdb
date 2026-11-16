mod common;
use giggles_core::{Admin, Limits, SqlSession, operations::{CatalogRequest, ColumnDefault, ColumnSpec, Engine}, relational::{DataType, StatementClock}};
fn execute(session: &mut SqlSession, sql: &str) -> Result<(), String> {
    let clock = StatementClock { unavailable: None, transaction_us: 100, statement_us: 100, zone: None, wall: None };
    session.execute_at(sql, &[], None, &clock).map(|_| ()).map_err(|e| giggles_core::sqlstate(&e).to_owned())
}
fn run(backend: giggles_core::Engine) {
    let admin = Admin::create_engine(common::fresh("parity-sequence-schema"), Limits::default(), backend).unwrap();
    let engine = Engine::new(admin.clone());
    let mut session = admin.sql_session();
    for (sql, code) in [
        ("DROP SEQUENCE nope.s", "3F000"),
        ("DROP SEQUENCE nope.s, public.zz", "3F000"),
        ("DROP SEQUENCE public.zz, nope.s", "42P01"),
    ] { assert_eq!(execute(&mut session, sql), Err(code.into()), "{sql}"); }
    execute(&mut session, "DROP SEQUENCE IF EXISTS nope.s").unwrap();
    execute(&mut session, "DROP SEQUENCE IF EXISTS nope.s, public.zz").unwrap();
    execute(&mut session, "CREATE SEQUENCE retained").unwrap();
    assert_eq!(execute(&mut session, "DROP SEQUENCE retained, nope.s"), Err("3F000".into()));
    execute(&mut session, "SELECT nextval('retained')").unwrap();
    assert_eq!(engine.execute_sequence_catalog(&CatalogRequest::DropSequence { name: "nope.s".into(), cascade: false }).unwrap_err().sqlstate(), Some("3F000"));
    assert_eq!(engine.execute_sequence_catalog(&CatalogRequest::DropSequence { name: "public.zz".into(), cascade: false }).unwrap_err().sqlstate(), Some("42P01"));
    execute(&mut session, "CREATE SCHEMA app").unwrap();
    for name in ["app.s", "public.s", "public.s2"] {
        engine.execute_sequence_catalog(&CatalogRequest::CreateSequence { name: name.into() }).unwrap();
    }
    execute(&mut session, "CREATE TABLE sql_defaults (a bigint DEFAULT nextval('app.s'), b bigint DEFAULT nextval('public.s2'))").unwrap();
    engine.execute_catalog(&CatalogRequest::CreateTable { table: "native_defaults".into(), columns: vec![
        ColumnSpec { name: "a".into(), data_type: DataType::BigInt, not_null: true, default: Some(ColumnDefault::Nextval("app.s".into())), generated: None },
        ColumnSpec { name: "b".into(), data_type: DataType::BigInt, not_null: false, default: Some(ColumnDefault::Nextval("public.s2".into())), generated: None },
    ], primary_key: vec!["a".into()] }).unwrap();
    for (path, expected) in [("public", "app.s"), ("app,public", "s"), ("public,app", "app.s")] {
        session.set_search_path(path, "giggles");
        let metadata = session.catalog_metadata().unwrap();
        for table in metadata.tables.iter().filter(|t| ["sql_defaults", "native_defaults"].contains(&t.name.as_str())) {
            assert_eq!(metadata.presentation.defaults[&table.columns[0].id], format!("nextval('{expected}'::regclass)"));
            assert_eq!(metadata.presentation.defaults[&table.columns[1].id], "nextval('s2'::regclass)");
        }
    }
    // Reading through another search path cannot rewrite the committed default.
    let before = admin.catalog_presentation().unwrap().defaults;
    session.set_search_path("app,public", "giggles");
    session.catalog_metadata().unwrap();
    assert_eq!(admin.catalog_presentation().unwrap().defaults, before);
}
#[test]
fn legacy() { run(giggles_core::Engine::Legacy); }
#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2() { run(giggles_core::Engine::V2); }
