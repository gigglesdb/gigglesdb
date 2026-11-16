mod common;
use giggles_core::{Admin, Limits, operations::{AlterTableAction, CatalogRequest as C, ColumnSpec, Engine, Error}, relational::DataType, sqlstate};
fn create(table: &str) -> C {
    C::CreateTable { table: table.into(), columns: vec![ColumnSpec { name: "id".into(), data_type: DataType::Integer, not_null: true, default: None, generated: None }], primary_key: vec!["id".into()] }
}
fn exercise(v2: bool) {
    let path = common::fresh("native-catalog-session");
    let admin = if v2 { Admin::create_engine(&path, Limits::default(), giggles_core::Engine::V2).unwrap() }
        else { Admin::create(&path, Limits::default()).unwrap() };
    let mut s = admin.sql_session();
    s.execute("CREATE SCHEMA app", &[], None).unwrap();
    s.set_search_path("app,public", "giggles");
    assert_eq!(s.native_creation_schema("t").unwrap(), "app");
    assert_eq!(s.native_creation_schema("public.t").unwrap(), "public");
    assert_eq!(sqlstate(&s.native_creation_schema("absent.t").unwrap_err()), "3F000");
    assert_eq!(s.native_index_table("missing").unwrap(), None);
    assert_eq!(s.native_index_table("app.missing").unwrap(), None);
    s.set_search_path("missing", "giggles");
    assert_eq!(sqlstate(&s.native_creation_schema("t").unwrap_err()), "3F000");
    s.set_search_path("app,public", "giggles");
    s.begin_native().unwrap();
    s.execute_catalog_native(&create("t")).unwrap();
    assert_eq!(s.native_relation_reference("t").unwrap(), "app\0t");
    assert!(admin.catalog().unwrap().1.is_empty());
    s.rollback_native().unwrap();
    assert!(admin.catalog().unwrap().1.is_empty());
    s.begin_native().unwrap();
    s.execute_catalog_native(&create("t")).unwrap();
    s.execute("SELECT id FROM app.t", &[], None).unwrap();
    s.savepoint("keep").unwrap();
    s.execute_catalog_native(&create("transient")).unwrap();
    s.rollback_to("keep").unwrap();
    assert!(s.execute("SELECT * FROM app.transient", &[], None).is_err());
    s.rollback_to("keep").unwrap();
    let index = C::CreateIndex { table: "t".into(), name: "ix".into(), columns: vec!["id".into()] };
    s.execute_catalog_native(&index).unwrap();
    assert_eq!(s.native_index_table("ix").unwrap(), Some("app\0t".into()));
    assert_eq!(s.native_index_table("app.ix").unwrap(), Some("app\0t".into()));
    s.rollback_to("keep").unwrap();
    assert_eq!(s.native_index_table("ix").unwrap(), None);
    s.execute_catalog_native(&index).unwrap();
    s.execute_catalog_native(&C::AlterTable { table: "t".into(), action: AlterTableAction::RenameColumn { from: "id".into(), to: "renamed".into() } }).unwrap();
    s.execute("SELECT renamed FROM t", &[], None).unwrap();
    s.execute("SELECT renamed FROM app.t", &[], None).unwrap();
    s.execute_catalog_native(&C::DropIndex { name: "ix".into(), cascade: false }).unwrap();
    s.commit_native().unwrap();
    assert_eq!(admin.catalog().unwrap().1.len(), 1);
    s.begin_native().unwrap();
    assert_eq!(s.execute_catalog_native(&create("t")), Err(Error::Sql { sqlstate: "42P07".into() }));
    assert_eq!(s.execute_catalog_native(&create("other")), Err(Error::Sql { sqlstate: "25P02".into() }));
    s.rollback_native().unwrap();
    s.begin_native().unwrap();
    s.savepoint("recover").unwrap();
    assert!(s.execute_catalog_native(&create("t")).is_err());
    s.rollback_to("recover").unwrap();
    s.execute_catalog_native(&create("recovered")).unwrap();
    s.rollback_native().unwrap();
    let engine = Engine::new(admin.clone());
    for (request, sql, state) in [
        (create("app.t"), "CREATE TABLE app.t(id int PRIMARY KEY)", "42P07"),
        (C::DropTable { table: "missing".into(), cascade: false }, "DROP TABLE missing", "42P01"),
        (C::DropIndex { name: "missing".into(), cascade: false }, "DROP INDEX missing", "42704"),
        (create("absent.t"), "CREATE TABLE absent.t(id int PRIMARY KEY)", "3F000"),
    ] {
        let expected = Err(Error::Sql { sqlstate: state.into() });
        assert_eq!(s.execute_catalog_native(&request), expected);
        assert_eq!(engine.execute_catalog(&request), expected);
        assert_eq!(sqlstate(&s.execute(sql, &[], None).unwrap_err()), state);
    }
    s.execute_catalog_native(&C::DropTable { table: "t".into(), cascade: false }).unwrap();
    assert!(admin.catalog().unwrap().1.is_empty());
    if !v2 {
        s.execute_catalog_native(&create("t")).unwrap();
        s.execute("INSERT INTO app.t VALUES(1)", &[], None).unwrap();
        assert_eq!(s.execute_catalog_native(&create("populated")), Err(Error::PlanRequired));
        assert_eq!(engine.execute_catalog(&create("populated")), Err(Error::PlanRequired));
    }
}
#[test]
fn legacy_catalog_session() { exercise(false); }
#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2_catalog_session() { exercise(true); }
