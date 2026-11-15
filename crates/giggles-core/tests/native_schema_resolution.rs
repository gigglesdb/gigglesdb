mod common;
use giggles_core::{Admin, Engine as Backend, Limits, operations::*, relational::{DataType, Value as V}};

fn engines() -> Vec<Backend> {
    #[allow(unused_mut)]
    let mut engines = vec![Backend::Legacy];
    #[cfg(feature = "engine-rocksdb")]
    engines.push(Backend::V2);
    engines
}
fn read(table: &str) -> Request {
    Request::Read { table: table.into(), filter: None, order: vec![], limit: None, projection: vec![] }
}
fn insert(table: &str, id: i64) -> Request {
    Request::Insert { table: table.into(), values: vec![("id".into(), ValueExpr::Literal(V::Int(id)))] }
}
fn key(id: i64) -> Expr {
    Expr::Compare(Box::new(Expr::Field("id".into())), Compare::Eq,
        Box::new(Expr::Value(ValueExpr::Literal(V::Int(id)))))
}
fn create(table: &str) -> CatalogRequest {
    CatalogRequest::CreateTable { table: table.into(),
        columns: vec![ColumnSpec { name: "id".into(), data_type: DataType::BigInt, not_null: true, default: None, generated: None }],
        primary_key: vec!["id".into()] }
}
fn ids(engine: &Engine, table: &str) -> Vec<V> {
    engine.execute(&read(table), &Bindings::new()).unwrap().rows.into_iter()
        .map(|r| r.values[0].clone()).collect()
}
#[test]
fn schema_isolation_dml_blocks_session_path_and_reopen() {
    for backend in engines() {
        let path = common::fresh("native-schema-dml");
        let admin = Admin::create_engine(&path, Limits::default(), backend).unwrap();
        let engine = Engine::new(admin.clone());
        let mut sql = admin.sql_session();
        for statement in ["CREATE SCHEMA app", "CREATE TABLE app.items(id BIGINT PRIMARY KEY, n BIGINT)",
            "CREATE TABLE public.items(id BIGINT PRIMARY KEY, n BIGINT)"] {
            sql.execute(statement, &[], None).unwrap();
        }
        engine.execute(&insert("app.items", 11), &Bindings::new()).unwrap();
        engine.execute(&insert("items", 22), &Bindings::new()).unwrap();
        assert_eq!(ids(&engine, "items"), [V::Int(22)]);
        assert_eq!(ids(&engine, "app.items"), [V::Int(11)]);
        sql.set_search_path("app, public", "giggles");
        assert_eq!(sql.execute_native(&read("items"), &Bindings::new()).unwrap().rows[0].values[0], V::Int(11));
        assert_eq!(sql.execute_native(&read("public.items"), &Bindings::new()).unwrap().rows[0].values[0], V::Int(22));
        sql.execute_native(&insert("items", 33), &Bindings::new()).unwrap();
        sql.execute_native(&insert("public.items", 44), &Bindings::new()).unwrap();
        let block = Transaction { read_only: false, steps: vec![
            TransactionStep::Execute(Request::Update { table: "app.items".into(), filter: key(11), guard: None,
                values: vec![("n".into(), ValueExpr::Literal(V::Int(12)))], intent: Intent::One }),
            TransactionStep::Execute(Request::OnConflict { request: Box::new(insert("app.items", 11)),
                target: vec!["id".into()], action: ConflictAction::Nothing }),
            TransactionStep::Execute(Request::Delete { table: "app.items".into(), filter: key(33), guard: None, cap: None }),
            TransactionStep::Execute(insert("public.items", 55)),
        ], emit: None };
        assert_eq!(engine.execute_transaction(&block, &Bindings::new()).unwrap().summaries.len(), 4);
        assert_eq!(ids(&engine, "app.items"), [V::Int(11)]);
        assert_eq!(ids(&engine, "items"), [V::Int(22), V::Int(44), V::Int(55)]);
        let session_block = Transaction { read_only: false,
            steps: vec![TransactionStep::Execute(insert("items", 66)), TransactionStep::Execute(insert("public.items", 77))], emit: None };
        sql.execute_native_transaction(&session_block, &Bindings::new()).unwrap();
        assert_eq!(ids(&engine, "app.items"), [V::Int(11), V::Int(66)]);
        let bound = engine.prepare_bound(&read("app.items"), &Bindings::new()).unwrap();
        let bound_block = engine.prepare_transaction_bound(&block, &Bindings::new()).unwrap();
        sql.execute("CREATE SCHEMA changed", &[], None).unwrap();
        assert_eq!(engine.execute_bound(&bound).unwrap_err().sqlstate(), Some("0A000"));
        assert_eq!(engine.execute_transaction_bound(&bound_block).unwrap_err().sqlstate(), Some("0A000"));
        drop(sql); drop(engine); drop(admin);
        let admin = Admin::open(&path, Limits::default()).unwrap();
        assert_eq!(ids(&Engine::new(admin), "app.items"), [V::Int(11), V::Int(66)]);
    }
}
#[test]
fn qualified_catalog_duplicate_missing_schema_and_strict_names() {
    for backend in engines() {
        let path = common::fresh("native-schema-catalog");
        let admin = Admin::create_engine(&path, Limits::default(), backend).unwrap();
        let engine = Engine::new(admin.clone());
        admin.sql_session().execute("CREATE SCHEMA app", &[], None).unwrap();
        let app = engine.execute_catalog(&create("app.items")).unwrap().table;
        let public = engine.execute_catalog(&create("items")).unwrap().table;
        assert_ne!(app.id, public.id);
        assert_eq!(engine.prepare_catalog_bound(&create("app.items")).unwrap_err().sqlstate(), Some("42P07"));
        for table in ["app.items", "public.items"] {
            let index = engine.execute_catalog(&CatalogRequest::CreateIndex {
                table: table.into(), name: "same_idx".into(), columns: vec!["id".into()] }).unwrap();
            assert_eq!(index.table.id, if table.starts_with("app") { app.id } else { public.id });
        }
        let dropped = engine.execute_catalog(&CatalogRequest::DropIndex { name: "app.same_idx".into(), cascade: false }).unwrap();
        assert_eq!(dropped.table.id, app.id);
        engine.execute_catalog(&CatalogRequest::DropIndex { name: "public.same_idx".into(), cascade: false }).unwrap();
        let bound = engine.prepare_catalog_bound(&create("app.later")).unwrap();
        admin.sql_session().execute("CREATE SCHEMA changed", &[], None).unwrap();
        assert_eq!(engine.execute_catalog_bound(&bound).unwrap_err().sqlstate(), Some("0A000"));
        for (native, statement) in [(create("absent.items"), "CREATE TABLE absent.items(id BIGINT PRIMARY KEY)"),
            (CatalogRequest::CreateIndex { table: "absent.items".into(), name: "missing_idx".into(), columns: vec!["id".into()] },
             "CREATE INDEX missing_idx ON absent.items(id)"),
            (CatalogRequest::DropTable { table: "absent.items".into(), cascade: false }, "DROP TABLE absent.items")] {
            let expected = giggles_core::sqlstate(&admin.sql_session().execute(statement, &[], None).unwrap_err()).to_owned();
            assert_eq!(engine.execute_catalog(&native).unwrap_err().sqlstate(), Some(expected.as_str()));
        }
        for request in [read("absent.items"), insert("absent.items", 1),
            Request::Update { table: "absent.items".into(), filter: key(1), guard: None, values: vec![("id".into(), ValueExpr::Literal(V::Int(2)))], intent: Intent::One },
            Request::Delete { table: "absent.items".into(), filter: key(1), guard: None, cap: None }] {
            assert_eq!(engine.execute(&request, &Bindings::new()).unwrap_err().sqlstate(), Some("42P01"));
        }
        for bad in ["app.items.extra", ".items", "app.", "App.items", "app\0items", "app..items"] {
            assert_eq!(engine.execute(&read(bad), &Bindings::new()).unwrap_err(), Error::Bind("identifier".into()));
        }
        engine.execute_catalog(&CatalogRequest::DropTable { table: "app.items".into(), cascade: false }).unwrap();
        assert!(engine.execute(&read("items"), &Bindings::new()).is_ok());
        assert_eq!(engine.execute(&read("app.items"), &Bindings::new()).unwrap_err().sqlstate(), Some("42P01"));
    }
}

#[test]
fn primary_key_index_drop_matches_sql_and_preserves_catalog() {
    for backend in engines() {
        let path = common::fresh("native-schema-pk-index-drop");
        let admin = Admin::create_engine(&path, Limits::default(), backend).unwrap();
        let engine = Engine::new(admin.clone());
        let mut sql = admin.sql_session();
        sql.execute("CREATE SCHEMA app", &[], None).unwrap();
        sql.execute("CREATE TABLE app.t(id BIGINT PRIMARY KEY)", &[], None).unwrap();
        let before = admin.catalog().unwrap();
        let primary_names = admin.catalog_presentation().unwrap().primary_names;
        // PG18.6: constraint t_pkey on app.t requires its index (2BP01).
        let error = sql.execute("DROP INDEX app.t_pkey", &[], None).unwrap_err();
        assert_eq!(giggles_core::sqlstate(&error), "2BP01");
        assert_eq!(engine.execute_catalog(&CatalogRequest::DropIndex {
            name: "app.t_pkey".into(), cascade: false,
        }).unwrap_err().sqlstate(), Some("2BP01"));
        assert_eq!(admin.catalog().unwrap(), before);
        assert_eq!(admin.catalog_presentation().unwrap().primary_names, primary_names);
    }
}
