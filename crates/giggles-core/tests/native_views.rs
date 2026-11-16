mod common;
use giggles_core::{Admin, CoreError, Engine as Backend, Limits, relational::*, operations::{self, CatalogRequest}};
fn read(table: &str) -> SingleSourceRead {
    SingleSourceRead { table: table.into(), projection: vec![ReadProjection {
        expression: ReadExpr::Column("id".into()), alias: None,
    }], filter: None }
}
fn create(name: &str, source: &str) -> CatalogRequest {
    CatalogRequest::CreateView { name: name.into(), read: read(source).into() }
}
fn drop_view(name: &str, cascade: bool) -> CatalogRequest { CatalogRequest::DropView { name: name.into(), cascade } }
fn state(code: &str) -> operations::Error { operations::Error::Sql { sqlstate: code.into() } }
#[test]
fn legacy_view_ddl_matches_sql_even_when_populated() {
    let path = common::fresh("native-view-legacy");
    let admin = Admin::create_engine(&path, Limits::default(), Backend::Legacy).unwrap();
    let engine = operations::Engine::new(admin.clone());
    let mut s = admin.sql_session();
    s.execute("CREATE TABLE t(id INT PRIMARY KEY)", &[], None).unwrap();
    for populated in [false, true] {
        if populated { s.execute("INSERT INTO t VALUES(1)", &[], None).unwrap(); }
        for (sql, request) in [("CREATE VIEW v AS SELECT id FROM t", create("v", "t")), ("DROP VIEW v", drop_view("v", false))] {
            let error = s.execute(sql, &[], None).unwrap_err();
            assert_eq!(error, CoreError::Sql(SqlError::Unsupported));
            assert_eq!(engine.execute_catalog(&request), Err(state(giggles_core::sqlstate(&error))));
        }
    }
    drop(s); drop(engine); drop(admin); std::fs::remove_dir_all(path).unwrap();
}
#[test]
fn view_requests_roundtrip_and_reject_unknown_options() {
    for request in [create("v", "t"), drop_view("v", true)] {
        assert_eq!(bincode::deserialize::<CatalogRequest>(&bincode::serialize(&request).unwrap()).unwrap(), request);
        let json = serde_json::to_value(&request).unwrap();
        assert_eq!(serde_json::from_value::<CatalogRequest>(json).unwrap(), request);
    }
    assert!(serde_json::from_str::<CatalogRequest>(r#"{"CreateView":{"name":"v","read":{"Single":{"table":"t","projection":[],"filter":null}},"replace":true}}"#).is_err());
    assert!(serde_json::from_str::<CatalogRequest>(r#"{"CreateView":{"name":"v","read":{"Single":{"table":"t","projection":[{"expression":{"Column":"id"},"alias":null,"secret":true}],"filter":null}}}}"#).is_err());
}
#[cfg(feature = "engine-rocksdb")]
#[test]
fn native_views_share_sql_lifecycle_dependencies_and_reopen() {
    let path = common::fresh("native-view-v2");
    let admin = Admin::create_engine(&path, Limits::default(), Backend::V2).unwrap();
    let engine = operations::Engine::new(admin.clone());
    let mut s = admin.sql_session();
    s.execute("CREATE TABLE t(id INT PRIMARY KEY)", &[], None).unwrap();
    s.execute("INSERT INTO t VALUES(1),(2)", &[], None).unwrap();
    let bound = engine.prepare_catalog_bound(&create("v", "t")).unwrap();
    let view = engine.execute_catalog_bound(&bound).unwrap();
    assert_eq!(view.table.name, "v");
    assert!(view.table.primary_key.is_empty());
    assert_eq!(view.table.columns[0].name, "id");
    assert_eq!(engine.execute_catalog_bound(&bound), Err(state("0A000")));
    assert_eq!(s.execute_typed_read(read("v"), None), s.execute("SELECT id FROM v", &[], None));
    let prepared = s.prepare("SELECT id FROM v").unwrap();
    let actual = s.describe_typed_read(read("v")).unwrap();
    let expected = s.describe(&prepared).unwrap();
    assert_eq!((actual.columns, actual.type_oids, actual.typmods), (expected.columns, expected.type_oids, expected.typmods));
    // Native reads of a view read SQL's expansion of it (PG 18.6 reads views in FROM).
    let bare = engine.execute(&operations::Request::Read { table: "v".into(), filter: None, order: vec![], limit: None, projection: vec![] }, &operations::Bindings::new()).unwrap();
    let expected = s.execute("SELECT * FROM v", &[], None).unwrap();
    assert_eq!((bare.columns, bare.rows.into_iter().map(|r| r.values).collect::<Vec<_>>()),
        (expected.columns, expected.rows.into_iter().map(|r| r.values).collect::<Vec<_>>()));
    engine.execute_catalog(&create("w", "v")).unwrap();
    assert_eq!(engine.execute_catalog(&drop_view("v", false)), Err(state("2BP01")));
    s.execute("BEGIN", &[], None).unwrap();
    assert_eq!(s.execute_typed_view("v".into(), None, true).unwrap(), view.table);
    s.execute("ROLLBACK", &[], None).unwrap();
    assert_eq!(s.execute_typed_read(read("w"), None).unwrap().rows.len(), 2);
    s.execute("BEGIN", &[], None).unwrap();
    s.execute_typed_view("temporary".into(), Some(read("t").into()), false).unwrap();
    s.execute("ROLLBACK", &[], None).unwrap();
    assert_eq!(s.execute_typed_read(read("temporary"), None), Err(CoreError::Sql(SqlError::UnknownTable)));
    assert_eq!(engine.execute_catalog(&CatalogRequest::DropTable { table: "t".into(), cascade: false }), Err(state("2BP01")));
    drop(s); drop(engine); drop(admin);
    let admin = Admin::open(&path, Limits::default()).unwrap();
    let engine = operations::Engine::new(admin.clone());
    let mut s = admin.sql_session();
    assert_eq!(s.execute_typed_read(read("w"), None).unwrap().rows.len(), 2);
    assert_eq!(engine.execute_catalog(&drop_view("v", true)).unwrap().table, view.table);
    assert_eq!(s.execute_typed_read(read("w"), None), Err(CoreError::Sql(SqlError::UnknownTable)));
    engine.execute_catalog(&create("v", "t")).unwrap();
    engine.execute_catalog(&CatalogRequest::DropTable { table: "t".into(), cascade: true }).unwrap();
    assert_eq!(s.execute_typed_read(read("v"), None), Err(CoreError::Sql(SqlError::UnknownTable)));
    drop(s); drop(engine); drop(admin); std::fs::remove_dir_all(path).unwrap();
}
#[cfg(feature = "engine-rocksdb")]
#[test]
fn typed_view_ddl_preserves_sql_rejections_and_search_path() {
    let path = common::fresh("native-view-errors");
    let admin = Admin::create_engine(&path, Limits::default(), Backend::V2).unwrap();
    let engine = operations::Engine::new(admin.clone());
    let mut s = admin.sql_session();
    s.execute("CREATE TABLE t(id INT PRIMARY KEY)", &[], None).unwrap();
    engine.execute_catalog(&create("v", "t")).unwrap();
    assert_eq!(engine.execute_catalog(&create("v", "t")), Err(operations::Error::Duplicate));
    assert_eq!(engine.execute_catalog(&drop_view("t", false)), Err(state("42809")));
    assert_eq!(engine.execute_catalog(&drop_view("missing", false)), Err(state("42P01")));
    let join = TwoSourceRead { left: ReadSource { table: "t".into(), alias: "a".into() },
        right: ReadSource { table: "t".into(), alias: "b".into() }, kind: ReadJoinKind::Inner,
        on: ReadPredicate::Compare(ReadExpr::QualifiedColumn { source: "a".into(), column: "id".into() }, ReadCompare::Eq,
            ReadExpr::QualifiedColumn { source: "b".into(), column: "id".into() }),
        group_by: vec![], projection: vec![ReadProjection { expression: ReadExpr::QualifiedColumn { source: "a".into(), column: "id".into() }, alias: None }], filter: None };
    let error = s.execute("CREATE VIEW joined AS SELECT a.id FROM t a JOIN t b ON a.id=b.id", &[], None).unwrap_err();
    assert_eq!(engine.execute_catalog(&CatalogRequest::CreateView { name: "joined".into(), read: join.into() }), Err(state(giggles_core::sqlstate(&error))));
    s.execute("CREATE SCHEMA private", &[], None).unwrap();
    s.set_search_path("private", "giggles");
    s.execute("CREATE TABLE t(id INT PRIMARY KEY)", &[], None).unwrap();
    s.execute("INSERT INTO t VALUES(9)", &[], None).unwrap();
    s.execute_typed_view("v".into(), Some(read("t").into()), false).unwrap();
    assert_eq!(s.execute_typed_read(read("v"), None).unwrap().rows[0].values, vec![Value::Int(9)]);
    s.execute_typed_view("v".into(), None, false).unwrap();
    drop(s); drop(engine); drop(admin); std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn qualified_native_views_match_sql() {
    let mut backends = vec![Backend::Legacy];
    if cfg!(feature = "engine-rocksdb") { backends.push(Backend::V2); }
    for backend in backends {
        let np = common::fresh("native-view-qualified");
        let sp = common::fresh("sql-view-qualified");
        let native = Admin::create_engine(&np, Limits::default(), backend).unwrap();
        let sql = Admin::create_engine(&sp, Limits::default(), backend).unwrap();
        let engine = operations::Engine::new(native.clone());
        let (mut ns, mut ss) = (native.sql_session(), sql.sql_session());
        for session in [&mut ns, &mut ss] {
            for statement in ["CREATE SCHEMA app", "CREATE TABLE t(id INT PRIMARY KEY, n INT)", "INSERT INTO t VALUES(1,9)"] {
                session.execute(statement, &[], None).unwrap();
            }
        }
        if matches!(backend, Backend::Legacy) {
            let error = ss.execute("CREATE VIEW app.v AS SELECT id FROM t", &[], None).unwrap_err();
            assert_eq!(engine.execute_catalog(&create("app.v", "t")), Err(state(giggles_core::sqlstate(&error))));
        } else {
            let same = || {
                let (mut left, mut right) = (native.catalog_presentation().unwrap(), sql.catalog_presentation().unwrap());
                left.epoch = 0; right.epoch = 0;
                assert_eq!(format!("{left:?}"), format!("{right:?}"));
            };
            for session in [&mut ns, &mut ss] { session.execute("CREATE VIEW public.v AS SELECT n FROM t", &[], None).unwrap(); }
            let outcome = engine.execute_catalog(&create("app.v", "t")).unwrap();
            ss.execute("CREATE VIEW app.v AS SELECT id FROM t", &[], None).unwrap();
            same();
            let presentation = native.catalog_presentation().unwrap();
            let app_view = presentation.views.iter().find(|(_, v)| v.table.columns[0].name == "id").unwrap();
            assert_eq!(outcome.table, app_view.1.table);
            for session in [&mut ns, &mut ss] { session.set_search_path("app, public", "giggles"); }
            let rows = ns.execute_typed_read(read("v"), None).unwrap();
            assert_eq!(rows, ss.execute_typed_read(read("v"), None).unwrap());
            assert_eq!(rows.rows[0].values, vec![Value::Int(1)]);
            assert_eq!(engine.execute_catalog(&drop_view("app.v", false)).unwrap().table, outcome.table);
            ss.execute("DROP VIEW app.v", &[], None).unwrap();
            same();
            assert_eq!(ns.execute("SELECT n FROM public.v", &[], None).unwrap().rows[0].values, vec![Value::Int(9)]);
            let before = format!("{:?}", native.catalog_presentation().unwrap());
            for (request, statement) in [(create("nope.v", "t"), "CREATE VIEW nope.v AS SELECT id FROM t"),
                (drop_view("nope.v", false), "DROP VIEW nope.v")] {
                let error = ss.execute(statement, &[], None).unwrap_err();
                // PG18.6: CREATE and DROP VIEW both reject a missing schema with 3F000.
                assert_eq!(giggles_core::sqlstate(&error), "3F000");
                assert_eq!(engine.execute_catalog(&request), Err(state(giggles_core::sqlstate(&error))));
                assert_eq!(format!("{:?}", native.catalog_presentation().unwrap()), before);
            }
            let before = format!("{:?}", native.catalog_presentation().unwrap());
            for name in ["app.v.x", "App.v"] {
                for request in [create(name, "t"), drop_view(name, false)] {
                    assert_eq!(engine.execute_catalog(&request), Err(operations::Error::Bind("identifier".into())));
                    assert_eq!(format!("{:?}", native.catalog_presentation().unwrap()), before);
                }
            }
        }
        drop(ns); drop(ss); drop(engine); drop(native); drop(sql);
        std::fs::remove_dir_all(np).unwrap(); std::fs::remove_dir_all(sp).unwrap();
    }
}
