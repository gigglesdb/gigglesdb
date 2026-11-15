mod common;
use giggles_core::{Admin, Limits, Engine as Backend, operations::*, relational::Value};

fn cases() -> Vec<(ConstraintSpec, &'static str, &'static str, &'static str)> {
    vec![
        (ConstraintSpec::Unique { columns: vec!["n".into()] }, "UNIQUE (n)", "INSERT INTO child VALUES (2, 1)", "23505"),
        (ConstraintSpec::Check { expression: Expr::Compare(Box::new(Expr::Field("n".into())), Compare::Gt, Box::new(Expr::Value(ValueExpr::Literal(Value::Int(0))))) }, "CHECK (n > 0)", "INSERT INTO child VALUES (2, -1)", "23514"),
        (ConstraintSpec::ForeignKey { columns: vec!["n".into()], ref_table: "parent".into(), ref_columns: None, on_delete: ForeignKeyAction::Cascade, on_update: ForeignKeyAction::Cascade }, "FOREIGN KEY (n) REFERENCES parent ON DELETE CASCADE ON UPDATE CASCADE", "INSERT INTO child VALUES (2, 99)", "23503"),
        (ConstraintSpec::ForeignKey { columns: vec!["n".into()], ref_table: "parent".into(), ref_columns: Some(vec!["id".into()]), on_delete: ForeignKeyAction::Restrict, on_update: ForeignKeyAction::NoAction }, "FOREIGN KEY (n) REFERENCES parent(id) ON DELETE RESTRICT", "INSERT INTO child VALUES (2, 99)", "23503"),
    ]
}
fn fixture(backend: Backend) -> (std::path::PathBuf, Admin) {
    let path = common::fresh("native-constraints");
    let admin = Admin::create_engine(&path, Limits::default(), backend).unwrap();
    for sql in ["CREATE TABLE parent (id BIGINT PRIMARY KEY)", "CREATE TABLE child (id BIGINT PRIMARY KEY, n BIGINT)"] {
        admin.sql_session().execute(sql, &[], None).unwrap();
    }
    (path, admin)
}
fn add(spec: ConstraintSpec, named: bool) -> CatalogRequest {
    CatalogRequest::AddConstraint { table: "child".into(), name: named.then(|| "rule".into()), constraint: spec }
}
fn drop_request(name: &str, cascade: bool) -> CatalogRequest {
    CatalogRequest::DropConstraint { table: "child".into(), name: name.into(), cascade }
}
fn same(native: &Admin, sql: &Admin) {
    assert_eq!(native.catalog().unwrap().1, sql.catalog().unwrap().1);
    let (mut left, mut right) = (native.catalog_presentation().unwrap(), sql.catalog_presentation().unwrap());
    // V2 fences are timestamps from independent database domains, not logical definitions.
    left.epoch = 0; right.epoch = 0;
    assert_eq!(format!("{left:?}"), format!("{right:?}"));
}
fn state(error: giggles_core::CoreError) -> String { giggles_core::sqlstate(&error).into() }
fn backends() -> Vec<Backend> {
    let mut out = vec![Backend::Legacy];
    if cfg!(feature = "engine-rocksdb") { out.push(Backend::V2); }
    out
}
#[test]
fn named_and_generated_constraints_match_sql_enforce_reopen_and_drop() {
    for backend in backends() { for named in [false, true] { for (spec, ddl, invalid, expected) in cases() {
        let (path, native) = fixture(backend);
        let (_, sql) = fixture(backend);
        let request = add(spec, named);
        let engine = Engine::new(native.clone());
        let bound = engine.prepare_catalog_bound(&request).unwrap();
        let outcome = engine.execute_catalog_bound(&bound).unwrap();
        sql.sql_session().execute(&format!("ALTER TABLE child ADD {}{ddl}", if named { "CONSTRAINT rule " } else { "" }), &[], None).unwrap();
        assert_eq!(outcome.table, native.catalog().unwrap().1.into_iter().find(|t| t.name == "child").unwrap());
        assert!(outcome.index_id.is_none());
        same(&native, &sql);
        drop(engine); drop(native);
        let native = Admin::open(&path, Limits::default()).unwrap();
        same(&native, &sql);
        let bad = match expected { "23514" => -1, "23503" => 99, _ => 1 };
        let write = Request::Insert { table: "child".into(), values: vec![("id".into(), ValueExpr::Literal(Value::Int(2))), ("n".into(), ValueExpr::Literal(Value::Int(bad)))] };
        for admin in [&native, &sql] {
            for text in ["INSERT INTO parent VALUES (1)", "INSERT INTO child VALUES (1, 1)"] { admin.sql_session().execute(text, &[], None).unwrap(); }
            assert_eq!(Engine::new(admin.clone()).execute(&write, &Bindings::new()).unwrap_err().sqlstate(), Some(expected));
            assert_eq!(state(admin.sql_session().execute(invalid, &[], None).unwrap_err()), expected);
            if ddl.contains("CASCADE") {
                admin.sql_session().execute("UPDATE parent SET id = 2 WHERE id = 1", &[], None).unwrap();
                let rows = admin.sql_session().execute("SELECT n FROM child LIMIT 10", &[], None).unwrap().rows;
                assert_eq!(rows[0].values, vec![Value::Int(2)]);
                admin.sql_session().execute("DELETE FROM parent WHERE id = 2", &[], None).unwrap();
                assert!(admin.sql_session().execute("SELECT n FROM child LIMIT 10", &[], None).unwrap().rows.is_empty());
            }
            admin.sql_session().execute("DELETE FROM child WHERE id = 1", &[], None).unwrap();
            admin.sql_session().execute("DELETE FROM parent WHERE id = 1", &[], None).unwrap();
        }
        let name = native.catalog_presentation().unwrap().constraints.into_iter().find(|c| c.kind != 'p').unwrap().name;
        Engine::new(native.clone()).execute_catalog(&drop_request(&name, false)).unwrap();
        sql.sql_session().execute(&format!("ALTER TABLE child DROP CONSTRAINT {name}"), &[], None).unwrap();
        same(&native, &sql);
        for admin in [&native, &sql] { admin.sql_session().execute(invalid, &[], None).unwrap(); }
    } } }
}
#[test]
fn drop_restrict_dependencies_and_commit_rollback_are_atomic() {
    for backend in backends() {
        let (path, admin) = fixture(backend);
        let engine = Engine::new(admin.clone());
        let request = add(cases().remove(0).0, true);
        engine.execute_catalog(&request).unwrap();
        admin.sql_session().execute("CREATE TABLE dependent (id BIGINT PRIMARY KEY, n BIGINT REFERENCES child(n))", &[], None).unwrap();
        let before = format!("{:?}", admin.catalog_presentation().unwrap());
        assert_eq!(engine.execute_catalog(&drop_request("rule", false)).unwrap_err().sqlstate(), Some("2BP01"));
        assert_eq!(state(admin.sql_session().execute("ALTER TABLE child DROP CONSTRAINT rule", &[], None).unwrap_err()), "2BP01");
        assert_eq!(format!("{:?}", admin.catalog_presentation().unwrap()), before);
        // Exercise rollback at the native catalog publication boundary on legacy.
        if backend == Backend::Legacy {
            admin.set_crash_point(giggles_core::CrashPoint::ErrorBeforeCommit).unwrap();
            assert_eq!(engine.execute_catalog(&add(cases().remove(1).0, false)), Err(Error::Unknown));
            drop(engine); drop(admin);
            let reopened = Admin::open(path, Limits::default()).unwrap();
            assert_eq!(format!("{:?}", reopened.catalog_presentation().unwrap()), before);
        }
    }
}
#[test]
fn populated_validation_and_failed_add_rollback_match_sql_or_legacy_plan_boundary() {
    for backend in backends() { for violating in [false, true] { for (spec, ddl, _, expected) in cases() {
        let (_, native) = fixture(backend);
        let (_, sql) = fixture(backend);
        let setup = if violating { "INSERT INTO child VALUES (1, -1), (2, -1)" } else { "INSERT INTO child VALUES (1, 1)" };
        for admin in [&native, &sql] {
            admin.sql_session().execute("INSERT INTO parent VALUES (1)", &[], None).unwrap();
            admin.sql_session().execute(setup, &[], None).unwrap();
        }
        let before = format!("{:?}", native.catalog_presentation().unwrap());
        let result = Engine::new(native.clone()).execute_catalog(&add(spec, true));
        if backend == Backend::Legacy {
            assert_eq!(result, Err(Error::PlanRequired));
            assert_eq!(format!("{:?}", native.catalog_presentation().unwrap()), before);
            continue;
        }
        let paired = sql.sql_session().execute(&format!("ALTER TABLE child ADD CONSTRAINT rule {ddl}"), &[], None);
        if violating {
            assert_eq!(result.unwrap_err().sqlstate(), Some(expected));
            assert_eq!(state(paired.unwrap_err()), expected);
            assert_eq!(format!("{:?}", native.catalog_presentation().unwrap()), before);
        } else {
            result.unwrap(); paired.unwrap();
            Engine::new(native.clone()).execute_catalog(&drop_request("rule", false)).unwrap();
            sql.sql_session().execute("ALTER TABLE child DROP CONSTRAINT rule", &[], None).unwrap();
        }
        same(&native, &sql);
    } } }
}
#[test]
fn cascade_known_gap_and_request_decoder_are_pinned() {
    let (_, admin) = fixture(Backend::Legacy);
    let request = add(cases().remove(0).0, true);
    let bytes = serde_json::to_vec(&request).unwrap();
    assert_eq!(serde_json::from_slice::<CatalogRequest>(&bytes).unwrap(), request);
    assert!(serde_json::from_str::<CatalogRequest>(r#"{"DropConstraint":{"table":"child","name":"rule","cascade":false,"extra":true}}"#).is_err());
    let engine = Engine::new(admin.clone());
    let rich = add(ConstraintSpec::Check { expression: Expr::Not(Box::new(Expr::Value(ValueExpr::Literal(Value::Bool(false))))) }, false);
    assert_eq!(engine.execute_catalog(&rich), Err(Error::Unsupported));
    let reserved = CatalogRequest::AddConstraint { table: "system_native".into(), name: None, constraint: cases().remove(0).0 };
    let expected = state(admin.sql_session().execute("ALTER TABLE system_native ADD UNIQUE(n)", &[], None).unwrap_err());
    assert_eq!(engine.execute_catalog(&reserved).unwrap_err().sqlstate(), Some(expected.as_str()));
    assert!(serde_json::from_str::<CatalogRequest>(r#"{"AddConstraint":{"table":"child","name":null,"constraint":{"Unique":{"columns":["n"],"deferrable":true}}}}"#).is_err());
    engine.execute_catalog(&request).unwrap();
    // Known SQL/PG gap, blocked by persisted FK-index identity: J:/gigglesdb-work/r2/r2-33/design.md (DD-0068).
    let sql_error = admin.sql_session().execute("ALTER TABLE child DROP CONSTRAINT rule CASCADE", &[], None).unwrap_err();
    assert_eq!(state(sql_error), "42601");
    assert_eq!(engine.execute_catalog(&drop_request("rule", true)).unwrap_err().sqlstate(), Some("42601"));
    assert!(admin.catalog_presentation().unwrap().constraints.iter().any(|c| c.name == "rule"));
    assert_eq!(engine.execute_catalog(&drop_request("missing", false)).unwrap_err().sqlstate(), Some("42704"));
}
#[test]
fn outcome_uses_sql_schema_resolution_for_same_named_tables() {
    for backend in backends() {
        let (_, native) = fixture(backend);
        let (_, sql) = fixture(backend);
        let target = native.catalog().unwrap().1.into_iter().find(|t| t.name == "child").unwrap();
        for admin in [&native, &sql] {
            for text in ["CREATE SCHEMA aaa", "CREATE TABLE aaa.child (id BIGINT PRIMARY KEY)"] { admin.sql_session().execute(text, &[], None).unwrap(); }
        }
        let engine = Engine::new(native.clone());
        assert_eq!(engine.execute_catalog(&add(cases().remove(0).0, true)).unwrap().table, target);
        sql.sql_session().execute("ALTER TABLE child ADD CONSTRAINT rule UNIQUE(n)", &[], None).unwrap();
        same(&native, &sql);
        assert_eq!(engine.execute_catalog(&drop_request("rule", false)).unwrap().table, target);
        sql.sql_session().execute("ALTER TABLE child DROP CONSTRAINT rule", &[], None).unwrap();
        same(&native, &sql);
    }
}

#[test]
fn qualified_constraints_match_sql_and_resolve_targets() {
    for backend in backends() {
        let mut paths = vec![];
        let admins: Vec<_> = (0..2).map(|_| {
            let path = common::fresh("qualified-constraints");
            let admin = Admin::create_engine(&path, Limits::default(), backend).unwrap();
            paths.push(path);
            for text in ["CREATE SCHEMA app", "CREATE TABLE app.t(id int PRIMARY KEY, c int)",
                "CREATE TABLE t(id int PRIMARY KEY, c int)", "CREATE TABLE app.k(id int PRIMARY KEY, pid int)"] {
                admin.sql_session().execute(text, &[], None).unwrap();
            }
            admin
        }).collect();
        let (native, sql) = (&admins[0], &admins[1]);
        let engine = Engine::new(native.clone());
        let unique = |table: &str| CatalogRequest::AddConstraint { table: table.into(), name: Some("uq".into()), constraint: ConstraintSpec::Unique { columns: vec!["c".into()] } };
        let foreign = |parent: &str| CatalogRequest::AddConstraint { table: "app.k".into(), name: Some("fk".into()), constraint: ConstraintSpec::ForeignKey { columns: vec!["pid".into()], ref_table: parent.into(), ref_columns: Some(vec!["id".into()]), on_delete: ForeignKeyAction::NoAction, on_update: ForeignKeyAction::NoAction } };
        let schemas = native.catalog_schema_metadata().unwrap();
        let target = |schema: &str, name: &str| native.catalog().unwrap().1.into_iter()
            .find(|t| t.name == name && schemas.tables.get(&t.id).map(String::as_str).unwrap_or("public") == schema).unwrap();
        let public = target("public", "t");
        let child = target("app", "k");
        let target = target("app", "t");
        let app = engine.execute_catalog(&unique("app.t")).unwrap().table;
        assert_eq!(app.id, target.id);
        assert_ne!(app.id, public.id);
        sql.sql_session().execute("ALTER TABLE app.t ADD CONSTRAINT uq UNIQUE(c)", &[], None).unwrap();
        same(native, sql);
        let outcome = engine.execute_catalog(&foreign("app.t")).unwrap();
        sql.sql_session().execute("ALTER TABLE app.k ADD CONSTRAINT fk FOREIGN KEY(pid) REFERENCES app.t(id)", &[], None).unwrap();
        assert_eq!(outcome.table.id, child.id);
        same(native, sql);
        let drop_request = CatalogRequest::DropConstraint { table: "app.t".into(), name: "uq".into(), cascade: false };
        assert_eq!(engine.execute_catalog(&drop_request).unwrap().table.id, app.id);
        sql.sql_session().execute("ALTER TABLE app.t DROP CONSTRAINT uq", &[], None).unwrap();
        same(native, sql);
        assert_eq!(engine.execute_catalog(&unique("t")).unwrap().table.id, public.id);
        sql.sql_session().execute("ALTER TABLE t ADD CONSTRAINT uq UNIQUE(c)", &[], None).unwrap();
        same(native, sql);
        let mut missing_parent = foreign("nope.t");
        if let CatalogRequest::AddConstraint { name, .. } = &mut missing_parent { *name = Some("bad".into()); }
        for (request, text) in [(unique("nope.t"), "ALTER TABLE nope.t ADD CONSTRAINT uq UNIQUE(c)"),
            (missing_parent, "ALTER TABLE app.k ADD CONSTRAINT bad FOREIGN KEY(pid) REFERENCES nope.t(id)")] {
            let before = format!("{:?}", native.catalog_presentation().unwrap());
            let expected = state(sql.sql_session().execute(text, &[], None).unwrap_err());
            // PG18.6: missing explicit DDL schemas give 3F000.
            assert_eq!(expected, "3F000");
            assert_eq!(engine.execute_catalog(&request).unwrap_err().sqlstate(), Some(expected.as_str()));
            assert_eq!(format!("{:?}", native.catalog_presentation().unwrap()), before);
        }
        let before = format!("{:?}", native.catalog_presentation().unwrap());
        for request in [unique("app.t.x"), unique("App.t"), foreign("App.t")] {
            assert_eq!(engine.execute_catalog(&request), Err(Error::Bind("identifier".into())));
            assert_eq!(format!("{:?}", native.catalog_presentation().unwrap()), before);
        }
        for admin in &admins {
            assert_eq!(state(admin.sql_session().execute("INSERT INTO app.k VALUES(1,99)", &[], None).unwrap_err()), "23503");
        }
        drop(engine); drop(admins);
        for path in paths { std::fs::remove_dir_all(path).unwrap(); }
    }
}
