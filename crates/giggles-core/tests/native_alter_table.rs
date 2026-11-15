mod common;
use giggles_core::{Admin, CoreError, Limits, operations::{AlterTableAction as A, CatalogRequest, ColumnSpec, Engine, Error}, relational::{DataType, SqlError}};

fn request(table: &str, action: A) -> CatalogRequest {
    CatalogRequest::AlterTable { table: table.into(), action }
}
fn add(name: &str) -> A {
    A::AddColumn { column: ColumnSpec { name: name.into(), data_type: DataType::Integer, not_null: false, default: None, generated: None } }
}
fn snapshot(admin: &Admin) -> String {
    let mut p = admin.catalog_presentation().unwrap();
    p.epoch = 0; // V2 commits use independent physical clocks.
    format!("{:?}|{p:?}|{:?}", admin.catalog().unwrap().1, admin.secondary_index_catalog().unwrap().1)
}
fn exercise(v2: bool) {
    let paths = [common::fresh("native-alter"), common::fresh("sql-alter")];
    let open = |path: &std::path::Path| if v2 {
        Admin::create_engine(path, Limits::default(), giggles_core::Engine::V2).unwrap()
    } else { Admin::create(path, Limits::default()).unwrap() };
    let native = open(&paths[0]);
    let sql = open(&paths[1]);
    for admin in [&native, &sql] {
        let mut s = admin.sql_session();
        s.execute("CREATE TABLE t(id int PRIMARY KEY, c int, keep int)", &[], None).unwrap();
        if v2 { s.execute("INSERT INTO t VALUES(1,2,3)", &[], None).unwrap(); }
    }
    let engine = Engine::new(native.clone());
    for (action, statement, state) in [
        (add("keep"), "ALTER TABLE t ADD COLUMN keep int", "42701"),
        (A::DropColumn { name: "missing".into(), cascade: false }, "ALTER TABLE t DROP COLUMN missing", "42703"),
    ] {
        let before = snapshot(&native);
        assert_eq!(engine.execute_catalog(&request("t", action)), Err(Error::Sql { sqlstate: state.into() }));
        let error = sql.sql_session().execute(statement, &[], None).unwrap_err();
        assert_eq!(error, CoreError::Sql(if state == "42701" { SqlError::DuplicateColumn } else { SqlError::UnknownColumn }));
        assert_eq!(snapshot(&native), before);
        assert_eq!(snapshot(&native), snapshot(&sql));
    }
    if v2 {
        for admin in [&native, &sql] {
            for statement in ["CREATE VIEW v AS SELECT c FROM t", "CREATE VIEW vv AS SELECT c FROM v",
                "COMMENT ON VIEW v IS 'view'", "COMMENT ON COLUMN vv.c IS 'column'"] {
                admin.sql_session().execute(statement, &[], None).unwrap();
            }
        }
        let before = snapshot(&native);
        assert_eq!(engine.execute_catalog(&request("t", A::DropColumn { name: "c".into(), cascade: false })),
            Err(Error::Sql { sqlstate: "2BP01".into() }));
        assert_eq!(sql.sql_session().execute("ALTER TABLE t DROP COLUMN c RESTRICT", &[], None),
            Err(CoreError::Sql(SqlError::DependentObjects)));
        assert_eq!(snapshot(&native), before);
        assert_eq!(snapshot(&native), snapshot(&sql));
    }
    for (table, action, statement, postname) in [
        ("t", add("extra"), "ALTER TABLE t ADD COLUMN extra int", "t"),
        ("t", A::RenameColumn { from: "extra".into(), to: "renamed".into() }, "ALTER TABLE t RENAME COLUMN extra TO renamed", "t"),
        ("t", A::DropColumn { name: "c".into(), cascade: true }, "ALTER TABLE t DROP COLUMN c CASCADE", "t"),
        ("t", A::RenameTable { to: "renamed_table".into() }, "ALTER TABLE t RENAME TO renamed_table", "renamed_table"),
    ] {
        let bound = engine.prepare_catalog_bound(&request(table, action)).unwrap();
        let result = engine.execute_catalog_bound(&bound).unwrap();
        let sql_result = sql.sql_session().execute(statement, &[], None).unwrap();
        assert_eq!(sql_result.affected, 0);
        assert_eq!(result.table.name, postname);
        assert_eq!(result.index_id, None);
        assert_eq!(native.catalog().unwrap().1, vec![result.table]);
        assert_eq!(snapshot(&native), snapshot(&sql));
    }
    if v2 {
        assert!(native.catalog_presentation().unwrap().views.is_empty());
        assert!(native.catalog_presentation().unwrap().comments.is_empty());
        assert_eq!(native.sql_session().execute("SELECT * FROM renamed_table", &[], None).unwrap().rows,
            sql.sql_session().execute("SELECT * FROM renamed_table", &[], None).unwrap().rows);
    }
    let expected = snapshot(&native);
    drop(engine); drop(native); drop(sql);
    for path in paths {
        let reopened = Admin::open(path, Limits::default()).unwrap();
        assert_eq!(snapshot(&reopened), expected);
    }
}
#[test]
fn legacy_alter_matches_sql() { exercise(false); }
#[cfg(feature = "engine-rocksdb")]
#[test]
fn populated_v2_alter_matches_sql() { exercise(true); }

#[test]
fn legacy_populated_requires_plan_and_bound_handles_fence_drift() {
    let admin = Admin::create(common::fresh("alter-plan"), Limits::default()).unwrap();
    let engine = Engine::new(admin.clone());
    admin.sql_session().execute("CREATE TABLE t(id int PRIMARY KEY, c int)", &[], None).unwrap();
    let stale = engine.prepare_catalog_bound(&request("t", add("later"))).unwrap();
    engine.execute_catalog(&request("t", add("first"))).unwrap();
    assert_eq!(engine.execute_catalog_bound(&stale), Err(Error::Sql { sqlstate: "0A000".into() }));
    let bound = engine.prepare_catalog_bound(&request("t", add("later"))).unwrap();
    admin.sql_session().execute("INSERT INTO t(id) VALUES(1)", &[], None).unwrap();
    let before = snapshot(&admin);
    assert_eq!(engine.execute_catalog_bound(&bound), Err(Error::PlanRequired));
    assert_eq!(engine.execute_catalog(&request("t", add("later"))), Err(Error::PlanRequired));
    assert_eq!(snapshot(&admin), before);
}

#[test]
fn alter_decode_is_strict_at_every_level() {
    let missing_cascade = serde_json::json!({"AlterTable":{"table":"t","action":{"DropColumn":{"name":"c"}}}});
    assert!(serde_json::from_value::<CatalogRequest>(missing_cascade).is_err());
    let valid = serde_json::to_value(request("t", add("c"))).unwrap();
    assert_eq!(serde_json::from_value::<CatalogRequest>(valid.clone()).unwrap(), request("t", add("c")));
    for level in 0..3 {
        let mut value = valid.clone();
        let object = match level {
            0 => &mut value["AlterTable"],
            1 => &mut value["AlterTable"]["action"]["AddColumn"],
            _ => &mut value["AlterTable"]["action"]["AddColumn"]["column"],
        };
        object.as_object_mut().unwrap().insert("ignored".into(), true.into());
        assert!(serde_json::from_value::<CatalogRequest>(value).is_err());
    }
    for action in [A::DropColumn { name: "c".into(), cascade: false },
        A::RenameColumn { from: "c".into(), to: "d".into() }, A::RenameTable { to: "u".into() }] {
        let value = serde_json::to_value(request("t", action)).unwrap();
        let mut invalid = value.clone();
        invalid["AlterTable"]["action"].as_object_mut().unwrap().values_mut().next().unwrap()
            .as_object_mut().unwrap().insert("ignored".into(), true.into());
        assert!(serde_json::from_value::<CatalogRequest>(invalid).is_err());
        assert!(serde_json::from_value::<CatalogRequest>(value).is_ok());
    }
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn native_cascade_commit_failure_rolls_back_columns_views_and_comments() {
    let path = common::fresh("native-alter-rollback");
    let admin = Admin::create_engine(&path, Limits::default(), giggles_core::Engine::V2).unwrap();
    for sql in ["CREATE TABLE t(id int PRIMARY KEY, c int)", "INSERT INTO t VALUES(1,2)",
        "CREATE VIEW v AS SELECT c FROM t", "COMMENT ON VIEW v IS 'keep'"] {
        admin.sql_session().execute(sql, &[], None).unwrap();
    }
    let before = snapshot(&admin);
    let engine = Engine::new(admin.clone());
    admin.set_crash_point(giggles_core::CrashPoint::ErrorBeforeCommit).unwrap();
    assert_eq!(engine.execute_catalog(&request("t", A::DropColumn { name: "c".into(), cascade: true })), Err(Error::Unknown));
    drop(engine); drop(admin);
    let admin = Admin::open(path, Limits::default()).unwrap();
    assert_eq!(snapshot(&admin), before);
    assert_eq!(admin.sql_session().execute("SELECT * FROM v", &[], None).unwrap().rows.len(), 1);
}

fn qualified_exercise(v2: bool) {
    let paths = [common::fresh("native-qualified-alter"), common::fresh("sql-qualified-alter")];
    let open = |path: &std::path::Path| if v2 {
        Admin::create_engine(path, Limits::default(), giggles_core::Engine::V2).unwrap()
    } else { Admin::create(path, Limits::default()).unwrap() };
    let native = open(&paths[0]);
    let sql = open(&paths[1]);
    for admin in [&native, &sql] {
        for statement in ["CREATE SCHEMA app", "CREATE TABLE app.t(id int PRIMARY KEY, c int)",
            "CREATE TABLE t(id int PRIMARY KEY, c int)"] {
            admin.sql_session().execute(statement, &[], None).unwrap();
        }
    }
    let tables = native.catalog().unwrap().1;
    let app_id = tables[0].id;
    let public_id = tables[1].id;
    let engine = Engine::new(native.clone());
    for (table, action, statement, id, postname) in [
        ("app.t", add("x"), "ALTER TABLE app.t ADD COLUMN x int", app_id, "t"),
        ("public.t", A::RenameColumn { from: "c".into(), to: "d".into() },
            "ALTER TABLE public.t RENAME COLUMN c TO d", public_id, "t"),
        ("app.t", A::RenameTable { to: "u".into() }, "ALTER TABLE app.t RENAME TO u", app_id, "u"),
    ] {
        let result = engine.execute_catalog(&request(table, action)).unwrap();
        sql.sql_session().execute(statement, &[], None).unwrap();
        assert_eq!(result.table.id, id);
        assert_eq!(result.table.name, postname);
        assert_eq!(snapshot(&native), snapshot(&sql));
    }
    let before = snapshot(&native);
    let error = sql.sql_session().execute("ALTER TABLE nope.t ADD COLUMN y int", &[], None).unwrap_err();
    assert_eq!(engine.execute_catalog(&request("nope.t", add("y"))),
        Err(Error::Sql { sqlstate: giggles_core::sqlstate(&error).into() }));
    assert_eq!(snapshot(&native), before);
    assert_eq!(snapshot(&native), snapshot(&sql));
    for (table, action) in [("app.t.x", add("y")), ("App.t", add("y")),
        ("app.u", A::RenameTable { to: "app.u".into() })] {
        assert!(matches!(engine.execute_catalog(&request(table, action)), Err(Error::Bind(_))));
        assert_eq!(snapshot(&native), before);
    }
}
#[test]
fn legacy_qualified_alter_matches_sql() { qualified_exercise(false); }
#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2_qualified_alter_matches_sql() { qualified_exercise(true); }
