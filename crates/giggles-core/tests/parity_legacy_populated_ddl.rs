mod common;
// Native ALTER TABLE and ADD/DROP CONSTRAINT on populated Legacy tables follow
// SQL's DDL executor: same result or SQLSTATE, rows, catalog and reopened state.
// They formerly returned native PlanRequired while SQL succeeded.
use giggles_core::{
    Admin, Limits,
    operations::{
        AlterTableAction as A, Arithmetic, Bindings, CatalogRequest, ColumnSpec, Compare,
        ConstraintSpec, Engine, Expr, ForeignKeyAction, Intent, Request, ValueExpr,
        once::{ONCE_CONTRACT_VERSION, OnceBody, OnceClockUnavailable, OnceIdentity, OnceRequest, OnceTime},
    },
    relational::{DataType, Value},
};
use std::path::PathBuf;

fn fixture() -> (PathBuf, Admin) {
    let path = common::fresh("legacy-populated-ddl");
    let admin = Admin::create(&path, Limits::default()).unwrap();
    for sql in [
        "CREATE TABLE parent (id BIGINT PRIMARY KEY)",
        "CREATE TABLE child (id BIGINT PRIMARY KEY, n BIGINT, v BIGINT)",
        "INSERT INTO parent VALUES (1), (2)",
        "INSERT INTO child VALUES (1, 1, 10), (2, 1, 20)",
    ] {
        admin.sql_session().execute(sql, &[], None).unwrap();
    }
    (path, admin)
}
fn snapshot(admin: &Admin) -> String {
    let (epoch, tables) = admin.catalog().unwrap();
    let mut out = format!("{epoch}|{tables:?}|{:?}", admin.catalog_presentation().unwrap());
    for table in tables {
        let rows = admin.sql_session().execute(&format!("SELECT * FROM {} ORDER BY id", table.name), &[], None).unwrap();
        out.push_str(&format!("|{:?}{:?}", rows.columns, rows.rows));
    }
    out
}
fn same(native: &Admin, sql: &Admin) -> String {
    let expected = snapshot(sql);
    assert_eq!(snapshot(native), expected);
    expected
}
fn reopen(path: &PathBuf) -> Admin {
    Admin::open(path, Limits::default()).unwrap()
}
fn column(name: &str, not_null: bool) -> ColumnSpec {
    ColumnSpec { name: name.into(), data_type: DataType::BigInt, not_null, default: None, generated: None }
}
fn alter(action: A) -> CatalogRequest {
    CatalogRequest::AlterTable { table: "child".into(), action }
}
fn add(name: Option<&str>, constraint: ConstraintSpec) -> CatalogRequest {
    CatalogRequest::AddConstraint { table: "child".into(), name: name.map(Into::into), constraint }
}
fn check_gt(field: &str, bound: i64) -> ConstraintSpec {
    ConstraintSpec::Check { expression: Expr::Compare(
        Box::new(Expr::Field(field.into())), Compare::Gt, Box::new(Expr::Value(ValueExpr::Literal(Value::Int(bound)))),
    ) }
}
fn fk(field: &str) -> ConstraintSpec {
    ConstraintSpec::ForeignKey { columns: vec![field.into()], ref_table: "parent".into(), ref_columns: None,
        on_delete: ForeignKeyAction::NoAction, on_update: ForeignKeyAction::NoAction }
}
fn cases() -> Vec<(CatalogRequest, &'static str, Option<&'static str>)> {
    vec![
        (alter(A::AddColumn { column: column("x", false) }), "ALTER TABLE child ADD COLUMN x BIGINT", None),
        (alter(A::AddColumn { column: column("req", true) }), "ALTER TABLE child ADD COLUMN req BIGINT NOT NULL", Some("23502")),
        (alter(A::DropColumn { name: "v".into(), cascade: false }), "ALTER TABLE child DROP COLUMN v", None),
        (alter(A::RenameColumn { from: "v".into(), to: "w".into() }), "ALTER TABLE child RENAME COLUMN v TO w", None),
        (alter(A::RenameTable { to: "kid".into() }), "ALTER TABLE child RENAME TO kid", None),
        (add(Some("pos"), check_gt("v", 0)), "ALTER TABLE child ADD CONSTRAINT pos CHECK (v > 0)", None),
        (add(None, check_gt("v", 15)), "ALTER TABLE child ADD CHECK (v > 15)", Some("23514")),
        (add(Some("uv"), ConstraintSpec::Unique { columns: vec!["v".into()] }), "ALTER TABLE child ADD CONSTRAINT uv UNIQUE (v)", None),
        (add(None, ConstraintSpec::Unique { columns: vec!["n".into()] }), "ALTER TABLE child ADD UNIQUE (n)", Some("23505")),
        (add(Some("fkn"), fk("n")), "ALTER TABLE child ADD CONSTRAINT fkn FOREIGN KEY (n) REFERENCES parent", None),
        (add(None, fk("v")), "ALTER TABLE child ADD FOREIGN KEY (v) REFERENCES parent", Some("23503")),
    ]
}

#[test]
fn populated_legacy_alter_and_constraints_match_sql_and_reopen() {
    for (request, statement, expected) in cases() {
        let (native_path, native) = fixture();
        let (sql_path, sql) = fixture();
        let before = same(&native, &sql);
        let actual = Engine::new(native.clone()).execute_catalog(&request);
        let paired = sql.sql_session().execute(statement, &[], None);
        match expected {
            Some(state) => {
                assert_eq!(giggles_core::sqlstate(&paired.unwrap_err()), state, "{statement}");
                assert_eq!(actual.unwrap_err().sqlstate(), Some(state), "{statement}");
                // A failed native ADD leaves rows and catalog unchanged.
                assert_eq!(same(&native, &sql), before, "{statement}");
            }
            None => {
                assert_eq!(paired.unwrap().affected, 0, "{statement}");
                let outcome = actual.unwrap_or_else(|e| panic!("{statement}: {e:?}"));
                assert_eq!(outcome.index_id, None);
                assert_eq!(outcome.catalog_epoch, native.catalog().unwrap().0);
                assert!(native.catalog().unwrap().1.contains(&outcome.table), "{statement}");
                assert_ne!(same(&native, &sql), before, "{statement}");
            }
        }
        // Later statements work identically on both, and the result survives reopen.
        let later = Engine::new(native.clone()).execute_catalog(&CatalogRequest::AlterTable {
            table: "parent".into(), action: A::AddColumn { column: column("later", false) } });
        later.unwrap_or_else(|e| panic!("{statement}: later {e:?}"));
        sql.sql_session().execute("ALTER TABLE parent ADD COLUMN later BIGINT", &[], None).unwrap();
        for admin in [&native, &sql] {
            admin.sql_session().execute("INSERT INTO parent VALUES (3, 30)", &[], None).unwrap();
        }
        let expected = same(&native, &sql);
        drop(native); drop(sql);
        let (native, sql) = (reopen(&native_path), reopen(&sql_path));
        assert_eq!(same(&native, &sql), expected, "{statement}");
    }
}

#[test]
fn populated_legacy_drop_constraint_matches_sql_and_restrict_errors() {
    let (native_path, native) = fixture();
    let (_, sql) = fixture();
    for admin in [&native, &sql] {
        for text in ["ALTER TABLE child ADD CONSTRAINT uv UNIQUE (v)",
            "CREATE TABLE dependent (id BIGINT PRIMARY KEY, v BIGINT REFERENCES child(v))",
            "INSERT INTO dependent VALUES (1, 10)",
            "ALTER TABLE child ADD CONSTRAINT pos CHECK (v > 0)"] {
            admin.sql_session().execute(text, &[], None).unwrap();
        }
    }
    let engine = Engine::new(native.clone());
    let drop_rule = |name: &str| CatalogRequest::DropConstraint { table: "child".into(), name: name.into(), cascade: false };
    let before = same(&native, &sql);
    for (name, state) in [("uv", "2BP01"), ("nope", "42704")] {
        assert_eq!(engine.execute_catalog(&drop_rule(name)).unwrap_err().sqlstate(), Some(state));
        let error = sql.sql_session().execute(&format!("ALTER TABLE child DROP CONSTRAINT {name}"), &[], None).unwrap_err();
        assert_eq!(giggles_core::sqlstate(&error), state);
        assert_eq!(same(&native, &sql), before);
    }
    // The bound path, with populated rows and a constraint still enforced until dropped.
    let bound = engine.prepare_catalog_bound(&drop_rule("pos")).unwrap();
    for admin in [&native, &sql] {
        let error = admin.sql_session().execute("INSERT INTO child VALUES (3, 1, -1)", &[], None).unwrap_err();
        assert_eq!(giggles_core::sqlstate(&error), "23514");
    }
    engine.execute_catalog_bound(&bound).unwrap();
    sql.sql_session().execute("ALTER TABLE child DROP CONSTRAINT pos", &[], None).unwrap();
    assert_ne!(same(&native, &sql), before);
    for admin in [&native, &sql] {
        admin.sql_session().execute("INSERT INTO child VALUES (3, 1, -1)", &[], None).unwrap();
    }
    let expected = same(&native, &sql);
    drop(engine); drop(native);
    assert_eq!(snapshot(&reopen(&native_path)), expected);
}

fn once_clock() -> Result<OnceTime, OnceClockUnavailable> {
    Ok(OnceTime {
        lower_unix_seconds: 1000,
        upper_unix_seconds: 1001,
        valid_until: std::time::Instant::now() + std::time::Duration::from_secs(10),
    })
}

#[test]
fn retained_once_outcomes_and_business_rows_do_not_block_alter() {
    // The Legacy empty-database policy also counts native once receipts and
    // business rows. Receipts replay before live catalog binding, so ALTER
    // cannot make them inconsistent, and SQL ALTER already runs in this state.
    let (native_path, native) = fixture();
    let (_, sql) = fixture();
    let increment = Request::Returning {
        request: Box::new(Request::UpdateExpressions {
            table: "child".into(),
            filter: Expr::Compare(Box::new(Expr::Field("id".into())), Compare::Eq,
                Box::new(Expr::Value(ValueExpr::Literal(Value::Int(1))))),
            guard: None,
            values: vec![("v".into(), Expr::Arithmetic(Box::new(Expr::Field("v".into())), Arithmetic::Add,
                Box::new(Expr::Value(ValueExpr::Literal(Value::Int(1))))))],
            intent: Intent::One,
        }),
        projection: vec![],
    };
    let mut receipts = Vec::new();
    for admin in [&native, &sql] {
        admin.put(common::key(1), common::row()).unwrap();
        let request = OnceRequest {
            contract_version: ONCE_CONTRACT_VERSION,
            identity: OnceIdentity {
                database_domain: admin.snapshot().unwrap().domain,
                tenant: 42,
                namespace: "application.jobs".into(),
                key: b"job-1".to_vec(),
            },
            retention_seconds: 60,
            body: OnceBody::Operation(increment.clone()),
        };
        let receipt = Engine::new(admin.clone()).execute_once(&request, &Bindings::new(), &once_clock).unwrap();
        receipts.push((request, receipt));
    }
    let engine = Engine::new(native.clone());
    for (request, statement) in [
        (alter(A::RenameColumn { from: "v".into(), to: "w".into() }), "ALTER TABLE child RENAME COLUMN v TO w"),
        (add(Some("pos"), check_gt("w", 0)), "ALTER TABLE child ADD CONSTRAINT pos CHECK (w > 0)"),
        (alter(A::RenameTable { to: "kid".into() }), "ALTER TABLE child RENAME TO kid"),
    ] {
        engine.execute_catalog(&request).unwrap_or_else(|e| panic!("{statement}: {e:?}"));
        sql.sql_session().execute(statement, &[], None).unwrap();
        same(&native, &sql);
    }
    let (request, first) = &receipts[0];
    let replay = engine.execute_once(request, &Bindings::new(), &once_clock).unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.outcome, first.outcome);
    let expected = same(&native, &sql);
    drop(engine); drop(native);
    assert_eq!(snapshot(&reopen(&native_path)), expected);
}
