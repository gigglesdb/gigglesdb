mod common;

use giggles_core::{
    Admin, CrashPoint, Limits,
    operations::{
        Bindings, Compare, ENGINE_CONTRACT_VERSION, Engine, Error, Expr, Intent, Request, Sort,
        ValueExpr,
    },
    relational::{DataType, SqlRow, Value as V},
};

fn fixture() -> (std::path::PathBuf, Admin, Engine) {
    let path = common::fresh("bound-operations");
    let admin = Admin::create(&path, Limits::default()).unwrap();
    admin
        .sql_session()
        .execute(
            "CREATE TABLE items (id BIGINT PRIMARY KEY, state TEXT NOT NULL, n INTEGER, note TEXT)",
            &[],
            None,
        )
        .unwrap();
    let engine = Engine::new(admin.clone());
    (path, admin, engine)
}

fn literal(value: V) -> ValueExpr {
    ValueExpr::Literal(value)
}
fn parameter(name: &str) -> ValueExpr {
    ValueExpr::Parameter(name.into())
}
fn yes() -> Expr {
    Expr::Value(literal(V::Bool(true)))
}
fn equals(field: &str, value: ValueExpr) -> Expr {
    Expr::Compare(
        Box::new(Expr::Field(field.into())),
        Compare::Eq,
        Box::new(Expr::Value(value)),
    )
}
fn read() -> Request {
    Request::Read {
        table: "items".into(),
        filter: None,
        order: vec![],
        limit: Some(literal(V::Int(1024))),
        projection: vec![],
    }
}
fn insert_request(id: i64) -> Request {
    Request::Insert {
        table: "items".into(),
        values: vec![
            ("id".into(), literal(V::Int(id))),
            ("state".into(), literal(V::Text("ready".into()))),
        ],
    }
}
fn update(intent: Intent, state: &str) -> Request {
    Request::Update {
        table: "items".into(),
        filter: yes(),
        guard: None,
        values: vec![("state".into(), literal(V::Text(state.into())))],
        intent,
    }
}
fn insert_sql(admin: &Admin, id: i64) {
    admin
        .sql_session()
        .execute(
            "INSERT INTO items VALUES ($1,'ready',0,NULL)",
            &[V::Int(id)],
            None,
        )
        .unwrap();
}
fn rows(admin: &Admin) -> Vec<SqlRow> {
    admin
        .sql_session()
        .execute("SELECT * FROM items ORDER BY id LIMIT 1024", &[], None)
        .unwrap()
        .rows
}
fn digest(admin: &Admin) -> [u8; 32] {
    admin.export(common::fresh("bound-state-export")).unwrap()
}
fn stale() -> Error {
    Error::Sql { sqlstate: "0A000".into() }
}

#[test]
fn preparation_binds_actual_values_without_any_durable_decision() {
    let (_, admin, engine) = fixture();
    insert_sql(&admin, 1);
    let before = digest(&admin);
    let request = Request::Update {
        table: "items".into(),
        filter: equals("id", parameter("id")),
        guard: None,
        values: vec![("n".into(), parameter("value"))],
        intent: Intent::One,
    };
    let bindings = Bindings::from([("id".into(), V::Int(1)), ("value".into(), V::Int(17))]);
    let prepared = engine.prepare_bound(&request, &bindings).unwrap();
    assert!(prepared.description().columns.is_empty());
    assert_eq!(digest(&admin), before);

    for (bindings, expected) in [
        (Bindings::new(), Error::Bind("parameter missing".into())),
        (
            Bindings::from([
                ("id".into(), V::Int(1)),
                ("value".into(), V::Int(17)),
                ("extra".into(), V::Int(0)),
            ]),
            Error::Bind("parameters".into()),
        ),
        (
            Bindings::from([
                ("id".into(), V::Int(1)),
                ("value".into(), V::Text("not an integer".into())),
            ]),
            Error::Type,
        ),
    ] {
        assert_eq!(
            engine.prepare_bound(&request, &bindings).unwrap_err(),
            expected
        );
        assert_eq!(digest(&admin), before);
    }
    assert_eq!(rows(&admin)[0].values[2], V::Int(0));
}

#[test]
fn prepared_values_and_intent_do_not_follow_mutated_caller_inputs() {
    let (_, admin, engine) = fixture();
    let original_payload = V::Text("literal ' ; delete from items; --\0".into());
    let mut request = insert_request(1);
    if let Request::Insert { values, .. } = &mut request {
        values.push(("note".into(), parameter("payload")));
    }
    let mut bindings = Bindings::from([("payload".into(), original_payload.clone())]);
    let prepared = engine.prepare_bound(&request, &bindings).unwrap();

    request = Request::Delete {
        table: "items".into(),
        filter: yes(),
        guard: None,
        cap: Some(literal(V::Int(1024))),
    };
    bindings.insert("payload".into(), V::Text("caller changed this".into()));
    assert!(matches!(request, Request::Delete { .. }));
    let outcome = engine.execute_bound(&prepared).unwrap();
    assert_eq!(
        (outcome.matched, outcome.affected, outcome.changed),
        (1, 1, 1)
    );
    assert_eq!(
        rows(&admin)[0].values,
        vec![
            V::Int(1),
            V::Text("ready".into()),
            V::Null,
            original_payload
        ]
    );
}

#[test]
fn description_exposes_stable_catalog_ids_and_typed_outputs() {
    let (_, admin, engine) = fixture();
    let (epoch, tables) = admin.catalog().unwrap();
    let table = &tables[0];
    let request = Request::Read {
        table: "items".into(),
        filter: Some(equals("id", parameter("id"))),
        order: vec![],
        limit: None,
        projection: vec!["note".into(), "id".into(), "n".into()],
    };
    let prepared = engine
        .prepare_bound(&request, &Bindings::from([("id".into(), V::Int(1))]))
        .unwrap();
    let description = prepared.description();
    assert_eq!(description.contract_version, ENGINE_CONTRACT_VERSION);
    assert_eq!(
        description.database_domain,
        admin.snapshot().unwrap().domain
    );
    assert_eq!(description.catalog_epoch, epoch);
    assert_eq!(&description.table, table);
    assert_eq!(
        description.columns,
        vec![
            table.columns[3].clone(),
            table.columns[0].clone(),
            table.columns[2].clone()
        ]
    );
    assert_eq!(
        description
            .columns
            .iter()
            .map(|column| column.data_type.clone())
            .collect::<Vec<_>>(),
        vec![DataType::Text, DataType::BigInt, DataType::Integer]
    );
    assert_eq!(prepared.clone().description(), description);

    let claim = engine
        .prepare_bound(
            &Request::Update {
                table: "items".into(),
                filter: equals("state", literal(V::Text("ready".into()))),
                guard: None,
                values: vec![("state".into(), literal(V::Text("claimed".into())))],
                intent: Intent::Claim {
                    order: vec![Sort {
                        field: "id".into(),
                        descending: false,
                    }],
                },
            },
            &Bindings::new(),
        )
        .unwrap();
    assert_eq!(claim.description().columns, table.columns);
    let insert = engine
        .prepare_bound(&insert_request(1), &Bindings::new())
        .unwrap();
    assert!(insert.description().columns.is_empty());
}

#[test]
fn execution_checks_fresh_cardinality_and_caps_each_time() {
    let (_, admin, engine) = fixture();
    let one = engine
        .prepare_bound(&update(Intent::One, "single"), &Bindings::new())
        .unwrap();
    let many = engine
        .prepare_bound(
            &update(
                Intent::Many {
                    cap: parameter("cap"),
                },
                "capped",
            ),
            &Bindings::from([("cap".into(), V::Int(1))]),
        )
        .unwrap();
    let empty = digest(&admin);
    assert_eq!(engine.execute_bound(&one), Err(Error::NotFound));
    assert_eq!(digest(&admin), empty);

    insert_sql(&admin, 1);
    insert_sql(&admin, 2);
    let two_rows = digest(&admin);
    assert_eq!(engine.execute_bound(&one), Err(Error::AmbiguousMatch));
    assert_eq!(engine.execute_bound(&many), Err(Error::TooManyMatches));
    assert_eq!(digest(&admin), two_rows);

    admin
        .sql_session()
        .execute("DELETE FROM items WHERE id=2", &[], None)
        .unwrap();
    assert_eq!(engine.execute_bound(&one).unwrap().changed, 1);
    assert_eq!(rows(&admin)[0].values[1], V::Text("single".into()));
    assert_eq!(engine.execute_bound(&many).unwrap().changed, 1);
    assert_eq!(rows(&admin)[0].values[1], V::Text("capped".into()));

    insert_sql(&admin, 3);
    let before_reuse = digest(&admin);
    assert_eq!(engine.execute_bound(&many), Err(Error::TooManyMatches));
    assert_eq!(digest(&admin), before_reuse);
}

#[test]
fn foreign_domains_and_catalog_changes_reject_before_effects() {
    let (_, admin, engine) = fixture();
    let (_, other_admin, other_engine) = fixture();
    let prepared = engine
        .prepare_bound(&insert_request(1), &Bindings::new())
        .unwrap();
    assert_eq!(admin.catalog().unwrap(), other_admin.catalog().unwrap());
    assert_ne!(
        admin.snapshot().unwrap().domain,
        other_admin.snapshot().unwrap().domain
    );
    let other_before = digest(&other_admin);
    assert_eq!(other_engine.execute_bound(&prepared), Err(stale()));
    assert_eq!(digest(&other_admin), other_before);

    admin
        .sql_session()
        .execute("CREATE TABLE unrelated (id BIGINT PRIMARY KEY)", &[], None)
        .unwrap();
    assert_eq!(
        admin
            .catalog()
            .unwrap()
            .1
            .into_iter()
            .find(|table| table.name == "items")
            .unwrap(),
        prepared.description().table
    );
    let after_ddl = digest(&admin);
    assert_eq!(engine.execute_bound(&prepared), Err(stale()));
    assert_eq!(digest(&admin), after_ddl);
    assert!(rows(&admin).is_empty());
}

#[test]
fn process_local_handle_survives_reopening_the_same_database() {
    let (path, admin, engine) = fixture();
    let prepared = engine
        .prepare_bound(&insert_request(1), &Bindings::new())
        .unwrap();
    let description = prepared.description().clone();
    drop(engine);
    drop(admin);
    let reopened = Admin::open(&path, Limits::default()).unwrap();
    assert_eq!(
        reopened.snapshot().unwrap().domain,
        description.database_domain
    );
    let engine = Engine::new(reopened.clone());
    assert_eq!(engine.execute_bound(&prepared).unwrap().changed, 1);
    assert_eq!(prepared.description(), &description);
    assert_eq!(rows(&reopened)[0].values[0], V::Int(1));
}

#[test]
fn restore_preserves_schema_but_its_new_domain_rejects_old_handles() {
    let (_, admin, engine) = fixture();
    let prepared = engine
        .prepare_bound(&insert_request(1), &Bindings::new())
        .unwrap();
    let export = common::fresh("bound-restore-export");
    admin.export(&export).unwrap();
    let restored =
        Admin::restore(export, common::fresh("bound-restored"), Limits::default()).unwrap();
    assert_eq!(admin.catalog().unwrap(), restored.catalog().unwrap());
    assert_ne!(
        restored.snapshot().unwrap().domain,
        prepared.description().database_domain
    );
    let before = digest(&restored);
    assert_eq!(
        Engine::new(restored.clone()).execute_bound(&prepared),
        Err(stale())
    );
    assert_eq!(digest(&restored), before);
}

#[test]
fn preparing_an_oversized_table_does_not_scan_but_execution_still_requires_completeness() {
    let (_, admin, engine) = fixture();
    let mut sql = admin.sql_session();
    sql.execute(
        "CREATE TABLE pairs (tenant BIGINT, id BIGINT, PRIMARY KEY (tenant,id))",
        &[],
        None,
    )
    .unwrap();
    sql.execute("BEGIN", &[], None).unwrap();
    let insert = sql.prepare("INSERT INTO pairs VALUES ($1,$2)").unwrap();
    for id in 0..=giggles_core::operations::MAX_ROWS {
        sql.execute_prepared(&insert, &[V::Int(7), V::Int(id as i64)], None)
            .unwrap();
    }
    sql.execute("COMMIT", &[], None).unwrap();
    let before = digest(&admin);
    let mut request = read();
    if let Request::Read { table, limit, .. } = &mut request {
        *table = "pairs".into();
        *limit = Some(literal(V::Int(1)));
    }
    let prepared = engine.prepare_bound(&request, &Bindings::new()).unwrap();
    assert_eq!(digest(&admin), before);
    assert_eq!(engine.execute_bound(&prepared), Err(Error::Limit));
    assert_eq!(digest(&admin), before);
    assert_eq!(
        sql.execute("SELECT * FROM pairs LIMIT 1024", &[], None)
            .unwrap()
            .rows
            .len(),
        giggles_core::operations::MAX_ROWS
    );
    assert_eq!(
        sql.execute("SELECT id FROM pairs WHERE tenant=7 AND id=1024", &[], None)
            .unwrap()
            .rows[0]
            .values,
        vec![V::Int(1024)]
    );
}

#[test]
fn prepared_commit_failures_remain_unknown_before_and_after_persistence() {
    for (point, persisted) in [
        (CrashPoint::ErrorBeforeCommit, false),
        (CrashPoint::ErrorAfterCommit, true),
    ] {
        let (path, admin, engine) = fixture();
        insert_sql(&admin, 1);
        let prepared = engine
            .prepare_bound(&update(Intent::One, "done"), &Bindings::new())
            .unwrap();
        admin.set_crash_point(point).unwrap();
        assert_eq!(engine.execute_bound(&prepared), Err(Error::Unknown));
        drop(engine);
        drop(admin);
        let reopened = Admin::open(path, Limits::default()).unwrap();
        assert_eq!(
            rows(&reopened)[0].values[1],
            V::Text(if persisted { "done" } else { "ready" }.into())
        );
    }
}
