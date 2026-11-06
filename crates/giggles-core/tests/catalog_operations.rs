mod common;

use giggles_core::{
    Admin, Cancellation, CoreError, CrashPoint, DecisionPoint, Limits, Principal, TaskId,
    TaskResources, Time,
    operations::{
        Bindings, CatalogRequest, ColumnSpec, Engine, Error, Request, Transaction, TransactionStep,
        ValueExpr,
    },
    relational::{DataType, MAX_COLUMNS, MAX_TABLES, SqlError, Value as V},
};
use std::{collections::BTreeSet, path::PathBuf, sync::Arc};

fn database() -> (PathBuf, Admin, Engine) {
    let path = common::fresh("catalog-operations");
    let admin = Admin::create(&path, Limits::default()).unwrap();
    (path, admin.clone(), Engine::new(admin))
}
fn column(name: &str, data_type: DataType, not_null: bool) -> ColumnSpec {
    ColumnSpec {
        name: name.into(),
        data_type,
        not_null,
    }
}
fn create(name: &str) -> CatalogRequest {
    CatalogRequest::CreateTable {
        table: name.into(),
        columns: vec![column("id", DataType::BigInt, true)],
        primary_key: vec!["id".into()],
    }
}
fn read(table: &str) -> Request {
    Request::Read {
        table: table.into(),
        filter: None,
        order: vec![],
        limit: None,
        projection: vec![],
    }
}
fn insert(table: &str, values: Vec<(&str, V)>) -> Request {
    Request::Insert {
        table: table.into(),
        values: values
            .into_iter()
            .map(|(name, value)| (name.into(), ValueExpr::Literal(value)))
            .collect(),
    }
}
fn digest(admin: &Admin) -> [u8; 32] {
    admin.export(common::fresh("catalog-export")).unwrap()
}
fn reject_definition(admin: &Admin, engine: &Engine, request: &CatalogRequest, expected: Error) {
    let before = digest(admin);
    let catalog = admin.catalog().unwrap();
    assert_eq!(engine.prepare_catalog_bound(request).unwrap_err(), expected);
    assert_eq!(engine.execute_catalog(request), Err(expected));
    assert_eq!(digest(admin), before);
    assert_eq!(admin.catalog().unwrap(), catalog);
}

#[test]
fn catalog_creation_shares_sql_metadata_and_type_constraints() {
    let (_, admin, engine) = database();
    let request = CatalogRequest::CreateTable {
        table: "items".into(),
        columns: vec![
            column("id", DataType::BigInt, true),
            column("flag", DataType::Boolean, true),
            column("n", DataType::Integer, true),
            column("small", DataType::SmallInt, false),
            column("text_value", DataType::Text, false),
            column("raw", DataType::Bytes, false),
        ],
        primary_key: vec!["id".into()],
    };
    let outcome = engine.execute_catalog(&request).unwrap();
    assert_eq!(outcome.catalog_epoch, 1);
    assert_eq!(admin.catalog().unwrap(), (1, vec![outcome.table.clone()]));
    assert_eq!(outcome.table.primary_key, [0]);
    assert_eq!(
        outcome
            .table
            .columns
            .iter()
            .map(|c| c.not_null)
            .collect::<Vec<_>>(),
        [true, true, true, false, false, false]
    );
    assert_eq!(
        outcome
            .table
            .columns
            .iter()
            .map(|c| c.data_type.clone())
            .collect::<Vec<_>>(),
        [
            DataType::BigInt,
            DataType::Boolean,
            DataType::Integer,
            DataType::SmallInt,
            DataType::Text,
            DataType::Bytes
        ]
    );
    let mut sql = admin.sql_session();
    sql.execute(
        "CREATE TABLE sql_defaults (id BIGINT PRIMARY KEY,optional BIGINT,required TEXT NOT NULL)",
        &[],
        None,
    )
    .unwrap();
    let (_, catalog) = admin.catalog().unwrap();
    let sql_table = catalog.iter().find(|t| t.name == "sql_defaults").unwrap();
    assert_eq!(
        sql_table
            .columns
            .iter()
            .map(|c| c.not_null)
            .collect::<Vec<_>>(),
        [true, false, true]
    );
    let metadata = engine
        .prepare_bound(&read("sql_defaults"), &Bindings::new())
        .unwrap();
    assert_eq!(metadata.description().columns, sql_table.columns);
    sql.execute(
        "INSERT INTO items(id,flag,n,small) VALUES(1,true,2147483647,32767)",
        &[],
        None,
    )
    .unwrap();
    engine
        .execute(
            &insert(
                "items",
                vec![
                    ("id", V::Int(2)),
                    ("flag", V::Bool(false)),
                    ("n", V::Int(i32::MIN as i64)),
                    ("small", V::Int(i16::MIN as i64)),
                    ("text_value", V::Text("x\0y".into())),
                    ("raw", V::Bytes(vec![0, 255, 1])),
                ],
            ),
            &Bindings::new(),
        )
        .unwrap();
    let native = engine.execute(&read("items"), &Bindings::new()).unwrap();
    let stored = sql
        .execute("SELECT * FROM items LIMIT 1024", &[], None)
        .unwrap();
    assert_eq!(native.rows, stored.rows);
    assert_eq!(
        stored.rows[0].values,
        [
            V::Int(1),
            V::Bool(true),
            V::Int(i32::MAX as i64),
            V::Int(i16::MAX as i64),
            V::Null,
            V::Null
        ]
    );
    assert_eq!(
        stored.rows[1].values[4..],
        [V::Text("x\0y".into()), V::Bytes(vec![0, 255, 1])]
    );
    for request in [
        insert("items", vec![("id", V::Int(3)), ("n", V::Int(1))]),
        insert(
            "items",
            vec![("id", V::Int(3)), ("flag", V::Null), ("n", V::Int(1))],
        ),
        insert(
            "items",
            vec![
                ("id", V::Int(3)),
                ("flag", V::Bool(true)),
                ("n", V::Int(i32::MAX as i64 + 1)),
            ],
        ),
        insert(
            "items",
            vec![
                ("id", V::Int(3)),
                ("flag", V::Bool(true)),
                ("n", V::Int(1)),
                ("small", V::Int(i16::MAX as i64 + 1)),
            ],
        ),
    ] {
        let before = digest(&admin);
        assert!(engine.execute(&request, &Bindings::new()).is_err());
        assert_eq!(digest(&admin), before);
    }
    for source in [
        "INSERT INTO items(id,n) VALUES(3,1)",
        "INSERT INTO items VALUES(3,1,1,NULL,NULL,NULL)",
        "INSERT INTO items(id,flag,n) VALUES(3,true,2147483648)",
        "INSERT INTO items(id,flag,n,small) VALUES(3,true,1,-32769)",
    ] {
        let before = digest(&admin);
        assert!(sql.execute(source, &[], None).is_err(), "{source}");
        assert_eq!(digest(&admin), before);
    }
    engine
        .execute(
            &insert(
                "sql_defaults",
                vec![("id", V::Int(1)), ("required", V::Text("yes".into()))],
            ),
            &Bindings::new(),
        )
        .unwrap();
    assert_eq!(
        sql.execute("SELECT optional FROM sql_defaults WHERE id=1", &[], None)
            .unwrap()
            .rows[0]
            .values,
        [V::Null]
    );

    let (_, admin, engine) = database();
    let request = CatalogRequest::CreateTable {
        table: "composite".into(),
        columns: vec![
            column("id", DataType::BigInt, true),
            column("tenant", DataType::Text, true),
            column("value", DataType::Text, false),
        ],
        primary_key: vec!["tenant".into(), "id".into()],
    };
    let outcome = engine.execute_catalog(&request).unwrap();
    assert_eq!(outcome.table.primary_key, [1, 0]);
    for (id, tenant) in [(1, "b"), (2, "a"), (1, "a")] {
        admin
            .sql_session()
            .execute(
                "INSERT INTO composite(id,tenant) VALUES($1,$2)",
                &[V::Int(id), V::Text(tenant.into())],
                None,
            )
            .unwrap();
    }
    let result = engine
        .execute(&read("composite"), &Bindings::new())
        .unwrap();
    assert_eq!(
        result
            .rows
            .iter()
            .map(|r| r.values.clone())
            .collect::<Vec<_>>(),
        [
            vec![V::Int(1), V::Text("a".into()), V::Null],
            vec![V::Int(2), V::Text("a".into()), V::Null],
            vec![V::Int(1), V::Text("b".into()), V::Null]
        ]
    );
    assert_eq!(
        admin
            .sql_session()
            .execute("INSERT INTO composite(id,tenant) VALUES(1,NULL)", &[], None),
        Err(CoreError::Sql(SqlError::Null))
    );
}

#[test]
fn catalog_definitions_limits_and_allocators_are_atomic() {
    let (_, admin, engine) = database();
    let base = engine.execute_catalog(&create("base")).unwrap();
    assert_eq!(base.table.id, 1);
    assert_eq!(base.table.columns[0].id, 2);
    for name in ["", "A", "two.parts", "1name", "é", "has space"] {
        reject_definition(
            &admin,
            &engine,
            &create(name),
            Error::Bind("identifier".into()),
        );
    }
    reject_definition(
        &admin,
        &engine,
        &create(&"a".repeat(64)),
        Error::Bind("identifier".into()),
    );
    reject_definition(
        &admin,
        &engine,
        &create(&"a".repeat(65_537)),
        Error::Bind("identifier".into()),
    );
    for name in [
        "deliveries",
        "giggles",
        "system",
        "information_schema",
        "pg_catalog",
        "repair",
        "internal",
        "tasks",
        "operations",
        "approvals",
        "receipts",
        "giggles_x",
        "system_x",
        "pg_x",
        "repair_x",
        "internal_x",
    ] {
        reject_definition(
            &admin,
            &engine,
            &create(name),
            Error::Core("Sql(Reserved)".into()),
        );
        let request = CatalogRequest::CreateTable {
            table: "candidate".into(),
            columns: vec![
                column("id", DataType::BigInt, true),
                column(name, DataType::Text, false),
            ],
            primary_key: vec!["id".into()],
        };
        reject_definition(
            &admin,
            &engine,
            &request,
            Error::Core("Sql(Reserved)".into()),
        );
    }
    for (columns, primary_key, expected) in [
        (vec![], vec!["id".into()], Error::Bind("columns".into())),
        (
            vec![column("id", DataType::BigInt, true)],
            vec![],
            Error::Bind("primary key".into()),
        ),
        (
            vec![column("id", DataType::BigInt, true); 2],
            vec!["id".into()],
            Error::Core("Sql(Duplicate)".into()),
        ),
        (
            vec![column("id", DataType::BigInt, true)],
            vec!["id".into(), "id".into()],
            Error::Core("Sql(Duplicate)".into()),
        ),
        (
            vec![column("id", DataType::BigInt, true)],
            vec!["absent".into()],
            Error::Core("Sql(UnknownColumn)".into()),
        ),
        (
            vec![column("id", DataType::BigInt, false)],
            vec!["id".into()],
            Error::Type,
        ),
        (
            (0..=MAX_COLUMNS)
                .map(|i| column(&format!("c{i}"), DataType::BigInt, true))
                .collect(),
            vec!["c0".into()],
            Error::Limit,
        ),
        (
            vec![column("id", DataType::BigInt, true)],
            vec!["id".into(); MAX_COLUMNS + 1],
            Error::Limit,
        ),
    ] {
        reject_definition(
            &admin,
            &engine,
            &CatalogRequest::CreateTable {
                table: "candidate".into(),
                columns,
                primary_key,
            },
            expected,
        );
    }
    reject_definition(
        &admin,
        &engine,
        &create("base"),
        Error::Core("Sql(Duplicate)".into()),
    );
    let valid = create(&"a".repeat(63));
    let result = engine.execute_catalog(&valid).unwrap();
    assert_eq!(result.table.id, 3);
    assert_eq!(result.table.columns[0].id, 4);
    assert_eq!(result.catalog_epoch, 2);
    let long_column = "b".repeat(63);
    let names = (0..MAX_COLUMNS)
        .map(|i| {
            if i == 0 {
                long_column.clone()
            } else {
                format!("c{i}")
            }
        })
        .collect::<Vec<_>>();
    let wide = CatalogRequest::CreateTable {
        table: "wide".into(),
        columns: names
            .iter()
            .map(|name| column(name, DataType::BigInt, true))
            .collect(),
        primary_key: names,
    };
    let created = engine.execute_catalog(&wide).unwrap();
    assert_eq!(created.table.columns.len(), 64);
    assert_eq!(
        created.table.primary_key,
        (0..MAX_COLUMNS).collect::<Vec<_>>()
    );
    assert_eq!(created.table.id, 5);
    let ids = created
        .table
        .columns
        .iter()
        .map(|c| c.id)
        .collect::<BTreeSet<_>>();
    assert_eq!(ids, (6..70).collect::<BTreeSet<_>>());
    for i in 3..MAX_TABLES {
        engine
            .execute_catalog(&create(&format!("table{i}")))
            .unwrap();
    }
    assert_eq!(admin.catalog().unwrap().1.len(), MAX_TABLES);
    reject_definition(
        &admin,
        &engine,
        &create("one_too_many"),
        Error::Core("Sql(Limit)".into()),
    );
    admin
        .sql_session()
        .execute("INSERT INTO base VALUES(1)", &[], None)
        .unwrap();
    assert_eq!(
        admin
            .sql_session()
            .execute("SELECT * FROM base LIMIT 1", &[], None)
            .unwrap()
            .rows[0]
            .version,
        1
    );
}

#[test]
fn catalog_preparation_freezes_definition_and_rechecks_empty_state() {
    let (_, admin, engine) = database();
    admin
        .sql_session()
        .execute("CREATE TABLE source (id BIGINT PRIMARY KEY)", &[], None)
        .unwrap();
    admin
        .sql_session()
        .execute("INSERT INTO source VALUES(1)", &[], None)
        .unwrap();
    let mut request = create("created");
    let before = digest(&admin);
    let bound = engine.prepare_catalog_bound(&request).unwrap();
    assert_eq!(bound.description().request, request);
    assert_eq!(bound.description().catalog_epoch, 1);
    assert_eq!(digest(&admin), before);
    assert_eq!(
        engine.execute_catalog_bound(&bound),
        Err(Error::PlanRequired)
    );
    assert_eq!(engine.execute_catalog(&request), Err(Error::PlanRequired));
    assert_eq!(digest(&admin), before);
    let CatalogRequest::CreateTable { table, columns, .. } = &mut request;
    *table = "mutated".into();
    columns[0].data_type = DataType::Text;
    admin
        .sql_session()
        .execute("DELETE FROM source WHERE id=1", &[], None)
        .unwrap();
    assert_eq!(admin.catalog().unwrap().0, 1);
    let created = engine.execute_catalog_bound(&bound).unwrap();
    assert_eq!(created.table.name, "created");
    assert_eq!(created.table.columns[0].data_type, DataType::BigInt);
    assert_eq!(created.catalog_epoch, 2);
    assert_eq!(
        engine.execute_catalog_bound(&bound),
        Err(Error::Core("Sql(StaleSchema)".into()))
    );
    let bound = engine.prepare_catalog_bound(&create("later")).unwrap();
    admin
        .sql_session()
        .execute("INSERT INTO source VALUES(2)", &[], None)
        .unwrap();
    let before = digest(&admin);
    assert_eq!(
        engine.execute_catalog_bound(&bound),
        Err(Error::PlanRequired)
    );
    assert_eq!(digest(&admin), before);
    // SQL retains its existing trusted CREATE behavior on populated databases.
    admin
        .sql_session()
        .execute(
            "CREATE TABLE sql_populated (id BIGINT PRIMARY KEY)",
            &[],
            None,
        )
        .unwrap();
    assert_eq!(
        engine.execute_catalog_bound(&bound),
        Err(Error::Core("Sql(StaleSchema)".into()))
    );
    let fresh = engine.prepare_catalog_bound(&create("later")).unwrap();
    assert_eq!(
        engine.execute_catalog_bound(&fresh),
        Err(Error::PlanRequired)
    );
    admin
        .sql_session()
        .execute("DELETE FROM source WHERE id=2", &[], None)
        .unwrap();
    engine.execute_catalog_bound(&fresh).unwrap();
    admin
        .sql_session()
        .execute("INSERT INTO source VALUES(3)", &[], None)
        .unwrap();
    assert_eq!(
        admin
            .sql_session()
            .execute("SELECT * FROM source LIMIT 1", &[], None)
            .unwrap()
            .rows[0]
            .version,
        3
    );
}

#[test]
fn catalog_safe_create_observes_business_state_and_metadata() {
    for kind in 0..6 {
        let (_, admin, engine) = database();
        if kind == 0 {
            admin.put(common::key(1), common::row()).unwrap();
        } else {
            let executor = admin.grant(common::grant()).unwrap();
            match kind {
                1 => {}
                2 => admin.revoke(TaskId(1)).unwrap(),
                3 => admin
                    .provision_task_resources(TaskId(1), TaskResources::default())
                    .unwrap(),
                4 => {
                    let _decision = executor
                        .execute(common::predicate(1), Time::Certified(10), None)
                        .unwrap();
                    assert!(!admin.snapshot().unwrap().operations.is_empty());
                }
                5 => {
                    let preview = executor
                        .preview(&common::predicate(1), Time::Certified(10))
                        .unwrap();
                    let approval = admin
                        .approve(Principal(3), preview, Time::Certified(10))
                        .unwrap();
                    admin.revoke_approval(approval).unwrap();
                    assert!(!admin.snapshot().unwrap().approvals.is_empty());
                }
                _ => unreachable!(),
            }
        }
        let before = digest(&admin);
        let bound = engine.prepare_catalog_bound(&create("safe")).unwrap();
        assert_eq!(
            engine.execute_catalog_bound(&bound),
            Err(Error::PlanRequired)
        );
        assert_eq!(
            engine.execute_catalog(&create("safe")),
            Err(Error::PlanRequired)
        );
        assert_eq!(digest(&admin), before);
    }
    // Empty tables and nonzero clocks, counters, generations and restore origin
    // are metadata, not populated application/business state.
    let (_, admin, engine) = database();
    admin.certify_server_time(40, 42).unwrap();
    admin
        .sql_session()
        .execute("CREATE TABLE old (id BIGINT PRIMARY KEY)", &[], None)
        .unwrap();
    admin
        .sql_session()
        .execute("INSERT INTO old VALUES(1)", &[], None)
        .unwrap();
    admin
        .sql_session()
        .execute("DELETE FROM old WHERE id=1", &[], None)
        .unwrap();
    engine.execute_catalog(&create("safe")).unwrap();
    let exported = common::fresh("catalog-empty-origin");
    admin.export(&exported).unwrap();
    let restored = Admin::restore(
        exported,
        common::fresh("catalog-empty-restored"),
        Limits::default(),
    )
    .unwrap();
    assert!(restored.authority_metadata().unwrap().3.is_some());
    Engine::new(restored)
        .execute_catalog(&create("after_restore"))
        .unwrap();
}

#[test]
fn catalog_epoch_domain_fences_survive_reopen_and_restore() {
    let (path, admin, engine) = database();
    engine.execute_catalog(&create("base")).unwrap();
    let mut sql = admin.sql_session();
    let sql_read = sql.prepare("SELECT * FROM base LIMIT 1").unwrap();
    let sql_create = sql
        .prepare("CREATE TABLE sql_later (id BIGINT PRIMARY KEY)")
        .unwrap();
    let native_read = engine
        .prepare_bound(&read("base"), &Bindings::new())
        .unwrap();
    let transaction = Transaction {
        read_only: true,
        steps: vec![TransactionStep::Execute(read("base"))],
        emit: None,
    };
    let native_block = engine
        .prepare_transaction_bound(&transaction, &Bindings::new())
        .unwrap();
    let sibling = engine.prepare_catalog_bound(&create("sibling")).unwrap();
    let created = engine.execute_catalog(&create("created")).unwrap();
    assert_eq!(created.catalog_epoch, 2);
    assert_eq!(
        sql.execute_prepared(&sql_read, &[], None),
        Err(CoreError::Sql(SqlError::StaleSchema))
    );
    assert_eq!(
        sql.execute_prepared(&sql_create, &[], None),
        Err(CoreError::Sql(SqlError::StaleSchema))
    );
    assert_eq!(
        engine.execute_bound(&native_read),
        Err(Error::Core("Sql(StaleSchema)".into()))
    );
    assert_eq!(
        engine.execute_transaction_bound(&native_block),
        Err(Error::Core("Sql(StaleSchema)".into()))
    );
    assert_eq!(
        engine.execute_catalog_bound(&sibling),
        Err(Error::Core("Sql(StaleSchema)".into()))
    );
    let sibling = engine.prepare_catalog_bound(&create("sibling")).unwrap();
    sql.execute("CREATE TABLE sql_later (id BIGINT PRIMARY KEY)", &[], None)
        .unwrap();
    assert_eq!(
        engine.execute_catalog_bound(&sibling),
        Err(Error::Core("Sql(StaleSchema)".into()))
    );
    let bound = engine
        .prepare_catalog_bound(&create("restored_later"))
        .unwrap();
    let expected = admin.catalog().unwrap();
    let exported = common::fresh("catalog-domain");
    admin.export(&exported).unwrap();
    let restored = Admin::restore(
        exported,
        common::fresh("catalog-domain-restored"),
        Limits::default(),
    )
    .unwrap();
    assert_eq!(restored.catalog().unwrap(), expected);
    let before = digest(&restored);
    assert_eq!(
        Engine::new(restored.clone()).execute_catalog_bound(&bound),
        Err(Error::Core("Sql(StaleSchema)".into()))
    );
    assert_eq!(digest(&restored), before);
    drop(sql);
    drop(engine);
    drop(admin);
    let reopened = Admin::open(path, Limits::default()).unwrap();
    assert_eq!(reopened.catalog().unwrap(), expected);
    let created = Engine::new(reopened.clone())
        .execute_catalog_bound(&bound)
        .unwrap();
    assert_eq!(created.catalog_epoch, expected.0 + 1);
    assert_eq!(created.table.name, "restored_later");
}

#[test]
fn catalog_cancellation_and_unknown_publish_no_speculative_descriptor() {
    for (point, wins) in [
        (DecisionPoint::BeforeDecision, true),
        (DecisionPoint::Decided, false),
    ] {
        let (_, admin, engine) = database();
        let before = digest(&admin);
        admin.set_decision_hook(Some(Arc::new(move |current| {
            if current == point {
                assert_eq!(Cancellation::current().unwrap().cancel(), wins);
            }
        })));
        let result = Cancellation::default().scope(|| engine.execute_catalog(&create("new_table")));
        admin.set_decision_hook(None);
        if wins {
            assert_eq!(result, Err(Error::Core(CoreError::Cancelled.to_string())));
            assert_eq!(digest(&admin), before);
            let created = engine.execute_catalog(&create("after_cancel")).unwrap();
            assert_eq!(created.table.id, 1);
            assert_eq!(created.table.columns[0].id, 2);
        } else {
            let created = result.unwrap();
            assert_eq!(
                admin.catalog().unwrap(),
                (created.catalog_epoch, vec![created.table])
            );
        }
    }
    for (point, persisted) in [
        (CrashPoint::ErrorBeforeCommit, false),
        (CrashPoint::ErrorAfterCommit, true),
    ] {
        let (path, admin, engine) = database();
        let bound = engine.prepare_catalog_bound(&create("new_table")).unwrap();
        admin.set_crash_point(point).unwrap();
        assert_eq!(engine.execute_catalog_bound(&bound), Err(Error::Unknown));
        drop(engine);
        drop(admin);
        let reopened = Admin::open(path, Limits::default()).unwrap();
        let catalog = reopened.catalog().unwrap();
        assert_eq!(catalog.1.len(), usize::from(persisted));
        assert_eq!(catalog.0, u64::from(persisted));
        let after = Engine::new(reopened)
            .execute_catalog(&create("after_unknown"))
            .unwrap();
        assert_eq!(after.table.id, if persisted { 3 } else { 1 });
        assert_eq!(after.table.columns[0].id, if persisted { 4 } else { 2 });
    }
}
