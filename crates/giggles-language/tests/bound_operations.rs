use giggles_core::{
    Admin, Limits,
    operations::ENGINE_CONTRACT_VERSION,
    relational::{DataType, with_sql_parser_forbidden},
};
use giggles_language::*;
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

static NEXT: AtomicU64 = AtomicU64::new(0);

fn fixture() -> (PathBuf, Admin, Engine) {
    let path = std::env::temp_dir().join(format!(
        "giggles-bound-native-{}-{}-{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let admin = Admin::create(&path, Limits::default()).unwrap();
    admin
        .sql_session()
        .execute(
            "create table items (id bigint primary key, state text not null, n integer)",
            &[],
            None,
        )
        .unwrap();
    let engine = Engine::new(admin.clone());
    (path, admin, engine)
}

fn rows(admin: &Admin) -> Vec<SqlRow> {
    admin
        .sql_session()
        .execute("select * from items order by id limit 1024", &[], None)
        .unwrap()
        .rows
}

fn insert(admin: &Admin, id: i64, state: &str) {
    admin
        .sql_session()
        .execute(
            "insert into items (id,state) values ($1,$2)",
            &[Value::Int(id), Value::Text(state.into())],
            None,
        )
        .unwrap();
}

#[test]
fn bound_description_has_native_version_catalog_identity_and_typed_shape() {
    let (_, admin, engine) = fixture();
    let (epoch, tables) = admin.catalog().unwrap();
    let table = &tables[0];
    with_sql_parser_forbidden(|| {
        let read = engine
            .prepare_bound_text("items select state,id,n", &Bindings::new())
            .unwrap();
        assert_eq!(read.language_version(), LANGUAGE_VERSION);
        assert_eq!(
            read.language_version(),
            capability_manifest().language_version
        );
        assert_eq!(read.profile(), profile().name);
        let description = read.description();
        assert_eq!(description.contract_version, ENGINE_CONTRACT_VERSION);
        assert_eq!(
            description.database_domain,
            admin.snapshot().unwrap().domain
        );
        assert_eq!(description.catalog_epoch, epoch);
        assert_eq!(&description.table, table);
        assert_eq!(
            description.columns,
            [
                table.columns[1].clone(),
                table.columns[0].clone(),
                table.columns[2].clone()
            ]
        );
        assert_eq!(description.columns[2].data_type, DataType::Integer);
        assert!(!description.columns[2].not_null);
        let result = engine.execute_bound(&read).unwrap();
        assert!(result.rows.is_empty());
        assert_eq!(result.columns, ["state", "id", "n"]);

        let claim = engine
            .prepare_bound_text(
                "claim items where state=\"ready\" order by id set state=\"taken\"",
                &Bindings::new(),
            )
            .unwrap();
        assert_eq!(claim.description().columns, table.columns);
        let empty = engine.execute_bound(&claim).unwrap();
        assert_eq!(empty.affected, 0);
        assert_eq!(empty.columns, ["id", "state", "n"]);
        for text in [
            "insert items set id=1,state=\"ready\"",
            "update items where id=1 set state=\"done\"",
            "delete items where id=1",
        ] {
            let operation = engine.prepare_bound_text(text, &Bindings::new()).unwrap();
            assert!(operation.description().columns.is_empty());
        }
    });
}

#[test]
fn preparation_freezes_real_bindings_and_caller_owned_trees() {
    let (_, admin, engine) = fixture();
    let mut bindings = Bindings::from([
        ("id".into(), Value::Int(1)),
        ("state".into(), Value::Text("ready".into())),
    ]);
    let mut tree = Statement::Insert {
        table: "items".into(),
        values: vec![
            ("id".into(), ValueExpr::Parameter("id".into())),
            ("state".into(), ValueExpr::Parameter("state".into())),
        ],
    };
    let bound = with_sql_parser_forbidden(|| engine.prepare_bound(&tree, &bindings)).unwrap();
    bindings.insert("id".into(), Value::Int(2));
    bindings.insert("state".into(), Value::Text("changed".into()));
    if let Statement::Insert { table, values } = &mut tree {
        *table = "missing".into();
        values.clear();
    }
    assert!(rows(&admin).is_empty());
    let cloned = bound.clone();
    assert_eq!(
        with_sql_parser_forbidden(|| engine.execute_bound(&bound))
            .unwrap()
            .changed,
        1
    );
    let inserted = rows(&admin);
    assert_eq!(
        inserted[0].values,
        [Value::Int(1), Value::Text("ready".into()), Value::Null]
    );
    // Re-execution is another request, not deduplication or a replay receipt.
    assert!(with_sql_parser_forbidden(|| engine.execute_bound(&cloned)).is_err());
    assert_eq!(rows(&admin), inserted);
}

#[test]
fn bound_execution_uses_fresh_rows_for_caps_guards_cardinality_and_claims() {
    let (_, admin, engine) = fixture();
    insert(&admin, 1, "ready");
    let (many, one, guarded, claim) = with_sql_parser_forbidden(|| {
        let bind = |text| engine.prepare_bound_text(text, &Bindings::new()).unwrap();
        (
            bind("update many items where state=\"ready\" set state=\"done\" at most 1"),
            bind("update items where state=\"ready\" set state=\"done\""),
            bind("update items where id=1 when state=\"ready\" set state=\"done\""),
            bind("claim items where state=\"ready\" order by id set state=\"taken\""),
        )
    });
    insert(&admin, 2, "ready");
    let before = rows(&admin);
    with_sql_parser_forbidden(|| {
        assert_eq!(engine.execute_bound(&many), Err(Error::TooManyMatches));
        assert_eq!(engine.execute_bound(&one), Err(Error::AmbiguousMatch));
    });
    assert_eq!(rows(&admin), before);
    admin
        .sql_session()
        .execute("update items set state='held' where id=1", &[], None)
        .unwrap();
    with_sql_parser_forbidden(|| {
        assert_eq!(engine.execute_bound(&guarded), Err(Error::ConditionFailed));
        let result = engine.execute_bound(&claim).unwrap();
        assert_eq!(result.rows[0].values[0], Value::Int(2));
        assert_eq!(result.rows[0].values[1], Value::Text("taken".into()));
        assert_eq!(engine.execute_bound(&claim).unwrap().affected, 0);
    });
    insert(&admin, 3, "ready");
    assert_eq!(
        with_sql_parser_forbidden(|| engine.execute_bound(&many))
            .unwrap()
            .changed,
        1
    );
    assert_eq!(rows(&admin)[2].values[1], Value::Text("done".into()));
}

#[test]
fn preparation_has_no_durable_effects_or_business_condition_checks() {
    let (path, admin, engine) = fixture();
    let before = admin.export(path.with_extension("before.export")).unwrap();
    let operations = with_sql_parser_forbidden(|| {
        [
            "items where true",
            "insert items set id=1,state=\"ready\"",
            "update items where id=1 when false set state=\"done\"",
            "delete items where id=1",
            "claim items where state=\"ready\" order by id set state=\"taken\"",
        ]
        .into_iter()
        .map(|text| engine.prepare_bound_text(text, &Bindings::new()).unwrap())
        .collect::<Vec<_>>()
    });
    assert_eq!(
        admin.export(path.with_extension("after.export")).unwrap(),
        before
    );
    assert!(rows(&admin).is_empty());
    assert_eq!(
        with_sql_parser_forbidden(|| engine.execute_bound(&operations[2])),
        Err(Error::NotFound)
    );
    assert_eq!(
        admin.export(path.with_extension("failed.export")).unwrap(),
        before
    );

    // Preparation cannot see schema created only inside another SQL branch.
    let mut uncommitted = admin.sql_session();
    uncommitted.execute("begin", &[], None).unwrap();
    uncommitted
        .execute("create table pending (id bigint primary key)", &[], None)
        .unwrap();
    assert_eq!(
        engine.prepare_bound_text("pending", &Bindings::new()).err(),
        // PG18: undefined table is 42P01; native errors carry SQL's SQLSTATE (R2 parity).
        Some(Error::Sql { sqlstate: "42P01".into() })
    );
    uncommitted.rollback();
}

#[test]
fn bound_operations_reject_catalog_changes_and_restored_database_domains() {
    let (path, admin, engine) = fixture();
    insert(&admin, 1, "ready");
    let bound = engine
        .prepare_bound_text(
            "update items where id=1 set state=\"done\"",
            &Bindings::new(),
        )
        .unwrap();
    let export = path.with_extension("restore.export");
    admin.export(&export).unwrap();
    let restored = Admin::restore(
        &export,
        path.with_extension("restored.db"),
        Limits::default(),
    )
    .unwrap();
    let restored_engine = Engine::new(restored.clone());
    assert_eq!(restored.catalog().unwrap(), admin.catalog().unwrap());
    assert_ne!(
        restored.snapshot().unwrap().domain,
        bound.description().database_domain
    );
    let stale = Err(Error::Sql { sqlstate: "0A000".into() });
    with_sql_parser_forbidden(|| assert_eq!(restored_engine.execute_bound(&bound), stale));
    assert_eq!(rows(&restored), rows(&admin));

    let before = rows(&admin);
    admin
        .sql_session()
        .execute("create table extra (id bigint primary key)", &[], None)
        .unwrap();
    with_sql_parser_forbidden(|| assert_eq!(engine.execute_bound(&bound), stale));
    assert_eq!(rows(&admin), before);
}

#[test]
fn preparation_checks_exact_arguments_and_native_tree_admission() {
    let (_, admin, engine) = fixture();
    with_sql_parser_forbidden(|| {
        let text = "update items where id=$id set n=$n";
        assert_eq!(
            engine.prepare_bound_text(text, &Bindings::new()).err(),
            Some(Error::Bind("parameter missing".into()))
        );
        let wrong = Bindings::from([
            ("id".into(), Value::Text("one".into())),
            ("n".into(), Value::Int(1)),
        ]);
        assert_eq!(
            engine.prepare_bound_text(text, &wrong).err(),
            Some(Error::Type)
        );
        let extra = Bindings::from([
            ("id".into(), Value::Int(1)),
            ("n".into(), Value::Int(1)),
            ("unused".into(), Value::Null),
        ]);
        assert_eq!(
            engine.prepare_bound_text(text, &extra).err(),
            Some(Error::Bind("parameters".into()))
        );
        let range = Bindings::from([
            ("id".into(), Value::Int(1)),
            ("n".into(), Value::Int(i64::MAX)),
        ]);
        assert_eq!(
            engine.prepare_bound_text(text, &range).err(),
            Some(Error::Type)
        );
        let malformed = Statement::Read {
            table: "items".into(),
            filter: None,
            order: vec![],
            limit: None,
            projection: vec!["claim".into()],
        };
        assert_eq!(
            engine.prepare_bound(&malformed, &Bindings::new()).err(),
            Some(Error::Bind("identifier".into()))
        );
        let oversized = Statement::Read {
            table: "x".repeat(MAX_INPUT),
            filter: None,
            order: vec![],
            limit: None,
            projection: vec![],
        };
        assert_eq!(
            engine.prepare_bound(&oversized, &Bindings::new()).err(),
            Some(Error::Limit)
        );
    });
    assert!(rows(&admin).is_empty());
}
