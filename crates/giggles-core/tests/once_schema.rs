mod common;

use giggles_core::{
    Admin, Limits,
    operations::{
        Arithmetic, Bindings, Compare, EmitDescription, EmitExpr, EmittedValue, Engine, Error,
        Expr, Intent, LocalRef, Projection, Request, Transaction, TransactionStep, ValueExpr,
        once::{
            ONCE_CONTRACT_VERSION, OnceBody, OnceClockUnavailable, OnceDescription, OnceIdentity,
            OnceOutcome, OnceReceipt, OnceRequest, OnceSchema, OnceStatus, OnceTime,
        },
    },
    relational::{ColumnDef, DataType, SqlRow, TableDef, Value as V},
};
use std::{
    path::PathBuf,
    time::{Duration, Instant},
};

fn with_sql_parser_forbidden<T>(work: impl FnOnce() -> T) -> T {
    #[cfg(feature = "test-sql-parser-trap")]
    {
        giggles_core::relational::with_sql_parser_forbidden(work)
    }
    #[cfg(not(feature = "test-sql-parser-trap"))]
    {
        work()
    }
}

fn value(value: V) -> Expr {
    Expr::Value(ValueExpr::Literal(value))
}
fn int(number: i64) -> Expr {
    value(V::Int(number))
}
fn field(name: &str) -> Expr {
    Expr::Field(name.into())
}
fn eq(left: Expr, right: Expr) -> Expr {
    Expr::Compare(Box::new(left), Compare::Eq, Box::new(right))
}
fn local(name: &str, column: &str) -> Expr {
    Expr::Local(LocalRef::Field {
        local: name.into(),
        field: column.into(),
    })
}
fn projection(expression: Expr, alias: Option<&str>) -> Projection {
    Projection {
        expression,
        alias: alias.map(str::to_owned),
    }
}
fn update(amount: Expr, guard: Option<Expr>) -> Request {
    Request::UpdateExpressions {
        table: "items".into(),
        filter: eq(field("id"), int(1)),
        guard,
        values: vec![("n".into(), amount)],
        intent: Intent::One,
    }
}
fn returning(request: Request, projection: Vec<Projection>) -> Request {
    Request::Returning {
        request: Box::new(request),
        projection,
    }
}
fn fixture() -> (PathBuf, Admin, Engine, u64, TableDef) {
    let path = common::fresh("once-schema");
    let admin = Admin::create(&path, Limits::default()).unwrap();
    let mut sql = admin.sql_session();
    sql.execute("CREATE TABLE items(id BIGINT PRIMARY KEY,n BIGINT NOT NULL,small SMALLINT,medium INTEGER,wide BIGINT,txt TEXT,raw BYTEA,flag BOOLEAN)", &[], None).unwrap();
    sql.execute("INSERT INTO items(id,n) VALUES(1,10)", &[], None)
        .unwrap();
    drop(sql);
    let (epoch, tables) = admin.catalog().unwrap();
    let table = tables
        .into_iter()
        .find(|table| table.name == "items")
        .unwrap();
    (path, admin.clone(), Engine::new(admin), epoch, table)
}
fn request(admin: &Admin, key: &[u8], body: OnceBody) -> OnceRequest {
    OnceRequest {
        contract_version: ONCE_CONTRACT_VERSION,
        identity: OnceIdentity {
            database_domain: admin.snapshot().unwrap().domain,
            tenant: 42,
            namespace: "schema.replay".into(),
            key: key.to_vec(),
        },
        retention_seconds: 60,
        body,
    }
}
fn clock() -> Result<OnceTime, OnceClockUnavailable> {
    Ok(OnceTime {
        lower_unix_seconds: 1000,
        upper_unix_seconds: 1001,
        valid_until: Instant::now() + Duration::from_secs(10),
    })
}
fn no_clock() -> Result<OnceTime, OnceClockUnavailable> {
    panic!("replay must not certify another decision")
}
fn column(table: &TableDef, name: &str) -> ColumnDef {
    table
        .columns
        .iter()
        .find(|column| column.name == name)
        .unwrap()
        .clone()
}
fn rows(admin: &Admin) -> Vec<SqlRow> {
    admin
        .sql_session()
        .execute("SELECT * FROM items ORDER BY id LIMIT 1024", &[], None)
        .unwrap()
        .rows
}
fn digest(admin: &Admin) -> [u8; 32] {
    admin.export(common::fresh("once-schema-export")).unwrap()
}
fn change_catalog(admin: &Admin) {
    admin
        .sql_session()
        .execute("CREATE TABLE unrelated(id BIGINT PRIMARY KEY)", &[], None)
        .unwrap();
}
fn assert_retained(engine: &Engine, original: &OnceRequest, expected: &OnceReceipt) {
    let replay =
        with_sql_parser_forbidden(|| engine.execute_once(original, &Bindings::new(), &no_clock))
            .unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.description, expected.description);
    assert_eq!(replay.outcome, expected.outcome);
    assert_eq!(replay.accepted_fingerprint, expected.accepted_fingerprint);
    assert_eq!(replay.retry_until_seconds, expected.retry_until_seconds);
    assert_eq!(
        with_sql_parser_forbidden(|| engine.outcome(&original.identity)),
        Ok(OnceStatus::Retained(replay))
    );
}

#[test]
fn once_return_schema_preserves_null_types_aliases_and_computed_columns_after_reopen() {
    let (path, admin, engine, epoch, table) = fixture();
    let nullable = [
        ("small", DataType::SmallInt),
        ("medium", DataType::Integer),
        ("wide", DataType::BigInt),
        ("txt", DataType::Text),
        ("raw", DataType::Bytes),
        ("flag", DataType::Boolean),
    ];
    let mut projection = nullable
        .iter()
        .map(|(name, _)| projection(field(name), None))
        .collect::<Vec<_>>();
    projection.push(projection_alias(field("small"), "small_copy"));
    projection.push(projection_alias(
        Expr::Arithmetic(Box::new(field("small")), Arithmetic::Add, Box::new(int(1))),
        "computed",
    ));
    projection.push(projection_alias(field("n"), "required_value"));
    let body = |amount| returning(update(amount, None), projection.clone());
    let original = request(
        &admin,
        b"typed-null-return",
        OnceBody::Operation(body(Expr::Value(ValueExpr::Parameter("amount".into())))),
    );
    let receipt = with_sql_parser_forbidden(|| {
        engine.execute_once(
            &original,
            &Bindings::from([("amount".into(), V::Int(11))]),
            &clock,
        )
    })
    .unwrap();
    let mut expected = nullable
        .iter()
        .map(|(name, ty)| {
            let actual = column(&table, name);
            assert_eq!(&actual.data_type, ty);
            assert!(!actual.not_null);
            actual
        })
        .collect::<Vec<_>>();
    let mut alias = column(&table, "small");
    alias.name = "small_copy".into();
    expected.push(alias);
    expected.push(ColumnDef {
        id: 0,
        name: "computed".into(),
        data_type: DataType::BigInt,
        not_null: false,
    });
    let mut required = column(&table, "n");
    required.name = "required_value".into();
    expected.push(required);
    assert_eq!(
        receipt.description,
        OnceDescription {
            catalog_epoch: epoch,
            output: OnceSchema::Operation(expected.clone())
        }
    );
    let OnceOutcome::Operation(out) = receipt.outcome.as_ref().unwrap() else {
        panic!("operation expected");
    };
    assert_eq!(
        out.columns,
        expected
            .iter()
            .map(|column| column.name.clone())
            .collect::<Vec<_>>()
    );
    assert_eq!(out.rows.len(), 1);
    assert_eq!(
        out.rows[0].values,
        [vec![V::Null; 8], vec![V::Int(11)]].concat()
    );
    assert!(out.rows[0].version > 0);

    // Literal/parameter resolution remains the same submitted identity. The
    // retained schema refinement must not introduce another acceptance decision.
    let mut literal = original.clone();
    literal.body = OnceBody::Operation(body(int(11)));
    assert_retained(&engine, &literal, &receipt);
    admin
        .sql_session()
        .execute(
            "UPDATE items SET n=99,small=5,medium=7,wide=9,txt='changed',flag=TRUE WHERE id=1",
            &[],
            None,
        )
        .unwrap();
    change_catalog(&admin);
    assert!(admin.catalog().unwrap().0 > epoch);
    let before = digest(&admin);
    drop(engine);
    drop(admin);
    let reopened = Admin::open(path, Limits::default()).unwrap();
    let engine = Engine::new(reopened.clone());
    assert_retained(&engine, &literal, &receipt);
    assert_eq!(
        digest(&reopened),
        before,
        "schema replay and lookup are read-only"
    );
    assert_eq!(rows(&reopened)[0].values[1], V::Int(99));
}

fn projection_alias(expression: Expr, name: &str) -> Projection {
    projection(expression, Some(name))
}

#[test]
fn once_transaction_schema_distinguishes_typed_null_scalars_and_matches_row_descriptions() {
    let (_, admin, engine, epoch, table) = fixture();
    let names = ["small", "medium", "wide", "txt", "raw", "flag", "n"];
    let mut emit = names
        .iter()
        .map(|name| EmitExpr::Scalar(local("changed", name)))
        .collect::<Vec<_>>();
    emit.push(EmitExpr::Scalar(value(V::Null)));
    emit.push(EmitExpr::Scalar(Expr::IsNull(
        Box::new(local("changed", "small")),
        false,
    )));
    emit.push(EmitExpr::Scalar(Expr::Arithmetic(
        Box::new(local("changed", "small")),
        Arithmetic::Add,
        Box::new(int(0)),
    )));
    emit.push(EmitExpr::Row("changed".into()));
    emit.push(EmitExpr::Rows("page".into()));
    let original = request(
        &admin,
        b"typed-null-emit",
        OnceBody::Transaction(Transaction {
            read_only: false,
            steps: vec![
                TransactionStep::Let {
                    name: "changed".into(),
                    request: returning(update(int(11), None), vec![]),
                },
                TransactionStep::Let {
                    name: "page".into(),
                    request: Request::Read {
                        table: "items".into(),
                        filter: None,
                        order: vec![],
                        limit: Some(ValueExpr::Literal(V::Int(1))),
                        projection: vec![],
                    },
                },
            ],
            emit: Some(emit),
        }),
    );
    let receipt =
        with_sql_parser_forbidden(|| engine.execute_once(&original, &Bindings::new(), &clock))
            .unwrap();
    let mut expected = names
        .iter()
        .map(|name| {
            let column = column(&table, name);
            EmitDescription::Scalar {
                data_type: Some(column.data_type),
                nullable: !column.not_null,
            }
        })
        .collect::<Vec<_>>();
    assert_eq!(
        expected[0],
        EmitDescription::Scalar {
            data_type: Some(DataType::SmallInt),
            nullable: true
        }
    );
    assert_eq!(
        expected[1],
        EmitDescription::Scalar {
            data_type: Some(DataType::Integer),
            nullable: true
        }
    );
    assert_eq!(
        expected[6],
        EmitDescription::Scalar {
            data_type: Some(DataType::BigInt),
            nullable: false
        }
    );
    expected.extend([
        EmitDescription::Scalar {
            data_type: None,
            nullable: true,
        },
        EmitDescription::Scalar {
            data_type: Some(DataType::Boolean),
            nullable: false,
        },
        EmitDescription::Scalar {
            data_type: Some(DataType::BigInt),
            nullable: true,
        },
        EmitDescription::Row {
            columns: table.columns.clone(),
        },
        EmitDescription::Rows {
            columns: table.columns.clone(),
        },
    ]);
    assert_eq!(
        receipt.description,
        OnceDescription {
            catalog_epoch: epoch,
            output: OnceSchema::Transaction(expected.clone())
        }
    );
    let OnceOutcome::Transaction(out) = receipt.outcome.as_ref().unwrap() else {
        panic!("transaction expected");
    };
    let scalar_values = out.emitted[..10]
        .iter()
        .map(|item| match item {
            EmittedValue::Scalar(value) => value.clone(),
            _ => panic!("scalar expected"),
        })
        .collect::<Vec<_>>();
    assert_eq!(
        scalar_values,
        [
            vec![V::Null; 6],
            vec![V::Int(11), V::Null, V::Bool(true), V::Null]
        ]
        .concat()
    );
    let (EmittedValue::Row { columns, row }, EmitDescription::Row { columns: described }) =
        (&out.emitted[10], &expected[10])
    else {
        panic!("row expected");
    };
    assert_eq!(columns, described);
    assert_eq!(row.values, rows(&admin)[0].values);
    let (
        EmittedValue::Rows {
            columns,
            rows: materialized,
            complete,
        },
        EmitDescription::Rows { columns: described },
    ) = (&out.emitted[11], &expected[11])
    else {
        panic!("rows expected");
    };
    assert_eq!(columns, described);
    assert_eq!(materialized, &vec![row.clone()]);
    assert!(*complete);

    admin
        .sql_session()
        .execute("INSERT INTO items(id,n) VALUES(2,20)", &[], None)
        .unwrap();
    admin
        .sql_session()
        .execute("UPDATE items SET n=99 WHERE id=1", &[], None)
        .unwrap();
    change_catalog(&admin);
    let before = digest(&admin);
    assert_retained(&engine, &original, &receipt);
    assert_eq!(digest(&admin), before);
    assert_eq!(rows(&admin).len(), 2);
}

#[test]
fn once_terminal_outcomes_retain_bound_schema_and_discard_provisional_writes() {
    let (_, admin, engine, epoch, table) = fixture();
    let original = request(
        &admin,
        b"terminal-return",
        OnceBody::Operation(returning(
            update(int(111), Some(eq(field("n"), int(99)))),
            vec![projection_alias(field("small"), "original_small")],
        )),
    );
    let before_rows = rows(&admin);
    let failed =
        with_sql_parser_forbidden(|| engine.execute_once(&original, &Bindings::new(), &clock))
            .unwrap();
    let mut renamed = column(&table, "small");
    renamed.name = "original_small".into();
    assert_eq!(failed.outcome, Err(Error::ConditionFailed));
    assert_eq!(
        failed.description,
        OnceDescription {
            catalog_epoch: epoch,
            output: OnceSchema::Operation(vec![renamed])
        }
    );
    assert_eq!(rows(&admin), before_rows);

    let transaction = request(
        &admin,
        b"terminal-emit",
        OnceBody::Transaction(Transaction {
            read_only: false,
            steps: vec![
                TransactionStep::Let {
                    name: "tentative".into(),
                    request: returning(update(int(88), None), vec![]),
                },
                TransactionStep::Require(value(V::Bool(false))),
            ],
            emit: Some(vec![
                EmitExpr::Scalar(local("tentative", "medium")),
                EmitExpr::Scalar(value(V::Null)),
                EmitExpr::Row("tentative".into()),
            ]),
        }),
    );
    let aborted =
        with_sql_parser_forbidden(|| engine.execute_once(&transaction, &Bindings::new(), &clock))
            .unwrap();
    assert_eq!(aborted.outcome, Err(Error::RequirementFailed));
    assert_eq!(
        aborted.description,
        OnceDescription {
            catalog_epoch: epoch,
            output: OnceSchema::Transaction(vec![
                EmitDescription::Scalar {
                    data_type: Some(DataType::Integer),
                    nullable: true
                },
                EmitDescription::Scalar {
                    data_type: None,
                    nullable: true
                },
                EmitDescription::Row {
                    columns: table.columns.clone()
                },
            ])
        }
    );
    assert_eq!(
        rows(&admin),
        before_rows,
        "terminal schema persistence must not retain staged data or versions"
    );
    admin
        .sql_session()
        .execute("UPDATE items SET n=99 WHERE id=1", &[], None)
        .unwrap();
    change_catalog(&admin);
    let before = digest(&admin);
    assert_retained(&engine, &original, &failed);
    assert_retained(&engine, &transaction, &aborted);
    assert_eq!(digest(&admin), before);
    assert_eq!(rows(&admin)[0].values[1], V::Int(99));
}

#[test]
fn once_unreturned_write_retains_an_explicit_empty_operation_schema() {
    let (_, admin, engine, epoch, _) = fixture();
    let original = request(
        &admin,
        b"no-return",
        OnceBody::Operation(update(int(12), None)),
    );
    let receipt =
        with_sql_parser_forbidden(|| engine.execute_once(&original, &Bindings::new(), &clock))
            .unwrap();
    assert_eq!(
        receipt.description,
        OnceDescription {
            catalog_epoch: epoch,
            output: OnceSchema::Operation(vec![])
        }
    );
    let OnceOutcome::Operation(out) = receipt.outcome.as_ref().unwrap() else {
        panic!("operation expected");
    };
    assert!(out.columns.is_empty());
    assert!(out.rows.is_empty());
    assert_eq!((out.matched, out.affected, out.changed), (1, 1, 1));
    assert_retained(&engine, &original, &receipt);
}
