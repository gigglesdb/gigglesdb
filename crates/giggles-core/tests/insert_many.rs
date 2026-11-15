mod common;

use giggles_core::{
    Admin, Cancellation, CoreError, CrashPoint, DecisionPoint, Limits,
    operations::{
        Arithmetic, Bindings, Compare, EmitExpr, EmittedValue, Engine, Error, Expr, Intent,
        LocalRef, MAX_DEPTH, MAX_INSERT_BYTES, MAX_INSERT_ROWS, Projection, Request, Transaction,
        TransactionStep, ValueExpr,
    },
    relational::{DataType, MAX_COLUMNS, MAX_RESULT_BYTES, MAX_ROW_BYTES, SqlRow, Value as V},
};
use std::{path::PathBuf, sync::Arc};

fn value(value: V) -> Expr {
    Expr::Value(ValueExpr::Literal(value))
}
fn int(n: i64) -> Expr {
    value(V::Int(n))
}
fn parameter(name: &str) -> Expr {
    Expr::Value(ValueExpr::Parameter(name.into()))
}
fn field(name: &str) -> Expr {
    Expr::Field(name.into())
}
fn binary(a: Expr, op: Arithmetic, b: Expr) -> Expr {
    Expr::Arithmetic(Box::new(a), op, Box::new(b))
}
fn equal(a: Expr, b: Expr) -> Expr {
    Expr::Compare(Box::new(a), Compare::Eq, Box::new(b))
}
fn projection(expression: Expr, alias: Option<&str>) -> Projection {
    Projection {
        expression,
        alias: alias.map(str::to_owned),
    }
}
fn many(columns: &[&str], rows: Vec<Vec<Expr>>, cap: i64) -> Request {
    Request::InsertMany {
        table: "items".into(),
        columns: columns.iter().map(|s| (*s).into()).collect(),
        rows,
        cap: ValueExpr::Literal(V::Int(cap)),
    }
}
fn returning(request: Request, projection: Vec<Projection>) -> Request {
    Request::Returning {
        request: Box::new(request),
        projection,
    }
}
fn database(schema: &str) -> (PathBuf, Admin, Engine) {
    let path = common::fresh("insert-many");
    let admin = Admin::create(&path, Limits::default()).unwrap();
    admin.sql_session().execute(schema, &[], None).unwrap();
    (path, admin.clone(), Engine::new(admin))
}
fn fixture() -> (PathBuf, Admin, Engine) {
    database("CREATE TABLE items (id BIGINT PRIMARY KEY,n BIGINT,tag TEXT)")
}
fn rows(admin: &Admin) -> Vec<SqlRow> {
    admin
        .sql_session()
        .execute("SELECT * FROM items LIMIT 1024", &[], None)
        .unwrap()
        .rows
}
fn digest(admin: &Admin) -> [u8; 32] {
    admin.export(common::fresh("insert-many-export")).unwrap()
}
fn runtime_failure(
    admin: &Admin,
    engine: &Engine,
    request: &Request,
    bindings: &Bindings,
    expected: Error,
) {
    let before = digest(admin);
    let catalog = admin.catalog().unwrap();
    let bound = engine.prepare_bound(request, bindings).unwrap();
    assert_eq!(engine.execute_bound(&bound), Err(expected.clone()));
    assert_eq!(engine.execute(request, bindings), Err(expected));
    assert_eq!(digest(admin), before);
    assert_eq!(admin.catalog().unwrap(), catalog);
}
fn static_failure(admin: &Admin, engine: &Engine, request: &Request, expected: Option<Error>) {
    let before = digest(admin);
    let prepared = engine.prepare_bound(request, &Bindings::new()).unwrap_err();
    assert_eq!(
        engine.execute(request, &Bindings::new()),
        Err(prepared.clone())
    );
    if let Some(expected) = expected {
        assert_eq!(prepared, expected);
    }
    assert_eq!(digest(admin), before);
}
fn seed_ids(admin: &Admin, count: usize) {
    let mut sql = admin.sql_session();
    sql.execute("BEGIN", &[], None).unwrap();
    for id in 0..count {
        sql.execute(
            "INSERT INTO items(id) VALUES($1)",
            &[V::Int(id as i64)],
            None,
        )
        .unwrap();
    }
    sql.execute("COMMIT", &[], None).unwrap();
}

#[test]
fn insert_many_binding_and_shape_checks_precede_execution() {
    let (_, admin, engine) = fixture();
    for (request, expected) in [
        (many(&[], vec![], 0), Error::Bind("columns".into())),
        (
            many(&["id", "id"], vec![], 0),
            Error::Bind("duplicate assignment".into()),
        ),
        (many(&["absent"], vec![], 0), Error::Sql { sqlstate: "42703".into() }),
        (many(&["id", "n"], vec![vec![int(1)]], 0), Error::Type),
        (
            many(
                &["id", "n"],
                vec![vec![int(1), int(2)], vec![int(2), value(V::Bool(true))]],
                0,
            ),
            Error::Sql { sqlstate: "42804".into() },
        ),
    ] {
        static_failure(&admin, &engine, &request, Some(expected));
    }
    for cap in [V::Null, V::Int(-1), V::Int(1025), V::Text("1".into())] {
        let mut request = many(&["id"], vec![], 0);
        if let Request::InsertMany { cap: target, .. } = &mut request {
            *target = ValueExpr::Literal(cap);
        }
        static_failure(&admin, &engine, &request, Some(Error::Type));
    }
    for request in [
        many(&["id", "n"], vec![vec![int(1), field("n")]], 0),
        many(
            &["id", "n"],
            vec![
                vec![int(1), binary(int(1), Arithmetic::Divide, int(0))],
                vec![int(2), parameter("missing")],
            ],
            0,
        ),
        returning(
            many(&["id"], vec![], 0),
            vec![projection(field("absent"), None)],
        ),
        returning(
            many(&["id"], vec![], 0),
            vec![projection(value(V::Null), Some("unknown"))],
        ),
    ] {
        static_failure(&admin, &engine, &request, None);
    }
    // No proposed row means there is no missing-required-id row constraint.
    for request in [
        many(&["n"], vec![], 0),
        returning(many(&["n"], vec![], 0), vec![]),
    ] {
        let prepared = engine.prepare_bound(&request, &Bindings::new()).unwrap();
        let result = engine.execute_bound(&prepared).unwrap();
        assert_eq!((result.matched, result.affected, result.changed), (0, 0, 0));
        assert!(result.rows.is_empty() && result.complete);
        assert_eq!(result.columns.len(), prepared.description().columns.len());
    }
    let request = returning(
        many(&["n"], vec![], 0),
        vec![projection(
            binary(int(1), Arithmetic::Divide, int(0)),
            Some("quotient"),
        )],
    );
    let bound = engine.prepare_bound(&request, &Bindings::new()).unwrap();
    assert_eq!(bound.description().columns[0].data_type, DataType::BigInt);
    assert_eq!(bound.description().columns[0].id, 0);
    assert!(engine.execute_bound(&bound).unwrap().rows.is_empty());
    runtime_failure(
        &admin,
        &engine,
        &many(&["n"], vec![vec![int(1)]], 1),
        &Bindings::new(),
        Error::Sql { sqlstate: "23502".into() },
    );
    let mut deep = int(1);
    for _ in 0..=MAX_DEPTH {
        deep = Expr::Negate(Box::new(deep));
    }
    static_failure(
        &admin,
        &engine,
        &many(&["id", "n"], vec![vec![int(1), deep]], 1),
        Some(Error::Limit),
    );
}

#[test]
fn insert_many_cap_precedes_scan_and_runtime_locals() {
    let (_, admin, engine) = fixture();
    runtime_failure(
        &admin,
        &engine,
        &returning(
            many(
                &["id", "n"],
                vec![vec![int(1), binary(int(1), Arithmetic::Divide, int(0))]],
                0,
            ),
            vec![projection(
                binary(int(1), Arithmetic::Divide, int(0)),
                Some("bad"),
            )],
        ),
        &Bindings::new(),
        Error::TooManyMatches,
    );
    seed_ids(&admin, MAX_INSERT_ROWS + 1);
    let request = many(&["id"], vec![vec![int(2000)]], 0);
    runtime_failure(
        &admin,
        &engine,
        &request,
        &Bindings::new(),
        Error::TooManyMatches,
    );
    runtime_failure(
        &admin,
        &engine,
        &many(&["id"], vec![vec![int(2000)]], 1),
        &Bindings::new(),
        Error::Limit,
    );

    let (_, admin, engine) = database(
        "CREATE TABLE items (id BIGINT PRIMARY KEY,n BIGINT,src TEXT,a TEXT,b TEXT,c TEXT)",
    );
    admin
        .sql_session()
        .execute(
            "INSERT INTO items(id,n,src) VALUES(1,0,$1)",
            &[V::Text("x".repeat(32768))],
            None,
        )
        .unwrap();
    let saved = Request::UpdateExpressions {
        table: "items".into(),
        filter: equal(field("id"), int(1)),
        guard: None,
        values: vec![("n".into(), binary(field("n"), Arithmetic::Add, int(1)))],
        intent: Intent::One,
    };
    let local = || {
        Expr::Local(LocalRef::Field {
            local: "saved".into(),
            field: "p".into(),
        })
    };
    for (cap, expected) in [(0, Error::TooManyMatches), (1, Error::Limit)] {
        let transaction = Transaction {
            read_only: false,
            steps: vec![
                TransactionStep::Let {
                    name: "saved".into(),
                    request: returning(saved.clone(), vec![projection(field("src"), Some("p"))]),
                },
                TransactionStep::Execute(many(
                    &["id", "a", "b", "c"],
                    vec![vec![int(2), local(), local(), local()]],
                    cap,
                )),
            ],
            emit: None,
        };
        let before = digest(&admin);
        let bound = engine
            .prepare_transaction_bound(&transaction, &Bindings::new())
            .unwrap();
        assert_eq!(
            engine.execute_transaction_bound(&bound),
            Err(expected.clone())
        );
        assert_eq!(
            engine.execute_transaction(&transaction, &Bindings::new()),
            Err(expected)
        );
        assert_eq!(digest(&admin), before);
    }
}

#[test]
fn insert_many_canonical_keys_and_existing_conflicts_are_atomic() {
    let (_, admin, engine) = database(
        "CREATE TABLE items (tenant TEXT,id BYTES,n BIGINT NOT NULL,PRIMARY KEY(tenant,id))",
    );
    admin
        .sql_session()
        .execute(
            "INSERT INTO items VALUES($1,$2,1)",
            &[V::Text("existing".into()), V::Bytes(vec![0])],
            None,
        )
        .unwrap();
    let row = |tenant: &str, key: &[u8], n: Expr| {
        vec![
            value(V::Text(tenant.into())),
            value(V::Bytes(key.into())),
            n,
        ]
    };
    let duplicate = many(
        &["tenant", "id", "n"],
        vec![
            row("a\0", &[0, 255], int(1)),
            row("new", &[], int(2)),
            row("a\0", &[0, 255], int(3)),
        ],
        3,
    );
    runtime_failure(
        &admin,
        &engine,
        &duplicate,
        &Bindings::new(),
        Error::DuplicateTarget,
    );
    let mixed = many(
        &["tenant", "id", "n"],
        vec![row("existing", &[0], int(1)), row("existing", &[0], int(2))],
        2,
    );
    runtime_failure(
        &admin,
        &engine,
        &mixed,
        &Bindings::new(),
        Error::DuplicateTarget,
    );
    let invalid = many(
        &["tenant", "id", "n"],
        vec![
            row("existing", &[0], int(1)),
            row("existing", &[0], value(V::Null)),
        ],
        2,
    );
    runtime_failure(&admin, &engine, &invalid, &Bindings::new(), Error::Sql { sqlstate: "23502".into() });
    let existing = Request::Insert {
        table: "items".into(),
        values: vec![
            (
                "tenant".into(),
                ValueExpr::Literal(V::Text("existing".into())),
            ),
            ("id".into(), ValueExpr::Literal(V::Bytes(vec![0]))),
            ("n".into(), ValueExpr::Literal(V::Int(3))),
        ],
    };
    let ordinary_error = engine.execute(&existing, &Bindings::new()).unwrap_err();
    assert_eq!(ordinary_error, Error::Duplicate);
    runtime_failure(
        &admin,
        &engine,
        &many(
            &["tenant", "id", "n"],
            vec![row("new", &[], int(2)), row("existing", &[0], int(3))],
            2,
        ),
        &Bindings::new(),
        ordinary_error,
    );
    let maximum = rows(&admin)[0].version;
    let distinct = returning(
        many(
            &["tenant", "id", "n"],
            vec![
                row("a\0", &[], int(3)),
                row("a", &[0, 0], int(2)),
                row("a", &[0], int(1)),
            ],
            3,
        ),
        vec![],
    );
    let result = engine.execute(&distinct, &Bindings::new()).unwrap();
    assert_eq!(
        result
            .rows
            .iter()
            .map(|r| r.values.clone())
            .collect::<Vec<_>>(),
        [
            vec![V::Text("a".into()), V::Bytes(vec![0]), V::Int(1)],
            vec![V::Text("a".into()), V::Bytes(vec![0, 0]), V::Int(2)],
            vec![V::Text("a\0".into()), V::Bytes(vec![]), V::Int(3)],
        ]
    );
    let mut versions = result.rows.iter().map(|r| r.version).collect::<Vec<_>>();
    versions.sort_unstable();
    assert_eq!(versions, [maximum + 1, maximum + 2, maximum + 3]);
    let (_, admin, engine) = fixture();
    runtime_failure(
        &admin,
        &engine,
        &many(
            &["id"],
            vec![vec![binary(int(1), Arithmetic::Add, int(1))], vec![int(2)]],
            2,
        ),
        &Bindings::new(),
        Error::DuplicateTarget,
    );
}

#[test]
fn insert_many_row_and_table_limits_are_finite() {
    assert_eq!(MAX_INSERT_ROWS, 1024);
    assert_eq!(MAX_INSERT_BYTES, 1024 * 1024);
    let (_, admin, engine) = fixture();
    let request = many(
        &["id"],
        (0..MAX_INSERT_ROWS).map(|i| vec![int(i as i64)]).collect(),
        MAX_INSERT_ROWS as i64,
    );
    let result = engine.execute(&request, &Bindings::new()).unwrap();
    assert_eq!(
        (result.matched, result.affected, result.changed),
        (1024, 1024, 1024)
    );
    assert!(result.rows.is_empty() && result.complete);
    assert_eq!(rows(&admin).len(), 1024);
    let mut over = request;
    if let Request::InsertMany { rows, .. } = &mut over {
        rows.push(vec![int(2000)]);
    }
    static_failure(&admin, &engine, &over, Some(Error::Limit));
    runtime_failure(
        &admin,
        &engine,
        &many(&["id"], vec![vec![int(2000)]], 1),
        &Bindings::new(),
        Error::Limit,
    );
    runtime_failure(
        &admin,
        &engine,
        &many(&["id"], vec![vec![int(1)]], 1),
        &Bindings::new(),
        Error::Duplicate,
    );
    let (_, admin, engine) = fixture();
    seed_ids(&admin, MAX_INSERT_ROWS - 1);
    runtime_failure(
        &admin,
        &engine,
        &many(&["id"], vec![vec![int(2000)], vec![int(2001)]], 2),
        &Bindings::new(),
        Error::Limit,
    );
    assert_eq!(
        engine
            .execute(&many(&["id"], vec![vec![int(2000)]], 1), &Bindings::new())
            .unwrap()
            .changed,
        1
    );

    let schema = format!(
        "CREATE TABLE items (id TEXT PRIMARY KEY,{})",
        (0..MAX_COLUMNS - 1)
            .map(|i| format!("c{i} BIGINT"))
            .collect::<Vec<_>>()
            .join(",")
    );
    let (_, admin, engine) = database(&schema);
    let mut columns = vec!["id".to_owned()];
    columns.extend((0..MAX_COLUMNS - 1).map(|i| format!("c{i}")));
    let empty = Request::InsertMany {
        table: "items".into(),
        columns: columns.clone(),
        rows: vec![],
        cap: ValueExpr::Literal(V::Int(0)),
    };
    assert!(
        engine
            .execute(&empty, &Bindings::new())
            .unwrap()
            .rows
            .is_empty()
    );
    columns.push("extra".into());
    static_failure(
        &admin,
        &engine,
        &Request::InsertMany {
            table: "items".into(),
            columns,
            rows: vec![],
            cap: ValueExpr::Literal(V::Int(0)),
        },
        Some(Error::Limit),
    );
    let payload = MAX_ROW_BYTES - 9 * (MAX_COLUMNS - 1);
    assert_eq!(payload, 64969);
    let request = returning(many(&["id"], vec![vec![parameter("p")]], 1), vec![]);
    let bindings = Bindings::from([("p".into(), V::Text("x".repeat(payload)))]);
    let result = engine.execute(&request, &bindings).unwrap();
    assert_eq!(result.rows[0].values.len(), MAX_COLUMNS);
    assert_eq!(result.rows[0].values[0], bindings["p"]);
    let over = Bindings::from([("p".into(), V::Text("y".repeat(payload + 1)))]);
    runtime_failure(
        &admin,
        &engine,
        &returning(
            many(&["id"], vec![vec![parameter("p")]], 1),
            vec![projection(
                binary(int(1), Arithmetic::Divide, int(0)),
                Some("bad"),
            )],
        ),
        &over,
        Error::Limit,
    );
    // The shared 64 KiB resolved-value allowance plus at most 1024*64
    // default NULLs already bounds these proposals below 1 MiB; do not
    // invent an unreachable exact aggregate-mutation fixture.
}

#[test]
fn insert_many_returning_is_complete_and_preflighted() {
    let (_, admin, engine) =
        database("CREATE TABLE items (tenant TEXT,id BIGINT,n BIGINT,PRIMARY KEY(tenant,id))");
    let tuples = (0..110)
        .rev()
        .map(|i| {
            vec![
                value(V::Text(format!("t{:02}", i / 11))),
                int(i % 11),
                int(i),
            ]
        })
        .collect();
    let request = returning(many(&["tenant", "id", "n"], tuples, 110), vec![]);
    let bound = engine.prepare_bound(&request, &Bindings::new()).unwrap();
    let result = engine.execute_bound(&bound).unwrap();
    assert_eq!(result.rows.len(), 110);
    assert!(result.complete);
    assert_eq!(result.rows, rows(&admin));
    for (i, row) in result.rows.iter().enumerate() {
        assert_eq!(
            row.values,
            [
                V::Text(format!("t{:02}", i / 11)),
                V::Int(i as i64 % 11),
                V::Int(i as i64)
            ]
        );
    }
    for extra in [0, 1] {
        let (_, admin, engine) =
            database("CREATE TABLE items (id BIGINT PRIMARY KEY,src TEXT,tail TEXT)");
        let mut output = (0..63)
            .map(|i| projection(field("src"), Some(&format!("p{i}"))))
            .collect::<Vec<_>>();
        output.push(projection(field("tail"), Some("last")));
        let request = returning(
            many(
                &["id", "src", "tail"],
                vec![vec![int(1), parameter("src"), parameter("tail")]],
                1,
            ),
            output,
        );
        let bindings = Bindings::from([
            ("src".into(), V::Text("x".repeat(16384))),
            ("tail".into(), V::Text("y".repeat(16384 + extra))),
        ]);
        if extra == 0 {
            let result = engine.execute(&request, &bindings).unwrap();
            assert_eq!(
                result.rows[0]
                    .values
                    .iter()
                    .map(|v| if let V::Text(s) = v {
                        s.len()
                    } else {
                        panic!("text")
                    })
                    .sum::<usize>(),
                MAX_RESULT_BYTES
            );
            assert_eq!(result.rows[0].version, rows(&admin)[0].version);
        } else {
            runtime_failure(&admin, &engine, &request, &bindings, Error::Limit);
        }
    }
    let (_, admin, engine) = fixture();
    runtime_failure(
        &admin,
        &engine,
        &returning(
            many(&["id"], vec![vec![int(1)], vec![int(2)]], 2),
            vec![projection(
                binary(
                    int(1),
                    Arithmetic::Divide,
                    binary(int(2), Arithmetic::Subtract, field("id")),
                ),
                Some("bad"),
            )],
        ),
        &Bindings::new(),
        Error::DivisionByZero,
    );
    assert!(rows(&admin).is_empty());
    let inserted = engine
        .execute(
            &returning(many(&["id"], vec![vec![int(1)]], 1), vec![]),
            &Bindings::new(),
        )
        .unwrap();
    assert_eq!(inserted.rows[0].version, 1);
}

#[test]
fn insert_many_prepared_and_transaction_local_contracts() {
    let (_, admin, engine) = fixture();
    let request = returning(
        many(&["id", "n"], vec![vec![parameter("id"), parameter("n")]], 1),
        vec![],
    );
    let mut bindings = Bindings::from([("id".into(), V::Int(1)), ("n".into(), V::Int(5))]);
    let before = digest(&admin);
    let bound = engine.prepare_bound(&request, &bindings).unwrap();
    assert_eq!(digest(&admin), before);
    bindings.insert("id".into(), V::Int(99));
    bindings.insert("n".into(), V::Int(99));
    admin
        .sql_session()
        .execute("INSERT INTO items(id,n) VALUES(1,8)", &[], None)
        .unwrap();
    let before = digest(&admin);
    assert_eq!(
        engine.execute_bound(&bound),
        Err(Error::Duplicate)
    );
    assert_eq!(digest(&admin), before);
    admin
        .sql_session()
        .execute("DELETE FROM items WHERE id=1", &[], None)
        .unwrap();
    assert_eq!(
        engine.execute_bound(&bound).unwrap().rows[0].values,
        [V::Int(1), V::Int(5), V::Null]
    );
    admin
        .sql_session()
        .execute("CREATE TABLE newer (id BIGINT PRIMARY KEY)", &[], None)
        .unwrap();
    assert_eq!(
        engine.execute_bound(&bound),
        Err(Error::Sql { sqlstate: "0A000".into() })
    );
    let bound = engine.prepare_bound(&request, &bindings).unwrap();
    let export = common::fresh("insert-many-domain-export");
    admin.export(&export).unwrap();
    let restored = Admin::restore(
        export,
        common::fresh("insert-many-domain-restored"),
        Limits::default(),
    )
    .unwrap();
    assert_eq!(
        Engine::new(restored).execute_bound(&bound),
        Err(Error::Sql { sqlstate: "0A000".into() })
    );

    let (_, admin, engine) = fixture();
    let base = returning(many(&["id"], vec![vec![int(1)]], 1), vec![]);
    for emitted in [
        EmitExpr::Row("batch".into()),
        EmitExpr::Rows("batch".into()),
        EmitExpr::Scalar(Expr::Local(LocalRef::Count {
            local: "batch".into(),
        })),
        EmitExpr::Scalar(Expr::Local(LocalRef::Field {
            local: "batch".into(),
            field: "id".into(),
        })),
    ] {
        let transaction = Transaction {
            read_only: false,
            steps: vec![TransactionStep::Let {
                name: "batch".into(),
                request: base.clone(),
            }],
            emit: Some(vec![emitted]),
        };
        let before = digest(&admin);
        assert!(
            engine
                .prepare_transaction_bound(&transaction, &Bindings::new())
                .is_err()
        );
        assert!(
            engine
                .execute_transaction(&transaction, &Bindings::new())
                .is_err()
        );
        assert_eq!(digest(&admin), before);
    }
    let changed = Expr::Local(LocalRef::Changed {
        local: "batch".into(),
    });
    let transaction = Transaction {
        read_only: false,
        steps: vec![
            TransactionStep::Let {
                name: "batch".into(),
                request: returning(many(&["id"], vec![vec![int(1)], vec![int(2)]], 2), vec![]),
            },
            TransactionStep::Require(equal(changed.clone(), int(2))),
        ],
        emit: Some(vec![
            EmitExpr::Scalar(Expr::Local(LocalRef::Status {
                local: "batch".into(),
            })),
            EmitExpr::Scalar(changed),
        ]),
    };
    for tail in [
        TransactionStep::Require(value(V::Bool(false))),
        TransactionStep::Require(value(V::Null)),
    ] {
        let mut failed = transaction.clone();
        failed.steps.push(tail);
        let before = digest(&admin);
        let bound = engine
            .prepare_transaction_bound(&failed, &Bindings::new())
            .unwrap();
        assert_eq!(
            engine.execute_transaction_bound(&bound),
            Err(Error::RequirementFailed)
        );
        assert_eq!(
            engine.execute_transaction(&failed, &Bindings::new()),
            Err(Error::RequirementFailed)
        );
        assert_eq!(digest(&admin), before);
    }
    let result = engine
        .execute_transaction(&transaction, &Bindings::new())
        .unwrap();
    assert_eq!(
        result.emitted,
        [
            EmittedValue::Scalar(V::Text("success".into())),
            EmittedValue::Scalar(V::Int(2))
        ]
    );
    assert_eq!(rows(&admin).len(), 2);
    let mut readonly = transaction;
    readonly.read_only = true;
    assert_eq!(
        engine
            .prepare_transaction_bound(&readonly, &Bindings::new())
            .unwrap_err(),
        Error::Unsupported
    );
}

#[test]
fn insert_many_cancellation_and_unknown_hide_speculative_results() {
    let request = returning(many(&["id"], vec![vec![int(1)], vec![int(2)]], 2), vec![]);
    for (point, wins) in [
        (DecisionPoint::BeforeDecision, true),
        (DecisionPoint::Decided, false),
    ] {
        let (_, admin, engine) = fixture();
        let before = digest(&admin);
        admin.set_decision_hook(Some(Arc::new(move |current| {
            if current == point {
                assert_eq!(Cancellation::current().unwrap().cancel(), wins);
            }
        })));
        let result = Cancellation::default().scope(|| engine.execute(&request, &Bindings::new()));
        admin.set_decision_hook(None);
        if wins {
            assert_eq!(result, Err(Error::Core(CoreError::Cancelled.to_string())));
            assert_eq!(digest(&admin), before);
        } else {
            assert_eq!(result.unwrap().rows, rows(&admin));
            assert_eq!(rows(&admin).len(), 2);
        }
    }
    for (point, persisted) in [
        (CrashPoint::ErrorBeforeCommit, false),
        (CrashPoint::ErrorAfterCommit, true),
    ] {
        let (path, admin, engine) = fixture();
        admin.set_crash_point(point).unwrap();
        assert_eq!(
            engine.execute(&request, &Bindings::new()),
            Err(Error::Unknown)
        );
        drop(engine);
        drop(admin);
        let reopened = Admin::open(path, Limits::default()).unwrap();
        assert_eq!(rows(&reopened).len(), if persisted { 2 } else { 0 });
    }
}
