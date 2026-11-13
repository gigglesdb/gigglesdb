mod common;

use giggles_core::{
    Admin, Cancellation, CoreError, CrashPoint, DecisionPoint, Limits,
    operations::{
        Arithmetic, Bindings, Compare, EmitDescription, EmitExpr, EmittedValue, Engine, Error,
        Expr, Intent, LocalRef, MAX_BINDINGS, MAX_BOUND_VALUE_BYTES, MAX_DEPTH, MAX_EMIT_BYTES,
        MAX_EMIT_VALUES, MAX_EXPRESSION_NODES, MAX_INPUT, MAX_RETAINED_LOCAL_BYTES,
        MAX_TRANSACTION_ITEMS, Projection, Request, Sort, Transaction, TransactionStep, ValueExpr,
    },
    relational::{DataType, MAX_TRANSACTIONS, SqlRow, Value as V},
};
use std::{
    collections::BTreeSet,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

fn value(v: V) -> Expr {
    Expr::Value(ValueExpr::Literal(v))
}
fn int(n: i64) -> Expr {
    value(V::Int(n))
}
fn field(name: &str) -> Expr {
    Expr::Field(name.into())
}
fn parameter(name: &str) -> Expr {
    Expr::Value(ValueExpr::Parameter(name.into()))
}
fn binary(a: Expr, op: Arithmetic, b: Expr) -> Expr {
    Expr::Arithmetic(Box::new(a), op, Box::new(b))
}
fn eq(a: Expr, b: Expr) -> Expr {
    Expr::Compare(Box::new(a), Compare::Eq, Box::new(b))
}
fn yes() -> Expr {
    value(V::Bool(true))
}
fn local(name: &str, column: &str) -> Expr {
    Expr::Local(LocalRef::Field {
        local: name.into(),
        field: column.into(),
    })
}
fn count(name: &str) -> Expr {
    Expr::Local(LocalRef::Count { local: name.into() })
}
fn changed(name: &str) -> Expr {
    Expr::Local(LocalRef::Changed { local: name.into() })
}
fn status(name: &str) -> Expr {
    Expr::Local(LocalRef::Status { local: name.into() })
}
fn projection(expression: Expr, alias: Option<&str>) -> Projection {
    Projection {
        expression,
        alias: alias.map(str::to_owned),
    }
}
fn returning(request: Request, projection: Vec<Projection>) -> Request {
    Request::Returning {
        request: Box::new(request),
        projection,
    }
}
fn many(cap: i64) -> Intent {
    Intent::Many {
        cap: ValueExpr::Literal(V::Int(cap)),
    }
}
fn update(filter: Expr, intent: Intent, values: Vec<(&str, Expr)>) -> Request {
    Request::UpdateExpressions {
        table: "items".into(),
        filter,
        guard: None,
        intent,
        values: values.into_iter().map(|(k, v)| (k.into(), v)).collect(),
    }
}
fn one_update(rhs: Expr) -> Request {
    update(eq(field("id"), int(1)), Intent::One, vec![("x", rhs)])
}
fn read(limit: Option<i64>) -> Request {
    Request::Read {
        table: "items".into(),
        filter: None,
        order: vec![Sort {
            field: "id".into(),
            descending: false,
        }],
        limit: limit.map(|n| ValueExpr::Literal(V::Int(n))),
        projection: vec!["id".into(), "x".into()],
    }
}
fn insert(id: i64) -> Request {
    Request::Insert {
        table: "items".into(),
        values: vec![
            ("id".into(), ValueExpr::Literal(V::Int(id))),
            ("x".into(), ValueExpr::Literal(V::Int(30))),
            ("state".into(), ValueExpr::Literal(V::Text("ready".into()))),
        ],
    }
}
fn let_step(name: &str, request: Request) -> TransactionStep {
    TransactionStep::Let {
        name: name.into(),
        request,
    }
}
fn transaction(
    read_only: bool,
    steps: Vec<TransactionStep>,
    emit: Option<Vec<EmitExpr>>,
) -> Transaction {
    Transaction {
        read_only,
        steps,
        emit,
    }
}
fn database(schema: &str) -> (PathBuf, Admin, Engine) {
    let path = common::fresh("transaction");
    let admin = Admin::create(&path, Limits::default()).unwrap();
    admin.sql_session().execute(schema, &[], None).unwrap();
    (path, admin.clone(), Engine::new(admin))
}
fn fixture() -> (PathBuf, Admin, Engine) {
    let (path, admin, engine) = database(
        "CREATE TABLE items (id BIGINT PRIMARY KEY,x BIGINT,narrow SMALLINT,state TEXT NOT NULL,payload TEXT)",
    );
    for source in [
        "CREATE TABLE outbox (id BIGINT PRIMARY KEY,x BIGINT)",
        "INSERT INTO items VALUES(1,10,NULL,'ready','source')",
        "INSERT INTO items VALUES(2,20,32767,'ready','source')",
    ] {
        admin.sql_session().execute(source, &[], None).unwrap();
    }
    (path, admin, engine)
}
fn rows(admin: &Admin) -> Vec<SqlRow> {
    admin
        .sql_session()
        .execute("SELECT * FROM items LIMIT 1024", &[], None)
        .unwrap()
        .rows
}
fn digest(admin: &Admin) -> [u8; 32] {
    admin.export(common::fresh("transaction-export")).unwrap()
}
fn runtime_failure(
    admin: &Admin,
    engine: &Engine,
    tx: &Transaction,
    bindings: &Bindings,
    expected: Error,
) {
    let before = digest(admin);
    let catalog = admin.catalog().unwrap();
    let existing = rows(admin);
    let prepared = engine.prepare_transaction_bound(tx, bindings).unwrap();
    assert_eq!(
        engine.execute_transaction_bound(&prepared),
        Err(expected.clone())
    );
    assert_eq!(engine.execute_transaction(tx, bindings), Err(expected));
    assert_eq!(digest(admin), before);
    assert_eq!(admin.catalog().unwrap(), catalog);
    assert_eq!(rows(admin), existing);
}
fn static_failure(admin: &Admin, engine: &Engine, tx: &Transaction, expected: Option<Error>) {
    let before = digest(admin);
    let prepared = engine
        .prepare_transaction_bound(tx, &Bindings::new())
        .expect_err("binding must fail");
    let actual = engine
        .execute_transaction(tx, &Bindings::new())
        .unwrap_err();
    assert_eq!(actual, prepared);
    if let Some(expected) = expected {
        assert_eq!(actual, expected);
    }
    assert_eq!(digest(admin), before);
}

#[test]
fn transaction_binds_every_step_and_lexical_local_shape_before_execution() {
    let (_, admin, engine) = fixture();
    let mut bad_read = read(None);
    if let Request::Read { projection, .. } = &mut bad_read {
        *projection = vec!["absent".into()];
    }
    for earlier in [
        TransactionStep::Execute(insert(1)),
        TransactionStep::Require(eq(binary(int(1), Arithmetic::Divide, int(0)), int(0))),
    ] {
        static_failure(
            &admin,
            &engine,
            &transaction(
                false,
                vec![earlier, TransactionStep::Execute(bad_read.clone())],
                None,
            ),
            Some(Error::Bind("column".into())),
        );
    }
    let singular = returning(
        one_update(field("x")),
        vec![projection(field("x"), Some("saved"))],
    );
    for tx in [
        transaction(
            false,
            vec![
                let_step("a", singular.clone()),
                let_step("a", singular.clone()),
            ],
            None,
        ),
        transaction(
            false,
            vec![
                TransactionStep::Require(eq(local("later", "saved"), int(10))),
                let_step("later", singular.clone()),
            ],
            None,
        ),
        transaction(
            false,
            vec![let_step("self_ref", one_update(local("self_ref", "saved")))],
            None,
        ),
        transaction(
            false,
            vec![let_step("a", singular.clone())],
            Some(vec![EmitExpr::Scalar(local("a", "x"))]),
        ),
        transaction(
            false,
            vec![let_step("a", one_update(field("x")))],
            Some(vec![EmitExpr::Row("a".into())]),
        ),
        transaction(
            false,
            vec![let_step("r", read(Some(1)))],
            Some(vec![EmitExpr::Row("r".into())]),
        ),
        transaction(
            false,
            vec![let_step("r", read(Some(1)))],
            Some(vec![EmitExpr::Scalar(status("r"))]),
        ),
        transaction(
            false,
            vec![let_step("w", singular.clone())],
            Some(vec![EmitExpr::Rows("w".into())]),
        ),
        transaction(
            false,
            vec![let_step("w", singular)],
            Some(vec![EmitExpr::Scalar(count("w"))]),
        ),
        transaction(false, vec![TransactionStep::Require(field("x"))], None),
        transaction(false, vec![], Some(vec![EmitExpr::Scalar(field("x"))])),
    ] {
        static_failure(&admin, &engine, &tx, None);
    }
    assert!(
        engine
            .execute(&one_update(local("missing", "saved")), &Bindings::new())
            .is_err()
    );
    let no_source = Request::InsertExpressions {
        table: "outbox".into(),
        values: vec![("id".into(), int(1)), ("x".into(), field("x"))],
    };
    assert!(engine.prepare_bound(&no_source, &Bindings::new()).is_err());
}

#[test]
fn transaction_locals_are_immutable_and_later_steps_read_staged_writes() {
    let (_, admin, engine) = fixture();
    let tx = transaction(
        false,
        vec![
            let_step(
                "saved",
                returning(
                    one_update(binary(field("x"), Arithmetic::Add, int(1))),
                    vec![
                        projection(field("x"), Some("value")),
                        projection(field("narrow"), Some("maybe")),
                    ],
                ),
            ),
            TransactionStep::Execute(one_update(binary(field("x"), Arithmetic::Add, int(1)))),
            let_step("fresh", {
                let mut r = read(None);
                if let Request::Read { filter, .. } = &mut r {
                    *filter = Some(eq(field("id"), int(1)));
                }
                r
            }),
            TransactionStep::Execute(Request::InsertExpressions {
                table: "outbox".into(),
                values: vec![
                    ("id".into(), int(77)),
                    ("x".into(), local("saved", "value")),
                ],
            }),
            TransactionStep::Require(eq(changed("saved"), int(1))),
        ],
        Some(vec![
            EmitExpr::Scalar(status("saved")),
            EmitExpr::Scalar(changed("saved")),
            EmitExpr::Row("saved".into()),
            EmitExpr::Rows("fresh".into()),
            EmitExpr::Scalar(local("saved", "maybe")),
            EmitExpr::Scalar(binary(local("saved", "maybe"), Arithmetic::Add, int(1))),
            EmitExpr::Scalar(value(V::Null)),
        ]),
    );
    let before = digest(&admin);
    let bound = engine
        .prepare_transaction_bound(&tx, &Bindings::new())
        .unwrap();
    assert_eq!(digest(&admin), before);
    assert_eq!(
        bound
            .description()
            .tables
            .iter()
            .map(|t| t.name.as_str())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["items", "outbox"])
    );
    assert!(matches!(
        &bound.description().emitted[4],
        EmitDescription::Scalar {
            data_type: Some(DataType::SmallInt),
            nullable: true
        }
    ));
    assert!(matches!(
        &bound.description().emitted[5],
        EmitDescription::Scalar {
            data_type: Some(DataType::BigInt),
            nullable: true
        }
    ));
    assert!(matches!(
        &bound.description().emitted[6],
        EmitDescription::Scalar {
            data_type: None,
            nullable: true
        }
    ));
    let outcome = engine.execute_transaction_bound(&bound).unwrap();
    assert_eq!(
        outcome.emitted[0],
        EmittedValue::Scalar(V::Text("success".into()))
    );
    assert_eq!(outcome.emitted[1], EmittedValue::Scalar(V::Int(1)));
    let EmittedValue::Row {
        row: saved,
        columns,
    } = &outcome.emitted[2]
    else {
        panic!("singular row")
    };
    assert_eq!(
        columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
        ["value", "maybe"]
    );
    assert_eq!(saved.values, [V::Int(11), V::Null]);
    let EmittedValue::Rows {
        rows: fresh,
        complete,
        ..
    } = &outcome.emitted[3]
    else {
        panic!("read rows")
    };
    assert!(*complete);
    assert_eq!(fresh[0].values, [V::Int(1), V::Int(12)]);
    assert!(fresh[0].version > saved.version);
    assert_eq!(fresh[0].version, rows(&admin)[0].version);
    for value in &outcome.emitted[4..] {
        assert_eq!(*value, EmittedValue::Scalar(V::Null));
    }
    assert_eq!(
        admin
            .sql_session()
            .execute("SELECT x FROM outbox WHERE id=77", &[], None)
            .unwrap()
            .rows[0]
            .values,
        [V::Int(11)]
    );
    let noop = transaction(
        false,
        vec![let_step("noop", one_update(field("x")))],
        Some(vec![
            EmitExpr::Scalar(changed("noop")),
            EmitExpr::Scalar(status("noop")),
        ]),
    );
    let outcome = engine.execute_transaction(&noop, &Bindings::new()).unwrap();
    assert_eq!(
        outcome.emitted,
        [
            EmittedValue::Scalar(V::Int(0)),
            EmittedValue::Scalar(V::Text("success".into()))
        ]
    );
}

#[test]
fn transaction_read_only_count_and_rows_preserve_snapshot_and_completeness() {
    let (_, admin, engine) = database("CREATE TABLE items (id BIGINT PRIMARY KEY,x BIGINT)");
    let mut sql = admin.sql_session();
    sql.execute("BEGIN", &[], None).unwrap();
    for id in 1..=101 {
        sql.execute("INSERT INTO items VALUES($1,$1)", &[V::Int(id)], None)
            .unwrap();
    }
    sql.execute("COMMIT", &[], None).unwrap();
    for (limit, empty, expected, complete) in [
        (None, false, 100, false),
        (Some(0), false, 0, false),
        (Some(0), true, 0, true),
    ] {
        let mut request = read(limit);
        if empty && let Request::Read { filter, .. } = &mut request {
            *filter = Some(value(V::Bool(false)));
        }
        let tx = transaction(
            true,
            vec![let_step("page", request)],
            Some(vec![
                EmitExpr::Scalar(count("page")),
                EmitExpr::Rows("page".into()),
            ]),
        );
        let before = digest(&admin);
        let bound = engine
            .prepare_transaction_bound(&tx, &Bindings::new())
            .unwrap();
        assert!(bound.description().read_only);
        let result = engine.execute_transaction_bound(&bound).unwrap();
        assert_eq!(result.emitted[0], EmittedValue::Scalar(V::Int(expected)));
        let EmittedValue::Rows {
            rows,
            complete: actual,
            ..
        } = &result.emitted[1]
        else {
            panic!("rows")
        };
        assert_eq!(rows.len(), expected as usize);
        assert_eq!(*actual, complete);
        assert_eq!(digest(&admin), before);
    }
    let write = returning(one_update(int(1)), vec![]);
    let tx = transaction(
        true,
        vec![
            TransactionStep::Require(eq(binary(int(1), Arithmetic::Divide, int(0)), int(0))),
            TransactionStep::Execute(write),
        ],
        None,
    );
    static_failure(&admin, &engine, &tx, Some(Error::Unsupported));
}

#[test]
fn transaction_uses_one_session_and_one_publication_without_leaking_slots() {
    let (_, admin, engine) = fixture();
    let mut held = Vec::new();
    for _ in 0..MAX_TRANSACTIONS - 1 {
        let mut s = admin.sql_session();
        s.execute("BEGIN", &[], None).unwrap();
        held.push(s);
    }
    let before_decision = Arc::new(AtomicUsize::new(0));
    let committed = Arc::new(AtomicUsize::new(0));
    let d = before_decision.clone();
    let c = committed.clone();
    admin.set_decision_hook(Some(Arc::new(move |point| {
        if point == DecisionPoint::BeforeDecision {
            d.fetch_add(1, Ordering::SeqCst);
        }
        if point == DecisionPoint::Committed {
            c.fetch_add(1, Ordering::SeqCst);
        }
    })));
    let tx = transaction(
        false,
        vec![
            TransactionStep::Execute(one_update(binary(field("x"), Arithmetic::Add, int(1)))),
            TransactionStep::Execute(one_update(binary(field("x"), Arithmetic::Add, int(1)))),
        ],
        None,
    );
    let bound = engine
        .prepare_transaction_bound(&tx, &Bindings::new())
        .unwrap();
    let result = engine.execute_transaction_bound(&bound).unwrap();
    assert!(result.emitted.is_empty());
    assert_eq!(before_decision.load(Ordering::SeqCst), 1);
    assert_eq!(committed.load(Ordering::SeqCst), 1);
    let readonly = transaction(
        true,
        vec![
            TransactionStep::Execute(read(None)),
            TransactionStep::Require(yes()),
        ],
        None,
    );
    let before = digest(&admin);
    engine
        .execute_transaction(&readonly, &Bindings::new())
        .unwrap();
    assert_eq!(digest(&admin), before);
    assert_eq!(committed.load(Ordering::SeqCst), 1);
    let failed = transaction(
        false,
        vec![
            TransactionStep::Execute(one_update(int(99))),
            TransactionStep::Require(value(V::Null)),
        ],
        None,
    );
    assert_eq!(
        engine.execute_transaction(&failed, &Bindings::new()),
        Err(Error::RequirementFailed)
    );
    assert_eq!(committed.load(Ordering::SeqCst), 1);
    assert_eq!(digest(&admin), before);
    admin.set_decision_hook(None);
    let mut last = admin.sql_session();
    last.execute("BEGIN", &[], None).unwrap();
    last.rollback();
    drop(held);
}

#[test]
fn transaction_late_failure_rolls_back_rows_generation_and_version_allocator() {
    for (require, emit, expected) in [
        (Some(value(V::Bool(false))), None, Error::RequirementFailed),
        (Some(value(V::Null)), None, Error::RequirementFailed),
        (
            None,
            Some(vec![EmitExpr::Scalar(binary(
                int(1),
                Arithmetic::Divide,
                int(0),
            ))]),
            Error::DivisionByZero,
        ),
    ] {
        let (_, admin, engine) = fixture();
        let maximum = rows(&admin).iter().map(|r| r.version).max().unwrap();
        let mut steps = vec![
            TransactionStep::Execute(insert(3)),
            TransactionStep::Execute(one_update(int(99))),
        ];
        if let Some(require) = require {
            steps.push(TransactionStep::Require(require));
        }
        runtime_failure(
            &admin,
            &engine,
            &transaction(false, steps, emit),
            &Bindings::new(),
            expected,
        );
        admin
            .sql_session()
            .execute(
                "INSERT INTO items (id,x,state) VALUES(3,30,'ready')",
                &[],
                None,
            )
            .unwrap();
        assert_eq!(rows(&admin)[2].version, maximum + 1);
    }
    let (_, admin, engine) = fixture();
    let duplicate = transaction(
        false,
        vec![
            TransactionStep::Execute(insert(3)),
            TransactionStep::Execute(insert(1)),
        ],
        None,
    );
    runtime_failure(
        &admin,
        &engine,
        &duplicate,
        &Bindings::new(),
        Error::Duplicate,
    );
}

fn large_fixture(bytes: usize, rows: i64) -> (PathBuf, Admin, Engine) {
    let (path, admin, engine) =
        database("CREATE TABLE items (id BIGINT PRIMARY KEY,x BIGINT,src TEXT)");
    let mut sql = admin.sql_session();
    sql.execute("BEGIN", &[], None).unwrap();
    for id in 1..=rows {
        sql.execute(
            "INSERT INTO items VALUES($1,0,$2)",
            &[V::Int(id), V::Text("x".repeat(bytes))],
            None,
        )
        .unwrap();
    }
    sql.execute("COMMIT", &[], None).unwrap();
    (path, admin, engine)
}
fn half_megabyte_local(name: &str) -> TransactionStep {
    let output = (0..32)
        .map(|i| Projection {
            expression: field("src"),
            alias: Some(format!("p{i}")),
        })
        .collect();
    let_step(
        name,
        returning(
            one_update(binary(field("x"), Arithmetic::Add, int(1))),
            output,
        ),
    )
}

#[test]
fn transaction_retained_and_emitted_payload_budgets_are_independent_and_exact() {
    assert_eq!(MAX_RETAINED_LOCAL_BYTES, 1024 * 1024);
    assert_eq!(MAX_EMIT_BYTES, 1024 * 1024);
    let (_, admin, engine) = large_fixture(16384, 1);
    let two = transaction(
        false,
        vec![half_megabyte_local("a"), half_megabyte_local("b")],
        None,
    );
    engine.execute_transaction(&two, &Bindings::new()).unwrap();
    let mut three = two.clone();
    three.steps.push(let_step(
        "c",
        returning(
            one_update(binary(field("x"), Arithmetic::Add, int(1))),
            vec![projection(value(V::Text("z".into())), Some("z"))],
        ),
    ));
    runtime_failure(&admin, &engine, &three, &Bindings::new(), Error::Limit);
    let emitted = transaction(
        false,
        vec![half_megabyte_local("a")],
        Some(vec![EmitExpr::Row("a".into()), EmitExpr::Row("a".into())]),
    );
    let result = engine
        .execute_transaction(&emitted, &Bindings::new())
        .unwrap();
    assert_eq!(result.emitted.len(), 2);
    let mut over = emitted;
    over.emit
        .as_mut()
        .unwrap()
        .push(EmitExpr::Scalar(value(V::Text("z".into()))));
    runtime_failure(&admin, &engine, &over, &Bindings::new(), Error::Limit);
}

#[test]
fn transaction_frozen_and_runtime_local_occurrences_share_one_binding_budget() {
    let (_, admin, engine) = large_fixture(32758, 1);
    let make = |tag: &str| {
        transaction(
            false,
            vec![
                let_step(
                    "a",
                    returning(
                        one_update(binary(field("x"), Arithmetic::Add, int(1))),
                        vec![projection(field("src"), Some("p"))],
                    ),
                ),
                TransactionStep::Require(eq(local("a", "p"), local("a", "p"))),
            ],
            Some(vec![EmitExpr::Scalar(value(V::Text(tag.into())))]),
        )
    };
    assert_eq!(18 + 2 + 2 * 32758, MAX_BOUND_VALUE_BYTES);
    let exact = make("zz");
    engine
        .prepare_transaction_bound(&exact, &Bindings::new())
        .unwrap();
    engine
        .execute_transaction(&exact, &Bindings::new())
        .unwrap();
    runtime_failure(
        &admin,
        &engine,
        &make("zzz"),
        &Bindings::new(),
        Error::Limit,
    );
    let (_, _admin, engine) = large_fixture(32758, 8);
    let filtered = Request::Read {
        table: "items".into(),
        filter: Some(eq(field("src"), local("a", "p"))),
        order: vec![],
        limit: Some(ValueExpr::Literal(V::Int(0))),
        projection: vec![],
    };
    let tx = transaction(
        false,
        vec![
            let_step(
                "a",
                returning(
                    one_update(field("x")),
                    vec![projection(field("src"), Some("p"))],
                ),
            ),
            TransactionStep::Execute(filtered.clone()),
        ],
        None,
    );
    assert_eq!(
        engine
            .execute_transaction(&tx, &Bindings::new())
            .unwrap()
            .summaries
            .last()
            .unwrap()
            .matched,
        8
    );
    // Each explicit parameter reference counts across steps, even though the
    // supplied map contains only one copy and every request alone would fit.
    let request = Request::ReadExpressions {
        table: "items".into(),
        filter: None,
        order: vec![],
        limit: None,
        projection: vec![projection(parameter("p"), Some("p"))],
    };
    let binding = Bindings::from([("p".into(), V::Text("p".repeat(16384)))]);
    let (_, small_admin, small_engine) = large_fixture(1, 1);
    let mut static_tx = transaction(
        true,
        (0..4)
            .map(|i| let_step(&format!("r{i}"), request.clone()))
            .collect(),
        None,
    );
    small_engine
        .prepare_transaction_bound(&static_tx, &binding)
        .unwrap();
    small_engine
        .execute_transaction(&static_tx, &binding)
        .unwrap();
    static_tx.steps.push(let_step("extra", request));
    let before = digest(&small_admin);
    assert_eq!(
        small_engine
            .prepare_transaction_bound(&static_tx, &binding)
            .err(),
        Some(Error::Limit)
    );
    assert_eq!(
        small_engine.execute_transaction(&static_tx, &binding),
        Err(Error::Limit)
    );
    assert_eq!(digest(&small_admin), before);
}

fn balanced_boolean(mut expressions: Vec<Expr>) -> Expr {
    while expressions.len() > 1 {
        let mut next = Vec::new();
        let mut it = expressions.into_iter();
        while let Some(a) = it.next() {
            next.push(match it.next() {
                Some(b) => Expr::And(Box::new(a), Box::new(b)),
                None => a,
            });
        }
        expressions = next;
    }
    expressions.pop().unwrap()
}

#[test]
fn transaction_nodes_and_input_admission_are_shared_across_all_roots() {
    let (_, admin, engine) = database("CREATE TABLE items (id BIGINT PRIMARY KEY,flag BOOLEAN)");
    let filter = balanced_boolean((0..1024).map(|_| field("flag")).collect());
    let read = Request::Read {
        table: "items".into(),
        filter: Some(filter),
        order: vec![],
        limit: None,
        projection: vec!["id".into()],
    };
    let mut exact = transaction(
        true,
        vec![
            TransactionStep::Execute(read.clone()),
            TransactionStep::Execute(read),
            TransactionStep::Require(yes()),
        ],
        Some(vec![EmitExpr::Scalar(yes())]),
    );
    assert_eq!(2047 * 2 + 2, MAX_EXPRESSION_NODES);
    assert!(serde_json::to_vec(&exact).unwrap().len() < MAX_INPUT);
    engine
        .prepare_transaction_bound(&exact, &Bindings::new())
        .unwrap();
    engine
        .execute_transaction(&exact, &Bindings::new())
        .unwrap();
    exact.steps.push(TransactionStep::Require(yes()));
    static_failure(&admin, &engine, &exact, Some(Error::Limit));
    let mut deep = yes();
    for _ in 0..=MAX_DEPTH {
        deep = Expr::Not(Box::new(deep));
    }
    for tx in [
        transaction(true, vec![TransactionStep::Require(deep.clone())], None),
        transaction(true, vec![], Some(vec![EmitExpr::Scalar(deep)])),
        transaction(
            true,
            vec![],
            Some(vec![EmitExpr::Scalar(value(V::Text(
                "x".repeat(MAX_INPUT),
            )))]),
        ),
    ] {
        static_failure(&admin, &engine, &tx, Some(Error::Limit));
    }
    let tx = transaction(true, vec![TransactionStep::Require(yes())], None);
    for bindings in [
        (0..=MAX_BINDINGS)
            .map(|i| (format!("p{i}"), V::Int(0)))
            .collect::<Bindings>(),
        Bindings::from([(
            "unused".into(),
            V::Text("x".repeat(MAX_BOUND_VALUE_BYTES + 1)),
        )]),
    ] {
        let before = digest(&admin);
        assert_eq!(
            engine.prepare_transaction_bound(&tx, &bindings).err(),
            Some(Error::Limit)
        );
        assert_eq!(
            engine.execute_transaction(&tx, &bindings),
            Err(Error::Limit)
        );
        assert_eq!(digest(&admin), before);
    }
}

#[test]
fn transaction_prepared_multi_table_fences_freeze_parameters_not_rows() {
    let (_, admin, engine) = fixture();
    let tx = transaction(
        false,
        vec![
            let_step(
                "a",
                returning(
                    one_update(binary(field("x"), Arithmetic::Add, parameter("delta"))),
                    vec![projection(field("x"), None)],
                ),
            ),
            TransactionStep::Execute(Request::InsertExpressions {
                table: "outbox".into(),
                values: vec![
                    ("id".into(), parameter("id")),
                    ("x".into(), local("a", "x")),
                ],
            }),
        ],
        Some(vec![EmitExpr::Row("a".into())]),
    );
    let mut bindings = Bindings::from([("delta".into(), V::Int(2)), ("id".into(), V::Int(77))]);
    let before = digest(&admin);
    let bound = engine.prepare_transaction_bound(&tx, &bindings).unwrap();
    assert_eq!(digest(&admin), before);
    assert_eq!(bound.description().tables.len(), 2);
    bindings.insert("delta".into(), V::Int(99));
    bindings.insert("id".into(), V::Int(88));
    admin
        .sql_session()
        .execute("UPDATE items SET x=40 WHERE id=1", &[], None)
        .unwrap();
    let result = engine.execute_transaction_bound(&bound).unwrap();
    let EmittedValue::Row { row, .. } = &result.emitted[0] else {
        panic!("row")
    };
    assert_eq!(row.values, [V::Int(42)]);
    assert_eq!(
        admin
            .sql_session()
            .execute("SELECT x FROM outbox WHERE id=77", &[], None)
            .unwrap()
            .rows[0]
            .values,
        [V::Int(42)]
    );
    admin
        .sql_session()
        .execute("CREATE TABLE newer (id BIGINT PRIMARY KEY)", &[], None)
        .unwrap();
    let before = digest(&admin);
    assert_eq!(
        engine.execute_transaction_bound(&bound),
        Err(Error::Core("Sql(StaleSchema)".into()))
    );
    assert_eq!(digest(&admin), before);
    let fresh = engine.prepare_transaction_bound(&tx, &bindings).unwrap();
    let exported = common::fresh("transaction-domain-export");
    admin.export(&exported).unwrap();
    let restored = Admin::restore(
        exported,
        common::fresh("transaction-restored"),
        Limits::default(),
    )
    .unwrap();
    assert_eq!(admin.catalog().unwrap(), restored.catalog().unwrap());
    let before = digest(&restored);
    assert_eq!(
        Engine::new(restored.clone()).execute_transaction_bound(&fresh),
        Err(Error::Core("Sql(StaleSchema)".into()))
    );
    assert_eq!(digest(&restored), before);
}

#[test]
fn transaction_cancellation_and_unknown_never_return_speculative_emits() {
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
        let tx = transaction(
            false,
            vec![
                let_step("a", returning(one_update(int(99)), vec![])),
                TransactionStep::Execute(insert(3)),
            ],
            Some(vec![EmitExpr::Row("a".into())]),
        );
        let result =
            Cancellation::default().scope(|| engine.execute_transaction(&tx, &Bindings::new()));
        admin.set_decision_hook(None);
        if wins {
            assert_eq!(result, Err(Error::Core(CoreError::Cancelled.to_string())));
            assert_eq!(digest(&admin), before);
        } else {
            assert_eq!(result.unwrap().emitted.len(), 1);
            assert_eq!(rows(&admin).len(), 3);
            assert_eq!(rows(&admin)[0].values[1], V::Int(99));
        }
    }
    for (point, persisted) in [
        (CrashPoint::ErrorBeforeCommit, false),
        (CrashPoint::ErrorAfterCommit, true),
    ] {
        let (path, admin, engine) = fixture();
        let tx = transaction(
            false,
            vec![
                let_step("a", returning(one_update(int(99)), vec![])),
                TransactionStep::Execute(insert(3)),
            ],
            Some(vec![EmitExpr::Row("a".into())]),
        );
        admin.set_crash_point(point).unwrap();
        assert_eq!(
            engine.execute_transaction(&tx, &Bindings::new()),
            Err(Error::Unknown)
        );
        drop(engine);
        drop(admin);
        let reopened = Admin::open(path, Limits::default()).unwrap();
        assert_eq!(rows(&reopened).len(), if persisted { 3 } else { 2 });
        assert_eq!(
            rows(&reopened)[0].values[1],
            V::Int(if persisted { 99 } else { 10 })
        );
    }
}

#[test]
fn transaction_item_emit_and_unsupported_claim_boundaries_are_finite() {
    let (_, admin, engine) = fixture();
    let exact = transaction(
        true,
        (0..MAX_TRANSACTION_ITEMS)
            .map(|_| TransactionStep::Require(yes()))
            .collect(),
        None,
    );
    engine
        .prepare_transaction_bound(&exact, &Bindings::new())
        .unwrap();
    engine
        .execute_transaction(&exact, &Bindings::new())
        .unwrap();
    let mut over = exact.clone();
    over.emit = Some(vec![EmitExpr::Scalar(int(1))]);
    static_failure(&admin, &engine, &over, Some(Error::Limit));
    let emitted = transaction(
        true,
        vec![],
        Some(
            (0..MAX_EMIT_VALUES)
                .map(|_| EmitExpr::Scalar(int(1)))
                .collect(),
        ),
    );
    assert_eq!(
        engine
            .execute_transaction(&emitted, &Bindings::new())
            .unwrap()
            .emitted
            .len(),
        MAX_EMIT_VALUES
    );
    let mut over = emitted;
    over.emit.as_mut().unwrap().push(EmitExpr::Scalar(int(1)));
    static_failure(&admin, &engine, &over, Some(Error::Limit));
    for tx in [
        transaction(false, vec![], None),
        transaction(false, vec![], Some(vec![])),
    ] {
        static_failure(&admin, &engine, &tx, None);
    }
    let claim = update(
        yes(),
        Intent::Claim {
            order: vec![Sort {
                field: "id".into(),
                descending: false,
            }],
        },
        vec![("x", int(1))],
    );
    for request in [claim.clone(), returning(claim, vec![])] {
        static_failure(
            &admin,
            &engine,
            &transaction(false, vec![TransactionStep::Execute(request)], None),
            Some(Error::Unsupported),
        );
    }
}

#[test]
fn insert_expressions_and_guarded_bulk_preserve_types_caps_and_atomicity() {
    let (_, admin, engine) = fixture();
    let insert = Request::InsertExpressions {
        table: "items".into(),
        values: vec![
            ("id".into(), int(3)),
            ("x".into(), binary(int(2), Arithmetic::Add, int(3))),
            ("state".into(), value(V::Text("ready".into()))),
        ],
    };
    assert_eq!(
        engine.execute(&insert, &Bindings::new()).unwrap().changed,
        1
    );
    assert_eq!(rows(&admin)[2].values[1], V::Int(5));
    let (_, admin, engine) = fixture();
    let make = |cap, guard| {
        let mut request = update(
            yes(),
            many(cap),
            vec![("x", binary(field("x"), Arithmetic::Add, int(1)))],
        );
        if let Request::UpdateExpressions { guard: slot, .. } = &mut request {
            *slot = Some(guard);
        }
        request
    };
    for (cap, guard, expected) in [
        (1, value(V::Bool(false)), Error::TooManyMatches),
        (2, value(V::Bool(false)), Error::ConditionFailed),
        (2, value(V::Null), Error::ConditionFailed),
        (
            2,
            Expr::Compare(
                Box::new(binary(
                    int(10),
                    Arithmetic::Divide,
                    binary(int(2), Arithmetic::Subtract, field("id")),
                )),
                Compare::Gt,
                Box::new(int(0)),
            ),
            Error::DivisionByZero,
        ),
    ] {
        let tx = transaction(
            false,
            vec![TransactionStep::Execute(make(cap, guard))],
            None,
        );
        runtime_failure(&admin, &engine, &tx, &Bindings::new(), expected);
    }
    let tx = transaction(
        false,
        vec![
            TransactionStep::Execute(make(2, yes())),
            TransactionStep::Execute(make(2, yes())),
        ],
        None,
    );
    let result = engine.execute_transaction(&tx, &Bindings::new()).unwrap();
    assert_eq!(
        result
            .summaries
            .iter()
            .map(|s| s.changed)
            .collect::<Vec<_>>(),
        [2, 2]
    );
    assert_eq!(rows(&admin)[0].values[1], V::Int(12));
    let deletion = Request::Delete {
        table: "items".into(),
        filter: yes(),
        guard: Some(value(V::Null)),
        cap: Some(ValueExpr::Literal(V::Int(2))),
    };
    let before = digest(&admin);
    assert_eq!(
        engine.execute(&deletion, &Bindings::new()),
        Err(Error::ConditionFailed)
    );
    assert_eq!(digest(&admin), before);
    let deletion = Request::Delete {
        table: "items".into(),
        filter: yes(),
        guard: Some(yes()),
        cap: Some(ValueExpr::Literal(V::Int(2))),
    };
    assert_eq!(
        engine.execute(&deletion, &Bindings::new()).unwrap().changed,
        2
    );
}
