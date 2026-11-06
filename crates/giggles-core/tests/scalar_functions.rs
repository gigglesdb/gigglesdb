mod common;

use giggles_core::{
    Admin, Limits,
    operations::{
        Arithmetic, Bindings, Compare, EmitDescription, EmitExpr, EmittedValue, Engine, Error,
        Expr, Intent, LocalRef, MAX_BOUND_VALUE_BYTES, MAX_DEPTH, MAX_EXPRESSION_NODES,
        MAX_FUNCTION_ARGUMENTS, MAX_INPUT, Projection, Request, ScalarFunction, Transaction,
        TransactionStep, ValueExpr,
    },
    relational::{DataType, Value as V},
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
fn call(function: ScalarFunction, arguments: Vec<Expr>) -> Expr {
    Expr::Call {
        function,
        arguments,
    }
}
fn abs(e: Expr) -> Expr {
    call(ScalarFunction::Abs, vec![e])
}
fn length(e: Expr) -> Expr {
    call(ScalarFunction::Length, vec![e])
}
fn coalesce(args: Vec<Expr>) -> Expr {
    call(ScalarFunction::Coalesce, args)
}
fn binary(a: Expr, op: Arithmetic, b: Expr) -> Expr {
    Expr::Arithmetic(Box::new(a), op, Box::new(b))
}
fn equal(a: Expr, b: Expr) -> Expr {
    Expr::Compare(Box::new(a), Compare::Eq, Box::new(b))
}
fn bad_division() -> Expr {
    binary(int(1), Arithmetic::Divide, int(0))
}
fn projection(e: Expr) -> Projection {
    Projection {
        expression: e,
        alias: Some("out".into()),
    }
}
fn read(e: Expr) -> Request {
    Request::ReadExpressions {
        table: "items".into(),
        filter: None,
        order: vec![],
        limit: None,
        projection: vec![projection(e)],
    }
}
fn returning(request: Request, e: Expr) -> Request {
    Request::Returning {
        request: Box::new(request),
        projection: vec![projection(e)],
    }
}
fn update(e: Expr, intent: Intent) -> Request {
    Request::UpdateExpressions {
        table: "items".into(),
        filter: value(V::Bool(true)),
        guard: None,
        values: vec![("n".into(), e)],
        intent,
    }
}
fn fixture(populated: bool) -> (Admin, Engine) {
    let admin = Admin::create(common::fresh("scalar-functions"), Limits::default()).unwrap();
    admin.sql_session().execute("CREATE TABLE items (id BIGINT PRIMARY KEY,n BIGINT,b BOOLEAN,txt TEXT,raw BYTES,small SMALLINT,required TEXT NOT NULL)",&[],None).unwrap();
    if populated {
        admin
            .sql_session()
            .execute(
                "INSERT INTO items VALUES(1,-5,true,'x',NULL,NULL,'ready')",
                &[],
                None,
            )
            .unwrap();
    }
    (admin.clone(), Engine::new(admin))
}
fn digest(admin: &Admin) -> [u8; 32] {
    admin
        .export(common::fresh("scalar-functions-export"))
        .unwrap()
}
fn result(engine: &Engine, e: Expr) -> Result<V, Error> {
    engine
        .execute(&read(e), &Bindings::new())
        .map(|out| out.rows[0].values[0].clone())
}
fn static_failure(
    admin: &Admin,
    engine: &Engine,
    request: &Request,
    bindings: &Bindings,
    expected: Option<Error>,
) {
    let before = digest(admin);
    let actual = engine.prepare_bound(request, bindings).unwrap_err();
    assert_eq!(engine.execute(request, bindings), Err(actual.clone()));
    if let Some(expected) = expected {
        assert_eq!(actual, expected);
    }
    assert_eq!(digest(admin), before);
}
fn runtime_failure(admin: &Admin, engine: &Engine, request: &Request, expected: Error) {
    let before = digest(admin);
    let bound = engine.prepare_bound(request, &Bindings::new()).unwrap();
    assert_eq!(engine.execute_bound(&bound), Err(expected.clone()));
    assert_eq!(engine.execute(request, &Bindings::new()), Err(expected));
    assert_eq!(digest(admin), before);
}

#[test]
fn scalar_functions_match_independent_integer_and_unicode_oracles() {
    let (admin, engine) = fixture(true);
    for n in [
        i64::MIN,
        i64::MIN + 1,
        -4_294_967_296,
        -32769,
        -32768,
        -1,
        0,
        1,
        32767,
        4_294_967_296,
        i64::MAX,
    ] {
        let magnitude = i128::from(n).abs();
        if magnitude > i128::from(i64::MAX) {
            runtime_failure(&admin, &engine, &read(abs(int(n))), Error::Overflow);
        } else {
            assert_eq!(result(&engine, abs(int(n))), Ok(V::Int(magnitude as i64)));
        }
    }
    for (text, scalars, bytes) in [
        ("", 0, 0),
        ("\0", 1, 1),
        ("\u{e9}", 1, 2),
        ("e\u{301}", 2, 3),
        ("\u{1f980}", 1, 4),
        ("\u{1f1fa}\u{1f1f8}", 2, 8),
        ("\u{1f469}\u{200d}\u{1f4bb}", 3, 11),
        ("\r\n", 2, 2),
    ] {
        assert_eq!(
            result(&engine, length(value(V::Text(text.into())))),
            Ok(V::Int(scalars)),
            "{text:?}"
        );
        assert_eq!(
            result(&engine, length(value(V::Bytes(text.as_bytes().into())))),
            Ok(V::Int(bytes)),
            "{text:?}"
        );
    }
    assert_eq!(
        result(&engine, length(value(V::Bytes(vec![0, 255, 128])))),
        Ok(V::Int(3))
    );
    assert_eq!(result(&engine, abs(value(V::Null))), Ok(V::Null));
    assert_eq!(result(&engine, length(value(V::Null))), Ok(V::Null));
    for (first, fallback) in [
        (V::Int(0), V::Int(1)),
        (V::Bool(false), V::Bool(true)),
        (V::Text("".into()), V::Text("fallback".into())),
        (V::Bytes(vec![]), V::Bytes(vec![1])),
    ] {
        assert_eq!(
            result(
                &engine,
                coalesce(vec![value(first.clone()), value(fallback)])
            ),
            Ok(first)
        );
    }
}

#[test]
fn scalar_function_binding_checks_every_argument_before_rows() {
    for populated in [false, true] {
        let (admin, engine) = fixture(populated);
        for expression in [
            coalesce(vec![]),
            coalesce(vec![int(1)]),
            call(ScalarFunction::Abs, vec![]),
            call(ScalarFunction::Abs, vec![int(1), field("missing")]),
            call(ScalarFunction::Length, vec![value(V::Null), value(V::Null)]),
            abs(value(V::Bool(true))),
            abs(value(V::Text("1".into()))),
            length(int(1)),
            length(value(V::Bool(false))),
            coalesce(vec![int(1), value(V::Text("wrong".into()))]),
            coalesce(vec![value(V::Text("x".into())), value(V::Bytes(vec![]))]),
        ] {
            static_failure(
                &admin,
                &engine,
                &read(expression),
                &Bindings::new(),
                Some(Error::Type),
            );
        }
        for expression in [
            coalesce(vec![int(1), field("missing")]),
            coalesce(vec![int(1), parameter("missing")]),
            coalesce(vec![
                int(1),
                Expr::Local(LocalRef::Changed {
                    local: "missing".into(),
                }),
            ]),
        ] {
            static_failure(&admin, &engine, &read(expression), &Bindings::new(), None);
        }
        let request = read(coalesce(vec![int(1), parameter("unused_result")]));
        static_failure(
            &admin,
            &engine,
            &request,
            &Bindings::from([("unused_result".into(), V::Text("wrong type".into()))]),
            Some(Error::Type),
        );
        static_failure(
            &admin,
            &engine,
            &read(coalesce(vec![int(1), int(2)])),
            &Bindings::from([("extra".into(), V::Int(1))]),
            Some(Error::Bind("parameters".into())),
        );
    }
    assert!(
        serde_json::from_value::<Expr>(
            serde_json::json!({"Call":{"function":"Round","arguments":[]}})
        )
        .is_err()
    );
}

#[test]
fn scalar_function_lazy_results_preserve_eager_boolean_rules() {
    let (admin, engine) = fixture(true);
    for expression in [
        coalesce(vec![int(7), bad_division()]),
        coalesce(vec![value(V::Null), int(7), abs(int(i64::MIN))]),
        coalesce(vec![int(7), abs(int(i64::MIN)), bad_division()]),
    ] {
        assert_eq!(result(&engine, expression), Ok(V::Int(7)));
    }
    for (expression, expected) in [
        (
            coalesce(vec![value(V::Null), bad_division(), int(7)]),
            Error::DivisionByZero,
        ),
        (
            coalesce(vec![bad_division(), int(7)]),
            Error::DivisionByZero,
        ),
        (
            coalesce(vec![value(V::Null), abs(int(i64::MIN)), int(7)]),
            Error::Overflow,
        ),
    ] {
        runtime_failure(&admin, &engine, &read(expression), expected);
    }
    let failure = equal(abs(int(i64::MIN)), int(1));
    assert_eq!(
        result(
            &engine,
            coalesce(vec![value(V::Bool(true)), failure.clone()])
        ),
        Ok(V::Bool(true))
    );
    runtime_failure(
        &admin,
        &engine,
        &read(Expr::Or(
            Box::new(value(V::Bool(true))),
            Box::new(failure.clone()),
        )),
        Error::Overflow,
    );
    runtime_failure(
        &admin,
        &engine,
        &read(Expr::And(
            Box::new(value(V::Bool(false))),
            Box::new(failure),
        )),
        Error::Overflow,
    );
    assert_eq!(
        result(
            &engine,
            length(coalesce(vec![
                value(V::Null),
                value(V::Text("\u{1f980}".into()))
            ]))
        ),
        Ok(V::Int(1))
    );
    assert_eq!(
        result(&engine, abs(length(value(V::Text("e\u{301}".into()))))),
        Ok(V::Int(2))
    );
}

#[test]
fn scalar_function_metadata_keeps_typed_and_untyped_nulls() {
    let (admin, engine) = fixture(true);
    for (expression, data_type) in [
        (abs(value(V::Null)), DataType::BigInt),
        (length(value(V::Null)), DataType::BigInt),
        (
            coalesce(vec![abs(value(V::Null)), length(value(V::Null))]),
            DataType::BigInt,
        ),
        (length(field("required")), DataType::BigInt),
        (coalesce(vec![int(1), int(2)]), DataType::BigInt),
        (
            coalesce(vec![value(V::Bool(false)), value(V::Bool(true))]),
            DataType::Boolean,
        ),
        (
            coalesce(vec![value(V::Text("x".into())), value(V::Text("y".into()))]),
            DataType::Text,
        ),
        (
            coalesce(vec![value(V::Bytes(vec![])), value(V::Bytes(vec![1]))]),
            DataType::Bytes,
        ),
        (coalesce(vec![field("small"), int(0)]), DataType::BigInt),
    ] {
        let bound = engine
            .prepare_bound(&read(expression), &Bindings::new())
            .unwrap();
        let c = &bound.description().columns[0];
        assert_eq!(c.id, 0);
        assert_eq!(c.data_type, data_type);
        assert!(
            !c.not_null,
            "calls intentionally keep conservative nullable metadata"
        );
    }
    let typed_null = coalesce(vec![abs(value(V::Null)), length(value(V::Null))]);
    assert_eq!(result(&engine, typed_null.clone()), Ok(V::Null));
    let all_null = coalesce(vec![value(V::Null), value(V::Null)]);
    static_failure(
        &admin,
        &engine,
        &read(all_null.clone()),
        &Bindings::new(),
        Some(Error::Type),
    );
    let filtered = Request::Read {
        table: "items".into(),
        filter: Some(all_null.clone()),
        order: vec![],
        limit: None,
        projection: vec![],
    };
    assert!(
        engine
            .execute(&filtered, &Bindings::new())
            .unwrap()
            .rows
            .is_empty()
    );
    let tx = Transaction {
        read_only: true,
        steps: vec![],
        emit: Some(vec![
            EmitExpr::Scalar(all_null.clone()),
            EmitExpr::Scalar(abs(value(V::Null))),
            EmitExpr::Scalar(length(value(V::Null))),
            EmitExpr::Scalar(typed_null),
        ]),
    };
    let bound = engine
        .prepare_transaction_bound(&tx, &Bindings::new())
        .unwrap();
    assert_eq!(
        bound.description().emitted,
        [
            EmitDescription::Scalar {
                data_type: None,
                nullable: true
            },
            EmitDescription::Scalar {
                data_type: Some(DataType::BigInt),
                nullable: true
            },
            EmitDescription::Scalar {
                data_type: Some(DataType::BigInt),
                nullable: true
            },
            EmitDescription::Scalar {
                data_type: Some(DataType::BigInt),
                nullable: true
            }
        ]
    );
    assert_eq!(
        engine.execute_transaction_bound(&bound).unwrap().emitted,
        [
            EmittedValue::Scalar(V::Null),
            EmittedValue::Scalar(V::Null),
            EmittedValue::Scalar(V::Null),
            EmittedValue::Scalar(V::Null)
        ]
    );
    engine
        .execute(&update(all_null.clone(), Intent::One), &Bindings::new())
        .unwrap();
    let mut required = update(all_null, Intent::One);
    if let Request::UpdateExpressions { values, .. } = &mut required {
        values[0].0 = "required".into();
    }
    runtime_failure(&admin, &engine, &required, Error::Type);
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
fn scalar_function_nodes_arity_and_value_admission_are_shared() {
    let (admin, engine) = fixture(true);
    assert_eq!(MAX_FUNCTION_ARGUMENTS, 64);
    let exact = Expr::Not(Box::new(Expr::And(
        Box::new(balanced_boolean(vec![field("b"); 2015])),
        Box::new(coalesce(vec![field("b"); MAX_FUNCTION_ARGUMENTS])),
    )));
    assert_eq!(
        2 * 2015 - 1 + 1 + MAX_FUNCTION_ARGUMENTS + 1 + 1,
        MAX_EXPRESSION_NODES
    );
    let request = read(exact.clone());
    assert!(serde_json::to_vec(&request).unwrap().len() < MAX_INPUT);
    engine.prepare_bound(&request, &Bindings::new()).unwrap();
    assert_eq!(result(&engine, exact.clone()), Ok(V::Bool(false)));
    static_failure(
        &admin,
        &engine,
        &read(Expr::Not(Box::new(exact))),
        &Bindings::new(),
        Some(Error::Limit),
    );
    let mut deep = int(1);
    for _ in 0..MAX_DEPTH {
        deep = abs(deep);
    }
    assert_eq!(result(&engine, deep.clone()), Ok(V::Int(1)));
    static_failure(
        &admin,
        &engine,
        &read(abs(deep)),
        &Bindings::new(),
        Some(Error::Limit),
    );
    for function in [
        ScalarFunction::Coalesce,
        ScalarFunction::Abs,
        ScalarFunction::Length,
    ] {
        static_failure(
            &admin,
            &engine,
            &read(call(
                function,
                vec![field("missing"); MAX_FUNCTION_ARGUMENTS + 1],
            )),
            &Bindings::new(),
            Some(Error::Limit),
        );
    }
    static_failure(
        &admin,
        &engine,
        &read(length(value(V::Text("x".repeat(MAX_INPUT))))),
        &Bindings::new(),
        Some(Error::Limit),
    );
    let request = read(coalesce(vec![parameter("p"); MAX_FUNCTION_ARGUMENTS]));
    let bindings = Bindings::from([("p".into(), V::Text("x".repeat(1024)))]);
    assert_eq!(MAX_FUNCTION_ARGUMENTS * 1024, MAX_BOUND_VALUE_BYTES);
    let bound = engine.prepare_bound(&request, &bindings).unwrap();
    assert_eq!(
        engine.execute_bound(&bound).unwrap().rows[0].values,
        [V::Text("x".repeat(1024))]
    );
    static_failure(
        &admin,
        &engine,
        &request,
        &Bindings::from([("p".into(), V::Text("x".repeat(1025)))]),
        Some(Error::Limit),
    );
    let request = read(coalesce(vec![
        value(V::Text("fixed".into())),
        parameter("p"),
    ]));
    let bindings = Bindings::from([("p".into(), V::Text("x".repeat(MAX_BOUND_VALUE_BYTES - 5)))]);
    assert_eq!(
        engine.execute(&request, &bindings).unwrap().rows[0].values,
        [V::Text("fixed".into())]
    );
    static_failure(
        &admin,
        &engine,
        &request,
        &Bindings::from([("p".into(), V::Text("x".repeat(MAX_BOUND_VALUE_BYTES - 4)))]),
        Some(Error::Limit),
    );
}

#[test]
fn scalar_function_local_occurrences_charge_skipped_arguments() {
    let (admin, engine) = fixture(true);
    admin
        .sql_session()
        .execute(
            "UPDATE items SET txt=$1 WHERE id=1",
            &[V::Text("x".repeat(32758))],
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
            field: "out".into(),
        })
    };
    let make = |tag: &str| Transaction {
        read_only: false,
        steps: vec![
            TransactionStep::Let {
                name: "saved".into(),
                request: returning(saved.clone(), field("txt")),
            },
            TransactionStep::Require(Expr::IsNull(
                Box::new(coalesce(vec![local(), local()])),
                true,
            )),
        ],
        emit: Some(vec![EmitExpr::Scalar(value(V::Text(tag.into())))]),
    };
    assert_eq!(18 + 2 * 32758 + 2, MAX_BOUND_VALUE_BYTES);
    let exact = make("zz");
    let bound = engine
        .prepare_transaction_bound(&exact, &Bindings::new())
        .unwrap();
    assert_eq!(
        engine.execute_transaction_bound(&bound).unwrap().emitted,
        [EmittedValue::Scalar(V::Text("zz".into()))]
    );
    let over = make("zzz");
    let before = digest(&admin);
    let bound = engine
        .prepare_transaction_bound(&over, &Bindings::new())
        .unwrap();
    assert_eq!(engine.execute_transaction_bound(&bound), Err(Error::Limit));
    assert_eq!(
        engine.execute_transaction(&over, &Bindings::new()),
        Err(Error::Limit)
    );
    assert_eq!(digest(&admin), before);
    for id in 2..=8 {
        admin
            .sql_session()
            .execute(
                "INSERT INTO items(id,n,b,required) VALUES($1,1,true,'ready')",
                &[V::Int(id)],
                None,
            )
            .unwrap();
    }
    let request = Request::Read {
        table: "items".into(),
        filter: Some(equal(length(local()), int(32758))),
        order: vec![],
        limit: Some(ValueExpr::Literal(V::Int(0))),
        projection: vec![],
    };
    let tx = Transaction {
        read_only: false,
        steps: vec![
            TransactionStep::Let {
                name: "saved".into(),
                request: returning(saved, field("txt")),
            },
            TransactionStep::Execute(request),
        ],
        emit: None,
    };
    assert_eq!(
        engine
            .execute_transaction(&tx, &Bindings::new())
            .unwrap()
            .summaries[1]
            .matched,
        8
    );
}

#[test]
fn scalar_function_pagination_caps_and_late_failures_are_atomic() {
    let (admin, engine) = fixture(true);
    admin
        .sql_session()
        .execute(
            "INSERT INTO items(id,n,b,required) VALUES(2,$1,true,'ready')",
            &[V::Int(i64::MIN)],
            None,
        )
        .unwrap();
    for limit in [0, 1] {
        let mut request = read(abs(field("n")));
        if let Request::ReadExpressions { limit: l, .. } = &mut request {
            *l = Some(ValueExpr::Literal(V::Int(limit)));
        }
        runtime_failure(&admin, &engine, &request, Error::Overflow);
    }
    let mut filtered = read(abs(field("n")));
    if let Request::ReadExpressions { filter, .. } = &mut filtered {
        *filter = Some(equal(field("id"), int(1)));
    }
    assert_eq!(
        engine.execute(&filtered, &Bindings::new()).unwrap().rows[0].values,
        [V::Int(5)]
    );
    let filter = Request::Read {
        table: "items".into(),
        filter: Some(Expr::Compare(
            Box::new(abs(field("n"))),
            Compare::Gt,
            Box::new(int(0)),
        )),
        order: vec![],
        limit: Some(ValueExpr::Literal(V::Int(0))),
        projection: vec![],
    };
    runtime_failure(&admin, &engine, &filter, Error::Overflow);
    for (intent, expected) in [
        (Intent::One, Error::AmbiguousMatch),
        (
            Intent::Many {
                cap: ValueExpr::Literal(V::Int(1)),
            },
            Error::TooManyMatches,
        ),
    ] {
        runtime_failure(
            &admin,
            &engine,
            &update(abs(int(i64::MIN)), intent),
            expected,
        );
    }
    let mut guarded = update(abs(int(i64::MIN)), Intent::One);
    if let Request::UpdateExpressions { filter, guard, .. } = &mut guarded {
        *filter = equal(field("id"), int(1));
        *guard = Some(coalesce(vec![
            value(V::Bool(false)),
            equal(bad_division(), int(1)),
        ]));
    }
    runtime_failure(&admin, &engine, &guarded, Error::ConditionFailed);
    runtime_failure(
        &admin,
        &engine,
        &update(
            abs(field("n")),
            Intent::Many {
                cap: ValueExpr::Literal(V::Int(2)),
            },
        ),
        Error::Overflow,
    );
    let many = Request::InsertMany {
        table: "items".into(),
        columns: vec!["id".into(), "n".into(), "required".into()],
        rows: vec![
            vec![int(3), int(1), value(V::Text("ready".into()))],
            vec![int(4), value(V::Null), value(V::Text("ready".into()))],
        ],
        cap: ValueExpr::Literal(V::Int(2)),
    };
    runtime_failure(
        &admin,
        &engine,
        &returning(many, coalesce(vec![field("n"), bad_division()])),
        Error::DivisionByZero,
    );
    let before = admin
        .sql_session()
        .execute("SELECT * FROM items LIMIT 1024", &[], None)
        .unwrap()
        .rows;
    assert_eq!(before.len(), 2);
    let invalid = Request::InsertMany {
        table: "items".into(),
        columns: vec!["id".into(), "n".into(), "required".into()],
        rows: vec![vec![
            int(3),
            abs(int(i64::MIN)),
            value(V::Text("ready".into())),
        ]],
        cap: ValueExpr::Literal(V::Int(0)),
    };
    runtime_failure(&admin, &engine, &invalid, Error::TooManyMatches);
    let maximum = before.iter().map(|r| r.version).max().unwrap();
    admin
        .sql_session()
        .execute(
            "INSERT INTO items(id,required) VALUES(3,'ready')",
            &[],
            None,
        )
        .unwrap();
    assert_eq!(
        admin
            .sql_session()
            .execute("SELECT * FROM items WHERE id=3", &[], None)
            .unwrap()
            .rows[0]
            .version,
        maximum + 1
    );
}
