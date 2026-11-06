mod common;
use giggles_core::{
    Admin, Limits,
    operations::{
        Arithmetic, Bindings, Compare, EmitDescription, EmitExpr, EmittedValue, Engine, Error,
        Expr, Intent, LocalRef, MAX_BOUND_VALUE_BYTES, MAX_CASE_ARMS, MAX_DEPTH,
        MAX_EXPRESSION_NODES, MAX_INPUT, Projection, Request, ScalarFunction, Transaction,
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
fn yes() -> Expr {
    value(V::Bool(true))
}
fn no() -> Expr {
    value(V::Bool(false))
}
fn field(name: &str) -> Expr {
    Expr::Field(name.into())
}
fn parameter(name: &str) -> Expr {
    Expr::Value(ValueExpr::Parameter(name.into()))
}
fn text(s: &str) -> Expr {
    value(V::Text(s.into()))
}
fn equal(a: Expr, b: Expr) -> Expr {
    Expr::Compare(Box::new(a), Compare::Eq, Box::new(b))
}
fn binary(a: Expr, op: Arithmetic, b: Expr) -> Expr {
    Expr::Arithmetic(Box::new(a), op, Box::new(b))
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
fn bad() -> Expr {
    binary(int(1), Arithmetic::Divide, int(0))
}
fn case(operand: Option<Expr>, arms: Vec<(Expr, Expr)>, else_expr: Option<Expr>) -> Expr {
    Expr::Case {
        operand: operand.map(Box::new),
        arms,
        else_expr: else_expr.map(Box::new),
    }
}
fn searched(arms: Vec<(Expr, Expr)>, otherwise: Option<Expr>) -> Expr {
    case(None, arms, otherwise)
}
fn projection(expression: Expr) -> Projection {
    Projection {
        expression,
        alias: Some("out".into()),
    }
}
fn read(expression: Expr) -> Request {
    Request::ReadExpressions {
        table: "items".into(),
        filter: None,
        order: vec![],
        limit: None,
        projection: vec![projection(expression)],
    }
}
fn update(expression: Expr, intent: Intent) -> Request {
    Request::UpdateExpressions {
        table: "items".into(),
        filter: yes(),
        guard: None,
        values: vec![("n".into(), expression)],
        intent,
    }
}
fn returning(request: Request, expression: Expr) -> Request {
    Request::Returning {
        request: Box::new(request),
        projection: vec![projection(expression)],
    }
}
fn fixture(populated: bool) -> (Admin, Engine) {
    let admin = Admin::create(common::fresh("case-expressions"), Limits::default()).unwrap();
    admin.sql_session().execute("CREATE TABLE items(id BIGINT PRIMARY KEY,n BIGINT,b BOOLEAN,txt TEXT,small SMALLINT,required TEXT NOT NULL)",&[],None).unwrap();
    if populated {
        admin
            .sql_session()
            .execute(
                "INSERT INTO items VALUES(1,-5,false,'x',NULL,'ready')",
                &[],
                None,
            )
            .unwrap();
    }
    (admin.clone(), Engine::new(admin))
}
fn digest(admin: &Admin) -> [u8; 32] {
    admin.export(common::fresh("case-export")).unwrap()
}
fn result(engine: &Engine, expression: Expr) -> Result<V, Error> {
    engine
        .execute(&read(expression), &Bindings::new())
        .map(|r| r.rows[0].values[0].clone())
}
fn static_failure(
    admin: &Admin,
    engine: &Engine,
    request: &Request,
    bindings: &Bindings,
    expected: Option<Error>,
) {
    let before = digest(admin);
    let error = engine.prepare_bound(request, bindings).unwrap_err();
    assert_eq!(engine.execute(request, bindings), Err(error.clone()));
    if let Some(expected) = expected {
        assert_eq!(error, expected);
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
fn case_binding_unifies_every_arm_before_rows() {
    for populated in [false, true] {
        let (admin, engine) = fixture(populated);
        for e in [
            searched(vec![], Some(int(1))),
            case(Some(int(1)), vec![], None),
            searched(vec![(int(1), int(1))], Some(int(2))),
            case(Some(int(1)), vec![(text("1"), int(1))], None),
            case(
                Some(value(V::Null)),
                vec![(int(1), int(1)), (text("x"), int(2))],
                None,
            ),
            searched(vec![(yes(), int(1)), (no(), text("wrong"))], None),
            searched(vec![(yes(), int(1))], Some(value(V::Bool(true)))),
        ] {
            static_failure(
                &admin,
                &engine,
                &read(e),
                &Bindings::new(),
                Some(Error::Type),
            );
        }
        for e in [
            searched(vec![(yes(), int(1)), (field("missing"), int(2))], None),
            searched(vec![(yes(), int(1)), (no(), field("missing"))], None),
            searched(vec![(yes(), int(1))], Some(parameter("missing"))),
            case(
                Some(int(1)),
                vec![(int(1), int(1)), (parameter("missing"), int(2))],
                None,
            ),
            searched(
                vec![(yes(), int(1))],
                Some(Expr::Local(LocalRef::Changed {
                    local: "missing".into(),
                })),
            ),
        ] {
            static_failure(&admin, &engine, &read(e), &Bindings::new(), None);
        }
        let request = read(searched(
            vec![(yes(), int(1))],
            Some(parameter("otherwise")),
        ));
        static_failure(
            &admin,
            &engine,
            &request,
            &Bindings::from([("otherwise".into(), V::Text("wrong".into()))]),
            Some(Error::Type),
        );
        static_failure(
            &admin,
            &engine,
            &read(searched(vec![(yes(), int(1))], None)),
            &Bindings::from([("unused".into(), V::Int(1))]),
            Some(Error::Bind("parameters".into())),
        );
    }
}

#[test]
fn case_selection_is_lazy_with_ordinary_null_equality() {
    let (admin, engine) = fixture(true);
    for e in [
        searched(
            vec![(yes(), int(7)), (equal(bad(), int(1)), abs(int(i64::MIN)))],
            Some(bad()),
        ),
        searched(
            vec![
                (no(), bad()),
                (value(V::Null), abs(int(i64::MIN))),
                (yes(), int(7)),
            ],
            Some(bad()),
        ),
        case(
            Some(int(2)),
            vec![(int(1), bad()), (int(2), int(7)), (bad(), int(9))],
            Some(abs(int(i64::MIN))),
        ),
        case(Some(int(1)), vec![(int(1), int(7)), (int(1), bad())], None),
    ] {
        assert_eq!(result(&engine, e), Ok(V::Int(7)));
    }
    assert_eq!(
        result(
            &engine,
            case(
                Some(value(V::Null)),
                vec![(value(V::Null), int(1)), (int(0), int(2))],
                Some(int(3))
            )
        ),
        Ok(V::Int(3))
    );
    assert_eq!(
        result(
            &engine,
            case(
                Some(value(V::Bool(false))),
                vec![(value(V::Null), int(1)), (no(), int(2))],
                None
            )
        ),
        Ok(V::Int(2))
    );
    assert_eq!(
        result(
            &engine,
            case(
                Some(text("\u{e9}")),
                vec![(text("e\u{301}"), int(1)), (text("\u{e9}"), int(2))],
                None
            )
        ),
        Ok(V::Int(2))
    );
    assert_eq!(
        result(
            &engine,
            case(
                Some(value(V::Bytes(vec![0, 255]))),
                vec![
                    (value(V::Bytes(vec![0])), int(1)),
                    (value(V::Bytes(vec![0, 255])), int(2))
                ],
                None
            )
        ),
        Ok(V::Int(2))
    );
    for e in [
        case(Some(value(V::Null)), vec![(bad(), int(1))], Some(int(3))),
        case(
            Some(bad()),
            vec![(abs(int(i64::MIN)), int(1))],
            Some(int(3)),
        ),
        searched(vec![(yes(), bad()), (yes(), int(7))], Some(int(9))),
        searched(
            vec![(
                Expr::And(Box::new(no()), Box::new(equal(bad(), int(0)))),
                int(1),
            )],
            Some(int(3)),
        ),
    ] {
        runtime_failure(&admin, &engine, &read(e), Error::DivisionByZero);
    }
    assert_eq!(
        result(
            &engine,
            searched(vec![(no(), int(1)), (value(V::Null), int(2))], None)
        ),
        Ok(V::Null)
    );
    let lazy_condition = call(ScalarFunction::Coalesce, vec![yes(), equal(bad(), int(0))]);
    assert_eq!(
        result(
            &engine,
            searched(vec![(lazy_condition, int(7))], Some(bad()))
        ),
        Ok(V::Int(7))
    );
}

#[test]
fn case_metadata_and_omitted_else_preserve_static_types() {
    let (admin, engine) = fixture(true);
    let typed_null = searched(
        vec![(no(), abs(value(V::Null)))],
        Some(length(value(V::Null))),
    );
    for (e, data_type, expected) in [
        (typed_null.clone(), DataType::BigInt, V::Null),
        (
            searched(vec![(no(), int(1))], None),
            DataType::BigInt,
            V::Null,
        ),
        (
            searched(vec![(yes(), field("small"))], None),
            DataType::BigInt,
            V::Null,
        ),
        (
            searched(vec![(yes(), yes())], Some(no())),
            DataType::Boolean,
            V::Bool(true),
        ),
        (
            searched(vec![(yes(), text("x"))], Some(text("y"))),
            DataType::Text,
            V::Text("x".into()),
        ),
        (
            searched(vec![(yes(), value(V::Bytes(vec![0])))], None),
            DataType::Bytes,
            V::Bytes(vec![0]),
        ),
    ] {
        let bound = engine
            .prepare_bound(&read(e.clone()), &Bindings::new())
            .unwrap();
        let c = &bound.description().columns[0];
        assert_eq!(c.id, 0);
        assert_eq!(c.data_type, data_type);
        assert!(!c.not_null);
        assert_eq!(result(&engine, e), Ok(expected));
    }
    let unknown = searched(vec![(no(), value(V::Null))], None);
    static_failure(
        &admin,
        &engine,
        &read(unknown.clone()),
        &Bindings::new(),
        Some(Error::Type),
    );
    let tx = Transaction {
        read_only: true,
        steps: vec![],
        emit: Some(vec![
            EmitExpr::Scalar(unknown.clone()),
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
            }
        ]
    );
    assert_eq!(
        engine.execute_transaction_bound(&bound).unwrap().emitted,
        [EmittedValue::Scalar(V::Null), EmittedValue::Scalar(V::Null)]
    );
    let mut small = update(
        searched(vec![(yes(), int(1))], Some(int(40000))),
        Intent::One,
    );
    if let Request::UpdateExpressions { values, .. } = &mut small {
        values[0].0 = "small".into();
    }
    engine.execute(&small, &Bindings::new()).unwrap();
    if let Request::UpdateExpressions { values, .. } = &mut small {
        values[0].1 = searched(vec![(no(), int(1))], Some(int(40000)));
    }
    runtime_failure(&admin, &engine, &small, Error::Type);
    engine
        .execute(&update(unknown, Intent::One), &Bindings::new())
        .unwrap();
    let mut required = update(searched(vec![(no(), text("x"))], None), Intent::One);
    if let Request::UpdateExpressions { values, .. } = &mut required {
        values[0].0 = "required".into();
    }
    runtime_failure(&admin, &engine, &required, Error::Type);
}

fn balanced(mut expressions: Vec<Expr>) -> Expr {
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
fn case_nodes_arity_and_frozen_value_budgets_are_shared() {
    let (admin, engine) = fixture(true);
    assert_eq!(MAX_CASE_ARMS, 64);
    let conditional = searched(
        vec![(field("b"), field("b")); MAX_CASE_ARMS],
        Some(field("b")),
    );
    let exact = Expr::And(
        Box::new(balanced(vec![field("b"); 1983])),
        Box::new(conditional.clone()),
    );
    assert_eq!(
        2 * 1983 - 1 + 1 + 2 * MAX_CASE_ARMS + 1 + 1,
        MAX_EXPRESSION_NODES
    );
    assert!(serde_json::to_vec(&read(exact.clone())).unwrap().len() < MAX_INPUT);
    assert_eq!(result(&engine, exact.clone()), Ok(V::Bool(false)));
    static_failure(
        &admin,
        &engine,
        &read(Expr::Not(Box::new(exact))),
        &Bindings::new(),
        Some(Error::Limit),
    );
    let with_operand = case(
        Some(field("b")),
        vec![(field("b"), field("b")); MAX_CASE_ARMS],
        Some(field("b")),
    );
    static_failure(
        &admin,
        &engine,
        &read(Expr::And(
            Box::new(balanced(vec![field("b"); 1983])),
            Box::new(with_operand),
        )),
        &Bindings::new(),
        Some(Error::Limit),
    );
    static_failure(
        &admin,
        &engine,
        &read(searched(
            vec![(field("missing"), field("missing")); MAX_CASE_ARMS + 1],
            None,
        )),
        &Bindings::new(),
        Some(Error::Limit),
    );
    let mut deep = int(1);
    for _ in 0..MAX_DEPTH {
        deep = searched(vec![(yes(), deep)], None);
    }
    assert_eq!(result(&engine, deep.clone()), Ok(V::Int(1)));
    static_failure(
        &admin,
        &engine,
        &read(searched(vec![(yes(), deep)], None)),
        &Bindings::new(),
        Some(Error::Limit),
    );
    let exact = searched(vec![(no(), parameter("p")); MAX_CASE_ARMS], None);
    assert_eq!(MAX_CASE_ARMS * (9 + 1015), MAX_BOUND_VALUE_BYTES);
    let bindings = Bindings::from([("p".into(), V::Text("x".repeat(1015)))]);
    let bound = engine
        .prepare_bound(&read(exact.clone()), &bindings)
        .unwrap();
    assert_eq!(bound.description().columns[0].data_type, DataType::Text);
    assert_eq!(
        engine.execute_bound(&bound).unwrap().rows[0].values,
        [V::Null]
    );
    static_failure(
        &admin,
        &engine,
        &read(exact.clone()),
        &Bindings::from([("p".into(), V::Text("x".repeat(1016)))]),
        Some(Error::Limit),
    );
    let mut explicit_else = exact;
    if let Expr::Case { else_expr, .. } = &mut explicit_else {
        *else_expr = Some(Box::new(value(V::Null)));
    }
    static_failure(
        &admin,
        &engine,
        &read(explicit_else),
        &bindings,
        Some(Error::Limit),
    );
    let simple = read(case(
        Some(parameter("p")),
        vec![(text("a"), int(1)), (text("b"), int(2))],
        Some(int(3)),
    ));
    assert_eq!(65507 + 2 + 3 * 9, MAX_BOUND_VALUE_BYTES);
    let bindings = Bindings::from([("p".into(), V::Text("x".repeat(65507)))]);
    assert_eq!(
        engine.execute(&simple, &bindings).unwrap().rows[0].values,
        [V::Int(3)]
    );
    static_failure(
        &admin,
        &engine,
        &simple,
        &Bindings::from([("p".into(), V::Text("x".repeat(65508)))]),
        Some(Error::Limit),
    );
}

#[test]
fn case_runtime_local_admission_counts_skipped_branches() {
    for (kind, size, frozen, occurrences, expected) in [
        (0, 65489, 47, 1, 3),
        (1, 32750, 36, 2, 1),
        (2, 65473, 63, 1, 1),
    ] {
        let admin = Admin::create(common::fresh("case-locals"), Limits::default()).unwrap();
        admin
            .sql_session()
            .execute(
                "CREATE TABLE items(id BIGINT PRIMARY KEY,n BIGINT,txt TEXT)",
                &[],
                None,
            )
            .unwrap();
        admin
            .sql_session()
            .execute(
                "INSERT INTO items VALUES(1,0,$1)",
                &[V::Text("x".repeat(size))],
                None,
            )
            .unwrap();
        let engine = Engine::new(admin.clone());
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
        let expression = match kind {
            0 => case(
                Some(local()),
                vec![(text("a"), int(1)), (text("b"), int(2))],
                Some(int(3)),
            ),
            1 => searched(
                vec![(yes(), int(1))],
                Some(length(call(
                    ScalarFunction::Coalesce,
                    vec![local(), local()],
                ))),
            ),
            _ => searched(
                vec![(yes(), int(1)), (equal(length(local()), int(0)), int(2))],
                Some(int(3)),
            ),
        };
        assert_eq!(frozen + size * occurrences, MAX_BOUND_VALUE_BYTES);
        let tx = Transaction {
            read_only: false,
            steps: vec![TransactionStep::Let {
                name: "saved".into(),
                request: returning(saved.clone(), field("txt")),
            }],
            emit: Some(vec![EmitExpr::Scalar(expression)]),
        };
        let bound = engine
            .prepare_transaction_bound(&tx, &Bindings::new())
            .unwrap();
        assert_eq!(
            engine.execute_transaction_bound(&bound).unwrap().emitted,
            [EmittedValue::Scalar(V::Int(expected))]
        );
        let mut over = tx;
        over.emit
            .as_mut()
            .unwrap()
            .push(EmitExpr::Scalar(text("z")));
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
        if kind == 2 {
            for id in 2..=8 {
                admin
                    .sql_session()
                    .execute("INSERT INTO items(id,n) VALUES($1,0)", &[V::Int(id)], None)
                    .unwrap();
            }
            let filter = searched(
                vec![(yes(), equal(length(local()), int(size as i64)))],
                Some(no()),
            );
            let read = Request::Read {
                table: "items".into(),
                filter: Some(filter),
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
                    TransactionStep::Execute(read),
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
    }
}

#[test]
fn case_output_and_mutation_failures_are_atomic() {
    let (admin, engine) = fixture(true);
    admin
        .sql_session()
        .execute(
            "INSERT INTO items(id,n,required) VALUES(2,$1,'ready')",
            &[V::Int(i64::MIN)],
            None,
        )
        .unwrap();
    let expression = searched(
        vec![(equal(field("id"), int(1)), int(7))],
        Some(abs(int(i64::MIN))),
    );
    for limit in [0, 1] {
        let mut request = read(expression.clone());
        if let Request::ReadExpressions { limit: l, .. } = &mut request {
            *l = Some(ValueExpr::Literal(V::Int(limit)));
        }
        runtime_failure(&admin, &engine, &request, Error::Overflow);
    }
    let mut filtered = read(expression);
    if let Request::ReadExpressions { filter, .. } = &mut filtered {
        *filter = Some(equal(field("id"), int(1)));
    }
    assert_eq!(
        engine.execute(&filtered, &Bindings::new()).unwrap().rows[0].values,
        [V::Int(7)]
    );
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
            &update(searched(vec![(yes(), bad())], None), intent),
            expected,
        );
    }
    let mut guarded = update(searched(vec![(yes(), bad())], None), Intent::One);
    if let Request::UpdateExpressions { filter, guard, .. } = &mut guarded {
        *filter = equal(field("id"), int(1));
        *guard = Some(searched(vec![(no(), equal(bad(), int(1)))], Some(no())));
    }
    runtime_failure(&admin, &engine, &guarded, Error::ConditionFailed);
    runtime_failure(
        &admin,
        &engine,
        &update(
            case(
                Some(field("n")),
                vec![(int(i64::MIN), abs(int(i64::MIN)))],
                Some(abs(field("n"))),
            ),
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
            vec![int(3), int(1), text("ready")],
            vec![int(4), value(V::Null), text("ready")],
        ],
        cap: ValueExpr::Literal(V::Int(2)),
    };
    runtime_failure(
        &admin,
        &engine,
        &returning(
            many,
            searched(
                vec![(Expr::IsNull(Box::new(field("n")), false), bad())],
                Some(field("n")),
            ),
        ),
        Error::DivisionByZero,
    );
    let before = admin
        .sql_session()
        .execute("SELECT * FROM items LIMIT 1024", &[], None)
        .unwrap()
        .rows;
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
    let mut noop = update(
        searched(vec![(yes(), field("n"))], Some(bad())),
        Intent::One,
    );
    if let Request::UpdateExpressions { filter, .. } = &mut noop {
        *filter = equal(field("id"), int(1));
    }
    let result = engine
        .execute(
            &returning(
                noop,
                case(Some(field("n")), vec![(int(-5), int(7))], Some(bad())),
            ),
            &Bindings::new(),
        )
        .unwrap();
    assert_eq!(result.changed, 0);
    assert_eq!(result.rows[0].values, [V::Int(7)]);
    assert_eq!(result.rows[0].version, before[0].version);
    for extra in [0, 1] {
        let admin = Admin::create(common::fresh("case-output"), Limits::default()).unwrap();
        admin
            .sql_session()
            .execute(
                "CREATE TABLE items(id BIGINT PRIMARY KEY,src TEXT,tail TEXT)",
                &[],
                None,
            )
            .unwrap();
        admin
            .sql_session()
            .execute(
                "INSERT INTO items VALUES(1,$1,$2)",
                &[
                    V::Text("x".repeat(16384)),
                    V::Text("y".repeat(16384 + extra)),
                ],
                None,
            )
            .unwrap();
        let engine = Engine::new(admin.clone());
        let mut output = (0..63)
            .map(|i| Projection {
                expression: searched(vec![(yes(), field("src"))], Some(text(""))),
                alias: Some(format!("p{i}")),
            })
            .collect::<Vec<_>>();
        output.push(Projection {
            expression: searched(vec![(no(), field("src"))], Some(field("tail"))),
            alias: Some("last".into()),
        });
        let request = Request::Returning {
            request: Box::new(Request::Delete {
                table: "items".into(),
                filter: yes(),
                guard: None,
                cap: None,
            }),
            projection: output,
        };
        if extra == 0 {
            let result = engine.execute(&request, &Bindings::new()).unwrap();
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
                1024 * 1024
            );
        } else {
            runtime_failure(&admin, &engine, &request, Error::Limit);
        }
    }
}
