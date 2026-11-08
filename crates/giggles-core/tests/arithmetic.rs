mod common;

use giggles_core::{
    Admin, CoreError, Limits,
    operations::{
        Arithmetic, Bindings, Compare, Engine, Error, Expr, Intent, MAX_BINDING_BYTES, MAX_DEPTH,
        MAX_EXPRESSION_NODES, MAX_INPUT, Request, Sort, ValueExpr,
    },
    relational::{SqlError, SqlRow, Value as V},
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
fn binary(left: Expr, op: Arithmetic, right: Expr) -> Expr {
    Expr::Arithmetic(Box::new(left), op, Box::new(right))
}
fn compare(left: Expr, op: Compare, right: Expr) -> Expr {
    Expr::Compare(Box::new(left), op, Box::new(right))
}
fn equal(left: Expr, right: Expr) -> Expr {
    compare(left, Compare::Eq, right)
}
fn read(filter: Expr, limit: Option<i64>) -> Request {
    Request::Read {
        table: "numbers".into(),
        filter: Some(filter),
        order: vec![Sort {
            field: "id".into(),
            descending: false,
        }],
        limit: limit.map(|n| ValueExpr::Literal(V::Int(n))),
        projection: vec![],
    }
}
fn fixture(values: &[(i64, Option<i64>)]) -> (Admin, Engine) {
    let admin = Admin::create(common::fresh("arithmetic"), Limits::default()).unwrap();
    let mut sql = admin.sql_session();
    sql.execute(
        "CREATE TABLE numbers (id BIGINT PRIMARY KEY, n BIGINT, note TEXT)",
        &[],
        None,
    )
    .unwrap();
    sql.execute("BEGIN", &[], None).unwrap();
    for (id, n) in values {
        sql.execute(
            "INSERT INTO numbers VALUES($1,$2,'original')",
            &[V::Int(*id), n.map_or(V::Null, V::Int)],
            None,
        )
        .unwrap();
    }
    sql.execute("COMMIT", &[], None).unwrap();
    (admin.clone(), Engine::new(admin))
}
fn rows(admin: &Admin) -> Vec<SqlRow> {
    admin
        .sql_session()
        .execute("SELECT * FROM numbers ORDER BY id LIMIT 1024", &[], None)
        .unwrap()
        .rows
}
fn digest(admin: &Admin) -> [u8; 32] {
    admin.export(common::fresh("arithmetic-export")).unwrap()
}
fn runtime_failure(admin: &Admin, engine: &Engine, request: &Request, error: Error) {
    let before = digest(admin);
    let catalog = admin.catalog().unwrap();
    let existing = rows(admin);
    // Arithmetic is not folded at preparation. Both execution paths must apply
    // the same runtime error and leave all durable state untouched.
    let prepared = engine.prepare_bound(request, &Bindings::new()).unwrap();
    assert_eq!(engine.execute_bound(&prepared), Err(error.clone()));
    assert_eq!(engine.execute(request, &Bindings::new()), Err(error));
    assert_eq!(digest(admin), before);
    assert_eq!(admin.catalog().unwrap(), catalog);
    assert_eq!(rows(admin), existing);
}

// This oracle intentionally uses wider arithmetic, never the checked-i64 helper
// or operation evaluator under test. i64 products and MIN/-1 fit in i128.
fn oracle(left: i64, op: Arithmetic, right: i64) -> Result<i64, Error> {
    let (a, b) = (i128::from(left), i128::from(right));
    let answer = match op {
        Arithmetic::Add => a + b,
        Arithmetic::Subtract => a - b,
        Arithmetic::Multiply => a * b,
        Arithmetic::Divide if b == 0 => return Err(Error::DivisionByZero),
        Arithmetic::Remainder if b == 0 => return Err(Error::DivisionByZero),
        Arithmetic::Divide => a / b,
        Arithmetic::Remainder => a % b,
    };
    i64::try_from(answer).map_err(|_| Error::Overflow)
}

#[test]
fn arithmetic_checked_integer_results_match_independent_i128_oracle() {
    let (_, engine) = fixture(&[(1, Some(1))]);
    let edges = [
        i64::MIN,
        i64::MIN + 1,
        -3,
        -1,
        0,
        1,
        3,
        i64::MAX - 1,
        i64::MAX,
    ];
    let mut pairs: Vec<_> = edges
        .into_iter()
        .flat_map(|a| edges.into_iter().map(move |b| (a, b)))
        .collect();
    let mut seed = 0x3478_a75d_9ac2_4411u64;
    for _ in 0..24 {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let left = seed as i64;
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        pairs.push((left, seed as i64));
    }
    for op in [
        Arithmetic::Add,
        Arithmetic::Subtract,
        Arithmetic::Multiply,
        Arithmetic::Divide,
        Arithmetic::Remainder,
    ] {
        for &(left, right) in &pairs {
            let expected = oracle(left, op, right);
            let request = read(
                equal(
                    binary(parameter("a"), op, parameter("b")),
                    int(expected.clone().unwrap_or(0)),
                ),
                None,
            );
            let bindings =
                Bindings::from([("a".into(), V::Int(left)), ("b".into(), V::Int(right))]);
            let actual = engine.execute(&request, &bindings);
            match expected {
                Ok(_) => assert_eq!(actual.unwrap().matched, 1, "{left} {op:?} {right}"),
                Err(error) => assert_eq!(actual, Err(error), "{left} {op:?} {right}"),
            }
        }
    }
    for n in edges {
        let request = read(equal(Expr::Negate(Box::new(int(n))), int(0)), None);
        let actual = engine.execute(&request, &Bindings::new());
        match i64::try_from(-i128::from(n)) {
            Ok(answer) => {
                let request = read(equal(Expr::Negate(Box::new(int(n))), int(answer)), None);
                assert_eq!(
                    engine.execute(&request, &Bindings::new()).unwrap().matched,
                    1
                );
                assert_eq!(actual.unwrap().matched, usize::from(answer == 0));
            }
            Err(_) => assert_eq!(actual, Err(Error::Overflow)),
        }
    }
}

#[test]
fn arithmetic_binding_is_eager_even_on_empty_relations() {
    let (admin, engine) = fixture(&[]);
    let before = digest(&admin);
    for expr in [
        binary(value(V::Text("1".into())), Arithmetic::Add, int(1)),
        binary(value(V::Null), Arithmetic::Multiply, value(V::Bool(false))),
        Expr::Negate(Box::new(value(V::Bytes(vec![1])))),
        Expr::And(
            Box::new(value(V::Bool(false))),
            Box::new(equal(
                binary(field("note"), Arithmetic::Add, int(1)),
                int(2),
            )),
        ),
    ] {
        let request = read(Expr::IsNull(Box::new(expr), false), None);
        assert_eq!(
            engine.prepare_bound(&request, &Bindings::new()).err(),
            Some(Error::Type)
        );
        assert_eq!(engine.execute(&request, &Bindings::new()), Err(Error::Type));
    }
    let missing = read(
        Expr::Or(
            Box::new(value(V::Bool(true))),
            Box::new(equal(
                binary(parameter("missing"), Arithmetic::Add, int(1)),
                int(2),
            )),
        ),
        None,
    );
    assert_eq!(
        engine.execute(&missing, &Bindings::new()),
        Err(Error::Bind("parameter missing".into()))
    );
    let absent = read(
        equal(binary(field("absent"), Arithmetic::Add, int(1)), int(2)),
        None,
    );
    assert_eq!(
        engine.execute(&absent, &Bindings::new()),
        Err(Error::Bind("column".into()))
    );
    assert_eq!(digest(&admin), before);
    let catalog = admin.catalog().unwrap();
    // Runtime failures are not evaluated against a nonexistent row, including
    // syntactically constant failures. Preparation must not fold them either.
    for expression in [
        binary(int(1), Arithmetic::Divide, int(0)),
        binary(int(i64::MAX), Arithmetic::Add, int(1)),
        Expr::Negate(Box::new(int(i64::MIN))),
    ] {
        let request = read(equal(expression, int(0)), None);
        let prepared = engine.prepare_bound(&request, &Bindings::new()).unwrap();
        assert_eq!(engine.execute_bound(&prepared).unwrap().matched, 0);
        assert_eq!(
            engine.execute(&request, &Bindings::new()).unwrap().matched,
            0
        );
    }
    // Successful reads commit the existing native transaction and may advance
    // its durable generation. Their data and catalog must remain unchanged.
    assert!(rows(&admin).is_empty());
    assert_eq!(admin.catalog().unwrap(), catalog);
}

#[test]
fn arithmetic_evaluation_is_eager_and_limit_cannot_hide_errors() {
    let (admin, engine) = fixture(&[(1, Some(1))]);
    let fail = || binary(int(1), Arithmetic::Divide, int(0));
    for expression in [
        Expr::And(
            Box::new(value(V::Bool(false))),
            Box::new(equal(fail(), int(0))),
        ),
        Expr::Or(
            Box::new(value(V::Bool(true))),
            Box::new(equal(fail(), int(0))),
        ),
        Expr::IsNull(
            Box::new(binary(value(V::Null), Arithmetic::Add, fail())),
            false,
        ),
        Expr::IsNull(
            Box::new(binary(fail(), Arithmetic::Add, value(V::Null))),
            false,
        ),
    ] {
        runtime_failure(
            &admin,
            &engine,
            &read(expression, None),
            Error::DivisionByZero,
        );
    }
    for op in [
        Arithmetic::Add,
        Arithmetic::Subtract,
        Arithmetic::Multiply,
        Arithmetic::Divide,
        Arithmetic::Remainder,
    ] {
        for (left, right) in [(value(V::Null), int(0)), (int(1), value(V::Null))] {
            let request = read(Expr::IsNull(Box::new(binary(left, op, right)), false), None);
            assert_eq!(
                engine.execute(&request, &Bindings::new()).unwrap().matched,
                1
            );
        }
    }
    assert_eq!(
        engine
            .execute(
                &read(
                    Expr::IsNull(Box::new(Expr::Negate(Box::new(value(V::Null)))), false),
                    None
                ),
                &Bindings::new()
            )
            .unwrap()
            .matched,
        1
    );

    let values: Vec<_> = (1..=101)
        .map(|id| (id, Some(i64::from(id != 101))))
        .collect();
    let (admin, engine) = fixture(&values);
    for limit in [Some(0), Some(1), None] {
        let request = read(
            compare(
                binary(int(10), Arithmetic::Divide, field("n")),
                Compare::Gt,
                int(0),
            ),
            limit,
        );
        runtime_failure(&admin, &engine, &request, Error::DivisionByZero);
    }
}

fn update(filter: Expr, guard: Option<Expr>, intent: Intent, column: &str, v: V) -> Request {
    Request::Update {
        table: "numbers".into(),
        filter,
        guard,
        intent,
        values: vec![(column.into(), ValueExpr::Literal(v))],
    }
}

#[test]
fn arithmetic_cardinality_guards_and_failures_leave_no_partial_writes() {
    let (admin, engine) = fixture(&[(1, Some(1)), (2, Some(0))]);
    let yes = || value(V::Bool(true));
    let guard = || equal(binary(int(1), Arithmetic::Divide, int(0)), int(1));
    for (filter, intent, expected) in [
        (yes(), Intent::One, Error::AmbiguousMatch),
        (equal(field("id"), int(99)), Intent::One, Error::NotFound),
        (
            yes(),
            Intent::Many {
                cap: ValueExpr::Literal(V::Int(1)),
            },
            Error::TooManyMatches,
        ),
        (
            equal(field("id"), int(1)),
            Intent::One,
            Error::DivisionByZero,
        ),
    ] {
        let selected_guard = matches!(intent, Intent::One).then(guard);
        runtime_failure(
            &admin,
            &engine,
            &update(
                filter,
                selected_guard,
                intent,
                "note",
                V::Text("changed".into()),
            ),
            expected,
        );
    }
    let late_failure = || {
        compare(
            binary(int(10), Arithmetic::Divide, field("n")),
            Compare::Gt,
            int(0),
        )
    };
    for request in [
        update(
            late_failure(),
            None,
            Intent::Many {
                cap: ValueExpr::Literal(V::Int(2)),
            },
            "note",
            V::Text("changed".into()),
        ),
        Request::Delete {
            table: "numbers".into(),
            filter: late_failure(),
            guard: None,
            cap: Some(ValueExpr::Literal(V::Int(2))),
        },
    ] {
        runtime_failure(&admin, &engine, &request, Error::DivisionByZero);
    }
    // Bulk guards now share the common engine's atomic guard evaluation.
    // The second row fails after the first guard succeeds, with zero effects.
    let guarded_bulk = update(
        yes(),
        Some(late_failure()),
        Intent::Many {
            cap: ValueExpr::Literal(V::Int(2)),
        },
        "note",
        V::Text("changed".into()),
    );
    runtime_failure(&admin, &engine, &guarded_bulk, Error::DivisionByZero);
    let before = rows(&admin);
    let no_op = update(
        equal(binary(field("id"), Arithmetic::Add, int(1)), int(2)),
        None,
        Intent::One,
        "n",
        V::Int(1),
    );
    let result = engine.execute(&no_op, &Bindings::new()).unwrap();
    assert_eq!((result.matched, result.affected, result.changed), (1, 1, 0));
    assert_eq!(rows(&admin), before);
}

#[test]
fn arithmetic_claim_rechecks_postimage_and_preserves_consumption() {
    let (admin, engine) = fixture(&[(1, Some(2)), (2, Some(3))]);
    let request = |n| {
        update(
            compare(
                binary(int(10), Arithmetic::Divide, field("n")),
                Compare::Gt,
                int(0),
            ),
            None,
            Intent::Claim {
                order: vec![Sort {
                    field: "id".into(),
                    descending: false,
                }],
            },
            "n",
            V::Int(n),
        )
    };
    runtime_failure(&admin, &engine, &request(0), Error::DivisionByZero);
    runtime_failure(&admin, &engine, &request(2), Error::ClaimNotConsumed);
    let before = rows(&admin);
    let result = engine.execute(&request(-2), &Bindings::new()).unwrap();
    assert_eq!((result.matched, result.affected, result.changed), (1, 1, 1));
    assert_eq!(
        result.rows[0].values,
        [V::Int(1), V::Int(-2), V::Text("original".into())]
    );
    assert_eq!(result.rows[0], rows(&admin)[0]);
    assert!(result.rows[0].version > before[0].version);
    assert_eq!(rows(&admin)[1], before[1]);
}

#[test]
fn arithmetic_nodes_keep_depth_size_and_binding_limits() {
    let (admin, engine) = fixture(&[]);
    let before = digest(&admin);
    let mut depth = int(1);
    for _ in 0..=MAX_DEPTH {
        depth = Expr::Negate(Box::new(depth));
    }
    let mut wide: Vec<_> = (0..=MAX_EXPRESSION_NODES).map(|_| int(1)).collect();
    while wide.len() > 1 {
        let mut next = Vec::new();
        let mut terms = wide.into_iter();
        while let Some(left) = terms.next() {
            next.push(match terms.next() {
                Some(right) => binary(left, Arithmetic::Add, right),
                None => left,
            });
        }
        wide = next;
    }
    for expression in [
        depth,
        wide.pop().unwrap(),
        binary(
            value(V::Text("x".repeat(MAX_INPUT))),
            Arithmetic::Add,
            int(1),
        ),
    ] {
        let request = read(equal(expression, int(1)), None);
        assert_eq!(
            engine.prepare_bound(&request, &Bindings::new()).err(),
            Some(Error::Limit)
        );
        assert_eq!(
            engine.execute(&request, &Bindings::new()),
            Err(Error::Limit)
        );
    }
    for (request, payload) in [
        (
            read(
                equal(binary(parameter("p"), Arithmetic::Add, int(1)), int(2)),
                None,
            ),
            MAX_BINDING_BYTES + 1,
        ),
        (
            read(
                equal(
                    binary(parameter("p"), Arithmetic::Add, parameter("p")),
                    int(2),
                ),
                None,
            ),
            MAX_BINDING_BYTES / 2 + 1,
        ),
    ] {
        let bindings = Bindings::from([("p".into(), V::Text("x".repeat(payload)))]);
        assert_eq!(
            engine.prepare_bound(&request, &bindings).err(),
            Some(Error::Limit)
        );
        assert_eq!(engine.execute(&request, &bindings), Err(Error::Limit));
    }
    assert_eq!(digest(&admin), before);
}

#[test]
fn sql_sum_and_scalar_predicates_preserve_primary_key_update_rejection() {
    let (admin, _) = fixture(&[(1, Some(-7)), (2, Some(2)), (3, None)]);
    let mut sql = admin.sql_session();
    assert_eq!(
        sql.execute("SELECT SUM(n.n) FROM numbers n", &[], None)
            .unwrap()
            .rows[0]
            .values,
        [V::Numeric(
            giggles_core::relational::Numeric::parse("-5").unwrap()
        )]
    );
    let before = rows(&admin);
    assert_eq!(
        sql.execute("UPDATE numbers SET id=id+1 WHERE id=1", &[], None),
        Err(CoreError::Sql(SqlError::Duplicate))
    );
    assert!(
        sql.execute("SELECT * FROM numbers WHERE n+1=2 LIMIT 1", &[], None)
            .unwrap()
            .rows
            .is_empty()
    );
    assert_eq!(
        sql.execute("SELECT * FROM numbers WHERE n%2=0 LIMIT 1", &[], None)
            .unwrap()
            .rows
            .into_iter()
            .map(|row| row.values)
            .collect::<Vec<_>>(),
        vec![vec![V::Int(2), V::Int(2), V::Text("original".into())]]
    );
    assert_eq!(rows(&admin), before);
    sql.execute(
        "UPDATE numbers SET n=$1 WHERE id=1",
        &[V::Int(i64::MAX)],
        None,
    )
    .unwrap();
    let before = rows(&admin);
    assert_eq!(
        sql.execute("SELECT SUM(n.n) FROM numbers n", &[], None)
            .unwrap()
            .rows[0]
            .values,
        [V::Numeric(
            giggles_core::relational::Numeric::parse("9223372036854775809").unwrap()
        )]
    );
    assert!(
        sql.execute("SELECT SUM(n.n) FROM numbers n LIMIT 0", &[], None)
            .unwrap()
            .rows
            .is_empty()
    );
    assert_eq!(rows(&admin), before);
}
