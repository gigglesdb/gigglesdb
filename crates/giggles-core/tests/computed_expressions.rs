mod common;

use giggles_core::{
    Admin, Cancellation, CoreError, DecisionPoint, Limits,
    operations::{
        Arithmetic, Bindings, Compare, Engine, Error, Expr, Intent, MAX_BINDING_BYTES, MAX_DEPTH,
        MAX_EXPRESSION_NODES, MAX_INPUT, MAX_MUTATION_BYTES, Projection, Request, Sort, ValueExpr,
    },
    relational::{DataType, MAX_RESULT_BYTES, MAX_ROW_BYTES, SqlRow, Value as V},
};
use std::sync::Arc;

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
fn eq(left: Expr, right: Expr) -> Expr {
    compare(left, Compare::Eq, right)
}
fn yes() -> Expr {
    value(V::Bool(true))
}
fn many(cap: i64) -> Intent {
    Intent::Many {
        cap: ValueExpr::Literal(V::Int(cap)),
    }
}
fn projection(expression: Expr, alias: Option<&str>) -> Projection {
    Projection {
        expression,
        alias: alias.map(str::to_owned),
    }
}
fn update(values: Vec<(&str, Expr)>, filter: Expr, intent: Intent) -> Request {
    Request::UpdateExpressions {
        table: "items".into(),
        filter,
        guard: None,
        values: values.into_iter().map(|(k, v)| (k.into(), v)).collect(),
        intent,
    }
}
fn select(projection: Vec<Projection>, limit: Option<i64>) -> Request {
    Request::ReadExpressions {
        table: "items".into(),
        filter: None,
        order: vec![Sort {
            field: "id".into(),
            descending: false,
        }],
        limit: limit.map(|n| ValueExpr::Literal(V::Int(n))),
        projection,
    }
}
fn database(schema: &str) -> (Admin, Engine) {
    let admin = Admin::create(common::fresh("computed"), Limits::default()).unwrap();
    admin.sql_session().execute(schema, &[], None).unwrap();
    (admin.clone(), Engine::new(admin))
}
fn fixture() -> (Admin, Engine) {
    let (admin, engine) = database(
        "CREATE TABLE items (id BIGINT PRIMARY KEY,x BIGINT,y BIGINT,narrow SMALLINT,medium INTEGER,flag BOOLEAN,src TEXT,dest TEXT,raw BYTEA,rawcopy BYTEA,required TEXT NOT NULL)",
    );
    let mut sql = admin.sql_session();
    for (id, x, y, narrow, medium, flag, src, raw) in [
        (1, 5, 7, 32767, i32::MAX as i64, true, "alpha", vec![0, 255]),
        (2, -3, 2, -32768, i32::MIN as i64, false, "beta", vec![2, 3]),
    ] {
        sql.execute(
            "INSERT INTO items VALUES($1,$2,$3,$4,$5,$6,$7,'old',$8,NULL,'required')",
            &[
                V::Int(id),
                V::Int(x),
                V::Int(y),
                V::Int(narrow),
                V::Int(medium),
                V::Bool(flag),
                V::Text(src.into()),
                V::Bytes(raw),
            ],
            None,
        )
        .unwrap();
    }
    (admin, engine)
}
fn rows(admin: &Admin) -> Vec<SqlRow> {
    admin
        .sql_session()
        .execute("SELECT * FROM items ORDER BY id LIMIT 1024", &[], None)
        .unwrap()
        .rows
}
fn digest(admin: &Admin) -> [u8; 32] {
    admin.export(common::fresh("computed-export")).unwrap()
}
fn runtime_failure(admin: &Admin, engine: &Engine, request: &Request, expected: Error) {
    let before = digest(admin);
    let catalog = admin.catalog().unwrap();
    let existing = rows(admin);
    let handle = engine.prepare_bound(request, &Bindings::new()).unwrap();
    assert_eq!(engine.execute_bound(&handle), Err(expected.clone()));
    assert_eq!(engine.execute(request, &Bindings::new()), Err(expected));
    assert_eq!(digest(admin), before);
    assert_eq!(admin.catalog().unwrap(), catalog);
    assert_eq!(rows(admin), existing);
}

#[test]
fn computed_assignments_use_each_old_row_and_preserve_noop_versions() {
    let mut results = Vec::new();
    for reverse in [false, true] {
        let (admin, engine) = fixture();
        let before = rows(&admin);
        let mut assignments = vec![
            ("x", field("y")),
            ("y", binary(field("x"), Arithmetic::Add, int(1))),
            ("dest", field("src")),
            ("rawcopy", field("raw")),
            ("flag", compare(field("x"), Compare::Lt, field("y"))),
        ];
        if reverse {
            assignments.reverse();
        }
        let outcome = engine
            .execute(&update(assignments, yes(), many(2)), &Bindings::new())
            .unwrap();
        assert_eq!(
            (outcome.matched, outcome.affected, outcome.changed),
            (2, 2, 2)
        );
        let after = rows(&admin);
        for (index, (x, y)) in [(7, 6), (2, -2)].into_iter().enumerate() {
            assert_eq!(after[index].values[1..3], [V::Int(x), V::Int(y)]);
            assert_eq!(after[index].values[5], V::Bool(true));
            assert_eq!(after[index].values[7], before[index].values[6]);
            assert_eq!(after[index].values[9], before[index].values[8]);
            assert!(after[index].version > before[index].version);
        }
        let noop = engine
            .execute(
                &update(
                    vec![
                        ("x", field("x")),
                        ("dest", field("dest")),
                        ("rawcopy", field("rawcopy")),
                    ],
                    yes(),
                    many(2),
                ),
                &Bindings::new(),
            )
            .unwrap();
        assert_eq!((noop.matched, noop.affected, noop.changed), (2, 2, 0));
        assert_eq!(rows(&admin), after);
        results.push(after);
    }
    assert_eq!(results[0], results[1]);
}

#[test]
fn computed_assignment_binding_and_runtime_destination_checks_are_distinct() {
    let (admin, engine) = database(
        "CREATE TABLE items (id BIGINT PRIMARY KEY,narrow SMALLINT,medium INTEGER,required TEXT NOT NULL,src TEXT)",
    );
    for request in [
        update(vec![("absent", int(1))], yes(), many(0)),
        update(vec![("narrow", int(1)), ("narrow", int(2))], yes(), many(0)),
        update(vec![("id", int(1))], yes(), many(0)),
        update(vec![("narrow", value(V::Text("1".into())))], yes(), many(0)),
        update(vec![("narrow", field("absent"))], yes(), many(0)),
        update(vec![("narrow", parameter("missing"))], yes(), many(0)),
    ] {
        assert!(engine.prepare_bound(&request, &Bindings::new()).is_err());
        assert!(engine.execute(&request, &Bindings::new()).is_err());
    }
    for rhs in [int(32768), binary(int(1), Arithmetic::Divide, int(0))] {
        let request = update(vec![("narrow", rhs)], yes(), many(0));
        let prepared = engine.prepare_bound(&request, &Bindings::new()).unwrap();
        assert_eq!(engine.execute_bound(&prepared).unwrap().changed, 0);
    }
    let request = update(vec![("required", value(V::Null))], yes(), many(0));
    assert_eq!(
        engine.execute(&request, &Bindings::new()).unwrap().matched,
        0
    );
    admin
        .sql_session()
        .execute(
            "INSERT INTO items VALUES(1,32767,2147483647,'required',NULL)",
            &[],
            None,
        )
        .unwrap();
    for (target, rhs, error) in [
        (
            "narrow",
            binary(field("narrow"), Arithmetic::Add, int(1)),
            Error::Overflow,
        ),
        (
            "medium",
            binary(field("medium"), Arithmetic::Add, int(1)),
            Error::Overflow,
        ),
        ("required", field("src"), Error::Sql { sqlstate: "23502".into() }),
        ("required", value(V::Null), Error::Sql { sqlstate: "23502".into() }),
        (
            "narrow",
            binary(int(i64::MAX), Arithmetic::Add, int(1)),
            Error::Overflow,
        ),
    ] {
        runtime_failure(
            &admin,
            &engine,
            &update(vec![(target, rhs)], yes(), Intent::One),
            error,
        );
    }
    let accepted = update(
        vec![("narrow", int(-32768)), ("medium", int(i32::MIN as i64))],
        yes(),
        Intent::One,
    );
    assert_eq!(
        engine.execute(&accepted, &Bindings::new()).unwrap().changed,
        1
    );
    assert_eq!(
        rows(&admin)[0].values[1..3],
        [V::Int(-32768), V::Int(i32::MIN as i64)]
    );
}

#[test]
fn computed_write_cardinality_guards_and_late_errors_are_atomic() {
    let (admin, engine) = fixture();
    let fail = || binary(int(1), Arithmetic::Divide, int(0));
    for (filter, intent, error) in [
        (yes(), Intent::One, Error::AmbiguousMatch),
        (eq(field("id"), int(99)), Intent::One, Error::NotFound),
        (yes(), many(1), Error::TooManyMatches),
        (eq(field("id"), int(1)), Intent::One, Error::DivisionByZero),
    ] {
        runtime_failure(
            &admin,
            &engine,
            &update(vec![("x", fail())], filter, intent),
            error,
        );
    }
    let mut guarded = update(vec![("x", fail())], eq(field("id"), int(1)), Intent::One);
    if let Request::UpdateExpressions { guard, .. } = &mut guarded {
        *guard = Some(value(V::Bool(false)));
    }
    runtime_failure(&admin, &engine, &guarded, Error::ConditionFailed);
    let late = update(
        vec![(
            "x",
            binary(
                int(10),
                Arithmetic::Divide,
                binary(int(2), Arithmetic::Subtract, field("id")),
            ),
        )],
        yes(),
        many(2),
    );
    runtime_failure(&admin, &engine, &late, Error::DivisionByZero);
    let late_width = update(
        vec![(
            "narrow",
            binary(int(-32766), Arithmetic::Subtract, field("id")),
        )],
        yes(),
        many(2),
    );
    // First row stores -32767; second remains -32768, both representable.
    assert_eq!(
        engine
            .execute(&late_width, &Bindings::new())
            .unwrap()
            .changed,
        1
    );
    let late_width = update(
        vec![(
            "narrow",
            binary(int(-32767), Arithmetic::Subtract, field("id")),
        )],
        yes(),
        many(2),
    );
    runtime_failure(&admin, &engine, &late_width, Error::Overflow);
    // The common engine now admits guarded bulk mutations; claims remain
    // outside that guard contract.
    let mut request = update(
        vec![("x", int(1))],
        yes(),
        Intent::Claim {
            order: vec![Sort {
                field: "id".into(),
                descending: false,
            }],
        },
    );
    if let Request::UpdateExpressions { guard, .. } = &mut request {
        *guard = Some(yes());
    }
    assert_eq!(
        engine.prepare_bound(&request, &Bindings::new()).err(),
        Some(Error::Unsupported)
    );
    assert_eq!(
        engine.execute(&request, &Bindings::new()),
        Err(Error::Unsupported)
    );
}

#[test]
fn computed_claims_and_prepared_values_use_current_rows_and_frozen_arguments() {
    let (admin, engine) = fixture();
    let claim = |rhs| {
        update(
            vec![("x", rhs)],
            compare(
                binary(int(10), Arithmetic::Divide, field("x")),
                Compare::Gt,
                int(0),
            ),
            Intent::Claim {
                order: vec![Sort {
                    field: "id".into(),
                    descending: false,
                }],
            },
        )
    };
    runtime_failure(
        &admin,
        &engine,
        &claim(binary(field("x"), Arithmetic::Subtract, field("x"))),
        Error::DivisionByZero,
    );
    runtime_failure(&admin, &engine, &claim(field("x")), Error::ClaimNotConsumed);
    let before = rows(&admin);
    let result = engine
        .execute(
            &claim(binary(field("x"), Arithmetic::Subtract, int(10))),
            &Bindings::new(),
        )
        .unwrap();
    assert_eq!((result.matched, result.affected, result.changed), (1, 1, 1));
    assert_eq!(result.rows[0], rows(&admin)[0]);
    assert_eq!(result.rows[0].values[1], V::Int(-5));
    assert!(result.rows[0].version > before[0].version);
    assert_eq!(rows(&admin)[1], before[1]);
    let request = update(
        vec![("x", binary(field("x"), Arithmetic::Add, parameter("delta")))],
        eq(field("id"), int(1)),
        Intent::One,
    );
    let mut bindings = Bindings::from([("delta".into(), V::Int(2))]);
    let prepared = engine.prepare_bound(&request, &bindings).unwrap();
    bindings.insert("delta".into(), V::Int(100));
    admin
        .sql_session()
        .execute("UPDATE items SET x=20 WHERE id=1", &[], None)
        .unwrap();
    assert_eq!(engine.execute_bound(&prepared).unwrap().changed, 1);
    assert_eq!(rows(&admin)[0].values[1], V::Int(22));
    assert_eq!(engine.execute_bound(&prepared).unwrap().changed, 1);
    assert_eq!(rows(&admin)[0].values[1], V::Int(24));
    engine.execute(&request, &bindings).unwrap();
    assert_eq!(rows(&admin)[0].values[1], V::Int(124));
}

fn byte_fixture(source_bytes: usize, markers: &[usize], initial_noops: usize) -> (Admin, Engine) {
    let (admin, engine) =
        database("CREATE TABLE items (id BIGINT PRIMARY KEY,src TEXT,copy TEXT,marker TEXT)");
    let mut sql = admin.sql_session();
    sql.execute("BEGIN", &[], None).unwrap();
    for (i, marker) in markers.iter().enumerate() {
        let text = "x".repeat(source_bytes);
        sql.execute(
            "INSERT INTO items VALUES($1,$2,$3,$4)",
            &[
                V::Int(i as i64 + 1),
                V::Text(text.clone()),
                if i < initial_noops {
                    V::Text(text)
                } else {
                    V::Null
                },
                V::Text("m".repeat(*marker)),
            ],
            None,
        )
        .unwrap();
    }
    sql.execute("COMMIT", &[], None).unwrap();
    (admin, engine)
}

#[test]
fn computed_postimage_byte_budgets_cover_exact_bounds_and_noops() {
    assert_eq!(MAX_MUTATION_BYTES, MAX_RESULT_BYTES);
    let request = update(vec![("copy", field("src"))], yes(), many(17));
    let (admin, engine) = byte_fixture(32763, &[1], 0);
    assert_eq!(9 + 32763 * 2 + 1, MAX_ROW_BYTES);
    assert_eq!(
        engine.execute(&request, &Bindings::new()).unwrap().changed,
        1
    );
    assert_eq!(rows(&admin)[0].values[1], rows(&admin)[0].values[2]);
    let (admin, engine) = byte_fixture(32763, &[2], 0);
    runtime_failure(&admin, &engine, &request, Error::Limit);
    let mut markers = vec![1; 17];
    markers[16] = 17;
    assert_eq!(
        16 * (9 + 30835 * 2 + 1) + (9 + 30835 * 2 + 17),
        MAX_MUTATION_BYTES
    );
    let (admin, engine) = byte_fixture(30835, &markers, 8);
    let before = rows(&admin);
    let result = engine.execute(&request, &Bindings::new()).unwrap();
    assert_eq!(
        (result.matched, result.affected, result.changed),
        (17, 17, 9)
    );
    assert_eq!(&rows(&admin)[..8], &before[..8]);
    // The eight no-op postimages must still count. Skipping them would admit
    // this over-budget request even though input scan bytes remain below 1 MiB.
    markers[16] += 1;
    let (admin, engine) = byte_fixture(30835, &markers, 8);
    runtime_failure(&admin, &engine, &request, Error::Limit);
}

#[test]
fn computed_projection_metadata_aliases_and_source_scope_are_explicit() {
    let (admin, engine) = fixture();
    let fields = admin.catalog().unwrap().1[0].columns.clone();
    let request = select(
        vec![
            projection(field("narrow"), Some("small")),
            projection(binary(field("x"), Arithmetic::Add, int(1)), Some("next")),
            projection(compare(field("x"), Compare::Lt, field("y")), Some("less")),
            projection(value(V::Text("constant".into())), Some("label")),
            projection(value(V::Bytes(vec![0, 255])), Some("bytes")),
            projection(
                Expr::IsNull(Box::new(field("dest")), false),
                Some("missing"),
            ),
            projection(
                binary(value(V::Null), Arithmetic::Add, int(1)),
                Some("nullable_int"),
            ),
        ],
        None,
    );
    let prepared = engine.prepare_bound(&request, &Bindings::new()).unwrap();
    let columns = &prepared.description().columns;
    let mut source = fields[3].clone();
    source.name = "small".into();
    assert_eq!(columns[0], source);
    for (i, data_type) in [
        (1, DataType::BigInt),
        (2, DataType::Boolean),
        (3, DataType::Text),
        (4, DataType::Bytes),
        (5, DataType::Boolean),
        (6, DataType::BigInt),
    ] {
        assert_eq!(columns[i].id, 0);
        assert_eq!(columns[i].data_type, data_type);
    }
    assert!(!columns[1].not_null);
    assert!(!columns[2].not_null);
    assert!(columns[5].not_null);
    assert!(!columns[6].not_null);
    let result = engine.execute_bound(&prepared).unwrap();
    assert_eq!(
        result.columns,
        [
            "small",
            "next",
            "less",
            "label",
            "bytes",
            "missing",
            "nullable_int"
        ]
    );
    assert_eq!(
        result.rows[0].values,
        [
            V::Int(32767),
            V::Int(6),
            V::Bool(true),
            V::Text("constant".into()),
            V::Bytes(vec![0, 255]),
            V::Bool(false),
            V::Null
        ]
    );
    assert_eq!(result.rows[0].version, rows(&admin)[0].version);
    let mut scoped = select(
        vec![projection(Expr::Negate(Box::new(field("x"))), Some("y"))],
        None,
    );
    if let Request::ReadExpressions { order, .. } = &mut scoped {
        *order = vec![Sort {
            field: "y".into(),
            descending: false,
        }];
    }
    assert_eq!(
        engine
            .execute(&scoped, &Bindings::new())
            .unwrap()
            .rows
            .into_iter()
            .map(|r| r.values)
            .collect::<Vec<_>>(),
        vec![vec![V::Int(3)], vec![V::Int(-5)]]
    );
    for mut invalid in [
        select(vec![projection(int(1), None)], None),
        select(vec![projection(value(V::Null), Some("untyped"))], None),
        select(
            vec![
                projection(field("x"), Some("same")),
                projection(field("y"), Some("same")),
            ],
            None,
        ),
        select(
            vec![
                projection(field("x"), Some("y")),
                projection(field("y"), None),
            ],
            None,
        ),
        select(vec![projection(field("absent"), Some("a"))], None),
    ] {
        assert!(engine.prepare_bound(&invalid, &Bindings::new()).is_err());
        if let Request::ReadExpressions { filter, .. } = &mut invalid {
            *filter = Some(value(V::Bool(false)));
        }
        assert!(engine.execute(&invalid, &Bindings::new()).is_err());
    }
    for order_alias in [false, true] {
        let mut invalid = select(
            vec![projection(
                binary(field("x"), Arithmetic::Add, int(1)),
                Some("computed"),
            )],
            None,
        );
        if let Request::ReadExpressions { filter, order, .. } = &mut invalid {
            if order_alias {
                *order = vec![Sort {
                    field: "computed".into(),
                    descending: false,
                }];
            } else {
                *filter = Some(eq(field("computed"), int(6)));
            }
        }
        assert_eq!(
            engine.prepare_bound(&invalid, &Bindings::new()).err(),
            Some(Error::Sql { sqlstate: "42703".into() })
        );
    }
    assert_eq!(
        engine
            .prepare_bound(
                &select(vec![projection(value(V::Null), Some("n"))], None),
                &Bindings::new()
            )
            .err(),
        Some(Error::Type)
    );
}

#[test]
fn computed_projection_errors_and_bytes_cover_all_eligible_rows_before_paging() {
    let (admin, engine) = fixture();
    for limit in [Some(0), Some(1), None] {
        let request = select(
            vec![projection(
                binary(
                    int(1),
                    Arithmetic::Divide,
                    binary(field("id"), Arithmetic::Subtract, int(2)),
                ),
                Some("quotient"),
            )],
            limit,
        );
        runtime_failure(&admin, &engine, &request, Error::DivisionByZero);
    }
    let mut filtered = select(
        vec![projection(
            binary(
                int(1),
                Arithmetic::Divide,
                binary(field("id"), Arithmetic::Subtract, int(2)),
            ),
            Some("quotient"),
        )],
        Some(1),
    );
    if let Request::ReadExpressions { filter, .. } = &mut filtered {
        *filter = Some(eq(field("id"), int(1)));
    }
    assert_eq!(
        engine.execute(&filtered, &Bindings::new()).unwrap().rows[0].values,
        [V::Int(-1)]
    );

    // Repeated aliased source fields may produce a >64 KiB output row: only
    // the full eligible projection's 1 MiB aggregate bound applies here.
    for extra in [0, 1] {
        let (admin, engine) =
            database("CREATE TABLE items (id BIGINT PRIMARY KEY,a BYTEA,b BYTEA)");
        admin
            .sql_session()
            .execute(
                "INSERT INTO items VALUES(1,$1,$2)",
                &[V::Bytes(vec![7; 16384]), V::Bytes(vec![8; 16384 + extra])],
                None,
            )
            .unwrap();
        let projections = (0..64)
            .map(|i| Projection {
                expression: field(if i == 63 { "b" } else { "a" }),
                alias: Some(format!("c{i}")),
            })
            .collect::<Vec<_>>();
        assert_eq!(63 * 16384 + 16384 + extra, MAX_RESULT_BYTES + extra);
        for limit in [Some(0), Some(1)] {
            let request = select(projections.clone(), limit);
            if extra == 0 {
                let result = engine.execute(&request, &Bindings::new()).unwrap();
                assert_eq!(result.matched, 1);
                assert_eq!(result.rows.len(), usize::from(limit == Some(1)));
                if let Some(row) = result.rows.first() {
                    assert_eq!(row.values.len(), 64);
                }
            } else {
                runtime_failure(&admin, &engine, &request, Error::Limit);
            }
        }
    }
    // A single admitted parameter expands once per eligible row, independently
    // of the presentation page and the binder's once-per-reference budget.
    let (admin, engine) = database("CREATE TABLE items (id BIGINT PRIMARY KEY)");
    let mut sql = admin.sql_session();
    sql.execute("BEGIN", &[], None).unwrap();
    for id in 1..=32 {
        sql.execute("INSERT INTO items VALUES($1)", &[V::Int(id)], None)
            .unwrap();
    }
    sql.execute("COMMIT", &[], None).unwrap();
    let request = select(
        vec![projection(parameter("payload"), Some("payload"))],
        Some(0),
    );
    let bindings = Bindings::from([("payload".into(), V::Text("x".repeat(32768)))]);
    let bound = engine.prepare_bound(&request, &bindings).unwrap();
    assert_eq!(engine.execute_bound(&bound).unwrap().matched, 32);
    sql.execute("INSERT INTO items VALUES(33)", &[], None)
        .unwrap();
    let before = digest(&admin);
    assert_eq!(engine.execute_bound(&bound), Err(Error::Limit));
    assert_eq!(engine.execute(&request, &bindings), Err(Error::Limit));
    assert_eq!(digest(&admin), before);
}

#[test]
fn computed_expression_roots_share_binding_depth_node_and_projection_limits() {
    let (admin, engine) = fixture();
    let before = digest(&admin);
    let mut deep = int(1);
    for _ in 0..=MAX_DEPTH {
        deep = Expr::Negate(Box::new(deep));
    }
    for request in [
        update(vec![("x", deep.clone())], yes(), many(2)),
        select(vec![projection(deep, Some("deep"))], None),
        update(
            vec![("dest", value(V::Text("x".repeat(MAX_INPUT))))],
            yes(),
            many(2),
        ),
    ] {
        assert_eq!(
            engine.prepare_bound(&request, &Bindings::new()).err(),
            Some(Error::Limit)
        );
        assert_eq!(
            engine.execute(&request, &Bindings::new()),
            Err(Error::Limit)
        );
    }
    let mut terms: Vec<_> = (0..MAX_EXPRESSION_NODES).map(|_| int(1)).collect();
    while terms.len() > 1 {
        let mut next = Vec::new();
        let mut it = terms.into_iter();
        while let Some(a) = it.next() {
            next.push(match it.next() {
                Some(b) => binary(a, Arithmetic::Add, b),
                None => a,
            });
        }
        terms = next;
    }
    assert_eq!(
        engine
            .prepare_bound(
                &update(vec![("x", terms.pop().unwrap())], yes(), many(2)),
                &Bindings::new()
            )
            .err(),
        Some(Error::Limit)
    );
    let too_many = (0..65)
        .map(|i| Projection {
            expression: field("x"),
            alias: Some(format!("c{i}")),
        })
        .collect();
    assert!(
        engine
            .prepare_bound(&select(too_many, None), &Bindings::new())
            .is_err()
    );
    let bindings = Bindings::from([("p".into(), V::Text("x".repeat(MAX_BINDING_BYTES / 2 + 1)))]);
    for request in [
        update(
            vec![("dest", parameter("p")), ("src", parameter("p"))],
            yes(),
            many(2),
        ),
        select(
            vec![
                projection(parameter("p"), Some("a")),
                projection(parameter("p"), Some("b")),
            ],
            None,
        ),
    ] {
        assert_eq!(
            engine.prepare_bound(&request, &bindings).err(),
            Some(Error::Limit)
        );
        assert_eq!(engine.execute(&request, &bindings), Err(Error::Limit));
    }
    assert_eq!(digest(&admin), before);
}

#[test]
fn computed_writes_preserve_cancellation_decision_boundary() {
    for (point, wins) in [
        (DecisionPoint::BeforeDecision, true),
        (DecisionPoint::Decided, false),
    ] {
        let (admin, engine) = fixture();
        let before = rows(&admin);
        let before_digest = digest(&admin);
        admin.set_decision_hook(Some(Arc::new(move |current| {
            if current == point {
                assert_eq!(Cancellation::current().unwrap().cancel(), wins);
            }
        })));
        let request = update(
            vec![("x", binary(field("x"), Arithmetic::Add, int(1)))],
            yes(),
            many(2),
        );
        let result = Cancellation::default().scope(|| engine.execute(&request, &Bindings::new()));
        admin.set_decision_hook(None);
        if wins {
            assert_eq!(result, Err(Error::Core(CoreError::Cancelled.to_string())));
            assert_eq!(rows(&admin), before);
            assert_eq!(digest(&admin), before_digest);
        } else {
            assert_eq!(result.unwrap().changed, 2);
            assert_eq!(rows(&admin)[0].values[1], V::Int(6));
        }
    }
}
