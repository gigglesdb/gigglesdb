mod common;

use giggles_core::{
    Admin, CrashPoint, Limits,
    operations::{
        Arithmetic, Bindings, Compare, Engine, Error, Expr, Intent, MAX_BINDING_BYTES, MAX_DEPTH,
        MAX_EXPRESSION_NODES, MAX_INPUT, Projection, Request, Sort, ValueExpr,
    },
    relational::{DataType, MAX_RESULT_BYTES, SqlRow, Value as V},
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
fn equal(a: Expr, b: Expr) -> Expr {
    Expr::Compare(Box::new(a), Compare::Eq, Box::new(b))
}
fn yes() -> Expr {
    value(V::Bool(true))
}
fn many(n: i64) -> Intent {
    Intent::Many {
        cap: ValueExpr::Literal(V::Int(n)),
    }
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
fn update(filter: Expr, intent: Intent, values: Vec<(&str, Expr)>) -> Request {
    Request::UpdateExpressions {
        table: "items".into(),
        filter,
        guard: None,
        intent,
        values: values.into_iter().map(|(n, e)| (n.into(), e)).collect(),
    }
}
fn delete(filter: Expr, cap: Option<i64>) -> Request {
    Request::Delete {
        table: "items".into(),
        filter,
        guard: None,
        cap: cap.map(|n| ValueExpr::Literal(V::Int(n))),
    }
}
fn database(schema: &str) -> (Admin, Engine) {
    let admin = Admin::create(common::fresh("returning"), Limits::default()).unwrap();
    admin.sql_session().execute(schema, &[], None).unwrap();
    (admin.clone(), Engine::new(admin))
}
fn fixture() -> (Admin, Engine) {
    let (admin, engine) =
        database("CREATE TABLE items (id BIGINT PRIMARY KEY,x BIGINT,state TEXT NOT NULL)");
    for sql in [
        "INSERT INTO items VALUES(1,10,'ready')",
        "INSERT INTO items VALUES(2,20,'ready')",
    ] {
        admin.sql_session().execute(sql, &[], None).unwrap();
    }
    (admin, engine)
}
fn rows(admin: &Admin) -> Vec<SqlRow> {
    admin
        .sql_session()
        .execute("SELECT * FROM items LIMIT 1024", &[], None)
        .unwrap()
        .rows
}
fn digest(admin: &Admin) -> [u8; 32] {
    admin.export(common::fresh("returning-export")).unwrap()
}
fn rejects_atomically(
    admin: &Admin,
    engine: &Engine,
    request: &Request,
    bindings: &Bindings,
    error: Error,
) {
    let before = digest(admin);
    let catalog = admin.catalog().unwrap();
    let existing = rows(admin);
    let bound = engine.prepare_bound(request, bindings).unwrap();
    assert_eq!(engine.execute_bound(&bound), Err(error.clone()));
    assert_eq!(engine.execute(request, bindings), Err(error));
    assert_eq!(digest(admin), before);
    assert_eq!(admin.catalog().unwrap(), catalog);
    assert_eq!(rows(admin), existing);
}

#[test]
fn returning_all_write_intents_report_correct_images_and_actual_versions() {
    let (admin, engine) = fixture();
    let insert = Request::Insert {
        table: "items".into(),
        values: vec![
            ("id".into(), ValueExpr::Literal(V::Int(3))),
            ("x".into(), ValueExpr::Literal(V::Int(30))),
            ("state".into(), ValueExpr::Literal(V::Text("ready".into()))),
        ],
    };
    let insert = returning(insert, vec![]);
    let bound = engine.prepare_bound(&insert, &Bindings::new()).unwrap();
    assert_eq!(
        bound.description().columns,
        admin.catalog().unwrap().1[0].columns
    );
    let inserted = engine.execute_bound(&bound).unwrap();
    assert_eq!(inserted.columns, ["id", "x", "state"]);
    assert_eq!(inserted.rows, [rows(&admin)[2].clone()]);
    assert_eq!(
        (inserted.matched, inserted.affected, inserted.changed),
        (1, 1, 1)
    );
    assert!(inserted.complete);
    let before = rows(&admin)[0].clone();
    let request = returning(
        update(
            equal(field("id"), int(1)),
            Intent::One,
            vec![("x", binary(field("x"), Arithmetic::Add, int(5)))],
        ),
        vec![
            projection(field("x"), Some("current")),
            projection(binary(field("x"), Arithmetic::Add, int(1)), Some("next")),
        ],
    );
    let bound = engine.prepare_bound(&request, &Bindings::new()).unwrap();
    let columns = &bound.description().columns;
    assert_eq!(columns[0].id, admin.catalog().unwrap().1[0].columns[1].id);
    assert_eq!(columns[0].name, "current");
    assert_eq!(columns[1].id, 0);
    assert_eq!(columns[1].data_type, DataType::BigInt);
    let updated = engine.execute_bound(&bound).unwrap();
    let persisted = rows(&admin)[0].clone();
    assert_eq!(updated.rows[0].values, [V::Int(15), V::Int(16)]);
    assert_eq!(updated.rows[0].version, persisted.version);
    assert!(persisted.version > before.version);
    let legacy = Request::Update {
        table: "items".into(),
        filter: equal(field("id"), int(1)),
        guard: None,
        intent: Intent::One,
        values: vec![("x".into(), ValueExpr::Literal(V::Int(42)))],
    };
    let legacy = returning(legacy, vec![]);
    let changed = engine.execute(&legacy, &Bindings::new()).unwrap();
    assert_eq!(changed.rows, [rows(&admin)[0].clone()]);
    assert_eq!(changed.changed, 1);
    let no_op = engine.execute(&legacy, &Bindings::new()).unwrap();
    assert_eq!((no_op.matched, no_op.affected, no_op.changed), (1, 1, 0));
    assert_eq!(no_op.rows, changed.rows);
    let before_delete = rows(&admin)[1].clone();
    let deleted = engine
        .execute(
            &returning(delete(equal(field("id"), int(2)), None), vec![]),
            &Bindings::new(),
        )
        .unwrap();
    assert_eq!(deleted.rows, [before_delete]);
    assert_eq!(deleted.changed, 1);
    assert!(!rows(&admin).iter().any(|r| r.values[0] == V::Int(2)));
    let claim = Request::Update {
        table: "items".into(),
        filter: equal(field("state"), value(V::Text("ready".into()))),
        guard: None,
        intent: Intent::Claim {
            order: vec![Sort {
                field: "id".into(),
                descending: true,
            }],
        },
        values: vec![(
            "state".into(),
            ValueExpr::Literal(V::Text("claimed".into())),
        )],
    };
    let claimed = engine
        .execute(
            &returning(
                claim,
                vec![
                    projection(field("id"), None),
                    projection(field("state"), Some("status")),
                ],
            ),
            &Bindings::new(),
        )
        .unwrap();
    assert_eq!(claimed.columns, ["id", "status"]);
    assert_eq!(
        claimed.rows[0].values,
        [V::Int(3), V::Text("claimed".into())]
    );
    assert_eq!(claimed.rows[0].version, rows(&admin)[1].version);
    assert_eq!(claimed.rows.len(), 1);
}

#[test]
fn returning_bulk_is_complete_in_composite_primary_key_order() {
    let (admin, engine) =
        database("CREATE TABLE items (tenant BIGINT,id BIGINT,n BIGINT,PRIMARY KEY(tenant,id))");
    let mut sql = admin.sql_session();
    sql.execute("BEGIN", &[], None).unwrap();
    for i in (0..110).rev() {
        sql.execute(
            "INSERT INTO items VALUES($1,$2,$3)",
            &[V::Int(i / 11), V::Int(i % 11), V::Int(i)],
            None,
        )
        .unwrap();
    }
    sql.execute("COMMIT", &[], None).unwrap();
    let projection = vec![
        projection(field("tenant"), None),
        projection(field("id"), None),
        projection(field("n"), None),
    ];
    let result = engine
        .execute(
            &returning(
                update(
                    yes(),
                    many(110),
                    vec![("n", binary(field("n"), Arithmetic::Add, int(1)))],
                ),
                projection,
            ),
            &Bindings::new(),
        )
        .unwrap();
    assert_eq!(
        (result.matched, result.affected, result.changed),
        (110, 110, 110)
    );
    assert_eq!(result.rows.len(), 110);
    assert!(result.complete);
    assert_eq!(result.rows, rows(&admin));
    for (i, row) in result.rows.iter().enumerate() {
        assert_eq!(
            row.values,
            [
                V::Int(i as i64 / 11),
                V::Int(i as i64 % 11),
                V::Int(i as i64 + 1)
            ]
        );
    }
    let before = rows(&admin);
    let deleted = engine
        .execute(
            &returning(delete(yes(), Some(110)), vec![]),
            &Bindings::new(),
        )
        .unwrap();
    assert_eq!(deleted.rows, before);
    assert_eq!(deleted.rows.len(), 110);
    assert!(deleted.complete);
    assert!(rows(&admin).is_empty());
}

#[test]
fn returning_projection_failures_and_write_constraints_are_atomic() {
    let (admin, engine) = fixture();
    let bad = || {
        vec![projection(
            binary(
                int(10),
                Arithmetic::Divide,
                binary(field("id"), Arithmetic::Subtract, int(2)),
            ),
            Some("quotient"),
        )]
    };
    for inner in [
        update(
            yes(),
            many(2),
            vec![("x", binary(field("x"), Arithmetic::Add, int(1)))],
        ),
        delete(yes(), Some(2)),
    ] {
        rejects_atomically(
            &admin,
            &engine,
            &returning(inner, bad()),
            &Bindings::new(),
            Error::DivisionByZero,
        );
    }
    let fail = || {
        vec![projection(
            binary(int(1), Arithmetic::Divide, int(0)),
            Some("bad"),
        )]
    };
    for (filter, intent, error) in [
        (yes(), Intent::One, Error::AmbiguousMatch),
        (equal(field("id"), int(99)), Intent::One, Error::NotFound),
        (yes(), many(1), Error::TooManyMatches),
    ] {
        rejects_atomically(
            &admin,
            &engine,
            &returning(update(filter, intent, vec![("x", int(1))]), fail()),
            &Bindings::new(),
            error,
        );
    }
    let mut guarded = update(equal(field("id"), int(1)), Intent::One, vec![("x", int(1))]);
    if let Request::UpdateExpressions { guard, .. } = &mut guarded {
        *guard = Some(value(V::Bool(false)));
    }
    rejects_atomically(
        &admin,
        &engine,
        &returning(guarded, fail()),
        &Bindings::new(),
        Error::ConditionFailed,
    );
    let insert = Request::Insert {
        table: "items".into(),
        values: vec![
            ("id".into(), ValueExpr::Literal(V::Int(9))),
            ("x".into(), ValueExpr::Literal(V::Int(9))),
            ("state".into(), ValueExpr::Literal(V::Text("ready".into()))),
        ],
    };
    rejects_atomically(
        &admin,
        &engine,
        &returning(insert, fail()),
        &Bindings::new(),
        Error::DivisionByZero,
    );
    for inner in [
        update(equal(field("id"), int(99)), many(0), vec![("x", int(1))]),
        delete(equal(field("id"), int(99)), Some(0)),
        update(
            equal(field("id"), int(99)),
            Intent::Claim {
                order: vec![Sort {
                    field: "id".into(),
                    descending: false,
                }],
            },
            vec![("x", int(1))],
        ),
    ] {
        let request = returning(inner, fail());
        let prepared = engine.prepare_bound(&request, &Bindings::new()).unwrap();
        let result = engine.execute_bound(&prepared).unwrap();
        assert_eq!(result.columns, ["bad"]);
        assert_eq!((result.matched, result.affected, result.changed), (0, 0, 0));
        assert!(result.rows.is_empty());
        assert!(result.complete);
    }
    for projection in [
        vec![projection(field("absent"), None)],
        vec![projection(value(V::Null), Some("untyped"))],
        vec![
            projection(field("x"), Some("same")),
            projection(field("id"), Some("same")),
        ],
    ] {
        let request = returning(delete(equal(field("id"), int(99)), Some(0)), projection);
        assert!(engine.prepare_bound(&request, &Bindings::new()).is_err());
        assert!(engine.execute(&request, &Bindings::new()).is_err());
    }
    let (admin, engine) = database(
        "CREATE TABLE items (id BIGINT PRIMARY KEY,n SMALLINT,required TEXT NOT NULL,optional TEXT)",
    );
    admin
        .sql_session()
        .execute(
            "INSERT INTO items VALUES(1,32767,'required',NULL)",
            &[],
            None,
        )
        .unwrap();
    for (column, rhs) in [
        ("n", binary(field("n"), Arithmetic::Add, int(1))),
        ("required", field("optional")),
    ] {
        let request = returning(update(yes(), Intent::One, vec![(column, rhs)]), vec![]);
        rejects_atomically(&admin, &engine, &request, &Bindings::new(), if column == "n" { Error::Overflow } else { Error::Sql { sqlstate: "23502".into() } });
    }
}

#[test]
fn returning_output_byte_limit_is_exact_and_rolls_back_every_write() {
    for extra in [0, 1] {
        for mode in ["insert", "update", "delete"] {
            let (admin, engine) =
                database("CREATE TABLE items (id BIGINT PRIMARY KEY,n BIGINT,a BYTEA,b BYTEA)");
            let bindings = Bindings::from([
                ("a".into(), V::Bytes(vec![7; 16384])),
                ("b".into(), V::Bytes(vec![8; 16384 + extra])),
            ]);
            if mode != "insert" {
                admin
                    .sql_session()
                    .execute(
                        "INSERT INTO items VALUES(1,0,$1,$2)",
                        &[bindings["a"].clone(), bindings["b"].clone()],
                        None,
                    )
                    .unwrap();
            }
            let before = rows(&admin);
            let inner = match mode {
                "insert" => Request::Insert {
                    table: "items".into(),
                    values: vec![
                        ("id".into(), ValueExpr::Literal(V::Int(1))),
                        ("n".into(), ValueExpr::Literal(V::Int(1))),
                        ("a".into(), ValueExpr::Parameter("a".into())),
                        ("b".into(), ValueExpr::Parameter("b".into())),
                    ],
                },
                "update" => update(yes(), Intent::One, vec![("n", int(1))]),
                _ => delete(yes(), None),
            };
            let projection = (0..64)
                .map(|i| Projection {
                    expression: field(if i == 63 { "b" } else { "a" }),
                    alias: Some(format!("c{i}")),
                })
                .collect();
            let request = returning(inner, projection);
            let actual_bindings = if mode == "insert" {
                bindings
            } else {
                Bindings::new()
            };
            assert_eq!(63 * 16384 + 16384 + extra, MAX_RESULT_BYTES + extra);
            if extra == 1 {
                rejects_atomically(&admin, &engine, &request, &actual_bindings, Error::Limit);
            } else {
                let result = engine.execute(&request, &actual_bindings).unwrap();
                assert_eq!(result.rows.len(), 1);
                assert_eq!(result.rows[0].values.len(), 64);
                assert_eq!(result.changed, 1);
                assert!(result.complete);
                let version = if mode == "delete" {
                    before[0].version
                } else {
                    rows(&admin)[0].version
                };
                assert_eq!(result.rows[0].version, version);
            }
        }
    }
    let (admin, engine) = database("CREATE TABLE items (id BIGINT PRIMARY KEY,a BYTEA,b BYTEA)");
    admin
        .sql_session()
        .execute(
            "INSERT INTO items VALUES(1,$1,$2)",
            &[V::Bytes(vec![0; 16384]), V::Bytes(vec![1; 16385])],
            None,
        )
        .unwrap();
    let projection = (0..64)
        .map(|i| Projection {
            expression: field(if i == 63 { "b" } else { "a" }),
            alias: Some(format!("c{i}")),
        })
        .collect();
    rejects_atomically(
        &admin,
        &engine,
        &returning(
            update(yes(), Intent::One, vec![("a", field("a"))]),
            projection,
        ),
        &Bindings::new(),
        Error::Limit,
    );
}

#[test]
fn returning_bound_values_are_frozen_but_rows_and_output_expansion_are_fresh() {
    let (admin, engine) = fixture();
    let request = returning(
        update(
            equal(field("id"), int(1)),
            Intent::One,
            vec![("x", binary(field("x"), Arithmetic::Add, parameter("delta")))],
        ),
        vec![
            projection(field("x"), Some("current")),
            projection(parameter("label"), Some("label")),
        ],
    );
    let mut bindings = Bindings::from([
        ("delta".into(), V::Int(2)),
        ("label".into(), V::Text("frozen".into())),
    ]);
    let prepared = engine.prepare_bound(&request, &bindings).unwrap();
    bindings.insert("delta".into(), V::Int(99));
    bindings.insert("label".into(), V::Text("new".into()));
    admin
        .sql_session()
        .execute("UPDATE items SET x=40 WHERE id=1", &[], None)
        .unwrap();
    for expected in [42, 44] {
        let result = engine.execute_bound(&prepared).unwrap();
        assert_eq!(
            result.rows[0].values,
            [V::Int(expected), V::Text("frozen".into())]
        );
        assert_eq!(result.rows[0].version, rows(&admin)[0].version);
    }
    assert_eq!(
        engine.execute(&request, &bindings).unwrap().rows[0].values,
        [V::Int(143), V::Text("new".into())]
    );
    let (admin, engine) = database("CREATE TABLE items (id BIGINT PRIMARY KEY,x BIGINT)");
    let mut sql = admin.sql_session();
    sql.execute("BEGIN", &[], None).unwrap();
    for id in 1..=32 {
        sql.execute("INSERT INTO items VALUES($1,0)", &[V::Int(id)], None)
            .unwrap();
    }
    sql.execute("COMMIT", &[], None).unwrap();
    let request = returning(
        update(
            yes(),
            many(33),
            vec![("x", binary(field("x"), Arithmetic::Add, int(1)))],
        ),
        vec![projection(parameter("payload"), Some("payload"))],
    );
    let mut bindings = Bindings::from([("payload".into(), V::Text("x".repeat(32768)))]);
    let prepared = engine.prepare_bound(&request, &bindings).unwrap();
    let result = engine.execute_bound(&prepared).unwrap();
    assert_eq!(result.rows.len(), 32);
    assert_eq!(result.changed, 32);
    assert!(result.complete);
    sql.execute("INSERT INTO items VALUES(33,0)", &[], None)
        .unwrap();
    let before = digest(&admin);
    let existing = rows(&admin);
    assert_eq!(engine.execute_bound(&prepared), Err(Error::Limit));
    assert_eq!(digest(&admin), before);
    assert_eq!(rows(&admin), existing);
    let capped = returning(
        update(
            yes(),
            many(32),
            vec![("x", binary(field("x"), Arithmetic::Add, int(1)))],
        ),
        vec![projection(parameter("payload"), Some("payload"))],
    );
    assert_eq!(
        engine.execute(&capped, &bindings),
        Err(Error::TooManyMatches)
    );
    assert_eq!(digest(&admin), before);
    bindings.insert("payload".into(), V::Text("small".into()));
    assert_eq!(engine.execute(&request, &bindings).unwrap().rows.len(), 33);
}

fn balanced(mut terms: Vec<Expr>) -> Expr {
    while terms.len() > 1 {
        let mut next = Vec::new();
        let mut iter = terms.into_iter();
        while let Some(a) = iter.next() {
            next.push(match iter.next() {
                Some(b) => Expr::And(Box::new(a), Box::new(b)),
                None => a,
            });
        }
        terms = next;
    }
    terms.pop().unwrap()
}

#[test]
fn returning_wrapper_shares_binding_node_and_depth_budgets() {
    let (admin, engine) =
        database("CREATE TABLE items (id BIGINT PRIMARY KEY,flag BOOLEAN,body TEXT)");
    let inner_filter = balanced((0..1024).map(|_| field("flag")).collect());
    let returned = inner_filter.clone();
    let inner = Request::Update {
        table: "items".into(),
        filter: inner_filter,
        guard: None,
        intent: many(0),
        values: vec![("flag".into(), ValueExpr::Literal(V::Bool(true)))],
    };
    let mut projections = vec![
        projection(returned, Some("all")),
        projection(field("flag"), Some("a")),
        projection(field("flag"), Some("b")),
    ];
    assert_eq!(2047 + 2047 + 2, MAX_EXPRESSION_NODES);
    let admitted = returning(inner.clone(), projections.clone());
    assert!(serde_json::to_vec(&admitted).unwrap().len() < MAX_INPUT);
    engine.prepare_bound(&admitted, &Bindings::new()).unwrap();
    projections.push(projection(field("flag"), Some("c")));
    let rejected = returning(inner, projections);
    assert_eq!(
        engine.prepare_bound(&rejected, &Bindings::new()).err(),
        Some(Error::Limit)
    );
    assert_eq!(
        engine.execute(&rejected, &Bindings::new()),
        Err(Error::Limit)
    );
    let mut depth = field("flag");
    for _ in 0..MAX_DEPTH {
        depth = Expr::Not(Box::new(depth));
    }
    let inner = Request::Update {
        table: "items".into(),
        filter: depth.clone(),
        guard: None,
        intent: many(0),
        values: vec![("flag".into(), ValueExpr::Literal(V::Bool(true)))],
    };
    engine
        .prepare_bound(
            &returning(inner, vec![projection(depth.clone(), Some("deep"))]),
            &Bindings::new(),
        )
        .unwrap();
    let too_deep = returning(
        delete(yes(), Some(0)),
        vec![projection(Expr::Not(Box::new(depth)), Some("deep"))],
    );
    assert_eq!(
        engine.prepare_bound(&too_deep, &Bindings::new()).err(),
        Some(Error::Limit)
    );
    let oversized = returning(
        delete(yes(), Some(0)),
        vec![projection(
            value(V::Text("x".repeat(MAX_INPUT))),
            Some("huge"),
        )],
    );
    assert_eq!(
        engine.execute(&oversized, &Bindings::new()),
        Err(Error::Limit)
    );
    let many_columns = returning(
        delete(yes(), Some(0)),
        (0..=giggles_core::relational::MAX_TARGET_COLUMNS)
            .map(|i| Projection {
                expression: field("flag"),
                alias: Some(format!("c{i}")),
            })
            .collect(),
    );
    assert_eq!(
        engine.prepare_bound(&many_columns, &Bindings::new()).err(),
        Some(Error::Limit)
    );
    admin
        .sql_session()
        .execute("INSERT INTO items VALUES(1,true,'old')", &[], None)
        .unwrap();
    let binding = Bindings::from([("p".into(), V::Text("x".repeat(32763)))]);
    let request = |tag: &str| {
        returning(
            Request::Update {
                table: "items".into(),
                filter: equal(field("id"), int(1)),
                guard: None,
                intent: Intent::One,
                values: vec![("body".into(), ValueExpr::Parameter("p".into()))],
            },
            vec![
                projection(parameter("p"), Some("body")),
                projection(value(V::Text(tag.into())), Some("tag")),
            ],
        )
    };
    assert_eq!(32763 * 2 + 9 + 1, MAX_BINDING_BYTES);
    engine.prepare_bound(&request("z"), &binding).unwrap();
    assert_eq!(engine.execute(&request("z"), &binding).unwrap().changed, 1);
    let before = digest(&admin);
    assert_eq!(
        engine.prepare_bound(&request("zz"), &binding).err(),
        Some(Error::Limit)
    );
    assert_eq!(engine.execute(&request("zz"), &binding), Err(Error::Limit));
    assert_eq!(digest(&admin), before);
}

#[test]
fn returning_wrapper_rejects_reads_and_nested_shapes_without_effects() {
    let (admin, engine) = fixture();
    let before = digest(&admin);
    let read = Request::Read {
        table: "items".into(),
        filter: None,
        order: vec![],
        limit: None,
        projection: vec![],
    };
    let read_expressions = Request::ReadExpressions {
        table: "items".into(),
        filter: None,
        order: vec![],
        limit: None,
        projection: vec![projection(field("x"), None)],
    };
    let mut nested = delete(equal(field("id"), int(1)), None);
    for _ in 0..MAX_DEPTH + 4 {
        nested = returning(nested, vec![]);
    }
    for request in [
        returning(read, vec![]),
        returning(read_expressions, vec![]),
        nested,
    ] {
        assert_eq!(
            engine.prepare_bound(&request, &Bindings::new()).err(),
            Some(Error::Unsupported)
        );
        assert_eq!(
            engine.execute(&request, &Bindings::new()),
            Err(Error::Unsupported)
        );
    }
    assert_eq!(digest(&admin), before);
    // Empty projections mean star only on the write wrapper.
    let read = Request::ReadExpressions {
        table: "items".into(),
        filter: None,
        order: vec![],
        limit: None,
        projection: vec![],
    };
    assert!(matches!(
        engine.prepare_bound(&read, &Bindings::new()).err(),
        Some(Error::Bind(_))
    ));
}

#[test]
fn returning_commit_storage_failure_never_exposes_a_successful_payload() {
    for (point, persisted) in [
        (CrashPoint::ErrorBeforeCommit, false),
        (CrashPoint::ErrorAfterCommit, true),
    ] {
        let path = common::fresh("returning-unknown");
        let admin = Admin::create(&path, Limits::default()).unwrap();
        let mut sql = admin.sql_session();
        sql.execute(
            "CREATE TABLE items (id BIGINT PRIMARY KEY,x BIGINT)",
            &[],
            None,
        )
        .unwrap();
        sql.execute("INSERT INTO items VALUES(1,10)", &[], None)
            .unwrap();
        drop(sql);
        let engine = Engine::new(admin.clone());
        let request = returning(
            update(
                yes(),
                Intent::One,
                vec![("x", binary(field("x"), Arithmetic::Add, int(1)))],
            ),
            vec![],
        );
        admin.set_crash_point(point).unwrap();
        assert_eq!(
            engine.execute(&request, &Bindings::new()),
            Err(Error::Unknown)
        );
        drop(engine);
        drop(admin);
        let admin = Admin::open(path, Limits::default()).unwrap();
        assert_eq!(
            rows(&admin)[0].values[1],
            V::Int(if persisted { 11 } else { 10 })
        );
    }
}

#[test]
fn returning_full_stored_row_bounds_precede_output_evaluation() {
    let (admin, engine) = database("CREATE TABLE items (id BIGINT PRIMARY KEY,src TEXT,dest TEXT)");
    admin
        .sql_session()
        .execute(
            "INSERT INTO items VALUES(1,$1,NULL)",
            &[V::Text("s".repeat(32768))],
            None,
        )
        .unwrap();
    let update = Request::Update {
        table: "items".into(),
        filter: equal(field("id"), int(1)),
        guard: None,
        intent: Intent::One,
        values: vec![("dest".into(), ValueExpr::Parameter("new".into()))],
    };
    let output = vec![projection(
        binary(int(1), Arithmetic::Divide, int(0)),
        Some("bad"),
    )];
    let bindings = Bindings::from([("new".into(), V::Text("d".repeat(32760)))]);
    assert_eq!(9 + 32768 + 32760, 65537);
    rejects_atomically(
        &admin,
        &engine,
        &returning(update, output),
        &bindings,
        Error::Limit,
    );

    let (admin, engine) =
        database("CREATE TABLE items (id BIGINT PRIMARY KEY,body TEXT,omitted TEXT)");
    let insert = Request::Insert {
        table: "items".into(),
        values: vec![
            ("id".into(), ValueExpr::Literal(V::Int(0))),
            ("body".into(), ValueExpr::Parameter("body".into())),
        ],
    };
    let output = vec![projection(
        binary(field("id"), Arithmetic::Divide, field("id")),
        Some("bad"),
    )];
    let bindings = Bindings::from([("body".into(), V::Text("x".repeat(65519)))]);
    // Explicit values fit; the generated omitted NULL adds another nine bytes.
    const { assert!(9 + 65519 < MAX_BINDING_BYTES) };
    assert_eq!(9 + 65519 + 9, 65537);
    rejects_atomically(
        &admin,
        &engine,
        &returning(insert, output),
        &bindings,
        Error::Limit,
    );
}
