mod common;

use giggles_core::{
    Admin, Cancellation, CoreError, CrashPoint, DecisionPoint, Limits,
    operations::{
        Arithmetic, Bindings, Compare, ConflictAction, ConflictSource, ConflictStatus,
        ConflictTupleOutcome, EmitExpr, EmittedValue, Engine, Error, Expr, Intent, LocalRef,
        MAX_BOUND_VALUE_BYTES, MAX_DEPTH, MAX_INPUT, MAX_INSERT_ROWS, MAX_MUTATION_BYTES,
        Projection, Request, Transaction, TransactionStep, ValueExpr,
    },
    relational::{MAX_RESULT_BYTES, MAX_ROW_BYTES, SqlRow, Value as V},
};
use std::{path::PathBuf, sync::Arc};

fn value(v: V) -> Expr {
    Expr::Value(ValueExpr::Literal(v))
}
fn int(v: i64) -> Expr {
    value(V::Int(v))
}
fn yes() -> Expr {
    value(V::Bool(true))
}
fn no() -> Expr {
    value(V::Bool(false))
}
fn parameter(name: &str) -> Expr {
    Expr::Value(ValueExpr::Parameter(name.into()))
}
fn field(name: &str) -> Expr {
    Expr::Field(name.into())
}
fn existing(name: &str) -> Expr {
    Expr::ConflictField {
        source: ConflictSource::Existing,
        field: name.into(),
    }
}
fn incoming(name: &str) -> Expr {
    Expr::ConflictField {
        source: ConflictSource::Incoming,
        field: name.into(),
    }
}
fn binary(a: Expr, op: Arithmetic, b: Expr) -> Expr {
    Expr::Arithmetic(Box::new(a), op, Box::new(b))
}
fn equal(a: Expr, b: Expr) -> Expr {
    Expr::Compare(Box::new(a), Compare::Eq, Box::new(b))
}
fn divide() -> Expr {
    binary(int(1), Arithmetic::Divide, int(0))
}
fn projection(expression: Expr, alias: Option<&str>) -> Projection {
    Projection {
        expression,
        alias: alias.map(str::to_owned),
    }
}
fn insert(id: i64, n: Expr) -> Request {
    Request::InsertExpressions {
        table: "items".into(),
        values: vec![("id".into(), int(id)), ("n".into(), n)],
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
fn conflict(request: Request, target: &[&str], action: ConflictAction) -> Request {
    Request::OnConflict {
        request: Box::new(request),
        target: target.iter().map(|s| (*s).into()).collect(),
        action,
    }
}
fn nothing(request: Request) -> Request {
    conflict(request, &["id"], ConflictAction::Nothing)
}
fn updating(request: Request, guard: Option<Expr>, values: Vec<(&str, Expr)>) -> Request {
    conflict(
        request,
        &["id"],
        ConflictAction::Update {
            guard,
            values: values.into_iter().map(|(s, e)| (s.into(), e)).collect(),
        },
    )
}
fn returning(request: Request, projection: Vec<Projection>) -> Request {
    Request::Returning {
        request: Box::new(request),
        projection,
    }
}
fn database(schema: &str) -> (PathBuf, Admin, Engine) {
    let path = common::fresh("upsert");
    let admin = Admin::create(&path, Limits::default()).unwrap();
    admin.sql_session().execute(schema, &[], None).unwrap();
    (path, admin.clone(), Engine::new(admin))
}
fn fixture() -> (PathBuf, Admin, Engine) {
    database("CREATE TABLE items(id BIGINT PRIMARY KEY,n BIGINT,txt TEXT)")
}
fn rows(admin: &Admin) -> Vec<SqlRow> {
    admin
        .sql_session()
        .execute("SELECT * FROM items LIMIT 1024", &[], None)
        .unwrap()
        .rows
}
fn digest(admin: &Admin) -> [u8; 32] {
    admin.export(common::fresh("upsert-export")).unwrap()
}
fn seed(admin: &Admin, id: i64, n: i64, text: &str) {
    admin
        .sql_session()
        .execute(
            "INSERT INTO items VALUES($1,$2,$3)",
            &[V::Int(id), V::Int(n), V::Text(text.into())],
            None,
        )
        .unwrap();
}
fn static_failure(admin: &Admin, engine: &Engine, request: &Request, expected: Option<Error>) {
    let before = digest(admin);
    let error = engine.prepare_bound(request, &Bindings::new()).unwrap_err();
    if let Some(expected) = expected {
        assert_eq!(error, expected);
    }
    assert_eq!(engine.execute(request, &Bindings::new()), Err(error));
    assert_eq!(digest(admin), before);
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
fn transaction_failure(admin: &Admin, engine: &Engine, transaction: &Transaction, expected: Error) {
    let before = digest(admin);
    let bound = engine
        .prepare_transaction_bound(transaction, &Bindings::new())
        .unwrap();
    assert_eq!(
        engine.execute_transaction_bound(&bound),
        Err(expected.clone())
    );
    assert_eq!(
        engine.execute_transaction(transaction, &Bindings::new()),
        Err(expected)
    );
    assert_eq!(digest(admin), before);
}
fn tuple(
    index: usize,
    status: ConflictStatus,
    changed: bool,
    row: Option<usize>,
) -> ConflictTupleOutcome {
    ConflictTupleOutcome {
        input_index: index,
        status,
        changed,
        returned_row: row,
    }
}
fn local_field(local: &str, field: &str) -> Expr {
    Expr::Local(LocalRef::Field {
        local: local.into(),
        field: field.into(),
    })
}

#[test]
fn upsert_binding_targets_scopes_and_wrapper_shapes_are_finite() {
    let (_, admin, engine) = fixture();
    let read = Request::Read {
        table: "items".into(),
        filter: None,
        order: vec![],
        limit: None,
        projection: vec![],
    };
    for inner in [
        read.clone(),
        nothing(insert(1, int(1))),
        returning(insert(1, int(1)), vec![]),
        Request::Delete {
            table: "items".into(),
            filter: yes(),
            guard: None,
            cap: None,
        },
    ] {
        static_failure(&admin, &engine, &nothing(inner), Some(Error::Unsupported));
    }
    static_failure(
        &admin,
        &engine,
        &returning(returning(nothing(insert(1, int(1))), vec![]), vec![]),
        Some(Error::Unsupported),
    );
    for target in [
        vec![],
        vec!["id", "id"],
        vec!["n"],
        vec!["absent"],
        vec!["id", "n"],
        vec!["ID"],
    ] {
        static_failure(
            &admin,
            &engine,
            &conflict(insert(1, int(1)), &target, ConflictAction::Nothing),
            None,
        );
    }
    static_failure(
        &admin,
        &engine,
        &conflict(insert(1, int(1)), &["id"; 65], ConflictAction::Nothing),
        Some(Error::Limit),
    );
    static_failure(
        &admin,
        &engine,
        &nothing(many(&["id"], vec![vec![int(1)]; 1025], 1024)),
        Some(Error::Limit),
    );
    for (guard, values) in [
        (None, vec![]),
        (None, vec![("n", int(1)), ("n", int(2))]),
        (None, vec![("id", incoming("id"))]),
        (None, vec![("missing", int(1))]),
        (None, vec![("n", field("n"))]),
        (None, vec![("n", existing("missing"))]),
        (None, vec![("n", incoming("missing"))]),
        (None, vec![("n", value(V::Text("wrong".into())))]),
        (Some(field("n")), vec![("n", int(1))]),
        (Some(int(1)), vec![("n", int(1))]),
        (Some(no()), vec![("n", parameter("missing"))]),
    ] {
        static_failure(
            &admin,
            &engine,
            &updating(insert(1, int(1)), guard, values),
            None,
        );
    }
    // Conflict fields have no meaning in inputs, RETURN, reads or block scalar roots.
    for request in [
        insert(1, existing("n")),
        nothing(insert(1, incoming("n"))),
        returning(
            nothing(insert(1, int(1))),
            vec![projection(existing("n"), Some("n"))],
        ),
        Request::ReadExpressions {
            table: "items".into(),
            filter: None,
            order: vec![],
            limit: None,
            projection: vec![projection(incoming("n"), Some("n"))],
        },
    ] {
        static_failure(&admin, &engine, &request, None);
    }
    for expression in [
        existing("n"),
        Expr::Case {
            operand: None,
            arms: vec![(yes(), int(1))],
            else_expr: Some(Box::new(incoming("n"))),
        },
    ] {
        for transaction in [
            Transaction {
                read_only: true,
                steps: vec![TransactionStep::Require(expression.clone())],
                emit: None,
            },
            Transaction {
                read_only: true,
                steps: vec![],
                emit: Some(vec![EmitExpr::Scalar(expression)]),
            },
        ] {
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
    }
    // All branches bind even if no tuple exists, no conflict exists, or the cap is zero.
    static_failure(
        &admin,
        &engine,
        &updating(
            many(&["n"], vec![], 0),
            None,
            vec![("n", incoming("missing"))],
        ),
        None,
    );
    static_failure(
        &admin,
        &engine,
        &updating(
            many(&["id"], vec![vec![int(1)]], 0),
            None,
            vec![("n", parameter("missing"))],
        ),
        None,
    );
    let empty = returning(
        updating(
            many(&["n"], vec![], 0),
            Some(equal(divide(), int(1))),
            vec![("n", divide())],
        ),
        vec![projection(divide(), Some("out"))],
    );
    let bound = engine.prepare_bound(&empty, &Bindings::new()).unwrap();
    assert_eq!(bound.description().columns.len(), 1);
    let out = engine.execute_bound(&bound).unwrap();
    assert_eq!((out.matched, out.affected, out.changed), (0, 0, 0));
    assert_eq!(out.conflict_results, Some(vec![]));
    assert!(out.rows.is_empty() && out.complete);
    // A raw legacy INSERT can be wrapped without changing its ordinary syntax.
    let legacy = Request::Insert {
        table: "items".into(),
        values: vec![("id".into(), ValueExpr::Literal(V::Int(1)))],
    };
    assert_eq!(
        engine
            .execute(&nothing(legacy), &Bindings::new())
            .unwrap()
            .changed,
        1
    );
}

#[test]
fn upsert_caps_incoming_constraints_and_duplicate_identities_precede_conflicts() {
    let (_, admin, engine) = fixture();
    seed(&admin, 1, 1, "present");
    let request = returning(
        updating(
            many(&["id", "n"], vec![vec![int(1), divide()]], 0),
            Some(equal(divide(), int(0))),
            vec![("n", divide())],
        ),
        vec![projection(divide(), Some("out"))],
    );
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
        &nothing(many(&["id"], vec![vec![int(1)], vec![int(1)]], 1)),
        &Bindings::new(),
        Error::TooManyMatches,
    );
    runtime_failure(
        &admin,
        &engine,
        &nothing(many(&["id"], vec![vec![int(1)], vec![int(1)]], 2)),
        &Bindings::new(),
        Error::DuplicateTarget,
    );
    runtime_failure(
        &admin,
        &engine,
        &nothing(insert(1, divide())),
        &Bindings::new(),
        Error::DivisionByZero,
    );
    let (_, admin, engine) =
        database("CREATE TABLE items(id BIGINT PRIMARY KEY,n SMALLINT NOT NULL,txt TEXT)");
    seed(&admin, 1, 1, "present");
    for bad in [value(V::Null), int(32768)] {
        runtime_failure(
            &admin,
            &engine,
            &nothing(many(
                &["id", "n"],
                vec![vec![int(1), int(1)], vec![int(1), bad]],
                2,
            )),
            &Bindings::new(),
            Error::Type,
        );
    }
    runtime_failure(
        &admin,
        &engine,
        &nothing(many(&["id"], vec![vec![int(1)]], 1)),
        &Bindings::new(),
        Error::Sql { sqlstate: "23502".into() },
    );
    // Identity comes from the canonical full key, independent of target-list order.
    let (_, admin, engine) =
        database("CREATE TABLE items(tenant TEXT,id BYTES,n BIGINT,PRIMARY KEY(tenant,id))");
    let key_rows = vec![
        vec![
            value(V::Text("a\0".into())),
            value(V::Bytes(vec![1])),
            int(1),
        ],
        vec![
            value(V::Text("a".into())),
            value(V::Bytes(vec![0, 1])),
            int(2),
        ],
        vec![value(V::Text("é".into())), value(V::Bytes(vec![0])), int(3)],
        vec![
            value(V::Text("e\u{301}".into())),
            value(V::Bytes(vec![0])),
            int(4),
        ],
    ];
    let insert = conflict(
        many(&["tenant", "id", "n"], key_rows.clone(), 4),
        &["id", "tenant"],
        ConflictAction::Nothing,
    );
    assert_eq!(
        engine.execute(&insert, &Bindings::new()).unwrap().changed,
        4
    );
    let before = rows(&admin);
    let out = engine.execute(&insert, &Bindings::new()).unwrap();
    assert_eq!((out.matched, out.affected, out.changed), (4, 0, 0));
    assert_eq!(rows(&admin), before);
    let mut duplicate = key_rows;
    duplicate.push(duplicate[0].clone());
    runtime_failure(
        &admin,
        &engine,
        &conflict(
            many(&["tenant", "id", "n"], duplicate, 5),
            &["tenant", "id"],
            ConflictAction::Nothing,
        ),
        &Bindings::new(),
        Error::DuplicateTarget,
    );
}

#[test]
fn upsert_tuple_metadata_correlates_complete_pk_ordered_results() {
    let (_, admin, engine) = fixture();
    seed(&admin, 1, 10, "old-one");
    seed(&admin, 2, 20, "old-two");
    let initial = rows(&admin);
    let request = returning(
        updating(
            many(
                &["id", "n"],
                vec![
                    vec![int(3), int(30)],
                    vec![int(1), int(11)],
                    vec![int(2), int(20)],
                ],
                3,
            ),
            None,
            vec![("n", incoming("n"))],
        ),
        vec![],
    );
    let out = engine.execute(&request, &Bindings::new()).unwrap();
    assert_eq!((out.matched, out.affected, out.changed), (3, 3, 2));
    assert_eq!(out.rows, rows(&admin));
    assert_eq!(out.rows[1].version, initial[1].version);
    assert!(out.rows[0].version > initial[1].version);
    assert!(out.rows[2].version > initial[1].version);
    assert_ne!(out.rows[0].version, out.rows[2].version);
    assert_eq!(
        out.conflict_results,
        Some(vec![
            tuple(0, ConflictStatus::Success, true, Some(2)),
            tuple(1, ConflictStatus::Success, true, Some(0)),
            tuple(2, ConflictStatus::Success, false, Some(1))
        ])
    );
    let decoded: giggles_core::operations::Outcome =
        serde_json::from_value(serde_json::to_value(&out).unwrap()).unwrap();
    assert_eq!(decoded, out);
    let plain = engine
        .execute(&insert(4, int(40)), &Bindings::new())
        .unwrap();
    let json = serde_json::to_value(&plain).unwrap();
    assert!(json.get("conflict_results").is_none());
    assert_eq!(json.as_object().unwrap().len(), 6);
    let decoded: giggles_core::operations::Outcome = serde_json::from_value(json).unwrap();
    assert!(decoded.conflict_results.is_none());
    let no_return = engine
        .execute(&nothing(insert(4, int(999))), &Bindings::new())
        .unwrap();
    assert_eq!(
        no_return.conflict_results,
        Some(vec![tuple(0, ConflictStatus::Skipped, false, None)])
    );
    assert!(no_return.rows.is_empty());
    let (_, admin, engine) = fixture();
    seed(&admin, 0, 0, "");
    seed(&admin, 1, 1, "");
    let request = returning(
        nothing(many(
            &["id", "n"],
            (0..132).rev().map(|id| vec![int(id), int(id)]).collect(),
            132,
        )),
        vec![projection(field("id"), None)],
    );
    let out = engine.execute(&request, &Bindings::new()).unwrap();
    assert_eq!((out.matched, out.affected, out.changed), (132, 130, 130));
    assert_eq!(out.rows.len(), 130);
    assert!(out.complete);
    for (index, result) in out.conflict_results.unwrap().iter().enumerate() {
        let id = 131 - index;
        if id < 2 {
            assert_eq!(*result, tuple(index, ConflictStatus::Skipped, false, None));
        } else {
            assert_eq!(
                *result,
                tuple(index, ConflictStatus::Success, true, Some(id - 2))
            );
            assert_eq!(out.rows[id - 2].values, [V::Int(id as i64)]);
        }
    }
    for returned in out.rows {
        let id = returned.values[0].clone();
        let stored = admin
            .sql_session()
            .execute("SELECT id FROM items WHERE id=$1", &[id], None)
            .unwrap()
            .rows
            .remove(0);
        assert_eq!(returned, stored);
    }
}

#[test]
fn upsert_all_guards_old_images_and_return_errors_are_atomic() {
    let (_, admin, engine) = fixture();
    seed(&admin, 1, 10, "one");
    seed(&admin, 2, 20, "two");
    // A later false winner guard wins over an earlier conflict RHS division.
    let input = || {
        many(
            &["id", "n"],
            vec![
                vec![int(1), int(1)],
                vec![int(2), int(2)],
                vec![int(3), int(3)],
            ],
            3,
        )
    };
    runtime_failure(
        &admin,
        &engine,
        &updating(
            input(),
            Some(equal(existing("id"), int(1))),
            vec![("n", divide())],
        ),
        &Bindings::new(),
        Error::ConditionFailed,
    );
    runtime_failure(
        &admin,
        &engine,
        &updating(input(), Some(value(V::Null)), vec![("n", divide())]),
        &Bindings::new(),
        Error::ConditionFailed,
    );
    let next = rows(&admin).last().unwrap().version + 1;
    runtime_failure(
        &admin,
        &engine,
        &returning(
            updating(input(), Some(yes()), vec![("n", incoming("n"))]),
            vec![projection(
                binary(
                    int(1),
                    Arithmetic::Divide,
                    binary(field("id"), Arithmetic::Subtract, int(3)),
                ),
                Some("out"),
            )],
        ),
        &Bindings::new(),
        Error::DivisionByZero,
    );
    seed(&admin, 4, 40, "four");
    assert_eq!(rows(&admin).last().unwrap().version, next);
    // No conflict: the action is bound but its runtime guard and RHS are not evaluated.
    let out = engine
        .execute(
            &updating(
                insert(5, int(5)),
                Some(equal(divide(), int(0))),
                vec![("n", divide())],
            ),
            &Bindings::new(),
        )
        .unwrap();
    assert_eq!(out.changed, 1);
    // All skipped: RETURN expressions are bound but there is no returned input to evaluate.
    let out = engine
        .execute(
            &returning(
                nothing(insert(1, int(99))),
                vec![projection(divide(), Some("out"))],
            ),
            &Bindings::new(),
        )
        .unwrap();
    assert_eq!((out.matched, out.affected, out.changed), (1, 0, 0));
    assert!(out.rows.is_empty());
    let (_, admin, engine) =
        database("CREATE TABLE items(id BIGINT PRIMARY KEY,n BIGINT,other BIGINT,txt TEXT)");
    admin
        .sql_session()
        .execute("INSERT INTO items VALUES(1,10,20,'old')", &[], None)
        .unwrap();
    let request = returning(
        updating(
            insert(1, int(100)),
            Some(equal(existing("n"), int(10))),
            vec![
                ("n", existing("other")),
                ("other", existing("n")),
                ("txt", incoming("txt")),
            ],
        ),
        vec![],
    );
    let out = engine.execute(&request, &Bindings::new()).unwrap();
    assert_eq!(
        out.rows[0].values,
        [V::Int(1), V::Int(20), V::Int(10), V::Null]
    );
    let (_, admin, engine) =
        database("CREATE TABLE items(id BIGINT PRIMARY KEY,n SMALLINT NOT NULL,txt TEXT)");
    seed(&admin, 1, 1, "");
    for expression in [int(32768), value(V::Null)] {
        runtime_failure(
            &admin,
            &engine,
            &updating(insert(1, int(1)), None, vec![("n", expression)]),
            &Bindings::new(),
            Error::Type,
        );
    }
}

#[test]
fn upsert_full_postimage_and_return_byte_budgets_include_noops_not_skips() {
    assert_eq!(MAX_MUTATION_BYTES, 1024 * 1024);
    for new_last in [false, true] {
        for extra in [0, 1] {
            let (_, admin, engine) = database(
                "CREATE TABLE items(id BIGINT PRIMARY KEY,n BIGINT,src TEXT,copy TEXT,marker TEXT)",
            );
            let large = "x".repeat(30830);
            let mut sql = admin.sql_session();
            sql.execute("BEGIN", &[], None).unwrap();
            for id in 0..if new_last { 16 } else { 17 } {
                sql.execute(
                    "INSERT INTO items VALUES($1,0,$2,$3,$4)",
                    &[
                        V::Int(id),
                        V::Text(large.clone()),
                        if id < 8 {
                            V::Text(large.clone())
                        } else {
                            V::Null
                        },
                        V::Text("m".repeat(if id == 16 { 34 + extra } else { 1 })),
                    ],
                    None,
                )
                .unwrap();
            }
            sql.execute("COMMIT", &[], None).unwrap();
            let before = rows(&admin);
            let mut input = (0..17)
                .map(|id| {
                    vec![
                        int(id),
                        int(0),
                        value(V::Null),
                        value(V::Null),
                        value(V::Null),
                    ]
                })
                .collect::<Vec<_>>();
            let bindings = if new_last {
                input[16] = vec![
                    int(16),
                    int(0),
                    parameter("text"),
                    parameter("text"),
                    value(V::Text("m".repeat(34 + extra))),
                ];
                Bindings::from([("text".into(), V::Text(large))])
            } else {
                Bindings::new()
            };
            let request = updating(
                many(&["id", "n", "src", "copy", "marker"], input, 17),
                None,
                vec![("copy", existing("src"))],
            );
            assert_eq!(
                16 * (18 + 2 * 30830 + 1) + (18 + 2 * 30830 + 34 + extra),
                MAX_MUTATION_BYTES + extra
            );
            if extra == 0 {
                let out = engine.execute(&request, &bindings).unwrap();
                assert_eq!((out.affected, out.changed), (17, 9));
                assert_eq!(
                    out.conflict_results
                        .unwrap()
                        .iter()
                        .filter(|r| !r.changed)
                        .count(),
                    8
                );
                assert_eq!(&rows(&admin)[..8], &before[..8]);
            } else {
                runtime_failure(&admin, &engine, &request, &bindings, Error::Limit);
            }
            if !new_last {
                let before = rows(&admin);
                let skipped = engine
                    .execute(
                        &nothing(many(&["id"], (0..17).map(|id| vec![int(id)]).collect(), 17)),
                        &Bindings::new(),
                    )
                    .unwrap();
                assert_eq!(
                    (skipped.matched, skipped.affected, skipped.changed),
                    (17, 0, 0)
                );
                assert_eq!(rows(&admin), before);
            }
        }
    }
    // Full incoming rows must fit even if the corresponding winner will be skipped.
    let (_, admin, engine) =
        database("CREATE TABLE items(id BIGINT PRIMARY KEY,n BIGINT,txt TEXT,omitted TEXT)");
    admin
        .sql_session()
        .execute("INSERT INTO items(id,n) VALUES(1,0)", &[], None)
        .unwrap();
    for (size, ok) in [(MAX_ROW_BYTES - 27, true), (MAX_ROW_BYTES - 26, false)] {
        let request = nothing(many(
            &["id", "txt"],
            vec![vec![int(1), parameter("text")]],
            1,
        ));
        let bindings = Bindings::from([("text".into(), V::Text("x".repeat(size)))]);
        if ok {
            assert_eq!(engine.execute(&request, &bindings).unwrap().affected, 0);
        } else {
            runtime_failure(&admin, &engine, &request, &bindings, Error::Limit);
        }
    }
    // A single RETURN row can exceed64KiB; its separate aggregate budget is exact.
    for extra in [0, 1] {
        let (_, admin, engine) =
            database("CREATE TABLE items(id BIGINT PRIMARY KEY,n BIGINT,src TEXT,tail TEXT)");
        admin
            .sql_session()
            .execute(
                "INSERT INTO items VALUES(1,0,$1,$2)",
                &[
                    V::Text("s".repeat(16384)),
                    V::Text("t".repeat(16384 + extra)),
                ],
                None,
            )
            .unwrap();
        let mut projected = (0..63)
            .map(|i| projection(field("src"), Some(&format!("p{i}"))))
            .collect::<Vec<_>>();
        projected.push(projection(field("tail"), Some("tail")));
        let request = returning(
            updating(insert(1, int(0)), None, vec![("n", existing("n"))]),
            projected,
        );
        if extra == 0 {
            let out = engine.execute(&request, &Bindings::new()).unwrap();
            assert_eq!(out.rows[0].values.len(), 64);
            assert_eq!(out.changed, 0);
            assert_eq!(out.conflict_results.unwrap()[0].returned_row, Some(0));
        } else {
            runtime_failure(&admin, &engine, &request, &Bindings::new(), Error::Limit);
        }
    }
    const {
        assert!(MAX_RESULT_BYTES > MAX_ROW_BYTES);
    }
}

#[test]
fn upsert_shared_admission_and_net_new_capacity_stay_bounded() {
    let (_, admin, engine) = fixture();
    // The frozen allowance is shared between the input, action and RETURN.
    let request = updating(insert(1, int(0)), None, vec![("txt", parameter("text"))]);
    let exact = MAX_BOUND_VALUE_BYTES - 18;
    let bindings = Bindings::from([("text".into(), V::Text("x".repeat(exact)))]);
    assert!(engine.prepare_bound(&request, &bindings).is_ok());
    let oversized = Bindings::from([("text".into(), V::Text("x".repeat(exact + 1)))]);
    assert_eq!(
        engine.prepare_bound(&request, &oversized).unwrap_err(),
        Error::Limit
    );
    assert_eq!(engine.execute(&request, &oversized), Err(Error::Limit));
    let mut deep = existing("n");
    for _ in 0..MAX_DEPTH {
        deep = Expr::Negate(Box::new(deep));
    }
    assert!(
        engine
            .prepare_bound(
                &updating(insert(1, int(0)), None, vec![("n", deep.clone())]),
                &Bindings::new()
            )
            .is_ok()
    );
    static_failure(
        &admin,
        &engine,
        &updating(
            insert(1, int(0)),
            None,
            vec![("n", Expr::Negate(Box::new(deep)))],
        ),
        Some(Error::Limit),
    );
    // All expression roots share4096 nodes, including input/action/RETURN.
    fn tree(nodes: usize) -> Expr {
        if nodes <= 25 {
            let mut e = yes();
            for _ in 1..nodes {
                e = Expr::Not(Box::new(e));
            }
            e
        } else {
            let left = nodes / 2;
            Expr::And(Box::new(tree(left)), Box::new(tree(nodes - 1 - left)))
        }
    }
    // input id+n=2, guard4091+NOT=4092, SET n=1, RETURN id=1 =>4096.
    let request = returning(
        updating(
            insert(1, int(0)),
            Some(Expr::Not(Box::new(tree(4091)))),
            vec![("n", existing("n"))],
        ),
        vec![projection(field("id"), None)],
    );
    assert!(serde_json::to_vec(&request).unwrap().len() < MAX_INPUT);
    assert!(engine.prepare_bound(&request, &Bindings::new()).is_ok());
    let request = returning(
        updating(
            insert(1, int(0)),
            Some(Expr::Not(Box::new(Expr::Not(Box::new(tree(4091)))))),
            vec![("n", existing("n"))],
        ),
        vec![projection(field("id"), None)],
    );
    static_failure(&admin, &engine, &request, Some(Error::Limit));
    let mut sql = admin.sql_session();
    sql.execute("BEGIN", &[], None).unwrap();
    for id in 0..MAX_INSERT_ROWS {
        sql.execute(
            "INSERT INTO items(id,n) VALUES($1,0)",
            &[V::Int(id as i64)],
            None,
        )
        .unwrap();
    }
    sql.execute("COMMIT", &[], None).unwrap();
    let all = nothing(many(
        &["id"],
        (0..MAX_INSERT_ROWS)
            .map(|id| vec![int(id as i64)])
            .collect(),
        MAX_INSERT_ROWS as i64,
    ));
    let out = engine.execute(&all, &Bindings::new()).unwrap();
    assert_eq!(
        (out.matched, out.affected, out.changed),
        (MAX_INSERT_ROWS, 0, 0)
    );
    let outcomes = out.conflict_results.unwrap();
    assert_eq!(outcomes.len(), MAX_INSERT_ROWS);
    assert!(
        outcomes
            .iter()
            .enumerate()
            .all(|(index, result)| result.input_index == index
                && result.status == ConflictStatus::Skipped
                && !result.changed
                && result.returned_row.is_none())
    );
    for request in [
        nothing(insert(0, int(0))),
        updating(insert(0, int(0)), None, vec![("n", existing("n"))]),
    ] {
        assert_eq!(
            engine.execute(&request, &Bindings::new()).unwrap().changed,
            0
        );
    }
    runtime_failure(
        &admin,
        &engine,
        &nothing(insert(2000, int(0))),
        &Bindings::new(),
        Error::Limit,
    );
    runtime_failure(
        &admin,
        &engine,
        &updating(
            many(
                &["id", "n"],
                vec![vec![int(0), int(1)], vec![int(2000), int(1)]],
                2,
            ),
            None,
            vec![("n", incoming("n"))],
        ),
        &Bindings::new(),
        Error::Limit,
    );
    sql.execute("INSERT INTO items(id,n) VALUES(2001,0)", &[], None)
        .unwrap();
    runtime_failure(
        &admin,
        &engine,
        &nothing(many(&["id"], vec![vec![int(0)]], 0)),
        &Bindings::new(),
        Error::TooManyMatches,
    );
    runtime_failure(
        &admin,
        &engine,
        &nothing(many(&["id"], vec![vec![int(0)]], 1)),
        &Bindings::new(),
        Error::Limit,
    );
}

#[test]
fn upsert_prepared_winners_and_skipped_transaction_locals_are_fresh() {
    let (_, admin, engine) = fixture();
    let request = returning(
        updating(
            insert(1, parameter("n")),
            None,
            vec![("n", binary(existing("n"), Arithmetic::Add, incoming("n")))],
        ),
        vec![],
    );
    let mut bindings = Bindings::from([("n".into(), V::Int(7))]);
    let bound = engine.prepare_bound(&request, &bindings).unwrap();
    bindings.insert("n".into(), V::Int(99));
    seed(&admin, 1, 10, "fresh");
    assert_eq!(
        engine.execute_bound(&bound).unwrap().rows[0].values,
        [V::Int(1), V::Int(17), V::Text("fresh".into())]
    );
    admin
        .sql_session()
        .execute("DELETE FROM items WHERE id=1", &[], None)
        .unwrap();
    assert_eq!(
        engine.execute_bound(&bound).unwrap().rows[0].values,
        [V::Int(1), V::Int(7), V::Null]
    );
    let saved = returning(
        nothing(insert(1, int(99))),
        vec![projection(field("id"), None)],
    );
    let block = |emit| Transaction {
        read_only: false,
        steps: vec![TransactionStep::Let {
            name: "saved".into(),
            request: saved.clone(),
        }],
        emit: Some(emit),
    };
    let out = engine
        .execute_transaction(
            &block(vec![
                EmitExpr::Scalar(Expr::Local(LocalRef::Status {
                    local: "saved".into(),
                })),
                EmitExpr::Scalar(Expr::Local(LocalRef::Changed {
                    local: "saved".into(),
                })),
            ]),
            &Bindings::new(),
        )
        .unwrap();
    assert_eq!(
        out.emitted,
        [
            EmittedValue::Scalar(V::Text("skipped".into())),
            EmittedValue::Scalar(V::Int(0))
        ]
    );
    assert_eq!(
        out.summaries[0].conflict_results,
        Some(vec![tuple(0, ConflictStatus::Skipped, false, None)])
    );
    for emit in [
        vec![EmitExpr::Row("saved".into())],
        vec![EmitExpr::Scalar(local_field("saved", "id"))],
        vec![EmitExpr::Scalar(Expr::Case {
            operand: None,
            arms: vec![(yes(), int(0))],
            else_expr: Some(Box::new(local_field("saved", "id"))),
        })],
    ] {
        transaction_failure(
            &admin,
            &engine,
            &block(emit),
            Error::Bind("local row".into()),
        );
    }
    let mut protected = block(vec![EmitExpr::Scalar(local_field("saved", "id"))]);
    protected.steps.push(TransactionStep::Require(equal(
        Expr::Local(LocalRef::Status {
            local: "saved".into(),
        }),
        value(V::Text("success".into())),
    )));
    transaction_failure(&admin, &engine, &protected, Error::RequirementFailed);
    // MANY is still a many local even with one tuple; all skipped MANY status is success.
    let many_block = Transaction {
        read_only: false,
        steps: vec![TransactionStep::Let {
            name: "batch".into(),
            request: returning(nothing(many(&["id"], vec![vec![int(1)]], 1)), vec![]),
        }],
        emit: Some(vec![EmitExpr::Scalar(Expr::Local(LocalRef::Status {
            local: "batch".into(),
        }))]),
    };
    assert_eq!(
        engine
            .execute_transaction(&many_block, &Bindings::new())
            .unwrap()
            .emitted,
        [EmittedValue::Scalar(V::Text("success".into()))]
    );
    let mut invalid = many_block.clone();
    invalid.emit = Some(vec![EmitExpr::Row("batch".into())]);
    assert!(
        engine
            .prepare_transaction_bound(&invalid, &Bindings::new())
            .is_err()
    );
    // Earlier writes are rolled back even when the missing row is referenced only in a lazy arm.
    let mut rollback = block(vec![EmitExpr::Scalar(Expr::Case {
        operand: None,
        arms: vec![(yes(), int(0))],
        else_expr: Some(Box::new(local_field("saved", "id"))),
    })]);
    rollback
        .steps
        .insert(0, TransactionStep::Execute(insert(2, int(2))));
    transaction_failure(&admin, &engine, &rollback, Error::Bind("local row".into()));
    // Input cap is checked before any conflict-action local materialization.
    admin
        .sql_session()
        .execute(
            "UPDATE items SET txt=$1 WHERE id=1",
            &[V::Text("x".repeat(32768))],
            None,
        )
        .unwrap();
    let read_local = returning(
        Request::Update {
            table: "items".into(),
            filter: equal(field("id"), int(1)),
            guard: None,
            values: vec![("n".into(), ValueExpr::Literal(V::Int(8)))],
            intent: Intent::One,
        },
        vec![projection(field("txt"), None)],
    );
    for (cap, expected) in [(0, Error::TooManyMatches), (1, Error::Limit)] {
        let guard = equal(local_field("large", "txt"), local_field("large", "txt"));
        let transaction = Transaction {
            read_only: false,
            steps: vec![
                TransactionStep::Let {
                    name: "large".into(),
                    request: read_local.clone(),
                },
                TransactionStep::Execute(updating(
                    many(&["id"], vec![vec![int(1)]], cap),
                    Some(guard),
                    vec![("n", int(0))],
                )),
            ],
            emit: None,
        };
        transaction_failure(&admin, &engine, &transaction, expected);
    }
    let (_, other, other_engine) = fixture();
    let before = digest(&other);
    assert!(other_engine.execute_bound(&bound).is_err());
    assert_eq!(digest(&other), before);
    admin
        .sql_session()
        .execute("CREATE TABLE changed(id BIGINT PRIMARY KEY)", &[], None)
        .unwrap();
    let before = digest(&admin);
    assert_eq!(
        engine.execute_bound(&bound),
        Err(Error::Sql { sqlstate: "0A000".into() })
    );
    assert_eq!(digest(&admin), before);
}

#[test]
fn upsert_cancellation_and_unknown_never_expose_tuple_results() {
    for (point, wins) in [
        (DecisionPoint::BeforeDecision, true),
        (DecisionPoint::Decided, false),
    ] {
        let (_, admin, engine) = fixture();
        seed(&admin, 1, 10, "");
        let request = returning(
            updating(
                many(
                    &["id", "n"],
                    vec![vec![int(1), int(11)], vec![int(2), int(20)]],
                    2,
                ),
                None,
                vec![("n", incoming("n"))],
            ),
            vec![],
        );
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
            let out = result.unwrap();
            assert_eq!(out.rows, rows(&admin));
            assert_eq!(out.conflict_results.unwrap().len(), 2);
        }
    }
    for (point, persisted) in [
        (CrashPoint::ErrorBeforeCommit, false),
        (CrashPoint::ErrorAfterCommit, true),
    ] {
        let (path, admin, engine) = fixture();
        seed(&admin, 1, 10, "");
        let request = returning(
            updating(
                many(
                    &["id", "n"],
                    vec![vec![int(1), int(11)], vec![int(2), int(20)]],
                    2,
                ),
                None,
                vec![("n", incoming("n"))],
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
        let reopened = Admin::open(path, Limits::default()).unwrap();
        let stored = rows(&reopened);
        assert_eq!(stored.len(), if persisted { 2 } else { 1 });
        assert_eq!(stored[0].values[1], V::Int(if persisted { 11 } else { 10 }));
    }
}
