use giggles_core::{Admin, Limits, relational::with_sql_parser_forbidden};
use giggles_language::*;
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::atomic::{AtomicU64, Ordering},
};

static NEXT: AtomicU64 = AtomicU64::new(0);

fn fixture() -> (Admin, Engine) {
    let path = std::env::temp_dir().join(format!(
        "giggles-engine-adapter-{}-{}-{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let admin = Admin::create(path, Limits::default()).unwrap();
    admin
        .sql_session()
        .execute(
            "create table items (id bigint primary key, state text not null)",
            &[],
            None,
        )
        .unwrap();
    let engine = Engine::new(admin.clone());
    (admin, engine)
}

fn literal(value: Value) -> ValueExpr {
    ValueExpr::Literal(value)
}

fn equal(field: &str, value: Value) -> Expr {
    Expr::Compare(
        Box::new(Expr::Field(field.into())),
        Compare::Eq,
        Box::new(Expr::Value(literal(value))),
    )
}

fn run(engine: &Engine, text: &str) -> Result<Outcome> {
    engine.execute_text(text, &Bindings::new())
}

fn sql_rows(admin: &Admin) -> Vec<SqlRow> {
    admin
        .sql_session()
        .execute("select * from items order by id limit 1024", &[], None)
        .unwrap()
        .rows
}

#[test]
fn native_text_and_trees_execute_with_the_sql_parser_forbidden() {
    let (admin, engine) = fixture();
    // A positive control makes this test fail if the parser trap stops working.
    // Parse against an existing snapshot so the intentional panic cannot poison
    // the shared catalog inspection mutex.
    let mut sql = admin.sql_session();
    sql.execute("begin", &[], None).unwrap();
    let trapped = catch_unwind(AssertUnwindSafe(|| {
        with_sql_parser_forbidden(|| sql.execute("select * from items", &[], None))
    }));
    assert!(trapped.is_err());
    sql.rollback();
    assert!(sql_rows(&admin).is_empty());

    with_sql_parser_forbidden(|| {
        assert_eq!(
            run(&engine, "insert items set id=1,state=\"ready\"")
                .unwrap()
                .changed,
            1
        );
        assert_eq!(
            engine
                .execute(
                    &Statement::Insert {
                        table: "items".into(),
                        values: vec![
                            ("id".into(), literal(Value::Int(2))),
                            ("state".into(), literal(Value::Text("ready".into()))),
                        ],
                    },
                    &Bindings::new()
                )
                .unwrap()
                .changed,
            1
        );

        let read = Statement::Read {
            table: "items".into(),
            filter: Some(equal("state", Value::Text("ready".into()))),
            order: vec![Sort {
                field: "id".into(),
                descending: true,
            }],
            limit: Some(literal(Value::Int(1))),
            projection: vec!["id".into()],
        };
        let page = engine.execute(&read, &Bindings::new()).unwrap();
        assert_eq!(page.rows[0].values, [Value::Int(2)]);
        assert_eq!(page.matched, 2);
        assert!(!page.complete);
        assert_eq!(
            run(&engine, "items where state=\"ready\"").unwrap().matched,
            2
        );

        let update = Statement::Update {
            table: "items".into(),
            filter: equal("id", Value::Int(2)),
            guard: Some(equal("state", Value::Text("ready".into()))),
            values: vec![("state".into(), literal(Value::Text("waiting".into())))],
            intent: Intent::One,
        };
        assert_eq!(
            engine.execute(&update, &Bindings::new()).unwrap().changed,
            1
        );
        assert_eq!(
            run(&engine, "update items where id=2 set state=\"ready\"")
                .unwrap()
                .changed,
            1
        );
        let claimed = run(
            &engine,
            "claim items where state=\"ready\" order by id set state=\"done\"",
        )
        .unwrap();
        assert_eq!(claimed.rows[0].values[0], Value::Int(1));

        let claim = Statement::Update {
            table: "items".into(),
            filter: equal("state", Value::Text("ready".into())),
            guard: None,
            values: vec![("state".into(), literal(Value::Text("done".into())))],
            intent: Intent::Claim {
                order: vec![Sort {
                    field: "id".into(),
                    descending: false,
                }],
            },
        };
        assert_eq!(
            engine.execute(&claim, &Bindings::new()).unwrap().rows[0].values[0],
            Value::Int(2)
        );

        assert_eq!(run(&engine, "delete items where id=1").unwrap().changed, 1);
        let delete = Statement::Delete {
            table: "items".into(),
            filter: equal("id", Value::Int(2)),
            guard: Some(equal("state", Value::Text("done".into()))),
            cap: None,
        };
        assert_eq!(
            engine.execute(&delete, &Bindings::new()).unwrap().changed,
            1
        );
        assert!(run(&engine, "items").unwrap().rows.is_empty());
    });
    assert!(sql_rows(&admin).is_empty());
}

#[test]
fn architecture_trap_catches_prepared_sql_entry_before_any_effect() {
    let (admin, engine) = fixture();
    let mut sql = admin.sql_session();
    let insert = sql.prepare("insert into items values (1,'ready')").unwrap();
    let read = sql.prepare("select * from items limit 1").unwrap();
    let digest = || {
        admin
            .export(std::env::temp_dir().join(format!(
            "giggles-adapter-export-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        )))
            .unwrap()
    };
    let baseline = digest();
    // These handles are already parsed. A parser-only trap would miss them.
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            with_sql_parser_forbidden(|| sql.execute_prepared(&insert, &[], None))
        }))
        .is_err()
    );
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            with_sql_parser_forbidden(|| sql.describe(&read))
        }))
        .is_err()
    );
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            with_sql_parser_forbidden(|| sql.prepare("begin"))
        }))
        .is_err()
    );
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            with_sql_parser_forbidden(|| sql.execute("begin", &[], None))
        }))
        .is_err()
    );
    assert_eq!(digest(), baseline);
    assert!(!sql.in_transaction());
    with_sql_parser_forbidden(|| {
        let bound = engine
            .prepare_bound_text("insert items set id=1,state=\"ready\"", &Bindings::new())
            .unwrap();
        assert_eq!(engine.execute_bound(&bound).unwrap().changed, 1);
    });
    // The scope restores public SQL entry after catching each deliberate panic.
    assert_eq!(
        sql.execute_prepared(&read, &[], None).unwrap().rows.len(),
        1
    );
}

#[test]
fn native_cap_overflow_has_zero_effects_and_exact_cap_succeeds_without_sql() {
    let (admin, engine) = fixture();
    for id in 1..=3 {
        admin
            .sql_session()
            .execute(
                "insert into items (id,state) values ($1,$2)",
                &[Value::Int(id), Value::Text("ready".into())],
                None,
            )
            .unwrap();
    }
    let before = sql_rows(&admin);
    with_sql_parser_forbidden(|| {
        for text in [
            "update many items where true set state=\"done\" at most 2",
            "delete many items where true at most 2",
        ] {
            assert_eq!(run(&engine, text), Err(Error::TooManyMatches));
            assert_eq!(run(&engine, "items").unwrap().rows, before);
        }
        let update = Statement::Update {
            table: "items".into(),
            filter: Expr::Value(literal(Value::Bool(true))),
            guard: None,
            values: vec![("state".into(), literal(Value::Text("done".into())))],
            intent: Intent::Many {
                cap: ValueExpr::Parameter("cap".into()),
            },
        };
        let bindings = Bindings::from([("cap".into(), Value::Int(2))]);
        assert_eq!(
            engine.execute(&update, &bindings),
            Err(Error::TooManyMatches)
        );
        assert_eq!(run(&engine, "items").unwrap().rows, before);

        let delete = Statement::Delete {
            table: "items".into(),
            filter: Expr::Value(literal(Value::Bool(true))),
            guard: None,
            cap: Some(literal(Value::Int(2))),
        };
        assert_eq!(
            engine.execute(&delete, &Bindings::new()),
            Err(Error::TooManyMatches)
        );
        assert_eq!(run(&engine, "items").unwrap().rows, before);

        let bindings = Bindings::from([("cap".into(), Value::Int(3))]);
        let done = engine.execute(&update, &bindings).unwrap();
        assert_eq!((done.matched, done.affected, done.changed), (3, 3, 3));
        // At-most also accepts exact-cardinality no-ops without version churn.
        let after = run(&engine, "items").unwrap().rows;
        let no_op = engine.execute(&update, &bindings).unwrap();
        assert_eq!((no_op.matched, no_op.affected, no_op.changed), (3, 3, 0));
        assert_eq!(run(&engine, "items").unwrap().rows, after);
        assert_eq!(
            run(&engine, "delete many items where true at most 3")
                .unwrap()
                .changed,
            3
        );
    });
    assert!(sql_rows(&admin).is_empty());
}

#[test]
fn sql_and_native_share_rows_and_native_no_ops_preserve_versions() {
    let (admin, engine) = fixture();
    admin
        .sql_session()
        .execute("insert into items (id,state) values (1,'sql')", &[], None)
        .unwrap();
    let inserted = sql_rows(&admin);
    with_sql_parser_forbidden(|| {
        assert_eq!(run(&engine, "items").unwrap().rows, inserted);
        let unchanged = run(&engine, "update items where id=1 set state=\"sql\"").unwrap();
        assert_eq!(
            (unchanged.matched, unchanged.affected, unchanged.changed),
            (1, 1, 0)
        );
    });
    assert_eq!(sql_rows(&admin), inserted);

    admin
        .sql_session()
        .execute("update items set state='sql changed' where id=1", &[], None)
        .unwrap();
    let sql_changed = sql_rows(&admin);
    assert_ne!(sql_changed[0].version, inserted[0].version);
    with_sql_parser_forbidden(|| {
        assert_eq!(run(&engine, "items").unwrap().rows, sql_changed);
        assert_eq!(
            run(&engine, "update items where id=1 set state=\"native\"")
                .unwrap()
                .changed,
            1
        );
    });
    let native_changed = sql_rows(&admin);
    assert_eq!(native_changed[0].values[1], Value::Text("native".into()));
    assert_ne!(native_changed[0].version, sql_changed[0].version);
}

#[test]
fn direct_tree_input_limits_and_native_identifiers_remain_enforced() {
    let (admin, engine) = fixture();
    with_sql_parser_forbidden(|| {
        let oversized = Statement::Insert {
            table: "items".into(),
            values: vec![
                ("id".into(), literal(Value::Int(1))),
                ("state".into(), literal(Value::Text("x".repeat(MAX_INPUT)))),
            ],
        };
        assert_eq!(
            engine.execute(&oversized, &Bindings::new()),
            Err(Error::Limit)
        );
        let mut filter = Expr::Value(literal(Value::Bool(true)));
        for _ in 0..=MAX_DEPTH {
            filter = Expr::Not(Box::new(filter));
        }
        let too_deep = Statement::Read {
            table: "items".into(),
            filter: Some(filter),
            order: vec![],
            limit: None,
            projection: vec![],
        };
        assert_eq!(
            engine.execute(&too_deep, &Bindings::new()),
            Err(Error::Limit)
        );
        let reserved = Statement::Read {
            table: "items".into(),
            filter: None,
            order: vec![],
            limit: None,
            projection: vec!["claim".into()],
        };
        assert_eq!(
            engine.execute(&reserved, &Bindings::new()),
            Err(Error::Bind("identifier".into()))
        );
    });
    assert!(sql_rows(&admin).is_empty());
}
