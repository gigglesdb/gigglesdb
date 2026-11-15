mod common;

use giggles_core::{
    Admin, CrashPoint, Limits,
    operations::{
        Bindings, Compare, Engine, Error, Expr, Intent, Outcome, Request, Sort, ValueExpr,
    },
    relational::Value as V,
};

fn fixture() -> (std::path::PathBuf, Admin, Engine) {
    let path = common::fresh("operations");
    let admin = Admin::create(&path, Limits::default()).unwrap();
    admin
        .sql_session()
        .execute(
            "create table items (id bigint primary key, state text not null, n integer, note text)",
            &[],
            None,
        )
        .unwrap();
    let engine = Engine::new(admin.clone());
    (path, admin, engine)
}

fn literal(v: V) -> ValueExpr {
    ValueExpr::Literal(v)
}
fn yes() -> Expr {
    Expr::Value(literal(V::Bool(true)))
}
fn equals(field: &str, v: V) -> Expr {
    Expr::Compare(
        Box::new(Expr::Field(field.into())),
        Compare::Eq,
        Box::new(Expr::Value(literal(v))),
    )
}
fn read(table: &str) -> Request {
    Request::Read {
        table: table.into(),
        filter: None,
        order: vec![],
        limit: Some(literal(V::Int(1024))),
        projection: vec![],
    }
}
fn run(engine: &Engine, request: &Request) -> giggles_core::operations::Result<Outcome> {
    engine.execute(request, &Bindings::new())
}
fn insert(engine: &Engine, id: i64) {
    run(
        engine,
        &Request::Insert {
            table: "items".into(),
            values: vec![
                ("id".into(), literal(V::Int(id))),
                ("state".into(), literal(V::Text("ready".into()))),
            ],
        },
    )
    .unwrap();
}
fn update(filter: Expr, guard: Option<Expr>, intent: Intent, state: &str) -> Request {
    Request::Update {
        table: "items".into(),
        filter,
        guard,
        intent,
        values: vec![("state".into(), literal(V::Text(state.into())))],
    }
}

#[test]
fn direct_requests_share_sql_storage_and_keep_bound_payloads_as_data() {
    let (_, admin, engine) = fixture();
    let payload = V::Text("'; delete from items; --\0".into());
    let request = Request::Insert {
        table: "items".into(),
        values: vec![
            ("id".into(), literal(V::Int(1))),
            ("state".into(), literal(V::Text("ready".into()))),
            ("note".into(), ValueExpr::Parameter("payload".into())),
        ],
    };
    let outcome = engine
        .execute(
            &request,
            &Bindings::from([("payload".into(), payload.clone())]),
        )
        .unwrap();
    assert_eq!(
        (outcome.matched, outcome.affected, outcome.changed),
        (1, 1, 1)
    );
    let sql = admin
        .sql_session()
        .execute("select note from items where id=1", &[], None)
        .unwrap();
    assert_eq!(sql.rows[0].values, [payload]);
    admin
        .sql_session()
        .execute("update items set state='sql' where id=1", &[], None)
        .unwrap();
    assert_eq!(
        run(&engine, &read("items")).unwrap().rows[0].values[1],
        V::Text("sql".into())
    );
}

#[test]
fn caps_and_singular_cardinality_are_checked_before_writes_and_guards() {
    let (_, _, engine) = fixture();
    for id in 1..=3 {
        insert(&engine, id);
    }
    let before = run(&engine, &read("items")).unwrap();
    let cap = Some(literal(V::Int(2)));
    assert_eq!(
        run(
            &engine,
            &update(
                yes(),
                None,
                Intent::Many {
                    cap: cap.clone().unwrap()
                },
                "done"
            )
        ),
        Err(Error::TooManyMatches)
    );
    assert_eq!(
        run(
            &engine,
            &Request::Delete {
                table: "items".into(),
                filter: yes(),
                guard: None,
                cap
            }
        ),
        Err(Error::TooManyMatches)
    );
    let false_guard = Some(Expr::Value(literal(V::Bool(false))));
    assert_eq!(
        run(
            &engine,
            &update(yes(), false_guard.clone(), Intent::One, "done")
        ),
        Err(Error::AmbiguousMatch)
    );
    assert_eq!(
        run(
            &engine,
            &update(
                equals("id", V::Int(9)),
                false_guard.clone(),
                Intent::One,
                "done"
            )
        ),
        Err(Error::NotFound)
    );
    assert_eq!(
        run(
            &engine,
            &update(equals("id", V::Int(1)), false_guard, Intent::One, "done")
        ),
        Err(Error::ConditionFailed)
    );
    assert_eq!(run(&engine, &read("items")).unwrap(), before);
    assert_eq!(
        run(
            &engine,
            &update(
                yes(),
                None,
                Intent::Many {
                    cap: literal(V::Int(3))
                },
                "done"
            )
        )
        .unwrap()
        .changed,
        3
    );
    assert_eq!(
        run(
            &engine,
            &Request::Delete {
                table: "items".into(),
                filter: yes(),
                guard: None,
                cap: Some(literal(V::Int(3)))
            }
        )
        .unwrap()
        .changed,
        3
    );
    assert_eq!(run(&engine, &read("items")).unwrap().matched, 0);
}

#[test]
fn no_op_intents_preserve_versions_while_sql_keeps_its_existing_policy() {
    let (_, admin, engine) = fixture();
    insert(&engine, 1);
    let before = run(&engine, &read("items")).unwrap();
    let outcome = run(
        &engine,
        &update(equals("id", V::Int(1)), None, Intent::One, "ready"),
    )
    .unwrap();
    assert_eq!(
        (outcome.matched, outcome.affected, outcome.changed),
        (1, 1, 0)
    );
    assert_eq!(run(&engine, &read("items")).unwrap(), before);
    admin
        .sql_session()
        .execute("update items set state='ready' where id=1", &[], None)
        .unwrap();
    let after = run(&engine, &read("items")).unwrap();
    assert_eq!(after.rows[0].values, before.rows[0].values);
    assert!(after.rows[0].version > before.rows[0].version);
}

#[test]
fn claim_uses_complete_primary_key_ties_and_returns_persisted_postimage() {
    let (_, admin, engine) = fixture();
    admin
        .sql_session()
        .execute(
            "create table pairs (tenant bigint, id bigint, state text, primary key (tenant,id))",
            &[],
            None,
        )
        .unwrap();
    for (tenant, id) in [(2, 1), (1, 2), (1, 1)] {
        run(
            &engine,
            &Request::Insert {
                table: "pairs".into(),
                values: vec![
                    ("tenant".into(), literal(V::Int(tenant))),
                    ("id".into(), literal(V::Int(id))),
                    ("state".into(), literal(V::Text("ready".into()))),
                ],
            },
        )
        .unwrap();
    }
    let before = run(&engine, &read("pairs")).unwrap();
    let mut request = Request::Update {
        table: "pairs".into(),
        filter: equals("state", V::Text("ready".into())),
        guard: None,
        values: vec![("state".into(), literal(V::Text("ready".into())))],
        intent: Intent::Claim {
            order: vec![Sort {
                field: "state".into(),
                descending: false,
            }],
        },
    };
    assert_eq!(run(&engine, &request), Err(Error::ClaimNotConsumed));
    assert_eq!(run(&engine, &read("pairs")).unwrap(), before);
    if let Request::Update { values, .. } = &mut request {
        values[0].1 = literal(V::Text("claimed".into()));
    }
    let outcome = run(&engine, &request).unwrap();
    assert_eq!(
        (outcome.matched, outcome.affected, outcome.changed),
        (1, 1, 1)
    );
    assert_eq!(outcome.columns, ["tenant", "id", "state"]);
    assert_eq!(
        outcome.rows[0].values,
        [V::Int(1), V::Int(1), V::Text("claimed".into())]
    );
    assert!(outcome.rows[0].version > before.rows[0].version);
    let sql = admin
        .sql_session()
        .execute("select * from pairs where tenant=1 and id=1", &[], None)
        .unwrap();
    assert_eq!(outcome.rows, sql.rows);
}

#[test]
fn binding_rejects_missing_extra_wrong_type_and_invalid_empty_mutations() {
    let (_, _, engine) = fixture();
    let mut request = read("items");
    if let Request::Read { filter, .. } = &mut request {
        *filter = Some(Expr::Compare(
            Box::new(Expr::Field("id".into())),
            Compare::Eq,
            Box::new(Expr::Value(ValueExpr::Parameter("id".into()))),
        ));
    }
    assert_eq!(
        run(&engine, &request),
        Err(Error::Bind("parameter missing".into()))
    );
    assert_eq!(
        engine.execute(
            &request,
            &Bindings::from([("id".into(), V::Text("wrong".into()))])
        ),
        Err(Error::Type)
    );
    assert_eq!(
        engine.execute(
            &request,
            &Bindings::from([("id".into(), V::Int(1)), ("extra".into(), V::Int(0))])
        ),
        Err(Error::Bind("parameters".into()))
    );
    assert_eq!(
        engine
            .execute(&request, &Bindings::from([("id".into(), V::Int(1))]))
            .unwrap()
            .matched,
        0
    );
    for (name, value, expected) in [
        ("n", V::Int(i64::MAX), Error::Type),
        ("state", V::Null, Error::Type),
        ("absent", V::Int(1), Error::Sql { sqlstate: "42703".into() }),
    ] {
        let invalid = Request::Update {
            table: "items".into(),
            filter: yes(),
            guard: None,
            values: vec![(name.into(), literal(value))],
            intent: Intent::Many {
                cap: literal(V::Int(0)),
            },
        };
        assert_eq!(run(&engine, &invalid), Err(expected));
    }
    assert_eq!(run(&engine, &read("items")).unwrap().matched, 0);
}

#[test]
fn direct_request_depth_and_size_limits_fail_without_effects() {
    let (_, _, engine) = fixture();
    insert(&engine, 1);
    let before = run(&engine, &read("items")).unwrap();
    let mut too_deep = yes();
    for _ in 0..=giggles_core::operations::MAX_DEPTH {
        too_deep = Expr::Not(Box::new(too_deep));
    }
    assert_eq!(
        run(&engine, &update(too_deep, None, Intent::One, "done")),
        Err(Error::Limit)
    );
    assert_eq!(
        run(
            &engine,
            &update(
                yes(),
                None,
                Intent::One,
                &"x".repeat(giggles_core::operations::MAX_INPUT)
            )
        ),
        Err(Error::Limit)
    );
    assert_eq!(run(&engine, &read("items")).unwrap(), before);
}

#[test]
fn table_bound_proof_compares_every_component_of_the_primary_key() {
    let (_, admin, engine) = fixture();
    let mut sql = admin.sql_session();
    sql.execute(
        "create table pairs (tenant bigint, id bigint, state text, primary key (tenant,id))",
        &[],
        None,
    )
    .unwrap();
    sql.execute("begin", &[], None).unwrap();
    for id in 0..1024 {
        sql.execute(
            "insert into pairs values (1,$1,'ready')",
            &[V::Int(id)],
            None,
        )
        .unwrap();
    }
    sql.execute("commit", &[], None).unwrap();
    assert_eq!(run(&engine, &read("pairs")).unwrap().matched, 1024);
    let insert = Request::Insert {
        table: "pairs".into(),
        values: vec![
            ("tenant".into(), literal(V::Int(1))),
            ("id".into(), literal(V::Int(1024))),
        ],
    };
    assert_eq!(run(&engine, &insert), Err(Error::Limit));
    sql.execute("insert into pairs values (1,1024,'ready')", &[], None)
        .unwrap();
    assert_eq!(run(&engine, &read("pairs")), Err(Error::Limit));
    assert_eq!(
        run(
            &engine,
            &Request::Delete {
                table: "pairs".into(),
                filter: equals("id", V::Int(1024)),
                guard: None,
                cap: None
            }
        ),
        Err(Error::Limit)
    );
    assert_eq!(
        sql.execute(
            "select state from pairs where tenant=1 and id=1024",
            &[],
            None
        )
        .unwrap()
        .rows[0]
            .values,
        [V::Text("ready".into())]
    );
}

#[test]
fn storage_commit_failure_is_unknown_regardless_of_which_effect_persisted() {
    for (point, persisted) in [
        (CrashPoint::ErrorBeforeCommit, false),
        (CrashPoint::ErrorAfterCommit, true),
    ] {
        let (path, admin, engine) = fixture();
        insert(&engine, 1);
        admin.set_crash_point(point).unwrap();
        assert_eq!(
            run(
                &engine,
                &update(equals("id", V::Int(1)), None, Intent::One, "done")
            ),
            Err(Error::Unknown)
        );
        drop(engine);
        drop(admin);
        let engine = Engine::new(Admin::open(path, Limits::default()).unwrap());
        assert_eq!(
            run(&engine, &read("items")).unwrap().rows[0].values[1],
            V::Text(if persisted { "done" } else { "ready" }.into())
        );
    }
}
