use super::*;
use crate::Limits;

fn fixture(indexed: bool) -> (Admin, Engine) {
    let mut token = [0; 16];
    getrandom::fill(&mut token).unwrap();
    let path = std::env::temp_dir().join(format!(
        "gd42-native-index-{:032x}",
        u128::from_be_bytes(token)
    ));
    let admin = Admin::create(path, Limits::default()).unwrap();
    admin
        .sql_session()
        .execute(
            "CREATE TABLE items (id BIGINT PRIMARY KEY, tag TEXT, payload NUMERIC)",
            &[],
            None,
        )
        .unwrap();
    let engine = Engine::new(admin.clone());
    if indexed {
        crate::relational::with_sql_parser_forbidden(|| {
            engine.execute_catalog(&CatalogRequest::CreateIndex {
                table: "items".into(),
                name: "by_tag".into(),
                columns: vec!["tag".into()],
            })
        })
        .unwrap();
    }
    (admin, engine)
}
fn read(op: Compare, value: Value) -> Request {
    Request::Read {
        table: "items".into(),
        filter: Some(Expr::Compare(
            Box::new(Expr::Field("tag".into())),
            op,
            Box::new(Expr::Value(ValueExpr::Literal(value))),
        )),
        order: vec![],
        limit: None,
        projection: vec!["id".into(), "payload".into()],
    }
}
#[test]
fn secondary_selector_native_matches_fallback_and_explain_keeps_materialization() {
    let (_plain, p) = fixture(false);
    let (_indexed, i) = fixture(true);
    SUCCESSES.with(|n| n.set(0));
    for engine in [&p, &i] {
        for (id, tag, number) in [
            (1, Some("z"), "1.25"),
            (2, None, "2.50"),
            (3, Some("a"), "3.75"),
            (4, Some("z"), "4.00"),
        ] {
            engine
                .execute(
                    &Request::Insert {
                        table: "items".into(),
                        values: vec![
                            ("id".into(), ValueExpr::Literal(Value::Int(id))),
                            (
                                "tag".into(),
                                ValueExpr::Literal(
                                    tag.map_or(Value::Null, |s| Value::Text(s.into())),
                                ),
                            ),
                            (
                                "payload".into(),
                                ValueExpr::Literal(Value::Numeric(
                                    crate::relational::Numeric::parse(number).unwrap(),
                                )),
                            ),
                        ],
                    },
                    &Bindings::new(),
                )
                .unwrap();
        }
    }
    for (op, value) in [
        (Compare::Eq, Value::Text("z".into())),
        (Compare::Lt, Value::Text("z".into())),
        (Compare::Ge, Value::Text("a".into())),
        (Compare::Eq, Value::Null),
    ] {
        let request = read(op, value);
        let expected = p.execute(&request, &Bindings::new()).unwrap();
        let actual =
            crate::relational::with_sql_parser_forbidden(|| i.execute(&request, &Bindings::new()))
                .unwrap();
        assert_eq!(actual, expected);
        let explain = i.explain_read(&request, &Bindings::new()).unwrap();
        assert!(
            explain
                .stages
                .contains(&ReadExplainStage::BoundedCompleteTableMaterialization)
        );
        let uses_index = explain
            .stages
            .iter()
            .any(|s| matches!(s, ReadExplainStage::SecondaryIndexCandidates { .. }));
        assert_eq!(
            uses_index,
            !matches!(request, Request::Read { filter: Some(Expr::Compare(_, _, ref v)), .. } if matches!(v.as_ref(), Expr::Value(ValueExpr::Literal(Value::Null))))
        );
        assert_eq!(explain.estimated_cost, None);
    }
    assert_eq!(
        SUCCESSES.with(|n| n.get()),
        3,
        "three actual secondary engine successes; NULL comparison falls back"
    );
}
#[test]
fn secondary_selector_catalog_stale_populated_and_errors_preserve_allocator() {
    let (admin, engine) = fixture(false);
    let request = CatalogRequest::CreateIndex {
        table: "items".into(),
        name: "by_tag".into(),
        columns: vec!["tag".into()],
    };
    let bound = engine.prepare_catalog_bound(&request).unwrap();
    let bound_read = engine
        .prepare_bound(
            &read(Compare::Eq, Value::Text("x".into())),
            &Bindings::new(),
        )
        .unwrap();
    let bad = CatalogRequest::CreateIndex {
        table: "items".into(),
        name: "bad".into(),
        columns: vec!["payload".into()],
    };
    assert_eq!(engine.execute_catalog(&bad), Err(Error::Unsupported));
    let before = admin.catalog().unwrap().0;
    let created = engine.execute_catalog(&request).unwrap();
    assert_eq!(created.catalog_epoch, before + 1);
    let table = &created.table;
    assert_eq!(
        created.index_id,
        Some(table.columns.iter().map(|c| c.id).max().unwrap() + 1)
    );
    assert!(engine.execute_catalog_bound(&bound).is_err());
    assert!(engine.execute_bound(&bound_read).is_err());
    let epoch = admin.catalog().unwrap().0;
    assert!(engine.execute_catalog(&request).is_err());
    assert_eq!(admin.catalog().unwrap().0, epoch);
    admin
        .sql_session()
        .execute("INSERT INTO items VALUES (1,'x',NULL)", &[], None)
        .unwrap();
    let another = CatalogRequest::CreateIndex {
        table: "items".into(),
        name: "another".into(),
        columns: vec!["tag".into()],
    };
    assert_eq!(engine.execute_catalog(&another), Err(Error::PlanRequired));
    assert_eq!(admin.catalog().unwrap().0, epoch);
}

#[test]
fn secondary_selector_raw_options_reject_before_any_catalog_effect() {
    let (admin, engine) = fixture(false);
    let before = admin.catalog().unwrap();
    for option in [
        "unique",
        "include",
        "where",
        "concurrently",
        "descending",
        "raw",
    ] {
        let json = format!(
            r#"{{"CreateIndex":{{"table":"items","name":"bad","columns":["tag"],"{option}":true}}}}"#
        );
        assert!(serde_json::from_str::<CatalogRequest>(&json).is_err());
    }
    assert!(
        admin
            .sql_session()
            .execute("CREATE INDEX bad ON items(tag)", &[], None)
            .is_err()
    );
    assert_eq!(admin.catalog().unwrap(), before);
    let index = engine
        .execute_catalog(&CatalogRequest::CreateIndex {
            table: "items".into(),
            name: "valid".into(),
            columns: vec!["tag".into()],
        })
        .unwrap();
    assert_eq!(
        index.index_id,
        Some(index.table.columns.last().unwrap().id + 1)
    );
}

#[test]
fn secondary_selector_conservative_projection_does_not_narrow_read() {
    let (_, plain) = fixture(false);
    let (_, indexed) = fixture(true);
    for engine in [&plain, &indexed] {
        for id in 0..33 {
            engine
                .execute(
                    &Request::Insert {
                        table: "items".into(),
                        values: vec![
                            ("id".into(), ValueExpr::Literal(Value::Int(id))),
                            ("tag".into(), ValueExpr::Literal(Value::Text("x".into()))),
                            (
                                "payload".into(),
                                ValueExpr::Literal(Value::Numeric(
                                    crate::relational::Numeric::parse("1").unwrap(),
                                )),
                            ),
                        ],
                    },
                    &Bindings::new(),
                )
                .unwrap();
        }
    }
    let request = read(Compare::Eq, Value::Text("x".into()));
    let expected = plain.execute(&request, &Bindings::new()).unwrap();
    assert_eq!(expected.rows.len(), 33);
    assert_eq!(
        indexed.execute(&request, &Bindings::new()).unwrap(),
        expected
    );
}

#[test]
fn worker3_optional_limit_explain_is_candidate_and_fallback_keeps_complete_input() {
    let (_, engine) = fixture(true);
    for id in 0..33 {
        engine
            .execute(
                &Request::Insert {
                    table: "items".into(),
                    values: vec![
                        ("id".into(), ValueExpr::Literal(Value::Int(id))),
                        ("tag".into(), ValueExpr::Literal(Value::Text("x".into()))),
                        (
                            "payload".into(),
                            ValueExpr::Literal(Value::Numeric(
                                crate::relational::Numeric::parse("1").unwrap(),
                            )),
                        ),
                    ],
                },
                &Bindings::new(),
            )
            .unwrap();
    }
    let request = read(Compare::Eq, Value::Text("x".into()));
    let explain = engine.explain_read(&request, &Bindings::new()).unwrap();
    assert!(
        explain
            .stages
            .iter()
            .any(|s| matches!(s, ReadExplainStage::SecondaryIndexCandidates { .. }))
    );
    assert!(
        explain
            .stages
            .contains(&ReadExplainStage::BoundedCompleteTableMaterialization)
    );
    assert_eq!(explain.estimated_cost, None);
    SUCCESSES.with(|n| n.set(0));
    assert_eq!(
        engine
            .execute(&request, &Bindings::new())
            .unwrap()
            .rows
            .len(),
        33
    );
    assert_eq!(
        SUCCESSES.with(|n| n.get()),
        0,
        "optional quota forces fallback despite candidate Explain stage"
    );
}
