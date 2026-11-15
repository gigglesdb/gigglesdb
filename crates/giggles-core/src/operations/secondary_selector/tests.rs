use super::*;
use crate::Limits;

#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2_indexed_native_reads_match_sql_and_fallback_with_private_writes_and_drop_fence() {
    use crate::Engine as Backend;
    let mut paths = Vec::new();
    let mut admins = Vec::new();
    for indexed in [false, true] {
        let mut token = [0; 16];
        getrandom::fill(&mut token).unwrap();
        let path = std::env::temp_dir().join(format!("v2-native-index-{:032x}", u128::from_be_bytes(token)));
        let admin = Admin::create_engine(&path, Limits::default(), Backend::V2).unwrap();
        let mut sql = admin.sql_session();
        for statement in [
            "CREATE TABLE items(id BIGINT PRIMARY KEY, tag TEXT, score SMALLINT)",
            "INSERT INTO items VALUES(1,'z',10),(2,NULL,20),(3,'a',30),(4,'z',40)",
        ] { sql.execute(statement, &[], None).unwrap(); }
        if indexed {
            sql.execute("CREATE INDEX by_tag ON items(tag,score)", &[], None).unwrap();
        }
        paths.push(path);
        admins.push(admin);
    }
    let engines = admins.iter().cloned().map(Engine::new).collect::<Vec<_>>();
    let mut sessions = admins.iter().map(Admin::sql_session).collect::<Vec<_>>();
    let term = |field: &str, op, value| Expr::Compare(Box::new(Expr::Field(field.into())), op,
        Box::new(Expr::Value(ValueExpr::Literal(value))));
    let request = |filter| Request::Read { table: "items".into(), filter: Some(filter), order: vec![],
        limit: None, projection: vec!["id".into(), "tag".into(), "score".into()] };
    SUCCESSES.set(0);
    for transactional in [false, true] {
        if transactional {
            for session in &mut sessions {
                session.begin_native().unwrap();
                for statement in ["INSERT INTO items VALUES(5,'z',50)",
                    "UPDATE items SET tag='z',score=35 WHERE id=3", "DELETE FROM items WHERE id=1"] {
                    session.execute(statement, &[], None).unwrap();
                }
            }
        }
        for (filter, predicate) in [
            (term("tag", Compare::Eq, Value::Text("z".into())), "tag='z'"),
            (term("tag", Compare::Lt, Value::Text("z".into())), "tag<'z'"),
            (term("tag", Compare::Ge, Value::Text("a".into())), "tag>='a'"),
            (Expr::And(Box::new(term("tag", Compare::Eq, Value::Text("z".into()))),
                Box::new(Expr::And(Box::new(term("score", Compare::Gt, Value::Int(10))),
                    Box::new(term("score", Compare::Le, Value::Int(40)))))), "tag='z' AND score>10 AND score<=40"),
        ] {
            let request = request(filter);
            let mut results = Vec::new();
            for i in 0..2 {
                let expected = sessions[i].execute(&format!(
                    "SELECT id,tag,score FROM items WHERE {predicate} ORDER BY id"), &[], None)
                    .unwrap().rows.into_iter().map(|r| r.values).collect::<Vec<_>>();
                if transactional && predicate == "tag='z'" {
                    assert_eq!(expected.iter().map(|r| r[0].clone()).collect::<Vec<_>>(),
                        vec![Value::Int(3), Value::Int(4), Value::Int(5)]);
                }
                let before = SUCCESSES.get();
                let result = crate::relational::with_sql_parser_forbidden(|| {
                    if transactional { sessions[i].execute_native(&request, &Bindings::new()) }
                    else { engines[i].execute(&request, &Bindings::new()) }
                }).unwrap();
                assert_eq!(SUCCESSES.get() - before, i, "only the indexed database must use the accessor");
                let actual = result.rows.into_iter().map(|r| r.values).collect::<Vec<_>>();
                assert_eq!(actual, expected);
                results.push(actual);
            }
            assert_eq!(results[0], results[1]);
        }
        if transactional { for session in &mut sessions { session.rollback_native().unwrap(); } }
    }
    assert_eq!(SUCCESSES.get(), 8);
    let outside = request(Expr::And(Box::new(term("tag", Compare::Eq, Value::Text("z".into()))),
        Box::new(term("score", Compare::Gt, Value::Int(32768)))));
    let expected = engines[0].execute(&outside, &Bindings::new()).unwrap();
    assert!(expected.rows.is_empty());
    assert_eq!(engines[1].execute(&outside, &Bindings::new()).unwrap(), expected);
    assert_eq!(SUCCESSES.get(), 8, "out-of-domain comparison operands must fall back");
    let bound = engines[1].prepare_bound(&request(term("tag", Compare::Eq, Value::Text("z".into()))),
        &Bindings::new()).unwrap();
    sessions[1].execute("DROP INDEX by_tag", &[], None).unwrap();
    assert_eq!(engines[1].execute_bound(&bound), Err(core(CoreError::Sql(SqlError::StaleSchema))));
    drop(sessions);
    drop(engines);
    drop(admins);
    for path in paths { std::fs::remove_dir_all(path).unwrap(); }
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2_secondary_projection_limit_preserves_complete_fallback() {
    let mut token = [0; 16];
    getrandom::fill(&mut token).unwrap();
    let path = std::env::temp_dir().join(format!("v2-native-index-limit-{:032x}", u128::from_be_bytes(token)));
    let admin = Admin::create_engine(&path, Limits::default(), crate::Engine::V2).unwrap();
    let mut session = admin.sql_session();
    session.execute("CREATE TABLE items(id BIGINT PRIMARY KEY, tag TEXT, payload NUMERIC)", &[], None).unwrap();
    let values = (0..33).map(|id| format!("({id},'x',1)")).collect::<Vec<_>>().join(",");
    session.execute(&format!("INSERT INTO items VALUES {values}"), &[], None).unwrap();
    let engine = Engine::new(admin.clone());
    let request = read(Compare::Eq, Value::Text("x".into()));
    let expected = engine.execute(&request, &Bindings::new()).unwrap();
    session.execute("CREATE INDEX by_tag ON items(tag)", &[], None).unwrap();
    SUCCESSES.set(0);
    let actual = crate::relational::with_sql_parser_forbidden(|| engine.execute(&request, &Bindings::new())).unwrap();
    assert_eq!(actual, expected);
    assert_eq!(actual.rows.len(), 33);
    assert_eq!(SUCCESSES.get(), 0, "the accessor's conservative Numeric quota must fall back");
    drop(engine);
    drop(session);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

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
            .execute("CREATE INDEX CONCURRENTLY bad ON items(tag)", &[], None)
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
