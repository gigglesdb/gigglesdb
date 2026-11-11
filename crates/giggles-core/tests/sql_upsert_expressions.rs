use giggles_core::{
    Admin, CoreError, Limits, SqlSession,
    relational::{SqlError as E, SqlResult, Value as V},
};
use std::sync::atomic::{AtomicU64, Ordering};
static NEXT: AtomicU64 = AtomicU64::new(0);

fn database(test: impl FnOnce(&Admin, &mut SqlSession)) {
    let path = std::env::temp_dir().join(format!(
        "core-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let admin = Admin::create(&path, Limits::default()).unwrap();
    let mut session = admin.sql_session();
    run(
        &mut session,
        "CREATE TABLE c (k text PRIMARY KEY, count int NOT NULL DEFAULT 0, updated timestamptz, note text)",
    );
    run(
        &mut session,
        "INSERT INTO c VALUES ('a', 1, '2024-01-01 00:00+00', 'x')",
    );
    test(&admin, &mut session);
    drop(session);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}
fn run(s: &mut SqlSession, sql: &str) -> SqlResult {
    s.execute(sql, &[], None)
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"))
}
fn values(s: &mut SqlSession) -> Vec<Vec<V>> {
    run(s, "SELECT k,count,updated,note FROM c ORDER BY k")
        .rows
        .into_iter()
        .map(|r| r.values)
        .collect()
}
fn returned(s: &mut SqlSession, sql: &str, expected: Vec<V>) {
    let result = run(s, sql);
    assert_eq!(result.affected, 1, "{sql}");
    assert_eq!(result.rows.len(), 1, "{sql}");
    assert_eq!(result.rows[0].values, expected, "{sql}");
}
fn error(s: &mut SqlSession, sql: &str, expected: E) {
    assert_eq!(
        s.execute(sql, &[], None),
        Err(CoreError::Sql(expected)),
        "{sql}"
    );
}
fn text(s: &str) -> V {
    V::Text(s.into())
}

#[test]
fn pg186_literal_oracle_aliases_functions_guard_tuple_default_and_returning() {
    database(|_, s| {
        let feb = run(s, "SELECT CAST('2024-02-01 00:00+00' AS timestamptz)")
            .rows
            .remove(0)
            .values
            .remove(0);
        returned(
            s,
            "INSERT INTO c AS t VALUES ('a', 5, '2024-02-01 00:00+00', 'y') ON CONFLICT (k) DO UPDATE SET count = t.count + excluded.count, updated = excluded.updated RETURNING *",
            vec![text("a"), V::Int(6), feb.clone(), text("x")],
        );
        returned(
            s,
            "INSERT INTO c VALUES ('a', 2, NULL, 'z') ON CONFLICT (k) DO UPDATE SET count = c.count + excluded.count, note = c.note || excluded.note RETURNING *",
            vec![text("a"), V::Int(8), feb.clone(), text("xz")],
        );
        for (sql, count) in [
            (
                "INSERT INTO c (k, count) VALUES ('a', 100) ON CONFLICT (k) DO UPDATE SET count = excluded.count + 100 RETURNING count",
                200,
            ),
            (
                "INSERT INTO c (k, count) VALUES ('a', 1) ON CONFLICT (k) DO UPDATE SET count = GREATEST(c.count, excluded.count) RETURNING count",
                200,
            ),
            (
                "INSERT INTO c (k, count) VALUES ('a', 1) ON CONFLICT (k) DO UPDATE SET count = coalesce(excluded.count, 0) * 2 RETURNING count",
                2,
            ),
            (
                "INSERT INTO c (k, count) VALUES ('a', 7) ON CONFLICT (k) DO UPDATE SET count = excluded.count WHERE c.count < excluded.count RETURNING count",
                7,
            ),
        ] {
            returned(s, sql, vec![V::Int(count)]);
        }
        let skipped = run(
            s,
            "INSERT INTO c (k, count) VALUES ('a', 3) ON CONFLICT (k) DO UPDATE SET count = excluded.count WHERE c.count < excluded.count RETURNING count",
        );
        assert_eq!(skipped.affected, 0);
        assert!(skipped.rows.is_empty());
        assert_eq!(
            values(s),
            vec![vec![text("a"), V::Int(7), feb.clone(), text("xz")]]
        );
        returned(
            s,
            "INSERT INTO c (k, count) VALUES ('b', 4) ON CONFLICT (k) DO UPDATE SET count = c.count + excluded.count RETURNING *",
            vec![text("b"), V::Int(4), V::Null, V::Null],
        );
        for (sql, expected) in [
            (
                "INSERT INTO c (k, count) VALUES ('a', 1) ON CONFLICT (k) DO UPDATE SET count = count + 1",
                E::AmbiguousColumn,
            ),
            (
                "INSERT INTO c (k, count) VALUES ('a', 1) ON CONFLICT (k) DO UPDATE SET count = excluded.nosuch",
                E::UnknownColumn,
            ),
            (
                "INSERT INTO c (k, count) VALUES ('a', 1) ON CONFLICT (k) DO UPDATE SET count = other.count",
                E::UnknownTable,
            ),
            (
                "INSERT INTO c (k, count) VALUES ('a', 1) ON CONFLICT (k) DO UPDATE SET count = excluded.count / 0",
                E::DivisionByZero,
            ),
            (
                "INSERT INTO c (k, count) VALUES ('a', 2147483647) ON CONFLICT (k) DO UPDATE SET count = c.count + excluded.count",
                E::Overflow,
            ),
            (
                "INSERT INTO c (k, count) VALUES ('a', 1), ('a', 2) ON CONFLICT (k) DO UPDATE SET count = excluded.count",
                E::CardinalityViolation,
            ),
            (
                "INSERT INTO c (k, count) VALUES ('a', $$5$$::int) ON CONFLICT (k) DO UPDATE SET count = c.count * excluded.count, updated = now() IS NOT NULL OR true RETURNING count",
                E::Type,
            ),
        ] {
            let before = values(s);
            error(s, sql, expected);
            assert_eq!(values(s), before, "{sql}");
        }
        // PG accepts a scalar SELECT here; the bounded SQL subset intentionally does not.
        error(
            s,
            "INSERT INTO c (k, count) VALUES ('a', 1) ON CONFLICT (k) DO UPDATE SET count = (SELECT 1)",
            E::Syntax,
        );
        returned(
            s,
            "INSERT INTO c (k, count) VALUES ('a', 1) ON CONFLICT (k) DO UPDATE SET (count, note) = (excluded.count + 1, upper(excluded.note)) RETURNING count, note",
            vec![V::Int(2), V::Null],
        );
        returned(
            s,
            "INSERT INTO c VALUES ('a', 1, NULL, 'w') ON CONFLICT (k) DO UPDATE SET count = DEFAULT RETURNING count",
            vec![V::Int(0)],
        );
        assert_eq!(
            values(s),
            vec![
                vec![text("a"), V::Int(0), feb, V::Null],
                vec![text("b"), V::Int(4), V::Null, V::Null]
            ]
        );
    });
}

#[test]
fn scalar_guards_bind_parameters_case_casts_and_name_resolution() {
    database(|_, s| {
        for guard in ["FALSE", "NULL", "c.count > excluded.count"] {
            let before = run(s, "SELECT * FROM c").rows;
            let result = run(
                s,
                &format!(
                    "INSERT INTO c (k, count) VALUES ('a', 10) ON CONFLICT(k) DO UPDATE SET count=excluded.count/0 WHERE {guard} RETURNING *"
                ),
            );
            assert_eq!(result.affected, 0);
            assert!(result.rows.is_empty());
            assert_eq!(run(s, "SELECT * FROM c").rows, before);
        }
        returned(
            s,
            "INSERT INTO c AS t (k, count) VALUES ('a', 3) ON CONFLICT(k) DO UPDATE SET count=CASE WHEN t.count < excluded.count AND TRUE THEN CAST(excluded.count AS integer) + abs(-2) ELSE 0 END WHERE TRUE RETURNING count",
            vec![V::Int(5)],
        );
        // Both accepted target qualifiers resolve to the old target image.
        returned(
            s,
            "INSERT INTO c AS t (k, count) VALUES ('a', 3) ON CONFLICT(k) DO UPDATE SET count=c.count+t.count+excluded.count RETURNING count",
            vec![V::Int(13)],
        );
        let p = s.prepare("INSERT INTO c (k, count) VALUES ('a', 2) ON CONFLICT(k) DO UPDATE SET count=c.count+excluded.count+$1 WHERE c.count < $2 RETURNING count").unwrap();
        let result = s
            .execute_prepared(&p, &[V::Int(4), V::Int(100)], None)
            .unwrap();
        assert_eq!(result.rows[0].values, [V::Int(19)]);
        for (rhs, expected) in [
            ("nosuch+1", E::UnknownColumn),
            ("count+1", E::AmbiguousColumn),
        ] {
            error(
                s,
                &format!("INSERT INTO c (k, count) VALUES ('a', 1) ON CONFLICT(k) DO UPDATE SET count={rhs}"),
                expected,
            );
        }
        for (guard, expected) in [
            ("count>0", E::AmbiguousColumn),
            ("nosuch>0", E::UnknownColumn),
            ("other.count>0", E::UnknownTable),
        ] {
            error(
                s,
                &format!(
                    "INSERT INTO c (k, count) VALUES ('a', 1) ON CONFLICT(k) DO UPDATE SET count=excluded.count WHERE {guard}"
                ),
                expected,
            );
        }
        for sql in [
            "INSERT INTO c (k, count) VALUES ('a', 1) ON CONFLICT(k) DO UPDATE SET count=CASE WHEN c.count IN (SELECT 1) THEN 1 ELSE 2 END",
            "INSERT INTO c (k, count) VALUES ('a', 1) ON CONFLICT(k) DO UPDATE SET count=excluded.count WHERE c.count IN (SELECT 1)",
        ] {
            error(s, sql, E::Unsupported);
        }
        returned(
            s,
            "INSERT INTO c VALUES ('plain', 9, NULL, 'inserted') RETURNING k,count,note",
            vec![text("plain"), V::Int(9), text("inserted")],
        );
    });
}

#[test]
fn least_declared_assignment_and_boolean_set_use_scalar_types() {
    use giggles_core::relational::DataType;
    database(|_, s| {
        returned(
            s,
            "INSERT INTO c (k, count) VALUES ('a', 9) ON CONFLICT(k) DO UPDATE SET count=LEAST(c.count, excluded.count) RETURNING count",
            vec![V::Int(1)],
        );
        for declared in [DataType::SmallInt, DataType::BigInt] {
            let prepared = s
                .prepare("INSERT INTO c (k, count) VALUES ('a', 9) ON CONFLICT(k) DO UPDATE SET count=$1 RETURNING count")
                .unwrap()
                .with_parameter_types(&[Some(declared.clone())])
                .unwrap();
            assert_eq!(s.describe(&prepared).unwrap().parameters, [declared]);
            let result = s.execute_prepared(&prepared, &[V::Int(7)], None).unwrap();
            assert_eq!(result.affected, 1);
            assert_eq!(result.rows[0].values, [V::Int(7)]);
        }
        run(
            s,
            "CREATE TABLE flags(id integer PRIMARY KEY, n integer, flag boolean)",
        );
        run(s, "INSERT INTO flags VALUES(1, 2, FALSE)");
        returned(
            s,
            "INSERT INTO flags VALUES(1, 3, FALSE) ON CONFLICT(id) DO UPDATE SET flag=(excluded.n > flags.n AND NOT flags.flag) OR excluded.flag RETURNING flag",
            vec![V::Bool(true)],
        );
        let before = run(s, "SELECT * FROM flags").rows;
        for (guard, expected) in [("flags.n", E::Type), ("flags.n / 0 > 0", E::DivisionByZero)] {
            error(
                s,
                &format!(
                    "INSERT INTO flags VALUES(1, 4, FALSE) ON CONFLICT(id) DO UPDATE SET flag=excluded.n > flags.n WHERE {guard}"
                ),
                expected,
            );
            assert_eq!(run(s, "SELECT * FROM flags").rows, before);
        }
    });
}

#[test]
fn multirow_scalar_failure_is_atomic_and_set_reads_simultaneous_old_image() {
    database(|_, s| {
        let before = run(s, "SELECT * FROM c").rows;
        for sql in [
            "INSERT INTO c (k, count) VALUES ('new', 8), ('a', 0) ON CONFLICT(k) DO UPDATE SET count=c.count/excluded.count",
            "INSERT INTO c (k, count) VALUES ('a', 2), ('new', 8), ('a', 3) ON CONFLICT(k) DO UPDATE SET count=excluded.count",
        ] {
            assert!(s.execute(sql, &[], None).is_err());
            assert_eq!(run(s, "SELECT * FROM c").rows, before);
        }
        returned(
            s,
            "INSERT INTO c (k, count) VALUES ('a', 10) ON CONFLICT(k) DO UPDATE SET count=c.count+excluded.count,note=CAST(c.count AS text) RETURNING count,note",
            vec![V::Int(11), text("1")],
        );
    });
}

#[test]
fn native_typed_conflict_expression_shape_and_execution_are_unchanged() {
    use giggles_core::operations::{
        Arithmetic, Bindings, ConflictAction, ConflictSource, Engine, Expr, Request, ValueExpr,
    };
    database(|admin, s| {
        run(
            s,
            "CREATE TABLE native_items(k text PRIMARY KEY, count integer)",
        );
        run(s, "INSERT INTO native_items VALUES ('a', 1)");
        let literal = |v| Expr::Value(ValueExpr::Literal(v));
        let request = Request::OnConflict {
            request: Box::new(Request::InsertExpressions {
                table: "native_items".into(),
                values: vec![
                    ("k".into(), literal(text("a"))),
                    ("count".into(), literal(V::Int(4))),
                ],
            }),
            target: vec!["k".into()],
            action: ConflictAction::Update {
                guard: None,
                values: vec![(
                    "count".into(),
                    Expr::Arithmetic(
                        Box::new(Expr::ConflictField {
                            source: ConflictSource::Existing,
                            field: "count".into(),
                        }),
                        Arithmetic::Add,
                        Box::new(Expr::ConflictField {
                            source: ConflictSource::Incoming,
                            field: "count".into(),
                        }),
                    ),
                )],
            },
        };
        assert_eq!(
            serde_json::to_value(&request).unwrap(),
            serde_json::json!({
                "OnConflict": {
                    "request": {"InsertExpressions": {
                        "table": "native_items",
                        "values": [["k", {"Value": {"Literal": {"Text": "a"}}}],
                                   ["count", {"Value": {"Literal": {"Int": 4}}}]]
                    }},
                    "target": ["k"],
                    "action": {"Update": {"guard": null, "values": [["count", {"Arithmetic": [
                        {"ConflictField": {"source": "Existing", "field": "count"}},
                        "Add",
                        {"ConflictField": {"source": "Incoming", "field": "count"}}
                    ]}]]}}
                }
            })
        );
        let result = Engine::new(admin.clone())
            .execute(&request, &Bindings::new())
            .unwrap();
        assert_eq!((result.matched, result.affected, result.changed), (1, 1, 1));
        assert_eq!(
            run(s, "SELECT count FROM native_items").rows[0].values,
            [V::Int(5)]
        );
    });
}

#[test]
fn sql_expression_assignment_rejects_varchar_overflow() {
    database(|_, s| {
        run(s, "CREATE TABLE bounded_v(id integer PRIMARY KEY, v varchar(3))");
        run(s, "INSERT INTO bounded_v VALUES (1, 'old')");
        error(
            s,
            "INSERT INTO bounded_v VALUES (1, 'ab') ON CONFLICT(id) DO UPDATE SET v=excluded.v || 'xx'",
            E::StringDataRightTruncation,
        );
        assert_eq!(run(s, "SELECT v FROM bounded_v").rows[0].values, [text("old")]);
    });
}

#[test]
fn sql_expression_assignment_pads_char_to_declared_length() {
    database(|_, s| {
        run(s, "CREATE TABLE bounded_c(id integer PRIMARY KEY, v char(4), source text)");
        run(s, "INSERT INTO bounded_c VALUES (1, 'old', 'old')");
        returned(
            s,
            "INSERT INTO bounded_c VALUES (1, 'a', 'b') ON CONFLICT(id) DO UPDATE SET v=excluded.source || 'x' RETURNING v",
            vec![text("bx  ")],
        );
        assert_eq!(run(s, "SELECT v FROM bounded_c").rows[0].values, [text("bx  ")]);
    });
}
