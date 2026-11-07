use giggles_core::{
    Admin, CoreError, Limits, SqlSession,
    relational::{DataType as T, SqlError as E, SqlResult, SqlRow, Value as V},
};
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

static NEXT: AtomicU64 = AtomicU64::new(0);

fn database(test: impl FnOnce(&mut SqlSession)) {
    let path = std::env::temp_dir().join(format!(
        "giggles-sql-returning-{}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        NEXT.fetch_add(1, Ordering::Relaxed),
    ));
    let admin = Admin::create(&path, Limits::default()).unwrap();
    let mut session = admin.sql_session();
    run(
        &mut session,
        "CREATE TABLE items (id BIGINT PRIMARY KEY, name TEXT)",
    );
    test(&mut session);
    drop(session);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

fn run(session: &mut SqlSession, sql: &str) -> SqlResult {
    session.execute(sql, &[], None).unwrap()
}

fn error(session: &mut SqlSession, sql: &str, expected: E) {
    assert_eq!(
        session.execute(sql, &[], None),
        Err(CoreError::Sql(expected)),
        "{sql}"
    );
}

fn rows(session: &mut SqlSession) -> Vec<SqlRow> {
    run(session, "SELECT id,name FROM items ORDER BY id LIMIT 1024").rows
}

#[test]
fn insert_returning_supports_star_columns_aliases_functions_and_casts() {
    database(|s| {
        for (id, projection, columns, values) in [
            (
                1,
                "*",
                vec!["id", "name"],
                vec![V::Int(1), V::Text("alice".into())],
            ),
            (
                2,
                "id,name",
                vec!["id", "name"],
                vec![V::Int(2), V::Text("alice".into())],
            ),
            (3, "name AS n", vec!["n"], vec![V::Text("alice".into())]),
            (
                4,
                "upper(name)",
                vec!["upper"],
                vec![V::Text("ALICE".into())],
            ),
            (5, "CAST('7' AS INTEGER) AS n", vec!["n"], vec![V::Int(7)]),
            (
                6,
                "upper(name) AS n",
                vec!["n"],
                vec![V::Text("ALICE".into())],
            ),
        ] {
            let result = run(
                s,
                &format!("INSERT INTO items VALUES({id},'alice') RETURNING {projection}"),
            );
            assert_eq!(result.affected, 1);
            assert_eq!(result.columns, columns);
            assert_eq!(result.rows.len(), 1);
            assert_eq!(result.rows[0].values, values);
            assert_eq!(result.rows[0].version, rows(s).last().unwrap().version);
        }
    });
}

#[test]
fn insert_values_scalar_expression_returns_the_stored_value() {
    database(|s| {
        let result = run(
            s,
            "INSERT INTO items VALUES(1,upper('a')) RETURNING id,name",
        );
        assert_eq!(result.affected, 1);
        assert_eq!(result.columns, ["id", "name"]);
        assert_eq!(result.rows[0].values, [V::Int(1), V::Text("A".into())]);
        assert_eq!(result.rows, rows(s));
        let result = run(
            s,
            "INSERT INTO items(name,id) VALUES(upper('b'),2) RETURNING id,name;",
        );
        assert_eq!(result.rows[0].values, [V::Int(2), V::Text("B".into())]);
        assert_eq!(result.rows[0], rows(s)[1]);
    });
}

#[test]
fn point_update_returns_new_values_and_the_written_version() {
    database(|s| {
        run(s, "INSERT INTO items VALUES(1,'old')");
        let old = rows(s).remove(0);
        let result = run(
            s,
            "UPDATE items SET name='new' WHERE id=1 RETURNING name,id",
        );
        assert_eq!(result.affected, 1);
        assert_eq!(result.columns, ["name", "id"]);
        assert_eq!(result.rows[0].values, [V::Text("new".into()), V::Int(1)]);
        let stored = rows(s).remove(0);
        assert_eq!(result.rows[0].version, stored.version);
        assert!(stored.version > old.version);
        assert_eq!(stored.values, [V::Int(1), V::Text("new".into())]);
        let missing = run(
            s,
            "UPDATE items SET name='missing' WHERE id=99 RETURNING name,id",
        );
        assert_eq!(missing.affected, 0);
        assert_eq!(missing.columns, ["name", "id"]);
        assert!(missing.rows.is_empty());
        assert_eq!(rows(s), [stored]);
    });
}

#[test]
fn point_delete_returns_the_old_row_and_old_version() {
    database(|s| {
        run(s, "INSERT INTO items VALUES(1,'old')");
        let old = rows(s).remove(0);
        let result = run(s, "DELETE FROM items WHERE id=1 RETURNING *");
        assert_eq!(result.affected, 1);
        assert_eq!(result.columns, ["id", "name"]);
        assert_eq!(result.rows, [old]);
        assert!(rows(s).is_empty());
        let missing = run(s, "DELETE FROM items WHERE id=1 RETURNING id,name");
        assert_eq!(missing.affected, 0);
        assert_eq!(missing.columns, ["id", "name"]);
        assert!(missing.rows.is_empty());
    });
}

#[test]
fn describe_returning_exposes_parameters_and_result_columns_without_writing() {
    database(|s| {
        for (sql, parameters, columns) in [
            (
                "INSERT INTO items VALUES($1,$2) RETURNING id",
                vec![T::BigInt, T::Text],
                vec![("id", T::BigInt)],
            ),
            (
                "INSERT INTO items VALUES($1,$2),($3,$4) RETURNING id",
                vec![T::BigInt, T::Text, T::BigInt, T::Text],
                vec![("id", T::BigInt)],
            ),
            (
                "UPDATE items SET name=$1 WHERE id=$2 RETURNING name,id",
                vec![T::Text, T::BigInt],
                vec![("name", T::Text), ("id", T::BigInt)],
            ),
        ] {
            let prepared = s.prepare(sql).unwrap();
            let description = s.describe(&prepared).unwrap();
            assert_eq!(description.parameters, parameters, "{sql}");
            assert_eq!(
                description
                    .columns
                    .iter()
                    .map(|c| (c.name.as_str(), c.data_type.clone()))
                    .collect::<Vec<_>>(),
                columns,
                "{sql}",
            );
            assert!(rows(s).is_empty());
        }
    });
}

#[test]
fn prepared_returning_binds_write_and_projection_parameters() {
    database(|s| {
        let prepared = s
            .prepare("INSERT INTO items VALUES($1,$2) RETURNING id,$3::text AS label")
            .unwrap();
        let description = s.describe(&prepared).unwrap();
        assert_eq!(description.parameters, [T::BigInt, T::Text, T::Text]);
        assert_eq!(
            description
                .columns
                .iter()
                .map(|c| c.data_type.clone())
                .collect::<Vec<_>>(),
            [T::BigInt, T::Text]
        );
        let result = s
            .execute_prepared(
                &prepared,
                &[
                    V::Int(1),
                    V::Text("stored".into()),
                    V::Text("returned".into()),
                ],
                None,
            )
            .unwrap();
        assert_eq!(result.columns, ["id", "label"]);
        assert_eq!(
            result.rows[0].values,
            [V::Int(1), V::Text("returned".into())]
        );
        assert_eq!(rows(s)[0].values, [V::Int(1), V::Text("stored".into())]);

        let prepared = s
            .prepare("UPDATE items SET name=$1 WHERE id=$2 RETURNING $3 AS label")
            .unwrap();
        assert!(matches!(
            s.describe(&prepared),
            Err(CoreError::Sql(E::Parameters))
        ));
        let before = rows(s);
        assert_eq!(
            s.execute_prepared(
                &prepared,
                &[
                    V::Text("changed".into()),
                    V::Int(1),
                    V::Text("untyped".into()),
                ],
                None,
            ),
            Err(CoreError::Sql(E::Parameters)),
        );
        assert_eq!(rows(s), before);
    });
}

#[test]
fn scalar_values_returning_preserves_interspersed_parameter_indexes() {
    database(|s| {
        let prepared = s
            .prepare("INSERT INTO items VALUES($2,upper($3)) RETURNING $1::integer AS tag,id,name,$2 AS repeated_id,$3 AS input_name")
            .unwrap();
        let description = s.describe(&prepared).unwrap();
        assert_eq!(description.parameters, [T::Integer, T::BigInt, T::Text]);
        assert_eq!(
            description
                .columns
                .iter()
                .map(|c| c.data_type.clone())
                .collect::<Vec<_>>(),
            [T::Integer, T::BigInt, T::Text, T::BigInt, T::Text],
        );
        let result = s
            .execute_prepared(
                &prepared,
                &[V::Int(7), V::Int(5), V::Text("a".into())],
                None,
            )
            .unwrap();
        assert_eq!(
            result.columns,
            ["tag", "id", "name", "repeated_id", "input_name"]
        );
        assert_eq!(result.affected, 1);
        assert_eq!(
            result.rows[0].values,
            [
                V::Int(7),
                V::Int(5),
                V::Text("A".into()),
                V::Int(5),
                V::Text("a".into())
            ],
        );
        let stored = rows(s).remove(0);
        assert_eq!(stored.values, [V::Int(5), V::Text("A".into())]);
        assert_eq!(result.rows[0].version, stored.version);
    });
}

#[test]
fn invalid_returning_projections_leave_each_write_unchanged() {
    database(|s| {
        run(s, "INSERT INTO items VALUES(1,'old')");
        let before = rows(s);
        for write in [
            "INSERT INTO items VALUES(2,'new')",
            "UPDATE items SET name='new' WHERE id=1",
            "DELETE FROM items WHERE id=1",
            "UPDATE items SET name='new' WHERE id=99",
            "DELETE FROM items WHERE id=99",
        ] {
            for (projection, expected) in [
                ("count(*)", E::GroupingError),
                ("sum(id)", E::GroupingError),
                ("min(id)", E::GroupingError),
                ("max(id)", E::GroupingError),
                ("nope", E::UnknownColumn),
            ] {
                error(s, &format!("{write} RETURNING {projection}"), expected);
                assert_eq!(rows(s), before, "{write} RETURNING {projection}");
            }
        }
    });
}

#[test]
fn returning_validation_errors_abort_an_explicit_transaction() {
    database(|s| {
        run(s, "INSERT INTO items VALUES(1,'old')");
        let before = rows(s);
        for (projection, expected) in [("count(*)", E::GroupingError), ("nope", E::UnknownColumn)] {
            run(s, "BEGIN");
            run(s, "INSERT INTO items VALUES(2,'staged')");
            error(
                s,
                &format!("UPDATE items SET name='new' WHERE id=1 RETURNING {projection}"),
                expected,
            );
            error(s, "SELECT id FROM items LIMIT 10", E::Aborted);
            run(s, "ROLLBACK");
            assert_eq!(rows(s), before);
        }
    });
}

#[test]
fn multirow_returning_preserves_values_order_and_stored_versions() {
    database(|s| {
        let result = run(
            s,
            "INSERT INTO items VALUES(3,'c'),(1,'a'),(2,'b') RETURNING *",
        );
        let stored = rows(s);
        assert_eq!(result.affected, 3);
        assert_eq!(result.columns, ["id", "name"]);
        assert_eq!(
            result.rows,
            [stored[2].clone(), stored[0].clone(), stored[1].clone()]
        );
        assert!(stored[0].version < stored[1].version && stored[1].version < stored[2].version);
        let result = run(s, "INSERT INTO items VALUES(5,'e'),(4,'d') RETURNING id");
        let stored = rows(s);
        assert_eq!(
            result
                .rows
                .iter()
                .map(|r| r.values.clone())
                .collect::<Vec<_>>(),
            [vec![V::Int(5)], vec![V::Int(4)]]
        );
        assert_eq!(result.rows[0].version, stored[4].version);
        assert_eq!(result.rows[1].version, stored[3].version);
    });
}

#[test]
fn conflict_returning_returns_only_successful_final_target_rows() {
    database(|s| {
        run(s, "INSERT INTO items VALUES(1,'old'),(2,'keep')");
        let old = rows(s);
        let result = run(
            s,
            "INSERT INTO items VALUES(3,'c'),(1,'skip'),(4,'d') ON CONFLICT(id) DO NOTHING RETURNING *",
        );
        let stored = rows(s);
        assert_eq!(result.affected, 2);
        assert_eq!(result.rows, [stored[2].clone(), stored[3].clone()]);
        assert_eq!(&stored[..2], &old);
        let result = run(
            s,
            "INSERT INTO items VALUES(4,'updated'),(5,'new'),(2,'skip') ON CONFLICT(id) DO UPDATE SET name=excluded.name WHERE items.id<>2 RETURNING *",
        );
        let stored = rows(s);
        assert_eq!(result.affected, 2);
        assert_eq!(result.rows, [stored[3].clone(), stored[4].clone()]);
        assert_eq!(stored[3].values, [V::Int(4), V::Text("updated".into())]);
        assert_eq!(stored[1], old[1]);
        for sql in [
            "INSERT INTO items VALUES(1,'skip') ON CONFLICT DO NOTHING RETURNING id",
            "INSERT INTO items VALUES(1,'skip') ON CONFLICT(id) DO UPDATE SET name=excluded.name WHERE false RETURNING id",
        ] {
            let result = run(s, sql);
            assert_eq!(result.affected, 0);
            assert_eq!(result.columns, ["id"]);
            assert!(result.rows.is_empty());
        }
        assert_eq!(rows(s), stored);
    });
}

#[test]
fn conflict_returning_id_and_unguarded_update_mix() {
    database(|s| {
        run(s, "INSERT INTO items VALUES(1,'old')");
        let old = rows(s).remove(0);
        let result = run(
            s,
            "INSERT INTO items VALUES(3,'c'),(1,'skip'),(2,'b') ON CONFLICT(id) DO NOTHING RETURNING id",
        );
        let stored = rows(s);
        assert_eq!(result.affected, 2);
        assert_eq!(result.columns, ["id"]);
        assert_eq!(result.rows[0].values, [V::Int(3)]);
        assert_eq!(result.rows[1].values, [V::Int(2)]);
        assert_eq!(result.rows[0].version, stored[2].version);
        assert_eq!(result.rows[1].version, stored[1].version);
        assert_eq!(stored[0], old);
        let result = run(
            s,
            "INSERT INTO items VALUES(3,'changed'),(4,'new'),(1,'updated') ON CONFLICT(id) DO UPDATE SET name=excluded.name RETURNING id,name",
        );
        let stored = rows(s);
        assert_eq!(result.affected, 3);
        assert_eq!(result.columns, ["id", "name"]);
        assert_eq!(
            result.rows,
            [stored[2].clone(), stored[3].clone(), stored[0].clone()]
        );
        assert_eq!(stored[2].values, [V::Int(3), V::Text("changed".into())]);
        assert_eq!(stored[0].values, [V::Int(1), V::Text("updated".into())]);
        assert!(stored[0].version > old.version);
    });
}

#[test]
fn multirow_prepared_returning_seeds_write_and_conflict_parameters() {
    database(|s| {
        let prepared = s.prepare("INSERT INTO items VALUES($1,$2),($3,$4) ON CONFLICT(id) DO UPDATE SET name=$5 WHERE items.id<>$6 RETURNING id,$7::text AS label,$2 AS input").unwrap();
        let description = s.describe(&prepared).unwrap();
        assert_eq!(
            description.parameters,
            [
                T::BigInt,
                T::Text,
                T::BigInt,
                T::Text,
                T::Text,
                T::BigInt,
                T::Text
            ]
        );
        assert_eq!(
            description
                .columns
                .iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>(),
            ["id", "label", "input"]
        );
        assert!(rows(s).is_empty());
        let result = s
            .execute_prepared(
                &prepared,
                &[
                    V::Int(2),
                    V::Text("b".into()),
                    V::Int(1),
                    V::Text("a".into()),
                    V::Text("updated".into()),
                    V::Int(99),
                    V::Text("tag".into()),
                ],
                None,
            )
            .unwrap();
        assert_eq!(
            result.rows[0].values,
            [V::Int(2), V::Text("tag".into()), V::Text("b".into())]
        );
        assert_eq!(result.rows[1].values[0], V::Int(1));
        let stored = rows(s);
        assert_eq!(result.rows[0].version, stored[1].version);
        assert_eq!(result.rows[1].version, stored[0].version);
    });
}

#[test]
fn multirow_and_conflict_returning_errors_are_atomic_including_empty_results() {
    database(|s| {
        run(s, "INSERT INTO items VALUES(1,'old')");
        let before = rows(s);
        for write in [
            "INSERT INTO items VALUES(3,'c'),(2,'b')",
            "INSERT INTO items VALUES(3,'c'),(1,'new') ON CONFLICT(id) DO UPDATE SET name=excluded.name",
            "INSERT INTO items VALUES(1,'new') ON CONFLICT DO NOTHING",
            "INSERT INTO items VALUES(1,'new') ON CONFLICT(id) DO UPDATE SET name=excluded.name WHERE false",
        ] {
            for (projection, expected) in
                [("nope", E::UnknownColumn), ("count(*)", E::GroupingError)]
            {
                error(s, &format!("{write} RETURNING {projection}"), expected);
                assert_eq!(rows(s), before);
            }
        }
        for write in [
            "INSERT INTO items VALUES(3,'c'),(2,'b')",
            "INSERT INTO items VALUES(3,'c'),(1,'new') ON CONFLICT(id) DO UPDATE SET name=excluded.name",
        ] {
            error(
                s,
                &format!("{write} RETURNING abs(-2147483648)"),
                E::Overflow,
            );
            assert_eq!(rows(s), before);
        }
    });
}

#[test]
fn multirow_and_conflict_without_returning_match_returning_storage() {
    let statements = [
        "INSERT INTO items VALUES(3,'c'),(1,'a'),(2,'b')",
        "INSERT INTO items VALUES(4,'d'),(1,'skip') ON CONFLICT DO NOTHING",
        "INSERT INTO items VALUES(3,'new'),(5,'e'),(2,'skip') ON CONFLICT(id) DO UPDATE SET name=excluded.name WHERE items.id<>2",
    ];
    let mut expected = Vec::new();
    database(|s| {
        for sql in statements {
            let result = run(s, &format!("{sql} RETURNING *"));
            expected.push((result.affected, rows(s)));
        }
    });
    database(|s| {
        for (sql, (affected, stored)) in statements.into_iter().zip(expected) {
            let result = run(s, sql);
            assert_eq!(result.affected, affected);
            assert!(result.columns.is_empty());
            assert!(result.rows.is_empty());
            assert_eq!(rows(s), stored);
        }
    });
}

#[test]
fn writes_without_returning_keep_empty_result_columns_and_rows() {
    database(|s| {
        for sql in [
            "INSERT INTO items VALUES(1,'old')",
            "UPDATE items SET name='new' WHERE id=1",
            "DELETE FROM items WHERE id=1",
        ] {
            let result = run(s, sql);
            assert_eq!(result.affected, 1, "{sql}");
            assert!(result.columns.is_empty(), "{sql}");
            assert!(result.rows.is_empty(), "{sql}");
        }
    });
}

#[test]
fn returning_identifier_and_qualified_column_remain_usable() {
    database(|s| {
        run(
            s,
            "CREATE TABLE returning (id BIGINT PRIMARY KEY, returning TEXT)",
        );
        let result = run(
            s,
            "INSERT INTO returning VALUES(1,'value') RETURNING returning.returning AS result",
        );
        assert_eq!(result.columns, ["result"]);
        assert_eq!(result.rows[0].values, [V::Text("value".into())]);
        let result = run(
            s,
            "UPDATE returning SET returning='changed' WHERE id=1 RETURNING returning AS name",
        );
        assert_eq!(result.columns, ["name"]);
        assert_eq!(result.rows[0].values, [V::Text("changed".into())]);
        let result = run(
            s,
            "DELETE FROM returning WHERE id=1 RETURNING returning.returning",
        );
        assert_eq!(result.rows[0].values, [V::Text("changed".into())]);
    });
}

#[test]
fn returning_runtime_errors_are_atomic_for_insert_update_and_delete() {
    database(|s| {
        run(s, "INSERT INTO items VALUES(1,'old')");
        let before = rows(s);
        for write in [
            "INSERT INTO items VALUES(2,'new')",
            "UPDATE items SET name='new' WHERE id=1",
            "DELETE FROM items WHERE id=1",
        ] {
            error(
                s,
                &format!("{write} RETURNING abs(-2147483648)"),
                E::Overflow,
            );
            assert_eq!(rows(s), before, "{write}");
        }
    });
}

#[test]
fn repeated_returning_projections_enforce_the_output_byte_cap_atomically() {
    database(|s| {
        run(s, "INSERT INTO items VALUES(1,'old')");
        let before = rows(s);
        let projection = vec!["name"; 32].join(",");
        let sql = format!("UPDATE items SET name=$1 WHERE id=1 RETURNING {projection}");
        assert_eq!(
            s.execute(&sql, &[V::Text("x".repeat(40_000))], None),
            Err(CoreError::Sql(E::Limit)),
        );
        assert_eq!(rows(s), before);
    });
}

#[test]
fn multirow_returning_enforces_cumulative_output_bytes_atomically() {
    database(|s| {
        run(s, "INSERT INTO items VALUES(1,'old')");
        let before = rows(s);
        let projection = vec!["name"; 16].join(",");
        let params = [V::Text("x".repeat(40_000))];
        for write in [
            "INSERT INTO items VALUES(2,$1),(3,$1)",
            "INSERT INTO items VALUES(1,$1),(2,$1) ON CONFLICT(id) DO UPDATE SET name=excluded.name",
        ] {
            let sql = format!("{write} RETURNING {projection}");
            assert_eq!(s.execute(&sql, &params, None), Err(CoreError::Sql(E::Limit)));
            assert_eq!(rows(s), before, "{sql}");
        }
    });
}

#[test]
fn heap_insert_returning_preserves_columns_values_order_and_hidden_rowid_error() {
    database(|s| {
        run(s, "CREATE TABLE h (a integer, b text)");
        let result = run(s, "INSERT INTO h VALUES (1,'x') RETURNING *");
        assert_eq!(result.columns, ["a", "b"]);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].values, [V::Int(1), V::Text("x".into())]);

        let result = run(s, "INSERT INTO h VALUES (3,'c'),(2,'b') RETURNING a");
        assert_eq!(result.columns, ["a"]);
        assert_eq!(
            result.rows.iter().map(|row| row.values.clone()).collect::<Vec<_>>(),
            [vec![V::Int(3)], vec![V::Int(2)]]
        );

        let select_error = s.execute("SELECT giggles_rowid FROM h", &[], None).unwrap_err();
        assert_eq!(select_error, CoreError::Sql(E::Reserved));
        assert_eq!(
            s.execute("INSERT INTO h VALUES (4,'d') RETURNING giggles_rowid", &[], None),
            Err(select_error)
        );
    });
}
