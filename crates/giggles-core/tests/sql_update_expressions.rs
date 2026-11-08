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
        "giggles-sql-update-expressions-{}-{}-{}",
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
        "CREATE TABLE items (id BIGINT PRIMARY KEY, n INTEGER, m INTEGER, name TEXT)",
    );
    run(&mut session, "INSERT INTO items VALUES(1,10,20,'alice')");
    test(&mut session);
    drop(session);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

fn run(session: &mut SqlSession, sql: &str) -> SqlResult {
    session.execute(sql, &[], None).unwrap()
}

fn row(session: &mut SqlSession) -> SqlRow {
    run(session, "SELECT id,n,m,name FROM items WHERE id=1")
        .rows
        .remove(0)
}

fn error(session: &mut SqlSession, sql: &str, expected: E) {
    assert_eq!(
        session.execute(sql, &[], None),
        Err(CoreError::Sql(expected)),
        "{sql}"
    );
}

#[test]
fn expressions_read_old_columns_and_return_new_values() {
    database(|s| {
        let old = row(s);
        let result = run(
            s,
            "UPDATE items SET n=n+1,name=upper(items.name),m=items.m+2 WHERE items.id=1 RETURNING id,n,m,name",
        );
        assert_eq!(result.affected, 1);
        assert_eq!(result.columns, ["id", "n", "m", "name"]);
        assert_eq!(
            result.rows[0].values,
            [V::Int(1), V::Int(11), V::Int(22), V::Text("ALICE".into())]
        );
        assert_eq!(result.rows[0], row(s));
        assert!(result.rows[0].version > old.version);
        let result = run(
            s,
            "UPDATE items SET n=m,m=n,name=upper(name) WHERE id=1 RETURNING n,m",
        );
        assert_eq!(result.rows[0].values, [V::Int(22), V::Int(11)]);
        assert_eq!(row(s).values[1..3], [V::Int(22), V::Int(11)]);
    });
}

#[test]
fn describe_and_execution_share_set_predicate_and_returning_parameters() {
    database(|s| {
        let prepared = s
            .prepare("UPDATE items SET n=n+$1 WHERE id=$2 RETURNING n,upper($3) AS label")
            .unwrap();
        let description = s.describe(&prepared).unwrap();
        assert_eq!(description.parameters, [T::Integer, T::BigInt, T::Text]);
        assert_eq!(
            description
                .columns
                .iter()
                .map(|column| (&*column.name, &column.data_type))
                .collect::<Vec<_>>(),
            [("n", &T::Integer), ("label", &T::Text)]
        );
        let result = s
            .execute_prepared(
                &prepared,
                &[V::Int(3), V::Int(1), V::Text("done".into())],
                None,
            )
            .unwrap();
        assert_eq!(result.rows[0].values, [V::Int(13), V::Text("DONE".into())]);
        run(s, "CREATE TABLE shared (id INTEGER PRIMARY KEY, n INTEGER)");
        run(s, "INSERT INTO shared VALUES(13,20)");
        let shared = s
            .prepare("UPDATE shared SET n=n+$1 WHERE id=$1 RETURNING n+$1")
            .unwrap();
        assert_eq!(s.describe(&shared).unwrap().parameters, [T::Integer]);
        let result = s.execute_prepared(&shared, &[V::Int(13)], None).unwrap();
        assert_eq!(result.rows[0].values, [V::Int(46)]);
        let conflict = s.prepare("UPDATE items SET n=n+$1 WHERE id=$1").unwrap();
        assert_eq!(s.describe(&conflict).unwrap_err(), CoreError::Sql(E::Type));
    });
}

#[test]
fn expression_errors_leave_all_values_and_version_unchanged() {
    database(|s| {
        run(s, "UPDATE items SET n=2147483647 WHERE id=1");
        let old = row(s);
        for (sql, expected) in [
            (
                "UPDATE items SET name=upper(name),m=n+1 WHERE id=1",
                E::Overflow,
            ),
            (
                "UPDATE items SET name=upper(name),m=m/0 WHERE id=1",
                E::DivisionByZero,
            ),
            (
                "UPDATE items SET name=upper(name),m=CAST('bad' AS INTEGER) WHERE id=1",
                E::InvalidTextRepresentation,
            ),
            (
                "UPDATE items SET name=upper(name),m=NULL WHERE id=1 RETURNING n/0",
                E::DivisionByZero,
            ),
        ] {
            error(s, sql, expected);
            assert_eq!(row(s), old, "{sql}");
        }
    });
}

#[test]
fn missing_rows_skip_scalar_evaluation_but_validate_types_and_inputs() {
    database(|s| {
        let old = row(s);
        let missing = run(
            s,
            "UPDATE items SET n=n/0,name=upper(name) WHERE id=99 RETURNING n,name",
        );
        assert_eq!(missing.affected, 0);
        assert_eq!(missing.columns, ["n", "name"]);
        assert!(missing.rows.is_empty());
        for (sql, expected) in [
            ("UPDATE items SET n=upper(name) WHERE id=99", E::Type),
            (
                "UPDATE items SET name=upper(missing.name) WHERE id=99",
                E::UnknownTable,
            ),
            ("UPDATE items SET n=missing+1 WHERE id=99", E::UnknownColumn),
            ("UPDATE items SET n='bad' WHERE id=99", E::Type),
            // PG18.6 rejects repeated SET targets with 42601.
            ("UPDATE items SET n=n+1,n=n+2 WHERE id=99", E::Syntax),
        ] {
            error(s, sql, expected);
        }
        let prepared = s.prepare("UPDATE items SET n=n+$1 WHERE id=$2").unwrap();
        assert_eq!(
            s.execute_prepared(&prepared, &[V::Text("bad".into()), V::Int(99)], None),
            Err(CoreError::Sql(E::Type))
        );
        assert_eq!(row(s), old);
    });
}

#[test]
fn assignment_casts_and_plain_literals_keep_their_existing_contracts() {
    database(|s| {
        run(
            s,
            "UPDATE items SET n=CAST('7' AS BIGINT),name=n+1 WHERE id=1",
        );
        assert_eq!(
            row(s).values,
            [V::Int(1), V::Int(7), V::Int(20), V::Text("11".into())]
        );
        run(
            s,
            "UPDATE items SET n=(NULL),m=(3),name=('plain') WHERE id=1",
        );
        assert_eq!(
            row(s).values,
            [V::Int(1), V::Null, V::Int(3), V::Text("plain".into())]
        );
        run(s, "UPDATE items SET n=+12,m=-4,name='literal' WHERE id=1");
        let prepared = s
            .prepare("UPDATE items SET n=$1,name=$2 WHERE id=$3")
            .unwrap();
        assert_eq!(
            s.describe(&prepared).unwrap().parameters,
            [T::Integer, T::Text, T::BigInt]
        );
        s.execute_prepared(
            &prepared,
            &[V::Int(9), V::Text("param".into()), V::Int(1)],
            None,
        )
        .unwrap();
        assert_eq!(run(s, "UPDATE items SET id=id+1 WHERE id=1").affected, 1);
        assert_eq!(run(s, "UPDATE items SET id=1 WHERE id=2").affected, 1);
        let old = row(s);
        for (sql, expected) in [
            ("UPDATE items SET n='12' WHERE id=1", E::Type),
            ("UPDATE items SET n=sum(n) WHERE id=1", E::GroupingError),
        ] {
            error(s, sql, expected);
            assert_eq!(row(s), old);
        }
        assert_eq!(run(s, "UPDATE items SET n=n+1").affected, 1);
        assert_eq!(row(s).values[1], V::Int(10));
        // PG18: SET col = DEFAULT on a column without a default assigns NULL.
        assert_eq!(run(s, "UPDATE items SET n=DEFAULT WHERE id=1").affected, 1);
        assert_eq!(row(s).values[1], V::Null);
    });
}

#[test]
fn plain_assignment_errors_preserve_validation_order_and_the_whole_row() {
    database(|s| {
        // SQL analysis precedes duplicate assignment validation.
        assert_eq!(run(s, "UPDATE items SET id=2 WHERE id=1").affected, 1);
        assert_eq!(run(s, "UPDATE items SET id=1 WHERE id=2").affected, 1);
        let old = row(s);
        for (sql, expected) in [
            // PG18.6 rejects repeated SET targets with 42601.
            ("UPDATE items SET n=1,n=2 WHERE id=1", E::Syntax),
            ("UPDATE items SET n='x' WHERE id=1", E::Type),
            ("UPDATE items SET id='x',n='x' WHERE id=1", E::Type),
            ("UPDATE items SET n='x',id=2 WHERE id=1", E::Type),
            ("UPDATE items SET n=1,n=2,m='x' WHERE id=1", E::Type),
            ("UPDATE items SET m='x',n=1,n=2 WHERE id=1", E::Type),
            ("UPDATE items SET n=1,n='x' WHERE id=1", E::Type),
            ("UPDATE items SET n='x',n=2 WHERE id=1", E::Type),
        ] {
            error(s, sql, expected);
            assert_eq!(row(s), old, "{sql}");
        }
        for (sql, expected) in [
            ("UPDATE items SET n=$1 WHERE id=1", E::Type),
            ("UPDATE items SET id=2,n=$1 WHERE id=1", E::Type),
            ("UPDATE items SET n=$1,id=2 WHERE id=1", E::Type),
            // PG18.6 rejects duplicate targets before binding parameter values.
            ("UPDATE items SET n=1,n=2,m=$1 WHERE id=1", E::Syntax),
            // PG18.6 rejects duplicate targets before binding parameter values.
            ("UPDATE items SET m=$1,n=1,n=2 WHERE id=1", E::Syntax),
        ] {
            assert_eq!(
                s.execute(sql, &[V::Text("x".into())], None),
                Err(CoreError::Sql(expected)),
                "{sql}"
            );
            assert_eq!(row(s), old, "{sql}");
        }

        run(
            s,
            "CREATE TABLE required (id BIGINT PRIMARY KEY, n INTEGER NOT NULL)",
        );
        run(s, "INSERT INTO required VALUES(1,10)");
        let select = "SELECT id,n FROM required WHERE id=1";
        let required_old = run(s, select).rows.remove(0);
        for (sql, expected) in [
            ("UPDATE required SET n=NULL WHERE id=1", E::Null),
            // PG18.6 rejects duplicate targets before postimage NOT NULL checks.
            ("UPDATE required SET n=1,n=NULL WHERE id=1", E::Syntax),
        ] {
            error(s, sql, expected);
            assert_eq!(run(s, select).rows.remove(0), required_old, "{sql}");
        }
    });
}

#[test]
fn searched_primary_key_moves_check_uniqueness_in_order_and_return_new_keys() {
    // Identity primary-key moves retain their existing assignment compatibility.
    database(|s| {
        run(
            s,
            "CREATE TABLE moves (id integer GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, name text)",
        );
        run(s, "INSERT INTO moves VALUES (1, 'alice'), (2, 'bob')");
        let ids = |s: &mut SqlSession| {
            run(s, "SELECT id, name FROM moves ORDER BY id")
                .rows
                .into_iter()
                .map(|r| r.values)
                .collect::<Vec<_>>()
        };
        let before = ids(s);
        // PG18.6: with ids 1 and 2, `SET id = id + 1` moves 1 onto the still-present 2 and
        // fails the non-deferrable primary key immediately (23505); nothing changes.
        error(s, "UPDATE moves SET id = id + 1 WHERE id >= 1", E::Duplicate);
        assert_eq!(ids(s), before);
        // A non-colliding move succeeds and RETURNING reports the new keys.
        let moved = run(s, "UPDATE moves SET id = id + 10 WHERE id >= 1 RETURNING id, name");
        let mut returned = moved.rows.into_iter().map(|r| r.values).collect::<Vec<_>>();
        returned.sort_by_key(|v| match v[0] {
            V::Int(i) => i,
            _ => 0,
        });
        let expected = vec![
            vec![V::Int(11), V::Text("alice".into())],
            vec![V::Int(12), V::Text("bob".into())],
        ];
        assert_eq!(returned, expected);
        assert_eq!(ids(s), expected);
    });
}

#[test]
fn duplicate_set_analysis_precedes_binding_and_row_lookup() {
    database(|s| {
        let old = row(s);
        for (sql, expected) in [
            ("UPDATE missing SET n=1,n=2 WHERE id=1", E::UnknownTable),
            (
                "UPDATE items SET n=1,n=2,missing=1 WHERE id=1",
                E::UnknownColumn,
            ),
            ("UPDATE items SET n=1,n=2 WHERE missing=1", E::UnknownColumn),
            (
                "UPDATE items SET n=1,n=2 WHERE id=1 RETURNING missing",
                E::UnknownColumn,
            ),
            (
                "UPDATE items SET n=missing+1,n=2 WHERE id=99",
                E::UnknownColumn,
            ),
            ("UPDATE items SET id=2,n=1,n=2 WHERE id=1", E::Syntax),
            ("UPDATE items SET n=n/0,n=n+2 WHERE id=99", E::Syntax),
            ("UPDATE items SET n=NULL,n=2 WHERE id>99", E::Syntax),
        ] {
            error(s, sql, expected);
            assert_eq!(row(s), old);
        }
        let prepared = s.prepare("UPDATE items SET n=$1,n=2 WHERE id=1").unwrap();
        assert_eq!(
            s.describe(&prepared).unwrap_err(),
            CoreError::Sql(E::Syntax)
        );
        for id in [1, 99] {
            for value in ["NULL", "n+1"] {
                error(
                    s,
                    &format!("UPDATE items SET n={value},n=2 WHERE id={id}"),
                    E::Syntax,
                );
            }
        }
    });
}

#[test]
fn not_null_checks_only_matched_update_postimages() {
    database(|s| {
        run(
            s,
            "CREATE TABLE required (id BIGINT PRIMARY KEY, n INTEGER NOT NULL, m INTEGER)",
        );
        run(s, "INSERT INTO required VALUES(1,10,20)");
        let select = "SELECT id,n,m FROM required WHERE id=1";
        let old = run(s, select).rows.remove(0);
        // PG18.6: NULL constraints inspect matched postimages, in both SET paths.
        for (predicate, matched) in [
            ("id=99", false),
            ("n>99", false),
            ("id=1", true),
            ("n>0", true),
        ] {
            for extra in ["", ",m=m+1"] {
                for rhs in ["NULL", "(NULL)", "DEFAULT"] {
                    let sql = format!("UPDATE required SET n={rhs}{extra} WHERE {predicate}");
                    if matched {
                        error(s, &sql, E::Null);
                    } else {
                        assert_eq!(run(s, &sql).affected, 0);
                    }
                    assert_eq!(run(s, select).rows.remove(0), old);
                }
                for declared in [None, Some(T::Integer), Some(T::BigInt)] {
                    let prepared = s
                        .prepare(&format!(
                            "UPDATE required SET n=$1{extra} WHERE {predicate}"
                        ))
                        .unwrap();
                    let prepared = if let Some(ty) = declared {
                        prepared.with_parameter_types(&[Some(ty)]).unwrap()
                    } else {
                        prepared
                    };
                    let result = s.execute_prepared(&prepared, &[V::Null], None);
                    if matched {
                        assert_eq!(result, Err(CoreError::Sql(E::Null)));
                    } else {
                        assert_eq!(result.unwrap().affected, 0);
                    }
                    assert_eq!(run(s, select).rows.remove(0), old);
                }
            }
            error(
                s,
                &format!("UPDATE required SET n='bad' WHERE {predicate}"),
                E::Type,
            );
        }
    });
}

#[test]
fn conflict_set_duplicates_and_guarded_nulls_follow_sql_analysis() {
    database(|s| {
        for id in [1, 99] {
            for (sets, expected) in [
                ("n=1,n=2", E::Syntax),
                ("n=excluded.n,n=items.n", E::Syntax),
                ("n=NULL,n=2", E::Syntax),
                ("n=1,n=2,missing=1", E::UnknownColumn),
                ("n=1,n=2,m='bad'", E::Type),
            ] {
                error(
                    s,
                    &format!(
                        "INSERT INTO items(id,n) VALUES({id},10) ON CONFLICT(id) DO UPDATE SET {sets}"
                    ),
                    expected,
                );
            }
        }
        run(
            s,
            "CREATE TABLE required (id BIGINT PRIMARY KEY, n INTEGER NOT NULL)",
        );
        run(s, "INSERT INTO required VALUES(1,10)");
        let old = run(s, "SELECT id,n FROM required WHERE id=1")
            .rows
            .remove(0);
        assert_eq!(
            run(
                s,
                "INSERT INTO required VALUES(1,20) ON CONFLICT(id) DO UPDATE SET n=NULL WHERE n<0"
            )
            .affected,
            0
        );
        error(
            s,
            "INSERT INTO required VALUES(1,20) ON CONFLICT(id) DO UPDATE SET n=NULL",
            E::Null,
        );
        assert_eq!(
            run(s, "SELECT id,n FROM required WHERE id=1")
                .rows
                .remove(0),
            old
        );
    });
}
