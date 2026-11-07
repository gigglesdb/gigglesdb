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
            ("UPDATE items SET n=n+1,n=n+2 WHERE id=99", E::Duplicate),
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
        let old = row(s);
        for (sql, expected) in [
            ("UPDATE items SET n='12' WHERE id=1", E::Type),
            ("UPDATE items SET id=id+1 WHERE id=1", E::Unsupported),
            ("UPDATE items SET n=DEFAULT WHERE id=1", E::Unsupported),
            ("UPDATE items SET n=sum(n) WHERE id=1", E::GroupingError),
        ] {
            error(s, sql, expected);
            assert_eq!(row(s), old);
        }
        assert_eq!(run(s, "UPDATE items SET n=n+1").affected, 1);
        assert_eq!(row(s).values[1], V::Int(10));
    });
}

#[test]
fn plain_assignment_errors_preserve_validation_order_and_the_whole_row() {
    database(|s| {
        let old = row(s);
        // Each assignment is validated in order; duplicate targets are checked last.
        for (sql, expected) in [
            ("UPDATE items SET id=2 WHERE id=1", E::Unsupported),
            ("UPDATE items SET n=1,n=2 WHERE id=1", E::Duplicate),
            ("UPDATE items SET n='x' WHERE id=1", E::Type),
            ("UPDATE items SET id='x',n='x' WHERE id=1", E::Unsupported),
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
            ("UPDATE items SET id=2,n=$1 WHERE id=1", E::Unsupported),
            ("UPDATE items SET n=$1,id=2 WHERE id=1", E::Type),
            ("UPDATE items SET n=1,n=2,m=$1 WHERE id=1", E::Type),
            ("UPDATE items SET m=$1,n=1,n=2 WHERE id=1", E::Type),
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
        for sql in [
            "UPDATE required SET n=NULL WHERE id=1",
            "UPDATE required SET n=1,n=NULL WHERE id=1",
        ] {
            error(s, sql, E::Null);
            assert_eq!(run(s, select).rows.remove(0), required_old, "{sql}");
        }
    });
}
