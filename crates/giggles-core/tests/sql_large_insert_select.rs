mod common;
use giggles_core::{
    Admin, Engine, Limits, SqlSession,
    relational::Value::{Int, Text},
};

fn fixture(name: &str) -> (std::path::PathBuf, SqlSession) {
    let path = common::fresh(name);
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    (path, admin.sql_session())
}

#[test]
fn insert_from_series_checks_count_and_sum() {
    let (path, mut session) = fixture("large-insert-series");
    session
        .execute("CREATE TABLE t (id int PRIMARY KEY, n int)", &[], None)
        .unwrap();
    assert_eq!(
        session
            .execute(
                "INSERT INTO t SELECT g, g FROM generate_series(1, 100000) g",
                &[],
                None,
            )
            .unwrap()
            .affected,
        100_000
    );
    assert_eq!(
        session
            .execute("SELECT count(*), sum(n) FROM t", &[], None)
            .unwrap()
            .rows[0]
            .values,
        vec![Int(100_000), Int(5_000_050_000)]
    );
    drop(session);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn narrow_statement_streams_into_transaction_and_rolls_back() {
    let (path, mut session) = fixture("large-insert-rollback");
    session
        .execute("CREATE TABLE t (id int PRIMARY KEY, n int)", &[], None)
        .unwrap();
    session.execute("BEGIN", &[], None).unwrap();
    assert_eq!(
        session
            .execute(
                "INSERT INTO t SELECT g, g FROM generate_series(1, 300000) g",
                &[],
                None,
            )
            .unwrap()
            .affected,
        300_000
    );
    session.execute("ROLLBACK", &[], None).unwrap();
    assert_eq!(
        session
            .execute("SELECT count(*) FROM t", &[], None)
            .unwrap()
            .rows[0]
            .values,
        vec![Int(0)]
    );
    drop(session);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn create_as_and_returning() {
    let (path, mut session) = fixture("large-create-as");
    assert_eq!(
        session
            .execute(
                "CREATE TABLE t AS SELECT g AS n FROM generate_series(1, 100000) g",
                &[],
                None,
            )
            .unwrap()
            .affected,
        100_000
    );
    assert_eq!(
        session
            .execute("SELECT count(*), sum(n) FROM t", &[], None)
            .unwrap()
            .rows[0]
            .values,
        vec![Int(100_000), Int(5_000_050_000)]
    );
    let result = session
        .execute(
            "INSERT INTO t SELECT g FROM generate_series(100001, 200000) g RETURNING n",
            &[],
            None,
        )
        .unwrap();
    assert_eq!(result.affected, 100_000);
    assert_eq!(result.rows.len(), 100_000);
    for (row, n) in result.rows.iter().zip(100001..=200000) {
        assert_eq!(row.values, vec![Int(n)]);
    }
    drop(session);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn materialized_source_and_late_failure_are_atomic() {
    let (path, mut session) = fixture("large-insert-materialized");
    session
        .execute("CREATE TABLE t (id int PRIMARY KEY)", &[], None)
        .unwrap();
    let failed = session.execute(
        "INSERT INTO t SELECT CASE WHEN g = 3001 THEN 1 ELSE g END FROM generate_series(1, 3001) g",
        &[],
        None,
    );
    assert_eq!(giggles_core::sqlstate(&failed.unwrap_err()), "23505");
    assert_eq!(
        session
            .execute("SELECT count(*) FROM t", &[], None)
            .unwrap()
            .rows[0]
            .values,
        vec![Int(0)]
    );
    assert_eq!(
        session
            .execute(
                "INSERT INTO t SELECT g FROM generate_series(1, 100000) g ORDER BY g DESC",
                &[],
                None,
            )
            .unwrap()
            .affected,
        100_000
    );
    drop(session);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn grouped_json_insert_source_with_correlated_exists() {
    let (path, mut session) = fixture("grouped-json-insert");
    for sql in [
        "CREATE TABLE events (id int PRIMARY KEY, account int, label text)",
        "CREATE TABLE eligible (id int PRIMARY KEY)",
        "CREATE TABLE summary (account int PRIMARY KEY, payload jsonb)",
        "INSERT INTO events VALUES (1,10,'a'),(2,10,'b'),(3,20,'c')",
        "INSERT INTO eligible VALUES (1),(2)",
    ] {
        session.execute(sql, &[], None).unwrap();
    }
    let inserted = session
        .execute(
            "INSERT INTO summary SELECT e.account, json_agg(e.label ORDER BY e.id)
             FROM events e WHERE EXISTS (SELECT 1 FROM eligible a WHERE a.id = e.id)
             GROUP BY e.account",
            &[],
            None,
        )
        .unwrap();
    assert_eq!(inserted.affected, 1);
    let result = session
        .execute("SELECT account, payload::text FROM summary", &[], None)
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].values, [Int(10), Text(r#"["a", "b"]"#.into())]);
    drop(session);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn materialized_insert_source_keeps_the_nested_query_row_limit() {
    let (path, mut session) = fixture("large-insert-subquery");
    session
        .execute("CREATE TABLE t (id int PRIMARY KEY)", &[], None)
        .unwrap();
    let failed = session.execute(
        "INSERT INTO t SELECT g FROM (SELECT g FROM generate_series(1, 70000) g ORDER BY g) s",
        &[],
        None,
    );
    assert_eq!(giggles_core::sqlstate(&failed.unwrap_err()), "54000");
    assert_eq!(
        session.execute("SELECT count(*) FROM t", &[], None).unwrap().rows[0].values,
        vec![Int(0)]
    );
    drop(session);
    std::fs::remove_dir_all(path).unwrap();
}
