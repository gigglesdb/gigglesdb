//! PostgreSQL 18.6 oracle: e1-sql/pg18-savepoint-oracle-20261107.json.
//! NoActiveTransaction=25P01, InvalidSavepoint=3B001, Aborted=25P02.
//! The local 64-live-savepoint bound (Limit=54000) deliberately differs from PG.
mod common;
use giggles_core::{
    Admin, CoreError, Limits, SqlSession,
    relational::{SqlError, StatementClock, Value as V},
};
use giggles_pg_types::temporal::{SessionZone, TimestampTz};

fn db() -> Admin {
    Admin::create(common::fresh("sql-savepoints"), Limits::default()).unwrap()
}
fn run(s: &mut SqlSession, sql: &str) {
    s.execute(sql, &[], None)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
}
fn rows(s: &mut SqlSession, sql: &str) -> Vec<Vec<V>> {
    s.execute(sql, &[], None)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
        .rows
        .into_iter()
        .map(|r| r.values)
        .collect()
}
fn error(s: &mut SqlSession, sql: &str, expected: SqlError) {
    assert!(
        matches!(s.execute(sql, &[], None), Err(CoreError::Sql(e)) if e == expected),
        "{sql}: expected {expected:?}"
    );
}
fn setup(s: &mut SqlSession) {
    run(s, "CREATE TABLE t(id BIGINT PRIMARY KEY, v BIGINT)");
    run(s, "INSERT INTO t VALUES (1, 10)");
    run(s, "BEGIN");
}
fn value(s: &mut SqlSession, n: i64) {
    assert_eq!(rows(s, "SELECT v FROM t WHERE id=1"), vec![vec![V::Int(n)]]);
}

#[test]
fn nested_rollback_keeps_target_and_release_older_discards_descendants() {
    let a = db();
    let mut s = a.sql_session();
    setup(&mut s);
    run(&mut s, "SAVEPOINT older");
    run(&mut s, "UPDATE t SET v=20 WHERE id=1");
    run(&mut s, "SAVEPOINT younger");
    run(&mut s, "UPDATE t SET v=30 WHERE id=1");
    run(&mut s, "ROLLBACK WORK TO SAVEPOINT younger");
    value(&mut s, 20);
    run(&mut s, "UPDATE t SET v=40 WHERE id=1");
    run(&mut s, "ROLLBACK TRANSACTION TO younger");
    value(&mut s, 20);
    run(&mut s, "RELEASE SAVEPOINT older");
    error(&mut s, "ROLLBACK TO younger", SqlError::InvalidSavepoint);
    run(&mut s, "ROLLBACK");
    value(&mut s, 10);
}

#[test]
fn duplicate_names_resolve_latest_then_expose_older_on_release() {
    let a = db();
    let mut s = a.sql_session();
    setup(&mut s);
    run(&mut s, "SAVEPOINT mark");
    run(&mut s, "UPDATE t SET v=20 WHERE id=1");
    run(&mut s, "SAVEPOINT mark");
    run(&mut s, "UPDATE t SET v=30 WHERE id=1");
    run(&mut s, "ROLLBACK TO mark");
    value(&mut s, 20);
    run(&mut s, "RELEASE mark");
    run(&mut s, "ROLLBACK TO mark");
    value(&mut s, 10);
    run(&mut s, "COMMIT");
    value(&mut s, 10);
}

#[test]
fn statement_failure_recovers_only_through_rollback_to_existing_mark() {
    let a = db();
    let mut s = a.sql_session();
    setup(&mut s);
    run(&mut s, "SAVEPOINT healthy");
    error(&mut s, "INSERT INTO t VALUES(1,99)", SqlError::Duplicate);
    error(&mut s, "SAVEPOINT bad", SqlError::Aborted);
    error(&mut s, "RELEASE healthy", SqlError::Aborted);
    error(&mut s, "ROLLBACK TO missing", SqlError::InvalidSavepoint);
    run(&mut s, "ROLLBACK TO healthy");
    run(&mut s, "UPDATE t SET v=11 WHERE id=1");
    run(&mut s, "COMMIT");
    value(&mut s, 11);
}

#[test]
fn outside_transaction_and_unknown_names_have_distinct_errors() {
    let a = db();
    let mut s = a.sql_session();
    for sql in ["SAVEPOINT a", "RELEASE a", "ROLLBACK TO a"] {
        error(&mut s, sql, SqlError::NoActiveTransaction);
        assert!(!s.in_transaction());
    }
    run(&mut s, "BEGIN");
    run(&mut s, "SAVEPOINT existing");
    error(&mut s, "RELEASE absent", SqlError::InvalidSavepoint);
    error(&mut s, "SELECT 1", SqlError::Aborted);
    run(&mut s, "ROLLBACK TO existing");
    run(&mut s, "COMMIT");
}

#[test]
fn savepoint_names_fold_quote_escape_and_truncate_at_63_bytes() {
    let a = db();
    let mut s = a.sql_session();
    run(&mut s, "BEGIN");
    run(&mut s, "SAVEPOINT MiXeD");
    run(&mut s, "RELEASE mixed");
    run(&mut s, "SAVEPOINT \"MiXeD\"");
    error(&mut s, "RELEASE mixed", SqlError::InvalidSavepoint);
    run(&mut s, "ROLLBACK TO \"MiXeD\"");
    run(&mut s, "RELEASE \"MiXeD\"");
    run(&mut s, "SAVEPOINT \"a\"\"b\"");
    run(&mut s, "RELEASE \"a\"\"b\"");
    let prefix = "a".repeat(63);
    run(&mut s, &format!("SAVEPOINT {prefix}b"));
    run(&mut s, &format!("RELEASE {prefix}c"));
    run(&mut s, "SAVEPOINT savepoint");
    run(&mut s, "RELEASE SAVEPOINT");
    run(&mut s, "COMMIT");
}

#[test]
fn rollback_restores_dml_and_create_alter_catalog_changes() {
    let a = db();
    let mut s = a.sql_session();
    setup(&mut s);
    run(&mut s, "SAVEPOINT before_ddl");
    run(&mut s, "UPDATE t SET v=20 WHERE id=1");
    run(&mut s, "INSERT INTO t VALUES(2,30)");
    run(&mut s, "ALTER TABLE t ADD COLUMN extra INTEGER DEFAULT 7");
    run(&mut s, "CREATE TABLE transient(id BIGINT PRIMARY KEY)");
    run(&mut s, "ROLLBACK TO before_ddl");
    assert_eq!(
        rows(&mut s, "SELECT * FROM t ORDER BY id"),
        vec![vec![V::Int(1), V::Int(10)]]
    );
    run(&mut s, "CREATE TABLE transient(id BIGINT PRIMARY KEY)");
    run(&mut s, "ALTER TABLE t ADD COLUMN extra INTEGER DEFAULT 8");
    run(&mut s, "COMMIT");
    assert_eq!(
        rows(&mut a.sql_session(), "SELECT extra FROM t"),
        vec![vec![V::Int(8)]]
    );
}

#[test]
fn sequence_allocations_and_currval_survive_savepoint_rollback() {
    let a = db();
    let mut s = a.sql_session();
    let clock = StatementClock {
        unavailable: None,
        transaction_us: 100,
        statement_us: 200,
        zone: Some(SessionZone::Fixed(0)),
        wall: None,
    };
    let sequence_rows = |s: &mut SqlSession, sql: &str| {
        s.execute_at(sql, &[], None, &clock)
            .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
            .rows
            .into_iter()
            .map(|r| r.values)
            .collect::<Vec<_>>()
    };
    run(&mut s, "CREATE TABLE tickets(id BIGSERIAL PRIMARY KEY)");
    run(&mut s, "BEGIN");
    run(&mut s, "SAVEPOINT before_alloc");
    assert_eq!(
        sequence_rows(&mut s, "SELECT nextval('tickets_id_seq')"),
        vec![vec![V::Int(1)]]
    );
    run(&mut s, "ROLLBACK TO before_alloc");
    assert_eq!(
        sequence_rows(&mut s, "SELECT currval('tickets_id_seq')"),
        vec![vec![V::Int(1)]]
    );
    assert_eq!(
        sequence_rows(&mut s, "SELECT nextval('tickets_id_seq')"),
        vec![vec![V::Int(2)]]
    );
    run(&mut s, "ROLLBACK");
    assert_eq!(
        sequence_rows(&mut s, "SELECT nextval('tickets_id_seq')"),
        vec![vec![V::Int(3)]]
    );
}

#[test]
fn rollback_to_preserves_transaction_clock_pin() {
    let a = db();
    let mut s = a.sql_session();
    run(&mut s, "BEGIN");
    let c = StatementClock {
        unavailable: None,
        transaction_us: 100,
        statement_us: 200,
        zone: Some(SessionZone::Fixed(0)),
        wall: None,
    };
    let before = s
        .execute_at("SELECT transaction_timestamp()", &[], None, &c)
        .unwrap()
        .rows;
    run(&mut s, "SAVEPOINT clock_mark");
    run(&mut s, "ROLLBACK TO clock_mark");
    let later = StatementClock {
        statement_us: 300,
        ..c
    };
    let after = s
        .execute_at("SELECT transaction_timestamp()", &[], None, &later)
        .unwrap()
        .rows;
    assert_eq!(
        before[0].values,
        vec![V::TimestampTz(TimestampTz::from_microseconds(100).unwrap())]
    );
    assert_eq!(before[0].values, after[0].values);
    run(&mut s, "ROLLBACK");
}

#[test]
fn local_live_mark_limit_recovers_and_release_frees_capacity() {
    let a = db();
    let mut s = a.sql_session();
    run(&mut s, "BEGIN");
    for i in 0..64 {
        run(&mut s, &format!("SAVEPOINT s{i}"));
    }
    error(&mut s, "SAVEPOINT overflow", SqlError::Limit);
    run(&mut s, "ROLLBACK TO s63");
    run(&mut s, "RELEASE s63");
    run(&mut s, "SAVEPOINT replacement");
    run(&mut s, "ROLLBACK TO s0");
    for i in 1..64 {
        run(&mut s, &format!("SAVEPOINT again{i}"));
    }
    run(&mut s, "COMMIT");
    run(&mut s, "BEGIN");
    run(&mut s, "SAVEPOINT fresh");
    run(&mut s, "ROLLBACK");
}
