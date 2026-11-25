mod common;
use giggles_core::{
    Admin, CoreError, Engine, Limits, SqlSession,
    relational::{SqlError, Value::Int},
};

fn fixture() -> SqlSession {
    let engine = if cfg!(feature = "engine-rocksdb") {
        Engine::V2
    } else {
        Engine::Legacy
    };
    Admin::create_engine(common::fresh("union-chain"), Limits::default(), engine)
        .unwrap()
        .sql_session()
}

fn chain(n: usize, separator: &str, branch: impl Fn(usize) -> String) -> String {
    (0..n).map(branch).collect::<Vec<_>>().join(separator)
}

fn execute(db: &mut SqlSession, sql: &str) -> Result<Vec<i64>, CoreError> {
    Ok(db
        .execute(sql, &[], None)?
        .rows
        .into_iter()
        .map(|row| {
            let [Int(n)] = row.values.as_slice() else {
                panic!("expected one integer")
            };
            *n
        })
        .collect())
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn flat_literal_and_table_arms_fit_default_stack() {
    let mut db = fixture();
    execute(&mut db, "CREATE TABLE t (id int PRIMARY KEY)").unwrap();
    execute(&mut db, "INSERT INTO t VALUES (0)").unwrap();
    for n in [34, 1000, 8192] {
        let literals = chain(n, " UNION ALL ", |i| format!("SELECT {i} AS x"));
        assert_eq!(
            execute(&mut db, &format!("SELECT count(*) FROM ({literals}) s")),
            Ok(vec![n as i64])
        );
        let table = chain(n, " UNION ALL ", |i| {
            format!("SELECT id FROM t WHERE id = 0 AND {i} >= 0")
        });
        assert_eq!(
            execute(&mut db, &format!("SELECT count(*) FROM ({table}) s")),
            Ok(vec![n as i64])
        );
    }
}

#[test]
fn union_distinct_and_mixed_associativity() {
    let mut db = fixture();
    let distinct = chain(1000, " UNION ", |i| format!("SELECT {} AS x", i % 13));
    assert_eq!(
        execute(&mut db, &format!("SELECT count(*) FROM ({distinct}) s")),
        Ok(vec![13])
    );
    for (expression, expected) in [
        (
            "SELECT 1 AS x UNION ALL SELECT 1 UNION SELECT 1 UNION ALL SELECT 1",
            vec![1, 1],
        ),
        (
            "SELECT 1 AS x UNION ALL (SELECT 1 UNION SELECT 1 UNION ALL SELECT 1)",
            vec![1, 1, 1],
        ),
        (
            "SELECT 1 AS x UNION ALL SELECT 2 INTERSECT SELECT 3",
            vec![1],
        ),
        (
            "SELECT 1 AS x UNION SELECT 2 EXCEPT SELECT 2 EXCEPT SELECT 1",
            vec![],
        ),
        (
            "SELECT 1 AS x INTERSECT ALL SELECT 1 INTERSECT SELECT 1",
            vec![1],
        ),
    ] {
        assert_eq!(
            execute(
                &mut db,
                &format!("SELECT x FROM ({expression}) s ORDER BY x")
            ),
            Ok(expected)
        );
    }
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn duplicate_series_arms_keep_only_distinct_rows() {
    let mut db = fixture();
    let text = chain(3000, " UNION ", |_| {
        "SELECT g FROM generate_series(1,1000) g".into()
    });
    assert_eq!(execute(&mut db, &text).unwrap(), (1..=1000).collect::<Vec<_>>());
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn over_cap_returns_stack_depth_error() {
    let mut db = fixture();
    let text = chain(8193, " UNION ALL ", |i| format!("SELECT {i}"));
    let error = execute(&mut db, &text).unwrap_err();
    assert_eq!(error, CoreError::Sql(SqlError::StackDepth));
    assert_eq!(giggles_core::sqlstate(&error), "54001");
    assert_eq!(
        giggles_core::relational::take_sql_error_message().as_deref(),
        Some("stack depth limit exceeded")
    );
}

#[test]
fn deeply_parenthesized_set_operations_are_rejected() {
    let mut db = fixture();
    let sql = format!(
        "SELECT count(*) FROM ({}SELECT 0{}) s",
        "(SELECT 1 UNION ALL ".repeat(64),
        ")".repeat(64)
    );
    assert_eq!(execute(&mut db, &sql), Err(CoreError::Sql(SqlError::Limit)));
}
