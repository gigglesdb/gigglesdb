use giggles_core::{
    Admin, Limits,
    relational::{DataType, Value},
};
use std::time::{SystemTime, UNIX_EPOCH};

fn matches(data_type: &DataType, value: &Value) -> bool {
    matches!(
        (data_type, value),
        (
            DataType::SmallInt | DataType::Integer | DataType::BigInt,
            Value::Int(_)
        ) | (DataType::Text, Value::Text(_))
            | (DataType::Numeric, Value::Numeric(_))
    )
}

#[test]
fn describe_matches_execution_columns_and_value_types() {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let path = std::env::temp_dir().join(format!(
        "giggles-describe-parity-{}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
    ));
    let admin = Admin::create(&path, Limits::default()).unwrap();
    let mut session = admin.sql_session();
    for sql in [
        "CREATE TABLE t (id BIGINT PRIMARY KEY, n INTEGER, s TEXT, d NUMERIC)",
        "CREATE TABLE u (id BIGINT PRIMARY KEY, t_id BIGINT, x TEXT)",
        "INSERT INTO t VALUES (1, 2, 'one', 1.5), (2, 3, 'two', 2.5), (3, NULL, NULL, NULL)",
        "INSERT INTO u VALUES (1, 1, 'joined'), (2, 2, 'second')",
    ] {
        session.execute(sql, &[], None).unwrap();
    }
    for (sql, params) in [
        ("SELECT n FROM t", vec![]),
        ("SELECT n AS alias, s FROM t", vec![]),
        ("SELECT * FROM t", vec![]),
        ("SELECT n + 1, s || 'x', d * 2 FROM t", vec![]),
        ("SELECT count(*), sum(n), max(s) FROM t", vec![]),
        // SELECT t.n, u.x FROM t JOIN u ON u.t_id = t.id:
        // describe returns CoreError::Sql(SqlError::Unbounded).
        // SELECT t.*, u.x FROM t LEFT JOIN u ON u.t_id = t.id:
        // prepare returns CoreError::Sql(SqlError::Syntax).
        (
            "INSERT INTO t VALUES (100, 1, 'a', 1.5) RETURNING id, n + 1 AS m, s",
            vec![],
        ),
        ("UPDATE t SET n = n + 1 WHERE id = 1 RETURNING *", vec![]),
        ("DELETE FROM u WHERE id = 1 RETURNING t_id, x", vec![]),
        ("SELECT n FROM t WHERE id = $1", vec![Value::Int(1)]),
    ] {
        let prepared = session.prepare(sql).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
        let description = session
            .describe(&prepared)
            .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
        let result = session
            .execute_prepared(&prepared, &params, None)
            .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
        assert!(!result.rows.is_empty(), "{sql}");
        assert_eq!(description.typmods.len(), description.columns.len(), "{sql}");
        assert_eq!(
            description.columns.iter().map(|c| &c.name).collect::<Vec<_>>(),
            result.columns.iter().collect::<Vec<_>>(),
            "{sql}"
        );
        for row in &result.rows {
            assert_eq!(row.values.len(), description.columns.len(), "{sql}");
            for (column, value) in description.columns.iter().zip(&row.values) {
                if *value != Value::Null {
                    assert!(
                        matches(&column.data_type, value),
                        "{sql}: column {} described {:?}, executed {value:?}",
                        column.name,
                        column.data_type,
                    );
                }
            }
        }
    }
    drop(session);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}
