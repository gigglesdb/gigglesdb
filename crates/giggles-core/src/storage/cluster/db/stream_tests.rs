//! Gateway cursors retain a timestamp, not the complete query result.
use super::local::LocalCluster;
use super::tests::{clock, gateway};
use crate::relational::{SqlError, Value, encode_key};
use crate::storage::CoreError;
use crate::storage::range_api::{RangeSpace, range_key};

fn splits() -> Vec<Vec<u8>> {
    // Split inside each possible first table's primary keys, not just between tables.
    (1..=12_u64)
        .map(|id| {
            let mut key = id.to_be_bytes().to_vec();
            key.extend(encode_key(&[Value::Int(128)]).unwrap());
            range_key(RangeSpace::Rows, &key)
        })
        .collect()
}

#[test]
fn gateway_cursor_streams_past_the_buffer_limit() {
    let cluster = LocalCluster::new(&splits());
    let a = gateway(&cluster, 1);
    let mut s = a.admin.sql_session();
    s.execute_at(
        "CREATE TABLE t(id INT PRIMARY KEY, payload TEXT)",
        &[],
        None,
        &clock(),
    )
    .unwrap();
    let payload = "x".repeat(16 * 1024);
    // Four MiB, across a row-range boundary and many range pages.
    for batch in 0..8 {
        let values = (batch * 32..(batch + 1) * 32)
            .map(|id| format!("({id}, '{payload}')"))
            .collect::<Vec<_>>()
            .join(",");
        s.execute_at(
            &format!("INSERT INTO t VALUES {values}"),
            &[],
            None,
            &clock(),
        )
        .unwrap();
    }
    let p = s.prepare("SELECT id, payload FROM t ORDER BY id").unwrap();
    s.describe(&p).unwrap();
    // Reuse the exact prepared statement: oracle timestamps advance, catalog epoch does not.
    for _ in 0..2 {
        let mut cursor = s.open_cursor_with_buffer_limit(&p, &[], 64 * 1024).unwrap();
        let mut seen = 0;
        while !cursor.is_exhausted() {
            let rows = cursor.fetch(3, 64 * 1024).unwrap();
            for row in rows {
                assert_eq!(
                    row.values,
                    vec![Value::Int(seen), Value::Text(payload.clone())]
                );
                seen += 1;
            }
        }
        assert_eq!(seen, 256);
    }
    // A writing transaction must still use the bounded read-your-writes bridge.
    s.execute_at("BEGIN", &[], None, &clock()).unwrap();
    s.execute_at("INSERT INTO t VALUES (256, 'own')", &[], None, &clock())
        .unwrap();
    assert!(matches!(
        s.open_cursor_with_buffer_limit(&p, &[], 64 * 1024),
        Err(CoreError::Sql(SqlError::Limit))
    ));
    s.execute_at("ROLLBACK", &[], None, &clock()).unwrap();
}

#[test]
fn gateway_cursor_keeps_its_snapshot() {
    let cluster = LocalCluster::new(&splits());
    let a = gateway(&cluster, 1);
    let mut s = a.admin.sql_session();
    s.execute_at(
        "CREATE TABLE t(id INT PRIMARY KEY, v INT)",
        &[],
        None,
        &clock(),
    )
    .unwrap();
    let values = (0..256)
        .map(|id| format!("({id}, {id})"))
        .collect::<Vec<_>>()
        .join(",");
    s.execute_at(
        &format!("INSERT INTO t VALUES {values}"),
        &[],
        None,
        &clock(),
    )
    .unwrap();
    let p = s.prepare("SELECT id, v FROM t ORDER BY id").unwrap();
    let mut cursor = s.open_cursor_with_buffer_limit(&p, &[], 1).unwrap();
    assert_eq!(
        cursor.fetch(1, 1024).unwrap()[0].values,
        vec![Value::Int(0), Value::Int(0)]
    );
    for sql in [
        "BEGIN",
        "UPDATE t SET v = -1 WHERE id = 200",
        "DELETE FROM t WHERE id = 201",
        "INSERT INTO t VALUES (300, 300)",
        "COMMIT",
    ] {
        s.execute_at(sql, &[], None, &clock()).unwrap();
    }
    let mut seen = 1;
    while !cursor.is_exhausted() {
        for row in cursor.fetch(7, 1024).unwrap() {
            assert_eq!(row.values, vec![Value::Int(seen), Value::Int(seen)]);
            seen += 1;
        }
    }
    assert_eq!(seen, 256);
}
