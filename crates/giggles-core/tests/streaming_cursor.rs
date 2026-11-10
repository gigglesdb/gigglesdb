mod common;
use giggles_core::{
    Admin, CoreError, Engine, Limits,
    relational::{SqlError, Value},
};

fn exercise(engine: Engine, count: usize, width: usize, duplicate: bool) {
    let path = common::fresh("streaming-cursor");
    let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
    let mut s = admin.sql_session();
    s.execute(
        "CREATE TABLE stream_rows (id bigint PRIMARY KEY, body text)",
        &[],
        None,
    )
    .unwrap();
    let insert = s.prepare("INSERT INTO stream_rows VALUES ($1,$2)").unwrap();
    let payload = "x".repeat(width);
    for i in 0..count {
        s.execute_prepared(
            &insert,
            &[Value::Int(i as i64), Value::Text(payload.clone())],
            None,
        )
        .unwrap();
    }
    let query = s
        .prepare(if duplicate {
            "SELECT id,body,body FROM stream_rows ORDER BY id"
        } else {
            "SELECT id,body FROM stream_rows ORDER BY id"
        })
        .unwrap();
    assert!(matches!(
        s.execute_prepared(&query, &[], None),
        Err(CoreError::Sql(SqlError::Limit))
    ));
    let mut c = s.open_cursor(&query, &[]).unwrap();
    assert!(matches!(
        c.fetch(10, 1),
        Err(CoreError::Sql(SqlError::Limit))
    ));
    // Cursor snapshot survives a later commit and does not include the new row.
    s.execute_prepared(
        &insert,
        &[Value::Int(count as i64), Value::Text(payload.clone())],
        None,
    )
    .unwrap();
    let mut seen = 0;
    let mut bytes = 0;
    while !c.is_exhausted() {
        let chunk = c.fetch(7, 128 * 1024).unwrap();
        assert!(chunk.len() <= 7);
        for row in chunk {
            let mut expected = vec![Value::Int(seen as i64), Value::Text(payload.clone())];
            if duplicate {
                expected.push(Value::Text(payload.clone()));
            }
            assert_eq!(row.values, expected);
            seen += 1;
            bytes += width * if duplicate { 2 } else { 1 };
        }
    }
    assert_eq!(seen, count);
    assert_eq!(bytes, count * width * if duplicate { 2 } else { 1 });
    assert!(c.fetch(7, 128 * 1024).unwrap().is_empty());
    drop(c);
    drop(s);
    drop(admin);
    if path.is_dir() {
        std::fs::remove_dir_all(path).unwrap();
    } else {
        std::fs::remove_file(path).unwrap();
    }
}
#[test]
fn legacy_streams_two_mib_and_preserves_snapshot() {
    exercise(Engine::Legacy, 40, 56 * 1024, false);
}
#[test]
fn filtered_reverse_scan_limit_and_buffer_budget() {
    let path = common::fresh("cursor-budget");
    let admin = Admin::create(&path, Limits::default()).unwrap();
    let mut s = admin.sql_session();
    s.execute(
        "CREATE TABLE t (id bigint PRIMARY KEY, body text)",
        &[],
        None,
    )
    .unwrap();
    for i in 0..50 {
        s.execute(
            "INSERT INTO t VALUES ($1,$2)",
            &[Value::Int(i), Value::Text("x".repeat(30_000))],
            None,
        )
        .unwrap();
    }
    let p = s
        .prepare("SELECT id FROM t WHERE id >= 10 AND id < 30 ORDER BY id DESC LIMIT 4")
        .unwrap();
    let mut c = s.open_cursor(&p, &[]).unwrap();
    let mut rows = Vec::new();
    while !c.is_exhausted() {
        rows.extend(c.fetch(2, 100).unwrap());
    }
    assert_eq!(
        rows.into_iter()
            .map(|r| r.values[0].clone())
            .collect::<Vec<_>>(),
        vec![
            Value::Int(29),
            Value::Int(28),
            Value::Int(27),
            Value::Int(26)
        ]
    );
    drop(c);
    let p = s
        .prepare("SELECT body,id FROM t ORDER BY body,id DESC")
        .unwrap();
    assert!(matches!(
        s.open_cursor_with_buffer_limit(&p, &[], 1024),
        Err(CoreError::Sql(SqlError::Limit))
    ));
    let mut c = s
        .open_cursor_with_buffer_limit(&p, &[], 8 * 1024 * 1024)
        .unwrap();
    let mut seen = 0;
    while !c.is_exhausted() {
        for row in c.fetch(3, 100_000).unwrap() {
            assert_eq!(row.values[1], Value::Int(49 - seen));
            seen += 1;
        }
    }
    assert_eq!(seen, 50);
    drop(c);
    drop(s);
    drop(admin);
    let _ = std::fs::remove_file(path);
}

#[test]
fn volatile_clock_projection_and_filter_do_not_look_ahead() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    let path = common::fresh("cursor-clock");
    let admin = Admin::create(&path, Limits::default()).unwrap();
    let mut session = admin.sql_session();
    session
        .execute("CREATE TABLE t (id bigint PRIMARY KEY)", &[], None)
        .unwrap();
    session
        .execute("INSERT INTO t VALUES (1),(2),(3)", &[], None)
        .unwrap();
    for sql in [
        "SELECT clock_timestamp() FROM t ORDER BY id",
        "SELECT id FROM t WHERE clock_timestamp() IS NOT NULL ORDER BY id",
    ] {
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = calls.clone();
        let clock = giggles_core::relational::StatementClock {
            unavailable: None,
            transaction_us: 0,
            statement_us: 0,
            zone: None,
            wall: Some(Arc::new(move || {
                Ok(counter.fetch_add(1, Ordering::SeqCst) as i64)
            })),
        };
        let prepared = session.prepare(sql).unwrap();
        let mut cursor = session.open_cursor_at(&prepared, &[], &clock).unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(cursor.fetch(1, 1024).unwrap().len(), 1);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        // Even a byte-limited fetch with a large row allowance must not
        // evaluate another volatile filter/projection to learn its size.
        assert_eq!(cursor.fetch(100, 9).unwrap().len(), 1);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        drop(cursor);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
    drop(session);
    drop(admin);
    let _ = std::fs::remove_file(path);
}
