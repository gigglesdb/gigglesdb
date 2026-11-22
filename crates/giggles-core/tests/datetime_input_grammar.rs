use giggles_core::{Admin, Limits, relational::StatementClock, sqlstate};
use giggles_pg_types::temporal::SessionZone;

#[test]
fn postgres_temporal_input_grammar_through_sql() {
    let path = std::env::temp_dir().join(format!("temporal-input-grammar-{}", std::process::id()));
    let admin = Admin::create(&path, Limits::default()).unwrap();
    let mut session = admin.sql_session();
    let clock = StatementClock {
        unavailable: None,
        transaction_us: 757_382_400_000_000,
        statement_us: 757_382_400_000_000,
        zone: Some(SessionZone::Fixed(0)),
        wall: None,
    };
    for (literal, ty, expected) in [
        ("20240101", "date", "2024-01-01"),
        ("J2451187", "date", "1999-01-08"),
        ("Jan 2 2024", "date", "2024-01-02"),
        ("Tuesday Jan 2 2024", "date", "2024-01-02"),
        ("2024-01-01 Monday", "date", "2024-01-01"),
        ("2024/01/02", "date", "2024-01-02"),
        ("01-01-01", "date", "2001-01-01"),
        (
            "2024-01-02 03:04:05 PST",
            "timestamptz",
            "2024-01-02 11:04:05+00",
        ),
        (
            "2024-07-02 03:04:05 EST5EDT",
            "timestamptz",
            "2024-07-02 07:04:05+00",
        ),
        (
            "Tue Jan 02 03:04:05 2024 PST",
            "timestamp",
            "2024-01-02 03:04:05",
        ),
        ("2024-01-02 03:04:05 PST", "time", "03:04:05"),
        ("03:04:05 PST", "timetz", "03:04:05-08"),
    ] {
        let sql = format!("SELECT '{literal}'::{ty}::text = '{expected}'");
        assert!(
            matches!(
                session.execute_at(&sql, &[], None, &clock).unwrap().rows[0].values[0],
                giggles_core::relational::Value::Bool(true)
            ),
            "{sql}"
        );
    }

    for (literal, ty, expected) in [
        ("J", "date", "22007"),
        ("Foo 2 2024", "date", "22007"),
        ("Monday 2024-01-01", "date", "22007"),
        ("Mon 2024-01-01", "date", "22007"),
        ("Monday, 2024-01-01", "date", "22007"),
        ("Monday 01/02/2024", "date", "22007"),
        ("Mon 2024-01-01 03:04", "timestamp", "22007"),
        ("2024-01-02 03:04:05 1999", "timestamp", "22007"),
        ("2024-01-02 03:04:05 1999", "timestamptz", "22007"),
        ("Jan 32 2024", "date", "22008"),
        ("2024-13-01", "date", "22008"),
        ("2024-01-02 03:04:05 XYZ", "timestamptz", "22007"),
        ("2024-01-02 03:04:05 Foo/Bar", "timestamptz", "22023"),
        ("1999-01-08 04:05:06 +02 PST", "timestamptz", "22007"),
        ("03:04:05 EST5EDT", "timetz", "22007"),
    ] {
        let sql = format!("SELECT '{literal}'::{ty}");
        let error = session.execute_at(&sql, &[], None, &clock).unwrap_err();
        assert_eq!(sqlstate(&error), expected, "{sql}: {error:?}");
    }
    drop(session);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn temporal_literals_use_the_execution_zone_and_clock() {
    use giggles_core::relational::Value;
    let path = std::env::temp_dir().join(format!("temporal-input-clock-{}", std::process::id()));
    let admin = Admin::create(&path, Limits::default()).unwrap();
    let mut session = admin.sql_session();
    let clock = StatementClock {
        unavailable: None,
        transaction_us: 757_382_400_000_000,
        statement_us: 757_382_400_000_000,
        zone: Some(SessionZone::named("Asia/Kolkata").unwrap()),
        wall: None,
    };
    let result = session
        .execute_at(
            "SELECT '03:04:05 IST'::timetz::text,
                '03:04:05 LMT'::timetz::text,
                '2024-01-02 03:04:05 IST'::timestamptz::text,
                '1800-01-02 03:04:05 LMT'::timestamptz::text,
                'today'::date = current_date,
                'tomorrow'::date - 'yesterday'::date,
                'allballs'::time::text,
                'epoch'::timestamptz = '1970-01-01 00:00+00'::timestamptz,
                'now'::timestamptz <= now()",
            &[],
            None,
            &clock,
        )
        .unwrap();
    assert_eq!(
        result.rows[0].values,
        [
            Value::Text("03:04:05+05:30".into()),
            Value::Text("03:04:05+05:53:28".into()),
            Value::Text("2024-01-02 03:04:05+05:30".into()),
            Value::Text("1800-01-02 03:04:05+05:53:28".into()),
            Value::Bool(true),
            Value::Int(2),
            Value::Text("00:00:00".into()),
            Value::Bool(true),
            Value::Bool(true),
        ]
    );
    drop(session);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}
