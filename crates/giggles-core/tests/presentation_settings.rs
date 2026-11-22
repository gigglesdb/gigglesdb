use giggles_core::{
    Admin, Limits,
    relational::{StatementClock, Value},
};
use giggles_pg_types::{
    bytea::ByteaOutput,
    temporal::{DateOrder, DateStyle, DateStyleSettings, SessionZone},
};
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn prepared_datetime_literals_keep_their_order_and_presentation_uses_execution_settings() {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!("giggles-presentation-{unique}"));
    let admin = Admin::create(&path, Limits::default()).unwrap();
    let mut session = admin.sql_session();
    let clock = StatementClock {
        transaction_us: 0,
        statement_us: 0,
        unavailable: None,
        zone: Some(SessionZone::setting("UTC").unwrap().0),
        wall: None,
    };
    let query = r"SELECT '01/02/2024'::date::text,
        '01/02/2024'::text::date::text,
        date '2024-01-02' || '',
        format('%s', timestamp '2024-01-02 03:04:05.678'),
        timestamptz '2024-01-02 03:04:05.678+00'::text,
        '\x00ff5c41'::bytea::text,
        '01/02/2024 03:04:05'::timestamptz::text,
        '01/02/2024 03:04:05'::text::timestamptz::text";
    let prepared = session.prepare_unnamed(query, &[]).unwrap();
    session.validate_immutable_constants(&prepared).unwrap();
    let iso = session
        .execute_prepared_at(&prepared, &[], None, &clock)
        .unwrap();
    assert_eq!(
        iso.rows[0].values,
        [
            "2024-01-02",
            "2024-01-02",
            "2024-01-02",
            "2024-01-02 03:04:05.678",
            "2024-01-02 03:04:05.678+00",
            r"\x00ff5c41",
            "2024-01-02 03:04:05+00",
            "2024-01-02 03:04:05+00",
        ]
        .map(|text| Value::Text(text.into()))
    );
    session.set_text_settings(
        DateStyleSettings {
            style: DateStyle::Sql,
            order: DateOrder::Dmy,
        },
        ByteaOutput::Escape,
    );
    session.validate_immutable_constants(&prepared).unwrap();
    let sql = session
        .execute_prepared_at(&prepared, &[], None, &clock)
        .unwrap();
    assert_eq!(
        sql.rows[0].values,
        [
            "02/01/2024",
            "01/02/2024",
            "02/01/2024",
            "02/01/2024 03:04:05.678",
            "02/01/2024 03:04:05.678 UTC",
            r"\000\377\\A",
            "02/01/2024 03:04:05 UTC",
            "01/02/2024 03:04:05 UTC",
        ]
        .map(|text| Value::Text(text.into()))
    );
    let newly_prepared = session.prepare_unnamed(query, &[]).unwrap();
    let new = session
        .execute_prepared_at(&newly_prepared, &[], None, &clock)
        .unwrap();
    assert_eq!(new.rows[0].values[0], Value::Text("01/02/2024".into()));

    session.execute_at(
        "CREATE TABLE presentation_defaults (id integer PRIMARY KEY, d date DEFAULT '01/02/2024')",
        &[], None, &clock,
    ).unwrap();
    session.set_text_settings(DateStyleSettings::default(), ByteaOutput::default());
    session
        .execute_at(
            "INSERT INTO presentation_defaults (id) VALUES (1)",
            &[],
            None,
            &clock,
        )
        .unwrap();
    let default = session
        .execute_at(
            "SELECT d::text FROM presentation_defaults",
            &[],
            None,
            &clock,
        )
        .unwrap();
    assert_eq!(default.rows[0].values, [Value::Text("2024-02-01".into())]);
    drop(session);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn date_order_applies_to_prepared_comparisons_joins_and_mutations() {
    let path = std::env::temp_dir().join(format!("presentation-lookups-{}", std::process::id()));
    let admin = Admin::create(&path, Limits::default()).unwrap();
    let mut session = admin.sql_session();
    for sql in [
        "CREATE TABLE date_lookup (id integer PRIMARY KEY, d date, marker integer)",
        "CREATE TABLE date_peers (id integer PRIMARY KEY)",
        "INSERT INTO date_lookup VALUES (1,'2002-03-04',0),(2,'2004-03-02',0)",
        "INSERT INTO date_peers VALUES (1),(2)",
    ] {
        session.execute(sql, &[], None).unwrap();
    }

    const EQUALITY: &str = "SELECT id FROM date_lookup WHERE d = '04/03/02'";
    let date_style = |order| DateStyleSettings {
        style: DateStyle::Iso,
        order,
    };
    session.set_text_settings(date_style(DateOrder::Dmy), ByteaOutput::default());
    let dmy_plan = session.prepare_unnamed(EQUALITY, &[]).unwrap();
    for (order, id) in [(DateOrder::Dmy, 1), (DateOrder::Ymd, 2)] {
        session.set_text_settings(date_style(order), ByteaOutput::default());
        for sql in [
            EQUALITY,
            "SELECT id FROM date_lookup WHERE d IN ('04/03/02')",
            "SELECT a.id FROM date_lookup a JOIN date_peers b ON a.id = b.id AND a.d = '04/03/02'",
            "UPDATE date_lookup SET marker = 1 WHERE d = '04/03/02' RETURNING id",
        ] {
            let result = session
                .execute(sql, &[], None)
                .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
            assert_eq!(result.rows.len(), 1, "{order:?}: {sql}");
            assert_eq!(result.rows[0].values, [Value::Int(id)], "{order:?}: {sql}");
        }
    }
    let result = session.execute_prepared(&dmy_plan, &[], None).unwrap();
    assert_eq!(result.rows[0].values, [Value::Int(1)]);
    let mut cursor = session.open_cursor(&dmy_plan, &[]).unwrap();
    let rows = cursor.fetch(8, 1024).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values, [Value::Int(1)]);
    drop(cursor);

    for (order, id) in [(DateOrder::Ymd, 2), (DateOrder::Dmy, 1)] {
        session.set_text_settings(date_style(order), ByteaOutput::default());
        let deleted = session
            .execute(
                "DELETE FROM date_lookup WHERE d = '04/03/02' RETURNING id",
                &[],
                None,
            )
            .unwrap();
        assert_eq!(deleted.rows.len(), 1);
        assert_eq!(deleted.rows[0].values, [Value::Int(id)]);
    }
    drop(session);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn unsupported_record_and_typed_array_constructors_keep_their_sqlstates() {
    let path =
        std::env::temp_dir().join(format!("presentation-constructors-{}", std::process::id()));
    let admin = Admin::create(&path, Limits::default()).unwrap();
    let mut session = admin.sql_session();
    for (sql, state) in [
        ("SELECT ROW(1,10) < ROW(1,9)", "0A000"),
        ("SELECT ROW(1.50, 'a b', true, NULL::int)", "42601"),
        ("SELECT ARRAY[date '2024-01-02']", "0A000"),
        ("SELECT ARRAY[timestamp '2024-01-02 03:04:05']", "0A000"),
        (
            "SELECT ARRAY[timestamptz '2024-01-02 03:04:05+00']",
            "0A000",
        ),
        (r"SELECT ARRAY['\x41'::bytea]", "0A000"),
    ] {
        let error = session.execute(sql, &[], None).unwrap_err();
        assert_eq!(giggles_core::sqlstate(&error), state, "{sql}: {error:?}");
    }

    session
        .execute(
            "CREATE TYPE presentation_record AS (a int, b text)",
            &[],
            None,
        )
        .unwrap();
    let composite = session
        .execute("SELECT ROW(1,'x y')::presentation_record", &[], None)
        .unwrap();
    assert_eq!(
        composite.rows[0].values,
        [Value::Text(r#"(1,"x y")"#.into())]
    );
    drop(session);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}
