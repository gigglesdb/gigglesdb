mod common;

use giggles_core::{Admin, CoreError, Limits, SqlSession, relational::*, sqlstate};
use giggles_pg_types::temporal::SessionZone;

const COLUMNS: [(&str, &str); 6] = [
    ("d", "date"),
    ("t", "time"),
    ("tz", "timetz"),
    ("ts", "timestamp"),
    ("tstz", "timestamptz"),
    ("i", "interval"),
];

type CastRows = [Option<&'static str>; 5];

const UTC_CASTS: [(&str, &str, CastRows); 13] = [
    (
        "d",
        "timestamp",
        [
            Some("2024-01-02 00:00:00"),
            Some("infinity"),
            None,
            Some("-infinity"),
            Some("2024-07-15 00:00:00"),
        ],
    ),
    (
        "d",
        "timestamptz",
        [
            Some("2024-01-02 00:00:00+00"),
            Some("infinity"),
            None,
            Some("-infinity"),
            Some("2024-07-15 00:00:00+00"),
        ],
    ),
    (
        "t",
        "interval",
        [
            Some("12:34:56.123456"),
            Some("00:00:00"),
            None,
            Some("24:00:00"),
            Some("23:59:59.999999"),
        ],
    ),
    (
        "t",
        "timetz",
        [
            Some("12:34:56.123456+00"),
            Some("00:00:00+00"),
            None,
            Some("24:00:00+00"),
            Some("23:59:59.999999+00"),
        ],
    ),
    (
        "tz",
        "time",
        [
            Some("12:34:56.123456"),
            Some("23:59:59"),
            None,
            Some("24:00:00"),
            Some("00:00:00"),
        ],
    ),
    (
        "ts",
        "date",
        [
            Some("2024-01-02"),
            Some("-infinity"),
            None,
            Some("infinity"),
            Some("2024-07-15"),
        ],
    ),
    (
        "ts",
        "time",
        [Some("12:34:56.123456"), None, None, None, Some("23:30:00")],
    ),
    (
        "ts",
        "timestamptz",
        [
            Some("2024-01-02 12:34:56.123456+00"),
            Some("-infinity"),
            None,
            Some("infinity"),
            Some("2024-07-15 23:30:00+00"),
        ],
    ),
    (
        "tstz",
        "date",
        [
            Some("2024-01-02"),
            Some("infinity"),
            None,
            Some("-infinity"),
            Some("2024-07-15"),
        ],
    ),
    (
        "tstz",
        "time",
        [Some("12:34:56.123456"), None, None, None, Some("23:30:00")],
    ),
    (
        "tstz",
        "timetz",
        [
            Some("12:34:56.123456+00"),
            None,
            None,
            None,
            Some("23:30:00+00"),
        ],
    ),
    (
        "tstz",
        "timestamp",
        [
            Some("2024-01-02 12:34:56.123456"),
            Some("infinity"),
            None,
            Some("-infinity"),
            Some("2024-07-15 23:30:00"),
        ],
    ),
    (
        "i",
        "time",
        [
            Some("12:34:56.123456"),
            Some("21:00:00"),
            None,
            Some("01:30:00"),
            Some("23:59:59"),
        ],
    ),
];

fn fixture() -> (Admin, SqlSession) {
    let admin = Admin::create(common::fresh("temporal-cast-matrix"), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    let columns =
        "id int PRIMARY KEY, d date, t time, tz timetz, ts timestamp, tstz timestamptz, i interval";
    for table in ["m", "empty_matrix", "a"] {
        execute(
            &mut session,
            &format!("CREATE TABLE {table} ({columns})"),
            &clock("UTC", 0),
        );
    }
    execute(
        &mut session,
        "INSERT INTO m VALUES \
         (1,'2024-01-02','12:34:56.123456','12:34:56.123456+05:30','2024-01-02 12:34:56.123456','2024-01-02 12:34:56.123456+00','1 day 12:34:56.123456'),\
         (2,'infinity','00:00:00','23:59:59-08','-infinity','infinity','-3 hours'),\
         (3,NULL,NULL,NULL,NULL,NULL,NULL),\
         (4,'-infinity','24:00:00','24:00:00+00','infinity','-infinity','25 hours 30 minutes'),\
         (5,'2024-07-15','23:59:59.999999','00:00:00-12','2024-07-15 23:30:00','2024-07-15 23:30:00+00','-1 day -00:00:01')",
        &clock("UTC", 0),
    );
    (admin, session)
}

fn clock(zone: &str, transaction_us: i64) -> StatementClock {
    StatementClock {
        unavailable: None,
        transaction_us,
        statement_us: transaction_us,
        zone: Some(SessionZone::named(zone).unwrap()),
        wall: None,
    }
}

fn execute(session: &mut SqlSession, sql: &str, clock: &StatementClock) -> Vec<Vec<Value>> {
    session
        .execute_at(sql, &[], None, clock)
        .unwrap_or_else(|err| panic!("{sql}: {} ({err:?})", sqlstate(&err)))
        .rows
        .into_iter()
        .map(|row| row.values)
        .collect()
}

fn assert_state<T>(result: Result<T, CoreError>, expected: &str, sql: &str) {
    match result {
        Err(err) => assert_eq!(sqlstate(&err), expected, "{sql}: {err:?}"),
        Ok(_) => panic!("{sql} succeeded, expected {expected}"),
    }
}

fn text(value: Option<&str>) -> Value {
    value.map_or(Value::Null, |value| Value::Text(value.into()))
}

fn assert_casts(
    session: &mut SqlSession,
    clock: &StatementClock,
    expression: &str,
    expected: [Option<&str>; 5],
) {
    let sql = format!("SELECT ({expression})::text FROM m ORDER BY id");
    let expected = expected.map(|value| vec![text(value)]);
    assert_eq!(execute(session, &sql, clock), expected, "{sql}");
}

#[test]
fn explicit_temporal_matrix_on_populated_and_empty_relations() {
    let (_admin, mut session) = fixture();
    let utc = clock("UTC", 0);
    for (column, source) in COLUMNS {
        for (_, target) in COLUMNS {
            let expression = format!("{column}::{target}");
            if source == target {
                let sql = format!("SELECT {column}, {expression} FROM m ORDER BY id");
                let rows = execute(&mut session, &sql, &utc);
                assert_eq!(rows.len(), 5, "{sql}");
                assert!(rows.iter().all(|row| row[0] == row[1]), "{sql}: {rows:?}");
            } else if let Some((_, _, expected)) = UTC_CASTS
                .iter()
                .find(|(from, to, _)| *from == column && *to == target)
            {
                assert_casts(&mut session, &utc, &expression, *expected);
            } else {
                for table in ["m", "empty_matrix"] {
                    let sql = format!("SELECT {expression} FROM {table} ORDER BY id");
                    assert_state(session.execute_at(&sql, &[], None, &utc), "42846", &sql);
                }
            }
        }
    }
}

#[test]
fn temporal_casts_use_the_session_zone_and_transaction_date() {
    let (_admin, mut session) = fixture();
    // UTC noon on 2024-01-02 and 2024-07-15, far from New York's DST transitions.
    let winter_us = 757_512_000_000_000;
    let summer_us = 774_273_600_000_000;
    let ny = clock("America/New_York", winter_us);
    for (expression, expected) in [
        (
            "d::timestamptz",
            [
                Some("2024-01-02 00:00:00-05"),
                Some("infinity"),
                None,
                Some("-infinity"),
                Some("2024-07-15 00:00:00-04"),
            ],
        ),
        (
            "ts::timestamptz",
            [
                Some("2024-01-02 12:34:56.123456-05"),
                Some("-infinity"),
                None,
                Some("infinity"),
                Some("2024-07-15 23:30:00-04"),
            ],
        ),
        (
            "tstz::date",
            [
                Some("2024-01-02"),
                Some("infinity"),
                None,
                Some("-infinity"),
                Some("2024-07-15"),
            ],
        ),
        (
            "tstz::time",
            [Some("07:34:56.123456"), None, None, None, Some("19:30:00")],
        ),
        (
            "tstz::timetz",
            [
                Some("07:34:56.123456-05"),
                None,
                None,
                None,
                Some("19:30:00-04"),
            ],
        ),
        (
            "tstz::timestamp",
            [
                Some("2024-01-02 07:34:56.123456"),
                Some("infinity"),
                None,
                Some("-infinity"),
                Some("2024-07-15 19:30:00"),
            ],
        ),
    ] {
        assert_casts(&mut session, &ny, expression, expected);
    }
    for (transaction_us, offset) in [(winter_us, "-05"), (summer_us, "-04")] {
        let expected = [
            "12:34:56.123456",
            "00:00:00",
            "",
            "24:00:00",
            "23:59:59.999999",
        ]
        .map(|v| format!("{v}{offset}"));
        assert_casts(
            &mut session,
            &clock("America/New_York", transaction_us),
            "t::timetz",
            [
                Some(&expected[0]),
                Some(&expected[1]),
                None,
                Some(&expected[3]),
                Some(&expected[4]),
            ],
        );
    }
    let kolkata = clock("Asia/Kolkata", winter_us);
    assert_casts(
        &mut session,
        &kolkata,
        "t::timetz",
        [
            Some("12:34:56.123456+05:30"),
            Some("00:00:00+05:30"),
            None,
            Some("24:00:00+05:30"),
            Some("23:59:59.999999+05:30"),
        ],
    );
    assert_casts(
        &mut session,
        &kolkata,
        "tstz::timetz",
        [
            Some("18:04:56.123456+05:30"),
            None,
            None,
            None,
            Some("05:00:00+05:30"),
        ],
    );
}

#[test]
fn cast_syntax_boundaries_and_declared_parameters() {
    let (_admin, mut session) = fixture();
    let utc = clock("UTC", 0);
    assert_eq!(
        execute(
            &mut session,
            "SELECT CAST(d AS timestamp)::text, CAST(ts AS date)::text FROM m WHERE id=1",
            &utc
        ),
        [vec![
            text(Some("2024-01-02 00:00:00")),
            text(Some("2024-01-02"))
        ]],
    );
    assert_eq!(
        execute(
            &mut session,
            "SELECT '294276-12-31'::date::timestamp::text, '-infinity'::timestamp::time, 'infinity'::timestamptz::timetz",
            &utc
        ),
        [vec![
            text(Some("294276-12-31 00:00:00")),
            Value::Null,
            Value::Null
        ]],
    );
    for sql in [
        "SELECT '5874897-12-31'::date::timestamp",
        "SELECT 'infinity'::interval::time",
        "SELECT '-infinity'::interval::time",
    ] {
        assert_state(session.execute_at(sql, &[], None, &utc), "22008", sql);
    }
    for sql in [
        "SELECT '2024-01-02'::date::time",
        "SELECT date '2024-01-02'::interval",
        "SELECT interval '1 day'::date",
        "SELECT time '12:00'::timestamp",
        "SELECT NULL::date::time",
    ] {
        assert_state(session.execute_at(sql, &[], None, &utc), "42846", sql);
    }

    for (sql, source, parameter, expected) in [
        (
            "SELECT $1::timestamp::text",
            DataType::Date,
            Value::Date(Date::from_days(0).unwrap()),
            vec![text(Some("2000-01-01 00:00:00"))],
        ),
        (
            "SELECT $1::time::text, $1::date::text",
            DataType::Timestamp,
            Value::Timestamp(Timestamp::from_microseconds(45_296_000_000).unwrap()),
            vec![text(Some("12:34:56")), text(Some("2000-01-01"))],
        ),
        (
            "SELECT $1::time::text",
            DataType::Interval,
            Value::Interval(Interval::new(0, 0, 97_500_000_000)),
            vec![text(Some("03:05:00"))],
        ),
    ] {
        let prepared = session
            .prepare(sql)
            .unwrap()
            .with_parameter_types(&[Some(source.clone())])
            .unwrap();
        assert_eq!(session.describe(&prepared).unwrap().parameters, [source]);
        let result = session
            .execute_prepared_at(&prepared, &[parameter], None, &utc)
            .unwrap();
        assert_eq!(result.rows[0].values, expected, "{sql}");
    }
    for (sql, source) in [
        ("SELECT $1::time", DataType::Date),
        ("SELECT $1::date", DataType::Time),
    ] {
        let prepared = session
            .prepare(sql)
            .unwrap()
            .with_parameter_types(&[Some(source)])
            .unwrap();
        assert_state(session.describe(&prepared), "42846", sql);
    }
}

#[test]
fn date_trunc_and_explicit_casts_in_writes() {
    let (_admin, mut session) = fixture();
    let utc = clock("UTC", 0);
    execute(
        &mut session,
        "CREATE TABLE trunc_input (id int PRIMARY KEY, t timestamp)",
        &utc,
    );
    execute(
        &mut session,
        "INSERT INTO trunc_input VALUES (1,'2024-05-08 10:00'),(2,'2024-01-03 01:02:03'),(3,NULL)",
        &utc,
    );
    let sql = "SELECT id, date_trunc('week', t::date)::text, pg_typeof(date_trunc('week', t::date))::text FROM trunc_input ORDER BY id";
    let rows = execute(&mut session, sql, &utc);
    assert_eq!(rows.len(), 3);
    for (i, expected) in [
        Some("2024-05-06 00:00:00+00"),
        Some("2024-01-01 00:00:00+00"),
        None,
    ]
    .into_iter()
    .enumerate()
    {
        assert_eq!(
            rows[i],
            [
                Value::Int(i as i64 + 1),
                text(expected),
                text(Some("timestamp with time zone")),
            ]
        );
    }
    let ny = clock("America/New_York", 0);
    assert_eq!(
        execute(
            &mut session,
            "SELECT date_trunc('week', t::date)::text FROM trunc_input ORDER BY id",
            &ny
        ),
        [
            vec![text(Some("2024-05-06 00:00:00-04"))],
            vec![text(Some("2024-01-01 00:00:00-05"))],
            vec![Value::Null]
        ],
    );
    execute(
        &mut session,
        "INSERT INTO a (id, d, t, ts) SELECT id, ts::date, tstz::time, ts FROM m WHERE id=1",
        &utc,
    );
    assert_eq!(
        execute(
            &mut session,
            "SELECT d::text, t::text FROM a WHERE id=1",
            &utc
        ),
        [vec![
            text(Some("2024-01-02")),
            text(Some("12:34:56.123456"))
        ]],
    );
    execute(
        &mut session,
        "UPDATE a SET ts = '2024-07-15 23:30:00'::timestamp WHERE id=1",
        &utc,
    );
    execute(
        &mut session,
        "UPDATE a SET d=ts::date, t=ts::time WHERE id=1",
        &utc,
    );
    assert_eq!(
        execute(
            &mut session,
            "SELECT d::text, t::text FROM a WHERE id=1",
            &utc
        ),
        [vec![text(Some("2024-07-15")), text(Some("23:30:00"))]],
    );
    for sql in [
        "INSERT INTO a (id, i) SELECT id, d::interval FROM empty_matrix",
        "UPDATE a SET i = d::interval WHERE id=2",
    ] {
        assert_state(session.execute_at(sql, &[], None, &utc), "42846", sql);
    }
}

#[test]
fn temporal_insert_select_assignment_matches_postgres() {
    let (_admin, mut session) = fixture();
    let utc = clock("UTC", 0);
    for (index, (source, target, expected)) in UTC_CASTS.iter().enumerate() {
        let (column, _) = COLUMNS.iter().find(|(_, ty)| ty == target).unwrap();
        let id = index + 1;
        execute(
            &mut session,
            &format!("INSERT INTO a (id, {column}) SELECT {id}, {source} FROM m WHERE id=1"),
            &utc,
        );
        assert_eq!(
            execute(
                &mut session,
                &format!("SELECT {column}::text FROM a WHERE id={id}"),
                &utc
            ),
            [vec![text(expected[0])]],
            "assignment {source} -> {column}",
        );
    }
    for (target, source) in [("tz", "ts"), ("d", "t"), ("i", "d"), ("ts", "i")] {
        let sql = format!("INSERT INTO a (id, {target}) SELECT id, {source} FROM m WHERE id=1");
        assert_state(session.execute_at(&sql, &[], None, &utc), "42804", &sql);
    }
}

#[test]
fn date_trunc_date_units_and_errors_match_postgres() {
    let (_admin, mut session) = fixture();
    let ny = clock("America/New_York", 0);
    for (units, expected) in [
        (
            &[
                "microsecon",
                "microsecond",
                "microseconds",
                "microsecondfoobar",
                "us",
                "usec",
                "usecs",
                "usecond",
                "useconds",
                "millisecon",
                "millisecond",
                "milliseconds",
                "ms",
                "msec",
                "msecs",
                "msecond",
                "mseconds",
                "s",
                "sec",
                "secs",
                "second",
                "seconds",
                "m",
                "min",
                "mins",
                "minute",
                "minutes",
                "h",
                "hr",
                "hrs",
                "hour",
                "hours",
                "d",
                "day",
                "days",
            ][..],
            "2024-05-08 00:00:00-04",
        ),
        (
            &["mon", "mons", "month", "months"],
            "2024-05-01 00:00:00-04",
        ),
        (&["qtr", "quarter"], "2024-04-01 00:00:00-04"),
        (&["w", "week", "weeks"], "2024-05-06 00:00:00-04"),
        (
            &["y", "yr", "yrs", "year", "years", "YEARs"],
            "2024-01-01 00:00:00-05",
        ),
        (
            &["dec", "decs", "decade", "decades"],
            "2020-01-01 00:00:00-05",
        ),
        (
            &[
                "c",
                "cent",
                "century",
                "centuries",
                "mil",
                "mils",
                "millennium",
                "millennia",
                "millenniums",
            ],
            "2001-01-01 00:00:00-05",
        ),
    ] {
        for unit in units {
            let sql = format!("SELECT date_trunc('{unit}', date '2024-05-08')::text");
            assert_eq!(
                execute(&mut session, &sql, &ny),
                [vec![text(Some(expected))]],
                "{sql}"
            );
        }
    }
    assert_eq!(
        execute(
            &mut session,
            "SELECT date_trunc('seconds'::varchar, date '2024-05-08')::text",
            &ny,
        ),
        [vec![text(Some("2024-05-08 00:00:00-04"))]],
    );
    for (unit, date, state) in [
        ("julian", "2024-03-10", "22023"),
        ("nonesuch", "2024-03-10", "22023"),
        ("epoch", "2024-03-10", "22023"),
        ("julian", "infinity", "22023"),
        ("epoch", "infinity", "22023"),
        ("j", "2024-05-08", "22023"),
        ("jd", "2024-05-08", "22023"),
        ("mm", "2024-05-08", "22023"),
        ("quarters", "2024-05-08", "22023"),
        (" year", "2024-05-08", "22023"),
        ("timezone", "2024-03-10", "0A000"),
        ("timezone_h", "2024-05-08", "0A000"),
        ("timezone_m", "2024-05-08", "0A000"),
        ("timezone_hour", "-infinity", "0A000"),
        ("timezone_minute", "2024-05-08", "0A000"),
    ] {
        let sql = format!("SELECT date_trunc('{unit}', date '{date}')");
        assert_state(session.execute_at(&sql, &[], None, &ny), state, &sql);
    }
    assert_eq!(
        execute(
            &mut session,
            "SELECT date_trunc('hour', date 'infinity')::text, \
                    date_trunc('hour', date '-infinity')::text, \
                    date_trunc('day', NULL::date), date_trunc(NULL::text, date '2024-03-10')",
            &ny,
        ),
        [vec![
            text(Some("infinity")),
            text(Some("-infinity")),
            Value::Null,
            Value::Null
        ]],
    );
}

#[test]
fn date_trunc_infers_an_unspecified_field_parameter() {
    let (_admin, mut session) = fixture();
    let ny = clock("America/New_York", 0);
    let prepared = session
        .prepare("SELECT date_trunc($1,date '2024-05-08')")
        .unwrap();
    let description = session.describe(&prepared).unwrap();
    assert_eq!(description.parameters, [DataType::Text]);
    assert_eq!(description.columns[0].data_type, DataType::TimestampTz);

    let expected = TimestampTz::parse(
        "2024-05-06 00:00:00-04",
        &giggles_pg_types::temporal::ParserSettings::default(),
    )
    .unwrap();
    let result = session
        .execute_prepared_at(&prepared, &[text(Some("weeks"))], None, &ny)
        .unwrap();
    assert_eq!(result.rows[0].values, [Value::TimestampTz(expected)]);
}

#[test]
fn zoned_casts_at_timestamp_bounds() {
    let (_admin, mut session) = fixture();
    let ny = clock("America/New_York", 0);
    let kolkata = clock("Asia/Kolkata", 0);
    let lower = "'4714-11-24 00:00:00+00 BC'::timestamptz";
    let upper = "'294276-12-31 23:59:59.999999+00'::timestamptz";
    for (sql, zone) in [
        (
            "SELECT '294276-12-31 23:59:59.999999'::timestamp::timestamptz".to_owned(),
            &ny,
        ),
        (format!("SELECT {lower}::timestamp"), &ny),
        (format!("SELECT {upper}::timestamp"), &kolkata),
        (
            "SELECT date '4714-11-24 BC'::timestamptz".to_owned(),
            &kolkata,
        ),
    ] {
        assert_state(session.execute_at(&sql, &[], None, zone), "22008", &sql);
    }
    assert_eq!(
        execute(
            &mut session,
            "SELECT (date '4714-11-24 BC'::timestamptz)::text, \
                    ('4714-11-24 04:56:02+00 BC'::timestamptz::date)::text",
            &ny,
        ),
        [vec![
            text(Some("4714-11-24 00:00:00-04:56:02 BC")),
            text(Some("4714-11-24 BC"))
        ]],
    );

    let lower_times = vec![text(Some("19:03:58")), text(Some("19:03:58-04:56:02"))];
    let upper_parts = vec![
        text(Some("294277-01-01")),
        text(Some("05:29:59.999999")),
        text(Some("05:29:59.999999+05:30")),
    ];
    assert_eq!(
        execute(
            &mut session,
            &format!("SELECT ({lower}::time)::text, ({lower}::timetz)::text"),
            &ny
        ),
        std::slice::from_ref(&lower_times),
    );
    assert_eq!(
        execute(
            &mut session,
            &format!(
                "SELECT ({upper}::date)::text, ({upper}::time)::text, ({upper}::timetz)::text"
            ),
            &kolkata
        ),
        std::slice::from_ref(&upper_parts),
    );
    execute(
        &mut session,
        &format!("INSERT INTO a (id, t, tz) SELECT 1, {lower}, {lower}"),
        &ny,
    );
    execute(
        &mut session,
        &format!("INSERT INTO a (id, d, t, tz) SELECT 2, {upper}, {upper}, {upper}"),
        &kolkata,
    );
    assert_eq!(
        execute(
            &mut session,
            "SELECT t::text, tz::text FROM a WHERE id=1",
            &ny
        ),
        [lower_times],
    );
    assert_eq!(
        execute(
            &mut session,
            "SELECT d::text, t::text, tz::text FROM a WHERE id=2",
            &kolkata
        ),
        [upper_parts],
    );
}
