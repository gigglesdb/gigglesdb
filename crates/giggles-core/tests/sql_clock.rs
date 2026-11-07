use giggles_core::{
    Admin, CoreError, Error, Limits, SqlSession, local,
    relational::{DataType, SqlError, StatementClock, Value},
};
use giggles_pg_types::temporal::{
    Date, ParserSettings, SessionZone, Time, TimeTz, Timestamp, TimestampTz,
};
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

struct Fixture {
    admin: Option<Admin>,
    path: PathBuf,
}
impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicI64 = AtomicI64::new(0);
        let path = std::env::temp_dir().join(format!(
            "giggles-sql-clock-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        Self {
            admin: Some(Admin::create(&path, Limits::default()).unwrap()),
            path,
        }
    }
    fn admin(&self) -> &Admin {
        self.admin.as_ref().unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        drop(self.admin.take());
        std::fs::remove_dir_all(&self.path).unwrap();
    }
}
fn clock(transaction_us: i64, statement_us: i64) -> StatementClock {
    StatementClock {
        unavailable: None,
        transaction_us,
        statement_us,
        zone: Some(SessionZone::Fixed(0)),
        wall: None,
    }
}
fn tz(us: i64) -> Value {
    Value::TimestampTz(TimestampTz::from_microseconds(us).unwrap())
}
fn ts(us: i64) -> Value {
    Value::Timestamp(Timestamp::from_microseconds(us).unwrap())
}
fn values(s: &mut SqlSession, sql: &str, c: &StatementClock) -> Vec<Value> {
    s.execute_at(sql, &[], None, c)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
        .rows[0]
        .values
        .clone()
}
fn sql_error(s: &mut SqlSession, sql: &str, c: &StatementClock, expected: SqlError) {
    assert!(
        matches!(s.execute_at(sql, &[], None, c), Err(CoreError::Sql(e)) if e == expected),
        "{sql}"
    );
}

#[test]
fn explicit_transaction_pins_time_and_statement_time_moves() {
    let f = Fixture::new();
    let mut s = f.admin().sql_session();
    s.execute(
        "CREATE TABLE clock_guard (id BIGINT PRIMARY KEY)",
        &[],
        None,
    )
    .unwrap();
    s.execute("BEGIN", &[], None).unwrap();
    let sql = "SELECT now(), transaction_timestamp(), CURRENT_TIMESTAMP, statement_timestamp()";
    assert_eq!(
        values(&mut s, sql, &clock(100, 200)),
        vec![tz(100), tz(100), tz(100), tz(200)]
    );
    assert_eq!(
        values(&mut s, sql, &clock(100, 300)),
        vec![tz(100), tz(100), tz(100), tz(300)]
    );
    sql_error(
        &mut s,
        "INSERT INTO clock_guard VALUES (1)",
        &clock(101, 400),
        SqlError::Type,
    );
    s.execute("ROLLBACK", &[], None).unwrap();
    assert!(
        s.execute("SELECT id FROM clock_guard", &[], None)
            .unwrap()
            .rows
            .is_empty()
    );
    s.execute("BEGIN", &[], None).unwrap();
    assert_eq!(
        values(&mut s, "SELECT now()", &clock(900, 900)),
        vec![tz(900)]
    );
    s.execute("COMMIT", &[], None).unwrap();
    s.execute("BEGIN", &[], None).unwrap();
    assert_eq!(
        values(&mut s, "SELECT now()", &clock(1000, 1000)),
        vec![tz(1000)]
    );
    s.execute("ROLLBACK", &[], None).unwrap();
}

#[test]
fn clock_validation_precedes_effects_and_rollback_clears_failed_pin() {
    let f = Fixture::new();
    let mut s = f.admin().sql_session();
    s.execute(
        "CREATE TABLE clock_guard (id BIGINT PRIMARY KEY)",
        &[],
        None,
    )
    .unwrap();
    sql_error(
        &mut s,
        "INSERT INTO clock_guard VALUES (1)",
        &clock(20, 19),
        SqlError::InvalidParameterValue,
    );
    assert!(
        s.execute("SELECT id FROM clock_guard", &[], None)
            .unwrap()
            .rows
            .is_empty()
    );
    s.execute("BEGIN", &[], None).unwrap();
    sql_error(
        &mut s,
        "SELECT now(1)",
        &clock(10, 20),
        SqlError::UndefinedFunction,
    );
    // A failed clocked statement must retain its transaction pin until rollback.
    sql_error(
        &mut s,
        "INSERT INTO clock_guard VALUES (2)",
        &clock(11, 30),
        SqlError::Type,
    );
    sql_error(
        &mut s,
        "INSERT INTO clock_guard VALUES (2)",
        &clock(10, 30),
        SqlError::Aborted,
    );
    s.execute("ROLLBACK", &[], None).unwrap();
    s.execute("BEGIN", &[], None).unwrap();
    assert_eq!(values(&mut s, "SELECT now()", &clock(40, 40)), vec![tz(40)]);
    s.execute("ROLLBACK", &[], None).unwrap();
    assert!(
        s.execute("SELECT id FROM clock_guard", &[], None)
            .unwrap()
            .rows
            .is_empty()
    );
}

#[test]
fn prepared_clocks_are_evaluated_at_execution_and_describe_has_temporal_types() {
    let f = Fixture::new();
    let mut s = f.admin().sql_session();
    let p = s.prepare("SELECT now(), transaction_timestamp(), statement_timestamp(), clock_timestamp(), CURRENT_TIMESTAMP, LOCALTIMESTAMP, CURRENT_DATE, CURRENT_TIME, LOCALTIME").unwrap();
    s.validate_immutable_constants(&p).unwrap();
    assert_eq!(
        s.describe(&p)
            .unwrap()
            .columns
            .iter()
            .map(|c| c.data_type.clone())
            .collect::<Vec<_>>(),
        vec![
            DataType::TimestampTz,
            DataType::TimestampTz,
            DataType::TimestampTz,
            DataType::TimestampTz,
            DataType::TimestampTz,
            DataType::Timestamp,
            DataType::Date,
            DataType::TimeTz,
            DataType::Time
        ]
    );
    for n in [123456, 987654] {
        let mut c = clock(n, n + 1);
        c.wall = Some(Arc::new(move || Ok(n + 2)));
        let row = s.execute_prepared_at(&p, &[], None, &c).unwrap().rows[0]
            .values
            .clone();
        assert_eq!(
            &row[..6],
            &[tz(n), tz(n), tz(n + 1), tz(n + 2), tz(n), ts(n)]
        );
    }
    assert!(matches!(
        s.execute_prepared(&p, &[], None),
        Err(CoreError::Sql(SqlError::Unsupported))
    ));
    assert_eq!(s.describe(&p).unwrap().typmods, vec![-1; 9]);
    let precision = s.prepare("SELECT CURRENT_TIMESTAMP(0), CURRENT_TIMESTAMP(7), LOCALTIMESTAMP(0), LOCALTIMESTAMP(7), CURRENT_TIME(0), CURRENT_TIME(7), LOCALTIME(0), LOCALTIME(7)").unwrap();
    assert_eq!(
        s.describe(&precision).unwrap().typmods,
        vec![0, 6, 0, 6, 0, 6, 0, 6]
    );
    s.execute(
        "CREATE TABLE quoted_clock (\"current_timestamp\" BIGINT PRIMARY KEY)",
        &[],
        None,
    )
    .unwrap();
    s.execute("INSERT INTO quoted_clock VALUES (17)", &[], None)
        .unwrap();
    assert_eq!(
        values(
            &mut s,
            "SELECT \"current_timestamp\" FROM quoted_clock",
            &clock(1, 1)
        ),
        vec![Value::Int(17)]
    );
}

#[test]
fn precision_matches_postgres18_half_away_from_zero_and_clamps_seven() {
    let f = Fixture::new();
    let mut s = f.admin().sql_session();
    // Literal results captured from PostgreSQL 18.6 (postgres:18.6), not computed by the implementation.
    // Precision 7 succeeds (SQLSTATE 00000), warning 22023 reduces precision to 6.
    let rounded = [0, 100000, 120000, 123000, 123500, 123460, 123456, 123456];
    for sign in [1, -1] {
        for (p, expected) in rounded.into_iter().enumerate() {
            let sql = format!("SELECT CURRENT_TIMESTAMP({p}), LOCALTIMESTAMP({p})");
            assert_eq!(
                values(&mut s, &sql, &clock(sign * 123456, sign * 123456)),
                vec![tz(sign * expected), ts(sign * expected)]
            );
        }
        for (p, input, output) in [
            (0, 500000, 1000000),
            (1, 50000, 100000),
            (2, 5000, 10000),
            (3, 500, 1000),
            (4, 50, 100),
            (5, 5, 10),
        ] {
            assert_eq!(
                values(
                    &mut s,
                    &format!("SELECT CURRENT_TIMESTAMP({p}), LOCALTIMESTAMP({p})"),
                    &clock(sign * input, sign * input)
                ),
                vec![tz(sign * output), ts(sign * output)]
            );
        }
    }
    // PG parses .9999995 to a whole second before typmod; fixed clocks have microsecond resolution.
    for p in 0..=6 {
        assert_eq!(
            values(
                &mut s,
                &format!("SELECT CURRENT_TIMESTAMP({p}), LOCALTIMESTAMP({p})"),
                &clock(1000000, 1000000)
            ),
            vec![tz(1000000), ts(1000000)]
        );
    }
    for (p, expected) in rounded.into_iter().enumerate() {
        assert_eq!(
            values(
                &mut s,
                &format!("SELECT CURRENT_TIME({p}), LOCALTIME({p})"),
                &clock(123456, 123456)
            ),
            vec![
                Value::TimeTz(TimeTz::new(Time::from_microseconds(expected).unwrap(), 0).unwrap()),
                Value::Time(Time::from_microseconds(expected).unwrap())
            ]
        );
    }
    assert_eq!(
        values(
            &mut s,
            "SELECT CURRENT_TIME(0), LOCALTIME(0)",
            &clock(86399999999, 86399999999)
        ),
        vec![
            Value::TimeTz(TimeTz::new(Time::from_microseconds(86400000000).unwrap(), 0).unwrap()),
            Value::Time(Time::from_microseconds(86400000000).unwrap())
        ]
    );
}

#[test]
fn zones_project_local_calendar_and_preserve_absolute_instants() {
    let f = Fixture::new();
    let mut s = f.admin().sql_session();
    let settings = ParserSettings::default();
    let cases = [
        (
            "2000-01-01 00:00:00.123456+00",
            SessionZone::Fixed(0),
            "2000-01-01 00:00:00.123456",
            "2000-01-01",
            123456,
            0,
        ),
        // PostgreSQL SET TimeZone='+05:30' uses the POSIX sign convention.
        (
            "2000-01-01 00:00:00.123456+00",
            SessionZone::Fixed(-19800),
            "1999-12-31 18:30:00.123456",
            "1999-12-31",
            66600123456,
            -19800,
        ),
        (
            "2024-03-10 07:59:59.123456+00",
            SessionZone::named("America/Chicago").unwrap(),
            "2024-03-10 01:59:59.123456",
            "2024-03-10",
            7199123456,
            -21600,
        ),
        (
            "2024-03-10 08:00:00.123456+00",
            SessionZone::named("America/Chicago").unwrap(),
            "2024-03-10 03:00:00.123456",
            "2024-03-10",
            10800123456,
            -18000,
        ),
    ];
    for (instant, zone, local_ts, date, time, offset) in cases {
        let absolute = TimestampTz::parse(instant, &settings).unwrap();
        let mut c = clock(absolute.microseconds(), absolute.microseconds());
        c.zone = Some(zone);
        assert_eq!(
            values(
                &mut s,
                "SELECT CURRENT_TIMESTAMP, LOCALTIMESTAMP, CURRENT_DATE, CURRENT_TIME, LOCALTIME",
                &c
            ),
            vec![
                Value::TimestampTz(absolute),
                Value::Timestamp(Timestamp::parse(local_ts, &settings).unwrap()),
                Value::Date(Date::parse(date, &settings).unwrap()),
                Value::TimeTz(TimeTz::new(Time::from_microseconds(time).unwrap(), offset).unwrap()),
                Value::Time(Time::from_microseconds(time).unwrap())
            ]
        );
    }
}

#[test]
fn absent_clock_zone_and_wall_fail_closed_and_parser_errors_match_pg() {
    let f = Fixture::new();
    let mut s = f.admin().sql_session();
    let mut c = clock(1, 1);
    c.zone = None;
    assert_eq!(
        values(&mut s, "SELECT now(), CURRENT_TIMESTAMP", &c),
        vec![tz(1), tz(1)]
    );
    for sql in [
        "SELECT LOCALTIMESTAMP",
        "SELECT CURRENT_DATE",
        "SELECT CURRENT_TIME",
        "SELECT LOCALTIME",
        "SELECT clock_timestamp()",
    ] {
        sql_error(&mut s, sql, &c, SqlError::Unsupported);
    }
    for sql in ["SELECT now()", "SELECT CURRENT_DATE"] {
        assert!(matches!(
            s.execute(sql, &[], None),
            Err(CoreError::Sql(SqlError::Unsupported))
        ));
    }
    // PostgreSQL 18.6: empty/negative keyword precision is SQLSTATE 42601.
    for sql in [
        "SELECT CURRENT_TIMESTAMP()",
        "SELECT CURRENT_TIMESTAMP(-1)",
        "SELECT LOCALTIMESTAMP(-1)",
        "SELECT CURRENT_TIME(-1)",
        "SELECT LOCALTIME(-1)",
    ] {
        sql_error(&mut s, sql, &clock(1, 1), SqlError::Syntax);
    }
    // PostgreSQL 18.6: unsupported function arity is SQLSTATE 42883.
    sql_error(
        &mut s,
        "SELECT now(1)",
        &clock(1, 1),
        SqlError::UndefinedFunction,
    );
    // Pins the AdminCommand clock follow-up: serialized admin SQL has no statement clock yet.
    // Unsupported maps to PostgreSQL 0A000 at the wire boundary.
    let input = serde_json::to_vec(&local::AdminCommand::Sql {
        sql: "SELECT now()".into(),
        params: vec![],
        expected_version: None,
    })
    .unwrap();
    let reply = local::admin(f.admin(), &input);
    assert_eq!(reply.code, 200);
    assert_eq!(reply.data["error"], "Sql(Unsupported)");
}

#[test]
fn unavailable_clock_only_fails_evaluated_time_functions() {
    let f = Fixture::new();
    let mut s = f.admin().sql_session();
    let mut c = clock(i64::MAX, i64::MIN);
    c.unavailable = Some(CoreError::Policy(Error::UncertainTime));
    c.zone = None;
    let calls = Arc::new(AtomicI64::new(0));
    let source = calls.clone();
    c.wall = Some(Arc::new(move || {
        source.fetch_add(1, Ordering::SeqCst);
        Err(CoreError::Storage(
            "wall must not override unavailable".into(),
        ))
    }));
    for sql in [
        "SELECT now()",
        "SELECT transaction_timestamp()",
        "SELECT statement_timestamp()",
        "SELECT clock_timestamp()",
        "SELECT CURRENT_TIMESTAMP",
        "SELECT LOCALTIMESTAMP",
        "SELECT CURRENT_DATE",
        "SELECT CURRENT_TIME",
        "SELECT LOCALTIME",
    ] {
        assert!(
            matches!(
                s.execute_at(sql, &[], None, &c),
                Err(CoreError::Policy(Error::UncertainTime))
            ),
            "{sql}"
        );
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(values(&mut s, "SELECT 1", &c), vec![Value::Int(1)]);
    s.execute_at(
        "CREATE TABLE unavailable_clock (id BIGINT PRIMARY KEY)",
        &[],
        None,
        &c,
    )
    .unwrap();
    s.execute_at("INSERT INTO unavailable_clock VALUES (1)", &[], None, &c)
        .unwrap();
    assert_eq!(
        values(&mut s, "SELECT id FROM unavailable_clock", &c),
        vec![Value::Int(1)]
    );
}

#[test]
fn unavailable_time_free_statements_preserve_or_leave_unset_transaction_pins() {
    let f = Fixture::new();
    let mut s = f.admin().sql_session();
    let mut unavailable = clock(i64::MAX, i64::MIN);
    unavailable.unavailable = Some(CoreError::Policy(Error::UncertainTime));
    for pin_first in [false, true] {
        s.execute("BEGIN", &[], None).unwrap();
        if pin_first {
            assert_eq!(
                values(&mut s, "SELECT now()", &clock(100, 100)),
                vec![tz(100)]
            );
        }
        assert_eq!(
            values(&mut s, "SELECT 1", &unavailable),
            vec![Value::Int(1)]
        );
        assert_eq!(
            values(&mut s, "SELECT now()", &clock(100, 200)),
            vec![tz(100)]
        );
        assert!(matches!(
            s.execute_at("SELECT now()", &[], None, &unavailable),
            Err(CoreError::Policy(Error::UncertainTime))
        ));
        assert!(s.is_aborted());
        s.execute("ROLLBACK", &[], None).unwrap();
    }
}

#[test]
fn wall_clock_is_called_per_invocation_and_preserves_core_errors() {
    let f = Fixture::new();
    let mut s = f.admin().sql_session();
    let calls = Arc::new(AtomicI64::new(30));
    let source = calls.clone();
    let mut c = clock(10, 20);
    c.wall = Some(Arc::new(move || Ok(source.fetch_add(1, Ordering::SeqCst))));
    assert_eq!(
        values(
            &mut s,
            "SELECT clock_timestamp(), clock_timestamp(), now(), statement_timestamp()",
            &c
        ),
        vec![tz(30), tz(31), tz(10), tz(20)]
    );
    assert_eq!(calls.load(Ordering::SeqCst), 32);
    // Giggles-specific policy refusal: PostgreSQL now() does not fail with UncertainTime.
    c.wall = Some(Arc::new(|| Err(CoreError::Policy(Error::UncertainTime))));
    assert!(matches!(
        s.execute_at("SELECT clock_timestamp()", &[], None, &c),
        Err(CoreError::Policy(Error::UncertainTime))
    ));
    assert_eq!(values(&mut s, "SELECT now()", &c), vec![tz(10)]);
    c.wall = Some(Arc::new(|| {
        Err(CoreError::Storage("clock sentinel".into()))
    }));
    assert!(
        matches!(s.execute_at("SELECT clock_timestamp()", &[], None, &c), Err(CoreError::Storage(message)) if message == "clock sentinel")
    );
}
#[test]
fn clocked_mutations_share_clock_in_values_assignments_and_returning() {
    let f = Fixture::new();
    let mut s = f.admin().sql_session();
    s.execute("CREATE TABLE clock_values (id BIGINT PRIMARY KEY, created TIMESTAMPTZ, changed TIMESTAMPTZ)", &[], None).unwrap();
    assert_eq!(
        values(
            &mut s,
            "INSERT INTO clock_values VALUES (1, now(), statement_timestamp()) RETURNING created, changed, CURRENT_TIMESTAMP, statement_timestamp()",
            &clock(100, 200)
        ),
        vec![tz(100), tz(200), tz(100), tz(200)]
    );
    assert_eq!(
        values(
            &mut s,
            "UPDATE clock_values SET changed = statement_timestamp() WHERE id = 1 RETURNING created, changed, now(), statement_timestamp()",
            &clock(300, 400)
        ),
        vec![tz(100), tz(400), tz(300), tz(400)]
    );
    assert_eq!(
        values(
            &mut s,
            "SELECT created, changed FROM clock_values",
            &clock(500, 600)
        ),
        vec![tz(100), tz(400)]
    );
}

#[test]
fn wall_clock_is_volatile_per_row_and_failure_leaves_no_mutation() {
    let f = Fixture::new();
    let mut s = f.admin().sql_session();
    s.execute(
        "CREATE TABLE clock_rows (id BIGINT PRIMARY KEY, observed TIMESTAMPTZ)",
        &[],
        None,
    )
    .unwrap();
    s.execute(
        "INSERT INTO clock_rows VALUES (1, NULL), (2, NULL)",
        &[],
        None,
    )
    .unwrap();
    let calls = Arc::new(AtomicI64::new(100));
    let source = calls.clone();
    let mut c = clock(10, 20);
    c.wall = Some(Arc::new(move || Ok(source.fetch_add(1, Ordering::SeqCst))));
    let rows = s
        .execute_at(
            "SELECT clock_timestamp() FROM clock_rows ORDER BY id",
            &[],
            None,
            &c,
        )
        .unwrap()
        .rows;
    assert_eq!(
        rows.iter().map(|r| r.values.clone()).collect::<Vec<_>>(),
        vec![vec![tz(100)], vec![tz(101)]]
    );
    assert_eq!(calls.load(Ordering::SeqCst), 102);
    c.wall = Some(Arc::new(|| {
        Err(CoreError::Storage("wall unavailable".into()))
    }));
    assert!(
        matches!(s.execute_at("INSERT INTO clock_rows VALUES (3, clock_timestamp())", &[], None, &c), Err(CoreError::Storage(message)) if message == "wall unavailable")
    );
    assert!(
        matches!(s.execute_at("UPDATE clock_rows SET observed = clock_timestamp() WHERE id = 1", &[], None, &c), Err(CoreError::Storage(message)) if message == "wall unavailable")
    );
    let rows = s
        .execute("SELECT id, observed FROM clock_rows ORDER BY id", &[], None)
        .unwrap()
        .rows;
    assert_eq!(
        rows.iter().map(|r| r.values.clone()).collect::<Vec<_>>(),
        vec![
            vec![Value::Int(1), Value::Null],
            vec![Value::Int(2), Value::Null]
        ]
    );
}

#[test]
fn clocked_begin_pins_transaction_before_prepared_execution() {
    let f = Fixture::new();
    let mut s = f.admin().sql_session();
    s.execute_at("BEGIN", &[], None, &clock(100, 100)).unwrap();
    let p = s.prepare("SELECT now(), statement_timestamp()").unwrap();
    assert_eq!(
        s.execute_prepared_at(&p, &[], None, &clock(100, 200))
            .unwrap()
            .rows[0]
            .values,
        vec![tz(100), tz(200)]
    );
    assert!(matches!(
        s.execute_prepared_at(&p, &[], None, &clock(101, 300)),
        Err(CoreError::Sql(SqlError::Type))
    ));
    s.execute("ROLLBACK", &[], None).unwrap();
}

#[test]
fn commit_clears_the_transaction_pin_for_a_later_unclocked_begin() {
    let f = Fixture::new();
    let mut s = f.admin().sql_session();
    s.execute_at("BEGIN", &[], None, &clock(100, 100)).unwrap();
    assert_eq!(values(&mut s, "SELECT now()", &clock(100, 150)), vec![tz(100)]);
    s.execute_at("COMMIT", &[], None, &clock(100, 160)).unwrap();
    // A BEGIN through the unclocked entry point must not inherit the committed pin.
    s.execute("BEGIN", &[], None).unwrap();
    assert_eq!(values(&mut s, "SELECT now()", &clock(200, 200)), vec![tz(200)]);
    assert!(matches!(
        s.execute_at("SELECT now()", &[], None, &clock(201, 300)),
        Err(CoreError::Sql(SqlError::Type))
    ));
    s.execute("ROLLBACK", &[], None).unwrap();
}

#[test]
fn unavailable_begin_does_not_pin_a_placeholder_transaction_time() {
    let f = Fixture::new();
    let mut s = f.admin().sql_session();
    let mut down = clock(0, 0);
    down.unavailable = Some(CoreError::Policy(Error::UncertainTime));
    s.execute_at("BEGIN", &[], None, &down).unwrap();
    // The first available statement pins its own transaction time, not the placeholder 0.
    assert_eq!(values(&mut s, "SELECT now()", &clock(500, 500)), vec![tz(500)]);
    assert_eq!(values(&mut s, "SELECT now()", &clock(500, 600)), vec![tz(500)]);
    s.execute("ROLLBACK", &[], None).unwrap();
}
