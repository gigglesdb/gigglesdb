// Expectations captured from PostgreSQL 18.6 in dd68-f16/probe/pg18.txt.
use giggles_core::{Admin, CoreError, Limits, SqlSession, relational::*};
use giggles_pg_types::temporal::SessionZone;
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

struct Fixture {
    admin: Option<Admin>,
    path: PathBuf,
}
impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "dd68-temporal-arithmetic-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        Self {
            admin: Some(Admin::create(&path, Limits::default()).unwrap()),
            path,
        }
    }
    fn session(&self) -> SqlSession {
        self.admin.as_ref().unwrap().sql_session()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        drop(self.admin.take());
        std::fs::remove_dir_all(&self.path).unwrap();
    }
}
fn clock(zone: &str) -> StatementClock {
    StatementClock {
        unavailable: None,
        transaction_us: 757_382_400_000_000,
        statement_us: 757_382_400_000_000,
        zone: Some(if zone == "UTC" {
            SessionZone::Fixed(0)
        } else {
            SessionZone::named(zone).unwrap()
        }),
        wall: None,
    }
}
fn scalar(s: &mut SqlSession, c: &StatementClock, sql: &str, ty: DataType, expected: Option<&str>) {
    let p = s.prepare(sql).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    assert_eq!(s.describe(&p).unwrap().columns[0].data_type, ty, "{sql}");
    let sql = format!("SELECT ({})::text", sql.strip_prefix("SELECT ").unwrap());
    let actual = s
        .execute_at(&sql, &[], None, c)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    assert_eq!(
        actual.rows[0].values,
        vec![expected.map_or(Value::Null, |v| Value::Text(v.into()))],
        "{sql}"
    );
}

#[test]
fn temporal_scalar_results_and_types_match_pg18() {
    let f = Fixture::new();
    let mut s = f.session();
    let c = clock("UTC");
    for (sql, ty, expected) in [
        (
            "SELECT DATE '2024-01-31' + 1",
            DataType::Date,
            Some("2024-02-01"),
        ),
        (
            "SELECT 1 + DATE '2024-02-28'",
            DataType::Date,
            Some("2024-02-29"),
        ),
        (
            "SELECT DATE '2024-03-01' - 1",
            DataType::Date,
            Some("2024-02-29"),
        ),
        (
            "SELECT DATE '2024-03-01' + -1",
            DataType::Date,
            Some("2024-02-29"),
        ),
        (
            "SELECT DATE '2024-01-01' + 1::int2",
            DataType::Date,
            Some("2024-01-02"),
        ),
        (
            "SELECT DATE '2024-03-01' - DATE '2024-01-01'",
            DataType::Integer,
            Some("60"),
        ),
        (
            "SELECT DATE '2024-01-01' - DATE '2024-03-01'",
            DataType::Integer,
            Some("-60"),
        ),
        (
            "SELECT DATE '2024-01-31' + INTERVAL '1 month'",
            DataType::Timestamp,
            Some("2024-02-29 00:00:00"),
        ),
        (
            "SELECT DATE '2024-03-31' - INTERVAL '1 month'",
            DataType::Timestamp,
            Some("2024-02-29 00:00:00"),
        ),
        (
            "SELECT DATE '2024-01-01' + INTERVAL '1 day 2 hours'",
            DataType::Timestamp,
            Some("2024-01-02 02:00:00"),
        ),
        (
            "SELECT INTERVAL '1 year' + DATE '2024-02-29'",
            DataType::Timestamp,
            Some("2025-02-28 00:00:00"),
        ),
        (
            "SELECT DATE '2024-01-01' + TIME '10:30'",
            DataType::Timestamp,
            Some("2024-01-01 10:30:00"),
        ),
        (
            "SELECT TIME '10:30' + DATE '2024-01-01'",
            DataType::Timestamp,
            Some("2024-01-01 10:30:00"),
        ),
        (
            "SELECT DATE '2024-12-31' + TIME '24:00'",
            DataType::Timestamp,
            Some("2025-01-01 00:00:00"),
        ),
        (
            "SELECT DATE '2024-01-01' - TIME '10:30'",
            DataType::Timestamp,
            Some("2023-12-31 13:30:00"),
        ),
        (
            "SELECT DATE '2024-01-01' + TIMETZ '10:00+02'",
            DataType::TimestampTz,
            Some("2024-01-01 08:00:00+00"),
        ),
        (
            "SELECT DATE '2024-01-02' - TIMESTAMP '2024-01-01 12:00'",
            DataType::Interval,
            Some("12:00:00"),
        ),
        (
            "SELECT DATE 'infinity' + 1",
            DataType::Date,
            Some("infinity"),
        ),
        (
            "SELECT DATE '-infinity' - 1",
            DataType::Date,
            Some("-infinity"),
        ),
        (
            "SELECT DATE '-infinity' + INTERVAL '1 day'",
            DataType::Timestamp,
            Some("-infinity"),
        ),
        (
            "SELECT DATE '2024-01-01' + INTERVAL 'infinity'",
            DataType::Timestamp,
            Some("infinity"),
        ),
        (
            "SELECT DATE 'infinity' + TIME '10:00'",
            DataType::Timestamp,
            Some("infinity"),
        ),
        (
            "SELECT DATE '5874897-12-31' - DATE '4714-11-24 BC'",
            DataType::Integer,
            Some("2147483493"),
        ),
        (
            "SELECT TIMESTAMP '2024-01-31 10:00' + INTERVAL '1 month'",
            DataType::Timestamp,
            Some("2024-02-29 10:00:00"),
        ),
        (
            "SELECT TIMESTAMP '2024-03-31 10:00' - INTERVAL '1 month 2 days 03:04:05.678'",
            DataType::Timestamp,
            Some("2024-02-27 06:55:54.322"),
        ),
        (
            "SELECT TIMESTAMP '2024-03-10 01:30' + INTERVAL '1 hour'",
            DataType::Timestamp,
            Some("2024-03-10 02:30:00"),
        ),
        (
            "SELECT INTERVAL '1 day' + TIMESTAMP '2024-01-01'",
            DataType::Timestamp,
            Some("2024-01-02 00:00:00"),
        ),
        (
            "SELECT TIMESTAMP '2024-02-29' + INTERVAL '1 year'",
            DataType::Timestamp,
            Some("2025-02-28 00:00:00"),
        ),
        (
            "SELECT TIMESTAMP '2024-03-31' + INTERVAL '-1 month'",
            DataType::Timestamp,
            Some("2024-02-29 00:00:00"),
        ),
        (
            "SELECT TIMESTAMP '2024-03-01' - TIMESTAMP '2024-01-01 12:00'",
            DataType::Interval,
            Some("59 days 12:00:00"),
        ),
        (
            "SELECT TIMESTAMP '2024-01-01' - TIMESTAMP '2024-01-03 01:02:03.5'",
            DataType::Interval,
            Some("-2 days -01:02:03.5"),
        ),
        (
            "SELECT TIMESTAMP '2024-01-01 00:00:00.000001' - TIMESTAMP '2024-01-01'",
            DataType::Interval,
            Some("00:00:00.000001"),
        ),
        (
            "SELECT TIMESTAMP '2024-01-02 03:00' - TIMESTAMP '2024-01-01 02:00'",
            DataType::Interval,
            Some("1 day 01:00:00"),
        ),
        (
            "SELECT TIMESTAMP '2024-01-02 01:00' - TIMESTAMP '2024-01-01 23:00'",
            DataType::Interval,
            Some("02:00:00"),
        ),
        (
            "SELECT TIMESTAMP 'infinity' + INTERVAL '1 day'",
            DataType::Timestamp,
            Some("infinity"),
        ),
        (
            "SELECT TIMESTAMP 'infinity' + INTERVAL 'infinity'",
            DataType::Timestamp,
            Some("infinity"),
        ),
        (
            "SELECT TIMESTAMP '2024-01-01' + INTERVAL '-infinity'",
            DataType::Timestamp,
            Some("-infinity"),
        ),
        (
            "SELECT TIMESTAMP 'infinity' - TIMESTAMP '2024-01-01'",
            DataType::Interval,
            Some("infinity"),
        ),
        (
            "SELECT TIMESTAMP '2024-01-01' - TIMESTAMP 'infinity'",
            DataType::Interval,
            Some("-infinity"),
        ),
        (
            "SELECT TIMESTAMP '-infinity' - TIMESTAMP 'infinity'",
            DataType::Interval,
            Some("-infinity"),
        ),
        (
            "SELECT TIMESTAMPTZ '2024-03-09 12:00-06' + INTERVAL '1 day'",
            DataType::TimestampTz,
            Some("2024-03-10 18:00:00+00"),
        ),
        (
            "SELECT TIMESTAMPTZ '2024-03-09 12:00-06' + INTERVAL '24 hours'",
            DataType::TimestampTz,
            Some("2024-03-10 18:00:00+00"),
        ),
        (
            "SELECT TIMESTAMPTZ '2024-11-02 12:00-05' + INTERVAL '1 day'",
            DataType::TimestampTz,
            Some("2024-11-03 17:00:00+00"),
        ),
        (
            "SELECT TIMESTAMPTZ '2024-03-11 02:30-05' - INTERVAL '1 day'",
            DataType::TimestampTz,
            Some("2024-03-10 07:30:00+00"),
        ),
        (
            "SELECT TIMESTAMPTZ '2024-03-10 01:30-06' + INTERVAL '1 hour'",
            DataType::TimestampTz,
            Some("2024-03-10 08:30:00+00"),
        ),
        (
            "SELECT TIMESTAMPTZ '2024-01-31 12:00+00' + INTERVAL '1 month'",
            DataType::TimestampTz,
            Some("2024-02-29 12:00:00+00"),
        ),
        (
            "SELECT TIMESTAMPTZ '2024-01-31 20:00+00' + INTERVAL '1 month'",
            DataType::TimestampTz,
            Some("2024-02-29 20:00:00+00"),
        ),
        (
            "SELECT TIMESTAMPTZ '2024-03-31 03:00+00' - INTERVAL '1 month'",
            DataType::TimestampTz,
            Some("2024-02-29 03:00:00+00"),
        ),
        (
            "SELECT INTERVAL '1 day 1 hour' + TIMESTAMPTZ '2024-03-09 12:00-06'",
            DataType::TimestampTz,
            Some("2024-03-10 19:00:00+00"),
        ),
        (
            "SELECT TIMESTAMPTZ '2024-02-29 23:30+00' + INTERVAL '1 year 1 day 1 hour'",
            DataType::TimestampTz,
            Some("2025-03-02 00:30:00+00"),
        ),
        (
            "SELECT TIMESTAMPTZ '2024-03-11 00:00-05' - TIMESTAMPTZ '2024-03-10 00:00-06'",
            DataType::Interval,
            Some("23:00:00"),
        ),
        (
            "SELECT TIMESTAMPTZ '2025-01-01 00:00+00' - TIMESTAMPTZ '2024-01-01 00:00+00'",
            DataType::Interval,
            Some("366 days"),
        ),
        (
            "SELECT TIMESTAMPTZ '2024-01-01 00:00+00' - TIMESTAMPTZ '2024-01-02 01:00:00.25+00'",
            DataType::Interval,
            Some("-1 days -01:00:00.25"),
        ),
        (
            "SELECT TIMESTAMPTZ 'infinity' + INTERVAL '1 month'",
            DataType::TimestampTz,
            Some("infinity"),
        ),
        (
            "SELECT TIMESTAMPTZ 'infinity' - TIMESTAMPTZ '2024-01-01 00:00+00'",
            DataType::Interval,
            Some("infinity"),
        ),
        (
            "SELECT TIMESTAMPTZ '2024-01-02 00:00+00' - TIMESTAMP '2024-01-01'",
            DataType::Interval,
            Some("1 day"),
        ),
        (
            "SELECT TIMESTAMP '2024-01-02' - TIMESTAMPTZ '2024-01-01 00:00+00'",
            DataType::Interval,
            Some("1 day"),
        ),
        (
            "SELECT TIMESTAMPTZ '2024-01-02 00:00+00' - DATE '2024-01-01'",
            DataType::Interval,
            Some("1 day"),
        ),
        (
            "SELECT TIMESTAMP '2024-01-01' + INTERVAL '1 day' = TIMESTAMPTZ '2024-01-02 00:00+00'",
            DataType::Boolean,
            Some("true"),
        ),
        (
            "SELECT DATE '2024-03-09' + INTERVAL '1 day'",
            DataType::Timestamp,
            Some("2024-03-10 00:00:00"),
        ),
        (
            "SELECT INTERVAL '1 month' + INTERVAL '2 days 03:00'",
            DataType::Interval,
            Some("1 mon 2 days 03:00:00"),
        ),
        (
            "SELECT INTERVAL '1 day' - INTERVAL '25 hours'",
            DataType::Interval,
            Some("1 day -25:00:00"),
        ),
        (
            "SELECT - INTERVAL '1 year 2 days 03:00'",
            DataType::Interval,
            Some("-1 years -2 days -03:00:00"),
        ),
        (
            "SELECT -(-INTERVAL '1 hour')",
            DataType::Interval,
            Some("01:00:00"),
        ),
        (
            "SELECT INTERVAL '1 month' * 1.5",
            DataType::Interval,
            Some("1 mon 15 days"),
        ),
        (
            "SELECT INTERVAL '1 day' * 0.5",
            DataType::Interval,
            Some("12:00:00"),
        ),
        (
            "SELECT INTERVAL '1 hour' * 3",
            DataType::Interval,
            Some("03:00:00"),
        ),
        (
            "SELECT 2 * INTERVAL '1 month 1 day'",
            DataType::Interval,
            Some("2 mons 2 days"),
        ),
        (
            "SELECT 2.5::float8 * INTERVAL '1 month 1 day 1 hour'",
            DataType::Interval,
            Some("2 mons 17 days 14:30:00"),
        ),
        (
            "SELECT INTERVAL '1 month' * 0.3333333",
            DataType::Interval,
            Some("9 days 23:59:59.9136"),
        ),
        (
            "SELECT INTERVAL '1 mon 2 days 03:00' * -1.5",
            DataType::Interval,
            Some("-1 mons -18 days -04:30:00"),
        ),
        (
            "SELECT INTERVAL '1 month' / 3",
            DataType::Interval,
            Some("10 days"),
        ),
        (
            "SELECT INTERVAL '1 day' / 7",
            DataType::Interval,
            Some("03:25:42.857143"),
        ),
        (
            "SELECT INTERVAL '1 year 1 day' / 2.5::float8",
            DataType::Interval,
            Some("4 mons 24 days 09:36:00"),
        ),
        (
            "SELECT INTERVAL '1 day' * 'infinity'::float8",
            DataType::Interval,
            Some("infinity"),
        ),
        (
            "SELECT INTERVAL '-1 day' * 'infinity'::float8",
            DataType::Interval,
            Some("-infinity"),
        ),
        (
            "SELECT INTERVAL 'infinity' * 2",
            DataType::Interval,
            Some("infinity"),
        ),
        (
            "SELECT INTERVAL 'infinity' * -1",
            DataType::Interval,
            Some("-infinity"),
        ),
        (
            "SELECT INTERVAL 'infinity' / 2",
            DataType::Interval,
            Some("infinity"),
        ),
        (
            "SELECT INTERVAL '1 day' / 'infinity'::float8",
            DataType::Interval,
            Some("00:00:00"),
        ),
        (
            "SELECT INTERVAL '1 day' * 1.5::numeric",
            DataType::Interval,
            Some("1 day 12:00:00"),
        ),
        (
            "SELECT INTERVAL '1 day' * 2::int8",
            DataType::Interval,
            Some("2 days"),
        ),
        (
            "SELECT INTERVAL '1 day' * '2'",
            DataType::Interval,
            Some("2 days"),
        ),
        (
            "SELECT INTERVAL 'infinity' + INTERVAL '1 day'",
            DataType::Interval,
            Some("infinity"),
        ),
        (
            "SELECT INTERVAL '-infinity' - INTERVAL 'infinity'",
            DataType::Interval,
            Some("-infinity"),
        ),
        (
            "SELECT - INTERVAL 'infinity'",
            DataType::Interval,
            Some("-infinity"),
        ),
        (
            "SELECT INTERVAL '1 day 1 second' * 0.000001",
            DataType::Interval,
            Some("00:00:00.086401"),
        ),
        (
            "SELECT INTERVAL '1 month 1 day' * 1.75",
            DataType::Interval,
            Some("1 mon 24 days 06:00:00"),
        ),
        (
            "SELECT INTERVAL '1 second' / 3",
            DataType::Interval,
            Some("00:00:00.333333"),
        ),
        (
            "SELECT INTERVAL '1 mon' / -3",
            DataType::Interval,
            Some("-10 days"),
        ),
        (
            "SELECT TIME '23:00' + INTERVAL '2 hours'",
            DataType::Time,
            Some("01:00:00"),
        ),
        (
            "SELECT TIME '01:00' - INTERVAL '2 hours'",
            DataType::Time,
            Some("23:00:00"),
        ),
        (
            "SELECT TIME '10:00' + INTERVAL '1 month 1 day 1 hour'",
            DataType::Time,
            Some("11:00:00"),
        ),
        (
            "SELECT INTERVAL '1 hour' + TIME '10:00'",
            DataType::Time,
            Some("11:00:00"),
        ),
        (
            "SELECT TIME '10:00' - TIME '12:30'",
            DataType::Interval,
            Some("-02:30:00"),
        ),
        (
            "SELECT TIME '24:00' - TIME '00:00'",
            DataType::Interval,
            Some("24:00:00"),
        ),
        (
            "SELECT TIME '10:00' + INTERVAL '49 hours 30 minutes'",
            DataType::Time,
            Some("11:30:00"),
        ),
        (
            "SELECT TIME '10:00' - INTERVAL '-49 hours'",
            DataType::Time,
            Some("11:00:00"),
        ),
        (
            "SELECT TIME '00:00' - INTERVAL '0.000001 sec'",
            DataType::Time,
            Some("23:59:59.999999"),
        ),
        (
            "SELECT INTERVAL '1 hour' - TIME '10:00'",
            DataType::Interval,
            Some("-09:00:00"),
        ),
        (
            "SELECT TIMETZ '23:00+02' + INTERVAL '2 hours'",
            DataType::TimeTz,
            Some("01:00:00+02"),
        ),
        (
            "SELECT TIMETZ '01:00-05' - INTERVAL '2 hours 30 minutes'",
            DataType::TimeTz,
            Some("22:30:00-05"),
        ),
        (
            "SELECT INTERVAL '1 day 1 hour' + TIMETZ '10:00+05:30'",
            DataType::TimeTz,
            Some("11:00:00+05:30"),
        ),
        (
            "SELECT TIMETZ '10:00+00' + TIME '01:00'",
            DataType::TimeTz,
            Some("11:00:00+00"),
        ),
        (
            "SELECT TIME '10:00' + TIMETZ '01:00+00'",
            DataType::TimeTz,
            Some("11:00:00+00"),
        ),
        (
            "SELECT DATE '2024-01-01' + 1 = DATE '2024-01-02'",
            DataType::Boolean,
            Some("true"),
        ),
        (
            "SELECT (DATE '2024-01-31' + INTERVAL '1 month') - TIMESTAMP '2024-01-01'",
            DataType::Interval,
            Some("59 days"),
        ),
        (
            "SELECT DATE '2024-01-01' + 1 + 1",
            DataType::Date,
            Some("2024-01-03"),
        ),
        (
            "SELECT INTERVAL '1 day' * 2 / 4 + INTERVAL '1 hour'",
            DataType::Interval,
            Some("13:00:00"),
        ),
        (
            "SELECT (DATE '2024-01-01' + 1)::text",
            DataType::Text,
            Some("2024-01-02"),
        ),
        (
            "SELECT DATE '2024-03-01' - DATE '2024-01-01' + 1",
            DataType::Integer,
            Some("61"),
        ),
        (
            "SELECT 'x' || (TIMESTAMP '2024-01-01' - TIMESTAMP '2023-12-31')",
            DataType::Text,
            Some("x1 day"),
        ),
        (
            "SELECT '2024-01-01'::date + 1",
            DataType::Date,
            Some("2024-01-02"),
        ),
        ("SELECT NULL::date + 1", DataType::Date, None),
        ("SELECT NULL + INTERVAL '1 day'", DataType::Interval, None),
        (
            "SELECT TIMESTAMPTZ '2024-01-01 00:00+00' - NULL",
            DataType::Interval,
            None,
        ),
        (
            "SELECT justify_hours(TIMESTAMP '2024-01-03' - TIMESTAMP '2024-01-01 12:00')",
            DataType::Interval,
            Some("1 day 12:00:00"),
        ),
        (
            "SELECT extract(epoch from TIMESTAMP '2024-01-02' - TIMESTAMP '2024-01-01')",
            DataType::Numeric,
            Some("86400.000000"),
        ),
        (
            "SELECT CASE WHEN true THEN DATE '2024-01-01' + 1 END",
            DataType::Date,
            Some("2024-01-02"),
        ),
        (
            "SELECT coalesce(NULL::date, DATE '2024-01-01') + 7",
            DataType::Date,
            Some("2024-01-08"),
        ),
        (
            "SELECT TIME '10:00' + '1 hour'",
            DataType::Time,
            Some("11:00:00"),
        ),
        (
            "SELECT TIMESTAMP '2024-01-01' + '1 day'",
            DataType::Timestamp,
            Some("2024-01-02 00:00:00"),
        ),
        (
            "SELECT INTERVAL '1 day' / '2'",
            DataType::Interval,
            Some("12:00:00"),
        ),
        (
            "SELECT TIMESTAMPTZ '2024-03-09 12:00-06' + '1 day'",
            DataType::TimestampTz,
            Some("2024-03-10 18:00:00+00"),
        ),
        (
            "SELECT DATE '2024-01-01' - '2023-12-01'",
            DataType::Integer,
            Some("31"),
        ),
        (
            "SELECT '2' * TIME '01:00'",
            DataType::Interval,
            Some("02:00:00"),
        ),
        (
            "SELECT TIMESTAMPTZ '2024-03-11 00:00-05' - '2024-03-10 00:00'",
            DataType::Interval,
            Some("1 day 05:00:00"),
        ),
        (
            "SELECT - TIME '10:00'",
            DataType::Interval,
            Some("-10:00:00"),
        ),
        (
            "SELECT INTERVAL '-infinity' + INTERVAL '-infinity'",
            DataType::Interval,
            Some("-infinity"),
        ),
        (
            "SELECT TIME '10:00' - TIME '10:00' * 2",
            DataType::Time,
            Some("14:00:00"),
        ),
        (
            "SELECT TIME '01:30' * 2",
            DataType::Interval,
            Some("03:00:00"),
        ),
        (
            "SELECT TIME '01:30' / 2",
            DataType::Interval,
            Some("00:45:00"),
        ),
        (
            "SELECT TIMESTAMPTZ '2024-03-10 12:00+00' - TIMESTAMP '2024-03-10 00:00'",
            DataType::Interval,
            Some("12:00:00"),
        ),
        (
            "SELECT DATE '2024-03-11' - TIMESTAMPTZ '2024-03-10 00:00+00'",
            DataType::Interval,
            Some("1 day"),
        ),
        (
            "SELECT TIMESTAMP '2024-01-01 23:00' + TIME '02:00'",
            DataType::Timestamp,
            Some("2024-01-02 01:00:00"),
        ),
        (
            "SELECT TIMESTAMPTZ '2024-03-10 12:00+00' - TIME '13:00'",
            DataType::TimestampTz,
            Some("2024-03-09 23:00:00+00"),
        ),
        (
            "SELECT DATE '2024-07-01' + TIMETZ '10:00-07'",
            DataType::TimestampTz,
            Some("2024-07-01 17:00:00+00"),
        ),
        (
            "SELECT DATE '294276-12-31' + TIME '23:59:59.999999'",
            DataType::Timestamp,
            Some("294276-12-31 23:59:59.999999"),
        ),
        (
            "SELECT DATE '-infinity' + TIMETZ '10:00+00'",
            DataType::TimestampTz,
            Some("-infinity"),
        ),
    ] {
        scalar(&mut s, &c, sql, ty, expected);
    }
}

#[test]
fn temporal_errors_match_pg18() {
    let f = Fixture::new();
    let mut s = f.session();
    let c = clock("UTC");
    for (sql, error) in [
        (
            "SELECT DATE '2024-01-01' + 1::int8",
            SqlError::UndefinedFunction,
        ),
        (
            "SELECT DATE '2024-01-01' + 1.5",
            SqlError::UndefinedFunction,
        ),
        (
            "SELECT DATE '2024-01-01' + '1'",
            SqlError::AmbiguousFunction,
        ),
        (
            "SELECT DATE '2024-01-01' + DATE '2024-01-02'",
            SqlError::UndefinedFunction,
        ),
        ("SELECT DATE '2024-01-01' * 2", SqlError::UndefinedFunction),
        (
            "SELECT INTERVAL '1 day' - DATE '2024-01-01'",
            SqlError::UndefinedFunction,
        ),
        (
            "SELECT DATE '2024-01-01' + TIMESTAMP '2024-01-01'",
            SqlError::UndefinedFunction,
        ),
        (
            "SELECT DATE 'infinity' - DATE 'infinity'",
            SqlError::DatetimeFieldOverflow,
        ),
        (
            "SELECT DATE 'infinity' - DATE '2024-01-01'",
            SqlError::DatetimeFieldOverflow,
        ),
        (
            "SELECT DATE '2024-01-01' - DATE 'infinity'",
            SqlError::DatetimeFieldOverflow,
        ),
        (
            "SELECT DATE 'infinity' + INTERVAL '-infinity'",
            SqlError::DatetimeFieldOverflow,
        ),
        (
            "SELECT DATE '5874897-12-31' + 1",
            SqlError::DatetimeFieldOverflow,
        ),
        (
            "SELECT DATE '4714-11-24 BC' - 1",
            SqlError::DatetimeFieldOverflow,
        ),
        (
            "SELECT DATE '2024-01-01' + 2147483647",
            SqlError::DatetimeFieldOverflow,
        ),
        (
            "SELECT DATE '2024-01-01' - (-2147483647)",
            SqlError::DatetimeFieldOverflow,
        ),
        (
            "SELECT DATE '2024-01-01' + INTERVAL '300000 years'",
            SqlError::DatetimeFieldOverflow,
        ),
        (
            "SELECT TIMESTAMP '2024-01-01' + TIMESTAMP '2024-01-01'",
            SqlError::UndefinedFunction,
        ),
        (
            "SELECT TIMESTAMP '2024-01-01' * 2",
            SqlError::UndefinedFunction,
        ),
        (
            "SELECT TIMESTAMP '2024-01-01' + 1",
            SqlError::UndefinedFunction,
        ),
        (
            "SELECT TIMESTAMP 'infinity' - INTERVAL 'infinity'",
            SqlError::DatetimeFieldOverflow,
        ),
        (
            "SELECT TIMESTAMP 'infinity' - TIMESTAMP 'infinity'",
            SqlError::DatetimeFieldOverflow,
        ),
        (
            "SELECT TIMESTAMP '294276-12-31 23:59:59' + INTERVAL '1 day'",
            SqlError::DatetimeFieldOverflow,
        ),
        (
            "SELECT TIMESTAMP '294276-12-31' - TIMESTAMP '4714-11-24 00:00 BC'",
            SqlError::DatetimeFieldOverflow,
        ),
        (
            "SELECT TIMESTAMP '2024-01-01' + INTERVAL '178956970 years'",
            SqlError::DatetimeFieldOverflow,
        ),
        (
            "SELECT TIMESTAMPTZ '2024-01-01 00:00+00' + TIMESTAMPTZ '2024-01-01 00:00+00'",
            SqlError::UndefinedFunction,
        ),
        (
            "SELECT TIMESTAMPTZ 'infinity' - TIMESTAMPTZ 'infinity'",
            SqlError::DatetimeFieldOverflow,
        ),
        (
            "SELECT TIMESTAMPTZ '-infinity' + INTERVAL 'infinity'",
            SqlError::DatetimeFieldOverflow,
        ),
        (
            "SELECT TIMESTAMPTZ '294276-12-31 00:00+00' + INTERVAL '2 days'",
            SqlError::DatetimeFieldOverflow,
        ),
        (
            "SELECT TIMESTAMPTZ '2024-01-01 00:00+00' + 1",
            SqlError::UndefinedFunction,
        ),
        ("SELECT + INTERVAL '1 day'", SqlError::UndefinedFunction),
        (
            "SELECT - INTERVAL '-2147483648 days'",
            SqlError::DatetimeFieldOverflow,
        ),
        ("SELECT INTERVAL '1 year' / 0", SqlError::DivisionByZero),
        (
            "SELECT INTERVAL '1 year' / 0.0::float8",
            SqlError::DivisionByZero,
        ),
        (
            "SELECT INTERVAL '1 day' * 'NaN'::float8",
            SqlError::DatetimeFieldOverflow,
        ),
        (
            "SELECT INTERVAL 'infinity' * 0",
            SqlError::DatetimeFieldOverflow,
        ),
        (
            "SELECT INTERVAL 'infinity' / 'infinity'::float8",
            SqlError::DatetimeFieldOverflow,
        ),
        (
            "SELECT INTERVAL '1 month' * 1e10",
            SqlError::DatetimeFieldOverflow,
        ),
        (
            "SELECT INTERVAL 'infinity' + INTERVAL '-infinity'",
            SqlError::DatetimeFieldOverflow,
        ),
        (
            "SELECT INTERVAL 'infinity' - INTERVAL 'infinity'",
            SqlError::DatetimeFieldOverflow,
        ),
        (
            "SELECT INTERVAL '178000000 years' + INTERVAL '178000000 years'",
            SqlError::DatetimeFieldOverflow,
        ),
        (
            "SELECT INTERVAL '2562047788 hours' + INTERVAL '2562047788 hours'",
            SqlError::DatetimeFieldOverflow,
        ),
        ("SELECT INTERVAL '1 day' + 1", SqlError::UndefinedFunction),
        (
            "SELECT INTERVAL '1 day' * INTERVAL '1 day'",
            SqlError::UndefinedFunction,
        ),
        (
            "SELECT INTERVAL '1 day' / INTERVAL '1 hour'",
            SqlError::UndefinedFunction,
        ),
        (
            "SELECT TIME '10:00' + TIME '01:00'",
            SqlError::AmbiguousFunction,
        ),
        (
            "SELECT TIME '10:00' + INTERVAL 'infinity'",
            SqlError::DatetimeFieldOverflow,
        ),
        (
            "SELECT TIME '10:00' - INTERVAL '-infinity'",
            SqlError::DatetimeFieldOverflow,
        ),
        ("SELECT TIME '10:00' + 1", SqlError::UndefinedFunction),
        (
            "SELECT TIMETZ '10:00+00' - TIMETZ '09:00+00'",
            SqlError::UndefinedFunction,
        ),
        (
            "SELECT TIMETZ '10:00+00' + INTERVAL 'infinity'",
            SqlError::DatetimeFieldOverflow,
        ),
        (
            "SELECT '2024-01-01' + INTERVAL '1 day'",
            SqlError::InvalidDatetimeFormat,
        ),
        ("SELECT '1 day' + '1 day'", SqlError::AmbiguousFunction),
        (
            "SELECT DATE '2024-01-01' + NULL",
            SqlError::AmbiguousFunction,
        ),
        (
            "SELECT TIMETZ '10:00+00' + '1 hour'",
            SqlError::AmbiguousFunction,
        ),
        (
            "SELECT '1 hour' - TIMETZ '10:00+00'",
            SqlError::UndefinedFunction,
        ),
        (
            "SELECT DATE '2024-01-01' - '1'",
            SqlError::InvalidDatetimeFormat,
        ),
        ("SELECT '2' / INTERVAL '1 day'", SqlError::UndefinedFunction),
        (
            "SELECT '1' + DATE '2024-01-01'",
            SqlError::AmbiguousFunction,
        ),
        ("SELECT - DATE '2024-01-01'", SqlError::UndefinedFunction),
        (
            "SELECT - TIMESTAMP '2024-01-01'",
            SqlError::UndefinedFunction,
        ),
        ("SELECT - TIMETZ '10:00+00'", SqlError::UndefinedFunction),
        (
            "SELECT - INTERVAL '-178956970 years -8 months'",
            SqlError::DatetimeFieldOverflow,
        ),
        (
            "SELECT TIMESTAMP '-infinity' - TIMESTAMP '-infinity'",
            SqlError::DatetimeFieldOverflow,
        ),
        (
            "SELECT DATE '2024-01-01' + 3000000000",
            SqlError::UndefinedFunction,
        ),
        (
            "SELECT INTERVAL '1 day' * 2147483648.0",
            SqlError::DatetimeFieldOverflow,
        ),
        (
            "SELECT TIMESTAMP '2024-01-01' - INTERVAL '-178956970 years -8 months'",
            SqlError::DatetimeFieldOverflow,
        ),
    ] {
        assert_eq!(
            s.execute_at(sql, &[], None, &c),
            Err(CoreError::Sql(error)),
            "{sql}"
        );
    }
}

#[test]
fn temporal_session_zone_arithmetic_matches_pg18() {
    let f = Fixture::new();
    let mut s = f.session();
    for (zone, sql, ty, expected) in [
        (
            "UTC",
            "SELECT TIMESTAMPTZ '2024-03-09 12:00-06' + INTERVAL '1 day'",
            DataType::TimestampTz,
            "2024-03-10 18:00:00+00",
        ),
        (
            "UTC",
            "SELECT TIMESTAMPTZ '2024-03-09 12:00-06' + INTERVAL '24 hours'",
            DataType::TimestampTz,
            "2024-03-10 18:00:00+00",
        ),
        (
            "UTC",
            "SELECT TIMESTAMPTZ '2024-11-02 12:00-05' + INTERVAL '1 day'",
            DataType::TimestampTz,
            "2024-11-03 17:00:00+00",
        ),
        (
            "UTC",
            "SELECT TIMESTAMPTZ '2024-03-11 02:30-05' - INTERVAL '1 day'",
            DataType::TimestampTz,
            "2024-03-10 07:30:00+00",
        ),
        (
            "UTC",
            "SELECT TIMESTAMPTZ '2024-03-10 01:30-06' + INTERVAL '1 hour'",
            DataType::TimestampTz,
            "2024-03-10 08:30:00+00",
        ),
        (
            "UTC",
            "SELECT TIMESTAMPTZ '2024-01-31 20:00+00' + INTERVAL '1 month'",
            DataType::TimestampTz,
            "2024-02-29 20:00:00+00",
        ),
        (
            "UTC",
            "SELECT TIMESTAMPTZ '2024-02-29 23:30+00' + INTERVAL '1 year 1 day 1 hour'",
            DataType::TimestampTz,
            "2025-03-02 00:30:00+00",
        ),
        (
            "UTC",
            "SELECT TIMESTAMPTZ '2024-01-02 00:00+00' - TIMESTAMP '2024-01-01'",
            DataType::Interval,
            "1 day",
        ),
        (
            "UTC",
            "SELECT TIMESTAMPTZ '2024-03-10 12:00+00' - TIMESTAMP '2024-03-10 00:00'",
            DataType::Interval,
            "12:00:00",
        ),
        (
            "UTC",
            "SELECT DATE '2024-03-11' - TIMESTAMPTZ '2024-03-10 00:00+00'",
            DataType::Interval,
            "1 day",
        ),
        (
            "UTC",
            "SELECT TIMESTAMPTZ '2024-03-10 12:00+00' - TIME '13:00'",
            DataType::TimestampTz,
            "2024-03-09 23:00:00+00",
        ),
        (
            "America/Chicago",
            "SELECT TIMESTAMPTZ '2024-03-09 12:00-06' + INTERVAL '1 day'",
            DataType::TimestampTz,
            "2024-03-10 12:00:00-05",
        ),
        (
            "America/Chicago",
            "SELECT TIMESTAMPTZ '2024-03-09 12:00-06' + INTERVAL '24 hours'",
            DataType::TimestampTz,
            "2024-03-10 13:00:00-05",
        ),
        (
            "America/Chicago",
            "SELECT TIMESTAMPTZ '2024-11-02 12:00-05' + INTERVAL '1 day'",
            DataType::TimestampTz,
            "2024-11-03 12:00:00-06",
        ),
        (
            "America/Chicago",
            "SELECT TIMESTAMPTZ '2024-03-11 02:30-05' - INTERVAL '1 day'",
            DataType::TimestampTz,
            "2024-03-10 03:30:00-05",
        ),
        (
            "America/Chicago",
            "SELECT TIMESTAMPTZ '2024-03-10 01:30-06' + INTERVAL '1 hour'",
            DataType::TimestampTz,
            "2024-03-10 03:30:00-05",
        ),
        (
            "America/Chicago",
            "SELECT TIMESTAMPTZ '2024-01-31 20:00+00' + INTERVAL '1 month'",
            DataType::TimestampTz,
            "2024-02-29 14:00:00-06",
        ),
        (
            "America/Chicago",
            "SELECT TIMESTAMPTZ '2024-02-29 23:30+00' + INTERVAL '1 year 1 day 1 hour'",
            DataType::TimestampTz,
            "2025-03-01 18:30:00-06",
        ),
        (
            "America/Chicago",
            "SELECT TIMESTAMPTZ '2024-01-02 00:00+00' - TIMESTAMP '2024-01-01'",
            DataType::Interval,
            "18:00:00",
        ),
        (
            "America/Chicago",
            "SELECT TIMESTAMPTZ '2024-03-10 12:00+00' - TIMESTAMP '2024-03-10 00:00'",
            DataType::Interval,
            "06:00:00",
        ),
        (
            "America/Chicago",
            "SELECT DATE '2024-03-11' - TIMESTAMPTZ '2024-03-10 00:00+00'",
            DataType::Interval,
            "1 day 05:00:00",
        ),
        (
            "America/Chicago",
            "SELECT TIMESTAMPTZ '2024-03-10 12:00+00' - TIME '13:00'",
            DataType::TimestampTz,
            "2024-03-09 17:00:00-06",
        ),
        (
            "Asia/Kolkata",
            "SELECT TIMESTAMPTZ '2024-03-09 12:00-06' + INTERVAL '1 day'",
            DataType::TimestampTz,
            "2024-03-10 23:30:00+05:30",
        ),
        (
            "Asia/Kolkata",
            "SELECT TIMESTAMPTZ '2024-03-09 12:00-06' + INTERVAL '24 hours'",
            DataType::TimestampTz,
            "2024-03-10 23:30:00+05:30",
        ),
        (
            "Asia/Kolkata",
            "SELECT TIMESTAMPTZ '2024-11-02 12:00-05' + INTERVAL '1 day'",
            DataType::TimestampTz,
            "2024-11-03 22:30:00+05:30",
        ),
        (
            "Asia/Kolkata",
            "SELECT TIMESTAMPTZ '2024-03-11 02:30-05' - INTERVAL '1 day'",
            DataType::TimestampTz,
            "2024-03-10 13:00:00+05:30",
        ),
        (
            "Asia/Kolkata",
            "SELECT TIMESTAMPTZ '2024-03-10 01:30-06' + INTERVAL '1 hour'",
            DataType::TimestampTz,
            "2024-03-10 14:00:00+05:30",
        ),
        (
            "Asia/Kolkata",
            "SELECT TIMESTAMPTZ '2024-01-31 20:00+00' + INTERVAL '1 month'",
            DataType::TimestampTz,
            "2024-03-01 01:30:00+05:30",
        ),
        (
            "Asia/Kolkata",
            "SELECT TIMESTAMPTZ '2024-02-29 23:30+00' + INTERVAL '1 year 1 day 1 hour'",
            DataType::TimestampTz,
            "2025-03-02 06:00:00+05:30",
        ),
        (
            "Asia/Kolkata",
            "SELECT TIMESTAMPTZ '2024-01-02 00:00+00' - TIMESTAMP '2024-01-01'",
            DataType::Interval,
            "1 day 05:30:00",
        ),
        (
            "Asia/Kolkata",
            "SELECT TIMESTAMPTZ '2024-03-10 12:00+00' - TIMESTAMP '2024-03-10 00:00'",
            DataType::Interval,
            "17:30:00",
        ),
        (
            "Asia/Kolkata",
            "SELECT DATE '2024-03-11' - TIMESTAMPTZ '2024-03-10 00:00+00'",
            DataType::Interval,
            "18:30:00",
        ),
        (
            "Asia/Kolkata",
            "SELECT TIMESTAMPTZ '2024-03-10 12:00+00' - TIME '13:00'",
            DataType::TimestampTz,
            "2024-03-10 04:30:00+05:30",
        ),
    ] {
        scalar(&mut s, &clock(zone), sql, ty, Some(expected));
    }
}

#[test]
fn inferred_temporal_parameters_use_pg18_preferred_types() {
    let f = Fixture::new();
    let mut s = f.session();
    for (sql, parameters, columns) in [
        (
            "SELECT INTERVAL '1 day' * $1",
            vec![DataType::DoublePrecision],
            vec![DataType::Interval],
        ),
        (
            "SELECT $1 - DATE '2024-01-01'",
            vec![DataType::Date],
            vec![DataType::Integer],
        ),
    ] {
        let p = s.prepare(sql).unwrap();
        let d = s.describe(&p).unwrap();
        assert_eq!(d.parameters, parameters, "{sql}");
        assert_eq!(
            d.columns
                .into_iter()
                .map(|c| c.data_type)
                .collect::<Vec<_>>(),
            columns,
            "{sql}"
        );
    }
    let c = clock("UTC");
    let p = s.prepare("SELECT INTERVAL '1 day' * $1").unwrap();
    assert_eq!(
        s.execute_prepared_at(
            &p,
            &[Value::Float8(giggles_pg_types::Float8::parse("2").unwrap())],
            None,
            &c
        )
        .unwrap()
        .rows[0]
            .values,
        vec![Value::Interval(
            giggles_pg_types::temporal::Interval::parse("2 days").unwrap()
        )]
    );
    let p = s.prepare("SELECT $1 - DATE '2024-01-01'").unwrap();
    assert_eq!(
        s.execute_prepared_at(
            &p,
            &[Value::Date(
                giggles_pg_types::temporal::Date::parse(
                    "2024-01-05",
                    &giggles_pg_types::temporal::ParserSettings::default()
                )
                .unwrap()
            )],
            None,
            &c
        )
        .unwrap()
        .rows[0]
            .values,
        vec![Value::Int(4)]
    );
}

fn query(
    s: &mut SqlSession,
    c: &StatementClock,
    sql: &str,
    text_sql: &str,
    types: &[DataType],
    expected: &[&[Option<&str>]],
) {
    let p = s.prepare(sql).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    assert_eq!(
        s.describe(&p)
            .unwrap()
            .columns
            .into_iter()
            .map(|c| c.data_type)
            .collect::<Vec<_>>(),
        types,
        "{sql}"
    );
    let actual = s
        .execute_at(text_sql, &[], None, c)
        .unwrap_or_else(|e| panic!("{text_sql}: {e:?}"));
    let expected: Vec<Vec<Value>> = expected
        .iter()
        .map(|row| {
            row.iter()
                .map(|v| v.map_or(Value::Null, |v| Value::Text(v.into())))
                .collect()
        })
        .collect();
    assert_eq!(
        actual
            .rows
            .into_iter()
            .map(|r| r.values)
            .collect::<Vec<_>>(),
        expected,
        "{sql}"
    );
}

#[test]
fn temporal_table_expressions_filters_sorting_and_updates() {
    let f = Fixture::new();
    let mut s = f.session();
    let c = clock("UTC");
    for sql in [
        "CREATE TABLE tt(id integer primary key, d date, ts timestamp, tz timestamptz, iv interval, t time)",
        "INSERT INTO tt VALUES (1, '2024-01-31', '2024-01-31 10:00', '2024-03-09 12:00-06', '1 month 1 day 1 hour', '23:30'), (2, '2024-02-29', '2024-03-10 01:30', '2024-11-02 12:00-05', '-1 day', '00:15'), (3, 'infinity', 'infinity', '-infinity', '2 hours', '12:00'), (4, NULL, NULL, NULL, NULL, NULL)",
    ] {
        s.execute_at(sql, &[], None, &c).unwrap();
    }
    query(
        &mut s,
        &c,
        "SELECT id, d + 1, d - 1, ts + iv, ts - iv, tz + iv, tz - iv, iv * 2, -iv, t + iv FROM tt ORDER BY id",
        "SELECT (id)::text, (d + 1)::text, (d - 1)::text, (ts + iv)::text, (ts - iv)::text, (tz + iv)::text, (tz - iv)::text, (iv * 2)::text, (-iv)::text, (t + iv)::text FROM tt ORDER BY id",
        &[
            DataType::Integer,
            DataType::Date,
            DataType::Date,
            DataType::Timestamp,
            DataType::Timestamp,
            DataType::TimestampTz,
            DataType::TimestampTz,
            DataType::Interval,
            DataType::Interval,
            DataType::Time,
        ],
        &[
            &[
                Some("1"),
                Some("2024-02-01"),
                Some("2024-01-30"),
                Some("2024-03-01 11:00:00"),
                Some("2023-12-30 09:00:00"),
                Some("2024-04-10 19:00:00+00"),
                Some("2024-02-08 17:00:00+00"),
                Some("2 mons 2 days 02:00:00"),
                Some("-1 mons -1 days -01:00:00"),
                Some("00:30:00"),
            ],
            &[
                Some("2"),
                Some("2024-03-01"),
                Some("2024-02-28"),
                Some("2024-03-09 01:30:00"),
                Some("2024-03-11 01:30:00"),
                Some("2024-11-01 17:00:00+00"),
                Some("2024-11-03 17:00:00+00"),
                Some("-2 days"),
                Some("1 day"),
                Some("00:15:00"),
            ],
            &[
                Some("3"),
                Some("infinity"),
                Some("infinity"),
                Some("infinity"),
                Some("infinity"),
                Some("-infinity"),
                Some("-infinity"),
                Some("04:00:00"),
                Some("-02:00:00"),
                Some("14:00:00"),
            ],
            &[
                Some("4"),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ],
        ],
    );
    query(
        &mut s,
        &c,
        "SELECT id, d - DATE '2024-01-01', ts - TIMESTAMP '2024-01-01', tz - TIMESTAMPTZ '2024-01-01 00:00+00', t - TIME '00:00' FROM tt WHERE id <> 3 ORDER BY id",
        "SELECT (id)::text, (d - DATE '2024-01-01')::text, (ts - TIMESTAMP '2024-01-01')::text, (tz - TIMESTAMPTZ '2024-01-01 00:00+00')::text, (t - TIME '00:00')::text FROM tt WHERE id <> 3 ORDER BY id",
        &[
            DataType::Integer,
            DataType::Integer,
            DataType::Interval,
            DataType::Interval,
            DataType::Interval,
        ],
        &[
            &[
                Some("1"),
                Some("30"),
                Some("30 days 10:00:00"),
                Some("68 days 18:00:00"),
                Some("23:30:00"),
            ],
            &[
                Some("2"),
                Some("59"),
                Some("69 days 01:30:00"),
                Some("306 days 17:00:00"),
                Some("00:15:00"),
            ],
            &[Some("4"), None, None, None, None],
        ],
    );
    query(
        &mut s,
        &c,
        "SELECT id, d + iv, d + t FROM tt ORDER BY id",
        "SELECT (id)::text, (d + iv)::text, (d + t)::text FROM tt ORDER BY id",
        &[DataType::Integer, DataType::Timestamp, DataType::Timestamp],
        &[
            &[
                Some("1"),
                Some("2024-03-01 01:00:00"),
                Some("2024-01-31 23:30:00"),
            ],
            &[
                Some("2"),
                Some("2024-02-28 00:00:00"),
                Some("2024-02-29 00:15:00"),
            ],
            &[Some("3"), Some("infinity"), Some("infinity")],
            &[Some("4"), None, None],
        ],
    );
    query(
        &mut s,
        &c,
        "SELECT id FROM tt WHERE d + 1 > DATE '2024-01-15' ORDER BY id",
        "SELECT (id)::text FROM tt WHERE d + 1 > DATE '2024-01-15' ORDER BY id",
        &[DataType::Integer],
        &[&[Some("1")], &[Some("2")], &[Some("3")]],
    );
    query(
        &mut s,
        &c,
        "SELECT id FROM tt WHERE tz + INTERVAL '1 day' > TIMESTAMPTZ '2024-03-10 12:00-05' ORDER BY id",
        "SELECT (id)::text FROM tt WHERE tz + INTERVAL '1 day' > TIMESTAMPTZ '2024-03-10 12:00-05' ORDER BY id",
        &[DataType::Integer],
        &[&[Some("1")], &[Some("2")]],
    );
    query(
        &mut s,
        &c,
        "SELECT id FROM tt ORDER BY ts + iv DESC, id",
        "SELECT (id)::text FROM tt ORDER BY ts + iv DESC, id",
        &[DataType::Integer],
        &[&[Some("4")], &[Some("3")], &[Some("2")], &[Some("1")]],
    );
    s.execute_at("UPDATE tt SET d = d + 1 WHERE id = 1", &[], None, &c)
        .unwrap();
    query(
        &mut s,
        &c,
        "SELECT d FROM tt WHERE id = 1",
        "SELECT d::text FROM tt WHERE id = 1",
        &[DataType::Date],
        &[&[Some("2024-02-01")]],
    );
}

// Known gap: PG18.6 accepts DEFAULT current_date + 7 / now() + INTERVAL '1 hour' /
// DATE '2024-01-31' + 1 (rows `def ...`), but column defaults go through the separate
// defaults.rs expression grammar, which has no binary operators yet. F16 follow-up.
#[test]
fn known_gap_temporal_arithmetic_defaults() {
    let f = Fixture::new();
    let mut s = f.session();
    let c = clock("UTC");
    for sql in [
        "CREATE TABLE td(id integer primary key, due date DEFAULT current_date + 7)",
        "CREATE TABLE td(id integer primary key, exp timestamptz DEFAULT now() + INTERVAL '1 hour')",
        "CREATE TABLE td(id integer primary key, lit date DEFAULT DATE '2024-01-31' + 1)",
    ] {
        assert!(
            matches!(
                s.execute_at(sql, &[], None, &c),
                Err(CoreError::Sql(SqlError::Unsupported))
            ),
            "{sql}"
        );
    }
}

#[test]
fn temporal_insert_values_arithmetic() {
    let f = Fixture::new();
    let mut s = f.session();
    let c = clock("UTC");
    for sql in [
        "CREATE TABLE ti(id integer primary key, d date)",
        "INSERT INTO ti(id, d) VALUES (1, DATE '2024-01-01' + 1)",
    ] {
        s.execute_at(sql, &[], None, &c)
            .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    }
    query(
        &mut s,
        &c,
        "SELECT d FROM ti WHERE id = 1",
        "SELECT d::text FROM ti WHERE id = 1",
        &[DataType::Date],
        &[&[Some("2024-01-02")]],
    );
}

#[test]
fn temporal_clock_arithmetic_uses_one_statement_clock() {
    let f = Fixture::new();
    let mut s = f.session();
    let c = clock("UTC");
    for (sql, ty, expected) in [
        (
            "SELECT now() - INTERVAL '1 day' < now()",
            DataType::Boolean,
            "true",
        ),
        (
            "SELECT current_date + 1 > current_date",
            DataType::Boolean,
            "true",
        ),
        ("SELECT now() - now()", DataType::Interval, "00:00:00"),
    ] {
        scalar(&mut s, &c, sql, ty, Some(expected));
    }
}

#[test]
fn temporal_binary_results_match_pg18() {
    let f = Fixture::new();
    let mut s = f.session();
    let c = clock("UTC");
    for (sql, ty, expected) in [
        ("SELECT DATE '2024-01-01' + 1", DataType::Date, "0000223f"),
        (
            "SELECT INTERVAL '1 month 1 day' * 1.5",
            DataType::Interval,
            "0000000a0eebb0000000001000000001",
        ),
        (
            "SELECT TIMESTAMP '2024-01-02 03:00' - TIMESTAMP '2024-01-01'",
            DataType::Interval,
            "0000000283baec000000000100000000",
        ),
        (
            "SELECT TIMESTAMPTZ '2024-03-09 12:00-06' + INTERVAL '1 day'",
            DataType::TimestampTz,
            "0002b650f657a800",
        ),
        (
            "SELECT TIME '10:00' - TIME '12:30'",
            DataType::Interval,
            "fffffffde78ee6000000000000000000",
        ),
        (
            "SELECT TIMETZ '23:00+02' + INTERVAL '2 hours'",
            DataType::TimeTz,
            "00000000d693a400ffffe3e0",
        ),
        (
            "SELECT DATE '2024-03-01' - DATE '2024-01-01'",
            DataType::Integer,
            "0000003c",
        ),
    ] {
        let p = s.prepare(sql).unwrap();
        assert_eq!(s.describe(&p).unwrap().columns[0].data_type, ty, "{sql}");
        let value = s
            .execute_prepared_at(&p, &[], None, &c)
            .unwrap()
            .rows
            .remove(0)
            .values
            .remove(0);
        let bytes = match value {
            Value::Date(v) => v.to_binary().to_vec(),
            Value::TimestampTz(v) => v.to_binary().to_vec(),
            Value::Interval(v) => v.to_binary().to_vec(),
            Value::TimeTz(v) => v.to_binary().to_vec(),
            Value::Int(v) => i32::try_from(v).unwrap().to_be_bytes().to_vec(),
            other => panic!("unexpected binary result: {other:?}"),
        };
        let actual: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(actual, expected, "{sql}");
    }
}
