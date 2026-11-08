use giggles_core::{Admin, CoreError, Error, Limits, SqlSession, relational::*};
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
            "dd68-session-zone-float-{}-{}",
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
fn value(s: &mut SqlSession, sql: &str, c: &StatementClock) -> Value {
    s.execute_at(&format!("SELECT {sql}"), &[], None, c)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
        .rows
        .remove(0)
        .values
        .remove(0)
}
fn text(s: &mut SqlSession, sql: &str, expected: &str, c: &StatementClock) {
    assert_eq!(value(s, sql, c), Value::Text(expected.into()), "{sql}");
}

// Expectations captured from PostgreSQL 18.6 in dd68-f14/probe/pg18.txt.
#[test]
fn timestamptz_text_uses_session_zone_in_every_text_path() {
    let f = Fixture::new();
    let mut s = f.session();
    for (zone, expected, fraction, bc) in [
        (
            "UTC",
            "2024-01-02 03:00:00+00",
            "2024-01-02 03:00:00.123456+00",
            "0044-03-15 12:00:00+00 BC",
        ),
        (
            "America/Chicago",
            "2024-01-01 21:00:00-06",
            "2024-01-01 21:00:00.123456-06",
            "0044-03-15 06:09:24-05:50:36 BC",
        ),
        (
            "Asia/Kolkata",
            "2024-01-02 08:30:00+05:30",
            "2024-01-02 08:30:00.123456+05:30",
            "0044-03-15 17:53:28+05:53:28 BC",
        ),
    ] {
        let c = clock(zone);
        let tz = "'2024-01-02 03:00+00'::timestamptz";
        text(&mut s, &format!("{tz}::text"), expected, &c);
        text(
            &mut s,
            &format!("'at ' || {tz}"),
            &format!("at {expected}"),
            &c,
        );
        text(&mut s, &format!("concat({tz})"), expected, &c);
        text(
            &mut s,
            &format!("concat_ws('|', 'at', {tz})"),
            &format!("at|{expected}"),
            &c,
        );
        text(
            &mut s,
            "'2024-01-02 03:00:00.123456+00'::timestamptz::text",
            fraction,
            &c,
        );
        text(
            &mut s,
            "'0044-03-15 12:00+00 BC'::timestamptz::text",
            bc,
            &c,
        );
        for infinity in ["infinity", "-infinity"] {
            for target in ["text", "date::text", "timestamp::text"] {
                text(
                    &mut s,
                    &format!("'{infinity}'::timestamptz::{target}"),
                    infinity,
                    &c,
                );
            }
        }
    }
    text(
        &mut s,
        "'2024-07-01 12:00+00'::timestamptz::text",
        "2024-07-01 07:00:00-05",
        &clock("America/Chicago"),
    );
}

#[test]
fn session_zone_controls_explicit_casts_and_dst_resolution() {
    let f = Fixture::new();
    let mut s = f.session();
    let c = clock("America/Chicago");
    for (input, output, epoch) in [
        (
            "2024-07-01 12:00",
            "2024-07-01 12:00:00-05",
            "1719853200.000000",
        ),
        (
            "2024-01-01 00:30",
            "2024-01-01 00:30:00-06",
            "1704090600.000000",
        ),
        (
            "2024-03-10 02:30",
            "2024-03-10 03:30:00-05",
            "1710059400.000000",
        ),
        (
            "2024-11-03 01:30",
            "2024-11-03 01:30:00-06",
            "1730619000.000000",
        ),
    ] {
        text(&mut s, &format!("'{input}'::timestamptz::text"), output, &c);
        text(
            &mut s,
            &format!("'{input}'::text::timestamptz::text"),
            output,
            &c,
        );
        text(
            &mut s,
            &format!("extract(epoch from '{input}'::timestamptz)::text"),
            epoch,
            &c,
        );
    }
    for (sql, expected) in [
        (
            "'2024-01-01 23:45+05:30'::timestamptz::text",
            "2024-01-01 12:15:00-06",
        ),
        (
            "'2024-01-02 03:00+00'::timestamptz::date::text",
            "2024-01-01",
        ),
        (
            "'2024-01-02 03:00+00'::timestamptz::timestamp::text",
            "2024-01-01 21:00:00",
        ),
        (
            "'2024-01-02 03:00'::timestamp::timestamptz::text",
            "2024-01-02 03:00:00-06",
        ),
        ("'2024-01-02 03:00+00'::timestamptz::time::text", "21:00:00"),
        (
            "'2024-01-02 03:00+00'::timestamptz::timetz::text",
            "21:00:00-06",
        ),
        (
            "'2024-07-02'::date::timestamptz::text",
            "2024-07-02 00:00:00-05",
        ),
    ] {
        text(&mut s, sql, expected, &c);
    }
    for zone in ["UTC", "Asia/Kolkata"] {
        text(
            &mut s,
            "'2024-01-01 23:45+05:30'::timestamptz::date::text",
            "2024-01-01",
            &clock(zone),
        );
    }
    text(
        &mut s,
        "'2024-01-01 23:45+05:30'::timestamptz::timestamp::text",
        "2024-01-01 18:15:00",
        &clock("UTC"),
    );
    text(
        &mut s,
        "'2024-01-02 03:00+00'::timestamptz::date::text",
        "2024-01-02",
        &clock("UTC"),
    );
    let no_offset = s.prepare("SELECT '2024-07-01 12:00'::timestamptz::text, extract(epoch from '2024-07-01 12:00'::timestamptz)::text").unwrap();
    for (zone, rendered, epoch) in [
        ("UTC", "2024-07-01 12:00:00+00", "1719835200.000000"),
        (
            "America/Chicago",
            "2024-07-01 12:00:00-05",
            "1719853200.000000",
        ),
    ] {
        assert_eq!(
            s.execute_prepared_at(&no_offset, &[], None, &clock(zone))
                .unwrap()
                .rows[0]
                .values,
            vec![Value::Text(rendered.into()), Value::Text(epoch.into())]
        );
    }
    let p = s
        .prepare("SELECT $1::timestamptz::text")
        .unwrap()
        .with_parameter_types(&[Some(DataType::Text)])
        .unwrap();
    let result = s
        .execute_prepared_at(&p, &[Value::Text("2024-07-01 12:00".into())], None, &c)
        .unwrap();
    assert_eq!(
        result.rows[0].values,
        vec![Value::Text("2024-07-01 12:00:00-05".into())]
    );
    let p = s
        .prepare("SELECT '2024-01-02 03:00+00'::timestamptz::text")
        .unwrap();
    for (zone, expected) in [
        ("UTC", "2024-01-02 03:00:00+00"),
        ("America/Chicago", "2024-01-01 21:00:00-06"),
    ] {
        assert_eq!(
            s.execute_prepared_at(&p, &[], None, &clock(zone))
                .unwrap()
                .rows[0]
                .values,
            vec![Value::Text(expected.into())]
        );
    }
    let p = s.prepare("SELECT '2024-01-02 03:00+00'::timestamptz::date, '2024-01-02 03:00+00'::timestamptz::timestamp, '2024-01-02 03:00+00'::timestamptz, '2024-01-02 03:00+00'::timestamptz::text").unwrap();
    assert_eq!(
        s.describe(&p)
            .unwrap()
            .columns
            .into_iter()
            .map(|c| c.data_type)
            .collect::<Vec<_>>(),
        vec![
            DataType::Date,
            DataType::Timestamp,
            DataType::TimestampTz,
            DataType::Text
        ]
    );
}

#[test]
fn unknown_assignment_and_comparison_use_session_zone() {
    let f = Fixture::new();
    let mut s = f.session();
    let chicago = clock("America/Chicago");
    s.execute_at(
        "CREATE TABLE tz_rows (tz TIMESTAMPTZ, id INT PRIMARY KEY)",
        &[],
        None,
        &chicago,
    )
    .unwrap();
    s.execute_at(
        "INSERT INTO tz_rows VALUES ('2024-07-01 00:30', 1)",
        &[],
        None,
        &chicago,
    )
    .unwrap();
    let r = s
        .execute_at(
            "SELECT tz::text FROM tz_rows WHERE tz = '2024-07-01 00:30'",
            &[],
            None,
            &chicago,
        )
        .unwrap();
    assert_eq!(
        r.rows[0].values,
        vec![Value::Text("2024-07-01 00:30:00-05".into())]
    );
    let r = s
        .execute_at("SELECT tz::text FROM tz_rows", &[], None, &clock("UTC"))
        .unwrap();
    assert_eq!(
        r.rows[0].values,
        vec![Value::Text("2024-07-01 05:30:00+00".into())]
    );
    let r = s
        .execute_at(
            "SELECT id FROM tz_rows WHERE tz = '2024-07-01 05:30+00'",
            &[],
            None,
            &chicago,
        )
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    let r = s
        .execute_at(
            "SELECT 1 WHERE '2024-07-01 00:30'::timestamptz = '2024-07-01 05:30+00'",
            &[],
            None,
            &chicago,
        )
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    // Keep UPDATE coverage independent of primary-key mutation support.
    s.execute_at(
        "CREATE TABLE tz_updates (id INT PRIMARY KEY, tz TIMESTAMPTZ)",
        &[],
        None,
        &chicago,
    )
    .unwrap();
    s.execute_at(
        "INSERT INTO tz_updates VALUES (1, '2024-07-01 00:30')",
        &[],
        None,
        &chicago,
    )
    .unwrap();
    s.execute_at(
        "UPDATE tz_updates SET tz = '2024-07-01 23:30' WHERE tz = '2024-07-01 00:30'",
        &[],
        None,
        &chicago,
    )
    .unwrap();
    let r = s
        .execute_at(
            "SELECT tz::text FROM tz_updates WHERE tz::date = '2024-07-01'",
            &[],
            None,
            &chicago,
        )
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(
        r.rows[0].values,
        vec![Value::Text("2024-07-01 23:30:00-05".into())]
    );
}

#[test]
fn multi_insert_and_conflict_update_cast_unknowns_in_session_zone() {
    let f = Fixture::new();
    let mut s = f.session();
    let chicago = clock("America/Chicago");
    s.execute_at(
        "CREATE TABLE tz_multi (id INT PRIMARY KEY, tz TIMESTAMPTZ)",
        &[],
        None,
        &chicago,
    )
    .unwrap();
    s.execute_at(
        "INSERT INTO tz_multi VALUES (1, '2024-07-01 00:30'), (2, '2024-07-01 23:30')",
        &[],
        None,
        &chicago,
    )
    .unwrap();
    let r = s
        .execute_at(
            "SELECT tz::text FROM tz_multi ORDER BY id",
            &[],
            None,
            &clock("UTC"),
        )
        .unwrap();
    assert_eq!(
        r.rows.iter().map(|r| r.values.clone()).collect::<Vec<_>>(),
        vec![
            vec![Value::Text("2024-07-01 05:30:00+00".into())],
            vec![Value::Text("2024-07-02 04:30:00+00".into())],
        ]
    );
    s.execute_at("INSERT INTO tz_multi VALUES (1, '2024-07-01 00:30') ON CONFLICT (id) DO UPDATE SET tz = '2024-07-01 23:30'", &[], None, &chicago).unwrap();
    let r = s
        .execute_at(
            "SELECT tz::text FROM tz_multi WHERE id = 1",
            &[],
            None,
            &clock("UTC"),
        )
        .unwrap();
    assert_eq!(
        r.rows[0].values,
        vec![Value::Text("2024-07-02 04:30:00+00".into())]
    );
}

#[test]
fn extra_float_digits_changes_text_paths_without_changing_stored_bits() {
    let f = Fixture::new();
    let mut s = f.session();
    let c = clock("UTC");
    s.execute_at(
        "CREATE TABLE floats (id INT PRIMARY KEY, f FLOAT8, r REAL)",
        &[],
        None,
        &c,
    )
    .unwrap();
    s.execute_at(
        "INSERT INTO floats VALUES (1, 0.1::float8, '1.1'::float4)",
        &[],
        None,
        &c,
    )
    .unwrap();
    text(&mut s, "(1/3.0)::float8::text", "0.3333333333333333", &c);
    text(
        &mut s,
        "'1.1'::float4::float8::text",
        "1.100000023841858",
        &c,
    );
    let prepared = s.prepare("SELECT (1/3.0)::float8::text").unwrap();
    for (efd, third, widened, real) in [
        (
            1,
            "0.3333333333333333",
            "1.100000023841858",
            "1.2345678e+07",
        ),
        (0, "0.333333333333333", "1.10000002384186", "1.23457e+07"),
        (-4, "0.33333333333", "1.1000000238", "1.2e+07"),
        (
            3,
            "0.3333333333333333",
            "1.100000023841858",
            "1.2345678e+07",
        ),
    ] {
        s.set_extra_float_digits(efd);
        text(&mut s, "(1/3.0)::float8::text", third, &c);
        assert_eq!(
            s.execute_prepared_at(&prepared, &[], None, &c)
                .unwrap()
                .rows[0]
                .values,
            vec![Value::Text(third.into())]
        );
        text(&mut s, "'1.1'::float4::float8::text", widened, &c);
        text(
            &mut s,
            "'x' || '1.1'::float4::float8",
            &format!("x{widened}"),
            &c,
        );
        text(&mut s, "concat((1/3.0)::float8)", third, &c);
        text(
            &mut s,
            "concat_ws('|', 'x', (1/3.0)::float8)",
            &format!("x|{third}"),
            &c,
        );
        text(
            &mut s,
            "length((1/3.0)::float8::text)::text",
            &third.len().to_string(),
            &c,
        );
        text(&mut s, "12345678::float4::text", real, &c);
        for special in ["-0", "NaN", "Infinity", "-Infinity"] {
            text(&mut s, &format!("'{special}'::float8::text"), special, &c);
            text(&mut s, &format!("'{special}'::float4::text"), special, &c);
        }
        let r = s
            .execute_at("SELECT f, r FROM floats", &[], None, &c)
            .unwrap();
        match &r.rows[0].values[..] {
            [Value::Float8(f), Value::Float4(r)] => {
                assert_eq!(f.to_bits(), 0.1f64.to_bits());
                assert_eq!(r.to_bits(), 1.1f32.to_bits());
            }
            values => panic!("unexpected float values: {values:?}"),
        }
    }
    s.set_extra_float_digits(0);
    text(&mut s, "(0.1::float8 + 0.2::float8)::text", "0.3", &c);
    let mut default = f.session();
    assert_eq!(
        default
            .execute(
                "SELECT (1/3.0)::float8::text, '2024-01-02 03:00+00'::timestamptz::text",
                &[],
                None
            )
            .unwrap()
            .rows[0]
            .values,
        vec![
            Value::Text("0.3333333333333333".into()),
            Value::Text("2024-01-02 03:00:00+00".into())
        ]
    );
}

#[test]
fn unavailable_clock_preserves_session_zone_for_explicit_timestamptz_text_casts() {
    let f = Fixture::new();
    let mut s = f.session();
    let mut c = clock("America/Chicago");
    c.unavailable = Some(CoreError::Policy(Error::UncertainTime));
    text(
        &mut s,
        "'2024-07-01 12:00:00+00'::timestamptz::text",
        "2024-07-01 07:00:00-05",
        &c,
    );
    text(
        &mut s,
        "'2024-07-01 12:00'::timestamptz::text",
        "2024-07-01 12:00:00-05",
        &c,
    );
    c.zone = None;
    text(
        &mut s,
        "'2024-07-01 12:00'::timestamptz::text",
        "2024-07-01 12:00:00+00",
        &c,
    );
}

#[test]
fn multi_column_update_of_zone_less_timestamptz_literal_passes_unclocked_precheck() {
    let f = Fixture::new();
    let mut s = f.session();
    let chicago = clock("America/Chicago");
    for sql in [
        "CREATE TABLE t (id INT PRIMARY KEY, ts TIMESTAMPTZ, other INT)",
        "INSERT INTO t VALUES (1, NULL, 0)",
        "UPDATE t SET ts = '2026-01-01 00:00', other = 1",
    ] {
        s.execute_at(sql, &[], None, &chicago).unwrap();
    }
    let r = s
        .execute_at("SELECT ts::text, other FROM t", &[], None, &clock("UTC"))
        .unwrap();
    assert_eq!(
        r.rows[0].values,
        vec![Value::Text("2026-01-01 06:00:00+00".into()), Value::Int(1)]
    );
}
