// Unknown string literals use the destination's input parser at assignment.
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
        let path = PathBuf::from("J:/gigglesdb-work/dd68-f18/tmp").join(format!(
            "unknown-literal-assignment-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
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
fn execute(s: &mut SqlSession, c: &StatementClock, sql: &str) {
    s.execute_at(sql, &[], None, c)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
}
fn expect_text(s: &mut SqlSession, c: &StatementClock, sql: &str, expected: &[Option<&str>]) {
    let result = s
        .execute_at(sql, &[], None, c)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    let actual: Vec<_> = result.rows.iter().map(|r| r.values.clone()).collect();
    let expected: Vec<_> = expected
        .iter()
        .map(|v| vec![v.map_or(Value::Null, |v| Value::Text(v.into()))])
        .collect();
    assert_eq!(actual, expected, "{sql}");
}
fn expect_error(s: &mut SqlSession, c: &StatementClock, sql: &str, expected: SqlError) {
    assert_eq!(
        s.execute_at(sql, &[], None, c).unwrap_err(),
        CoreError::Sql(expected),
        "{sql}"
    );
}

#[test]
fn numeric_input_errors_and_special_values_match_in_all_input_routes() {
    use SqlError::*;
    let f = Fixture::new();
    let mut s = f.session();
    let c = clock("UTC");
    execute(
        &mut s,
        &c,
        "CREATE TABLE n (id integer PRIMARY KEY, v numeric)",
    );
    for (input, expected) in [
        ("abc", Err(InvalidTextRepresentation)),
        (" ", Err(InvalidTextRepresentation)),
        ("", Err(InvalidTextRepresentation)),
        ("1.2.3", Err(InvalidTextRepresentation)),
        ("e5", Err(InvalidTextRepresentation)),
        ("1e", Err(InvalidTextRepresentation)),
        ("1e1000000", Err(Overflow)),
        ("1e5", Ok("100000")),
        ("NaN", Ok("NaN")),
        ("Infinity", Ok("Infinity")),
        ("-inf", Ok("-Infinity")),
        (" +.5e+1000000 ", Err(Overflow)),
        ("-1.e-1000000", Err(Overflow)),
        ("1e+", Err(InvalidTextRepresentation)),
        ("1e1000000x", Err(InvalidTextRepresentation)),
        ("1e999999999999999999999", Err(Overflow)),
        (".e999999999999999999999", Err(InvalidTextRepresentation)),
        (
            "1.2.3e999999999999999999999",
            Err(InvalidTextRepresentation),
        ),
        ("0xff", Ok("255")),
        ("0o10", Ok("8")),
        ("0b10", Ok("2")),
        ("0xg", Err(InvalidTextRepresentation)),
    ] {
        for route in 0..3 {
            execute(&mut s, &c, "DELETE FROM n");
            if route == 2 {
                execute(&mut s, &c, "INSERT INTO n (id) VALUES (1)");
            }
            let sql = match route {
                0 => format!("SELECT ('{input}'::numeric)::text"),
                1 => format!("INSERT INTO n (id,v) VALUES (1,'{input}')"),
                2 => format!("UPDATE n SET v='{input}' WHERE id=1"),
                _ => unreachable!(),
            };
            match &expected {
                Ok(value) if route == 0 => {
                    expect_text(&mut s, &c, &sql, &[Some(value)]);
                }
                Ok(value) => {
                    execute(&mut s, &c, &sql);
                    expect_text(&mut s, &c, "SELECT v::text FROM n", &[Some(value)]);
                }
                Err(error) => expect_error(&mut s, &c, &sql, error.clone()),
            }
        }
    }
}

#[test]
fn quoted_literals_use_each_destination_input_parser() {
    use SqlError::*;
    let f = Fixture::new();
    let mut s = f.session();
    let c = clock("UTC");
    let cases = [
        ("bytea", r"'\x0001ff'", Ok(r"\x0001ff")),
        ("bytea", r"'\x'", Ok(r"\x")),
        ("bytea", r"'abc\000'", Ok(r"\x61626300")),
        ("bytea", r"'\x123'", Err(InvalidParameterValue)),
        ("bytea", r"'\xzz'", Err(InvalidParameterValue)),
        ("bytea", r"'a\q'", Err(InvalidTextRepresentation)),
        ("boolean", "'t'", Ok("true")),
        ("boolean", "'off'", Ok("false")),
        ("boolean", "'maybe'", Err(InvalidTextRepresentation)),
        ("smallint", "'42'", Ok("42")),
        ("smallint", "' -7 '", Ok("-7")),
        ("smallint", "'40000'", Err(Overflow)),
        ("smallint", "'1.5'", Err(InvalidTextRepresentation)),
        ("integer", "'2147483647'", Ok("2147483647")),
        ("integer", "'-0012'", Ok("-12")),
        ("integer", "'2147483648'", Err(Overflow)),
        ("integer", "''", Err(InvalidTextRepresentation)),
        ("bigint", "'922337203685477580'", Ok("922337203685477580")),
        ("bigint", "'+5'", Ok("5")),
        ("bigint", "'9223372036854775808'", Err(Overflow)),
        ("numeric", "'12.3400'", Ok("12.3400")),
        ("numeric", "'NaN'", Ok("NaN")),
        ("real", "'1.5'", Ok("1.5")),
        ("real", "'1e40'", Err(Overflow)),
        ("double precision", "'0.1'", Ok("0.1")),
        ("double precision", "'1e400'", Err(Overflow)),
        ("date", "'2024-02-29'", Ok("2024-02-29")),
        ("date", "'2023-02-29'", Err(DatetimeFieldOverflow)),
        ("date", "'notadate'", Err(InvalidDatetimeFormat)),
        ("time", "'12:34:56.789'", Ok("12:34:56.789")),
        ("time", "'25:00'", Err(DatetimeFieldOverflow)),
        (
            "timestamp",
            "'2024-01-02 03:04:05+09'",
            Ok("2024-01-02 03:04:05"),
        ),
        ("interval", "'1 day 02:03:04'", Ok("1 day 02:03:04")),
    ];
    for (i, (ty, literal, expected)) in cases.into_iter().enumerate() {
        let table = format!("input_case_{i}");
        execute(&mut s, &c, &format!("CREATE TABLE {table} (v {ty})"));
        let sql = format!("INSERT INTO {table} VALUES ({literal})");
        match expected {
            Ok(value) => {
                execute(&mut s, &c, &sql);
                expect_text(
                    &mut s,
                    &c,
                    &format!("SELECT v::text FROM {table}"),
                    &[Some(value)],
                );
            }
            Err(error) => expect_error(&mut s, &c, &sql, error),
        }
    }
}

#[test]
fn unknown_literals_reach_input_parsers_in_every_assignment_path() {
    use SqlError::*;
    let f = Fixture::new();
    let mut s = f.session();
    let c = clock("UTC");
    for (case, (ty, good, value, bad, error)) in [
        (
            "bytea",
            r"'\x0001ff'",
            r"\x0001ff",
            r"'a\q'",
            InvalidTextRepresentation,
        ),
        (
            "boolean",
            "'off'",
            "false",
            "'maybe'",
            InvalidTextRepresentation,
        ),
        ("integer", "'-0012'", "-12", "'2147483648'", Overflow),
    ]
    .into_iter()
    .enumerate()
    {
        for path in 0..6 {
            for succeeds in [true, false] {
                let table = format!(
                    "path_{case}_{path}_{}",
                    if succeeds { "good" } else { "bad" }
                );
                execute(
                    &mut s,
                    &c,
                    &format!("CREATE TABLE {table} (id integer PRIMARY KEY, v {ty})"),
                );
                if path >= 4 {
                    execute(
                        &mut s,
                        &c,
                        &format!("INSERT INTO {table} VALUES (1, {good})"),
                    );
                }
                let literal = if succeeds { good } else { bad };
                let sql = match path {
                    0 => format!("INSERT INTO {table} VALUES (1, {literal})"),
                    1 => format!("INSERT INTO {table} (v, id) VALUES ({literal}, 1)"),
                    2 => format!("INSERT INTO {table} VALUES (1, {good}), (2, {literal})"),
                    3 => format!("INSERT INTO {table} VALUES (1, {literal}) RETURNING v"),
                    4 => format!("UPDATE {table} SET v={literal} WHERE id=1"),
                    5 => format!(
                        "INSERT INTO {table} VALUES (1, {good}) ON CONFLICT (id) DO UPDATE SET v={literal}"
                    ),
                    _ => unreachable!(),
                };
                if succeeds {
                    let result = s
                        .execute_at(&sql, &[], None, &c)
                        .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
                    if path == 3 {
                        assert_eq!(result.rows.len(), 1, "{sql}");
                    }
                    let expected = if path == 2 {
                        vec![Some(value), Some(value)]
                    } else {
                        vec![Some(value)]
                    };
                    expect_text(
                        &mut s,
                        &c,
                        &format!("SELECT v::text FROM {table} ORDER BY id"),
                        &expected,
                    );
                } else {
                    expect_error(&mut s, &c, &sql, error.clone());
                }
            }
        }
    }
}

#[test]
fn timezone_free_timestamptz_literals_use_the_session_zone() {
    let f = Fixture::new();
    let mut s = f.session();
    for (i, (zone, expected)) in [
        ("UTC", "2024-07-01 12:00:00+00"),
        ("America/Chicago", "2024-07-01 12:00:00-05"),
    ]
    .into_iter()
    .enumerate()
    {
        let c = clock(zone);
        execute(
            &mut s,
            &c,
            &format!("CREATE TABLE zone_{i} (v timestamptz)"),
        );
        execute(
            &mut s,
            &c,
            &format!("INSERT INTO zone_{i} VALUES ('2024-07-01 12:00:00')"),
        );
        expect_text(
            &mut s,
            &c,
            &format!("SELECT v::text FROM zone_{i}"),
            &[Some(expected)],
        );
    }
}

#[test]
fn typed_values_keep_assignment_coercion_rules() {
    use SqlError::*;
    let f = Fixture::new();
    let mut s = f.session();
    let c = clock("UTC");
    for (i, (ty, expression, expected)) in [
        ("bytea", "1", Err(Type)),
        ("integer", "true", Err(Type)),
        ("date", "1", Err(Type)),
        ("numeric", "true", Err(Type)),
        ("text", "1", Ok("1")),
        ("integer", "1.5", Ok("2")),
        ("smallint", "70000", Err(Overflow)),
    ]
    .into_iter()
    .enumerate()
    {
        let table = format!("typed_{i}");
        execute(&mut s, &c, &format!("CREATE TABLE {table} (v {ty})"));
        let sql = format!("INSERT INTO {table} VALUES ({expression})");
        match expected {
            Ok(value) => {
                execute(&mut s, &c, &sql);
                expect_text(
                    &mut s,
                    &c,
                    &format!("SELECT v::text FROM {table}"),
                    &[Some(value)],
                );
            }
            Err(error) => expect_error(&mut s, &c, &sql, error),
        }
    }
}

#[test]
fn pgdump_fixture_inserts_accept_unknown_bytea_and_timestamp_literals() {
    let f = Fixture::new();
    let mut s = f.session();
    let c = clock("UTC");
    // Verbatim statements from compat/pgdump/fixture.sql in the r0 harness.
    for sql in [
        "CREATE TABLE r0p_users (id integer PRIMARY KEY, email text NOT NULL, active boolean, created timestamptz);",
        "CREATE TABLE r0p_orders (id bigint PRIMARY KEY, user_id integer, total_cents bigint, note text);",
        "CREATE TABLE r0p_blobs (id integer PRIMARY KEY, data bytea);",
        "CREATE INDEX r0p_orders_user_id ON r0p_orders(user_id);",
        "INSERT INTO r0p_users VALUES (1, 'alice@example.test', true, '2024-01-02 03:04:05+00');",
        "INSERT INTO r0p_users VALUES (2, 'bob@example.test', false, '2024-02-03 04:05:06+00');",
        "INSERT INTO r0p_users VALUES (3, 'carol@example.test', NULL, NULL);",
        "INSERT INTO r0p_orders VALUES (101, 1, 1250, 'first order');",
        "INSERT INTO r0p_orders VALUES (102, 2, 0, NULL);",
        "INSERT INTO r0p_orders VALUES (103, 1, 922337203685477580, 'quote '' and newline');",
        r"INSERT INTO r0p_blobs VALUES (1, '\x0001ff');",
        r"INSERT INTO r0p_blobs VALUES (2, '\x');",
        "INSERT INTO r0p_blobs VALUES (3, NULL);",
    ] {
        execute(&mut s, &c, sql);
    }
    expect_text(
        &mut s,
        &c,
        "SELECT data::text FROM r0p_blobs ORDER BY id",
        &[Some(r"\x0001ff"), Some(r"\x"), None],
    );
}

fn at(zone: &str, micros: i64) -> StatementClock {
    let mut c = clock(zone);
    c.transaction_us = micros;
    c.statement_us = micros;
    c
}

// Exercise both the INSERT input route and UPDATE expression assignment route.
fn assignment(
    s: &mut SqlSession,
    c: &StatementClock,
    table: &str,
    ty: &str,
    expr: &str,
    value: &str,
) {
    execute(s, c, &format!("CREATE TABLE {table} (v {ty})"));
    execute(s, c, &format!("INSERT INTO {table} VALUES ({expr})"));
    let query = format!("SELECT v::text FROM {table}");
    expect_text(s, c, &query, &[Some(value)]);
    execute(s, c, &format!("UPDATE {table} SET v=NULL"));
    execute(s, c, &format!("UPDATE {table} SET v={expr}"));
    expect_text(s, c, &query, &[Some(value)]);
}

#[test]
fn special_datetime_words_use_the_execution_clock_in_both_assignment_routes() {
    let f = Fixture::new();
    let mut s = f.session();
    for (z, zone) in ["UTC", "America/Chicago"].into_iter().enumerate() {
        let c = at(zone, 763_416_000_000_000); // 2024-03-10 20:00 UTC.
        let chicago = z == 1;
        let now = ["2024-03-10 20:00:00", "2024-03-10 15:00:00"][z];
        let tz_now = ["2024-03-10 20:00:00+00", "2024-03-10 15:00:00-05"][z];
        let tz_dates = if chicago {
            [
                "2024-03-10 00:00:00-06",
                "2024-03-11 00:00:00-05",
                "2024-03-09 00:00:00-06",
                "1969-12-31 18:00:00-06",
            ]
        } else {
            [
                "2024-03-10 00:00:00+00",
                "2024-03-11 00:00:00+00",
                "2024-03-09 00:00:00+00",
                "1970-01-01 00:00:00+00",
            ]
        };
        let mut cases = vec![
            ("date", " NOW ", "2024-03-10"),
            ("timestamp", " NOW ", now),
            ("timestamptz", " NOW ", tz_now),
            ("time", "now", ["20:00:00", "15:00:00"][z]),
            ("timetz", "now", ["20:00:00+00", "15:00:00-05"][z]),
            ("time", "allballs", "00:00:00"),
            ("timetz", "allballs", "00:00:00+00"),
        ];
        for (i, (word, date, timestamp)) in [
            ("Today", "2024-03-10", "2024-03-10 00:00:00"),
            ("tomorrow", "2024-03-11", "2024-03-11 00:00:00"),
            ("yesterday", "2024-03-09", "2024-03-09 00:00:00"),
            ("epoch", "1970-01-01", "1970-01-01 00:00:00"),
        ]
        .into_iter()
        .enumerate()
        {
            cases.extend([
                ("date", word, date),
                ("timestamp", word, timestamp),
                ("timestamptz", word, tz_dates[i]),
            ]);
        }
        for (i, (ty, word, expected)) in cases.into_iter().enumerate() {
            assignment(
                &mut s,
                &c,
                &format!("special_{z}_{i}"),
                ty,
                &format!("'{word}'"),
                expected,
            );
        }
        for (i, (ty, word)) in [
            ("time", "today"),
            ("time", "epoch"),
            ("timetz", "tomorrow"),
            ("date", "allballs"),
        ]
        .into_iter()
        .enumerate()
        {
            let table = format!("invalid_special_{z}_{i}");
            execute(&mut s, &c, &format!("CREATE TABLE {table} (v {ty})"));
            expect_error(
                &mut s,
                &c,
                &format!("INSERT INTO {table} VALUES ('{word}')"),
                SqlError::InvalidDatetimeFormat,
            );
            execute(&mut s, &c, &format!("INSERT INTO {table} VALUES (NULL)"));
            expect_error(
                &mut s,
                &c,
                &format!("UPDATE {table} SET v='{word}'"),
                SqlError::InvalidDatetimeFormat,
            );
        }
        expect_text(
            &mut s,
            &c,
            "SELECT ('today'::date)::text",
            &[Some("2024-03-10")],
        );
        expect_text(
            &mut s,
            &c,
            "SELECT (' NOW '::timestamptz)::text",
            &[Some(tz_now)],
        );
    }
}

#[test]
fn update_precheck_keeps_duplicate_errors_and_execution_clock_separate() {
    let f = Fixture::new();
    let mut s = f.session();
    let c = at("Asia/Kolkata", 763_416_000_000_000);
    execute(
        &mut s,
        &c,
        "CREATE TABLE u (id int PRIMARY KEY,d date,ts timestamp,tz timestamptz,t time,tt time with time zone,n int)",
    );
    execute(&mut s, &c, "INSERT INTO u (id) VALUES (1)");
    for (column, word, expected) in [
        ("d", "today", "2024-03-11"),
        ("ts", "now", "2024-03-11 01:30:00"),
        ("tz", "now", "2024-03-11 01:30:00+05:30"),
        ("tt", "now", "01:30:00+05:30"),
    ] {
        execute(
            &mut s,
            &c,
            &format!("UPDATE u SET {column}='{word}',n=1 WHERE id=1"),
        );
        expect_text(
            &mut s,
            &c,
            &format!("SELECT {column}::text FROM u WHERE id=1"),
            &[Some(expected)],
        );
    }
    execute(&mut s, &c, "INSERT INTO u (id,d) VALUES (2,'today')");
    expect_text(
        &mut s,
        &c,
        "SELECT d::text FROM u WHERE id=2",
        &[Some("2024-03-11")],
    );
    let rows = |s: &mut SqlSession| {
        s.execute_at("SELECT * FROM u ORDER BY id", &[], None, &c)
            .unwrap()
            .rows
    };
    let before = rows(&mut s);
    for (column, word) in [("d", "today"), ("ts", "now"), ("tt", "now")] {
        expect_error(
            &mut s,
            &c,
            &format!("UPDATE u SET {column}='{word}',{column}='{word}' WHERE id=1"),
            SqlError::Syntax,
        );
        assert_eq!(rows(&mut s), before);
    }
    expect_error(
        &mut s,
        &c,
        "UPDATE u SET d='bad',d='x' WHERE id=1",
        SqlError::InvalidDatetimeFormat,
    );
    assert_eq!(rows(&mut s), before);
    assert_eq!(
        s.execute_at("UPDATE u SET d='today',n=1 WHERE id=99", &[], None, &c)
            .unwrap()
            .affected,
        0
    );
    assert_eq!(rows(&mut s), before);
}

#[test]
fn offset_free_timetz_uses_the_statement_date_for_the_session_offset() {
    let f = Fixture::new();
    let mut s = f.session();
    for (i, (zone, instant, expected)) in [
        ("America/Chicago", 847_368_000_000_000, "01:02:00-06"),
        ("America/Chicago", 836_222_400_000_000, "01:02:00-05"),
        ("UTC", 847_368_000_000_000, "01:02:00+00"),
    ]
    .into_iter()
    .enumerate()
    {
        let c = at(zone, instant);
        assignment(
            &mut s,
            &c,
            &format!("offset_{i}"),
            "timetz",
            "'01:02'",
            expected,
        );
        expect_text(
            &mut s,
            &c,
            "SELECT ('01:02'::timetz)::text",
            &[Some(expected)],
        );
        assignment(
            &mut s,
            &c,
            &format!("explicit_offset_{i}"),
            "timetz",
            "'12:34:56+05:30'",
            "12:34:56+05:30",
        );
        expect_text(
            &mut s,
            &c,
            "SELECT ('12:34:56+05:30'::timetz)::text",
            &[Some("12:34:56+05:30")],
        );
    }
}

#[test]
fn typed_assignment_pairs_preserve_temporal_and_numeric_semantics() {
    let f = Fixture::new();
    let mut s = f.session();
    for (z, zone) in ["UTC", "America/Chicago"].into_iter().enumerate() {
        let c = at(zone, 847_368_000_000_000);
        for (i, (ty, expression, expected)) in [
            ("integer", "1.5", "2"),
            ("integer", "2.5", "3"),
            ("integer", "-2.5", "-3"),
            ("smallint", "1.5", "2"),
            ("text", "'2024-01-02'::date", "2024-01-02"),
            ("text", "'1 day'::interval", "1 day"),
            ("text", "'ab'::bytea", r"\x6162"),
            ("date", "'2024-01-02 03:04'::timestamp", "2024-01-02"),
            (
                "date",
                "'2024-01-02 03:04+00'::timestamptz",
                ["2024-01-02", "2024-01-01"][z],
            ),
            ("time", "'2024-01-02 03:04'::timestamp", "03:04:00"),
            ("time", "'25 hours'::interval", "01:00:00"),
            ("time", "'01:02+03'::timetz", "01:02:00"),
            ("timetz", "'01:02'::time", ["01:02:00+00", "01:02:00-06"][z]),
            (
                "timetz",
                "'2024-01-02 03:04+00'::timestamptz",
                ["03:04:00+00", "21:04:00-06"][z],
            ),
            ("timestamp", "'2024-01-02'::date", "2024-01-02 00:00:00"),
            (
                "timestamp",
                "'2024-01-02 03:04+00'::timestamptz",
                ["2024-01-02 03:04:00", "2024-01-01 21:04:00"][z],
            ),
            (
                "timestamptz",
                "'2024-01-02'::date",
                ["2024-01-02 00:00:00+00", "2024-01-02 00:00:00-06"][z],
            ),
            (
                "timestamptz",
                "'2024-01-02 03:04'::timestamp",
                ["2024-01-02 03:04:00+00", "2024-01-02 03:04:00-06"][z],
            ),
            ("interval", "'01:02'::time", "01:02:00"),
        ]
        .into_iter()
        .enumerate()
        {
            assignment(
                &mut s,
                &c,
                &format!("pair_{z}_{i}"),
                ty,
                expression,
                expected,
            );
        }
    }
}

#[test]
fn typed_text_and_unrelated_values_are_not_unknown_input_literals() {
    let f = Fixture::new();
    let mut s = f.session();
    let c = clock("UTC");
    let mut cases = vec![
        ("bytea", "1"),
        ("bytea", "1.5"),
        ("bytea", "true"),
        ("bytea", "'2024-01-02'::date"),
        ("boolean", "1"),
        ("boolean", "1.5"),
        ("integer", "true"),
        ("integer", "'2024-01-02'::date"),
        ("integer", "'1 day'::interval"),
        ("numeric", "true"),
        ("time", "'2024-01-02'::date"),
        ("date", "'01:02'::time"),
        ("date", "1"),
        ("time", "1"),
        ("timestamp", "1"),
        ("interval", "1"),
    ];
    for ty in [
        "bytea",
        "boolean",
        "smallint",
        "integer",
        "bigint",
        "numeric",
        "real",
        "double precision",
        "date",
        "time",
        "timetz",
        "timestamp",
        "timestamptz",
        "interval",
    ] {
        cases.push((ty, "'1'::text"));
    }
    for (i, (ty, expression)) in cases.into_iter().enumerate() {
        let table = format!("rejected_pair_{i}");
        execute(&mut s, &c, &format!("CREATE TABLE {table} (v {ty})"));
        expect_error(
            &mut s,
            &c,
            &format!("INSERT INTO {table} VALUES ({expression})"),
            SqlError::Type,
        );
        execute(&mut s, &c, &format!("INSERT INTO {table} VALUES (NULL)"));
        expect_error(
            &mut s,
            &c,
            &format!("UPDATE {table} SET v={expression}"),
            SqlError::Type,
        );
        expect_text(&mut s, &c, &format!("SELECT v::text FROM {table}"), &[None]);
    }
}

#[test]
fn prepared_special_literal_casts_observe_each_execution_clock() {
    let f = Fixture::new();
    let mut s = f.session();
    let p = s
        .prepare("SELECT ('today'::date)::text, (' NOW '::timestamptz)::text")
        .unwrap();
    for (micros, date, timestamp) in [
        (763_416_000_000_000, "2024-03-10", "2024-03-10 20:00:00+00"),
        (847_368_000_000_000, "2026-11-07", "2026-11-07 12:00:00+00"),
    ] {
        let result = s
            .execute_prepared_at(&p, &[], None, &at("UTC", micros))
            .unwrap();
        assert_eq!(
            result.rows[0].values,
            vec![Value::Text(date.into()), Value::Text(timestamp.into())]
        );
    }
}
