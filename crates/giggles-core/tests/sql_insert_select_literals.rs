// All 94 INSERT SELECT rows, with equivalent fresh-fixture VALUES witnesses, from DD-0068's PostgreSQL 18.6 F18 oracle.
// Source: J:/gigglesdb-work/dd68-f18/probe/insert_select_for_e3.txt and probe_f18.py.
// The oracle excludes numeric(5,2), whose CREATE TABLE is unsupported.
use giggles_core::{Admin, CoreError, Limits, relational::*};
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
        let path =
            PathBuf::from(std::env::var_os("TEMP").expect("task TEMP configured")).join(format!(
                "insert-select-literals-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
        std::fs::create_dir_all(&path).unwrap();
        Self {
            admin: Some(Admin::create(path.join("db.redb"), Limits::default()).unwrap()),
            path,
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        drop(self.admin.take());
        std::fs::remove_dir_all(&self.path).expect("remove insert-select literal fixture");
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

// These mappings agree with the public server mappings for oracle error kinds.
// Unexpected errors fail immediately instead of being normalized to an oracle code.
fn sqlstate(error: &CoreError) -> &str {
    match error {
        CoreError::Sql(SqlError::Syntax) => "42601",
        CoreError::Sql(SqlError::Unsupported | SqlError::Reserved) => "0A000",
        CoreError::Sql(SqlError::Type | SqlError::Parameters) => "42804",
        CoreError::Sql(SqlError::Overflow) => "22003",
        CoreError::Sql(SqlError::InvalidTextRepresentation) => "22P02",
        CoreError::Sql(SqlError::InvalidParameterValue) => "22023",
        CoreError::Sql(SqlError::InvalidDatetimeFormat) => "22007",
        CoreError::Sql(SqlError::DatetimeFieldOverflow) => "22008",
        CoreError::Sql(SqlError::InvalidTimeZoneDisplacementValue) => "22009",
        CoreError::Sql(SqlError::IntervalFieldOverflow) => "22015",
        _ => panic!("unexpected INSERT SELECT error: {error:?}"),
    }
}

// Normalize native readback to the oracle's PG wire text representation.
// PG wire `t` is checked as native Value::Bool(true), with explicit t/f
// normalization. All other values retain their exact PG text representation.
fn wire_text(value: &Value, zone: SessionZone) -> String {
    match value {
        Value::Bool(true) => "t".into(),
        Value::Bool(false) => "f".into(),
        Value::Int(value) => value.to_string(),
        Value::Text(value) => value.clone(),
        Value::Bytes(value) => {
            let mut text = String::from(r"\x");
            for byte in value {
                use std::fmt::Write;
                write!(text, "{byte:02x}").unwrap();
            }
            text
        }
        Value::Numeric(value) => value.to_string(),
        Value::Float4(value) => value.to_text(),
        Value::Float8(value) => value.to_text(),
        Value::Date(value) => value.to_string(),
        Value::Time(value) => value.to_string(),
        Value::TimeTz(value) => value.to_string(),
        Value::Timestamp(value) => value.to_string(),
        Value::TimestampTz(value) => value.format_in_zone(zone).unwrap(),
        Value::Interval(value) => value.to_string(),
        Value::Null => panic!("oracle successful rows have no NULL values"),
    }
}

#[derive(Debug)]
enum PgExpected {
    Readback([&'static str; 2]),
    Sqlstate(&'static str),
}
struct Case {
    name: &'static str,
    key: &'static str,
    ty: &'static str,
    literal: &'static str,
    pg: PgExpected,
    // Every error row pins an exact current variant. Shared divergences must
    // receive an explicit per-row expectation and ownership comment.
    expected_error: Option<SqlError>,
}

#[derive(Debug, PartialEq, Eq)]
enum Observed {
    Success {
        affected: u64,
        rows: Vec<Vec<Value>>,
    },
    Error(CoreError),
}
fn observe(case: &Case, clock: &StatementClock, select: bool) -> Observed {
    let fixture = Fixture::new();
    let mut session = fixture.admin.as_ref().unwrap().sql_session();
    let table = format!("f18_{}", case.key);
    session
        .execute_at(
            &format!(
                "CREATE TABLE {table} (id integer PRIMARY KEY, v {})",
                case.ty
            ),
            &[],
            None,
            clock,
        )
        .unwrap();
    let source = if select {
        format!("SELECT 1, {}", case.literal)
    } else {
        format!("VALUES (1, {})", case.literal)
    };
    let sql = format!("INSERT INTO {table} (id, v) {source}");
    let result = session.execute_at(&sql, &[], None, clock);
    let rows = session
        .execute_at(
            &format!("SELECT id, v FROM {table} ORDER BY id"),
            &[],
            None,
            clock,
        )
        .unwrap()
        .rows
        .into_iter()
        .map(|row| row.values)
        .collect::<Vec<_>>();
    match result {
        Ok(result) => Observed::Success {
            affected: result.affected,
            rows,
        },
        Err(error) => {
            assert!(
                rows.is_empty(),
                "{}: rejected INSERT changed the table",
                sql
            );
            Observed::Error(error)
        }
    }
}
fn describe(observed: &Observed, zone: SessionZone) -> String {
    match observed {
        Observed::Success { affected, rows } => {
            let rows = rows
                .iter()
                .map(|row| {
                    row.iter()
                        .map(|value| wire_text(value, zone))
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();
            format!("OK affected={affected} readback={rows:?}")
        }
        Observed::Error(error) => format!("ERROR {} ({error:?})", sqlstate(error)),
    }
}

#[test]
fn all_94_postgresql_f18_rows_match_values_and_pinned_literal_inputs() {
    let cases = [
        Case {
            name: "bool select",
            key: "bool",
            ty: "boolean",
            literal: "'t'",
            pg: PgExpected::Readback(["t", "t"]),
            expected_error: None,
        },
        Case {
            name: "bool bad word select",
            key: "bool",
            ty: "boolean",
            literal: "'maybe'",
            pg: PgExpected::Sqlstate("22P02"),
            expected_error: Some(SqlError::InvalidTextRepresentation),
        },
        Case {
            name: "bool bad empty select",
            key: "bool",
            ty: "boolean",
            literal: "''",
            pg: PgExpected::Sqlstate("22P02"),
            expected_error: Some(SqlError::InvalidTextRepresentation),
        },
        Case {
            name: "int2 select",
            key: "int2",
            ty: "smallint",
            literal: "'42'",
            pg: PgExpected::Readback(["42", "42"]),
            expected_error: None,
        },
        Case {
            name: "int2 bad range select",
            key: "int2",
            ty: "smallint",
            literal: "'40000'",
            pg: PgExpected::Sqlstate("22003"),
            expected_error: Some(SqlError::Overflow),
        },
        Case {
            name: "int2 bad alpha select",
            key: "int2",
            ty: "smallint",
            literal: "'4x'",
            pg: PgExpected::Sqlstate("22P02"),
            expected_error: Some(SqlError::InvalidTextRepresentation),
        },
        Case {
            name: "int2 bad frac select",
            key: "int2",
            ty: "smallint",
            literal: "'1.5'",
            pg: PgExpected::Sqlstate("22P02"),
            expected_error: Some(SqlError::InvalidTextRepresentation),
        },
        Case {
            name: "int4 select",
            key: "int4",
            ty: "integer",
            literal: "'2147483647'",
            pg: PgExpected::Readback(["2147483647", "2147483647"]),
            expected_error: None,
        },
        Case {
            name: "int4 bad range select",
            key: "int4",
            ty: "integer",
            literal: "'2147483648'",
            pg: PgExpected::Sqlstate("22003"),
            expected_error: Some(SqlError::Overflow),
        },
        Case {
            name: "int4 bad empty select",
            key: "int4",
            ty: "integer",
            literal: "''",
            pg: PgExpected::Sqlstate("22P02"),
            expected_error: Some(SqlError::InvalidTextRepresentation),
        },
        Case {
            name: "int8 select",
            key: "int8",
            ty: "bigint",
            literal: "'922337203685477580'",
            pg: PgExpected::Readback(["922337203685477580", "922337203685477580"]),
            expected_error: None,
        },
        Case {
            name: "int8 bad range select",
            key: "int8",
            ty: "bigint",
            literal: "'9223372036854775808'",
            pg: PgExpected::Sqlstate("22003"),
            expected_error: Some(SqlError::Overflow),
        },
        Case {
            name: "int8 bad alpha select",
            key: "int8",
            ty: "bigint",
            literal: "'abc'",
            pg: PgExpected::Sqlstate("22P02"),
            expected_error: Some(SqlError::InvalidTextRepresentation),
        },
        Case {
            name: "num select",
            key: "num",
            ty: "numeric",
            literal: "'12.3400'",
            pg: PgExpected::Readback(["12.3400", "12.3400"]),
            expected_error: None,
        },
        Case {
            name: "num bad alpha select",
            key: "num",
            ty: "numeric",
            literal: "'1e'",
            pg: PgExpected::Sqlstate("22P02"),
            expected_error: Some(SqlError::InvalidTextRepresentation),
        },
        Case {
            name: "num bad empty select",
            key: "num",
            ty: "numeric",
            literal: "' '",
            pg: PgExpected::Sqlstate("22P02"),
            expected_error: Some(SqlError::InvalidTextRepresentation),
        },
        Case {
            name: "f4 select",
            key: "f4",
            ty: "real",
            literal: "'1.5'",
            pg: PgExpected::Readback(["1.5", "1.5"]),
            expected_error: None,
        },
        Case {
            name: "f4 bad range select",
            key: "f4",
            ty: "real",
            literal: "'1e40'",
            pg: PgExpected::Sqlstate("22003"),
            expected_error: Some(SqlError::Overflow),
        },
        // PG18.6 22P02 invalid_text_representation
        Case {
            name: "f4 bad alpha select",
            key: "f4",
            ty: "real",
            literal: "'one'",
            pg: PgExpected::Sqlstate("22P02"),
            expected_error: Some(SqlError::InvalidTextRepresentation),
        },
        Case {
            name: "f8 select",
            key: "f8",
            ty: "double precision",
            literal: "'0.1'",
            pg: PgExpected::Readback(["0.1", "0.1"]),
            expected_error: None,
        },
        Case {
            name: "f8 bad range select",
            key: "f8",
            ty: "double precision",
            literal: "'1e400'",
            pg: PgExpected::Sqlstate("22003"),
            expected_error: Some(SqlError::Overflow),
        },
        // PG18.6 22P02 invalid_text_representation
        Case {
            name: "f8 bad alpha select",
            key: "f8",
            ty: "double precision",
            literal: "'1..2'",
            pg: PgExpected::Sqlstate("22P02"),
            expected_error: Some(SqlError::InvalidTextRepresentation),
        },
        Case {
            name: "text select",
            key: "text",
            ty: "text",
            literal: "'hello'",
            pg: PgExpected::Readback(["hello", "hello"]),
            expected_error: None,
        },
        Case {
            name: "bytea select",
            key: "bytea",
            ty: "bytea",
            literal: "'\\x0001ff'",
            pg: PgExpected::Readback(["\\x0001ff", "\\x0001ff"]),
            expected_error: None,
        },
        Case {
            name: "bytea bad hexodd select",
            key: "bytea",
            ty: "bytea",
            literal: "'\\x123'",
            pg: PgExpected::Sqlstate("22023"),
            expected_error: Some(SqlError::InvalidParameterValue),
        },
        Case {
            name: "bytea bad hexbad select",
            key: "bytea",
            ty: "bytea",
            literal: "'\\xzz'",
            pg: PgExpected::Sqlstate("22023"),
            expected_error: Some(SqlError::InvalidParameterValue),
        },
        Case {
            name: "bytea bad escape select",
            key: "bytea",
            ty: "bytea",
            literal: "'a\\q'",
            pg: PgExpected::Sqlstate("22P02"),
            expected_error: Some(SqlError::InvalidTextRepresentation),
        },
        Case {
            name: "bytea_esc select",
            key: "bytea_esc",
            ty: "bytea",
            literal: "'abc\\000'",
            pg: PgExpected::Readback(["\\x61626300", "\\x61626300"]),
            expected_error: None,
        },
        Case {
            name: "date select",
            key: "date",
            ty: "date",
            literal: "'2024-02-29'",
            pg: PgExpected::Readback(["2024-02-29", "2024-02-29"]),
            expected_error: None,
        },
        Case {
            name: "date bad field select",
            key: "date",
            ty: "date",
            literal: "'2023-02-29'",
            pg: PgExpected::Sqlstate("22008"),
            expected_error: Some(SqlError::DatetimeFieldOverflow),
        },
        Case {
            name: "date bad syntax select",
            key: "date",
            ty: "date",
            literal: "'notadate'",
            pg: PgExpected::Sqlstate("22007"),
            expected_error: Some(SqlError::InvalidDatetimeFormat),
        },
        Case {
            name: "date bad range select",
            key: "date",
            ty: "date",
            literal: "'5874898-01-01'",
            pg: PgExpected::Sqlstate("22008"),
            expected_error: Some(SqlError::DatetimeFieldOverflow),
        },
        Case {
            name: "time select",
            key: "time",
            ty: "time",
            literal: "'12:34:56.789'",
            pg: PgExpected::Readback(["12:34:56.789", "12:34:56.789"]),
            expected_error: None,
        },
        Case {
            name: "time bad field select",
            key: "time",
            ty: "time",
            literal: "'25:00'",
            pg: PgExpected::Sqlstate("22008"),
            expected_error: Some(SqlError::DatetimeFieldOverflow),
        },
        Case {
            name: "time bad syntax select",
            key: "time",
            ty: "time",
            literal: "'noon-ish'",
            pg: PgExpected::Sqlstate("22023"),
            expected_error: Some(SqlError::InvalidParameterValue),
        },
        Case {
            name: "timetz select",
            key: "timetz",
            ty: "time with time zone",
            literal: "'12:34:56+05:30'",
            pg: PgExpected::Readback(["12:34:56+05:30", "12:34:56+05:30"]),
            expected_error: None,
        },
        Case {
            name: "timetz bad field select",
            key: "timetz",
            ty: "time with time zone",
            literal: "'12:00+16'",
            pg: PgExpected::Sqlstate("22009"),
            expected_error: Some(SqlError::InvalidTimeZoneDisplacementValue),
        },
        Case {
            name: "timetz bad syntax select",
            key: "timetz",
            ty: "time with time zone",
            literal: "'x'",
            pg: PgExpected::Sqlstate("22007"),
            expected_error: Some(SqlError::InvalidDatetimeFormat),
        },
        Case {
            name: "ts select",
            key: "ts",
            ty: "timestamp",
            literal: "'2024-01-02 03:04:05.5'",
            pg: PgExpected::Readback(["2024-01-02 03:04:05.5", "2024-01-02 03:04:05.5"]),
            expected_error: None,
        },
        Case {
            name: "ts bad field select",
            key: "ts",
            ty: "timestamp",
            literal: "'2024-01-32 00:00'",
            pg: PgExpected::Sqlstate("22008"),
            expected_error: Some(SqlError::DatetimeFieldOverflow),
        },
        Case {
            name: "ts bad syntax select",
            key: "ts",
            ty: "timestamp",
            literal: "'yesterday-ish'",
            pg: PgExpected::Sqlstate("22007"),
            expected_error: Some(SqlError::InvalidDatetimeFormat),
        },
        Case {
            name: "tstz select",
            key: "tstz",
            ty: "timestamptz",
            literal: "'2024-07-01 12:00:00+00'",
            pg: PgExpected::Readback(["2024-07-01 12:00:00+00", "2024-07-01 07:00:00-05"]),
            expected_error: None,
        },
        Case {
            name: "tstz bad field select",
            key: "tstz",
            ty: "timestamptz",
            literal: "'2024-07-01 25:00'",
            pg: PgExpected::Sqlstate("22008"),
            expected_error: Some(SqlError::DatetimeFieldOverflow),
        },
        Case {
            name: "tstz bad syntax select",
            key: "tstz",
            ty: "timestamptz",
            literal: "'nope'",
            pg: PgExpected::Sqlstate("22007"),
            expected_error: Some(SqlError::InvalidDatetimeFormat),
        },
        Case {
            name: "iv select",
            key: "iv",
            ty: "interval",
            literal: "'1 day 02:03:04'",
            pg: PgExpected::Readback(["1 day 02:03:04", "1 day 02:03:04"]),
            expected_error: None,
        },
        Case {
            name: "iv bad syntax select",
            key: "iv",
            ty: "interval",
            literal: "'bogus'",
            pg: PgExpected::Sqlstate("22007"),
            expected_error: Some(SqlError::InvalidDatetimeFormat),
        },
        Case {
            name: "iv bad range select",
            key: "iv",
            ty: "interval",
            literal: "'2147483648 days'",
            pg: PgExpected::Sqlstate("22015"),
            expected_error: Some(SqlError::IntervalFieldOverflow),
        },
    ];
    assert_eq!(
        cases.len(),
        47,
        "47 oracle rows in each of two session zones"
    );
    let mut checked = 0;
    let mut unexpected = Vec::new();
    for (z, zone) in ["UTC", "America/Chicago"].into_iter().enumerate() {
        let clock = clock(zone);
        for case in &cases {
            let select = observe(case, &clock, true);
            let values = observe(case, &clock, false);
            let selected = describe(&select, clock.zone.unwrap());
            let inserted = describe(&values, clock.zone.unwrap());
            let pg = match &case.pg {
                PgExpected::Readback(expected) => format!("OK readback=[[1, {:?}]]", expected[z]),
                PgExpected::Sqlstate(state) => format!("ERROR {state}"),
            };
            let context = format!(
                "{} [{zone}], literal {}: PG18.6 {pg}; SELECT {selected}; VALUES {inserted}",
                case.name, case.literal
            );
            // Compare exact native variants and values, including the affected
            // count, before considering any pinned baseline divergence.
            assert_eq!(select, values, "SELECT/VALUES divergence: {context}");
            let matches = match (&case.expected_error, &case.pg, &select) {
                (Some(expected), _, Observed::Error(error)) => {
                    error == &CoreError::Sql(expected.clone())
                }
                (None, PgExpected::Readback(expected), Observed::Success { affected, rows }) => {
                    let actual = rows
                        .iter()
                        .map(|row| {
                            row.iter()
                                .map(|value| wire_text(value, clock.zone.unwrap()))
                                .collect::<Vec<_>>()
                        })
                        .collect::<Vec<_>>();
                    *affected == 1 && actual == vec![vec!["1".to_owned(), expected[z].to_owned()]]
                }
                _ => false,
            };
            if !matches {
                // Evaluate the complete matrix to report new shared mismatches,
                // but fail the suite until every current result is explicitly pinned.
                unexpected.push(context.clone());
            }
            eprintln!("{context}");
            checked += 1;
        }
    }
    assert_eq!(checked, 94);
    assert!(
        unexpected.is_empty(),
        "unrecognized literal-input results:\n{}",
        unexpected.join("\n")
    );
}
