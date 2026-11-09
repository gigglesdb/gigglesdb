mod common;
use common::fresh;
use giggles_core::{
    Admin, CoreError, Limits, SqlSession,
    relational::{DataType as T, SqlError as E, StatementClock, TimestampTz, Value},
};
use giggles_pg_types::{temporal::SessionZone, typmod::NumericTypmod};
use std::path::PathBuf;

struct Fixture {
    admin: Option<Admin>,
    path: PathBuf,
}

impl Fixture {
    fn new(group: &str) -> Self {
        let path = fresh(&format!("dd68-cast-type-names-{group}"));
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

fn text(value: &Value) -> Option<String> {
    Some(match value {
        Value::Null => return None,
        Value::Text(value) => value.clone(),
        Value::Int(value) => value.to_string(),
        Value::Numeric(value) => value.to_string(),
        Value::Float4(value) => value.to_text(),
        Value::Float8(value) => value.to_text(),
        Value::Date(value) => value.to_string(),
        Value::Time(value) => value.to_string(),
        Value::TimeTz(value) => value.to_string(),
        Value::Timestamp(value) => value.to_string(),
        Value::TimestampTz(value) => value.to_string(),
        Value::Interval(value) => value.to_string(),
        Value::Bool(value) => if *value { "t" } else { "f" }.into(),
        Value::Bytes(value) => format!("{value:?}"),
    })
}

// Report every row in a group, including describe and execution mismatches.
fn check_query(
    session: &mut SqlSession,
    failures: &mut Vec<String>,
    sql: &str,
    params: &[Value],
    expected_rows: &[&[Option<&str>]],
    expected_types: &[T],
    expected_typmods: &[i32],
) {
    let prepared = match session.prepare(sql) {
        Ok(prepared) => prepared,
        Err(error) => {
            failures.push(format!("{sql}: prepare: {error:?}"));
            return;
        }
    };
    match session.describe(&prepared) {
        Ok(description) => {
            let types: Vec<_> = description
                .columns
                .iter()
                .map(|column| column.data_type.clone())
                .collect();
            if types != expected_types {
                failures.push(format!(
                    "{sql}: types {types:?}, expected {expected_types:?}"
                ));
            }
            if description.typmods != expected_typmods {
                failures.push(format!(
                    "{sql}: typmods {:?}, expected {expected_typmods:?}",
                    description.typmods
                ));
            }
        }
        Err(error) => failures.push(format!("{sql}: describe: {error:?}")),
    }
    // Core receives TimeZone through the execution clock; SET is wire-session SQL.
    let clock = StatementClock {
        unavailable: None,
        transaction_us: 757_382_400_000_000,
        statement_us: 757_382_400_000_000,
        zone: Some(SessionZone::Fixed(0)),
        wall: None,
    };
    match session.execute_prepared_at(&prepared, params, None, &clock) {
        Ok(result) => {
            let rows: Vec<Vec<_>> = result
                .rows
                .iter()
                .map(|row| row.values.iter().map(text).collect())
                .collect();
            let expected: Vec<Vec<_>> = expected_rows
                .iter()
                .map(|row| row.iter().map(|value| value.map(str::to_owned)).collect())
                .collect();
            if rows != expected {
                failures.push(format!("{sql}: rows {rows:?}, expected {expected:?}"));
            }
        }
        Err(error) => failures.push(format!("{sql}: execute: {error:?}")),
    }
}

fn check_scalars(group: &str, cases: &[(&str, Option<&str>, T, i32)]) {
    let fixture = Fixture::new(group);
    let mut session = fixture.session();
    let mut failures = Vec::new();
    for (expression, expected, ty, typmod) in cases {
        check_query(
            &mut session,
            &mut failures,
            &format!("SELECT {expression}"),
            &[],
            &[&[*expected]],
            std::slice::from_ref(ty),
            &[*typmod],
        );
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn character_cast_spelling_values_and_typmods() {
    check_scalars(
        "character",
        &[
            ("'abcdef'::varchar(3)", Some("abc"), T::Varchar, 7),
            ("'abc'::character varying(2)", Some("ab"), T::Varchar, 6),
            ("'abc'::varchar", Some("abc"), T::Varchar, -1),
            ("'ab'::char(4)", Some("ab  "), T::Bpchar, 8),
            ("'abc'::char", Some("a"), T::Bpchar, 5),
            ("'abc'::bpchar", Some("abc"), T::Bpchar, -1),
            ("'abc'::bpchar(2)", Some("ab"), T::Bpchar, 6),
            ("'ab  '::char(4)::varchar", Some("ab"), T::Varchar, -1),
            ("'ab  '::char(4)::text", Some("ab"), T::Text, -1),
            ("'ab'::char(4)::char(6)", Some("ab    "), T::Bpchar, 10),
            ("123::varchar(2)", Some("12"), T::Varchar, 6),
            ("123::char(5)", Some("123  "), T::Bpchar, 9),
            ("1.5::varchar(2)", Some("1."), T::Varchar, 6),
            ("true::varchar(3)", Some("tru"), T::Varchar, 7),
            ("NULL::varchar(3)", None, T::Varchar, 7),
            ("'éa漢x'::varchar(2)", Some("éa"), T::Varchar, 6),
            ("CAST('abcdef' AS varchar(3))", Some("abc"), T::Varchar, 7),
            ("'a'::nchar(3)", Some("a  "), T::Bpchar, 7),
            (
                "'abcdef'::national character varying(3)",
                Some("abc"),
                T::Varchar,
                7,
            ),
            ("'abc'::VaRcHaR(2)", Some("ab"), T::Varchar, 6),
            ("CAST('abc' AS CHARACTER(2))", Some("ab"), T::Bpchar, 6),
            ("'abc'::pg_catalog.varchar(2)", Some("ab"), T::Varchar, 6),
            ("'ab'::\"bpchar\"(4)", Some("ab  "), T::Bpchar, 8),
        ],
    );
}

#[test]
fn numeric_cast_values_and_typmods() {
    check_scalars(
        "numeric",
        &[
            ("1.236::numeric(4,2)", Some("1.24"), T::Numeric, 262150),
            ("1234::numeric(4,-1)", Some("1230"), T::Numeric, 264195),
            ("'NaN'::numeric(4,2)", Some("NaN"), T::Numeric, 262150),
            ("7::int::numeric(3,1)", Some("7.0"), T::Numeric, 196613),
            (
                "1.236::numeric(4,2)::numeric(3,1)",
                Some("1.2"),
                T::Numeric,
                196613,
            ),
            ("1.236::numeric(4,2)+1", Some("2.24"), T::Numeric, -1),
        ],
    );
}

#[test]
fn temporal_precision_and_type_aliases() {
    check_scalars(
        "temporal-aliases",
        &[
            (
                "'2024-01-02 03:04:05.678+00'::timestamp(1) with time zone",
                Some("2024-01-02 03:04:05.7+00"),
                T::TimestampTz,
                1,
            ),
            (
                "'03:04:05.678'::time(2) without time zone",
                Some("03:04:05.68"),
                T::Time,
                2,
            ),
            (
                "'03:04:05.678'::time with time zone",
                Some("03:04:05.678+00"),
                T::TimeTz,
                -1,
            ),
            ("'12:00'::time(7)", Some("12:00:00"), T::Time, 6),
            ("1::double precision", Some("1"), T::DoublePrecision, -1),
            ("CAST(7.25 AS float(24))", Some("7.25"), T::Real, -1),
            ("'1.5'::dec", Some("1.5"), T::Numeric, -1),
        ],
    );
}

#[test]
fn ordinary_temporal_typed_literals_keep_their_values_and_metadata() {
    check_scalars(
        "typed-literals",
        &[
            ("date '2024-01-02'", Some("2024-01-02"), T::Date, -1),
            ("time '03:04:05.678'", Some("03:04:05.678"), T::Time, -1),
            (
                "time with time zone '03:04:05.678+00'",
                Some("03:04:05.678+00"),
                T::TimeTz,
                -1,
            ),
            (
                "timestamp '2024-01-02 03:04:05.678'",
                Some("2024-01-02 03:04:05.678"),
                T::Timestamp,
                -1,
            ),
            (
                "timestamp with time zone '2024-01-02 03:04:05.678+00'",
                Some("2024-01-02 03:04:05.678+00"),
                T::TimestampTz,
                -1,
            ),
        ],
    );
}

#[test]
fn cast_type_name_errors_are_exact() {
    let fixture = Fixture::new("errors");
    let mut session = fixture.session();
    let mut failures = Vec::new();
    for (expression, error) in [
        ("'a'::nosuchtype", E::UndefinedObject),
        ("'a'::\"int\"", E::UndefinedObject),
        ("'a'::double", E::UndefinedObject),
        ("'a'::\"VARCHAR\"", E::UndefinedObject),
        ("'a'::nosuchtype(", E::Syntax),
        ("'a'::public.nosuchtype", E::Unsupported),
        ("'a'::uuid", E::Unsupported),
        ("'a'::\"char\"", E::Unsupported),
        ("'a'::varchar(0)", E::InvalidParameterValue),
        ("'a'::varchar(10485761)", E::InvalidParameterValue),
        ("'a'::varchar(-1)", E::Syntax),
        ("'a'::varchar(2,3)", E::Syntax),
        ("'a'::varchar(", E::Syntax),
        ("1::numeric(1001)", E::InvalidParameterValue),
        ("1::numeric(4,1001)", E::InvalidParameterValue),
        ("1::numeric(1,2,3)", E::InvalidParameterValue),
        ("1::numeric('a')", E::InvalidTextRepresentation),
        ("'a'::bpchar(1,2)", E::InvalidParameterValue),
        ("'a'::bpchar('a')", E::InvalidTextRepresentation),
        ("'12:00'::timetz(-1)", E::InvalidParameterValue),
        ("1::int(3)", E::Syntax),
        ("1::int4(3)", E::Syntax),
        ("'a'::text(3)", E::Syntax),
        ("1.5::float(54)", E::InvalidParameterValue),
        ("'2024-01-01'::timestamptz(1) with time zone", E::Syntax),
        ("'2024-01-01'::timestamp with zone", E::Syntax),
        ("CAST('a' AS varchar(0))", E::InvalidParameterValue),
        ("'1 day'::interval(3)", E::Unsupported),
        ("123.4::numeric(4,2)", E::Overflow),
        ("'Infinity'::numeric(4,2)", E::Overflow),
        ("CAST('a' varchar(3))", E::Syntax),
        ("CAST('a' AS varchar(3)", E::Syntax),
        ("time(2) '03:04:05.678'", E::Unsupported),
        (
            "timestamp(2) with time zone '2024-01-02 03:04:05.678+00'",
            E::Unsupported,
        ),
    ] {
        let sql = format!("SELECT {expression}");
        let actual = session.execute(&sql, &[], None).map(|_| ());
        let expected = Err(CoreError::Sql(error));
        if actual != expected {
            failures.push(format!("{sql}: {actual:?}, expected {expected:?}"));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn bpchar_cast_scalar_consumers_remain_fail_closed() {
    let fixture = Fixture::new("fail-closed");
    let mut session = fixture.session();
    let mut failures = Vec::new();
    for sql in ["SELECT 'a'::char(3)='a'", "SELECT length('ab'::char(4))"] {
        let actual = session.execute(sql, &[], None).map(|_| ());
        let expected = Err(CoreError::Sql(E::Unsupported));
        if actual != expected {
            failures.push(format!("{sql}: {actual:?}, expected {expected:?}"));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn casts_work_in_table_projection_predicates_and_insert_assignment() {
    let fixture = Fixture::new("tables");
    let mut session = fixture.session();
    session
        .execute(
            "CREATE TABLE t (id int PRIMARY KEY,x text,c char(4),v varchar(5),n numeric)",
            &[],
            None,
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO t VALUES (1,'abcdef','ab','abc',12.345),(2,'xy','wxyz','a',-0.05)",
            &[],
            None,
        )
        .unwrap();
    let mut failures = Vec::new();
    for (sql, values, ty, typmod) in [
        (
            "SELECT x::varchar(2) FROM t ORDER BY id",
            ["ab", "xy"],
            T::Varchar,
            6,
        ),
        (
            "SELECT c::varchar FROM t ORDER BY id",
            ["ab", "wxyz"],
            T::Varchar,
            -1,
        ),
        (
            "SELECT v::char(2) FROM t ORDER BY id",
            ["ab", "a "],
            T::Bpchar,
            6,
        ),
        (
            "SELECT n::numeric(3,1) FROM t ORDER BY id",
            ["12.3", "-0.1"],
            T::Numeric,
            196613,
        ),
    ] {
        check_query(
            &mut session,
            &mut failures,
            sql,
            &[],
            &[&[Some(values[0])], &[Some(values[1])]],
            &[ty],
            &[typmod],
        );
    }
    check_query(
        &mut session,
        &mut failures,
        "SELECT id FROM t WHERE x::varchar(2)='ab'",
        &[],
        &[&[Some("1")]],
        &[T::Integer],
        &[-1],
    );
    let insert = "INSERT INTO t VALUES (4,'q','abcdefgh'::char(3),'abcdefgh'::varchar(3),1.236::numeric(4,2))";
    if let Err(error) = session.execute(insert, &[], None) {
        failures.push(format!("{insert}: {error:?}"));
    }
    check_query(
        &mut session,
        &mut failures,
        "SELECT c,v,n FROM t WHERE id=4",
        &[],
        &[&[Some("abc "), Some("abc"), Some("1.24")]],
        &[T::Bpchar, T::Varchar, T::Numeric],
        &[8, 9, -1],
    );
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn text_parameter_cast_applies_character_typmod() {
    let fixture = Fixture::new("parameter");
    let mut session = fixture.session();
    let mut failures = Vec::new();
    let sql = "SELECT $1::varchar(3)";
    let prepared = session
        .prepare(sql)
        .unwrap()
        .with_parameter_types(&[Some(T::Text)])
        .unwrap();
    match session.describe(&prepared) {
        Ok(description) => {
            if description.parameters != [T::Text] {
                failures.push(format!(
                    "{sql}: parameter types {:?}, expected [Text]",
                    description.parameters
                ));
            }
            let types: Vec<_> = description
                .columns
                .iter()
                .map(|column| column.data_type.clone())
                .collect();
            if types != [T::Varchar] || description.typmods != [7] {
                failures.push(format!(
                    "{sql}: metadata {types:?}/{:?}, expected [Varchar]/[7]",
                    description.typmods
                ));
            }
        }
        Err(error) => failures.push(format!("{sql}: describe: {error:?}")),
    }
    match session.execute_prepared(&prepared, &[Value::Text("abcdef".into())], None) {
        Ok(result) => {
            let rows: Vec<Vec<_>> = result
                .rows
                .iter()
                .map(|row| row.values.iter().map(text).collect())
                .collect();
            if rows != [vec![Some("abc".into())]] {
                failures.push(format!("{sql}: rows {rows:?}, expected [[Some(\"abc\")]]"));
            }
        }
        Err(error) => failures.push(format!("{sql}: execute: {error:?}")),
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn parameter_inference_sees_cast_targets_through_typmod_wrappers() {
    let fixture = Fixture::new("parameter-inference");
    let mut session = fixture.session();
    let mut failures = Vec::new();
    for (sql, params, parameter_types, result_type, expected_value) in [
        (
            "SELECT coalesce($1::varchar(3),$2)",
            vec![Value::Text("abcdef".into()), Value::Text("fallback".into())],
            vec![T::Varchar, T::Text],
            T::Text,
            "abc",
        ),
        (
            "SELECT date_trunc($1,$2::timestamptz(2),$3)",
            vec![
                Value::Text("day".into()),
                Value::TimestampTz(TimestampTz::from_microseconds(45_296_123_456).unwrap()),
                Value::Text("UTC".into()),
            ],
            vec![T::Text, T::TimestampTz, T::Text],
            T::TimestampTz,
            "2000-01-01 00:00:00+00",
        ),
    ] {
        match session.prepare(sql).and_then(|p| session.describe(&p)) {
            Ok(description) => {
                if description.parameters != parameter_types {
                    failures.push(format!(
                        "{sql}: parameter types {:?}, expected {parameter_types:?}",
                        description.parameters
                    ));
                }
            }
            Err(error) => failures.push(format!("{sql}: describe parameters: {error:?}")),
        }
        check_query(
            &mut session,
            &mut failures,
            sql,
            &params,
            &[&[Some(expected_value)]],
            &[result_type],
            &[-1],
        );
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn typmod_cast_column_names_follow_the_inner_cast() {
    let fixture = Fixture::new("column-names");
    let mut session = fixture.session();
    session
        .execute("CREATE TABLE t (id int PRIMARY KEY, x text)", &[], None)
        .unwrap();
    session
        .execute("INSERT INTO t VALUES (1,'abcd')", &[], None)
        .unwrap();
    let mut failures = Vec::new();
    for (sql, expected_value, typmod, expected_name) in [
        ("SELECT 'abc'::varchar(3)", "abc", 7, "varchar"),
        ("SELECT x::varchar(2) FROM t", "ab", 6, "x"),
    ] {
        check_query(
            &mut session,
            &mut failures,
            sql,
            &[],
            &[&[Some(expected_value)]],
            &[T::Varchar],
            &[typmod],
        );
        match session.prepare(sql).and_then(|p| session.describe(&p)) {
            Ok(description) => {
                let names: Vec<_> = description
                    .columns
                    .iter()
                    .map(|column| column.name.as_str())
                    .collect();
                if names != [expected_name] {
                    failures.push(format!(
                        "{sql}: names {names:?}, expected [{expected_name:?}]"
                    ));
                }
            }
            Err(error) => failures.push(format!("{sql}: describe names: {error:?}")),
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn numeric_typmod_encoding_matches_postgres() {
    let mut failures = Vec::new();
    for (precision, scale, expected) in [
        (4, 2, 262150),
        (4, 0, 262148),
        (4, -1, 264195),
        (1000, 0, 65536004),
        (1, -1000, 66588),
        (4, -1000, 263196),
        (1000, 2, 65536006),
    ] {
        let modifier = NumericTypmod::new(precision, scale).unwrap();
        let actual = modifier.to_typmod();
        if actual != expected {
            failures.push(format!(
                "numeric({precision},{scale}): {actual}, expected {expected}"
            ));
        }
        if NumericTypmod::from_typmod(expected) != Some(modifier) {
            failures.push(format!(
                "numeric({precision},{scale}): failed decode of {expected}"
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn user_sql_cannot_name_internal_nul_routines() {
    let fixture = Fixture::new("internal-nul-routines");
    let mut session = fixture.session();
    let mut failures = Vec::new();
    for (sql, error) in [
        ("SELECT \"\0cast:typmod\"('x', 3)", E::Syntax),
        ("SELECT \"\0aggregate-filter:apply\"(1,2)", E::Syntax),
        // Our lexer has no U& escape processing: U&"\0000x" lexes as the word u, the operator &, and a
        // quoted identifier holding the literal text \0000x, so it can never produce a NUL byte. The
        // unsupported & operator rejects it (0A000); PG18.6 rejects \0000 in the scanner with 42601.
        (r#"SELECT U&"\0000cast:typmod"('x', 3)"#, E::Unsupported),
        (
            r#"SELECT U&"\0000aggregate-filter:apply"(1)"#,
            E::Unsupported,
        ),
        ("SELECT \"cast:typmod\"('x', 3)", E::UndefinedFunction),
    ] {
        let prepared = session.prepare(sql);
        eprintln!("{sql:?}: prepare: {:?}", prepared.as_ref().map(|_| ()));
        let actual = match prepared {
            Ok(prepared) => {
                let result = session.execute_prepared(&prepared, &[], None);
                eprintln!("{sql:?}: execute: {result:?}");
                result.map(|_| ())
            }
            Err(error) => Err(error),
        };
        let expected = Err(CoreError::Sql(error));
        if actual != expected {
            failures.push(format!("{sql:?}: {actual:?}, expected {expected:?}"));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
