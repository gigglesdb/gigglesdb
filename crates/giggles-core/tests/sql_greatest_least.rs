// PostgreSQL 18.6 GREATEST/LEAST compatibility expectations.
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
        let path = PathBuf::from("J:/gigglesdb-work/dd68-f17/tmp").join(format!(
            "greatest-least-{}-{}",
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
fn scalar(
    s: &mut SqlSession,
    c: &StatementClock,
    expr: &str,
    ty: DataType,
    expected: Option<&str>,
) {
    let sql = format!("SELECT {expr}");
    let p = s.prepare(&sql).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    assert_eq!(s.describe(&p).unwrap().columns[0].data_type, ty, "{sql}");
    let sql = format!("SELECT ({expr})::text");
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
fn numeric_text_null_and_composition_match_pg18() {
    use DataType::*;
    let f = Fixture::new();
    let mut s = f.session();
    let c = clock("UTC");
    for (expr, ty, expected) in [
        ("greatest(1::int2,2::int2)", SmallInt, Some("2")),
        ("least(1,2.5)", Numeric, Some("1")),
        ("greatest(1,2.5::float8)", DoublePrecision, Some("2.5")),
        ("greatest(1::int8,2.5::float4)", Real, Some("2.5")),
        ("greatest(1.5::float4,2.5::numeric)", Real, Some("2.5")),
        (
            "greatest(1.5::float4,2.5::float8)",
            DoublePrecision,
            Some("2.5"),
        ),
        ("least(1.5,2.5::float8)", DoublePrecision, Some("1.5")),
        ("greatest('NaN'::float8,1)", DoublePrecision, Some("NaN")),
        ("least('NaN'::float8,1)", DoublePrecision, Some("1")),
        ("greatest('NaN'::numeric,1)", Numeric, Some("NaN")),
        ("least('NaN'::numeric,1)", Numeric, Some("1")),
        ("greatest(NULL,1,NULL)", Integer, Some("1")),
        ("least(2,NULL)", Integer, Some("2")),
        ("least(NULL::int,NULL)", Integer, None),
        ("least('a','b')", Text, Some("a")),
        ("greatest('a','B')", Text, Some("a")),
        ("least('é','z')", Text, Some("z")),
        ("greatest('x'::text,'y')", Text, Some("y")),
        (
            "CASE WHEN greatest(1,2)=2 THEN 'y' ELSE 'n' END",
            Text,
            Some("y"),
        ),
        ("greatest(1,2)>1", Boolean, Some("true")),
        ("greatest(1,3,2)", Integer, Some("3")),
        ("least(1,3,2)", Integer, Some("1")),
        ("greatest(5)", Integer, Some("5")),
        ("greatest(1::int2,2::int8)", BigInt, Some("2")),
        ("least(1::int2,2::int2)", SmallInt, Some("1")),
        ("greatest(1,2.5)", Numeric, Some("2.5")),
        ("greatest(1,2::float8)", DoublePrecision, Some("2")),
        ("greatest(1::int8,2::float4)", Real, Some("2")),
        ("greatest(1::float4,2::numeric)", Real, Some("2")),
        ("greatest(1::float4,2::float8)", DoublePrecision, Some("2")),
        ("greatest(1::numeric,2::float8)", DoublePrecision, Some("2")),
        ("greatest(1.0,1.00)", Numeric, Some("1.0")),
        ("least(1.00,1.0)", Numeric, Some("1.00")),
        (
            "greatest(1::float8,'NaN'::float8)",
            DoublePrecision,
            Some("NaN"),
        ),
        ("least('NaN'::float8,1::float8)", DoublePrecision, Some("1")),
        ("greatest(1::numeric,'NaN'::numeric)", Numeric, Some("NaN")),
        ("least('NaN'::numeric,1::numeric)", Numeric, Some("1")),
        (
            "greatest('-0'::float8,'0'::float8)",
            DoublePrecision,
            Some("-0"),
        ),
        (
            "least('0'::float8,'-0'::float8)",
            DoublePrecision,
            Some("0"),
        ),
        ("greatest(NULL,2,NULL,1)", Integer, Some("2")),
        ("least(NULL,2,NULL,1)", Integer, Some("1")),
        ("greatest(NULL,NULL)", Text, None),
        ("least(NULL,NULL)", Text, None),
        ("greatest(NULL::int,NULL)", Integer, None),
        ("greatest(NULL::int8,1)", BigInt, Some("1")),
        ("greatest('a','b')", Text, Some("b")),
        ("least('a','B')", Text, Some("B")),
        ("greatest('ab','a')", Text, Some("ab")),
        ("greatest('é','z')", Text, Some("é")),
        ("greatest('a'::text,'b')", Text, Some("b")),
        ("greatest(1,'2')", Integer, Some("2")),
        ("least('0',1)", Integer, Some("0")),
        ("greatest(true,false)", Boolean, Some("true")),
        ("least(true,false)", Boolean, Some("false")),
        (
            r"greatest('\x01'::bytea,'\x0100'::bytea)",
            Bytes,
            Some(r"\x0100"),
        ),
        (
            r"least('\x01'::bytea,'\x0100'::bytea)",
            Bytes,
            Some(r"\x01"),
        ),
        ("greatest(1,least(5,3))+1", Integer, Some("4")),
        ("greatest(2,3)*2", Integer, Some("6")),
        ("format('%s',greatest(1,2))", Text, Some("2")),
        ("coalesce(greatest(NULL,NULL::int),7)", Integer, Some("7")),
        (
            "CASE WHEN greatest(1,2)=2 THEN 7 ELSE 0 END",
            Integer,
            Some("7"),
        ),
        ("greatest(1,2)>least(2,3)", Boolean, Some("false")),
    ] {
        scalar(&mut s, &c, expr, ty, expected);
    }
}
#[test]
fn temporal_results_and_timezone_coercion_match_pg18() {
    use DataType::*;
    let f = Fixture::new();
    let mut s = f.session();
    for zone in ["UTC", "America/Chicago"] {
        let c = clock(zone);
        let utc = zone == "UTC";
        for (expr, ty, expected) in [
            (
                "greatest(date '2024-01-01',date '2023-01-01')",
                Date,
                "2024-01-01",
            ),
            (
                "greatest(date '2024-01-01',timestamp '2024-01-01 01:00')",
                Timestamp,
                "2024-01-01 01:00:00",
            ),
            (
                "greatest(timestamp '2024-01-01 01:00',date '2024-01-01')",
                Timestamp,
                "2024-01-01 01:00:00",
            ),
            (
                "greatest(interval '1 day',interval '23 hours')",
                Interval,
                "1 day",
            ),
            (
                "least('-infinity'::date,date '2024-01-01')",
                Date,
                "-infinity",
            ),
            (
                "greatest(date '2024-07-01',date '2024-07-02')",
                Date,
                "2024-07-02",
            ),
            (
                "greatest(date '2024-07-01','2024-07-02')",
                Date,
                "2024-07-02",
            ),
            (
                "greatest(date '2024-07-01',timestamp '2024-07-01 10:00')",
                Timestamp,
                "2024-07-01 10:00:00",
            ),
            (
                "least(timestamp '2024-07-01 10:00',date '2024-07-01')",
                Timestamp,
                "2024-07-01 00:00:00",
            ),
            (
                "greatest(date '2024-07-01',timestamptz '2024-07-01 03:00+00')",
                TimestampTz,
                if utc {
                    "2024-07-01 03:00:00+00"
                } else {
                    "2024-07-01 00:00:00-05"
                },
            ),
            (
                "least(timestamptz '2024-07-01 12:00+00',timestamp '2024-07-01 10:00')",
                TimestampTz,
                if utc {
                    "2024-07-01 10:00:00+00"
                } else {
                    "2024-07-01 07:00:00-05"
                },
            ),
            (
                "greatest(timestamp '2024-07-01 10:00',timestamptz '2024-07-01 12:00+00')",
                TimestampTz,
                if utc {
                    "2024-07-01 12:00:00+00"
                } else {
                    "2024-07-01 10:00:00-05"
                },
            ),
            (
                "greatest(timestamptz '2024-07-01 12:00+00','2024-07-01 10:00')",
                TimestampTz,
                if utc {
                    "2024-07-01 12:00:00+00"
                } else {
                    "2024-07-01 10:00:00-05"
                },
            ),
            (
                "greatest(interval '1 day',interval '24 hours')",
                Interval,
                "1 day",
            ),
            (
                "least(interval '24 hours',interval '1 day')",
                Interval,
                "24:00:00",
            ),
            (
                "least(interval '1 day',interval '23 hours')",
                Interval,
                "23:00:00",
            ),
            (
                "greatest(date 'infinity',date '2024-01-01')",
                Date,
                "infinity",
            ),
            (
                "least(timestamp '-infinity',timestamp '2024-01-01')",
                Timestamp,
                "-infinity",
            ),
            (
                "greatest(timestamptz 'infinity',now())",
                TimestampTz,
                "infinity",
            ),
            (
                "least(timestamptz '-infinity',now())",
                TimestampTz,
                "-infinity",
            ),
            (
                "greatest(interval 'infinity',interval '1 day')",
                Interval,
                "infinity",
            ),
        ] {
            scalar(&mut s, &c, expr, ty, Some(expected));
        }
    }
}
#[test]
fn incompatible_types_and_routine_spelling_fail_at_prepare() {
    let f = Fixture::new();
    let mut s = f.session();
    for (expr, error) in [
        (
            "least(time '10:00',date '2024-01-01')",
            SqlError::CannotCoerce,
        ),
        ("greatest(1,'x')", SqlError::InvalidTextRepresentation),
        ("greatest(1,'2'::text)", SqlError::Type),
        ("greatest(1,NULL::text)", SqlError::Type),
        ("greatest(1,true)", SqlError::Type),
        ("greatest('2'::text,1)", SqlError::Type),
        ("greatest(1,'2'::text,true)", SqlError::Type),
        (
            "greatest(date '2024-01-01',timestamp '2024-01-01','x'::text)",
            SqlError::Type,
        ),
        (
            "greatest(date '2024-01-01',time '10:00')",
            SqlError::CannotCoerce,
        ),
        (
            "greatest(time '10:00',date '2024-01-01')",
            SqlError::CannotCoerce,
        ),
        (
            "greatest(date '2024-01-01',time '10:00',timestamp '2024-01-01')",
            SqlError::CannotCoerce,
        ),
        (
            "greatest(timetz '10:00+00',timestamp '2024-01-01')",
            SqlError::CannotCoerce,
        ),
        (
            "greatest(time '10:00',timetz '10:00+00')",
            SqlError::Unsupported,
        ),
        ("greatest(interval '1 day',time '10:00')", SqlError::Type),
        ("greatest(interval '1 day',1)", SqlError::Type),
        ("greatest(date '2024-01-01',1)", SqlError::Type),
        ("greatest()", SqlError::Syntax),
        ("least()", SqlError::Syntax),
        ("\"greatest\"(1,2)", SqlError::UndefinedFunction),
        ("pg_catalog.greatest(1,2)", SqlError::UndefinedFunction),
        ("\"least\"(1,2)", SqlError::UndefinedFunction),
        ("pg_catalog.least(1,2)", SqlError::UndefinedFunction),
    ] {
        let sql = format!("SELECT {expr}");
        let actual = match s.prepare(&sql) {
            Err(e) => e,
            Ok(p) => s.describe(&p).unwrap_err(),
        };
        assert_eq!(actual, CoreError::Sql(error), "{sql}");
    }
}
fn table(s: &mut SqlSession) {
    s.execute("CREATE TABLE gt(id integer primary key,a integer,i2 smallint,b numeric,s text,d date,ts timestamp,tz timestamptz,f double precision)", &[], None).unwrap();
    s.execute("INSERT INTO gt VALUES (1,1,7,2.5,'a','2024-07-01','2024-07-01 10:00','2024-07-01 12:00+00',0.1),(2,9,3,1,'z','2024-07-02','2024-07-01 23:00','2024-07-02 01:00+00',1e16),(3,4,NULL,NULL,NULL,NULL,'2024-07-01 10:00',NULL,NULL),(4,NULL,NULL,4,'m','2024-07-01',NULL,'2024-06-30 23:00+00','NaN')", &[], None).unwrap();
}
fn rows(s: &mut SqlSession, sql: &str, types: &[DataType], expected: &[&[Option<&str>]]) {
    let p = s.prepare(sql).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    assert_eq!(
        s.describe(&p)
            .unwrap()
            .columns
            .iter()
            .map(|c| c.data_type.clone())
            .collect::<Vec<_>>(),
        types,
        "{sql}"
    );
    let actual = s
        .execute_at(sql, &[], None, &clock("UTC"))
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    // Use the same PostgreSQL text formatting as scalar tests, preserving NULL.
    let actual: Vec<Vec<Option<String>>> = actual
        .rows
        .into_iter()
        .map(|r| {
            r.values
                .into_iter()
                .map(|v| match v {
                    Value::Null => None,
                    Value::Text(v) => Some(v),
                    Value::Int(v) => Some(v.to_string()),
                    other => panic!("unexpected value {other:?}: {sql}"),
                })
                .collect()
        })
        .collect();
    let expected: Vec<Vec<Option<String>>> = expected
        .iter()
        .map(|r| r.iter().map(|v| v.map(str::to_owned)).collect())
        .collect();
    assert_eq!(actual, expected, "{sql}");
}
#[test]
fn table_projection_filters_ordering_and_temporal_rows_match_pg18() {
    use DataType::*;
    let f = Fixture::new();
    let mut s = f.session();
    table(&mut s);
    let p = s.prepare("SELECT greatest(a,i2),least(a,b),greatest(s,'m'),greatest(f,a),greatest(d,ts),least(ts,tz),greatest(d,tz) FROM gt").unwrap();
    assert_eq!(
        s.describe(&p)
            .unwrap()
            .columns
            .iter()
            .map(|c| c.data_type.clone())
            .collect::<Vec<_>>(),
        vec![
            Integer,
            Numeric,
            Text,
            DoublePrecision,
            Timestamp,
            TimestampTz,
            TimestampTz
        ]
    );
    rows(
        &mut s,
        "SELECT greatest(a,i2)::text,least(a,b)::text,greatest(s,'m') FROM gt ORDER BY id",
        &[Text, Text, Text],
        &[
            &[Some("7"), Some("1"), Some("m")],
            &[Some("9"), Some("1"), Some("z")],
            &[Some("4"), Some("4"), Some("m")],
            &[None, Some("4"), Some("m")],
        ],
    );
    for (sql, ids) in [
        (
            "SELECT id FROM gt WHERE greatest(a,i2)>5 ORDER BY id",
            vec![1, 2],
        ),
        (
            "SELECT id FROM gt WHERE least(a,b)<=2 ORDER BY id",
            vec![1, 2],
        ),
        (
            "SELECT id FROM gt WHERE greatest(s,'m')='m' ORDER BY id",
            vec![1, 3, 4],
        ),
        (
            "SELECT id FROM gt ORDER BY greatest(a,i2) DESC,id",
            vec![4, 2, 1, 3],
        ),
        (
            "SELECT id FROM gt ORDER BY least(b,100),id",
            vec![2, 1, 4, 3],
        ),
    ] {
        let p = s.prepare(sql).unwrap();
        assert_eq!(s.describe(&p).unwrap().columns[0].data_type, Integer);
        assert_eq!(
            s.execute(sql, &[], None)
                .unwrap()
                .rows
                .into_iter()
                .map(|r| r.values)
                .collect::<Vec<_>>(),
            ids.into_iter()
                .map(|i| vec![Value::Int(i)])
                .collect::<Vec<_>>(),
            "{sql}"
        );
    }
    rows(
        &mut s,
        "SELECT greatest(f,a)::text FROM gt ORDER BY id",
        &[Text],
        &[&[Some("1")], &[Some("1e+16")], &[Some("4")], &[Some("NaN")]],
    );
    rows(
        &mut s,
        "SELECT greatest(d,ts)::text,least(ts,tz)::text,greatest(d,tz)::text FROM gt ORDER BY id",
        &[Text, Text, Text],
        &[
            &[
                Some("2024-07-01 10:00:00"),
                Some("2024-07-01 10:00:00+00"),
                Some("2024-07-01 12:00:00+00"),
            ],
            &[
                Some("2024-07-02 00:00:00"),
                Some("2024-07-01 23:00:00+00"),
                Some("2024-07-02 01:00:00+00"),
            ],
            &[
                Some("2024-07-01 10:00:00"),
                Some("2024-07-01 10:00:00+00"),
                None,
            ],
            &[
                Some("2024-07-01 00:00:00"),
                Some("2024-06-30 23:00:00+00"),
                Some("2024-07-01 00:00:00+00"),
            ],
        ],
    );
}
#[test]
fn parameter_inference_and_declarations_match_pg18() {
    use DataType::*;
    let f = Fixture::new();
    let mut s = f.session();
    for (expr, declared, params, inferred, ty, expected) in [
        (
            "greatest($1,2)",
            vec![],
            vec![Value::Int(5)],
            vec![Integer],
            Integer,
            "5",
        ),
        (
            "greatest($1,$2)",
            vec![],
            vec![Value::Text("a".into()), Value::Text("b".into())],
            vec![Text, Text],
            Text,
            "b",
        ),
        (
            "least($1,2.5)",
            vec![],
            vec![Value::Numeric(
                giggles_pg_types::Numeric::parse("3").unwrap(),
            )],
            vec![Numeric],
            Numeric,
            "2.5",
        ),
        (
            "greatest($1,2)",
            vec![Some(BigInt)],
            vec![Value::Int(5)],
            vec![BigInt],
            BigInt,
            "5",
        ),
        (
            "greatest($1,timestamp '2024-01-01 01:00')",
            vec![Some(Date)],
            vec![Value::Date(
                giggles_pg_types::temporal::Date::parse(
                    "2024-01-01",
                    &giggles_pg_types::temporal::ParserSettings::default(),
                )
                .unwrap(),
            )],
            vec![Date],
            Timestamp,
            "2024-01-01 01:00:00",
        ),
        (
            "greatest($1::int,2.5)",
            vec![],
            vec![Value::Int(3)],
            vec![Integer],
            Numeric,
            "3",
        ),
        (
            "greatest($1,2)",
            vec![Some(Integer)],
            vec![Value::Null],
            vec![Integer],
            Integer,
            "2",
        ),
    ] {
        let sql = format!("SELECT {expr}");
        let p = s
            .prepare(&sql)
            .unwrap()
            .with_parameter_types(&declared)
            .unwrap();
        let d = s.describe(&p).unwrap();
        assert_eq!(d.parameters, inferred, "{sql}");
        assert_eq!(d.columns[0].data_type, ty, "{sql}");
        let text_sql = format!("SELECT ({expr})::text");
        let p = s
            .prepare(&text_sql)
            .unwrap()
            .with_parameter_types(&declared)
            .unwrap();
        assert_eq!(
            s.execute_prepared_at(&p, &params, None, &clock("UTC"))
                .unwrap()
                .rows[0]
                .values,
            vec![Value::Text(expected.into())],
            "{sql}"
        );
    }
    let invalid = s
        .prepare("SELECT greatest($1,2)")
        .unwrap()
        .with_parameter_types(&[Some(Text)]);
    match invalid {
        Err(e) => assert_eq!(e, SqlError::Type),
        Ok(p) => assert_eq!(s.describe(&p).unwrap_err(), CoreError::Sql(SqlError::Type)),
    }
    table(&mut s);
    let p = s
        .prepare("SELECT id FROM gt WHERE greatest(a,$1)=$1 ORDER BY id")
        .unwrap();
    assert_eq!(s.describe(&p).unwrap().parameters, vec![Integer]);
    assert_eq!(
        s.execute_prepared_at(&p, &[Value::Int(4)], None, &clock("UTC"))
            .unwrap()
            .rows
            .into_iter()
            .map(|r| r.values)
            .collect::<Vec<_>>(),
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(3)],
            vec![Value::Int(4)]
        ]
    );
}
