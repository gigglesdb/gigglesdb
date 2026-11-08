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
            "dd68-assignment-{}-{}",
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
fn clock(chicago: bool) -> StatementClock {
    StatementClock {
        unavailable: None,
        transaction_us: 757_382_400_000_000,
        statement_us: 757_382_400_000_000,
        zone: Some(if chicago {
            SessionZone::named("America/Chicago").unwrap()
        } else {
            SessionZone::Fixed(0)
        }),
        wall: None,
    }
}
fn value(s: &mut SqlSession, sql: &str, c: &StatementClock) -> Value {
    s.execute_at(&format!("SELECT {sql}"), &[], None, c)
        .unwrap()
        .rows
        .remove(0)
        .values
        .remove(0)
}

#[test]
fn declared_scalar_assignment_insert_multi_and_update() {
    use DataType::*;
    let fixture = Fixture::new();
    let mut s = fixture.session();
    let c = clock(false);
    let cases = [
        (Numeric, "'1.5'::numeric", "INT", "2"),
        (Numeric, "'-1.5'::numeric", "SMALLINT", "-2"),
        (Numeric, "'2.5'::numeric", "BIGINT", "3"),
        (Real, "'2.5'::real", "INT", "2"),
        (DoublePrecision, "'3.5'::float8", "INT", "4"),
        (Real, "'7.5'::real", "SMALLINT", "8"),
        (Real, "'2.5'::real", "BIGINT", "2"),
        (DoublePrecision, "'2.5'::float8", "SMALLINT", "2"),
        (DoublePrecision, "'7.5'::float8", "BIGINT", "8"),
        (
            Numeric,
            "'9007199254740993'::numeric",
            "BIGINT",
            "9007199254740993",
        ),
        (Numeric, "'1.25'::numeric", "REAL", "'1.25'::real"),
        (Numeric, "'1.25'::numeric", "FLOAT8", "'1.25'::float8"),
        (DoublePrecision, "'1.25'::float8", "REAL", "'1.25'::real"),
        (
            Real,
            "'0.1'::real",
            "FLOAT8",
            "'0.10000000149011612'::float8",
        ),
        (Real, "'1.23456789'::real", "NUMERIC", "'1.23457'::numeric"),
        (
            DoublePrecision,
            "'1.2345678901234567'::float8",
            "NUMERIC",
            "'1.23456789012346'::numeric",
        ),
        (Boolean, "true", "TEXT", "'true'"),
        (Numeric, "'1.2300'::numeric", "TEXT", "'1.2300'"),
        (Real, "'1.25'::real", "TEXT", "'1.25'"),
        (DoublePrecision, "'1.25'::float8", "TEXT", "'1.25'"),
        (
            Date,
            "'2024-01-02'::date",
            "TIMESTAMP",
            "'2024-01-02 00:00'::timestamp",
        ),
        (
            Date,
            "'infinity'::date",
            "TIMESTAMP",
            "'infinity'::timestamp",
        ),
        (
            Timestamp,
            "'-infinity'::timestamp",
            "DATE",
            "'-infinity'::date",
        ),
        (
            Date,
            "'2024-01-02'::date",
            "TIMESTAMPTZ",
            "'2024-01-02 00:00+00'::timestamptz",
        ),
        (
            Timestamp,
            "'2024-01-02 03:04'::timestamp",
            "DATE",
            "'2024-01-02'::date",
        ),
        (
            Timestamp,
            "'2024-01-02 03:04'::timestamp",
            "TIMESTAMPTZ",
            "'2024-01-02 03:04+00'::timestamptz",
        ),
        (
            Timestamp,
            "'2024-01-02 03:04'::timestamp",
            "TIME",
            "'03:04'::time",
        ),
        (
            TimestampTz,
            "'2024-01-02 03:04+00'::timestamptz",
            "DATE",
            "'2024-01-02'::date",
        ),
        (
            TimestampTz,
            "'2024-01-02 03:04+00'::timestamptz",
            "TIMESTAMP",
            "'2024-01-02 03:04'::timestamp",
        ),
        (
            TimestampTz,
            "'2024-01-02 03:04+00'::timestamptz",
            "TIMETZ",
            "'03:04+00'::timetz",
        ),
        (Time, "'03:04'::time", "TIMETZ", "'03:04+00'::timetz"),
        (TimeTz, "'03:04+05'::timetz", "TIME", "'03:04'::time"),
        (Interval, "'-1 hour'::interval", "TIME", "'23:00'::time"),
        (Time, "'03:04'::time", "INTERVAL", "'03:04'::interval"),
        (Date, "'2024-01-02'::date", "TEXT", "'2024-01-02'"),
        (Time, "'03:04'::time", "TEXT", "'03:04:00'"),
        (TimeTz, "'03:04+05'::timetz", "TEXT", "'03:04:00+05'"),
        (
            Timestamp,
            "'2024-01-02 03:04'::timestamp",
            "TEXT",
            "'2024-01-02 03:04:00'",
        ),
        (
            TimestampTz,
            "'2024-01-02 03:04+00'::timestamptz",
            "TEXT",
            "'2024-01-02 03:04:00+00'",
        ),
        (Interval, "'03:04'::interval", "TEXT", "'03:04:00'"),
        (Boolean, "false", "TEXT", "'false'"),
        (Text, "'unchanged'", "TEXT", "'unchanged'"),
        (
            Interval,
            "'2 days 03:04'::interval",
            "TIME",
            "'03:04'::time",
        ),
        (Time, "'24:00'::time", "INTERVAL", "'24 hours'::interval"),
        (Real, "'NaN'::real", "NUMERIC", "'NaN'::numeric"),
        (Real, "'Infinity'::real", "NUMERIC", "'Infinity'::numeric"),
        (Real, "'-Infinity'::real", "NUMERIC", "'-Infinity'::numeric"),
        (
            DoublePrecision,
            "'NaN'::float8",
            "NUMERIC",
            "'NaN'::numeric",
        ),
        (
            DoublePrecision,
            "'Infinity'::float8",
            "NUMERIC",
            "'Infinity'::numeric",
        ),
        (
            DoublePrecision,
            "'-Infinity'::float8",
            "NUMERIC",
            "'-Infinity'::numeric",
        ),
    ];
    for (i, (source, input, target, expected)) in cases.into_iter().enumerate() {
        let name = format!("a{i}");
        s.execute(
            &format!("CREATE TABLE {name}(id INT PRIMARY KEY,v {target})"),
            &[],
            None,
        )
        .unwrap();
        let input = value(&mut s, input, &c);
        let expected = value(&mut s, expected, &c);
        for (sql, count) in [
            (format!("INSERT INTO {name} VALUES(1,$1) RETURNING v"), 1),
            (
                format!("INSERT INTO {name} VALUES(2,$1),(3,$1) RETURNING v"),
                2,
            ),
            (format!("UPDATE {name} SET v=$1 WHERE id=1 RETURNING v"), 1),
        ] {
            let p = s
                .prepare(&sql)
                .unwrap()
                .with_parameter_types(&[Some(source.clone())])
                .unwrap();
            assert_eq!(
                s.describe(&p).unwrap().parameters,
                vec![source.clone()],
                "{sql}"
            );
            let rows = s
                .execute_prepared_at(&p, std::slice::from_ref(&input), None, &c)
                .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
                .rows;
            assert_eq!(rows.len(), count, "{sql}");
            for row in rows {
                assert_eq!(row.values, vec![expected.clone()], "{sql}");
            }
        }
    }
}

#[test]
fn assignment_rejections_and_declaration_provenance() {
    use DataType::*;
    let f = Fixture::new();
    let mut s = f.session();
    s.execute(
        "CREATE TABLE t(id INT PRIMARY KEY,i INT,d DATE,b BOOLEAN,tm TIME)",
        &[],
        None,
    )
    .unwrap();
    for (source, column) in [
        (Text, "i"),
        (Text, "d"),
        (Text, "b"),
        (Boolean, "i"),
        (Date, "tm"),
        (Date, "i"),
        (Interval, "d"),
        (Numeric, "d"),
        (Timestamp, "i"),
    ] {
        let p = s
            .prepare(&format!("INSERT INTO t(id,{column}) VALUES(1,$1)"))
            .unwrap()
            .with_parameter_types(&[Some(source)])
            .unwrap();
        assert_eq!(s.describe(&p).unwrap_err(), CoreError::Sql(SqlError::Type));
    }
    let undeclared = s.prepare("INSERT INTO t(id,i) VALUES(1,$1)").unwrap();
    assert_eq!(
        s.execute_prepared(
            &undeclared,
            &[Value::Numeric(
                giggles_pg_types::Numeric::parse("1.5").unwrap()
            )],
            None
        ),
        Err(CoreError::Sql(SqlError::Type))
    );
    for declarations in [&[][..], &[None][..]] {
        let p = s
            .prepare("INSERT INTO t(id,i) VALUES(1,$1)")
            .unwrap()
            .with_parameter_types(declarations)
            .unwrap();
        assert_eq!(
            s.execute_prepared(
                &p,
                &[Value::Numeric(
                    giggles_pg_types::Numeric::parse("1.5").unwrap()
                )],
                None
            ),
            Err(CoreError::Sql(SqlError::Type))
        );
    }
}

#[test]
fn assignment_numeric_errors_and_declared_limit() {
    use DataType::*;
    let f = Fixture::new();
    let mut s = f.session();
    let c = clock(false);
    s.execute("CREATE TABLE t(id INT PRIMARY KEY,i INT)", &[], None)
        .unwrap();
    for (case, (source, input, target, error)) in [
        (Numeric, "'Infinity'::numeric", "INT", SqlError::Overflow),
        (Numeric, "'-Infinity'::numeric", "INT", SqlError::Overflow),
        (Numeric, "'NaN'::numeric", "INT", SqlError::Unsupported),
        (
            Numeric,
            "'32767.5'::numeric",
            "SMALLINT",
            SqlError::Overflow,
        ),
        (
            Numeric,
            "'-32768.5'::numeric",
            "SMALLINT",
            SqlError::Overflow,
        ),
        (Numeric, "'2147483648'::numeric", "INT", SqlError::Overflow),
        (
            Numeric,
            "'9223372036854775807.5'::numeric",
            "BIGINT",
            SqlError::Overflow,
        ),
        (
            Numeric,
            "'-9223372036854775808.5'::numeric",
            "BIGINT",
            SqlError::Overflow,
        ),
        (Numeric, "'1e39'::numeric", "REAL", SqlError::Overflow),
        (
            DoublePrecision,
            "'1e39'::float8",
            "REAL",
            SqlError::Overflow,
        ),
        (
            DoublePrecision,
            "'2147483648'::float8",
            "INT",
            SqlError::Overflow,
        ),
        (
            DoublePrecision,
            "'9223372036854775808'::float8",
            "BIGINT",
            SqlError::Overflow,
        ),
        (DoublePrecision, "'NaN'::float8", "INT", SqlError::Overflow),
        (
            DoublePrecision,
            "'Infinity'::float8",
            "INT",
            SqlError::Overflow,
        ),
        (
            DoublePrecision,
            "'-Infinity'::float8",
            "INT",
            SqlError::Overflow,
        ),
        (Real, "'Infinity'::real", "INT", SqlError::Overflow),
        (Real, "'-Infinity'::real", "INT", SqlError::Overflow),
        (Real, "'NaN'::real", "INT", SqlError::Overflow),
    ]
    .into_iter()
    .enumerate()
    {
        let name = format!("errors{case}");
        s.execute(
            &format!("CREATE TABLE {name}(id INT PRIMARY KEY,v {target})"),
            &[],
            None,
        )
        .unwrap();
        s.execute(&format!("INSERT INTO {name} VALUES(1,0)"), &[], None)
            .unwrap();
        let input = value(&mut s, input, &c);
        for sql in [
            format!("INSERT INTO {name} VALUES(2,$1) RETURNING v"),
            format!("INSERT INTO {name} VALUES(3,$1),(4,$1) RETURNING v"),
            format!("UPDATE {name} SET v=$1 WHERE id=1 RETURNING v"),
        ] {
            let p = s
                .prepare(&sql)
                .unwrap()
                .with_parameter_types(&[Some(source.clone())])
                .unwrap();
            assert_eq!(
                s.execute_prepared_at(&p, std::slice::from_ref(&input), None, &c),
                Err(CoreError::Sql(error.clone())),
                "{sql}, {input:?}"
            );
        }
    }
    s.execute("INSERT INTO t VALUES(1,1),(2,2),(3,3)", &[], None)
        .unwrap();
    let p = s
        .prepare("SELECT id FROM t ORDER BY id LIMIT $1")
        .unwrap()
        .with_parameter_types(&[Some(Numeric)])
        .unwrap();
    let input = value(&mut s, "'1.5'::numeric", &c);
    assert_eq!(
        s.execute_prepared(&p, &[input], None).unwrap().rows.len(),
        2
    );
    for declarations in [&[][..], &[None][..]] {
        let inferred = s
            .prepare("SELECT $1::numeric FROM t LIMIT $1")
            .unwrap()
            .with_parameter_types(declarations)
            .unwrap();
        let input = value(&mut s, "'1.5'::numeric", &c);
        assert_eq!(
            s.execute_prepared(&inferred, &[input], None),
            Err(CoreError::Sql(SqlError::Limit))
        );
    }
    let input = value(&mut s, "'-1.5'::numeric", &c);
    assert_eq!(
        s.execute_prepared(&p, &[input], None),
        Err(CoreError::Sql(SqlError::Limit))
    );
    let input = value(&mut s, "'9223372036854775808'::numeric", &c);
    assert_eq!(
        s.execute_prepared(&p, &[input], None),
        Err(CoreError::Sql(SqlError::Overflow))
    );
}

#[test]
fn assignment_uses_session_zone_and_transaction_date() {
    use DataType::*;
    let f = Fixture::new();
    let mut s = f.session();
    let c = clock(true);
    s.execute(
        "CREATE TABLE t(id INT PRIMARY KEY,d DATE,ts TIMESTAMP,tz TIMESTAMPTZ,ttz TIMETZ,s TEXT)",
        &[],
        None,
    )
    .unwrap();
    for (id, source, input, column, expected) in [
        (
            1,
            Date,
            "'2024-01-02'::date",
            "tz",
            "'2024-01-02 06:00+00'::timestamptz",
        ),
        (
            2,
            Timestamp,
            "'2024-01-02 03:04'::timestamp",
            "tz",
            "'2024-01-02 09:04+00'::timestamptz",
        ),
        (
            3,
            TimestampTz,
            "'2024-01-02 03:04+00'::timestamptz",
            "d",
            "'2024-01-01'::date",
        ),
        (
            4,
            TimestampTz,
            "'2024-01-02 03:04+00'::timestamptz",
            "ts",
            "'2024-01-01 21:04'::timestamp",
        ),
        (
            5,
            TimestampTz,
            "'2024-01-02 03:04+00'::timestamptz",
            "ttz",
            "'21:04-06'::timetz",
        ),
        (6, Time, "'03:04'::time", "ttz", "'03:04-06'::timetz"),
        (
            7,
            TimestampTz,
            "'2024-01-02 03:04+00'::timestamptz",
            "s",
            "'2024-01-01 21:04:00-06'",
        ),
    ] {
        let input = value(&mut s, input, &c);
        let expected = value(&mut s, expected, &c);
        let p = s
            .prepare(&format!(
                "INSERT INTO t(id,{column}) VALUES({id},$1) RETURNING {column}"
            ))
            .unwrap()
            .with_parameter_types(&[Some(source)])
            .unwrap();
        assert_eq!(
            s.execute_prepared_at(&p, &[input], None, &c).unwrap().rows[0].values,
            vec![expected]
        );
    }
}
