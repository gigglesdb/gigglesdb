mod common;
use common::*;
use giggles_core::{Admin, Limits, relational::*};

#[test]
fn catalog_qualification_preserves_binding_and_metadata() {
    let admin = Admin::create(fresh("scalar-catalog-qualification"), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    for expression in [
        "abs(-2)",
        "sqrt(4::float8)",
        "cbrt('-0'::float8)",
        "scale(1.2300)",
        "trim_scale(1.2300)",
        "pi()",
        "substring('abcd',2,2)",
        "lower('ABC')",
        "sqrt(NULL)",
        "sqrt('4')",
        "power(2::float8,3::float8)",
    ] {
        let plain = session.prepare(&format!("SELECT {expression}")).unwrap();
        let qualified = session
            .prepare(&format!("SELECT pg_catalog.{expression}"))
            .unwrap();
        assert_eq!(
            session.describe(&plain).unwrap().columns,
            session.describe(&qualified).unwrap().columns,
            "{expression}"
        );
        assert_eq!(
            session.execute_prepared(&plain, &[], None).unwrap().rows,
            session
                .execute_prepared(&qualified, &[], None)
                .unwrap()
                .rows,
            "{expression}"
        );
    }
    let prepared = session.prepare("SELECT pg_catalog.sqrt($1)").unwrap();
    assert_eq!(
        session
            .execute_prepared(&prepared, &[Value::Null], None)
            .unwrap()
            .rows[0]
            .values[0],
        Value::Null
    );
    for query in [
        "SELECT public.abs(-1)",
        "SELECT other.pg_catalog.abs(-1)",
        "SELECT pg_catalog.coalesce(NULL,1)",
        "SELECT pg_catalog.substring('abc' FROM 1)",
        "SELECT pg_catalog.abs()",
        "SELECT pg_catalog.sqrt(true)",
        "SELECT pg_catalog.sqrt(-1::float8)",
    ] {
        assert!(session.execute(query, &[], None).is_err(), "{query}");
    }
}

#[test]
fn numeric_sqrt_uses_exact_api_and_preserves_sql_type() {
    let admin = Admin::create(fresh("scalar-numeric-sqrt"), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    for (input, expected) in [
        ("4", "2.000000000000000"),
        ("2.0000", "1.414213562373095"),
        ("0.0000", "0.000000000000000"),
        ("NaN", "NaN"),
        ("Infinity", "Infinity"),
    ] {
        let prepared = session
            .prepare("SELECT pg_catalog.sqrt($1::numeric)")
            .unwrap();
        assert_eq!(
            session.describe(&prepared).unwrap().columns[0].data_type,
            DataType::Numeric
        );
        let value = Value::Numeric(Numeric::parse(input).unwrap());
        let result = session.execute_prepared(&prepared, &[value], None).unwrap();
        assert_eq!(
            result.rows[0].values[0],
            Value::Numeric(Numeric::parse(expected).unwrap()),
            "{input}"
        );
        // Numeric equality ignores scale, so check the actual formatted result too.
        let Value::Numeric(ref n) = result.rows[0].values[0] else {
            panic!("numeric result required")
        };
        assert_eq!(n.to_string(), expected, "{input}");
        assert_eq!(
            session
                .execute_prepared(&prepared, &[Value::Null], None)
                .unwrap()
                .rows[0]
                .values[0],
            Value::Null
        );
    }
    for input in ["-1", "-Infinity"] {
        assert!(
            session
                .execute(&format!("SELECT sqrt('{input}'::numeric)"), &[], None)
                .is_err()
        );
    }
    assert_eq!(
        session
            .execute("SELECT sqrt(NULL::numeric)", &[], None)
            .unwrap()
            .rows[0]
            .values[0],
        Value::Null
    );
    assert!(
        session
            .execute("SELECT power(2::numeric,3::numeric)", &[], None)
            .is_ok()
    );
}

#[test]
fn qualified_host_cbrt_semantics() {
    let admin = Admin::create(fresh("scalar-cbrt-bits"), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    let prepared = session
        .prepare("SELECT pg_catalog.cbrt($1::float8)")
        .unwrap();
    for (input, expected) in [
        (2.5_f64.to_bits(), 0x3ff5b7209557b0ed),
        ((-2.5_f64).to_bits(), 0xbff5b7209557b0ed),
        (1.005_f64.to_bits(), 0x3ff006d0b8d645ed),
        (1, 0x298fffffffffffff),
        (0, 0),
        (0x8000000000000000, 0x8000000000000000),
        (0x7ff0000000000000, 0x7ff0000000000000),
        (0xfff0000000000000, 0xfff0000000000000),
    ] {
        let result = session
            .execute_prepared(&prepared, &[Value::Float8(Float8::from_bits(input))], None)
            .unwrap();
        let Value::Float8(value) = result.rows[0].values[0] else {
            panic!("float8 result required")
        };
        // Host libm may differ from the frozen Linux PG18 bits above.
        // The immutable differential corpus retains those exact failures.
        let expected = f64::from_bits(expected);
        if expected == 0.0 || expected.is_infinite() {
            assert_eq!(value.value().to_bits(), expected.to_bits());
        } else {
            assert!((value.value() - expected).abs() <= expected.abs() * 1e-15);
        }
    }
}

#[test]
fn numeric_sqrt_errors_keep_core_categories_and_eager_arguments() {
    let admin = Admin::create(fresh("scalar-sqrt-errors"), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    for (query, expected) in [
        (
            "SELECT sqrt('-1'::numeric)",
            "Sql(InvalidArgumentForPowerFunction)",
        ),
        (
            "SELECT sqrt('-Infinity'::numeric)",
            "Sql(InvalidArgumentForPowerFunction)",
        ),
        (
            "SELECT sqrt('bad'::numeric)",
            "Sql(InvalidTextRepresentation)",
        ),
        (
            "SELECT sqrt('bad'::numeric) LIMIT 0",
            "Sql(InvalidTextRepresentation)",
        ),
        (
            "SELECT coalesce(NULL::numeric,sqrt('-1'::numeric))",
            "Sql(InvalidArgumentForPowerFunction)",
        ),
    ] {
        assert_eq!(
            format!("{:?}", session.execute(query, &[], None).unwrap_err()),
            expected,
            "{query}"
        );
    }
}
