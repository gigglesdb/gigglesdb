mod common;
use common::*;
use giggles_core::{Admin, Limits, relational::*};

#[test]
fn exact_nonlinear_overloads_and_contexts() {
    let admin = Admin::create(fresh("nonlinear-sql"), Limits::default()).unwrap();
    let mut s = admin.sql_session();
    s.execute(
        "CREATE TABLE nonlinear_empty (id bigint primary key, n numeric)",
        &[],
        None,
    )
    .unwrap();
    for (expr, expected) in [
        ("exp(0::numeric)", "1.0000000000000000"),
        ("ln(2::numeric)", "0.6931471805599453"),
        ("log(100::numeric)", "2.0000000000000000"),
        ("log10(100::numeric)", "2.0000000000000000"),
        ("log(10::numeric,100::numeric)", "2.0000000000000000"),
        ("log(10,100)", "2.0000000000000000"),
    ] {
        for prefix in ["", "pg_catalog."] {
            let query = format!("SELECT {prefix}{expr}");
            let p = s.prepare(&query).unwrap();
            assert_eq!(
                s.describe(&p).unwrap().columns[0].data_type,
                DataType::Numeric,
                "{query}"
            );
            let result = s.execute_prepared(&p, &[], None).unwrap();
            let Value::Numeric(ref n) = result.rows[0].values[0] else {
                panic!("{query}")
            };
            assert_eq!(n.to_string(), expected, "{query}");
            assert!(
                s.execute(&format!("{query} LIMIT 0"), &[], None)
                    .unwrap()
                    .rows
                    .is_empty()
            );
        }
    }
    for name in ["exp", "ln", "log", "log10"] {
        let p = s
            .prepare(&format!("SELECT pg_catalog.{name}($1::numeric)"))
            .unwrap();
        assert_eq!(
            s.describe(&p).unwrap().columns[0].data_type,
            DataType::Numeric
        );
        for (input, expected) in [("NaN", "NaN"), ("Infinity", "Infinity")] {
            let r = s
                .execute_prepared(&p, &[Value::Numeric(Numeric::parse(input).unwrap())], None)
                .unwrap();
            let Value::Numeric(ref n) = r.rows[0].values[0] else {
                panic!("numeric")
            };
            assert_eq!(n.to_string(), expected);
        }
        assert_eq!(
            s.execute_prepared(&p, &[Value::Null], None).unwrap().rows[0].values[0],
            Value::Null
        );
        assert!(
            s.execute(&format!("SELECT {name}(n) FROM nonlinear_empty"), &[], None)
                .unwrap()
                .rows
                .is_empty()
        );
        let inferred = s.prepare(&format!("SELECT {name}($1)")).unwrap();
        assert_eq!(
            s.describe(&inferred).unwrap().columns[0].data_type,
            DataType::DoublePrecision
        );
        assert!(
            s.execute_prepared(
                &inferred,
                &[Value::Float8(Float8::from_bits(1f64.to_bits()))],
                None
            )
            .is_ok()
        );
    }
    let p = s.prepare("SELECT pg_catalog.log($1,$2)").unwrap();
    assert_eq!(
        s.describe(&p).unwrap().columns[0].data_type,
        DataType::Numeric
    );
    let args = ["10", "100"].map(|v| Value::Numeric(Numeric::parse(v).unwrap()));
    assert!(s.execute_prepared(&p, &args, None).is_ok());
    assert_eq!(
        s.execute_prepared(&p, &[Value::Null, args[1].clone()], None)
            .unwrap()
            .rows[0]
            .values[0],
        Value::Null
    );
    for q in [
        "SELECT log(n,n) FROM nonlinear_empty",
        "SELECT log(-1::numeric,2::numeric) LIMIT 0",
    ] {
        assert!(s.execute(q, &[], None).unwrap().rows.is_empty());
    }
    for q in [
        "SELECT ln(0::numeric)",
        "SELECT ln(-1::numeric)",
        "SELECT log(1::numeric,2::numeric)",
        "SELECT log(0::numeric,2::numeric)",
        "SELECT log(10::numeric,-1::numeric)",
        "SELECT ln('-Infinity'::numeric)",
        "SELECT log10(0::numeric)",
        "SELECT log(true,2)",
        "SELECT exp(true)",
        "SELECT log(2::float8,4::float8)",
        "SELECT exp('bad'::numeric) LIMIT 0",
        "SELECT log(NULL::numeric,'bad'::numeric)",
        "SELECT log('bad',2) LIMIT 0",
    ] {
        assert!(s.execute(q, &[], None).is_err(), "{q}");
    }
}

#[test]
fn binary_log_nonfinite_and_strict_null() {
    let admin = Admin::create(fresh("nonlinear-log-special"), Limits::default()).unwrap();
    let mut s = admin.sql_session();
    let p = s
        .prepare("SELECT pg_catalog.log($1::numeric,$2::numeric)")
        .unwrap();
    for (base, value, expected) in [
        ("NaN", "-1", "NaN"),
        ("-1", "NaN", "NaN"),
        ("Infinity", "Infinity", "NaN"),
        ("Infinity", "2", "0"),
        ("10", "Infinity", "Infinity"),
    ] {
        let args = [base, value].map(|v| Value::Numeric(Numeric::parse(v).unwrap()));
        let r = s.execute_prepared(&p, &args, None).unwrap();
        let Value::Numeric(ref n) = r.rows[0].values[0] else {
            panic!("numeric")
        };
        assert_eq!(n.to_string(), expected, "{base}/{value}");
    }
    let n = Value::Numeric(Numeric::parse("10").unwrap());
    for args in [
        [Value::Null, n.clone()],
        [n, Value::Null],
        [Value::Null, Value::Null],
    ] {
        assert_eq!(
            s.execute_prepared(&p, &args, None).unwrap().rows[0].values[0],
            Value::Null
        );
    }
    for q in [
        "SELECT log(10::numeric,'-Infinity'::numeric)",
        "SELECT log('-Infinity'::numeric,2::numeric)",
        "SELECT log(10::numeric,0::numeric)",
        "SELECT log(1::numeric,1::numeric)",
    ] {
        assert!(s.execute(q, &[], None).is_err(), "{q}");
    }
}

#[test]
fn nonlinear_error_precedence_and_display_scale() {
    let admin = Admin::create(fresh("nonlinear-precedence"), Limits::default()).unwrap();
    let mut s = admin.sql_session();
    for (q, expected) in [
        ("SELECT ln(-1::numeric)", "Sql(Overflow)"),
        ("SELECT log(1::numeric,2::numeric)", "Sql(DivisionByZero)"),
        (
            "SELECT log(NULL::numeric,'bad'::numeric)",
            "Sql(InvalidTextRepresentation)",
        ),
        (
            "SELECT log('bad',2) LIMIT 0",
            "Sql(InvalidTextRepresentation)",
        ),
        (
            "SELECT ln('bad'::numeric) LIMIT 0",
            "Sql(InvalidTextRepresentation)",
        ),
    ] {
        assert_eq!(
            format!("{:?}", s.execute(q, &[], None).unwrap_err()),
            expected,
            "{q}"
        );
    }
    let p = s.prepare("SELECT pg_catalog.ln($1::numeric)").unwrap();
    let r = s
        .execute_prepared(
            &p,
            &[Value::Numeric(
                Numeric::parse("1.00000000000000000000000000001").unwrap(),
            )],
            None,
        )
        .unwrap();
    let Value::Numeric(ref n) = r.rows[0].values[0] else {
        panic!("numeric")
    };
    assert_eq!(
        n.to_string(),
        "0.000000000000000000000000000010000000000000000"
    );
    assert_eq!(n.scale(), Some(45));
}
#[test]
fn numeric_power_exact_overload_and_precedence() {
    let admin = Admin::create(fresh("numeric-power-sql"), Limits::default()).unwrap();
    let mut s = admin.sql_session();
    s.execute(
        "CREATE TABLE power_empty (id bigint primary key, n numeric)",
        &[],
        None,
    )
    .unwrap();
    for (base, exp, expected) in [
        ("2", "3", "8.0000000000000000"),
        ("-2", "3", "-8.0000000000000000"),
        ("2", "-3", "0.1250000000000000"),
        ("2", "0.5", "1.4142135623730950"),
        ("NaN", "0", "1"),
        ("1", "NaN", "1"),
        ("0", "0", "1.0000000000000000"),
        ("Infinity", "-1", "0"),
        ("-Infinity", "3", "-Infinity"),
    ] {
        let p = s
            .prepare("SELECT pg_catalog.power($1::numeric,$2::numeric)")
            .unwrap();
        assert_eq!(
            s.describe(&p).unwrap().columns[0].data_type,
            DataType::Numeric
        );
        let args = [base, exp].map(|v| Value::Numeric(Numeric::parse(v).unwrap()));
        let r = s.execute_prepared(&p, &args, None).unwrap();
        let Value::Numeric(ref n) = r.rows[0].values[0] else {
            panic!("numeric")
        };
        assert_eq!(n.to_string(), expected, "{base}^{exp}");
        for args in [
            [Value::Null, args[1].clone()],
            [args[0].clone(), Value::Null],
        ] {
            assert_eq!(
                s.execute_prepared(&p, &args, None).unwrap().rows[0].values[0],
                Value::Null
            );
        }
    }
    for (query, ty) in [
        ("SELECT power(2,3)", DataType::DoublePrecision),
        ("SELECT power(2::numeric,3)", DataType::Numeric),
        ("SELECT power(2,3::numeric)", DataType::Numeric),
        (
            "SELECT power(2::numeric,3::float8)",
            DataType::DoublePrecision,
        ),
        ("SELECT power($1,3::numeric)", DataType::Numeric),
        ("SELECT power(2::numeric,$1)", DataType::Numeric),
        ("SELECT power($1,$2)", DataType::DoublePrecision),
        ("SELECT power('2',3::numeric)", DataType::Numeric),
    ] {
        let p = s.prepare(query).unwrap();
        assert_eq!(s.describe(&p).unwrap().columns[0].data_type, ty, "{query}");
    }
    for q in [
        "SELECT power(n,n) FROM power_empty",
        "SELECT power(-2::numeric,0.5::numeric) LIMIT 0",
    ] {
        assert!(s.execute(q, &[], None).unwrap().rows.is_empty());
    }
    for q in [
        "SELECT power(-2::numeric,0.5::numeric)",
        "SELECT power(0::numeric,-1::numeric)",
        "SELECT power('bad',2::numeric) LIMIT 0",
        "SELECT power(NULL::numeric,'bad'::numeric)",
        "SELECT power(true,2::numeric)",
    ] {
        assert!(s.execute(q, &[], None).is_err(), "{q}");
    }
    let p = s.prepare("SELECT pg_catalog.power($1,3::numeric)").unwrap();
    assert!(
        s.execute_prepared(&p, &[Value::Numeric(Numeric::parse("2").unwrap())], None)
            .is_ok()
    );
}
