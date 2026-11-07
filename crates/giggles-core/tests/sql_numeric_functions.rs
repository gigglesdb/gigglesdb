mod common;
use common::*;
use giggles_core::{Admin, Limits, relational::*};
#[test]
fn exact_numeric_functions_pg18_regressions() {
    let a = Admin::create(fresh("numeric-functions"), Limits::default()).unwrap();
    let mut s = a.sql_session();
    for (expr, expected, ty) in [
        ("div(-7.00,2.0)", "-3", DataType::Numeric),
        ("round(1.25,'1')", "1.3", DataType::Numeric),
        ("mod('Infinity'::numeric,2)", "NaN", DataType::Numeric),
        ("div('NaN'::numeric,0)", "NaN", DataType::Numeric),
        ("mod(1::numeric,'Infinity')", "1", DataType::Numeric),
        ("gcd('Infinity'::numeric,0)", "NaN", DataType::Numeric),
        ("mod(-7.00,2.0)", "-1.00", DataType::Numeric),
        ("gcd(12.30,0.90)", "0.30", DataType::Numeric),
        ("lcm(12.30,0.90)", "36.90", DataType::Numeric),
        ("gcd(0.00,0.000)", "0.000", DataType::Numeric),
        ("lcm(1.20,0.00)", "0.00", DataType::Numeric),
        (
            "mod(100000000000000000000000000000000000001,3)",
            "2",
            DataType::Numeric,
        ),
        ("div(-9.99,2)", "-4", DataType::Numeric),
        ("mod(9.99,-2)", "1.99", DataType::Numeric),
        ("gcd(48,18)", "6", DataType::Integer),
        ("mod(1::smallint,'2')", "1", DataType::SmallInt),
        ("lcm(-21,6)", "42", DataType::Integer),
        ("sign('NaN'::float8)", "0", DataType::DoublePrecision),
        (
            "'1.2345678'::float4::float8",
            "1.2345677614212036",
            DataType::DoublePrecision,
        ),
        (
            "'1.2345678901234567'::float8::numeric",
            "1.23456789012346",
            DataType::Numeric,
        ),
        ("'1.2345678'::float4::numeric", "1.23457", DataType::Numeric),
        ("'0'::float8::numeric", "0", DataType::Numeric),
    ] {
        let q = format!("SELECT {expr}");
        let p = s.prepare(&q).unwrap();
        assert_eq!(s.describe(&p).unwrap().columns[0].data_type, ty, "{expr}");
        let got = s
            .execute(&format!("SELECT ({expr})::text"), &[], None)
            .unwrap();
        assert_eq!(
            got.rows[0].values[0],
            Value::Text(expected.into()),
            "{expr}"
        );
    }
    for q in [
        "SELECT gcd('12','8')",
        "SELECT gcd(1::smallint,'2')",
        "SELECT round(1.25,1::bigint)",
        "SELECT gcd(-2147483648,0)",
        "SELECT lcm(2147483647,2)",
        "SELECT div(1,0)",
        "SELECT mod(1,0)",
    ] {
        assert!(s.execute(q, &[], None).is_err(), "{q}");
    }
    assert_eq!(
        s.execute("SELECT div(NULL,2),gcd(NULL,2)", &[], None)
            .unwrap()
            .rows[0]
            .values,
        vec![Value::Null, Value::Null]
    );
}
#[test]
fn numeric_binary_and_prepared_pg18_regressions() {
    let a = Admin::create(fresh("numeric-functions-params"), Limits::default()).unwrap();
    let mut s = a.sql_session();
    for (expr, hex) in [
        ("div(-7.00,2.0)", "00010000400000000003"),
        ("mod(-7.00,2.0)", "00010000400000020001"),
        ("gcd(12.30,0.90)", "0001ffff000000020bb8"),
        ("lcm(12.30,0.90)", "000200000000000200242328"),
    ] {
        let r = s.execute(&format!("SELECT {expr}"), &[], None).unwrap();
        let Value::Numeric(n) = &r.rows[0].values[0] else {
            panic!()
        };
        let got = n
            .to_binary()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        assert_eq!(got, hex, "{expr}");
    }
    for (q, ty) in [
        ("SELECT div($1,2)", DataType::Numeric),
        ("SELECT gcd($1,2)", DataType::Integer),
        ("SELECT round($1,2)", DataType::Numeric),
        ("SELECT sign($1)", DataType::DoublePrecision),
    ] {
        let p = s.prepare(q).unwrap();
        assert_eq!(s.describe(&p).unwrap().parameters, vec![ty]);
        assert_eq!(
            s.execute_prepared(&p, &[Value::Null], None).unwrap().rows[0].values,
            vec![Value::Null]
        );
    }
    let p = s.prepare("SELECT gcd($1,$2)").unwrap();
    assert!(s.describe(&p).is_err());
    assert_eq!(
        s.execute("SELECT mod(1::smallint,2::smallint)", &[], None)
            .unwrap()
            .rows[0]
            .values,
        vec![Value::Int(1)]
    );
    let p = s.prepare("SELECT gcd(1::smallint,2::smallint)").unwrap();
    assert!(s.describe(&p).is_err());
}

#[test]
fn unknown_literals_reject_at_binding_but_parameters_at_execution() {
    let a = Admin::create(fresh("numeric-bind-errors"), Limits::default()).unwrap();
    let mut s = a.sql_session();
    for q in [
        "SELECT div('bad',2)",
        "SELECT mod('bad',2)",
        "SELECT round('bad')",
        "SELECT round(1.25,'bad')",
    ] {
        let p = s.prepare(q).unwrap();
        assert!(s.describe(&p).is_err(), "{q}");
    }
    let p = s.prepare("SELECT div($1,2)").unwrap();
    assert!(s.describe(&p).is_ok());
    assert!(
        s.execute_prepared(&p, &[Value::Text("bad".into())], None)
            .is_err()
    );
}
