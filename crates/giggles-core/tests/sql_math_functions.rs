mod common;
use common::*;
use giggles_core::{Admin, Limits, relational::*};
#[test]
fn exact_math_and_binding() {
    let a = Admin::create(fresh("math-functions"), Limits::default()).unwrap();
    let mut s = a.sql_session();
    for (expr, expected) in [
        ("scale(1.2300)", "4"),
        ("min_scale(1.2300)", "2"),
        ("trim_scale(1.2300)", "1.23"),
        ("trim_scale(0.000)", "0"),
        ("factorial(0)", "1"),
        ("factorial(30)", "265252859812191058636308480000000"),
        ("sqrt(4::float4)", "2"),
        ("cbrt(-8::float8)", "-2"),
        ("power(2::float8,3::float8)", "8"),
        ("ln(1::float8)", "0"),
        ("log10(100::float8)", "2"),
        ("exp(0::float8)", "1"),
        ("sin(0::float8)", "0"),
        ("cos(0::float8)", "1"),
        ("min_scale(trim_scale(1.23000))", "2"),
    ] {
        let got = s
            .execute(&format!("SELECT ({expr})::text"), &[], None)
            .unwrap();
        assert_eq!(
            got.rows[0].values[0],
            Value::Text(expected.into()),
            "{expr}"
        );
    }
    for name in [
        "scale",
        "min_scale",
        "trim_scale",
        "factorial",
        "sqrt",
        "ln",
        "sin",
    ] {
        let p = s.prepare(&format!("SELECT {name}($1)")).unwrap();
        assert_eq!(
            s.execute_prepared(&p, &[Value::Null], None).unwrap().rows[0].values[0],
            Value::Null
        );
    }
    s.execute(
        "CREATE TABLE math_empty (id bigint primary key, n numeric, f float8)",
        &[],
        None,
    )
    .unwrap();
    for query in [
        "SELECT trim_scale(n) FROM math_empty LIMIT 10",
        "SELECT sqrt(f) FROM math_empty LIMIT 10",
        "SELECT sqrt(-1::float8) LIMIT 0",
    ] {
        assert!(s.execute(query, &[], None).unwrap().rows.is_empty());
    }
    for query in [
        "SELECT factorial(-1)",
        "SELECT factorial(1.5)",
        "SELECT sqrt(-1::float8)",
        "SELECT ln(0::float8)",
        "SELECT power(-2::float8,0.5::float8)",
        "SELECT exp(10000::float8)",
        "SELECT exp(-10000::float8)",
        "SELECT scale(1::float8)",
    ] {
        assert!(s.execute(query, &[], None).is_err(), "{query}");
    }
    let p = s.prepare("SELECT sqrt(4::float4)").unwrap();
    assert_eq!(
        s.describe(&p).unwrap().columns[0].data_type,
        DataType::DoublePrecision
    );
}

#[test]
fn float_bits_and_domains() {
    let a = Admin::create(fresh("math-bits"), Limits::default()).unwrap();
    let mut s = a.sql_session();
    for (expr, bits) in [
        ("sqrt('-0'::float8)", 0x8000000000000000),
        ("radians('-0'::float8)", 0x8000000000000000),
        ("pi()", 0x400921fb54442d18),
        ("power('NaN'::float8,0::float8)", 0x3ff0000000000000),
        ("power(1::float8,'NaN'::float8)", 0x3ff0000000000000),
        ("atanh(1::float8)", 0x7ff0000000000000),
        ("cot('-0'::float8)", 0xfff0000000000000),
    ] {
        let got = s.execute(&format!("SELECT {expr}"), &[], None).unwrap();
        let Value::Float8(v) = got.rows[0].values[0] else {
            panic!("{expr}");
        };
        assert_eq!(v.value().to_bits(), bits, "{expr}");
    }
    assert!(
        s.execute("SELECT radians('5e-324'::float8)", &[], None)
            .is_err()
    );
    let p = s.prepare("SELECT pg_catalog.sqrt(1::numeric)").unwrap();
    assert_eq!(
        s.describe(&p).unwrap().columns[0].data_type,
        DataType::Numeric
    );
    let got = s.execute_prepared(&p, &[], None).unwrap();
    let Value::Numeric(ref n) = got.rows[0].values[0] else {
        panic!("numeric required")
    };
    assert_eq!(n.to_string(), "1.000000000000000");
    assert!(
        s.execute("SELECT sqrt(1::numeric) LIMIT 0", &[], None)
            .unwrap()
            .rows
            .is_empty()
    );
    assert!(s.execute("SELECT sqrt(-1::numeric)", &[], None).is_err());
    assert!(
        s.execute("SELECT power(1::numeric,2::numeric)", &[], None)
            .is_ok()
    );
}
