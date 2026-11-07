use giggles_core::{
    Admin, Limits,
    relational::{Float4, Float8, Numeric, Value},
};

#[test]
fn pg18_integer_cast_preexisting_pipeline_controls() {
    let path = std::env::temp_dir().join(format!(
        "integer-cast-controls-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let admin = Admin::create(path, Limits::default()).unwrap();
    let mut session = admin.sql_session();
    let cases = [
        (
            "text_hex",
            "SELECT $1::text::smallint",
            Value::Text("0x_41".into()),
            Ok(65),
        ),
        (
            "text_syntax",
            "SELECT $1::text::integer",
            Value::Text("1__0".into()),
            Err("Sql(InvalidTextRepresentation)"),
        ),
        (
            "text_width_prefix",
            "SELECT $1::text::smallint",
            Value::Text("327680x".into()),
            Err("Sql(Overflow)"),
        ),
        (
            "float4_exact_value",
            "SELECT $1::real::integer",
            Value::Float4(Float4::from_bits(0x4effffff)),
            Ok(2147483520),
        ),
        (
            "float4_upper_bound",
            "SELECT $1::real::integer",
            Value::Float4(Float4::from_bits(0x4f000000)),
            Err("Sql(Overflow)"),
        ),
        (
            "float8_minimum",
            "SELECT $1::double precision::bigint",
            Value::Float8(Float8::from_bits(0xc3e0000000000000)),
            Ok(i64::MIN),
        ),
        (
            "float8_nan",
            "SELECT $1::double precision::integer",
            Value::Float8(Float8::from_bits(0x7ff8000000000000)),
            Err("Sql(Overflow)"),
        ),
        (
            "numeric_nan",
            "SELECT $1::numeric::integer",
            Value::Numeric(Numeric::parse("NaN").unwrap()),
            Err("Sql(Unsupported)"),
        ),
    ];
    let mut report = Vec::new();
    let mut differences = 0;
    for (id, sql, param, expected) in cases {
        let p = session.prepare(sql).unwrap();
        let described = session.describe(&p).unwrap();
        let actual = session
            .execute_prepared(&p, &[param], None)
            .map(|r| match &r.rows[0].values[0] {
                Value::Int(n) => *n,
                other => panic!("{other:?}"),
            })
            .map_err(|e| format!("{e:?}"));
        let expected = expected.map_err(str::to_owned);
        if actual != expected {
            differences += 1;
        }
        report.push(serde_json::json!({"id":id,"query":sql,"metadata":format!("{described:?}"),"expected":format!("{expected:?}"),"actual":format!("{actual:?}")}));
    }
    let path = std::env::var("INTEGER_CAST_REPORT").unwrap();
    std::fs::write(path, serde_json::to_vec_pretty(&report).unwrap()).unwrap();
    println!(
        "integer_cast_controls={}, differences={differences}",
        report.len()
    );
    assert_eq!(differences, 0);
}
