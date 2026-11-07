mod common;
use common::*;
use giggles_core::{Admin, Limits, relational::*};

#[test]
fn numeric_scale_clamps_preserve_pg18_text_type_and_dscale() {
    let a = Admin::create(fresh("scalar-boundaries"), Limits::default()).unwrap();
    let mut s = a.sql_session();
    for f in ["round", "trunc"] {
        let p = s
            .prepare(&format!("SELECT {f}($1::numeric,$2::integer)"))
            .unwrap();
        assert_eq!(
            s.describe(&p).unwrap().parameters,
            vec![DataType::Numeric, DataType::Integer]
        );
        assert_eq!(
            s.describe(&p).unwrap().columns[0].data_type,
            DataType::Numeric
        );
        for input in ["1.2500", "-1.2500", "0.000", "NaN", "Infinity", "-Infinity"] {
            for scale in [i32::MIN, i32::MAX, 0, 16383, 16384, -16384] {
                let n = Numeric::parse(input).unwrap();
                let expected = if n.scale().is_none() {
                    input.to_owned()
                } else if scale < 0 {
                    "0".to_owned()
                } else if scale == 0 {
                    if input.starts_with('-') {
                        "-1".into()
                    } else if input.starts_with('0') {
                        "0".into()
                    } else {
                        "1".into()
                    }
                } else {
                    format!(
                        "{}{}",
                        if input.starts_with('0') {
                            "0.000"
                        } else {
                            input
                        },
                        "0".repeat(16383 - if input.starts_with('0') { 3 } else { 4 })
                    )
                };
                let r = s
                    .execute_prepared(&p, &[Value::Numeric(n), Value::Int(i64::from(scale))], None)
                    .unwrap();
                let Value::Numeric(got) = &r.rows[0].values[0] else {
                    panic!()
                };
                assert_eq!(got.to_string(), expected, "{f}({input},{scale})");
                assert_eq!(got.scale(), Numeric::parse(&expected).unwrap().scale());
            }
        }
    }
    for (e, text) in [
        ("round(round(1.25,2147483647),1)", "1.3"),
        ("trunc(trunc(-1.25,2147483647),1)", "-1.2"),
        ("round(9.5,0)", "10"),
        ("trunc(9.5,0)", "9"),
    ] {
        let r = s.execute(&format!("SELECT {e}"), &[], None).unwrap();
        let Value::Numeric(n) = &r.rows[0].values[0] else {
            panic!()
        };
        assert_eq!(n.to_string(), text);
    }
}

#[test]
fn pg18_untyped_abs_and_typed_bytea_contracts() {
    let a = Admin::create(fresh("scalar-inference"), Limits::default()).unwrap();
    let mut s = a.sql_session();
    let p = s.prepare("SELECT abs($1)").unwrap();
    assert_eq!(
        s.describe(&p).unwrap().parameters,
        vec![DataType::DoublePrecision]
    );
    assert_eq!(
        s.describe(&p).unwrap().columns[0].data_type,
        DataType::DoublePrecision
    );
    assert_eq!(
        s.execute_prepared(
            &p,
            &[Value::Float8(Float8::from_bits((-2.5f64).to_bits()))],
            None
        )
        .unwrap()
        .rows[0]
            .values,
        vec![Value::Float8(Float8::from_bits(2.5f64.to_bits()))]
    );
    let p = s.prepare("SELECT CAST($1 AS bytea)").unwrap();
    assert_eq!(s.describe(&p).unwrap().parameters, vec![DataType::Bytes]);
    assert!(
        s.execute_prepared(&p, &[Value::Text("abc".into())], None)
            .is_err()
    );
    assert_eq!(
        s.execute_prepared(&p, &[Value::Bytes(vec![65, 66])], None)
            .unwrap()
            .rows[0]
            .values,
        vec![Value::Bytes(vec![65, 66])]
    );
}

#[test]
fn frozen_numeric_coefficient_boundary_is_preserved() {
    let a = Admin::create(fresh("scalar-capacity"), Limits::default()).unwrap();
    let mut s = a.sql_session();
    let digits = "9".repeat(16384);
    for f in ["round", "trunc"] {
        let p = s
            .prepare(&format!("SELECT {f}($1::numeric,$2::integer)"))
            .unwrap();
        let n = Value::Numeric(Numeric::parse(&digits).unwrap());
        let r = s
            .execute_prepared(&p, &[n.clone(), Value::Int(i64::from(i32::MIN))], None)
            .unwrap();
        assert_eq!(
            r.rows[0].values,
            vec![Value::Numeric(Numeric::parse("0").unwrap())]
        );
        assert!(
            s.execute_prepared(&p, &[n.clone(), Value::Int(i64::from(i32::MAX))], None)
                .is_err()
        );
        let result = s.execute_prepared(&p, &[n, Value::Int(-16384)], None);
        if f == "round" {
            assert!(result.is_err());
        } else {
            assert_eq!(
                result.unwrap().rows[0].values,
                vec![Value::Numeric(Numeric::parse("0").unwrap())]
            );
        }
    }
    let n = Value::Numeric(Numeric::parse(&format!("{}.9", "9".repeat(16383))).unwrap());
    let p = s.prepare("SELECT trunc($1::numeric,0)").unwrap();
    let r = s.execute_prepared(&p, &[n], None).unwrap();
    assert_eq!(
        r.rows[0].values[0],
        Value::Numeric(Numeric::parse(&"9".repeat(16383)).unwrap())
    );
}

#[test]
fn extreme_scale_output_budget_rejects_atomically() {
    let a = Admin::create(fresh("scalar-output-budget"), Limits::default()).unwrap();
    let mut s = a.sql_session();
    let expression = "round(1.25,2147483647)";
    let fits = format!("SELECT {}", vec![expression; 31].join(","));
    let exceeds = format!("SELECT {}", vec![expression; 32].join(","));
    assert_eq!(
        s.execute(&fits, &[], None).unwrap().rows[0].values.len(),
        31
    );
    assert!(s.execute(&exceeds, &[], None).is_err());
    assert_eq!(
        s.execute(&fits, &[], None).unwrap().rows[0].values.len(),
        31
    );
}
