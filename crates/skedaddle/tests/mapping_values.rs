use skedaddle::mapping::{
    Array, ArrayDimension, Classification, Date, DomainConstraints, EncodedBytes, IdentityMapping,
    Jsonb, Mapping, MappingError, Numeric, PgType, Timestamp, TypeIdentity, Value,
};

fn encoded(bytes: &[u8], encoding: &str) -> EncodedBytes {
    EncodedBytes {
        bytes: bytes.to_vec(),
        encoding: encoding.into(),
    }
}
fn id(name: &str) -> TypeIdentity {
    TypeIdentity {
        schema: "public".into(),
        name: name.into(),
        definition_digest: [7; 32],
    }
}
fn text_type() -> PgType {
    PgType::Text {
        encoding: "UTF8".into(),
    }
}
fn roundtrip(ty: PgType, values: Vec<Value>) {
    let mapping = IdentityMapping::new(ty.clone());
    for value in values {
        let bytes = value.canonical_bytes().unwrap();
        assert_eq!(Value::from_canonical_bytes(&bytes).unwrap(), value);
        assert_eq!(
            mapping.inverse(&mapping.forward(&value).unwrap()).unwrap(),
            value
        );
        let text = value.format_pg_text(&ty).unwrap();
        assert_eq!(
            Value::parse_pg_text(&ty, text.as_deref()).unwrap(),
            value,
            "type {ty:?}"
        );
    }
}

#[test]
fn boundary_values_roundtrip_exact_bool() {
    roundtrip(
        PgType::Bool,
        vec![Value::Bool(false), Value::Bool(true), Value::Null],
    );
}
#[test]
fn boundary_values_roundtrip_exact_int2() {
    roundtrip(
        PgType::Int2,
        [i16::MIN, -1, 0, i16::MAX].map(Value::Int2).into(),
    );
}
#[test]
fn boundary_values_roundtrip_exact_int4() {
    roundtrip(
        PgType::Int4,
        [i32::MIN, -1, 0, i32::MAX].map(Value::Int4).into(),
    );
}
#[test]
fn boundary_values_roundtrip_exact_int8() {
    roundtrip(
        PgType::Int8,
        [i64::MIN, -1, 0, i64::MAX].map(Value::Int8).into(),
    );
}
#[test]
fn boundary_values_roundtrip_exact_numeric() {
    let mut inputs = vec![
        "NaN".into(),
        "Infinity".into(),
        "-Infinity".into(),
        "0.00000".into(),
        "-0.00100".into(),
        "9".repeat(1000),
    ];
    inputs.push(format!("-{}.{}", "9".repeat(500), "0".repeat(500)));
    for input in &inputs {
        assert_eq!(Numeric::parse(input).unwrap().to_string(), *input);
    }
    roundtrip(
        PgType::Numeric,
        inputs
            .iter()
            .map(|s| Value::Numeric(Numeric::parse(s).unwrap()))
            .collect(),
    );
    assert_eq!(Numeric::parse("1.2300e2").unwrap().to_string(), "123.00");
    assert_eq!(Numeric::parse("1e3").unwrap().to_string(), "1000");
    assert_eq!(Numeric::parse("-000.00").unwrap().to_string(), "0.00");
    for bad in [
        "",
        ".",
        "1.2.3",
        "1e999999999",
        "1e-16384",
        "1e131072",
        "--1",
    ] {
        assert!(Numeric::parse(bad).is_err(), "{bad}");
    }
}
#[test]
fn boundary_values_roundtrip_exact_float4() {
    roundtrip(
        PgType::Float4,
        [
            0.0_f32,
            -0.0,
            f32::MIN,
            f32::MAX,
            f32::MIN_POSITIVE,
            f32::from_bits(1),
            f32::INFINITY,
            f32::NEG_INFINITY,
            f32::NAN,
        ]
        .map(|f| Value::Float4(f.to_bits()))
        .into(),
    );
}
#[test]
fn boundary_values_roundtrip_exact_float8() {
    roundtrip(
        PgType::Float8,
        [
            0.0_f64,
            -0.0,
            f64::MIN,
            f64::MAX,
            f64::MIN_POSITIVE,
            f64::from_bits(1),
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::NAN,
        ]
        .map(|f| Value::Float8(f.to_bits()))
        .into(),
    );
}
#[test]
fn nan_payloads_are_exact_in_canonical_and_identity_and_block_lossy_text() {
    for (ty, values) in [
        (
            PgType::Float4,
            vec![Value::Float4(0x7fa1_2345), Value::Float4(0xffc0_1234)],
        ),
        (
            PgType::Float8,
            vec![
                Value::Float8(0x7ff0_1234_5678_9abc),
                Value::Float8(0xfff8_0000_0000_0042),
            ],
        ),
    ] {
        let mapping = IdentityMapping::new(ty.clone());
        for v in values {
            assert_eq!(
                Value::from_canonical_bytes(&v.canonical_bytes().unwrap()).unwrap(),
                v
            );
            assert_eq!(mapping.inverse(&mapping.forward(&v).unwrap()).unwrap(), v);
            assert!(matches!(
                v.format_pg_text(&ty),
                Err(MappingError::LossyText(_))
            ));
        }
    }
}
#[test]
fn boundary_values_roundtrip_exact_text() {
    roundtrip(
        text_type(),
        vec![
            Value::Text(encoded(b"", "UTF8")),
            Value::Text(encoded(
                "quote \" slash \\ newline\n\u{1f600}".as_bytes(),
                "UTF8",
            )),
        ],
    );
    roundtrip(
        PgType::Text {
            encoding: "LATIN1".into(),
        },
        vec![Value::Text(encoded(&[0xe9, 0xff], "LATIN1"))],
    );
}
#[test]
fn boundary_values_roundtrip_exact_varchar() {
    roundtrip(
        PgType::Varchar {
            encoding: "UTF8".into(),
        },
        vec![
            Value::Varchar(encoded(b"", "UTF8")),
            Value::Varchar(encoded(b"trailing  ", "UTF8")),
        ],
    );
}
#[test]
fn boundary_values_roundtrip_exact_char() {
    roundtrip(
        PgType::Char {
            encoding: "SQL_ASCII".into(),
        },
        vec![
            Value::Char(encoded(b" ", "SQL_ASCII")),
            Value::Char(encoded(&[0xff, b' '], "SQL_ASCII")),
        ],
    );
}
#[test]
fn boundary_values_roundtrip_exact_bytea() {
    roundtrip(
        PgType::Bytea,
        vec![Value::Bytea(vec![]), Value::Bytea((0..=255).collect())],
    );
    assert_eq!(
        Value::parse_pg_text(&PgType::Bytea, Some(br"\000\377\\a")).unwrap(),
        Value::Bytea(vec![0, 255, b'\\', b'a'])
    );
}
#[test]
fn boundary_values_roundtrip_exact_large_values() {
    // 8 MiB each, sequentially: enough to exercise TOAST-scale allocations.
    for (ty, value) in [
        (
            text_type(),
            Value::Text(encoded(&vec![b'x'; 8 * 1024 * 1024], "UTF8")),
        ),
        (PgType::Bytea, Value::Bytea(vec![255; 8 * 1024 * 1024])),
    ] {
        let mapping = IdentityMapping::new(ty.clone());
        assert_eq!(mapping.forward(&value).unwrap(), value);
        let bytes = value.format_pg_text(&ty).unwrap();
        assert_eq!(Value::parse_pg_text(&ty, bytes.as_deref()).unwrap(), value);
        let bytes = value.canonical_bytes().unwrap();
        assert_eq!(Value::from_canonical_bytes(&bytes).unwrap(), value);
    }
}
#[test]
fn boundary_values_roundtrip_exact_uuid() {
    roundtrip(
        PgType::Uuid,
        vec![
            Value::Uuid([0; 16]),
            Value::Uuid([255; 16]),
            Value::Uuid(
                *uuid::Uuid::parse_str("123e4567-e89b-12d3-a456-426614174000")
                    .unwrap()
                    .as_bytes(),
            ),
        ],
    );
}
#[test]
fn boundary_values_roundtrip_exact_date() {
    roundtrip(
        PgType::Date,
        vec![
            Value::Date(Date::Finite(-2_451_545)),
            Value::Date(Date::Finite(2_145_031_948)),
            Value::Date(Date::Finite(0)),
            Value::Date(Date::NegativeInfinity),
            Value::Date(Date::PositiveInfinity),
        ],
    );
    assert_eq!(
        Value::parse_pg_text(&PgType::Date, Some(b"2000-01-01")).unwrap(),
        Value::Date(Date::Finite(0))
    );
    assert!(Value::parse_pg_text(&PgType::Date, Some(b"1900-02-29")).is_err());
    assert!(Value::parse_pg_text(&PgType::Date, Some(b"0000-01-01")).is_err());
}
#[test]
fn boundary_values_roundtrip_exact_time() {
    roundtrip(
        PgType::Time,
        [0, 1, 86_399_999_999, 86_400_000_000]
            .map(Value::Time)
            .into(),
    );
}
#[test]
fn boundary_values_roundtrip_exact_timetz() {
    roundtrip(
        PgType::Timetz,
        vec![
            Value::Timetz {
                micros: 0,
                offset_east: -57_599,
            },
            Value::Timetz {
                micros: 86_400_000_000,
                offset_east: 57_599,
            },
            Value::Timetz {
                micros: 1234567,
                offset_east: 0,
            },
        ],
    );
}
fn timestamps() -> Vec<Timestamp> {
    vec![
        Timestamp::Finite(-211_813_488_000_000_000),
        Timestamp::Finite(9_223_371_331_199_999_999),
        Timestamp::Finite(-1),
        Timestamp::Finite(0),
        Timestamp::Finite(1),
        Timestamp::NegativeInfinity,
        Timestamp::PositiveInfinity,
    ]
}
#[test]
fn boundary_values_roundtrip_exact_timestamp() {
    roundtrip(
        PgType::Timestamp,
        timestamps().into_iter().map(Value::Timestamp).collect(),
    );
}
#[test]
fn boundary_values_roundtrip_exact_timestamptz() {
    roundtrip(
        PgType::Timestamptz,
        timestamps().into_iter().map(Value::Timestamptz).collect(),
    );
}
#[test]
fn differing_numeric_timezone_offsets_preserve_instants() {
    let a = Value::parse_pg_text(&PgType::Timestamptz, Some(b"2000-01-01 05:30:00+05:30")).unwrap();
    let b = Value::parse_pg_text(&PgType::Timestamptz, Some(b"1999-12-31 17:00:00-07")).unwrap();
    assert_eq!(a, Value::Timestamptz(Timestamp::Finite(0)));
    assert_eq!(a, b);
    assert_eq!(a.digest().unwrap(), b.digest().unwrap());
}
#[test]
fn boundary_values_roundtrip_exact_interval() {
    roundtrip(
        PgType::Interval,
        vec![
            Value::Interval {
                months: i32::MIN,
                days: i32::MAX,
                micros: i64::MIN,
            },
            Value::Interval {
                months: i32::MAX,
                days: i32::MIN,
                micros: i64::MAX,
            },
            Value::Interval {
                months: 0,
                days: 0,
                micros: 0,
            },
        ],
    );
    assert_eq!(
        Value::parse_pg_text(
            &PgType::Interval,
            Some(b"1 year -2 mons +3 days -04:05:06.000001")
        )
        .unwrap(),
        Value::Interval {
            months: 10,
            days: 3,
            micros: -14_706_000_001
        }
    );
}
#[test]
fn boundary_values_roundtrip_exact_json() {
    roundtrip(
        PgType::Json,
        vec![
            Value::Json(" { \"x\": 1.00, \"x\":2, \"a\": [true, null] } \n".into()),
            Value::Json(r#""\u0000\ud800""#.into()),
            Value::Json("1e999999999999999999".into()),
        ],
    );
}
#[test]
fn boundary_values_roundtrip_exact_jsonb() {
    let huge = format!(
        "{{\"z\":{},\"a\":[null,true,\"\\uD83D\\uDE00\"]}}",
        "9".repeat(1000)
    );
    roundtrip(
        PgType::Jsonb,
        vec![
            Value::Jsonb(Jsonb::parse(&huge).unwrap()),
            Value::Jsonb(Jsonb::parse("null").unwrap()),
        ],
    );
    let a = Value::Jsonb(Jsonb::parse(r#"{ "z":0, "a":1.00, "z":2 }"#).unwrap());
    let b = Value::Jsonb(Jsonb::parse(r#"{"z":2e0,"a":1}"#).unwrap());
    assert_eq!(a, b);
    assert_eq!(a.digest().unwrap(), b.digest().unwrap());
    for bad in [
        r#""\u0000""#,
        r#""\ud800""#,
        "NaN",
        "01",
        "[1,]",
        "{\"x\":1,}",
        "1e999999",
    ] {
        assert!(Jsonb::parse(bad).is_err(), "{bad}");
    }
}
#[test]
fn boundary_values_roundtrip_exact_arrays() {
    let a = Array {
        element_type: Box::new(text_type()),
        dimensions: vec![
            ArrayDimension {
                length: 2,
                lower_bound: -7,
            },
            ArrayDimension {
                length: 3,
                lower_bound: 42,
            },
        ],
        elements: vec![
            Value::Null,
            Value::Text(encoded(b"NULL", "UTF8")),
            Value::Text(encoded(b"", "UTF8")),
            Value::Text(encoded(b"a,b{c}\\\"", "UTF8")),
            Value::Text(encoded(b" space ", "UTF8")),
            Value::Text(encoded(b"x\ny", "UTF8")),
        ],
    };
    roundtrip(
        PgType::Array(Box::new(text_type())),
        vec![
            Value::Array(a),
            Value::Array(Array {
                element_type: Box::new(text_type()),
                dimensions: vec![],
                elements: vec![],
            }),
        ],
    );
    let a = Array {
        element_type: Box::new(PgType::Int4),
        dimensions: (0..6)
            .map(|n| ArrayDimension {
                length: 1,
                lower_bound: n - 3,
            })
            .collect(),
        elements: vec![Value::Int4(i32::MIN)],
    };
    roundtrip(PgType::Array(Box::new(PgType::Int4)), vec![Value::Array(a)]);
}
#[test]
fn boundary_values_roundtrip_exact_enum() {
    let ty = PgType::Enum {
        type_id: id("mood"),
        labels: vec![vec![], b"happy".to_vec(), b"a,b\\\"".to_vec()],
        encoding: "UTF8".into(),
    };
    roundtrip(
        ty.clone(),
        vec![
            Value::Enum {
                type_id: id("mood"),
                label: encoded(b"happy", "UTF8"),
            },
            Value::Enum {
                type_id: id("mood"),
                label: encoded(b"", "UTF8"),
            },
        ],
    );
    let wrong = Value::Enum {
        type_id: id("another_mood"),
        label: encoded(b"happy", "UTF8"),
    };
    assert!(IdentityMapping::new(ty).forward(&wrong).is_err());
}

#[test]
fn definition_identity_must_match_before_mapping() {
    let source = PgType::Enum {
        type_id: id("mood"),
        labels: vec![b"happy".to_vec()],
        encoding: "UTF8".into(),
    };
    let destination = PgType::Enum {
        type_id: id("mood"),
        labels: vec![b"sad".to_vec()],
        encoding: "UTF8".into(),
    };
    assert!(IdentityMapping::between(source.clone(), &destination).is_err());
    assert!(
        IdentityMapping::between(source.clone(), &source)
            .unwrap()
            .is_reversible()
    );
    let mut changed_id = id("mood");
    changed_id.definition_digest[0] = 8;
    let v = Value::Enum {
        type_id: changed_id,
        label: encoded(b"happy", "UTF8"),
    };
    assert!(IdentityMapping::new(source).forward(&v).is_err());
}

struct Positive;
impl DomainConstraints for Positive {
    fn check(
        &self,
        type_id: &TypeIdentity,
        constraint_ref: &str,
        base: &Value,
    ) -> Result<(), MappingError> {
        assert_eq!(type_id, &id("positive"));
        assert_eq!(constraint_ref, "positive-v1");
        if matches!(base, Value::Int4(v) if *v > 0) {
            Ok(())
        } else {
            Err(MappingError::OutOfDomain(
                "positive and non-NULL required".into(),
            ))
        }
    }
}
fn domain_type() -> PgType {
    PgType::Domain {
        type_id: id("positive"),
        base: Box::new(PgType::Int4),
        constraint_ref: "positive-v1".into(),
    }
}
fn domain(v: i32) -> Value {
    Value::Domain {
        type_id: id("positive"),
        constraint_ref: "positive-v1".into(),
        base: Box::new(Value::Int4(v)),
    }
}
#[test]
fn boundary_values_roundtrip_exact_domain() {
    let ty = domain_type();
    let mapping = IdentityMapping::new(ty.clone()).with_constraints(&Positive);
    for v in [1, i32::MAX] {
        let value = domain(v);
        let text = value.format_pg_text(&ty).unwrap();
        assert_eq!(Value::parse_pg_text(&ty, text.as_deref()).unwrap(), value);
        assert_eq!(
            Value::from_canonical_bytes(&value.canonical_bytes().unwrap()).unwrap(),
            value
        );
        assert_eq!(
            mapping.inverse(&mapping.forward(&value).unwrap()).unwrap(),
            value
        );
    }
}
#[test]
fn out_of_domain_blocks_not_converts() {
    let missing = IdentityMapping::new(domain_type());
    assert_eq!(missing.classification(), Classification::Unverified);
    assert!(!missing.is_reversible());
    assert!(matches!(
        missing.forward(&domain(1)),
        Err(MappingError::Unverified(_))
    ));
    let mapping = missing.with_constraints(&Positive);
    assert!(mapping.is_reversible());
    for value in [domain(0), domain(-1), Value::Null, Value::Int4(1)] {
        assert!(matches!(
            mapping.forward(&value),
            Err(MappingError::OutOfDomain(_))
        ));
        assert!(mapping.inverse(&value).is_err());
    }
    assert!(
        IdentityMapping::new(PgType::Int2)
            .forward(&Value::Int4(1))
            .is_err()
    );
    assert!(
        IdentityMapping::new(text_type())
            .forward(&Value::Text(encoded(b"a", "LATIN1")))
            .is_err()
    );
}

#[test]
fn domain_arrays_check_every_element_and_null_constraint() {
    let ty = PgType::Array(Box::new(domain_type()));
    let value = |elements: Vec<Value>| {
        Value::Array(Array {
            element_type: Box::new(domain_type()),
            dimensions: vec![ArrayDimension {
                length: elements.len() as u32,
                lower_bound: -1,
            }],
            elements,
        })
    };
    let mapping = IdentityMapping::new(ty.clone()).with_constraints(&Positive);
    assert!(mapping.forward(&value(vec![domain(1), domain(2)])).is_ok());
    assert!(mapping.forward(&value(vec![domain(1), domain(0)])).is_err());
    assert!(
        mapping
            .forward(&value(vec![domain(1), Value::Null]))
            .is_err()
    );
    assert!(IdentityMapping::new(ty).forward(&Value::Null).is_err());
    let unknown = PgType::Array(Box::new(PgType::Other {
        name: "unknown".into(),
        known_lossy: false,
    }));
    assert!(
        IdentityMapping::new(unknown.clone())
            .forward(&Value::Null)
            .is_err()
    );
    assert!(Value::Null.format_pg_text(&unknown).is_err());
}
#[test]
fn inverse_of_forward_is_identity() {
    for n in -1000..1000 {
        let m = IdentityMapping::new(PgType::Int8);
        let v = Value::Int8(n);
        assert_eq!(m.inverse(&m.forward(&v).unwrap()).unwrap(), v);
    }
    assert_eq!(
        IdentityMapping::new(PgType::Other {
            name: "extension".into(),
            known_lossy: false
        })
        .classification(),
        Classification::Unverified
    );
    let lossy = IdentityMapping::new(PgType::Other {
        name: "lossy".into(),
        known_lossy: true,
    });
    assert_eq!(lossy.classification(), Classification::Unsupported);
    assert!(lossy.forward(&Value::Null).is_err());
    assert!(IdentityMapping::between(PgType::Int4, &PgType::Int8).is_err());
}
#[test]
fn null_vs_empty_distinct() {
    let values = vec![
        Value::Null,
        Value::Text(encoded(b"", "UTF8")),
        Value::Varchar(encoded(b"", "UTF8")),
        Value::Char(encoded(b"", "UTF8")),
        Value::Bytea(vec![]),
        Value::Json("null".into()),
        Value::Jsonb(Jsonb::parse("null").unwrap()),
        Value::Array(Array {
            element_type: Box::new(PgType::Int4),
            dimensions: vec![],
            elements: vec![],
        }),
    ];
    for a in &values {
        for b in &values {
            assert_eq!(
                a == b,
                a.canonical_bytes().unwrap() == b.canonical_bytes().unwrap()
            );
        }
    }
    assert_eq!(Value::Null.format_pg_text(&text_type()).unwrap(), None);
    assert_eq!(
        Value::Text(encoded(b"", "UTF8"))
            .format_pg_text(&text_type())
            .unwrap(),
        Some(vec![])
    );
}
#[test]
fn canonical_encoding_stable_and_injective_for_unequal_values() {
    let mut values = vec![
        Value::Null,
        Value::Bool(false),
        Value::Int2(0),
        Value::Int4(0),
        Value::Int8(0),
        Value::Float4(0),
        Value::Float4(0x8000_0000),
        Value::Float4(0x7fc0_0001),
        Value::Float4(0x7fc0_0002),
        Value::Float8(0),
        Value::Float8(0x8000_0000_0000_0000),
        Value::Numeric(Numeric::parse("1.0").unwrap()),
        Value::Numeric(Numeric::parse("1.00").unwrap()),
        Value::Text(encoded(b"abc", "UTF8")),
        Value::Text(encoded(b"abc", "LATIN1")),
        Value::Json("{}".into()),
        Value::Json(" {}".into()),
    ];
    for lower_bound in [-1, 1] {
        values.push(Value::Array(Array {
            element_type: Box::new(PgType::Int4),
            dimensions: vec![ArrayDimension {
                length: 1,
                lower_bound,
            }],
            elements: vec![Value::Int4(1)],
        }));
    }
    for a in &values {
        assert_eq!(
            a.canonical_bytes().unwrap(),
            a.clone().canonical_bytes().unwrap()
        );
        assert_eq!(a.digest().unwrap(), a.clone().digest().unwrap());
        for b in &values {
            assert_eq!(
                a == b,
                a.canonical_bytes().unwrap() == b.canonical_bytes().unwrap()
            );
            assert_eq!(a == b, a.digest().unwrap() == b.digest().unwrap());
        }
    }
    assert_eq!(
        Value::Int4(42).canonical_bytes().unwrap(),
        b"SKVA\0\x01{\"Int4\":42}"
    );
    assert!(Value::from_canonical_bytes(b"SKVA\0\x01{ \"Int4\":42}").is_err());
    assert!(Value::from_canonical_bytes(b"SKVA\0\x02{\"Int4\":42}").is_err());
}
#[test]
fn malformed_values_block() {
    for input in [
        b"{{1},{2,3}}".as_slice(),
        b"[0:2]={1,2}",
        b"{1,}",
        b"{\"unterminated}",
        b"{{}}",
        b"[2147483647:2147483648]={1,2}",
    ] {
        assert!(Value::parse_pg_text(&PgType::Array(Box::new(PgType::Int4)), Some(input)).is_err());
    }
    for (ty, input) in [
        (PgType::Time, "24:00:00.000001"),
        (PgType::Timetz, "12:00:00+16"),
        (PgType::Timestamptz, "2000-01-01 00:00:00"),
        (PgType::Timestamp, "294277-01-01 00:00:00"),
        (PgType::Float4, "1e1000"),
        (PgType::Bytea, "\\x0"),
        (PgType::Int2, "32768"),
    ] {
        assert!(
            Value::parse_pg_text(&ty, Some(input.as_bytes())).is_err(),
            "{ty:?} {input}"
        );
    }
    assert!(
        Value::Text(encoded(&[0xff], "UTF8"))
            .canonical_bytes()
            .is_err()
    );
    assert!(
        Value::Text(encoded(b"a\0b", "SQL_ASCII"))
            .canonical_bytes()
            .is_err()
    );
}
