//! Focused SUT decoder regressions; no oracle/report files or external services.
use super::decode;
use giggles_core::relational::{DataType, Value};

#[test]
fn integer_width_grammar_range_and_nul_contracts() {
    for (ty, minimum, maximum, below, above) in [
        (DataType::SmallInt, "-32768", "32767", "-32769", "32768"),
        (
            DataType::Integer,
            "-2147483648",
            "2147483647",
            "-2147483649",
            "2147483648",
        ),
        (
            DataType::BigInt,
            "-9223372036854775808",
            "9223372036854775807",
            "-9223372036854775809",
            "9223372036854775808",
        ),
    ] {
        for text in [minimum, maximum] {
            assert_eq!(
                decode(text.as_bytes(), &ty, 0),
                Ok(Value::Int(text.parse().unwrap()))
            );
        }
        for text in [below, above] {
            assert_eq!(decode(text.as_bytes(), &ty, 0), Err("22003"));
        }
        for text in ["", "1__2", "12z", "1\0", "999999999999999999999\0"] {
            assert_eq!(decode(text.as_bytes(), &ty, 0), Err("22P02"));
        }
        for (text, value) in [
            (" \t+0x7f\r", 127),
            ("0b10_10", 10),
            ("0o17", 15),
            ("1_000", 1000),
        ] {
            assert_eq!(decode(text.as_bytes(), &ty, 0), Ok(Value::Int(value)));
        }
    }
    assert_eq!(decode(b"32768z", &DataType::SmallInt, 0), Err("22P02"));
    assert_eq!(
        decode(b"999999999999999999999z", &DataType::SmallInt, 0),
        Err("22003")
    );
    assert_eq!(
        decode(&i16::MIN.to_be_bytes(), &DataType::SmallInt, 1),
        Ok(Value::Int(i16::MIN.into()))
    );
    assert_eq!(
        decode(&i32::MAX.to_be_bytes(), &DataType::Integer, 1),
        Ok(Value::Int(i32::MAX.into()))
    );
    assert_eq!(
        decode(&i64::MIN.to_be_bytes(), &DataType::BigInt, 1),
        Ok(Value::Int(i64::MIN))
    );
}

#[test]
fn integer_resource_classification_executes_decoder() {
    let input = vec![b'1'; giggles_pg_types::integer::MAX_INPUT_BYTES + 1];
    assert_eq!(decode(&input, &DataType::BigInt, 0), Err("54000"));
}

#[test]
fn text_and_bytea_domain_and_format_controls() {
    assert_eq!(
        decode("snowman ☃".as_bytes(), &DataType::Text, 0),
        Ok(Value::Text("snowman ☃".into()))
    );
    assert_eq!(decode(b"a\0b", &DataType::Text, 0), Err("22021"));
    assert_eq!(decode(&[0xff], &DataType::Text, 0), Err("22021"));
    assert_eq!(
        decode(b"a\0b", &DataType::Text, 1),
        Ok(Value::Text("a\0b".into()))
    );
    for input in [
        br"\x00ff".as_slice(),
        br"\x00 ff".as_slice(),
        br"\000\377".as_slice(),
    ] {
        assert_eq!(
            decode(input, &DataType::Bytes, 0),
            Ok(Value::Bytes(vec![0, 255]))
        );
    }
    assert_eq!(
        decode(br"a\\b", &DataType::Bytes, 0),
        Ok(Value::Bytes(br"a\b".to_vec()))
    );
    assert_eq!(decode(br"\x0", &DataType::Bytes, 0), Err("22023"));
    assert_eq!(decode(br"\xgg", &DataType::Bytes, 0), Err("22023"));
    assert_eq!(decode(br"\8", &DataType::Bytes, 0), Err("22P02"));
    assert_eq!(decode(b"a\0b", &DataType::Bytes, 0), Err("22021"));
    assert_eq!(
        decode(&[0, 255], &DataType::Bytes, 1),
        Ok(Value::Bytes(vec![0, 255]))
    );
}

#[test]
fn character_variants_keep_pg_oids_and_text_payloads() {
    for (ty, oid) in [(DataType::Varchar, 1043), (DataType::Bpchar, 1042)] {
        assert_eq!(super::oid(&ty), oid);
        assert_eq!(super::size(&ty), -1);
        assert_eq!(super::core_declared_type(oid), Some(ty.clone()));
        assert_eq!(super::sql_declared_type(&ty, oid), Some(ty.clone()));
        for format in [0, 1] {
            let value = Value::Text("a  ".into());
            assert_eq!(decode(b"a  ", &ty, format), Ok(value.clone()));
            assert_eq!(
                super::encode(&value, &ty, format, &Default::default(), super::SessionZone::Fixed(0)),
                Ok(Some(b"a  ".to_vec()))
            );
            assert_eq!(decode(&[0xff], &ty, format), Err("22021"));
        }
        assert_eq!(decode(b"a\0", &ty, 0), Err("22021"));
    }
    assert_eq!(
        super::sql_declared_type(&DataType::Text, 1043),
        Some(DataType::Varchar)
    );
    assert_eq!(
        super::sql_declared_type(&DataType::Varchar, 25),
        Some(DataType::Text)
    );
}

#[test]
fn interval_binary_is_independent_of_output_style() {
    let interval = giggles_pg_types::temporal::Interval::parse("-1 year 2 days 04:05:06.7").unwrap();
    for style in ["postgres", "postgres_verbose", "sql_standard", "iso_8601"] {
        let settings = crate::pg_settings::Settings { intervalstyle: style.into(), ..Default::default() };
        assert_eq!(super::encode(&Value::Interval(interval), &DataType::Interval, 1, &settings, super::SessionZone::Fixed(0)).unwrap(), Some(interval.to_binary().to_vec()));
    }
}
