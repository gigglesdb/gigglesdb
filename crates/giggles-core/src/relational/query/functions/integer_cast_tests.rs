use super::*;

#[test]
fn integer_cast_charge_once_and_reject_before_charge() {
    let success = [
        (Value::Text("0x_41".into()), DataType::SmallInt, 65),
        (
            Value::Float4(Float4::from_bits(0x4effffff)),
            DataType::Integer,
            2147483520,
        ),
        (
            Value::Float8(Float8::from_bits(0xc3e0000000000000)),
            DataType::BigInt,
            i64::MIN,
        ),
        (
            Value::Numeric(Numeric::parse("65.5").unwrap()),
            DataType::Integer,
            66,
        ),
    ];
    for (value, ty, expected) in success {
        let mut budget = MAX_RESULT_BYTES - 9;
        assert_eq!(
            cast_value(value.clone(), &ty, &mut budget),
            Ok(Value::Int(expected))
        );
        assert_eq!(budget, MAX_RESULT_BYTES);
        let mut budget = MAX_RESULT_BYTES - 8;
        assert_eq!(cast_value(value, &ty, &mut budget), Err(SqlError::Limit));
        // Preserve the existing charge function's error-time mutation.
        assert_eq!(budget, MAX_RESULT_BYTES + 1);
    }
    for (value, error) in [
        (
            Value::Text("1__0".into()),
            SqlError::InvalidTextRepresentation,
        ),
        (Value::Text("21474836480x".into()), SqlError::Overflow),
        (
            Value::Float8(Float8::from_bits(f64::NAN.to_bits())),
            SqlError::Overflow,
        ),
        (
            Value::Numeric(Numeric::parse("NaN").unwrap()),
            SqlError::Unsupported,
        ),
        (
            Value::Text("0".repeat(giggles_pg_types::integer::MAX_INPUT_BYTES + 1)),
            SqlError::Limit,
        ),
    ] {
        let mut budget = 17;
        assert_eq!(
            cast_value(value, &DataType::Integer, &mut budget),
            Err(error)
        );
        assert_eq!(budget, 17);
    }
}
