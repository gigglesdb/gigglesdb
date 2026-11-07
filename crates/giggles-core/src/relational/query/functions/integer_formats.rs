//! PostgreSQL 18 integer formatting, preserving the selected int4/int8 overload.
use super::*;

pub(super) fn known_name(name: &str) -> bool {
    matches!(name, "to_hex" | "to_bin" | "to_oct")
}

/// Neither int2 nor unknown selects a unique overload in PostgreSQL 18.
pub(super) fn argument_type(ty: Option<&DataType>) -> R<DataType> {
    match ty {
        Some(DataType::Integer) => Ok(DataType::Integer),
        Some(DataType::BigInt) => Ok(DataType::BigInt),
        None | Some(DataType::SmallInt) => Err(SqlError::AmbiguousFunction),
        _ => Err(SqlError::UndefinedFunction),
    }
}

pub(super) fn eval(
    name: &str,
    values: &[Value],
    argument: Option<&DataType>,
    budget: &mut usize,
) -> Option<R<Value>> {
    let base = match name {
        "to_bin" => 2,
        "to_oct" => 8,
        "to_hex" => 16,
        _ => return None,
    };
    Some(if values.len() == 1 {
        evaluate(base, &values[0], argument, budget)
    } else {
        Err(SqlError::UndefinedFunction)
    })
}

fn evaluate(base: u64, value: &Value, argument: Option<&DataType>, budget: &mut usize) -> R<Value> {
    let ty = argument_type(argument)?;
    let Value::Int(integer) = value else {
        return if matches!(value, Value::Null) {
            Ok(Value::Null)
        } else {
            Err(SqlError::UndefinedFunction)
        };
    };
    let mut bits = match ty {
        DataType::Integer => u64::from(i32::try_from(*integer).map_err(|_| SqlError::Type)? as u32),
        DataType::BigInt => *integer as u64,
        _ => unreachable!(),
    };
    // At most 64 base-two digits; no intermediate heap allocation or formatting.
    let mut buffer = [0u8; 64];
    let mut start = buffer.len();
    loop {
        start -= 1;
        buffer[start] = b"0123456789abcdef"[(bits % base) as usize];
        bits /= base;
        if bits == 0 {
            break;
        }
    }
    let output = &buffer[start..];
    reserve(budget, output.len())?;
    let text = std::str::from_utf8(output).map_err(|_| SqlError::Type)?;
    Ok(Value::Text(text.to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integer_format_overload_width_and_exact_reservation() {
        for (name, ty, expected) in [
            ("to_hex", DataType::Integer, "ffffffff"),
            ("to_hex", DataType::BigInt, "ffffffffffffffff"),
            ("to_oct", DataType::Integer, "37777777777"),
            ("to_oct", DataType::BigInt, "1777777777777777777777"),
            (
                "to_bin",
                DataType::Integer,
                "11111111111111111111111111111111",
            ),
            (
                "to_bin",
                DataType::BigInt,
                "1111111111111111111111111111111111111111111111111111111111111111",
            ),
        ] {
            let mut budget = MAX_RESULT_BYTES - expected.len();
            assert_eq!(
                eval(name, &[Value::Int(-1)], Some(&ty), &mut budget).unwrap(),
                Ok(Value::Text(expected.into()))
            );
            assert_eq!(budget, MAX_RESULT_BYTES);
            let mut budget = MAX_RESULT_BYTES - expected.len() + 1;
            let before = budget;
            assert_eq!(
                eval(name, &[Value::Int(-1)], Some(&ty), &mut budget).unwrap(),
                Err(SqlError::Limit)
            );
            assert_eq!(budget, before);
        }
    }

    #[test]
    fn integer_format_ambiguity_null_and_rejected_width_do_not_charge() {
        assert_eq!(argument_type(None), Err(SqlError::AmbiguousFunction));
        assert_eq!(
            argument_type(Some(&DataType::SmallInt)),
            Err(SqlError::AmbiguousFunction)
        );
        assert_eq!(
            argument_type(Some(&DataType::Text)),
            Err(SqlError::UndefinedFunction)
        );
        let mut budget = 19;
        assert_eq!(
            eval(
                "to_hex",
                &[Value::Null],
                Some(&DataType::Integer),
                &mut budget
            )
            .unwrap(),
            Ok(Value::Null)
        );
        assert_eq!(budget, 19);
        assert_eq!(
            eval(
                "to_hex",
                &[Value::Int(i64::MAX)],
                Some(&DataType::Integer),
                &mut budget
            )
            .unwrap(),
            Err(SqlError::Type)
        );
        assert_eq!(budget, 19);
    }
}
