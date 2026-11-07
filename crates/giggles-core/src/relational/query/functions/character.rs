//! PostgreSQL 18 UTF8 character operations with libc C character classification.
use super::*;

type Signature = (Vec<Option<DataType>>, DataType);

pub(super) fn known_name(name: &str) -> bool {
    matches!(name, "ascii" | "chr" | "initcap")
}

pub(super) fn signature(name: &str, count: usize) -> Option<R<Signature>> {
    let (argument, result) = match name {
        "ascii" => (DataType::Text, DataType::Integer),
        "chr" => (DataType::Integer, DataType::Text),
        "initcap" => (DataType::Text, DataType::Text),
        _ => return None,
    };
    Some(if count == 1 {
        Ok((vec![Some(argument)], result))
    } else {
        Err(SqlError::UndefinedFunction)
    })
}

pub(super) fn eval(name: &str, values: &[Value], budget: &mut usize) -> Option<R<Value>> {
    if let Err(error) = signature(name, values.len())? {
        return Some(Err(error));
    }
    Some(evaluate(name, &values[0], budget))
}

fn evaluate(name: &str, value: &Value, budget: &mut usize) -> R<Value> {
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    match (name, value) {
        ("ascii", Value::Text(input)) => {
            // UTF8 PostgreSQL ascii returns the first Unicode scalar, not its first byte.
            let code = input.chars().next().map_or(0, |c| c as u32);
            reserve(budget, std::mem::size_of::<i64>())?;
            Ok(Value::Int(i64::from(code)))
        }
        ("chr", Value::Int(code)) => {
            if *code < 0 {
                return Err(SqlError::InvalidParameterValue);
            }
            // PG18 uses 54000 for NUL, surrogates and code points above U+10FFFF.
            let character = u32::try_from(*code)
                .ok()
                .filter(|n| *n != 0)
                .and_then(char::from_u32)
                .ok_or(SqlError::Limit)?;
            let size = character.len_utf8();
            reserve(budget, size)?;
            let mut output = String::with_capacity(size);
            output.push(character);
            Ok(Value::Text(output))
        }
        ("initcap", Value::Text(input)) => {
            // C locale changes only ASCII case and recognizes only ASCII alnum words.
            // Every replacement has identical UTF8 width, so the exact output size is
            // known without allocating or scanning an intermediate representation.
            // This also bounds the following scan to MAX_ROW_BYTES before it starts.
            reserve(budget, input.len())?;
            let mut output = String::with_capacity(input.len());
            let mut in_word = false;
            for character in input.chars() {
                output.push(if in_word {
                    character.to_ascii_lowercase()
                } else {
                    character.to_ascii_uppercase()
                });
                in_word = character.is_ascii_alphanumeric();
            }
            Ok(Value::Text(output))
        }
        _ => Err(SqlError::UndefinedFunction),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn character_outputs_reserve_exact_bytes_before_allocation() {
        let value = Value::Text("éHELLO_123ABC".into());
        let Value::Text(input) = &value else {
            unreachable!()
        };
        let mut budget = MAX_RESULT_BYTES - input.len();
        assert_eq!(
            eval("initcap", std::slice::from_ref(&value), &mut budget).unwrap(),
            Ok(Value::Text("éHello_123abc".into()))
        );
        assert_eq!(budget, MAX_RESULT_BYTES);
        let mut rejected = MAX_RESULT_BYTES - input.len() + 1;
        let before = rejected;
        assert_eq!(
            eval("initcap", std::slice::from_ref(&value), &mut rejected).unwrap(),
            Err(SqlError::Limit)
        );
        assert_eq!(rejected, before);

        let mut budget = MAX_RESULT_BYTES - 4;
        assert_eq!(
            eval("chr", &[Value::Int(0x1f600)], &mut budget).unwrap(),
            Ok(Value::Text("😀".into()))
        );
        assert_eq!(budget, MAX_RESULT_BYTES);
        let mut budget = MAX_RESULT_BYTES - 3;
        let before = budget;
        assert_eq!(
            eval("chr", &[Value::Int(0x1f600)], &mut budget).unwrap(),
            Err(SqlError::Limit)
        );
        assert_eq!(budget, before);

        let mut budget = MAX_RESULT_BYTES - 8;
        assert_eq!(
            eval("ascii", &[Value::Text("😀rest".into())], &mut budget).unwrap(),
            Ok(Value::Int(0x1f600))
        );
        assert_eq!(budget, MAX_RESULT_BYTES);
    }

    #[test]
    fn character_row_cap_and_errors_leave_budget_unchanged() {
        for length in [MAX_ROW_BYTES, MAX_ROW_BYTES + 1] {
            let input = Value::Text("a".repeat(length));
            let mut budget = 0;
            let result = eval("initcap", &[input], &mut budget).unwrap();
            if length == MAX_ROW_BYTES {
                assert!(matches!(result, Ok(Value::Text(s)) if s.len() == length));
                assert_eq!(budget, length);
            } else {
                assert_eq!(result, Err(SqlError::Limit));
                assert_eq!(budget, 0);
            }
        }
        for (value, error) in [
            (-1, SqlError::InvalidParameterValue),
            (0, SqlError::Limit),
            (0xd800, SqlError::Limit),
            (0xdfff, SqlError::Limit),
            (0x110000, SqlError::Limit),
        ] {
            let mut budget = 17;
            assert_eq!(
                eval("chr", &[Value::Int(value)], &mut budget).unwrap(),
                Err(error)
            );
            assert_eq!(budget, 17);
        }
    }
}
