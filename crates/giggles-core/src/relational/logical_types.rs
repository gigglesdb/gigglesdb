use super::*;
impl DataType {
    pub fn is_extended_text(&self) -> bool {
        matches!(
            self,
            Self::Uuid | Self::Jsonb | Self::IntArray | Self::BigIntArray | Self::TextArray
        )
    }
}
pub fn coerce(value: Value, ty: &DataType) -> R<Value> {
    let Value::Text(s) = value else {
        return if value == Value::Null {
            Ok(value)
        } else {
            Err(SqlError::Type)
        };
    };
    let bad = || SqlError::InvalidTextRepresentation;
    Ok(Value::Text(match ty {
        DataType::Uuid => {
            let s = s
                .strip_prefix('{')
                .and_then(|s| s.strip_suffix('}'))
                .unwrap_or(&s);
            let mut digits = String::new();
            for c in s.chars() {
                if c == '-' {
                    if digits.is_empty() || !digits.len().is_multiple_of(4) {
                        return Err(bad());
                    }
                } else if c.is_ascii_hexdigit() {
                    digits.push(c.to_ascii_lowercase());
                } else {
                    return Err(bad());
                }
            }
            if digits.len() != 32 || s.ends_with('-') || s.contains("--") {
                return Err(bad());
            }
            format!(
                "{}-{}-{}-{}-{}",
                &digits[..8],
                &digits[8..12],
                &digits[12..16],
                &digits[16..20],
                &digits[20..]
            )
        }
        DataType::Jsonb => {
            json_output(&serde_json::from_str::<serde_json::Value>(&s).map_err(|_| bad())?)?
        }
        DataType::IntArray | DataType::BigIntArray | DataType::TextArray => {
            let vals = array_values(&s)?;
            let vals = vals
                .into_iter()
                .map(|v| {
                    v.map(|s| match ty {
                        DataType::IntArray => giggles_pg_types::integer::parse_i32(&s)
                            .map(|n| n.to_string())
                            .map_err(integer_input_error),
                        DataType::BigIntArray => giggles_pg_types::integer::parse_i64(&s)
                            .map(|n| n.to_string())
                            .map_err(integer_input_error),
                        _ => Ok(array_quote(&s)),
                    })
                    .transpose()
                    .map(|s| s.unwrap_or_else(|| "NULL".into()))
                })
                .collect::<R<Vec<_>>>()?;
            format!("{{{}}}", vals.join(","))
        }
        _ => return Err(SqlError::Type),
    }))
}
pub fn json_output(v: &serde_json::Value) -> R<String> {
    use serde_json::Value as J;
    Ok(match v {
        J::Object(m) => {
            let mut entries = m.iter().collect::<Vec<_>>();
            entries.sort_by(|(a, _), (b, _)| a.len().cmp(&b.len()).then(a.cmp(b)));
            let mut out = Vec::new();
            for (k, v) in entries {
                if k.contains('\0') {
                    return Err(SqlError::InvalidTextRepresentation);
                }
                out.push(format!(
                    "{}: {}",
                    serde_json::to_string(k).unwrap(),
                    json_output(v)?
                ));
            }
            format!("{{{}}}", out.join(", "))
        }
        J::Array(a) => format!(
            "[{}]",
            a.iter().map(json_output).collect::<R<Vec<_>>>()?.join(", ")
        ),
        J::String(s) => {
            if s.contains('\0') {
                return Err(SqlError::InvalidTextRepresentation);
            }
            serde_json::to_string(s).unwrap()
        }
        J::Number(n) => Numeric::parse(&n.to_string())
            .map_err(|_| SqlError::InvalidTextRepresentation)?
            .to_string(),
        _ => v.to_string(),
    })
}
pub fn array_quote(s: &str) -> String {
    if s.is_empty()
        || s.eq_ignore_ascii_case("null")
        || s.chars()
            .any(|c| c.is_whitespace() || "{},\\\"".contains(c))
    {
        format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""))
    } else {
        s.into()
    }
}
pub fn array_values(s: &str) -> R<Vec<Option<String>>> {
    let bad = || {
        user_types::view_replace_error(format!("malformed array literal: \"{s}\""));
        SqlError::InvalidTextRepresentation
    };
    let inner = s
        .trim()
        .strip_prefix('{')
        .and_then(|s| s.strip_suffix('}'))
        .ok_or_else(bad)?;
    if inner.trim().is_empty() {
        return Ok(Vec::new());
    }
    let mut chars = inner.chars().peekable();
    let mut out = Vec::new();
    loop {
        while chars.peek().is_some_and(|c| c.is_whitespace()) {
            chars.next();
        }
        let quoted = chars.peek() == Some(&'"');
        if quoted {
            chars.next();
        }
        let mut v = String::new();
        let mut escaped = false;
        let mut closed = !quoted;
        while let Some(c) = chars.next() {
            if c == '\\' {
                v.push(chars.next().ok_or_else(bad)?);
                escaped = true;
            } else if quoted && c == '"' {
                closed = true;
                break;
            } else if !quoted && c == ',' {
                break;
            } else if !quoted && "{}\"".contains(c) {
                return Err(bad());
            } else {
                v.push(c);
            }
        }
        if !closed {
            return Err(bad());
        }
        if quoted {
            while chars.peek().is_some_and(|c| c.is_whitespace()) {
                chars.next();
            }
            if chars.peek().is_some() && chars.next() != Some(',') {
                return Err(bad());
            }
        }
        let v = if quoted { v } else { v.trim().into() };
        if !quoted && v.is_empty() {
            return Err(bad());
        }
        out.push(if !quoted && !escaped && v.eq_ignore_ascii_case("NULL") {
            None
        } else {
            Some(v)
        });
        if chars.peek().is_none() {
            if inner.trim_end().ends_with(',') {
                return Err(bad());
            }
            break;
        }
    }
    Ok(out)
}
pub fn eval(name: &str, ty: &DataType, values: &[Value]) -> R<Value> {
    if values.contains(&Value::Null) {
        return Ok(Value::Null);
    }
    let Value::Text(s) = &values[0] else {
        return Err(SqlError::Type);
    };
    if name == "array_length" || name == "\0array_subscript" {
        let Value::Int(index) = values[1] else {
            return Err(SqlError::Type);
        };
        let vals = array_values(s)?;
        if name == "array_length" {
            return Ok(if index == 1 && !vals.is_empty() {
                Value::Int(vals.len() as i64)
            } else {
                Value::Null
            });
        }
        let Some(Some(v)) = index
            .checked_sub(1)
            .and_then(|i| usize::try_from(i).ok())
            .and_then(|i| vals.get(i))
        else {
            return Ok(Value::Null);
        };
        return Ok(match ty {
            DataType::TextArray => Value::Text(v.clone()),
            _ => Value::Int(v.parse().map_err(|_| SqlError::InvalidTextRepresentation)?),
        });
    }
    let json: serde_json::Value =
        serde_json::from_str(s).map_err(|_| SqlError::InvalidTextRepresentation)?;
    let found = match (&json, &values[1]) {
        (serde_json::Value::Object(m), Value::Text(k)) => m.get(k),
        (serde_json::Value::Array(a), Value::Int(i)) => {
            let i = if *i < 0 { a.len() as i64 + *i } else { *i };
            usize::try_from(i).ok().and_then(|i| a.get(i))
        }
        (v, Value::Int(0 | -1)) if !v.is_object() && !v.is_array() => Some(v),
        _ => None,
    };
    let Some(found) = found else {
        return Ok(Value::Null);
    };
    Ok(if name == "\0json_get" {
        Value::Text(json_output(found)?)
    } else {
        match found {
            serde_json::Value::Null => Value::Null,
            serde_json::Value::String(s) => Value::Text(s.clone()),
            _ => Value::Text(json_output(found)?),
        }
    })
}

fn integer_input_error(error: giggles_pg_types::integer::IntegerError) -> SqlError {
    use giggles_pg_types::integer::IntegerError;
    match error {
        IntegerError::Syntax => SqlError::InvalidTextRepresentation,
        IntegerError::Range => SqlError::Overflow,
        IntegerError::Limit => SqlError::Limit,
    }
}

/// The existing array codec accepts flat, one-based arrays only. Compare decoded
/// elements, with NULL elements last, then the length (the only varying shape).
pub(super) fn compare_text_arrays(
    a: &Value,
    b: &Value,
    mut element: impl FnMut(&str, &str) -> std::cmp::Ordering,
) -> R<std::cmp::Ordering> {
    use std::cmp::Ordering;
    validate_text_array(a)?;
    validate_text_array(b)?;
    let (Value::Text(a), Value::Text(b)) = (a, b) else {
        return Ok(match (a, b) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => Ordering::Less,
            (_, Value::Null) => Ordering::Greater,
            _ => return Err(SqlError::InternalError),
        });
    };
    let a = array_values(a)?;
    let b = array_values(b)?;
    Ok(a.iter().zip(&b).map(|(a, b)| match (a, b) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Greater,
        (Some(_), None) => Ordering::Less,
        (Some(a), Some(b)) => element(a, b),
    }).find(|o| *o != Ordering::Equal).unwrap_or_else(|| a.len().cmp(&b.len())))
}

/// Validate before values enter infallible sort/map comparators.
pub(crate) fn validate_text_array(value: &Value) -> R<()> {
    match value {
        Value::Null => Ok(()),
        Value::Text(text) => array_values(text).map(|_| ()),
        _ => Err(SqlError::InternalError),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn malformed_array_comparison_is_an_error() {
        let error = compare_text_arrays(&Value::Text("{a,b".into()),
            &Value::Text("{a}".into()), str::cmp).unwrap_err();
        assert_eq!(crate::sqlstate(&crate::CoreError::Sql(error)), "22P02");
        assert_eq!(take_sql_error_message().as_deref(), Some("malformed array literal: \"{a,b\""));
    }
    #[test]
    fn invalid_array_operand_is_internal_error() {
        for other in [Value::Null, Value::Text("{}".into())] {
            let error=compare_text_arrays(&Value::Int(1), &other, str::cmp).unwrap_err();
            assert_eq!(crate::sqlstate(&crate::CoreError::Sql(error)), "XX000");
        }
    }
}
