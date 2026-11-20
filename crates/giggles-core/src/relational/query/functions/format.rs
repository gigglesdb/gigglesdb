//! PostgreSQL format() scanning and bounded text construction.
use super::*;

fn number(input: &[u8], at: &mut usize) -> R<Option<i32>> {
    let start = *at;
    let mut value = 0i32;
    while let Some(byte) = input.get(*at).filter(|b| b.is_ascii_digit()) {
        value = value
            .checked_mul(10)
            .and_then(|n| n.checked_add(i32::from(*byte - b'0')))
            .ok_or(SqlError::Overflow)?;
        *at += 1;
    }
    Ok((*at != start).then_some(value))
}
fn position(value: i32) -> R<usize> {
    if value == 0 {
        Err(SqlError::InvalidParameterValue)
    } else {
        Ok(value as usize - 1)
    }
}
fn output_size(out: &str, extra: usize) -> R<()> {
    if out
        .len()
        .checked_add(extra)
        .is_none_or(|n| n > crate::relational::max_value_bytes())
    {
        Err(SqlError::Limit)
    } else {
        Ok(())
    }
}
fn append(out: &mut String, value: &str, budget: &mut usize) -> R<()> {
    output_size(out, value.len())?;
    reserve(budget, value.len())?;
    out.push_str(value);
    Ok(())
}
fn padding(out: &mut String, count: usize, budget: &mut usize) -> R<()> {
    output_size(out, count)?;
    reserve(budget, count)?;
    out.extend(std::iter::repeat_n(' ', count));
    Ok(())
}
fn value_text(value: &Value, budget: &mut usize) -> R<String> {
    // concat's conversion admits the exact bytes before allocating, and preserves
    // PostgreSQL output spelling (not the distinct explicit bool-to-text cast).
    match text::eval("concat", std::slice::from_ref(value), budget).unwrap()? {
        Value::Text(s) => Ok(s),
        _ => unreachable!(),
    }
}
fn width(value: &Value, ty: &DataType, budget: &mut usize) -> R<Option<i32>> {
    if *value == Value::Null {
        return Ok(None);
    }
    if *ty == DataType::Integer
        && let Value::Int(n) = value
    {
        return i32::try_from(*n).map(Some).map_err(|_| SqlError::Overflow);
    }
    let input = value_text(value, budget)?;
    match cast_value(Value::Text(input), &DataType::Integer, budget)? {
        Value::Int(n) => Ok(Some(n as i32)),
        _ => unreachable!(),
    }
}

pub(super) fn eval(values: &[Value], arguments: &[BoundScalar], budget: &mut usize) -> R<Value> {
    if values[0] == Value::Null {
        return Ok(Value::Null);
    }
    let Value::Text(format) = &values[0] else {
        return Err(SqlError::Type);
    };
    let bytes = format.as_bytes();
    let mut out = String::new();
    let mut at = 0;
    let mut next = 0usize;
    let args = &values[1..];
    let types = &arguments[1..];
    while at < bytes.len() {
        let start = at;
        while at < bytes.len() && bytes[at] != b'%' {
            at += 1;
        }
        append(&mut out, &format[start..at], budget)?;
        if at == bytes.len() {
            break;
        }
        at += 1;
        if bytes.get(at) == Some(&b'%') {
            append(&mut out, "%", budget)?;
            at += 1;
            continue;
        }
        let digits = number(bytes, &mut at)?;
        let mut argument = None;
        let mut field_width = None;
        let mut width_argument = None;
        if let Some(n) = digits {
            if bytes.get(at) == Some(&b'$') {
                argument = Some(position(n)?);
                at += 1;
            } else {
                field_width = Some(n);
            }
        }
        let mut left = false;
        if field_width.is_none() {
            while bytes.get(at) == Some(&b'-') {
                left = true;
                at += 1;
            }
            if bytes.get(at) == Some(&b'*') {
                at += 1;
                let explicit = number(bytes, &mut at)?;
                let index = if let Some(n) = explicit {
                    if bytes.get(at) != Some(&b'$') {
                        return Err(SqlError::InvalidParameterValue);
                    }
                    at += 1;
                    position(n)?
                } else {
                    next
                };
                width_argument = Some(index);
            } else {
                field_width = number(bytes, &mut at)?;
            }
        }
        let kind = *bytes.get(at).ok_or(SqlError::InvalidParameterValue)?;
        if !matches!(kind, b's' | b'I' | b'L') {
            return Err(SqlError::InvalidParameterValue);
        }
        at += 1;
        if let Some(index) = width_argument {
            next = index + 1;
            let v = args.get(index).ok_or(SqlError::InvalidParameterValue)?;
            field_width = width(v, &types[index].ty, budget)?;
        }
        let field_width = field_width.unwrap_or(0);
        if field_width < 0 {
            left = true;
        }
        let field_width = field_width.checked_abs().ok_or(SqlError::Overflow)? as usize;
        let index = argument.unwrap_or(next);
        next = index + 1;
        let value = args.get(index).ok_or(SqlError::InvalidParameterValue)?;
        // Conversions and quoting reserve their own temporary output before
        // allocation. append separately admits the final output chunk.
        let rendered = if *value == Value::Null {
            match kind {
                b'I' => return Err(SqlError::NullValueNotAllowed),
                b'L' => {
                    reserve(budget, 4)?;
                    "NULL".into()
                }
                _ => String::new(),
            }
        } else {
            let value = value_text(value, budget)?;
            if kind == b's' {
                value
            } else {
                let name = if kind == b'I' {
                    "quote_ident"
                } else {
                    "quote_literal"
                };
                match quoting::eval(name, &[Value::Text(value)], budget).unwrap()? {
                    Value::Text(s) => s,
                    _ => unreachable!(),
                }
            }
        };
        let spaces = field_width.saturating_sub(rendered.chars().count());
        output_size(
            &out,
            rendered.len().checked_add(spaces).ok_or(SqlError::Limit)?,
        )?;
        if !left {
            padding(&mut out, spaces, budget)?;
        }
        append(&mut out, &rendered, budget)?;
        if left {
            padding(&mut out, spaces, budget)?;
        }
    }
    Ok(Value::Text(out))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bounded_chunks_reject_before_push() {
        let mut out = String::from("kept");
        let mut budget = MAX_RESULT_BYTES;
        assert_eq!(append(&mut out, "x", &mut budget), Err(SqlError::Limit));
        assert_eq!(out, "kept");
        assert_eq!(
            padding(&mut out, MAX_ROW_BYTES, &mut 0),
            Err(SqlError::Limit)
        );
        assert_eq!(out, "kept");
    }
}
