//! PostgreSQL 18 text operations, UTF8 encoding and libc C locale.
use super::*;
use std::collections::{HashMap, HashSet};
type Signature = (Vec<Option<DataType>>, DataType);
pub(super) fn signature(n: &str, count: usize) -> Option<R<Signature>> {
    let (min, max, ints, result) = match n {
        "char_length" | "character_length" | "octet_length" | "bit_length" => {
            (1, 1, vec![], DataType::Integer)
        }
        "reverse" => (1, 1, vec![], DataType::Text),
        "starts_with" => (2, 2, vec![], DataType::Boolean),
        "strpos" => (2, 2, vec![], DataType::Integer),
        "repeat" | "left" | "right" => (2, 2, vec![1], DataType::Text),
        "replace" | "translate" => (3, 3, vec![], DataType::Text),
        "lpad" | "rpad" => (2, 3, vec![1], DataType::Text),
        "btrim" | "ltrim" | "rtrim" => (1, 2, vec![], DataType::Text),
        "split_part" => (3, 3, vec![2], DataType::Text),
        "concat" => (1, MAX_COLUMNS, vec![], DataType::Text),
        "concat_ws" => (2, MAX_COLUMNS, vec![], DataType::Text),
        _ => return None,
    };
    Some(if count < min || count > max {
        Err(SqlError::Type)
    } else {
        Ok((
            (0..count)
                .map(|i| {
                    if n == "concat" || n == "concat_ws" && i > 0 {
                        None
                    } else {
                        Some(if ints.contains(&i) {
                            DataType::Integer
                        } else {
                            DataType::Text
                        })
                    }
                })
                .collect(),
            result,
        ))
    })
}
pub(super) fn eval(n: &str, v: &[Value], budget: &mut usize) -> Option<R<Value>> {
    if let Err(e) = signature(n, v.len())? {
        return Some(Err(e));
    }
    Some(evaluate(n, v, budget))
}
fn evaluate(n: &str, v: &[Value], budget: &mut usize) -> R<Value> {
    let output = |s: &str, budget: &mut usize| {
        reserve(budget, s.len())?;
        Ok(Value::Text(s.into()))
    };
    if n == "concat" || n == "concat_ws" {
        if n == "concat_ws" && v[0] == Value::Null {
            return Ok(Value::Null);
        }
        let sep = if n == "concat_ws" { text(&v[0])? } else { "" };
        // Determine exact bytes without allocating clones or conversion temporaries.
        let args = &v[usize::from(n == "concat_ws")..];
        let mut size = 0usize;
        let mut count = 0usize;
        for value in args {
            let len = match value {
                Value::Date(n) => n.to_string().len(),
                Value::Time(n) => n.to_string().len(),
                Value::TimeTz(n) => n.to_string().len(),
                Value::Timestamp(n) => n.to_string().len(),
                Value::TimestampTz(n) => n.to_string().len(),
                Value::Interval(n) => n.to_string().len(),
                Value::Numeric(n) => n.to_string().len(),
                Value::Float4(n) => n.to_text().len(),
                Value::Float8(n) => n.to_text().len(),
                Value::Null => continue,
                Value::Text(s) => s.len(),
                Value::Bool(_) => 1,
                Value::Int(n) => {
                    let magnitude = n.unsigned_abs();
                    (if magnitude == 0 {
                        1
                    } else {
                        magnitude.ilog10() as usize + 1
                    }) + usize::from(*n < 0)
                }
                Value::Bytes(b) => b
                    .len()
                    .checked_mul(2)
                    .and_then(|n| n.checked_add(2))
                    .ok_or(SqlError::Limit)?,
            };
            size = size.checked_add(len).ok_or(SqlError::Limit)?;
            if count > 0 {
                size = size.checked_add(sep.len()).ok_or(SqlError::Limit)?;
            }
            count += 1;
        }
        reserve(budget, size)?;
        let mut out = String::with_capacity(size);
        let mut first = true;
        use std::fmt::Write;
        for value in args {
            if matches!(value, Value::Null) {
                continue;
            }
            if !first {
                out.push_str(sep);
            }
            first = false;
            match value {
                Value::Date(n) => write!(out, "{n}").map_err(|_| SqlError::Type)?,
                Value::Time(n) => write!(out, "{n}").map_err(|_| SqlError::Type)?,
                Value::TimeTz(n) => write!(out, "{n}").map_err(|_| SqlError::Type)?,
                Value::Timestamp(n) => write!(out, "{n}").map_err(|_| SqlError::Type)?,
                Value::TimestampTz(n) => write!(out, "{n}").map_err(|_| SqlError::Type)?,
                Value::Interval(n) => write!(out, "{n}").map_err(|_| SqlError::Type)?,
                Value::Numeric(n) => write!(out, "{n}").map_err(|_| SqlError::Type)?,
                Value::Float4(n) => out.push_str(&n.to_text()),
                Value::Float8(n) => out.push_str(&n.to_text()),
                Value::Null => unreachable!(),
                Value::Text(s) => out.push_str(s),
                Value::Bool(b) => out.push(if *b { 't' } else { 'f' }),
                Value::Int(n) => write!(out, "{n}").map_err(|_| SqlError::Type)?,
                Value::Bytes(b) => {
                    out.push_str("\\x");
                    for byte in b {
                        write!(out, "{byte:02x}").map_err(|_| SqlError::Type)?;
                    }
                }
            }
        }
        return Ok(Value::Text(out));
    }
    if v.contains(&Value::Null) {
        return Ok(Value::Null);
    }
    if let Value::Bytes(b) = &v[0] {
        let size = match n {
            "octet_length" => b.len(),
            "bit_length" => b.len().checked_mul(8).ok_or(SqlError::Overflow)?,
            _ => return Err(SqlError::Type),
        };
        return Ok(Value::Int(
            i32::try_from(size).map_err(|_| SqlError::Overflow)? as i64,
        ));
    }
    let s = text(&v[0])?;
    let num = |i: usize| match v[i] {
        Value::Int(n) => Ok(n),
        _ => Err(SqlError::Type),
    };
    match n {
        "char_length" | "character_length" | "octet_length" | "bit_length" => {
            let count = match n {
                "octet_length" => s.len(),
                "bit_length" => s.len().checked_mul(8).ok_or(SqlError::Overflow)?,
                _ => s.chars().count(),
            };
            Ok(Value::Int(
                i32::try_from(count).map_err(|_| SqlError::Overflow)? as i64,
            ))
        }
        "starts_with" => Ok(Value::Bool(s.starts_with(text(&v[1])?))),
        "strpos" => Ok(Value::Int(
            s.find(text(&v[1])?)
                .map_or(0, |i| s[..i].chars().count() as i64 + 1),
        )),
        "reverse" => {
            reserve(budget, s.len())?;
            Ok(Value::Text(s.chars().rev().collect()))
        }
        "repeat" => {
            let count = num(1)?.max(0) as usize;
            let size = s.len().checked_mul(count).ok_or(SqlError::Limit)?;
            reserve(budget, size)?;
            Ok(Value::Text(s.repeat(count)))
        }
        "replace" => {
            let from = text(&v[1])?;
            let to = text(&v[2])?;
            if from.is_empty() {
                return output(s, budget);
            }
            let count = s.matches(from).count();
            let size = (s.len() - count * from.len())
                .checked_add(count.checked_mul(to.len()).ok_or(SqlError::Limit)?)
                .ok_or(SqlError::Limit)?;
            reserve(budget, size)?;
            Ok(Value::Text(s.replace(from, to)))
        }
        "translate" => {
            let mut map = HashMap::new();
            let mut to = text(&v[2])?.chars();
            for c in text(&v[1])?.chars() {
                let replacement = to.next();
                map.entry(c).or_insert(replacement);
            }
            let chars = || {
                s.chars()
                    .filter_map(|c| map.get(&c).copied().unwrap_or(Some(c)))
            };
            let size = chars().try_fold(0usize, |a, c| {
                a.checked_add(c.len_utf8()).ok_or(SqlError::Limit)
            })?;
            reserve(budget, size)?;
            Ok(Value::Text(chars().collect()))
        }
        "left" | "right" => {
            let len = s.chars().count();
            let k = num(1)?;
            // PG18 right(text, INT_MIN) retains the full input (int32 negation edge).
            let count = if n == "right" && k == i32::MIN as i64 {
                len
            } else if k >= 0 {
                (k as usize).min(len)
            } else {
                len.saturating_sub(k.unsigned_abs() as usize)
            };
            let (skip, take) = if n == "left" {
                (0, count)
            } else {
                (len - count, count)
            };
            let start = s.char_indices().nth(skip).map_or(s.len(), |(i, _)| i);
            let end = s
                .char_indices()
                .nth(skip + take)
                .map_or(s.len(), |(i, _)| i);
            output(&s[start..end], budget)
        }
        "lpad" | "rpad" => {
            let count = num(1)?.max(0) as usize;
            let len = s.chars().count();
            if count <= len {
                let end = s.char_indices().nth(count).map_or(s.len(), |(i, _)| i);
                return output(&s[..end], budget);
            }
            let pad = if v.len() == 3 { text(&v[2])? } else { " " };
            if pad.is_empty() {
                return output(s, budget);
            }
            let needed = count - len;
            let plen = pad.chars().count();
            let cycles = needed / plen;
            let rem = needed % plen;
            let tail = pad.char_indices().nth(rem).map_or(pad.len(), |(i, _)| i);
            let size = pad
                .len()
                .checked_mul(cycles)
                .and_then(|x| x.checked_add(tail))
                .and_then(|x| x.checked_add(s.len()))
                .ok_or(SqlError::Limit)?;
            reserve(budget, size)?;
            let mut out = String::with_capacity(size);
            if n == "rpad" {
                out.push_str(s);
            }
            for _ in 0..cycles {
                out.push_str(pad);
            }
            out.push_str(&pad[..tail]);
            if n == "lpad" {
                out.push_str(s);
            }
            Ok(Value::Text(out))
        }
        "btrim" | "ltrim" | "rtrim" => {
            let set: HashSet<char> = if v.len() == 2 { text(&v[1])? } else { " " }
                .chars()
                .collect();
            let has = |c: char| set.contains(&c);
            output(
                match n {
                    "ltrim" => s.trim_start_matches(has),
                    "rtrim" => s.trim_end_matches(has),
                    _ => s.trim_matches(has),
                },
                budget,
            )
        }
        "split_part" => {
            let sep = text(&v[1])?;
            let k = num(2)?;
            if k == 0 {
                return Err(SqlError::Type);
            }
            if sep.is_empty() {
                return output(if k == 1 || k == -1 { s } else { "" }, budget);
            }
            let index = if k > 0 {
                k as usize - 1
            } else {
                let len = s.split(sep).count();
                let abs = k.unsigned_abs() as usize;
                if abs > len {
                    return output("", budget);
                }
                len - abs
            };
            output(s.split(sep).nth(index).unwrap_or(""), budget)
        }
        _ => Err(SqlError::Unsupported),
    }
}
fn text(v: &Value) -> R<&str> {
    if let Value::Text(s) = v {
        Ok(s)
    } else {
        Err(SqlError::Type)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strpos_bytea_is_an_undefined_function() {
        let state = Relational::default();
        for (sql, types) in [
            (
                "SELECT strpos($1,$2)",
                vec![Some(DataType::Bytes), Some(DataType::Bytes)],
            ),
            (
                "SELECT strpos($1,$2)",
                vec![Some(DataType::Bytes), Some(DataType::Text)],
            ),
            (
                "SELECT strpos($1,$2)",
                vec![Some(DataType::Text), Some(DataType::Bytes)],
            ),
            ("SELECT strpos($1,$2)", vec![Some(DataType::Bytes)]),
            ("SELECT strpos($1::bytea, 'a')", vec![]),
            ("SELECT strpos('a', $1::bytea)", vec![]),
            ("SELECT strpos('a'::bytea, 'a'::bytea)", vec![]),
        ] {
            let prepared = Prepared::parse(sql, 1, state.epoch)
                .unwrap()
                .with_parameter_types(&types)
                .unwrap();
            assert!(
                matches!(prepared.describe(&state), Err(SqlError::UndefinedFunction)),
                "{sql}, {types:?}"
            );
        }
    }

    #[test]
    fn declared_bytea_bare_projection_preserves_types_and_names() {
        let mut state = Relational::default();
        for (sql, declarations, parameters, types) in [
            (
                "SELECT $1",
                vec![Some(DataType::Bytes)],
                vec![Value::Bytes(vec![1, 2, 3])],
                vec![DataType::Bytes],
            ),
            (
                "SELECT $1, $2",
                vec![Some(DataType::Bytes), None],
                vec![Value::Bytes(vec![1, 2, 3]), Value::Text("hello".into())],
                vec![DataType::Bytes, DataType::Text],
            ),
        ] {
            let prepared = Prepared::parse(sql, 1, state.epoch)
                .unwrap()
                .with_parameter_types(&declarations)
                .unwrap();
            let description = prepared.describe(&state).unwrap();
            assert_eq!(description.parameters, types);
            assert_eq!(
                description
                    .columns
                    .iter()
                    .map(|c| c.data_type.clone())
                    .collect::<Vec<_>>(),
                types
            );
            assert!(description.columns.iter().all(|c| c.name == "?column?"));
            assert_eq!(
                state
                    .execute(&prepared, &parameters, None)
                    .unwrap()
                    .rows
                    .into_iter()
                    .map(|row| row.values)
                    .collect::<Vec<_>>(),
                vec![parameters]
            );
        }
        let inferred = Prepared::parse("SELECT $1::bytea, $2", 1, state.epoch).unwrap();
        assert!(matches!(
            inferred.describe(&state),
            Err(SqlError::Parameters)
        ));
    }

    #[test]
    fn declared_text_bytea_cast_decodes_input_and_preserves_errors() {
        let mut state = Relational::default();
        let prepared = Prepared::parse("SELECT $1::bytea", 1, state.epoch)
            .unwrap()
            .with_parameter_types(&[Some(DataType::Text)])
            .unwrap();
        let description = prepared.describe(&state).unwrap();
        assert_eq!(description.parameters, vec![DataType::Text]);
        assert_eq!(description.columns[0].data_type, DataType::Bytes);
        for (input, output) in [("abc", &b"abc"[..]), ("A", &b"A"[..]), (r"\x41", &b"A"[..])] {
            assert_eq!(
                state
                    .execute(&prepared, &[Value::Text(input.into())], None)
                    .unwrap()
                    .rows
                    .into_iter()
                    .map(|row| row.values)
                    .collect::<Vec<_>>(),
                vec![vec![Value::Bytes(output.to_vec())]]
            );
        }
        for (input, error) in [
            (r"\x4", SqlError::InvalidParameterValue),
            (r"\xzz", SqlError::InvalidParameterValue),
            (r"\zz", SqlError::InvalidTextRepresentation),
        ] {
            assert_eq!(
                state.execute(&prepared, &[Value::Text(input.into())], None),
                Err(error)
            );
        }
    }

    #[test]
    fn concat_reserves_exact_bytes_once_and_rejects_atomically() {
        for (name, args, expected) in [
            (
                "concat",
                vec![Value::Text("x".repeat(32768)); 2],
                "x".repeat(65536),
            ),
            (
                "concat_ws",
                vec![
                    Value::Text("::".into()),
                    Value::Text("x".repeat(32767)),
                    Value::Null,
                    Value::Text("x".repeat(32767)),
                ],
                format!("{}::{}", "x".repeat(32767), "x".repeat(32767)),
            ),
            (
                "concat",
                vec![
                    Value::Int(i64::MIN),
                    Value::Int(0),
                    Value::Bool(false),
                    Value::Bytes(vec![0, 255]),
                ],
                "-92233720368547758080f\\x00ff".into(),
            ),
        ] {
            let mut budget = MAX_RESULT_BYTES - expected.len();
            assert_eq!(
                evaluate(name, &args, &mut budget),
                Ok(Value::Text(expected.clone()))
            );
            assert_eq!(budget, MAX_RESULT_BYTES);
            let mut budget = MAX_RESULT_BYTES - expected.len() + 1;
            let before = budget;
            assert_eq!(evaluate(name, &args, &mut budget), Err(SqlError::Limit));
            assert_eq!(budget, before);
        }
    }
}
