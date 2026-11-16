//! Array and jsonb concatenation overloads; unknown operands adopt the known type.
use super::*;
fn element(ty: &DataType) -> Option<DataType> {
    Some(match ty {
        DataType::TextArray => DataType::Text,
        DataType::IntArray => DataType::Integer,
        DataType::BigIntArray => DataType::BigInt,
        _ => return None,
    })
}
pub(super) fn signature(
    a: Option<&DataType>,
    b: Option<&DataType>,
) -> Option<R<(DataType, DataType, DataType)>> {
    let ty = a
        .filter(|t| element(t).is_some())
        .or_else(|| b.filter(|t| element(t).is_some()));
    if let Some(ty) = ty {
        let at = a.unwrap_or(ty);
        let bt = b.unwrap_or(ty);
        let elem = element(ty).unwrap();
        return Some(
            if (at == ty && (bt == ty || *bt == elem)) || (bt == ty && *at == elem) {
                Ok((at.clone(), bt.clone(), ty.clone()))
            } else {
                Err(SqlError::UndefinedFunction)
            },
        );
    }
    if a == Some(&DataType::Jsonb) || b == Some(&DataType::Jsonb) {
        let at = a.unwrap_or(&DataType::Jsonb);
        let bt = b.unwrap_or(&DataType::Jsonb);
        // The text overload still permits explicitly typed text beside jsonb.
        if *at == DataType::Text || *bt == DataType::Text {
            return None;
        }
        return Some(if *at == DataType::Jsonb && *bt == DataType::Jsonb {
            Ok((at.clone(), bt.clone(), DataType::Jsonb))
        } else {
            Err(SqlError::UndefinedFunction)
        });
    }
    None
}
pub(super) fn name(a: &DataType, b: &DataType, result: &DataType) -> Option<&'static str> {
    if element(result).is_some() {
        Some(if a == result && b == result {
            "\u{1}operator:array_cat"
        } else if a == result {
            "\u{1}operator:array_append"
        } else {
            "\u{1}operator:array_prepend"
        })
    } else if *result == DataType::Jsonb {
        Some("\u{1}operator:jsonb_cat")
    } else {
        None
    }
}
pub(super) fn eval(name: &str, values: &[Value], budget: &mut usize) -> Option<R<Value>> {
    let op = name.strip_prefix("\u{1}operator:")?;
    if !matches!(
        op,
        "array_cat" | "array_append" | "array_prepend" | "jsonb_cat"
    ) {
        return None;
    }
    Some((|| {
        use crate::relational::logical_types::{array_quote, array_values, json_output};
        let out = if op == "jsonb_cat" {
            if values.contains(&Value::Null) {
                return Ok(Value::Null);
            }
            let parse = |v: &Value| -> R<serde_json::Value> {
                let Value::Text(s) = v else {
                    return Err(SqlError::Type);
                };
                serde_json::from_str(s).map_err(|_| SqlError::InvalidTextRepresentation)
            };
            use serde_json::Value as J;
            let a = parse(&values[0])?;
            let b = parse(&values[1])?;
            let merged = match (a, b) {
                (J::Object(mut a), J::Object(b)) => {
                    a.extend(b);
                    J::Object(a)
                }
                (a, b) => {
                    let mut a = match a {
                        J::Array(a) => a,
                        a => vec![a],
                    };
                    match b {
                        J::Array(b) => a.extend(b),
                        b => a.push(b),
                    };
                    J::Array(a)
                }
            };
            json_output(&merged)?
        } else {
            let array = |v: &Value| -> R<Vec<Option<String>>> {
                match v {
                    Value::Null => Ok(vec![]),
                    Value::Text(s) => array_values(s),
                    _ => Err(SqlError::Type),
                }
            };
            let scalar = |v: &Value| -> R<Option<String>> {
                match v {
                    Value::Null => Ok(None),
                    Value::Text(s) => Ok(Some(s.clone())),
                    Value::Int(n) => Ok(Some(n.to_string())),
                    _ => Err(SqlError::Type),
                }
            };
            let mut a = if op == "array_prepend" {
                vec![scalar(&values[0])?]
            } else {
                array(&values[0])?
            };
            if op == "array_append" {
                a.push(scalar(&values[1])?);
            } else {
                a.extend(array(&values[1])?);
            }
            if op == "array_cat" && values.iter().all(|v| *v == Value::Null) {
                return Ok(Value::Null);
            }
            format!(
                "{{{}}}",
                a.iter()
                    .map(|v| v.as_ref().map_or_else(|| "NULL".into(), |s| array_quote(s)))
                    .collect::<Vec<_>>()
                    .join(",")
            )
        };
        reserve(budget, out.len())?;
        Ok(Value::Text(out))
    })())
}
