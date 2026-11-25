//! Array and jsonb concatenation overloads; unknown operands adopt the known type.
use super::*;
use array_constructor::{element_type, query_only};
use giggles_pg_types::temporal::SessionZone;

fn string(ty: &DataType) -> bool {
    matches!(ty, DataType::Text | DataType::Varchar)
}

pub(super) fn check_json_operand(operands: &[Option<BoundScalar>; 2]) -> R<()> {
    use DataType::ExpressionArray as Array;
    for [array, scalar] in [[&operands[0], &operands[1]], [&operands[1], &operands[0]]] {
        if let (Some(array), Some(scalar)) = (array, scalar)
            && query_only(&array.ty)
            && scalar.ty == DataType::Text
            && (array.ty == Array(ExpressionArray::Json))
                != (scalar.result_oid() == json_aggregate::OID)
        {
            return Err(SqlError::UndefinedFunction);
        }
    }
    Ok(())
}
pub(super) fn signature(
    a: Option<&DataType>,
    b: Option<&DataType>,
) -> Option<R<(DataType, DataType, DataType)>> {
    let ty = a
        .filter(|t| element_type(t).is_some())
        .or_else(|| b.filter(|t| element_type(t).is_some()));
    if let Some(ty) = ty {
        let at = a.unwrap_or(ty);
        let bt = b.unwrap_or(ty);
        let elem = element_type(ty).unwrap();
        if query_only(at) || query_only(bt) {
            let other = if at == ty { bt } else { at };
            let other_element = element_type(other);
            let string_arrays = other_element.as_ref().is_some_and(|other| string(&elem) && string(other))
                && ![at, bt].contains(&&DataType::ExpressionArray(ExpressionArray::Json));
            let result = if at == bt || string_arrays {
                Ok((ty.clone(), ty.clone(), ty.clone()))
            } else if other_element.is_none()
                && (*other == elem || string(&elem) && string(other)
                    || elem == DataType::Numeric && integer(other)
                    || elem == DataType::Real && (integer(other) || *other == DataType::Numeric))
            {
                if at == ty {
                    Ok((ty.clone(), elem, ty.clone()))
                } else {
                    Ok((elem, ty.clone(), ty.clone()))
                }
            } else if (other_element.is_none()
                && ty != &DataType::ExpressionArray(ExpressionArray::Json)
                && array_constructor::common_elements(&elem, other))
                || array_constructor::mixed_query_arrays(at, bt)
            {
                Err(SqlError::Unsupported)
            } else {
                Err(SqlError::UndefinedFunction)
            };
            return Some(result);
        }
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

pub(super) fn check_literal(value: &Value, target: &DataType) -> R<()> {
    if query_only(target) && let Value::Text(text) = value {
        let text = text.trim_start();
        if text.starts_with('[') || text.strip_prefix('{').is_some_and(|s| s.trim_start().starts_with('{')) {
            return Err(SqlError::Unsupported);
        }
    }
    Ok(())
}
pub(super) fn name(a: &DataType, b: &DataType, result: &DataType) -> Option<&'static str> {
    if element_type(result).is_some() {
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
pub(super) fn array_operator(name: &str) -> bool {
    matches!(name, "\u{1}operator:array_cat" | "\u{1}operator:array_append" | "\u{1}operator:array_prepend")
}

pub(super) fn bind_enum_operands(sources: [&Scalar; 2], args: &mut [BoundScalar]) -> R<()> {
    let Some(enum_type) = args.iter().find_map(BoundScalar::attached_enum).cloned() else {
        return Ok(());
    };
    for (source, bound) in sources.into_iter().zip(args.iter_mut()) {
        if let Scalar::Literal(value @ (Value::Text(_) | Value::Null)) = source {
            *bound = BoundScalar::enum_literal(value, &bound.ty, &enum_type)?;
        }
    }
    if !args.iter().all(|arg| arg.attached_enum() == Some(&enum_type)) {
        return Err(enum_order::missing_operator("||", &args[0], &args[1]));
    }
    Ok(())
}

pub(super) fn eval(
    name: &str,
    args: &[BoundScalar],
    values: &[Value],
    zone: SessionZone,
    budget: &mut usize,
) -> Option<R<Value>> {
    let op = name.strip_prefix("\u{1}operator:")?;
    if !matches!(
        op,
        "array_cat" | "array_append" | "array_prepend" | "jsonb_cat"
    ) {
        return None;
    }
    Some((|| {
        use crate::relational::logical_types::json_output;
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
            let mut a = if op == "array_prepend" {
                vec![array_aggregate::encode_element(&values[0], &args[0].ty, zone)?]
            } else {
                encoded_array(&args[0], &values[0], zone)?
            };
            if op == "array_append" {
                a.push(array_aggregate::encode_element(&values[1], &args[1].ty, zone)?);
            } else {
                a.extend(encoded_array(&args[1], &values[1], zone)?);
            }
            if op == "array_cat" && values.iter().all(|v| *v == Value::Null) {
                return Ok(Value::Null);
            }
            format!("{{{}}}", a.join(","))
        };
        reserve(budget, out.len())?;
        Ok(Value::Text(out))
    })())
}

fn encoded_array(arg: &BoundScalar, value: &Value, zone: SessionZone) -> R<Vec<String>> {
    let text = match value {
        Value::Null => return Ok(vec![]),
        Value::Text(text) => text,
        _ => return Err(SqlError::Type),
    };
    // The scalar binder gives unknown literals the other operand's array type
    // after checking the array syntax. Check and normalize the element inputs.
    let input_type = if query_only(&arg.ty) && matches!(arg.node, Node::Literal(_) | Node::Param(_)) {
        element_type(&arg.ty)
    } else {
        None
    };
    logical_types::array_values(text)?
        .into_iter()
        .map(|item| match (item, &input_type) {
            (None, _) => Ok("NULL".into()),
            (Some(text), Some(element)) => {
                let value = if arg.ty == DataType::ExpressionArray(ExpressionArray::Json) {
                    json_aggregate::validate_input(&text)?;
                    Value::Text(text)
                } else if *element == DataType::TimestampTz {
                    use giggles_pg_types::temporal::SessionSettings;
                    Value::TimestampTz(TimestampTz::parse_in_session(
                        &text, &SessionSettings {
                            date_order: crate::relational::clock::LiteralDateOrder::current(),
                            zone,
                        },
                    ).map_err(pg_error)?)
                } else {
                    coerce_clocked(Value::Text(text), element, None)?
                };
                array_aggregate::encode_element(&value, element, zone)
            }
            (Some(text), None) => Ok(logical_types::array_quote(&text)),
        })
        .collect()
}
