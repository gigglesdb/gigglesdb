//! Scalar JSONB operations needed by JSONB document updates.
use super::*;
use serde_json::Value as Json;

pub(super) const SET_TYPES: [DataType; 4] = [
    DataType::Jsonb,
    DataType::TextArray,
    DataType::Jsonb,
    DataType::Boolean,
];

pub(super) fn bind_set(original: &[Scalar], args: &mut [BoundScalar]) -> R<()> {
    if !(3..=4).contains(&args.len()) {
        return Err(SqlError::UndefinedFunction);
    }
    for ((original, arg), ty) in original.iter().zip(args).zip(SET_TYPES.iter()) {
        if let Scalar::Literal(value @ (Value::Text(_) | Value::Null)) = original {
            arg.node = literal_cast_node(value.clone(), ty)?;
            arg.ty = ty.clone();
        }
        if &arg.ty != ty {
            return Err(SqlError::UndefinedFunction);
        }
    }
    Ok(())
}

fn parse(value: &Value) -> R<Json> {
    let Value::Text(text) = value else {
        return Err(SqlError::Type);
    };
    serde_json::from_str(text).map_err(|_| SqlError::InvalidTextRepresentation)
}

pub(super) fn json_type(value: &Value) -> R<Value> {
    let name = match parse(value)? {
        Json::Null => "null",
        Json::Bool(_) => "boolean",
        Json::Number(_) => "number",
        Json::String(_) => "string",
        Json::Array(_) => "array",
        Json::Object(_) => "object",
    };
    Ok(Value::Text(name.into()))
}

pub(super) fn bind_type(original: &[Scalar], args: &mut [BoundScalar]) -> R<()> {
    let ([original], [arg]) = (original, args) else {
        return Err(SqlError::UndefinedFunction);
    };
    if let Scalar::Literal(value @ (Value::Text(_) | Value::Null)) = original {
        arg.node = literal_cast_node(value.clone(), &DataType::Jsonb)?;
        arg.ty = DataType::Jsonb;
    }
    if arg.ty == DataType::Jsonb {
        Ok(())
    } else {
        Err(SqlError::UndefinedFunction)
    }
}

pub(super) fn set(args: &[Value]) -> R<Value> {
    let mut document = parse(&args[0])?;
    let Value::Text(path) = &args[1] else {
        return Err(SqlError::Type);
    };
    let path = logical_types::array_values(path)?;
    if !document.is_array() && !document.is_object() {
        user_types::view_replace_error("cannot set path in scalar".into());
        return Err(SqlError::InvalidParameterValue);
    }
    if path.is_empty() {
        return Ok(args[0].clone());
    }
    let create = args.get(3).is_none_or(|value| *value == Value::Bool(true));
    set_path(&mut document, &path, parse(&args[2])?, create, 1)?;
    Ok(Value::Text(logical_types::json_output(&document)?))
}

fn set_path(
    value: &mut Json,
    path: &[Option<String>],
    replacement: Json,
    create: bool,
    position: usize,
) -> R<()> {
    let [key, tail @ ..] = path else {
        return Ok(());
    };
    let Some(key) = key else {
        user_types::view_replace_error(format!("path element at position {position} is null"));
        return Err(SqlError::NullValueNotAllowed);
    };
    match value {
        Json::Object(entries) => {
            if tail.is_empty() && (create || entries.contains_key(key)) {
                entries.insert(key.clone(), replacement);
            } else if let Some(child) = entries.get_mut(key) {
                set_path(child, tail, replacement, create, position + 1)?;
            }
        }
        Json::Array(entries) => {
            let index = key.trim_start_matches(|c: char| c.is_ascii_whitespace() || c == '\x0b').parse::<i32>().map_err(|_| {
                user_types::view_replace_error(format!(
                    r#"path element at position {position} is not an integer: "{key}""#
                ));
                SqlError::InvalidTextRepresentation
            })?;
            let mut index = i64::from(index);
            if index < 0 {
                index += entries.len() as i64;
            }
            if let Ok(index) = usize::try_from(index)
                && let Some(child) = entries.get_mut(index)
            {
                if tail.is_empty() {
                    *child = replacement;
                } else {
                    set_path(child, tail, replacement, create, position + 1)?;
                }
            } else if tail.is_empty() && create {
                if index < 0 {
                    entries.insert(0, replacement);
                } else {
                    entries.push(replacement);
                }
            }
        }
        _ => {}
    }
    Ok(())
}

pub(super) fn contains(left: &Value, right: &Value, budget: &mut usize) -> R<bool> {
    let left = parse(left)?;
    let mut right = parse(right)?;
    if left.is_array() && !right.is_array() && !right.is_object() {
        right = Json::Array(vec![right]);
    }
    contains_value(&left, &right, budget)
}

pub(super) fn equal(left: &Value, right: &Value, budget: &mut usize) -> R<bool> {
    equal_value(&parse(left)?, &parse(right)?, budget)
}

fn equal_value(left: &Json, right: &Json, budget: &mut usize) -> R<bool> {
    reserve(budget, 1)?;
    match (left, right) {
        (Json::Object(left), Json::Object(right)) if left.len() == right.len() => {
            for (key, value) in left {
                let Some(other) = right.get(key) else {
                    return Ok(false);
                };
                if !equal_value(value, other, budget)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        (Json::Array(left), Json::Array(right)) if left.len() == right.len() => {
            for (a, b) in left.iter().zip(right) {
                if !equal_value(a, b, budget)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        (Json::Number(a), Json::Number(b)) => equal_numbers(a, b),
        _ => Ok(left == right),
    }
}

fn equal_numbers(a: &serde_json::Number, b: &serde_json::Number) -> R<bool> {
    let numeric = |n: &serde_json::Number| {
        Numeric::parse(&n.to_string()).map_err(|_| SqlError::InvalidTextRepresentation)
    };
    Ok(cmp(&Value::Numeric(numeric(a)?), &Value::Numeric(numeric(b)?)).is_eq())
}

fn contains_value(left: &Json, right: &Json, budget: &mut usize) -> R<bool> {
    reserve(budget, 1)?;
    match (left, right) {
        (Json::Object(left), Json::Object(right)) => {
            for (key, value) in right {
                let Some(found) = left.get(key) else {
                    return Ok(false);
                };
                if !contains_value(found, value, budget)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        (Json::Array(left), Json::Array(right)) => {
            for value in right {
                let mut found = false;
                for element in left {
                    if contains_value(element, value, budget)? {
                        found = true;
                        break;
                    }
                }
                if !found {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        (Json::Number(a), Json::Number(b)) => equal_numbers(a, b),
        _ => Ok(left == right),
    }
}
