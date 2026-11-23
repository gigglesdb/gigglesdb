//! Set-returning JSONB functions use the existing canonical JSONB representation.
use super::*;
use serde_json::Value as Json;

pub(super) fn known(name: &str) -> bool {
    matches!(
        name,
        "jsonb_array_elements"
            | "jsonb_array_elements_text"
            | "jsonb_each"
            | "jsonb_each_text"
            | "jsonb_object_keys"
    )
}

pub(super) fn record(name: &str) -> bool {
    matches!(name, "jsonb_each" | "jsonb_each_text")
}

pub(super) fn element_column(name: &str) -> bool {
    matches!(name, "jsonb_array_elements" | "jsonb_array_elements_text")
}

pub(super) fn output_type(name: &str) -> DataType {
    if name.ends_with("_text") || name == "jsonb_object_keys" {
        DataType::Text
    } else {
        DataType::Jsonb
    }
}

pub(super) fn emit(name: &str, input: &Value, emit: &mut impl FnMut(Vec<Value>) -> R<()>) -> R<()> {
    let Value::Text(input) = input else {
        return Err(SqlError::Type);
    };
    let document: Json =
        serde_json::from_str(input).map_err(|_| SqlError::InvalidTextRepresentation)?;
    let text = name.ends_with("_text");
    let cell = |value: Json| -> R<Value> {
        if text {
            match value {
                Json::Null => return Ok(Value::Null),
                Json::String(s) => return Ok(Value::Text(s)),
                _ => {}
            }
        }
        Ok(Value::Text(logical_types::json_output(&value)?))
    };
    let message = if name.starts_with("jsonb_array_elements") {
        match document {
            Json::Array(items) => {
                for item in items {
                    emit(vec![cell(item)?])?;
                }
                return Ok(());
            }
            Json::Object(_) => "cannot extract elements from an object".into(),
            _ => "cannot extract elements from a scalar".into(),
        }
    } else if let Json::Object(items) = document {
        let mut items = items.into_iter().collect::<Vec<_>>();
        items.sort_by(|(a, _), (b, _)| a.len().cmp(&b.len()).then(a.cmp(b)));
        for (key, value) in items {
            let mut row = vec![Value::Text(key)];
            if record(name) {
                row.push(cell(value)?);
            }
            emit(row)?;
        }
        return Ok(());
    } else if name == "jsonb_object_keys" {
        let kind = if document.is_array() {
            "an array"
        } else {
            "a scalar"
        };
        format!("cannot call {name} on {kind}")
    } else {
        format!("cannot call {name} on a non-object")
    };
    user_types::view_replace_error(message);
    Err(SqlError::InvalidParameterValue)
}
