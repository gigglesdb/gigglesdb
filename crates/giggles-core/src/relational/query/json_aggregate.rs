//! JSON array aggregation uses the ordinary GROUP BY transition and order sorter.
use super::*;
use giggles_pg_types::temporal::SessionZone;

pub(super) const OID: u32 = 114;
pub(super) const ARRAY_OID: u32 = 199;
pub(super) const TEXT_RESULT: &str = "\0json_aggregate_text_result";
pub(super) const INPUT: &str = "\0json_input";
// A cast whose input was validated at bind time or is already JSON.
pub(super) const VALIDATED_INPUT: &str = "\0json_input_validated";
pub(super) const OBJECT: &str = "jsonb_build_object";
pub(super) const RECORD_KEY: &str = "\0jsonb_object_record_key";

pub(super) fn type_name(scalar: &BoundScalar) -> &'static str {
    if scalar.result_oid() == OID {
        "json"
    } else {
        functions::typeof_expr::name(&scalar.ty)
    }
}

pub(super) fn common_branches<'a>(
    name: &str,
    branches: impl Iterator<Item = &'a BoundScalar> + Clone,
) -> R<bool> {
    if !branches.clone().any(|branch| branch.result_oid() == OID) {
        return Ok(false);
    }
    let mut names = branches.map(type_name);
    let first = names.next().ok_or(SqlError::InternalError)?;
    if let Some(other) = names.find(|other| *other != first) {
        if matches!((first, other), ("json", "jsonb") | ("jsonb", "json")) {
            user_types::view_replace_error(format!("{name} could not convert type {other} to {first}"));
            return Err(SqlError::CannotCoerce);
        }
        user_types::view_replace_error(format!("{name} types {first} and {other} cannot be matched"));
        return Err(SqlError::Type);
    }
    Ok(true)
}

pub(super) fn validate_input(text: &str) -> R<()> {
    // Validate syntax without decoding strings, normalizing numbers or discarding
    // duplicate keys. The json type preserves its original input.
    serde_json::from_str::<&serde_json::value::RawValue>(text).map_err(|_| {
        user_types::view_replace_error("invalid input syntax for type json".into());
        SqlError::InvalidTextRepresentation
    })?;
    Ok(())
}


pub(super) fn typed_text(scalar: Scalar) -> Scalar {
    Scalar::Call(TEXT_RESULT.into(), vec![scalar])
}

pub(super) fn json_result(kind: Aggregate) -> bool {
    matches!(kind.base(), Aggregate::Json | Aggregate::JsonObject)
}

fn zoned_timestamp(value: TimestampTz, zone: SessionZone) -> R<String> {
    let local = zone.to_local(value).map_err(|_| SqlError::Overflow)?;
    let text = local.to_string().replacen(' ', "T", 1);
    if matches!(value.microseconds(), i64::MIN | i64::MAX) {
        return Ok(text);
    }
    let offset =
        ((i128::from(local.microseconds()) - i128::from(value.microseconds())) / 1_000_000) as i32;
    let sign = if offset < 0 { '-' } else { '+' };
    let offset = offset.unsigned_abs();
    let (local, era) = text
        .strip_suffix(" BC")
        .map_or((text.as_str(), ""), |s| (s, " BC"));
    let seconds = offset % 60;
    let mut text = format!("{local}{sign}{:02}:{:02}", offset / 3600, offset / 60 % 60);
    if seconds != 0 {
        text.push_str(&format!(":{seconds:02}"));
    }
    text.push_str(era);
    Ok(text)
}

pub(super) fn result_type(kind: Aggregate) -> Option<DataType> {
    match kind.base() {
        Aggregate::Json | Aggregate::JsonObject => Some(DataType::Text),
        Aggregate::Jsonb | Aggregate::JsonbObject => Some(DataType::Jsonb),
        _ => None,
    }
}

pub(super) fn object(kind: Aggregate) -> bool {
    matches!(kind.base(), Aggregate::JsonObject | Aggregate::JsonbObject)
}

pub(super) fn element(value: &Value, ty: &DataType, zone: SessionZone) -> R<String> {
    let quote = |text: &str| serde_json::to_string(text).map_err(|_| SqlError::Type);
    match value {
        Value::Null => Ok("null".into()),
        Value::Int(n) if *ty == DataType::Oid => quote(&n.to_string()),
        Value::Int(n) if *ty != DataType::RegType => Ok(n.to_string()),
        Value::Bool(v) => Ok(v.to_string()),
        Value::Timestamp(v) => quote(&v.to_string().replacen(' ', "T", 1)),
        Value::TimestampTz(v) => quote(&zoned_timestamp(*v, zone)?),
        Value::Text(s) if *ty == DataType::Jsonb => Ok(s.clone()),
        Value::Text(s) if ty.is_array() => {
            let element_type = plan::array_element_type(ty)?;
            let entries = logical_types::array_values(s)?
                .into_iter()
                .map(|item| {
                    let value = item.map_or(Ok(Value::Null), |s| {
                        functions::coerce_clocked(Value::Text(s), &element_type, None)
                    })?;
                    element(&value, &element_type, zone)
                })
                .collect::<R<Vec<_>>>()?;
            Ok(format!("[{}]", entries.join(",")))
        }
        Value::Text(s) => quote(s),
        other => {
            let Value::Text(s) = functions::coerce_clocked(other.clone(), &DataType::Text, None)?
            else {
                return Err(SqlError::Type);
            };
            if matches!(
                ty,
                DataType::Numeric | DataType::Real | DataType::DoublePrecision
            ) && !matches!(s.as_str(), "NaN" | "Infinity" | "-Infinity")
            {
                Ok(s)
            } else {
                quote(&s)
            }
        }
    }
}

pub(super) fn bound_element(value: &Value, scalar: &BoundScalar, zone: SessionZone) -> R<String> {
    if scalar.result_oid() == OID
        && let Value::Text(text) = value
    {
        return Ok(text.clone());
    }
    element(value, &scalar.ty, zone)
}

pub(super) fn push(
    value: &mut Value,
    input: &Value,
    scalar: &BoundScalar,
    kind: Aggregate,
    zone: SessionZone,
    retention: &mut scan::Retention,
) -> R<()> {
    let mut entry = bound_element(input, scalar, zone)?;
    if matches!(kind, Aggregate::Jsonb) {
        entry =
            logical_types::json_output(&serde_json::from_str(&entry).map_err(|_| SqlError::Type)?)?;
    }
    let separator =
        if matches!(kind, Aggregate::Json) && scalar.ty.is_array() && *input != Value::Null {
            ", \n "
        } else {
            ", "
        };
    let bytes = entry.len() + separator.len();
    let old = match value {
        Value::Null => 0,
        Value::Text(text) => text.len(),
        _ => return Err(SqlError::Type),
    };
    if old.saturating_add(bytes) > crate::relational::max_value_bytes() {
        return Err(SqlError::Limit);
    }
    retention.charge(bytes)?;
    match value {
        Value::Null => *value = Value::Text(format!("[{entry}")),
        Value::Text(text) => {
            text.push_str(separator);
            text.push_str(&entry);
        }
        _ => unreachable!(),
    }
    Ok(())
}

pub(super) fn build_object(
    args: &[BoundScalar],
    values: &[Value],
    zone: SessionZone,
    budget: &mut usize,
) -> R<Value> {
    if !values.len().is_multiple_of(2) {
        user_types::view_replace_error("argument list must have even number of elements".into());
        user_types::sql_error_hint(
            "The arguments of jsonb_build_object() must consist of alternating keys and values.",
        );
        return Err(SqlError::InvalidParameterValue);
    }
    let kind = Aggregate::JsonbObject;
    let mut object = Value::Text(String::new());
    let mut retention = scan::Retention::new(crate::relational::max_value_bytes());
    let (value_pairs, _) = values.as_chunks::<2>();
    let (argument_pairs, _) = args.as_chunks::<2>();
    for (index, (input, args)) in value_pairs.iter().zip(argument_pairs).enumerate() {
        if input[0] == Value::Null {
            user_types::view_replace_error(format!("argument {}: key must not be null", index * 2 + 1));
            return Err(SqlError::InvalidParameterValue);
        }
        if args[0].result_oid() == OID || args[0].is_json_object_record_key() {
            return Err(invalid_object_key());
        }
        push_object(&mut object, input, args, kind, zone, &mut retention)?;
    }
    if values.is_empty() {
        object = Value::Text("{".into());
    }
    finish(&mut object, kind, budget)?;
    Ok(object)
}

fn invalid_object_key() -> SqlError {
    user_types::view_replace_error("key value must be scalar, not array, composite, or json".into());
    SqlError::InvalidParameterValue
}

pub(super) fn compare_distinct(input: Option<&BoundScalar>, a: &Value, b: &Value) -> R<Ordering> {
    match (a, b) {
        (Value::Null, Value::Null) => Ok(Ordering::Equal),
        (Value::Null, _) => Ok(Ordering::Greater),
        (_, Value::Null) => Ok(Ordering::Less),
        _ => input.map_or_else(|| Ok(cmp(a, b)), |input| input.try_compare_values(a, b)),
    }
}

pub(super) fn push_object(
    state: &mut Value,
    input: &[Value],
    args: &[BoundScalar],
    kind: Aggregate,
    zone: SessionZone,
    retention: &mut scan::Retention,
) -> R<()> {
    let binary = matches!(kind.base(), Aggregate::JsonbObject);
    let key = &input[0];
    if *key == Value::Null {
        let (message, error) = if binary {
            (
                "field name must not be null",
                SqlError::InvalidParameterValue,
            )
        } else {
            (
                "null value not allowed for object key",
                SqlError::NullValueNotAllowed,
            )
        };
        user_types::view_replace_error(message.into());
        return Err(error);
    }
    let key = object_key(key, &args[0], zone)?;
    let entry = bound_element(&input[1], &args[1], zone)?;
    let bytes = key.len() + entry.len() + 6;
    let text = match state {
        Value::Text(text) => text,
        Value::Null => {
            *state = Value::Text(String::new());
            let Value::Text(text) = state else {
                unreachable!()
            };
            text
        }
        _ => return Err(SqlError::Type),
    };
    if text.len().saturating_add(bytes) > crate::relational::max_value_bytes() {
        return Err(SqlError::Limit);
    }
    retention.charge(bytes)?;
    text.push_str(if text.is_empty() { "{ " } else { ", " });
    text.push_str(&key);
    text.push_str(" : ");
    text.push_str(&entry);
    Ok(())
}

pub(super) fn object_key(key: &Value, scalar: &BoundScalar, zone: SessionZone) -> R<String> {
    if *key == Value::Null {
        user_types::view_replace_error("field name must not be null".into());
        return Err(SqlError::InvalidParameterValue);
    }
    let key_type = &scalar.ty;
    if key_type.is_array() || *key_type == DataType::Jsonb || scalar.result_oid() == OID {
        return Err(invalid_object_key());
    }
    let key = element(key, key_type, zone)?;
    if key.starts_with('"') {
        Ok(key)
    } else {
        serde_json::to_string(&key).map_err(|_| SqlError::Type)
    }
}
pub(super) fn finish(value: &mut Value, kind: Aggregate, work: &mut usize) -> R<()> {
    if let Value::Text(text) = value {
        if object(kind) {
            text.push_str(" }");
            if matches!(kind.base(), Aggregate::JsonbObject) {
                *text = logical_types::json_output(
                    &serde_json::from_str(text).map_err(|_| SqlError::Type)?,
                )?;
            }
        } else {
            text.push(']');
        }
        charge(work, value)?;
    }
    Ok(())
}
