//! Binding and text output for the flat, text-backed array aggregate.
use super::*;
use giggles_pg_types::temporal::SessionZone;

pub(super) fn result_type(input: &BoundScalar, kind: Aggregate) -> R<DataType> {
    use DataType::*;
    if let Some((name, args)) = aggregate_order::parts(input)
        && args[aggregate_order::input_width(name, args.len())..]
            .iter()
            .any(|key| key.result_oid() == json_aggregate::OID)
    {
        user_types::view_replace_error(
            "could not identify an ordering operator for type json".into(),
        );
        user_types::sql_error_hint("Use an explicit ordering operator or modify the query.");
        return Err(SqlError::UndefinedFunction);
    }
    Ok(match input.ty {
        Text if array_order_input(input).result_oid() == json_aggregate::OID => {
            if kind.distinct() {
                user_types::view_replace_error(
                    "could not identify an equality operator for type json".into(),
                );
                return Err(SqlError::UndefinedFunction);
            }
            DataType::ExpressionArray(super::ExpressionArray::Json)
        }
        Text => TextArray,
        SmallInt => SmallIntArray,
        Integer => IntArray,
        BigInt => BigIntArray,
        Uuid => UuidArray,
        Real => RealArray,
        PgChar => PgCharArray,
        RegDictionary => RegDictionaryArray,
        Varchar => DataType::ExpressionArray(super::ExpressionArray::Varchar),
        Numeric => DataType::ExpressionArray(super::ExpressionArray::Numeric),
        Boolean => DataType::ExpressionArray(super::ExpressionArray::Boolean),
        Date => DataType::ExpressionArray(super::ExpressionArray::Date),
        Timestamp => DataType::ExpressionArray(super::ExpressionArray::Timestamp),
        TimestampTz => DataType::ExpressionArray(super::ExpressionArray::TimestampTz),
        Jsonb => DataType::ExpressionArray(super::ExpressionArray::Jsonb),
        _ => return Err(SqlError::Unsupported),
    })
}

pub(super) fn signature_error(
    args: &[Scalar],
    resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
    tables: &[&TableDef],
) -> SqlError {
    let types = args
        .iter()
        .map(|arg| {
            if arg.aggregate_unknown(&[]) {
                return Ok("unknown");
            }
            arg.bind(resolve, tables, &[])
                .map(|arg| json_aggregate::type_name(&arg))
        })
        .collect::<R<Vec<_>>>();
    let types = match types {
        Ok(types) => types,
        Err(error) => return error,
    };
    user_types::view_replace_error(format!(
        "function array_agg({}) does not exist",
        types.join(", ")
    ));
    SqlError::UndefinedFunction
}

pub(super) fn unknown_input(arg: &Scalar, parameters: &[Option<DataType>]) -> Option<SqlError> {
    let arg = match arg {
        Scalar::Call(name, args) if aggregate_order::internal(name) => &args[0],
        arg => arg,
    };
    if arg.aggregate_unknown(parameters) {
        user_types::view_replace_error("function array_agg(unknown) is not unique".into());
        Some(SqlError::AmbiguousFunction)
    } else {
        None
    }
}

fn array_order_input(input: &BoundScalar) -> &BoundScalar {
    aggregate_order::parts(input).map_or(input, |(_, args)| &args[0])
}

pub(super) fn element(value: &Value, input: &BoundScalar, zone: SessionZone) -> R<String> {
    let text = match value {
        Value::Null => return Ok("NULL".into()),
        Value::Text(text) => return Ok(logical_types::array_quote(text)),
        Value::Bool(v) => return Ok(if *v { "t".into() } else { "f".into() }),
        Value::TimestampTz(v) => v.format_in_zone(zone).map_err(|_| SqlError::Overflow)?,
        other => {
            let scalar = array_order_input(input);
            let value = text_search_types::cast_typed(other, &scalar.ty, &DataType::Text)
                .unwrap_or_else(|| {
                    functions::coerce_clocked(other.clone(), &DataType::Text, None)
                })?;
            let Value::Text(text) = value else {
                return Err(SqlError::InternalError);
            };
            text
        }
    };
    Ok(logical_types::array_quote(&text))
}

pub(in crate::relational) fn decode_element(text: &str, array: ExpressionArray) -> R<Value> {
    functions::coerce_clocked(Value::Text(text.into()), &array.metadata().0, None)
}

pub(super) fn compare_distinct(input: Option<&BoundScalar>, a: &Value, b: &Value) -> R<Ordering> {
    if input.is_some_and(|input| input.ty == DataType::Jsonb)
        && *a != Value::Null
        && *b != Value::Null
    {
        compare_jsonb(a, b)
    } else {
        json_aggregate::compare_distinct(input, a, b)
    }
}

pub(super) fn compare_jsonb(a: &Value, b: &Value) -> R<Ordering> {
    let (Value::Text(a), Value::Text(b)) = (a, b) else {
        return Err(SqlError::InternalError);
    };
    let parse = |s| serde_json::from_str(s).map_err(|_| SqlError::InvalidTextRepresentation);
    compare_json(&parse(a)?, &parse(b)?, true)
}

fn compare_json(a: &serde_json::Value, b: &serde_json::Value, root: bool) -> R<Ordering> {
    use serde_json::Value as J;
    // PostgreSQL compares container counts before contents. The root empty
    // array sorts before the single-element wrappers used for root scalars.
    let rank = |value: &J| match value {
        J::Array(values) if root && values.is_empty() => 0,
        J::Null => 1,
        J::String(_) => 2,
        J::Number(_) => 3,
        J::Bool(_) => 4,
        J::Array(_) => 5,
        J::Object(_) => 6,
    };
    Ok(match (a, b) {
        (J::String(a), J::String(b)) => a.cmp(b),
        (J::Bool(a), J::Bool(b)) => a.cmp(b),
        (J::Number(a), J::Number(b)) => {
            let numeric = |n: &serde_json::Number| {
                Numeric::parse(&n.to_string()).map_err(|_| SqlError::InvalidTextRepresentation)
            };
            numeric(a)?.cmp(&numeric(b)?)
        }
        (J::Array(a), J::Array(b)) if a.len() == b.len() => {
            for (a, b) in a.iter().zip(b) {
                let order = compare_json(a, b, false)?;
                if !order.is_eq() {
                    return Ok(order);
                }
            }
            Ordering::Equal
        }
        (J::Array(a), J::Array(b)) => a.len().cmp(&b.len()),
        (J::Object(a), J::Object(b)) if a.len() == b.len() => {
            fn entries(value: &serde_json::Map<String, J>) -> Vec<(&String, &J)> {
                let mut entries = value.iter().collect::<Vec<_>>();
                entries.sort_by(|(a, _), (b, _)| a.len().cmp(&b.len()).then(a.cmp(b)));
                entries
            }
            for ((ka, va), (kb, vb)) in entries(a).into_iter().zip(entries(b)) {
                let order = ka.cmp(kb);
                if !order.is_eq() {
                    return Ok(order);
                }
                let order = compare_json(va, vb, false)?;
                if !order.is_eq() {
                    return Ok(order);
                }
            }
            Ordering::Equal
        }
        (J::Object(a), J::Object(b)) => a.len().cmp(&b.len()),
        _ => rank(a).cmp(&rank(b)),
    })
}
