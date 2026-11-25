//! PostgreSQL JSON constructors and typed JSON access.
use super::*;
use giggles_pg_types::temporal::SessionZone;

const JSON: &str = "json_build_array";
const JSONB: &str = "jsonb_build_array";
const JSON_VARIADIC: &str = "\0variadic_json_build_array";
const JSONB_VARIADIC: &str = "\0variadic_jsonb_build_array";
pub(in crate::relational::query) const ROW: &str = "\0row_to_json";
const TO_JSON_ROW: &str = "\0to_json_row";
const TO_JSONB_ROW: &str = "\0to_jsonb_row";

pub(super) fn name(name: &str) -> Option<&str> {
    match name {
        JSON | JSON_VARIADIC => Some(JSON),
        JSONB | JSONB_VARIADIC => Some(JSONB),
        ROW => Some("row_to_json"),
        TO_JSON_ROW => Some("to_json"),
        TO_JSONB_ROW => Some("to_jsonb"),
        "json_build_object" | json_aggregate::OBJECT | "to_json" | "to_jsonb" | "json_scalar" => {
            Some(name)
        }
        _ => None,
    }
}

pub(super) fn variadic_name(name: &str) -> Option<&'static str> {
    match name {
        JSON => Some(JSON_VARIADIC),
        JSONB => Some(JSONB_VARIADIC),
        _ => None,
    }
}

pub(super) fn binary(call: &str) -> bool {
    matches!(name(call), Some(JSONB | json_aggregate::OBJECT | "to_jsonb"))
}

pub(in crate::relational::query) fn row_name(call: &str) -> Option<&'static str> {
    match call {
        "row_to_json" => Some(ROW),
        "to_json" => Some(TO_JSON_ROW),
        "to_jsonb" => Some(TO_JSONB_ROW),
        _ => None,
    }
}

pub(super) fn parse_row(
    p: &mut Parser,
    row_name: &str,
    depth: usize,
    aggregate_argument: bool,
) -> R<Scalar> {
    p.at += 2;
    let mut fields = vec![];
    if !p.sym(')') {
        loop {
            if fields.len() >= MAX_COLUMNS * 2 {
                return Err(SqlError::Limit);
            }
            fields.push(Scalar::Literal(Value::Text(format!(
                "f{}",
                fields.len() / 2 + 1
            ))));
            fields.push(Scalar::parse_expr(p, depth + 1, aggregate_argument)?);
            if !p.sym(',') {
                p.punct(')')?;
                break;
            }
        }
    }
    let pretty = if row_name == ROW && p.sym(',') {
        Scalar::parse_expr(p, depth + 1, aggregate_argument)?
    } else {
        Scalar::Literal(Value::Bool(false))
    };
    p.punct(')')?;
    fields.insert(0, pretty);
    Ok(Scalar::Call(row_name.into(), fields))
}

pub(in crate::relational::query) fn lower_row(
    scalar: &mut Scalar,
    columns: &impl Fn(&ColumnRef) -> Option<Vec<(String, Scalar)>>,
) {
    let Scalar::Call(call, args) = scalar else {
        return;
    };
    let Some(row_call) = row_name(call) else {
        return;
    };
    let [Scalar::Column(column), rest @ ..] = args.as_slice() else {
        return;
    };
    if column.qualifier.is_some() || rest.len() > usize::from(row_call == ROW) {
        return;
    }
    let Some(columns) = columns(column) else {
        return;
    };
    let pretty = rest
        .first()
        .cloned()
        .unwrap_or(Scalar::Literal(Value::Bool(false)));
    let fields = columns
        .into_iter()
        .flat_map(|(key, value)| [Scalar::Literal(Value::Text(key)), value]);
    *scalar = Scalar::Call(
        row_call.into(),
        std::iter::once(pretty).chain(fields).collect(),
    );
}

fn variadic(name: &str) -> bool {
    matches!(name, JSON_VARIADIC | JSONB_VARIADIC)
}

pub(super) fn bind(
    function: &str,
    args: &[Scalar],
    resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
    tables: &[&TableDef],
    parameters: &[Option<DataType>],
) -> R<BoundScalar> {
    match function {
        "to_json" | "to_jsonb" | "json_scalar" => {
            let [arg] = args else {
                return Err(SqlError::UndefinedFunction);
            };
            if arg.aggregate_unknown(parameters) && function != "json_scalar" {
                return Err(SqlError::Type);
            }
        }
        "json_build_object" if !args.len().is_multiple_of(2) => {
            return Err(SqlError::InvalidParameterValue);
        }
        _ => {}
    }
    let row = matches!(function, ROW | TO_JSON_ROW | TO_JSONB_ROW);
    let args = if variadic(function) {
        let [arg] = args else {
            return Err(SqlError::UndefinedFunction);
        };
        let arg = match arg {
            Scalar::Call(name, items) if name == array_constructor::NAME => {
                array_constructor::bind(
                    items,
                    array_constructor::Binding::VariadicJson,
                    resolve,
                    tables,
                    parameters,
                )?
            }
            Scalar::Cast(source, ty)
                if ty.is_array()
                    && matches!(source.as_ref(), Scalar::Literal(Value::Text(literal)) if multidimensional(literal)) =>
            {
                return Err(SqlError::Unsupported);
            }
            _ => arg.bind(resolve, tables, parameters)?,
        };
        if !arg.ty.is_array() {
            user_types::view_replace_error("VARIADIC argument must be an array".into());
            return Err(SqlError::Type);
        }
        vec![arg]
    } else {
        args.iter()
            .enumerate()
            .map(|(i, arg)| {
                if row && i == 0 && matches!(arg, Scalar::Literal(Value::Text(_) | Value::Null)) {
                    return Scalar::Cast(Box::new(arg.clone()), DataType::Boolean)
                        .bind(resolve, tables, parameters);
                }
                let bound = arg.bind(resolve, tables, parameters)?;
                preserve_json_column(arg, bound, resolve, tables)
            })
            .collect::<R<Vec<_>>>()?
    };
    if row && args[0].ty != DataType::Boolean {
        return Err(SqlError::UndefinedFunction);
    }
    let ty = if binary(function) {
        DataType::Jsonb
    } else {
        DataType::Text
    };
    Ok(BoundScalar {
        ty,
        node: Node::ZonedOperator(function.into(), args, None),
    })
}

pub(super) fn preserve_json_column(
    arg: &Scalar,
    bound: BoundScalar,
    resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
    tables: &[&TableDef],
) -> R<BoundScalar> {
    if let Scalar::Column(column) = arg {
        let column = resolve(column, tables.len())?;
        if collation::materialized_json(tables[column.source].columns[column.column].id) {
            // Materialized json is stored as text but embeds as JSON.
            return Ok(BoundScalar {
                ty: bound.ty.clone(),
                node: Node::Call(json_aggregate::TEXT_RESULT.into(), vec![bound]),
            });
        }
    }
    Ok(bound)
}

fn multidimensional(literal: &str) -> bool {
    literal
        .trim_start()
        .strip_prefix('{')
        .is_some_and(|inner| inner.trim_start().starts_with('{'))
}

pub(super) fn eval(
    call: &str,
    args: &[BoundScalar],
    values: &[Value],
    zone: Option<SessionZone>,
    budget: &mut usize,
) -> R<Value> {
    let zone = zone.unwrap_or(SessionZone::Fixed(0));
    let canonical = binary(call);
    if matches!(call, "to_json" | "to_jsonb" | "json_scalar") {
        if values[0] == Value::Null {
            return Ok(Value::Null);
        }
        let entry = json_aggregate::bound_element(&values[0], &args[0], zone)?;
        let text = if canonical {
            canonicalize(&entry)?
        } else {
            entry
        };
        reserve(budget, text.len())?;
        return Ok(Value::Text(text));
    }
    if matches!(call, ROW | TO_JSON_ROW | TO_JSONB_ROW) {
        if values[0] == Value::Null {
            return Ok(Value::Null);
        }
        return object(
            &args[1..],
            &values[1..],
            zone,
            canonical,
            true,
            values[0] == Value::Bool(true),
            budget,
        );
    }
    if call == "json_build_object" {
        return object(args, values, zone, canonical, false, false, budget);
    }
    let explicit = variadic(call);
    if explicit && values[0] == Value::Null {
        return Ok(Value::Null);
    }
    reserve(budget, 2)?;
    let mut output = String::from("[");
    let mut append = |entry: String| {
        let entry = if canonical {
            canonicalize(&entry)?
        } else {
            entry
        };
        let separator = if output.len() == 1 { "" } else { ", " };
        reserve(budget, separator.len() + entry.len())?;
        output.push_str(separator);
        output.push_str(&entry);
        Ok::<_, SqlError>(())
    };
    if explicit {
        let Value::Text(array) = &values[0] else {
            return Err(SqlError::Type);
        };
        let element = array_constructor::element_type(&args[0].ty).ok_or(SqlError::Type)?;
        let json = args[0].result_oid() == json_aggregate::ARRAY_OID;
        for item in logical_types::array_values(array)? {
            let entry = match item {
                None => "null".into(),
                Some(text) if json => text,
                Some(text) => {
                    let value = coerce_clocked(Value::Text(text), &element, None)?;
                    json_aggregate::element(&value, &element, zone)?
                }
            };
            append(entry)?;
        }
    } else {
        for (arg, value) in args.iter().zip(values) {
            append(json_aggregate::bound_element(value, arg, zone)?)?;
        }
    }
    output.push(']');
    Ok(Value::Text(output))
}

fn canonicalize(text: &str) -> R<String> {
    logical_types::json_output(&serde_json::from_str(text).map_err(|_| {
        user_types::view_replace_error("invalid input syntax for type json".into());
        SqlError::InvalidTextRepresentation
    })?)
}

fn object(
    args: &[BoundScalar],
    values: &[Value],
    zone: SessionZone,
    canonical: bool,
    row: bool,
    pretty: bool,
    budget: &mut usize,
) -> R<Value> {
    let mut output = String::from("{");
    reserve(budget, 2)?;
    let (fields, _) = args.as_chunks::<2>();
    let (values, _) = values.as_chunks::<2>();
    for (fields, values) in fields.iter().zip(values) {
        let key = json_aggregate::object_key(&values[0], &fields[0], zone)?;
        let entry = json_aggregate::bound_element(&values[1], &fields[1], zone)?;
        let separator = if output.len() == 1 {
            ""
        } else if pretty {
            ",\n "
        } else if row {
            ","
        } else {
            ", "
        };
        let colon = if row { ":" } else { " : " };
        reserve(
            budget,
            separator.len() + key.len() + colon.len() + entry.len(),
        )?;
        output.push_str(separator);
        output.push_str(&key);
        output.push_str(colon);
        output.push_str(&entry);
    }
    output.push('}');
    if canonical {
        output = canonicalize(&output)?;
    }
    Ok(Value::Text(output))
}

pub(super) fn access(name: &str, values: &[Value]) -> R<Value> {
    use serde_json::value::RawValue;

    let [Value::Text(json), key] = values else {
        return Ok(Value::Null);
    };
    let invalid = |_| SqlError::InvalidTextRepresentation;
    let raw = match (json.trim_start().as_bytes().first(), key) {
        (Some(b'['), Value::Int(index)) => {
            let array = serde_json::from_str::<Vec<&RawValue>>(json).map_err(invalid)?;
            let index = if *index < 0 {
                array.len() as i64 + index
            } else {
                *index
            };
            usize::try_from(index)
                .ok()
                .and_then(|i| array.get(i))
                .copied()
        }
        (Some(b'{'), Value::Text(key)) => {
            serde_json::from_str::<std::collections::BTreeMap<String, &RawValue>>(json)
                .map_err(invalid)?
                .get(key)
                .copied()
        }
        _ => None,
    };
    let Some(raw) = raw.map(RawValue::get) else {
        return Ok(Value::Null);
    };
    if name == "\0json_text" {
        if raw == "null" {
            return Ok(Value::Null);
        }
        if raw.starts_with('"') {
            return serde_json::from_str(raw).map(Value::Text).map_err(invalid);
        }
    }
    Ok(Value::Text(raw.into()))
}
