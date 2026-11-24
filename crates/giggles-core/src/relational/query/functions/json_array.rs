//! PostgreSQL JSON array builders for scalar values and explicit VARIADIC arrays.
use super::*;
use giggles_pg_types::temporal::SessionZone;

const JSON: &str = "json_build_array";
const JSONB: &str = "jsonb_build_array";
const JSON_VARIADIC: &str = "\0variadic_json_build_array";
const JSONB_VARIADIC: &str = "\0variadic_jsonb_build_array";

pub(super) fn name(name: &str) -> Option<&'static str> {
    match name {
        JSON | JSON_VARIADIC => Some(JSON),
        JSONB | JSONB_VARIADIC => Some(JSONB),
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
            .map(|arg| arg.bind(resolve, tables, parameters))
            .collect::<R<Vec<_>>>()?
    };
    let ty = if name(function) == Some(JSONB) {
        DataType::Jsonb
    } else {
        DataType::Text
    };
    Ok(BoundScalar {
        ty,
        node: Node::ZonedOperator(function.into(), args, None),
    })
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
    let canonical = name(call) == Some(JSONB);
    let explicit = variadic(call);
    if explicit && values[0] == Value::Null {
        return Ok(Value::Null);
    }
    reserve(budget, 2)?;
    let mut output = String::from("[");
    let mut append = |entry: String| {
        let entry = if canonical {
            logical_types::json_output(&serde_json::from_str(&entry).map_err(|_| {
                user_types::view_replace_error("invalid input syntax for type json".into());
                SqlError::InvalidTextRepresentation
            })?)?
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
