//! ARRAY constructors use the existing text-backed array codec and scalar casts.
use super::*;
pub(in crate::relational::query) const NAME: &str = "\0array_constructor";

pub(super) enum Binding<'a> {
    Inferred,
    Cast(&'a DataType),
    VariadicJson,
}

pub(super) fn parse(p: &mut Parser, depth: usize, aggregate: bool) -> R<Scalar> {
    p.need("array")?;
    p.punct('[')?;
    let mut args = vec![];
    if !p.sym(']') {
        loop {
            args.push(Scalar::parse_expr(p, depth + 1, aggregate)?);
            if args.len() > MAX_COLUMNS {
                return Err(SqlError::Limit);
            }
            if !p.sym(',') {
                break;
            }
        }
        p.punct(']')?;
    }
    Ok(Scalar::Call(NAME.into(), args))
}
pub(super) fn element_type(array: &DataType) -> Option<DataType> {
    Some(match array {
        DataType::IntArray => DataType::Integer,
        DataType::BigIntArray => DataType::BigInt,
        DataType::TextArray => DataType::Text,
        DataType::UuidArray => DataType::Uuid,
        DataType::RealArray => DataType::Real,
        DataType::PgCharArray => DataType::PgChar,
        DataType::SmallIntArray => DataType::SmallInt,
        _ => return None,
    })
}
fn hint(
    args: &[Scalar],
    resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
    tables: &[&TableDef],
    parameters: &[Option<DataType>],
) -> R<DataType> {
    let types = args
        .iter()
        .filter(|a| !a.aggregate_unknown(parameters))
        .map(|a| a.bind(resolve, tables, parameters).map(|b| b.ty))
        .collect::<R<Vec<_>>>()?;
    common_result_type(types)
}
pub(super) fn infer(
    args: &[Scalar],
    target: Option<&DataType>,
    parameters: &mut [Option<DataType>],
    resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
    tables: &[&TableDef],
) -> R<()> {
    for a in args {
        a.infer(None, parameters, resolve, tables)?;
    }
    let ty = if let Some(ty) = target.and_then(element_type) {
        ty
    } else {
        hint(args, resolve, tables, parameters)?
    };
    for a in args {
        let expected = a.aggregate_unknown(parameters).then_some(&ty);
        a.infer(expected, parameters, resolve, tables)?;
    }
    Ok(())
}
pub(super) fn bind(
    args: &[Scalar],
    binding: Binding<'_>,
    resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
    tables: &[&TableDef],
    parameters: &[Option<DataType>],
) -> R<BoundScalar> {
    let target = match binding {
        Binding::Cast(ty) => Some(ty),
        _ => None,
    };
    let element = if let Some(ty) = target.and_then(element_type) {
        ty
    } else {
        hint(args, resolve, tables, parameters)?
    };
    let ty = match element {
        DataType::Integer => DataType::IntArray,
        DataType::BigInt => DataType::BigIntArray,
        DataType::Text => DataType::TextArray,
        DataType::Uuid => DataType::UuidArray,
        DataType::Real => DataType::RealArray,
        DataType::PgChar => DataType::PgCharArray,
        DataType::SmallInt => DataType::SmallIntArray,
        _ => return Err(SqlError::Unsupported),
    };
    if target.is_none() && element == DataType::Text {
        let bound = args
            .iter()
            .map(|arg| arg.bind(resolve, tables, parameters))
            .collect::<R<Vec<_>>>()?;
        let typed = bound.iter().zip(args).filter_map(|(bound, source)| {
            (!source.aggregate_unknown(parameters)).then_some(bound)
        });
        if json_aggregate::common_branches("ARRAY", typed.clone())? {
            // The variadic builders consume this array directly. Other SQL
            // array operations lack a general JSON element type.
            if !matches!(binding, Binding::VariadicJson) {
                return Err(SqlError::Unsupported);
            }
            let args = args
                .iter()
                .zip(bound)
                .map(|(source, bound)| {
                    if source.aggregate_unknown(parameters) {
                        Scalar::Call(json_aggregate::INPUT.into(), vec![source.clone()])
                            .bind(resolve, tables, parameters)
                    } else {
                        Ok(bound)
                    }
                })
                .collect::<R<Vec<_>>>()?;
            return Ok(BoundScalar { ty, node: Node::Call(NAME.into(), args) });
        }
        if let Some(enum_type) = enum_order::common_enum("ARRAY", typed.map(BoundScalar::type_identity))? {
            let enum_type = enum_type.clone();
            let args = args.iter().zip(bound).map(|(source, bound)| {
                if let Scalar::Literal(value @ (Value::Text(_) | Value::Null)) = source {
                    BoundScalar::enum_literal(value, &element, &enum_type)
                } else {
                    Ok(bound)
                }
            }).collect::<R<Vec<_>>>()?;
            return Ok(BoundScalar { ty, node: Node::Call(NAME.into(), args) });
        }
    }
    let args = args
        .iter()
        .map(|a| {
            if element == DataType::PgChar {
                let input = match a {
                    Scalar::Cast(input, DataType::PgChar) => input.as_ref(),
                    _ => a,
                };
                return Scalar::bind_cast(input, &element, true, resolve, tables, parameters);
            }
            if target.is_some()
                && let Some(source) = arithmetic::known(a, resolve, tables, parameters)?
            {
                // The flat array codec cannot store a nested constructor.
                if source.is_array() {
                    return if element == DataType::Uuid {
                        a.bind(resolve, tables, parameters)
                    } else {
                        Err(SqlError::Unsupported)
                    };
                }
                if element == DataType::Uuid
                    && !matches!(source, DataType::Uuid | DataType::Text | DataType::Varchar | DataType::Bpchar)
                {
                    return Err(uuid_array_operators::cannot_cast(&source, &element));
                }
            }
            Scalar::Cast(Box::new(a.clone()), element.clone()).bind(resolve, tables, parameters)
        })
        .collect::<R<Vec<_>>>()?;
    Ok(BoundScalar {
        ty,
        node: Node::Call(NAME.into(), args),
    })
}
pub(super) fn eval(args: &[BoundScalar], values: &[Value], budget: &mut usize) -> R<Value> {
    let mut parts = Vec::with_capacity(values.len());
    let mut nested = false;
    for (arg, value) in args.iter().zip(values) {
        let array = arg.ty.is_array();
        nested |= array;
        parts.push(if *value == Value::Null {
            "NULL".into()
        } else {
            let text = text_search_types::cast_typed(value, &arg.ty, &DataType::Text)
                .unwrap_or_else(|| cast_value(value.clone(), &DataType::Text, budget))?;
            let Value::Text(text) = text else {
                return Err(SqlError::Type);
            };
            let text = if array { text } else { logical_types::array_quote(&text) };
            reserve(budget, text.len())?;
            text
        });
    }
    let literal = format!("{{{}}}", parts.join(","));
    if nested {
        return Err(logical_types::malformed_array_literal(&literal));
    }
    let value = Value::Text(literal);
    charge(budget, &value)?;
    Ok(value)
}
