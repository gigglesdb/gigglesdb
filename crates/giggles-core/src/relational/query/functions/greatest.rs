//! GREATEST/LEAST use their own implicit common-type rules.
use super::*;

pub(super) fn known_name(name: &str) -> bool {
    matches!(name, "greatest" | "least")
}

fn numeric_rank(ty: &DataType) -> Option<u8> {
    use DataType::*;
    Some(match ty {
        SmallInt => 0,
        Integer => 1,
        BigInt => 2,
        Numeric => 3,
        Real => 4,
        DoublePrecision => 5,
        _ => return None,
    })
}

fn same_category(a: &DataType, b: &DataType) -> bool {
    use DataType::*;
    a == b
        || numeric_rank(a).is_some() && numeric_rank(b).is_some()
        || matches!(a, Date | Timestamp | TimestampTz | Time | TimeTz)
            && matches!(b, Date | Timestamp | TimestampTz | Time | TimeTz)
}

fn implicit(source: &DataType, target: &DataType) -> bool {
    use DataType::*;
    source == target
        || matches!((numeric_rank(source), numeric_rank(target)), (Some(a), Some(b)) if a < b)
        || matches!(
            (source, target),
            (Date, Timestamp | TimestampTz) | (Timestamp, TimestampTz) | (Time, TimeTz)
        )
}

fn result_type(types: &[DataType]) -> R<DataType> {
    if types.contains(&DataType::Bpchar) {
        return Err(SqlError::Unsupported);
    }
    let Some(first) = types.first() else {
        return Ok(DataType::Text);
    };
    // Category errors precede both impossible conversions and literal parsing.
    if types.iter().any(|ty| !same_category(first, ty)) {
        return Err(SqlError::Type);
    }
    let mut candidate = first.clone();
    for ty in &types[1..] {
        if implicit(&candidate, ty) && !implicit(ty, &candidate) {
            candidate = ty.clone();
        }
    }
    if types.iter().any(|ty| !implicit(ty, &candidate)) {
        return Err(SqlError::CannotCoerce);
    }
    if candidate == DataType::TimeTz && types.contains(&DataType::Time) {
        return Err(SqlError::Unsupported);
    }
    Ok(candidate)
}

pub(super) fn hint(
    args: &[Scalar],
    resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
    tables: &[&TableDef],
    parameters: &[Option<DataType>],
) -> R<(DataType, Option<std::sync::Arc<TypeDef>>)> {
    if args.is_empty() {
        return Err(SqlError::Syntax);
    }
    let bound = args
        .iter()
        .filter(|arg| !arg.aggregate_unknown(parameters))
        .map(|arg| arg.bind(resolve, tables, parameters))
        .collect::<R<Vec<_>>>()?;
    let ty = result_type(&bound.iter().map(|value| {
        if value.ty == DataType::Varchar { DataType::Text } else { value.ty.clone() }
    }).collect::<Vec<_>>())?;
    let definition = bound.iter().find_map(BoundScalar::enum_definition).cloned();
    Ok((ty, definition))
}

pub(super) fn bind(
    name: &str,
    args: &[Scalar],
    resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
    tables: &[&TableDef],
    parameters: &[Option<DataType>],
) -> R<BoundScalar> {
    if args.is_empty() {
        return Err(SqlError::Syntax);
    }
    let bound = args
        .iter()
        .map(|arg| arg.bind(resolve, tables, parameters))
        .collect::<R<Vec<_>>>()?;
    let typed = bound.iter().zip(args).filter_map(|(bound, source)| {
        (!source.aggregate_unknown(parameters)).then_some(bound)
    });
    let enum_type = enum_order::common_enum(
        if name == "greatest" { "GREATEST" } else { "LEAST" },
        typed.clone().map(BoundScalar::type_identity),
    )?.cloned();
    let ty = result_type(
        &typed.map(|bound| {
            if bound.ty == DataType::Varchar { DataType::Text } else { bound.ty.clone() }
        }).collect::<Vec<_>>(),
    )?;
    let args = args
        .iter()
        .zip(bound)
        .map(|(arg, bound)| {
            let node = match arg {
                Scalar::Literal(value @ (Value::Text(_) | Value::Null)) => {
                    if let Some(enum_type) = &enum_type {
                        return BoundScalar::enum_literal(value, &ty, enum_type);
                    }
                    literal_cast_node(value.clone(), &ty)?
                }
                _ if bound.ty == ty => return Ok(bound),
                _ => cast_node(bound, &ty),
            };
            Ok(BoundScalar { ty: ty.clone(), node })
        })
        .collect::<R<Vec<_>>>()?;
    if let Some(n) = collation::derive(args.iter())? { collation::ready(n)?; }
    Ok(BoundScalar {
        ty,
        node: Node::Call(name.into(), args),
    })
}

pub(super) fn eval(name: &str, values: Vec<Value>, input: &BoundScalar) -> R<Value> {
    let mut best = Value::Null;
    for value in values {
        if value == Value::Null {
            continue;
        }
        if best == Value::Null
            || input.try_compare_values(&value, &best)? == if name == "greatest" {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        {
            best = value;
        }
    }
    Ok(best)
}
