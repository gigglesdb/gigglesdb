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
) -> R<DataType> {
    if args.is_empty() {
        return Err(SqlError::Syntax);
    }
    let types = args
        .iter()
        .filter(|arg| !arg.aggregate_unknown(parameters))
        .map(|arg| {
            arg.bind(resolve, tables, parameters).map(|bound| {
                if bound.ty == DataType::Varchar {
                    DataType::Text
                } else {
                    bound.ty
                }
            })
        })
        .collect::<R<Vec<_>>>()?;
    result_type(&types)
}

pub(super) fn bind(
    name: &str,
    args: &[Scalar],
    resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
    tables: &[&TableDef],
    parameters: &[Option<DataType>],
) -> R<BoundScalar> {
    let ty = hint(args, resolve, tables, parameters)?;
    let enum_ty = args
        .iter()
        .map(|arg| arg.bind(resolve, tables, parameters))
        .collect::<R<Vec<_>>>()?
        .iter()
        .find_map(|arg| arg.enum_type(tables));
    let args = args
        .iter()
        .map(|arg| {
            let node = match arg {
                Scalar::Literal(Value::Text(text)) => {
                    literal_cast_node(Value::Text(text.clone()), &ty)?
                }
                Scalar::Literal(Value::Null) => Node::Literal(Value::Null),
                _ => {
                    let bound = arg.bind(resolve, tables, parameters)?;
                    if bound.ty == ty {
                        return Ok(bound);
                    }
                    cast_node(bound, &ty)
                }
            };
            Ok(BoundScalar {
                ty: ty.clone(),
                node,
            })
        })
        .collect::<R<Vec<_>>>()?;
    let args = args
        .into_iter()
        .map(|arg| {
            if let Some(ty) = &enum_ty {
                arg.with_enum(ty)
            } else {
                arg
            }
        })
        .collect();
    Ok(BoundScalar {
        ty,
        node: Node::Call(name.into(), args),
    })
}

pub(super) fn eval(name: &str, values: Vec<Value>, input: Option<&BoundScalar>) -> Value {
    let mut best = Value::Null;
    for value in values {
        if value == Value::Null {
            continue;
        }
        if best == Value::Null
            || input.map_or_else(
                || cmp(&value, &best),
                |input| input.compare_values(&value, &best),
            ) == if name == "greatest" {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        {
            best = value;
        }
    }
    best
}
