//! Pure projection of a wire advisory call's arguments into its lock key.
use super::*;
use crate::relational::query::advisory::KEY_PREFIX;

type Signature = (Vec<Option<DataType>>, DataType);

fn input_type(name: &str, count: usize) -> Option<Option<DataType>> {
    match (name == "pg_advisory_unlock_all", count) {
        (true, 0) => Some(None),
        (false, 1) => Some(Some(DataType::BigInt)),
        (false, 2) => Some(Some(DataType::Integer)),
        _ => None,
    }
}

pub(super) fn signature(name: &str, count: usize) -> Option<R<Signature>> {
    let name = name.strip_prefix(KEY_PREFIX)?;
    input_type(name, count).map(|ty| Ok((vec![ty; count], DataType::BigInt)))
}

pub(super) fn bind(name: &str, source: &[Scalar], mut args: Vec<BoundScalar>) -> R<BoundScalar> {
    let routine = name
        .strip_prefix(KEY_PREFIX)
        .expect("advisory key expression");
    let undefined = |args: &[BoundScalar]| {
        let types: Vec<_> = source
            .iter()
            .zip(args)
            .map(|(source, arg)| {
                if matches!(source, Scalar::Literal(Value::Text(_) | Value::Null)) {
                    "unknown"
                } else {
                    typeof_expr::name(&arg.ty)
                }
            })
            .collect();
        let mut diagnostic = RoutineDiagnostic::new(
            "42883",
            format!("function {routine}({}) does not exist", types.join(", ")),
        );
        diagnostic.hint = Some("No function matches the given name and argument types. You might need to add explicit type casts.".into());
        diagnostic.error()
    };
    let target = input_type(routine, args.len()).ok_or_else(|| undefined(&args))?;
    if let Some(target) = target {
        if source.iter().zip(&args).any(|(source, arg)| {
            let accepts_type = match arg.ty {
                DataType::SmallInt | DataType::Integer => true,
                DataType::BigInt => target == DataType::BigInt,
                _ => false,
            };
            !accepts_type && !matches!(source, Scalar::Literal(Value::Text(_) | Value::Null))
        }) {
            return Err(undefined(&args));
        }
        for (source, arg) in source.iter().zip(&mut args) {
            if let Scalar::Literal(value @ (Value::Text(_) | Value::Null)) = source {
                *arg = BoundScalar {
                    ty: target.clone(),
                    node: literal_cast_node(value.clone(), &target)?,
                };
            }
        }
    }
    Ok(BoundScalar {
        ty: DataType::BigInt,
        node: Node::Call(name.into(), args),
    })
}

pub(super) fn eval(values: &[Value]) -> R<Value> {
    let key = match values {
        [] => 0,
        [Value::Int(key)] => *key,
        [Value::Int(first), Value::Int(second)] => (*first << 32) | i64::from(*second as u32),
        _ => return Err(SqlError::InternalError),
    };
    Ok(Value::Int(key))
}
