use super::*;

// Reuse bounded Unnest materialization and the existing correlated-query scope.
pub(super) fn compile(mut args: Vec<E>) -> R<E> {
    if !(2..=3).contains(&args.len()) {
        return Err(Error::Unsupported);
    }
    array_element(args[0].ty)?;
    expect(&mut args[1], PgType::Int4)?;
    if args.len() == 3 {
        expect(&mut args[2], PgType::Bool)?;
    }
    for arg in &args {
        no_set_or_window(arg)?;
    }
    let name = if matches!(args[0].ty, PgType::Int2Vector | PgType::OidVector) {
        "_vector_subscripts"
    } else {
        "_array_subscripts"
    };
    Ok(E {
        kind: K::Call(name.into(), args),
        ty: PgType::Int4Array,
    })
}

pub(super) fn evaluate(runtime: &mut Runtime<'_>, name: &str, args: Vec<Value>, remaining: Option<usize>) -> R<Value> {
    if args.contains(&Value::Null) || args[1] != Value::Int(1) {
        return Ok(Value::Array(vec![]));
    }
    let Value::Array(values) = &args[0] else {
        return Err(Error::Type);
    };
    if values.len() > MAX_ROWS
        || values.len().saturating_mul(Value::Int(0).bytes()) > remaining.unwrap_or(MAX_BYTES)
    {
        return Err(Error::Limit);
    }
    runtime.charge(values.len())?;
    let lower = i64::from(name != "_vector_subscripts");
    let mut result: Vec<_> = (lower..lower + values.len() as i64).map(Value::Int).collect();
    if args.get(2) == Some(&Value::Bool(true)) {
        result.reverse();
    }
    Ok(Value::Array(result))
}
