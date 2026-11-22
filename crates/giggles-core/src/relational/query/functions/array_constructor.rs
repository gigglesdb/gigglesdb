//! ARRAY constructors use the existing text-backed array codec and scalar casts.
use super::*;
pub(super) const NAME: &str = "\0array_constructor";

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
    target: Option<&DataType>,
    resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
    tables: &[&TableDef],
    parameters: &[Option<DataType>],
) -> R<BoundScalar> {
    let element = if let Some(ty) = target.and_then(element_type) {
        ty
    } else {
        hint(args, resolve, tables, parameters)?
    };
    let ty = match element {
        DataType::Integer => DataType::IntArray,
        DataType::BigInt => DataType::BigIntArray,
        DataType::Text => DataType::TextArray,
        _ => return Err(SqlError::Unsupported),
    };
    let args = args
        .iter()
        .map(|a| {
            if target.is_some()
                && arithmetic::known(a, resolve, tables, parameters)?
                    .as_ref().and_then(element_type).is_some()
            {
                // The flat array codec cannot store a nested constructor.
                return Err(SqlError::Unsupported);
            }
            Scalar::Cast(Box::new(a.clone()), element.clone()).bind(resolve, tables, parameters)
        })
        .collect::<R<Vec<_>>>()?;
    Ok(BoundScalar {
        ty,
        node: Node::Call(NAME.into(), args),
    })
}
pub(super) fn eval(values: &[Value], budget: &mut usize) -> R<Value> {
    let mut parts = Vec::with_capacity(values.len());
    for value in values {
        parts.push(if *value == Value::Null {
            "NULL".into()
        } else {
            let Value::Text(text) = cast_value(value.clone(), &DataType::Text, budget)? else {
                return Err(SqlError::Type);
            };
            let text = crate::relational::logical_types::array_quote(&text);
            reserve(budget, text.len())?;
            text
        });
    }
    let value = Value::Text(format!("{{{}}}", parts.join(",")));
    charge(budget, &value)?;
    Ok(value)
}
