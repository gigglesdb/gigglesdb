//! Aggregate-local ORDER BY keeps its keys with each deferred input.
//! Collation conflicts are rejected while binding, including empty inputs.
use super::*;
pub(super) const PREFIX: &str = "\0aggregate-order:";
pub(super) fn internal(name: &str) -> bool {
    name.starts_with(PREFIX)
}
// Keep the owned Scalar temporary out of primary_context's recursive frame.
pub(super) fn parse_args(p: &mut Parser, args: &mut Vec<Scalar>, depth: usize) -> R<()> {
    if !matches!(p.tokens.get(p.at), Some(Token::Word(w)) if w == "order") {
        return Ok(());
    }
    let input = args.pop().ok_or(SqlError::Syntax)?;
    args.push(parse(p, input, depth)?);
    Ok(())
}
pub(super) fn parse(p: &mut Parser, input: Scalar, depth: usize) -> R<Scalar> {
    if !p.word("order") {
        return Ok(input);
    }
    parse_order(p, vec![input], depth)
}
pub(super) fn parse_object(p: &mut Parser, inputs: Vec<Scalar>, depth: usize) -> R<Scalar> {
    if p.word("order") {
        parse_order(p, inputs, depth)
    } else {
        Ok(Scalar::Call(PREFIX.into(), inputs))
    }
}
fn parse_order(p: &mut Parser, mut args: Vec<Scalar>, depth: usize) -> R<Scalar> {
    p.need("by")?;
    let mut name = PREFIX.to_owned();
    loop {
        if args.len() >= MAX_COLUMNS {
            return Err(SqlError::Limit);
        }
        args.push(Scalar::parse_expr(p, depth + 1, true)?);
        let descending = p.word("desc");
        if !descending {
            p.word("asc");
        }
        let first = if p.word("nulls") {
            if p.word("first") {
                true
            } else {
                p.need("last")?;
                false
            }
        } else {
            descending
        };
        name.push(if descending { 'd' } else { 'a' });
        name.push(if first { 'f' } else { 'l' });
        if !p.sym(',') {
            break;
        }
    }
    Ok(Scalar::Call(name.into(), args))
}
pub(super) fn parts(input: &BoundScalar) -> Option<(&str, &[BoundScalar])> {
    input.aggregate_order_parts()
}
pub(super) fn check_types(input: &BoundScalar) -> R<()> {
    if let Some((name, args)) = parts(input)
        && args[input_width(name, args.len())..].iter().any(|key| key.ty == DataType::Bpchar)
    {
        return Err(SqlError::Unsupported);
    }
    Ok(())
}
pub(super) fn validate_distinct(
    input: &Scalar,
    resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
    tables: &[&TableDef],
) -> R<()> {
    if let Scalar::Call(name, args) = input && internal(name) {
        let width = input_width(name, args.len());
        let inputs = args[..width].iter().map(|s| grouping::canonical(s, resolve, tables.len(), tables))
            .collect::<R<Vec<_>>>()?;
        for key in &args[width..] {
            if !inputs.contains(&grouping::canonical(key, resolve, tables.len(), tables)?) {
                user_types::view_replace_error(
                    "in an aggregate with DISTINCT, ORDER BY expressions must appear in argument list".into(),
                );
                return Err(SqlError::InvalidColumnReference);
            }
        }
    }
    Ok(())
}
pub(super) fn input_width(name: &str, args: usize) -> usize {
    args - (name.len() - PREFIX.len()) / 2
}
pub(super) fn same_inputs(args: &[BoundScalar], a: &[Value], b: &[Value], kind: Aggregate) -> R<bool> {
    for ((arg, a), b) in args.iter().zip(a).zip(b) {
        let order = if kind.base() == Aggregate::ArrayAgg {
            array_aggregate::compare_distinct(Some(arg), a, b)?
        } else {
            arg.try_compare_values(a, b)?
        };
        if order != Ordering::Equal {
            return Ok(false);
        }
    }
    Ok(true)
}
pub(super) fn compare(name: &str, args: &[BoundScalar], a: &[Value], b: &[Value], kind: Aggregate) -> R<Ordering> {
    let width = input_width(name, args.len());
    for (((flags, scalar), a), b) in name.as_bytes()[PREFIX.len()..]
        .as_chunks::<2>().0.iter()
        .zip(&args[width..])
        .zip(&a[width..])
        .zip(&b[width..])
    {
        let first = flags[1] == b'f';
        let order = match (a, b) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => {
                if first {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            (_, Value::Null) => {
                if first {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            _ => {
                let order = if kind.base() == Aggregate::ArrayAgg && scalar.ty == DataType::Jsonb {
                    array_aggregate::compare_jsonb(a, b)?
                } else {
                    scalar.try_compare_values(a, b)?
                };
                if flags[0] == b'd' {
                    order.reverse()
                } else {
                    order
                }
            }
        };
        if order != Ordering::Equal {
            return Ok(order);
        }
    }
    // PostgreSQL adds DISTINCT inputs that aren't ORDER BY keys as ascending
    // tiebreakers. Use the already evaluated argument values.
    if kind.distinct() && width > 1 {
        for ((arg, a), b) in args[..width].iter().zip(a).zip(b) {
            let order = json_aggregate::compare_distinct(Some(arg), a, b)?;
            if order != Ordering::Equal {
                return Ok(order);
            }
        }
    }
    Ok(Ordering::Equal)
}

/// Parse and assign collations before reporting unsupported aggregate execution.
pub(super) fn deferred(name: &str) -> Option<bool> {
    let distinct = name.starts_with("\0aggregate_distinct_");
    let name = name.strip_prefix("\0aggregate_distinct_").unwrap_or(name);
    matches!(name, "array_agg" | "string_agg").then_some(distinct)
}
