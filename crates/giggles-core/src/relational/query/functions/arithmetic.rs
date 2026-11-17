//! Closed scalar operator resolution and checked PostgreSQL arithmetic.
use super::*;

const PREFIX: &str = "\u{1}operator:";
pub(super) fn internal_name(name: &str) -> bool {
    name.starts_with(PREFIX)
}
fn name(op: BinaryOp) -> &'static str {
    match op {
        BinaryOp::Add => "\u{1}operator:+",
        BinaryOp::Sub => "\u{1}operator:-",
        BinaryOp::Mul => "\u{1}operator:*",
        BinaryOp::Div => "\u{1}operator:/",
        BinaryOp::Mod => "\u{1}operator:%",
        BinaryOp::Concat => "\u{1}operator:||",
    }
}
pub(super) fn token_operator(token: Option<&Token>) -> Option<(BinaryOp, u8)> {
    match token {
        Some(Token::Symbol('*')) => Some((BinaryOp::Mul, 3)),
        Some(Token::Op(op)) => Some(match op.as_str() {
            "+" => (BinaryOp::Add, 2),
            "-" => (BinaryOp::Sub, 2),
            "/" => (BinaryOp::Div, 3),
            "%" => (BinaryOp::Mod, 3),
            "||" => (BinaryOp::Concat, 1),
            _ => return None,
        }),
        _ => None,
    }
}
pub(super) fn admit_shape(value: &Scalar, depth: usize, nodes: &mut usize) -> R<()> {
    if depth > 32 || *nodes == MAX_WORK {
        return Err(SqlError::Limit);
    }
    *nodes += 1;
    match value {
        Scalar::Predicate(filter) => filter.admit_shape(depth + 1, nodes),
        Scalar::Case(case) => {
            for (condition, result) in &case.arms {
                condition.admit_shape(depth + 1, nodes)?;
                admit_shape(result, depth + 1, nodes)?;
            }
            case.otherwise
                .as_ref()
                .map_or(Ok(()), |result| admit_shape(result, depth + 1, nodes))
        }
        Scalar::Cast(a, _) | Scalar::Domain(a, ..) | Scalar::Unary(_, a) => {
            admit_shape(a, depth + 1, nodes)
        }
        Scalar::Binary(_, a, b) => {
            admit_shape(a, depth + 1, nodes)?;
            admit_shape(b, depth + 1, nodes)
        }
        Scalar::Call(_, args) => args
            .iter()
            .try_for_each(|a| admit_shape(a, depth + 1, nodes)),
        Scalar::SubstringOrder(args) => args
            .iter()
            .try_for_each(|a| admit_shape(a, depth + 1, nodes)),
        _ => Ok(()),
    }
}
fn known(
    a: &Scalar,
    resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
    tables: &[&TableDef],
    parameters: &[Option<DataType>],
) -> R<Option<DataType>> {
    if a.aggregate_unknown(parameters) {
        Ok(None)
    } else {
        Ok(Some(a.bind(resolve, tables, parameters)?.ty))
    }
}
// PostgreSQL's temporal overloads. Implicit conversions are deliberately closed.
fn temporal_signature(
    op: BinaryOp,
    a: Option<&DataType>,
    b: Option<&DataType>,
) -> R<Option<(DataType, DataType, DataType)>> {
    use BinaryOp::*;
    use DataType::*;
    let temporal =
        |t: &DataType| matches!(t, Date | Time | TimeTz | Timestamp | TimestampTz | Interval);
    if !a.is_some_and(temporal) && !b.is_some_and(temporal) {
        return Ok(None);
    }
    let rows = [
        (Add, Date, Integer, Date),
        (Add, Integer, Date, Date),
        (Add, Date, Time, Timestamp),
        (Add, Time, Date, Timestamp),
        (Add, Date, TimeTz, TimestampTz),
        (Add, TimeTz, Date, TimestampTz),
        (Add, Date, Interval, Timestamp),
        (Add, Interval, Date, Timestamp),
        (Add, Time, Interval, Time),
        (Add, Interval, Time, Time),
        (Add, TimeTz, Interval, TimeTz),
        (Add, Interval, TimeTz, TimeTz),
        (Add, Timestamp, Interval, Timestamp),
        (Add, Interval, Timestamp, Timestamp),
        (Add, TimestampTz, Interval, TimestampTz),
        (Add, Interval, TimestampTz, TimestampTz),
        (Add, Interval, Interval, Interval),
        (Sub, Date, Integer, Date),
        (Sub, Date, Date, Integer),
        (Sub, Date, Interval, Timestamp),
        (Sub, Time, Time, Interval),
        (Sub, Time, Interval, Time),
        (Sub, TimeTz, Interval, TimeTz),
        (Sub, Timestamp, Timestamp, Interval),
        (Sub, TimestampTz, TimestampTz, Interval),
        (Sub, Timestamp, Interval, Timestamp),
        (Sub, TimestampTz, Interval, TimestampTz),
        (Sub, Interval, Interval, Interval),
        (Mul, Interval, DoublePrecision, Interval),
        (Mul, DoublePrecision, Interval, Interval),
        (Div, Interval, DoublePrecision, Interval),
    ];
    let cost = |from: &DataType, to: &DataType| -> Option<usize> {
        if from == to {
            Some(0)
        } else if matches!(
            (from, to),
            (SmallInt, Integer)
                | (Time, Interval)
                | (Date, Timestamp)
                | (Timestamp, TimestampTz)
                | (Date, TimestampTz)
        ) || number(from) && *to == DoublePrecision
        {
            Some(1)
        } else {
            None
        }
    };
    if let (Some(known), None) | (None, Some(known)) = (a, b) {
        let left_known = a.is_some();
        let unknown = match (op, known, left_known) {
            (Add, Date | TimeTz, _) => return Err(SqlError::AmbiguousFunction),
            (Add, Time | Timestamp | TimestampTz | Interval, _) => Interval,
            (Sub, TimeTz, true) => Interval,
            (Sub, Date | Time | Timestamp | TimestampTz | Interval, _) => known.clone(),
            (Mul, Time | Interval, _) | (Div, Time | Interval, true) => DoublePrecision,
            _ => return Err(SqlError::UndefinedFunction),
        };
        return temporal_signature(op, Some(a.unwrap_or(&unknown)), Some(b.unwrap_or(&unknown)));
    }
    let mut candidates = Vec::new();
    for (o, at, bt, out) in rows {
        if o != op {
            continue;
        }
        let ac = a.map_or(Some(0), |a| cost(a, &at));
        let bc = b.map_or(Some(0), |b| cost(b, &bt));
        if let (Some(ac), Some(bc)) = (ac, bc) {
            candidates.push((ac + bc, at, bt, out));
        }
    }
    let Some(best) = candidates.iter().map(|r| r.0).min() else {
        return Err(SqlError::UndefinedFunction);
    };
    candidates.retain(|r| r.0 == best);
    // Preferred datetime type resolves subtraction's mixed timestamp candidates.
    if candidates.len() > 1 && op == Sub && a.is_some() && b.is_some() {
        candidates.retain(|(_, at, bt, _)| *at == TimestampTz && *bt == TimestampTz);
    }
    if candidates.len() != 1 {
        return Err(SqlError::AmbiguousFunction);
    }
    let (_, at, bt, out) = candidates.pop().unwrap();
    Ok(Some((at, bt, out)))
}
fn unary_type(op: UnaryOp, ty: &DataType) -> R<DataType> {
    if *ty == DataType::Bpchar {
        return Err(SqlError::Unsupported);
    }
    if number(ty) {
        Ok(ty.clone())
    } else if op == UnaryOp::Neg && matches!(ty, DataType::Time | DataType::Interval) {
        Ok(DataType::Interval)
    } else {
        Err(SqlError::UndefinedFunction)
    }
}
fn signature(
    op: BinaryOp,
    a: Option<DataType>,
    b: Option<DataType>,
) -> R<(DataType, DataType, DataType)> {
    use DataType::*;
    if a == Some(Bpchar) || b == Some(Bpchar) {
        return Err(SqlError::Unsupported);
    }
    let a = a.map(|ty| if ty == Varchar { Text } else { ty });
    let b = b.map(|ty| if ty == Varchar { Text } else { ty });
    if op == BinaryOp::Concat {
        if let Some(signature) = concat::signature(a.as_ref(), b.as_ref()) {
            return signature;
        }
        let (a, b) = match (a, b) {
            (None, None) => (Text, Text),
            (None, Some(Bytes)) | (Some(Bytes), None) => (Bytes, Bytes),
            (None, Some(b)) => (Text, b),
            (Some(a), None) => (a, Text),
            (Some(a), Some(b)) => (a, b),
        };
        if a == Bytes && b == Bytes {
            return Ok((Bytes, Bytes, Bytes));
        }
        if a == Text || b == Text {
            return Ok((a, b, Text));
        }
        return Err(SqlError::UndefinedFunction);
    }
    if let Some(signature) = temporal_signature(op, a.as_ref(), b.as_ref())? {
        return Ok(signature);
    }
    let (a, b) = match (a, b) {
        (None, None) => return Err(SqlError::AmbiguousFunction),
        (Some(a), None) => (a.clone(), a),
        (None, Some(b)) => (b.clone(), b),
        (Some(a), Some(b)) => (a, b),
    };
    if !number(&a) || !number(&b) {
        return Err(SqlError::UndefinedFunction);
    }
    let result = if matches!(a, Real | DoublePrecision) || matches!(b, Real | DoublePrecision) {
        if op == BinaryOp::Mod {
            return Err(SqlError::UndefinedFunction);
        }
        if a == Real && b == Real {
            Real
        } else {
            DoublePrecision
        }
    } else if a == Numeric || b == Numeric {
        Numeric
    } else if a == BigInt || b == BigInt {
        BigInt
    } else if a == Integer || b == Integer {
        Integer
    } else {
        SmallInt
    };
    Ok((result.clone(), result.clone(), result))
}
pub(super) fn infer(
    value: &Scalar,
    parameters: &mut [Option<DataType>],
    resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
    tables: &[&TableDef],
) -> R<()> {
    match value {
        Scalar::Unary(op, a) => {
            a.infer(None, parameters, resolve, tables)?;
            let ty = known(a, resolve, tables, parameters)?.ok_or(SqlError::AmbiguousFunction)?;
            unary_type(*op, &ty)?;
            a.infer(Some(&ty), parameters, resolve, tables)
        }
        Scalar::Binary(op, a, b) => {
            a.infer(None, parameters, resolve, tables)?;
            b.infer(None, parameters, resolve, tables)?;
            let (at, bt, _) = signature(
                *op,
                known(a, resolve, tables, parameters)?,
                known(b, resolve, tables, parameters)?,
            )?;
            // Overload coercions convert typed expressions; they only assign unknown parameters.
            if a.aggregate_unknown(parameters) {
                a.infer(Some(&at), parameters, resolve, tables)?;
            }
            if b.aggregate_unknown(parameters) {
                b.infer(Some(&bt), parameters, resolve, tables)?;
            }
            Ok(())
        }
        _ => unreachable!(),
    }
}
fn operand(
    a: &Scalar,
    target: &DataType,
    resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
    tables: &[&TableDef],
    parameters: &[Option<DataType>],
) -> R<BoundScalar> {
    if let Scalar::Literal(v @ (Value::Text(_) | Value::Null)) = a {
        return Ok(BoundScalar {
            ty: target.clone(),
            node: literal_cast_node(v.clone(), target).map_err(|error| {
                // Date's bounded parser rejects short year fields before it checks
                // missing month/day fields. Bare digits are invalid PG dates.
                if error == SqlError::Unsupported
                    && *target == DataType::Date
                    && matches!(v, Value::Text(s) if !s.trim().is_empty()
                        && s.trim().bytes().all(|b| b.is_ascii_digit()))
                {
                    SqlError::InvalidDatetimeFormat
                } else {
                    error
                }
            })?,
        });
    }
    let bound = a.bind(resolve, tables, parameters)?;
    if bound.ty == *target {
        Ok(bound)
    } else {
        Ok(BoundScalar {
            ty: target.clone(),
            node: cast_node(bound, target),
        })
    }
}
pub(super) fn bind(
    value: &Scalar,
    resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
    tables: &[&TableDef],
    parameters: &[Option<DataType>],
) -> R<BoundScalar> {
    match value {
        Scalar::Unary(op, a) => {
            let source =
                known(a, resolve, tables, parameters)?.ok_or(SqlError::AmbiguousFunction)?;
            let ty = unary_type(*op, &source)?;
            let a = operand(a, &ty, resolve, tables, parameters)?;
            Ok(BoundScalar {
                ty,
                node: Node::Call(
                    if *op == UnaryOp::Neg {
                        "\u{1}operator:neg"
                    } else {
                        "\u{1}operator:plus"
                    }
                    .into(),
                    vec![a],
                ),
            })
        }
        Scalar::Binary(op, a, b) => {
            let (at, bt, ty) = signature(
                *op,
                known(a, resolve, tables, parameters)?,
                known(b, resolve, tables, parameters)?,
            )?;
            let mut args = vec![
                operand(a, &at, resolve, tables, parameters)?,
                operand(b, &bt, resolve, tables, parameters)?,
            ];
            collation::derive(args.iter())?;
            if *op == BinaryOp::Concat {
                for argument in &mut args {
                    if session_sensitive(&argument.ty, &DataType::Text) {
                        let old = std::mem::replace(
                            argument,
                            BoundScalar {
                                ty: DataType::Text,
                                node: Node::Literal(Value::Null),
                            },
                        );
                        argument.node = Node::SessionCast(Box::new(old), None);
                    }
                }
            }
            Ok(BoundScalar {
                ty: ty.clone(),
                node: if matches!(
                    (*op, args[0].ty.clone(), args[1].ty.clone()),
                    (
                        BinaryOp::Add | BinaryOp::Sub,
                        DataType::TimestampTz,
                        DataType::Interval
                    ) | (BinaryOp::Add, DataType::Interval, DataType::TimestampTz)
                ) {
                    Node::ZonedOperator(name(*op).into(), args, None)
                } else {
                    Node::Call(
                        if *op == BinaryOp::Concat { concat::name(&at, &bt, &ty).unwrap_or(name(*op)) } else { name(*op) }.into(),
                        args,
                    )
                },
            })
        }
        _ => unreachable!(),
    }
}
fn checked_integer(value: i64, ty: &DataType) -> R<Value> {
    if (*ty == DataType::SmallInt && i16::try_from(value).is_err())
        || (*ty == DataType::Integer && i32::try_from(value).is_err())
    {
        return Err(SqlError::Overflow);
    }
    Ok(Value::Int(value))
}
pub(super) fn eval(name: &str, ty: &DataType, values: &[Value], budget: &mut usize) -> R<Value> {
    if let Some(result) = concat::eval(name, values, budget) {
        return result;
    }
    if values.contains(&Value::Null) {
        return Ok(Value::Null);
    }
    let op = name
        .strip_prefix(PREFIX)
        .ok_or(SqlError::UndefinedFunction)?;
    if op == "plus" {
        if let Value::Numeric(a) = &values[0] {
            reserve(budget, a.display_byte_bound())?;
        } else {
            charge(budget, &values[0])?;
        }
        return Ok(values[0].clone());
    }
    if op == "neg" {
        if let Value::Numeric(a) = &values[0] {
            reserve(budget, a.display_byte_bound())?;
        } else {
            charge(budget, &values[0])?;
        }
        return match &values[0] {
            Value::Interval(a) => a
                .checked_neg()
                .map(Value::Interval)
                .map_err(temporal::function_error),
            Value::Int(a) => checked_integer(a.checked_neg().ok_or(SqlError::Overflow)?, ty),
            Value::Numeric(a) => Numeric::parse("0")
                .map_err(pg_error)?
                .checked_sub(a)
                .map(Value::Numeric)
                .map_err(pg_error),
            Value::Float4(a) => Ok(Value::Float4(Float4::from_bits((-a.value()).to_bits()))),
            Value::Float8(a) => Ok(Value::Float8(Float8::from_bits((-a.value()).to_bits()))),
            _ => Err(SqlError::UndefinedFunction),
        };
    }
    if op == "||" {
        if *ty == DataType::Text {
            use std::borrow::Cow;
            fn output<'a>(value: &'a Value, budget: &mut usize) -> R<Cow<'a, str>> {
                if let Value::Text(text) = value {
                    return Ok(Cow::Borrowed(text));
                }
                let Value::Text(text) = cast_value(value.clone(), &DataType::Text, budget)? else {
                    return Err(SqlError::UndefinedFunction);
                };
                Ok(Cow::Owned(text))
            }
            let a = output(&values[0], budget)?;
            let b = output(&values[1], budget)?;
            reserve(budget, a.len().checked_add(b.len()).ok_or(SqlError::Limit)?)?;
            let mut out = String::with_capacity(a.len() + b.len());
            out.push_str(&a);
            out.push_str(&b);
            return Ok(Value::Text(out));
        }
        if let (Value::Bytes(a), Value::Bytes(b)) = (&values[0], &values[1]) {
            reserve(budget, a.len().checked_add(b.len()).ok_or(SqlError::Limit)?)?;
            let mut out = Vec::with_capacity(a.len() + b.len());
            out.extend_from_slice(a);
            out.extend_from_slice(b);
            return Ok(Value::Bytes(out));
        }
        return Err(SqlError::UndefinedFunction);
    }
    if let Some(value) = eval_temporal(op, values)? {
        charge(budget, &value)?;
        return Ok(value);
    }
    match (&values[0], &values[1]) {
        (Value::Int(a), Value::Int(b)) => {
            reserve(budget, 8)?;
            if matches!(op, "/" | "%") && *b == 0 {
                return Err(SqlError::DivisionByZero);
            }
            let out = match op {
                "+" => a.checked_add(*b),
                "-" => a.checked_sub(*b),
                "*" => a.checked_mul(*b),
                "/" => a.checked_div(*b),
                "%" if *b == -1 => Some(0),
                "%" => a.checked_rem(*b),
                _ => None,
            }
            .ok_or(SqlError::Overflow)?;
            checked_integer(out, ty)
        }
        (Value::Numeric(a), Value::Numeric(b)) => {
            let out = match op {
                "+" => a.checked_add(b),
                "-" => a.checked_sub(b),
                "*" => a.checked_mul(b),
                "/" => a.checked_div(b),
                "%" => a.checked_rem_pg(b),
                _ => return Err(SqlError::UndefinedFunction),
            }
            .map_err(pg_error)?;
            reserve(budget, out.display_byte_bound())?;
            Ok(Value::Numeric(out))
        }
        (Value::Float4(a), Value::Float4(b)) => {
            reserve(budget, 4)?;
            let (a, b) = (a.value(), b.value());
            if op == "/" && b == 0.0 && !a.is_nan() {
                return Err(SqlError::DivisionByZero);
            }
            let out = match op {
                "+" => a + b,
                "-" => a - b,
                "*" => a * b,
                "/" => a / b,
                _ => return Err(SqlError::UndefinedFunction),
            };
            float_check(op, a as f64, b as f64, out as f64)?;
            Ok(Value::Float4(Float4::from_bits(out.to_bits())))
        }
        (Value::Float8(a), Value::Float8(b)) => {
            reserve(budget, 8)?;
            let (a, b) = (a.value(), b.value());
            if op == "/" && b == 0.0 && !a.is_nan() {
                return Err(SqlError::DivisionByZero);
            }
            let out = match op {
                "+" => a + b,
                "-" => a - b,
                "*" => a * b,
                "/" => a / b,
                _ => return Err(SqlError::UndefinedFunction),
            };
            float_check(op, a, b, out)?;
            Ok(Value::Float8(Float8::from_bits(out.to_bits())))
        }
        _ => Err(SqlError::UndefinedFunction),
    }
}
fn float_check(op: &str, a: f64, b: f64, result: f64) -> R<()> {
    if result.is_infinite() && a.is_finite() && b.is_finite()
        || matches!(op, "*" | "/") && result == 0.0 && a != 0.0 && b != 0.0 && b.is_finite()
    {
        return Err(SqlError::Overflow);
    }
    Ok(())
}

fn eval_temporal(op: &str, values: &[Value]) -> R<Option<Value>> {
    use Value::*;
    let out = match (op, &values[0], &values[1]) {
        ("+", Date(a), Int(b)) | ("+", Int(b), Date(a)) => {
            Date(a.add_days(*b).map_err(temporal::function_error)?)
        }
        ("-", Date(a), Int(b)) => Date(a.sub_days(*b).map_err(temporal::function_error)?),
        ("-", Date(a), Date(b)) => Int(i64::from(a.diff(*b).map_err(temporal::function_error)?)),
        ("+", Date(a), Time(b)) | ("+", Time(b), Date(a)) => {
            Timestamp(a.add_time(*b).map_err(temporal::function_error)?)
        }
        ("+", Date(a), TimeTz(b)) | ("+", TimeTz(b), Date(a)) => {
            TimestampTz(a.add_timetz(*b).map_err(temporal::function_error)?)
        }
        ("+", Date(a), Interval(b)) | ("+", Interval(b), Date(a)) => {
            Timestamp(a.add_interval(*b).map_err(temporal::function_error)?)
        }
        ("-", Date(a), Interval(b)) => {
            Timestamp(a.sub_interval(*b).map_err(temporal::function_error)?)
        }
        ("+", Timestamp(a), Interval(b)) | ("+", Interval(b), Timestamp(a)) => {
            Timestamp(a.add_interval(*b).map_err(temporal::function_error)?)
        }
        ("-", Timestamp(a), Interval(b)) => {
            Timestamp(a.sub_interval(*b).map_err(temporal::function_error)?)
        }
        ("-", Timestamp(a), Timestamp(b)) => {
            Interval(a.diff(*b).map_err(temporal::function_error)?)
        }
        ("-", TimestampTz(a), TimestampTz(b)) => {
            Interval(a.diff(*b).map_err(temporal::function_error)?)
        }
        ("+", Time(a), Interval(b)) | ("+", Interval(b), Time(a)) => {
            Time(a.add_interval(*b).map_err(temporal::function_error)?)
        }
        ("-", Time(a), Interval(b)) => Time(a.sub_interval(*b).map_err(temporal::function_error)?),
        ("-", Time(a), Time(b)) => Interval(a.diff(*b)),
        ("+", TimeTz(a), Interval(b)) | ("+", Interval(b), TimeTz(a)) => {
            TimeTz(a.add_interval(*b).map_err(temporal::function_error)?)
        }
        ("-", TimeTz(a), Interval(b)) => {
            TimeTz(a.sub_interval(*b).map_err(temporal::function_error)?)
        }
        ("+", Interval(a), Interval(b)) => {
            Interval(a.checked_add(*b).map_err(temporal::function_error)?)
        }
        ("-", Interval(a), Interval(b)) => {
            Interval(a.checked_sub(*b).map_err(temporal::function_error)?)
        }
        ("*", Interval(a), Float8(b)) | ("*", Float8(b), Interval(a)) => {
            Interval(a.mul_f64(b.value()).map_err(temporal::function_error)?)
        }
        ("/", Interval(a), Float8(b)) => {
            Interval(a.div_f64(b.value()).map_err(temporal::function_error)?)
        }
        _ => return Ok(None),
    };
    Ok(Some(out))
}
pub(super) fn eval_zoned(
    name: &str,
    values: &[Value],
    clock: Option<&crate::relational::clock::ExecutionClock>,
    budget: &mut usize,
) -> R<Value> {
    if values.contains(&Value::Null) {
        return Ok(Value::Null);
    }
    use giggles_pg_types::temporal::SessionZone;
    let zone = clock
        .and_then(|c| c.zone())
        .unwrap_or(SessionZone::Fixed(0));
    let op = name
        .strip_prefix(PREFIX)
        .ok_or(SqlError::UndefinedFunction)?;
    let value = match (op, &values[0], &values[1]) {
        ("+", Value::TimestampTz(a), Value::Interval(b))
        | ("+", Value::Interval(b), Value::TimestampTz(a)) => a.add_interval(*b, zone),
        ("-", Value::TimestampTz(a), Value::Interval(b)) => a.sub_interval(*b, zone),
        _ => return Err(SqlError::UndefinedFunction),
    }
    .map(Value::TimestampTz)
    .map_err(temporal::function_error)?;
    charge(budget, &value)?;
    Ok(value)
}
