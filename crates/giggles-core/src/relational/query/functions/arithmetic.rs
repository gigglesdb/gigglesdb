//! Closed scalar operator resolution and checked PostgreSQL arithmetic.
use super::*;

const PREFIX: &str = "\u{1}operator:";
const UNKNOWN_PREFIX: &str = "\0unknown_operator:";

pub(super) fn unknown_operator(op: &str) -> String {
    format!("{UNKNOWN_PREFIX}{op}")
}

pub(super) fn is_unknown_operator(name: &str) -> bool {
    name.starts_with(UNKNOWN_PREFIX)
}

pub(super) fn bind_unknown_operator(
    name: &str,
    args: &[Scalar],
    resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
    tables: &[&TableDef],
    parameters: &[Option<DataType>],
) -> R<BoundScalar> {
    let op = name.strip_prefix(UNKNOWN_PREFIX).ok_or(SqlError::InternalError)?;
    let ty = |arg| known(arg, resolve, tables, parameters);
    let (left, right) = match args {
        [right] => (None, ty(right)?),
        [left, right] => (Some(ty(left)?), ty(right)?),
        _ => return Err(SqlError::InternalError),
    };
    Err(operator_error(op, left.as_ref().map(Option::as_ref), right.as_ref()))
}
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
        BinaryOp::Pow => "\u{1}operator:^",
        BinaryOp::BitAnd => "\u{1}operator:&",
        BinaryOp::BitOr => "\u{1}operator:|",
        BinaryOp::BitXor => "\u{1}operator:#",
        BinaryOp::ShiftLeft => "\u{1}operator:<<",
        BinaryOp::ShiftRight => "\u{1}operator:>>",
        BinaryOp::Like => "\u{1}operator:~~",
        BinaryOp::NotLike => "\u{1}operator:!~~",
        BinaryOp::Ilike => "\u{1}operator:~~*",
        BinaryOp::NotIlike => "\u{1}operator:!~~*",
        BinaryOp::StartsWith => "\u{1}operator:^@",
        BinaryOp::InfixCbrt => "\u{1}operator:||/",
        BinaryOp::JsonPath => "\u{1}operator:#>",
        BinaryOp::JsonPathText => "\u{1}operator:#>>",
        BinaryOp::Contains => "\u{1}operator:@>",
        BinaryOp::ContainedBy => "\u{1}operator:<@",
        BinaryOp::Overlap => "\u{1}operator:&&",
        BinaryOp::Regex => "\u{1}operator:~",
        BinaryOp::NotRegex => "\u{1}operator:!~",
        BinaryOp::RegexInsensitive => "\u{1}operator:~*",
        BinaryOp::NotRegexInsensitive => "\u{1}operator:!~*",
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
            "^" => (BinaryOp::Pow, 4),
            "&" => (BinaryOp::BitAnd, 1),
            "|" => (BinaryOp::BitOr, 1),
            "#" => (BinaryOp::BitXor, 1),
            "<<" => (BinaryOp::ShiftLeft, 1),
            ">>" => (BinaryOp::ShiftRight, 1),
            "~~" => (BinaryOp::Like, 1),
            "!~~" => (BinaryOp::NotLike, 1),
            "~~*" => (BinaryOp::Ilike, 1),
            "!~~*" => (BinaryOp::NotIlike, 1),
            "^@" => (BinaryOp::StartsWith, 1),
            "||/" => (BinaryOp::InfixCbrt, 1),
            "#>" => (BinaryOp::JsonPath, 1),
            "#>>" => (BinaryOp::JsonPathText, 1),
            "@>" => (BinaryOp::Contains, 1),
            "<@" => (BinaryOp::ContainedBy, 1),
            "&&" => (BinaryOp::Overlap, 1),
            "~" => (BinaryOp::Regex, 1),
            "!~" => (BinaryOp::NotRegex, 1),
            "~*" => (BinaryOp::RegexInsensitive, 1),
            "!~*" => (BinaryOp::NotRegexInsensitive, 1),
            _ => return None,
        }),
        _ => None,
    }
}
pub(super) fn admit_shape(value: &Scalar, depth: usize, nodes: &mut usize) -> R<()> {
    if depth > 32 || *nodes == max_statement_tokens() {
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
pub(super) fn known(
    a: &Scalar,
    resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
    tables: &[&TableDef],
    parameters: &[Option<DataType>],
) -> R<Option<DataType>> {
    Ok(known_operand(a, resolve, tables, parameters)?.map(|operand| operand.ty))
}

fn known_operand(
    a: &Scalar,
    resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
    tables: &[&TableDef],
    parameters: &[Option<DataType>],
) -> R<Option<BoundScalar>> {
    if a.aggregate_unknown(parameters) {
        Ok(None)
    } else {
        a.bind(resolve, tables, parameters).map(Some)
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
    if matches!(op, UnaryOp::Sqrt | UnaryOp::Cbrt) && number(ty) {
        return Ok(DataType::DoublePrecision);
    }
    if op == UnaryOp::BitNot && !integer(ty) {
        return Err(SqlError::UndefinedFunction);
    }
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
    use BinaryOp::*;
    use DataType::*;
    if matches!(op, Regex | NotRegex | RegexInsensitive | NotRegexInsensitive) {
        if [a.as_ref(), b.as_ref()].into_iter().flatten().all(regexp::text_type) {
            // PostgreSQL has a character operator whose source retains padding.
            let source = if a == Some(Bpchar) { Bpchar } else { Text };
            return Ok((source, Text, Boolean));
        }
        return Err(undefined_operator(op, [a.as_ref(), b.as_ref()]));
    }
    if a == Some(Bpchar) || b == Some(Bpchar) {
        return Err(SqlError::Unsupported);
    }
    if let Some(array) = [a.as_ref(), b.as_ref()].into_iter().flatten().find(|ty| array_constructor::query_only(ty)) {
        if op == Concat {
            return concat::signature(a.as_ref(), b.as_ref()).expect("known array overload");
        }
        if matches!(op, Contains | ContainedBy | Overlap)
            && *array != DataType::ExpressionArray(crate::relational::ExpressionArray::Json)
            && [a.as_ref(), b.as_ref()].into_iter().flatten().all(|ty| ty == array)
        {
            return Err(SqlError::Unsupported);
        }
    }
    let a = a.map(|ty| if ty == Varchar { Text } else { ty });
    let b = b.map(|ty| if ty == Varchar { Text } else { ty });
    let types = [a.as_ref(), b.as_ref()];
    if types.iter().flatten().any(|ty| ty.is_text_search()) {
        match op {
            BinaryOp::Concat if types.into_iter().flatten().all(|ty| ty == &TsVector) =>
                return Ok((TsVector, TsVector, TsVector)),
            BinaryOp::Concat | BinaryOp::Contains | BinaryOp::ContainedBy | BinaryOp::Overlap
                if types.into_iter().flatten().all(|ty| ty == &TsQuery) =>
            {
                let result = if matches!(op, BinaryOp::Concat | BinaryOp::Overlap) { TsQuery } else { Boolean };
                return Ok((TsQuery, TsQuery, result));
            }
            BinaryOp::Concat if types.contains(&Some(&Text)) => {}
            _ => return Err(undefined_operator(op, types)),
        }
    }
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
    if matches!(op, JsonPath | JsonPathText) {
        return if matches!(a, None | Some(Jsonb)) && matches!(b, None | Some(TextArray)) {
            Ok((Jsonb, TextArray, if op == JsonPath { Jsonb } else { Text }))
        } else {
            Err(undefined_operator(op, [a.as_ref(), b.as_ref()]))
        };
    }
    if matches!(op, Contains | ContainedBy | Overlap) {
        let types = [a.as_ref(), b.as_ref()];
        if op != Overlap && types.contains(&Some(&Jsonb)) {
            return if types.into_iter().flatten().all(|ty| *ty == Jsonb) {
                Ok((Jsonb, Jsonb, Boolean))
            } else {
                Err(undefined_operator(op, types))
            };
        }
        if types.contains(&Some(&UuidArray)) {
            return if types.into_iter().flatten().all(|ty| *ty == UuidArray) {
                Ok((UuidArray, UuidArray, Boolean))
            } else {
                Err(undefined_operator(op, types))
            };
        }
        return if op == Overlap {
            Err(undefined_operator(op, types))
        } else {
            Err(SqlError::Unsupported)
        };
    }
    if matches!(op, Like | NotLike | Ilike | NotIlike | StartsWith) {
        return if [a, b].iter().flatten().all(|ty| *ty == Text) {
            Ok((Text, Text, Boolean))
        } else {
            Err(SqlError::UndefinedFunction)
        };
    }
    if op == Pow {
        if ![&a, &b].into_iter().flatten().all(number) {
            return Err(SqlError::UndefinedFunction);
        }
        // PG18: numeric ^ numeric when no operand is real/double; numeric with a float
        // operand resolves to float8 ^ float8 (oracle: 1::numeric ^ 2::real is double precision).
        let float = [&a, &b].into_iter().flatten().any(|ty| matches!(ty, Real | DoublePrecision));
        let ty = if !float && (a == Some(Numeric) || b == Some(Numeric)) { Numeric } else { DoublePrecision };
        return Ok((ty.clone(), ty.clone(), ty));
    }
    if op == InfixCbrt {
        return Err(undefined_operator(op, [a.as_ref(), b.as_ref()]));
    }
    if matches!(op, BitAnd | BitOr | BitXor | ShiftLeft | ShiftRight) {
        let spelling = name(op).strip_prefix(PREFIX).unwrap();
        let shift = matches!(op, ShiftLeft | ShiftRight);
        let types = [a.as_ref(), b.as_ref()];
        if a.is_none() && b.is_none() {
            user_types::view_replace_error(format!("operator is not unique: unknown {spelling} unknown"));
            user_types::sql_error_hint("Could not choose a best candidate operator. You might need to add explicit type casts.");
            return Err(SqlError::AmbiguousFunction);
        }
        if !types.into_iter().flatten().all(integer) || shift && b == Some(BigInt) {
            return Err(undefined_operator(op, types));
        }
        let left = a.clone().unwrap_or_else(|| if shift { Integer } else { b.clone().unwrap() });
        let right = if shift { Integer } else { b.unwrap_or_else(|| left.clone()) };
        let result = if shift { left.clone() } else { common_result_type([left.clone(), right.clone()])? };
        return Ok((left, right, result));
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
fn undefined_operator(op: BinaryOp, types: [Option<&DataType>; 2]) -> SqlError {
    let spelling = name(op).strip_prefix(PREFIX).unwrap();
    operator_error(spelling, Some(types[0]), types[1])
}

pub(super) fn operator_error(spelling: &str, left: Option<Option<&DataType>>, right: Option<&DataType>) -> SqlError {
    let label = |ty: Option<&DataType>| ty.map_or("unknown", typeof_expr::name);
    let right = label(right);
    let signature = left.map_or_else(
        || format!("{spelling} {right}"),
        |left| format!("{} {spelling} {right}", label(left)),
    );
    user_types::view_replace_error(format!("operator does not exist: {signature}"));
    user_types::sql_error_hint("No operator matches the given name and argument types. You might need to add explicit type casts.");
    SqlError::UndefinedFunction
}

fn unary_source(op: UnaryOp, ty: Option<DataType>) -> R<DataType> {
    ty.or(match op {
        UnaryOp::Abs | UnaryOp::Sqrt | UnaryOp::Cbrt => Some(DataType::DoublePrecision),
        _ => None,
    })
    .ok_or(SqlError::AmbiguousFunction)
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
            let source = known(a, resolve, tables, parameters)?;
            let ty = unary_type(*op, &unary_source(*op, source.clone())?)?;
            if source.is_none() {
                a.infer(Some(&ty), parameters, resolve, tables)?;
            }
            Ok(())
        }
        Scalar::Binary(op, a, b) => {
            a.infer(None, parameters, resolve, tables)?;
            b.infer(None, parameters, resolve, tables)?;
            let operands = [
                known_operand(a, resolve, tables, parameters)?,
                known_operand(b, resolve, tables, parameters)?,
            ];
            if let Some(definition) = enum_containment_type(*op, &operands) {
                for (scalar, operand) in [a.as_ref(), b.as_ref()].into_iter().zip(&operands) {
                    if matches!(scalar, Scalar::Param(_)) && operand.is_none() {
                        return scalar.infer_enum_parameter(
                            &DataType::TextArray,
                            Some(&definition),
                            parameters,
                        );
                    }
                }
                return bind_enum_containment(*op, [a, b], operands, &definition).map(|_| ());
            }
            if *op == BinaryOp::Concat {
                concat::check_json_operand(&operands)?;
            }
            let [left, right] = operands.map(|operand| operand.map(|operand| operand.ty));
            let (at, bt, _) = signature(*op, left, right)?;
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
pub(super) fn operand(
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
fn enum_containment_type(
    op: BinaryOp,
    operands: &[Option<BoundScalar>; 2],
) -> Option<std::sync::Arc<TypeDef>> {
    if !matches!(op, BinaryOp::Contains | BinaryOp::ContainedBy | BinaryOp::Overlap) {
        return None;
    }
    operands
        .iter()
        .flatten()
        .filter(|operand| operand.ty == DataType::TextArray)
        .find_map(BoundScalar::enum_definition)
        .cloned()
}

fn bind_enum_containment(
    op: BinaryOp,
    [a, b]: [&Scalar; 2],
    [left, right]: [Option<BoundScalar>; 2],
    definition: &TypeDef,
) -> R<BoundScalar> {
    let bind = |scalar: &Scalar, operand: Option<BoundScalar>| match scalar {
        Scalar::Literal(value @ (Value::Null | Value::Text(_))) => {
            BoundScalar::enum_literal(value, &DataType::TextArray, definition)
        }
        _ => operand.ok_or(SqlError::Parameters),
    };
    let left = bind(a, left)?;
    let right = bind(b, right)?;
    if left.type_identity() != right.type_identity() {
        return Err(enum_order::missing_operator(name(op).strip_prefix(PREFIX).unwrap(), &left, &right));
    }
    Ok(BoundScalar { ty: DataType::Boolean, node: Node::Call(name(op).into(), vec![left, right]) })
}

pub(super) fn bind(
    value: &Scalar,
    resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
    tables: &[&TableDef],
    parameters: &[Option<DataType>],
) -> R<BoundScalar> {
    match value {
        Scalar::Unary(op, a) => {
            let source = unary_source(*op, known(a, resolve, tables, parameters)?)?;
            let ty = unary_type(*op, &source)?;
            let a = operand(a, &ty, resolve, tables, parameters)?;
            Ok(BoundScalar {
                ty,
                node: Node::Call(
                    match op {
                        UnaryOp::Neg => "\u{1}operator:neg",
                        UnaryOp::Plus => "\u{1}operator:plus",
                        UnaryOp::Abs => "\u{1}operator:@",
                        UnaryOp::Sqrt => "\u{1}operator:|/",
                        UnaryOp::Cbrt => "\u{1}operator:||/",
                        UnaryOp::BitNot => "\u{1}operator:~",
                    }.into(),
                    vec![a],
                ),
            })
        }
        Scalar::Binary(op, a, b) => {
            let operands = [
                known_operand(a, resolve, tables, parameters)?,
                known_operand(b, resolve, tables, parameters)?,
            ];
            if let Some(definition) = enum_containment_type(*op, &operands) {
                return bind_enum_containment(*op, [a, b], operands, &definition);
            }
            if *op == BinaryOp::Concat {
                concat::check_json_operand(&operands)?;
            }
            let [left, right] = operands.map(|operand| operand.map(|operand| operand.ty));
            let (at, bt, ty) = signature(*op, left, right)?;
            if *op == BinaryOp::Concat {
                for (source, target) in [(a.as_ref(), &at), (b.as_ref(), &bt)] {
                    if let Scalar::Literal(value) = source {
                        concat::check_literal(value, target)?;
                    }
                }
            }
            let mut args = vec![
                operand(a, &at, resolve, tables, parameters)?,
                operand(b, &bt, resolve, tables, parameters)?,
            ];
            if *op == BinaryOp::Concat {
                if ty == DataType::TextArray {
                    concat::bind_enum_operands([a, b], &mut args)?;
                } else if ty == DataType::Text {
                    args.iter_mut().for_each(attach_text_cast);
                }
            }
            collation::derive(args.iter())?;
            Ok(BoundScalar {
                ty: ty.clone(),
                node: if *op == BinaryOp::Concat && array_constructor::query_only(&ty) {
                    Node::ZonedOperator(
                        concat::name(&at, &bt, &ty).expect("known array overload").into(),
                        args,
                        None,
                    )
                } else if matches!(
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
                        match (*op, &at, &bt) {
                            (BinaryOp::Concat, DataType::TsVector, DataType::TsVector) => "\0ts_concat",
                            (BinaryOp::Concat, DataType::TsQuery, DataType::TsQuery) => "\0ts_or",
                            (BinaryOp::Overlap, DataType::TsQuery, DataType::TsQuery) => "\0ts_and",
                            (BinaryOp::Contains, DataType::TsQuery, DataType::TsQuery) => "\0ts_contains",
                            (BinaryOp::ContainedBy, DataType::TsQuery, DataType::TsQuery) => "\0ts_contained_by",
                            (BinaryOp::Concat, _, _) => concat::name(&at, &bt, &ty).unwrap_or(name(*op)),
                            _ => name(*op),
                        }.into(),
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
pub(super) fn eval(
    name: &str, ty: &DataType, args: &[BoundScalar], values: &[Value], budget: &mut usize,
) -> R<Value> {
    if let Some(result) = concat::eval(
        name, args, values, giggles_pg_types::temporal::SessionZone::Fixed(0), budget,
    ) {
        return result;
    }
    if values.contains(&Value::Null) {
        return Ok(Value::Null);
    }
    let op = name
        .strip_prefix(PREFIX)
        .ok_or(SqlError::UndefinedFunction)?;
    if let Some(function) = match op {
        "^" => Some("power"),
        "|/" => Some("sqrt"),
        "||/" => Some("cbrt"),
        _ => None,
    } {
        let value = numeric::eval(function, values).ok_or(SqlError::InternalError)?
            .inspect_err(|error| {
                if *error == SqlError::Overflow && *ty == DataType::Numeric {
                    user_types::view_replace_error("value overflows numeric format".into());
                }
            })?;
        charge(budget, &value)?;
        return Ok(value);
    }
    if op == "@" {
        return eval_abs(ty, &values[0], budget);
    }
    if op == "~" && let Value::Int(n) = values[0] {
        reserve(budget, 8)?;
        return Ok(Value::Int(!n));
    }
    if matches!(op, "@>" | "<@" | "&&") {
        let value = if args[0].ty == DataType::Jsonb {
            let [a, b] = values else { return Err(SqlError::Type) };
            let (left, right) = if op == "@>" { (a, b) } else { (b, a) };
            Value::Bool(jsonb_mutation::contains(left, right, budget)?)
        } else {
            uuid_array_operators::eval(op, values)?
        };
        charge(budget, &value)?;
        return Ok(value);
    }
    if matches!(op, "#>" | "#>>") {
        let value = logical_types::eval(op, &args[0].ty, values)?;
        charge(budget, &value)?;
        return Ok(value);
    }
    if matches!(op, "~" | "!~" | "~*" | "!~*")
        && let [Value::Text(value), Value::Text(pattern)] = values
    {
        return collation::regex(op, args, value, pattern, budget);
    }
    if op == "^@" {
        return collation::starts_with(args, values, budget);
    }
    if matches!(op, "~~" | "!~~" | "~~*" | "!~~*") {
        let escape = super::super::super::like::Escape::parse("\\")?;
        return collation::like(
            [&args[0], &args[1]], &values[0], &values[1], escape,
            (op.ends_with('*'), op.starts_with('!')), budget,
        )
        .map(|value| value.map_or(Value::Null, Value::Bool));
    }
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
                "&" => Some(a & b),
                "|" => Some(a | b),
                "#" => Some(a ^ b),
                "<<" | ">>" => {
                    let count = *b as u32;
                    Some(match (op, ty) {
                        ("<<", DataType::BigInt) => a.wrapping_shl(count),
                        (">>", DataType::BigInt) => a.wrapping_shr(count),
                        ("<<", DataType::SmallInt) => i64::from((*a as i32).wrapping_shl(count) as i16),
                        ("<<", _) => i64::from((*a as i32).wrapping_shl(count)),
                        _ => i64::from((*a as i32).wrapping_shr(count)),
                    })
                }
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
pub(super) fn eval_abs(ty: &DataType, value: &Value, budget: &mut usize) -> R<Value> {
    let out = match value {
        Value::Numeric(n) => Value::Numeric(
            Numeric::parse(n.to_string().trim_start_matches('-')).map_err(pg_error)?,
        ),
        Value::Float4(n) => Value::Float4(Float4::from_bits(n.value().abs().to_bits())),
        Value::Float8(n) => Value::Float8(Float8::from_bits(n.value().abs().to_bits())),
        Value::Int(n) => n.checked_abs().ok_or(SqlError::Overflow)
            .and_then(|n| checked_integer(n, ty))
            .inspect_err(|_| user_types::view_replace_error(format!("{} out of range", typeof_expr::name(ty))))?,
        _ => return Err(SqlError::UndefinedFunction),
    };
    charge(budget, &out)?;
    Ok(out)
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
