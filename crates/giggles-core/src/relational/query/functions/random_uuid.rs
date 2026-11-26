//! PostgreSQL 18 random and UUID builtin overloads.
use super::*;
use crate::relational::volatile::SessionVolatile;
use giggles_pg_types::temporal::SessionZone;

pub(super) const VOID_OID: u32 = 2278;

fn unqualified(name: &str) -> &str {
    name.strip_prefix("pg_catalog.").unwrap_or(name)
}

pub(super) fn volatile(name: &str) -> bool {
    matches!(
        unqualified(name),
        "gen_random_uuid" | "uuidv4" | "uuidv7" | "random" | "setseed"
    )
}

pub(in crate::relational::query) fn known(name: &str) -> bool {
    volatile(name)
        || matches!(unqualified(name), "uuid_extract_version" | "uuid_extract_timestamp")
}

fn default_routine(name: &str) -> bool {
    matches!(name, "gen_random_uuid" | "uuidv4" | "uuidv7" | "random"
        | "uuid_extract_version" | "uuid_extract_timestamp")
}

pub(in crate::relational) fn default_candidate(schema: Option<&str>, routine: &str) -> bool {
    (matches!(schema, None | Some("pg_catalog")) && default_routine(routine))
        || (schema.is_none() && routine.strip_prefix("pg_catalog.").is_some_and(default_routine))
}

pub(in crate::relational) fn parse_default(
    parser: &mut Parser,
    schema: Option<&str>,
    routine: &str,
    depth: usize,
) -> R<(String, DataType)> {
    if depth > 16 {
        return Err(SqlError::Limit);
    }
    let mut args = Vec::new();
    if !parser.sym(')') {
        loop {
            if args.len() == MAX_COLUMNS {
                return Err(SqlError::Limit);
            }
            args.push(Scalar::parse_aggregate_argument(parser, depth)?);
            if parser.sym(')') {
                break;
            }
            parser.punct(',')?;
        }
    }
    for arg in &args {
        check_default_shape(arg, depth + 1)?;
    }
    let no_columns = |_: &ColumnRef, _: usize| Err(SqlError::Unsupported);
    if !default_routine(routine) {
        // A quoted dotted identifier reached here as one routine name.
        let types = args.iter().map(|arg| {
            if arg.aggregate_unknown(&[]) {
                Ok(None)
            } else {
                arg.bind(&no_columns, &[], &[]).map(|arg| Some(arg.ty))
            }
        }).collect::<R<Vec<_>>>()?;
        let types = types.iter().map(Option::as_ref).collect::<Vec<_>>();
        return Err(diagnostic(routine, &types, SqlError::UndefinedFunction));
    }
    let name = schema.map_or_else(|| routine.to_owned(), |schema| format!("{schema}.{routine}"));
    let bound = Scalar::Call(name.into(), args).bind(&no_columns, &[], &[])?;
    Ok((default_sql(&bound, &mut 0)?, bound.ty))
}

fn check_default_shape(value: &Scalar, depth: usize) -> R<()> {
    if depth > 16 {
        return Err(SqlError::Limit);
    }
    match value {
        Scalar::Literal(_) => Ok(()),
        Scalar::Unary(UnaryOp::Neg | UnaryOp::Plus, arg)
        | Scalar::Cast(
            arg,
            DataType::SmallInt | DataType::Integer | DataType::BigInt
            | DataType::Numeric | DataType::Uuid | DataType::Interval,
        ) => check_default_shape(arg, depth + 1),
        Scalar::Call(name, args) if default_routine(unqualified(name)) => {
            for arg in args {
                check_default_shape(arg, depth + 1)?;
            }
            Ok(())
        }
        _ => Err(SqlError::Unsupported),
    }
}

fn default_sql(value: &BoundScalar, budget: &mut usize) -> R<String> {
    use crate::relational::defaults::{quoted, type_name};

    // Deparse calls without evaluating them, even when all arguments are constant.
    match &value.node {
        Node::Call(name, args) | Node::ZonedOperator(name, args, _) if default_routine(name) => {
            let args = args.iter().map(|arg| default_sql(arg, budget)).collect::<R<Vec<_>>>()?;
            return Ok(format!("{name}({})", args.join(", ")));
        }
        _ => {}
    }

    let constant = value.plan_constant(budget)?;
    Ok(match constant {
        Some(Value::Int(n)) if value.ty == DataType::Integer && n >= 0 => n.to_string(),
        Some(Value::Numeric(n)) => {
            let text = n.to_string();
            if text.contains('.') && !text.starts_with('-') {
                text
            } else {
                format!("{}::numeric", quoted(&text))
            }
        }
        Some(Value::Null) => format!("NULL::{}", type_name(&value.ty)),
        Some(constant) => {
            let text = output_text(&constant)?.ok_or(SqlError::Type)?;
            format!("{}::{}", quoted(&text), type_name(&value.ty))
        }
        None => match &value.node {
            Node::Cast(arg) => format!("({})::{}", default_sql(arg, budget)?, type_name(&value.ty)),
            _ => return Err(SqlError::Unsupported),
        },
    })
}

pub(super) fn overload(name: &str, types: &[Option<&DataType>]) -> R<(Option<DataType>, DataType)> {
    use DataType::*;
    let (argument, result) = match (unqualified(name), types) {
        ("gen_random_uuid" | "uuidv4" | "uuidv7", []) => (None, Uuid),
        ("random", []) => (None, DoublePrecision),
        ("uuidv7", [None | Some(Interval)]) => (Some(Interval), Uuid),
        ("uuid_extract_timestamp", [None | Some(Uuid)]) => (Some(Uuid), TimestampTz),
        ("uuid_extract_version", [None | Some(Uuid)]) => (Some(Uuid), SmallInt),
        (
            "setseed",
            [None | Some(SmallInt | Integer | BigInt | Numeric | Real | DoublePrecision)],
        ) => {
            // The scalar has an empty text representation. Its result OID is void.
            (Some(DoublePrecision), Text)
        }
        ("random", [lo, hi]) => {
            let result = match (lo, hi) {
                (
                    Some(SmallInt | Integer | BigInt | Numeric) | None,
                    Some(SmallInt | Integer | BigInt | Numeric) | None,
                ) => {
                    if [lo, hi].contains(&&Some(&Numeric)) {
                        Numeric
                    } else if [lo, hi].contains(&&Some(&BigInt)) {
                        BigInt
                    } else if [lo, hi].contains(&&Some(&Integer)) {
                        Integer
                    } else {
                        return Err(SqlError::AmbiguousFunction);
                    }
                }
                _ => return Err(SqlError::UndefinedFunction),
            };
            (Some(result.clone()), result)
        }
        _ => return Err(SqlError::UndefinedFunction),
    };
    Ok((argument, result))
}

pub(super) fn diagnostic(name: &str, types: &[Option<&DataType>], error: SqlError) -> SqlError {
    let types = types
        .iter()
        .map(|ty| ty.map_or("unknown", typeof_expr::name))
        .collect::<Vec<_>>()
        .join(", ");
    let (message, hint) = if error == SqlError::AmbiguousFunction {
        (
            "is not unique",
            "Could not choose a best candidate function. You might need to add explicit type casts.",
        )
    } else {
        (
            "does not exist",
            "No function matches the given name and argument types. You might need to add explicit type casts.",
        )
    };
    user_types::view_replace_error(format!("function {name}({types}) {message}"));
    user_types::sql_error_hint(hint);
    error
}

#[inline(never)]
pub(super) fn infer(
    name: &str,
    args: &[Scalar],
    parameters: &mut [Option<DataType>],
    resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
    tables: &[&TableDef],
) -> R<()> {
    for arg in args {
        arg.infer(None, parameters, resolve, tables)?;
    }
    let bound = args
        .iter()
        .map(|arg| {
            if arg.aggregate_unknown(parameters) {
                Ok(None)
            } else {
                arg.bind(resolve, tables, parameters)
                    .map(|arg| Some(arg.ty))
            }
        })
        .collect::<R<Vec<_>>>()?;
    let types = bound.iter().map(Option::as_ref).collect::<Vec<_>>();
    let (target, _) = overload(name, &types).map_err(|e| diagnostic(name, &types, e))?;
    for arg in args {
        if arg.aggregate_unknown(parameters) {
            arg.infer(target.as_ref(), parameters, resolve, tables)?;
        }
    }
    Ok(())
}

pub(super) fn bind(name: &str, original: &[Scalar], args: Vec<BoundScalar>) -> R<BoundScalar> {
    let types = original
        .iter()
        .zip(&args)
        .map(|(source, arg)| {
            (!matches!(source, Scalar::Literal(Value::Text(_) | Value::Null))).then_some(&arg.ty)
        })
        .collect::<Vec<_>>();
    let (target, ty) = overload(name, &types).map_err(|e| diagnostic(name, &types, e))?;
    let args = if let Some(target) = target {
        args.into_iter()
            .map(|arg| {
                if arg.ty == target {
                    return Ok(arg);
                }
                let node = match &arg.node {
                    Node::Literal(Value::Null) => Node::Literal(Value::Null),
                    Node::Literal(Value::Text(s)) => {
                        literal_cast_node(Value::Text(s.clone()), &target)?
                    }
                    _ => cast_node(arg, &target),
                };
                Ok(BoundScalar {
                    ty: target.clone(),
                    node,
                })
            })
            .collect::<R<Vec<_>>>()?
    } else {
        args
    };
    let name = unqualified(name);
    let node = if name == "uuidv7" && !args.is_empty() {
        Node::ZonedOperator(name.to_owned(), args, None)
    } else {
        Node::Call(name.to_owned(), args)
    };
    Ok(BoundScalar { ty, node })
}

pub(super) fn eval(name: &str, values: &[Value]) -> R<Value> {
    match (name, values) {
        // gen_random_uuid() deliberately returns UUIDv7, not PostgreSQL's v4,
        // because v7 is better for indexes: time-ordered keys keep B-tree
        // inserts local instead of scattering them, so we prefer it (user
        // decision 2026-11-22). uuidv4() keeps PostgreSQL's version 4 behavior.
        ("gen_random_uuid" | "uuidv7", []) => v7(None, SessionZone::Fixed(0)),
        ("uuidv4", []) => uuid_ossp::v4(),
        ("uuid_extract_version" | "uuid_extract_timestamp", [Value::Text(uuid)]) => {
            extract(name, uuid)
        }
        ("setseed", [Value::Float8(seed)]) => {
            let value = seed.value();
            if !(-1.0..=1.0).contains(&value) {
                return Err(invalid_parameter(format!(
                    "setseed parameter {} is out of allowed range [-1,1]",
                    seed.to_text()
                )));
            }
            SessionVolatile::with(|state| {
                state.seed(value);
                Ok(Value::Text(String::new()))
            })
        }
        ("random", []) => SessionVolatile::with(|state| {
            Ok(Value::Float8(Float8::from_bits(
                state.random()?.double().to_bits(),
            )))
        }),
        ("random", [Value::Int(lo), Value::Int(hi)]) => {
            check_bounds(lo, hi)?;
            SessionVolatile::with(|state| {
                let span = (*hi as u64).wrapping_sub(*lo as u64);
                Ok(Value::Int(
                    (*lo as u64).wrapping_add(state.random()?.inclusive(span)) as i64,
                ))
            })
        }
        ("random", [Value::Numeric(lo), Value::Numeric(hi)]) => {
            for (bound, value) in [("lower", lo), ("upper", hi)] {
                if value.scale().is_none() {
                    let kind = if value.to_string() == "NaN" {
                        "NaN"
                    } else {
                        "infinity"
                    };
                    return Err(invalid_parameter(format!("{bound} bound cannot be {kind}")));
                }
            }
            check_bounds(lo, hi)?;
            SessionVolatile::with(|state| {
                let random = state.random()?;
                lo.random_between(hi, |max| random.inclusive(max))
                    .map(Value::Numeric)
                    .map_err(|_| SqlError::Overflow)
            })
        }
        _ => Err(SqlError::UndefinedFunction),
    }
}

fn check_bounds<T: Ord>(lo: &T, hi: &T) -> R<()> {
    if lo > hi {
        return Err(invalid_parameter(
            "lower bound must be less than or equal to upper bound".into(),
        ));
    }
    Ok(())
}

fn invalid_parameter(message: String) -> SqlError {
    user_types::view_replace_error(message);
    SqlError::InvalidParameterValue
}

const UNIX_EPOCH_US: i64 = 946_684_800_000_000;
const MAX_V7_MS: u64 = (1 << 48) - 1;

pub(super) fn v7_interval(values: &[Value], zone: Option<SessionZone>) -> R<Value> {
    match values {
        [Value::Null] => Ok(Value::Null),
        [Value::Interval(shift)] => v7(Some(*shift), zone.unwrap_or(SessionZone::Fixed(0))),
        _ => Err(SqlError::Type),
    }
}

fn v7(shift: Option<Interval>, zone: SessionZone) -> R<Value> {
    if shift.is_some_and(|shift| shift == Interval::INFINITY || shift == Interval::NEG_INFINITY) {
        user_types::view_replace_error("interval out of range for UUID version 7".into());
        user_types::sql_error_detail("UUID version 7 does not support infinite intervals.".into());
        return Err(SqlError::DatetimeFieldOverflow);
    }
    let ns = SessionVolatile::with(SessionVolatile::uuid_nanos)?;
    let pg_range = || {
        user_types::view_replace_error("timestamp out of range".into());
        SqlError::DatetimeFieldOverflow
    };
    let ns = if let Some(shift) = shift {
        let us = i64::try_from(ns / 1_000).map_err(|_| SqlError::DatetimeFieldOverflow)?;
        let timestamp = TimestampTz::from_microseconds(us - UNIX_EPOCH_US)
            .map_err(|_| pg_range())?
            .add_interval(shift, zone)
            .map_err(|_| pg_range())?;
        let micros = i128::from(timestamp.microseconds()) + i128::from(UNIX_EPOCH_US);
        if !(0..=i128::from(MAX_V7_MS) * 1_000).contains(&micros) {
            user_types::view_replace_error("timestamp out of range for UUID version 7".into());
            user_types::sql_error_detail(
                "UUID version 7 supports timestamps from 1970-01-01 to approximately year 10889."
                    .into(),
            );
            return Err(SqlError::DatetimeFieldOverflow);
        }
        (micros as u128) * 1_000 + ns % 1_000
    } else {
        ns
    };
    let ms = u64::try_from(ns / 1_000_000).map_err(|_| SqlError::DatetimeFieldOverflow)?;
    if ms > MAX_V7_MS {
        return Err(SqlError::DatetimeFieldOverflow);
    }
    let fraction = ((ns % 1_000_000) * 4_096 / 1_000_000) as u16;
    let mut bytes = [0; 16];
    bytes[..6].copy_from_slice(&ms.to_be_bytes()[2..]);
    bytes[6..8].copy_from_slice(&(0x7000 | fraction).to_be_bytes());
    getrandom::fill(&mut bytes[8..]).map_err(|_| SqlError::Unsupported)?;
    bytes[8] = (bytes[8] & 63) | 128;
    Ok(uuid_ossp::format_uuid(bytes))
}

fn extract(name: &str, uuid: &str) -> R<Value> {
    // UUID values reach the scalar evaluator in their canonical text form.
    let parse = |start, end| {
        uuid.get(start..end)
            .and_then(|digits| u64::from_str_radix(digits, 16).ok())
            .ok_or(SqlError::InvalidTextRepresentation)
    };
    if parse(19, 20)? & 12 != 8 {
        return Ok(Value::Null);
    }
    let version = parse(14, 15)?;
    if name == "uuid_extract_version" {
        return Ok(Value::Int(version as i64));
    }
    let micros = match version {
        1 => {
            let ticks = ((parse(14, 18)? & 0xfff) << 48) | (parse(9, 13)? << 32) | parse(0, 8)?;
            (ticks / 10) as i64 - 13_165_977_600_000_000
        }
        7 => {
            let millis = (parse(0, 8)? << 16) | parse(9, 13)?;
            (millis * 1_000) as i64 - UNIX_EPOCH_US
        }
        _ => return Ok(Value::Null),
    };
    TimestampTz::from_microseconds(micros)
        .map(Value::TimestampTz)
        .map_err(|_| SqlError::DatetimeFieldOverflow)
}
