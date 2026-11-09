//! Explicit cast target grammar and in-memory type modifiers.
use super::*;
use giggles_pg_types::character::{CharKind, CharTypmod};
use giggles_pg_types::typmod::NumericTypmod;

pub(super) fn explicit_target(value: &Scalar) -> Option<&DataType> {
    match value {
        Scalar::Cast(_, ty) => Some(ty),
        Scalar::Call(name, args) if name == CAST_TYPMOD => match args.first() {
            Some(Scalar::Cast(_, ty)) => Some(ty),
            _ => None,
        },
        _ => None,
    }
}

pub(super) fn typed_literal(p: &Parser) -> bool {
    if !matches!(p.tokens.get(p.at), Some(Token::Word(word)) if ["date", "time", "timetz", "timestamp", "timestamptz", "interval"].contains(&word.as_str()))
    {
        return false;
    }
    let follows_type = |at| {
        matches!(p.tokens.get(at), Some(Token::String(_)))
            || matches!(p.tokens.get(at), Some(Token::Word(word)) if word == "with" || word == "without")
    };
    if follows_type(p.at + 1) {
        return true;
    }
    // A precision followed by a literal belongs to type-literal syntax. Keep
    // ordinary calls such as time(2) on the existing routine path.
    if p.tokens.get(p.at + 1) != Some(&Token::Symbol('(')) {
        return false;
    }
    p.tokens[p.at + 2..]
        .iter()
        .position(|token| *token == Token::Symbol(')'))
        .is_some_and(|close| follows_type(p.at + 3 + close))
}

pub(super) fn expression(value: Scalar, (ty, modifier): (DataType, Option<i32>)) -> Scalar {
    let value = Scalar::Cast(Box::new(value), ty);
    match modifier {
        Some(modifier) => Scalar::Call(
            CAST_TYPMOD.into(),
            vec![value, Scalar::Literal(Value::Int(i64::from(modifier)))],
        ),
        None => value,
    }
}

fn unsigned_modifier(p: &mut Parser) -> R<Option<String>> {
    if !p.sym('(') {
        return Ok(None);
    }
    let Some(Token::Number(number)) = p.tokens.get(p.at) else {
        return Err(SqlError::Syntax);
    };
    if number.is_empty() || !number.bytes().all(|b| b.is_ascii_digit()) {
        return Err(SqlError::Syntax);
    }
    let number = number.clone();
    p.at += 1;
    p.punct(')')?;
    Ok(Some(number))
}

fn modifiers(p: &mut Parser) -> R<Option<Vec<String>>> {
    if !p.sym('(') {
        return Ok(None);
    }
    let mut items = Vec::new();
    loop {
        let sign = match p.tokens.get(p.at) {
            Some(Token::Op(sign)) if sign == "+" || sign == "-" => {
                let sign = sign.clone();
                p.at += 1;
                Some(sign)
            }
            _ => None,
        };
        let item = match (sign, p.tokens.get(p.at)) {
            (Some(sign), Some(Token::Number(number))) => format!("{sign}{number}"),
            (None, Some(Token::Number(item) | Token::String(item) | Token::Word(item))) => {
                item.clone()
            }
            _ => return Err(SqlError::Syntax),
        };
        p.at += 1;
        items.push(item);
        if p.sym(')') {
            return Ok(Some(items));
        }
        p.punct(',')?;
    }
}

fn integer(item: &str) -> R<i32> {
    let item = item.trim();
    let digits = item.strip_prefix(['+', '-']).unwrap_or(item);
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(SqlError::InvalidTextRepresentation);
    }
    // Preserve integer syntax beyond i32 so type-specific range validation and
    // temporal precision clamping still determine the result.
    Ok(item.parse().unwrap_or(if item.starts_with('-') {
        i32::MIN
    } else {
        i32::MAX
    }))
}

fn character_modifier(ty: &DataType, length: i32) -> R<i32> {
    CharTypmod::new(
        if *ty == DataType::Varchar {
            CharKind::Varchar
        } else {
            CharKind::Bpchar
        },
        length,
    )
    .map(CharTypmod::to_typmod)
    .map_err(|_| SqlError::InvalidParameterValue)
}

fn generic_modifier(ty: &DataType, items: Option<Vec<String>>) -> R<Option<i32>> {
    let Some(items) = items else {
        return Ok(None);
    };
    let modifier = match ty {
        DataType::Numeric => {
            if !matches!(items.len(), 1 | 2) {
                return Err(SqlError::InvalidParameterValue);
            }
            let precision = integer(&items[0])?;
            let scale = items.get(1).map_or(Ok(0), |item| integer(item))?;
            NumericTypmod::new(precision, scale)
                .map_err(|_| SqlError::InvalidParameterValue)?
                .to_typmod()
        }
        DataType::Varchar | DataType::Bpchar => {
            if items.len() != 1 {
                return Err(SqlError::InvalidParameterValue);
            }
            character_modifier(ty, integer(&items[0])?)?
        }
        DataType::Time | DataType::TimeTz | DataType::Timestamp | DataType::TimestampTz => {
            if items.len() != 1 {
                return Err(SqlError::InvalidParameterValue);
            }
            let precision = integer(&items[0])?;
            if precision < 0 {
                return Err(SqlError::InvalidParameterValue);
            }
            precision.min(6)
        }
        DataType::Interval => return Err(SqlError::Unsupported),
        _ => return Err(SqlError::Syntax),
    };
    Ok(Some(modifier))
}

fn generic(p: &mut Parser, name: &str) -> R<(DataType, Option<i32>)> {
    // Modifier syntax is checked before catalog lookup, even for unknown types.
    let items = modifiers(p)?;
    let ty = match name {
        "int2" => DataType::SmallInt,
        "int4" => DataType::Integer,
        "int8" => DataType::BigInt,
        "text" => DataType::Text,
        "bool" => DataType::Boolean,
        "bytea" => DataType::Bytes,
        "numeric" => DataType::Numeric,
        "float4" => DataType::Real,
        "float8" => DataType::DoublePrecision,
        "date" => DataType::Date,
        "time" => DataType::Time,
        "timetz" => DataType::TimeTz,
        "timestamp" => DataType::Timestamp,
        "timestamptz" => DataType::TimestampTz,
        "interval" => DataType::Interval,
        "varchar" => DataType::Varchar,
        "uuid" => DataType::Uuid,
        "jsonb" => DataType::Jsonb,
        "bpchar" => DataType::Bpchar,
        _ => {
            return Err(if pg18_types::PG_CATALOG.binary_search(&name).is_ok() {
                SqlError::Unsupported
            } else {
                SqlError::UndefinedObject
            });
        }
    };
    let modifier = generic_modifier(&ty, items)?;
    Ok((ty, modifier))
}

pub(super) fn target(p: &mut Parser) -> R<(DataType, Option<i32>)> {
    let (name, quoted) = match p.tokens.get(p.at) {
        Some(Token::Word(name)) => (name.clone(), false),
        Some(Token::QuotedIdent(name)) => (name.clone(), true),
        _ => return Err(SqlError::Syntax),
    };
    p.at += 1;
    let result = if p.sym('.') {
        let name2 = match p.tokens.get(p.at) {
            Some(Token::Word(name) | Token::QuotedIdent(name)) => name.clone(),
            _ => return Err(SqlError::Syntax),
        };
        p.at += 1;
        if name != "pg_catalog" {
            modifiers(p)?;
            return Err(SqlError::Unsupported);
        }
        generic(p, &name2)?
    } else if quoted {
        generic(p, &name)?
    } else {
        match name.as_str() {
            "smallint" | "int" | "integer" | "bigint" | "real" | "boolean" => {
                let ty = match name.as_str() {
                    "smallint" => DataType::SmallInt,
                    "int" | "integer" => DataType::Integer,
                    "bigint" => DataType::BigInt,
                    "real" => DataType::Real,
                    _ => DataType::Boolean,
                };
                (ty, None)
            }
            "double" if p.word("precision") => (DataType::DoublePrecision, None),
            "float" => {
                let ty = match unsigned_modifier(p)? {
                    None => DataType::DoublePrecision,
                    Some(precision) => match precision.parse::<u32>() {
                        Ok(1..=24) => DataType::Real,
                        Ok(25..=53) => DataType::DoublePrecision,
                        _ => return Err(SqlError::InvalidParameterValue),
                    },
                };
                (ty, None)
            }
            "numeric" | "decimal" | "dec" => {
                let items = modifiers(p)?;
                (
                    DataType::Numeric,
                    generic_modifier(&DataType::Numeric, items)?,
                )
            }
            "varchar" | "char" | "character" | "nchar" | "national" => {
                if name == "national" && !p.word("character") && !p.word("char") {
                    return Err(SqlError::Syntax);
                }
                let varying = name == "varchar" || name != "nchar" && p.word("varying");
                let ty = if varying {
                    DataType::Varchar
                } else {
                    DataType::Bpchar
                };
                let modifier = match unsigned_modifier(p)? {
                    Some(length) => Some(character_modifier(
                        &ty,
                        length
                            .parse()
                            .map_err(|_| SqlError::InvalidParameterValue)?,
                    )?),
                    None if !varying => Some(5),
                    None => None,
                };
                (ty, modifier)
            }
            "time" | "timestamp" => {
                let precision = unsigned_modifier(p)?
                    .map(|number| number.parse::<u32>().unwrap_or(u32::MAX).min(6) as i32);
                let ty = if name == "time" {
                    DataType::Time
                } else {
                    DataType::Timestamp
                };
                (
                    super::super::super::ddl_type::time_zone_suffix(p, ty)?,
                    precision,
                )
            }
            "interval" => {
                if matches!(p.tokens.get(p.at), Some(Token::Symbol('(')))
                    || matches!(p.tokens.get(p.at), Some(Token::Word(word)) if ["year", "month", "day", "hour", "minute", "second"].contains(&word.as_str()))
                {
                    return Err(SqlError::Unsupported);
                }
                (DataType::Interval, None)
            }
            "bit" => return Err(SqlError::Unsupported),
            _ => generic(p, &name)?,
        }
    };
    // A second modifier or zone suffix is never a projection alias.
    if matches!(
        p.tokens.get(p.at),
        Some(Token::Symbol('(') | Token::Symbol('.'))
    ) || matches!(p.tokens.get(p.at), Some(Token::Word(word)) if ["with", "without"].contains(&word.as_str()))
    {
        return Err(SqlError::Syntax);
    }
    Ok(result)
}

pub(super) fn apply(value: Value, ty: &DataType, modifier: i32, budget: &mut usize) -> R<Value> {
    if value == Value::Null {
        return Ok(value);
    }
    let temporal_error = |_| SqlError::DatetimeFieldOverflow;
    Ok(match (value, ty) {
        (Value::Text(text), DataType::Varchar | DataType::Bpchar) => {
            let kind = if *ty == DataType::Varchar {
                CharKind::Varchar
            } else {
                CharKind::Bpchar
            };
            let modifier = CharTypmod::from_typmod(kind, modifier).ok_or(SqlError::Type)?;
            let value = Value::Text(
                giggles_pg_types::character::coerce_explicit(&text, modifier).into_owned(),
            );
            charge(budget, &value)?;
            value
        }
        (Value::Numeric(value), DataType::Numeric) => Value::Numeric(
            value
                .apply_typmod(NumericTypmod::from_typmod(modifier).ok_or(SqlError::Type)?)
                .map_err(|_| SqlError::Overflow)?,
        ),
        (Value::Time(value), DataType::Time) => Value::Time(
            value
                .with_precision(modifier as u32)
                .map_err(temporal_error)?,
        ),
        (Value::TimeTz(value), DataType::TimeTz) => Value::TimeTz(
            value
                .with_precision(modifier as u32)
                .map_err(temporal_error)?,
        ),
        (Value::Timestamp(value), DataType::Timestamp) => Value::Timestamp(
            value
                .with_precision(modifier as u32)
                .map_err(temporal_error)?,
        ),
        (Value::TimestampTz(value), DataType::TimestampTz) => Value::TimestampTz(
            value
                .with_precision(modifier as u32)
                .map_err(temporal_error)?,
        ),
        _ => return Err(SqlError::Type),
    })
}
