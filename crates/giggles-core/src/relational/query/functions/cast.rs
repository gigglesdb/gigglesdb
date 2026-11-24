//! Explicit cast target grammar and in-memory type modifiers.
use super::*;
use giggles_pg_types::character::{CharKind, CharTypmod};
use giggles_pg_types::typmod::{
    NumericTypmod, TemporalType, TypmodError, float_precision_is_real, interval_typmod,
};

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

fn typmod_error(error: TypmodError, diagnose: bool) -> SqlError {
    if diagnose {
        user_types::view_replace_error(error.message());
    }
    SqlError::InvalidParameterValue
}

fn character_modifier(ty: &DataType, length: i32, diagnose: bool) -> R<i32> {
    CharTypmod::new(
        if *ty == DataType::Varchar {
            CharKind::Varchar
        } else {
            CharKind::Bpchar
        },
        length,
    )
    .map(CharTypmod::to_typmod)
    .map_err(|error| typmod_error(error, diagnose))
}

fn generic_modifier(
    ty: &DataType,
    items: Option<Vec<String>>,
    diagnose: bool,
) -> R<Option<i32>> {
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
                .map_err(|error| typmod_error(error, diagnose))?
                .to_typmod()
        }
        DataType::Varchar | DataType::Bpchar => {
            if items.len() != 1 {
                return Err(SqlError::InvalidParameterValue);
            }
            character_modifier(ty, integer(&items[0])?, diagnose)?
        }
        DataType::Time | DataType::TimeTz | DataType::Timestamp | DataType::TimestampTz => {
            let [precision] = items.as_slice() else {
                return Err(typmod_error(TypmodError::InvalidTypeModifier, diagnose));
            };
            let kind = match ty {
                DataType::Time => TemporalType::Time,
                DataType::TimeTz => TemporalType::TimeTz,
                DataType::Timestamp => TemporalType::Timestamp,
                _ => TemporalType::TimestampTz,
            };
            kind.precision(integer(precision)?).map_err(|e| typmod_error(e, diagnose))?
        }
        DataType::Interval if diagnose => {
            let (range, precision) = match items.as_slice() {
                [range] => (range, None),
                [range, precision] => (range, Some(integer(precision)?)),
                _ => return Err(typmod_error(TypmodError::InvalidIntervalModifier, diagnose)),
            };
            interval_typmod(integer(range)?, precision).map_err(|e| typmod_error(e, diagnose))?;
            return Ok(None);
        }
        DataType::Interval => return Err(SqlError::Unsupported),
        _ => return Err(SqlError::Syntax),
    };
    Ok(Some(modifier))
}

fn generic(
    p: &mut Parser, name: &str, schema: Option<&str>, diagnose: bool,
) -> R<(DataType, Option<i32>)> {
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
        "regtype" => DataType::RegType,
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
    let modifier = generic_modifier(&ty, items, diagnose).inspect_err(|error| {
        if diagnose && *error == SqlError::Syntax {
            let qualified = schema.map_or_else(|| name.to_owned(), |schema| format!("{schema}.{name}"));
            user_types::view_replace_error(format!(
                r#"type modifier is not allowed for type "{qualified}""#
            ));
        }
    })?;
    Ok((ty, modifier))
}

pub(super) fn target(p: &mut Parser, diagnose: bool) -> R<(DataType, Option<i32>)> {
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
        generic(p, &name2, Some(&name), diagnose)?
    } else if quoted {
        generic(p, &name, None, diagnose)?
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
                    Some(precision) => {
                        let precision = precision.parse::<u32>()
                            .map_err(|_| SqlError::InvalidParameterValue)?;
                        if float_precision_is_real(precision).map_err(|error| typmod_error(error, diagnose))? {
                            DataType::Real
                        } else {
                            DataType::DoublePrecision
                        }
                    }
                };
                (ty, None)
            }
            "numeric" | "decimal" | "dec" => {
                let items = modifiers(p)?;
                (
                    DataType::Numeric,
                    generic_modifier(&DataType::Numeric, items, diagnose)?,
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
                        diagnose,
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
            "interval" if diagnose && p.tokens.get(p.at) == Some(&Token::Symbol('(')) => {
                if let Some(precision) = unsigned_modifier(p)? {
                    TemporalType::Interval.precision(integer(&precision)?)
                        .map_err(|e| typmod_error(e, diagnose))?;
                }
                (DataType::Interval, None)
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
            _ => generic(p, &name, None, diagnose)?,
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

// Builtin grammar keeps its existing diagnostics; unknown names bind to domains.
pub(super) fn parse_expression(p: &mut Parser, value: Scalar) -> R<Scalar> {
    if p.privilege_casts {
        let start = p.at;
        if p.word("pg_catalog") && !p.sym('.') { p.at = start; }
        if let Some(Token::Word(name)) = p.tokens.get(p.at)
            && matches!(name.as_str(), "oid" | "name") {
                let name = format!("\0inquiry_{name}");
                p.at += 1;
                return Ok(Scalar::Call(name, vec![value]));
        }
        p.at = start;
    }
    let start = p.at;
    match super::cast_type(p) {
        Ok(target) => Ok(expression(value, target)),
        Err(error @ (SqlError::UndefinedObject | SqlError::Unsupported)) => {
            p.at = start;
            let name = user_types::TypeReference::parse(p)?;
            if p.tokens.get(p.at) == Some(&Token::Symbol('(')) {
                return Err(SqlError::Unsupported);
            }
            if matches!(name.name.as_str(), "json" | "pg_catalog\0json") && !name.array
            {
                return Ok(Scalar::Call(json_aggregate::INPUT.into(), vec![value]));
            }
            Ok(Scalar::Domain(Box::new(value), name, Box::new(Err(error))))
        }
        Err(error) => Err(error),
    }
}

// PG18 oid input accepts negative int4 spellings; int8 conversion is unsigned.
pub(super) fn inquiry(value: Value, ty: &DataType, name: &str, budget: &mut usize) -> R<Value> {
    if value == Value::Null { return Ok(value); }
    if name == "\0inquiry_name" {
        let Value::Text(mut text) = cast_value(value, &DataType::Text, budget)? else { return Err(SqlError::Type); };
        let mut end = text.len().min(63);
        while !text.is_char_boundary(end) { end -= 1; }
        text.truncate(end);
        return Ok(Value::Text(text));
    }
    let number = match value {
        Value::Text(text) => {
            let text = text.trim();
            let unsigned = text.strip_prefix(['+', '-']).unwrap_or(text);
            let (radix, digits) = if let Some(d) = unsigned.strip_prefix("0x").or_else(|| unsigned.strip_prefix("0X")) { (16, d) }
                else if let Some(d) = unsigned.strip_prefix("0b").or_else(|| unsigned.strip_prefix("0B")) { (2, d) }
                else if unsigned.starts_with('0') && unsigned.len() > 1 { (8, unsigned) }
                else { (10, unsigned) };
            if digits.is_empty() || !digits.chars().all(|c| c.is_digit(radix)) {
                return Err(SqlError::InvalidTextRepresentation);
            }
            let magnitude = i64::from_str_radix(digits, radix).map_err(|_| SqlError::Overflow)?;
            let number = if text.starts_with('-') { -magnitude } else { magnitude };
            if !(i64::from(i32::MIN)..=i64::from(u32::MAX)).contains(&number) { return Err(SqlError::Overflow); }
            number
        }
        Value::Int(number) => {
            let min = if *ty == DataType::BigInt { 0 } else { i64::from(i32::MIN) };
            if !(min..=i64::from(u32::MAX)).contains(&number) { return Err(SqlError::Overflow); }
            number
        }
        _ => return Err(SqlError::Type),
    };
    Ok(Value::Int(i64::from(number as u32)))
}
