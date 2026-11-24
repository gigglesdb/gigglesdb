//! DDL type spellings and modifiers. Expression casts retain their own grammar.
use super::*;
thread_local! {
    static WARNINGS: std::cell::RefCell<Vec<String>> = const { std::cell::RefCell::new(Vec::new()) };
}
/// Consume temporal precision warnings after execution on this thread.
pub fn take_sql_warnings() -> Vec<String> {
    WARNINGS.with(|warnings| std::mem::take(&mut *warnings.borrow_mut()))
}
pub(super) fn publish(warnings: &[String]) {
    WARNINGS.with(|pending| *pending.borrow_mut() = warnings.to_vec());
}

#[derive(Clone, Copy)]
pub(super) enum Site {
    Create,
    Alter,
}
pub(super) struct Parsed {
    pub data_type: DataType,
    pub typmod: Option<i32>,
    pub semantic_error: Option<SqlError>,
}
pub(super) fn parse_column_type(
    p: &mut Parser,
    site: Site,
) -> R<(Parsed, Option<user_types::TypeReference>)> {
    let start = p.at;
    match parse_inner(p, site, SqlError::UndefinedObject) {
        Ok(parsed) => Ok((parsed, None)),
        Err(error) if user_type_fallback(p, start, &error) => {
            p.at = start;
            let name = user_types::TypeReference::parse(p)?;
            Ok((Parsed {
                data_type: if name.array { DataType::TextArray } else { DataType::Text },
                typmod: None,
                semantic_error: None,
            }, Some(name)))
        }
        Err(SqlError::UndefinedObject) => Err(SqlError::Unsupported),
        Err(error) => Err(error),
    }
}

// Keep diagnostics from built-in type grammar, including unsupported arrays.
fn user_type_fallback(p: &Parser, start: usize, error: &SqlError) -> bool {
    if matches!(p.tokens.get(start), Some(Token::QuotedIdent(_))) {
        return matches!(error, SqlError::Syntax | SqlError::Unsupported);
    }
    *error == SqlError::UndefinedObject && p.at == start + 1 && match p.tokens.get(p.at) {
        None | Some(Token::Symbol(',' | ')' | ';' | '.' | '[')) => true,
        Some(Token::Word(word)) => matches!(
            word.as_str(),
            "not" | "null" | "default" | "constraint" | "check" | "unique"
                | "references" | "primary" | "generated" | "collate" | "using"
        ),
        _ => false,
    }
}

pub(super) fn parse(p: &mut Parser, site: Site) -> R<Parsed> {
    parse_inner(p, site, SqlError::Unsupported)
}

fn parse_inner(p: &mut Parser, site: Site, unknown_type: SqlError) -> R<Parsed> {
    let Some(Token::Word(word)) = p.tokens.get(p.at).cloned() else {
        return Err(match site {
            Site::Create => SqlError::Unsupported,
            Site::Alter => SqlError::Syntax,
        });
    };
    p.at += 1;
    let mut data_type = match word.as_str() {
        "int" | "integer" => DataType::Integer,
        "smallint" => DataType::SmallInt,
        "bigint" => DataType::BigInt,
        "int2" if matches!(site, Site::Alter) => DataType::SmallInt,
        "int4" if matches!(site, Site::Alter) => DataType::Integer,
        "int8" if matches!(site, Site::Alter) => DataType::BigInt,
        "bool" | "boolean" => DataType::Boolean,
        "numeric" | "decimal" => DataType::Numeric,
        "real" | "float4" => DataType::Real,
        "float8" => DataType::DoublePrecision,
        "double" => {
            p.need("precision")?;
            DataType::DoublePrecision
        }
        "varchar" => DataType::Varchar,
        "char" | "character" => {
            if p.word("varying") {
                DataType::Varchar
            } else {
                DataType::Bpchar
            }
        }
        "bpchar" => DataType::Bpchar,
        "text" => DataType::Text,
        "uuid" => DataType::Uuid,
        "jsonb" => DataType::Jsonb,
        "bytea" => DataType::Bytes,
        "bytes" if matches!(site, Site::Create) => DataType::Bytes,
        "date" => DataType::Date,
        "time" => DataType::Time,
        "timetz" => DataType::TimeTz,
        "timestamp" => DataType::Timestamp,
        "timestamptz" => DataType::TimestampTz,
        "interval" => DataType::Interval,
        "serial" | "serial2" | "serial4" | "serial8" | "smallserial" | "bigserial" => {
            return Err(SqlError::Unsupported);
        }
        _ => return Err(unknown_type),
    };
    // Keep unsupported array families rejected before CREATE precision policy.
    // The lexer now admits brackets for the supported integer, text and UUID arrays.
    if !matches!(
        data_type,
        DataType::Integer | DataType::BigInt | DataType::Text | DataType::Uuid
    ) {
        let mut nesting = 0usize;
        for token in &p.tokens[p.at..] {
            match token {
                Token::Symbol('[') => return Err(SqlError::Unsupported),
                Token::Symbol('(') => nesting += 1,
                Token::Symbol(')') if nesting > 0 => nesting -= 1,
                Token::Symbol(',' | ')' | ';') if nesting == 0 => break,
                _ => {}
            }
        }
    }
    let temporal_suffix = matches!(word.as_str(), "time" | "timestamp");
    let character = matches!(data_type, DataType::Varchar | DataType::Bpchar);
    let temporal = matches!(
        data_type,
        DataType::Time | DataType::TimeTz | DataType::Timestamp | DataType::TimestampTz
    );
    let mut requested_precision = None;
    // CREATE accepts character, numeric and temporal modifiers.
    if matches!(site, Site::Create)
        && !character
        && data_type != DataType::Numeric
        && !temporal
        && p.tokens.get(p.at) == Some(&Token::Symbol('('))
    {
        return Err(SqlError::Syntax);
    }
    let mut semantic_error = None;
    let accepts_modifier =
        matches!(site, Site::Alter) || character || data_type == DataType::Numeric || temporal;
    let mut typmod = if accepts_modifier && p.sym('(') {
        if temporal_suffix && matches!(p.tokens.get(p.at), Some(Token::Op(op)) if op == "-" || op == "+") {
            return Err(SqlError::Syntax);
        }
        let first = if data_type == DataType::Numeric {
            numeric_integer(p)?
        } else {
            integer(p)?
        };
        let value = if character {
            let kind = if data_type == DataType::Varchar {
                giggles_pg_types::character::CharKind::Varchar
            } else {
                giggles_pg_types::character::CharKind::Bpchar
            };
            match giggles_pg_types::character::CharTypmod::new(kind, first) {
                Ok(value) => value.to_typmod(),
                Err(_) => {
                    semantic_error = Some(SqlError::InvalidParameterValue);
                    0
                }
            }
        } else if data_type == DataType::Numeric {
            let scale = if p.sym(',') { numeric_integer(p)? } else { 0 };
            // PostgreSQL reports valid integer modifiers with excess arity as
            // an invalid parameter, while malformed argument lists stay syntax errors.
            while p.sym(',') {
                numeric_integer(p)?;
                semantic_error = Some(SqlError::InvalidParameterValue);
            }
            match giggles_pg_types::typmod::NumericTypmod::new(first, scale) {
                Ok(value) => value.to_typmod(),
                Err(_) => {
                    semantic_error = Some(SqlError::InvalidParameterValue);
                    0
                }
            }
        } else if temporal {
            if !temporal_suffix && p.sym(',') {
                numeric_integer(p)?;
                while p.sym(',') {
                    numeric_integer(p)?;
                }
                p.punct(')')?;
                user_types::invalid_type_modifier_error();
                return Err(SqlError::InvalidParameterValue);
            }
            if first < 0 {
                semantic_error = Some(SqlError::InvalidParameterValue);
            }
            requested_precision = Some(first);
            first.min(6)
        } else {
            first
        };
        p.punct(')')?;
        Some(if data_type == DataType::Interval {
            (32767 << 16) | value
        } else {
            value
        })
    } else {
        None
    };
    if typmod.is_none() && data_type == DataType::Bpchar && word != "bpchar" {
        typmod = Some(5);
    }
    if temporal_suffix {
        data_type = time_zone_suffix(p, data_type)?;
    }
    if matches!(site, Site::Alter) && data_type == DataType::Interval {
        let field = |p: &mut Parser| -> Option<i32> {
            for (word, mask) in [
                ("year", 4),
                ("month", 2),
                ("day", 8),
                ("hour", 1024),
                ("minute", 2048),
                ("second", 4096),
            ] {
                if p.word(word) {
                    return Some(mask);
                }
            }
            None
        };
        if let Some(first) = field(p) {
            let mask = if p.word("to") {
                let last = field(p).ok_or(SqlError::Unsupported)?;
                match (first, last) {
                    (4, 2) => 6,
                    (8, 1024) => 1032,
                    (8, 2048) => 3080,
                    (8, 4096) => 7176,
                    (1024, 2048) => 3072,
                    (1024, 4096) => 7168,
                    (2048, 4096) => 6144,
                    _ => return Err(SqlError::InvalidParameterValue),
                }
            } else {
                first
            };
            if typmod.is_some() {
                return Err(SqlError::Unsupported);
            }
            let precision = if p.sym('(') {
                let value = integer(p)?;
                p.punct(')')?;
                value
            } else {
                65535
            };
            let value = (mask << 16) | precision;
            typmod = Some(value);
        }
    }

    // A modifier belongs before WITH/WITHOUT TIME ZONE. Reject a second
    // suffix here. Arrays retain the lexer rejection shared by all aliases.
    if data_type.is_temporal()
        && (matches!(p.tokens.get(p.at), Some(Token::Symbol('(')))
            || matches!(p.tokens.get(p.at), Some(Token::Word(w)) if w == "with" || w == "without"))
    {
        return Err(SqlError::Syntax);
    }
    if p.sym('[') {
        p.punct(']')?;
        data_type = match data_type {
            DataType::Integer => DataType::IntArray,
            DataType::BigInt => DataType::BigIntArray,
            DataType::Text => DataType::TextArray,
            DataType::Uuid => DataType::UuidArray,
            _ => return Err(SqlError::Unsupported),
        };
    }
    if let Some(precision) = requested_precision.filter(|p| *p > 6) {
        let (base, zone) = match data_type {
            DataType::Timestamp => ("TIMESTAMP", ""),
            DataType::TimestampTz => ("TIMESTAMP", " WITH TIME ZONE"),
            DataType::Time => ("TIME", ""),
            _ => ("TIME", " WITH TIME ZONE"),
        };
        WARNINGS.with(|warnings| {
            warnings.borrow_mut().push(format!(
                "{base}({precision}){zone} precision reduced to maximum allowed, 6"
            ))
        });
    }
    Ok(Parsed {
        data_type,
        typmod,
        semantic_error,
    })
}
/// SQL-standard `WITH | WITHOUT TIME ZONE` after `time` or `timestamp` (and any precision).
/// This is the single resolution point for the multi-word temporal spellings; the cast
/// grammar (DD-0068 F19) is meant to share it, so the two cannot drift.
pub(super) fn time_zone_suffix(p: &mut Parser, base: DataType) -> R<DataType> {
    let with = p.word("with");
    if !with && !p.word("without") {
        return Ok(base);
    }
    p.need("time")?;
    p.need("zone")?;
    Ok(match (base, with) {
        (DataType::Time, true) => DataType::TimeTz,
        (DataType::Timestamp, true) => DataType::TimestampTz,
        (base, _) => base,
    })
}
fn integer(p: &mut Parser) -> R<i32> {
    match p.expr()? {
        Expr::Literal(Value::Int(n)) => n.try_into().map_err(|_| SqlError::InvalidParameterValue),
        _ => Err(SqlError::Syntax),
    }
}

// The expression parser uses Unsupported for absent or unsupported primaries;
// within a numeric modifier list these are malformed arguments.
fn numeric_integer(p: &mut Parser) -> R<i32> {
    integer(p).map_err(|error| match error {
        SqlError::Unsupported => SqlError::Syntax,
        error => error,
    })
}
