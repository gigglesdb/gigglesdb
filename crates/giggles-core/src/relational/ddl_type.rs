//! DDL type spellings and modifiers. Expression casts retain their own grammar.
use super::*;

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
pub(super) fn parse(p: &mut Parser, site: Site) -> R<Parsed> {
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
        "bytea" => DataType::Bytes,
        "bytes" if matches!(site, Site::Create) => DataType::Bytes,
        "date" => DataType::Date,
        "time" => DataType::Time,
        "timetz" => DataType::TimeTz,
        "timestamp" => DataType::Timestamp,
        "timestamptz" => DataType::TimestampTz,
        "interval" => DataType::Interval,
        _ => return Err(SqlError::Unsupported),
    };
    let temporal_suffix = matches!(word.as_str(), "time" | "timestamp");
    let character = matches!(data_type, DataType::Varchar | DataType::Bpchar);
    // CREATE accepts character and numeric modifiers.
    if matches!(site, Site::Create)
        && !character
        && data_type != DataType::Numeric
        && p.tokens.get(p.at) == Some(&Token::Symbol('('))
    {
        return Err(SqlError::Syntax);
    }
    let mut semantic_error = None;
    let mut typmod = if (matches!(site, Site::Alter) || character || data_type == DataType::Numeric)
        && p.sym('(')
    {
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
