//! SQL and pgwire representation of PostgreSQL full-text search values.
use super::*;
use giggles_pg_tsearch::{Config, Dictionary, SearchResult, TsError, TsQuery, TsVector};
use std::cmp::Ordering;

impl DataType {
    pub fn is_text_search(&self) -> bool {
        matches!(self, Self::TsVector | Self::TsQuery)
    }

    pub fn text_search_oid(&self) -> Option<u32> {
        Some(match self {
            Self::TsVector => 3614,
            Self::TsQuery => 3615,
            Self::RegConfig => 3734,
            Self::RegDictionary => 3769,
            Self::Oid => 26,
            Self::PgChar => 18,
            Self::RealArray => 1021,
            Self::PgCharArray => 1002,
            Self::SmallIntArray => 1005,
            Self::RegDictionaryArray => 3770,
            _ => return None,
        })
    }

    pub(super) fn text_search_name(&self) -> Option<&'static str> {
        Some(match self {
            Self::TsVector => "tsvector",
            Self::TsQuery => "tsquery",
            Self::RegConfig => "regconfig",
            Self::RegDictionary => "regdictionary",
            Self::Oid => "oid",
            Self::PgChar => "\"char\"",
            Self::RealArray => "real[]",
            Self::PgCharArray => "\"char\"[]",
            Self::SmallIntArray => "smallint[]",
            Self::RegDictionaryArray => "regdictionary[]",
            _ => return None,
        })
    }
}

// Keep text-search errors field-less so the existing SqlError discriminants
// and serialized tags remain stable. These new errors are transient.
fn error_from_state(state: &str) -> SqlError {
    use SqlError::*;
    match state {
        "08P01" => TextSearchProtocolViolation,
        "0A000" => TextSearchUnsupported,
        "22003" => TextSearchNumericOutOfRange,
        "22004" => TextSearchNullValue,
        "2200F" => TextSearchZeroLength,
        "22021" => TextSearchInvalidEncoding,
        "22023" => TextSearchInvalidParameter,
        "2202E" => TextSearchInvalidSubscript,
        "22P02" => TextSearchInvalidText,
        "22P03" => TextSearchInvalidBinary,
        "3F000" => TextSearchInvalidSchema,
        "42601" => TextSearchSyntax,
        "42602" => TextSearchInvalidName,
        "42704" => TextSearchUndefinedObject,
        "42725" => TextSearchAmbiguousFunction,
        "42804" => TextSearchDatatypeMismatch,
        "42883" => TextSearchUndefinedFunction,
        "54000" => TextSearchProgramLimit,
        "54001" => TextSearchStatementTooComplex,
        _ => TextSearchInternal,
    }
}

/// Whether this error carries a PostgreSQL text-search diagnostic.
pub fn is_error(error: &SqlError) -> bool {
    matches!(
        error,
        SqlError::TextSearchInternal
            | SqlError::TextSearchProtocolViolation
            | SqlError::TextSearchUnsupported
            | SqlError::TextSearchNumericOutOfRange
            | SqlError::TextSearchNullValue
            | SqlError::TextSearchZeroLength
            | SqlError::TextSearchInvalidEncoding
            | SqlError::TextSearchInvalidParameter
            | SqlError::TextSearchInvalidSubscript
            | SqlError::TextSearchInvalidText
            | SqlError::TextSearchInvalidBinary
            | SqlError::TextSearchInvalidSchema
            | SqlError::TextSearchSyntax
            | SqlError::TextSearchInvalidName
            | SqlError::TextSearchUndefinedObject
            | SqlError::TextSearchAmbiguousFunction
            | SqlError::TextSearchDatatypeMismatch
            | SqlError::TextSearchUndefinedFunction
            | SqlError::TextSearchProgramLimit
            | SqlError::TextSearchStatementTooComplex
    )
}

pub(super) fn diagnostic(error: TsError) -> SqlError {
    let state = error_message(error.sqlstate, error.message);
    if let Some(detail) = error.detail {
        user_types::sql_error_detail(detail);
    }
    state
}

pub(super) fn error_message(state: &'static str, message: String) -> SqlError {
    user_types::view_replace_error(message);
    error_from_state(state)
}

pub(super) fn with_notices<T>(result: SearchResult<T>) -> T {
    for notice in result.notices {
        match notice.detail {
            Some(detail) => user_types::sql_notice_detail(notice.message, detail, notice.sqlstate),
            None => user_types::sql_notice(notice.message, notice.sqlstate),
        }
    }
    result.value
}

pub(super) fn vector(value: &Value) -> R<TsVector> {
    let Value::Bytes(bytes) = value else {
        return Err(SqlError::Type);
    };
    TsVector::from_storage(bytes).map_err(|_| SqlError::InternalError)
}

pub(super) fn query(value: &Value) -> R<TsQuery> {
    let Value::Bytes(bytes) = value else {
        return Err(SqlError::Type);
    };
    TsQuery::from_storage(bytes).map_err(|_| SqlError::InternalError)
}

pub(super) fn valid(ty: &DataType, value: &Value) -> bool {
    let Value::Bytes(bytes) = value else {
        return false;
    };
    match ty {
        DataType::TsVector => TsVector::from_storage(bytes).is_ok(),
        DataType::TsQuery => TsQuery::from_storage(bytes).is_ok(),
        _ => false,
    }
}

pub fn input(ty: &DataType, text: &str) -> R<Value> {
    Ok(match ty {
        DataType::TsVector => {
            Value::Bytes(text.parse::<TsVector>().map_err(diagnostic)?.to_storage())
        }
        DataType::TsQuery => {
            Value::Bytes(with_notices(TsQuery::parse(text).map_err(diagnostic)?).to_storage())
        }
        DataType::RegConfig => {
            Value::Int(reg_oid(text, |name| Config::lookup(name).map(Config::oid))?.into())
        }
        DataType::RegDictionary => {
            Value::Int(reg_oid(text, |name| Dictionary::lookup(name).map(Dictionary::oid))?.into())
        }
        DataType::Oid => Value::Int(
            text.trim_ascii()
                .parse::<u32>()
                .map_err(|_| SqlError::InvalidTextRepresentation)?
                .into(),
        ),
        DataType::PgChar => Value::Int(i64::from(text.bytes().next().unwrap_or(0) as i8)),
        _ => return Err(SqlError::Type),
    })
}

fn reg_oid(text: &str, lookup: impl FnOnce(&str) -> Result<u32, TsError>) -> R<u32> {
    if text == "-" {
        return Ok(0);
    }
    if text.as_bytes().first().is_some_and(u8::is_ascii_digit)
        && let Ok(oid) = text.trim_ascii_end().parse::<u32>()
    {
        return Ok(oid);
    }
    lookup(text).map_err(diagnostic)
}

pub fn output(ty: &DataType, value: &Value) -> R<String> {
    Ok(match (ty, value) {
        (DataType::TsVector, _) => vector(value)?.to_string(),
        (DataType::TsQuery, _) => query(value)?.to_string(),
        (DataType::RegConfig, Value::Int(oid)) => {
            reg_output(*oid, |oid| Config::from_oid(oid).map(Config::name))
        }
        (DataType::RegDictionary, Value::Int(oid)) => {
            reg_output(*oid, |oid| Dictionary::from_oid(oid).map(Dictionary::name))
        }
        (DataType::PgChar, Value::Int(0)) => String::new(),
        (DataType::PgChar, Value::Int(byte)) => {
            let byte = *byte as u8;
            if byte.is_ascii() {
                char::from(byte).to_string()
            } else {
                format!("\\{byte:03o}")
            }
        }
        (DataType::Oid, Value::Int(oid)) => oid.to_string(),
        _ => return Err(SqlError::Type),
    })
}

fn reg_output(oid: i64, lookup: impl FnOnce(u32) -> Option<&'static str>) -> String {
    match oid {
        0 => "-".into(),
        oid => lookup(oid as u32).map_or_else(|| oid.to_string(), str::to_owned),
    }
}

pub fn binary_input(ty: &DataType, bytes: &[u8]) -> R<Value> {
    Ok(match ty {
        DataType::TsVector => Value::Bytes(TsVector::recv(bytes).map_err(diagnostic)?.to_storage()),
        DataType::TsQuery => Value::Bytes(TsQuery::recv(bytes).map_err(diagnostic)?.to_storage()),
        DataType::RegConfig | DataType::RegDictionary | DataType::Oid => {
            let bytes = bytes
                .try_into()
                .map_err(|_| SqlError::TextSearchInvalidBinary)?;
            Value::Int(u32::from_be_bytes(bytes).into())
        }
        DataType::PgChar => {
            let [byte] = bytes else {
                return Err(SqlError::TextSearchInvalidBinary);
            };
            Value::Int(i64::from(*byte as i8))
        }
        _ => return Err(SqlError::Type),
    })
}

pub fn binary_output(ty: &DataType, value: &Value) -> R<Vec<u8>> {
    Ok(match (ty, value) {
        (DataType::TsVector, _) => vector(value)?.send(),
        (DataType::TsQuery, _) => query(value)?.send(),
        (DataType::RegConfig | DataType::RegDictionary | DataType::Oid, Value::Int(oid)) => {
            (*oid as u32).to_be_bytes().to_vec()
        }
        (DataType::PgChar, Value::Int(byte)) => vec![*byte as u8],
        _ => return Err(SqlError::Type),
    })
}

pub(super) fn compare(ty: &DataType, a: &Value, b: &Value) -> R<Ordering> {
    match ty {
        DataType::TsVector => Ok(vector(a)?.cmp(&vector(b)?)),
        DataType::TsQuery => Ok(query(a)?.cmp(&query(b)?)),
        _ => Err(SqlError::Type),
    }
}

enum DefaultConfig {
    Name(String),
    Resolved(Result<Config, TsError>),
}

thread_local! {
    static DEFAULT_CONFIG: std::cell::RefCell<DefaultConfig> =
        const { std::cell::RefCell::new(DefaultConfig::Resolved(Ok(Config::English))) };
}

pub(super) fn default_config() -> R<Config> {
    DEFAULT_CONFIG.with_borrow_mut(|setting| {
        if let DefaultConfig::Name(name) = setting {
            *setting = DefaultConfig::Resolved(Config::lookup(name));
        }
        let DefaultConfig::Resolved(result) = setting else { unreachable!() };
        result.clone().map_err(diagnostic)
    })
}

/// Resolve a connection's setting only if a function needs its configuration.
pub fn with_default_config<T>(name: String, execute: impl FnOnce() -> T) -> T {
    struct Restore(Option<DefaultConfig>);
    impl Drop for Restore {
        fn drop(&mut self) {
            if let Some(previous) = self.0.take() {
                DEFAULT_CONFIG.replace(previous);
            }
        }
    }
    let _restore = Restore(Some(DEFAULT_CONFIG.replace(DefaultConfig::Name(name))));
    execute()
}

/// Test a canonical name during GUC validation, independent of engine support.
pub fn is_builtin_config(name: &str) -> bool {
    Config::is_builtin(name)
}

pub(super) fn cast_supported(source: &DataType, target: &DataType) -> Option<bool> {
    use DataType::*;
    if source.text_search_oid().is_none() && target.text_search_oid().is_none() {
        return None;
    }
    Some(
        source == target
            || matches!(
                (source, target),
                (
                    Text | Varchar | Bpchar,
                    TsVector
                        | TsQuery
                        | RegConfig
                        | RegDictionary
                        | Oid
                        | PgChar
                        | RealArray
                        | PgCharArray
                        | SmallIntArray
                        | RegDictionaryArray
                ) | (
                    TsVector
                        | TsQuery
                        | RegConfig
                        | RegDictionary
                        | Oid
                        | PgChar
                        | RealArray
                        | PgCharArray
                        | SmallIntArray
                        | RegDictionaryArray,
                    Text | Varchar | Bpchar
                ) | (RegConfig | RegDictionary, Oid)
                    | (Oid, RegConfig | RegDictionary)
                    | (SmallInt | Integer | BigInt, Oid | PgChar)
                    | (Oid | PgChar, SmallInt | Integer | BigInt)
            ),
    )
}

pub(super) fn cast_typed(value: &Value, source: &DataType, target: &DataType) -> Option<R<Value>> {
    use DataType::*;
    if source == target || *value == Value::Null {
        return None;
    }
    match (source, target, value) {
        (
            TsVector | TsQuery | RegConfig | RegDictionary | Oid | PgChar,
            Text | Varchar | Bpchar,
            _,
        ) => Some(output(source, value).map(Value::Text)),
        (RegConfig | RegDictionary, Oid, _) | (Oid, RegConfig | RegDictionary, _) => {
            Some(Ok(value.clone()))
        }
        (SmallInt | Integer | BigInt, Oid, Value::Int(n)) => Some(
            u32::try_from(*n)
                .or_else(|e| i32::try_from(*n).map(|n| n as u32).map_err(|_| e))
                .map(|n| Value::Int(n.into()))
                .map_err(|_| SqlError::Overflow),
        ),
        (SmallInt | Integer | BigInt, PgChar, Value::Int(n)) => Some(
            i8::try_from(*n)
                .map(|n| Value::Int(n.into()))
                .map_err(|_| SqlError::Overflow),
        ),
        (Oid, Integer, Value::Int(n)) => Some(Ok(Value::Int((*n as i32).into()))),
        _ => None,
    }
}
