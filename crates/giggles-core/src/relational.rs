//! Bounded trusted relational subset. This module grants no task authority.
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
mod alter_table;
mod comments;
mod views;
#[cfg(test)]
#[path = "relational/catalog_image_tests.rs"]
mod catalog_image_tests;
#[cfg(test)]
mod constraint_acceptance_tests;
mod constraints;
mod ctid;
pub(crate) use constraints::{DeferralState, Event as ConstraintEvent};
mod assign;
pub(crate) mod clock;
mod constraint_lookup;
mod constraint_primary_key;
mod ddl_type;
pub use ddl_type::take_sql_warnings;
mod user_types;
pub use user_types::{
    TypeDef, UserTypeCatalog, take_sql_error_message, take_sql_notices, type_oid,
};
mod defaults;
pub use clock::StatementClock;
#[allow(dead_code)]
pub(crate) mod catalog_ext;
mod catalog_presentation;
pub use catalog_presentation::{CatalogConstraint, CatalogPresentation, CatalogView};
use catalog_ext::*;
mod ident;
use ident::ident;
mod like;
mod numeric;
mod projector_inspection;
pub(crate) mod projector_reconstruction;
mod query;
pub use query::typed_read::{SingleSourceRead, ReadProjection, ReadExpr, ReadPredicate, ReadCompare};
use query::{TypmodSource, result_typmod};
pub(crate) mod cursor;
mod returning;
pub(crate) mod row_sink;
pub(crate) mod scan;
mod searched;
#[cfg(test)]
mod source_plumbing_tests;
use returning::Returning;
// Candidate codec/read APIs await later internal release.
#[allow(dead_code)]
pub(crate) mod secondary_index;
pub(crate) mod secondary_index_access;
mod secondary_index_adapter;
pub(crate) use secondary_index_adapter::{BatchRowChange, UpdateUnique};
mod secondary_index_catalog;
mod secondary_index_unique_readiness;
pub(crate) mod secondary_unique_lookup;
mod sql_copy;
mod sql_index;
mod sql_insert;
pub(crate) use giggles_pg_types::temporal::Time;
pub use giggles_pg_types::temporal::{
    Date, Interval, Time as PgTime, TimeTz, Timestamp, TimestampTz,
};
pub use giggles_pg_types::{Float4, Float8, Numeric};
pub use sql_copy::{Copy, CopyFormat};

pub const MAX_SQL: usize = 64 * 1024;
/// v2 SQL text budget. Flat token/VALUES storage is linear in this byte budget.
pub const V2_MAX_SQL: usize = 64 * 1024 * 1024;
#[cfg(feature = "engine-rocksdb")]
pub const MAX_STATEMENT_SQL: usize = V2_MAX_SQL;
#[cfg(not(feature = "engine-rocksdb"))]
pub const MAX_STATEMENT_SQL: usize = MAX_SQL;
/// INSERT uses flat tuples, never a recursively nested AST for the VALUES list.
pub const MAX_INSERT_TOKENS: usize = 1_000_000;
pub const MAX_ROWS: usize = 1024;
pub const MAX_ROW_BYTES: usize = 64 * 1024;
pub const MAX_COLUMNS: usize = 64;
pub const MAX_PARAMS: usize = 65_535;
pub const MAX_TABLES: usize = 128;
pub const MAX_RESULT_BYTES: usize = 1024 * 1024;
pub const MAX_TRANSACTIONS: usize = 8;

#[cfg(any(test, feature = "test-sql-parser-trap"))]
thread_local! {
    static SQL_PARSER_FORBIDDEN: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Test instrumentation for synchronous native-to-engine execution checks.
/// Forbids SQL parsing and public SQL session entry points, including execution
/// of handles prepared before this scope. Typed engine calls remain available.
/// Nested scopes and panics restore the thread's previous setting.
#[cfg(any(test, feature = "test-sql-parser-trap"))]
pub fn with_sql_parser_forbidden<T>(work: impl FnOnce() -> T) -> T {
    struct Restore(bool);
    impl Drop for Restore {
        fn drop(&mut self) {
            SQL_PARSER_FORBIDDEN.with(|forbidden| forbidden.set(self.0));
        }
    }
    let _restore = Restore(SQL_PARSER_FORBIDDEN.with(|forbidden| forbidden.replace(true)));
    work()
}

#[inline]
pub(crate) fn assert_sql_frontend_allowed() {
    #[cfg(any(test, feature = "test-sql-parser-trap"))]
    SQL_PARSER_FORBIDDEN.with(|forbidden| {
        assert!(
            !forbidden.get(),
            "native execution entered the SQL frontend"
        );
    });
}

#[cfg(test)]
mod heap_tests;

#[cfg(test)]
mod parser_trap_tests {
    use super::*;

    #[test]
    fn scoped_parser_trap_catches_sql_and_restores_after_nested_panics() {
        let parse = || Prepared::parse("BEGIN", 1, 0).unwrap();
        let trapped = std::panic::catch_unwind(|| {
            with_sql_parser_forbidden(|| {
                assert!(std::thread::spawn(parse).join().is_ok());
                assert!(std::panic::catch_unwind(|| with_sql_parser_forbidden(parse)).is_err());
                // Leaving the inner scope must not disable the outer guard.
                parse();
            });
        });
        assert!(trapped.is_err());
        parse();
    }
}

pub(crate) fn value_bytes(v: &Value) -> usize {
    match v {
        Value::Text(s) => s.len(),
        Value::Bytes(b) => b.len(),
        Value::Numeric(n) => n.to_string().len(),
        Value::Float4(_) => 4,
        Value::Float8(_) => 8,
        Value::Date(_) => 4,
        Value::Time(_) | Value::Timestamp(_) | Value::TimestampTz(_) => 8,
        Value::TimeTz(_) => 12,
        Value::Interval(_) => 16,
        Value::Null | Value::Bool(_) | Value::Int(_) => 9,
    }
}
fn payload_bound<'a>(values: impl Iterator<Item = &'a Value>) -> R<()> {
    let mut bytes = 0usize;
    for v in values {
        bytes = bytes.checked_add(value_bytes(v)).ok_or(SqlError::Limit)?;
        if bytes > MAX_ROW_BYTES {
            return Err(SqlError::Limit);
        }
    }
    Ok(())
}

// Per-row and per-key bounds still apply where rows and keys are formed.
fn params_bound<'a>(values: impl Iterator<Item = &'a Value>) -> R<()> {
    let mut bytes = 0usize;
    for v in values {
        bytes = bytes.checked_add(value_bytes(v)).ok_or(SqlError::Limit)?;
        if bytes > MAX_STATEMENT_SQL {
            return Err(SqlError::Limit);
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SqlError {
    Unsupported,
    Syntax,
    Reserved,
    UnknownTable,
    UnknownColumn,
    Duplicate,
    Type,
    Null,
    Parameters,
    Limit,
    Unbounded,
    Version,
    Conflict,
    StaleSchema,
    Transaction,
    Aborted,
    Overflow,
    UndefinedFunction,
    InvalidParameterValue,
    InvalidDatetimeFormat,
    DatetimeFieldOverflow,
    IntervalFieldOverflow,
    AmbiguousFunction,
    GroupingError,
    IndeterminateDatatype,
    AmbiguousParameter,
    InvalidTextRepresentation,
    SubstringError,
    CharacterNotInRepertoire,
    DuplicateTable,
    DuplicateColumn,
    DuplicateObject,
    UndefinedObject,
    CheckViolation,
    ForeignKeyViolation,
    DependentObjects,
    GeneratedAlways,
    ObjectNotInPrerequisiteState,
    SequenceLimit,
    NoActiveTransaction,
    InvalidSavepoint,
    InvalidForeignKey,
    InvalidSchema,
    DivisionByZero,
    InvalidEscapeSequence,
    InvalidTableDefinition,
    /// 23001 restrict_violation: a RESTRICT referential action rejected the change immediately.
    RestrictViolation,
    /// 42809 wrong_object_type: an operation names an object of a kind that does not support it
    /// (for example SET CONSTRAINTS on a non-deferrable constraint).
    WrongObjectType,
    /// 22004 null_value_not_allowed
    NullValueNotAllowed,
    /// 42846 cannot_coerce
    CannotCoerce,
    /// 22P04 bad_copy_file_format
    BadCopyFileFormat,
    /// 22001 string_data_right_truncation
    StringDataRightTruncation,
    /// 22009 invalid_time_zone_displacement_value
    InvalidTimeZoneDisplacementValue,
    /// 42702 ambiguous_column
    AmbiguousColumn,
    /// 2201W invalid_row_count_in_limit_clause
    InvalidRowCountInLimitClause,
    /// 2201X invalid_row_count_in_result_offset_clause
    InvalidRowCountInResultOffsetClause,
    /// 21000 cardinality_violation
    CardinalityViolation,
    /// 42P10 invalid_column_reference
    InvalidColumnReference,
}
type R<T> = Result<T, SqlError>;
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Text(String),
    Bytes(Vec<u8>),
    Numeric(#[serde(with = "crate::pg_serde::numeric")] Numeric),
    Float4(#[serde(with = "crate::pg_serde::real")] Float4),
    Float8(#[serde(with = "crate::pg_serde::double")] Float8),
    Date(#[serde(with = "crate::pg_serde::date")] Date),
    Time(#[serde(with = "crate::pg_serde::time")] Time),
    TimeTz(#[serde(with = "crate::pg_serde::timetz")] TimeTz),
    Timestamp(#[serde(with = "crate::pg_serde::timestamp")] Timestamp),
    TimestampTz(#[serde(with = "crate::pg_serde::timestamptz")] TimestampTz),
    Interval(#[serde(with = "crate::pg_serde::interval")] Interval),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataType {
    Boolean,
    SmallInt,
    Integer,
    BigInt,
    Text,
    Bytes,
    Numeric,
    Real,
    DoublePrecision,
    Date,
    Time,
    TimeTz,
    Timestamp,
    TimestampTz,
    Interval,
    Varchar,
    Bpchar,
    Uuid,
    Jsonb,
    IntArray,
    BigIntArray,
    TextArray,
}
impl Serialize for DataType {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let (index, name) = match self {
            Self::Boolean => (0, "Boolean"),
            Self::SmallInt => (1, "SmallInt"),
            Self::Integer => (2, "Integer"),
            Self::BigInt => (3, "BigInt"),
            Self::Text
            | Self::Varchar
            | Self::Bpchar
            | Self::Uuid
            | Self::Jsonb
            | Self::IntArray
            | Self::BigIntArray
            | Self::TextArray => (4, "Text"),
            Self::Bytes => (5, "Bytes"),
            Self::Numeric => (6, "Numeric"),
            Self::Real => (7, "Real"),
            Self::DoublePrecision => (8, "DoublePrecision"),
            Self::Date => (9, "Date"),
            Self::Time => (10, "Time"),
            Self::TimeTz => (11, "TimeTz"),
            Self::Timestamp => (12, "Timestamp"),
            Self::TimestampTz => (13, "TimestampTz"),
            Self::Interval => (14, "Interval"),
        };
        serializer.serialize_unit_variant("DataType", index, name)
    }
}
impl<'de> Deserialize<'de> for DataType {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(rename = "DataType")]
        enum LegacyDataType {
            Boolean,
            SmallInt,
            Integer,
            BigInt,
            Text,
            Bytes,
            Numeric,
            Real,
            DoublePrecision,
            Date,
            Time,
            TimeTz,
            Timestamp,
            TimestampTz,
            Interval,
        }
        Ok(match LegacyDataType::deserialize(deserializer)? {
            LegacyDataType::Boolean => Self::Boolean,
            LegacyDataType::SmallInt => Self::SmallInt,
            LegacyDataType::Integer => Self::Integer,
            LegacyDataType::BigInt => Self::BigInt,
            LegacyDataType::Text => Self::Text,
            LegacyDataType::Bytes => Self::Bytes,
            LegacyDataType::Numeric => Self::Numeric,
            LegacyDataType::Real => Self::Real,
            LegacyDataType::DoublePrecision => Self::DoublePrecision,
            LegacyDataType::Date => Self::Date,
            LegacyDataType::Time => Self::Time,
            LegacyDataType::TimeTz => Self::TimeTz,
            LegacyDataType::Timestamp => Self::Timestamp,
            LegacyDataType::TimestampTz => Self::TimestampTz,
            LegacyDataType::Interval => Self::Interval,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnDef {
    pub id: u64,
    pub name: String,
    pub data_type: DataType,
    pub not_null: bool,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableDef {
    pub id: u64,
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub primary_key: Vec<usize>,
}

pub const HIDDEN_ROWID: &str = "giggles_rowid";

impl TableDef {
    /// Recognize the persisted heap marker without trusting catalog offsets.
    pub fn hidden_rowid(&self) -> Option<usize> {
        let [index] = self.primary_key.as_slice() else {
            return None;
        };
        self.columns
            .get(*index)
            .filter(|column| {
                column.name == HIDDEN_ROWID
                    && column.data_type == DataType::BigInt
                    && column.not_null
            })
            .map(|_| *index)
    }

    pub fn logical_columns(&self) -> impl Iterator<Item = &ColumnDef> + '_ {
        let hidden = self.hidden_rowid();
        self.columns
            .iter()
            .enumerate()
            .filter(move |(index, _)| Some(*index) != hidden)
            .map(|(_, column)| column)
    }

    pub(crate) fn logical_offsets(&self) -> Vec<usize> {
        let hidden = self.hidden_rowid();
        (0..self.columns.len())
            .filter(|index| Some(*index) != hidden)
            .collect()
    }

    // Existing catalog names may bypass ident only for the exact heap marker.
    fn validate_column_ident(&self, index: usize) -> R<()> {
        if self.hidden_rowid() == Some(index) {
            Ok(())
        } else {
            ident(&self.columns[index].name)
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqlRow {
    pub values: Vec<Value>,
    pub version: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SqlResult {
    pub columns: Vec<String>,
    pub rows: Vec<SqlRow>,
    pub affected: u64,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Relational {
    // Base in-memory format is always 1; wire format is 2 iff ext is nonempty.
    format: u32,
    pub generation: u64,
    pub epoch: u64,
    next_id: u64,
    next_version: u64,
    pub tables: BTreeMap<String, TableDef>,
    pub(crate) view_names: BTreeMap<String, (u64, R<query::views::Projection>)>, // Runtime-only; absent from namespace-5.
    rows: BTreeMap<u64, BTreeMap<Vec<u8>, SqlRow>>,
    ext: CatalogExt,
    secondary: secondary_index::Sidecar,
}
impl Default for Relational {
    fn default() -> Self {
        Self {
            format: 1,
            generation: 0,
            epoch: 0,
            next_id: 1,
            next_version: 1,
            tables: BTreeMap::new(),
            view_names: BTreeMap::new(),
            rows: BTreeMap::new(),
            ext: CatalogExt::default(),
            secondary: secondary_index::Sidecar::default(),
        }
    }
}
impl Relational {
    /// Builds a rows-free v2 catalog image; row access must use RowSource/RowSink only.
    #[allow(dead_code)] // Awaiting v2 catalog integration.
    pub(crate) fn catalog_image(
        tables: BTreeMap<String, TableDef>,
        mut ext: CatalogExt,
        next_id: u64,
        next_version: u64,
    ) -> R<Relational> {
        Self::normalize_ext(&mut ext);
        let image = Self {
            rows: tables
                .values()
                .map(|table| (table.id, BTreeMap::new()))
                .collect(),
            tables,
            ext,
            next_id,
            next_version,
            ..Self::default()
        };
        image.validate()?;
        Ok(image)
    }
    /// Fork a validated rows-free catalog template for a mutable statement.
    /// The SQL bridge supplies the same reserved/derived allocator ceilings as
    /// catalog_image; metadata and cursor readers retain the immutable template.
    pub(crate) fn catalog_image_for_statement(&self, next_id: u64, next_version: u64) -> Self {
        let mut image = self.clone();
        image.next_id = next_id;
        image.next_version = next_version;
        image
    }
    pub(crate) fn catalog_indexes(&mut self, descriptors: &[secondary_index::Descriptor]) -> R<()> {
        self.secondary = self.secondary_build_complete_engine(descriptors, || {
            crate::cancellation::check().is_err()
        })?;
        Ok(())
    }
    #[allow(dead_code)] // Awaiting v2 catalog integration.
    pub(crate) fn catalog_parts(&self) -> (BTreeMap<String, TableDef>, CatalogExt, u64, u64) {
        (
            self.tables.clone(),
            self.ext.clone(),
            self.next_id,
            self.next_version,
        )
    }
}
fn allocate(n: &mut u64) -> R<u64> {
    let id = *n;
    *n = n.checked_add(1).ok_or(SqlError::Overflow)?;
    Ok(id)
}
pub(crate) fn check(v: &Value, c: &ColumnDef) -> R<()> {
    if *v == Value::Null {
        return if c.not_null {
            Err(SqlError::Null)
        } else {
            Ok(())
        };
    }
    let yes = match (&c.data_type, v) {
        (DataType::Date, Value::Date(_))
        | (DataType::Time, Value::Time(_))
        | (DataType::TimeTz, Value::TimeTz(_))
        | (DataType::Timestamp, Value::Timestamp(_))
        | (DataType::TimestampTz, Value::TimestampTz(_))
        | (DataType::Interval, Value::Interval(_))
        | (DataType::Numeric, Value::Numeric(_))
        | (DataType::Real, Value::Float4(_))
        | (DataType::DoublePrecision, Value::Float8(_))
        | (DataType::Boolean, Value::Bool(_))
        | (DataType::BigInt, Value::Int(_))
        | (
            DataType::Text
            | DataType::Varchar
            | DataType::Bpchar
            | DataType::Uuid
            | DataType::Jsonb
            | DataType::IntArray
            | DataType::BigIntArray
            | DataType::TextArray,
            Value::Text(_),
        )
        | (DataType::Bytes, Value::Bytes(_)) => true,
        (DataType::SmallInt, Value::Int(n)) => i16::try_from(*n).is_ok(),
        (DataType::Integer, Value::Int(n)) => i32::try_from(*n).is_ok(),
        _ => false,
    };
    if yes { Ok(()) } else { Err(SqlError::Type) }
}
fn integer_type(ty: &DataType) -> bool {
    matches!(
        ty,
        DataType::SmallInt | DataType::Integer | DataType::BigInt
    )
}

// Only explicit declarations admit assignment conversion or cross-width comparison.
fn infer_mutation_parameter(
    value: &Expr,
    target: &DataType,
    parameters: &mut [Option<DataType>],
    declared: &[Option<DataType>],
    assignment: bool,
) -> R<()> {
    let Expr::Param(n) = value else { return Ok(()) };
    let slot = parameters.get_mut(n - 1).ok_or(SqlError::Parameters)?;
    if let Some(Some(ty)) = declared.get(n - 1) {
        let compatible = if assignment {
            query::assignment::compatible(ty, target)
        } else {
            ty == target || (integer_type(ty) && integer_type(target))
        };
        if !compatible {
            return Err(if assignment {
                SqlError::Type
            } else {
                SqlError::UndefinedFunction
            });
        }
        *slot = Some(ty.clone());
    } else {
        if slot.as_ref().is_some_and(|old| old != target) {
            return Err(SqlError::Type);
        }
        *slot = Some(target.clone());
    }
    Ok(())
}
pub(crate) fn coerce_to_column(rel: &Relational, column: &ColumnDef, value: Value) -> R<Value> {
    let value = coerce_base_to_column(rel, column, value)?;
    user_types::coerce(rel, column, &value)?;
    Ok(value)
}
fn coerce_base_to_column(rel: &Relational, column: &ColumnDef, value: Value) -> R<Value> {
    if matches!(
        column.data_type,
        DataType::Time | DataType::TimeTz | DataType::Timestamp | DataType::TimestampTz
    ) {
        return defaults::apply_typmod(value, rel.column_typmod(column.id));
    }
    if column.data_type.is_extended_text() {
        return logical_types::coerce(value, &column.data_type);
    }
    use giggles_pg_types::character::{CharKind, CharTypmod, coerce_assignment};
    if column.data_type == DataType::Numeric {
        let Some(typmod) =
            giggles_pg_types::typmod::NumericTypmod::from_typmod(rel.column_typmod(column.id))
        else {
            return Ok(value);
        };
        return match value {
            Value::Numeric(value) => Ok(Value::Numeric(
                value.apply_typmod(typmod).map_err(|_| SqlError::Overflow)?,
            )),
            value => Ok(value),
        };
    }
    let kind = match column.data_type {
        DataType::Varchar => CharKind::Varchar,
        DataType::Bpchar => CharKind::Bpchar,
        _ => return Ok(value),
    };
    let Some(typmod) = CharTypmod::from_typmod(kind, rel.column_typmod(column.id)) else {
        return Ok(value);
    };
    match value {
        Value::Text(text) => Ok(Value::Text(
            coerce_assignment(&text, typmod)
                .map_err(|_| SqlError::StringDataRightTruncation)?
                .into_owned(),
        )),
        value => Ok(value),
    }
}
pub(crate) fn row_check(t: &TableDef, values: &[Value]) -> R<()> {
    if values.len() != t.columns.len() {
        return Err(SqlError::Type);
    }
    let mut size = 0usize;
    for (v, c) in values.iter().zip(&t.columns) {
        check(v, c)?;
        size = size.checked_add(value_bytes(v)).ok_or(SqlError::Limit)?;
    }
    if size > MAX_ROW_BYTES {
        return Err(SqlError::Limit);
    }
    Ok(())
}
/// Canonical memcomparable encoding v1: binary collation; NULL sorts first.
pub fn encode_key(values: &[Value]) -> R<Vec<u8>> {
    if values.len() > MAX_COLUMNS {
        return Err(SqlError::Limit);
    }
    payload_bound(values.iter())?;
    let mut out = Vec::new();
    for v in values {
        match v {
            Value::Date(_)
            | Value::Time(_)
            | Value::TimeTz(_)
            | Value::Timestamp(_)
            | Value::TimestampTz(_)
            | Value::Interval(_) => return Err(SqlError::Unsupported),
            Value::Numeric(n) => {
                out.push(5);
                numeric::key(n, &mut out);
            }
            Value::Float4(n) => {
                out.push(6);
                out.extend(numeric::float_key(n.value() as f64).to_be_bytes());
            }
            Value::Float8(n) => {
                out.push(7);
                out.extend(numeric::float_key(n.value()).to_be_bytes());
            }
            Value::Null => out.push(0),
            Value::Bool(b) => {
                out.push(1);
                out.push(u8::from(*b));
            }
            Value::Int(n) => {
                out.push(2);
                out.extend(((*n as u64) ^ (1 << 63)).to_be_bytes());
            }
            Value::Text(s) => {
                out.push(3);
                escaped(s.as_bytes(), &mut out);
            }
            Value::Bytes(b) => {
                out.push(4);
                escaped(b, &mut out);
            }
        }
        if out.len() > MAX_ROW_BYTES * 2 + MAX_COLUMNS * 3 {
            return Err(SqlError::Limit);
        }
    }
    Ok(out)
}
fn escaped(b: &[u8], out: &mut Vec<u8>) {
    for c in b {
        if *c == 0 {
            out.extend([0, 255]);
        } else {
            out.push(*c);
        }
    }
    out.extend([0, 0]);
}
fn pk(t: &TableDef, v: &[Value]) -> R<Vec<u8>> {
    encode_key(
        &t.primary_key
            .iter()
            .map(|i| v[*i].clone())
            .collect::<Vec<_>>(),
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Token {
    Word(String),
    QuotedIdent(String),
    String(String),
    Number(String),
    Param(usize),
    Symbol(char),
    Op(String),
}
fn lex(s: &str) -> R<Vec<Token>> {
    let mut out = Vec::new();
    lex_into(s, &mut out, false)?;
    Ok(out)
}
fn lex_into(s: &str, out: &mut Vec<Token>, routing: bool) -> R<()> {
    if s.len() > MAX_STATEMENT_SQL {
        return Err(SqlError::Limit);
    }
    let b = s.as_bytes();
    let mut i = 0;
    while i < b.len() {
        if b[i].is_ascii_whitespace() {
            i += 1;
            continue;
        }
        let c = b[i];
        if c == b'-' && b.get(i + 1) == Some(&b'-') {
            i += 2;
            while i < b.len() && b[i] != b'\n' && b[i] != b'\r' {
                i += 1;
            }
            continue;
        } else if c == b'/' && b.get(i + 1) == Some(&b'*') {
            i += 2;
            let mut depth = 1usize;
            while i < b.len() && depth != 0 {
                if b[i] == b'/' && b.get(i + 1) == Some(&b'*') {
                    depth += 1;
                    i += 2;
                } else if b[i] == b'*' && b.get(i + 1) == Some(&b'/') {
                    depth -= 1;
                    i += 2;
                } else {
                    i += 1;
                }
            }
            if depth != 0 {
                return Err(SqlError::Syntax);
            }
            continue;
        } else if c == b'\'' {
            i += 1;
            let mut v = Vec::new();
            let mut closed = false;
            while i < b.len() {
                if b[i] == b'\'' {
                    if b.get(i + 1) == Some(&b'\'') {
                        v.push(b'\'');
                        i += 2;
                    } else {
                        i += 1;
                        closed = true;
                        break;
                    }
                } else {
                    v.push(b[i]);
                    i += 1;
                }
            }
            if !closed {
                return Err(SqlError::Syntax);
            }
            if v.len() > MAX_SQL {
                return Err(SqlError::Limit);
            }
            out.push(Token::String(
                String::from_utf8(v).map_err(|_| SqlError::Syntax)?,
            ));
        } else if c == b'"' {
            i += 1;
            let mut v = Vec::new();
            let mut closed = false;
            while i < b.len() {
                if b[i] == 0 {
                    return Err(SqlError::Syntax);
                }
                if b[i] == b'"' {
                    if b.get(i + 1) == Some(&b'"') {
                        v.push(b'"');
                        i += 2;
                    } else {
                        i += 1;
                        closed = true;
                        break;
                    }
                } else {
                    v.push(b[i]);
                    i += 1;
                }
            }
            // Input, decoded allocation, total scan work and token count retain SQL bounds.
            if !closed || v.is_empty() {
                return Err(SqlError::Syntax);
            }
            out.push(Token::QuotedIdent(
                String::from_utf8(v).map_err(|_| SqlError::Syntax)?,
            ));
        } else if c.is_ascii_alphabetic() || c == b'_' || c >= 0x80 {
            let start = i;
            i += 1;
            while i < b.len()
                && (b[i].is_ascii_alphanumeric() || b[i] == b'_' || b[i] == b'$' || b[i] >= 0x80)
            {
                i += 1;
            }
            out.push(Token::Word(s[start..i].to_ascii_lowercase()));
        } else if c.is_ascii_digit() {
            let start = i;
            i += 1;
            while i < b.len() && b[i].is_ascii_digit() {
                i += 1;
            }
            if b.get(i) == Some(&b'.') {
                i += 1;
                while i < b.len() && b[i].is_ascii_digit() {
                    i += 1;
                }
            }
            if b.get(i).is_some_and(|c| *c == b'e' || *c == b'E') {
                i += 1;
                if b.get(i).is_some_and(|c| *c == b'+' || *c == b'-') {
                    i += 1;
                }
                while i < b.len() && b[i].is_ascii_digit() {
                    i += 1;
                }
            }
            out.push(Token::Number(s[start..i].into()));
        } else if c == b'$' {
            if b.get(i + 1) == Some(&b'$') {
                i += 2;
                let start = i;
                let end = s[start..].find("$$").ok_or(SqlError::Syntax)? + start;
                if end - start > MAX_SQL {
                    return Err(SqlError::Limit);
                }
                out.push(Token::String(s[start..end].into()));
                i = end + 2;
                if out.len() > token_limit(out, routing) {
                    return Err(SqlError::Limit);
                }
                continue;
            }
            i += 1;
            let start = i;
            while i < b.len() && b[i].is_ascii_digit() {
                i += 1;
            }
            let n = s[start..i]
                .parse::<usize>()
                .map_err(|_| SqlError::Parameters)?;
            if n == 0 || n > MAX_PARAMS {
                return Err(SqlError::Parameters);
            }
            out.push(Token::Param(n));
        } else if b"(),*;.:[]".contains(&c) {
            out.push(Token::Symbol(c as char));
            i += 1;
        } else if c == b'-' && b.get(i + 1) == Some(&b'>') {
            let start = i;
            i += 2;
            if b.get(i) == Some(&b'>') {
                i += 1;
            }
            out.push(Token::Op(s[start..i].into()));
        } else if b"+-/%".contains(&c) {
            out.push(Token::Op((c as char).to_string()));
            i += 1;
        } else if c == b'|' && b.get(i + 1) == Some(&b'|') {
            out.push(Token::Op("||".into()));
            i += 2;
        } else if b"=<>!".contains(&c) {
            if c == b'!' && b.get(i + 1) != Some(&b'=') {
                return Err(SqlError::Unsupported);
            }
            let start = i;
            i += 1;
            if b.get(i) == Some(&b'=') || (c == b'<' && b.get(i) == Some(&b'>')) {
                i += 1;
            }
            out.push(Token::Op(s[start..i].into()));
        } else {
            return Err(SqlError::Unsupported);
        }
        if out.len() > token_limit(out, routing) {
            return Err(SqlError::Limit);
        }
    }
    Ok(())
}
fn token_limit(tokens: &[Token], routing: bool) -> usize {
    if routing || matches!(tokens.first(), Some(Token::Word(word)) if word == "insert") {
        MAX_INSERT_TOKENS
    } else {
        4096
    }
}
#[derive(Debug, Clone)]
enum Expr {
    Literal(Value),
    Param(usize),
}
impl Expr {
    fn value(&self, p: &[Value]) -> R<Value> {
        match self {
            Self::Literal(v) => Ok(v.clone()),
            Self::Param(n) => p.get(n - 1).cloned().ok_or(SqlError::Parameters),
        }
    }
}
#[derive(Debug, Clone)]
struct Predicate {
    column: String,
    op: String,
    value: Expr,
}
#[derive(Debug, Clone)]
enum Statement {
    Schema(schemas::Ddl),
    Sequence(defaults::SequenceDdl),
    TypeDdl(user_types::Ddl),
    CompositeSelect(user_types::CompositeSelect),
    DropTable {
        names: Vec<String>,
        optional: bool,
        cascade: bool,
        resolution_error: Option<SqlError>,
    },
    Truncate {
        names: Vec<String>,
        restart: bool,
        cascade: bool,
    },
    SetConstraints(Vec<String>, bool),
    AlterTable(alter_table::AlterTable),
    Comment(comments::Comment),
    View(views::Ddl),
    CreateIndex(sql_index::CreateIndex),
    DropIndex(sql_index::DropIndex),
    InsertMulti(Box<sql_insert::Insert>),
    Query(query::Query),
    Create {
        name: String,
        optional: bool,
        columns: Vec<ColumnDef>,
        typmods: Vec<Option<i32>>,
        user_types: Vec<Option<String>>,
        defaults: Vec<Option<String>>,
        sequences: Vec<Option<defaults::SequenceSpec>>,
        keys: Vec<String>,
        constraints: Vec<constraints::Definition>,
        primary_name: Option<String>,
    },
    InsertExpressions(query::InsertExpressions),
    Insert {
        name: String,
        columns: Vec<String>,
        values: Vec<Expr>,
        returning: Option<Returning>,
    },
    Select {
        name: String,
        projection: Vec<String>,
        predicates: Vec<Predicate>,
        order: Vec<(String, bool)>,
        limit: Option<Expr>,
    },
    Update {
        name: String,
        sets: Vec<(String, Expr)>,
        predicates: Vec<Predicate>,
        search: Option<Box<query::Query>>,
        returning: Option<Returning>,
    },
    UpdateExpressions {
        name: String,
        sets: Vec<(String, assign::Value)>,
        predicates: Vec<Predicate>,
        search: Option<Box<query::Query>>,
        returning: Option<Returning>,
    },
    Delete {
        name: String,
        predicates: Vec<Predicate>,
        search: Option<Box<query::Query>>,
        returning: Option<Returning>,
    },
    Begin,
    Commit,
    Rollback,
    Savepoint(String),
    Release(String),
    RollbackTo(String),
}
mod schema_binding;
pub(crate) mod schemas;
mod truncate;
#[doc(hidden)]
pub use schema_binding::relation_key;
pub(crate) use schema_binding::split_key;
pub use schemas::{SchemaMetadata, schema_search_path};

struct Parser {
    tokens: Vec<Token>,
    at: usize,
    params: BTreeSet<usize>,
    relations: Vec<schemas::Reference>,
}
impl Parser {
    // Normalize mutation target aliases before any expression binder sees them.
    fn mutation_alias(&mut self, table: &str, update: bool) -> R<()> {
        let table = table.rsplit('\0').next().unwrap_or(table);
        let explicit = self.word("as");
        let alias = if explicit {
            Some(self.name()?)
        } else {
            match self.tokens.get(self.at) {
                Some(Token::QuotedIdent(_)) => Some(self.name()?),
                Some(Token::Word(w)) if !["set", "where", "returning"].contains(&w.as_str()) => {
                    Some(self.name()?)
                }
                _ => None,
            }
        };
        let Some(alias) = alias else {
            return Ok(());
        };
        fn ident(t: &Token) -> Option<&str> {
            match t {
                Token::Word(w) | Token::QuotedIdent(w) => Some(w.as_str()),
                _ => None,
            }
        }
        // Conservatively reject shadowing subqueries rather than changing their scope.
        for i in self.at..self.tokens.len().saturating_sub(1) {
            if !matches!(&self.tokens[i], Token::Word(w) if w == "from" || w == "join") {
                continue;
            }
            let mut end = i + 2;
            while self.tokens.get(end) == Some(&Token::Symbol('.')) {
                end += 2;
            }
            if matches!(self.tokens.get(end), Some(Token::Word(w)) if w == "as") {
                end += 1;
            }
            if ident(&self.tokens[i + 1]) == Some(alias.as_str())
                || self.tokens.get(end).and_then(ident) == Some(alias.as_str())
            {
                return Err(SqlError::Unsupported);
            }
        }
        let start = self.at + usize::from(update);
        for i in (start..self.tokens.len().saturating_sub(2)).rev() {
            if self.tokens[i + 1] != Token::Symbol('.') || ident(&self.tokens[i + 2]).is_none() {
                continue;
            }
            if ident(&self.tokens[i]) == Some(alias.as_str()) {
                self.tokens[i] = Token::QuotedIdent(table.into());
            } else if alias != table && ident(&self.tokens[i]) == Some(table) {
                return Err(SqlError::UnknownTable);
            }
        }
        Ok(())
    }
    fn word(&mut self, s: &str) -> bool {
        if self.tokens.get(self.at) == Some(&Token::Word(s.into())) {
            self.at += 1;
            true
        } else {
            false
        }
    }
    fn sym(&mut self, c: char) -> bool {
        if self.tokens.get(self.at) == Some(&Token::Symbol(c)) {
            self.at += 1;
            true
        } else {
            false
        }
    }
    fn need(&mut self, s: &str) -> R<()> {
        if self.word(s) {
            Ok(())
        } else {
            Err(SqlError::Syntax)
        }
    }
    fn punct(&mut self, c: char) -> R<()> {
        if self.sym(c) {
            Ok(())
        } else {
            Err(SqlError::Syntax)
        }
    }
    fn expr(&mut self) -> R<Expr> {
        let mut t = self.tokens.get(self.at).cloned().ok_or(SqlError::Syntax)?;
        self.at += 1;
        if let Token::Op(sign) = &t
            && (sign == "+" || sign == "-")
        {
            let Some(Token::Number(number)) = self.tokens.get(self.at) else {
                return Err(SqlError::Syntax);
            };
            t = Token::Number(format!("{sign}{number}"));
            self.at += 1;
        }
        Ok(match t {
            Token::Param(n) => {
                self.params.insert(n);
                Expr::Param(n)
            }
            Token::String(s) => Expr::Literal(Value::Text(s)),
            Token::Number(n) => Expr::Literal(match n.parse::<i64>() {
                Ok(n) => Value::Int(n),
                Err(_) => Value::Numeric(Numeric::parse(&n).map_err(|_| SqlError::Type)?),
            }),
            Token::Word(s) if s == "true" || s == "false" => {
                Expr::Literal(Value::Bool(s == "true"))
            }
            Token::Word(s) if s == "null" => Expr::Literal(Value::Null),
            _ => return Err(SqlError::Unsupported),
        })
    }
    fn optional_savepoint_keyword(&mut self) {
        // With no following identifier, SAVEPOINT itself is the name.
        if matches!(
            self.tokens.get(self.at + 1),
            Some(Token::Word(_) | Token::QuotedIdent(_))
        ) {
            self.word("savepoint");
        }
    }
    fn savepoint_name(&mut self) -> R<String> {
        let name = match self.tokens.get(self.at) {
            Some(Token::Word(name) | Token::QuotedIdent(name)) => name,
            _ => return Err(SqlError::Syntax),
        };
        if name.is_empty() || name.contains('\0') {
            return Err(SqlError::Syntax);
        }
        let name = ident::truncate(name, 63).to_owned();
        self.at += 1;
        Ok(name)
    }
    fn names(&mut self) -> R<Vec<String>> {
        self.punct('(')?;
        let mut v = vec![self.name()?];
        while self.sym(',') {
            v.push(self.name()?);
        }
        self.punct(')')?;
        Ok(v)
    }
    fn predicates(&mut self, table: &str) -> R<Vec<Predicate>> {
        let mut out = vec![];
        if !self.word("where") {
            return Ok(out);
        }
        loop {
            let (relation, column) = self.qualified_name()?;
            if relation.as_deref().is_some_and(|name| name != table) {
                return Err(SqlError::UnknownTable);
            }
            let Some(Token::Op(op)) = self.tokens.get(self.at).cloned() else {
                return Err(SqlError::Syntax);
            };
            self.at += 1;
            if !["=", "<", "<=", ">", ">="].contains(&op.as_str()) {
                return Err(SqlError::Unsupported);
            }
            let value = self.expr()?;
            out.push(Predicate { column, op, value });
            if !self.word("and") {
                break;
            }
        }
        Ok(out)
    }
    fn parse(&mut self) -> R<Statement> {
        if views::claims(&self.tokens) {
            return views::parse(self).map(Statement::View);
        }
        if self.word("comment") {
            return comments::parse(self).map(Statement::Comment);
        }
        if schemas::claims(&self.tokens) {
            return schemas::parse(self).map(Statement::Schema);
        }
        if user_types::claims(self) {
            return user_types::parse(self).map(Statement::TypeDdl);
        }
        if matches!(self.tokens.get(self.at), Some(Token::Word(word)) if matches!(word.as_str(), "create" | "alter" | "drop"))
            && matches!(self.tokens.get(self.at + 1), Some(Token::Word(word)) if word == "sequence")
        {
            return defaults::sequence_ddl(self).map(Statement::Sequence);
        }
        if matches!(self.tokens.get(self.at), Some(Token::Word(word)) if word == "create")
            && matches!(self.tokens.get(self.at + 1), Some(Token::Word(word)) if matches!(word.as_str(), "temp" | "temporary" | "unlogged"))
            && matches!(self.tokens.get(self.at + 2), Some(Token::Word(word)) if word == "sequence")
        {
            return Err(SqlError::Unsupported);
        }
        if matches!(self.tokens.get(self.at), Some(Token::Word(w)) if w == "alter") {
            return alter_table::parse(self).map(Statement::AlterTable);
        }
        if matches!(self.tokens.get(self.at), Some(Token::Word(word)) if word == "insert")
            && sql_insert::claims(&self.tokens)
        {
            return sql_insert::parse(self)
                .map(Box::new)
                .map(Statement::InsertMulti);
        }
        if matches!(self.tokens.get(self.at), Some(Token::Word(w)) if w == "create")
            && matches!(self.tokens.get(self.at + 1), Some(Token::Word(w)) if w == "index" || w == "unique")
        {
            return sql_index::parse(self).map(Statement::CreateIndex);
        }
        if matches!(self.tokens.get(self.at), Some(Token::Word(w)) if w == "drop")
            && matches!(self.tokens.get(self.at + 1), Some(Token::Word(w)) if w == "index")
        {
            return sql_index::parse_drop(self).map(Statement::DropIndex);
        }
        let st = if self.word("drop") {
            if !self.word("table") {
                return Err(SqlError::Unsupported);
            }
            let optional = if self.word("if") {
                self.need("exists")?;
                true
            } else {
                false
            };
            let mut names = Vec::new();
            let mut resolution_error = None;
            loop {
                let first = alter_table::raw_name(self)?;
                let (name, schema) = if self.sym('.') {
                    (alter_table::raw_name(self)?, Some(first))
                } else { (first, None) };
                resolution_error = resolution_error.or_else(|| ident::ident(&name).err());
                self.relations.push(schemas::Reference::read(name.clone(), schema.clone()));
                names.push(schema.map_or_else(|| name.clone(), |s| format!("{s}\0{name}")));
                if !self.sym(',') { break; }
            }
            let cascade = self.word("cascade");
            if !cascade {
                self.word("restrict");
            }
            Statement::DropTable {
                names,
                optional,
                cascade,
                resolution_error,
            }
        } else if self.word("set") {
            if !self.word("constraints") {
                return Err(SqlError::Unsupported);
            }
            let mut names = vec![];
            if !self.word("all") {
                names.push(self.name()?);
                while self.sym(',') {
                    names.push(self.name()?);
                }
            }
            let deferred = if self.word("deferred") {
                true
            } else {
                self.need("immediate")?;
                false
            };
            Statement::SetConstraints(names, deferred)
        } else if self.word("create") {
            self.need("table")?;
            let optional = if self.word("if") {
                self.need("not")?;
                self.need("exists")?;
                true
            } else {
                false
            };
            let name = self.create_table_name()?;
            self.punct('(')?;
            let mut columns = vec![];
            let mut defaults = vec![];
            let mut typmods = vec![];
            let mut user_types = vec![];
            let mut sequences = vec![];
            let mut keys = vec![];
            let mut constraints = vec![];
            let mut primary_name = None;
            loop {
                let constraint_name = if self.word("constraint") {
                    Some(self.name()?)
                } else {
                    None
                };
                if self.word("check") {
                    constraints.push(constraints::parse_check(self, constraint_name)?);
                } else if self.word("foreign") {
                    self.need("key")?;
                    let cols = self.names()?;
                    self.need("references")?;
                    constraints.push(constraints::parse_foreign(self, constraint_name, cols)?);
                } else if self.word("unique") {
                    constraints.push(constraints::Definition::Unique {
                        name: constraint_name,
                        columns: self.names()?,
                    });
                } else if self.word("primary") {
                    self.need("key")?;
                    if !keys.is_empty() {
                        return Err(SqlError::Duplicate);
                    }
                    keys = self.names()?;
                    primary_name = constraint_name;
                } else {
                    let name = self.name()?;
                    let serial = if self.word("smallserial") || self.word("serial2") {
                        Some(DataType::SmallInt)
                    } else if self.word("serial") || self.word("serial4") {
                        Some(DataType::Integer)
                    } else if self.word("bigserial") || self.word("serial8") {
                        Some(DataType::BigInt)
                    } else {
                        None
                    };
                    let mut user_type = None;
                    let parsed = if let Some(ty) = &serial {
                        ddl_type::Parsed {
                            data_type: ty.clone(),
                            typmod: None,
                            semantic_error: None,
                        }
                    } else {
                        let start = self.at;
                        match ddl_type::parse(self, ddl_type::Site::Create) {
                            Ok(parsed) => parsed,
                            Err(SqlError::Unsupported)
                                if self.at == start + 1
                                    && self.tokens.get(self.at).is_some_and(
                                        |token| match token {
                                            Token::Symbol(',' | ')' | '.') => true,
                                            Token::Word(w) => matches!(
                                                w.as_str(),
                                                "not"
                                                    | "null"
                                                    | "default"
                                                    | "constraint"
                                                    | "check"
                                                    | "unique"
                                                    | "references"
                                                    | "primary"
                                                    | "generated"
                                            ),
                                            _ => false,
                                        },
                                    ) =>
                            {
                                self.at = start;
                                user_type = Some(user_types::parse_name(self)?);
                                ddl_type::Parsed {
                                    data_type: DataType::Text,
                                    typmod: None,
                                    semantic_error: None,
                                }
                            }
                            Err(e) => return Err(e),
                        }
                    };
                    if let Some(error) = parsed.semantic_error {
                        return Err(error);
                    }
                    let data_type = parsed.data_type;
                    typmods.push(parsed.typmod);
                    user_types.push(user_type);
                    let mut not_null = serial.is_some();
                    let mut default = None;
                    let mut sequence = serial.map(|_| defaults::SequenceSpec::default());
                    loop {
                        let column_constraint = if self.word("constraint") {
                            Some(self.name()?)
                        } else {
                            None
                        };
                        if self.word("generated") {
                            if sequence.is_some() || default.is_some() {
                                return Err(SqlError::Syntax);
                            }
                            sequence = Some(defaults::identity(self)?);
                            not_null = true;
                        } else if self.word("default") {
                            if default.is_some() || sequence.is_some() {
                                return Err(SqlError::Syntax);
                            }
                            default = Some(defaults::parse(self, &data_type)?);
                        } else if self.word("check") {
                            constraints.push(constraints::parse_check(self, column_constraint)?);
                        } else if self.word("unique") {
                            constraints.push(constraints::Definition::Unique {
                                name: column_constraint,
                                columns: vec![name.clone()],
                            });
                        } else if self.word("references") {
                            constraints.push(constraints::parse_foreign(
                                self,
                                column_constraint,
                                vec![name.clone()],
                            )?);
                        } else if self.word("not") {
                            self.need("null")?;
                            not_null = true;
                        } else if self.word("null") {
                        } else if self.word("primary") {
                            self.need("key")?;
                            if !keys.is_empty() {
                                return Err(SqlError::Duplicate);
                            }
                            keys.push(name.clone());
                            primary_name = column_constraint;
                        } else {
                            break;
                        }
                    }
                    defaults.push(default);
                    sequences.push(sequence);
                    columns.push(ColumnDef {
                        id: 0,
                        name,
                        data_type,
                        not_null,
                    });
                }
                if !self.sym(',') {
                    break;
                }
            }
            self.punct(')')?;
            Statement::Create {
                name,
                optional,
                user_types,
                typmods,
                defaults,
                sequences,
                columns,
                keys,
                constraints,
                primary_name,
            }
        } else if self.word("insert") {
            self.need("into")?;
            let name = self.table_name()?;
            let columns = if self.tokens.get(self.at) == Some(&Token::Symbol('(')) {
                self.names()?
            } else {
                vec![]
            };
            self.need("values")?;
            self.punct('(')?;
            let mut values = vec![self.expr()?];
            while self.sym(',') {
                values.push(self.expr()?);
            }
            self.punct(')')?;
            let returning = Returning::parse(self, &name)?;
            Statement::Insert {
                name,
                columns,
                values,
                returning,
            }
        } else if self.word("update") {
            self.word("only");
            let name = self.table_name()?;
            self.sym('*');
            self.mutation_alias(&name, true)?;
            self.need("set")?;
            let mut sets = vec![];
            loop {
                let c = self.name()?;
                if self.tokens.get(self.at) != Some(&Token::Op("=".into())) {
                    return Err(SqlError::Syntax);
                }
                self.at += 1;
                sets.push((c, assign::Value::parse(self, &name)?));
                if !self.sym(',') {
                    break;
                }
            }
            let (predicates, search) = searched::parse_target(self, &name)?;
            let returning = Returning::parse(self, &name)?;
            if sets
                .iter()
                .all(|(_, value)| matches!(value, assign::Value::Plain(_)))
            {
                let sets = sets
                    .into_iter()
                    .map(|(name, value)| {
                        let assign::Value::Plain(value) = value else {
                            unreachable!()
                        };
                        (name, value)
                    })
                    .collect();
                Statement::Update {
                    name,
                    sets,
                    predicates,
                    search,
                    returning,
                }
            } else {
                Statement::UpdateExpressions {
                    name,
                    sets,
                    predicates,
                    search,
                    returning,
                }
            }
        } else if self.word("truncate") {
            self.word("table");
            let mut names = Vec::new();
            loop {
                self.word("only");
                names.push(self.table_name()?);
                self.sym('*');
                if !self.sym(',') {
                    break;
                }
            }
            let restart = if self.word("restart") {
                self.need("identity")?;
                true
            } else {
                if self.word("continue") {
                    self.need("identity")?;
                }
                false
            };
            let cascade = self.word("cascade");
            if !cascade {
                self.word("restrict");
            }
            Statement::Truncate {
                names,
                restart,
                cascade,
            }
        } else if self.word("delete") {
            self.need("from")?;
            let name = self.table_name()?;
            self.mutation_alias(&name, false)?;
            let (predicates, search) = searched::parse_target(self, &name)?;
            let returning = Returning::parse(self, &name)?;
            Statement::Delete {
                name,
                predicates,
                search,
                returning,
            }
        } else if self.word("begin") {
            Statement::Begin
        } else if self.word("start") {
            self.need("transaction")?;
            Statement::Begin
        } else if self.word("commit") {
            Statement::Commit
        } else if self.word("savepoint") {
            Statement::Savepoint(self.savepoint_name()?)
        } else if self.word("release") {
            self.optional_savepoint_keyword();
            Statement::Release(self.savepoint_name()?)
        } else if self.word("rollback") {
            let _ = self.word("work") || self.word("transaction");
            if self.word("to") {
                self.optional_savepoint_keyword();
                Statement::RollbackTo(self.savepoint_name()?)
            } else {
                Statement::Rollback
            }
        } else {
            return Err(SqlError::Unsupported);
        };
        self.sym(';');
        if self.at != self.tokens.len() {
            return Err(SqlError::Unsupported);
        }
        Ok(st)
    }
}
#[derive(Debug, Clone)]
pub struct Prepared {
    pub(crate) domain: u128,
    pub(crate) epoch: u64,
    pub(crate) transaction: Option<u128>,
    statement: Statement,
    unbound_statement: Option<Box<Statement>>,
    params: usize,
    parameter_types: Vec<Option<DataType>>,
    relations: Vec<schemas::Reference>,
    search_path: Vec<String>,
    schema_user: String,
    warnings: Vec<String>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)] // Awaiting v2 dispatch integration.
pub(crate) enum V2Class {
    Control(&'static str),
    Ddl,
    Dml,
    Read,
    Refused(&'static str),
}
#[derive(Debug, Clone)]
pub struct Description {
    /// Optional result OID overrides; storage types remain unchanged.
    pub type_oids: Vec<u32>,
    pub parameters: Vec<DataType>,
    pub columns: Vec<ColumnDef>,
    pub typmods: Vec<i32>,
}

/// Native ordered access compiled through the same binder and transaction path.
#[derive(Debug, Clone)]
pub struct Scan {
    pub table: String,
    pub projection: Vec<String>,
    pub prefix: Vec<Value>,
    pub lower: Option<(Value, bool)>,
    pub upper: Option<(Value, bool)>,
    pub reverse: bool,
    pub limit: usize,
}
/// Shared CREATE definition checks. SQL keeps its own nullability policy;
/// engine catalog intents additionally reject explicitly nullable key fields.
pub(crate) fn validate_create_definition(
    name: &str,
    columns: &[ColumnDef],
    keys: &[String],
) -> R<()> {
    ident(name)?;
    if columns.is_empty() || columns.len() > MAX_COLUMNS {
        return Err(SqlError::Limit);
    }
    if keys.is_empty() && columns.len() == MAX_COLUMNS {
        return Err(SqlError::Limit);
    }
    unique(columns.iter().map(|column| &column.name))?;
    unique(keys)?;
    for column in columns {
        ident(&column.name)?;
    }
    for key in keys {
        ident(key)?;
        if columns.iter().any(|column| {
            column.name == *key
                && (column.data_type.is_temporal() || column.data_type == DataType::Bpchar)
        }) {
            return Err(SqlError::Unsupported);
        }
        if !columns.iter().any(|column| column.name == *key) {
            return Err(SqlError::UnknownColumn);
        }
    }
    Ok(())
}
impl Prepared {
    #[allow(dead_code)] // Awaiting v2 dispatch integration.
    pub(crate) fn v2_class(&self) -> V2Class {
        match &self.statement {
            Statement::Begin => V2Class::Control("begin"),
            Statement::Commit => V2Class::Control("commit"),
            Statement::Rollback => V2Class::Control("rollback"),
            Statement::Savepoint(_) => V2Class::Control("savepoint"),
            Statement::Release(_) => V2Class::Control("release"),
            Statement::RollbackTo(_) => V2Class::Control("rollback_to"),
            Statement::CompositeSelect(_) => V2Class::Read,
            Statement::Select { .. } | Statement::Query(_) => V2Class::Read,
            // ON CONFLICT's unique-index dependency requires catalog/private conflict data.
            Statement::InsertMulti(_)
            | Statement::InsertExpressions(_)
            | Statement::Insert { .. }
            | Statement::Update { .. }
            | Statement::UpdateExpressions { .. }
            | Statement::Delete { .. } => V2Class::Dml,
            Statement::Truncate { .. } | Statement::Schema(_) | Statement::TypeDdl(_) => {
                V2Class::Ddl
            }
            Statement::Sequence(_) | Statement::DropTable { .. } | Statement::Comment(_) | Statement::View(_) => V2Class::Ddl,
            Statement::Create { .. } => V2Class::Ddl,
            Statement::CreateIndex(_) | Statement::DropIndex(_) => V2Class::Ddl,
            Statement::AlterTable(alter) => {
                // v2 supports every ALTER action. The exhaustive match makes any
                // newly added action fail to compile until it is classified here.
                for action in &alter.actions {
                    match action {
                        alter_table::Action::RenameColumn(..)
                        | alter_table::Action::RenameTable(_)
                        | alter_table::Action::Default(..)
                        | alter_table::Action::NotNull(_, false) => (),
                        alter_table::Action::Add(..)
                        | alter_table::Action::Drop(..)
                        | alter_table::Action::NotNull(_, true) => (),
                        alter_table::Action::Unique(..)
                        | alter_table::Action::Constraint(constraints::Definition::Unique {
                            ..
                        })
                        | alter_table::Action::PrimaryKey(..) => (),
                        alter_table::Action::Constraint(
                            constraints::Definition::Check { .. }
                            | constraints::Definition::ForeignKey { .. },
                        )
                        | alter_table::Action::Validate(_)
                        | alter_table::Action::DropConstraint(..) => (),
                    }
                }
                V2Class::Ddl
            }
            Statement::SetConstraints(..) => V2Class::Dml,
        }
    }

    /// DD-0068 F9 (GD-0001 Q8): table-free SELECT whose only non-table-free parts are deferred undefined / unknown-schema routine calls. Classification only; grants nothing by itself.
    pub fn deferred_routine_only(&self) -> bool {
        matches!(&self.statement, Statement::Query(query) if query.deferred_routine_only())
    }
    fn engine(statement: Statement, domain: u128, epoch: u64) -> Self {
        Self {
            domain,
            epoch,
            transaction: None,
            statement,
            params: 0,
            parameter_types: vec![],
            relations: Vec::new(),
            search_path: vec!["public".into()],
            schema_user: "giggles".into(),
            unbound_statement: None,
            warnings: vec![],
        }
    }
    /// Construct an unbound read; callers still perform their normal authorization.
    pub fn from_single_source_read(domain: u128, epoch: u64, read: SingleSourceRead) -> R<Self> {
        let table = read.table.clone();
        let mut prepared = Self::engine(Statement::Query(query::Query::from_single_source_read(read)?), domain, epoch);
        prepared.relations.push(schemas::Reference::read(table, None));
        Ok(prepared)
    }
    pub(crate) fn create_engine(
        name: String,
        columns: Vec<ColumnDef>,
        keys: Vec<String>,
        domain: u128,
        state: &Relational,
    ) -> R<Self> {
        // These names did not pass through a text parser. Require canonical
        // spelling before applying the shared reserved/definition checks.
        let native_name = |value: &str| {
            value
                .as_bytes()
                .first()
                .is_some_and(u8::is_ascii_alphabetic)
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
        };
        if !native_name(&name)
            || columns.iter().any(|column| !native_name(&column.name))
            || keys.iter().any(|key| !native_name(key))
        {
            return Err(SqlError::Syntax);
        }
        if columns.iter().any(|column| column.id != 0) {
            return Err(SqlError::Type);
        }
        if keys.len() > MAX_COLUMNS {
            return Err(SqlError::Limit);
        }
        if keys.is_empty() {
            // Preserve the native CREATE error and its original precedence.
            ident(&name)?;
            if columns.is_empty() || columns.len() > MAX_COLUMNS {
                return Err(SqlError::Limit);
            }
            return Err(SqlError::Unbounded);
        }
        validate_create_definition(&name, &columns, &keys)?;
        for key in &keys {
            if !columns
                .iter()
                .any(|column| column.name == *key && column.not_null)
            {
                return Err(SqlError::Null);
            }
        }
        if state.tables.contains_key(&name) {
            return Err(SqlError::Duplicate);
        }
        if state.tables.len() >= MAX_TABLES {
            return Err(SqlError::Limit);
        }
        Ok(Self::engine(
            Statement::Create {
                name,
                optional: false,
                user_types: vec![None; columns.len()],
                typmods: vec![None; columns.len()],
                defaults: vec![None; columns.len()],
                sequences: vec![None; columns.len()],
                columns,
                keys,
                constraints: vec![],
                primary_name: None,
            },
            domain,
            state.epoch,
        ))
    }
    pub(crate) fn begin_engine(domain: u128, epoch: u64) -> Self {
        Self::engine(Statement::Begin, domain, epoch)
    }
    pub(crate) fn commit_engine(domain: u128, epoch: u64) -> Self {
        Self::engine(Statement::Commit, domain, epoch)
    }
    pub(crate) fn rollback_engine(domain: u128, epoch: u64) -> Self {
        Self::engine(Statement::Rollback, domain, epoch)
    }
    fn engine_table(table: &TableDef, state: &Relational) -> R<()> {
        // Callers cannot smuggle stale IDs, column types or unchecked key indexes
        // into the executor through a hand-built descriptor.
        if state.tables.get(&state.table_key(table)) != Some(table) {
            return Err(SqlError::StaleSchema);
        }
        Ok(())
    }
    fn engine_key(table: &TableDef, key: Vec<Value>) -> R<Vec<Predicate>> {
        if key.len() != table.primary_key.len() {
            return Err(SqlError::Unbounded);
        }
        payload_bound(key.iter())?;
        key.into_iter()
            .zip(&table.primary_key)
            .map(|(value, index)| {
                let column = &table.columns[*index];
                check(&value, column)?;
                Ok(Predicate {
                    column: column.name.clone(),
                    op: "=".into(),
                    value: Expr::Literal(value),
                })
            })
            .collect()
    }
    pub(crate) fn insert_engine(
        table: &TableDef,
        values: Vec<Value>,
        domain: u128,
        state: &Relational,
    ) -> R<Self> {
        if table.hidden_rowid().is_some() {
            return Err(SqlError::Unsupported);
        }
        Relational::secondary_admit_values(values.iter())?;
        Self::engine_table(table, state)?;
        let values = values
            .into_iter()
            .zip(&table.columns)
            .map(|(value, column)| coerce_to_column(state, column, value))
            .collect::<R<Vec<_>>>()?;
        row_check(table, &values)?;
        Ok(Self::engine(
            Statement::Insert {
                name: table.name.clone(),
                columns: vec![],
                values: values.into_iter().map(Expr::Literal).collect(),
                returning: None,
            },
            domain,
            state.epoch,
        ))
    }
    pub(crate) fn update_engine(
        table: &TableDef,
        key: Vec<Value>,
        sets: Vec<(usize, Value)>,
        domain: u128,
        state: &Relational,
    ) -> R<Self> {
        Self::engine_table(table, state)?;
        if table.hidden_rowid().is_some() {
            return Err(SqlError::Unsupported);
        }
        if sets.is_empty() {
            return Err(SqlError::Unsupported);
        }
        if sets.len() > MAX_COLUMNS {
            return Err(SqlError::Limit);
        }
        if key.len() != table.primary_key.len() {
            return Err(SqlError::Unbounded);
        }
        Relational::secondary_admit_values(key.iter().chain(sets.iter().map(|(_, value)| value)))?;
        payload_bound(key.iter().chain(sets.iter().map(|(_, value)| value)))?;
        let predicates = Self::engine_key(table, key)?;
        unique(sets.iter().map(|(index, _)| index))?;
        let sets = sets
            .into_iter()
            .map(|(index, value)| {
                let column = table.columns.get(index).ok_or(SqlError::UnknownColumn)?;
                if table.primary_key.contains(&index) {
                    return Err(SqlError::Unsupported);
                }
                let value = coerce_to_column(state, column, value)?;
                check(&value, column)?;
                Ok((column.name.clone(), Expr::Literal(value)))
            })
            .collect::<R<Vec<_>>>()?;
        Ok(Self::engine(
            Statement::Update {
                name: table.name.clone(),
                sets,
                predicates,
                search: None,
                returning: None,
            },
            domain,
            state.epoch,
        ))
    }
    pub(crate) fn delete_engine(
        table: &TableDef,
        key: Vec<Value>,
        domain: u128,
        state: &Relational,
    ) -> R<Self> {
        Self::engine_table(table, state)?;
        if table.hidden_rowid().is_some() {
            return Err(SqlError::Unsupported);
        }
        let predicates = Self::engine_key(table, key)?;
        Ok(Self::engine(
            Statement::Delete {
                name: table.name.clone(),
                predicates,
                search: None,
                returning: None,
            },
            domain,
            state.epoch,
        ))
    }
    pub(crate) fn validate_immutable_constants(&self, state: &Relational) -> R<()> {
        let bound = self.bind_schemas(state)?;
        let this = &bound;
        if this.epoch != state.epoch {
            return Err(SqlError::StaleSchema);
        }
        if let Statement::InsertExpressions(values) = &this.statement {
            return returning::validate_insert_constants(
                values,
                state,
                this.params,
                &this.parameter_types,
            );
        }
        if let Statement::Query(q) = &this.statement {
            return q
                .bind(state, this.params, &this.parameter_types)?
                .validate_immutable_constants();
        }
        Ok(())
    }
    pub(crate) fn describe(&self, state: &Relational) -> R<Description> {
        let bound = self.bind_schemas(state)?;
        let this = &bound;
        this.validate_schemas(state)?;
        if this.epoch != state.epoch {
            return Err(SqlError::StaleSchema);
        }
        if assign::has_duplicates(self) {
            assign::check_duplicate_literals(self, state)?;
            this.describe_inner(state)?;
            return Err(SqlError::Syntax);
        }
        if let Statement::Create { user_types, .. } = &this.statement {
            for name in user_types.iter().flatten() {
                state
                    .resolve_type(name, &this.search_path)?
                    .ok_or(SqlError::UndefinedObject)?
                    .1
                    .storage_type()?;
            }
        }
        let mut description = this.describe_inner(state)?;
        if description.type_oids.is_empty() {
            description.type_oids = vec![0; description.columns.len()];
        }
        for (oid, column) in description.type_oids.iter_mut().zip(&description.columns) {
            if *oid == 0
                && let Some(id) = state.ext.columns.get(&column.id).and_then(|c| c.type_id)
                && matches!(state.ext.types.get(&id), Some(TypeDef::Enum { .. }))
            {
                *oid = type_oid(id)?;
            }
        }
        Ok(description)
    }
    fn describe_inner(&self, state: &Relational) -> R<Description> {
        if matches!(self.command(), "INSERT" | "UPDATE" | "DELETE") && self.table().is_some_and(|n| state.view_names.contains_key(n)) { return Err(SqlError::Unsupported); }
        if matches!(self.statement, Statement::Truncate { .. }) {
            self.truncate_authority_tables(state)?;
            return Ok(Description {
                parameters: vec![],
                columns: vec![],
                type_oids: vec![],
                typmods: vec![],
            });
        }
        if self.epoch != state.epoch {
            return Err(SqlError::StaleSchema);
        }
        if let Statement::CompositeSelect(select) = &self.statement {
            return select.describe(state, self.params, &self.search_path);
        }
        if let Statement::AlterTable(alter) = &self.statement {
            if let Some(error) = &alter.resolution_error {
                return Err(error.clone());
            }
            state
                .tables
                .get(&alter.table)
                .ok_or(SqlError::UnknownTable)?;
        }
        if let Statement::InsertMulti(insert) = &self.statement {
            return insert.describe(state, self.params, &self.parameter_types);
        }
        if let Statement::InsertExpressions(values) = &self.statement {
            return returning::describe_insert(values, state, self.params, &self.parameter_types);
        }
        if let Statement::Query(q) = &self.statement {
            if !q.is_table_free() && state.sequence_by_name(q.target()).is_some() {
                return Err(SqlError::Unsupported);
            }
            return q
                .bind_description(state, self.params, &self.parameter_types)
                .map(|b| b.description(state));
        }
        if searched::target(self).is_some() {
            return searched::describe(self, state);
        }
        if let Statement::UpdateExpressions {
            name,
            sets,
            predicates,
            returning,
            ..
        } = &self.statement
        {
            return assign::describe(
                name,
                sets,
                predicates,
                returning.as_ref(),
                state,
                self.params,
                &self.parameter_types,
            );
        }
        let mut params = vec![None; self.params];
        if self.parameter_types.len() > params.len() {
            return Err(SqlError::Parameters);
        }
        params[..self.parameter_types.len()].clone_from_slice(&self.parameter_types);
        let mut columns = vec![];
        if let Some(name) = self.table() {
            if self.is_ddl() {
                return Err(SqlError::Unsupported);
            }
            let t = state.tables.get(name).ok_or(SqlError::UnknownTable)?;
            let mut bind = |e: &Expr, ty: &DataType| -> R<()> {
                if let Expr::Param(n) = e {
                    let slot = params.get_mut(n - 1).ok_or(SqlError::Parameters)?;
                    if slot.as_ref().is_some_and(|old| old != ty) {
                        return Err(SqlError::Type);
                    }
                    *slot = Some(ty.clone());
                }
                Ok(())
            };
            match &self.statement {
                Statement::Insert {
                    columns: names,
                    values,
                    ..
                } => {
                    let indexes = if names.is_empty() {
                        t.logical_offsets()
                    } else {
                        names.iter().map(|n| col(t, n)).collect::<R<Vec<_>>>()?
                    };
                    if indexes.len() != values.len() {
                        return Err(SqlError::Type);
                    }
                    for (i, e) in indexes.into_iter().zip(values) {
                        infer_mutation_parameter(
                            e,
                            &t.columns[i].data_type,
                            &mut params,
                            &self.parameter_types,
                            true,
                        )?;
                    }
                }
                Statement::Select {
                    projection,
                    predicates,
                    limit,
                    ..
                } => {
                    if t.hidden_rowid().is_some() {
                        return Err(SqlError::Unsupported);
                    }
                    columns = if projection.is_empty() {
                        t.columns.clone()
                    } else {
                        projection
                            .iter()
                            .map(|n| Ok(t.columns[col(t, n)?].clone()))
                            .collect::<R<Vec<_>>>()?
                    };
                    for p in predicates {
                        bind(&p.value, &t.columns[col(t, &p.column)?].data_type)?;
                    }
                    if let Some(e) = limit {
                        bind(e, &DataType::BigInt)?;
                    }
                }
                Statement::Update {
                    sets, predicates, ..
                } => {
                    for (n, e) in sets {
                        infer_mutation_parameter(
                            e,
                            &t.columns[col(t, n)?].data_type,
                            &mut params,
                            &self.parameter_types,
                            true,
                        )?;
                    }
                    for p in predicates {
                        infer_mutation_parameter(
                            &p.value,
                            &t.columns[col(t, &p.column)?].data_type,
                            &mut params,
                            &self.parameter_types,
                            false,
                        )?;
                    }
                }
                Statement::Delete { predicates, .. } => {
                    for p in predicates {
                        infer_mutation_parameter(
                            &p.value,
                            &t.columns[col(t, &p.column)?].data_type,
                            &mut params,
                            &self.parameter_types,
                            false,
                        )?;
                    }
                }
                _ => {}
            }
        }
        if let Some(returning) = self.returning() {
            let (columns, parameters, typmods) = returning.describe(state, params)?;
            return Ok(Description {
                type_oids: vec![],
                typmods,
                columns,
                parameters,
            });
        }
        Ok(Description {
            type_oids: vec![],
            parameters: params
                .into_iter()
                .map(|t| t.ok_or(SqlError::Parameters))
                .collect::<R<Vec<_>>>()?,
            typmods: columns
                .iter()
                .map(|c| result_typmod(state, TypmodSource::Column(c)))
                .collect(),
            columns,
        })
    }
    pub(crate) fn v2_index_predicates(&self, params: &[Value]) -> Vec<(String, String, Value)> {
        match &self.statement {
            Statement::Query(query) => query.v2_index_predicates(params),
            _ => Vec::new(),
        }
    }
    /// Parsed table target for gateway authorization. Controls have no target.
    pub fn table(&self) -> Option<&str> {
        match &self.statement {
            Statement::View(ddl) => ddl.names.first().map(String::as_str),
            Statement::Truncate { names, .. } | Statement::DropTable { names, .. } => names.first().map(String::as_str),
            Statement::Query(q) => {
                if q.is_table_free() {
                    None
                } else {
                    Some(q.target())
                }
            }
            Statement::InsertExpressions(values) => Some(&values.name),
            Statement::AlterTable(alter) => Some(&alter.table),
            Statement::Comment(comment) => Some(&comment.table),
            Statement::CreateIndex(index) => Some(&index.table),
            Statement::DropIndex(index) => index.table.as_deref(),
            Statement::InsertMulti(insert) => Some(&insert.table),
            Statement::Create { name, .. }
            | Statement::Insert { name, .. }
            | Statement::Select { name, .. }
            | Statement::Update { name, .. }
            | Statement::UpdateExpressions { name, .. }
            | Statement::Delete { name, .. } => Some(name),
            _ => None,
        }
    }
    /// Tables whose rows are read by this statement, in first-seen order.
    /// Mutation targets are included only when an IN subquery reads them.
    pub fn read_tables(&self) -> Vec<String> {
        match &self.statement {
            Statement::View(ddl) => ddl.query.as_ref().map_or_else(Vec::new, query::Query::read_tables),
            Statement::Query(query) => query.read_tables(),
            Statement::InsertMulti(insert) => insert
                .source
                .as_ref()
                .map_or_else(Vec::new, |source| source.read_tables()),
            Statement::Select { name, .. } => vec![name.clone()],
            _ => searched::target(self).map_or_else(Vec::new, query::Query::subquery_tables),
        }
    }

    /// Complete syntactic reads, collected before gateway classification or binding.
    pub fn authorization_tables(&self) -> R<Vec<String>> {
        let query = match &self.statement {
            Statement::View(ddl) => ddl.query.as_ref(),
            Statement::Query(query) => Some(query),
            Statement::InsertMulti(insert) => insert.source.as_deref(),
            _ => searched::target(self).filter(|query| query.has_in_subquery()),
        };
        match query {
            Some(query) => {
                let mut tables = query.authorization_tables()?;
                if searched::target(self).is_some() {
                    // The target keeps its command privilege; child reads need SELECT.
                    let children = query.subquery_tables();
                    tables.retain(|table| children.contains(table));
                }
                Ok(tables)
            }
            None => Ok(self.read_tables()),
        }
    }

    pub fn is_ddl(&self) -> bool {
        matches!(
            self.statement,
            Statement::Schema(_)
                | Statement::Sequence(_)
                | Statement::TypeDdl(_)
                | Statement::Create { .. }
                | Statement::CreateIndex(_)
                | Statement::DropIndex(_)
                | Statement::AlterTable(_)
                | Statement::Comment(_)
                | Statement::View(_)
                | Statement::DropTable { .. }
        )
    }
    pub fn drop_tables(&self) -> &[String] {
        match &self.statement { Statement::DropTable { names, .. } => names, Statement::View(ddl) if ddl.query.is_none() => &ddl.names, _ => &[] }
    }
    pub fn is_drop_table(&self) -> bool {
        matches!(self.statement, Statement::DropTable { .. }) || matches!(&self.statement, Statement::View(ddl) if ddl.query.is_none())
    }
    pub fn is_alter_table(&self) -> bool {
        matches!(self.statement, Statement::AlterTable(_))
    }
    pub fn table_rename(&self) -> Option<(&str, &str)> {
        if let Statement::AlterTable(alter) = &self.statement {
            alter.actions.iter().rev().find_map(|action| match action {
                alter_table::Action::RenameTable(name) => {
                    Some((alter.table.as_str(), name.as_str()))
                }
                _ => None,
            })
        } else {
            None
        }
    }
    pub fn command(&self) -> &'static str {
        match self.statement {
            Statement::Schema(ref ddl) => {
                if ddl.create {
                    "CREATE SCHEMA"
                } else {
                    "DROP SCHEMA"
                }
            }
            Statement::TypeDdl(ref ddl) => ddl.command(),
            Statement::CompositeSelect(_) => "SELECT",
            Statement::Sequence(ref ddl) => match ddl {
                defaults::SequenceDdl::Create { .. } => "CREATE SEQUENCE",
                defaults::SequenceDdl::Alter { .. } => "ALTER SEQUENCE",
                defaults::SequenceDdl::Drop { .. } => "DROP SEQUENCE",
            },
            Statement::Query(ref q) => {
                if q.explain {
                    "EXPLAIN"
                } else {
                    "SELECT"
                }
            }
            Statement::DropTable { .. } => "DROP TABLE",
            Statement::SetConstraints(..) => "SET CONSTRAINTS",
            Statement::AlterTable(_) => "ALTER TABLE",
            Statement::Comment(_) => "COMMENT",
            Statement::View(ref ddl) => if ddl.query.is_some() { "CREATE VIEW" } else { "DROP VIEW" },
            Statement::Create { .. } => "CREATE TABLE",
            Statement::CreateIndex(_) => "CREATE INDEX",
            Statement::DropIndex(_) => "DROP INDEX",
            Statement::Insert { .. }
            | Statement::InsertExpressions(_)
            | Statement::InsertMulti(_) => "INSERT",
            Statement::Select { .. } => "SELECT",
            Statement::Update { .. } | Statement::UpdateExpressions { .. } => "UPDATE",
            Statement::Delete { .. } => "DELETE",
            Statement::Truncate { .. } => "TRUNCATE TABLE",
            Statement::Begin => "BEGIN",
            Statement::Commit => "COMMIT",
            Statement::Rollback | Statement::RollbackTo(_) => "ROLLBACK",
            Statement::Savepoint(_) => "SAVEPOINT",
            Statement::Release(_) => "RELEASE",
        }
    }
    pub(crate) fn scan(scan: Scan, domain: u128, state: &Relational) -> R<Self> {
        ident(&scan.table)?;
        let t = state
            .tables
            .get(&scan.table)
            .ok_or(SqlError::UnknownTable)?;
        if t.hidden_rowid().is_some() {
            return Err(SqlError::Unsupported);
        }
        if scan.prefix.len() > t.primary_key.len()
            || scan.limit > MAX_ROWS
            || scan.projection.len() > MAX_COLUMNS
        {
            return Err(SqlError::Limit);
        }
        payload_bound(
            scan.prefix
                .iter()
                .chain(scan.lower.iter().map(|(v, _)| v))
                .chain(scan.upper.iter().map(|(v, _)| v)),
        )?;
        let mut predicates = vec![];
        for (i, v) in scan.prefix.into_iter().enumerate() {
            predicates.push(Predicate {
                column: t.columns[t.primary_key[i]].name.clone(),
                op: "=".into(),
                value: Expr::Literal(v),
            });
        }
        let used = predicates.len();
        for (bound, lower) in [(scan.lower, true), (scan.upper, false)] {
            if let Some((v, inclusive)) = bound {
                let i = *t.primary_key.get(used).ok_or(SqlError::Unbounded)?;
                predicates.push(Predicate {
                    column: t.columns[i].name.clone(),
                    op: match (lower, inclusive) {
                        (true, true) => ">=",
                        (true, false) => ">",
                        (false, true) => "<=",
                        (false, false) => "<",
                    }
                    .into(),
                    value: Expr::Literal(v),
                });
            }
        }
        Ok(Self {
            domain,
            epoch: state.epoch,
            transaction: None,
            params: 0,
            parameter_types: vec![],
            relations: Vec::new(),
            search_path: vec!["public".into()],
            schema_user: "giggles".into(),
            unbound_statement: None,
            warnings: vec![],
            statement: Statement::Select {
                name: scan.table,
                projection: scan.projection,
                predicates,
                order: t
                    .primary_key
                    .iter()
                    .map(|i| (t.columns[*i].name.clone(), scan.reverse))
                    .collect(),
                limit: Some(Expr::Literal(Value::Int(scan.limit as i64))),
            },
        })
    }
    /// Declare input types for SELECT, INSERT VALUES, UPDATE and DELETE before type inference.
    /// A shorter slice or `None` entries leave the corresponding inputs inferred.
    /// Other statement kinds retain their existing parameter inference.
    pub fn with_parameter_types(mut self, types: &[Option<DataType>]) -> R<Self> {
        assert_sql_frontend_allowed();
        if types.len() > MAX_PARAMS {
            return Err(SqlError::Limit);
        }
        if types.len() > self.params {
            return Err(SqlError::Parameters);
        }
        if matches!(
            self.statement,
            Statement::Query(_)
                | Statement::Insert { .. }
                | Statement::InsertMulti(_)
                | Statement::InsertExpressions(_)
                | Statement::Update { .. }
                | Statement::UpdateExpressions { .. }
                | Statement::Delete { .. }
        ) {
            self.parameter_types = types.to_vec();
        }
        Ok(self)
    }
    pub fn parameter_count(&self) -> usize {
        self.params
    }
    /// Sequence references in syntax, without resolving catalog names.
    pub fn sequence_dependencies(sql: &str) -> Vec<Option<String>> {
        Self::parse(sql, 0, 0)
            .map(|prepared| prepared.sequences())
            .unwrap_or_default()
    }
    pub fn sequences(&self) -> Vec<Option<String>> {
        let mut dependencies = match &self.statement {
            Statement::Query(query) => query.sequence_dependencies(),
            Statement::InsertExpressions(values) => values.sequence_dependencies(),
            Statement::InsertMulti(insert) => insert.sequence_dependencies(),
            Statement::AlterTable(alter) => alter.sequence_dependencies(),
            Statement::Create { defaults, .. } => defaults
                .iter()
                .flatten()
                .flat_map(|sql| defaults::dependencies(sql).unwrap_or_default())
                .map(Some)
                .collect(),
            Statement::UpdateExpressions { sets, .. } => sets
                .iter()
                .flat_map(|(_, value)| match value {
                    assign::Value::Scalar(query) => query.sequence_dependencies(),
                    _ => vec![],
                })
                .collect(),
            _ => vec![],
        };
        if let Some(returning) = self.returning() {
            dependencies.extend(returning.sequence_dependencies());
        }
        if let Some(search) = searched::target(self) {
            dependencies.extend(search.sequence_dependencies());
        }
        dependencies
    }
    pub(crate) fn implicit_sequences(&self, state: &Relational) -> R<Vec<Option<String>>> {
        if let Statement::InsertMulti(insert) = &self.statement {
            return insert.implicit_sequences(state);
        }
        let mut dependencies = vec![];
        let mut add = |column: &ColumnDef| -> R<()> {
            if let Some(sql) = defaults::default_sql(state, column) {
                dependencies.extend(defaults::dependencies(&sql)?.into_iter().map(Some));
            }
            Ok(())
        };
        match &self.statement {
            Statement::Insert { name, columns, .. } => {
                let table = state.tables.get(name).ok_or(SqlError::UnknownTable)?;
                if !columns.is_empty() {
                    for column in table.logical_columns() {
                        if !columns.contains(&column.name) {
                            add(column)?;
                        }
                    }
                }
            }
            Statement::InsertExpressions(values) => {
                let table = state
                    .tables
                    .get(&values.name)
                    .ok_or(SqlError::UnknownTable)?;
                if !values.columns.is_empty() {
                    for column in table.logical_columns() {
                        if !values.columns.contains(&column.name) {
                            add(column)?;
                        }
                    }
                }
            }
            Statement::UpdateExpressions { name, sets, .. } => {
                let table = state.tables.get(name).ok_or(SqlError::UnknownTable)?;
                for (name, value) in sets {
                    if matches!(value, assign::Value::Default) {
                        add(&table.columns[col(table, name)?])?;
                    }
                }
            }
            _ => {}
        }
        Ok(dependencies)
    }
    pub fn sequence_only(&self) -> bool {
        self.is_sequence_ddl()
            || matches!(&self.statement, Statement::Query(query) if query.sequence_only())
    }
    pub fn is_sequence_ddl(&self) -> bool {
        matches!(self.statement, Statement::Sequence(_))
    }
    pub fn is_alter_sequence(&self) -> bool {
        matches!(
            self.statement,
            Statement::Sequence(defaults::SequenceDdl::Alter { .. })
        )
    }
    pub fn is_drop_sequence(&self) -> bool {
        matches!(
            self.statement,
            Statement::Sequence(defaults::SequenceDdl::Drop { .. })
        )
    }
    pub fn sequence_ddl_names(&self) -> Vec<&str> {
        match &self.statement {
            Statement::Sequence(ddl) => ddl.names(),
            _ => vec![],
        }
    }
    pub fn sequence_owned_by_table(&self) -> Option<&str> {
        match &self.statement {
            Statement::Sequence(ddl) => ddl.owned_by_table(),
            _ => None,
        }
    }
    pub fn sequence_ddl_targets(sql: &str) -> R<Vec<String>> {
        let tokens = lex(sql)?;
        if !matches!(tokens.first(), Some(Token::Word(word)) if matches!(word.as_str(), "create" | "alter" | "drop"))
            || !matches!(tokens.get(1), Some(Token::Word(word)) if word == "sequence")
        {
            return Ok(vec![]);
        }
        let parsed = Self::parse(sql, 0, 0)?;
        Ok(parsed
            .sequence_ddl_names()
            .into_iter()
            .map(str::to_owned)
            .collect())
    }
    pub fn sequence_owned_by(sql: &str) -> R<Option<String>> {
        if Self::sequence_ddl_targets(sql)?.is_empty() {
            return Ok(None);
        }
        Ok(Self::parse(sql, 0, 0)?
            .sequence_owned_by_table()
            .map(str::to_owned))
    }
    pub fn sequence_ddl_is_create(sql: &str) -> R<bool> {
        if Self::sequence_ddl_targets(sql)?.is_empty() {
            return Ok(false);
        }
        Ok(matches!(
            Self::parse(sql, 0, 0)?.statement,
            Statement::Sequence(defaults::SequenceDdl::Create { .. })
        ))
    }
    pub(crate) fn sequence_restart_seed(&self, state: &Relational) -> Option<(u64, i64)> {
        match &self.statement {
            Statement::Sequence(ddl) => ddl.restart_seed(state),
            _ => None,
        }
    }
    pub(crate) fn sequence_copies_counter(&self) -> bool {
        matches!(&self.statement, Statement::Sequence(ddl) if ddl.copies_counter())
    }
    pub(crate) fn validate_sequence_current(
        &self,
        state: &Relational,
        current: impl Fn(u64) -> Option<i64>,
    ) -> R<()> {
        match &self.statement {
            Statement::Sequence(ddl) => ddl.validate_current(state, current),
            _ => Ok(()),
        }
    }
    /// Return the ALTER target only after parsing the complete statement.
    pub fn alter_table_target(sql: &str) -> R<Option<String>> {
        let mut tokens = Vec::new();
        if let Err(error) = lex_into(sql, &mut tokens, false) {
            // Retain the leading token on failure; repeatedly re-lexing prefixes
            // is quadratic for long comments/whitespace preceding a bad body.
            return if matches!(tokens.first(), Some(Token::Word(word)) if word == "alter") {
                Err(error)
            } else {
                Ok(None)
            };
        }
        if !matches!(tokens.first(), Some(Token::Word(word)) if word == "alter") {
            return Ok(None);
        }
        if matches!(tokens.get(1), Some(Token::Word(word)) if word == "sequence") {
            return Ok(None);
        }
        let mut parser = Parser {
            tokens,
            at: 0,
            params: BTreeSet::new(),
            relations: Vec::new(),
        };
        Ok(Some(alter_table::parse(&mut parser)?.table))
    }
    /// Parse DROP ownership targets without consulting the catalog.
    pub fn drop_table_targets(sql: &str) -> R<Vec<String>> {
        let Ok(tokens) = lex(sql) else {
            return Ok(vec![]);
        };
        if !matches!(tokens.first(),Some(Token::Word(word)) if word == "drop") {
            return Ok(vec![]);
        }
        let mut parser = Parser {
            tokens,
            at: 0,
            params: BTreeSet::new(),
            relations: Vec::new(),
        };
        match parser.parse()? {
            Statement::DropTable { names, .. } => Ok(names),
            _ => Ok(vec![]),
        }
    }
    /// Parse-only negative routing hint. No binding, evaluation, metadata or authority.
    /// Parse failures and dependency-bearing queries keep their existing gateway path.
    pub fn rejects_unsupported_table_free_scalar(sql: &str) -> bool {
        Self::parse(sql, 0, 0).is_ok_and(|prepared| {
            matches!(&prepared.statement,
                Statement::Query(query) if query.rejects_unsupported_table_free_scalar())
        })
    }
    /// Lexical routing hint only: no binding, routine classification or errors.
    /// Every identifier is checked conservatively, including nested statements.
    pub fn application_dml(sql: &str, catalog_identifier: impl Fn(&str) -> bool) -> bool {
        let mut tokens = Vec::new();
        if lex_into(sql, &mut tokens, true).is_err() {
            return false;
        }
        matches!(tokens.first(), Some(Token::Word(w)) if matches!(w.as_str(), "insert" | "update" | "delete" | "truncate"))
            && !tokens.iter().any(|token| match token {
                Token::Word(w) | Token::QuotedIdent(w) => catalog_identifier(w),
                _ => false,
            })
    }
    /// Parse FK dependencies before binding or relation lookup.
    pub fn constraint_references(sql: &str) -> R<Vec<String>> {
        let tokens = lex(sql)?;
        let mut references = vec![];
        let own = if matches!(tokens.first(),Some(Token::Word(w)) if w=="create") {
            tokens
                .iter()
                .position(|t| matches!(t,Token::Word(w) if w=="table"))
                .and_then(|i| match tokens.get(i + 1) {
                    Some(Token::Word(w) | Token::QuotedIdent(w)) => Some(w.clone()),
                    _ => None,
                })
        } else {
            None
        };
        for (i, token) in tokens.iter().enumerate() {
            if matches!(token,Token::Word(w) if w=="references") {
                let mut at = i + 1;
                let first = match tokens.get(at) {
                    Some(Token::Word(w) | Token::QuotedIdent(w)) => w.clone(),
                    _ => continue,
                };
                at += 1;
                let name = if tokens.get(at) == Some(&Token::Symbol('.')) {
                    match tokens.get(at + 1) {
                        Some(Token::Word(w) | Token::QuotedIdent(w)) => w.clone(),
                        _ => continue,
                    }
                } else {
                    first
                };
                if own.as_ref() != Some(&name) {
                    references.push(name);
                }
            }
        }
        Ok(references)
    }
    pub fn constraint_reference_tables(&self) -> Vec<String> {
        let (definitions, own) = match &self.statement {
            Statement::Create {
                constraints, name, ..
            } => (constraints.iter().collect::<Vec<_>>(), Some(name)),
            Statement::AlterTable(alter) => (
                alter
                    .actions
                    .iter()
                    .filter_map(|a| {
                        if let alter_table::Action::Constraint(d) = a {
                            Some(d)
                        } else {
                            None
                        }
                    })
                    .collect(),
                None,
            ),
            _ => return vec![],
        };
        definitions
            .into_iter()
            .filter_map(|d| {
                if let constraints::Definition::ForeignKey { parent, .. } = d {
                    if Some(parent) != own {
                        Some(parent.clone())
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect()
    }
    pub(crate) fn parse(sql: &str, domain: u128, epoch: u64) -> R<Self> {
        assert_sql_frontend_allowed();
        take_sql_warnings();
        let mut p = Parser {
            tokens: lex(sql)?,
            at: 0,
            params: BTreeSet::new(),
            relations: Vec::new(),
        };
        let statement = if matches!(p.tokens.first(), Some(Token::Word(word)) if word == "select" || word == "explain")
        {
            if user_types::CompositeSelect::claims(&p) {
                Statement::CompositeSelect(user_types::CompositeSelect::parse(&mut p)?)
            } else {
                Statement::Query(query::Query::parse(&mut p)?)
            }
        } else {
            match p.parse() {
                Ok(statement) => statement,
                Err(_)
                    if matches!(p.tokens.first(), Some(Token::Word(word)) if word == "insert")
                        && !sql_insert::claims(&p.tokens) =>
                {
                    p.at = 0;
                    p.params.clear();
                    Statement::InsertExpressions(query::InsertExpressions::parse(&mut p)?)
                }
                Err(error) => return Err(error),
            }
        };
        if let Statement::AlterTable(alter) = &statement
            && let Some(error) = &alter.resolution_error
        {
            return Err(error.clone());
        }
        if let Statement::DropTable {
            resolution_error: Some(error),
            ..
        } = &statement
        {
            return Err(error.clone());
        }
        let params = p.params.last().copied().unwrap_or(0);
        if p.params.len() != params {
            return Err(SqlError::Parameters);
        }
        Ok(Self {
            domain,
            epoch,
            transaction: None,
            statement,
            params,
            parameter_types: vec![],
            relations: p.relations,
            search_path: vec!["public".into()],
            schema_user: "giggles".into(),
            unbound_statement: None,
            warnings: take_sql_warnings(),
        })
    }
    /// Transaction-local name, normalized using PostgreSQL identifier rules.
    pub(crate) fn set_constraints(&self) -> Option<(&[String], bool)> {
        match &self.statement {
            Statement::SetConstraints(names, deferred) => Some((names, *deferred)),
            _ => None,
        }
    }
    pub fn savepoint_name(&self) -> Option<&str> {
        match &self.statement {
            Statement::Savepoint(name) | Statement::Release(name) | Statement::RollbackTo(name) => {
                Some(name)
            }
            _ => None,
        }
    }
    /// Recovery controls remain available in a failed transaction block.
    pub fn allows_aborted_transaction(&self) -> bool {
        matches!(
            self.statement,
            Statement::Rollback | Statement::RollbackTo(_)
        )
    }
    pub(crate) fn control(&self) -> Option<&'static str> {
        match self.statement {
            Statement::Begin => Some("begin"),
            Statement::Commit => Some("commit"),
            Statement::Rollback => Some("rollback"),
            Statement::Savepoint(_) => Some("savepoint"),
            Statement::Release(_) => Some("release"),
            Statement::RollbackTo(_) => Some("rollback_to"),
            _ => None,
        }
    }
    pub(crate) fn is_read(&self) -> bool {
        matches!(
            self.statement,
            Statement::Select { .. } | Statement::Query(_) | Statement::CompositeSelect(_)
        )
    }
}
fn col(t: &TableDef, name: &str) -> R<usize> {
    t.columns
        .iter()
        .position(|c| c.name == name)
        .ok_or(SqlError::UnknownColumn)
}
fn unique<T: Ord>(v: impl IntoIterator<Item = T>) -> R<()> {
    let mut set = BTreeSet::new();
    for item in v {
        if !set.insert(item) {
            return Err(SqlError::Duplicate);
        }
    }
    Ok(())
}
struct Bounds {
    low: Vec<u8>,
    high: Vec<u8>,
    low_inclusive: bool,
    high_inclusive: bool,
    point: bool,
}
fn successor(mut v: Vec<u8>) -> Option<Vec<u8>> {
    while let Some(b) = v.pop() {
        if b < 255 {
            v.push(b + 1);
            return Some(v);
        }
    }
    None
}
fn bounds(t: &TableDef, preds: &[Predicate], params: &[Value]) -> R<Bounds> {
    let mut by: BTreeMap<usize, Vec<(&str, Value)>> = BTreeMap::new();
    for p in preds {
        let i = col(t, &p.column)?;
        if !t.primary_key.contains(&i) {
            return Err(SqlError::Unbounded);
        }
        let mut v = p.value.value(params)?;
        if let Expr::Literal(literal) = &p.value
            && let Some(typed) =
                query::unknown_comparison_literal(literal, &t.columns[i].data_type)?
        {
            // A clock-dependent literal is compared in the full filter.
            v = typed.ok_or(SqlError::Unbounded)?;
        }
        // NULL is a comparison operand, not a stored key value. Let the full
        // WHERE evaluator apply SQL's unknown result instead of the column's
        // NOT NULL constraint (or encoding NULL as an ordinary point key).
        if v == Value::Null {
            return Err(SqlError::Unbounded);
        }
        if let Value::Int(n) = &v
            && match t.columns[i].data_type {
                DataType::SmallInt => i16::try_from(*n).is_err(),
                DataType::Integer => i32::try_from(*n).is_err(),
                _ => false,
            }
        {
            return Err(SqlError::Unbounded);
        }
        if query::comparison_requires_filter(&v, &t.columns[i].data_type)? {
            return Err(SqlError::Unbounded);
        }
        check(&v, &t.columns[i])?;
        by.entry(i).or_default().push((&p.op, v));
    }
    let mut prefix = vec![];
    let mut consumed = 0;
    for i in &t.primary_key {
        let Some(ps) = by.get(i) else { break };
        if ps.len() == 1 && ps[0].0 == "=" {
            prefix.extend(encode_key(&[ps[0].1.clone()])?);
            consumed += 1;
        } else {
            break;
        }
    }
    if consumed == t.primary_key.len() {
        if by.len() != consumed {
            return Err(SqlError::Unbounded);
        }
        return Ok(Bounds {
            low: prefix.clone(),
            high: prefix,
            low_inclusive: true,
            high_inclusive: true,
            point: true,
        });
    }
    let mut low = prefix.clone();
    let mut high = successor(prefix.clone()).unwrap_or_else(|| vec![255]);
    let mut low_inclusive = true;
    let mut high_inclusive = false;
    if let Some(ps) = by.get(&t.primary_key[consumed]) {
        let mut has_low = false;
        let mut has_high = false;
        for (op, v) in ps {
            let mut b = prefix.clone();
            b.extend(encode_key(std::slice::from_ref(v))?);
            match *op {
                ">" | ">=" => {
                    if has_low {
                        return Err(SqlError::Unsupported);
                    }
                    has_low = true;
                    if *op == ">" {
                        low = successor(b).ok_or(SqlError::Unbounded)?;
                    } else {
                        low = b;
                    }
                    low_inclusive = true;
                }
                "<" | "<=" => {
                    if has_high {
                        return Err(SqlError::Unsupported);
                    }
                    has_high = true;
                    if *op == "<=" {
                        high = successor(b).ok_or(SqlError::Unbounded)?;
                    } else {
                        high = b;
                    }
                    high_inclusive = false;
                }
                _ => return Err(SqlError::Unsupported),
            }
        }
        consumed += 1;
    }
    if by.len() != consumed {
        return Err(SqlError::Unbounded);
    }
    Ok(Bounds {
        low,
        high,
        low_inclusive,
        high_inclusive,
        point: false,
    })
}
impl DataType {
    pub fn is_temporal(&self) -> bool {
        matches!(
            self,
            Self::Date
                | Self::Time
                | Self::TimeTz
                | Self::Timestamp
                | Self::TimestampTz
                | Self::Interval
        )
    }
}
impl Value {
    pub fn is_temporal(&self) -> bool {
        matches!(
            self,
            Self::Date(_)
                | Self::Time(_)
                | Self::TimeTz(_)
                | Self::Timestamp(_)
                | Self::TimestampTz(_)
                | Self::Interval(_)
        )
    }
}
impl Relational {
    pub(crate) fn primary_key_constraint_name(&self, table: &TableDef) -> Option<String> {
        constraint_primary_key::primary_key_name(self, table)
    }
    pub(crate) fn drop_table(&mut self, name: &str) -> R<bool> {
        self.drop_table_with_actions(name, false)
    }
    fn drop_table_with_actions(&mut self, name: &str, cascade: bool) -> R<bool> {
        let Some(table) = self.tables.get(name) else {
            return Ok(false);
        };
        let table_id = table.id;
        let epoch = self.epoch.checked_add(1).ok_or(SqlError::Overflow)?;
        let mut ext = self.ext.clone();
        for column in &table.columns {
            ext.columns.remove(&column.id);
            ext.logical_kinds.remove(&column.id);
        }
        ext.constraints.remove(&table_id);
        ext.table_schemas.remove(&table_id);
        for constraints in ext.constraints.values_mut() {
            let incoming = |constraint: &Constraint| matches!(&constraint.kind,ConstraintKind::ForeignKey {ref_table,..} if *ref_table == table_id);
            if !cascade && constraints.iter().any(incoming) {
                return Err(SqlError::DependentObjects);
            }
            constraints.retain(|constraint| !incoming(constraint));
        }
        ext.sequences
            .retain(|_, seq| !matches!(seq.owned_by, Some((id, _)) if id == table_id));
        Self::normalize_ext(&mut ext);
        self.validate_ext_candidate(&ext, self.next_id, Some(table_id))?;
        self.rows.remove(&table_id);
        self.secondary.drop_table(table_id);
        self.tables.remove(name);
        self.ext = ext;
        self.epoch = epoch;
        Ok(true)
    }

    pub(crate) fn contains_heap(&self) -> bool {
        self.tables
            .values()
            .any(|table| table.hidden_rowid().is_some())
    }

    pub(crate) fn contains_temporal(&self) -> bool {
        self.tables
            .values()
            .any(|t| t.columns.iter().any(|c| c.data_type.is_temporal()))
            || self
                .rows
                .values()
                .flat_map(|r| r.values())
                .any(|r| r.values.iter().any(Value::is_temporal))
    }
    pub(crate) fn contains_typed(&self) -> bool {
        self.tables.values().any(|t| {
            t.columns.iter().any(|c| {
                matches!(
                    c.data_type,
                    DataType::Numeric | DataType::Real | DataType::DoublePrecision
                )
            })
        })
    }

    /// Check all relation row maps without scanning or cloning their values.
    pub(crate) fn rows_empty_engine(&self) -> bool {
        self.rows.values().all(BTreeMap::is_empty)
    }
    /// Read only the version of one exact primary key from the staged relation.
    pub(crate) fn row_version_engine(&self, table: &TableDef, key: &[Value]) -> R<u64> {
        Prepared::engine_table(table, self)?;
        if table.hidden_rowid().is_some() {
            return Err(SqlError::Unsupported);
        }
        if key.len() != table.primary_key.len() {
            return Err(SqlError::Unbounded);
        }
        payload_bound(key.iter())?;
        for (value, index) in key.iter().zip(&table.primary_key) {
            check(value, &table.columns[*index])?;
        }
        let encoded = encode_key(key)?;
        self.rows
            .get(&table.id)
            .and_then(|rows| rows.get(&encoded))
            .map(|row| row.version)
            .ok_or(SqlError::Conflict)
    }
    pub(crate) fn validate(&self) -> R<()> {
        if self.format != 1 {
            return Err(SqlError::Unsupported);
        }
        self.validate_ext()?;
        if self.tables.len() > MAX_TABLES || self.rows.len() != self.tables.len() {
            return Err(SqlError::Type);
        }
        let mut ids = BTreeSet::new();
        let mut versions = BTreeSet::new();
        for (name, t) in &self.tables {
            schema_binding::validate_key(name)?;
            if name != &relation_key(self.table_schema(t.id), &t.name)
                || t.id == 0
                || t.id >= self.next_id
                || !ids.insert(t.id)
                || t.columns.is_empty()
                || t.columns.len() > MAX_COLUMNS
                || t.primary_key.is_empty()
            {
                return Err(SqlError::Type);
            }
            unique(t.columns.iter().map(|c| &c.name))?;
            unique(t.primary_key.iter())?;
            for (index, c) in t.columns.iter().enumerate() {
                t.validate_column_ident(index)?;
                if c.id == 0 || c.id >= self.next_id || !ids.insert(c.id) {
                    return Err(SqlError::Type);
                }
            }
            for i in &t.primary_key {
                if *i >= t.columns.len() || !t.columns[*i].not_null {
                    return Err(SqlError::Null);
                }
            }
            for (k, r) in self.rows.get(&t.id).ok_or(SqlError::Type)? {
                row_check(t, &r.values)?;
                if pk(t, &r.values)? != *k
                    || r.version == 0
                    || r.version >= self.next_version
                    || !versions.insert(r.version)
                {
                    return Err(SqlError::Version);
                }
            }
        }
        if self.has_secondary_indexes_engine() {
            self.secondary_validate_engine(|| crate::cancellation::check().is_err())?;
        }
        Ok(())
    }
    pub(crate) fn execute(
        &mut self,
        prepared: &Prepared,
        params: &[Value],
        expected: Option<u64>,
    ) -> R<SqlResult> {
        self.execute_with_cancel(prepared, params, expected, || {
            crate::cancellation::check().is_err()
        })
    }
    pub(crate) fn execute_with_cancel(
        &mut self,
        prepared: &Prepared,
        params: &[Value],
        expected: Option<u64>,
        mut cancel: impl FnMut() -> bool,
    ) -> R<SqlResult> {
        self.execute_clocked(prepared, params, expected, &mut cancel, None)
    }
    pub(crate) fn execute_clocked(
        &mut self,
        prepared: &Prepared,
        params: &[Value],
        expected: Option<u64>,
        cancel: impl FnMut() -> bool,
        clock: Option<&clock::ExecutionClock>,
    ) -> R<SqlResult> {
        self.execute_clocked_inner(prepared, params, expected, cancel, clock, None)
    }
    #[cfg(test)]
    pub(crate) fn execute_with_source(
        &mut self,
        prepared: &Prepared,
        params: &[Value],
        expected: Option<u64>,
        source: &dyn scan::RowSource,
    ) -> R<SqlResult> {
        self.execute_clocked_inner(prepared, params, expected, || false, None, Some(source))
    }
    fn execute_clocked_inner(
        &mut self,
        prepared: &Prepared,
        params: &[Value],
        expected: Option<u64>,
        mut cancel: impl FnMut() -> bool,
        clock: Option<&clock::ExecutionClock>,
        injected_source: Option<&dyn scan::RowSource>,
    ) -> R<SqlResult> {
        let adds_constraints = matches!(&prepared.statement,Statement::Create { constraints,.. } if !constraints.is_empty())
            || matches!(&prepared.statement,Statement::AlterTable(alter) if alter.actions.iter().any(|a|matches!(a,alter_table::Action::Constraint(_))));
        if self.ext.constraints.is_empty() && !adds_constraints {
            return self.execute_statement_inner(
                prepared,
                params,
                expected,
                cancel,
                clock,
                injected_source,
                &mut Vec::new(),
            );
        }
        let mut deferred = DeferralState::default();
        let mut candidate = self.clone();
        let mut events = Vec::new();
        let result = candidate.execute_statement_inner(
            prepared,
            params,
            expected,
            &mut cancel,
            clock,
            injected_source,
            &mut events,
        )?;
        constraints::enforce(
            &mut candidate,
            self,
            events,
            &mut deferred,
            &mut cancel,
            clock,
        )?;
        candidate.validate_deferred(&mut deferred, &mut cancel)?;
        *self = candidate;
        Ok(result)
    }
    pub(crate) fn execute_deferred(
        &mut self,
        prepared: &Prepared,
        params: &[Value],
        expected: Option<u64>,
        mut cancel: impl FnMut() -> bool,
        clock: Option<&clock::ExecutionClock>,
        deferred: &mut DeferralState,
    ) -> R<SqlResult> {
        let mut candidate = self.clone();
        let mut pending = deferred.clone();
        let mut events = Vec::new();
        let result = candidate.execute_statement_inner(
            prepared,
            params,
            expected,
            &mut cancel,
            clock,
            None,
            &mut events,
        )?;
        constraints::enforce(
            &mut candidate,
            self,
            events,
            &mut pending,
            &mut cancel,
            clock,
        )?;
        *self = candidate;
        *deferred = pending;
        Ok(result)
    }
    #[expect(
        clippy::too_many_arguments,
        reason = "statement inputs plus statement-local referential events"
    )]
    fn execute_statement_inner(
        &mut self,
        prepared: &Prepared,
        params: &[Value],
        expected: Option<u64>,
        mut cancel: impl FnMut() -> bool,
        clock: Option<&clock::ExecutionClock>,
        injected_source: Option<&dyn scan::RowSource>,
        events: &mut Vec<constraints::Event>,
    ) -> R<SqlResult> {
        let bound = prepared.bind_schemas(self)?;
        let prepared = &bound;
        prepared.validate_schemas(self)?;
        user_types::clear_sql_error_message();
        ddl_type::publish(&prepared.warnings);
        if prepared.epoch != self.epoch {
            return Err(SqlError::StaleSchema);
        }
        if params.len() != prepared.params || params.len() > MAX_PARAMS {
            return Err(SqlError::Parameters);
        }
        if matches!(
            prepared.statement,
            Statement::Insert { .. }
                | Statement::InsertExpressions(_)
                | Statement::InsertMulti(_)
                | Statement::Update { .. }
                | Statement::UpdateExpressions { .. }
                | Statement::Delete { .. }
        ) {
            self.secondary_admit_mutation(&mut cancel)?;
        }
        if matches!(
            prepared.statement,
            Statement::Insert { .. }
                | Statement::InsertExpressions(_)
                | Statement::Update { .. }
                | Statement::UpdateExpressions { .. }
                | Statement::Delete { .. }
        ) {
            let mut source_values = Vec::new();
            match &prepared.statement {
                Statement::Insert { values, .. } => {
                    for e in values {
                        if let Expr::Literal(v) = e {
                            source_values.push(v);
                        }
                    }
                }
                Statement::Update { sets, .. } => {
                    for (_, e) in sets {
                        if let Expr::Literal(v) = e {
                            source_values.push(v);
                        }
                    }
                }
                Statement::UpdateExpressions { sets, .. } => {
                    for (_, e) in sets {
                        if let assign::Value::Plain(Expr::Literal(v)) = e {
                            source_values.push(v);
                        }
                    }
                }
                _ => {}
            }
            Self::secondary_admit_values(params.iter())?;
            Self::secondary_admit_values(source_values.iter().copied())?;
            payload_bound(source_values.into_iter())?;
            if let Statement::InsertExpressions(values) = &prepared.statement {
                values.admit_literals()?;
            }
            if let Statement::UpdateExpressions { sets, .. } = &prepared.statement {
                assign::admit_literals(sets)?;
            }
        }
        // Bound parameters even if a statement affects no rows.
        params_bound(params.iter())?;
        if prepared.parameter_types.iter().any(Option::is_some)
            && matches!(
                prepared.statement,
                Statement::Insert { .. }
                    | Statement::InsertMulti(_)
                    | Statement::InsertExpressions(_)
                    | Statement::Update { .. }
                    | Statement::UpdateExpressions { .. }
                    | Statement::Delete { .. }
            )
        {
            prepared.describe(self)?;
        }
        if assign::has_duplicates(prepared)
            || matches!(&prepared.statement, Statement::InsertMulti(insert) if insert.has_duplicate_assignments())
        {
            prepared.describe(self)?;
        }
        let returning = returning::Plan::new(prepared, self, params, clock)?;
        let mut returned_rows = vec![];
        // Normalize expression outputs after mutation input admission.
        let normalized;
        let prepared = if let Statement::InsertExpressions(values) = &prepared.statement {
            values.check_identity(self)?;
            if expected.is_some() {
                return Err(SqlError::Unsupported);
            }
            if cancel() {
                return Err(SqlError::Aborted);
            }
            let output = returning::evaluate_insert(
                values,
                self,
                prepared.params,
                &prepared.parameter_types,
                params,
                &mut cancel,
                clock,
            )?;
            normalized = Prepared {
                domain: prepared.domain,
                epoch: prepared.epoch,
                transaction: prepared.transaction,
                params: prepared.params,
                parameter_types: vec![],
                relations: Vec::new(),
                search_path: vec!["public".into()],
                schema_user: "giggles".into(),
                unbound_statement: None,
                warnings: vec![],
                statement: Statement::Insert {
                    name: values.name.clone(),
                    columns: values.columns.clone(),
                    values: output,
                    returning: values.returning.clone(),
                },
            };
            &normalized
        } else {
            prepared
        };
        let mut result = SqlResult::default();
        match &prepared.statement {
            Statement::TypeDdl(ddl) => {
                user_types::execute(self, ddl, prepared)?;
            }
            Statement::CompositeSelect(select) => {
                return select.execute(self, params, &prepared.search_path);
            }
            Statement::Query(q) => {
                if expected.is_some() {
                    return Err(SqlError::Unsupported);
                }
                if !q.is_table_free() && self.sequence_by_name(q.target()).is_some() {
                    return Err(SqlError::Unsupported);
                }
                let legacy = scan::LegacyRows(&*self);
                let source = injected_source.unwrap_or(&legacy);
                return q
                    .bind(self, prepared.params, &prepared.parameter_types)?
                    .with_clock(clock)
                    .execute_declared(
                        self,
                        source,
                        params,
                        &mut cancel,
                        &prepared.parameter_types,
                    );
            }
            Statement::InsertExpressions(_) => return Err(SqlError::Unsupported),
            Statement::InsertMulti(insert) => {
                return sql_insert::execute_declared(
                    self,
                    insert,
                    params,
                    expected,
                    &returning,
                    clock,
                    &mut cancel,
                    &prepared.parameter_types,
                    events,
                    injected_source,
                );
            }
            Statement::Schema(ddl) => {
                return self.schema_ddl(ddl, &prepared.schema_user);
            }
            Statement::Truncate { restart, .. } => {
                if expected.is_some() {
                    return Err(SqlError::Unsupported);
                }
                let names = prepared.truncate_tables(self)?;
                for name in &names {
                    if cancel() {
                        return Err(SqlError::Aborted);
                    }
                    self.rows
                        .get_mut(&self.tables[name].id)
                        .ok_or(SqlError::UnknownTable)?
                        .clear();
                }
                let descriptors = self.secondary_descriptors_engine().to_vec();
                self.secondary = self.secondary_build_complete_engine(&descriptors, &mut cancel)?;
                if *restart {
                    prepared.truncate_restart(self, &names)?;
                }
            }
            Statement::Sequence(ddl) => {
                if expected.is_some() {
                    return Err(SqlError::Unsupported);
                }
                result.affected = defaults::execute_sequence(self, ddl)?;
            }
            Statement::AlterTable(alter) => {
                if expected.is_some() {
                    return Err(SqlError::Unsupported);
                }
                alter_table::execute(self, alter, &prepared.search_path, clock, &mut cancel)?;
            }
            Statement::Comment(_) | Statement::View(_) => return Err(SqlError::Unsupported), // v2 bridge only
            Statement::DropTable {
                names,
                optional,
                cascade,
                resolution_error,
            } => {
                if let Some(error) = resolution_error {
                    return Err(error.clone());
                }
                if expected.is_some() {
                    return Err(SqlError::Unsupported);
                }
                // Validate the entire set before removing anything. Dependencies
                // between listed tables do not require CASCADE (including cycles).
                let ids = names.iter().filter_map(|n| self.tables.get(n).map(|t| t.id)).collect::<BTreeSet<_>>();
                if !optional && names.iter().any(|n| !self.tables.contains_key(n)) {
                    return Err(SqlError::UnknownTable);
                }
                if !cascade && self.ext.constraints.iter().any(|(id, constraints)|
                    !ids.contains(id) && constraints.iter().any(|c|
                        matches!(&c.kind, ConstraintKind::ForeignKey { ref_table, .. } if ids.contains(ref_table)))) {
                    return Err(SqlError::DependentObjects);
                }
                for name in names { self.drop_table_with_actions(name, true)?; }
            }
            Statement::SetConstraints(_, _) => return Err(SqlError::NoActiveTransaction),
            Statement::DropIndex(index) => {
                let descriptor = self
                    .secondary_descriptors_engine()
                    .iter()
                    .find(|d| d.name == index.name)
                    .cloned();
                if let Some(descriptor) = descriptor {
                    if self.ext.constraints.values().flatten().any(|c|
                        matches!(c.kind, ConstraintKind::Unique { index, .. } if index == descriptor.id)) {
                        return Err(SqlError::DependentObjects);
                    }
                    let descriptors = self
                        .secondary_descriptors_engine()
                        .iter()
                        .filter(|d| d.id != descriptor.id)
                        .cloned()
                        .collect::<Vec<_>>();
                    self.secondary =
                        self.secondary_build_complete_engine(&descriptors, &mut cancel)?;
                    self.epoch = self.epoch.checked_add(1).ok_or(SqlError::Overflow)?;
                } else if !index.optional {
                    return Err(SqlError::UndefinedObject);
                }
            }
            Statement::CreateIndex(index) => {
                if expected.is_some() {
                    return Err(SqlError::Unsupported);
                }
                if index.if_not_exists
                    && self
                        .secondary_descriptors_engine()
                        .iter()
                        .any(|d| d.name == relation_key(split_key(&index.table).0, &index.name))
                {
                    return Ok(result);
                }
                let table = self
                    .tables
                    .get(&index.table)
                    .ok_or(SqlError::UnknownTable)?
                    .clone();
                let ids = index
                    .columns
                    .iter()
                    .map(|name| col(&table, name).map(|i| table.columns[i].id))
                    .collect::<R<Vec<_>>>()?;
                self.create_secondary_index_populated_engine(
                    &table,
                    self.epoch,
                    &index.name,
                    &ids,
                    index.unique,
                    &mut cancel,
                )?;
            }
            Statement::Create {
                name,
                optional,
                columns,
                typmods,
                defaults,
                sequences,
                keys,
                constraints,
                primary_name,
                user_types,
            } => {
                if expected.is_some() {
                    return Err(SqlError::Unsupported);
                }
                let mut columns = columns.clone();
                let mut typmods = typmods.clone();
                let mut type_ids = vec![None; columns.len()];
                for (i, name) in user_types.iter().enumerate() {
                    if let Some(name) = name {
                        let (id, ty) = self
                            .resolve_type(name, &prepared.search_path)?
                            .ok_or(SqlError::UndefinedObject)?;
                        columns[i].data_type = ty.storage_type()?;
                        if let TypeDef::Domain { typmod, .. } = ty {
                            typmods[i] = *typmod;
                        }
                        type_ids[i] = Some(id);
                    }
                }
                let columns = &columns;
                let typmods = &typmods;
                if self.type_by_name(split_key(name).1).is_some() {
                    return Err(SqlError::DuplicateObject);
                }
                if self.relation_name_taken(name) {
                    if *optional {
                        return Ok(result);
                    }
                    return Err(SqlError::DuplicateTable);
                }
                if self.tables.len() >= MAX_TABLES
                    || columns.is_empty()
                    || columns.len() > MAX_COLUMNS
                {
                    return Err(SqlError::Limit);
                }
                let mut column_names = BTreeSet::new();
                for column in columns {
                    if column.name == "ctid" {
                        return Err(SqlError::DuplicateColumn);
                    }
                    if !column_names.insert(&column.name) {
                        return Err(SqlError::DuplicateColumn);
                    }
                }
                if typmods.len() != columns.len() {
                    return Err(SqlError::InvalidParameterValue);
                }
                for (column, typmod) in columns.iter().zip(typmods) {
                    if let Some(typmod) = typmod {
                        validate_typmod(&column.data_type, *typmod)
                            .map_err(|_| SqlError::InvalidParameterValue)?;
                    }
                }
                validate_create_definition(split_key(name).1, columns, keys)?;
                let mut next_id = self.next_id;
                let mut t = TableDef {
                    id: allocate(&mut next_id)?,
                    name: split_key(name).1.into(),
                    columns: columns.clone(),
                    primary_key: vec![],
                };
                if keys.is_empty() {
                    t.primary_key.push(t.columns.len());
                    t.columns.push(ColumnDef {
                        id: 0,
                        name: HIDDEN_ROWID.into(),
                        data_type: DataType::BigInt,
                        not_null: true,
                    });
                }
                for key in keys {
                    let i = col(&t, key)?;
                    t.primary_key.push(i);
                    t.columns[i].not_null = true;
                }
                for c in &mut t.columns {
                    c.id = allocate(&mut next_id)?;
                }
                let epoch = self.epoch.checked_add(1).ok_or(SqlError::Overflow)?;
                let chosen_primary =
                    if keys.is_empty() {
                        None
                    } else {
                        Some(primary_name.clone().unwrap_or_else(|| {
                            self.choose_constraint_name(name, &[], AutoName::Pkey)
                        }))
                    };
                let schema = prepared.creation_schema(self)?;
                defaults::install_create(
                    self,
                    t,
                    defaults,
                    sequences,
                    typmods,
                    (next_id, epoch),
                    (&schema, &prepared.search_path),
                )?;
                let table = self.tables.get(name).ok_or(SqlError::UnknownTable)?.clone();
                self.mutate_ext(|ext, _| {
                    for (column, type_id) in table.columns.iter().zip(&type_ids) {
                        if let Some(id) = type_id {
                            ext.columns.entry(column.id).or_default().type_id = Some(*id);
                        }
                    }
                    Ok(())
                })?;
                if let Some(name) = chosen_primary {
                    constraint_primary_key::record_name(self, &table, &name)?;
                }
                // Candidate keys must exist before self-referencing FKs bind,
                // including UNIQUE declarations later in the CREATE body.
                for definition in constraints.iter().filter(|definition| {
                    matches!(definition, constraints::Definition::Unique { .. })
                }) {
                    constraints::install(self, &table, definition, &mut cancel)?;
                }
                for definition in constraints.iter().filter(|definition| {
                    !matches!(definition, constraints::Definition::Unique { .. })
                }) {
                    constraints::install(self, &table, definition, &mut cancel)?;
                }
            }
            Statement::Insert {
                name,
                columns,
                values,
                ..
            } => {
                if expected.is_some() {
                    return Err(SqlError::Unsupported);
                }
                let t = self.tables.get(name).ok_or(SqlError::UnknownTable)?;
                let indexes = if columns.is_empty() {
                    t.logical_offsets()
                } else {
                    columns.iter().map(|c| col(t, c)).collect::<R<Vec<_>>>()?
                };
                unique(&indexes)?;
                if indexes.len() != values.len() {
                    return Err(SqlError::Type);
                }
                for index in &indexes {
                    defaults::check_explicit(self, &t.columns[*index])?;
                }
                let mut row = defaults::row(self, t, &indexes, clock)?;
                for (i, v) in indexes.iter().zip(values) {
                    row[*i] = if matches!(v, Expr::Literal(_)) {
                        query::assignment::literal(
                            v.value(params)?,
                            &t.columns[*i].data_type,
                            clock,
                        )?
                    } else if matches!(v, Expr::Param(n) if prepared.parameter_types.get(n - 1).is_some_and(Option::is_some))
                    {
                        assign::plain_value(
                            self,
                            v,
                            &t.columns[*i],
                            &prepared.parameter_types,
                            params,
                            clock,
                        )?
                    } else {
                        v.value(params)?
                    };
                }
                let mut next_version = self.next_version;
                let heap_version = if let Some(index) = t.hidden_rowid() {
                    let version = allocate(&mut next_version)?;
                    row[index] =
                        Value::Int(i64::try_from(version).map_err(|_| SqlError::Overflow)?);
                    Some(version)
                } else {
                    None
                };
                for (value, column) in row.iter_mut().zip(&t.columns) {
                    *value = coerce_to_column(self, column, value.clone())?;
                }
                row_check(t, &row)?;
                let key = pk(t, &row)?;
                let table_id = t.id;
                let legacy = scan::LegacyRows(&*self);
                let source = injected_source.unwrap_or(&legacy);
                if source.contains(t, &key)? {
                    return Err(SqlError::Duplicate);
                }
                let version = match heap_version {
                    Some(version) => version,
                    None => allocate(&mut next_version)?,
                };
                let row = SqlRow {
                    values: row,
                    version,
                };
                returned_rows = returning::project(&returning, self, params, &[&row])?;
                result = self.secondary_apply_statement(
                    table_id,
                    None,
                    Some((&key, &row)),
                    next_version,
                    &mut cancel,
                )?;
            }
            Statement::Select {
                name,
                projection,
                predicates,
                order,
                limit,
            } => {
                if projection.len() > MAX_COLUMNS {
                    return Err(SqlError::Limit);
                }
                if expected.is_some() {
                    return Err(SqlError::Unsupported);
                }
                let t = self.tables.get(name).ok_or(SqlError::UnknownTable)?;
                if t.hidden_rowid().is_some() {
                    return Err(SqlError::Unsupported);
                }
                let b = bounds(t, predicates, params)?;
                let limit = match limit {
                    Some(e) => match e.value(params)? {
                        Value::Int(n) if n >= 0 && (n as u64) <= MAX_ROWS as u64 => n as usize,
                        _ => return Err(SqlError::Limit),
                    },
                    None if b.point => 1,
                    None => return Err(SqlError::Unbounded),
                };
                let indexes = if projection.is_empty() {
                    (0..t.columns.len()).collect::<Vec<_>>()
                } else {
                    projection
                        .iter()
                        .map(|c| col(t, c))
                        .collect::<R<Vec<_>>>()?
                };
                let desc = order.first().is_some_and(|v| v.1);
                if !order.is_empty() {
                    let ordered = order
                        .iter()
                        .map(|(c, d)| {
                            if *d != desc {
                                return Err(SqlError::Unsupported);
                            }
                            col(t, c)
                        })
                        .collect::<R<Vec<_>>>()?;
                    let eq = predicates.iter().filter(|p| p.op == "=").count();
                    let suffix = &t.primary_key[eq.min(t.primary_key.len())..];
                    if ordered != t.primary_key && ordered != suffix {
                        return Err(SqlError::Unsupported);
                    }
                }
                result.columns = indexes.iter().map(|i| t.columns[*i].name.clone()).collect();
                let legacy = scan::LegacyRows(&*self);
                let source = injected_source.unwrap_or(&legacy);
                let iter: Box<dyn Iterator<Item = R<scan::SourceRow<'_>>>> = if b.point {
                    Box::new(source.get(t, &b.low)?.into_iter().map(Ok))
                } else {
                    let range = scan::KeyRange::from_bounds(b);
                    let direction = if desc {
                        scan::Direction::Reverse
                    } else {
                        scan::Direction::Forward
                    };
                    source.scan(t, &range, direction)?
                };
                let mut bytes = 0usize;
                for row in iter.take(limit) {
                    let row = row?;
                    let r = &row.row;
                    bytes += indexes
                        .iter()
                        .map(|i| value_bytes(&r.values[*i]))
                        .sum::<usize>();
                    if bytes > MAX_RESULT_BYTES {
                        return Err(SqlError::Limit);
                    }
                    let values: Vec<Value> = indexes.iter().map(|i| r.values[*i].clone()).collect();
                    result.rows.push(SqlRow {
                        values,
                        version: r.version,
                    });
                }
            }
            Statement::Update {
                name, predicates, ..
            }
            | Statement::UpdateExpressions {
                name, predicates, ..
            } => {
                let t = self.tables.get(name).ok_or(SqlError::UnknownTable)?;
                let b = match bounds(t, predicates, params) {
                    Ok(b)
                        if b.point
                            && !searched::target(prepared)
                                .is_some_and(query::Query::has_in_subquery) =>
                    {
                        b
                    }
                    other => {
                        return searched::route(
                            self,
                            prepared,
                            params,
                            expected,
                            &returning,
                            other.map(|_| ()),
                            &mut cancel,
                            clock,
                            injected_source,
                            events,
                        );
                    }
                };
                let assignments = assign::Plan::new(t, self, prepared, params, clock)?;
                let table_id = t.id;
                let legacy = scan::LegacyRows(&*self);
                let source = injected_source.unwrap_or(&legacy);
                let old = source.get(t, &b.low)?.map(|row| row.row.into_owned());
                if let Some(want) = expected
                    && old.as_ref().is_none_or(|r| r.version != want)
                {
                    return Err(SqlError::Version);
                }
                if let Some(old) = old {
                    let indexes = assignments.evaluate(t, &old, params, &mut cancel)?;
                    let mut row = old.clone();
                    for (i, v) in indexes {
                        row.values[i] = v;
                    }
                    row_check(t, &row.values)?;
                    self.check_insert_candidate(t, &row.values)?;
                    let new_key = pk(t, &row.values)?;
                    // One-row UPDATE: per-row and statement-end UNIQUE checking coincide, so the
                    // existing final-batch validation is exact here; no UpdateUnique occupancy needed.
                    let mut next_version = self.next_version;
                    row.version = allocate(&mut next_version)?;
                    returned_rows = returning::project(&returning, self, params, &[&row])?;
                    events.push(constraints::Event {
                        table: table_id,
                        old: old.clone(),
                        new: Some(row.clone()),
                    });
                    // Witness ownership is admitted by secondary_admit_mutation before
                    // expression/row construction; stage checks its exact raw value.
                    result = if new_key == b.low {
                        self.secondary_apply_statement(
                            table_id,
                            Some((&b.low, &old)),
                            Some((&new_key, &row)),
                            next_version,
                            &mut cancel,
                        )?
                    } else {
                        use secondary_index_adapter::BatchRowChange;
                        let changes = [
                            BatchRowChange {
                                table: table_id,
                                old: Some((&b.low, &old)),
                                new: None,
                            },
                            BatchRowChange {
                                table: table_id,
                                old: None,
                                new: Some((&new_key, &row)),
                            },
                        ];
                        let mut result = self.secondary_apply_batch_engine(
                            self.epoch,
                            &changes,
                            next_version,
                            &mut cancel,
                        )?;
                        result.affected = 1;
                        result
                    };
                } else {
                    result = self.secondary_apply_statement(
                        table_id,
                        None,
                        None,
                        self.next_version,
                        &mut cancel,
                    )?;
                }
            }
            Statement::Delete {
                name, predicates, ..
            } => {
                let t = self.tables.get(name).ok_or(SqlError::UnknownTable)?;
                let b = match bounds(t, predicates, params) {
                    Ok(b)
                        if b.point
                            && !searched::target(prepared)
                                .is_some_and(query::Query::has_in_subquery) =>
                    {
                        b
                    }
                    other => {
                        return searched::route(
                            self,
                            prepared,
                            params,
                            expected,
                            &returning,
                            other.map(|_| ()),
                            &mut cancel,
                            clock,
                            injected_source,
                            events,
                        );
                    }
                };
                let table_id = t.id;
                let legacy = scan::LegacyRows(&*self);
                let source = injected_source.unwrap_or(&legacy);
                let old = source.get(t, &b.low)?.map(|row| row.row.into_owned());
                if let Some(want) = expected
                    && old.as_ref().is_none_or(|r| r.version != want)
                {
                    return Err(SqlError::Version);
                }
                returned_rows =
                    returning::project(&returning, self, params, &old.iter().collect::<Vec<_>>())?;
                result = self.secondary_apply_statement(
                    table_id,
                    old.as_ref().map(|r| (b.low.as_slice(), r)),
                    None,
                    self.next_version,
                    &mut cancel,
                )?;
            }
            _ => return Err(SqlError::Transaction),
        }
        returning::attach(&returning, &mut result, returned_rows);
        Ok(result)
    }
}

#[cfg(test)]
mod arithmetic_lexer_tests {
    use super::*;

    #[test]
    fn operators_and_comments_are_distinct_tokens() {
        assert_eq!(
            lex("1-2 +3 /4 %5 ||6 *7 -- ignored / %\n /* outer /* nested */ end */ -8").unwrap(),
            vec![
                Token::Number("1".into()),
                Token::Op("-".into()),
                Token::Number("2".into()),
                Token::Op("+".into()),
                Token::Number("3".into()),
                Token::Op("/".into()),
                Token::Number("4".into()),
                Token::Op("%".into()),
                Token::Number("5".into()),
                Token::Op("||".into()),
                Token::Number("6".into()),
                Token::Symbol('*'),
                Token::Number("7".into()),
                Token::Op("-".into()),
                Token::Number("8".into()),
            ]
        );
        assert_eq!(lex("1 /* unterminated /* nested */"), Err(SqlError::Syntax));
        assert_eq!(
            lex("'-- /* */'"),
            Ok(vec![Token::String("-- /* */".into())])
        );
    }

    #[test]
    fn legacy_signed_number_preserves_minimum_integer() {
        for (sql, expected) in [
            ("-9223372036854775808", i64::MIN),
            ("+7", 7),
            ("- /*gap*/ 7", -7),
        ] {
            let mut parser = Parser {
                tokens: lex(sql).unwrap(),
                at: 0,
                params: BTreeSet::new(),
                relations: Vec::new(),
            };
            assert_eq!(
                parser.expr().unwrap().value(&[]).unwrap(),
                Value::Int(expected)
            );
            assert_eq!(parser.at, parser.tokens.len());
        }
    }
}

#[cfg(test)]
mod catalog_ext_tests;

#[cfg(test)]
mod catalog_names_tests;
#[cfg(test)]
mod describe_create_tests;

#[cfg(test)]
mod catalog_typmods_tests;

#[cfg(test)]
mod text_kind_tests;

pub mod logical_types;

#[cfg(test)]
#[path = "storage/v2_join_cache_tests.rs"]
pub(crate) mod v2_join_cache_tests;

#[cfg(all(test, feature = "engine-rocksdb"))]
pub(crate) use query::select_points::{DISABLE_SELECT_POINTS, SELECT_POINT_READS, SELECT_FILTER_SKIPS};
