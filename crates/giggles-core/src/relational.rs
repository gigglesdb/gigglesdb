//! Bounded trusted relational subset. This module grants no task authority.
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
mod assign;
pub(crate) mod clock;
pub use clock::StatementClock;
#[allow(dead_code)]
pub(crate) mod catalog_ext;
use catalog_ext::*;
mod ident;
use ident::ident;
mod numeric;
mod projector_inspection;
pub(crate) mod projector_reconstruction;
mod query;
use query::{TypmodSource, result_typmod};
mod returning;
mod scan;
mod searched;
use returning::Returning;
// Candidate codec/read APIs await later internal release.
#[allow(dead_code)]
pub(crate) mod secondary_index;
pub(crate) mod secondary_index_access;
mod secondary_index_adapter;
pub(crate) use secondary_index_adapter::BatchRowChange;
mod secondary_index_catalog;
mod secondary_index_unique_readiness;
pub(crate) mod secondary_unique_lookup;
mod sql_index;
mod sql_insert;
pub(crate) use giggles_pg_types::temporal::Time;
pub use giggles_pg_types::temporal::{
    Date, Interval, Time as PgTime, TimeTz, Timestamp, TimestampTz,
};
pub use giggles_pg_types::{Float4, Float8, Numeric};

pub const MAX_SQL: usize = 64 * 1024;
pub const MAX_ROWS: usize = 1024;
pub const MAX_ROW_BYTES: usize = 64 * 1024;
pub const MAX_COLUMNS: usize = 64;
pub const MAX_PARAMS: usize = 256;
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
            rows: BTreeMap::new(),
            ext: CatalogExt::default(),
            secondary: secondary_index::Sidecar::default(),
        }
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
        | (DataType::Text, Value::Text(_))
        | (DataType::Bytes, Value::Bytes(_)) => true,
        (DataType::SmallInt, Value::Int(n)) => i16::try_from(*n).is_ok(),
        (DataType::Integer, Value::Int(n)) => i32::try_from(*n).is_ok(),
        _ => false,
    };
    if yes { Ok(()) } else { Err(SqlError::Type) }
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
    if s.len() > MAX_SQL {
        return Err(SqlError::Limit);
    }
    let b = s.as_bytes();
    let mut i = 0;
    let mut out = Vec::new();
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
        } else if b"(),*;.:".contains(&c) {
            out.push(Token::Symbol(c as char));
            i += 1;
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
        if out.len() > 4096 {
            return Err(SqlError::Limit);
        }
    }
    Ok(out)
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
    CreateIndex(sql_index::CreateIndex),
    InsertMulti(sql_insert::Insert),
    Query(query::Query),
    Create {
        name: String,
        columns: Vec<ColumnDef>,
        typmods: Vec<Option<i32>>,
        keys: Vec<String>,
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
}
struct Parser {
    tokens: Vec<Token>,
    at: usize,
    params: BTreeSet<usize>,
}
impl Parser {
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
        if matches!(self.tokens.get(self.at), Some(Token::Word(word)) if word == "insert")
            && sql_insert::claims(&self.tokens)
        {
            return sql_insert::parse(self).map(Statement::InsertMulti);
        }
        if matches!(self.tokens.get(self.at), Some(Token::Word(w)) if w == "create")
            && matches!(self.tokens.get(self.at + 1), Some(Token::Word(w)) if w == "index" || w == "unique")
        {
            return sql_index::parse(self).map(Statement::CreateIndex);
        }
        let st = if self.word("create") {
            self.need("table")?;
            let name = self.create_table_name()?;
            self.punct('(')?;
            let mut columns = vec![];
            let mut keys = vec![];
            loop {
                if self.word("primary") {
                    self.need("key")?;
                    if !keys.is_empty() {
                        return Err(SqlError::Duplicate);
                    }
                    keys = self.names()?;
                } else {
                    let name = self.name()?;
                    let data_type = if self.word("boolean") || self.word("bool") {
                        DataType::Boolean
                    } else if self.word("smallint") {
                        DataType::SmallInt
                    } else if self.word("integer") || self.word("int") {
                        DataType::Integer
                    } else if self.word("bigint") {
                        DataType::BigInt
                    } else if self.word("numeric") || self.word("decimal") {
                        DataType::Numeric
                    } else if self.word("real") || self.word("float4") {
                        DataType::Real
                    } else if self.word("double") {
                        self.need("precision")?;
                        DataType::DoublePrecision
                    } else if self.word("float8") {
                        DataType::DoublePrecision
                    } else if self.word("date") {
                        DataType::Date
                    } else if self.word("time") {
                        DataType::Time
                    } else if self.word("timetz") {
                        DataType::TimeTz
                    } else if self.word("timestamp") {
                        DataType::Timestamp
                    } else if self.word("timestamptz") {
                        DataType::TimestampTz
                    } else if self.word("interval") {
                        DataType::Interval
                    } else if self.word("text") {
                        DataType::Text
                    } else if self.word("bytea") || self.word("bytes") {
                        DataType::Bytes
                    } else {
                        return Err(SqlError::Unsupported);
                    };
                    let mut not_null = false;
                    if self.word("not") {
                        self.need("null")?;
                        not_null = true;
                    } else {
                        self.word("null");
                    }
                    if self.word("primary") {
                        self.need("key")?;
                        if !keys.is_empty() {
                            return Err(SqlError::Duplicate);
                        }
                        keys.push(name.clone());
                    }
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
                typmods: vec![None; columns.len()],
                columns,
                keys,
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
            let name = self.table_name()?;
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
        } else if self.word("delete") {
            self.need("from")?;
            let name = self.table_name()?;
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
        } else if self.word("rollback") {
            Statement::Rollback
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
    params: usize,
    parameter_types: Vec<Option<DataType>>,
}
#[derive(Debug, Clone)]
pub struct Description {
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
        if columns
            .iter()
            .any(|column| column.name == *key && column.data_type.is_temporal())
        {
            return Err(SqlError::Unsupported);
        }
        if !columns.iter().any(|column| column.name == *key) {
            return Err(SqlError::UnknownColumn);
        }
    }
    Ok(())
}
impl Prepared {
    fn engine(statement: Statement, domain: u128, epoch: u64) -> Self {
        Self {
            domain,
            epoch,
            transaction: None,
            statement,
            params: 0,
            parameter_types: vec![],
        }
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
                typmods: vec![None; columns.len()],
                columns,
                keys,
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
    fn engine_table(table: &TableDef, state: &Relational) -> R<()> {
        // Callers cannot smuggle stale IDs, column types or unchecked key indexes
        // into the executor through a hand-built descriptor.
        if state.tables.get(&table.name) != Some(table) {
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
        if self.epoch != state.epoch {
            return Err(SqlError::StaleSchema);
        }
        if let Statement::InsertExpressions(values) = &self.statement {
            return returning::validate_insert_constants(values, state, self.params);
        }
        if let Statement::Query(q) = &self.statement {
            return q
                .bind(state, self.params, &self.parameter_types)?
                .validate_immutable_constants();
        }
        Ok(())
    }
    pub(crate) fn describe(&self, state: &Relational) -> R<Description> {
        if self.epoch != state.epoch {
            return Err(SqlError::StaleSchema);
        }
        if let Statement::InsertMulti(insert) = &self.statement {
            return insert.describe(state, self.params);
        }
        if let Statement::InsertExpressions(values) = &self.statement {
            return returning::describe_insert(values, state, self.params);
        }
        if let Statement::Query(q) = &self.statement {
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
                        bind(e, &t.columns[i].data_type)?;
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
                        bind(e, &t.columns[col(t, n)?].data_type)?;
                    }
                    for p in predicates {
                        bind(&p.value, &t.columns[col(t, &p.column)?].data_type)?;
                    }
                }
                Statement::Delete { predicates, .. } => {
                    for p in predicates {
                        bind(&p.value, &t.columns[col(t, &p.column)?].data_type)?;
                    }
                }
                _ => {}
            }
        }
        if let Some(returning) = self.returning() {
            let (columns, parameters, typmods) = returning.describe(state, params)?;
            return Ok(Description {
                typmods,
                columns,
                parameters,
            });
        }
        Ok(Description {
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
    /// Parsed table target for gateway authorization. Controls have no target.
    pub fn table(&self) -> Option<&str> {
        match &self.statement {
            Statement::Query(q) => {
                if q.is_table_free() {
                    None
                } else {
                    Some(q.target())
                }
            }
            Statement::InsertExpressions(values) => Some(&values.name),
            Statement::CreateIndex(index) => Some(&index.table),
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
    pub fn is_ddl(&self) -> bool {
        matches!(
            self.statement,
            Statement::Create { .. } | Statement::CreateIndex(_)
        )
    }
    pub fn command(&self) -> &'static str {
        match self.statement {
            Statement::Query(ref q) => {
                if q.explain {
                    "EXPLAIN"
                } else {
                    "SELECT"
                }
            }
            Statement::Create { .. } => "CREATE TABLE",
            Statement::CreateIndex(_) => "CREATE INDEX",
            Statement::Insert { .. }
            | Statement::InsertExpressions(_)
            | Statement::InsertMulti(_) => "INSERT",
            Statement::Select { .. } => "SELECT",
            Statement::Update { .. } | Statement::UpdateExpressions { .. } => "UPDATE",
            Statement::Delete { .. } => "DELETE",
            Statement::Begin => "BEGIN",
            Statement::Commit => "COMMIT",
            Statement::Rollback => "ROLLBACK",
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
    /// Declare input types for SELECT and UPDATE parameters before type inference.
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
            Statement::Query(_) | Statement::Update { .. } | Statement::UpdateExpressions { .. }
        ) {
            self.parameter_types = types.to_vec();
        }
        Ok(self)
    }
    pub fn parameter_count(&self) -> usize {
        self.params
    }
    /// Parse-only negative routing hint. No binding, evaluation, metadata or authority.
    /// Parse failures and dependency-bearing queries keep their existing gateway path.
    pub fn rejects_unsupported_table_free_scalar(sql: &str) -> bool {
        Self::parse(sql, 0, 0).is_ok_and(|prepared| {
            matches!(&prepared.statement,
                Statement::Query(query) if query.rejects_unsupported_table_free_scalar())
        })
    }
    pub(crate) fn parse(sql: &str, domain: u128, epoch: u64) -> R<Self> {
        assert_sql_frontend_allowed();
        let mut p = Parser {
            tokens: lex(sql)?,
            at: 0,
            params: BTreeSet::new(),
        };
        let statement = if matches!(p.tokens.first(), Some(Token::Word(word)) if word == "select" || word == "explain")
        {
            Statement::Query(query::Query::parse(&mut p)?)
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
        })
    }
    pub(crate) fn control(&self) -> Option<&'static str> {
        match self.statement {
            Statement::Begin => Some("begin"),
            Statement::Commit => Some("commit"),
            Statement::Rollback => Some("rollback"),
            _ => None,
        }
    }
    pub(crate) fn is_read(&self) -> bool {
        matches!(
            self.statement,
            Statement::Select { .. } | Statement::Query(_)
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
        let v = p.value.value(params)?;
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
    pub(crate) fn drop_table(&mut self, name: &str) -> R<bool> {
        let Some(table) = self.tables.get(name) else {
            return Ok(false);
        };
        let table_id = table.id;
        let epoch = self.epoch.checked_add(1).ok_or(SqlError::Overflow)?;
        let mut ext = self.ext.clone();
        for column in &table.columns {
            ext.columns.remove(&column.id);
        }
        ext.constraints.remove(&table_id);
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
            ident(name)?;
            if name != &t.name
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
        mut cancel: impl FnMut() -> bool,
        clock: Option<&clock::ExecutionClock>,
    ) -> R<SqlResult> {
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
        payload_bound(params.iter())?;
        let returning = returning::Plan::new(prepared, self, params, clock)?;
        let mut returned_rows = vec![];
        // Normalize expression outputs after mutation input admission.
        let normalized;
        let prepared = if let Statement::InsertExpressions(values) = &prepared.statement {
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
            Statement::Query(q) => {
                if expected.is_some() {
                    return Err(SqlError::Unsupported);
                }
                return q
                    .bind(self, prepared.params, &prepared.parameter_types)?
                    .with_clock(clock)
                    .execute(self, params, &mut cancel);
            }
            Statement::InsertExpressions(_) => return Err(SqlError::Unsupported),
            Statement::InsertMulti(insert) => {
                return sql_insert::execute(
                    self,
                    insert,
                    params,
                    expected,
                    &returning,
                    &mut cancel,
                );
            }
            Statement::CreateIndex(index) => {
                if expected.is_some() {
                    return Err(SqlError::Unsupported);
                }
                if index.if_not_exists
                    && self
                        .secondary_descriptors_engine()
                        .iter()
                        .any(|d| d.name == index.name)
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
                columns,
                typmods,
                keys,
            } => {
                if expected.is_some() {
                    return Err(SqlError::Unsupported);
                }
                if self.relation_name_taken(name) {
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
                validate_create_definition(name, columns, keys)?;
                let mut next_id = self.next_id;
                let mut t = TableDef {
                    id: allocate(&mut next_id)?,
                    name: name.clone(),
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
                // Typmods were validated above against these exact column types, so the ext
                // update cannot fail; no whole-catalog clone is needed for atomicity.
                for (column, typmod) in t.columns.iter().zip(typmods) {
                    if let Some(typmod) = typmod {
                        self.ext.columns.entry(column.id).or_default().typmod = Some(*typmod);
                    }
                }
                self.next_id = next_id;
                self.epoch = epoch;
                self.rows.insert(t.id, BTreeMap::new());
                self.tables.insert(name.clone(), t);
                debug_assert!(self.validate_ext().is_ok());
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
                let mut row = vec![Value::Null; t.columns.len()];
                for (i, v) in indexes.iter().zip(values) {
                    row[*i] = if matches!(v, Expr::Literal(_))
                        && matches!(
                            t.columns[*i].data_type,
                            DataType::Numeric | DataType::Real | DataType::DoublePrecision
                        )
                        || matches!(v, Expr::Literal(_)) && t.columns[*i].data_type.is_temporal()
                    {
                        query::coerce(v.value(params)?, &t.columns[*i].data_type)?
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
                row_check(t, &row)?;
                let key = pk(t, &row)?;
                let table_id = t.id;
                let rows = self.rows.get(&table_id).ok_or(SqlError::UnknownTable)?;
                if rows.contains_key(&key) {
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
                let rows = self.rows.get(&t.id).ok_or(SqlError::UnknownTable)?;
                if b.low > b.high || (b.low == b.high && !b.point) {
                    return Ok(result);
                }
                use std::ops::Bound::{Excluded, Included};
                let lo = if b.low_inclusive {
                    Included(b.low)
                } else {
                    Excluded(b.low)
                };
                let hi = if b.high_inclusive {
                    Included(b.high)
                } else {
                    Excluded(b.high)
                };
                let iter = rows.range((lo, hi));
                let iter: Box<dyn Iterator<Item = (&Vec<u8>, &SqlRow)>> = if desc {
                    Box::new(iter.rev())
                } else {
                    Box::new(iter)
                };
                let mut bytes = 0usize;
                for (_, r) in iter.take(limit) {
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
                    Ok(b) if b.point => b,
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
                        );
                    }
                };
                let assignments = assign::Plan::new(t, self, prepared, params, clock)?;
                let table_id = t.id;
                let rows = self.rows.get(&table_id).ok_or(SqlError::UnknownTable)?;
                let old = rows.get(&b.low);
                if let Some(want) = expected
                    && old.is_none_or(|r| r.version != want)
                {
                    return Err(SqlError::Version);
                }
                if let Some(old) = old {
                    let indexes = assignments.evaluate(t, old, params, &mut cancel)?;
                    let mut row = old.clone();
                    for (i, v) in indexes {
                        row.values[i] = v;
                    }
                    row_check(t, &row.values)?;
                    let mut next_version = self.next_version;
                    row.version = allocate(&mut next_version)?;
                    returned_rows = returning::project(&returning, self, params, &[&row])?;
                    // Witness cloning is admitted by secondary_admit_mutation before
                    // expression/row construction; stage checks its exact raw value.
                    let old = old.clone();
                    result = self.secondary_apply_statement(
                        table_id,
                        Some((&b.low, &old)),
                        Some((&b.low, &row)),
                        next_version,
                        &mut cancel,
                    )?;
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
                    Ok(b) if b.point => b,
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
                        );
                    }
                };
                let table_id = t.id;
                let rows = self.rows.get(&table_id).ok_or(SqlError::UnknownTable)?;
                let old = rows.get(&b.low);
                if let Some(want) = expected
                    && old.is_none_or(|r| r.version != want)
                {
                    return Err(SqlError::Version);
                }
                returned_rows = returning::project(
                    &returning,
                    self,
                    params,
                    &old.into_iter().collect::<Vec<_>>(),
                )?;
                let old = old.cloned();
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
