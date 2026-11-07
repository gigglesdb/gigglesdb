//! Read-only, bounded PostgreSQL metadata over an already authorized snapshot.
//! No storage, sessions, SQL forwarding, or authority is held by this crate.
//!
//! `plan` returns `None` for parsed ordinary application statements. Recognized
//! catalog statements must be a single read-only SELECT; mixed application
//! relations, unknown catalog columns/functions, writes, locking, and unsupported
//! expression forms fail explicitly. Nonrecursive CTEs, UNION, grouped/ordered
//! array_agg and lateral unnest form part of the finite metadata profile.
//! Parse errors are `Error::Syntax`, so the caller controls application fallback.
//! `describe` performs no execution; `execute` binds parameters and rebuilds the
//! virtual relations exclusively from the supplied snapshot on every invocation.
//! The caller owns fresh authorization, coherent schema snapshots, transaction
//! fencing, and admission of the complete result into its session/wire cache.
//!
//! Limits: 64 KiB SQL, 8192 flat tokens / 256 constructor tokens before parsing,
//! parser recursion 64, expression nesting 48 / 4096 AST
//! expressions, 32 sources and queries, 128 projection columns, 256 parameters,
//! 128 input tables / 64 columns each / 64 KiB total input names. Materialized
//! operator results admit at most 8192 rows / 4 MiB conservative escaped bytes;
//! execution also admits at most two million charged work units. Input values
//! have a 4096-node / 16-level / 4 MiB limit. Exceeding a limit returns no result.
//!
//! This is a finite virtual catalog, not a PostgreSQL server: application types
//! are the fifteen `ScalarType` variants. Catalog arrays, vectors, and rowtype
//! identities do not grant additional application SQL types. No PostgreSQL MVCC
//! tuple exists (`xmin` is NULL); transaction IDs, general timestamp functions, procedural execution,
//! arbitrary SQL/data access, and unmodeled DataGrip discovery remain unsupported.
mod catalog;
mod engine;
pub use engine::{Plan, describe, execute, execute_with_startup, plan, plan_gateway};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarType {
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDef {
    pub id: u64,
    pub name: String,
    pub data_type: ScalarType,
    pub not_null: bool,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableDef {
    pub id: u64,
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub primary_key: Vec<usize>,
}
/// The caller must supply a coherent, freshly authorized set, including on repeated Execute.
#[derive(Debug, Clone, Default)]
pub struct CatalogSnapshot {
    pub tables: Vec<TableDef>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgType {
    Bool,
    Int2,
    Int4,
    Int8,
    Oid,
    Text,
    Name,
    Char,
    Bytea,
    Int2Array,
    Int4Array,
    Int8Array,
    BoolArray,
    ByteaArray,
    CharArray,
    Int2Vector,
    OidVector,
    OidArray,
    TextArray,
    NameArray,
    AclItemArray,
    Xid,
    Record,
    Numeric,
    Float4,
    Float8,
}
impl PgType {
    pub fn oid(self) -> u32 {
        match self {
            Self::Bool => 16,
            Self::Bytea => 17,
            Self::Char => 18,
            Self::Name => 19,
            Self::Int8 => 20,
            Self::Int2 => 21,
            Self::Int4 => 23,
            Self::Text => 25,
            Self::Oid => 26,
            Self::Int2Array => 1005,
            Self::Int4Array => 1007,
            Self::Int8Array => 1016,
            Self::BoolArray => 1000,
            Self::ByteaArray => 1001,
            Self::CharArray => 1002,
            Self::Int2Vector => 22,
            Self::OidVector => 30,
            Self::OidArray => 1028,
            Self::TextArray => 1009,
            Self::NameArray => 1003,
            Self::AclItemArray => 1034,
            Self::Xid => 28,
            Self::Record => 2249,
            Self::Numeric => 1700,
            Self::Float4 => 700,
            Self::Float8 => 701,
        }
    }
    pub fn type_size(self) -> i16 {
        match self {
            Self::Bool | Self::Char => 1,
            Self::Int2 => 2,
            Self::Int4 | Self::Oid | Self::Xid | Self::Float4 => 4,
            Self::Int8 | Self::Float8 => 8,
            Self::Name => 64,
            _ => -1,
        }
    }
    /// Non-null array wire encoders can use this instead of matching each variant.
    /// AclItemArray is only used for NULL catalog cells and has no scalar variant.
    /// Int2Vector/OidVector use a space-separated representation, not array braces.
    pub fn array_element(self) -> Option<Self> {
        Some(match self {
            Self::Int2Array => Self::Int2,
            Self::Int4Array => Self::Int4,
            Self::Int8Array => Self::Int8,
            Self::OidArray => Self::Oid,
            Self::TextArray => Self::Text,
            Self::NameArray => Self::Name,
            Self::BoolArray => Self::Bool,
            Self::ByteaArray => Self::Bytea,
            Self::CharArray => Self::Char,
            _ => return None,
        })
    }
}
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Text(String),
    Bytes(Vec<u8>),
    Array(Vec<Value>),
    Record(Vec<Value>),
    Numeric(giggles_pg_types::Numeric),
    Float4(giggles_pg_types::Float4),
    Float8(giggles_pg_types::Float8),
}
impl Value {
    pub(crate) fn bytes(&self) -> usize {
        match self {
            Self::Null => 4,
            Self::Bool(_) => 1,
            Self::Int(_) => 20,
            Self::Numeric(v) => v.to_string().len(),
            Self::Float4(_) | Self::Float8(_) => 32,
            // Includes quoting/escaping when the value is nested in a PG text array.
            Self::Text(s) => s.len() * 2 + 8,
            Self::Bytes(b) => b.len() * 2 + 6,
            Self::Array(v) | Self::Record(v) => {
                v.iter().map(Self::bytes).sum::<usize>() + v.len() * 4 + 8
            }
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Field {
    pub name: String,
    pub data_type: PgType,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultSet {
    pub fields: Vec<Field>,
    pub rows: Vec<Vec<Value>>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Syntax,
    Unsupported,
    MixedRelations,
    Mutation,
    UnknownColumn,
    AmbiguousColumn,
    Type,
    Parameters,
    InvalidSnapshot,
    Limit,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "catalog {self:?}")
    }
}
impl std::error::Error for Error {}
pub(crate) type R<T> = Result<T, Error>;
pub const MAX_SQL: usize = 65536;
pub const MAX_ROWS: usize = 8192;
pub const MAX_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_WORK: usize = 2_000_000;
