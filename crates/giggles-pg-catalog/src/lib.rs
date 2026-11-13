//! Read-only, bounded PostgreSQL metadata over an already authorized snapshot.
//! No storage, sessions, SQL forwarding, or authority is held by this crate.
//!
//! `plan` returns `None` for parsed ordinary application statements. Recognized
//! catalog statements must be a single read-only SELECT; application relations
//! require caller-supplied authorized schemas and rows. Unknown columns/functions, writes, locking, and unsupported
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
//! are the seventeen `ScalarType` variants. Catalog arrays, vectors, and rowtype
//! identities do not grant additional application SQL types. No PostgreSQL MVCC
//! tuple exists: `xmin` exposes the durable catalog generation for conservative refresh.
//! General transaction IDs, timestamp functions, procedural execution,
//! arbitrary SQL/data access, and unmodeled DataGrip discovery remain unsupported.
mod catalog;
pub use engine::catalog_identifier;
mod engine;
pub use engine::{
    Plan, describe, execute, execute_with_startup, plan, plan_gateway, plan_gateway_with_relations,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarType {
    Uuid,
    Jsonb,
    IntArray,
    BigIntArray,
    TextArray,

    Boolean,
    SmallInt,
    Integer,
    BigInt,
    Text,
    Varchar,
    Bpchar,
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexDef {
    pub id: u64,
    pub table: u64,
    pub name: String,
    /// Ordered stable column IDs, not attribute numbers.
    pub columns: Vec<u64>,
    pub unique: bool,
}
/// Read-only CHECK/FK/UNIQUE metadata, using stable core IDs and PostgreSQL action codes.
#[derive(Debug, Clone, Default)]
pub struct ConstraintDef {
    pub id: u64, pub table: u64, pub name: String, pub kind: char,
    pub columns: Vec<u64>, pub foreign_table: u64, pub foreign_columns: Vec<u64>,
    pub update: char, pub delete: char, pub deferrable: bool, pub deferred: bool,
    pub validated: bool, pub expression: Option<String>, pub index: u64,
}
/// Presentation metadata only; these flags do not implement role privileges or policies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoleDef {
    pub oid: u32,
    pub name: String,
    pub superuser: bool,
    pub login: bool,
    pub createrole: bool,
    pub createdb: bool,
    pub inherit: bool,
    pub replication: bool,
    pub bypassrls: bool,
    pub connlimit: i32,
}
/// Presentation of a user-defined type; enum labels are in declaration order.
#[derive(Debug, Clone)]
pub struct UserType {
    pub schema: String,
    pub oid: u32,
    pub name: String,
    pub kind: char,
    pub base: u32,
    pub typmod: i32,
    pub not_null: bool,
    pub labels: Vec<String>,
}
/// The caller must supply a coherent, freshly authorized set, including on repeated Execute.
/// Roles include exactly one bootstrap role (OID 10); predefined PostgreSQL roles
/// are supplied by the catalog. Ownership references snapshot roles, with omitted
/// table owners defaulting to OID 10. `user` independently controls current_user;
/// none of this metadata establishes authentication, membership, or privileges.
#[derive(Debug, Clone)]
pub struct CatalogSnapshot {
    /// Catalog xid: legacy publication counter or V2 physical HLC seconds.
    /// xmin exposes its low 32 bits; this is never a raw HLC timestamp.
    pub generation: u64,
    /// View IDs identify descriptors in tables; definitions are SELECT text.
    pub views: std::collections::BTreeMap<u64, String>,
    pub constraints: Vec<ConstraintDef>,
    pub column_defaults: std::collections::BTreeMap<u64, String>,
    pub column_identities: std::collections::BTreeMap<u64, char>,
    pub primary_names: std::collections::BTreeMap<u64, String>,
    /// Stable (table ID, column ID), with zero identifying a table comment.
    pub comments: std::collections::BTreeMap<(u64, u64), String>,
    /// Current wire backend identity, supplied by the host (0 in offline snapshots).
    pub backend_pid: i32,
    /// Whether this connection actually completed a TLS handshake.
    pub backend_ssl: bool,
    /// Freshly authorized application rows, supplied by the hosting session.
    pub user_relations: std::collections::BTreeMap<String, ResultSet>,
    pub user_types: Vec<UserType>,
    pub column_type_oids: std::collections::BTreeMap<u64, u32>,
    pub tables: Vec<TableDef>,
    pub schemas: std::collections::BTreeMap<String, (u32, u32)>,
    pub table_schemas: std::collections::BTreeMap<u64, String>,
    pub indexes: Vec<IndexDef>,
    pub user: String,
    pub database: String,
    /// Existing schemas in the session's explicit search path, in lookup order.
    pub search_path: Vec<String>,
    /// All snapshot login and nonlogin roles, including exactly one OID 10.
    /// Defaults to `giggles` (OID 10), all flags true and connlimit -1.
    pub roles: Vec<RoleDef>,
    /// Table ID to snapshot role OID; missing entries use OID 10. Empty by default.
    pub table_owners: std::collections::BTreeMap<u64, u32>,
    /// Owner of the current pg_database row (`datdba`); defaults to OID 10.
    pub database_owner: u32,
    /// Stable column ID to PostgreSQL typmod; omitted entries mean -1.
    pub column_typmods: std::collections::BTreeMap<u64, i32>,
}
impl CatalogSnapshot {
    pub fn table_schema(&self, id: u64) -> (&str, u32) {
        self.table_schemas
            .get(&id)
            .and_then(|name| self.schemas.get(name).map(|(oid, _)| (name.as_str(), *oid)))
            .unwrap_or(("public", 2200))
    }
}
impl Default for CatalogSnapshot {
    fn default() -> Self {
        Self {
            generation: 0,
            views: Default::default(),
            constraints: Vec::new(), column_defaults: Default::default(),
            column_identities: Default::default(), primary_names: Default::default(),
            comments: Default::default(),
            backend_pid: 0,
            backend_ssl: false,
            user_relations: Default::default(),
            user_types: Vec::new(),
            column_type_oids: std::collections::BTreeMap::new(),
            tables: Vec::new(),
            schemas: Default::default(),
            table_schemas: Default::default(),
            indexes: Vec::new(),
            user: "giggles".into(),
            database: "giggles".into(),
            search_path: vec!["public".into()],
            roles: vec![RoleDef {
                oid: 10,
                name: "giggles".into(),
                superuser: true,
                login: true,
                createrole: true,
                createdb: true,
                inherit: true,
                replication: true,
                bypassrls: true,
                connlimit: -1,
            }],
            table_owners: std::collections::BTreeMap::new(),
            database_owner: 10,
            column_typmods: std::collections::BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgType {
    Bool,
    Int2,
    Int4,
    Int8,
    Oid,
    Text,
    Varchar,
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
    /// Metadata descriptor for finite catalog handler OIDs, not a routine registry.
    Regproc,
    /// Metadata descriptor only; non-NULL wire I/O is unsupported.
    PgNodeTree,
    /// Metadata descriptor only; missing-value arrays are always NULL.
    AnyArray,
    /// Metadata descriptor only; subscription LSNs are always NULL.
    PgLsn,
    /// Metadata descriptor only; lock wait timestamps are always NULL.
    TimestampTz,
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
            Self::Varchar => 1043,
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
            Self::Regproc => 24,
            Self::PgNodeTree => 194,
            Self::AnyArray => 2277,
            Self::PgLsn => 3220,
            Self::TimestampTz => 1184,
        }
    }
    pub fn type_size(self) -> i16 {
        match self {
            Self::Bool | Self::Char => 1,
            Self::Int2 => 2,
            Self::Int4 | Self::Oid | Self::Xid | Self::Float4 | Self::Regproc => 4,
            Self::Int8 | Self::Float8 | Self::PgLsn | Self::TimestampTz => 8,
            Self::Name => 64,
            _ => -1,
        }
    }
    /// Non-null array wire encoders can use this instead of matching each variant.
    /// AclItemArray stores ACL text elements and has no scalar PgType variant.
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
    DivisionByZero,
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

/// PostgreSQL xidin's unsigned 32-bit input, including signed compatibility,
/// C-style hexadecimal/octal prefixes and surrounding ASCII whitespace.
pub fn parse_xid(text: &str) -> Result<u32, Error> {
    let text = text.trim_matches([' ', '\t', '\n', '\r', '\x0b', '\x0c']);
    let (negative, digits) = if let Some(digits) = text.strip_prefix('-') {
        (true, digits)
    } else {
        (false, text.strip_prefix('+').unwrap_or(text))
    };
    let (radix, digits) = if let Some(digits) = digits
        .strip_prefix("0x")
        .or_else(|| digits.strip_prefix("0X"))
    {
        (16, digits)
    } else if digits.starts_with('0') && digits.len() > 1 {
        (8, &digits[1..])
    } else {
        (10, digits)
    };
    if digits.is_empty() || !digits.bytes().all(|b| (b as char).is_digit(radix)) {
        return Err(Error::Type);
    }
    let value = u32::from_str_radix(digits, radix).map_err(|_| Error::Type)?;
    if negative {
        if value > 1 << 31 {
            return Err(Error::Type);
        }
        Ok(0u32.wrapping_sub(value))
    } else {
        Ok(value)
    }
}
