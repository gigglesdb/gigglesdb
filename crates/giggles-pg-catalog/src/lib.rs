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
//! expressions, 32 sources and queries, 1664 projection columns, 256 parameters,
//! Input tables may have up to 1600 columns each. Catalog intermediates allow
//! 196608 rows and 384 MiB of owned rows for column metadata. Construction
//! allows 640 MiB for column metadata to handle about 100 full-width tables,
//! including the temporarily live sources of information_schema.columns.
//! Queries reading column metadata have a 64 million work-unit allowance.
//! Other queries allow two million work units. Projected results admit up to
//! 8192 rows and 4 MiB of conservatively escaped bytes. Input values have a
//! 4096-node / 16-level / 4 MiB limit. Exceeding a limit returns no result.
//!
//! This is a finite virtual catalog, not a PostgreSQL server: application types
//! are the seventeen `ScalarType` variants. Catalog arrays, vectors, and rowtype
//! identities do not grant additional application SQL types. No PostgreSQL MVCC
//! tuple exists: `xmin` exposes the durable catalog generation for conservative refresh.
//! General transaction IDs, timestamp functions, procedural execution,
//! arbitrary SQL/data access, and unmodeled DataGrip discovery remain unsupported.
mod catalog;
pub use catalog::routines::builtin_regproc_name;
mod timezone_abbrevs;
/// Git revision embedded at build time, or an explicit build identifier override.
pub const BUILD_ID: &str = env!("GIGGLES_BUILD_ID");
/// Each token names a GD-0077 guarantee; tokens are only ever added when the
/// guarantee ships with tests. Tokens are sorted and comma-separated without spaces.
pub const CAPABILITIES: &str =
    "add_column_if_not_exists,role_session_termination,server_encoding,union_common_type";

pub use engine::{catalog_identifier, resolve_type_names};
pub use engine::explain::{ExplainQuery, explain_query, explain_statement};
/// Quote an identifier for PostgreSQL plan text using the catalog's rules.
pub fn quote_explain_identifier(name: &str) -> String { catalog::quote_identifier(name) }
mod engine;
pub mod privilege_inquiry;
pub use engine::{
    Plan, application_read_hint, describe, execute, execute_with_startup, plan, plan_gateway,
    plan_gateway_with_relations, plan_task_gateway,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarType {
    Uuid,
    Jsonb,
    IntArray,
    BigIntArray,
    TextArray,
    UuidArray,
    TsVector,
    TsQuery,

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

#[derive(Debug, Clone)]
pub struct GinIndexMetadata {
    pub opclasses: Vec<u32>,
    pub options: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
pub struct GeneratedColumn {
    pub kind: char,
    pub expression: String,
    pub pretty_expression: String,
    pub references: Vec<u64>,
}
/// Persisted referenced key; index IDs identify secondary index descriptors.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ForeignKeyTarget { PrimaryKey, Index(u64) }
/// Read-only CHECK/FK/UNIQUE metadata, using stable core IDs and PostgreSQL action codes.
#[derive(Debug, Clone, Default)]
pub struct ConstraintDef {
    pub id: u64, pub table: u64, pub name: String, pub kind: char,
    pub columns: Vec<u64>, pub foreign_table: u64, pub foreign_columns: Vec<u64>,
    /// None denotes an unknown legacy binding.
    pub foreign_key_target: Option<ForeignKeyTarget>,
    pub update: char, pub delete: char, pub deferrable: bool, pub deferred: bool,
    pub validated: bool, pub expression: Option<String>, pub index: u64,
}
/// Role metadata used for catalog presentation and privilege inquiry functions.
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
/// Privileges representable in a PostgreSQL table or column ACL.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum AclPrivilege {
    Usage,
    Insert,
    Select,
    Update,
    Delete,
    Truncate,
    References,
    Trigger,
    Maintain,
}
impl AclPrivilege {
    pub(crate) fn code(self) -> char {
        match self {
            Self::Insert => 'a',
            Self::Select => 'r',
            Self::Update => 'w',
            Self::Delete => 'd',
            Self::Truncate => 'D',
            Self::References => 'x',
            Self::Trigger => 't',
            Self::Maintain => 'm',
            Self::Usage => 'U',
        }
    }
}
/// A grant from one role to another (grantee 0 means PUBLIC).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AclEntry {
    pub grantee: u32,
    pub grantor: u32,
    pub privileges: Vec<AclPrivilege>,
    pub grant_option: bool,
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
    pub label_oids: Vec<u32>,
}
/// A prepared statement visible only to its owning session.
#[derive(Debug, Clone)]
pub struct PreparedStatementRow {
    pub name: String,
    pub statement: String,
    pub prepare_time: i64,
    pub parameter_types: Vec<u32>,
    pub result_types: Option<Vec<u32>>,
    pub from_sql: bool,
    pub generic_plans: i64,
    pub custom_plans: i64,
}

/// The caller must supply a coherent, freshly authorized set, including on repeated Execute.
/// Roles include exactly one bootstrap role (OID 10); predefined PostgreSQL roles
/// are supplied by the catalog. Ownership references snapshot roles, with omitted
/// table owners defaulting to OID 10. `user` independently controls current_user;
/// the host remains responsible for authentication, membership, and authorization.
#[derive(Debug, Clone)]
pub struct CatalogSnapshot {
    pub routines: std::collections::BTreeMap<u32, RoutineDef>,
    pub triggers: Vec<TriggerDef>,
    pub sequences: Vec<SequenceDef>,
    /// Direct role/schema CREATE and USAGE grants, projected by the host.
    pub schema_privileges: std::collections::BTreeMap<(u32, String), [bool; 2]>,
    pub extensions: Vec<ExtensionDef>,
    /// Catalog xid: legacy publication counter or V2 physical HLC seconds.
    /// xmin exposes its low 32 bits; this is never a raw HLC timestamp.
    pub generation: u64,
    /// View IDs identify descriptors in tables; definitions are SELECT text.
    pub views: std::collections::BTreeMap<u64, String>,
    pub constraints: Vec<ConstraintDef>,
    pub column_defaults: std::collections::BTreeMap<u64, String>,
    pub generated_columns: std::collections::BTreeMap<u64, GeneratedColumn>,
    pub column_identities: std::collections::BTreeMap<u64, char>,
    pub primary_names: std::collections::BTreeMap<u64, String>,
    /// Stable (table ID, column ID), with zero identifying a table comment.
    pub comments: std::collections::BTreeMap<(u64, u64), String>,
    /// Current wire backend identity, supplied by the host (0 in offline snapshots).
    pub backend_pid: i32,
    /// Named prepared statements supplied by the host for the current session.
    pub prepared_statements: Vec<PreparedStatementRow>,
    /// Granted relation locks supplied by the engine: (transaction, table ID, pid, mode, fastpath).
    pub table_locks: Vec<(u64, u64, i32, String, bool)>,
    /// Optional explicit Unix second for deterministic timezone presentation.
    pub timezone_now: Option<i64>,
    /// Current session TimeZone, supplied by the host.
    pub timezone: giggles_pg_types::temporal::SessionZone,
    /// Whether this connection actually completed a TLS handshake.
    pub backend_ssl: bool,
    /// Freshly authorized application rows, supplied by the hosting session.
    pub user_relations: std::collections::BTreeMap<String, ResultSet>,
    pub user_types: Vec<UserType>,
    pub collations: Vec<Vec<Value>>,
    pub collation_oids: std::collections::BTreeMap<u64,Vec<u32>>,
    pub column_type_oids: std::collections::BTreeMap<u64, u32>,
    pub tables: Vec<TableDef>,
    pub schemas: std::collections::BTreeMap<String, (u32, u32)>,
    pub table_schemas: std::collections::BTreeMap<u64, String>,
    pub indexes: Vec<IndexDef>,
    pub index_predicates: std::collections::BTreeMap<u64, String>,
    pub gin_indexes: std::collections::BTreeMap<u64, GinIndexMetadata>,
    pub user: String,
    pub session_user: Option<String>,
    pub role_memberships: Vec<Vec<Value>>,
    pub role_privileges: std::collections::BTreeMap<(String, String), [bool; 4]>,
    pub database: String,
    /// Existing schemas in the session's explicit search path, in lookup order.
    pub search_path: Vec<String>,
    /// All snapshot login and nonlogin roles, including exactly one OID 10.
    /// Defaults to `giggles` (OID 10), all flags true and connlimit -1.
    pub roles: Vec<RoleDef>,
    /// Table ID to snapshot role OID; missing entries use OID 10. Empty by default.
    pub table_owners: std::collections::BTreeMap<u64, u32>,
    /// Explicit table ACLs keyed by stable table ID. Absent means NULL/default;
    /// a present empty vector means an explicitly empty ACL.
    pub table_acls: std::collections::BTreeMap<u64, Vec<AclEntry>>,
    /// Explicit column ACLs keyed by (stable table ID, stable column ID).
    pub column_acls: std::collections::BTreeMap<(u64, u64), Vec<AclEntry>>,
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
            routines: Default::default(),
            triggers: Vec::new(),
            sequences: Vec::new(),
            schema_privileges: Default::default(),
            extensions: vec![ExtensionDef::plpgsql(), ExtensionDef::uuid_ossp()],
            generation: 0,
            views: Default::default(),
            constraints: Vec::new(), column_defaults: Default::default(),
            gin_indexes: Default::default(),
            generated_columns: Default::default(),
            column_identities: Default::default(), primary_names: Default::default(),
            comments: Default::default(),
            backend_pid: 0,
            prepared_statements: Vec::new(),
            table_locks: Vec::new(),
            timezone_now: None,
            timezone: giggles_pg_types::temporal::SessionZone::Fixed(0),
            backend_ssl: false,
            user_relations: Default::default(),
            user_types: Vec::new(),
            collations: Vec::new(),
            collation_oids: Default::default(),
            column_type_oids: std::collections::BTreeMap::new(),
            tables: Vec::new(),
            schemas: Default::default(),
            table_schemas: Default::default(),
            indexes: Vec::new(),
            index_predicates: std::collections::BTreeMap::new(),
            user: "giggles".into(),
            session_user: None,
            role_privileges: Default::default(),
            role_memberships: Vec::new(),
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
            table_acls: Default::default(),
            column_acls: Default::default(),
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
    Regproc,
    Regtype,
    RegtypeArray,
    Regprocedure,
    /// Metadata descriptor only; non-NULL wire I/O is unsupported.
    PgNodeTree,
    /// Metadata descriptor only; missing-value arrays are always NULL.
    AnyArray,
    /// Metadata descriptor only; subscription LSNs are always NULL.
    PgLsn,
    /// Timezone offsets, represented as PostgreSQL interval text.
    Interval,
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
            Self::Regtype => 2206,
            Self::RegtypeArray => 2211,
            Self::Regprocedure => 2202,
            Self::PgNodeTree => 194,
            Self::AnyArray => 2277,
            Self::PgLsn => 3220,
            Self::TimestampTz => 1184,
            Self::Interval => 1186,
        }
    }
    pub fn type_size(self) -> i16 {
        match self {
            Self::Bool | Self::Char => 1,
            Self::Int2 => 2,
            Self::Int4 | Self::Oid | Self::Xid | Self::Float4 | Self::Regproc | Self::Regtype | Self::Regprocedure => 4,
            Self::Int8 | Self::Float8 | Self::PgLsn | Self::TimestampTz => 8,
            Self::Name => 64,
            Self::Interval => 16,
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
            Self::RegtypeArray => Self::Regtype,
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
    Regtype(u32, String),
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
            Self::Text(s) | Self::Regtype(_, s) => s.len() * 2 + 8,
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
    /// SQLSTATE for a privilege inquiry object lookup or kind error.
    Inquiry(&'static str),
    Syntax,
    Unsupported,
    MixedRelations,
    TaskRestrictedRelation,
    Mutation,
    UnknownColumn,
    UndefinedFunction(String),
    UnknownRelation(String),
    UnknownRole(String),
    AmbiguousColumn,
    DivisionByZero,
    Type,
    Parameters,
    InvalidSnapshot,
    InvalidPrivilege(String),
    InvalidTypeModifier(String),
    TypeModifierNotAllowed(String),
    TypeNameSyntax { near: &'static str, input: Option<String> },
    TooManyColumns,
    Limit,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Self::UnknownRole(name) = self { return write!(f, "role \"{name}\" does not exist"); }
        if let Self::UndefinedFunction(message) | Self::InvalidPrivilege(message) | Self::InvalidTypeModifier(message) = self {
            return f.write_str(message);
        }
        if let Self::TypeModifierNotAllowed(name) = self {
            return write!(f, r#"type modifier is not allowed for type "{name}""#);
        }
        if let Self::TypeNameSyntax { near, .. } = self {
            return write!(f, r#"syntax error at or near "{near}""#);
        }
        if *self == Self::TooManyColumns {
            return write!(f, "target lists can have at most {MAX_TARGET_COLUMNS} entries");
        }
        write!(f, "catalog {self:?}")
    }
}
impl std::error::Error for Error {}
pub(crate) type R<T> = Result<T, Error>;
pub const MAX_SQL: usize = 65536;
pub const MAX_TABLE_COLUMNS: usize = 1600;
pub const MAX_TARGET_COLUMNS: usize = 1664;
pub const MAX_ROWS: usize = 8192;
pub const MAX_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_WORK: usize = 2_000_000;
const MAX_COLUMN_ROWS: usize = 192 * 1024;
const MAX_COLUMN_BUILD_BYTES: usize = 640 * 1024 * 1024;
const MAX_COLUMN_BYTES: usize = 384 * 1024 * 1024;
const MAX_COLUMN_WORK: usize = 64_000_000;

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

/// Built-in extension availability and optional per-database installation.
#[derive(Debug, Clone)]
pub struct ExtensionDef {
    pub name: String, pub default_version: String, pub versions: Vec<String>, pub comment: String,
    pub relocatable: bool, pub superuser: bool, pub trusted: bool, pub schema: Option<String>,
    pub requires: Vec<String>, pub installed: Option<(u32, u32, u32, String)>,
}
impl ExtensionDef {
    pub fn plpgsql() -> Self {
        Self { name: "plpgsql".into(), default_version: "1.0".into(), versions: vec!["1.0".into()],
            comment: "PL/pgSQL procedural language".into(), relocatable: false, superuser: true, trusted: true,
            schema: Some("pg_catalog".into()), requires: vec![], installed: Some((13643, 10, 11, "1.0".into())) }
    }
    pub fn uuid_ossp() -> Self {
        Self { name: "uuid-ossp".into(), default_version: "1.1".into(), versions: vec!["1.1".into()],
            comment: "generate universally unique identifiers (UUIDs)".into(), relocatable: true, superuser: true, trusted: true,
            schema: None, requires: vec![], installed: None }
    }
}

#[derive(Debug, Clone)]
pub struct RoutineDef {
    pub schema: String,
    pub name: String,
    pub owner: u32,
    pub body: String,
    pub volatility: char,
    pub strict: bool,
    pub security_definer: bool,
    pub leakproof: bool,
    pub parallel: char,
    pub cost: giggles_pg_types::Float4,
}

#[derive(Debug, Clone)]
pub struct TriggerDef {
    pub oid: u32,
    pub table: u64,
    pub routine: u32,
    pub name: String,
    /// PostgreSQL's tgtype bitmask, including timing and orientation.
    pub kind: u8,
    pub enabled: char,
    pub update_columns: Vec<u64>,
    pub args: Vec<String>,
    pub condition: Option<String>,
    pub condition_columns: Vec<u64>,
}

/// In-memory sequence identity and owning role; no storage encoding is implied.
#[derive(Debug, Clone)]
pub struct SequenceDef { pub oid: u32, pub schema: String, pub name: String, pub owner: u32, pub acl: Option<Vec<AclEntry>> }
