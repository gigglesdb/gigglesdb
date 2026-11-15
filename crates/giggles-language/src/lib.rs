//! Finite trusted embedded native intents over the GigglesDB typed operation engine.
//!
//! Raw `Admin` and file owners are trusted. This library is not task authorization.
//! A commit storage error is [`Error::Unknown`]; never blindly retry it.
//! [`Engine::prepare_bound`] freezes real argument values in an immutable local
//! handle. Each [`Engine::execute_bound`] call uses fresh rows and revalidates
//! database/catalog identity. Handles are neither serialized requests nor
//! authority credentials; preparation supplies no automatic retry or deduplication.
//!
//! ```
//! use giggles_language::{parse, Statement};
//! let tree = parse("work where id=$id order by id limit 10 select id")?;
//! assert!(matches!(tree, Statement::Read { .. }));
//! # Ok::<(), giggles_language::Error>(())
//! ```
pub use giggles_core::operations::{
    CatalogOutcome, ConflictStatus, ConflictTupleOutcome, EmitDescription, EmittedValue,
    StepSummary, TransactionOutcome,
};
pub use giggles_core::relational::{SqlRow, Value};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
mod capabilities;
mod execute;
mod explain;
mod inspection;
pub mod once;
mod parser;
pub use capabilities::{
    Capability, CapabilityInterface, CapabilityLimits, CapabilityManifest, CapabilityState,
    DeploymentProfile, ReferenceRevision, TestEvidence, capability_manifest,
    capability_manifest_for,
};
pub use execute::{BoundCatalogOperation, BoundOperation, BoundTransaction, Engine, Lowered, lower_text};
pub use giggles_core::operations::ReadExplainOutcome;
pub use inspection::InspectionOutcome;
pub use once::{OnceError, OnceResult, OnceScope};
pub use parser::{
    parse, parse_catalog_with_version, parse_explain_with_version, parse_input_with_version,
    parse_inspection_with_version, parse_once_with_version, parse_outcome_with_version,
    parse_transaction_with_version, parse_with_version,
};

pub type Result<T> = std::result::Result<T, Error>;
pub type Bindings = BTreeMap<String, Value>;
pub const LANGUAGE_VERSION: &str = "0.1";
pub const PROFILE_NAME: &str = "embedded-trusted-native-0.1";
pub const MAX_INPUT: usize = 65_536;
pub const MAX_DEPTH: usize = 32;
pub const MAX_ROWS: usize = giggles_core::relational::MAX_ROWS;
pub const MAX_TOKENS: usize = 4096;
pub const MAX_EXPRESSION_NODES: usize = giggles_core::operations::MAX_EXPRESSION_NODES;
pub const MAX_BINDINGS: usize = giggles_core::operations::MAX_BINDINGS;
pub const MAX_BINDING_BYTES: usize = giggles_core::operations::MAX_BINDING_BYTES;
pub const MAX_BOUND_VALUE_BYTES: usize = giggles_core::operations::MAX_BOUND_VALUE_BYTES;
pub const MAX_MUTATION_BYTES: usize = giggles_core::operations::MAX_MUTATION_BYTES;
pub const MAX_TRANSACTION_ITEMS: usize = giggles_core::operations::MAX_TRANSACTION_ITEMS;
pub const MAX_TRANSACTION_LOCALS: usize = giggles_core::operations::MAX_TRANSACTION_LOCALS;
pub const MAX_EMIT_VALUES: usize = giggles_core::operations::MAX_EMIT_VALUES;
pub const MAX_RETAINED_LOCAL_BYTES: usize = giggles_core::operations::MAX_RETAINED_LOCAL_BYTES;
pub const MAX_EMIT_BYTES: usize = giggles_core::operations::MAX_EMIT_BYTES;
pub const MAX_INSERT_ROWS: usize = giggles_core::operations::MAX_INSERT_ROWS;
pub const MAX_INSERT_BYTES: usize = giggles_core::operations::MAX_INSERT_BYTES;
pub const MAX_FUNCTION_ARGUMENTS: usize = giggles_core::operations::MAX_FUNCTION_ARGUMENTS;
pub const MAX_CASE_ARMS: usize = giggles_core::operations::MAX_CASE_ARMS;
pub const MAX_INSPECTION_METADATA_BYTES: usize =
    giggles_core::operations::MAX_INSPECTION_METADATA_BYTES;

/// Explicit finite language selection. No version claims full native v1.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LanguageVersion {
    #[serde(rename = "0.1")]
    Embedded01,
    #[serde(rename = "0.2")]
    Embedded02,
    #[serde(rename = "0.3")]
    Embedded03,
    #[serde(rename = "0.4")]
    Embedded04,
    #[serde(rename = "0.5")]
    Embedded05,
    #[serde(rename = "0.6")]
    Embedded06,
    #[serde(rename = "0.7")]
    Embedded07,
    #[serde(rename = "0.8")]
    Embedded08,
    #[serde(rename = "0.9")]
    Embedded09,
    #[serde(rename = "0.10")]
    Embedded010,
    #[serde(rename = "0.11")]
    Embedded011,
    #[serde(rename = "0.12")]
    Embedded012,
    #[serde(rename = "0.13")]
    Embedded013,
    #[serde(rename = "0.14")]
    Embedded014,
    #[serde(rename = "0.15")]
    Embedded015,
    #[serde(rename = "0.16")]
    Embedded016,
    #[serde(rename = "0.17")]
    Embedded017,
    #[serde(rename = "0.18")]
    Embedded018,
}
impl LanguageVersion {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Embedded01 => LANGUAGE_VERSION,
            Self::Embedded02 => "0.2",
            Self::Embedded03 => "0.3",
            Self::Embedded04 => "0.4",
            Self::Embedded05 => "0.5",
            Self::Embedded06 => "0.6",
            Self::Embedded07 => "0.7",
            Self::Embedded08 => "0.8",
            Self::Embedded09 => "0.9",
            Self::Embedded010 => "0.10",
            Self::Embedded011 => "0.11",
            Self::Embedded012 => "0.12",
            Self::Embedded013 => "0.13",
            Self::Embedded014 => "0.14",
            Self::Embedded015 => "0.15",
            Self::Embedded016 => "0.16",
            Self::Embedded017 => "0.17",
            Self::Embedded018 => "0.18",
        }
    }
    pub const fn profile_name(self) -> &'static str {
        match self {
            Self::Embedded01 => PROFILE_NAME,
            Self::Embedded02 => "embedded-trusted-native-0.2",
            Self::Embedded03 => "embedded-trusted-native-0.3",
            Self::Embedded04 => "embedded-trusted-native-0.4",
            Self::Embedded05 => "embedded-trusted-native-0.5",
            Self::Embedded06 => "embedded-trusted-native-0.6",
            Self::Embedded07 => "embedded-trusted-native-0.7",
            Self::Embedded08 => "embedded-trusted-native-0.8",
            Self::Embedded09 => "embedded-trusted-native-0.9",
            Self::Embedded010 => "embedded-trusted-native-0.10",
            Self::Embedded011 => "embedded-trusted-native-0.11",
            Self::Embedded012 => "embedded-trusted-native-0.12",
            Self::Embedded013 => "embedded-trusted-native-0.13",
            Self::Embedded014 => "embedded-trusted-native-0.14",
            Self::Embedded015 => "embedded-trusted-native-0.15",
            Self::Embedded016 => "embedded-trusted-native-0.16",
            Self::Embedded017 => "embedded-trusted-native-0.17",
            Self::Embedded018 => "embedded-trusted-native-0.18",
        }
    }
    pub(crate) const fn supports_schema_names(self) -> bool {
        matches!(self, Self::Embedded018)
    }
    pub(crate) const fn supports_indexes(self) -> bool {
        matches!(self, Self::Embedded014 | Self::Embedded015 | Self::Embedded016 | Self::Embedded017 | Self::Embedded018)
    }
    pub(crate) const fn supports_computed(self) -> bool {
        matches!(
            self,
            Self::Embedded03
                | Self::Embedded04
                | Self::Embedded05
                | Self::Embedded06
                | Self::Embedded07
                | Self::Embedded08
                | Self::Embedded09
                | Self::Embedded010
                | Self::Embedded011
                | Self::Embedded012
                | Self::Embedded013
                | Self::Embedded014
                | Self::Embedded015 | Self::Embedded016 | Self::Embedded017 | Self::Embedded018
        )
    }
    pub(crate) const fn supports_return(self) -> bool {
        matches!(
            self,
            Self::Embedded04
                | Self::Embedded05
                | Self::Embedded06
                | Self::Embedded07
                | Self::Embedded08
                | Self::Embedded09
                | Self::Embedded010
                | Self::Embedded011
                | Self::Embedded012
                | Self::Embedded013
                | Self::Embedded014
                | Self::Embedded015 | Self::Embedded016 | Self::Embedded017 | Self::Embedded018
        )
    }
    pub(crate) const fn supports_transactions(self) -> bool {
        matches!(
            self,
            Self::Embedded05
                | Self::Embedded06
                | Self::Embedded07
                | Self::Embedded08
                | Self::Embedded09
                | Self::Embedded010
                | Self::Embedded011
                | Self::Embedded012
                | Self::Embedded013
                | Self::Embedded014
                | Self::Embedded015 | Self::Embedded016 | Self::Embedded017 | Self::Embedded018
        )
    }
    pub(crate) const fn supports_insert_many(self) -> bool {
        matches!(
            self,
            Self::Embedded06
                | Self::Embedded07
                | Self::Embedded08
                | Self::Embedded09
                | Self::Embedded010
                | Self::Embedded011
                | Self::Embedded012
                | Self::Embedded013
                | Self::Embedded014
                | Self::Embedded015 | Self::Embedded016 | Self::Embedded017 | Self::Embedded018
        )
    }
    pub(crate) const fn supports_catalog(self) -> bool {
        matches!(
            self,
            Self::Embedded07
                | Self::Embedded08
                | Self::Embedded09
                | Self::Embedded010
                | Self::Embedded011
                | Self::Embedded012
                | Self::Embedded013
                | Self::Embedded014
                | Self::Embedded015 | Self::Embedded016 | Self::Embedded017 | Self::Embedded018
        )
    }
    pub(crate) const fn supports_scalar_functions(self) -> bool {
        matches!(
            self,
            Self::Embedded08
                | Self::Embedded09
                | Self::Embedded010
                | Self::Embedded011
                | Self::Embedded012
                | Self::Embedded013
                | Self::Embedded014
                | Self::Embedded015 | Self::Embedded016 | Self::Embedded017 | Self::Embedded018
        )
    }
    pub(crate) const fn supports_case(self) -> bool {
        matches!(
            self,
            Self::Embedded09
                | Self::Embedded010
                | Self::Embedded011
                | Self::Embedded012
                | Self::Embedded013
                | Self::Embedded014
                | Self::Embedded015 | Self::Embedded016 | Self::Embedded017 | Self::Embedded018
        )
    }
    pub(crate) const fn supports_quoted_identifiers(self) -> bool {
        matches!(
            self,
            Self::Embedded09
                | Self::Embedded010
                | Self::Embedded011
                | Self::Embedded012
                | Self::Embedded013
                | Self::Embedded014
                | Self::Embedded015 | Self::Embedded016 | Self::Embedded017 | Self::Embedded018
        )
    }
    pub(crate) const fn supports_upsert(self) -> bool {
        matches!(
            self,
            Self::Embedded010
                | Self::Embedded011
                | Self::Embedded012
                | Self::Embedded013
                | Self::Embedded014
                | Self::Embedded015 | Self::Embedded016 | Self::Embedded017 | Self::Embedded018
        )
    }
    pub(crate) const fn supports_once(self) -> bool {
        matches!(
            self,
            Self::Embedded011
                | Self::Embedded012
                | Self::Embedded013
                | Self::Embedded014
                | Self::Embedded015 | Self::Embedded016 | Self::Embedded017 | Self::Embedded018
        )
    }
    pub(crate) const fn supports_explain(self) -> bool {
        matches!(
            self,
            Self::Embedded013 | Self::Embedded014 | Self::Embedded015 | Self::Embedded016 | Self::Embedded017 | Self::Embedded018
        )
    }
    pub(crate) const fn supports_inspection(self) -> bool {
        matches!(
            self,
            Self::Embedded012 | Self::Embedded013 | Self::Embedded014 | Self::Embedded015 | Self::Embedded016 | Self::Embedded017 | Self::Embedded018
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Error {
    Syntax {
        offset: usize,
    },
    Unsupported,
    Limit,
    Bind(String),
    Type,
    NotFound,
    AmbiguousMatch,
    ConditionFailed,
    TooManyMatches,
    ClaimNotConsumed,
    Conflict,
    Core(String),
    /// The COMMIT storage result cannot prove whether effects persisted.
    Unknown,
    Overflow,
    DivisionByZero,
    RequirementFailed,
    DuplicateTarget,
    PlanRequired,
    Duplicate,
    Sql { sqlstate: String },
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
impl Error {
    /// SQLSTATE for selected native error identities.
    pub fn sqlstate(&self) -> Option<&str> {
        match self {
            Self::Sql { sqlstate } => Some(sqlstate),
            Self::Duplicate => Some("23505"),
            Self::Overflow => Some("22003"),
            Self::DivisionByZero => Some("22012"),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ValueExpr {
    Literal(Value),
    Parameter(String),
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Compare {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Arithmetic {
    Add,
    Subtract,
    Multiply,
    Divide,
    Remainder,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ScalarFunction {
    Coalesce,
    Abs,
    Length,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConflictSource {
    Existing,
    Incoming,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Expr {
    Value(ValueExpr),
    Field(String),
    Compare(Box<Expr>, Compare, Box<Expr>),
    IsNull(Box<Expr>, bool),
    Not(Box<Expr>),
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Negate(Box<Expr>),
    Arithmetic(Box<Expr>, Arithmetic, Box<Expr>),
    Local(LocalRef),
    Call {
        function: ScalarFunction,
        arguments: Vec<Expr>,
    },
    Case {
        operand: Option<Box<Expr>>,
        arms: Vec<(Expr, Expr)>,
        else_expr: Option<Box<Expr>>,
    },
    ConflictField {
        source: ConflictSource,
        field: String,
    },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LocalRef {
    Field { local: String, field: String },
    Status { local: String },
    Changed { local: String },
    Count { local: String },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Sort {
    pub field: String,
    pub descending: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Projection {
    pub expression: Expr,
    pub alias: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Intent {
    One,
    Many { cap: ValueExpr },
    Claim { order: Vec<Sort> },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConflictAction {
    Nothing,
    Update {
        guard: Option<Expr>,
        values: Vec<(String, Expr)>,
    },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Statement {
    Read {
        table: String,
        filter: Option<Expr>,
        order: Vec<Sort>,
        limit: Option<ValueExpr>,
        projection: Vec<String>,
    },
    Insert {
        table: String,
        values: Vec<(String, ValueExpr)>,
    },
    Update {
        table: String,
        filter: Expr,
        guard: Option<Expr>,
        values: Vec<(String, ValueExpr)>,
        intent: Intent,
    },
    Delete {
        table: String,
        filter: Expr,
        guard: Option<Expr>,
        cap: Option<ValueExpr>,
    },
    UpdateExpressions {
        table: String,
        filter: Expr,
        guard: Option<Expr>,
        values: Vec<(String, Expr)>,
        intent: Intent,
    },
    ReadExpressions {
        table: String,
        filter: Option<Expr>,
        order: Vec<Sort>,
        limit: Option<ValueExpr>,
        projection: Vec<Projection>,
    },
    Returning {
        statement: Box<Statement>,
        /// Empty means an explicit `return *`, not an omitted result clause.
        projection: Vec<Projection>,
    },
    InsertExpressions {
        table: String,
        values: Vec<(String, Expr)>,
    },
    InsertMany {
        table: String,
        columns: Vec<String>,
        rows: Vec<Vec<Expr>>,
        cap: ValueExpr,
    },
    OnConflict {
        statement: Box<Statement>,
        target: Vec<String>,
        action: ConflictAction,
    },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnSpec {
    pub name: String,
    pub data_type: CatalogType,
    /// Native fields are required unless explicitly declared nullable.
    pub nullable: bool,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CatalogType {
    Bool,
    Int32,
    Int64,
    Text,
    Bytes,
    Numeric,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CatalogStatement {
    DropTable { table: String, cascade: bool },
    DropIndex { name: String, cascade: bool },
    CreateIndex {
        table: String,
        name: String,
        columns: Vec<String>,
    },
    CreateTable {
        table: String,
        columns: Vec<ColumnSpec>,
        primary_key: Vec<String>,
    },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Transaction {
    pub read_only: bool,
    pub steps: Vec<TransactionStep>,
    pub emit: Option<Vec<EmitExpr>>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactionStep {
    Execute(Statement),
    Let { name: String, statement: Statement },
    Require(Expr),
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EmitExpr {
    Scalar(Expr),
    Row(String),
    Rows(String),
}

/// A fixed elapsed duration used only by the finite once retention header.
/// Execution requires positive, exact whole seconds, at most seven days.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurationLiteral {
    pub microseconds: i64,
}

/// The complete keyed business unit. Individual block items cannot carry once.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OnceBody {
    Statement(Statement),
    Transaction(Transaction),
}

/// Persist this selected version and original request with the caller's scope.
/// Parsing does not obtain a database identity, namespace, clock or authority.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnceEnvelope {
    pub language_version: LanguageVersion,
    pub key: ValueExpr,
    pub retention: DurationLiteral,
    pub body: OnceBody,
}

/// Catalog-free lookup syntax; execution still requires the original scope.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutcomeQuery {
    pub language_version: LanguageVersion,
    pub key: ValueExpr,
}

/// A standalone static explanation. Only a simple `Statement::Read` is admitted.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExplainStatement {
    pub read: Statement,
}

/// Standalone trusted discovery; never a data-block or once-envelope body.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum InspectionStatement {
    ShowCapabilities,
    ShowLimits,
    ShowTables,
    DescribeTable { table: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ParsedInput {
    Explain(ExplainStatement),
    Statement(Statement),
    Transaction(Transaction),
    Catalog(CatalogStatement),
    Once(OnceEnvelope),
    Outcome(OutcomeQuery),
    Inspection(InspectionStatement),
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Outcome {
    pub columns: Vec<String>,
    pub rows: Vec<SqlRow>,
    pub matched: usize,
    pub affected: usize,
    pub changed: usize,
    pub complete: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conflict_results: Option<Vec<ConflictTupleOutcome>>,
}
impl Default for Outcome {
    fn default() -> Self {
        Self {
            columns: vec![],
            rows: vec![],
            matched: 0,
            affected: 0,
            changed: 0,
            complete: true,
            conflict_results: None,
        }
    }
}
#[derive(Clone, Debug, Serialize)]
pub struct Profile {
    pub name: &'static str,
    pub max_rows: usize,
    pub default_page: usize,
    pub scalar_types: &'static [&'static str],
    pub unsupported: &'static [&'static str],
    pub trusted_admin: bool,
    pub automatic_retries: bool,
}
pub fn profile() -> Profile {
    profile_for(LanguageVersion::Embedded01)
}
pub fn profile_for(version: LanguageVersion) -> Profile {
    Profile {
        name: version.profile_name(),
        max_rows: MAX_ROWS,
        default_page: 100,
        scalar_types: if matches!(version, LanguageVersion::Embedded015 | LanguageVersion::Embedded016 | LanguageVersion::Embedded017 | LanguageVersion::Embedded018) {
            &["boolean", "smallint", "integer", "bigint", "text", "bytes", "null", "numeric parameters"]
        } else {
            &["boolean", "smallint", "integer", "bigint", "text", "bytes", "null"]
        },
        unsupported: match version {
            LanguageVersion::Embedded01 => &[
                "once",
                "receipts",
                "atomic blocks",
                "task authorization",
                "server",
                "decimal",
                "uuid",
                "schema jobs",
                "joins",
                "arithmetic",
                "functions",
                "quoted identifiers",
                "qualified names",
                "cursors",
            ],
            LanguageVersion::Embedded02 => &[
                "once",
                "receipts",
                "atomic blocks",
                "task authorization",
                "server",
                "decimal",
                "uuid",
                "schema jobs",
                "joins",
                "computed projections",
                "computed assignments",
                "arithmetic in ordering, limits or caps",
                "functions",
                "quoted identifiers",
                "qualified names",
                "cursors",
            ],
            LanguageVersion::Embedded03 | LanguageVersion::Embedded04 => &[
                "once",
                "receipts",
                "atomic blocks",
                "task authorization",
                "server",
                "decimal",
                "uuid",
                "schema jobs",
                "joins",
                "computed inserts",
                "unaliased computed projections",
                "arithmetic in ordering, limits or caps",
                "functions",
                "quoted identifiers",
                "qualified names",
                "cursors",
            ],
            LanguageVersion::Embedded05
            | LanguageVersion::Embedded06
            | LanguageVersion::Embedded07 => &[
                "once",
                "receipts",
                "nested transactions",
                "claim in transactions",
                "savepoints",
                "recovery",
                "task authorization",
                "server",
                "decimal",
                "uuid",
                "schema jobs",
                "joins",
                "computed INSERT SET",
                "unaliased computed projections",
                "arithmetic in ordering, limits or caps",
                "functions",
                "quoted identifiers",
                "qualified names",
                "cursors",
            ],
            LanguageVersion::Embedded08 => &[
                "once",
                "receipts",
                "nested transactions",
                "claim in transactions",
                "savepoints",
                "recovery",
                "task authorization",
                "server",
                "decimal",
                "uuid",
                "schema jobs",
                "joins",
                "computed INSERT SET",
                "unaliased computed projections",
                "arithmetic in ordering, limits or caps",
                "other scalar functions",
                "CASE",
                "quoted identifiers",
                "qualified names",
                "cursors",
            ],
            LanguageVersion::Embedded09 => &[
                "once",
                "receipts",
                "nested transactions",
                "claim in transactions",
                "savepoints",
                "recovery",
                "task authorization",
                "server",
                "decimal",
                "uuid",
                "schema jobs",
                "joins",
                "computed INSERT SET",
                "unaliased computed projections",
                "arithmetic in ordering, limits or caps",
                "other scalar functions",
                "noncanonical quoted identifiers",
                "qualified names",
                "cursors",
            ],
            LanguageVersion::Embedded010 => &[
                "once",
                "receipts",
                "nested transactions",
                "claim in transactions",
                "savepoints",
                "recovery",
                "task authorization",
                "server",
                "decimal",
                "uuid",
                "schema jobs",
                "joins",
                "computed INSERT SET",
                "unaliased computed projections",
                "arithmetic in ordering, limits or caps",
                "other scalar functions",
                "noncanonical quoted identifiers",
                "general qualified names",
                "secondary unique conflict targets",
                "cursors",
            ],
            LanguageVersion::Embedded011
            | LanguageVersion::Embedded012
            | LanguageVersion::Embedded013
            | LanguageVersion::Embedded014
            | LanguageVersion::Embedded015 | LanguageVersion::Embedded016 | LanguageVersion::Embedded017 | LanguageVersion::Embedded018 => &[
                "once without explicit original scope and trusted clock",
                "CLI once execution and outcome lookup",
                "duration parameters",
                "subsecond retention",
                "general duration expressions",
                "nested transactions",
                "claim in transactions",
                "per-item once",
                "savepoints",
                "recovery",
                "task authorization",
                "server",
                "decimal",
                "uuid",
                "schema jobs",
                "joins",
                "computed INSERT SET",
                "unaliased computed projections",
                "arithmetic in ordering, limits or caps",
                "other scalar functions",
                "noncanonical quoted identifiers",
                "general qualified names",
                "secondary unique conflict targets",
                "cursors",
            ],
        },
        trusted_admin: true,
        automatic_retries: false,
    }
}

#[cfg(test)]
mod duplicate_error_encoding_regression {
    use super::Error;
    #[test]
    fn existing_error_bytes_are_stable() {
        for (error, expected) in [
            (Error::Syntax { offset: 7 }, r#"{"Syntax":{"offset":7}}"#),
            (Error::Unsupported, r#""Unsupported""#),
            (Error::Limit, r#""Limit""#),
            (Error::Bind("x".into()), r#"{"Bind":"x"}"#),
            (Error::Type, r#""Type""#),
            (Error::NotFound, r#""NotFound""#),
            (Error::AmbiguousMatch, r#""AmbiguousMatch""#),
            (Error::ConditionFailed, r#""ConditionFailed""#),
            (Error::TooManyMatches, r#""TooManyMatches""#),
            (Error::ClaimNotConsumed, r#""ClaimNotConsumed""#),
            (Error::Conflict, r#""Conflict""#),
            (Error::Core("x".into()), r#"{"Core":"x"}"#),
            (Error::Unknown, r#""Unknown""#),
            (Error::Overflow, r#""Overflow""#),
            (Error::DivisionByZero, r#""DivisionByZero""#),
            (Error::RequirementFailed, r#""RequirementFailed""#),
            (Error::DuplicateTarget, r#""DuplicateTarget""#),
            (Error::PlanRequired, r#""PlanRequired""#),
        ] {
            assert_eq!(serde_json::to_vec(&error).unwrap(), expected.as_bytes());
        }
    }
}
