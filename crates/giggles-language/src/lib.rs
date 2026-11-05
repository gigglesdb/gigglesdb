//! Finite trusted embedded native intents over the GigglesDB SQL engine.
//!
//! Raw `Admin` and file owners are trusted. This library is not task authorization.
//! A commit storage error is [`Error::Unknown`]; never blindly retry it.
//!
//! ```
//! use giggles_language::{parse, Statement};
//! let tree = parse("work where id=$id order by id limit 10 select id")?;
//! assert!(matches!(tree, Statement::Read { .. }));
//! # Ok::<(), giggles_language::Error>(())
//! ```
pub use giggles_core::relational::{SqlRow, Value};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
mod execute;
mod parser;
pub use execute::Engine;
pub use parser::parse;

pub type Result<T> = std::result::Result<T, Error>;
pub type Bindings = BTreeMap<String, Value>;
pub const MAX_INPUT: usize = 65_536;
pub const MAX_DEPTH: usize = 32;
pub const MAX_ROWS: usize = giggles_core::relational::MAX_ROWS;

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
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}

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
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Expr {
    Value(ValueExpr),
    Field(String),
    Compare(Box<Expr>, Compare, Box<Expr>),
    IsNull(Box<Expr>, bool),
    Not(Box<Expr>),
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Sort {
    pub field: String,
    pub descending: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Intent {
    One,
    Many { cap: ValueExpr },
    Claim { order: Vec<Sort> },
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
}
impl Statement {
    pub(crate) fn table(&self) -> &str {
        match self {
            Self::Read { table, .. }
            | Self::Insert { table, .. }
            | Self::Update { table, .. }
            | Self::Delete { table, .. } => table,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Outcome {
    pub columns: Vec<String>,
    pub rows: Vec<SqlRow>,
    pub matched: usize,
    pub affected: usize,
    pub changed: usize,
    pub complete: bool,
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
    Profile {
        name: "embedded-trusted-native-0.1",
        max_rows: MAX_ROWS,
        default_page: 100,
        scalar_types: &[
            "boolean", "smallint", "integer", "bigint", "text", "bytes", "null",
        ],
        unsupported: &[
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
        trusted_admin: true,
        automatic_retries: false,
    }
}
