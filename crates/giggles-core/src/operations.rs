//! Syntax-independent operations for the bounded trusted embedded engine.
//!
//! Frontends lower requests into these types without generating SQL. The engine
//! binds against one transaction snapshot and publishes each request atomically.
//! Table references accept canonical `table` or `schema.table`; bare names follow
//! the session search path (a fresh Engine session defaults to public).
//! This initial surface supports the existing finite native subset; it is not
//! task authorization and does not replace the SQL frontend or its semantics.
//! Storage failures during commit return [`Error::Unknown`] and must not be
//! retried blindly.

pub use crate::relational::{SqlRow, Value};
pub use crate::storage::native_input_rows;
thread_local! {
    static FORCE_NATIVE_FULL_SCAN: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}
/// Run a test with native access-path narrowing disabled, restoring it on unwind.
#[doc(hidden)]
pub fn force_native_full_scan<T>(f: impl FnOnce() -> T) -> T {
    struct Restore(bool);
    impl Drop for Restore {
        fn drop(&mut self) { FORCE_NATIVE_FULL_SCAN.set(self.0); }
    }
    let _restore = Restore(FORCE_NATIVE_FULL_SCAN.replace(true));
    f()
}

#[cfg(test)]
mod case_tests;
mod catalog;
pub(crate) mod state_adapter;
use state_adapter::Context;

mod explain;
#[cfg(test)]
mod heap_native_tests;
mod secondary_selector;
pub use explain::*;
mod inspection;
pub mod once;
#[cfg(test)]
mod scalar_tests;
mod session;
mod sequence;
pub use sequence::*;
mod transaction;
mod unique_conflict;
mod upsert;
#[cfg(test)]
mod upsert_tests;
use crate::{
    Admin, CoreError, SqlSession,
    relational::{ColumnDef, DataType, Scan, SqlError, TableDef, value_bytes},
    storage::CompleteRelation,
};
pub use catalog::*;
pub use inspection::*;
use serde::{Deserialize, Serialize};
use std::{
    borrow::Cow,
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
};
pub use transaction::*;

pub type Result<T> = std::result::Result<T, Error>;
pub type Bindings = BTreeMap<String, Value>;
pub const MAX_INPUT: usize = 65_536;
pub const MAX_DEPTH: usize = 32;
pub const MAX_EXPRESSION_NODES: usize = 4096;
/// Maximum argument count for a fixed engine scalar function.
pub const MAX_FUNCTION_ARGUMENTS: usize = 64;
/// Maximum WHEN/THEN pairs in one searched or simple CASE expression.
pub const MAX_CASE_ARMS: usize = 64;
pub const MAX_ROWS: usize = crate::relational::MAX_ROWS;
/// Maximum number of explicit tuples in an INSERT MANY request.
pub const MAX_INSERT_ROWS: usize = MAX_ROWS;
/// Aggregate proposed full-row payload admitted before an INSERT MANY stages.
pub const MAX_INSERT_BYTES: usize = crate::relational::MAX_RESULT_BYTES;
/// Serialized bound for an INSERT MANY request. Its tuples are flat, as SQL
/// VALUES, so text and trees scale with SQL's statement budget rather than
/// MAX_INPUT. Row counts (legacy) and proposed bytes are admitted at bind.
pub const MAX_INSERT_REQUEST_BYTES: usize = 8 * crate::relational::MAX_STATEMENT_SQL;
/// Maximum entries in a supplied parameter map, including unused entries.
// Native operation bindings have an independent limit from SQL parameters.
pub const MAX_BINDINGS: usize = 256;
/// Sum of supplied value payloads: text/bytes length, nine bytes per scalar.
pub const MAX_BINDING_BYTES: usize = crate::relational::MAX_ROW_BYTES;
/// Sum of resolved literal/parameter payloads, charging every occurrence.
/// Explicit limit/cap values count too; this is not total heap accounting.
pub const MAX_BOUND_VALUE_BYTES: usize = crate::relational::MAX_ROW_BYTES;
/// Selected full postimage payload for computed updates, including no-op rows.
pub const MAX_MUTATION_BYTES: usize = crate::relational::MAX_RESULT_BYTES;
pub const ENGINE_CONTRACT_VERSION: u32 = 1;

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
    /// Process-local task diagnostics; never part of the serialized operation format.
    #[serde(skip)]
    Task(crate::task::TaskError),
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
impl Error {
    /// SQLSTATE identity preserved by native execution.
    pub fn sqlstate(&self) -> Option<&str> {
        match self {
            Self::Sql { sqlstate } => Some(sqlstate),
            Self::Task(error) => Some(error.sqlstate()),
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
pub struct Projection {
    pub expression: Expr,
    pub alias: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConflictAction {
    Nothing,
    Update {
        guard: Option<Expr>,
        values: Vec<(String, Expr)>,
    },
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConflictStatus {
    Success,
    Skipped,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConflictTupleOutcome {
    pub input_index: usize,
    pub status: ConflictStatus,
    pub changed: bool,
    /// Index into this operation's RETURN rows, never the block's emitted values.
    pub returned_row: Option<usize>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Request {
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
        request: Box<Request>,
        /// An empty projection selects all target columns, as `return *`.
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
        request: Box<Request>,
        target: Vec<String>,
        action: ConflictAction,
    },
}
struct RequestParts<'a> {
    body: &'a Request,
    returning: Option<&'a [Projection]>,
    conflict: Option<(&'a [String], &'a ConflictAction)>,
}
impl Request {
    // Inspect at most Returning -> OnConflict -> direct insert. Malformed
    // caller-owned wrapper chains never enter recursive traversal or serde.
    fn parts(&self) -> Result<RequestParts<'_>> {
        let (mut body, returning) = if let Self::Returning {
            request,
            projection,
        } = self
        {
            if !matches!(
                request.as_ref(),
                Self::Insert { .. }
                    | Self::InsertExpressions { .. }
                    | Self::InsertMany { .. }
                    | Self::Update { .. }
                    | Self::UpdateExpressions { .. }
                    | Self::Delete { .. }
                    | Self::OnConflict { .. }
            ) {
                return Err(Error::Unsupported);
            }
            if projection.len() > crate::relational::MAX_TARGET_COLUMNS {
                return Err(Error::Limit);
            }
            (request.as_ref(), Some(projection.as_slice()))
        } else {
            (self, None)
        };
        let conflict = if let Self::OnConflict {
            request,
            target,
            action,
        } = body
        {
            if !matches!(
                request.as_ref(),
                Self::Insert { .. } | Self::InsertExpressions { .. } | Self::InsertMany { .. }
            ) {
                return Err(Error::Unsupported);
            }
            if target.len() > crate::relational::MAX_COLUMNS {
                return Err(Error::Limit);
            }
            if target.is_empty() {
                return Err(Error::Bind("conflict target".into()));
            }
            if let ConflictAction::Update { values, .. } = action
                && (values.is_empty() || values.len() > crate::relational::MAX_TABLE_COLUMNS)
            {
                return Err(Error::Bind("assignments".into()));
            }
            body = request.as_ref();
            Some((target.as_slice(), action))
        } else {
            None
        };
        if matches!(body, Self::Returning { .. } | Self::OnConflict { .. })
            || (returning.is_some()
                && !matches!(
                    body,
                    Self::Insert { .. }
                        | Self::InsertExpressions { .. }
                        | Self::InsertMany { .. }
                        | Self::Update { .. }
                        | Self::UpdateExpressions { .. }
                        | Self::Delete { .. }
                ))
        {
            return Err(Error::Unsupported);
        }
        Ok(RequestParts {
            body,
            returning,
            conflict,
        })
    }
    fn split_returning(&self) -> Result<(&Self, Option<&[Projection]>)> {
        let parts = self.parts()?;
        Ok((parts.body, parts.returning))
    }
    fn table(&self) -> Result<&str> {
        let (request, _) = self.split_returning()?;
        Ok(match request {
            Self::Read { table, .. }
            | Self::Insert { table, .. }
            | Self::InsertExpressions { table, .. }
            | Self::InsertMany { table, .. }
            | Self::Update { table, .. }
            | Self::Delete { table, .. }
            | Self::UpdateExpressions { table, .. }
            | Self::ReadExpressions { table, .. } => table,
            Self::Returning { .. } | Self::OnConflict { .. } => return Err(Error::Unsupported),
        })
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

/// One bounded native read, with an exclusive ORDER BY continuation key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Page {
    pub outcome: Outcome,
    pub next: Option<Vec<Value>>,
}

/// Stable metadata for an immutable, value-bound operation. This is not authority.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundDescription {
    pub contract_version: u32,
    pub database_domain: u128,
    pub catalog_epoch: u64,
    pub table: TableDef,
    /// Typed output columns, empty for mutations that return only counts.
    pub columns: Vec<ColumnDef>,
}

/// Process-local operation with all parameter values frozen at preparation.
/// There is no deserialization or public constructor. Executing it still needs
/// a trusted engine, a compatible catalog and a fresh transaction snapshot.
#[derive(Clone)]
pub struct BoundOperation {
    description: BoundDescription,
    operation: Operation,
}
impl BoundOperation {
    pub fn description(&self) -> &BoundDescription {
        &self.description
    }
}
impl std::fmt::Debug for BoundOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BoundOperation")
            .field("description", &self.description)
            .finish_non_exhaustive()
    }
}

/// Trusted embedded executor. Independent Admin holders retain unrestricted authority.
#[derive(Clone)]
pub struct Engine {
    admin: Admin,
}
fn core(e: CoreError) -> Error {
    match e {
        CoreError::Sql(SqlError::Conflict) | CoreError::SqlSerialization(_) => Error::Conflict,
        CoreError::Sql(SqlError::Duplicate) => Error::Duplicate,
        CoreError::Sql(SqlError::Overflow) => Error::Overflow,
        CoreError::Sql(_) | CoreError::SequencePrivilege => Error::Sql {
            sqlstate: crate::sqlstate(&e).into(),
        },
        // A task statement stopped by a limit inside the read (rows scanned,
        // rows-read allowance; GD-0061 R3-402) keeps its SQLSTATE.
        CoreError::Task(e @ crate::task::TaskError::BudgetExceeded(_)) => task_error(e),
        _ => Error::Core(e.to_string()),
    }
}
impl Engine {
    pub fn new(admin: Admin) -> Self {
        Self { admin }
    }
    /// Read a page using an explicit total order ending in the full primary key.
    pub fn read_page(&self, request: &Request, after: Option<&[Value]>, bindings: &Bindings) -> Result<Page> {
        self.admin.sql_session().read_page(request, after, bindings)
    }
    /// Bind and execute one bounded request in its own transaction.
    pub fn execute(&self, request: &Request, bindings: &Bindings) -> Result<Outcome> {
        validate_request(request, Admission::engine(self.admin.engine() == crate::Engine::V2))?;
        validate_bindings(bindings)?;
        self.execute_operation(|session| bind_request(session, request, bindings))
    }

    /// Freeze actual values and bind stable catalog identities without a logical
    /// row scan or durable decision. Snapshot creation still clones bounded state.
    /// This is not a parameter template.
    pub fn prepare_bound(&self, request: &Request, bindings: &Bindings) -> Result<BoundOperation> {
        validate_request(request, Admission::engine(self.admin.engine() == crate::Engine::V2))?;
        validate_bindings(bindings)?;
        let mut session = self.admin.sql_session();
        session.begin_engine().map_err(core)?;
        let prepared = (|| {
            let (domain, epoch) = session.snapshot_identity_engine().map_err(core)?;
            let (table, operation) = bind_request(&mut session, request, bindings)?;
            let columns = operation.columns(&table);
            Ok(BoundOperation {
                description: BoundDescription {
                    contract_version: ENGINE_CONTRACT_VERSION,
                    database_domain: domain,
                    catalog_epoch: epoch,
                    table,
                    columns,
                },
                operation,
            })
        })();
        // Preparation never publishes even a read-only transaction, so it does
        // not advance durable generation, row versions or catalog allocators.
        session.rollback();
        prepared
    }

    /// Execute frozen intent against current rows. Catalog/domain incompatibility
    /// rejects before scanning or effects; row values and caps are checked anew.
    /// A commit storage error remains Unknown and must not be blindly retried.
    pub fn execute_bound(&self, prepared: &BoundOperation) -> Result<Outcome> {
        self.execute_operation(|session| {
            let description = &prepared.description;
            let (domain, epoch) = session.snapshot_identity_engine().map_err(core)?;
            let stale = description.contract_version != ENGINE_CONTRACT_VERSION
                || domain != description.database_domain
                || epoch != description.catalog_epoch;
            let current_table = session
                .inspect_catalog_engine(|_, tables| {
                    !stale && tables.values().any(|table| table == &description.table)
                })
                .map_err(core)?;
            if stale
                || !(current_table || current_view(session, &description.table, &prepared.operation)?)
            {
                return Err(core(CoreError::Sql(SqlError::StaleSchema)));
            }
            Ok((description.table.clone(), prepared.operation.clone()))
        })
    }

    fn execute_operation(
        &self,
        bind: impl FnOnce(&mut SqlSession) -> Result<(TableDef, Operation)>,
    ) -> Result<Outcome> {
        let mut session = self.admin.sql_session();
        session.begin_engine().map_err(core)?;
        let staged = (|| {
            // Catalog binding and every read use the same transaction snapshot.
            let (table, operation) = bind(&mut session)?;
            run_complete(&mut session, &table, operation, None)
        })();
        let outcome = match staged {
            Ok(v) => v,
            Err(e) => {
                session.rollback();
                return Err(e);
            }
        };
        #[cfg(test)]
        upsert_tests::before_publish();
        match session.commit_engine() {
            Ok(()) => Ok(outcome),
            Err(e) => {
                session.rollback();
                Err(match e {
                    CoreError::Storage(_) => Error::Unknown,
                    _ => core(e),
                })
            }
        }
    }
}
impl SqlSession {
    /// Read within the current transaction, or a temporary read-only snapshot.
    /// `limit` is the page size (default 100); a zero size has no continuation.
    /// `matched` counts eligible rows after the key; `complete` means no more rows.
    /// No server caller and no twin: refused in a task session (GD-0061 R3-301).
    pub fn read_page(&mut self, request: &Request, after: Option<&[Value]>, bindings: &Bindings) -> Result<Page> {
        self.refuse_task_session().map_err(core)?;
        if self.is_aborted() { return Err(core(CoreError::Sql(SqlError::Aborted))); }
        let owned = !self.in_transaction();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            validate_request(request, Admission::engine(self.is_v2_engine()))?;
            validate_bindings(bindings)?;
            if !matches!(request, Request::Read { .. }) { return Err(Error::Unsupported); }
            if owned { self.begin_engine().map_err(core)?; }
            self.begin_native_step().map_err(core)?;
            read_page(self, request, after, bindings)
        }));
        if owned { self.rollback(); }
        match result {
            Ok(result) => {
                if result.is_err() && !owned { self.abort(); }
                result
            }
            Err(panic) => {
                if !owned { self.abort(); }
                std::panic::resume_unwind(panic)
            }
        }
    }
}

fn read_page(session: &mut SqlSession, request: &Request, after: Option<&[Value]>, bindings: &Bindings) -> Result<Page> {
    // Keyset pages need a primary key, so they read tables only.
    let key = native_relation_key(request.table()?);
    let table = session.resolve_table_engine(&key).map_err(core)?;
    let (table, operation) = bind_table(table, request, bindings, session.max_read_rows())?;
    let Request::Read { order: requested, .. } = request else { return Err(Error::Unsupported); };
    let Operation::Read { filter, order, limit, projection } = operation else { return Err(Error::Unsupported); };
    if requested.len() < table.primary_key.len()
        || !requested[requested.len() - table.primary_key.len()..].iter()
            .zip(&table.primary_key).all(|(sort, index)| sort.field == table.columns[*index].name)
    {
        return Err(Error::Bind("paging order must end in the full primary key".into()));
    }
    if let Some(key) = after {
        if key.len() != order.len() { return Err(Error::Bind("paging key length".into())); }
        let mut remaining = MAX_BOUND_VALUE_BYTES;
        for (value, (index, _)) in key.iter().zip(&order) {
            let column = &table.columns[*index];
            if matches!((Some(colty(column)), ty(value)), (Some(Ty::Int), Some(Ty::Text | Ty::Bool)) | (Some(Ty::Text | Ty::Bool), Some(Ty::Int))) {
                return Err(core(CoreError::Sql(SqlError::UndefinedFunction)));
            }
            check(value, column)?;
            charge_value(&mut remaining, value)?;
        }
    }
    let cmp_key = |row: &SqlRow, key: &[Value]| {
        order.iter().zip(key).map(|((index, desc), value)| {
            let cmp = page_compare(&row.values[*index], value);
            if *desc { cmp.reverse() } else { cmp }
        }).find(|cmp| *cmp != Ordering::Equal).unwrap_or(Ordering::Equal)
    };
    let key = |row: &SqlRow| order.iter().map(|(i, _)| row.values[*i].clone()).collect::<Vec<_>>();
    let mut rows: Vec<SqlRow> = Vec::new();
    let mut matched = 0;
    let mut retained = 0;
    session.visit_native_page(&table, &mut |row| {
        if after.is_some_and(|after| cmp_key(row, after) != Ordering::Greater)
            || !filter.accepts(row)? { return Ok(()); }
        matched += 1;
        if limit == 0 { return Ok(()); }
        let position = rows.binary_search_by(|other| order.iter().map(|(index, desc)| {
            let cmp = page_compare(&other.values[*index], &row.values[*index]);
            if *desc { cmp.reverse() } else { cmp }
        }).find(|cmp| *cmp != Ordering::Equal).unwrap_or(Ordering::Equal)).unwrap_err();
        if position >= limit { return Ok(()); }
        if rows.len() == limit {
            retained -= rows.pop().unwrap().values.iter().map(value_bytes).sum::<usize>();
        }
        let bytes = row.values.iter().map(value_bytes).sum::<usize>();
        retained += bytes;
        if retained > crate::relational::scan::MAX_RETAINED_BYTES { return Err(Error::Limit); }
        rows.insert(position, row.clone());
        Ok(())
    }, core)?;
    crate::cancellation::check().map_err(core)?;
    let complete = matched <= limit;
    let next = if complete { None } else { rows.last().map(key) };
    let mut remaining = crate::relational::MAX_RESULT_BYTES;
    for row in &rows {
        for index in &projection { charge_value(&mut remaining, &row.values[*index])?; }
    }
    Ok(Page { next, outcome: Outcome {
        columns: projection.iter().map(|i| table.columns[*i].name.clone()).collect(),
        rows: rows.into_iter().map(|row| SqlRow { version: row.version,
            values: projection.iter().map(|i| row.values[*i].clone()).collect() }).collect(),
        matched, complete, ..Outcome::default()
    } })
}
fn page_compare(a: &Value, b: &Value) -> Ordering {
    match (a, b) {
        (Value::Date(a), Value::Date(b)) => a.cmp(b),
        (Value::Time(a), Value::Time(b)) => a.cmp(b),
        (Value::TimeTz(a), Value::TimeTz(b)) => a.cmp(b),
        (Value::Timestamp(a), Value::Timestamp(b)) => a.cmp(b),
        (Value::TimestampTz(a), Value::TimestampTz(b)) => a.cmp(b),
        (Value::Interval(a), Value::Interval(b)) => a.cmp(b),
        _ => compare(a, b),
    }
}
/// INSERT MANY admission before binding.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Admission {
    /// The pre-R2-NOCAP rules: MAX_INSERT_ROWS tuples, one aggregate
    /// expression-node budget and MAX_INPUT serialized bytes. Once intents
    /// keep them on every engine: the V1 intent codec inherits these rules
    /// (see intent.rs), and changing them needs a codec decision.
    Base,
    /// Legacy engine: the Base row and node budgets; INSERT MANY requests
    /// (and their transaction steps) may serialize up to
    /// MAX_INSERT_REQUEST_BYTES, so 1,024-tuple literal text is accepted
    /// (R2-NOCAP-3).
    Legacy,
    /// V2: no row bound; each flat tuple has its own node budget, as SQL
    /// VALUES; proposed bytes are admitted at bind.
    V2,
}
impl Admission {
    pub(super) fn engine(v2: bool) -> Self {
        if v2 { Self::V2 } else { Self::Legacy }
    }
}
fn validate_request(request: &Request, admission: Admission) -> Result<()> {
    validate_request_nodes(request, &mut 0, admission == Admission::V2)?;
    if admission != Admission::Base && matches!(request.parts()?.body, Request::InsertMany { .. }) {
        validate_size_within(request, MAX_INSERT_REQUEST_BYTES)?;
    } else {
        validate_size(request)?;
    }
    if !relation_identifier(request.table()?) {
        return Err(Error::Bind("identifier".into()));
    }
    Ok(())
}
fn validate_bindings(bindings: &Bindings) -> Result<()> {
    // Count is available without walking even an adversarial caller-owned map.
    // Only borrow values: neither their payloads nor unused key names are copied.
    if bindings.len() > MAX_BINDINGS {
        return Err(Error::Limit);
    }
    let mut remaining = MAX_BINDING_BYTES;
    for value in bindings.values() {
        remaining = remaining
            .checked_sub(value_bytes(value))
            .ok_or(Error::Limit)?;
    }
    Ok(())
}
fn bind_request(
    session: &mut impl Context,
    request: &Request,
    bindings: &Bindings,
) -> Result<(TableDef, Operation)> {
    bind_request_planned(session, request, bindings).map(|(table, operation, _)| (table, operation))
}
/// `bind_request`, with the output plan of a task session's column rules
/// (`None` outside a task session), for the request's release tag (R3-308).
fn bind_request_planned(
    session: &mut impl Context,
    request: &Request,
    bindings: &Bindings,
) -> Result<(TableDef, Operation, Option<crate::task::columns::OutputPlan>)> {
    if let Some(rules) = task_rules(session)? {
        let (table, request, plan) = task_target(session, &rules, request)?;
        let (table, operation) = bind_table(table, &request, bindings, session.max_read_rows())?;
        return Ok((table, operation, Some(plan)));
    }
    let key = native_relation_key(request.table()?);
    let table = resolve_relation(session, &key, request)?;
    let (table, operation) = bind_table(table, request, bindings, session.max_read_rows())?;
    Ok((table, operation, None))
}
/// A task refusal on the native surface keeps its SQLSTATE and its name and
/// fixed message: 42501 or 54T01 alone cannot identify the refusal, and a
/// budget stop inside the read (R3-402) must reach native clients as the same
/// task error every other surface reports (R3-1101).
fn task_error(e: crate::task::TaskError) -> Error {
    Error::Task(e)
}
/// `core`, except that a task refusal keeps its SQLSTATE (`task_error`).
fn core_or_task(e: CoreError) -> Error {
    match e {
        CoreError::Task(e) => task_error(e),
        other => core(other),
    }
}
/// The column rules of the session's task; `None` outside a task session and
/// for contexts that are not a SQL session.
fn task_rules(
    session: &mut impl Context,
) -> Result<Option<crate::task::columns::NativeRules>> {
    match session.session() {
        Ok(sql) => crate::task::columns::NativeRules::load(sql).map_err(task_error),
        Err(_) => Ok(None),
    }
}
/// GD-0061 R3-304: a task request is refused by class before resolution,
/// resolves only as a table (a view or unknown name is another relation), and
/// binds the request `apply_native` returns (empty projections expanded).
/// R3-308: a write is then refused (the protected commit is a task's only
/// write path), and the plan is returned for the release tag.
fn task_target(
    session: &mut impl Context,
    rules: &crate::task::columns::NativeRules,
    request: &Request,
) -> Result<(TableDef, Request, crate::task::columns::OutputPlan)> {
    use crate::task::{DenyRule, TaskError};
    rules.admit(request).map_err(task_error)?;
    let key = native_relation_key(request.table()?);
    let table = match session.resolve_table_engine(&key) {
        Ok(table) => table,
        Err(CoreError::Sql(SqlError::UnknownTable)) => {
            return Err(task_error(TaskError::Denied(DenyRule::OtherTable)));
        }
        Err(e) => return Err(core(e)),
    };
    let (request, plan) = rules.apply(&table, request).map_err(task_error)?;
    rules.refuse_write(&request).map_err(task_error)?;
    Ok((table, request, plan))
}
/// Resolve a request's target. Reads may name a view, as SQL FROM may; every
/// other request, and any unknown name, keeps the table resolution and 42P01.
fn resolve_relation(session: &mut impl Context, key: &str, request: &Request) -> Result<TableDef> {
    match session.resolve_table_engine(key) {
        Err(CoreError::Sql(SqlError::UnknownTable))
            if matches!(request, Request::Read { .. } | Request::ReadExpressions { .. }) =>
        {
            session.resolve_view_engine(key)
                .and_then(|view| view.ok_or(CoreError::Sql(SqlError::UnknownTable)))
        }
        other => other,
    }
    .map_err(core)
}
/// Whether a bound read's descriptor is a current view (its prepared fence).
fn current_view(session: &mut SqlSession, table: &TableDef, operation: &Operation) -> Result<bool> {
    Ok(table.primary_key.is_empty()
        && matches!(operation, Operation::Read { .. } | Operation::ReadExpressions { .. })
        && session.native_view_engine_current(table).map_err(core)?)
}
fn bind_table(
    table: TableDef,
    request: &Request,
    bindings: &Bindings,
    max_read_rows: usize,
) -> Result<(TableDef, Operation)> {
    if table.hidden_rowid().is_some() {
        return Err(Error::Unsupported);
    }
    let mut binder = Binder {
        table: Some(&table),
        allow_fields: true,
        allow_conflict_fields: false,
        locals: None,
        bindings,
        used: BTreeSet::new(),
        remaining_value_bytes: MAX_BOUND_VALUE_BYTES,
        row_limit: MAX_ROWS,
    };
    let operation = binder.bind(request, max_read_rows)?;
    // Every recorded name resolved successfully, so used is a subset. Counts
    // establish exact membership without copying any unused caller-owned keys.
    if binder.used.len() != bindings.len() {
        return Err(Error::Bind("parameters".into()));
    }
    Ok((table, operation))
}
// Native references contain one or two canonical components; never accept wire keys.
fn relation_identifier(s: &str) -> bool {
    s.split_once('.').map_or_else(|| identifier(s), |(schema, name)| {
        identifier(schema) && identifier(name)
    })
}
pub(crate) fn native_relation_key(s: &str) -> String {
    s.split_once('.').map_or_else(|| s.into(), |(schema, name)| {
        format!("{schema}\0{name}")
    })
}
// Canonical catalog names, independent of frontend-specific reserved words.
fn identifier(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 63
        && s.as_bytes()[0].is_ascii_alphabetic()
        && s.bytes()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_')
}
fn validate_size(request: &impl Serialize) -> Result<()> {
    validate_size_within(request, MAX_INPUT).map(drop)
}
/// The serialized size of `request`, or Limit once it exceeds `budget`.
fn validate_size_within(request: &impl Serialize, budget: usize) -> Result<usize> {
    // Preserve the serialized request limit without allocating a serialized
    // copy of an oversized caller-owned literal or assignment collection.
    struct Counter(usize, usize);
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > self.1 - self.0 {
                return Err(std::io::Error::other("request size limit"));
            }
            self.0 += bytes.len();
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter(0, budget);
    serde_json::to_writer(&mut counter, request).map_err(|_| Error::Limit)?;
    Ok(counter.0)
}
fn validate_request_nodes(stmt: &Request, nodes: &mut usize, v2: bool) -> Result<()> {
    let RequestParts {
        body: stmt,
        returning,
        conflict,
    } = stmt.parts()?;
    let mut pending = vec![];
    if let Some(projection) = returning {
        pending.extend(projection.iter().map(|item| (&item.expression, 0)));
    }
    if let Some((_, ConflictAction::Update { guard, values })) = conflict {
        pending.extend(guard.iter().map(|expression| (expression, 0)));
        pending.extend(values.iter().map(|(_, expression)| (expression, 0)));
    }
    match stmt {
        Request::Read {
            filter: Some(e), ..
        } => pending.push((e, 0)),
        Request::Update { filter, guard, .. } | Request::Delete { filter, guard, .. } => {
            pending.push((filter, 0));
            if let Some(e) = guard {
                pending.push((e, 0))
            }
        }
        Request::InsertMany { columns, rows, .. } if !v2 => {
            validate_insert_shape(columns, rows, MAX_INSERT_ROWS)?;
            let roots = rows
                .len()
                .checked_mul(columns.len())
                .and_then(|count| count.checked_add(pending.len()))
                .ok_or(Error::Limit)?;
            if roots > MAX_EXPRESSION_NODES.saturating_sub(*nodes) {
                return Err(Error::Limit);
            }
            pending.extend(rows.iter().flatten().map(|expression| (expression, 0)));
        }
        Request::InsertMany { columns, rows, .. } => {
            // V2 row counts are unbounded; proposed bytes are admitted at bind.
            validate_insert_shape(columns, rows, usize::MAX)?;
            // Tuples are flat, as SQL VALUES: each has its own node budget.
            for row in rows {
                validate_expression_nodes(row.iter().map(|e| (e, 0)).collect(), &mut 0)?;
            }
        }
        Request::InsertExpressions { values, .. } => {
            if values.len() > crate::relational::MAX_TABLE_COLUMNS {
                return Err(Error::Bind("assignments".into()));
            }
            pending.extend(values.iter().map(|(_, e)| (e, 0)));
        }
        Request::UpdateExpressions {
            filter,
            guard,
            values,
            ..
        } => {
            if values.len() > crate::relational::MAX_TABLE_COLUMNS {
                return Err(Error::Bind("assignments".into()));
            }
            pending.push((filter, 0));
            pending.extend(guard.iter().map(|e| (e, 0)));
            pending.extend(values.iter().map(|(_, e)| (e, 0)));
        }
        Request::ReadExpressions {
            filter, projection, ..
        } => {
            if projection.len() > crate::relational::MAX_TARGET_COLUMNS {
                return Err(Error::Limit);
            }
            pending.extend(filter.iter().map(|e| (e, 0)));
            pending.extend(projection.iter().map(|item| (&item.expression, 0)));
        }
        _ => (),
    }
    validate_expression_nodes(pending, nodes)
}
fn validate_insert_shape(columns: &[String], rows: &[Vec<Expr>], max_rows: usize) -> Result<()> {
    if columns.len() > crate::relational::MAX_TABLE_COLUMNS || rows.len() > max_rows {
        return Err(Error::Limit);
    }
    if columns.is_empty() {
        return Err(Error::Bind("columns".into()));
    }
    if rows.iter().any(|row| row.len() != columns.len()) {
        return Err(Error::Type);
    }
    Ok(())
}
fn validate_expression_nodes(mut pending: Vec<(&Expr, usize)>, nodes: &mut usize) -> Result<()> {
    while let Some((e, d)) = pending.pop() {
        *nodes += 1;
        if d > MAX_DEPTH || *nodes > MAX_EXPRESSION_NODES {
            return Err(Error::Limit);
        }
        match e {
            Expr::Case {
                operand,
                arms,
                else_expr,
            } => {
                validate_case(arms.len())?;
                let children = arms.len() * 2
                    + usize::from(operand.is_some())
                    + usize::from(else_expr.is_some());
                if children
                    > MAX_EXPRESSION_NODES
                        .saturating_sub(*nodes)
                        .saturating_sub(pending.len())
                {
                    return Err(Error::Limit);
                }
                pending.extend(operand.iter().map(|expr| (expr.as_ref(), d + 1)));
                pending.extend(
                    arms.iter()
                        .flat_map(|(when, then)| [(when, d + 1), (then, d + 1)]),
                );
                pending.extend(else_expr.iter().map(|expr| (expr.as_ref(), d + 1)));
            }
            Expr::Call {
                function,
                arguments,
            } => {
                validate_call(*function, arguments.len())?;
                if arguments.len()
                    > MAX_EXPRESSION_NODES
                        .saturating_sub(*nodes)
                        .saturating_sub(pending.len())
                {
                    return Err(Error::Limit);
                }
                pending.extend(arguments.iter().map(|argument| (argument, d + 1)));
            }
            Expr::Compare(a, _, b)
            | Expr::And(a, b)
            | Expr::Or(a, b)
            | Expr::Arithmetic(a, _, b) => {
                pending.push((a, d + 1));
                pending.push((b, d + 1))
            }
            Expr::IsNull(e, _) | Expr::Not(e) | Expr::Negate(e) => pending.push((e, d + 1)),
            _ => (),
        }
    }
    Ok(())
}
fn validate_case(arm_count: usize) -> Result<()> {
    if arm_count > MAX_CASE_ARMS {
        return Err(Error::Limit);
    }
    if arm_count == 0 {
        return Err(Error::Type);
    }
    Ok(())
}
fn validate_call(function: ScalarFunction, argument_count: usize) -> Result<()> {
    if argument_count > MAX_FUNCTION_ARGUMENTS {
        return Err(Error::Limit);
    }
    let valid = match function {
        ScalarFunction::Coalesce => argument_count >= 2,
        ScalarFunction::Abs | ScalarFunction::Length => argument_count == 1,
    };
    if valid { Ok(()) } else { Err(Error::Type) }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Ty {
    Bool,
    Int,
    Text,
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
fn unify_type(left: Option<Ty>, right: Option<Ty>) -> Result<Option<Ty>> {
    if left == Some(Ty::Bpchar) || right == Some(Ty::Bpchar) {
        return Err(Error::Unsupported);
    }
    if left.is_some() && right.is_some() && left != right {
        return Err(Error::Type);
    }
    Ok(left.or(right))
}
fn numeric_literal(bound: &mut Bound, ty: &mut Option<Ty>, other: Option<Ty>) {
    if other == Some(Ty::Numeric)
        && let Bound::Value(Value::Int(n)) = bound
        && let Ok(numeric) = crate::relational::Numeric::parse(&n.to_string())
    {
        *bound = Bound::Value(Value::Numeric(numeric));
        *ty = Some(Ty::Numeric);
    }
}
fn ty(v: &Value) -> Option<Ty> {
    match v {
        Value::Null => None,
        Value::Bool(_) => Some(Ty::Bool),
        Value::Int(_) => Some(Ty::Int),
        Value::Text(_) => Some(Ty::Text),
        Value::Bytes(_) => Some(Ty::Bytes),
        Value::Numeric(_) => Some(Ty::Numeric),
        Value::Float4(_) => Some(Ty::Real),
        Value::Float8(_) => Some(Ty::DoublePrecision),
        Value::Date(_) => Some(Ty::Date),
        Value::Time(_) => Some(Ty::Time),
        Value::TimeTz(_) => Some(Ty::TimeTz),
        Value::Timestamp(_) => Some(Ty::Timestamp),
        Value::TimestampTz(_) => Some(Ty::TimestampTz),
        Value::Interval(_) => Some(Ty::Interval),
    }
}
fn colty(c: &ColumnDef) -> Ty {
    match c.data_type {
        DataType::Boolean => Ty::Bool,
        DataType::SmallInt | DataType::Integer | DataType::BigInt => Ty::Int,
        DataType::RegType
        | DataType::ExpressionArray(_)
        | DataType::Uuid
        | DataType::Jsonb
        | DataType::IntArray
        | DataType::BigIntArray
        | DataType::TextArray
        | DataType::UuidArray
        | DataType::TsVector | DataType::TsQuery | DataType::RegConfig | DataType::RegDictionary
        | DataType::Oid | DataType::PgChar | DataType::RealArray | DataType::PgCharArray | DataType::SmallIntArray | DataType::RegDictionaryArray
        | DataType::Text
        | DataType::Varchar => Ty::Text,
        DataType::Bpchar => Ty::Bpchar,
        DataType::Bytes => Ty::Bytes,
        DataType::Numeric => Ty::Numeric,
        DataType::Real => Ty::Real,
        DataType::DoublePrecision => Ty::DoublePrecision,
        DataType::Date => Ty::Date,
        DataType::Time => Ty::Time,
        DataType::TimeTz => Ty::TimeTz,
        DataType::Timestamp => Ty::Timestamp,
        DataType::TimestampTz => Ty::TimestampTz,
        DataType::Interval => Ty::Interval,
    }
}
fn assignment_error(source: Option<Ty>, column: &ColumnDef) -> Error {
    if matches!((source, colty(column)), (Some(Ty::Text), Ty::Int | Ty::Numeric) | (Some(Ty::Bool), Ty::Int)) {
        core(CoreError::Sql(SqlError::Type))
    } else { Error::Type }
}
fn check(v: &Value, c: &ColumnDef) -> Result<()> {
    if *v == Value::Null {
        return if c.not_null { Err(core(CoreError::Sql(SqlError::Null))) } else { Ok(()) };
    }
    if ty(v)
        != Some(if c.data_type == DataType::Bpchar {
            Ty::Text
        } else {
            colty(c)
        })
    {
        return Err(assignment_error(ty(v), c));
    }
    if let Value::Int(n) = v {
        match c.data_type {
            DataType::SmallInt if i16::try_from(*n).is_err() => return Err(Error::Overflow),
            DataType::Integer if i32::try_from(*n).is_err() => return Err(Error::Overflow),
            _ => (),
        }
    }
    Ok(())
}
#[derive(Clone)]
enum Bound {
    Value(Value),
    Local(transaction::BoundLocal),
    Field(usize),
    ConflictField {
        source: ConflictSource,
        index: usize,
    },
    Compare(Box<Bound>, Compare, Box<Bound>),
    IsNull(Box<Bound>, bool),
    Not(Box<Bound>),
    And(Box<Bound>, Box<Bound>),
    Or(Box<Bound>, Box<Bound>),
    Negate(Box<Bound>),
    Arithmetic(Box<Bound>, Arithmetic, Box<Bound>),
    Call {
        function: ScalarFunction,
        arguments: Vec<Bound>,
    },
    Case {
        operand: Option<Box<Bound>>,
        arms: Vec<(Bound, Bound)>,
        else_expr: Option<Box<Bound>>,
    },
}
struct Binder<'a> {
    table: Option<&'a TableDef>,
    allow_fields: bool,
    allow_conflict_fields: bool,
    locals: Option<&'a [transaction::LocalShape]>,
    bindings: &'a Bindings,
    used: BTreeSet<String>,
    remaining_value_bytes: usize,
    /// Engine row ceiling for explicit counts: legacy keeps MAX_ROWS, V2 has none.
    row_limit: usize,
}
impl Binder<'_> {
    fn table(&self) -> Result<&TableDef> {
        self.table.ok_or_else(|| Error::Bind("column scope".into()))
    }
    fn field(&self, s: &str) -> Result<usize> {
        if !identifier(s) {
            return Err(Error::Bind("identifier".into()));
        }
        self.table()?
            .columns
            .iter()
            .position(|c| c.name == s)
            .ok_or_else(|| core(CoreError::Sql(SqlError::UnknownColumn)))
    }
    fn value(&mut self, e: &ValueExpr) -> Result<Value> {
        let (value, parameter) = match e {
            ValueExpr::Literal(v) => (v, None),
            ValueExpr::Parameter(n) => {
                if n.len() > 63
                    || n.is_empty()
                    || !(n.as_bytes()[0].is_ascii_alphabetic() || n.starts_with('_'))
                    || !n
                        .bytes()
                        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_')
                {
                    return Err(Error::Bind("parameter name".into()));
                }
                let value = self
                    .bindings
                    .get(n)
                    .ok_or_else(|| Error::Bind("parameter missing".into()))?;
                (value, Some(n))
            }
        };
        // A small map can still expand one large value many times. Charge each
        // resolution before cloning either the payload or a newly used name.
        self.remaining_value_bytes = self
            .remaining_value_bytes
            .checked_sub(value_bytes(value))
            .ok_or(Error::Limit)?;
        if let Some(name) = parameter
            && !self.used.contains(name)
        {
            self.used.insert(name.clone());
        }
        Ok(value.clone())
    }
    fn count(&mut self, e: &ValueExpr) -> Result<usize> {
        self.read_count(e, self.row_limit)
    }
    fn read_count(&mut self, e: &ValueExpr, maximum: usize) -> Result<usize> {
        match self.value(e)? {
            Value::Int(n) => usize::try_from(n).ok().filter(|n| *n <= maximum).ok_or(Error::Type),
            _ => Err(Error::Type),
        }
    }
    fn expr(&mut self, e: &Expr, d: usize) -> Result<(Bound, Option<Ty>)> {
        if d > MAX_DEPTH {
            return Err(Error::Limit);
        }
        let boolean = |t: Option<Ty>| {
            if t.is_none() || t == Some(Ty::Bool) {
                Ok(())
            } else {
                Err(Error::Type)
            }
        };
        Ok(match e {
            Expr::ConflictField { source, field } => {
                if !self.allow_conflict_fields {
                    return Err(Error::Bind("conflict scope".into()));
                }
                let index = self.field(field)?;
                (
                    Bound::ConflictField {
                        source: *source,
                        index,
                    },
                    Some(colty(&self.table()?.columns[index])),
                )
            }
            Expr::Case {
                operand,
                arms,
                else_expr,
            } => {
                validate_case(arms.len())?;
                let (bound_operand, mut comparison_type) = match operand {
                    Some(expression) => {
                        let (expression, ty) = self.expr(expression, d + 1)?;
                        (Some(Box::new(expression)), ty)
                    }
                    None => (None, None),
                };
                let mut result_type = None;
                let mut bound_arms = Vec::with_capacity(arms.len());
                // Bind every condition and result before execution. Simple
                // CASE has one comparison family even when its base is NULL;
                // result branches have a separate compatible family.
                for (when, then) in arms {
                    let (when, when_type) = self.expr(when, d + 1)?;
                    if operand.is_some() {
                        comparison_type = unify_type(comparison_type, when_type)?;
                    } else {
                        boolean(when_type)?;
                    }
                    let (then, then_type) = self.expr(then, d + 1)?;
                    result_type = unify_type(result_type, then_type)?;
                    bound_arms.push((when, then));
                }
                let bound_else = match else_expr {
                    Some(expression) => {
                        let (expression, ty) = self.expr(expression, d + 1)?;
                        result_type = unify_type(result_type, ty)?;
                        Some(Box::new(expression))
                    }
                    // The implicit NULL is not an explicit input occurrence.
                    None => None,
                };
                (
                    Bound::Case {
                        operand: bound_operand,
                        arms: bound_arms,
                        else_expr: bound_else,
                    },
                    result_type,
                )
            }
            Expr::Call {
                function,
                arguments,
            } => {
                validate_call(*function, arguments.len())?;
                let mut bound = Vec::with_capacity(arguments.len());
                let mut result_type = None;
                // Binding and value admission cover every argument even when
                // COALESCE will not evaluate a later argument at runtime.
                for argument in arguments {
                    let (argument, argument_type) = self.expr(argument, d + 1)?;
                    if argument_type == Some(Ty::Bpchar) {
                        return Err(Error::Unsupported);
                    }
                    match function {
                        ScalarFunction::Coalesce => {
                            if let Some(argument_type) = argument_type {
                                if result_type.is_some_and(|ty| ty != argument_type) {
                                    return Err(Error::Type);
                                }
                                result_type = Some(argument_type);
                            }
                        }
                        ScalarFunction::Abs => {
                            if argument_type.is_some_and(|ty| ty != Ty::Int) {
                                return Err(Error::Type);
                            }
                        }
                        ScalarFunction::Length => {
                            if argument_type.is_some_and(|ty| !matches!(ty, Ty::Text | Ty::Bytes)) {
                                return Err(Error::Type);
                            }
                        }
                    }
                    bound.push(argument);
                }
                (
                    Bound::Call {
                        function: *function,
                        arguments: bound,
                    },
                    match function {
                        ScalarFunction::Coalesce => result_type,
                        ScalarFunction::Abs | ScalarFunction::Length => Some(Ty::Int),
                    },
                )
            }
            Expr::Value(v) => {
                let v = self.value(v)?;
                let t = ty(&v);
                (Bound::Value(v), t)
            }
            Expr::Local(reference) => {
                let local =
                    transaction::bind_local(self.locals.ok_or(Error::Unsupported)?, reference)?;
                let ty = local.ty();
                (Bound::Local(local), Some(ty))
            }
            Expr::Field(n) => {
                if !self.allow_fields {
                    return Err(Error::Bind("column scope".into()));
                }
                let i = self.field(n)?;
                (Bound::Field(i), Some(colty(&self.table()?.columns[i])))
            }
            Expr::Negate(e) => {
                let (e, t) = self.expr(e, d + 1)?;
                if t == Some(Ty::Bpchar) {
                    return Err(Error::Unsupported);
                }
                if t.is_some() && t != Some(Ty::Int) {
                    return Err(Error::Type);
                }
                (Bound::Negate(Box::new(e)), Some(Ty::Int))
            }
            Expr::Arithmetic(a, op, b) => {
                let (a, ta) = self.expr(a, d + 1)?;
                let (b, tb) = self.expr(b, d + 1)?;
                if ta == Some(Ty::Bpchar) || tb == Some(Ty::Bpchar) {
                    return Err(Error::Unsupported);
                }
                if [ta, tb]
                    .into_iter()
                    .any(|t| t.is_some() && t != Some(Ty::Int))
                {
                    return Err(Error::Type);
                }
                (
                    Bound::Arithmetic(Box::new(a), *op, Box::new(b)),
                    Some(Ty::Int),
                )
            }
            Expr::Compare(a, op, b) => {
                let (mut a, mut ta) = self.expr(a, d + 1)?;
                let (mut b, mut tb) = self.expr(b, d + 1)?;
                // As in SQL, an integer literal compared with a numeric operand
                // is resolved as numeric (`k = 1` on a NUMERIC column).
                numeric_literal(&mut a, &mut ta, tb);
                numeric_literal(&mut b, &mut tb, ta);
                if ta == Some(Ty::Bpchar) || tb == Some(Ty::Bpchar) {
                    return Err(Error::Unsupported);
                }
                if ta.is_some() && tb.is_some() && ta != tb {
                    return Err(if matches!((ta, tb), (Some(Ty::Int), Some(Ty::Text | Ty::Bool)) | (Some(Ty::Text | Ty::Bool), Some(Ty::Int))) {
                        core(CoreError::Sql(SqlError::UndefinedFunction))
                    } else { Error::Type });
                }
                (
                    Bound::Compare(Box::new(a), *op, Box::new(b)),
                    Some(Ty::Bool),
                )
            }
            Expr::IsNull(e, neg) => {
                let (e, _) = self.expr(e, d + 1)?;
                (Bound::IsNull(Box::new(e), *neg), Some(Ty::Bool))
            }
            Expr::Not(e) => {
                let (e, t) = self.expr(e, d + 1)?;
                if t == Some(Ty::Bpchar) {
                    return Err(Error::Unsupported);
                }
                boolean(t)?;
                (Bound::Not(Box::new(e)), Some(Ty::Bool))
            }
            Expr::And(a, b) | Expr::Or(a, b) => {
                let (a, ta) = self.expr(a, d + 1)?;
                let (b, tb) = self.expr(b, d + 1)?;
                if ta == Some(Ty::Bpchar) || tb == Some(Ty::Bpchar) {
                    return Err(Error::Unsupported);
                }
                boolean(ta)?;
                boolean(tb)?;
                (
                    if matches!(e, Expr::And(..)) {
                        Bound::And(Box::new(a), Box::new(b))
                    } else {
                        Bound::Or(Box::new(a), Box::new(b))
                    },
                    Some(Ty::Bool),
                )
            }
        })
    }
    fn predicate(&mut self, e: &Expr) -> Result<Bound> {
        let (e, t) = self.expr(e, 0)?;
        if t.is_some() && t != Some(Ty::Bool) {
            return Err(Error::Type);
        }
        Ok(e)
    }
    fn optional(&mut self, e: &Option<Expr>) -> Result<Bound> {
        match e {
            Some(e) => self.predicate(e),
            None => Ok(Bound::Value(Value::Bool(true))),
        }
    }
    fn order(&self, order: &[Sort]) -> Result<Vec<(usize, bool)>> {
        let mut seen = BTreeSet::new();
        let mut out = vec![];
        for o in order {
            if self.table()?.columns[self.field(&o.field)?].data_type == DataType::Bpchar {
                return Err(Error::Unsupported);
            }
            let i = self.field(&o.field)?;
            if !seen.insert(i) {
                return Err(Error::Bind("duplicate order".into()));
            }
            out.push((i, o.descending))
        }
        for i in &self.table()?.primary_key {
            if seen.insert(*i) {
                out.push((*i, false))
            }
        }
        Ok(out)
    }
    fn insert_targets<'a>(&self, names: impl IntoIterator<Item = &'a str>) -> Result<()> {
        let mut seen = BTreeSet::new();
        for name in names {
            if !seen.insert(self.field(name)?) {
                return Err(core(CoreError::Sql(SqlError::DuplicateColumn)));
            }
        }
        Ok(())
    }
    fn assignments(
        &mut self,
        v: &[(String, ValueExpr)],
        insert: bool,
    ) -> Result<Vec<(usize, Value)>> {
        if v.is_empty() || v.len() > crate::relational::MAX_TABLE_COLUMNS {
            return Err(Error::Bind("assignments".into()));
        }
        if insert { self.insert_targets(v.iter().map(|(name, _)| name.as_str()))?; }
        let mut seen = BTreeSet::new();
        let mut duplicate = false;
        let mut out = vec![];
        let mut deferred = None;
        for (n, e) in v {
            let i = self.field(n)?;
            if !seen.insert(i) {
                if insert { return Err(core(CoreError::Sql(SqlError::DuplicateColumn))); }
                duplicate = true;
            }
            if !insert && self.table()?.primary_key.contains(&i) {
                return Err(Error::Unsupported);
            }
            let v = assign_number(self.value(e)?, &self.table()?.columns[i])?;
            if (!insert || v != Value::Null)
                && let Err(error) = check(&v, &self.table()?.columns[i]) {
                if !insert && (error == Error::Overflow || error.sqlstate() == Some("23502")) {
                    if deferred.is_none() || error == Error::Overflow { deferred = Some(error); }
                } else { return Err(error); }
            }
            out.push((i, v))
        }
        if duplicate { return Err(core(CoreError::Sql(SqlError::Syntax))); }
        if let Some(error) = deferred { return Err(error); }
        Ok(out)
    }
    fn expression_assignments(&mut self, values: &[(String, Expr)]) -> Result<Vec<(usize, Bound)>> {
        if values.is_empty() || values.len() > crate::relational::MAX_TABLE_COLUMNS {
            return Err(Error::Bind("assignments".into()));
        }
        let mut seen = BTreeSet::new();
        let mut duplicate = false;
        let mut assignments = Vec::new();
        for (name, expression) in values {
            let index = self.field(name)?;
            duplicate |= !seen.insert(index);
            if self.table()?.primary_key.contains(&index) {
                return Err(Error::Unsupported);
            }
            let (expression, ty) = self.expr(expression, 0)?;
            if ty.is_some()
                && !numeric_assignment(ty, &self.table()?.columns[index])
                && ty.map(|ty| if ty == Ty::Bpchar { Ty::Text } else { ty })
                    != Some(
                        if self.table()?.columns[index].data_type == DataType::Bpchar {
                            Ty::Text
                        } else {
                            colty(&self.table()?.columns[index])
                        },
                    )
            {
                return Err(assignment_error(ty, &self.table()?.columns[index]));
            }
            // Column width and nullability depend on the selected postimages.
            // The legacy value-only request retains its eager checks above.
            assignments.push((index, expression));
        }
        if duplicate { return Err(core(CoreError::Sql(SqlError::Syntax))); }
        Ok(assignments)
    }
    fn projections(&mut self, items: &[Projection]) -> Result<Vec<BoundProjection>> {
        if items.is_empty() {
            return Err(Error::Bind("projection".into()));
        }
        if items.len() > crate::relational::MAX_TARGET_COLUMNS {
            return Err(Error::Limit);
        }
        let mut names = BTreeSet::new();
        let mut projection = Vec::new();
        for item in items {
            let (expression, ty) = self.expr(&item.expression, 0)?;
            let mut column = match &expression {
                Bound::Field(index) => self.table()?.columns[*index].clone(),
                _ => ColumnDef {
                    id: 0,
                    name: String::new(),
                    data_type: match ty.ok_or(Error::Type)? {
                        Ty::Bool => DataType::Boolean,
                        Ty::Int => DataType::BigInt,
                        Ty::Text => DataType::Text,
                        Ty::Bpchar => DataType::Bpchar,
                        Ty::Bytes => DataType::Bytes,
                        Ty::Numeric => DataType::Numeric,
                        Ty::Real => DataType::Real,
                        Ty::DoublePrecision => DataType::DoublePrecision,
                        Ty::Date => DataType::Date,
                        Ty::Time => DataType::Time,
                        Ty::TimeTz => DataType::TimeTz,
                        Ty::Timestamp => DataType::Timestamp,
                        Ty::TimestampTz => DataType::TimestampTz,
                        Ty::Interval => DataType::Interval,
                    },
                    not_null: !expression.nullable(self.table),
                },
            };
            if let Some(alias) = &item.alias {
                if !identifier(alias) {
                    return Err(Error::Bind("identifier".into()));
                }
                column.name = alias.clone();
            } else if !matches!(expression, Bound::Field(_)) {
                return Err(Error::Bind("projection alias".into()));
            }
            if !names.insert(column.name.clone()) {
                return Err(Error::Bind("duplicate projection".into()));
            }
            projection.push(BoundProjection { expression, column });
        }
        Ok(projection)
    }
    fn selection(&mut self, intent: &Intent) -> Result<Selection> {
        Ok(match intent {
            Intent::One => Selection::One,
            Intent::Many { cap } => Selection::Many(self.count(cap)?),
            Intent::Claim { order } => {
                if order.is_empty() {
                    return Err(Error::Bind("claim order".into()));
                }
                Selection::Claim(self.order(order)?)
            }
        })
    }
    fn bind(&mut self, s: &Request, max_read_rows: usize) -> Result<Operation> {
        self.row_limit = max_read_rows;
        let RequestParts {
            body: s,
            returning,
            conflict,
        } = s.parts()?;
        let mut operation = match s {
            Request::Read {
                filter,
                order,
                limit,
                projection,
                ..
            } => {
                let filter = self.optional(filter)?;
                let order = self.order(order)?;
                let limit = match limit {
                    Some(v) => self.read_count(v, max_read_rows)?,
                    None => 100,
                };
                let projection = if projection.is_empty() {
                    (0..self.table()?.columns.len()).collect()
                } else {
                    let mut seen = BTreeSet::new();
                    let mut out = vec![];
                    for n in projection {
                        let i = self.field(n)?;
                        if !seen.insert(i) {
                            return Err(Error::Bind("duplicate projection".into()));
                        }
                        out.push(i)
                    }
                    out
                };
                Operation::Read {
                    filter,
                    order,
                    limit,
                    projection,
                }
            }
            Request::ReadExpressions {
                filter,
                order,
                limit,
                projection,
                ..
            } => Operation::ReadExpressions {
                filter: self.optional(filter)?,
                order: self.order(order)?,
                limit: match limit {
                    Some(value) => self.read_count(value, max_read_rows)?,
                    None => 100,
                },
                projection: self.projections(projection)?,
            },
            Request::InsertMany {
                columns, rows, cap, ..
            } => {
                validate_insert_shape(columns, rows, self.row_limit)?;
                // V2 tuple literals are data, as SQL VALUES: they also share the
                // proposed-row budget. That extra budget is spent first and only
                // by these tuples; the base budget is restored afterwards, so
                // later clauses and transaction steps never inherit it.
                let base = self.remaining_value_bytes;
                if self.row_limit > MAX_ROWS {
                    self.remaining_value_bytes = base
                        .saturating_add(MAX_INSERT_BYTES.saturating_sub(MAX_BOUND_VALUE_BYTES));
                }
                self.insert_targets(columns.iter().map(String::as_str))?;
                let mut seen = BTreeSet::new();
                let mut indexes = Vec::new();
                for name in columns {
                    let index = self.field(name)?;
                    if !seen.insert(index) {
                        return Err(core(CoreError::Sql(SqlError::DuplicateColumn)));
                    }
                    indexes.push(index);
                }
                let mut bound_rows = Vec::new();
                for row in rows {
                    let mut bound_row = Vec::new();
                    for (expression, index) in row.iter().zip(&indexes) {
                        self.allow_fields = false;
                        let bound = self.expr(expression, 0);
                        self.allow_fields = true;
                        let (expression, ty) = bound?;
                        if ty.is_some()
                            && !numeric_assignment(ty, &self.table()?.columns[*index])
                            && ty.map(|ty| if ty == Ty::Bpchar { Ty::Text } else { ty })
                                != Some(
                                    if self.table()?.columns[*index].data_type == DataType::Bpchar {
                                        Ty::Text
                                    } else {
                                        colty(&self.table()?.columns[*index])
                                    },
                                )
                        {
                            return Err(assignment_error(ty, &self.table()?.columns[*index]));
                        }
                        bound_row.push(expression);
                    }
                    bound_rows.push(bound_row);
                }
                self.remaining_value_bytes = self.remaining_value_bytes.min(base);
                Operation::InsertMany {
                    columns: indexes,
                    rows: bound_rows,
                    cap: self.count(cap)?,
                    returning: None,
                }
            }
            Request::InsertExpressions { values, .. } => {
                if values.is_empty() || values.len() > crate::relational::MAX_TABLE_COLUMNS {
                    return Err(Error::Bind("assignments".into()));
                }
                self.insert_targets(values.iter().map(|(name, _)| name.as_str()))?;
                let mut seen = BTreeSet::new();
                let mut expressions = vec![Bound::Value(Value::Null); self.table()?.columns.len()];
                for (name, expression) in values {
                    let index = self.field(name)?;
                    if !seen.insert(index) {
                        return Err(core(CoreError::Sql(SqlError::DuplicateColumn)));
                    }
                    self.allow_fields = false;
                    let bound = self.expr(expression, 0);
                    self.allow_fields = true;
                    let (expression, ty) = bound?;
                    if ty.is_some()
                        && !numeric_assignment(ty, &self.table()?.columns[index])
                        && ty.map(|ty| if ty == Ty::Bpchar { Ty::Text } else { ty })
                            != Some(
                                if self.table()?.columns[index].data_type == DataType::Bpchar {
                                    Ty::Text
                                } else {
                                    colty(&self.table()?.columns[index])
                                },
                            )
                    {
                        return Err(assignment_error(ty, &self.table()?.columns[index]));
                    }
                    expressions[index] = expression;
                }
                if seen.len() == expressions.len() {
                    Operation::InsertExpressions { values: expressions, returning: None }
                } else {
                    let values = expressions.into_iter().enumerate()
                        .filter_map(|(i, value)| seen.contains(&i).then_some(value)).collect();
                    Operation::InsertMany {
                        columns: seen.into_iter().collect(), rows: vec![values], cap: 1, returning: None,
                    }
                }
            }
            Request::Insert { values, .. } => {
                let assignments = self.assignments(values, true)?;
                let (columns, values) = assignments.into_iter().unzip();
                Operation::Insert {
                    columns,
                    values,
                    returning: None,
                }
            }
            Request::Update {
                filter,
                guard,
                values,
                intent,
                ..
            } => {
                if guard.is_some() && matches!(intent, Intent::Claim { .. }) {
                    return Err(Error::Unsupported);
                }
                let filter = self.predicate(filter)?;
                let guard = self.optional(guard)?;
                let sets = self.assignments(values, false)?;
                let selection = self.selection(intent)?;
                Operation::Mutate {
                    filter,
                    guard,
                    sets: Some(Assignments::Values(sets)),
                    selection,
                    returning: None,
                }
            }
            Request::UpdateExpressions {
                filter,
                guard,
                values,
                intent,
                ..
            } => {
                if guard.is_some() && matches!(intent, Intent::Claim { .. }) {
                    return Err(Error::Unsupported);
                }
                Operation::Mutate {
                    filter: self.predicate(filter)?,
                    guard: self.optional(guard)?,
                    sets: Some(Assignments::Expressions(
                        self.expression_assignments(values)?,
                    )),
                    selection: self.selection(intent)?,
                    returning: None,
                }
            }
            Request::Delete {
                filter, guard, cap, ..
            } => Operation::Mutate {
                filter: self.predicate(filter)?,
                guard: self.optional(guard)?,
                sets: None,
                selection: match cap {
                    Some(v) => Selection::Many(self.count(v)?),
                    None => Selection::One,
                },
                returning: None,
            },
            Request::Returning { .. } | Request::OnConflict { .. } => {
                return Err(Error::Unsupported);
            }
        };
        if let Some((target, action)) = conflict {
            let mut indexes = BTreeSet::new();
            for name in target {
                if !indexes.insert(self.field(name)?) {
                    return Err(Error::Bind("conflict target".into()));
                }
            }
            if indexes
                != self
                    .table()?
                    .primary_key
                    .iter()
                    .copied()
                    .collect::<BTreeSet<_>>()
            {
                return Err(Error::Bind("conflict target".into()));
            }
            let old_fields = self.allow_fields;
            self.allow_fields = false;
            self.allow_conflict_fields = true;
            let bound_action: Result<BoundConflict> = (|| {
                Ok(match action {
                    ConflictAction::Nothing => BoundConflict::Nothing,
                    ConflictAction::Update { guard, values } => BoundConflict::Update {
                        guard: self.optional(guard)?,
                        values: self.expression_assignments(values)?,
                    },
                })
            })();
            self.allow_fields = old_fields;
            self.allow_conflict_fields = false;
            let action = bound_action?;
            let (columns, rows, cap) = match operation {
                Operation::Insert { columns, values, .. } => (
                    columns,
                    vec![values.into_iter().map(Bound::Value).collect()],
                    1,
                ),
                Operation::InsertExpressions { values, .. } => {
                    ((0..values.len()).collect(), vec![values], 1)
                }
                Operation::InsertMany {
                    columns, rows, cap, ..
                } => (columns, rows, cap),
                _ => return Err(Error::Unsupported),
            };
            operation = Operation::Upsert {
                columns,
                rows,
                cap,
                action,
                returning: None,
            };
        }
        if let Some(items) = returning {
            let projection = if items.is_empty() {
                self.table()?
                    .columns
                    .iter()
                    .enumerate()
                    .map(|(index, column)| BoundProjection {
                        expression: Bound::Field(index),
                        column: column.clone(),
                    })
                    .collect()
            } else {
                self.projections(items)?
            };
            match &mut operation {
                Operation::Insert { returning, .. }
                | Operation::InsertExpressions { returning, .. }
                | Operation::InsertMany { returning, .. }
                | Operation::Upsert { returning, .. }
                | Operation::Mutate { returning, .. } => {
                    *returning = Some(projection);
                }
                _ => return Err(Error::Unsupported),
            }
        }
        Ok(operation)
    }
}
#[derive(Clone)]
enum Selection {
    One,
    Many(usize),
    Claim(Vec<(usize, bool)>),
}
#[derive(Clone)]
struct BoundProjection {
    expression: Bound,
    column: ColumnDef,
}
#[derive(Clone)]
enum Assignments {
    Values(Vec<(usize, Value)>),
    Expressions(Vec<(usize, Bound)>),
}
/// SQL-only prebound scalar evaluation; deliberately outside serialized types.
pub(crate) trait SqlConflictEvaluation {
    fn guard(&self, old: &SqlRow, incoming: &[Value]) -> std::result::Result<bool, SqlError>;
    fn values(
        &self,
        table: &TableDef,
        old: &SqlRow,
        incoming: &[Value],
    ) -> std::result::Result<Vec<(usize, Value)>, SqlError>;
}

#[derive(Clone)]
enum BoundConflict {
    Nothing,
    Update {
        guard: Bound,
        values: Vec<(usize, Bound)>,
    },
}
#[derive(Clone)]
enum Operation {
    Upsert {
        columns: Vec<usize>,
        rows: Vec<Vec<Bound>>,
        cap: usize,
        action: BoundConflict,
        returning: Option<Vec<BoundProjection>>,
    },
    InsertMany {
        columns: Vec<usize>,
        rows: Vec<Vec<Bound>>,
        cap: usize,
        returning: Option<Vec<BoundProjection>>,
    },
    InsertExpressions {
        values: Vec<Bound>,
        returning: Option<Vec<BoundProjection>>,
    },
    Read {
        filter: Bound,
        order: Vec<(usize, bool)>,
        limit: usize,
        projection: Vec<usize>,
    },
    ReadExpressions {
        filter: Bound,
        order: Vec<(usize, bool)>,
        limit: usize,
        projection: Vec<BoundProjection>,
    },
    Insert {
        columns: Vec<usize>,
        values: Vec<Value>,
        returning: Option<Vec<BoundProjection>>,
    },
    Mutate {
        filter: Bound,
        guard: Bound,
        sets: Option<Assignments>,
        selection: Selection,
        returning: Option<Vec<BoundProjection>>,
    },
}
fn compare(a: &Value, b: &Value) -> Ordering {
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Greater,
        (_, Value::Null) => Ordering::Less,
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        (Value::Numeric(a), Value::Numeric(b)) => a.cmp(b),
        (Value::Float4(a), Value::Float4(b)) => a.cmp(b),
        (Value::Float8(a), Value::Float8(b)) => a.cmp(b),
        (Value::Int(a), Value::Int(b)) => a.cmp(b),
        (Value::Text(a), Value::Text(b)) => a.cmp(b),
        (Value::Bytes(a), Value::Bytes(b)) => a.cmp(b),
        (Value::Date(a), Value::Date(b)) => a.cmp(b),
        (Value::Time(a), Value::Time(b)) => a.cmp(b),
        (Value::TimeTz(a), Value::TimeTz(b)) => a.cmp(b),
        (Value::Timestamp(a), Value::Timestamp(b)) => a.cmp(b),
        (Value::TimestampTz(a), Value::TimestampTz(b)) => a.cmp(b),
        (Value::Interval(a), Value::Interval(b)) => a.cmp(b),
        _ => unreachable!("bound comparable types"),
    }
}
fn truth(v: &Value) -> Option<bool> {
    match v {
        Value::Bool(b) => Some(*b),
        Value::Null => None,
        _ => unreachable!("bound boolean"),
    }
}
fn boolval(v: Option<bool>) -> Value {
    v.map_or(Value::Null, Value::Bool)
}
fn arithmetic_error(error: crate::scalar::IntegerError) -> Error {
    match error {
        crate::scalar::IntegerError::Overflow => Error::Overflow,
        crate::scalar::IntegerError::DivisionByZero => Error::DivisionByZero,
    }
}
impl Bound {
    fn coerce_equality(&mut self, context: &impl Context, table: &TableDef) -> Result<()> {
        match self {
            Self::Compare(left, Compare::Eq, right) => {
                let pair = match (left.as_mut(), right.as_mut()) {
                    (Self::Field(index), Self::Value(value))
                    | (Self::Value(value), Self::Field(index)) => Some((*index, value)),
                    _ => None,
                };
                if let Some((index, value)) = pair
                    && matches!(table.columns[index].data_type, DataType::Uuid | DataType::Jsonb)
                {
                    // Logical text types require the same canonical form as SQL.
                    // Temporal comparison operands retain their original precision.
                    *value = context.coerce_column(&table.columns[index], value.clone())?;
                }
                left.coerce_equality(context, table)?;
                right.coerce_equality(context, table)?;
            }
            Self::Compare(left, _, right) | Self::Arithmetic(left, _, right)
            | Self::And(left, right) | Self::Or(left, right) => {
                left.coerce_equality(context, table)?;
                right.coerce_equality(context, table)?;
            }
            Self::Not(value) | Self::IsNull(value, _) | Self::Negate(value) => {
                value.coerce_equality(context, table)?;
            }
            Self::Call { arguments, .. } => {
                for value in arguments { value.coerce_equality(context, table)?; }
            }
            Self::Case { operand, arms, else_expr } => {
                if let Some(value) = operand { value.coerce_equality(context, table)?; }
                for (when, then) in arms {
                    when.coerce_equality(context, table)?;
                    then.coerce_equality(context, table)?;
                }
                if let Some(value) = else_expr { value.coerce_equality(context, table)?; }
            }
            _ => {}
        }
        Ok(())
    }
    fn nullable(&self, table: Option<&TableDef>) -> bool {
        match self {
            Self::Value(value) => *value == Value::Null,
            Self::Field(index) | Self::ConflictField { index, .. } => {
                table.is_none_or(|table| !table.columns[*index].not_null)
            }
            Self::Local(local) => local.nullable,
            // Keep function and conditional output metadata conservative;
            // binding knows the scalar family independently of runtime NULLs.
            Self::Call { .. } | Self::Case { .. } => true,
            Self::IsNull(..) => false,
            Self::Not(expression) | Self::Negate(expression) => expression.nullable(table),
            Self::Compare(left, _, right)
            | Self::Arithmetic(left, _, right)
            | Self::And(left, right)
            | Self::Or(left, right) => left.nullable(table) || right.nullable(table),
        }
    }
    fn eval<'a>(&'a self, row: &'a [Value]) -> Result<Cow<'a, Value>> {
        self.eval_fields(&|index| &row[index])
    }
    // An overlay can expose proposed fields without copying the old row. Leaf
    // payloads stay borrowed through validation, comparison and byte admission.
    fn eval_fields<'a>(&'a self, field: &impl Fn(usize) -> &'a Value) -> Result<Cow<'a, Value>> {
        self.eval_context(&|source, index| match source {
            None => Ok(field(index)),
            Some(_) => Err(Error::Bind("conflict scope".into())),
        })
    }
    fn eval_conflict<'a>(
        &'a self,
        existing: &impl Fn(usize) -> &'a Value,
        incoming: &impl Fn(usize) -> &'a Value,
    ) -> Result<Cow<'a, Value>> {
        crate::cancellation::check().map_err(core)?;
        self.eval_context(&|source, index| match source {
            Some(ConflictSource::Existing) => Ok(existing(index)),
            Some(ConflictSource::Incoming) => Ok(incoming(index)),
            None => Err(Error::Bind("column scope".into())),
        })
    }
    fn eval_context<'a>(
        &'a self,
        field: &impl Fn(Option<ConflictSource>, usize) -> Result<&'a Value>,
    ) -> Result<Cow<'a, Value>> {
        Ok(Cow::Owned(match self {
            Self::Value(v) => return Ok(Cow::Borrowed(v)),
            Self::Local(_) => return Err(Error::Unsupported),
            Self::Field(i) => return Ok(Cow::Borrowed(field(None, *i)?)),
            Self::ConflictField { source, index } => {
                return Ok(Cow::Borrowed(field(Some(*source), *index)?));
            }
            Self::Case {
                operand,
                arms,
                else_expr,
            } => {
                crate::cancellation::check().map_err(core)?;
                // Keep the base Cow once for all comparisons; do not duplicate
                // its evaluation or copy a text/bytes payload.
                let operand = operand
                    .as_ref()
                    .map(|expression| expression.eval_context(field))
                    .transpose()?;
                for (when, then) in arms {
                    crate::cancellation::check().map_err(core)?;
                    let condition = when.eval_context(field)?;
                    let matches = match &operand {
                        Some(operand) => {
                            // compare() also orders NULLs for sorting. CASE
                            // needs ordinary equality: NULL never matches.
                            **operand != Value::Null
                                && *condition != Value::Null
                                && compare(operand.as_ref(), condition.as_ref()) == Ordering::Equal
                        }
                        None => *condition == Value::Bool(true),
                    };
                    if matches {
                        return then.eval_context(field);
                    }
                }
                return match else_expr {
                    Some(expression) => expression.eval_context(field),
                    None => Ok(Cow::Owned(Value::Null)),
                };
            }
            Self::Call {
                function,
                arguments,
            } => {
                crate::cancellation::check().map_err(core)?;
                match function {
                    ScalarFunction::Coalesce => {
                        for argument in arguments {
                            let value = argument.eval_context(field)?;
                            if *value != Value::Null {
                                // A selected text/bytes leaf stays borrowed
                                // until the caller admits its output payload.
                                return Ok(value);
                            }
                        }
                        Value::Null
                    }
                    ScalarFunction::Abs => match arguments[0].eval_context(field)?.as_ref() {
                        Value::Null => Value::Null,
                        Value::Int(value) => {
                            Value::Int(crate::scalar::abs(*value).map_err(arithmetic_error)?)
                        }
                        _ => unreachable!("bound integer argument"),
                    },
                    ScalarFunction::Length => {
                        let value = arguments[0].eval_context(field)?;
                        let length = match value.as_ref() {
                            Value::Null => return Ok(Cow::Owned(Value::Null)),
                            Value::Text(text) => text.chars().count(),
                            Value::Bytes(bytes) => bytes.len(),
                            _ => unreachable!("bound text or bytes argument"),
                        };
                        Value::Int(i64::try_from(length).map_err(|_| Error::Overflow)?)
                    }
                }
            }
            Self::Negate(e) => match e.eval_context(field)?.as_ref() {
                Value::Null => Value::Null,
                Value::Int(value) => {
                    Value::Int(crate::scalar::negate(*value).map_err(arithmetic_error)?)
                }
                _ => unreachable!("bound integer operand"),
            },
            Self::Arithmetic(a, op, b) => {
                let a = a.eval_context(field)?;
                let b = b.eval_context(field)?;
                match (a.as_ref(), b.as_ref()) {
                    (Value::Null, _) | (_, Value::Null) => Value::Null,
                    (Value::Int(a), Value::Int(b)) => {
                        let value = match op {
                            Arithmetic::Add => crate::scalar::add(*a, *b),
                            Arithmetic::Subtract => crate::scalar::subtract(*a, *b),
                            Arithmetic::Multiply => crate::scalar::multiply(*a, *b),
                            Arithmetic::Divide => crate::scalar::divide(*a, *b),
                            Arithmetic::Remainder => crate::scalar::remainder(*a, *b),
                        }
                        .map_err(arithmetic_error)?;
                        Value::Int(value)
                    }
                    _ => unreachable!("bound integer operands"),
                }
            }
            Self::Compare(a, op, b) => {
                let a = a.eval_context(field)?;
                let b = b.eval_context(field)?;
                if *a == Value::Null || *b == Value::Null {
                    return Ok(Cow::Owned(Value::Null));
                }
                let c = compare(&a, &b);
                Value::Bool(match op {
                    Compare::Eq => c == Ordering::Equal,
                    Compare::Ne => c != Ordering::Equal,
                    Compare::Lt => c == Ordering::Less,
                    Compare::Le => c != Ordering::Greater,
                    Compare::Gt => c == Ordering::Greater,
                    Compare::Ge => c != Ordering::Less,
                })
            }
            Self::IsNull(e, neg) => Value::Bool((*e.eval_context(field)? == Value::Null) != *neg),
            Self::Not(e) => boolval(truth(e.eval_context(field)?.as_ref()).map(|b| !b)),
            Self::And(a, b) => {
                let a = truth(a.eval_context(field)?.as_ref());
                let b = truth(b.eval_context(field)?.as_ref());
                boolval(if a == Some(false) || b == Some(false) {
                    Some(false)
                } else if a == Some(true) && b == Some(true) {
                    Some(true)
                } else {
                    None
                })
            }
            Self::Or(a, b) => {
                let a = truth(a.eval_context(field)?.as_ref());
                let b = truth(b.eval_context(field)?.as_ref());
                boolval(if a == Some(true) || b == Some(true) {
                    Some(true)
                } else if a == Some(false) && b == Some(false) {
                    Some(false)
                } else {
                    None
                })
            }
        }))
    }
    fn accepts(&self, row: &SqlRow) -> Result<bool> {
        Ok(*self.eval(&row.values)? == Value::Bool(true))
    }
}
fn matching_rows(s: &impl Context, rows: Vec<SqlRow>, filter: &Bound) -> Result<Vec<SqlRow>> {
    let mut matches = Vec::new();
    for row in rows {
        s.checkpoint().map_err(core)?;
        if filter.accepts(&row)? {
            matches.push(row);
        }
    }
    Ok(matches)
}
fn compare_rows(a: &SqlRow, b: &SqlRow, order: &[(usize, bool)]) -> Ordering {
    for (i, desc) in order {
        let c = compare(&a.values[*i], &b.values[*i]);
        let c = if *desc { c.reverse() } else { c };
        if c != Ordering::Equal {
            return c;
        }
    }
    Ordering::Equal
}
fn sort(rows: &mut [SqlRow], order: &[(usize, bool)]) {
    rows.sort_by(|a, b| compare_rows(a, b, order))
}
fn charge_value(remaining: &mut usize, value: &Value) -> Result<()> {
    *remaining = remaining
        .checked_sub(value_bytes(value))
        .ok_or(Error::Limit)?;
    Ok(())
}
fn project_returning<'a>(
    projection: &'a [BoundProjection],
    field: &impl Fn(usize) -> &'a Value,
    remaining: &mut usize,
) -> Result<Vec<Cow<'a, Value>>> {
    projection
        .iter()
        .map(|item| {
            let value = item.expression.eval_fields(field)?;
            charge_value(remaining, value.as_ref())?;
            Ok(value)
        })
        .collect()
}
/// Filter, order, page and project complete read input: a table's rows, or
/// the rows of SQL's expansion of a view.
fn read_rows<C: Context>(
    s: &mut C,
    t: &TableDef,
    op: Operation,
    rows: Vec<SqlRow>,
    mut retained: Option<&mut usize>,
) -> Result<Outcome> {
    let mut out = Outcome::default();
    match op {
        Operation::Read {
            filter,
            order,
            limit,
            projection,
        } => {
            let mut rows = matching_rows(s, rows, &filter)?;
            sort(&mut rows, &order);
            out.matched = rows.len();
            out.complete = rows.len() <= limit;
            out.columns = projection
                .iter()
                .map(|i| t.columns[*i].name.clone())
                .collect();
            // V2 has no row ceiling, so its page is bounded by result bytes;
            // legacy keeps its row-bounded behaviour unchanged.
            let mut remaining = if s.max_read_rows() > MAX_ROWS {
                crate::relational::MAX_RESULT_BYTES
            } else {
                usize::MAX
            };
            for row in rows.iter().take(limit) {
                s.checkpoint().map_err(core)?;
                for index in &projection {
                    charge_value(&mut remaining, &row.values[*index])?;
                    charge_retained(&mut retained, &row.values[*index])?;
                }
            }
            out.rows = rows
                .into_iter()
                .take(limit)
                .map(|r| SqlRow {
                    version: r.version,
                    values: projection.iter().map(|i| r.values[*i].clone()).collect(),
                })
                .collect();
        }
        Operation::ReadExpressions {
            filter,
            order,
            limit,
            projection,
        } => {
            let mut rows = matching_rows(s, rows, &filter)?;
            sort(&mut rows, &order);
            out.matched = rows.len();
            out.complete = rows.len() <= limit;
            out.columns = projection
                .iter()
                .map(|item| item.column.name.clone())
                .collect();
            let mut remaining = crate::relational::MAX_RESULT_BYTES;
            for (index, row) in rows.iter().enumerate() {
                s.checkpoint().map_err(core)?;
                let mut values = Vec::new();
                for item in &projection {
                    let value = item.expression.eval(&row.values)?;
                    // Charge all eligible output, including rows beyond the
                    // page. Large borrowed leaves are admitted before cloning.
                    charge_value(&mut remaining, value.as_ref())?;
                    values.push(value);
                }
                if index < limit {
                    for value in &values {
                        charge_retained(&mut retained, value)?;
                    }
                    out.rows.push(SqlRow {
                        version: row.version,
                        values: values.into_iter().map(Cow::into_owned).collect(),
                    });
                }
            }
        }
        _ => return Err(Error::Unsupported),
    }
    Ok(out)
}
/// A read of a view: its rows come from SQL's own view expansion in the same
/// transaction snapshot, then native evaluation applies as for a table.
fn run_view(
    s: &mut SqlSession,
    t: &TableDef,
    mut op: Operation,
    rows: Vec<SqlRow>,
    retained: Option<&mut usize>,
) -> Result<Outcome> {
    match &mut op {
        Operation::Read { filter, .. } | Operation::ReadExpressions { filter, .. } => {
            filter.coerce_equality(s, t)?;
        }
        _ => return Err(Error::Unsupported),
    }
    read_rows(s, t, op, rows, retained)
}
/// Run a bound operation on its complete input. A table supplies certified
/// complete rows; a read bound to a view descriptor reads the view's rows.
fn run_complete(
    session: &mut SqlSession,
    table: &TableDef,
    operation: Operation,
    retained: Option<&mut usize>,
) -> Result<Outcome> {
    operation.check_input_cap()?;
    let limit = |error| match error {
        CoreError::Limit => Error::Limit,
        other => core(other),
    };
    if table.primary_key.is_empty()
        && matches!(operation, Operation::Read { .. } | Operation::ReadExpressions { .. })
    {
        if session.is_v2_engine() && session.native_view_engine_current(table).map_err(limit)? {
            let mut operation = operation;
            let (filter, order, limit) = match &mut operation {
                Operation::Read { filter, order, limit, .. }
                | Operation::ReadExpressions { filter, order, limit, .. } => (filter, order, *limit),
                _ => unreachable!(),
            };
            filter.coerce_equality(session, table)?;
            let mut matched = 0usize;
            let rows = session.native_view_rows_where(table, &mut |row| {
                let accepted = filter.accepts(row)?;
                matched += usize::from(accepted);
                Ok(accepted)
            }, (limit, (!order.is_empty()).then_some(|a: &SqlRow, b: &SqlRow| compare_rows(a, b, order))),
                input_error)?.ok_or_else(|| core(CoreError::Sql(SqlError::StaleSchema)))?;
            let mut outcome = read_rows(session, table, operation, rows, retained)?;
            outcome.complete = matched <= limit;
            outcome.matched = matched;
            return Ok(outcome);
        }
        if !session.is_v2_engine()
            && let Some(rows) = session.native_view_rows_engine(table).map_err(limit)?
        {
            return run_view(session, table, operation, rows, retained);
        }
    }
    if session.is_v2_engine()
        && let Operation::Read { filter, .. } | Operation::ReadExpressions { filter, .. }
            | Operation::Mutate { filter, .. } = &operation
    {
        // V2 filtered reads and writes retain only matching rows, and a filter
        // that fixes the full primary key reads that one key. Their result
        // depends only on matching rows; run re-applies the same filter.
        let mut filter = filter.clone();
        filter.coerce_equality(session, table)?;
        let access = if FORCE_NATIVE_FULL_SCAN.get() {
            NativeAccess::Full
        } else {
            native_access(session, table, &filter, matches!(operation,
                Operation::Read { .. } | Operation::ReadExpressions { .. })).map_err(input_error)?
        };
        // Only the rows the result can depend on are retained. A read keeps
        // its top `limit` rows (it counts every match); a write with a
        // cardinality cap keeps one row past it, which run reports as
        // TooManyMatches or AmbiguousMatch; a claim keeps its first row.
        // Every row is still filtered, so filter errors surface as before.
        let (limit, order) = match &operation {
            Operation::Read { limit, order, .. } => (*limit, order.clone()),
            Operation::Mutate { selection: Selection::One, .. } => (2, Vec::new()),
            Operation::Mutate { selection: Selection::Many(cap), .. } => (cap.saturating_add(1), Vec::new()),
            Operation::Mutate { selection: Selection::Claim(order), .. } => (1, order.clone()),
            _ => (usize::MAX, Vec::new()),
        };
        let mut matched = 0usize;
        let input = session.complete_read_table_where(
            table,
            &access,
            &mut |row| {
                let accepted = filter.accepts(row)?;
                matched += usize::from(accepted);
                Ok(accepted)
            },
            (limit, (!order.is_empty()).then_some(|a: &SqlRow, b: &SqlRow| compare_rows(a, b, &order))),
            input_error,
        )?;
        let read = matches!(operation, Operation::Read { .. });
        let mut outcome = run(session, table, operation, input, retained)?;
        if read {
            outcome.complete = matched <= limit;
            outcome.matched = matched;
        }
        return Ok(outcome);
    }
    // V2 inserts and upserts read only the keys they propose (insert_many and
    // upsert look them up); a single insert relies on the write path's
    // per-row 23505, as SQL does. Legacy keeps its bounded complete-table proof.
    let input = if session.is_v2_engine() {
        session.complete_keyed_engine(table).map_err(limit)?
    } else {
        session.complete_read_table_engine(table).map_err(limit)?
    };
    run(session, table, operation, input, retained)
}
fn input_error(error: CoreError) -> Error {
    match error {
        CoreError::Limit => Error::Limit,
        other => core(other),
    }
}
/// The full primary key when `filter` is a conjunction of column-to-literal
/// comparisons, each literal of its column's own storage class and admissible
/// for that column, that fixes every key column by equality. Such terms cannot
/// fail on other rows, so one key read equals a filtered scan.
fn primary_key_point(table: &TableDef, filter: &Bound) -> Option<Vec<Value>> {
    if table.primary_key.is_empty() || table.hidden_rowid().is_some() {
        return None;
    }
    let mut key = vec![None; table.primary_key.len()];
    let mut pending = vec![filter];
    while let Some(bound) = pending.pop() {
        let (index, op, value) = match bound {
            Bound::And(a, b) => {
                pending.extend([a.as_ref(), b.as_ref()]);
                continue;
            }
            Bound::Compare(a, op, b) => match (a.as_ref(), b.as_ref()) {
                (Bound::Field(i), Bound::Value(v)) | (Bound::Value(v), Bound::Field(i)) => (*i, *op, v),
                _ => return None,
            },
            _ => return None,
        };
        comparison_literal(table, index, value)?;
        if op == Compare::Eq
            && let Some(slot) = table.primary_key.iter().position(|p| *p == index)
            && key[slot].replace(value.clone()).is_some()
        {
            return None;
        }
    }
    key.into_iter().collect()
}
/// Only comparisons whose storage classes agree are total on every stored row.
/// Keep this shared with the explain point classifier.
fn comparison_literal<'a>(table: &TableDef, index: usize, value: &'a Value) -> Option<&'a Value> {
    let column = table.columns.get(index)?;
    match (&column.data_type, value) {
        (DataType::Boolean, Value::Bool(_))
        | (DataType::SmallInt | DataType::Integer | DataType::BigInt, Value::Int(_))
        | (DataType::Text | DataType::Varchar | DataType::Uuid, Value::Text(_))
        | (DataType::Numeric, Value::Numeric(_))
        | (DataType::Bytes, Value::Bytes(_)) => {}
        _ => return None,
    }
    crate::relational::check(value, column).ok()?;
    Some(value)
}
fn literal_comparison(bound: &Bound) -> Option<(usize, Compare, &Value)> {
    let Bound::Compare(a, op, b) = bound else { return None };
    match (a.as_ref(), b.as_ref()) {
        (Bound::Field(i), Bound::Value(v)) => Some((*i, *op, v)),
        (Bound::Value(v), Bound::Field(i)) => Some((*i, match op {
            Compare::Lt => Compare::Gt, Compare::Le => Compare::Ge,
            Compare::Gt => Compare::Lt, Compare::Ge => Compare::Le,
            other => *other,
        }, v)),
        _ => None,
    }
}
fn total_filter(table: &TableDef, filter: &Bound) -> bool {
    match filter {
        Bound::And(a, b) | Bound::Or(a, b) => total_filter(table, a) && total_filter(table, b),
        _ => literal_comparison(filter)
            .is_some_and(|(i, _, v)| comparison_literal(table, i, v).is_some()),
    }
}
/// Mandatory key sets may come from either conjunct; every OR arm must have one.
fn primary_key_points(table: &TableDef, filter: &Bound) -> Option<Vec<Vec<Value>>> {
    let mut key = vec![None; table.primary_key.len()];
    let mut pending = vec![filter];
    while let Some(term) = pending.pop() {
        if let Bound::And(a, b) = term {
            pending.extend([a.as_ref(), b.as_ref()]);
        } else if let Some((index, Compare::Eq, value)) = literal_comparison(term)
            && let Some(slot) = table.primary_key.iter().position(|p| *p == index)
        {
            key[slot] = Some(value.clone());
        }
    }
    if let Some(key) = key.into_iter().collect::<Option<Vec<_>>>() {
        return Some(vec![key]);
    }
    match filter {
        Bound::And(a, b) => primary_key_points(table, a).or_else(|| primary_key_points(table, b)),
        Bound::Or(a, b) => {
            let mut keys = primary_key_points(table, a)?;
            keys.extend(primary_key_points(table, b)?);
            Some(keys)
        }
        _ => None,
    }
}
pub(crate) enum NativeAccess {
    Full,
    Keys(Vec<Vec<Value>>),
    Range(std::ops::Bound<Vec<u8>>, std::ops::Bound<Vec<u8>>),
    Empty,
    Index(crate::relational::secondary_index::Descriptor, std::ops::Bound<Vec<u8>>, std::ops::Bound<Vec<u8>>),
}
/// Only mandatory conjuncts can narrow an index visit; OR arms are residuals.
fn native_access(session: &mut SqlSession, table: &TableDef, filter: &Bound, reading: bool)
    -> std::result::Result<NativeAccess, CoreError>
{
    let primary = primary_native_access(table, filter, reading);
    if !matches!(primary, NativeAccess::Full) || !reading || !total_filter(table, filter)
        || table.primary_key.is_empty() || table.hidden_rowid().is_some() {
        return Ok(primary);
    }
    let mut predicates = Vec::new();
    let mut pending = vec![filter];
    while let Some(term) = pending.pop() {
        if let Bound::And(a, b) = term { pending.extend([a.as_ref(), b.as_ref()]); continue; }
        let Some((i, op, value)) = literal_comparison(term) else { continue };
        let op = match op {
            Compare::Eq => "=", Compare::Gt => ">", Compare::Ge => ">=",
            Compare::Lt => "<", Compare::Le => "<=", _ => continue,
        };
        predicates.push((table.columns[i].name.clone(), op.to_string(), value.clone()));
    }
    session.native_index_access(table, &predicates)
}
fn primary_native_access(table: &TableDef, filter: &Bound, reading: bool) -> NativeAccess {
    use crate::storage::engine::codec::pk_bytes;
    use std::ops::Bound as Side;
    if table.primary_key.is_empty() || table.hidden_rowid().is_some() || !total_filter(table, filter) {
        return NativeAccess::Full;
    }
    if let Some(keys) = primary_key_points(table, filter) {
        let sorted = keys.into_iter().map(|key| Some((crate::storage::engine::codec::typed_pk_bytes(table, &key).ok()?, key)))
            .collect::<Option<BTreeMap<_, _>>>();
        // NULL has no PK encoding. Preserve the scan fallback.
        return sorted.map_or(NativeAccess::Full, |keys| NativeAccess::Keys(keys.into_values().collect()));
    }
    let [index] = table.primary_key.as_slice() else { return NativeAccess::Full };
    if !reading || !matches!(table.columns[*index].data_type,
        DataType::SmallInt | DataType::Integer | DataType::BigInt) {
        return NativeAccess::Full;
    }
    let mut lo: Option<(i64, bool)> = None;
    let mut hi: Option<(i64, bool)> = None;
    let mut pending = vec![filter];
    while let Some(term) = pending.pop() {
        if let Bound::And(a, b) = term { pending.extend([a.as_ref(), b.as_ref()]); continue; }
        let Some((i, op, Value::Int(n))) = literal_comparison(term) else { continue };
        if i != *index { continue; }
        if matches!(op, Compare::Eq | Compare::Gt | Compare::Ge) {
            let next = (*n, op != Compare::Gt);
            if lo.is_none_or(|old| next.0 > old.0 || (next.0 == old.0 && !next.1)) { lo = Some(next); }
        }
        if matches!(op, Compare::Eq | Compare::Lt | Compare::Le) {
            let next = (*n, op != Compare::Lt);
            if hi.is_none_or(|old| next.0 < old.0 || (next.0 == old.0 && !next.1)) { hi = Some(next); }
        }
    }
    if lo.is_none() && hi.is_none() { return NativeAccess::Full; }
    if let (Some(lo), Some(hi)) = (lo, hi)
        && (lo.0 > hi.0 || (lo.0 == hi.0 && !(lo.1 && hi.1))) {
        return NativeAccess::Empty;
    }
    let encode = |side: Option<(i64, bool)>| -> Option<Side<Vec<u8>>> {
        Some(match side {
            None => Side::Unbounded,
            Some((n, inclusive)) => {
                let bytes = pk_bytes(&[Value::Int(n)]).ok()?;
                if inclusive { Side::Included(bytes) } else { Side::Excluded(bytes) }
            }
        })
    };
    match (encode(lo), encode(hi)) {
        (Some(lo), Some(hi)) => NativeAccess::Range(lo, hi),
        _ => NativeAccess::Full,
    }
}

fn run<C: Context>(
    s: &mut C,
    t: &TableDef,
    mut op: Operation,
    input: C::Input,
    mut retained: Option<&mut usize>,
) -> Result<Outcome> {
    match &mut op {
        Operation::Read { filter, .. } | Operation::ReadExpressions { filter, .. } => {
            filter.coerce_equality(s, t)?;
        }
        Operation::Mutate { filter, guard, sets, .. } => {
            filter.coerce_equality(s, t)?;
            guard.coerce_equality(s, t)?;
            if let Some(Assignments::Values(values)) = sets {
                for (index, value) in values {
                    if matches!(t.columns[*index].data_type, DataType::Uuid | DataType::Jsonb) {
                        *value = s.coerce_column(&t.columns[*index], value.clone())?;
                    }
                }
            }
        }
        _ => {}
    }
    op.check_input_cap()?;
    // A presentation page cannot enter cardinality or cap evaluation. The
    // certificate also rejects data captured before a staged write or rollback.
    let rows = s.consume_complete_engine(input, t).map_err(core)?;
    if let Operation::Upsert {
        columns,
        rows: proposed,
        action,
        returning,
        ..
    } = op
    {
        let session = s.session()?;
        let mut normalized = Vec::new();
        let mut remaining = MAX_INSERT_BYTES;
        for expressions in proposed {
            let values = expressions.iter().map(|e| e.eval(&[]).map(Cow::into_owned))
                .collect::<Result<Vec<_>>>()?;
            let values = normalize_insert(session, t, &columns, values)?;
            for value in &values { charge_value(&mut remaining, value)?; }
            normalized.push(values.into_iter().map(Bound::Value).collect());
        }
        return upsert::run(
            session,
            t,
            upsert::Plan {
                columns: (0..t.columns.len()).collect(),
                expressions: normalized,
                action,
                returning,
            },
            rows,
            retained,
        );
    }
    if matches!(
        op,
        Operation::Insert { .. } | Operation::InsertExpressions { .. }
    ) && rows.len() >= s.max_read_rows()
    {
        return Err(Error::Limit);
    }
    let op = match op {
        Operation::InsertExpressions { values, returning } => {
            s.checkpoint().map_err(core)?;
            let evaluated = values.iter().map(|v| v.eval(&[]).map(Cow::into_owned))
                .collect::<Result<Vec<_>>>()?;
            Operation::Insert { columns: (0..evaluated.len()).collect(), values: evaluated, returning }
        }
        other => other,
    };
    let mut out = Outcome::default();
    match op {
        Operation::InsertMany {
            columns,
            rows: proposed,
            returning,
            ..
        } => {
            return insert_many(
                s.session()?,
                t,
                columns,
                proposed,
                returning,
                rows,
                retained,
            );
        }
        Operation::InsertExpressions { .. } | Operation::Upsert { .. } => {
            return Err(Error::Unsupported);
        }
        read @ (Operation::Read { .. } | Operation::ReadExpressions { .. }) => {
            let rows = match &read {
                Operation::Read { filter, .. } => secondary_selector::read(s, t, filter, rows)?,
                _ => rows,
            };
            return read_rows(s, t, read, rows, retained);
        }
        Operation::Insert { columns, values, returning } => {
            let values = normalize_insert(s.session()?, t, &columns, values)?;
            let key = if let Some(projection) = &returning {
                s.checkpoint().map_err(core)?;
                let mut remaining_row = crate::relational::MAX_ROW_BYTES;
                for (value, column) in values.iter().zip(&t.columns) {
                    check(value, column)?;
                    charge_value(&mut remaining_row, value)?;
                }
                let mut remaining = crate::relational::MAX_RESULT_BYTES;
                let projected = project_returning(projection, &|i| &values[i], &mut remaining)?;
                for value in &projected {
                    charge_retained(&mut retained, value)?;
                }
                out.columns = projection
                    .iter()
                    .map(|item| item.column.name.clone())
                    .collect();
                out.rows.push(SqlRow {
                    values: projected.into_iter().map(Cow::into_owned).collect(),
                    // Filled from the staged row before any successful result.
                    version: 0,
                });
                Some(
                    t.primary_key
                        .iter()
                        .map(|i| values[*i].clone())
                        .collect::<Vec<_>>(),
                )
            } else {
                None
            };
            s.insert_engine(t, values).map_err(core)?;
            if let Some(key) = key {
                out.rows[0].version = s.row_version_engine(t, &key).map_err(core)?;
            }
            out.matched = 1;
            out.affected = 1;
            out.changed = 1;
        }
        Operation::Mutate {
            filter,
            guard,
            sets,
            selection,
            returning,
        } => {
            let mut matches = matching_rows(s, rows, &filter)?;
            // Cardinality is checked before guards and before staging any write.
            match &selection {
                Selection::One => {
                    if matches.is_empty() {
                        return Err(Error::NotFound);
                    }
                    if matches.len() != 1 {
                        return Err(Error::AmbiguousMatch);
                    }
                }
                Selection::Many(cap) => {
                    if matches.len() > *cap {
                        return Err(Error::TooManyMatches);
                    }
                }
                Selection::Claim(order) => {
                    sort(&mut matches, order);
                    matches.truncate(1);
                }
            }
            if returning.is_some() && matches!(selection, Selection::Many(_)) {
                let order = t
                    .primary_key
                    .iter()
                    .map(|i| (*i, false))
                    .collect::<Vec<_>>();
                sort(&mut matches, &order);
            }
            out.matched = matches.len();
            for row in &matches {
                s.checkpoint().map_err(core)?;
                if !guard.accepts(row)? {
                    return Err(Error::ConditionFailed);
                }
            }
            let mut work = vec![];
            let computed = matches!(&sets, Some(Assignments::Expressions(_)));
            let mut remaining_mutation = MAX_MUTATION_BYTES;
            for row in &matches {
                s.checkpoint().map_err(core)?;
                let evaluated = match &sets {
                    Some(Assignments::Values(values)) => Some(
                        values
                            .iter()
                            .map(|(index, value)| (*index, Cow::Borrowed(value)))
                            .collect::<Vec<_>>(),
                    ),
                    Some(Assignments::Expressions(expressions)) => Some(
                        expressions
                            .iter()
                            .map(|(index, expression)| Ok((*index, expression.eval(&row.values)?)))
                            .collect::<Result<Vec<_>>>()?,
                    ),
                    None => None,
                };
                let evaluated: Option<Vec<(usize, Cow<'_, Value>)>> = evaluated
                    .map(|values| {
                        values
                            .into_iter()
                            .map(|(index, value)| {
                                s.coerce_column(&t.columns[index], value.into_owned())
                                    .and_then(|value| {
                                        // Admit assignment widths before postimage NOT NULL.
                                        if value != Value::Null {
                                            check(&value, &t.columns[index])?;
                                        }
                                        Ok((index, Cow::Owned(value)))
                                    })
                            })
                            .collect::<Result<Vec<_>>>()
                    })
                    .transpose()?;
                let post = |index| {
                    evaluated
                        .as_ref()
                        .and_then(|values| values.iter().find(|(target, _)| *target == index))
                        .map_or(&row.values[index], |(_, value)| value.as_ref())
                };
                if computed || (returning.is_some() && evaluated.is_some()) {
                    let mut remaining_row = crate::relational::MAX_ROW_BYTES;
                    for (index, column) in t.columns.iter().enumerate() {
                        let value = post(index);
                        check(value, column)?;
                        charge_value(&mut remaining_row, value)?;
                        if computed {
                            // Full computed postimages count, including unchanged
                            // fields and rows that will prove to be no-ops below.
                            charge_value(&mut remaining_mutation, value)?;
                        }
                    }
                }
                if matches!(selection, Selection::Claim(_))
                    && *filter.eval_fields(&post)? == Value::Bool(true)
                {
                    return Err(Error::ClaimNotConsumed);
                }
                out.affected += 1;
                // This intent policy differs deliberately from legacy SQL:
                // matching a no-op affects the count but preserves row version.
                let changed = evaluated.is_none()
                    || !row.values.iter().enumerate().all(|(i, v)| post(i) == v);
                if changed {
                    out.changed += 1;
                }
                // Keep no-op candidates for RETURN as well as changed rows.
                work.push((row, evaluated, changed));
            }
            if let Some(projection) = &returning {
                out.columns = projection
                    .iter()
                    .map(|item| item.column.name.clone())
                    .collect();
                let mut remaining = crate::relational::MAX_RESULT_BYTES;
                let mut projected = Vec::new();
                for (row, evaluated, _) in &work {
                    s.checkpoint().map_err(core)?;
                    let post = |index| {
                        evaluated
                            .as_ref()
                            .and_then(|values| values.iter().find(|(target, _)| *target == index))
                            .map_or(&row.values[index], |(_, value)| value.as_ref())
                    };
                    // DELETE has no assignment overlay, so it returns the
                    // preimage. Updates and claims expose the full postimage.
                    projected.push((
                        row.version,
                        project_returning(projection, &post, &mut remaining)?,
                    ));
                }
                for (_, values) in &projected {
                    for value in values {
                        charge_retained(&mut retained, value)?;
                    }
                }
                // All output expressions and their aggregate payload have
                // passed before copying any large result value or staging.
                out.rows = projected
                    .into_iter()
                    .map(|(version, values)| SqlRow {
                        values: values.into_iter().map(Cow::into_owned).collect(),
                        version,
                    })
                    .collect();
            }
            // Every candidate has passed guards and expression evaluation;
            // computed updates also passed final-row checks and their budget.
            // No mutation is staged before this complete preflight succeeds.
            s.checkpoint().map_err(core)?;
            let claimed_key = if returning.is_none() && matches!(selection, Selection::Claim(_)) {
                out.columns = t.columns.iter().map(|c| c.name.clone()).collect();
                matches.first().map(|row| {
                    t.primary_key
                        .iter()
                        .map(|i| row.values[*i].clone())
                        .collect::<Vec<_>>()
                })
            } else {
                None
            };
            if !work.is_empty() && work.iter().all(|(_, evaluated, _)| evaluated.is_none()) {
                let targets = work.iter().map(|(row, _, _)| (
                    t.primary_key.iter().map(|i| row.values[*i].clone()).collect(), row.version,
                )).collect();
                #[cfg(test)]
                NATIVE_DELETE_BEFORE_WRITE.with_borrow_mut(|hook| {
                    if let Some(hook) = hook.as_mut() { hook(); }
                });
                let result = s.session()?.delete_many_engine(t, targets).map_err(core)?;
                if result.affected != work.len() as u64 { return Err(Error::Conflict); }
                work.clear();
            }
            for (index, (row, evaluated, changed)) in work.into_iter().enumerate() {
                if !changed {
                    continue;
                }
                s.checkpoint().map_err(core)?;
                let key = t
                    .primary_key
                    .iter()
                    .map(|i| row.values[*i].clone())
                    .collect::<Vec<_>>();
                let returned_key = if returning.is_some() && evaluated.is_some() {
                    Some(key.clone())
                } else {
                    None
                };
                let result = match evaluated {
                    Some(values) => s.update_engine(
                        t,
                        key,
                        values
                            .into_iter()
                            .map(|(index, value)| (index, value.into_owned()))
                            .collect(),
                        row.version,
                    ),
                    None => s.delete_engine(t, key, row.version),
                }
                .map_err(core)?;
                if result.affected != 1 {
                    return Err(Error::Conflict);
                }
                if let Some(key) = returned_key {
                    out.rows[index].version = s.row_version_engine(t, &key).map_err(core)?;
                }
            }
            if let Some(key) = claimed_key {
                let result = s
                    .scan(Scan {
                        table: t.name.clone(),
                        projection: vec![],
                        prefix: key,
                        lower: None,
                        upper: None,
                        reverse: false,
                        limit: 1,
                    })
                    .map_err(core)?;
                if result.rows.len() != 1 {
                    return Err(Error::Conflict);
                }
                out.rows = result.rows;
            }
        }
    }
    Ok(out)
}

fn charge_retained(remaining: &mut Option<&mut usize>, value: &Value) -> Result<()> {
    if let Some(remaining) = remaining {
        charge_value(remaining, value)?;
    }
    Ok(())
}
impl Operation {
    // A static input count is a runtime intent check: preparation binds all
    // expressions but never decides this cap. Run it before resolving locals
    // or scanning, so rejected input cannot consume RHS work.
    fn check_input_cap(&self) -> Result<()> {
        if let Self::InsertMany { rows, cap, .. } | Self::Upsert { rows, cap, .. } = self
            && rows.len() > *cap
        {
            return Err(Error::TooManyMatches);
        }
        Ok(())
    }
    fn columns(&self, table: &TableDef) -> Vec<ColumnDef> {
        match self {
            Self::Read { projection, .. } => projection
                .iter()
                .map(|i| table.columns[*i].clone())
                .collect(),
            Self::ReadExpressions { projection, .. } => {
                projection.iter().map(|p| p.column.clone()).collect()
            }
            Self::Insert {
                returning: Some(projection),
                ..
            }
            | Self::InsertExpressions {
                returning: Some(projection),
                ..
            }
            | Self::InsertMany {
                returning: Some(projection),
                ..
            }
            | Self::Upsert {
                returning: Some(projection),
                ..
            }
            | Self::Mutate {
                returning: Some(projection),
                ..
            } => projection.iter().map(|p| p.column.clone()).collect(),
            Self::Mutate {
                selection: Selection::Claim(_),
                ..
            } => table.columns.clone(),
            _ => Vec::new(),
        }
    }
}

fn numeric_assignment(source: Option<Ty>, column: &ColumnDef) -> bool {
    let number = |ty| matches!(ty, Ty::Int | Ty::Numeric | Ty::Real | Ty::DoublePrecision);
    source.is_some_and(number) && number(colty(column))
}
fn assign_number(value: Value, column: &ColumnDef) -> Result<Value> {
    crate::relational::assign::assign_number_engine(value, column)
        .map_err(|error| core(CoreError::Sql(error)))
}
// Preserve native admission errors for explicit input and finite row budgets;
// omissions and identity violations retain the SQL error produced by defaults.
fn normalize_insert(
    session: &mut SqlSession,
    table: &TableDef,
    columns: &[usize],
    values: Vec<Value>,
) -> Result<Vec<Value>> {
    normalize_insert_inner(session, table, columns, values, false)
}
fn normalize_insert_inner(
    session: &mut SqlSession,
    table: &TableDef,
    columns: &[usize],
    values: Vec<Value>,
    deferred: bool,
) -> Result<Vec<Value>> {
    let values = columns.iter().zip(values)
        .map(|(index, value)| assign_number(value, &table.columns[*index]))
        .collect::<Result<Vec<_>>>()?;
    let invalid_supplied = columns.iter().zip(&values)
        .filter_map(|(index, value)| check(value, &table.columns[*index]).err())
        .max_by_key(|error| usize::from(*error == Error::Overflow));
    let result = if deferred {
        session.normalize_insert_engine_deferred(table, columns, values)
    } else {
        session.normalize_insert_engine(table, columns, values)
    };
    result.map_err(|error| match error {
        CoreError::Sql(SqlError::Limit) => Error::Limit,
        error @ CoreError::Sql(SqlError::Null | SqlError::Type) => invalid_supplied.unwrap_or_else(|| core(error)),
        error => core(error),
    })
}
fn insert_many(
    session: &mut SqlSession,
    table: &TableDef,
    columns: Vec<usize>,
    expressions: Vec<Vec<Bound>>,
    returning: Option<Vec<BoundProjection>>,
    existing: Vec<SqlRow>,
    retained: Option<&mut usize>,
) -> Result<Outcome> {
    let result = insert_many_deferred(session, table, columns, expressions, returning, existing, retained);
    // RETURN rows, versions and value-bearing errors must all be durable-covered.
    // A settlement failure takes precedence over the original request error.
    if let Err(error) = session.settle_sequences() {
        session.abort();
        return Err(core(error));
    }
    // Errors keep their existing abort behaviour (inner failures abort themselves).
    #[cfg(test)]
    NATIVE_MANY_ON_EXIT.with(|observer| {
        if let Some(observer) = observer.get() { observer(session); }
    });
    result
}
#[cfg(test)]
type NativeManyHook = Box<dyn FnMut(&mut SqlSession, usize)>;
#[cfg(test)]
thread_local! {
    pub(crate) static NATIVE_MANY_AFTER_NORMALIZE: std::cell::RefCell<Option<NativeManyHook>> = const { std::cell::RefCell::new(None) };
    pub(crate) static NATIVE_MANY_BEFORE_WRITE: std::cell::RefCell<Option<NativeManyHook>> = const { std::cell::RefCell::new(None) };
    pub(crate) static NATIVE_DELETE_BEFORE_WRITE: std::cell::RefCell<Option<Box<dyn FnMut()>>> = const { std::cell::RefCell::new(None) };
    // Per-thread crash injection keeps other native requests unaffected.
    pub(crate) static NATIVE_MANY_ON_EXIT: std::cell::Cell<Option<fn(&SqlSession)>> = const { std::cell::Cell::new(None) };
    pub(crate) static NATIVE_MANY_CRASH_AFTER: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
}
fn insert_many_deferred(
    session: &mut SqlSession,
    table: &TableDef,
    columns: Vec<usize>,
    expressions: Vec<Vec<Bound>>,
    returning: Option<Vec<BoundProjection>>,
    existing: Vec<SqlRow>,
    mut retained: Option<&mut usize>,
) -> Result<Outcome> {
    let count = expressions.len();
    let mut proposed = Vec::new();
    let mut remaining = MAX_INSERT_BYTES;
    // Normalize each actual proposal before full-row and aggregate admission.
    // Defaults are consumed once, before key checks and RETURN evaluation.
    for expressions in &expressions {
        crate::cancellation::check().map_err(core)?;
        let values = expressions.iter().map(|e| e.eval(&[]).map(Cow::into_owned))
            .collect::<Result<Vec<_>>>()?;
        let row = normalize_insert_inner(session, table, &columns, values, true)?
            .into_iter().map(Cow::Owned).collect::<Vec<_>>();
        let mut remaining_row = crate::relational::MAX_ROW_BYTES;
        for (value, column) in row.iter().zip(&table.columns) {
            check(value, column)?;
            charge_value(&mut remaining_row, value)?;
            charge_value(&mut remaining, value)?;
        }
        proposed.push(row);
        #[cfg(test)]
        NATIVE_MANY_AFTER_NORMALIZE.with_borrow_mut(|hook| {
            if let Some(hook) = hook.as_mut() { hook(session, proposed.len()); }
        });
        #[cfg(test)]
        NATIVE_MANY_CRASH_AFTER.with(|after| {
            if after.get() == Some(proposed.len()) {
                crate::storage::crash_now();
            }
        });
    }
    // The same canonical complete-key codec used by SQL defines identity.
    // Key payload copies happen only after every proposed row was admitted.
    let mut keys = Vec::new();
    let mut unique = BTreeSet::new();
    for (index, row) in proposed.iter().enumerate() {
        crate::cancellation::check().map_err(core)?;
        let key = table
            .primary_key
            .iter()
            .map(|i| row[*i].as_ref().clone())
            .collect::<Vec<_>>();
        let encoded =
            crate::relational::encode_primary_key(table, &key).map_err(|error| core(CoreError::Sql(error)))?;
        if !unique.insert(encoded.clone()) {
            return Err(Error::DuplicateTarget);
        }
        keys.push((encoded, key, index));
    }
    let existing = if session.is_v2_engine() {
        // Keyed input: only rows sharing a proposed full primary key can conflict.
        session.native_key_rows(table, keys.iter().map(|(_, key, _)| key.clone())).map_err(core)?
    } else {
        existing
    };
    for row in &existing {
        crate::cancellation::check().map_err(core)?;
        let key = table
            .primary_key
            .iter()
            .map(|i| row.values[*i].clone())
            .collect::<Vec<_>>();
        let encoded =
            crate::relational::encode_primary_key(table, &key).map_err(|error| core(CoreError::Sql(error)))?;
        if unique.contains(&encoded) {
            return Err(core(CoreError::Sql(SqlError::Duplicate)));
        }
    }
    if existing.len().checked_add(count).ok_or(Error::Limit)? > Context::max_read_rows(session) {
        return Err(Error::Limit);
    }
    // RETURN order is canonical full-PK order, independent of input tuple order.
    keys.sort_by(|left, right| left.0.cmp(&right.0));
    let mut out = Outcome {
        matched: count,
        affected: count,
        changed: count,
        ..Outcome::default()
    };
    if let Some(projection) = &returning {
        out.columns = projection
            .iter()
            .map(|item| item.column.name.clone())
            .collect();
        let mut remaining = crate::relational::MAX_RESULT_BYTES;
        let mut projected = Vec::new();
        for (_, _, index) in &keys {
            crate::cancellation::check().map_err(core)?;
            let row = &proposed[*index];
            projected.push(project_returning(
                projection,
                &|i| row[i].as_ref(),
                &mut remaining,
            )?);
        }
        for row in &projected {
            for value in row {
                charge_retained(&mut retained, value)?;
            }
        }
        // Every RETURN expression and retention budget passed before copying.
        out.rows = projected
            .into_iter()
            .map(|values| SqlRow {
                values: values.into_iter().map(Cow::into_owned).collect(),
                version: 0,
            })
            .collect();
    }
    crate::cancellation::check().map_err(core)?;
    // Move each admitted proposal once; no duplicate full postimage is retained.
    let mut proposed = proposed.into_iter().map(Some).collect::<Vec<_>>();
    let mut batch = Vec::with_capacity(count);
    for (_, _, proposal_index) in &keys {
        crate::cancellation::check().map_err(core)?;
        let values = proposed[*proposal_index].take().ok_or(Error::DuplicateTarget)?
            .into_iter().map(Cow::into_owned).collect();
        #[cfg(test)]
        NATIVE_MANY_BEFORE_WRITE.with_borrow_mut(|hook| {
            if let Some(hook) = hook.as_mut() { hook(session, batch.len()); }
        });
        batch.push(values);
    }
    if !batch.is_empty() {
        let result = session.insert_many_engine_deferred(table, batch).map_err(core)?;
        if result.affected != count as u64 { return Err(Error::Conflict); }
    }
    if returning.is_some() {
        for (output_index, (_, key, _)) in keys.iter().enumerate() {
            out.rows[output_index].version = session.row_version_engine(table, key).map_err(core)?;
        }
    }
    Ok(out)
}

#[cfg(test)]
mod duplicate_error_encoding_regression {
    use super::Error;
    #[test]
    fn task_diagnostics_are_process_local() {
        let task = crate::task::TaskError::Denied(crate::task::DenyRule::ColumnNotAvailable);
        let error = super::task_error(task.clone());
        assert_eq!(error, Error::Task(task));
        assert_eq!(error.sqlstate(), Some("42501"));
        assert!(bincode::serialize(&error).is_err());
    }

    #[test]
    fn existing_error_bytes_are_stable() {
        let errors = [
            Error::Syntax { offset: 7 },
            Error::Unsupported,
            Error::Limit,
            Error::Bind("x".into()),
            Error::Type,
            Error::NotFound,
            Error::AmbiguousMatch,
            Error::ConditionFailed,
            Error::TooManyMatches,
            Error::ClaimNotConsumed,
            Error::Conflict,
            Error::Core("x".into()),
            Error::Unknown,
            Error::Overflow,
            Error::DivisionByZero,
            Error::RequirementFailed,
            Error::DuplicateTarget,
            Error::PlanRequired,
        ];
        for (index, error) in errors.iter().enumerate() {
            let mut expected = (index as u32).to_le_bytes().to_vec();
            match index {
                0 => expected.extend(7u64.to_le_bytes()),
                3 | 11 => {
                    expected.extend(1u64.to_le_bytes());
                    expected.push(b'x');
                }
                _ => {}
            }
            assert_eq!(bincode::serialize(error).unwrap(), expected);
        }
    }
}
