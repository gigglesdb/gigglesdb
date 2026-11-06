//! Syntax-independent operations for the bounded trusted embedded engine.
//!
//! Frontends lower requests into these types without generating SQL. The engine
//! binds against one transaction snapshot and publishes each request atomically.
//! This initial surface supports the existing finite native subset; it is not
//! task authorization and does not replace the SQL frontend or its semantics.
//! Storage failures during commit return [`Error::Unknown`] and must not be
//! retried blindly.

pub use crate::relational::{SqlRow, Value};
#[cfg(test)]
mod case_tests;
mod catalog;
pub mod once;
#[cfg(test)]
mod scalar_tests;
mod session;
mod transaction;
mod upsert;
#[cfg(test)]
mod upsert_tests;
use crate::{
    Admin, CoreError, SqlSession,
    relational::{ColumnDef, DataType, Scan, SqlError, TableDef, value_bytes},
    storage::CompleteRelation,
};
pub use catalog::*;
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
/// Maximum entries in a supplied parameter map, including unused entries.
pub const MAX_BINDINGS: usize = crate::relational::MAX_PARAMS;
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
            if projection.len() > crate::relational::MAX_COLUMNS {
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
                && (values.is_empty() || values.len() > crate::relational::MAX_COLUMNS)
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
        CoreError::Sql(SqlError::Conflict) => Error::Conflict,
        _ => Error::Core(e.to_string()),
    }
}
impl Engine {
    pub fn new(admin: Admin) -> Self {
        Self { admin }
    }
    /// Bind and execute one bounded request in its own transaction.
    pub fn execute(&self, request: &Request, bindings: &Bindings) -> Result<Outcome> {
        validate_request(request)?;
        validate_bindings(bindings)?;
        self.execute_operation(|session| bind_request(session, request, bindings))
    }

    /// Freeze actual values and bind stable catalog identities without a logical
    /// row scan or durable decision. Snapshot creation still clones bounded state.
    /// This is not a parameter template.
    pub fn prepare_bound(&self, request: &Request, bindings: &Bindings) -> Result<BoundOperation> {
        validate_request(request)?;
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
            let (_, tables) = session.catalog_engine().map_err(core)?;
            if description.contract_version != ENGINE_CONTRACT_VERSION
                || domain != description.database_domain
                || epoch != description.catalog_epoch
                || !tables.iter().any(|table| table == &description.table)
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
            operation.check_input_cap()?;
            let input = session
                .complete_table_engine(&table)
                .map_err(|error| match error {
                    CoreError::Limit => Error::Limit,
                    other => core(other),
                })?;
            run(&mut session, &table, operation, input, None)
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
fn validate_request(request: &Request) -> Result<()> {
    validate_depth(request)?;
    validate_size(request)?;
    if !identifier(request.table()?) {
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
    session: &mut SqlSession,
    request: &Request,
    bindings: &Bindings,
) -> Result<(TableDef, Operation)> {
    let table_name = request.table()?;
    let (_, tables) = session.catalog_engine().map_err(core)?;
    let table = tables
        .into_iter()
        .find(|table| table.name == table_name)
        .ok_or_else(|| Error::Bind("table".into()))?;
    let mut binder = Binder {
        table: Some(&table),
        allow_fields: true,
        allow_conflict_fields: false,
        locals: None,
        bindings,
        used: BTreeSet::new(),
        remaining_value_bytes: MAX_BOUND_VALUE_BYTES,
    };
    let operation = binder.bind(request)?;
    // Every recorded name resolved successfully, so used is a subset. Counts
    // establish exact membership without copying any unused caller-owned keys.
    if binder.used.len() != bindings.len() {
        return Err(Error::Bind("parameters".into()));
    }
    Ok((table, operation))
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
    // Preserve the serialized request limit without allocating a serialized
    // copy of an oversized caller-owned literal or assignment collection.
    struct Counter(usize);
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > MAX_INPUT - self.0 {
                return Err(std::io::Error::other("request size limit"));
            }
            self.0 += bytes.len();
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    serde_json::to_writer(Counter(0), request).map_err(|_| Error::Limit)
}
pub(crate) fn validate_depth(stmt: &Request) -> Result<()> {
    validate_request_nodes(stmt, &mut 0)
}
fn validate_request_nodes(stmt: &Request, nodes: &mut usize) -> Result<()> {
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
        Request::InsertMany { columns, rows, .. } => {
            validate_insert_shape(columns, rows)?;
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
        Request::InsertExpressions { values, .. } => {
            if values.len() > crate::relational::MAX_COLUMNS {
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
            if values.len() > crate::relational::MAX_COLUMNS {
                return Err(Error::Bind("assignments".into()));
            }
            pending.push((filter, 0));
            pending.extend(guard.iter().map(|e| (e, 0)));
            pending.extend(values.iter().map(|(_, e)| (e, 0)));
        }
        Request::ReadExpressions {
            filter, projection, ..
        } => {
            if projection.len() > crate::relational::MAX_COLUMNS {
                return Err(Error::Limit);
            }
            pending.extend(filter.iter().map(|e| (e, 0)));
            pending.extend(projection.iter().map(|item| (&item.expression, 0)));
        }
        _ => (),
    }
    validate_expression_nodes(pending, nodes)
}
fn validate_insert_shape(columns: &[String], rows: &[Vec<Expr>]) -> Result<()> {
    if columns.len() > crate::relational::MAX_COLUMNS || rows.len() > MAX_INSERT_ROWS {
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
    Bytes,
}
fn unify_type(left: Option<Ty>, right: Option<Ty>) -> Result<Option<Ty>> {
    if left.is_some() && right.is_some() && left != right {
        return Err(Error::Type);
    }
    Ok(left.or(right))
}
fn ty(v: &Value) -> Option<Ty> {
    match v {
        Value::Null => None,
        Value::Bool(_) => Some(Ty::Bool),
        Value::Int(_) => Some(Ty::Int),
        Value::Text(_) => Some(Ty::Text),
        Value::Bytes(_) => Some(Ty::Bytes),
    }
}
fn colty(c: &ColumnDef) -> Ty {
    match c.data_type {
        DataType::Boolean => Ty::Bool,
        DataType::SmallInt | DataType::Integer | DataType::BigInt => Ty::Int,
        DataType::Text => Ty::Text,
        DataType::Bytes => Ty::Bytes,
    }
}
fn check(v: &Value, c: &ColumnDef) -> Result<()> {
    if *v == Value::Null {
        return if c.not_null { Err(Error::Type) } else { Ok(()) };
    }
    if ty(v) != Some(colty(c)) {
        return Err(Error::Type);
    }
    if let Value::Int(n) = v {
        match c.data_type {
            DataType::SmallInt if i16::try_from(*n).is_err() => return Err(Error::Type),
            DataType::Integer if i32::try_from(*n).is_err() => return Err(Error::Type),
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
            .ok_or_else(|| Error::Bind("column".into()))
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
        match self.value(e)? {
            Value::Int(n) if (0..=MAX_ROWS as i64).contains(&n) => Ok(n as usize),
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
                if t.is_some() && t != Some(Ty::Int) {
                    return Err(Error::Type);
                }
                (Bound::Negate(Box::new(e)), Some(Ty::Int))
            }
            Expr::Arithmetic(a, op, b) => {
                let (a, ta) = self.expr(a, d + 1)?;
                let (b, tb) = self.expr(b, d + 1)?;
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
                let (a, ta) = self.expr(a, d + 1)?;
                let (b, tb) = self.expr(b, d + 1)?;
                if ta.is_some() && tb.is_some() && ta != tb {
                    return Err(Error::Type);
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
                boolean(t)?;
                (Bound::Not(Box::new(e)), Some(Ty::Bool))
            }
            Expr::And(a, b) | Expr::Or(a, b) => {
                let (a, ta) = self.expr(a, d + 1)?;
                let (b, tb) = self.expr(b, d + 1)?;
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
    fn assignments(
        &mut self,
        v: &[(String, ValueExpr)],
        insert: bool,
    ) -> Result<Vec<(usize, Value)>> {
        if v.is_empty() || v.len() > crate::relational::MAX_COLUMNS {
            return Err(Error::Bind("assignments".into()));
        }
        let mut seen = BTreeSet::new();
        let mut out = vec![];
        for (n, e) in v {
            let i = self.field(n)?;
            if !seen.insert(i) {
                return Err(Error::Bind("duplicate assignment".into()));
            }
            if !insert && self.table()?.primary_key.contains(&i) {
                return Err(Error::Unsupported);
            }
            let v = self.value(e)?;
            check(&v, &self.table()?.columns[i])?;
            out.push((i, v))
        }
        Ok(out)
    }
    fn expression_assignments(&mut self, values: &[(String, Expr)]) -> Result<Vec<(usize, Bound)>> {
        if values.is_empty() || values.len() > crate::relational::MAX_COLUMNS {
            return Err(Error::Bind("assignments".into()));
        }
        let mut seen = BTreeSet::new();
        let mut assignments = Vec::new();
        for (name, expression) in values {
            let index = self.field(name)?;
            if !seen.insert(index) {
                return Err(Error::Bind("duplicate assignment".into()));
            }
            if self.table()?.primary_key.contains(&index) {
                return Err(Error::Unsupported);
            }
            let (expression, ty) = self.expr(expression, 0)?;
            if ty.is_some() && ty != Some(colty(&self.table()?.columns[index])) {
                return Err(Error::Type);
            }
            // Column width and nullability depend on the selected postimages.
            // The legacy value-only request retains its eager checks above.
            assignments.push((index, expression));
        }
        Ok(assignments)
    }
    fn projections(&mut self, items: &[Projection]) -> Result<Vec<BoundProjection>> {
        if items.is_empty() {
            return Err(Error::Bind("projection".into()));
        }
        if items.len() > crate::relational::MAX_COLUMNS {
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
                        Ty::Bytes => DataType::Bytes,
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
    fn bind(&mut self, s: &Request) -> Result<Operation> {
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
                    Some(v) => self.count(v)?,
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
                    Some(value) => self.count(value)?,
                    None => 100,
                },
                projection: self.projections(projection)?,
            },
            Request::InsertMany {
                columns, rows, cap, ..
            } => {
                validate_insert_shape(columns, rows)?;
                let mut seen = BTreeSet::new();
                let mut indexes = Vec::new();
                for name in columns {
                    let index = self.field(name)?;
                    if !seen.insert(index) {
                        return Err(Error::Bind("duplicate assignment".into()));
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
                        if ty.is_some() && ty != Some(colty(&self.table()?.columns[*index])) {
                            return Err(Error::Type);
                        }
                        bound_row.push(expression);
                    }
                    bound_rows.push(bound_row);
                }
                if !rows.is_empty() {
                    for (index, column) in self.table()?.columns.iter().enumerate() {
                        if !seen.contains(&index) {
                            check(&Value::Null, column)?;
                        }
                    }
                }
                Operation::InsertMany {
                    columns: indexes,
                    rows: bound_rows,
                    cap: self.count(cap)?,
                    returning: None,
                }
            }
            Request::InsertExpressions { values, .. } => {
                if values.is_empty() || values.len() > crate::relational::MAX_COLUMNS {
                    return Err(Error::Bind("assignments".into()));
                }
                let mut seen = BTreeSet::new();
                let mut expressions = vec![Bound::Value(Value::Null); self.table()?.columns.len()];
                for (name, expression) in values {
                    let index = self.field(name)?;
                    if !seen.insert(index) {
                        return Err(Error::Bind("duplicate assignment".into()));
                    }
                    self.allow_fields = false;
                    let bound = self.expr(expression, 0);
                    self.allow_fields = true;
                    let (expression, ty) = bound?;
                    if ty.is_some() && ty != Some(colty(&self.table()?.columns[index])) {
                        return Err(Error::Type);
                    }
                    expressions[index] = expression;
                }
                for (index, column) in self.table()?.columns.iter().enumerate() {
                    if !seen.contains(&index) {
                        check(&Value::Null, column)?;
                    }
                }
                Operation::InsertExpressions {
                    values: expressions,
                    returning: None,
                }
            }
            Request::Insert { values, .. } => {
                let a = self.assignments(values, true)?;
                let mut values = vec![Value::Null; self.table()?.columns.len()];
                for (i, v) in a {
                    values[i] = v
                }
                for (v, c) in values.iter().zip(&self.table()?.columns) {
                    check(v, c)?
                }
                Operation::Insert {
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
                Operation::Insert { values, .. } => (
                    (0..values.len()).collect(),
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
        (Value::Int(a), Value::Int(b)) => a.cmp(b),
        (Value::Text(a), Value::Text(b)) => a.cmp(b),
        (Value::Bytes(a), Value::Bytes(b)) => a.cmp(b),
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
fn matching_rows(rows: Vec<SqlRow>, filter: &Bound) -> Result<Vec<SqlRow>> {
    let mut matches = Vec::new();
    for row in rows {
        crate::cancellation::check().map_err(core)?;
        if filter.accepts(&row)? {
            matches.push(row);
        }
    }
    Ok(matches)
}
fn sort(rows: &mut [SqlRow], order: &[(usize, bool)]) {
    rows.sort_by(|a, b| {
        for (i, desc) in order {
            let c = compare(&a.values[*i], &b.values[*i]);
            let c = if *desc { c.reverse() } else { c };
            if c != Ordering::Equal {
                return c;
            }
        }
        Ordering::Equal
    })
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
fn run(
    s: &mut SqlSession,
    t: &TableDef,
    op: Operation,
    input: CompleteRelation,
    mut retained: Option<&mut usize>,
) -> Result<Outcome> {
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
        return upsert::run(
            s,
            t,
            upsert::Plan {
                columns,
                expressions: proposed,
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
    ) && rows.len() == MAX_ROWS
    {
        return Err(Error::Limit);
    }
    let op = match op {
        Operation::InsertExpressions { values, returning } => {
            crate::cancellation::check().map_err(core)?;
            let evaluated = values
                .iter()
                .map(|value| value.eval(&[]))
                .collect::<Result<Vec<_>>>()?;
            let mut remaining = crate::relational::MAX_ROW_BYTES;
            for (value, column) in evaluated.iter().zip(&t.columns) {
                check(value, column)?;
                charge_value(&mut remaining, value)?;
            }
            Operation::Insert {
                values: evaluated.into_iter().map(Cow::into_owned).collect(),
                returning,
            }
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
            return insert_many(s, t, columns, proposed, returning, rows, retained);
        }
        Operation::InsertExpressions { .. } | Operation::Upsert { .. } => {
            return Err(Error::Unsupported);
        }
        Operation::Read {
            filter,
            order,
            limit,
            projection,
        } => {
            let mut rows = matching_rows(rows, &filter)?;
            sort(&mut rows, &order);
            out.matched = rows.len();
            out.complete = rows.len() <= limit;
            out.columns = projection
                .iter()
                .map(|i| t.columns[*i].name.clone())
                .collect();
            for row in rows.iter().take(limit) {
                crate::cancellation::check().map_err(core)?;
                for index in &projection {
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
            let mut rows = matching_rows(rows, &filter)?;
            sort(&mut rows, &order);
            out.matched = rows.len();
            out.complete = rows.len() <= limit;
            out.columns = projection
                .iter()
                .map(|item| item.column.name.clone())
                .collect();
            let mut remaining = crate::relational::MAX_RESULT_BYTES;
            for (index, row) in rows.iter().enumerate() {
                crate::cancellation::check().map_err(core)?;
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
        Operation::Insert { values, returning } => {
            let key = if let Some(projection) = &returning {
                crate::cancellation::check().map_err(core)?;
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
            let mut matches = matching_rows(rows, &filter)?;
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
                crate::cancellation::check().map_err(core)?;
                if !guard.accepts(row)? {
                    return Err(Error::ConditionFailed);
                }
            }
            let mut work = vec![];
            let computed = matches!(&sets, Some(Assignments::Expressions(_)));
            let mut remaining_mutation = MAX_MUTATION_BYTES;
            for row in &matches {
                crate::cancellation::check().map_err(core)?;
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
                    crate::cancellation::check().map_err(core)?;
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
            crate::cancellation::check().map_err(core)?;
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
            for (index, (row, evaluated, changed)) in work.into_iter().enumerate() {
                if !changed {
                    continue;
                }
                crate::cancellation::check().map_err(core)?;
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

fn insert_many(
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
    // Borrow explicit payload through full-row and aggregate admission. Omitted
    // nullable fields are generated only for actual proposed rows.
    for expressions in &expressions {
        crate::cancellation::check().map_err(core)?;
        let mut row = vec![Cow::Owned(Value::Null); table.columns.len()];
        for (index, expression) in columns.iter().zip(expressions) {
            row[*index] = expression.eval(&[])?;
        }
        let mut remaining_row = crate::relational::MAX_ROW_BYTES;
        for (value, column) in row.iter().zip(&table.columns) {
            check(value, column)?;
            charge_value(&mut remaining_row, value)?;
            charge_value(&mut remaining, value)?;
        }
        proposed.push(row);
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
            crate::relational::encode_key(&key).map_err(|error| core(CoreError::Sql(error)))?;
        if !unique.insert(encoded.clone()) {
            return Err(Error::DuplicateTarget);
        }
        keys.push((encoded, key, index));
    }
    for row in &existing {
        crate::cancellation::check().map_err(core)?;
        let key = table
            .primary_key
            .iter()
            .map(|i| row.values[*i].clone())
            .collect::<Vec<_>>();
        let encoded =
            crate::relational::encode_key(&key).map_err(|error| core(CoreError::Sql(error)))?;
        if unique.contains(&encoded) {
            return Err(core(CoreError::Sql(SqlError::Duplicate)));
        }
    }
    if existing.len().checked_add(count).ok_or(Error::Limit)? > MAX_ROWS {
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
    for (output_index, (_, key, proposal_index)) in keys.into_iter().enumerate() {
        crate::cancellation::check().map_err(core)?;
        let values = proposed[proposal_index]
            .take()
            .ok_or(Error::DuplicateTarget)?
            .into_iter()
            .map(Cow::into_owned)
            .collect();
        let result = session.insert_engine(table, values).map_err(core)?;
        if result.affected != 1 {
            return Err(Error::Conflict);
        }
        if returning.is_some() {
            out.rows[output_index].version =
                session.row_version_engine(table, &key).map_err(core)?;
        }
    }
    Ok(out)
}
