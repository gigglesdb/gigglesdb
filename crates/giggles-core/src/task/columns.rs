//! Column references of a task statement (GD-0061 design section 3.2).
//!
//! The SQL front end that fills a [`StatementShape`] is `relational::task_refs`
//! (it needs the private SQL AST); the native front end is [`native_shape`]
//! (R3-304). Both are judged by the same rule function, `check` (R3-303).

pub use super::admit::StatementClass;

use std::collections::BTreeSet;

use super::definition::{ColumnMode, TaskDef};
use super::error::{DenyRule, TaskError};
use crate::operations::{Expr, Intent, Projection, Request};
use crate::relational::{Prepared, Relational, TableDef, Value};

/// Where a column reference sits in a statement.
///
/// `Output` is a top-level select item that is exactly a column (an alias is
/// allowed). A column anywhere under a call, operator, cast, `CASE`, predicate
/// or `SUBSTRING` form is `Expression`, except under an aggregate (`Aggregate`),
/// in an assignment value (`AssignSource`) or in RETURNING (`ReturningExpr`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Position {
    Output,
    Filter,
    Sort,
    Join,
    Group,
    Aggregate,
    Expression,
    AssignSource,
    AssignTarget,
    Returning,
    ReturningExpr,
}

/// One column reference, with the column name as written (any qualifier is
/// dropped; task statements read one table).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Reference {
    pub column: String,
    pub position: Position,
}

/// Syntactic shape of one statement, collected before binding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatementShape {
    pub class: StatementClass,
    /// Every relation named in the statement, including subqueries and derived
    /// tables, in first-seen order without duplicates.
    pub tables: Vec<String>,
    /// Column references in walk order; an identical (column, position) pair is
    /// recorded once.
    pub refs: Vec<Reference>,
    /// Routine and aggregate call names as the parser stores them (distinct
    /// aggregates use their `\0aggregate_distinct_*` call name), first-seen
    /// order without duplicates.
    pub functions: Vec<String>,
    /// A domain cast (`Scalar::Domain`) or a composite-type SELECT.
    pub casts_user_type: bool,
    /// Subqueries and derived tables, at any depth.
    pub subqueries: usize,
    /// Joined sources: explicit JOINs, or extra comma-separated sources.
    pub joins: usize,
    /// `*` or `t.*` in a select list (or the legacy all-columns projection).
    pub star: bool,
    /// `RETURNING *` or `RETURNING t.*`.
    pub returning_star: bool,
    pub explain: bool,
}

impl StatementShape {
    pub fn new(class: StatementClass) -> Self {
        Self {
            class,
            tables: Vec::new(),
            refs: Vec::new(),
            functions: Vec::new(),
            casts_user_type: false,
            subqueries: 0,
            joins: 0,
            star: false,
            returning_star: false,
            explain: false,
        }
    }

    pub fn add_ref(&mut self, column: &str, position: Position) {
        if !self
            .refs
            .iter()
            .any(|r| r.position == position && r.column == column)
        {
            self.refs.push(Reference {
                column: column.to_owned(),
                position,
            });
        }
    }

    pub fn add_table(&mut self, name: &str) {
        if !self.tables.iter().any(|t| t == name) {
            self.tables.push(name.to_owned());
        }
    }

    pub fn add_function(&mut self, name: &str) {
        if !self.functions.iter().any(|f| f == name) {
            self.functions.push(name.to_owned());
        }
    }
}

/// Routine and aggregate call names a task statement may use (sorted).
///
/// Reviewed code, not catalog data. `nullif` is left out: the binder
/// refuses it as a call today (`Unsupported`). Operators, comparisons, `CASE`, `LIKE`,
/// `IS NULL` and casts to built-in types are AST nodes, not calls.
pub(crate) const TASK_FUNCTIONS: &[&str] = &[
    "\0aggregate_distinct_avg",
    "\0aggregate_distinct_count",
    "\0aggregate_distinct_max",
    "\0aggregate_distinct_min",
    "\0aggregate_distinct_sum",
    "abs",
    "avg",
    "btrim",
    "ceil",
    "ceiling",
    "char_length",
    "coalesce",
    "count",
    "current_date",
    "current_timestamp",
    "date_part",
    "date_trunc",
    "extract",
    "floor",
    "greatest",
    "least",
    "left",
    "length",
    "lower",
    "ltrim",
    "max",
    "min",
    "mod",
    "now",
    "octet_length",
    "position",
    "replace",
    "right",
    "round",
    "rtrim",
    "sign",
    "starts_with",
    "strpos",
    "substr",
    "substring",
    "sum",
    "trunc",
    "upper",
];

const SEQUENCE_FUNCTIONS: &[&str] = &["currval", "lastval", "nextval", "setval"];

/// Row-identity names that are never available to a task.
const ROW_IDENTITY: &[&str] = &["ctid", crate::relational::HIDDEN_ROWID];

/// Untrusted-text marks of one table's columns, by column name, read from the
/// catalog's kind-21 column labels (GD-0061 R3-307, design section 5).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ColumnMarks {
    pub untrusted: BTreeSet<String>,
}

impl ColumnMarks {
    /// Marks from column names that carry the `untrusted_text` label.
    pub fn new<I: IntoIterator<Item = S>, S: Into<String>>(untrusted: I) -> Self {
        Self {
            untrusted: untrusted.into_iter().map(Into::into).collect(),
        }
    }

    /// Marks of `table` from the catalog's label map, keyed by
    /// (table id, column id).
    pub(crate) fn from_labels(
        table: &TableDef,
        labels: &std::collections::BTreeMap<(u64, u64), u8>,
    ) -> Self {
        Self::new(
            table
                .columns
                .iter()
                .filter(|column| {
                    labels
                        .get(&(table.id, column.id))
                        .is_some_and(|flags| flags & crate::storage::UNTRUSTED_TEXT_LABEL != 0)
                })
                .map(|column| column.name.clone()),
        )
    }

    pub fn is_untrusted(&self, column: &str) -> bool {
        self.untrusted.contains(column)
    }
}

/// What release does with one output column.
#[derive(Debug, Clone, PartialEq)]
pub enum OutputMode {
    Plain,
    /// Replace with this value (`Null` or the definition's constant).
    Mask(Value),
    /// Untrusted and not allowed: withheld at release.
    Withhold,
    /// Untrusted but allowed: released and counted.
    CountUntrusted,
}

/// The release plan of one admitted statement, stored in its `ReleaseTag`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct OutputPlan {
    pub outputs: Vec<OutputMode>,
    /// (column id, position bit set) for each column the statement reads.
    pub refs: Vec<(u64, u16)>,
}

/// A column's mode for one task after the untrusted-text marks are applied.
#[derive(Debug, Clone, PartialEq)]
pub struct EffectiveMode {
    pub mode: ColumnMode,
    /// The value is withheld as NULL because the column is marked untrusted
    /// and not listed in `untrusted_allowed` (counted in `withheld_untrusted`).
    pub withheld: bool,
    /// The column is marked untrusted and allowed: values are released
    /// unchanged and counted in `untrusted_released`.
    pub untrusted_released: bool,
}

/// The definition's mode for `column`, except that a marked column not listed
/// in `untrusted_allowed` becomes `Masked { constant: None }` and is withheld.
/// Unlisted, hidden, row-identity and nonexistent columns are all `Hidden`;
/// a mark never reveals them.
pub fn effective_mode(def: &TaskDef, marks: &ColumnMarks, column: &str) -> EffectiveMode {
    if ROW_IDENTITY.contains(&column) {
        return EffectiveMode {
            mode: ColumnMode::Hidden,
            withheld: false,
            untrusted_released: false,
        };
    }
    let mode = def
        .columns
        .get(column)
        .cloned()
        .unwrap_or(ColumnMode::Hidden);
    let marked = marks.is_untrusted(column) && mode != ColumnMode::Hidden;
    if marked && !def.untrusted_allowed.contains(column) {
        return EffectiveMode {
            mode: ColumnMode::Masked { constant: None },
            withheld: true,
            untrusted_released: false,
        };
    }
    EffectiveMode {
        mode,
        withheld: false,
        untrusted_released: marked,
    }
}

fn position_bit(p: Position) -> u16 {
    1 << (p as u16)
}

fn denied(rule: DenyRule) -> TaskError {
    TaskError::Denied(rule)
}

fn function_allowed(name: &str) -> bool {
    TASK_FUNCTIONS.binary_search(&name).is_ok()
}

fn same_table(written: &str, table: &str) -> bool {
    let bare = written.rsplit('.').next().unwrap_or(written);
    bare == table
}

/// Judge one statement shape against the task definition (design section 2).
///
/// For a SELECT the plan's `outputs` follow the plain-column select items in
/// order; `apply` runs this after the star rewrite.
pub(crate) fn check(
    def: &TaskDef,
    marks: &ColumnMarks,
    table: &TableDef,
    shape: &StatementShape,
) -> Result<OutputPlan, TaskError> {
    match shape.class {
        StatementClass::Select | StatementClass::Update => {}
        StatementClass::Explain => return Err(denied(DenyRule::Explain)),
        other => return Err(TaskError::NotAllowedInTaskSession(other)),
    }
    if shape.explain {
        return Err(denied(DenyRule::Explain));
    }
    if shape.casts_user_type {
        return Err(denied(DenyRule::UserType));
    }
    if shape.tables.iter().any(|t| !same_table(t, &table.name)) {
        return Err(denied(DenyRule::OtherTable));
    }
    if shape.joins > 0 || shape.tables.len() > 1 {
        return Err(denied(DenyRule::Join));
    }
    if shape.subqueries > 0 {
        return Err(denied(DenyRule::Subquery));
    }
    for f in &shape.functions {
        if SEQUENCE_FUNCTIONS.contains(&f.to_ascii_lowercase().as_str()) {
            return Err(denied(DenyRule::Sequence));
        }
        if !function_allowed(f) {
            return Err(denied(DenyRule::FunctionNotAllowed));
        }
    }
    let writable = def.allowed_write.as_ref().map(|w| &w.columns);
    let mut plan = OutputPlan::default();
    for r in &shape.refs {
        let eff = effective_mode(def, marks, &r.column);
        let Some(id) = table
            .logical_columns()
            .find(|c| c.name == r.column)
            .map(|c| c.id)
        else {
            return Err(denied(DenyRule::ColumnNotAvailable));
        };
        match (&eff.mode, r.position) {
            (ColumnMode::Hidden, _) => return Err(denied(DenyRule::ColumnNotAvailable)),
            (ColumnMode::Masked { .. }, Position::Output) => {}
            (ColumnMode::Masked { .. }, _) => {
                return Err(denied(DenyRule::MaskedColumnPosition));
            }
            (ColumnMode::Readable, Position::AssignTarget) => {
                if !writable.is_some_and(|w| w.contains(&r.column)) {
                    return Err(denied(DenyRule::ColumnNotWritable));
                }
            }
            (ColumnMode::Readable, _) => {}
        }
        match plan.refs.iter_mut().find(|(c, _)| *c == id) {
            Some((_, bits)) => *bits |= position_bit(r.position),
            None => plan.refs.push((id, position_bit(r.position))),
        }
    }
    if shape.class == StatementClass::Select {
        for r in shape.refs.iter().filter(|r| r.position == Position::Output) {
            plan.outputs
                .push(output_mode(&effective_mode(def, marks, &r.column)));
        }
    }
    Ok(plan)
}

fn output_mode(eff: &EffectiveMode) -> OutputMode {
    if eff.withheld {
        return OutputMode::Withhold;
    }
    match &eff.mode {
        ColumnMode::Masked { constant } => OutputMode::Mask(constant.clone().unwrap_or(Value::Null)),
        _ if eff.untrusted_released => OutputMode::CountUntrusted,
        _ => OutputMode::Plain,
    }
}

/// Readable and masked columns, in `TableDef::logical_columns` order.
pub(crate) fn star_columns(def: &TaskDef, marks: &ColumnMarks, table: &TableDef) -> Vec<String> {
    table
        .logical_columns()
        .filter(|c| effective_mode(def, marks, &c.name).mode != ColumnMode::Hidden)
        .map(|c| c.name.clone())
        .collect()
}

/// Readable columns only, for `RETURNING *`.
pub(crate) fn returning_columns(
    def: &TaskDef,
    marks: &ColumnMarks,
    table: &TableDef,
) -> Vec<String> {
    table
        .logical_columns()
        .filter(|c| {
            let e = effective_mode(def, marks, &c.name);
            e.mode == ColumnMode::Readable && !e.withheld
        })
        .map(|c| c.name.clone())
        .collect()
}

/// Check a prepared task statement, rewrite its stars and build its plan.
///
/// `bound_def_version` is the definition version the session bound; a
/// mismatch returns `DefinitionChanged`. Idempotent: a statement without a
/// star comes back as an equal clone.
pub(crate) fn apply(
    def: &TaskDef,
    bound_def_version: u64,
    marks: &ColumnMarks,
    table: &TableDef,
    p: &Prepared,
) -> Result<(Prepared, OutputPlan), TaskError> {
    if def.def_version != bound_def_version {
        return Err(denied(DenyRule::DefinitionChanged));
    }
    let shape = p.task_shape().map_err(TaskError::Sql)?;
    // Refuse by shape before expanding, so a star over another table never
    // expands and a refused statement reads no column list.
    check(def, marks, table, &shape)?;
    let star = star_columns(def, marks, table);
    let returning = returning_columns(def, marks, table);
    if (shape.star && star.is_empty()) || (shape.returning_star && returning.is_empty()) {
        return Err(denied(DenyRule::ColumnNotAvailable));
    }
    let expanded = if shape.star || shape.returning_star {
        p.task_expand_star(&star, &returning)
    } else {
        p.clone()
    };
    debug_assert!(!expanded.task_has_star());
    let shape = expanded.task_shape().map_err(TaskError::Sql)?;
    let plan = check(def, marks, table, &shape)?;
    Ok((expanded, plan))
}

/// `apply` against the catalog snapshot a statement binds on.
///
/// This is the entry point for the three SQL call sites (design section 2):
/// `SqlSession::prepare`, the `execute_v2` read/DML arm after
/// `db.catalog().snapshot(txn)`, and `open_cursor_inner`. They pass the
/// session's `TaskContext::bound_def_version` once R3-202 puts the context on
/// `SqlSession`, and store the returned `OutputPlan` in the statement's
/// `ReleaseTag` (R3-301). `labels` is the same snapshot's kind-21 column label
/// map (`CatalogView::column_labels`, R3-307); the table's untrusted-text marks
/// are read from it. A task whose table no longer exists in `catalog` refuses
/// every statement with `OtherTable`.
#[allow(dead_code, reason = "call sites wait for R3-202 session state")]
pub(crate) fn apply_in_catalog(
    def: &TaskDef,
    bound_def_version: u64,
    labels: &std::collections::BTreeMap<(u64, u64), u8>,
    catalog: &Relational,
    p: &Prepared,
) -> Result<(Prepared, OutputPlan), TaskError> {
    if def.def_version != bound_def_version {
        return Err(denied(DenyRule::DefinitionChanged));
    }
    let table = catalog
        .tables
        .values()
        .find(|t| t.id == def.table)
        .ok_or(denied(DenyRule::OtherTable))?;
    let marks = ColumnMarks::from_labels(table, labels);
    apply(def, bound_def_version, &marks, table, p)
}

/// The SQL rules for one statement text, for tests that compare the native
/// and SQL front ends (R3-304 `same_denials_as_sql`). Parse errors come back
/// as `TaskError::Sql`.
#[doc(hidden)]
pub fn apply_sql_text(
    def: &TaskDef,
    bound_def_version: u64,
    marks: &ColumnMarks,
    table: &TableDef,
    sql: &str,
) -> Result<OutputPlan, TaskError> {
    let p = Prepared::parse(sql, 1, 0).map_err(TaskError::Sql)?;
    apply(def, bound_def_version, marks, table, &p).map(|(_, plan)| plan)
}

/// The task class of a native request (as the server's `task_class`):
/// `Insert*` and `OnConflict` are `Insert`, `Delete` is `Delete`, and a
/// `Returning` takes its inner request's class.
fn native_class(request: &Request) -> StatementClass {
    match request {
        Request::Read { .. } | Request::ReadExpressions { .. } => StatementClass::Select,
        Request::Update { .. } | Request::UpdateExpressions { .. } => StatementClass::Update,
        Request::Delete { .. } => StatementClass::Delete,
        Request::Insert { .. }
        | Request::InsertExpressions { .. }
        | Request::InsertMany { .. }
        | Request::OnConflict { .. } => StatementClass::Insert,
        Request::Returning { request, .. } => match request.as_ref() {
            Request::Returning { .. } => StatementClass::Other,
            inner => native_class(inner),
        },
    }
}

/// Record every column under `expr` at `position`, and every scalar call.
/// Iterative, so an unvalidated deep tree cannot overflow the stack.
fn expr_refs(out: &mut StatementShape, expr: &Expr, position: Position) {
    let mut stack = vec![expr];
    while let Some(e) = stack.pop() {
        match e {
            Expr::Value(_) | Expr::Local(_) => {}
            Expr::Field(column) => out.add_ref(column, position),
            // Only an ON CONFLICT action names these; that request is refused
            // by class, but its columns are still recorded.
            Expr::ConflictField { field, .. } => out.add_ref(field, position),
            Expr::IsNull(e, _) | Expr::Not(e) | Expr::Negate(e) => stack.push(e),
            Expr::Compare(a, _, b)
            | Expr::And(a, b)
            | Expr::Or(a, b)
            | Expr::Arithmetic(a, _, b) => {
                stack.push(b);
                stack.push(a);
            }
            Expr::Call {
                function,
                arguments,
            } => {
                out.add_function(&format!("{function:?}").to_ascii_lowercase());
                stack.extend(arguments.iter().rev());
            }
            Expr::Case {
                operand,
                arms,
                else_expr,
            } => {
                stack.extend(else_expr.iter().map(Box::as_ref));
                for (when, then) in arms.iter().rev() {
                    stack.push(then);
                    stack.push(when);
                }
                stack.extend(operand.iter().map(Box::as_ref));
            }
        }
    }
}

fn projection_refs(
    out: &mut StatementShape,
    items: &[Projection],
    bare: Position,
    expr: Position,
) {
    for item in items {
        match &item.expression {
            Expr::Field(column) => out.add_ref(column, bare),
            other => expr_refs(out, other, expr),
        }
    }
}

fn mutation_refs(out: &mut StatementShape, filter: &Expr, guard: Option<&Expr>, intent: &Intent) {
    expr_refs(out, filter, Position::Filter);
    if let Some(guard) = guard {
        expr_refs(out, guard, Position::Filter);
    }
    if let Intent::Claim { order } = intent {
        for sort in order {
            out.add_ref(&sort.field, Position::Sort);
        }
    }
}

/// Syntactic shape of a native request (design section 2, native front end).
///
/// - `Read.projection` is Output (an empty projection is `star`).
///   `ReadExpressions.projection` is Output for a bare `Expr::Field`,
///   otherwise Expression.
/// - `filter` and `guard` are Filter; `order` and a claim order are Sort.
/// - `Update.values` and `UpdateExpressions.values` names are AssignTarget;
///   `UpdateExpressions.values` expressions are AssignSource.
/// - `Returning.projection` is Returning for a bare field, otherwise
///   ReturningExpr; an empty projection is `returning_star`.
/// - `Insert*`, `Delete` and `OnConflict` yield their refused class.
///
/// Native scalar functions (`coalesce`, `abs`, `length`) are recorded under
/// their lower-case names, all of which the allowlist holds.
pub fn native_shape(request: &Request) -> StatementShape {
    let mut out = StatementShape::new(native_class(request));
    let (body, returning) = match request {
        Request::Returning {
            request,
            projection,
        } => (request.as_ref(), Some(projection)),
        other => (other, None),
    };
    match body {
        Request::Read {
            table,
            filter,
            order,
            projection,
            ..
        } => {
            out.add_table(table);
            out.star = projection.is_empty();
            for column in projection {
                out.add_ref(column, Position::Output);
            }
            if let Some(filter) = filter {
                expr_refs(&mut out, filter, Position::Filter);
            }
            for sort in order {
                out.add_ref(&sort.field, Position::Sort);
            }
        }
        Request::ReadExpressions {
            table,
            filter,
            order,
            projection,
            ..
        } => {
            out.add_table(table);
            projection_refs(&mut out, projection, Position::Output, Position::Expression);
            if let Some(filter) = filter {
                expr_refs(&mut out, filter, Position::Filter);
            }
            for sort in order {
                out.add_ref(&sort.field, Position::Sort);
            }
        }
        Request::Update {
            table,
            filter,
            guard,
            values,
            intent,
        } => {
            out.add_table(table);
            for (column, _) in values {
                out.add_ref(column, Position::AssignTarget);
            }
            mutation_refs(&mut out, filter, guard.as_ref(), intent);
        }
        Request::UpdateExpressions {
            table,
            filter,
            guard,
            values,
            intent,
        } => {
            out.add_table(table);
            for (column, value) in values {
                out.add_ref(column, Position::AssignTarget);
                expr_refs(&mut out, value, Position::AssignSource);
            }
            mutation_refs(&mut out, filter, guard.as_ref(), intent);
        }
        Request::Insert { table, .. }
        | Request::InsertExpressions { table, .. }
        | Request::InsertMany { table, .. }
        | Request::Delete { table, .. } => out.add_table(table),
        Request::Returning { .. } | Request::OnConflict { .. } => {}
    }
    if let Some(projection) = returning {
        out.returning_star = projection.is_empty();
        projection_refs(&mut out, projection, Position::Returning, Position::ReturningExpr);
    }
    out
}

/// The request with an empty `Read.projection` expanded to `star` and an
/// empty `Returning.projection` to `returning`. Idempotent.
fn native_expand(request: &Request, star: &[String], returning: &[String]) -> Request {
    let mut out = request.clone();
    let (body, projection) = match &mut out {
        Request::Returning {
            request,
            projection,
        } => (request.as_mut(), Some(projection)),
        other => (other, None),
    };
    if let Request::Read { projection, .. } = body
        && projection.is_empty()
    {
        projection.extend(star.iter().cloned());
    }
    if let Some(projection) = projection
        && projection.is_empty()
    {
        projection.extend(returning.iter().map(|column| Projection {
            expression: Expr::Field(column.clone()),
            alias: None,
        }));
    }
    out
}

/// Release mode of one computed output: counted when it reads an untrusted
/// column the task may see (a withheld column never reaches an expression).
fn expression_mode(def: &TaskDef, marks: &ColumnMarks, expr: &Expr) -> OutputMode {
    let mut shape = StatementShape::new(StatementClass::Select);
    expr_refs(&mut shape, expr, Position::Expression);
    if shape
        .refs
        .iter()
        .any(|r| effective_mode(def, marks, &r.column).untrusted_released)
    {
        OutputMode::CountUntrusted
    } else {
        OutputMode::Plain
    }
}

fn projection_modes(def: &TaskDef, marks: &ColumnMarks, items: &[Projection]) -> Vec<OutputMode> {
    items
        .iter()
        .map(|item| match &item.expression {
            Expr::Field(column) => output_mode(&effective_mode(def, marks, column)),
            other => expression_mode(def, marks, other),
        })
        .collect()
}

/// One mode per result column of an expanded, checked request.
fn native_outputs(def: &TaskDef, marks: &ColumnMarks, request: &Request) -> Vec<OutputMode> {
    match request {
        Request::Read { projection, .. } => projection
            .iter()
            .map(|column| output_mode(&effective_mode(def, marks, column)))
            .collect(),
        Request::ReadExpressions { projection, .. } | Request::Returning { projection, .. } => {
            projection_modes(def, marks, projection)
        }
        _ => Vec::new(),
    }
}

/// Check a native request, expand its empty projections and build its plan
/// (design section 2; the native twin of `apply`).
///
/// `table` is the task's table, already resolved by the caller. An empty
/// `Read.projection` becomes the readable and masked columns and an empty
/// `Returning.projection` the readable columns, in `logical_columns` order.
/// The plan's `outputs` has one mode per result column. Idempotent.
pub fn apply_native(
    def: &TaskDef,
    bound_def_version: u64,
    marks: &ColumnMarks,
    table: &TableDef,
    request: &Request,
) -> Result<(Request, OutputPlan), TaskError> {
    if def.def_version != bound_def_version {
        return Err(denied(DenyRule::DefinitionChanged));
    }
    let shape = native_shape(request);
    // Refuse by shape before expanding, as `apply` does.
    check(def, marks, table, &shape)?;
    let star = star_columns(def, marks, table);
    let returning = returning_columns(def, marks, table);
    if (shape.star && star.is_empty()) || (shape.returning_star && returning.is_empty()) {
        return Err(denied(DenyRule::ColumnNotAvailable));
    }
    let expanded = native_expand(request, &star, &returning);
    let mut plan = check(def, marks, table, &native_shape(&expanded))?;
    plan.outputs = native_outputs(def, marks, &expanded);
    Ok((expanded, plan))
}

/// The task rules a native request binds under in one task session: the
/// current definition, the bound version and the column labels of the
/// session's catalog snapshot (R3-304). Used by `operations::bind_request`
/// and by every step of `bind_transaction`, so a block's steps are all
/// checked before any step runs.
pub(crate) struct NativeRules {
    def: std::sync::Arc<TaskDef>,
    bound_def_version: u64,
    labels: std::collections::BTreeMap<(u64, u64), u8>,
}

impl NativeRules {
    /// `None` outside a task session. A task that is gone, revoked or
    /// replaced by another of the same name reads as `Revoked`.
    pub(crate) fn load(session: &crate::SqlSession) -> Result<Option<Self>, TaskError> {
        let Some(ctx) = session.task() else {
            return Ok(None);
        };
        let def = session
            .tasks()
            .lookup(ctx.name.as_str())
            .map_err(|e| TaskError::Core(Box::new(e)))?
            .filter(|def| def.id == ctx.task && !def.revoked)
            .ok_or(TaskError::Revoked)?;
        let labels = session
            .task_column_labels()
            .map_err(|e| TaskError::Core(Box::new(e)))?;
        Ok(Some(Self {
            def,
            bound_def_version: ctx.bound_def_version,
            labels,
        }))
    }

    /// Refusals that need no table: a changed definition and a refused
    /// class. They run before the request's relation is resolved.
    pub(crate) fn admit(&self, request: &Request) -> Result<(), TaskError> {
        if self.def.def_version != self.bound_def_version {
            return Err(denied(DenyRule::DefinitionChanged));
        }
        match native_class(request) {
            StatementClass::Select | StatementClass::Update => Ok(()),
            other => Err(TaskError::NotAllowedInTaskSession(other)),
        }
    }

    /// `apply_native` on the request's resolved relation, which must be the
    /// task's table.
    pub(crate) fn apply(&self, table: &TableDef, request: &Request) -> Result<Request, TaskError> {
        if table.id != self.def.table {
            return Err(denied(DenyRule::OtherTable));
        }
        let marks = ColumnMarks::from_labels(table, &self.labels);
        // The plan goes into the statement's ReleaseTag with the release
        // protocol (R3-308); until then a task page fails closed at release.
        apply_native(&self.def, self.bound_def_version, &marks, table, request)
            .map(|(request, _plan)| request)
    }
}

#[cfg(test)]
#[path = "columns_tests.rs"]
mod tests;
