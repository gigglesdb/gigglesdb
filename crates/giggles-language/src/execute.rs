use crate::*;
use giggles_core::{Admin, operations};
use std::io::{self, Write};

/// Trusted embedded native frontend for the common typed operation engine.
/// Independent Admin holders retain unrestricted SQL authority.
#[derive(Clone)]
pub struct Engine {
    pub(crate) inner: operations::Engine,
    pub(crate) version: LanguageVersion,
}

/// An immutable operation with argument values captured at preparation.
///
/// This handle has no parameter slots, database session, network encoding or
/// authority credential. Executing it again starts another operation; it is not
/// an idempotency key. Only the receiving [`Engine`]'s trusted Admin executes it.
///
/// Handles deliberately have no serialization format:
/// ```compile_fail
/// fn encode(operation: &giggles_language::BoundOperation) {
///     let _ = serde_json::to_string(operation);
/// }
/// ```
#[derive(Clone)]
pub struct BoundOperation {
    inner: operations::BoundOperation,
    language_version: &'static str,
    profile: &'static str,
}
impl BoundOperation {
    pub fn language_version(&self) -> &'static str {
        self.language_version
    }
    pub fn profile(&self) -> &'static str {
        self.profile
    }
    /// Stable catalog identities and typed output columns captured at prepare.
    /// A returned description is metadata, not permission to execute.
    pub fn description(&self) -> &operations::BoundDescription {
        self.inner.description()
    }
}

/// Immutable local transaction plan with frozen argument values and catalog fences.
/// It has no serialization or retry/receipt contract.
#[derive(Clone, Debug)]
pub struct BoundTransaction {
    inner: operations::BoundTransaction,
    version: LanguageVersion,
}
impl BoundTransaction {
    pub fn language_version(&self) -> &'static str {
        self.version.as_str()
    }
    pub fn profile(&self) -> &'static str {
        self.version.profile_name()
    }
    pub fn description(&self) -> &operations::BoundTransactionDescription {
        self.inner.description()
    }
}

/// Immutable proposed catalog definition, with no allocated table or column IDs.
/// The handle is local, value-bound, and not a migration receipt or credential.
#[derive(Clone, Debug)]
pub struct BoundCatalogOperation {
    inner: operations::BoundCatalogOperation,
    version: LanguageVersion,
}
impl BoundCatalogOperation {
    pub fn language_version(&self) -> &'static str {
        self.version.as_str()
    }
    pub fn profile(&self) -> &'static str {
        self.version.profile_name()
    }
    pub fn description(&self) -> &operations::BoundCatalogDescription {
        self.inner.description()
    }
}

impl Engine {
    pub fn new(admin: Admin) -> Self {
        Self::with_version(admin, LanguageVersion::Embedded01)
    }
    pub fn with_version(admin: Admin, version: LanguageVersion) -> Self {
        Self {
            inner: operations::Engine::new(admin),
            version,
        }
    }
    pub fn execute_text(&self, text: &str, bindings: &Bindings) -> Result<Outcome> {
        self.execute(&parse_with_version(text, self.version)?, bindings)
    }
    pub fn execute(&self, stmt: &Statement, bindings: &Bindings) -> Result<Outcome> {
        let request = lower_checked(stmt, self.version)?;
        self.inner
            .execute(&request, bindings)
            .map(outcome)
            .map_err(error)
    }

    /// Parse and bind text with the exact supplied values, without executing it.
    pub fn prepare_bound_text(&self, text: &str, bindings: &Bindings) -> Result<BoundOperation> {
        self.prepare_bound(&parse_with_version(text, self.version)?, bindings)
    }

    /// Bind an immutable operation against a fresh committed catalog snapshot.
    ///
    /// All arguments are required now and are copied into the bound operation.
    /// Preparation checks names and types but does not scan rows, evaluate
    /// cardinality/guards, or make durable changes. Subsequent argument changes
    /// require preparing another handle. The existing [`Engine::execute`] API
    /// continues binding and executing in a single transaction.
    pub fn prepare_bound(&self, stmt: &Statement, bindings: &Bindings) -> Result<BoundOperation> {
        let request = lower_checked(stmt, self.version)?;
        let inner = self
            .inner
            .prepare_bound(&request, bindings)
            .map_err(error)?;
        Ok(BoundOperation {
            inner,
            language_version: self.version.as_str(),
            profile: self.version.profile_name(),
        })
    }

    /// Execute frozen intent against fresh rows after checking its catalog fence.
    ///
    /// Schema changes or another database domain reject the handle before
    /// effects. Data changes are observed at execution, so caps and singular
    /// cardinality are checked again. There are no automatic retries; an
    /// [`Error::Unknown`] commit still needs application reconciliation.
    pub fn execute_bound(&self, operation: &BoundOperation) -> Result<Outcome> {
        if operation.language_version != self.version.as_str()
            || operation.profile != self.version.profile_name()
        {
            return Err(Error::Unsupported);
        }
        self.inner
            .execute_bound(&operation.inner)
            .map(outcome)
            .map_err(error)
    }
    pub fn execute_transaction_text(
        &self,
        text: &str,
        bindings: &Bindings,
    ) -> Result<TransactionOutcome> {
        self.execute_transaction(
            &parse_transaction_with_version(text, self.version)?,
            bindings,
        )
    }
    pub fn execute_transaction(
        &self,
        transaction: &Transaction,
        bindings: &Bindings,
    ) -> Result<TransactionOutcome> {
        let transaction = lower_transaction_checked(transaction, self.version)?;
        self.inner
            .execute_transaction(&transaction, bindings)
            .map_err(error)
    }
    pub fn prepare_transaction_bound_text(
        &self,
        text: &str,
        bindings: &Bindings,
    ) -> Result<BoundTransaction> {
        self.prepare_transaction_bound(
            &parse_transaction_with_version(text, self.version)?,
            bindings,
        )
    }
    pub fn prepare_transaction_bound(
        &self,
        transaction: &Transaction,
        bindings: &Bindings,
    ) -> Result<BoundTransaction> {
        let transaction = lower_transaction_checked(transaction, self.version)?;
        Ok(BoundTransaction {
            inner: self
                .inner
                .prepare_transaction_bound(&transaction, bindings)
                .map_err(error)?,
            version: self.version,
        })
    }
    pub fn execute_transaction_bound(
        &self,
        transaction: &BoundTransaction,
    ) -> Result<TransactionOutcome> {
        if self.version != transaction.version || !self.version.supports_transactions() {
            return Err(Error::Unsupported);
        }
        self.inner
            .execute_transaction_bound(&transaction.inner)
            .map_err(error)
    }

    /// Execute one safe catalog operation in its own transaction.
    pub fn execute_catalog_text(&self, text: &str) -> Result<CatalogOutcome> {
        self.execute_catalog(&parse_catalog_with_version(text, self.version)?)
    }
    pub fn execute_catalog(&self, statement: &CatalogStatement) -> Result<CatalogOutcome> {
        let request = lower_catalog_checked(statement, self.version)?;
        self.inner.execute_catalog(&request).map_err(error)
    }
    /// Validate an immutable proposed definition without allocating catalog IDs.
    /// Population policy is checked against fresh state only at execution.
    pub fn prepare_catalog_bound_text(&self, text: &str) -> Result<BoundCatalogOperation> {
        self.prepare_catalog_bound(&parse_catalog_with_version(text, self.version)?)
    }
    pub fn prepare_catalog_bound(
        &self,
        statement: &CatalogStatement,
    ) -> Result<BoundCatalogOperation> {
        let request = lower_catalog_checked(statement, self.version)?;
        Ok(BoundCatalogOperation {
            inner: self.inner.prepare_catalog_bound(&request).map_err(error)?,
            version: self.version,
        })
    }
    pub fn execute_catalog_bound(
        &self,
        operation: &BoundCatalogOperation,
    ) -> Result<CatalogOutcome> {
        if self.version != operation.version || !self.version.supports_catalog() {
            return Err(Error::Unsupported);
        }
        self.inner
            .execute_catalog_bound(&operation.inner)
            .map_err(error)
    }
}

pub(crate) fn validate_catalog(
    statement: &CatalogStatement,
    version: LanguageVersion,
) -> Result<()> {
    let CatalogStatement::CreateTable {
        table,
        columns,
        primary_key,
    } = statement;
    if columns.len() > giggles_core::relational::MAX_COLUMNS
        || primary_key.len() > giggles_core::relational::MAX_COLUMNS
    {
        return Err(Error::Limit);
    }
    // Every name is checked before any definition strings are cloned.
    if !parser::semantic_identifier(table, version)
        || columns
            .iter()
            .any(|column| !parser::semantic_identifier(&column.name, version))
        || primary_key
            .iter()
            .any(|key| !parser::semantic_identifier(key, version))
    {
        return Err(Error::Bind("identifier".into()));
    }
    validate_size(statement)
}

fn lower_catalog_checked(
    statement: &CatalogStatement,
    version: LanguageVersion,
) -> Result<operations::CatalogRequest> {
    if !version.supports_catalog() {
        return Err(Error::Unsupported);
    }
    validate_catalog(statement, version)?;
    let CatalogStatement::CreateTable {
        table,
        columns,
        primary_key,
    } = statement;
    Ok(operations::CatalogRequest::CreateTable {
        table: table.clone(),
        columns: columns
            .iter()
            .map(|column| operations::ColumnSpec {
                name: column.name.clone(),
                data_type: match column.data_type {
                    CatalogType::Bool => giggles_core::relational::DataType::Boolean,
                    CatalogType::Int32 => giggles_core::relational::DataType::Integer,
                    CatalogType::Int64 => giggles_core::relational::DataType::BigInt,
                    CatalogType::Text => giggles_core::relational::DataType::Text,
                    CatalogType::Bytes => giggles_core::relational::DataType::Bytes,
                },
                not_null: !column.nullable,
            })
            .collect(),
        primary_key: primary_key.clone(),
    })
}

pub(crate) fn lower_checked(
    stmt: &Statement,
    version: LanguageVersion,
) -> Result<operations::Request> {
    // Check caller-supplied trees before serialization or recursive lowering.
    validate_depth(stmt)?;
    validate_size(stmt)?;
    validate_version(stmt, version)?;
    lower_statement(stmt, version)
}

pub(crate) fn validate_depth(stmt: &Statement) -> Result<()> {
    validate_statement_depth(stmt, &mut 0)
}
struct StatementParts<'a> {
    statement: &'a Statement,
    projection: Option<&'a [Projection]>,
    conflict: Option<(&'a [String], &'a ConflictAction)>,
}
// Admit only optional RETURN -> optional ON CONFLICT -> direct operation.
// Inspect at most two boxes, before serialization or recursive lowering.
fn statement_parts(statement: &Statement) -> Result<StatementParts<'_>> {
    let (statement, projection) = if let Statement::Returning {
        statement,
        projection,
    } = statement
    {
        (statement.as_ref(), Some(projection.as_slice()))
    } else {
        (statement, None)
    };
    let (statement, conflict) = if let Statement::OnConflict {
        statement,
        target,
        action,
    } = statement
    {
        if !matches!(
            statement.as_ref(),
            Statement::Insert { .. }
                | Statement::InsertExpressions { .. }
                | Statement::InsertMany { .. }
        ) {
            return Err(Error::Unsupported);
        }
        if target.len() > giggles_core::relational::MAX_COLUMNS {
            return Err(Error::Limit);
        }
        if let ConflictAction::Update { values, .. } = action
            && (values.is_empty() || values.len() > giggles_core::relational::MAX_COLUMNS)
        {
            return Err(Error::Bind("assignments".into()));
        }
        (statement.as_ref(), Some((target.as_slice(), action)))
    } else {
        (statement, None)
    };
    if matches!(
        statement,
        Statement::Returning { .. } | Statement::OnConflict { .. }
    ) || (projection.is_some()
        && !matches!(
            statement,
            Statement::Insert { .. }
                | Statement::InsertExpressions { .. }
                | Statement::InsertMany { .. }
                | Statement::Update { .. }
                | Statement::UpdateExpressions { .. }
                | Statement::Delete { .. }
        ))
    {
        return Err(Error::Unsupported);
    }
    if projection.is_some_and(|projection| projection.len() > giggles_core::relational::MAX_COLUMNS)
    {
        return Err(Error::Limit);
    }
    Ok(StatementParts {
        statement,
        projection,
        conflict,
    })
}
fn validate_statement_depth(stmt: &Statement, nodes: &mut usize) -> Result<()> {
    let parts = statement_parts(stmt)?;
    let mut pending = Vec::new();
    if let Some(projection) = parts.projection {
        pending.extend(projection.iter().map(|p| (&p.expression, 0)));
    }
    if let Some((_, ConflictAction::Update { guard, values })) = parts.conflict {
        pending.extend(guard.iter().map(|expr| (expr, 0)));
        pending.extend(values.iter().map(|(_, expr)| (expr, 0)));
    }
    let stmt = parts.statement;
    match stmt {
        Statement::Read {
            filter: Some(e), ..
        } => pending.push((e, 0)),
        Statement::Update { filter, guard, .. } | Statement::Delete { filter, guard, .. } => {
            pending.push((filter, 0));
            if let Some(e) = guard {
                pending.push((e, 0))
            }
        }
        Statement::UpdateExpressions {
            filter,
            guard,
            values,
            ..
        } => {
            if values.len() > giggles_core::relational::MAX_COLUMNS {
                return Err(Error::Bind("assignments".into()));
            }
            pending.push((filter, 0));
            pending.extend(guard.iter().map(|e| (e, 0)));
            pending.extend(values.iter().map(|(_, e)| (e, 0)));
        }
        Statement::ReadExpressions {
            filter, projection, ..
        } => {
            if projection.len() > giggles_core::relational::MAX_COLUMNS {
                return Err(Error::Limit);
            }
            pending.extend(filter.iter().map(|e| (e, 0)));
            pending.extend(projection.iter().map(|p| (&p.expression, 0)));
        }
        Statement::InsertExpressions { values, .. } => {
            if values.len() > giggles_core::relational::MAX_COLUMNS {
                return Err(Error::Bind("assignments".into()));
            }
            pending.extend(values.iter().map(|(_, expr)| (expr, 0)));
        }
        Statement::InsertMany { columns, rows, .. } => {
            // Check every collection and arity before gathering expression roots.
            if columns.len() > giggles_core::relational::MAX_COLUMNS || rows.len() > MAX_INSERT_ROWS
            {
                return Err(Error::Limit);
            }
            if columns.is_empty() {
                return Err(Error::Bind("columns".into()));
            }
            if rows.iter().any(|row| row.len() != columns.len()) {
                return Err(Error::Type);
            }
            let roots = rows.len().checked_mul(columns.len()).ok_or(Error::Limit)?;
            if nodes
                .checked_add(pending.len())
                .and_then(|n| n.checked_add(roots))
                .is_none_or(|n| n > MAX_EXPRESSION_NODES)
            {
                return Err(Error::Limit);
            }
            pending.extend(rows.iter().flatten().map(|expr| (expr, 0)));
        }
        _ => (),
    }
    validate_nodes(pending, nodes)
}
pub(crate) fn validate_call(function: ScalarFunction, count: usize) -> Result<()> {
    if count > MAX_FUNCTION_ARGUMENTS {
        return Err(Error::Limit);
    }
    if match function {
        ScalarFunction::Coalesce => count < 2,
        ScalarFunction::Abs | ScalarFunction::Length => count != 1,
    } {
        return Err(Error::Type);
    }
    Ok(())
}
pub(crate) fn validate_case(count: usize) -> Result<()> {
    if count > MAX_CASE_ARMS {
        return Err(Error::Limit);
    }
    if count == 0 {
        return Err(Error::Type);
    }
    Ok(())
}
fn validate_nodes(mut pending: Vec<(&Expr, usize)>, nodes: &mut usize) -> Result<()> {
    while let Some((e, d)) = pending.pop() {
        *nodes += 1;
        if d > MAX_DEPTH || *nodes > MAX_EXPRESSION_NODES {
            return Err(Error::Limit);
        }
        match e {
            Expr::Compare(a, _, b)
            | Expr::And(a, b)
            | Expr::Or(a, b)
            | Expr::Arithmetic(a, _, b) => {
                pending.push((a, d + 1));
                pending.push((b, d + 1))
            }
            Expr::IsNull(e, _) | Expr::Not(e) | Expr::Negate(e) => pending.push((e, d + 1)),
            Expr::Call {
                function,
                arguments,
            } => {
                validate_call(*function, arguments.len())?;
                pending.extend(arguments.iter().map(|argument| (argument, d + 1)));
            }
            Expr::Case {
                operand,
                arms,
                else_expr,
            } => {
                validate_case(arms.len())?;
                pending.extend(operand.iter().map(|expr| (expr.as_ref(), d + 1)));
                for (condition, result) in arms {
                    pending.push((condition, d + 1));
                    pending.push((result, d + 1));
                }
                pending.extend(else_expr.iter().map(|expr| (expr.as_ref(), d + 1)));
            }
            _ => (),
        }
    }
    Ok(())
}

pub(crate) fn validate_version(stmt: &Statement, version: LanguageVersion) -> Result<()> {
    let parts = statement_parts(stmt)?;
    if parts.projection.is_some() && !version.supports_return() {
        return Err(Error::Unsupported);
    }
    if parts.conflict.is_some() && !version.supports_upsert() {
        return Err(Error::Unsupported);
    }
    let stmt = parts.statement;
    if matches!(stmt, Statement::InsertMany { .. }) && !version.supports_insert_many() {
        return Err(Error::Unsupported);
    }
    if !version.supports_transactions() {
        match stmt {
            Statement::InsertExpressions { .. } => return Err(Error::Unsupported),
            Statement::Update {
                guard: Some(_),
                intent: Intent::Many { .. },
                ..
            }
            | Statement::UpdateExpressions {
                guard: Some(_),
                intent: Intent::Many { .. },
                ..
            }
            | Statement::Delete {
                guard: Some(_),
                cap: Some(_),
                ..
            } => return Err(Error::Unsupported),
            _ => (),
        }
    }
    if matches!(
        stmt,
        Statement::ReadExpressions { .. } | Statement::UpdateExpressions { .. }
    ) && !version.supports_computed()
    {
        return Err(Error::Unsupported);
    }
    let mut pending = Vec::new();
    if let Some(projection) = parts.projection {
        pending.extend(projection.iter().map(|p| &p.expression));
    }
    if let Some((_, ConflictAction::Update { guard, values })) = parts.conflict {
        pending.extend(guard.iter());
        pending.extend(values.iter().map(|(_, expr)| expr));
    }
    match stmt {
        Statement::Read { filter, .. } => pending.extend(filter.iter()),
        Statement::Update { filter, guard, .. } | Statement::Delete { filter, guard, .. } => {
            pending.push(filter);
            pending.extend(guard.iter());
        }
        Statement::Insert { .. } => (),
        Statement::ReadExpressions {
            filter, projection, ..
        } => {
            pending.extend(filter.iter());
            pending.extend(projection.iter().map(|p| &p.expression));
        }
        Statement::UpdateExpressions {
            filter,
            guard,
            values,
            ..
        } => {
            pending.push(filter);
            pending.extend(guard.iter());
            pending.extend(values.iter().map(|(_, expr)| expr));
        }
        Statement::InsertExpressions { values, .. } => {
            pending.extend(values.iter().map(|(_, expr)| expr))
        }
        Statement::InsertMany { rows, .. } => pending.extend(rows.iter().flatten()),
        Statement::Returning { .. } | Statement::OnConflict { .. } => {
            unreachable!("wrappers normalized")
        }
    }
    validate_expression_version(pending, version)
}

fn validate_expression_version(mut pending: Vec<&Expr>, version: LanguageVersion) -> Result<()> {
    while let Some(expr) = pending.pop() {
        match expr {
            Expr::ConflictField { .. } if !version.supports_upsert() => {
                return Err(Error::Unsupported);
            }
            Expr::Local(_) if !version.supports_transactions() => {
                return Err(Error::Unsupported);
            }
            Expr::Negate(_) | Expr::Arithmetic(..) if version == LanguageVersion::Embedded01 => {
                return Err(Error::Unsupported);
            }
            Expr::Compare(a, _, b)
            | Expr::And(a, b)
            | Expr::Or(a, b)
            | Expr::Arithmetic(a, _, b) => {
                pending.push(a);
                pending.push(b);
            }
            Expr::IsNull(expr, _) | Expr::Not(expr) | Expr::Negate(expr) => pending.push(expr),
            Expr::Call { arguments, .. } => {
                if !version.supports_scalar_functions() {
                    return Err(Error::Unsupported);
                }
                pending.extend(arguments);
            }
            Expr::Case {
                operand,
                arms,
                else_expr,
            } => {
                if !version.supports_case() {
                    return Err(Error::Unsupported);
                }
                pending.extend(operand.iter().map(Box::as_ref));
                for (condition, result) in arms {
                    pending.push(condition);
                    pending.push(result);
                }
                pending.extend(else_expr.iter().map(Box::as_ref));
            }
            Expr::Value(_) | Expr::Field(_) | Expr::Local(_) | Expr::ConflictField { .. } => (),
        }
    }
    Ok(())
}

// Enforce the existing serialized-AST bound without allocating an unbounded
// temporary buffer for a caller-supplied string or vector.
pub(crate) fn validate_size(stmt: &impl serde::Serialize) -> Result<()> {
    struct Budget(usize);
    impl Write for Budget {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0 = self
                .0
                .checked_sub(bytes.len())
                .ok_or_else(|| io::Error::other("native statement exceeds input bound"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    serde_json::to_writer(Budget(MAX_INPUT), stmt).map_err(|_| Error::Limit)
}

fn identifier(name: &str, version: LanguageVersion) -> Result<String> {
    if parser::semantic_identifier(name, version) {
        Ok(name.to_owned())
    } else {
        Err(Error::Bind("identifier".into()))
    }
}
fn local_reference(reference: &LocalRef, version: LanguageVersion) -> Result<operations::LocalRef> {
    Ok(match reference {
        LocalRef::Field { local, field } => operations::LocalRef::Field {
            local: identifier(local, version)?,
            field: identifier(field, version)?,
        },
        LocalRef::Status { local } => operations::LocalRef::Status {
            local: identifier(local, version)?,
        },
        LocalRef::Changed { local } => operations::LocalRef::Changed {
            local: identifier(local, version)?,
        },
        LocalRef::Count { local } => operations::LocalRef::Count {
            local: identifier(local, version)?,
        },
    })
}

pub(crate) fn validate_transaction(
    transaction: &Transaction,
    version: LanguageVersion,
) -> Result<()> {
    if !version.supports_transactions() {
        return Err(Error::Unsupported);
    }
    let count = transaction
        .steps
        .len()
        .checked_add(usize::from(transaction.emit.is_some()))
        .ok_or(Error::Limit)?;
    if count == 0 {
        return Err(Error::Bind("transaction".into()));
    }
    if count > MAX_TRANSACTION_ITEMS {
        return Err(Error::Limit);
    }
    let mut nodes = 0;
    let mut locals = 0;
    for step in &transaction.steps {
        match step {
            TransactionStep::Execute(statement) | TransactionStep::Let { statement, .. } => {
                validate_statement_depth(statement, &mut nodes)?;
                validate_version(statement, version)?;
                let statement = statement_parts(statement)?.statement;
                if matches!(
                    statement,
                    Statement::Update {
                        intent: Intent::Claim { .. },
                        ..
                    } | Statement::UpdateExpressions {
                        intent: Intent::Claim { .. },
                        ..
                    }
                ) {
                    return Err(Error::Unsupported);
                }
                if transaction.read_only
                    && !matches!(
                        statement,
                        Statement::Read { .. } | Statement::ReadExpressions { .. }
                    )
                {
                    return Err(Error::Unsupported);
                }
                if matches!(step, TransactionStep::Let { .. }) {
                    locals += 1;
                }
            }
            TransactionStep::Require(expr) => {
                validate_nodes(vec![(expr, 0)], &mut nodes)?;
                validate_expression_version(vec![expr], version)?;
            }
        }
    }
    if locals > MAX_TRANSACTION_LOCALS {
        return Err(Error::Limit);
    }
    if let Some(emit) = &transaction.emit {
        if emit.is_empty() {
            return Err(Error::Bind("emit".into()));
        }
        if emit.len() > MAX_EMIT_VALUES {
            return Err(Error::Limit);
        }
        for value in emit {
            if let EmitExpr::Scalar(expr) = value {
                validate_nodes(vec![(expr, 0)], &mut nodes)?;
                validate_expression_version(vec![expr], version)?;
            }
        }
    }
    validate_size(transaction)
}
pub(crate) fn lower_transaction_checked(
    transaction: &Transaction,
    version: LanguageVersion,
) -> Result<operations::Transaction> {
    validate_transaction(transaction, version)?;
    Ok(operations::Transaction {
        read_only: transaction.read_only,
        steps: transaction
            .steps
            .iter()
            .map(|step| {
                Ok(match step {
                    TransactionStep::Execute(statement) => {
                        operations::TransactionStep::Execute(lower_statement(statement, version)?)
                    }
                    TransactionStep::Let { name, statement } => operations::TransactionStep::Let {
                        name: identifier(name, version)?,
                        request: lower_statement(statement, version)?,
                    },
                    TransactionStep::Require(expr) => {
                        operations::TransactionStep::Require(expression(expr, version)?)
                    }
                })
            })
            .collect::<Result<_>>()?,
        emit: transaction
            .emit
            .as_ref()
            .map(|values| {
                values
                    .iter()
                    .map(|value| {
                        Ok(match value {
                            EmitExpr::Scalar(expr) => {
                                operations::EmitExpr::Scalar(expression(expr, version)?)
                            }
                            EmitExpr::Row(local) => {
                                operations::EmitExpr::Row(identifier(local, version)?)
                            }
                            EmitExpr::Rows(local) => {
                                operations::EmitExpr::Rows(identifier(local, version)?)
                            }
                        })
                    })
                    .collect::<Result<_>>()
            })
            .transpose()?,
    })
}
fn value(value: &ValueExpr) -> operations::ValueExpr {
    match value {
        ValueExpr::Literal(value) => operations::ValueExpr::Literal(value.clone()),
        ValueExpr::Parameter(name) => operations::ValueExpr::Parameter(name.clone()),
    }
}
fn compare(compare: Compare) -> operations::Compare {
    match compare {
        Compare::Eq => operations::Compare::Eq,
        Compare::Ne => operations::Compare::Ne,
        Compare::Lt => operations::Compare::Lt,
        Compare::Le => operations::Compare::Le,
        Compare::Gt => operations::Compare::Gt,
        Compare::Ge => operations::Compare::Ge,
    }
}
fn arithmetic(op: Arithmetic) -> operations::Arithmetic {
    match op {
        Arithmetic::Add => operations::Arithmetic::Add,
        Arithmetic::Subtract => operations::Arithmetic::Subtract,
        Arithmetic::Multiply => operations::Arithmetic::Multiply,
        Arithmetic::Divide => operations::Arithmetic::Divide,
        Arithmetic::Remainder => operations::Arithmetic::Remainder,
    }
}
fn expression(expr: &Expr, version: LanguageVersion) -> Result<operations::Expr> {
    Ok(match expr {
        Expr::Value(v) => operations::Expr::Value(value(v)),
        Expr::Field(name) => operations::Expr::Field(identifier(name, version)?),
        Expr::ConflictField { source, field } => operations::Expr::ConflictField {
            source: match source {
                ConflictSource::Existing => operations::ConflictSource::Existing,
                ConflictSource::Incoming => operations::ConflictSource::Incoming,
            },
            field: identifier(field, version)?,
        },
        Expr::Compare(left, op, right) => operations::Expr::Compare(
            Box::new(expression(left, version)?),
            compare(*op),
            Box::new(expression(right, version)?),
        ),
        Expr::IsNull(expr, negate) => {
            operations::Expr::IsNull(Box::new(expression(expr, version)?), *negate)
        }
        Expr::Not(expr) => operations::Expr::Not(Box::new(expression(expr, version)?)),
        Expr::And(left, right) => operations::Expr::And(
            Box::new(expression(left, version)?),
            Box::new(expression(right, version)?),
        ),
        Expr::Or(left, right) => operations::Expr::Or(
            Box::new(expression(left, version)?),
            Box::new(expression(right, version)?),
        ),
        Expr::Negate(expr) => operations::Expr::Negate(Box::new(expression(expr, version)?)),
        Expr::Arithmetic(left, op, right) => operations::Expr::Arithmetic(
            Box::new(expression(left, version)?),
            arithmetic(*op),
            Box::new(expression(right, version)?),
        ),
        Expr::Local(reference) => operations::Expr::Local(local_reference(reference, version)?),
        Expr::Call {
            function,
            arguments,
        } => operations::Expr::Call {
            function: match function {
                ScalarFunction::Coalesce => operations::ScalarFunction::Coalesce,
                ScalarFunction::Abs => operations::ScalarFunction::Abs,
                ScalarFunction::Length => operations::ScalarFunction::Length,
            },
            arguments: arguments
                .iter()
                .map(|expr| expression(expr, version))
                .collect::<Result<_>>()?,
        },
        Expr::Case {
            operand,
            arms,
            else_expr,
        } => operations::Expr::Case {
            operand: operand
                .as_deref()
                .map(|expr| expression(expr, version).map(Box::new))
                .transpose()?,
            arms: arms
                .iter()
                .map(|(condition, result)| {
                    Ok((
                        expression(condition, version)?,
                        expression(result, version)?,
                    ))
                })
                .collect::<Result<_>>()?,
            else_expr: else_expr
                .as_deref()
                .map(|expr| expression(expr, version).map(Box::new))
                .transpose()?,
        },
    })
}
fn sorts(order: &[Sort], version: LanguageVersion) -> Result<Vec<operations::Sort>> {
    order
        .iter()
        .map(|sort| {
            Ok(operations::Sort {
                field: identifier(&sort.field, version)?,
                descending: sort.descending,
            })
        })
        .collect()
}
fn assignments(
    values: &[(String, ValueExpr)],
    version: LanguageVersion,
) -> Result<Vec<(String, operations::ValueExpr)>> {
    values
        .iter()
        .map(|(name, v)| Ok((identifier(name, version)?, value(v))))
        .collect()
}
fn projections(
    items: &[Projection],
    version: LanguageVersion,
) -> Result<Vec<operations::Projection>> {
    items
        .iter()
        .map(|p| {
            Ok(operations::Projection {
                expression: expression(&p.expression, version)?,
                alias: p
                    .alias
                    .as_deref()
                    .map(|name| identifier(name, version))
                    .transpose()?,
            })
        })
        .collect()
}
fn intent(intent: &Intent, version: LanguageVersion) -> Result<operations::Intent> {
    Ok(match intent {
        Intent::One => operations::Intent::One,
        Intent::Many { cap } => operations::Intent::Many { cap: value(cap) },
        Intent::Claim { order } => operations::Intent::Claim {
            order: sorts(order, version)?,
        },
    })
}
fn lower_statement(stmt: &Statement, version: LanguageVersion) -> Result<operations::Request> {
    Ok(match stmt {
        Statement::Read {
            table,
            filter,
            order,
            limit,
            projection,
        } => operations::Request::Read {
            table: identifier(table, version)?,
            filter: filter
                .as_ref()
                .map(|expr| expression(expr, version))
                .transpose()?,
            order: sorts(order, version)?,
            limit: limit.as_ref().map(value),
            projection: projection
                .iter()
                .map(|name| identifier(name, version))
                .collect::<Result<_>>()?,
        },
        Statement::Insert { table, values } => operations::Request::Insert {
            table: identifier(table, version)?,
            values: assignments(values, version)?,
        },
        Statement::Update {
            table,
            filter,
            guard,
            values,
            intent: write_intent,
        } => operations::Request::Update {
            table: identifier(table, version)?,
            filter: expression(filter, version)?,
            guard: guard
                .as_ref()
                .map(|expr| expression(expr, version))
                .transpose()?,
            values: assignments(values, version)?,
            intent: intent(write_intent, version)?,
        },
        Statement::Delete {
            table,
            filter,
            guard,
            cap,
        } => operations::Request::Delete {
            table: identifier(table, version)?,
            filter: expression(filter, version)?,
            guard: guard
                .as_ref()
                .map(|expr| expression(expr, version))
                .transpose()?,
            cap: cap.as_ref().map(value),
        },
        Statement::UpdateExpressions {
            table,
            filter,
            guard,
            values,
            intent: write_intent,
        } => operations::Request::UpdateExpressions {
            table: identifier(table, version)?,
            filter: expression(filter, version)?,
            guard: guard
                .as_ref()
                .map(|expr| expression(expr, version))
                .transpose()?,
            values: values
                .iter()
                .map(|(name, expr)| Ok((identifier(name, version)?, expression(expr, version)?)))
                .collect::<Result<_>>()?,
            intent: intent(write_intent, version)?,
        },
        Statement::ReadExpressions {
            table,
            filter,
            order,
            limit,
            projection,
        } => operations::Request::ReadExpressions {
            table: identifier(table, version)?,
            filter: filter
                .as_ref()
                .map(|expr| expression(expr, version))
                .transpose()?,
            order: sorts(order, version)?,
            limit: limit.as_ref().map(value),
            projection: projections(projection, version)?,
        },
        Statement::OnConflict {
            statement,
            target,
            action,
        } => operations::Request::OnConflict {
            request: Box::new(lower_statement(statement, version)?),
            target: target
                .iter()
                .map(|name| identifier(name, version))
                .collect::<Result<_>>()?,
            action: match action {
                ConflictAction::Nothing => operations::ConflictAction::Nothing,
                ConflictAction::Update { guard, values } => operations::ConflictAction::Update {
                    guard: guard
                        .as_ref()
                        .map(|expr| expression(expr, version))
                        .transpose()?,
                    values: values
                        .iter()
                        .map(|(name, expr)| {
                            Ok((identifier(name, version)?, expression(expr, version)?))
                        })
                        .collect::<Result<_>>()?,
                },
            },
        },
        Statement::Returning {
            statement,
            projection,
        } => operations::Request::Returning {
            request: Box::new(lower_statement(statement, version)?),
            projection: projections(projection, version)?,
        },
        Statement::InsertExpressions { table, values } => operations::Request::InsertExpressions {
            table: identifier(table, version)?,
            values: values
                .iter()
                .map(|(name, expr)| Ok((identifier(name, version)?, expression(expr, version)?)))
                .collect::<Result<_>>()?,
        },
        Statement::InsertMany {
            table,
            columns,
            rows,
            cap,
        } => operations::Request::InsertMany {
            table: identifier(table, version)?,
            columns: columns
                .iter()
                .map(|name| identifier(name, version))
                .collect::<Result<_>>()?,
            rows: rows
                .iter()
                .map(|row| {
                    row.iter()
                        .map(|expr| expression(expr, version))
                        .collect::<Result<_>>()
                })
                .collect::<Result<_>>()?,
            cap: value(cap),
        },
    })
}
fn outcome(outcome: operations::Outcome) -> Outcome {
    let operations::Outcome {
        columns,
        rows,
        matched,
        affected,
        changed,
        complete,
        conflict_results,
    } = outcome;
    Outcome {
        columns,
        rows,
        matched,
        affected,
        changed,
        complete,
        conflict_results,
    }
}
fn error(error: operations::Error) -> Error {
    match error {
        operations::Error::Syntax { offset } => Error::Syntax { offset },
        operations::Error::Unsupported => Error::Unsupported,
        operations::Error::Limit => Error::Limit,
        operations::Error::Bind(message) => Error::Bind(message),
        operations::Error::Type => Error::Type,
        operations::Error::NotFound => Error::NotFound,
        operations::Error::AmbiguousMatch => Error::AmbiguousMatch,
        operations::Error::ConditionFailed => Error::ConditionFailed,
        operations::Error::TooManyMatches => Error::TooManyMatches,
        operations::Error::ClaimNotConsumed => Error::ClaimNotConsumed,
        operations::Error::Conflict => Error::Conflict,
        operations::Error::Core(message) => Error::Core(message),
        operations::Error::Unknown => Error::Unknown,
        operations::Error::Overflow => Error::Overflow,
        operations::Error::DivisionByZero => Error::DivisionByZero,
        operations::Error::RequirementFailed => Error::RequirementFailed,
        operations::Error::DuplicateTarget => Error::DuplicateTarget,
        operations::Error::PlanRequired => Error::PlanRequired,
    }
}
