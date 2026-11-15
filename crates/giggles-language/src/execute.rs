use crate::*;
use giggles_core::{Admin, operations};
use std::io::{self, Write};

/// Pure native data operation, ready for authorization before catalog binding.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Lowered {
    Operation {
        request: operations::Request,
        bindings: Bindings,
    },
    Transaction {
        transaction: operations::Transaction,
        bindings: Bindings,
    },
}

/// Parse and lower native data text without database access or Admin authority.
///
/// Uses the same profile validation and lowering as [`Engine`]. Bindings are
/// copied without substitution; catalog-dependent name and type binding remains
/// the operation engine's responsibility. Callers must authorize every lowered
/// table before passing the result to that engine. Catalog, inspection, explain,
/// and receipt/once requests are not admitted by this API.
pub fn lower_text(text: &str, bindings: &Bindings, version: LanguageVersion) -> Result<Lowered> {
    match parse_input_with_version(text, version)? {
        ParsedInput::Statement(statement) => {
            validate_parameter_admission(&statement, bindings, version)?;
            Ok(Lowered::Operation {
                request: lower_checked(&statement, version)?,
                bindings: bindings.clone(),
            })
        }
        ParsedInput::Transaction(transaction) => {
            validate_transaction_parameters(&transaction, bindings, version)?;
            Ok(Lowered::Transaction {
                transaction: lower_transaction_checked(&transaction, version)?,
                bindings: bindings.clone(),
            })
        }
        _ => Err(Error::Unsupported),
    }
}

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
        validate_parameter_admission(stmt, bindings, self.version)?;
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
        validate_parameter_admission(stmt, bindings, self.version)?;
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
        validate_transaction_parameters(transaction, bindings, self.version)?;
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
        validate_transaction_parameters(transaction, bindings, self.version)?;
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
    if let CatalogStatement::CreateIndex {
        table,
        name,
        columns,
    } = statement
    {
        if !version.supports_indexes() {
            return Err(Error::Unsupported);
        }
        if columns.len() > giggles_core::relational::MAX_COLUMNS {
            return Err(Error::Limit);
        }
        if std::iter::once(table)
            .chain(std::iter::once(name))
            .chain(columns.iter())
            .any(|name| !parser::semantic_identifier(name, version))
        {
            return Err(Error::Bind("identifier".into()));
        }
        return validate_size(statement);
    }
    let CatalogStatement::CreateTable {
        table,
        columns,
        primary_key,
    } = statement
    else {
        unreachable!()
    };
    if columns.len() > giggles_core::relational::MAX_COLUMNS
        || primary_key.len() > giggles_core::relational::MAX_COLUMNS
    {
        return Err(Error::Limit);
    }
    if version != LanguageVersion::Embedded016
        && columns.iter().any(|column| column.data_type == CatalogType::Numeric)
    {
        return Err(Error::Unsupported);
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
    if let CatalogStatement::CreateIndex {
        table,
        name,
        columns,
    } = statement
    {
        return Ok(operations::CatalogRequest::CreateIndex {
            table: table.clone(),
            name: name.clone(),
            columns: columns.clone(),
        });
    }
    let CatalogStatement::CreateTable {
        table,
        columns,
        primary_key,
    } = statement
    else {
        unreachable!()
    };
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
                    CatalogType::Numeric => giggles_core::relational::DataType::Numeric,
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
    // Check typed values at their AST locations, never by serialized key names.
    // Exhaustive matching makes new statement variants require an admission review.
    match stmt {
        Statement::Insert { values, .. } | Statement::Update { values, .. } => {
            for (_, value) in values {
                validate_native_value(value)?;
            }
            if let Statement::Update {
                intent: Intent::Many { cap },
                ..
            } = stmt
            {
                validate_native_value(cap)?;
            }
        }
        Statement::Read { limit, .. } | Statement::ReadExpressions { limit, .. } => {
            if let Some(value) = limit {
                validate_native_value(value)?;
            }
        }
        Statement::Delete { cap, .. } => {
            if let Some(value) = cap {
                validate_native_value(value)?;
            }
        }
        Statement::UpdateExpressions {
            intent: Intent::Many { cap },
            ..
        }
        | Statement::InsertMany { cap, .. } => validate_native_value(cap)?,
        Statement::UpdateExpressions { .. } | Statement::InsertExpressions { .. } => (),
        Statement::Returning { .. } | Statement::OnConflict { .. } => unreachable!(),
    }
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
            Expr::Value(value) => validate_native_value(value)?,
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
            Expr::Field(_) | Expr::Local(_) | Expr::ConflictField { .. } => (),
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

fn validate_native_value(value: &ValueExpr) -> Result<()> {
    match value {
        ValueExpr::Parameter(_) => Ok(()),
        ValueExpr::Literal(value) => match value {
            Value::Numeric(_)
            | Value::Float4(_)
            | Value::Float8(_)
            | Value::Date(_)
            | Value::Time(_)
            | Value::TimeTz(_)
            | Value::Timestamp(_)
            | Value::TimestampTz(_)
            | Value::Interval(_) => Err(Error::Unsupported),
            Value::Null | Value::Bool(_) | Value::Int(_) | Value::Text(_) | Value::Bytes(_) => {
                Ok(())
            }
        },
    }
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
pub(crate) fn error(error: operations::Error) -> Error {
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
        operations::Error::Duplicate => Error::Duplicate,
        operations::Error::Sql { sqlstate } => Error::Sql { sqlstate },
    }
}

// Only direct column assignments admit Numeric. Core binding then checks that
// each destination is NUMERIC and applies the shared SQL column coercion.
fn validate_parameter_admission(
    stmt: &Statement,
    bindings: &Bindings,
    version: LanguageVersion,
) -> Result<()> {
    if !matches!(version, LanguageVersion::Embedded015 | LanguageVersion::Embedded016)
        || !bindings.values().any(|v| matches!(v, Value::Numeric(_)))
    {
        return validate_numeric_bindings(bindings);
    }
    let mut remaining = stmt.clone();
    fn value(v: &mut ValueExpr, bindings: &Bindings) {
        if let ValueExpr::Parameter(name) = v
            && matches!(bindings.get(name), Some(Value::Numeric(_)))
        {
            *v = ValueExpr::Literal(Value::Null);
        }
    }
    fn scrub(stmt: &mut Statement, bindings: &Bindings) {
        match stmt {
            Statement::Insert { values, .. } | Statement::Update { values, .. } => {
                for (_, v) in values {
                    value(v, bindings);
                }
            }
            Statement::InsertExpressions { values, .. }
            | Statement::UpdateExpressions { values, .. } => {
                for (_, e) in values {
                    if let Expr::Value(v) = e {
                        value(v, bindings);
                    }
                }
            }
            Statement::InsertMany { rows, .. } => {
                for row in rows {
                    for e in row {
                        if let Expr::Value(v) = e {
                            value(v, bindings);
                        }
                    }
                }
            }
            Statement::Returning { statement, .. } => scrub(statement, bindings),
            Statement::OnConflict {
                statement, action, ..
            } => {
                scrub(statement, bindings);
                if let ConflictAction::Update { values, .. } = action {
                    for (_, e) in values {
                        if let Expr::Value(v) = e {
                            value(v, bindings);
                        }
                    }
                }
            }
            _ => (),
        }
    }
    scrub(&mut remaining, bindings);
    reject_numeric_references(&remaining, bindings)?;
    let other: Bindings = bindings
        .iter()
        .filter(|(_, v)| !matches!(v, Value::Numeric(_)))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    validate_numeric_bindings(&other)
}

// Inspect the public tree encoding after removing permitted assignment slots.
// This catches every other occurrence, including nested functions and emits.
fn reject_numeric_references(tree: &impl serde::Serialize, bindings: &Bindings) -> Result<()> {
    fn visit(v: &serde_json::Value, bindings: &Bindings) -> bool {
        match v {
            serde_json::Value::Object(map) => {
                map.get("Parameter")
                    .and_then(|v| v.as_str())
                    .is_some_and(|name| matches!(bindings.get(name), Some(Value::Numeric(_))))
                    || map.values().any(|v| visit(v, bindings))
            }
            serde_json::Value::Array(values) => values.iter().any(|v| visit(v, bindings)),
            _ => false,
        }
    }
    if visit(
        &serde_json::to_value(tree).map_err(|_| Error::Unsupported)?,
        bindings,
    ) {
        Err(Error::Unsupported)
    } else {
        Ok(())
    }
}

fn validate_transaction_parameters(
    transaction: &Transaction,
    bindings: &Bindings,
    version: LanguageVersion,
) -> Result<()> {
    if !matches!(version, LanguageVersion::Embedded015 | LanguageVersion::Embedded016)
        || !bindings.values().any(|v| matches!(v, Value::Numeric(_)))
    {
        return validate_numeric_bindings(bindings);
    }
    for step in &transaction.steps {
        match step {
            TransactionStep::Execute(stmt)
            | TransactionStep::Let {
                statement: stmt, ..
            } => validate_parameter_admission(stmt, bindings, version)?,
            TransactionStep::Require(expr) => reject_numeric_references(expr, bindings)?,
        }
    }
    reject_numeric_references(&transaction.emit, bindings)
}

pub(crate) fn validate_numeric_bindings(bindings: &Bindings) -> Result<()> {
    for v in bindings.values() {
        match v {
            Value::Numeric(_)
            | Value::Float4(_)
            | Value::Float8(_)
            | Value::Date(_)
            | Value::Time(_)
            | Value::TimeTz(_)
            | Value::Timestamp(_)
            | Value::TimestampTz(_)
            | Value::Interval(_) => {
                return Err(Error::Unsupported);
            }
            Value::Null | Value::Bool(_) | Value::Int(_) | Value::Text(_) | Value::Bytes(_) => (),
        }
    }
    Ok(())
}
#[cfg(test)]
mod lowering_tests {
    use super::*;

    #[test]
    fn operation_preserves_unbound_parameters_and_copies_values() {
        let mut bindings = Bindings::from([("id".into(), Value::Int(7))]);
        let lowered = lower_text(
            "insert missing set id=$id",
            &bindings,
            LanguageVersion::Embedded016,
        )
        .unwrap();
        bindings.insert("id".into(), Value::Int(8));
        assert_eq!(
            lowered,
            Lowered::Operation {
                request: operations::Request::Insert {
                    table: "missing".into(),
                    values: vec![("id".into(), operations::ValueExpr::Parameter("id".into()))],
                },
                bindings: Bindings::from([("id".into(), Value::Int(7))]),
            }
        );
    }

    #[test]
    fn transaction_lowers_all_steps_without_catalog_access() {
        let lowered = lower_text(
            "transaction { insert missing set id=1; delete missing where id=1 }",
            &Bindings::new(),
            LanguageVersion::Embedded016,
        )
        .unwrap();
        let Lowered::Transaction {
            transaction,
            bindings,
        } = lowered else {
            panic!("expected a transaction");
        };
        assert!(!transaction.read_only);
        assert!(bindings.is_empty());
        assert_eq!(transaction.steps.len(), 2);
        assert!(matches!(&transaction.steps[0], operations::TransactionStep::Execute(
            operations::Request::Insert { table, .. }) if table == "missing"));
        assert!(matches!(&transaction.steps[1], operations::TransactionStep::Execute(
            operations::Request::Delete { table, .. }) if table == "missing"));
    }

    #[test]
    fn non_data_requests_are_unsupported_and_syntax_keeps_offset() {
        for text in [
            "create table t (id int64,primary key(id))",
            "show tables",
            "describe table t",
            "explain t where id=1",
        ] {
            assert_eq!(
                lower_text(text, &Bindings::new(), LanguageVersion::Embedded016),
                Err(Error::Unsupported),
                "{text}"
            );
        }
        assert_eq!(
            lower_text(
                "insert t set id=",
                &Bindings::new(),
                LanguageVersion::Embedded016,
            ),
            Err(Error::Syntax { offset: 16 })
        );
        let bindings = Bindings::from([(
            "n".into(),
            Value::Numeric(giggles_core::relational::Numeric::parse("1.25").unwrap()),
        )]);
        for text in ["t where id=$n", "transaction { t where id=$n }"] {
            assert_eq!(
                lower_text(text, &bindings, LanguageVersion::Embedded016),
                Err(Error::Unsupported)
            );
        }
    }
}

#[cfg(test)]
mod numeric_profile_tests {
    use super::*;
    #[test]
    fn numeric_admission_is_limited_to_direct_assignments() {
        let bindings = Bindings::from([("n".into(), Value::Numeric(
            giggles_core::relational::Numeric::parse("1.25").unwrap()))]);
        for (text, allowed) in [
            ("insert t set n=$n", true),
            ("update t where id=1 set n=$n", true),
            ("update t where n=$n set n=1", false),
            ("update t where id=1 set n=abs($n)", false),
        ] {
            let stmt = parse_with_version(text, LanguageVersion::Embedded015).unwrap();
            assert_eq!(validate_parameter_admission(&stmt, &bindings, LanguageVersion::Embedded015).is_ok(), allowed);
        }
    }
    #[test]
    fn old_profiles_reject_new_inputs_but_transport_is_lossless() {
        use giggles_core::relational::{Float4, Float8, Numeric};
        let values = [
            Value::Numeric(Numeric::parse("9007199254740993.00100").unwrap()),
            Value::Float4(Float4::from_bits(0x7fc01234)),
            Value::Float8(Float8::from_bits(0x8000000000000000)),
        ];
        for v in values {
            assert!(matches!(
                validate_numeric_bindings(&Bindings::from([("x".into(), v.clone())])),
                Err(Error::Unsupported)
            ));
            assert!(matches!(
                validate_native_value(&ValueExpr::Literal(v.clone())),
                Err(Error::Unsupported)
            ));
            let bytes = serde_json::to_vec(&v).unwrap();
            let decoded: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(serde_json::to_vec(&decoded).unwrap(), bytes);
        }
        assert!(validate_size(&ValueExpr::Literal(Value::Text("Numeric".into()))).is_ok());
    }
}
