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
    admin: Admin,
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
    inner: BoundIntent,
    language_version: &'static str,
    profile: &'static str,
}
#[derive(Clone)]
enum BoundIntent {
    Native(operations::BoundOperation),
    Typed { fence: operations::BoundTransaction, description: operations::BoundDescription, intent: TypedIntent },
}
#[derive(Clone)]
enum TypedIntent { Query(TypedQuery), Insert(giggles_core::relational::TypedInsertSelect) }
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
        match &self.inner {
            BoundIntent::Native(inner) => inner.description(),
            BoundIntent::Typed { description, .. } => description,
        }
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
            inner: operations::Engine::new(admin.clone()),
            admin,
            version,
        }
    }
    /// Read one page in an explicit order ending in the full primary key.
    /// Reuse the returned `next` values as the exclusive `after` key.
    pub fn read_page_text(&self, text: &str, bindings: &Bindings) -> Result<Page> {
        self.read_page(&parse_page_with_version(text, self.version)?, bindings)
    }
    /// Read one bounded page; `next` supplies the exclusive key for the following call.
    pub fn read_page(&self, statement: &PageStatement, bindings: &Bindings) -> Result<Page> {
        if !self.version.supports_r2_wave2() || !matches!(statement.read, Statement::Read { .. }) {
            return Err(Error::Unsupported);
        }
        let request = lower_checked(&statement.read, self.version)?;
        validate_size(statement)?;
        reject_numeric_references(statement, bindings)?;
        validate_parameter_admission(&statement.read, bindings, self.version)?;
        if bindings.len() > MAX_BINDINGS { return Err(Error::Limit); }
        let mut read_bindings = bindings.clone();
        let mut read_names = std::collections::BTreeSet::new();
        let Statement::Read { filter, limit, .. } = &statement.read else { unreachable!() };
        let mut pending: Vec<&Expr> = filter.iter().collect();
        if let Some(ValueExpr::Parameter(n)) = limit { read_names.insert(n); }
        while let Some(expr) = pending.pop() {
            match expr {
                Expr::Value(ValueExpr::Parameter(n)) => { read_names.insert(n); }
                Expr::Compare(a, _, b) | Expr::Arithmetic(a, _, b) | Expr::And(a,b) | Expr::Or(a,b) => pending.extend([a.as_ref(), b.as_ref()]),
                Expr::IsNull(e, _) | Expr::Not(e) | Expr::Negate(e) => pending.push(e),
                Expr::Call { arguments, .. } => pending.extend(arguments),
                Expr::Case { operand, arms, else_expr } => {
                    pending.extend(operand.as_deref()); pending.extend(else_expr.as_deref());
                    for (a,b) in arms { pending.extend([a,b]); }
                }
                _ => (),
            }
        }
        let after = statement.after.as_ref().map(|values| values.iter().map(|v| {
            validate_native_value(v, self.version)?;
            let resolved = match v {
                ValueExpr::Literal(v) => v,
                ValueExpr::Parameter(n) => {
                    crate::once::parameter_name(n)?;
                    if !read_names.contains(n) { read_bindings.remove(n); }
                    bindings.get(n).ok_or_else(|| Error::Bind("parameter missing".into()))?
                }
            };
            Ok(resolved.clone())
        }).collect::<Result<Vec<_>>>()).transpose()?;
        self.inner.read_page(&request, after.as_deref(), &read_bindings)
            .map(|page| Page { outcome: outcome(page.outcome), next: page.next }).map_err(error)
    }
    pub fn execute_text(&self, text: &str, bindings: &Bindings) -> Result<Outcome> {
        self.execute(&parse_with_version(text, self.version)?, bindings)
    }
    pub fn execute(&self, stmt: &Statement, bindings: &Bindings) -> Result<Outcome> {
        validate_parameter_admission(stmt, bindings, self.version)?;
        if let Statement::Query(query) = stmt {
            validate_version(stmt, self.version)?;
            return self.execute_query(query, bindings);
        }
        if let Statement::InsertSelect { table, columns, source } = stmt {
            validate_version(stmt, self.version)?;
            return self.execute_insert_select(&mut self.admin.sql_session(), table, columns, source, bindings);
        }
        let request = lower_checked(stmt, self.version)?;
        let result = self.inner.execute(&request, bindings).map(outcome).map_err(error);
        if self.version.supports_r2_wave2()
            && matches!(&result, Err(Error::Sql { sqlstate }) if sqlstate == "42P01")
            && let Some(query) = read_as_query(stmt)
        {
            // Retrying is safe because reads do not write.
            return self.execute_query(&query, bindings);
        }
        result
    }

    fn execute_query(&self, query: &Query, bindings: &Bindings) -> Result<Outcome> {
        self.execute_query_in(&mut self.admin.sql_session(), query, bindings)
    }
    fn execute_query_in(&self, session: &mut giggles_core::SqlSession, query: &Query, bindings: &Bindings) -> Result<Outcome> {
        Self::execute_typed_query(session, lower_query(query, bindings, self.version)?)
    }
    fn execute_typed_query(session: &mut giggles_core::SqlSession, query: TypedQuery) -> Result<Outcome> {
        let result = match query {
            TypedQuery::Single(read) => session.execute_typed_read(*read, None),
            TypedQuery::Join(read) => session.execute_typed_join(*read, None),
        }.map_err(control_error)?;
        Ok(Outcome { columns: result.columns, matched: result.rows.len(),
            rows: result.rows, ..Outcome::default() })
    }

    fn execute_insert_select(&self, session: &mut giggles_core::SqlSession, table: &str,
        columns: &[String], source: &Query, bindings: &Bindings) -> Result<Outcome> {
        let insert = self.lower_insert_select(session, table, columns, source, bindings)?;
        Self::execute_typed_insert(session, insert)
    }
    fn lower_insert_select(&self, session: &mut giggles_core::SqlSession, table: &str,
        columns: &[String], source: &Query, bindings: &Bindings) -> Result<giggles_core::relational::TypedInsertSelect> {
        if !self.version.supports_r2_wave2() && table.contains('.') { return Err(Error::Unsupported); }
        let table = if self.version.supports_r2_wave2() { relation_name(table, self.version)? }
            else { identifier(table, self.version)? };
        let source = lower_query(source, bindings, self.version)?;
        let columns = if columns.is_empty() {
            let target = Self::typed_relation(session, &table)?;
            target.logical_columns().map(|c| c.name.clone()).collect()
        } else { columns.iter().map(|c| identifier(c, self.version)).collect::<Result<Vec<_>>>()? };
        Ok(giggles_core::relational::TypedInsertSelect { table, columns, source })
    }
    fn typed_relation(session: &giggles_core::SqlSession, name: &str) -> Result<giggles_core::relational::TableDef> {
        let key = if let Some((schema, name)) = name.split_once('.') {
            giggles_core::relational::relation_key(schema, name)
        } else { session.resolve_relation_reference(name).map_err(control_error)? };
        let (schema, name) = key.split_once('\0').unwrap_or(("public", &key));
        let metadata = session.catalog_metadata().map_err(control_error)?;
        metadata.tables.iter().chain(metadata.presentation.views.values().map(|v| &v.table))
            .find(|t| t.name == name
                && metadata.schemas.tables.get(&t.id).map_or("public", String::as_str) == schema)
            .cloned().ok_or_else(|| Error::Sql { sqlstate: "42P01".into() })
    }
    fn execute_typed_insert(session: &mut giggles_core::SqlSession, insert: giggles_core::relational::TypedInsertSelect) -> Result<Outcome> {
        let result = session.execute_typed_insert_select(insert, None).map_err(|e| match e {
            giggles_core::CoreError::Sql(_) => Error::Sql { sqlstate: giggles_core::sqlstate(&e).into() },
            other => control_error(other),
        })?;
        let affected = usize::try_from(result.affected).map_err(|_| Error::Limit)?;
        Ok(Outcome { matched: affected, affected, changed: affected, ..Outcome::default() })
    }
    fn describe_typed(session: &mut giggles_core::SqlSession, query: TypedQuery) -> Result<Vec<giggles_core::relational::ColumnDef>> {
        match query {
            TypedQuery::Single(read) => session.describe_typed_read(*read),
            TypedQuery::Join(read) => session.describe_typed_join(*read),
        }.map(|d| d.columns).map_err(control_error)
    }
    fn describe_rows(&self, session: &mut giggles_core::SqlSession, statement: &Statement, bindings: &Bindings)
        -> Result<Vec<giggles_core::relational::ColumnDef>> {
        let Statement::Query(query) = statement else {
            // Blocks cannot change the catalog; preparation supplies shape only.
            return Ok(self.inner.prepare_bound(&lower_checked(statement, self.version)?, bindings)
                .map_err(error)?.description().columns.clone());
        };
        let description = match lower_query(query, bindings, self.version)? {
            TypedQuery::Single(read) => session.describe_typed_read(*read),
            TypedQuery::Join(read) => session.describe_typed_join(*read),
        }.map_err(control_error)?;
        Ok(description.columns)
    }

    fn catalog_fence(&self) -> Result<operations::BoundTransaction> {
        self.inner.prepare_transaction_bound(&operations::Transaction { read_only: true,
            steps: vec![operations::TransactionStep::Require(operations::Expr::Value(
                operations::ValueExpr::Literal(Value::Bool(true))))], emit: None }, &Bindings::new()).map_err(error)
    }
    pub(crate) fn describe_query(&self, query: &Query, bindings: &Bindings) -> Result<Vec<giggles_core::relational::ColumnDef>> {
        if !self.version.supports_r2_wave2() { return Err(Error::Unsupported); }
        let statement = Statement::Query(Box::new(query.clone()));
        validate_parameter_admission(&statement, bindings, self.version)?;
        validate_version(&statement, self.version)?;
        Self::describe_typed(&mut self.admin.sql_session(), lower_query(query, bindings, self.version)?)
    }

    /// Parse and bind text with the exact supplied values, without executing it.
    pub fn prepare_bound_text(&self, text: &str, bindings: &Bindings) -> Result<BoundOperation> {
        self.prepare_bound(&parse_with_version(text, self.version)?, bindings)
    }

    /// Bind an immutable operation against a fresh committed catalog snapshot.
    ///
    /// All arguments are required now and are copied into the bound operation.
    /// Native preparation checks names and types. Typed INSERT FROM describes
    /// its source and freezes target columns; assignment and constraint checks
    /// occur at execution. Preparation does not scan rows, evaluate
    /// cardinality/guards, or make durable changes. Subsequent argument changes
    /// require preparing another handle. The existing [`Engine::execute`] API
    /// continues binding and executing in a single transaction.
    pub fn prepare_bound(&self, stmt: &Statement, bindings: &Bindings) -> Result<BoundOperation> {
        validate_parameter_admission(stmt, bindings, self.version)?;
        let inner = if matches!(stmt, Statement::Query(_) | Statement::InsertSelect { .. }) {
            if !self.version.supports_r2_wave2() { return Err(Error::Unsupported); }
            validate_version(stmt, self.version)?;
            let fence = self.catalog_fence()?;
            let mut session = self.admin.sql_session();
            let (table, intent, columns) = match stmt {
                Statement::Query(query) => {
                    let read = lower_query(query, bindings, self.version)?;
                    let columns = Self::describe_typed(&mut session, read.clone())?;
                    (query.source.table.clone(), TypedIntent::Query(read), columns)
                }
                Statement::InsertSelect { table, columns, source } => {
                    let insert = self.lower_insert_select(&mut session, table, columns, source, bindings)?;
                    Self::describe_typed(&mut session, insert.source.clone())?;
                    (table.clone(), TypedIntent::Insert(insert), vec![])
                }
                _ => unreachable!(),
            };
            let identity = fence.description();
            let table = Self::typed_relation(&session, &table)?;
            let description = operations::BoundDescription { contract_version: identity.contract_version,
                database_domain: identity.database_domain, catalog_epoch: identity.catalog_epoch, table, columns };
            self.inner.execute_transaction_bound(&fence).map_err(error)?;
            BoundIntent::Typed { fence, description, intent }
        } else {
            match self.inner.prepare_bound(&lower_checked(stmt, self.version)?, bindings).map_err(error) {
                Err(Error::Sql { sqlstate }) if self.version.supports_r2_wave2() && sqlstate == "42P01" => {
                    if let Some(query) = read_as_query(stmt) { return self.prepare_bound(&Statement::Query(Box::new(query)), bindings); }
                    return Err(Error::Sql { sqlstate });
                }
                result => BoundIntent::Native(result?),
            }
        };
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
        match &operation.inner {
            BoundIntent::Native(inner) => self.inner.execute_bound(inner).map(outcome).map_err(error),
            BoundIntent::Typed { fence, intent, .. } => {
                self.inner.execute_transaction_bound(fence).map_err(error)?;
                let mut session = self.admin.sql_session();
                session.begin_native().map_err(control_error)?;
                let result = (|| {
                    let result = match intent {
                        TypedIntent::Query(read) => Self::execute_typed_query(&mut session, read.clone()),
                        TypedIntent::Insert(insert) => Self::execute_typed_insert(&mut session, insert.clone()),
                    }?;
                    self.inner.execute_transaction_bound(fence).map_err(error)?;
                    if matches!(intent, TypedIntent::Query(_)) { session.rollback_native().map_err(control_error)?; }
                    else { session.commit_native().map_err(|e| match e {
                        giggles_core::CoreError::Storage(_) => Error::Unknown, other => control_error(other),
                    })?; }
                    Ok(result)
                })();
                if result.is_err() { let _ = session.rollback_native(); }
                result
            }
        }
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
    /// Version 0.21 skips aborted block steps until an existing savepoint recovers.
    /// A block ending aborted returns its original error and rolls back.
    pub fn execute_transaction(
        &self,
        transaction: &Transaction,
        bindings: &Bindings,
    ) -> Result<TransactionOutcome> {
        validate_transaction_parameters(transaction, bindings, self.version)?;
        if has_savepoints(transaction) || has_typed_steps(transaction) {
            validate_transaction(transaction, self.version)?;
            return self.execute_savepoint_transaction(transaction, bindings);
        }
        let transaction = lower_transaction_checked(transaction, self.version)?;
        self.inner
            .execute_transaction(&transaction, bindings)
            .map_err(error)
    }
    fn execute_savepoint_transaction(&self, block: &Transaction, bindings: &Bindings) -> Result<TransactionOutcome> {
        let mut session = self.admin.sql_session();
        let result = (|| {
            session.begin_native().map_err(control_error)?;
            let mut summaries = Vec::new();
            let mut pending: Option<Error> = None;
            let mut locals = std::collections::BTreeMap::new();
            for (item_index, step) in block.steps.iter().enumerate() {
                if pending.is_some() && !matches!(step, TransactionStep::RollbackTo(_)) {
                    if self.version.supports_r2_wave3() { continue; }
                    assert!(pending.is_none());
                }
                match step {
                    TransactionStep::Savepoint(name) => session.savepoint(name).map_err(control_error)?,
                    TransactionStep::RollbackTo(name) => {
                        if let Err(err) = session.rollback_to(name) {
                            if self.version.supports_r2_wave3() && pending.is_some() { continue; }
                            return Err(pending.take().unwrap_or_else(|| control_error(err)));
                        }
                        pending = None;
                    }
                    TransactionStep::Release(name) => session.release(name).map_err(control_error)?,
                    TransactionStep::Sequence { name, call } => {
                        let result = sequence_call(call, bindings, self.version)
                            .and_then(|call| session.execute_sequence_native(&call).map_err(error));
                        let value = match result {
                            Ok(value) => value,
                            Err(err) if !matches!(err, Error::Unknown)
                                && block.steps[item_index + 1..].iter().any(|s| matches!(s, TransactionStep::RollbackTo(_))) => {
                                pending = Some(err);
                                continue;
                            }
                            Err(err) => return Err(err),
                        };
                        if let Some(name) = name {
                            let column = match call {
                                SequenceStatement::Nextval(_) => "nextval", SequenceStatement::Currval(_) => "currval",
                                SequenceStatement::Setval { .. } => "setval",
                            };
                            locals.insert(name.clone(), operations::EmittedValue::Rows {
                                columns: vec![giggles_core::relational::ColumnDef { id: 0, name: column.into(),
                                    data_type: giggles_core::relational::DataType::BigInt, not_null: true }],
                                rows: vec![sequence_row(value)], complete: true });
                        }
                        summaries.push(operations::StepSummary { item_index, matched: 1, affected: 0, changed: 0,
                            complete: true, conflict_results: None });
                    }
                    TransactionStep::Execute(statement) | TransactionStep::Let { statement, .. } => {
                        let result = (|| {
                            let out = match statement {
                                Statement::Query(q) => self.execute_query_in(&mut session, q, bindings)?,
                                Statement::InsertSelect { table, columns, source } =>
                                    self.execute_insert_select(&mut session, table, columns, source, bindings)?,
                                _ => outcome(session.execute_native(&lower_checked(statement, self.version)?, bindings).map_err(error)?),
                            };
                            if let TransactionStep::Let { name, .. } = step {
                                if out.columns.is_empty() { return Err(Error::Unsupported); }
                                let columns = self.describe_rows(&mut session, statement, bindings)?;
                                locals.insert(name.clone(), operations::EmittedValue::Rows {
                                    columns, rows: out.rows.clone(), complete: true });
                            }
                            Ok(out)
                        })();
                        let out = match result {
                            Ok(out) => out,
                            Err(err) => {
                                if self.version.supports_r2_wave2() && !matches!(err, Error::Unknown)
                                    && (if self.version.supports_r2_wave3() {
                                        block.steps[item_index + 1..].iter().any(|s| matches!(s, TransactionStep::RollbackTo(_)))
                                    } else { matches!(block.steps.get(item_index + 1), Some(TransactionStep::RollbackTo(_))) }) {
                                    pending = Some(err);
                                    continue;
                                }
                                return Err(err);
                            }
                        };
                        summaries.push(operations::StepSummary { item_index, matched: out.matched,
                            affected: out.affected, changed: out.changed, complete: out.complete,
                            conflict_results: out.conflict_results });
                    }
                    _ => return Err(Error::Unsupported),
                }
            }
            if let Some(err) = pending { return Err(err); }
            let emitted = block.emit.iter().flatten().map(|emit| {
                let EmitExpr::Rows(name) = emit else { return Err(Error::Unsupported); };
                locals.get(name).cloned().ok_or(Error::Unsupported)
            }).collect::<Result<Vec<_>>>()?;
            if block.read_only { session.rollback_native().map_err(control_error)?; }
            else { session.commit_native().map_err(|e| match e {
                giggles_core::CoreError::Storage(_) => Error::Unknown,
                other => control_error(other),
            })?; }
            Ok(TransactionOutcome { summaries, emitted })
        })();
        if result.is_err() { let _ = session.rollback_native(); }
        result
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

    /// Profile 0.21 CREATE/DROP SEQUENCE text; see [`Self::execute_sequence_catalog`].
    pub fn execute_sequence_catalog_text(&self, text: &str) -> Result<Option<u64>> {
        self.execute_sequence_catalog(&parse_catalog_with_version(text, self.version)?)
    }
    /// Run CREATE/DROP SEQUENCE through SQL's sequence DDL in one commit and
    /// return the catalog epoch. `None`: DROP IF EXISTS found nothing (42P01/3F000)
    /// or CREATE IF NOT EXISTS found an existing relation (42P07).
    /// Table, view and constraint DDL use [`Self::execute_catalog`].
    pub fn execute_sequence_catalog(&self, statement: &CatalogStatement) -> Result<Option<u64>> {
        if !matches!(statement, CatalogStatement::CreateSequence { .. } | CatalogStatement::DropSequence { .. }) { return Err(Error::Unsupported); }
        let request = lower_catalog_checked(statement, self.version)?;
        match self.inner.execute_sequence_catalog(&request).map_err(error) {
            Err(Error::Sql { sqlstate }) if matches!(statement, CatalogStatement::DropSequence { if_exists: true, .. })
                && matches!(sqlstate.as_str(), "42P01" | "3F000") => Ok(None),
            Err(Error::Sql { sqlstate }) if matches!(statement, CatalogStatement::CreateSequence { if_not_exists: true, .. })
                && sqlstate == "42P07" => Ok(None),
            result => result.map(Some),
        }
    }
    /// Profile 0.21 `nextval(s)` / `currval(s)` / `setval(s, v [, is_called])` text.
    pub fn execute_sequence_text(&self, text: &str, bindings: &Bindings) -> Result<i64> {
        self.execute_sequence(&parse_sequence_with_version(text, self.version)?, bindings)
    }
    /// Each standalone call runs in a fresh session, like a new SQL connection,
    /// so `currval` gives 55000 here; use a transaction block step for
    /// session-scoped currval. nextval is never rolled back.
    pub fn execute_sequence(&self, statement: &SequenceStatement, bindings: &Bindings) -> Result<i64> {
        let call = sequence_call(statement, bindings, self.version)?;
        self.inner.execute_sequence(&call).map_err(error)
    }
    /// Execute one safe catalog operation in its own transaction.
    pub fn execute_catalog_text(&self, text: &str) -> Result<CatalogOutcome> {
        self.execute_catalog(&parse_catalog_with_version(text, self.version)?)
    }
    /// Execute a definition; use [`Self::execute_catalog_optional`] for DROP VIEW IF EXISTS or DROP CONSTRAINT IF EXISTS.
    pub fn execute_catalog(&self, statement: &CatalogStatement) -> Result<CatalogOutcome> {
        reject_optional_catalog(statement)?;
        self.execute_catalog_lowered(statement)
    }
    /// Profile 0.21 CREATE TABLE with foreign keys runs as CreateTable then one
    /// AddConstraint per key, in one core catalog commit.
    fn execute_catalog_lowered(&self, statement: &CatalogStatement) -> Result<CatalogOutcome> {
        let (request, constraints) = lower_catalog_parts(statement, self.version)?;
        if constraints.is_empty() { self.inner.execute_catalog(&request) }
        else { self.inner.execute_catalog_with_constraints(&request, &constraints) }.map_err(error)
    }
    /// Execute catalog text, returning None for a missing optional view or constraint target.
    pub fn execute_catalog_optional_text(&self, text: &str) -> Result<Option<CatalogOutcome>> {
        self.execute_catalog_optional(&parse_catalog_with_version(text, self.version)?)
    }
    /// Execute catalog DDL; optional views suppress SQL 42P01/3F000; optional constraints suppress 42704.
    pub fn execute_catalog_optional(&self, statement: &CatalogStatement) -> Result<Option<CatalogOutcome>> {
        if matches!(statement, CatalogStatement::CreateSequence { .. } | CatalogStatement::DropSequence { .. }) { return Err(Error::Unsupported); }
        match self.execute_catalog_lowered(statement) {
            Err(Error::Sql { sqlstate }) if matches!(statement, CatalogStatement::DropView { if_exists: true, .. })
                && matches!(sqlstate.as_str(), "42P01" | "3F000") => Ok(None),
            Err(Error::Sql { sqlstate }) if matches!(statement, CatalogStatement::AlterTable { action: AlterTableAction::DropConstraint { if_exists: true, .. }, .. })
                && sqlstate == "42704" => Ok(None),
            result => result.map(Some),
        }
    }
    /// Validate an immutable proposed definition without allocating catalog IDs.
    /// Population policy is checked against fresh state only at execution.
    pub fn prepare_catalog_bound_text(&self, text: &str) -> Result<BoundCatalogOperation> {
        self.prepare_catalog_bound(&parse_catalog_with_version(text, self.version)?)
    }
    /// Prepare a definition; DROP VIEW IF EXISTS and DROP CONSTRAINT IF EXISTS require [`Self::execute_catalog_optional`].
    /// CREATE TABLE with foreign keys has no single bound request and is Unsupported here.
    pub fn prepare_catalog_bound(
        &self,
        statement: &CatalogStatement,
    ) -> Result<BoundCatalogOperation> {
        reject_optional_catalog(statement)?;
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

fn lower_fk_action(action: ForeignKeyAction) -> operations::ForeignKeyAction {
    match action {
        ForeignKeyAction::NoAction => operations::ForeignKeyAction::NoAction,
        ForeignKeyAction::Restrict => operations::ForeignKeyAction::Restrict,
        ForeignKeyAction::Cascade => operations::ForeignKeyAction::Cascade,
        ForeignKeyAction::SetNull => operations::ForeignKeyAction::SetNull,
        ForeignKeyAction::SetDefault => operations::ForeignKeyAction::SetDefault,
    }
}

fn reject_optional_catalog(statement: &CatalogStatement) -> Result<()> {
    if matches!(statement, CatalogStatement::CreateSequence { .. } | CatalogStatement::DropSequence { .. } | CatalogStatement::DropView { if_exists: true, .. } | CatalogStatement::AlterTable { action: AlterTableAction::DropConstraint { if_exists: true, .. }, .. }) { Err(Error::Unsupported) } else { Ok(()) }
}

pub(crate) fn validate_catalog(
    statement: &CatalogStatement,
    version: LanguageVersion,
) -> Result<()> {
    if let CatalogStatement::CreateTableWith { table, columns, primary_key, constraints } = statement {
        // Only foreign keys are admitted; each is validated as its ALTER TABLE ADD CONSTRAINT.
        if !version.supports_r2_wave3() || constraints.iter().any(|c| !matches!(c.constraint, ConstraintSpec::ForeignKey { .. })) { return Err(Error::Unsupported); }
        if columns.len() > giggles_core::relational::MAX_COLUMNS || primary_key.len() > giggles_core::relational::MAX_COLUMNS
            || constraints.len() > giggles_core::relational::MAX_COLUMNS { return Err(Error::Limit); }
        validate_size(statement)?;
        for column in columns {
            if let Some(ColumnDefaultSpec::Nextval(name)) = &column.default
                && !parser::semantic_relation(name, version) { return Err(Error::Bind("identifier".into())); }
        }
        validate_catalog(&CatalogStatement::CreateTable { table: table.clone(), columns: columns.iter().map(|c| c.column.clone()).collect(), primary_key: primary_key.clone() }, version)?;
        for constraint in constraints { validate_catalog(&table_constraint_statement(table, constraint), version)?; }
        return Ok(());
    }
    if let CatalogStatement::CreateSequence { name, .. } | CatalogStatement::DropSequence { name, .. } = statement {
        if !version.supports_r2_wave3() { return Err(Error::Unsupported); }
        if !parser::semantic_relation(name, version) { return Err(Error::Bind("identifier".into())); }
        return validate_size(statement);
    }
    if let CatalogStatement::CreateView { name, .. } | CatalogStatement::DropView { name, .. } = statement {
        if !version.supports_r2_wave2() { return Err(Error::Unsupported); }
        if !parser::semantic_relation(name, version) { return Err(Error::Bind("identifier".into())); }
        if let CatalogStatement::CreateView { query, .. } = statement {
            validate_query(query, &mut 0, version)?;
            if query_has_parameter(query) { return Err(Error::Unsupported); }
        }
        return validate_size(statement);
    }
    if let CatalogStatement::AlterTable { table, action } = statement {
        if !version.supports_r2_native() { return Err(Error::Unsupported); }
        if matches!(action, AlterTableAction::AddConstraint { .. } | AlterTableAction::DropConstraint { .. }) && !version.supports_r2_wave2() { return Err(Error::Unsupported); }
        let names = match action {
            AlterTableAction::AddConstraint { name, .. } => name.iter().collect(),
            AlterTableAction::DropConstraint { name, .. } => vec![name],
            AlterTableAction::AddColumn(column) => vec![&column.name],
            AlterTableAction::DropColumn { name, .. } => vec![name],
            AlterTableAction::RenameColumn { from, to } => vec![from, to],
            AlterTableAction::RenameTable { to } => vec![to],
        };
        if !parser::semantic_relation(table, version) || names.iter().any(|n| !parser::semantic_identifier(n, version)) {
            return Err(Error::Bind("identifier".into()));
        }
        return validate_size(statement);
    }
    if let CatalogStatement::DropTable { table: name, .. } | CatalogStatement::DropIndex { name, .. } = statement {
        if !matches!(version, LanguageVersion::Embedded017 | LanguageVersion::Embedded018 | LanguageVersion::Embedded019 | LanguageVersion::Embedded020 | LanguageVersion::Embedded021) { return Err(Error::Unsupported); }
        if !parser::semantic_relation(name, version) { return Err(Error::Bind("identifier".into())); }
        return validate_size(statement);
    }
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
        if !parser::semantic_relation(table, version) || std::iter::once(name)
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
    if !matches!(version, LanguageVersion::Embedded016 | LanguageVersion::Embedded017 | LanguageVersion::Embedded018 | LanguageVersion::Embedded019 | LanguageVersion::Embedded020 | LanguageVersion::Embedded021)
        && columns.iter().any(|column| column.data_type == CatalogType::Numeric)
    {
        return Err(Error::Unsupported);
    }
    // Every name is checked before any definition strings are cloned.
    if !parser::semantic_relation(table, version)
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

fn table_constraint_statement(table: &str, constraint: &TableConstraint) -> CatalogStatement {
    CatalogStatement::AlterTable { table: table.to_owned(), action: AlterTableAction::AddConstraint {
        name: constraint.name.clone(), constraint: constraint.constraint.clone() } }
}

/// A single catalog request; CREATE TABLE with foreign keys has none (Unsupported).
fn lower_catalog_checked(
    statement: &CatalogStatement,
    version: LanguageVersion,
) -> Result<operations::CatalogRequest> {
    let (request, constraints) = lower_catalog_parts(statement, version)?;
    if !constraints.is_empty() { return Err(Error::Unsupported); }
    Ok(request)
}

/// The catalog request, plus the AddConstraint requests that must follow it in
/// the same commit (CREATE TABLE foreign keys; core's CreateTable carries none).
fn lower_catalog_parts(
    statement: &CatalogStatement,
    version: LanguageVersion,
) -> Result<(operations::CatalogRequest, Vec<operations::CatalogRequest>)> {
    let request = lower_catalog_single(statement, version)?;
    let CatalogStatement::CreateTableWith { table, constraints, .. } = statement else { return Ok((request, Vec::new())) };
    let constraints = constraints.iter()
        .map(|constraint| lower_catalog_single(&table_constraint_statement(table, constraint), version))
        .collect::<Result<Vec<_>>>()?;
    Ok((request, constraints))
}

fn lower_catalog_single(
    statement: &CatalogStatement,
    version: LanguageVersion,
) -> Result<operations::CatalogRequest> {
    if !version.supports_catalog() {
        return Err(Error::Unsupported);
    }
    validate_catalog(statement, version)?;
    match statement {
        CatalogStatement::CreateTableWith { table, columns, primary_key, .. } => return Ok(operations::CatalogRequest::CreateTable {
            table: table.clone(), primary_key: primary_key.clone(), columns: columns.iter().map(|c| {
                let mut column = lower_column(&c.column);
                column.default = c.default.as_ref().map(|d| match d {
                    ColumnDefaultSpec::Value(v) => operations::ColumnDefault::Value(v.clone()),
                    ColumnDefaultSpec::Nextval(n) => operations::ColumnDefault::Nextval(n.clone()),
                });
                column.generated = c.generated.map(|g| match g {
                    ColumnGenerated::IdentityAlways => operations::Generated::IdentityAlways,
                    ColumnGenerated::IdentityByDefault => operations::Generated::IdentityByDefault,
                    ColumnGenerated::Serial => operations::Generated::Serial,
                });
                column
            }).collect(),
        }),
        CatalogStatement::CreateView { name, query } => return Ok(operations::CatalogRequest::CreateView {
            name: name.clone(), read: match lower_query(query, &Bindings::default(), version)? {
                TypedQuery::Single(read) => operations::ViewRead::Single(read),
                TypedQuery::Join(read) => operations::ViewRead::Two(read),
            },
        }),
        CatalogStatement::DropView { name, cascade, .. } => return Ok(operations::CatalogRequest::DropView { name: name.clone(), cascade: *cascade }),
        CatalogStatement::CreateSequence { name, .. } => return Ok(operations::CatalogRequest::CreateSequence { name: name.clone() }),
        CatalogStatement::DropSequence { name, cascade, .. } => return Ok(operations::CatalogRequest::DropSequence { name: name.clone(), cascade: *cascade }),
        CatalogStatement::AlterTable { table, action: AlterTableAction::AddConstraint { name, constraint } } => return Ok(operations::CatalogRequest::AddConstraint {
            table: table.clone(), name: name.clone(), constraint: match constraint {
                ConstraintSpec::Unique { columns } => operations::ConstraintSpec::Unique { columns: columns.clone() },
                ConstraintSpec::Check(expr) => operations::ConstraintSpec::Check { expression: expression(expr, version)? },
                ConstraintSpec::ForeignKey { columns, ref_table, ref_columns, on_delete, on_update } => operations::ConstraintSpec::ForeignKey {
                    columns: columns.clone(), ref_table: ref_table.clone(), ref_columns: ref_columns.clone(),
                    on_delete: lower_fk_action(*on_delete), on_update: lower_fk_action(*on_update),
                },
            },
        }),
        CatalogStatement::AlterTable { table, action: AlterTableAction::DropConstraint { name, cascade, .. } } => return Ok(operations::CatalogRequest::DropConstraint { table: table.clone(), name: name.clone(), cascade: *cascade }),
        CatalogStatement::AlterTable { table, action } => return Ok(operations::CatalogRequest::AlterTable {
            table: table.clone(), action: match action {
                AlterTableAction::AddConstraint { .. } | AlterTableAction::DropConstraint { .. } => unreachable!(),
                AlterTableAction::AddColumn(column) => operations::AlterTableAction::AddColumn { column: lower_column(column) },
                AlterTableAction::DropColumn { name, cascade } => operations::AlterTableAction::DropColumn { name: name.clone(), cascade: *cascade },
                AlterTableAction::RenameColumn { from, to } => operations::AlterTableAction::RenameColumn { from: from.clone(), to: to.clone() },
                AlterTableAction::RenameTable { to } => operations::AlterTableAction::RenameTable { to: to.clone() },
            },
        }),
        CatalogStatement::DropTable { table, cascade } => return Ok(operations::CatalogRequest::DropTable { table: table.clone(), cascade: *cascade }),
        CatalogStatement::DropIndex { name, cascade } => return Ok(operations::CatalogRequest::DropIndex { name: name.clone(), cascade: *cascade }),
        _ => {}
    }
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
            .map(lower_column)
            .collect(),
        primary_key: primary_key.clone(),
    })
}

fn lower_column(column: &ColumnSpec) -> operations::ColumnSpec {
    operations::ColumnSpec {
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
        default: None,
        generated: None,
    }
}

pub(crate) fn lower_checked(
    stmt: &Statement,
    version: LanguageVersion,
) -> Result<operations::Request> {
    // Check caller-supplied trees before serialization or recursive lowering.
    validate_depth_for(stmt, version)?;
    if matches!(statement_parts(stmt)?.statement, Statement::InsertMany { .. }) {
        validate_size_within(stmt, MAX_INSERT_TREE_BYTES)?;
    } else {
        validate_size(stmt)?;
    }
    validate_version(stmt, version)?;
    lower_statement(stmt, version)
}

pub(crate) fn validate_depth(stmt: &Statement) -> Result<()> {
    validate_depth_for(stmt, LanguageVersion::Embedded018)
}
fn validate_depth_for(stmt: &Statement, version: LanguageVersion) -> Result<()> {
    validate_statement_depth(stmt, &mut 0, version)
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
fn validate_query(query: &Query, nodes: &mut usize, version: LanguageVersion) -> Result<()> {
    enum Node<'a> { Query(&'a Query), Predicate(&'a QueryPredicate), Expr(&'a QueryExpr), Value(&'a ValueExpr) }
    let mut pending = vec![(Node::Query(query), 0)];
    while let Some((node, depth)) = pending.pop() {
        *nodes += 1;
        if depth > MAX_DEPTH || *nodes > MAX_EXPRESSION_NODES { return Err(Error::Limit); }
        let d = depth + 1;
        match node {
            Node::Query(q) => {
                let size = q.group_by.len().saturating_add(q.order.len()).saturating_add(q.projection.len());
                if size > MAX_EXPRESSION_NODES || pending.len().saturating_add(size) > MAX_EXPRESSION_NODES { return Err(Error::Limit); }
                *nodes += 1 + 2 * usize::from(q.join.is_some()) + q.order.len() + q.projection.len();
                if *nodes > MAX_EXPRESSION_NODES { return Err(Error::Limit); }
                if let Some(join) = &q.join { pending.push((Node::Predicate(&join.on), depth)); }
                pending.extend(q.filter.iter().chain(q.having.iter()).map(|p| (Node::Predicate(p), depth)));
                pending.extend(q.group_by.iter().chain(q.order.iter().map(|o| &o.expression)).chain(q.projection.iter().map(|p| &p.expression)).map(|e| (Node::Expr(e), depth)));
                pending.extend(q.limit.iter().map(|v| (Node::Value(v), depth)));
            }
            Node::Predicate(p) => match p {
                QueryPredicate::Compare(a, _, b) => { pending.push((Node::Expr(a), d)); pending.push((Node::Expr(b), d)); }
                QueryPredicate::IsNull(e, _) => pending.push((Node::Expr(e), d)),
                QueryPredicate::Not(p) => pending.push((Node::Predicate(p), d)),
                QueryPredicate::And(a, b) | QueryPredicate::Or(a, b) => { pending.push((Node::Predicate(a), d)); pending.push((Node::Predicate(b), d)); }
                QueryPredicate::In { expr, subquery, .. } => { pending.push((Node::Expr(expr), d)); pending.push((Node::Query(subquery), d)); }
                QueryPredicate::Exists { subquery, .. } => pending.push((Node::Query(subquery), d)),
            },
            Node::Expr(QueryExpr::Value(v)) | Node::Value(v) => validate_native_value(v, version)?,
            Node::Expr(_) => (),
        }
    }
    Ok(())
}
fn validate_statement_depth(stmt: &Statement, nodes: &mut usize, version: LanguageVersion) -> Result<()> {
    let parts = statement_parts(stmt)?;
    match parts.statement {
        Statement::Query(q) | Statement::InsertSelect { source: q, .. } => return validate_query(q, nodes, version),
        _ => (),
    }
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
                validate_native_value(value, version)?;
            }
            if let Statement::Update {
                intent: Intent::Many { cap },
                ..
            } = stmt
            {
                validate_native_value(cap, version)?;
            }
        }
        Statement::Read { limit, .. } | Statement::ReadExpressions { limit, .. } => {
            if let Some(value) = limit {
                validate_native_value(value, version)?;
            }
        }
        Statement::Delete { cap, .. } => {
            if let Some(value) = cap {
                validate_native_value(value, version)?;
            }
        }
        Statement::UpdateExpressions {
            intent: Intent::Many { cap },
            ..
        }
        | Statement::InsertMany { cap, .. } => validate_native_value(cap, version)?,
        Statement::UpdateExpressions { .. } | Statement::InsertExpressions { .. } => (),
        Statement::Query(_) | Statement::InsertSelect { .. } => unreachable!(),
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
            // Row counts are engine-specific and admitted by core at bind time.
            if columns.len() > giggles_core::relational::MAX_COLUMNS || rows.len() > MAX_INSERT_TOKENS
            {
                return Err(Error::Limit);
            }
            if columns.is_empty() {
                return Err(Error::Bind("columns".into()));
            }
            if rows.iter().any(|row| row.len() != columns.len()) {
                return Err(Error::Type);
            }
            // Tuples are flat, as SQL VALUES: each tuple has its own node budget.
            for row in rows {
                validate_nodes(row.iter().map(|expr| (expr, 0)).collect(), &mut 0, version)?;
            }
        }
        _ => (),
    }
    validate_nodes(pending, nodes, version)
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
fn validate_nodes(mut pending: Vec<(&Expr, usize)>, nodes: &mut usize, version: LanguageVersion) -> Result<()> {
    while let Some((e, d)) = pending.pop() {
        *nodes += 1;
        if d > MAX_DEPTH || *nodes > MAX_EXPRESSION_NODES {
            return Err(Error::Limit);
        }
        match e {
            Expr::Value(value) => validate_native_value(value, version)?,
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
    if matches!(stmt, Statement::Query(_) | Statement::InsertSelect { .. }) {
        return if version.supports_r2_native() { Ok(()) } else { Err(Error::Unsupported) };
    }
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
        Statement::Query(_) | Statement::InsertSelect { .. } => unreachable!(),
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
    validate_size_within(stmt, MAX_INPUT)
}
fn validate_size_within(stmt: &impl serde::Serialize, budget: usize) -> Result<()> {
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
    serde_json::to_writer(Budget(budget), stmt).map_err(|_| Error::Limit)
}

fn validate_native_value(value: &ValueExpr, version: LanguageVersion) -> Result<()> {
    match value {
        ValueExpr::Parameter(_) => Ok(()),
        ValueExpr::Literal(value) if version.supports_r2_native() && typed_parameter(value) => Ok(()),
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
fn relation_name(name: &str, version: LanguageVersion) -> Result<String> {
    if parser::semantic_relation(name, version) {
        Ok(name.to_owned())
    } else {
        Err(Error::Bind("identifier".into()))
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

pub(crate) fn validate_sequence(statement: &SequenceStatement, version: LanguageVersion) -> Result<()> {
    if !version.supports_r2_wave3() { return Err(Error::Unsupported); }
    let (SequenceStatement::Nextval(name) | SequenceStatement::Currval(name) | SequenceStatement::Setval { name, .. }) = statement;
    if !parser::semantic_relation(name, version) { return Err(Error::Bind("identifier".into())); }
    if let SequenceStatement::Setval { value, is_called, .. } = statement {
        for value in std::iter::once(value).chain(is_called) {
            validate_native_value(value, version)?;
            if let ValueExpr::Parameter(name) = value { crate::once::parameter_name(name)?; }
        }
    }
    validate_size(statement)
}
fn sequence_call(statement: &SequenceStatement, bindings: &Bindings, version: LanguageVersion) -> Result<operations::SequenceCall> {
    validate_sequence(statement, version)?;
    if bindings.len() > MAX_BINDINGS { return Err(Error::Limit); }
    let resolve = |value: &ValueExpr| -> Result<Value> {
        match value {
            ValueExpr::Literal(value) => Ok(value.clone()),
            ValueExpr::Parameter(name) => bindings.get(name).cloned().ok_or_else(|| Error::Bind("parameter missing".into())),
        }
    };
    Ok(match statement {
        SequenceStatement::Nextval(name) => operations::SequenceCall::Nextval(name.clone()),
        SequenceStatement::Currval(name) => operations::SequenceCall::Currval(name.clone()),
        SequenceStatement::Setval { name, value, is_called } => operations::SequenceCall::Setval {
            name: name.clone(),
            value: match resolve(value)? { Value::Int(v) => v, _ => return Err(Error::Type) },
            is_called: is_called.as_ref().map(|v| match resolve(v)? { Value::Bool(b) => Ok(b), _ => Err(Error::Type) }).transpose()?,
        },
    })
}
pub(crate) fn has_savepoints(block: &Transaction) -> bool {
    block.steps.iter().any(|step| matches!(step,
        TransactionStep::Savepoint(_) | TransactionStep::RollbackTo(_) | TransactionStep::Release(_)))
}
fn has_typed_steps(block: &Transaction) -> bool {
    block.steps.iter().any(|step| match step {
        TransactionStep::Execute(s) | TransactionStep::Let { statement: s, .. } =>
            matches!(s, Statement::Query(_) | Statement::InsertSelect { .. }),
        TransactionStep::Sequence { .. } => true,
        _ => false,
    })
}
fn contains_local(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Object(map) => map.contains_key("Local") || map.values().any(contains_local),
        serde_json::Value::Array(values) => values.iter().any(contains_local),
        _ => false,
    }
}
fn control_error(e: giggles_core::CoreError) -> Error {
    use giggles_core::{CoreError, relational::SqlError};
    error(match e {
        CoreError::Sql(SqlError::Conflict) => operations::Error::Conflict,
        CoreError::Sql(SqlError::Duplicate) => operations::Error::Duplicate,
        CoreError::Sql(SqlError::Overflow) => operations::Error::Overflow,
        CoreError::Sql(_) => operations::Error::Sql { sqlstate: giggles_core::sqlstate(&e).into() },
        _ => operations::Error::Core(e.to_string()),
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
    let typed = has_typed_steps(transaction);
    if typed && !version.supports_r2_wave2() { return Err(Error::Unsupported); }
    let savepoints = has_savepoints(transaction);
    if savepoints && !typed && (!version.supports_r2_native()
        || (!version.supports_r2_wave3() && (transaction.emit.is_some()
            || transaction.steps.iter().any(|step| matches!(step, TransactionStep::Let { .. }))))
        || transaction.steps.iter().any(|step| matches!(step, TransactionStep::Require(_)))) {
        return Err(Error::Unsupported);
    }
    let mut nodes = 0;
    let mut locals = 0;
    for step in &transaction.steps {
        match step {
            TransactionStep::Execute(statement) | TransactionStep::Let { statement, .. } => {
                validate_statement_depth(statement, &mut nodes, version)?;
                validate_version(statement, version)?;
                if typed && contains_local(&serde_json::to_value(statement).map_err(|_| Error::Unsupported)?) {
                    return Err(Error::Unsupported);
                }
                if typed && let TransactionStep::Let { name, .. } = step { identifier(name, version)?; }
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
                        Statement::Read { .. } | Statement::ReadExpressions { .. } | Statement::Query(_)
                    )
                {
                    return Err(Error::Unsupported);
                }
                if matches!(step, TransactionStep::Let { .. }) {
                    locals += 1;
                }
            }
            TransactionStep::Savepoint(name) | TransactionStep::RollbackTo(name) | TransactionStep::Release(name) => {
                identifier(name, version)?;
            }
            TransactionStep::Sequence { name, call } => {
                validate_sequence(call, version)?;
                if transaction.read_only && !matches!(call, SequenceStatement::Currval(_)) { return Err(Error::Unsupported); }
                if let Some(name) = name { identifier(name, version)?; locals += 1; }
            }
            TransactionStep::Require(expr) => {
                if typed { return Err(Error::Unsupported); }
                validate_nodes(vec![(expr, 0)], &mut nodes, version)?;
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
            if typed {
                let EmitExpr::Rows(name) = value else { return Err(Error::Unsupported); };
                identifier(name, version)?;
            }
            if let EmitExpr::Scalar(expr) = value {
                validate_nodes(vec![(expr, 0)], &mut nodes, version)?;
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
    if has_savepoints(transaction) || has_typed_steps(transaction) { return Err(Error::Unsupported); }
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
                    _ => return Err(Error::Unsupported),
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
        Statement::Query(_) | Statement::InsertSelect { .. } => return Err(Error::Unsupported),
        Statement::Read {
            table,
            filter,
            order,
            limit,
            projection,
        } => operations::Request::Read {
            table: relation_name(table, version)?,
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
            table: relation_name(table, version)?,
            values: assignments(values, version)?,
        },
        Statement::Update {
            table,
            filter,
            guard,
            values,
            intent: write_intent,
        } => operations::Request::Update {
            table: relation_name(table, version)?,
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
            table: relation_name(table, version)?,
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
            table: relation_name(table, version)?,
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
            table: relation_name(table, version)?,
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
            table: relation_name(table, version)?,
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
            table: relation_name(table, version)?,
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
fn read_as_query(stmt: &Statement) -> Option<Query> {
    fn field(name: &str) -> QueryExpr {
        QueryExpr::Column { source: None, column: name.into() }
    }
    fn expr(e: &Expr) -> Option<QueryExpr> {
        match e {
            Expr::Field(name) => Some(field(name)),
            Expr::Value(value) => Some(QueryExpr::Value(value.clone())),
            _ => None,
        }
    }
    fn predicate(e: &Expr) -> Option<QueryPredicate> {
        Some(match e {
            Expr::Compare(a, op, b) => QueryPredicate::Compare(expr(a)?, *op, expr(b)?),
            Expr::IsNull(e, negated) => QueryPredicate::IsNull(expr(e)?, *negated),
            Expr::Not(e) => QueryPredicate::Not(Box::new(predicate(e)?)),
            Expr::And(a, b) => QueryPredicate::And(Box::new(predicate(a)?), Box::new(predicate(b)?)),
            Expr::Or(a, b) => QueryPredicate::Or(Box::new(predicate(a)?), Box::new(predicate(b)?)),
            _ => return None,
        })
    }
    let (table, filter, order, limit, projection) = match stmt {
        Statement::Read { table, filter, order, limit, projection } =>
            (table, filter, order, limit, projection.iter().map(|name| QueryProjection {
                expression: field(name), alias: None }).collect::<Vec<_>>()),
        Statement::ReadExpressions { table, filter, order, limit, projection } => {
            let projection = projection.iter().map(|p| match &p.expression {
                Expr::Field(name) => Some(QueryProjection { expression: field(name), alias: p.alias.clone() }),
                _ => None,
            }).collect::<Option<Vec<_>>>()?;
            (table, filter, order, limit, projection)
        }
        _ => return None,
    };
    if projection.is_empty() { return None; }
    let filter = match filter { Some(e) => Some(predicate(e)?), None => None };
    Some(Query { source: QuerySource { table: table.clone(), alias: None }, join: None,
        filter, group_by: vec![], having: None, limit: limit.clone(), projection,
        order: order.iter().map(|s| QueryOrder { expression: field(&s.field), descending: s.descending }).collect() })
}
use giggles_core::relational::TypedRead as TypedQuery;
fn query_has_parameter(query: &Query) -> bool {
    fn expr(e: &QueryExpr) -> bool { matches!(e, QueryExpr::Value(ValueExpr::Parameter(_))) }
    fn predicate(p: &QueryPredicate) -> bool {
        match p {
            QueryPredicate::Compare(a, _, b) => expr(a) || expr(b),
            QueryPredicate::IsNull(e, _) => expr(e),
            QueryPredicate::Not(p) => predicate(p),
            QueryPredicate::And(a, b) | QueryPredicate::Or(a, b) => predicate(a) || predicate(b),
            QueryPredicate::In { expr: e, subquery, .. } => expr(e) || query_has_parameter(subquery),
            QueryPredicate::Exists { subquery, .. } => query_has_parameter(subquery),
        }
    }
    matches!(query.limit, Some(ValueExpr::Parameter(_)))
        || query.projection.iter().any(|p| expr(&p.expression)) || query.group_by.iter().any(expr)
        || query.order.iter().any(|o| expr(&o.expression)) || query.filter.as_ref().is_some_and(predicate)
        || query.having.as_ref().is_some_and(predicate) || query.join.as_ref().is_some_and(|j| predicate(&j.on))
}
fn lower_query(query: &Query, bindings: &Bindings, version: LanguageVersion) -> Result<TypedQuery> {
    use giggles_core::relational as r;
    if query.projection.is_empty() || (!version.supports_r2_wave2() && (query.source.table.contains('.')
        || query.join.as_ref().is_some_and(|j| j.source.table.contains('.')))) { return Err(Error::Unsupported); }
    let own_alias = if query.join.is_none() { Some(query.source.alias.as_deref().unwrap_or_else(|| query.source.table.rsplit('.').next().unwrap())) } else { None };
    let qualifier = |source: &Option<String>| -> Result<Option<String>> {
        source.as_deref().map(|s| identifier(s, version)).transpose()
            .map(|s| s.filter(|s| Some(s.as_str()) != own_alias))
    };
    let value = |v: &ValueExpr| -> Result<Value> { Ok(match v {
        ValueExpr::Literal(v) => v.clone(),
        ValueExpr::Parameter(n) => bindings.get(n).ok_or_else(|| Error::Bind("parameter missing".into()))?.clone(),
    }) };
    let expr = |e: &QueryExpr| -> Result<r::ReadExpr> { Ok(match e {
        QueryExpr::Value(v) => r::ReadExpr::Literal(value(v)?),
        QueryExpr::Column { source, column } => {
            let column = identifier(column, version)?;
            match qualifier(source)? { Some(source) => r::ReadExpr::QualifiedColumn { source, column },
                None => r::ReadExpr::Column(column) }
        }
        QueryExpr::Aggregate { function, source, column } => {
            let aggregate = match (function, column) {
                (AggregateFunction::Count, None) => r::ReadAggregate::CountStar,
                (f, Some(c)) => { let c = identifier(c, version)?; match f {
                    AggregateFunction::Count => r::ReadAggregate::Count(c), AggregateFunction::Sum => r::ReadAggregate::Sum(c),
                    AggregateFunction::Min => r::ReadAggregate::Min(c), AggregateFunction::Max => r::ReadAggregate::Max(c),
                    AggregateFunction::Avg => r::ReadAggregate::Avg(c),
                } },
                _ => return Err(Error::Unsupported),
            };
            r::ReadExpr::Aggregate(match qualifier(source)? {
                Some(source) => r::ReadAggregate::Qualified { source, aggregate: Box::new(aggregate) }, None => aggregate,
            })
        }
    }) };
    fn subquery_read(q: &Query, bindings: &Bindings, version: LanguageVersion) -> Result<Box<r::SingleSourceRead>> {
        if q.source.alias.is_some() || q.join.is_some() || !q.group_by.is_empty()
            || q.having.is_some() || !q.order.is_empty() || q.limit.is_some() { return Err(Error::Unsupported); }
        let mut q = q.clone();
        if q.projection.is_empty() { q.projection.push(QueryProjection {
            expression: QueryExpr::Value(ValueExpr::Literal(Value::Int(1))), alias: None }); }
        let TypedQuery::Single(read) = lower_query(&q, bindings, version)? else { return Err(Error::Unsupported); };
        Ok(read)
    }
    fn predicate(p: &QueryPredicate, expr: &impl Fn(&QueryExpr) -> Result<r::ReadExpr>, bindings: &Bindings, version: LanguageVersion) -> Result<r::ReadPredicate> {
        Ok(match p {
            QueryPredicate::Compare(a, op, b) => r::ReadPredicate::Compare(expr(a)?, match op {
                Compare::Eq => r::ReadCompare::Eq, Compare::Ne => r::ReadCompare::Ne, Compare::Lt => r::ReadCompare::Lt,
                Compare::Le => r::ReadCompare::Le, Compare::Gt => r::ReadCompare::Gt, Compare::Ge => r::ReadCompare::Ge,
            }, expr(b)?),
            QueryPredicate::IsNull(e, n) => r::ReadPredicate::IsNull(expr(e)?, *n),
            QueryPredicate::Not(p) => r::ReadPredicate::Not(Box::new(predicate(p, expr, bindings, version)?)),
            QueryPredicate::And(a, b) => r::ReadPredicate::And(Box::new(predicate(a, expr, bindings, version)?), Box::new(predicate(b, expr, bindings, version)?)),
            QueryPredicate::Or(a, b) => r::ReadPredicate::Or(Box::new(predicate(a, expr, bindings, version)?), Box::new(predicate(b, expr, bindings, version)?)),
            QueryPredicate::In { expr: e, negated, subquery } => {
                if subquery.projection.len() != 1 { return Err(Error::Unsupported); }
                let read = subquery_read(subquery, bindings, version)?;
                if *negated { r::ReadPredicate::NotIn { expr: expr(e)?, subquery: read } }
                else { r::ReadPredicate::In { expr: expr(e)?, subquery: read } }
            }
            QueryPredicate::Exists { negated, subquery } => {
                let read = subquery_read(subquery, bindings, version)?;
                if *negated { r::ReadPredicate::NotExists(read) } else { r::ReadPredicate::Exists(read) }
            }
        })
    }
    let mut clauses = r::ReadClauses {
        having: query.having.as_ref().map(|p| predicate(p, &expr, bindings, version)).transpose()?,
        order_by: query.order.iter().map(|o| Ok(r::ReadOrder { expression: expr(&o.expression)?,
            descending: o.descending, nulls_first: None })).collect::<Result<_>>()?,
        limit: query.limit.as_ref().map(|v| match value(v)? { Value::Int(n) => Ok(n), _ => Err(Error::Type) }).transpose()?,
        ..r::ReadClauses::default()
    };
    let projection = query.projection.iter().map(|p| Ok(r::ReadProjection { expression: expr(&p.expression)?,
        alias: p.alias.as_deref().map(|s| identifier(s, version)).transpose()? })).collect::<Result<_>>()?;
    let filter = query.filter.as_ref().map(|p| predicate(p, &expr, bindings, version)).transpose()?;
    let attach = !query.group_by.is_empty() || query.having.is_some() || !query.order.is_empty() || query.limit.is_some();
    let source = |s: &QuerySource| -> Result<r::ReadSource> { Ok(r::ReadSource {
        table: if version.supports_r2_wave2() { relation_name(&s.table, version)? } else { identifier(&s.table, version)? },
        alias: identifier(s.alias.as_deref().unwrap_or_else(|| s.table.rsplit('.').next().unwrap()), version)?,
    }) };
    if let Some(join) = &query.join {
        let read = r::TwoSourceRead { left: source(&query.source)?, right: source(&join.source)?,
            kind: match join.kind { JoinKind::Inner => r::ReadJoinKind::Inner, JoinKind::Left => r::ReadJoinKind::Left },
            on: predicate(&join.on, &expr, bindings, version)?, group_by: query.group_by.iter().map(expr).collect::<Result<_>>()?, projection, filter };
        Ok(TypedQuery::Join(Box::new(if attach { read.with_clauses(clauses) } else { read })))
    } else {
        source(&query.source)?;
        clauses.group_by = query.group_by.iter().map(|e| match expr(e)? {
            r::ReadExpr::Column(c) => Ok(c), _ => Err(Error::Unsupported),
        }).collect::<Result<_>>()?;
        let read = r::SingleSourceRead { table: query.source.table.clone(), projection, filter };
        Ok(TypedQuery::Single(Box::new(if attach { read.with_clauses(clauses) } else { read })))
    }
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
    if matches!(stmt, Statement::Query(_) | Statement::InsertSelect { .. }) {
        validate_depth_for(stmt, version)?;
        validate_size(stmt)?;
        return Ok(());
    }
    if !matches!(version, LanguageVersion::Embedded015 | LanguageVersion::Embedded016 | LanguageVersion::Embedded017 | LanguageVersion::Embedded018 | LanguageVersion::Embedded019 | LanguageVersion::Embedded020 | LanguageVersion::Embedded021)
        || !bindings.values().any(|v| matches!(v, Value::Numeric(_)))
    {
        return validate_bindings_for(bindings, version);
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
    validate_bindings_for(&other, version)
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
    if !matches!(version, LanguageVersion::Embedded015 | LanguageVersion::Embedded016 | LanguageVersion::Embedded017 | LanguageVersion::Embedded018 | LanguageVersion::Embedded019 | LanguageVersion::Embedded020 | LanguageVersion::Embedded021)
        || !bindings.values().any(|v| matches!(v, Value::Numeric(_)))
    {
        return validate_bindings_for(bindings, version);
    }
    for step in &transaction.steps {
        match step {
            TransactionStep::Execute(stmt)
            | TransactionStep::Let {
                statement: stmt, ..
            } => validate_parameter_admission(stmt, bindings, version)?,
            TransactionStep::Require(expr) => reject_numeric_references(expr, bindings)?,
            TransactionStep::Savepoint(_) | TransactionStep::RollbackTo(_) | TransactionStep::Release(_)
            | TransactionStep::Sequence { .. } => {},
        }
    }
    reject_numeric_references(&transaction.emit, bindings)
}

fn typed_parameter(value: &Value) -> bool {
    matches!(value, Value::Float4(_) | Value::Float8(_) | Value::Date(_) | Value::Time(_)
        | Value::TimeTz(_) | Value::Timestamp(_) | Value::TimestampTz(_) | Value::Interval(_))
}
pub(crate) fn validate_numeric_bindings(bindings: &Bindings) -> Result<()> {
    validate_bindings_for(bindings, LanguageVersion::Embedded018)
}
fn validate_bindings_for(bindings: &Bindings, version: LanguageVersion) -> Result<()> {
    for v in bindings.values() {
        if version.supports_r2_native() && typed_parameter(v) { continue; }
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
                validate_native_value(&ValueExpr::Literal(v.clone()), LanguageVersion::Embedded018),
                Err(Error::Unsupported)
            ));
            let bytes = serde_json::to_vec(&v).unwrap();
            let decoded: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(serde_json::to_vec(&decoded).unwrap(), bytes);
        }
        assert!(validate_size(&ValueExpr::Literal(Value::Text("Numeric".into()))).is_ok());
    }
}
fn sequence_row(value: i64) -> SqlRow { SqlRow { values: vec![Value::Int(value)], version: 0 } }
