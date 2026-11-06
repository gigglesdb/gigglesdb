//! Explicit trusted retry envelopes over the core durable once contract.
//!
//! Persist the original scope and selected language version before the first
//! attempt. Scope labels confer no tenant/task authority. This adapter never
//! derives a database domain, supplies a clock, or retries an operation.
//!
//! Header bounds are independent of the core body's 64 KiB resolved-value
//! allowance, which includes runtime scalar-local occurrences. The complete
//! native envelope still obeys the serialized input bound, and all supplied
//! parameters share one count/payload admission before header-only parameters
//! are removed from the body's map.
//! Text keys use their UTF-8 bytes; a Bytes key with those same bytes denotes
//! the same opaque identity. Namespace spelling remains case-sensitive.

use crate::{
    Bindings, DurationLiteral, EmitExpr, Engine, Error, Expr, Intent, LanguageVersion,
    MAX_BINDING_BYTES, MAX_BINDINGS, OnceBody, OnceEnvelope, OutcomeQuery, Result, Statement,
    Transaction, TransactionStep, Value, ValueExpr, execute,
};
use giggles_core::operations::once as core_once;
use serde::{Deserialize, Serialize};
use std::{borrow::Cow, collections::BTreeSet, fmt};

pub use core_once::{
    MAX_ONCE_CLOCK_LEASE_SECONDS, MAX_ONCE_INTENT_BYTES, MAX_ONCE_KEY_BYTES, MAX_ONCE_LEDGER_BYTES,
    MAX_ONCE_NAMESPACE_BYTES, MAX_ONCE_RECORDS, MAX_ONCE_RESULT_BYTES, MAX_ONCE_RETENTION_SECONDS,
    ONCE_CONTRACT_VERSION, OnceClock, OnceClockUnavailable, OnceDescription, OnceOutcome,
    OnceReceipt, OnceSchema, OnceStatus, OnceTime,
};

/// Caller-persisted original identity scope, independent of the opened database.
/// Restore never silently replaces this domain with the restored database's one.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnceScope {
    pub database_domain: u128,
    pub tenant: u64,
    pub namespace: String,
}

/// Frontend admission and core durable decisions remain distinguishable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OnceError {
    Frontend(Error),
    Engine(core_once::OnceError),
}
impl fmt::Display for OnceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for OnceError {}
impl From<Error> for OnceError {
    fn from(error: Error) -> Self {
        Self::Frontend(error)
    }
}
impl From<core_once::OnceError> for OnceError {
    fn from(error: core_once::OnceError) -> Self {
        Self::Engine(error)
    }
}
pub type OnceResult<T> = std::result::Result<T, OnceError>;

pub(crate) fn retention_seconds(retention: &DurationLiteral) -> Result<u64> {
    const MICROS_PER_SECOND: i64 = 1_000_000;
    if retention.microseconds <= 0 || retention.microseconds % MICROS_PER_SECOND != 0 {
        return Err(Error::Type);
    }
    let seconds = (retention.microseconds / MICROS_PER_SECOND) as u64;
    if seconds > MAX_ONCE_RETENTION_SECONDS {
        return Err(Error::Limit);
    }
    Ok(seconds)
}

fn selected_version(actual: LanguageVersion, selected: LanguageVersion) -> Result<()> {
    if !selected.supports_once() || actual != selected {
        return Err(Error::Unsupported);
    }
    Ok(())
}
fn parameter_name(name: &str) -> Result<()> {
    if name.is_empty()
        || name.len() > 63
        || !(name.as_bytes()[0].is_ascii_alphabetic() || name.starts_with('_'))
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(Error::Bind("parameter name".into()));
    }
    Ok(())
}
fn key_bytes(value: &Value) -> Result<&[u8]> {
    let bytes = match value {
        Value::Text(text) => text.as_bytes(),
        Value::Bytes(bytes) => bytes,
        _ => return Err(Error::Type),
    };
    if bytes.is_empty() {
        return Err(Error::Bind("once key".into()));
    }
    if bytes.len() > MAX_ONCE_KEY_BYTES {
        return Err(Error::Limit);
    }
    Ok(bytes)
}
fn validate_key(key: &ValueExpr) -> Result<()> {
    match key {
        ValueExpr::Literal(value) => key_bytes(value).map(|_| ()),
        ValueExpr::Parameter(name) => parameter_name(name),
    }
}
fn validate_scope(scope: &OnceScope) -> Result<()> {
    if scope.namespace.is_empty() {
        return Err(Error::Bind("once scope".into()));
    }
    if scope.namespace.len() > MAX_ONCE_NAMESPACE_BYTES {
        return Err(Error::Limit);
    }
    Ok(())
}

/// Structural admission for parsed and caller-owned envelopes, without cloning.
pub(crate) fn validate_once(envelope: &OnceEnvelope, version: LanguageVersion) -> Result<()> {
    selected_version(envelope.language_version, version)?;
    match &envelope.body {
        OnceBody::Statement(statement) => {
            execute::validate_depth(statement)?;
            execute::validate_version(statement, version)?;
            // validate_depth has already admitted the finite wrapper shape.
            let statement = if let Statement::Returning { statement, .. } = statement {
                statement.as_ref()
            } else {
                statement
            };
            let statement = if let Statement::OnConflict { statement, .. } = statement {
                statement.as_ref()
            } else {
                statement
            };
            if matches!(
                statement,
                Statement::Read { .. } | Statement::ReadExpressions { .. }
            ) {
                return Err(Error::Unsupported);
            }
        }
        OnceBody::Transaction(transaction) => {
            execute::validate_transaction(transaction, version)?;
            if transaction.read_only {
                return Err(Error::Unsupported);
            }
        }
    }
    retention_seconds(&envelope.retention)?;
    validate_key(&envelope.key)?;
    execute::validate_size(envelope)
}
pub(crate) fn validate_outcome(query: &OutcomeQuery, version: LanguageVersion) -> Result<()> {
    selected_version(query.language_version, version)?;
    validate_key(&query.key)?;
    execute::validate_size(query)
}

fn validate_bindings(bindings: &Bindings) -> Result<()> {
    if bindings.len() > MAX_BINDINGS {
        return Err(Error::Limit);
    }
    let mut remaining = MAX_BINDING_BYTES;
    for value in bindings.values() {
        // This is the existing core supplied-value metric, not JSON size.
        let bytes = match value {
            Value::Text(text) => text.len(),
            Value::Bytes(bytes) => bytes.len(),
            _ => 9,
        };
        remaining = remaining.checked_sub(bytes).ok_or(Error::Limit)?;
    }
    Ok(())
}

struct Inputs<'a> {
    bindings: &'a Bindings,
    body_names: BTreeSet<&'a str>,
}
impl<'a> Inputs<'a> {
    fn new(bindings: &'a Bindings) -> Self {
        Self {
            bindings,
            body_names: BTreeSet::new(),
        }
    }
    fn value(&mut self, value: &'a ValueExpr) -> Result<()> {
        if let ValueExpr::Parameter(name) = value {
            parameter_name(name)?;
            if !self.bindings.contains_key(name) {
                return Err(Error::Bind("parameter missing".into()));
            }
            // Every inserted name resolved in the globally bounded map, so
            // this borrowed-name set can never exceed MAX_BINDINGS entries.
            self.body_names.insert(name);
        }
        Ok(())
    }
    fn expression(&mut self, expression: &'a Expr) -> Result<()> {
        let mut pending = vec![expression];
        while let Some(expression) = pending.pop() {
            match expression {
                Expr::Value(value) => self.value(value)?,
                Expr::Compare(left, _, right)
                | Expr::And(left, right)
                | Expr::Or(left, right)
                | Expr::Arithmetic(left, _, right) => {
                    pending.push(right);
                    pending.push(left);
                }
                Expr::IsNull(value, _) | Expr::Not(value) | Expr::Negate(value) => {
                    pending.push(value)
                }
                Expr::Call { arguments, .. } => pending.extend(arguments.iter().rev()),
                Expr::Case {
                    operand,
                    arms,
                    else_expr,
                } => {
                    pending.extend(else_expr.iter().map(Box::as_ref));
                    for (when, then) in arms.iter().rev() {
                        pending.push(then);
                        pending.push(when);
                    }
                    pending.extend(operand.iter().map(Box::as_ref));
                }
                Expr::Field(_) | Expr::Local(_) | Expr::ConflictField { .. } => (),
            }
        }
        Ok(())
    }
    fn statement(&mut self, statement: &'a Statement) -> Result<()> {
        match statement {
            Statement::Read { filter, limit, .. } => {
                if let Some(expression) = filter {
                    self.expression(expression)?;
                }
                if let Some(value) = limit {
                    self.value(value)?;
                }
            }
            Statement::ReadExpressions {
                filter,
                limit,
                projection,
                ..
            } => {
                if let Some(expression) = filter {
                    self.expression(expression)?;
                }
                if let Some(value) = limit {
                    self.value(value)?;
                }
                for item in projection {
                    self.expression(&item.expression)?;
                }
            }
            Statement::Insert { values, .. } => {
                for (_, value) in values {
                    self.value(value)?;
                }
            }
            Statement::InsertExpressions { values, .. } => {
                for (_, expression) in values {
                    self.expression(expression)?;
                }
            }
            Statement::InsertMany { rows, cap, .. } => {
                for expression in rows.iter().flatten() {
                    self.expression(expression)?;
                }
                self.value(cap)?;
            }
            Statement::Update {
                filter,
                guard,
                values,
                intent,
                ..
            } => {
                self.expression(filter)?;
                if let Some(expression) = guard {
                    self.expression(expression)?;
                }
                for (_, value) in values {
                    self.value(value)?;
                }
                if let Intent::Many { cap } = intent {
                    self.value(cap)?;
                }
            }
            Statement::UpdateExpressions {
                filter,
                guard,
                values,
                intent,
                ..
            } => {
                self.expression(filter)?;
                if let Some(expression) = guard {
                    self.expression(expression)?;
                }
                for (_, expression) in values {
                    self.expression(expression)?;
                }
                if let Intent::Many { cap } = intent {
                    self.value(cap)?;
                }
            }
            Statement::Delete {
                filter, guard, cap, ..
            } => {
                self.expression(filter)?;
                if let Some(expression) = guard {
                    self.expression(expression)?;
                }
                if let Some(value) = cap {
                    self.value(value)?;
                }
            }
            Statement::Returning {
                statement,
                projection,
            } => {
                self.statement(statement)?;
                for item in projection {
                    self.expression(&item.expression)?;
                }
            }
            Statement::OnConflict {
                statement, action, ..
            } => {
                self.statement(statement)?;
                if let crate::ConflictAction::Update { guard, values } = action {
                    if let Some(expression) = guard {
                        self.expression(expression)?;
                    }
                    for (_, expression) in values {
                        self.expression(expression)?;
                    }
                }
            }
        }
        Ok(())
    }
    fn transaction(&mut self, transaction: &'a Transaction) -> Result<()> {
        for step in &transaction.steps {
            match step {
                TransactionStep::Execute(statement) | TransactionStep::Let { statement, .. } => {
                    self.statement(statement)?
                }
                TransactionStep::Require(expression) => self.expression(expression)?,
            }
        }
        if let Some(emitted) = &transaction.emit {
            for value in emitted {
                if let EmitExpr::Scalar(expression) = value {
                    self.expression(expression)?;
                }
            }
        }
        Ok(())
    }
    fn admit_key(&self, key: &'a ValueExpr) -> Result<(&'a [u8], bool)> {
        let (value, header_only) = match key {
            ValueExpr::Literal(value) => (value, false),
            ValueExpr::Parameter(name) => {
                parameter_name(name)?;
                let value = self
                    .bindings
                    .get(name)
                    .ok_or_else(|| Error::Bind("parameter missing".into()))?;
                (value, !self.body_names.contains(name.as_str()))
            }
        };
        let bytes = key_bytes(value)?;
        if self.body_names.len() + usize::from(header_only) != self.bindings.len() {
            return Err(Error::Bind("parameters".into()));
        }
        Ok((bytes, header_only))
    }
    fn body_bindings(&self, header_only: bool) -> Cow<'a, Bindings> {
        if !header_only {
            return Cow::Borrowed(self.bindings);
        }
        // All values and used names passed finite admission before any copies.
        // Preserve a key parameter when the body references that same name.
        Cow::Owned(
            self.bindings
                .iter()
                .filter(|(name, _)| self.body_names.contains(name.as_str()))
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect(),
        )
    }
}

fn identity(scope: &OnceScope, key: &[u8]) -> core_once::OnceIdentity {
    core_once::OnceIdentity {
        database_domain: scope.database_domain,
        tenant: scope.tenant,
        namespace: scope.namespace.clone(),
        key: key.to_vec(),
    }
}

impl Engine {
    /// Parse and execute an explicit once envelope with caller-owned context.
    pub fn execute_once_text(
        &self,
        text: &str,
        bindings: &Bindings,
        scope: &OnceScope,
        clock: &dyn OnceClock,
    ) -> OnceResult<OnceReceipt> {
        let envelope = crate::parse_once_with_version(text, self.version)?;
        self.execute_once(&envelope, bindings, scope, clock)
    }

    /// Resolve an original key before live catalog binding or business work.
    /// Header labels and limits do not consume the core body's value allowance.
    pub fn execute_once(
        &self,
        envelope: &OnceEnvelope,
        bindings: &Bindings,
        scope: &OnceScope,
        clock: &dyn OnceClock,
    ) -> OnceResult<OnceReceipt> {
        validate_once(envelope, self.version)?;
        validate_bindings(bindings)?;
        validate_scope(scope)?;
        let mut inputs = Inputs::new(bindings);
        match &envelope.body {
            OnceBody::Statement(statement) => inputs.statement(statement)?,
            OnceBody::Transaction(transaction) => inputs.transaction(transaction)?,
        }
        let (key, header_only) = inputs.admit_key(&envelope.key)?;
        let body = match &envelope.body {
            OnceBody::Statement(statement) => {
                core_once::OnceBody::Operation(execute::lower_checked(statement, self.version)?)
            }
            OnceBody::Transaction(transaction) => core_once::OnceBody::Transaction(
                execute::lower_transaction_checked(transaction, self.version)?,
            ),
        };
        let request = core_once::OnceRequest {
            contract_version: ONCE_CONTRACT_VERSION,
            identity: identity(scope, key),
            retention_seconds: retention_seconds(&envelope.retention)?,
            body,
        };
        let body_bindings = inputs.body_bindings(header_only);
        self.inner
            .execute_once(&request, &body_bindings, clock)
            .map_err(Into::into)
    }

    /// Parse a lookup using the original persisted scope; no clock is required.
    pub fn outcome_text(
        &self,
        text: &str,
        bindings: &Bindings,
        scope: &OnceScope,
    ) -> OnceResult<OnceStatus> {
        let query = crate::parse_outcome_with_version(text, self.version)?;
        self.outcome(&query, bindings, scope)
    }

    /// Look up the original domain/key without rebinding or issuing new work.
    pub fn outcome(
        &self,
        query: &OutcomeQuery,
        bindings: &Bindings,
        scope: &OnceScope,
    ) -> OnceResult<OnceStatus> {
        validate_outcome(query, self.version)?;
        validate_bindings(bindings)?;
        validate_scope(scope)?;
        let inputs = Inputs::new(bindings);
        let (key, _) = inputs.admit_key(&query.key)?;
        self.inner
            .outcome(&identity(scope, key))
            .map_err(Into::into)
    }
}
