//! Application-authorized typed execution on the existing retained Session.
use super::codec::Invocation;
use crate::{Role, Session};
use giggles_core::{CoreError, Error, operations as ops, relational::SqlError};

pub(crate) enum NativeResult {
    Control,
    Operation(ops::Outcome),
    Transaction(ops::TransactionOutcome),
    Error(ops::Error),
}

fn authorize_request(
    request: &ops::Request,
    tables: &std::collections::BTreeSet<String>,
) -> Result<(), CoreError> {
    let mut request = request;
    for _ in 0..=2 {
        request = match request {
            ops::Request::Returning { request, .. } | ops::Request::OnConflict { request, .. } => {
                request
            }
            ops::Request::Read { table, .. }
            | ops::Request::Insert { table, .. }
            | ops::Request::Update { table, .. }
            | ops::Request::Delete { table, .. }
            | ops::Request::UpdateExpressions { table, .. }
            | ops::Request::ReadExpressions { table, .. }
            | ops::Request::InsertExpressions { table, .. }
            | ops::Request::InsertMany { table, .. } => {
                return if table != "giggles_join" && tables.contains(table) {
                    Ok(())
                } else {
                    Err(CoreError::Policy(Error::Scope))
                };
            }
        };
    }
    Err(CoreError::Limit)
}

pub(crate) fn run(
    session: &mut Session,
    invocation: Invocation,
) -> Result<NativeResult, CoreError> {
    let result = (|| {
        session.check_binding()?;
        let Role::Application { domain, tables } = &session.role else {
            return Err(CoreError::Policy(Error::Scope));
        };
        // Traverse all targets before consulting any catalog or executing an
        // earlier block step. Expressions cannot introduce additional tables.
        match &invocation {
            Invocation::Operation { request, .. } => authorize_request(request, tables)?,
            Invocation::Transaction { transaction, .. } => {
                if transaction.steps.len() > ops::MAX_TRANSACTION_ITEMS {
                    return Err(CoreError::Limit);
                }
                for step in &transaction.steps {
                    if let ops::TransactionStep::Execute(request)
                    | ops::TransactionStep::Let { request, .. } = step
                    {
                        authorize_request(request, tables)?;
                    }
                }
            }
            Invocation::Begin | Invocation::Commit | Invocation::Rollback => {}
            Invocation::Legacy(_) | Invocation::Text { .. } => return Err(CoreError::Unsupported),
        }
        if session.service.admin.authority_metadata()?.0.to_string() != *domain {
            return Err(CoreError::Policy(Error::Authentication));
        }
        if session.is_aborted() && !matches!(invocation, Invocation::Rollback) {
            return Err(CoreError::Sql(SqlError::Aborted));
        }
        let result = match invocation {
            Invocation::Operation { request, bindings } => session
                .sql
                .execute_native(&request, &bindings)
                .map(NativeResult::Operation),
            Invocation::Transaction {
                transaction,
                bindings,
            } => session
                .sql
                .execute_native_transaction(&transaction, &bindings)
                .map(NativeResult::Transaction),
            Invocation::Begin => {
                session.sql.begin_native()?;
                return Ok(NativeResult::Control);
            }
            Invocation::Commit => {
                session.sql.commit_native()?;
                return Ok(NativeResult::Control);
            }
            Invocation::Rollback => {
                session.sql.rollback_native()?;
                return Ok(NativeResult::Control);
            }
            Invocation::Legacy(_) | Invocation::Text { .. } => unreachable!(),
        };
        match result {
            Ok(outcome) => Ok(outcome),
            Err(error) => {
                // Keep durable ambiguity dominant. Never inspect Core(String).
                if error != ops::Error::Unknown {
                    giggles_core::cancellation::check()?;
                }
                Ok(NativeResult::Error(error))
            }
        }
    })();
    if result.is_err() {
        session.abort();
    }
    result
}
