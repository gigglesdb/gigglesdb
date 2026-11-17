//! GD-0061 R3-707 (design-7 D7.1): PREVIEW, APPLY CHANGE and the approver
//! forms over pgwire.
//!
//! | Who, where                    | SQL                                                          |
//! |-------------------------------|--------------------------------------------------------------|
//! | Executor, agent listener      | `PREVIEW UPDATE ...`                                         |
//! | Executor, agent listener      | `OPERATION '<id>' APPLY CHANGE <id> DIGEST '<64 hex>'`        |
//! | Approver, PostgreSQL listener | `SHOW CHANGES OF TASK <name>`; `SHOW CHANGE <id> OF TASK <name>` |
//! | Approver, PostgreSQL listener | `APPROVE CHANGE <id> OF TASK <name> DIGEST '<64 hex>'`        |
//! | Approver, PostgreSQL listener | `REVOKE APPROVAL OF CHANGE <id> OF TASK <name>`              |
//!
//! The executor forms are parsed on the agent path (`agent::statement`, ruling
//! R2 adds the operation-ID prefix to APPLY) and run here from
//! `protected_seam`. The approver forms are parsed by core
//! (`task::statements::parse_change_statement`) on the ordinary listeners.
//! A task session never reaches them: its own path classes them `Other`. On
//! any other session they run only when the session passes D7.1's
//! "PostgreSQL listener only" test (a database role that is not certificate
//! bound); a certificate-bound role on an mTLS listener gets 42501.
use super::statement::OperationId;
use crate::Session;
use giggles_core::{
    CoreError, Error,
    relational::{ColumnDef, DataType, Description, SqlResult, Value},
    task::{
        self, Actor, PreviewId, ProtectedRequest, StatementClass, Surface, TaskError,
        approval::{CHANGE_COLUMNS, CHANGES_COLUMNS},
        definition::TaskName,
        statements::{ChangeStatement, parse_change_statement, preview_result},
    },
};

/// The approver forms of D7.1, and the undo forms (R3-804, `agent::undo`),
/// on a session without a task. `Ok(None)` is any other statement. PREVIEW
/// and APPLY CHANGE are `None` too: off the agent listener they keep the
/// reply of an unknown statement (R3-206). `PREVIEW UNDO` is an undo form.
pub(crate) fn approver_statement(sql: &str) -> Result<Option<ChangeStatement>, &'static str> {
    let mut words = sql.split_whitespace();
    let first = words.next().unwrap_or("");
    let preview_undo = first.eq_ignore_ascii_case("preview")
        && words.next().is_some_and(|w| w.eq_ignore_ascii_case("undo"));
    if (first.eq_ignore_ascii_case("preview") && !preview_undo) || first.eq_ignore_ascii_case("apply") {
        return Ok(None);
    }
    match parse_change_statement(sql) {
        None => Ok(None),
        Some(Ok(statement)) if statement.is_approver_form() => Ok(Some(statement)),
        Some(Ok(_)) => Ok(None),
        Some(Err(error)) => Err(crate::service::sqlstate(&CoreError::Sql(error))),
    }
}

fn column(name: &str, data_type: DataType) -> ColumnDef {
    ColumnDef {
        id: 0,
        name: name.into(),
        data_type,
        not_null: false,
    }
}

/// The result shape of an approver form: the D7.5 columns of SHOW CHANGES
/// and SHOW CHANGE, none for APPROVE and REVOKE APPROVAL.
pub(crate) fn approver_description(statement: &ChangeStatement) -> Description {
    use DataType::{BigInt, Text, TimestampTz};
    if let Some(columns) = super::undo::columns(statement) {
        let columns: Vec<_> = columns
            .into_iter()
            .map(|(name, data_type)| column(name, data_type))
            .collect();
        return Description {
            type_oids: vec![],
            typmods: vec![-1; columns.len()],
            parameters: vec![],
            columns,
        };
    }
    let types: &[DataType] = match statement {
        ChangeStatement::ShowChanges { .. } => {
            &[BigInt, Text, Text, Text, BigInt, TimestampTz, TimestampTz, Text]
        }
        ChangeStatement::ShowChange { .. } => &[BigInt, Text, Text, Text, Text],
        _ => &[],
    };
    let names: &[&str] = match statement {
        ChangeStatement::ShowChanges { .. } => &CHANGES_COLUMNS,
        ChangeStatement::ShowChange { .. } => &CHANGE_COLUMNS,
        _ => &[],
    };
    let columns: Vec<_> = names
        .iter()
        .zip(types)
        .map(|(name, data_type)| column(name, data_type.clone()))
        .collect();
    Description {
        type_oids: vec![],
        typmods: vec![-1; columns.len()],
        parameters: vec![],
        columns,
    }
}

/// The CommandComplete word of an approver form.
pub(crate) fn approver_command(statement: &ChangeStatement) -> &'static str {
    if let Some(command) = super::undo::command(statement) {
        return command;
    }
    match statement {
        ChangeStatement::Approve { .. } => "APPROVE",
        ChangeStatement::RevokeApproval { .. } => "REVOKE APPROVAL",
        _ => "SHOW",
    }
}

/// Approval records are written outside the SQL snapshot, so an approver
/// form runs on its own, never inside a transaction block (as role DDL).
pub(crate) fn approver_guard(session: &Session) -> Result<(), &'static str> {
    if session.in_transaction() {
        return Err("25001");
    }
    if session.is_aborted() {
        return Err("25P02");
    }
    Ok(())
}

/// Runs an approver form as the session's role. A task session is refused
/// by class; a session that fails the PostgreSQL-listener test gets 42501.
/// An unknown or malformed task name is 42704, as for an unknown task.
pub(crate) fn execute_approver(
    session: &mut Session,
    statement: &ChangeStatement,
) -> Result<SqlResult, CoreError> {
    if session.task().is_some() {
        return Err(CoreError::Task(TaskError::NotAllowedInTaskSession(
            StatementClass::Other,
        )));
    }
    session.authority_scope(|session| {
        let role = session
            .postgresql_listener_role()
            .map(str::to_owned)
            .ok_or(CoreError::Policy(Error::Scope))?;
        let name = |task: &str| {
            TaskName::new(task).map_err(|_| {
                CoreError::Sql(giggles_core::relational::SqlError::UndefinedObject)
            })
        };
        let sql = &mut session.sql;
        match statement {
            ChangeStatement::ShowChanges { task } => task::list_changes(sql, &role, &name(task)?)
                .map_err(CoreError::from)?
                .untasked(),
            ChangeStatement::ShowChange { change, task } => {
                task::show_change(sql, &role, &name(task)?, PreviewId(*change))
                    .map_err(CoreError::from)?
                    .untasked()
            }
            ChangeStatement::Approve {
                change,
                task,
                digest,
            } => {
                task::approve(sql, &role, &name(task)?, PreviewId(*change), *digest)
                    .map_err(CoreError::from)?;
                Ok(SqlResult::default())
            }
            ChangeStatement::RevokeApproval { change, task } => {
                task::revoke_approval(sql, &role, &name(task)?, PreviewId(*change))
                    .map_err(CoreError::from)?;
                Ok(SqlResult::default())
            }
            ChangeStatement::PreviewUndo { task, .. }
            | ChangeStatement::ShowUndo { task, .. }
            | ChangeStatement::ApproveUndo { task, .. } => {
                super::undo::execute(sql, &role, &name(task)?, statement)
            }
            ChangeStatement::Preview { .. } | ChangeStatement::Apply { .. } => {
                Err(CoreError::Unsupported)
            }
        }
    })
}

/// `PREVIEW UPDATE ...` in a task session: `task::preview` of the wrapped
/// UPDATE's text and parameters under the statement's clock, answered with
/// one row `(change_id, digest, row_count, expires_at)`.
pub(crate) fn preview(
    session: &mut Session,
    text: &str,
    params: &[Value],
) -> Result<SqlResult, CoreError> {
    let ctx = session
        .task()
        .cloned()
        .ok_or(CoreError::Policy(Error::Scope))?;
    let clock = session.copy_clock();
    let summary = task::preview(&mut session.sql, &ctx, Surface::Sql, text, params, &clock)
        .map_err(CoreError::from)?;
    preview_result(&summary)
}

/// `OPERATION '<id>' APPLY CHANGE <id> DIGEST '<hex>'` in a task session: one
/// protected commit of the stored preview, answered `UPDATE n` with the
/// receipt's row count, as the direct write is.
pub(crate) fn apply(
    session: &mut Session,
    op: &OperationId,
    change: u64,
    digest: [u8; 32],
) -> Result<SqlResult, CoreError> {
    let ctx = session
        .task()
        .cloned()
        .ok_or(CoreError::Policy(Error::Scope))?;
    let op = op.to_core()?;
    let request = ProtectedRequest::Apply {
        preview: PreviewId(change),
        digest,
    };
    let receipt = task::protected_commit(&mut session.sql, Actor::Executor(&ctx), &op, request)
        .map_err(CoreError::from)?;
    Ok(SqlResult {
        columns: Vec::new(),
        rows: Vec::new(),
        affected: u64::from(receipt.rows_affected),
    })
}
