//! GD-0061 R3-110: the server glue for the task statements of design-1
//! section 5 (CREATE TASK, REVOKE TASK, SHOW TASK(S), SHOW TASK READS,
//! SHOW TASK EXPOSURE and GRANT / REVOKE { UNDO | INSPECT } ON TASK).
//!
//! The pgwire simple and extended paths parse with
//! `giggles_core::task::statements::parse` before the role parser, which
//! claims every GRANT and REVOKE head (design-1 section 11). The statement then
//! runs through `Session::execute_task_ddl`, which checks the session and calls
//! `run` here. Errors keep their SQLSTATE and message (`TaskDdlError`).
//!
//! Like role DDL, a task statement is standalone: 25001 inside a transaction
//! block, and the sole statement of a simple Query. Task sessions never get
//! here: the agent path refuses every task statement as RoleStatement before
//! it runs (`pgwire_task.rs`), and `execute_task_ddl` has the same backstop.
//! A task-only role cannot log in on an ordinary listener at all (D6).
use crate::service::Session;
use giggles_core::{
    CoreError,
    relational::{Description, SqlResult},
    roles::CatalogRole,
    task::{
        catalog::TaskDdlError,
        definition::WallClockMs,
        statements::{self, TaskOutcome, TaskStatement},
    },
};

/// The task statement in `sql`, or `None` for every other statement.
pub(crate) fn parse(sql: &str) -> Option<TaskStatement> {
    statements::parse(sql)
}

/// The CommandComplete tag of `stmt`.
pub(crate) fn command(stmt: &TaskStatement) -> &'static str {
    statements::command_tag(stmt)
}

/// The result shape of `stmt`: empty unless it is a SHOW form.
pub(crate) fn description(stmt: &TaskStatement) -> Description {
    statements::description(stmt)
}

/// Whether `stmt` writes the catalog: everything but the SHOW forms (and a
/// parse error, which never runs). Writes pass the read-only check.
pub(crate) fn writes(stmt: &TaskStatement) -> bool {
    match stmt {
        TaskStatement::Create(_)
        | TaskStatement::Revoke(_)
        | TaskStatement::Grant { .. }
        | TaskStatement::IssueCredential { .. }
        | TaskStatement::RevokeCredential(_) => true,
        TaskStatement::Error(_)
        | TaskStatement::Show(_)
        | TaskStatement::ShowReads { .. }
        | TaskStatement::ShowExposure(_)
        | TaskStatement::ShowExposureDraft(_) => false,
    }
}

/// A core error as a task DDL error: its SQLSTATE and message.
pub(crate) fn from_core(error: CoreError) -> TaskDdlError {
    TaskDdlError {
        sqlstate: crate::service::sqlstate(&error),
        message: error.to_string(),
    }
}

/// Runs `stmt` as `actor` (already checked by `Session::execute_task_ddl`).
/// The SHOW READS and SHOW EXPOSURE forms read the session; the others run
/// on the task catalog. Every result is one the server builds from catalog
/// and Kernel records, never application rows.
pub(crate) fn run(
    session: &mut Session,
    actor: &CatalogRole,
    stmt: &TaskStatement,
    now: WallClockMs,
) -> Result<TaskOutcome, TaskDdlError> {
    let rows = |result: Result<SqlResult, CoreError>| {
        result
            .map(|result| TaskOutcome::Rows(description(stmt), result))
            .map_err(from_core)
    };
    match stmt {
        TaskStatement::ShowReads { task, after, limit } => rows(
            giggles_core::task::readlog::show_reads(&session.sql, actor, task, *after, *limit),
        ),
        TaskStatement::ShowExposure(task) => {
            let extra = crate::agent::exposure_listener_rows(&session.service, task);
            rows(giggles_core::task::exposure::show_exposure(
                &session.sql,
                actor,
                task,
                &extra,
            ))
        }
        TaskStatement::ShowExposureDraft(new) => {
            let extra = crate::agent::exposure_listener_rows(&session.service, &new.name);
            giggles_core::task::exposure::draft_exposure(&session.sql, actor, new, now, &extra)
                .map(|result| TaskOutcome::Rows(description(stmt), result))
        }
        TaskStatement::Error(_)
        | TaskStatement::Create(_)
        | TaskStatement::Revoke(_)
        | TaskStatement::IssueCredential { .. }
        | TaskStatement::RevokeCredential(_)
        | TaskStatement::Show(_)
        | TaskStatement::Grant { .. } => {
            statements::execute(&session.service.admin, actor, stmt, now)
        }
    }
}
