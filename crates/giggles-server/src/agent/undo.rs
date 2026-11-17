//! GD-0061 R3-804 (design-7 D7.1, D8): the undo statements over pgwire.
//!
//! | Who, where                     | SQL                                                                   |
//! |--------------------------------|-----------------------------------------------------------------------|
//! | Undo role, PostgreSQL listener | `PREVIEW UNDO OF TASK <name> [OPERATION '<op id>']`                   |
//! | Undo role, PostgreSQL listener | `SHOW UNDO <id> OF TASK <name>`                                       |
//! | Undo role, PostgreSQL listener | `APPROVE UNDO <id> OF TASK <name> DIGEST '<hex>' MODE {COMPLETE\|PARTIAL}` |
//!
//! Core parses them (`task::statements::parse_change_statement`) as approver
//! forms, so they take R3-707's routing arms in `pgwire.rs`,
//! `pgwire_simple.rs` and `pgwire_task.rs`: a task session (the agent
//! listener) refuses them as `Other` (0A000), and elsewhere they run only as
//! the role of a session that passes D7.1's PostgreSQL-listener test
//! (`agent::preview::execute_approver`; a certificate-bound role gets
//! 42501). Here each form runs the core API as that role:
//! - `PREVIEW UNDO`: `task::undo_preview`, one row `(change_id, digest,
//!   restorable, conflicts, superseded, settled, expires_at)`;
//! - `SHOW UNDO`: `task::show_undo`, `(class, key, column_name,
//!   current_value, restored_value, reason)`;
//! - `APPROVE UNDO`: `task::undo_approve`, the full receipt as one row
//!   (`task::statements::UNDO_RECEIPT_COLUMNS`).
use giggles_core::{
    CoreError, SqlSession,
    relational::{DataType, SqlResult},
    task::{
        self, PreviewId, UndoScope,
        definition::{RoleName, TaskName},
        statements::{
            ChangeStatement, UNDO_COLUMNS, UNDO_PREVIEW_COLUMNS, UNDO_RECEIPT_COLUMNS,
            undo_preview_result, undo_receipt_result,
        },
    },
};

/// The result columns of an undo form, with their types; `None` for any
/// other statement.
pub(crate) fn columns(statement: &ChangeStatement) -> Option<Vec<(&'static str, DataType)>> {
    use DataType::{BigInt, Text, TimestampTz};
    let (names, types): (&[&str], Vec<DataType>) = match statement {
        ChangeStatement::PreviewUndo { .. } => (
            &UNDO_PREVIEW_COLUMNS,
            vec![BigInt, Text, BigInt, BigInt, BigInt, BigInt, TimestampTz],
        ),
        ChangeStatement::ShowUndo { .. } => (&UNDO_COLUMNS, vec![Text; 6]),
        ChangeStatement::ApproveUndo { .. } => {
            // operation_id, decision, receipt_seq, change_id, mode; the
            // eight counts; the two digests and three roles; def_version,
            // decided_at.
            let mut types = vec![Text, Text, BigInt, BigInt, Text];
            types.extend(std::iter::repeat_n(BigInt, 8));
            types.extend(std::iter::repeat_n(Text, 5));
            types.extend([BigInt, TimestampTz]);
            (&UNDO_RECEIPT_COLUMNS, types)
        }
        _ => return None,
    };
    Some(names.iter().copied().zip(types).collect())
}

/// The CommandComplete word of an undo form; `None` for any other statement.
pub(crate) fn command(statement: &ChangeStatement) -> Option<&'static str> {
    match statement {
        ChangeStatement::PreviewUndo { .. } => Some("PREVIEW UNDO"),
        ChangeStatement::ShowUndo { .. } => Some("SHOW"),
        ChangeStatement::ApproveUndo { .. } => Some("APPROVE UNDO"),
        _ => None,
    }
}

/// Runs an undo form as `role`, the role of a session that already passed
/// the PostgreSQL-listener test. `task` is the checked task name.
pub(crate) fn execute(
    sql: &mut SqlSession,
    role: &RoleName,
    task: &TaskName,
    statement: &ChangeStatement,
) -> Result<SqlResult, CoreError> {
    match statement {
        ChangeStatement::PreviewUndo { operation, .. } => {
            let scope = operation
                .clone()
                .map_or(UndoScope::Task, UndoScope::Operation);
            let summary = task::undo_preview(sql, role, task, scope).map_err(CoreError::from)?;
            undo_preview_result(&summary)
        }
        ChangeStatement::ShowUndo { undo, .. } => {
            task::show_undo(sql, role, task, PreviewId(*undo))
                .map_err(CoreError::from)?
                .untasked()
        }
        ChangeStatement::ApproveUndo {
            undo, digest, mode, ..
        } => {
            let receipt = task::undo_approve(sql, role, task, PreviewId(*undo), *digest, *mode)
                .map_err(CoreError::from)?;
            undo_receipt_result(&receipt)
        }
        _ => Err(CoreError::Unsupported),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use giggles_core::task::{OperationId, UndoMode};

    #[test]
    fn every_undo_column_has_a_type() {
        let task = String::from("t");
        for (statement, width) in [
            (
                ChangeStatement::PreviewUndo {
                    task: task.clone(),
                    operation: OperationId::new("a1"),
                },
                UNDO_PREVIEW_COLUMNS.len(),
            ),
            (
                ChangeStatement::ShowUndo {
                    undo: 1,
                    task: task.clone(),
                },
                UNDO_COLUMNS.len(),
            ),
            (
                ChangeStatement::ApproveUndo {
                    undo: 1,
                    task: task.clone(),
                    digest: [0; 32],
                    mode: UndoMode::Partial,
                },
                UNDO_RECEIPT_COLUMNS.len(),
            ),
        ] {
            assert_eq!(
                columns(&statement).map(|c| c.len()),
                Some(width),
                "{statement:?}"
            );
            assert!(command(&statement).is_some());
        }
        let other = ChangeStatement::ShowChanges { task };
        assert!(columns(&other).is_none() && command(&other).is_none());
    }
}
