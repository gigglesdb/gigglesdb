//! Native task SHOW forms (GD-0061 R3-502, design-3 sections 6 and 8):
//! `show task exposure <task>`, `show task exposure for create task ...` and
//! `show task reads <task> [after <n>] [limit <n>]`.
//!
//! They lower to the same `TaskStatement` variants the SQL parser produces,
//! and [`show_task`] runs those through the same core functions, so a native
//! and an SQL statement give identical rows. As in SQL, `show task reads` and
//! `show task exposure` with nothing after them name a task called `reads` or
//! `exposure`, and `show task exposure for` names a task called `for`.
use super::TaskParser;
use crate::Result;
use crate::parser::Token;
use giggles_core::SqlSession;
use giggles_core::relational::SqlResult;
use giggles_core::roles::CatalogRole;
use giggles_core::task::catalog::TaskDdlError;
use giggles_core::task::definition::WallClockMs;
use giggles_core::task::exposure::{draft_exposure, show_exposure};
use giggles_core::task::readlog::show_reads;
use giggles_core::task::statements::TaskStatement;

impl TaskParser {
    /// `show task ...` after the two head words.
    pub(super) fn show_task(&mut self) -> Result<TaskStatement> {
        let named = matches!(
            self.p.tokens.get(self.p.pos + 1),
            Some((Token::Word(_) | Token::QuotedIdentifier(_) | Token::String(_), _))
        );
        if named && self.peek_word(0) == Some("reads") {
            self.p.pos += 1;
            let task = self.ident()?;
            let after = if self.p.word("after") { Some(self.number()?) } else { None };
            let limit = if self.p.word("limit") { Some(self.number()?) } else { None };
            return Ok(TaskStatement::ShowReads { task, after, limit });
        }
        if named && self.peek_word(0) == Some("exposure") {
            self.p.pos += 1;
            if self.peek_word(0) == Some("for") && self.peek_word(1) == Some("create") {
                self.p.pos += 1;
                return Ok(TaskStatement::ShowExposureDraft(Box::new(self.create()?)));
            }
            return Ok(TaskStatement::ShowExposure(self.ident()?));
        }
        Ok(TaskStatement::Show(Some(self.ident()?)))
    }
}

/// Runs a task report statement, native or SQL, as `role`: SHOW TASK READS,
/// SHOW TASK EXPOSURE and its draft form. `now` is the draft's validation
/// time; `extra` holds the server's `listener` rows. Any other statement is
/// `None`. Errors carry the SQLSTATE the core function reports.
pub fn show_task(
    session: &SqlSession,
    role: &CatalogRole,
    statement: &TaskStatement,
    now: WallClockMs,
    extra: &[(String, String)],
) -> Option<std::result::Result<SqlResult, TaskDdlError>> {
    Some(match statement {
        TaskStatement::ShowReads { task, after, limit } => {
            show_reads(session, role, task, *after, *limit).map_err(TaskDdlError::from)
        }
        TaskStatement::ShowExposure(task) => {
            show_exposure(session, role, task, extra).map_err(TaskDdlError::from)
        }
        TaskStatement::ShowExposureDraft(new) => draft_exposure(session, role, new, now, extra),
        _ => return None,
    })
}
