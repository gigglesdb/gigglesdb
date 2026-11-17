//! Release of task-session results (GD-0061 R3-301, design-3 section 4).
//!
//! Every core exit that can hand rows to a caller has an `*_unreleased` twin
//! returning [`Unreleased<T>`]. An `Unreleased` value exposes nothing: the only
//! ways out are [`SqlSession::release_page`], which turns it into a [`Released<T>`]
//! (the only thing encoders accept), and [`Unreleased::untasked`], which works
//! only when the value was produced outside a task session.
//!
//! The plain exits (`execute`, `execute_prepared`, `Cursor::fetch`,
//! `execute_native`, ...) are kept for trusted local callers. They refuse a task
//! session before running anything, so a caller that was never converted to the
//! twins cannot leak a task's rows.
//!
//! This item is pass-through: without a task, `release_page` is a move. With a task
//! it fails closed (`CoreError::Unsupported`) until the release protocol
//! (R3-308) lands; `still_valid` and `finish_task_statement` are no-ops.
//!
//! ```compile_fail
//! // `Unreleased` has no public field and no public constructor.
//! let page = giggles_core::Unreleased { value: Vec::<u8>::new(), tag: None };
//! ```
//!
//! ```compile_fail
//! // No accessor reads the value of an `Unreleased` page.
//! fn peek(page: &giggles_core::Unreleased<giggles_core::relational::SqlResult>) -> usize {
//!     page.rows.len()
//! }
//! ```
//!
//! ```compile_fail
//! // `Unreleased` is not `Clone`, so it cannot be duplicated around a release.
//! fn copy(page: &giggles_core::Unreleased<Vec<giggles_core::relational::SqlRow>>)
//!     -> giggles_core::Unreleased<Vec<giggles_core::relational::SqlRow>> {
//!     page.clone()
//! }
//! ```
//!
//! ```compile_fail
//! // `Unreleased` is not `Serialize`.
//! fn encode(page: &giggles_core::Unreleased<giggles_core::relational::SqlResult>) -> String {
//!     serde_json::to_string(page).unwrap()
//! }
//! ```
use std::sync::Arc;

use super::admit::StatementClass;
use super::columns::OutputPlan;
use super::context::TaskContext;
use super::error::TaskError;
use super::records::TaskObjectId;
use crate::operations::{EmittedValue, Outcome, TransactionOutcome};
use crate::relational::{SqlResult, SqlRow, Value};
use crate::storage::{CoreError, SqlSession};

/// What the release of one task statement needs. Created when the statement
/// starts in a task session and shared by every page it produces.
///
/// R3-301 only carries the binding; R3-308 adds the read sequence, statement
/// fingerprint and text, snapshot timestamp and meter, and R3-303 supplies the
/// `OutputPlan` from `columns::apply_in_catalog`.
#[derive(Debug)]
pub struct ReleaseTag {
    #[allow(dead_code, reason = "read by the release protocol (R3-308)")]
    task: TaskObjectId,
    #[allow(dead_code, reason = "read by the release protocol (R3-308)")]
    bound_def_version: u64,
    #[allow(dead_code, reason = "filled by R3-303, read by R3-308")]
    plan: OutputPlan,
}

impl ReleaseTag {
    pub(crate) fn new(ctx: &TaskContext, plan: OutputPlan) -> Self {
        Self {
            task: ctx.task,
            bound_def_version: ctx.bound_def_version,
            plan,
        }
    }
}

/// A result produced by a core exit that has not been released.
///
/// It has no public field, no `Deref`, no `Clone`, no `Serialize` and a
/// `Debug` that never shows the value.
#[must_use = "an unreleased page must be released or dropped"]
pub struct Unreleased<T> {
    value: T,
    tag: Option<Arc<ReleaseTag>>,
}

impl<T> Unreleased<T> {
    pub(crate) fn new(value: T, tag: Option<Arc<ReleaseTag>>) -> Self {
        Self { value, tag }
    }

    /// The value, for a result produced outside a task session. A task
    /// session's result is refused.
    pub fn untasked(self) -> Result<T, CoreError> {
        match self.tag {
            None => Ok(self.value),
            Some(_) => Err(refusal()),
        }
    }

    /// Whether this result was produced in a task session.
    pub fn is_task(&self) -> bool {
        self.tag.is_some()
    }

    /// Crate-internal split, for exits that carry the tag into another
    /// container (a buffered cursor). Never exposed.
    pub(crate) fn into_parts(self) -> (T, Option<Arc<ReleaseTag>>) {
        (self.value, self.tag)
    }
}

impl<T> std::fmt::Debug for Unreleased<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Unreleased")
            .field("task", &self.is_task())
            .finish_non_exhaustive()
    }
}

/// A released result: the only form an encoder accepts.
#[derive(Debug, Clone, PartialEq)]
pub struct Released<T>(T);

impl<T> Released<T> {
    /// A result the server builds itself (SHOW, settings functions, empty
    /// command completions). It cannot wrap core rows, because `Unreleased`
    /// exposes none.
    pub fn synthetic(value: T) -> Self {
        Self(value)
    }
    pub fn get(&self) -> &T {
        &self.0
    }
    pub fn into_inner(self) -> T {
        self.0
    }
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> Released<U> {
        Released(f(self.0))
    }
}

/// A result whose rows the release protocol masks and charges.
///
/// A row is visited as a value slice, so a scalar emitted by a native
/// transaction block is a one-value row.
pub trait Page {
    /// Every released row, in order.
    fn for_each_row(&self, f: &mut dyn FnMut(&[Value]));
    /// Every released row, in order, for masking in place.
    fn for_each_row_mut(&mut self, f: &mut dyn FnMut(&mut [Value]));
    /// The number of rows `for_each_row` visits.
    fn row_count(&self) -> usize {
        let mut n = 0;
        self.for_each_row(&mut |_| n += 1);
        n
    }
}

impl Page for Vec<SqlRow> {
    fn for_each_row(&self, f: &mut dyn FnMut(&[Value])) {
        self.iter().for_each(|row| f(&row.values));
    }
    fn for_each_row_mut(&mut self, f: &mut dyn FnMut(&mut [Value])) {
        self.iter_mut().for_each(|row| f(&mut row.values));
    }
}

impl Page for SqlResult {
    fn for_each_row(&self, f: &mut dyn FnMut(&[Value])) {
        self.rows.for_each_row(f);
    }
    fn for_each_row_mut(&mut self, f: &mut dyn FnMut(&mut [Value])) {
        self.rows.for_each_row_mut(f);
    }
}

impl Page for Outcome {
    fn for_each_row(&self, f: &mut dyn FnMut(&[Value])) {
        self.rows.for_each_row(f);
    }
    fn for_each_row_mut(&mut self, f: &mut dyn FnMut(&mut [Value])) {
        self.rows.for_each_row_mut(f);
    }
}

impl Page for TransactionOutcome {
    fn for_each_row(&self, f: &mut dyn FnMut(&[Value])) {
        for emitted in &self.emitted {
            match emitted {
                EmittedValue::Scalar(value) => f(std::slice::from_ref(value)),
                EmittedValue::Row { row, .. } => f(&row.values),
                EmittedValue::Rows { rows, .. } => rows.for_each_row(f),
            }
        }
    }
    fn for_each_row_mut(&mut self, f: &mut dyn FnMut(&mut [Value])) {
        for emitted in &mut self.emitted {
            match emitted {
                EmittedValue::Scalar(value) => f(std::slice::from_mut(value)),
                EmittedValue::Row { row, .. } => f(&mut row.values),
                EmittedValue::Rows { rows, .. } => rows.for_each_row_mut(f),
            }
        }
    }
}

/// The refusal of a plain exit in a task session.
pub(crate) fn refusal() -> CoreError {
    CoreError::Task(TaskError::NotAllowedInTaskSession(StatementClass::Other))
}

impl SqlSession {
    /// The tag for a statement starting now; `None` outside a task session
    /// (no allocation).
    pub(crate) fn release_tag(&self) -> Option<Arc<ReleaseTag>> {
        self.task
            .as_ref()
            .map(|ctx| Arc::new(ReleaseTag::new(ctx, OutputPlan::default())))
    }

    /// Guard for the plain exits: a task session must use the twins.
    pub(crate) fn refuse_task_session(&self) -> Result<(), CoreError> {
        match self.task {
            None => Ok(()),
            Some(_) => Err(refusal()),
        }
    }

    /// Releases one page (named `release_page` because `release` is the
    /// savepoint RELEASE). Without a task this is a move: no lock, catalog
    /// read, transaction or allocation. A task's page fails closed until the
    /// release protocol (R3-308) lands.
    pub fn release_page<T: Page>(&self, page: Unreleased<T>) -> Result<Released<T>, CoreError> {
        match page.tag {
            None => Ok(Released(page.value)),
            Some(_) => Err(CoreError::Unsupported),
        }
    }

    /// Validity only, for re-serving an already charged page. No-op until R3-308.
    pub fn still_valid(&self) -> Result<(), CoreError> {
        Ok(())
    }

    /// Read-log finalisation of a task statement. No-op until R3-308/R3-309.
    pub fn finish_task_statement(&self, _outcome: Result<(), &CoreError>) {}
}

#[cfg(test)]
#[path = "release_tests.rs"]
mod tests;
