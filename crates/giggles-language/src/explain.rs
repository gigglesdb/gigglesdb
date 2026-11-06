//! Nonexecuting trusted simple-read explanation through the common typed engine.
use crate::{
    Bindings, Engine, Error, ExplainStatement, LanguageVersion, ReadExplainOutcome, Result,
    execute, parse_explain_with_version,
};
use giggles_core::operations;

pub(crate) fn lower(
    statement: &ExplainStatement,
    version: LanguageVersion,
) -> Result<operations::Request> {
    if !version.supports_explain() || !matches!(statement.read, crate::Statement::Read { .. }) {
        return Err(Error::Unsupported);
    }
    execute::lower_checked(&statement.read, version)
}

impl Engine {
    /// Bind actual arguments against the current committed catalog without executing the read.
    pub fn explain_text(&self, text: &str, bindings: &Bindings) -> Result<ReadExplainOutcome> {
        self.explain(&parse_explain_with_version(text, self.version)?, bindings)
    }

    /// No prepared credentials or cached catalog IDs are retained by this operation.
    pub fn explain(
        &self,
        statement: &ExplainStatement,
        bindings: &Bindings,
    ) -> Result<ReadExplainOutcome> {
        let request = lower(statement, self.version)?;
        self.inner
            .explain_read(&request, bindings)
            .map_err(execute::error)
    }
}
