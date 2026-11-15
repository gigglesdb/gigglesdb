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
    /// Simple reads only; typed Query statements return Unsupported.
    /// Bind actual arguments against the current committed catalog without executing the read.
    pub fn explain_text(&self, text: &str, bindings: &Bindings) -> Result<ReadExplainOutcome> {
        self.explain(&parse_explain_with_version(text, self.version)?, bindings)
    }

    /// Simple reads only; use explain_query for typed Query statements.
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

impl Engine {
    /// Describe a typed Query without executing it; available in explicit 0.20.
    pub fn explain_query_text(&self, text: &str, bindings: &Bindings) -> Result<crate::QueryExplainOutcome> {
        self.explain_query(&parse_explain_with_version(text, self.version)?, bindings)
    }
    /// Return typed output columns and source names as written, without scanning rows.
    pub fn explain_query(&self, statement: &ExplainStatement, bindings: &Bindings) -> Result<crate::QueryExplainOutcome> {
        let crate::Statement::Query(query) = &statement.read else { return Err(Error::Unsupported); };
        let columns = self.describe_query(query, bindings)?;
        let sources = std::iter::once(&query.source).chain(query.join.iter().map(|j| &j.source))
            .map(|s| s.table.clone()).collect();
        Ok(crate::QueryExplainOutcome { columns, sources })
    }
}
