//! Materialized query operators. Scalar semantics remain in the shared expression engine.
use super::*;
mod ast;
mod bind;
mod cte;
mod dml;
mod exec;
pub(super) mod join;
mod parse;
mod setops;
pub(super) mod subquery;
mod srf;
pub(in crate::relational) use ast::Plan;
use ast::*;

#[cfg(test)]
mod tests;

/// A planner decline preserves the original parser error; a recognized error does not.
#[derive(Debug)]
pub(in crate::relational) enum ParseError {
    Decline,
    Definite(SqlError),
}
impl From<SqlError> for ParseError {
    fn from(error: SqlError) -> Self {
        // Only recognition failures permit a decline. Resource and semantic
        // errors from the shared parser must survive planner fallback unchanged.
        match error {
            SqlError::Unsupported | SqlError::Syntax | SqlError::Reserved => Self::Decline,
            error => Self::Definite(error),
        }
    }
}
