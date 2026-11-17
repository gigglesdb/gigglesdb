//! Materialized query operators. Scalar semantics remain in the shared expression engine.
use super::*;
mod ast;
mod bind;
mod cte;
mod dml;
mod exec;
mod parse;
mod setops;
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
        if error == SqlError::Unsupported {
            Self::Decline
        } else {
            Self::Definite(error)
        }
    }
}
