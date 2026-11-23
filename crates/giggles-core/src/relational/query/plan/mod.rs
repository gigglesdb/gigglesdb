//! Materialized query operators. Scalar semantics remain in the shared expression engine.
use super::*;
mod ast;
mod bind;
mod cte;
pub(super) mod correlation;
mod dml;
mod mutation;
pub(in crate::relational) use mutation::assignment_target_starts;
mod merge;
mod exec;
pub(super) mod join;
mod parse;
mod paging;
mod group_sets;
mod insert_source;
mod setops;
pub(super) mod subquery;
mod json_srf;
mod srf;
pub(super) use srf::element as array_element_type;
mod window;
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

pub(in crate::relational) use super::returning_images::record;
