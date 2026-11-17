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
