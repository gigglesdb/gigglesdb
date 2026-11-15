//! SQL dialect lowering contract.
use crate::{
    error::Refusal, ir::ResolvedStatement, policy::Policy, schema::Snapshot, value::ColumnType,
};
/// GoogleSQL lowering support.
pub mod googlesql;
/// PostgreSQL lowering support.
pub mod pg;
/// Spanner struct support.
pub mod spanner_struct;
/// Lower a client query to the dialect-neutral statement representation.
pub trait Lowering {
    /// Split and lower a query, refusing the whole string if any statement cannot be analysed.
    fn lower(
        &self,
        sql: &str,
        snapshot: &Snapshot,
        policy: &Policy,
        param_types: &[Option<ColumnType>],
    ) -> Result<Vec<ResolvedStatement>, Refusal>;
}
