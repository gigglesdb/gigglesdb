//! Delegated agent tasks (GD-0061, R3): definitions and their catalog records.
//!
//! New task types live here so they never collide with the legacy prototype
//! names (`crate::types::TaskId`, `policy.rs`, `accounting.rs`).
pub mod admit;
pub mod catalog;
pub mod columns;
pub mod context;
pub mod definition;
pub mod effects;
pub mod error;
// Consumed by the protected commit path (R3-613+); until then only its tests use it.
#[allow(dead_code)]
pub(crate) mod ledger;
// Most codecs are consumed by later R3 items (release, authority, ledger,
// receipt, preview, approval, read log); until they land only the format gates use them.
#[allow(dead_code)]
pub(crate) mod records;
// R3-701 / R3-801: consumed by the preview store and apply (R3-703..R3-706)
// and the undo dry run and commit (R3-802..R3-804).
#[allow(dead_code)]
pub(crate) mod preview;
#[allow(dead_code)]
pub(crate) mod undo;
pub mod statements;
#[cfg(test)]
mod statements_tests;

pub use admit::StatementClass;
pub use error::{Budget, DenyRule, TaskError};
