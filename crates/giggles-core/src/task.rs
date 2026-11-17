//! Delegated agent tasks (GD-0061, R3): definitions and their catalog records.
//!
//! New task types live here so they never collide with the legacy prototype
//! names (`crate::types::TaskId`, `policy.rs`, `accounting.rs`).
pub mod admit;
// R3-611: consumed by protected_commit (R3-615+), the preview store and undo.
#[allow(dead_code)]
pub(crate) mod authority;
pub mod catalog;
// R3-615: the protected commit; the agent seam (R3-207) calls it.
#[allow(dead_code)]
pub(crate) mod commit;
pub mod columns;
pub mod context;
// R3-614: consumed by protected_commit (R3-615+).
#[allow(dead_code)]
pub(crate) mod decide;
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
// R3-613: consumed by protected_commit (R3-615+) and outcome lookup.
#[allow(dead_code)]
pub(crate) mod receipt;
pub mod readlog;
pub mod release;
// R3-622: driven by the v2 maintenance thread.
pub(crate) mod retention;
pub mod rows;
pub mod statements;
#[cfg(test)]
mod statements_tests;

pub use admit::StatementClass;
// R3-615 through the agent seam (R3-207): the server's direct protected write.
pub use commit::{inspect_outcome, outcome, protected_commit, Actor, ProtectedRequest};
// R3-619: the protected commit's crash seams, armed by `Admin::set_protected_point`.
#[doc(hidden)]
pub use commit::ProtectedPoint;
pub use ledger::{OperationId, Outcome};
pub use receipt::Receipt;
pub use readlog::{fingerprint_and_cap, show_reads, ReadRecord, Reconciliation};
pub use release::{Page, ReleaseTag, Released, Unreleased};
pub use retention::{RetentionConfig, RetentionStats};
pub use error::{Budget, DenyRule, TaskError};
