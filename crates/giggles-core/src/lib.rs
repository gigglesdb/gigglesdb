//! Durable local task-repair kernel. See README for authority and durability limits.
mod accounting;
mod types;
pub use accounting::{TaskAccounting, TaskResources};
pub use types::*;
pub mod pg_serde;
mod policy;
mod scalar;
mod storage;
pub use storage::SqlSession;
pub use storage::replica;
pub use storage::roles;
pub use storage::{
    Admin, CoreError, CrashPoint, DecisionPoint, Executor, IndexInfo, Limits, RecoveryPoint,
};
mod sql;
pub use sql::parse_repair;

pub mod local;
pub mod operations;
pub mod relational;

pub mod cancellation;
pub use cancellation::Cancellation;

#[cfg(test)]
fn test_artifact_root() -> std::path::PathBuf {
    std::env::var_os("GIGGLES_TEST_ARTIFACTS")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("giggles-core-artifacts"))
}
