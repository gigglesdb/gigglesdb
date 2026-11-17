//! Durable local task-repair kernel. See README for authority and durability limits.
mod accounting;
mod types;
pub use accounting::{TaskAccounting, TaskResources};
pub use types::*;
pub mod pg_serde;
mod policy;
mod scalar;
mod storage;
pub use storage::replica;
pub use storage::range_api;
#[cfg(feature = "engine-rocksdb")]
pub use storage::cluster_node;
#[cfg(feature = "engine-rocksdb")]
pub use storage::replicated;
pub use storage::roles;
pub use storage::{
    Admin, CoreError, CrashPoint, DecisionPoint, Engine, Executor, IndexInfo, Limits, ReclaimStats,
    RecoveryPoint, SqlIsolation, UpgradePoint, UpgradeReport,
};
pub use storage::{Cursor, SqlSession, SqlTableLockMode};
mod sqlstate;
pub use sqlstate::sqlstate;
mod sql;
pub use sql::parse_repair;

pub mod local;
pub mod operations;
pub mod relational;
pub mod task;
pub use task::release::{ReleaseTag, Released, Unreleased};

pub mod cancellation;
pub use cancellation::Cancellation;

#[cfg(test)]
fn test_artifact_root() -> std::path::PathBuf {
    std::env::var_os("GIGGLES_TEST_ARTIFACTS")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("giggles-core-artifacts"))
}
