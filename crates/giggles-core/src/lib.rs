//! Durable local task-repair kernel. See README for authority and durability limits.
// Persisted legacy record types remain available to decode, validate, and refuse
// upgrades of old stores. The executable task API is opt-in.
mod accounting;
mod types;
#[cfg(feature = "legacy-task")]
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
pub use storage::{FenceCause, FenceClass, FenceRecovery, FenceSite, Holder, RocksStatus, StorageStatus};
pub use storage::{
    Admin, CoreError, CrashPoint, DecisionPoint, Engine, IndexInfo, Limits, ReclaimStats,
    RecoveryPoint, SqlIsolation, UpgradePoint, UpgradeReport, WriteFenceStatus,
};
pub use storage::{SequenceDefaultContext, SequenceDefaultGrants};
pub use storage::{CatalogMetadata, PrivilegeCallback, PrivilegeMetadata, SequenceAuthority, Cursor, PreparedCatalogState, SqlReplyCheck, SqlSession, SqlTableLockMode, TableAuthoritySnapshot};
mod sqlstate;
pub use sqlstate::sqlstate;
#[cfg(feature = "legacy-task")]
mod sql;
#[cfg(feature = "legacy-task")]
pub use sql::parse_repair;
#[cfg(feature = "legacy-task")]
pub use storage::Executor;

pub mod local;
pub mod operations;
// The trigger catalog and SPI adapter are integrated in the next stack.
#[allow(dead_code)]
pub(crate) mod plpgsql;
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

/// Test-only counters: transaction, cluster and local metadata acquisitions,
/// provenance reads, and transactions opened for local metadata, respectively.
#[cfg(feature = "test-metadata-counts")]
#[doc(hidden)]
pub mod metadata_read_counts {
    thread_local! {
        static COUNTS: std::cell::Cell<[usize; 5]> = const { std::cell::Cell::new([0; 5]) };
    }
    pub(crate) fn increment(index: usize) {
        COUNTS.with(|counts| { let mut n = counts.get(); n[index] += 1; counts.set(n); });
    }
    pub fn take() -> [usize; 5] {
        COUNTS.with(|counts| counts.replace([0; 5]))
    }
}

mod durable;
pub use durable::{DurabilityKind, DurableTracker, LiveToken};

#[cfg(any(test, feature = "test-commit-sync"))]
mod commit_sync_hook;
#[cfg(any(test, feature = "test-commit-sync"))]
pub use commit_sync_hook::CommitSyncHook;
