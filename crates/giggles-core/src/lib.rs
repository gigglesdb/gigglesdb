//! Durable local task-repair kernel. See README for authority and durability limits.
mod types;
pub use types::*;
mod policy;
mod storage;
pub use storage::{Admin, CoreError, CrashPoint, Executor, Limits, RecoveryPoint};
mod sql;
pub use sql::parse_repair;

pub mod local;
