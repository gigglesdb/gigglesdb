//! Multi-range cluster mode: every node is a SQL gateway over range groups (GD-0075 Phase 7).
#[cfg(feature = "engine-rocksdb")]
pub mod cli;
pub mod config;
#[cfg(feature = "engine-rocksdb")]
pub mod runtime;
