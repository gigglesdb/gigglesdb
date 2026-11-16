//! Tokenizing database gateway shared types and modules.
/// Administration HTTP endpoint.
pub mod admin;
/// Operator configuration.
pub mod config;
/// Cli support.
pub mod cli;
/// Dialect support.
pub mod dialect;
/// Error support.
pub mod error;
/// Ir support.
pub mod ir;
/// Pgwire support.
pub mod pgwire;
/// Policy support.
pub mod policy;
/// Readlog support.
pub mod readlog;
/// Schema support.
pub mod schema;
/// Spanner support.
pub mod spanner;
/// Durable state, authority and connection registry.
pub mod state;
/// Legacy task store used only by the old CLI task commands; removed when the CLI group is removed.
pub mod task;
/// Token support.
pub mod token;
/// Value support.
pub mod value;

/// Gateway service lifecycle.
pub mod serve;

/// Native Spanner listener lifecycle.
pub mod spanner_serve;

mod reload;
