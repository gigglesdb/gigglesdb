//! Three-voter Raft replication for the v2 engine (GD-0074 R4).
//! giggles-core owns all storage; this crate owns the Raft log, the node
//! driver, transport, snapshots and membership. See giggles_core::replicated.
pub mod admin;
pub mod barrier;
pub mod cluster;
pub mod core_store;
pub mod entry;
pub mod error;
pub mod failpoint;
pub mod log_store;
pub mod membership;
pub mod node;
pub mod replace;
pub mod role;
pub mod sink;
pub mod snapshot;
pub mod state;
pub mod store_slot;
pub mod transport;
