//! Three-voter Raft replication for the v2 engine (GD-0074 R4).
//! giggles-core owns all storage; this crate owns the Raft log, the node
//! driver, transport, snapshots and membership. See giggles_core::replicated.
pub mod admin;
pub mod barrier;
pub mod bootstrap;
pub mod call_codec;
pub mod client;
pub mod cluster;
pub mod cluster_node;
pub mod core_store;
pub mod directory;
pub mod entry;
pub mod error;
pub mod failpoint;
pub mod liveness;
pub mod group;
pub mod host;
pub mod log_store;
pub mod membership;
pub mod node;
pub mod range_guard;
pub mod range_role;
pub mod range_state;
pub mod replace;
pub mod role;
pub mod route;
pub mod sink;
pub mod snapshot;
pub mod state;
pub mod store_slot;
pub mod transport;
