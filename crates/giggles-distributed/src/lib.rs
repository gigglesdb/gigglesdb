//! Bounded range lifecycle runtime. Owner controls are trusted local input.
pub mod machine;
pub mod node;
pub mod protected;
pub mod runtime;
pub mod store;
pub mod transaction;
pub mod transport;
pub use machine::*;
pub use node::Node;
pub use store::Store;
pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
pub const VOTERS: [u64; 3] = [1, 2, 3];
pub const MAX_GROUPS: usize = 16;
pub const MAX_COMMAND: usize = 4 * 1024 * 1024;
pub const MAX_FRAME: usize = 24 * 1024 * 1024;
