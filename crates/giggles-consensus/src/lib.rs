//! Bounded single-range Raft integration. Local tests are not HA qualification.
pub mod authority;
pub mod native;
pub mod node;
pub mod store;
pub mod transport;
pub use giggles_core::replica::{Command, Image, TaskAction};
pub use node::{Applied, Node, Output};
pub use store::Store;
pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
pub const MAX_FRAME: usize = 24 * 1024 * 1024;
pub const MAX_COMMAND: usize = 1024 * 1024;
pub const VOTERS: [u64; 3] = [1, 2, 3];
