//! Durable membership prerequisite. See the package README for adapter obligations.
//!
//! ```
//! use giggles_membership::{Identity, Membership};
//! let m = Membership::new(Identity { group: 7, owner_epoch: 1 }, vec![1,2,3], 8)?;
//! assert_eq!(m.epoch, 1);
//! # Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
//! ```
#![forbid(unsafe_code)]

pub mod durable;
mod membership;
pub use membership::*;
pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
