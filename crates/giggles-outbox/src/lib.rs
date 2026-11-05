//! Bounded, trusted local transactional outbox over GigglesDB SQL.
//!
//! Application mutations and enqueue share one explicit transaction. Delivery is
//! at least once: a stable effect ID must reach an idempotent receiver. A database
//! enqueue receipt is distinct from a downstream acknowledgment. Raw Admin and
//! filesystem owners are trusted. See the package README for time and retry rules.
#![doc = include_str!("../README.md")]
mod state;
mod store;
pub use state::*;
pub use store::*;

/// Public core values and bounded application scan description.
pub use giggles_core::relational::{Scan, SqlResult, Value};

/// Failures are deliberately coarse; core errors contain no echoed SQL here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Invalid,
    Limit,
    Conflict,
    StaleClaim,
    Time,
    Overflow,
    Corrupt,
    Forbidden,
    Aborted,
    Core,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
pub(crate) type Result<T> = std::result::Result<T, Error>;
