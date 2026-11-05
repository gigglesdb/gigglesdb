//! Normalized offline rehearsal, not a qualified ScyllaDB migration adapter.
//!
//! Expiry is derived only from explicit normalized source inputs:
//! ```
//! use giggles_migrate::expiry_from_ttl;
//! assert_eq!(expiry_from_ttl(10, 2)?, 2_000_010);
//! assert!(expiry_from_ttl(i64::MAX, 1).is_err());
//! # Ok::<(), giggles_migrate::Error>(())
//! ```
pub mod destination;
pub mod model;
pub mod reconcile;
pub use destination::*;
pub use model::*;
pub use reconcile::*;

/// Fail-closed validation, storage or comparison error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error(pub String);
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for Error {}
impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Self(e.to_string())
    }
}
impl From<giggles_core::CoreError> for Error {
    fn from(e: giggles_core::CoreError) -> Self {
        Self(format!("SQL: {e:?}"))
    }
}
impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self(e.to_string())
    }
}
pub type Result<T> = std::result::Result<T, Error>;
pub(crate) fn require(ok: bool, message: &str) -> Result<()> {
    if ok {
        Ok(())
    } else {
        Err(Error(message.into()))
    }
}
