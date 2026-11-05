//! Bounded local full snapshot backup catalog and fresh identity restoration.
//! Integrity checks are not authentication. See the product README for limits.
mod catalog;
mod format;
mod fs;
mod restore;
pub use catalog::*;
pub use format::*;
pub use restore::*;

/// Errors intentionally exclude paths, SQL and stored values from Display.
#[derive(Debug)]
pub enum OpsError {
    /// An operating system operation failed. Source is available to trusted callers.
    Io(std::io::Error),
    /// The core rejected the operation.
    Core(giggles_core::CoreError),
    /// Malformed or unsupported catalog data.
    Invalid,
    /// An explicit resource ceiling was exceeded.
    Limit,
    /// A path contains a symbolic link or reparse point.
    UnsafePath,
    /// Existing destinations are never replaced.
    Exists,
    /// Another catalog writer holds the nonblocking admission lock.
    Busy,
    /// A deliberate trusted test fault occurred.
    Injected,
}
impl OpsError {
    /// Stable sanitized CLI classification.
    pub fn code(&self) -> &'static str {
        match self {
            Self::Io(_) => "io",
            Self::Core(_) => "core",
            Self::Invalid => "invalid",
            Self::Limit => "limit",
            Self::UnsafePath => "unsafe_path",
            Self::Exists => "exists",
            Self::Busy => "busy",
            Self::Injected => "injected",
        }
    }
}
impl std::fmt::Display for OpsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.code())
    }
}
impl std::error::Error for OpsError {}
impl From<std::io::Error> for OpsError {
    fn from(e: std::io::Error) -> Self {
        if e.kind() == std::io::ErrorKind::AlreadyExists {
            Self::Exists
        } else {
            Self::Io(e)
        }
    }
}
impl From<giggles_core::CoreError> for OpsError {
    fn from(e: giggles_core::CoreError) -> Self {
        Self::Core(e)
    }
}
/// Result returned by operations APIs.
pub type Result<T> = std::result::Result<T, OpsError>;
/// Reject linked/reparse source ancestors and nonregular core data files before
/// passing a trusted local source to Admin::open. Does not acquire a database lock.
pub fn validate_source(path: &std::path::Path) -> Result<()> {
    fs::checked(path)?;
    let _file = fs::open(&path.join("data.redb"))?;
    Ok(())
}
