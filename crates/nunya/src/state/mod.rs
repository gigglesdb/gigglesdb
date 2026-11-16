//! Durable gateway state with opaque transactional records.
/// Domain records and their storage encoding.
pub mod model;
/// State table definitions and root-key continuity derivation.
pub mod schema;
/// Transactional state storage and ownership.
pub mod store;

/// In-memory authoritative view with write-through.
pub mod authority;
