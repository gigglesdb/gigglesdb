//! Durable gateway state with opaque transactional records.
/// State table definitions and root-key continuity derivation.
pub mod schema;
/// Transactional state storage and ownership.
pub mod store;
