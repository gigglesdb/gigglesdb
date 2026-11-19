//! Durable gateway state, authority and connection tracking.
/// Domain records and their storage encoding.
pub mod model;
/// Open connections by access session, with a cancellation signal per connection.
pub mod registry;
/// State table definitions and root-key continuity derivation.
pub mod schema;
/// Transactional state storage and ownership.
pub mod store;

/// In-memory authoritative view with write-through.
pub mod authority;

/// Service-lifetime state ownership and revocation wiring.
pub mod runtime;

/// Serving-only state interface.
pub mod serving_state;

/// Authenticated leader state transport.
#[cfg(feature = "kubernetes")]
pub mod peer;
