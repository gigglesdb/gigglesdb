//! Authenticated management API.
/// Bearer authentication.
pub mod auth;
/// Synchronous management routing.
pub mod routes;
/// Bounded HTTP management listeners.
pub mod server;
/// Lifecycle audit adapter boundary.
pub mod audit;
/// Management API client.
pub mod client;
