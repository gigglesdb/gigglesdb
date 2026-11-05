//! Durable local owner controller for one preprovisioned voter replacement.
//!
//! Declared placement is not observed cloud availability. The replicated runtime
//! owns joint consensus and authenticates learner readiness; this crate never
//! accepts owner-supplied applied acknowledgments.
//!
//! ```
//! use giggles_placement::{decode, Code, Request, MAX_BYTES};
//! let error = decode::<Request>(&vec![b' '; MAX_BYTES + 1]).unwrap_err();
//! assert_eq!(error.code, Code::Limit);
//! ```
#![forbid(unsafe_code)]
mod journal;
pub mod owner;
mod plan;
use giggles_distributed::runtime::Observation;
use giggles_membership::Intent;
pub use journal::{Controller, Event, StepResult};
pub use plan::{Plan, Request};
use serde::{Deserialize, Serialize};

/// Maximum serialized request, plan, observation or command size.
pub const MAX_BYTES: usize = 2 * 1024 * 1024;
/// Local controller profile, deliberately narrower than the parent design.
pub const PROFILE: &str = "One explicit preprovisioned learner replacement; declared locality/dependency/capacity only; no cloud availability, security, performance or production qualification.";

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum Code {
    InvalidTopology,
    IneligibleResidency,
    IneligibleProfile,
    DependencyUnknown,
    DependencyFailed,
    CapacityUnknown,
    CapacityFailed,
    StalePlan,
    NoQuorum,
    IdentityConflict,
    Journal,
    Transport,
    Limit,
    Runtime,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Error {
    pub code: Code,
    pub message: String,
}
impl Error {
    pub(crate) fn new(code: Code, message: impl ToString) -> Self {
        Self {
            code,
            message: message.to_string(),
        }
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.code, self.message)
    }
}
impl std::error::Error for Error {}
pub type Result<T> = std::result::Result<T, Error>;

/// Only these mutations are available to placement reconciliation.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum Action {
    Begin { group: u64, intent: Intent },
    Resume { group: u64, id: u64 },
}
/// Implementations must use trusted owner control and a fresh quorum read for
/// `current`. A caller-provided JSON diagnostic is not an authority adapter.
pub trait Owner {
    fn current(&mut self, group: u64) -> Result<Observation>;
    fn act(&mut self, action: &Action) -> Result<()>;
}
/// Decode a bounded strict product document.
pub fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    if bytes.len() > MAX_BYTES {
        return Err(Error::new(Code::Limit, "document bytes"));
    }
    serde_json::from_slice(bytes).map_err(|e| Error::new(Code::StalePlan, e))
}
