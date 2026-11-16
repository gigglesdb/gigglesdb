//! The read log implements this trait in B7; each record serializes as one JSON object.
use serde::Serialize;

/// Fixed lifecycle event kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleKind {
    /// A new session was issued.
    Issued,
    /// A session was renewed.
    Renewed,
    /// A session was revoked.
    Revoked,
    /// A principal was disabled.
    PrincipalDisabled,
    /// An ended session could not resume.
    ResumeRefused,
}

/// Non-secret identity of the mutation caller.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Actor {
    /// Session or principal owner.
    Owner,
    /// Store administrator principal in hex.
    Admin(String),
    /// Configuration administrator, without its name.
    ConfigAdmin,
    /// Trusted local socket.
    Socket,
}

/// Data-minimal session lifecycle evidence.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct LifecycleRecord {
    /// Fixed event kind.
    pub kind: LifecycleKind,
    /// Authority clock timestamp.
    pub at_ms: u64,
    /// Affected principal identifier in hex.
    pub principal: String,
    /// Affected session identifier in hex, if any.
    pub session: Option<String>,
    /// Affected route, if any.
    pub route: Option<String>,
    /// Mutation caller category.
    pub actor: Actor,
    /// Fixed, data-free reason.
    pub reason: &'static str,
}

/// The lifecycle sink could not accept a record.
#[derive(Debug, PartialEq, Eq)]
pub struct AuditUnavailable;

/// Small adapter boundary for durable lifecycle recording.
pub trait LifecycleSink: Send + Sync {
    /// Accept one record or refuse without exposing sink details.
    fn record(&self, record: &LifecycleRecord) -> Result<(), AuditUnavailable>;
}
