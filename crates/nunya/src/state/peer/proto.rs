//! Strict wire messages. Authentication material deliberately has no Debug implementation.
use crate::{
    state::model::*,
    token::{ScopeId, SessionId},
};
use serde::{Deserialize, Serialize};
macro_rules! message {
    ($(#[$meta:meta])* $name:ident { $($field:ident : $ty:ty),* $(,)? }) => {
        $(#[$meta])* #[derive(Serialize, Deserialize)] #[serde(deny_unknown_fields)]
        pub struct $name { $(#[doc = stringify!($field)] pub $field: $ty),* }
    };
}
message!(/// Session lookup request.
Lookup { session_id: SessionId });
message!(/// Scope lookup request.
ScopeGet { scope_id: ScopeId });
/// Session state and credential digests; never client secrets.
#[derive(Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum LookupResult {
    /// Live session authentication material.
    Active {
        /// Session row.
        session: Box<AccessSession>,
        /// Stored credential verifiers and hashes.
        credentials: Vec<SessionCredential>,
        /// SCRAM rotation selection from the existing login resolver.
        verifiers: Vec<(CredentialId, Verifier)>,
    },
    /// No such session.
    Unknown,
    /// Unusable session.
    Ended {
        /// Ending reason.
        reason: Option<EndReason>,
    },
}
message!(/// Optional derivation row.
ScopeResult { scope: Option<TokenScope> });
message!(/// Change cursor request; cursors include a leader-local random epoch.
ChangesRequest { since: u64, touched: Vec<SessionId> });
message!(/// One ended session.
Change { session_id: SessionId, kind: ChangeKind });
message!(/// Bounded invalidation feed.
Changes { reset: bool, changes: Vec<Change>, next: u64, generation: u64 });
message!(/// Reserve request; limits are server-owned.
Reserve { session_id: SessionId, domain: String, want: u64 });
message!(/// Reserved durable counts.
Reservation { granted: u64, batch_id: String, expires_ms: u64 });
message!(/// Return the unused portion of one batch.
Return { batch_id: String, unused: u64 });
/// Activity event; ending a connection never revokes a session.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Event {
    /// Connection started.
    Start,
    /// Connection ended.
    End,
}
message!(/// Minimal activity report.
Report { session_id: SessionId, event: Event, connections: Option<u64> });
message!(/// Successful empty result.
Ack { ok: bool });
message!(/// Sanitized error envelope.
Failure { error: FailureCode, generation: u64, leader: Option<String> });
/// Closed peer redirection error code.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureCode {
    /// The contacted pod is not leader.
    NotLeader,
}

/// Counter reservation result, including an ended session without a grant.
#[derive(Serialize, Deserialize)]
#[serde(untagged)]
pub enum ReserveResult {
    /// Durable counts reserved.
    Granted(Reservation),
    /// Session no longer authorizes tokenization.
    Ended(Ended),
}
message!(/// Ended-session response.
Ended { state: EndedState });
/// Closed ended-state discriminator.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EndedState {
    /// Session ended.
    Ended,
}

/// Closed invalidation operation.
#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ChangeKind {
    /// Evict authentication and counters without ending connections.
    Refresh {},
    /// End connections with the authority reason.
    End { /// Authority ending reason.
        reason: EndReason },
}
