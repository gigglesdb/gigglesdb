//! Synchronous serving-only state operations; peer transport runs on a dedicated runtime.
use super::{
    authority::{
        AdmitError, Admitted, Authority, Operation, SessionLogin, TokenizeError, TokenizeLimits,
    },
    model::*,
};
use crate::{
    policy::DomainName,
    token::{ScopeId, SessionId},
};
/// State operations available to protocol serving paths.
pub trait ServingState: Send + Sync {
    /// Resolve live login material.
    fn session_login(&self, id: SessionId, route: &RouteId) -> Option<SessionLogin>;
    /// Verify a client secret locally.
    fn session_secret_credential(&self, id: SessionId, secret: &[u8]) -> Option<CredentialId>;
    /// Authorize one operation.
    fn admit(&self, id: SessionId, op: Operation) -> Result<Admitted, AdmitError>;
    /// Consume one tokenization allowance.
    fn record_tokenization(
        &self,
        id: SessionId,
        domain: &DomainName,
        limits: &TokenizeLimits,
    ) -> Result<(), TokenizeError>;
    /// Read a session snapshot.
    fn session(&self, id: SessionId) -> Option<AccessSession>;
    /// Read scope derivation metadata.
    fn scope(&self, id: ScopeId) -> Option<TokenScope>;
    /// Report a connection start or end, best effort.
    fn report(&self, _id: SessionId, _start: bool) {}
    /// Whether peer state is currently usable.
    fn available(&self) -> bool {
        true
    }
}
impl ServingState for Authority {
    fn session_login(&self, id: SessionId, route: &RouteId) -> Option<SessionLogin> {
        Authority::session_login(self, id, route)
    }
    fn session_secret_credential(&self, id: SessionId, secret: &[u8]) -> Option<CredentialId> {
        Authority::session_secret_credential(self, id, secret)
    }
    fn admit(&self, id: SessionId, op: Operation) -> Result<Admitted, AdmitError> {
        Authority::admit(self, id, op)
    }
    fn record_tokenization(
        &self,
        id: SessionId,
        domain: &DomainName,
        limits: &TokenizeLimits,
    ) -> Result<(), TokenizeError> {
        Authority::record_tokenization(self, id, domain, limits)
    }
    fn session(&self, id: SessionId) -> Option<AccessSession> {
        Authority::session(self, id)
    }
    fn scope(&self, id: ScopeId) -> Option<TokenScope> {
        Authority::scope(self, id)
    }
}
/// Reports connection close on every exit path.
pub struct ConnectionReport<'a> {
    state: &'a dyn ServingState,
    id: SessionId,
}
impl<'a> ConnectionReport<'a> {
    /// Retain a close hook for a connection whose start was already reported.
    pub fn closing(state: &'a dyn ServingState, id: SessionId) -> Self {
        Self { state, id }
    }
    /// Report the first admitted connection and retain its close hook.
    pub fn start(state: &'a dyn ServingState, id: SessionId) -> Self {
        state.report(id, true);
        Self { state, id }
    }
}
impl Drop for ConnectionReport<'_> {
    fn drop(&mut self) {
        self.state.report(self.id, false);
    }
}
