//! Read-only management projections without credential material.
use super::*;

/// Non-secret principal credential metadata.
#[derive(Clone, Debug, serde::Serialize)]
pub struct CredentialSummary {
    /// Credential identifier.
    pub id: CredentialId,
    /// Credential lifecycle state.
    pub state: CredentialState,
    /// Optional expiration time.
    pub expires_ms: Option<u64>,
}
impl Authority {
    /// List principals in name order.
    pub fn principals(&self) -> Vec<Principal> {
        let mut rows: Vec<_> = self
            .view
            .read()
            .unwrap()
            .principals
            .values()
            .cloned()
            .collect();
        rows.sort_by(|a, b| a.name.cmp(&b.name));
        rows
    }
    /// List a principal's grants in route order.
    pub fn grants_of(&self, id: PrincipalId) -> Vec<(RouteId, Mode)> {
        let mut rows: Vec<_> = self
            .view
            .read()
            .unwrap()
            .grants
            .iter()
            .filter(|((p, _), _)| *p == id)
            .map(|((_, r), m)| (r.clone(), *m))
            .collect();
        rows.sort_by(|a, b| a.0.cmp(&b.0));
        rows
    }
    /// List credential summaries without verifiers.
    pub fn credentials_of(&self, id: PrincipalId) -> Vec<CredentialSummary> {
        let mut rows: Vec<_> = self
            .view
            .read()
            .unwrap()
            .credentials
            .values()
            .filter(|c| c.principal == id)
            .map(|c| CredentialSummary {
                id: c.id,
                state: c.state,
                expires_ms: c.expires_ms,
            })
            .collect();
        rows.sort_by_key(|c| c.id);
        rows
    }
    /// List all sessions in creation time and identifier order.
    pub fn all_sessions(&self) -> Vec<AccessSession> {
        let mut rows: Vec<_> = self
            .view
            .read()
            .unwrap()
            .sessions
            .values()
            .cloned()
            .collect();
        rows.sort_by_key(|s| (s.created_ms, s.id));
        rows
    }
    /// Return the store deployment identifier.
    pub fn deployment_id(&self) -> [u8; 16] {
        self.store.meta().deployment_id
    }
    /// Return the durable store format version.
    pub fn store_format_version(&self) -> u32 {
        self.store.meta().format_version
    }
    /// Whether the session retains its original deployment and route scope.
    pub fn scope_continuous(&self, session: &AccessSession) -> bool {
        self.view
            .read()
            .unwrap()
            .scopes
            .get(&session.scope)
            .is_some_and(|scope| {
                scope.deployment_id == self.deployment_id() && scope.route == session.route
            })
    }
}
