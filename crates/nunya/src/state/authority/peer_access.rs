//! Small authority operations used exclusively by the leader peer service.
use super::*;
impl Authority {
    /// Current authority clock for counter expiry.
    pub fn peer_now(&self) -> u64 { self.store.now_ms() }
    /// Refresh pod activity before sweeping idle sessions.
    pub fn peer_touch(&self, ids: &[SessionId]) {
        let now = self.store.now_ms();
        let mut v = self.view.write().unwrap();
        for id in ids {
            if let Some(s) = v.sessions.get_mut(id)
                && s.state == SessionState::Active && now < s.expires_ms {
                s.last_used_ms = s.last_used_ms.max(now);
            }
        }
    }
    /// Read live login credentials without refreshing activity.
    pub fn peer_credentials(&self, id: SessionId) -> Option<Vec<SessionCredential>> {
        let row = self.session(id)?;
        self.session_login(id, &row.route)?;
        let now = self.store.now_ms();
        let view = self.view.read().ok()?;
        Some(
            view.session_credentials
                .values()
                .filter(|c| {
                    c.session == id && c.state == CredentialState::Active && now < c.expires_ms
                })
                .cloned()
                .collect(),
        )
    }
    /// Read the bounded change feed without replacing connection termination hooks.
    pub fn peer_changes(&self, since: u64, generation: u64) -> crate::state::peer::Changes {
        self.peer_changes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .read(since, generation)
    }
}
