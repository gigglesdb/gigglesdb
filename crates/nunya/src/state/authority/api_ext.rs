//! Management projections and session mutation retry records.
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
    /// Renew a session with a caller-local retry record and credential deduplication.
    pub fn renew_idempotent(
        &self,
        principal: PrincipalId,
        id: SessionId,
        mode: Option<Mode>,
        add_credential: Option<(Verifier, [u8; 32])>,
        key: &str,
    ) -> Result<AccessSession, AuthorityError> {
        let key = format!("\0renew:{key}");
        let (verifier, digest) = match &add_credential {
            Some((v, h)) => (Some(v), Some(h)),
            None => (None, None),
        };
        let request_hash = hash(&("renew", id, mode, verifier, digest));
        if let Some(s) = self.session_retry(principal, &key, request_hash)? {
            return if s.state == SessionState::Active {
                Ok(s)
            } else {
                Err(AuthorityError::SessionEnded)
            };
        }
        let add_credential = add_credential.filter(|(_, digest)| {
            !self
                .view
                .read()
                .unwrap()
                .session_credentials
                .values()
                .any(|c| {
                    c.session == id
                        && c.state == CredentialState::Active
                        && c.secret_hash == *digest
                })
        });
        let session = self.renew(principal, id, mode, add_credential)?;
        // Write after the mutation: state-setting replay is a no-op, including deduplicated credentials.
        self.record_session_retry(principal, key, request_hash, id)?;
        Ok(session)
    }
    /// Revoke a session with a caller-local retry record.
    pub fn revoke_idempotent(
        &self,
        owner: PrincipalId,
        id: SessionId,
        reason: EndReason,
        key: &str,
    ) -> Result<AccessSession, AuthorityError> {
        let key = format!("\0revoke:{key}");
        let request_hash = hash(&("revoke", id));
        if let Some(s) = self.session_retry(owner, &key, request_hash)? {
            return Ok(s);
        }
        self.revoke(id, reason)?;
        // Write after the mutation: revocation sets state, so replay is a no-op.
        self.record_session_retry(owner, key, request_hash, id)?;
        self.session(id).ok_or(AuthorityError::NotFound)
    }
    fn session_retry(
        &self,
        owner: PrincipalId,
        key: &str,
        request_hash: [u8; 32],
    ) -> Result<Option<AccessSession>, AuthorityError> {
        let v = self.view.read().unwrap();
        let Some(record) = v.idempotency.get(&(owner, key.to_owned())) else {
            return Ok(None);
        };
        if record.request_hash != request_hash {
            return Err(AuthorityError::IdempotencyConflict);
        }
        SessionId::from_hex(&record.result)
            .and_then(|id| v.sessions.get(&id).cloned())
            .map(Some)
            .ok_or(AuthorityError::Store)
    }
    fn record_session_retry(
        &self,
        owner: PrincipalId,
        key: String,
        request_hash: [u8; 32],
        id: SessionId,
    ) -> Result<(), AuthorityError> {
        let record = IdempotencyRecord {
            principal: owner,
            key: key.clone(),
            request_hash,
            result: id.to_hex(),
        };
        let mut tx = self.store.write()?;
        tx.put(
            Table::Idempotency,
            &keys::idempotency(owner, &key),
            &encode(&record),
        )?;
        tx.commit()?;
        self.view
            .write()
            .unwrap()
            .idempotency
            .insert((owner, key), record);
        Ok(())
    }
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

#[cfg(test)]
mod tests {
    use super::super::tests::{Fixture, verifier};
    use super::*;

    #[test]
    fn renewal_retry_deduplicates_credentials_and_survives_reload() {
        let f = Fixture::new();
        let p = f.principal("renew-owner", Mode::ReadWrite);
        let s = f.acquire(p.id, "acquire");
        let credential = || Some((verifier(), Sha256::digest(b"added-secret").into()));
        let first =
            f.a.renew_idempotent(p.id, s.id, Some(Mode::ReadOnly), credential(), "retry")
                .unwrap();
        let retry =
            f.a.renew_idempotent(p.id, s.id, Some(Mode::ReadOnly), credential(), "retry")
                .unwrap();
        assert_eq!(encode(&first), encode(&retry));
        assert_eq!(f.a.view.read().unwrap().session_credentials.len(), 2);
        // Simulate a successful mutation whose separate retry-record write failed.
        f.a.view
            .write()
            .unwrap()
            .idempotency
            .remove(&(p.id, "\0renew:retry".into()));
        f.a.renew_idempotent(p.id, s.id, Some(Mode::ReadOnly), credential(), "retry")
            .unwrap();
        assert_eq!(f.a.view.read().unwrap().session_credentials.len(), 2);
        let loaded = Authority::load(f.store.clone(), SessionLimits::default()).unwrap();
        loaded
            .renew_idempotent(p.id, s.id, Some(Mode::ReadOnly), credential(), "retry")
            .unwrap();
        assert_eq!(loaded.view.read().unwrap().session_credentials.len(), 2);
        assert_eq!(
            loaded
                .renew_idempotent(p.id, s.id, None, None, "retry")
                .unwrap_err(),
            AuthorityError::IdempotencyConflict
        );
        loaded
            .revoke_idempotent(p.id, s.id, EndReason::Requested, "retry")
            .unwrap();
        assert_eq!(
            loaded
                .renew_idempotent(p.id, s.id, Some(Mode::ReadOnly), credential(), "retry")
                .unwrap_err(),
            AuthorityError::SessionEnded
        );
        let loaded = Authority::load(f.store.clone(), SessionLimits::default()).unwrap();
        let ended = loaded
            .revoke_idempotent(p.id, s.id, EndReason::Requested, "retry")
            .unwrap();
        assert_eq!(ended.revoke_reason, Some(EndReason::Requested));
    }
}
