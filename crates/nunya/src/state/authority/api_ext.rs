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
/// Renewal failure without credential data.
#[derive(Debug, PartialEq, Eq)]
pub enum RenewError {
    /// Existing authority refusal.
    Authority(AuthorityError),
    /// Rotation parameters differ from the active credential.
    RotationSaltMismatch,
}
impl RenewError {
    /// Extract an authority refusal, or none for a rotation parameter mismatch.
    pub fn into_authority(self) -> Option<AuthorityError> {
        match self {
            Self::Authority(error) => Some(error),
            Self::RotationSaltMismatch => None,
        }
    }
}
impl From<AuthorityError> for RenewError {
    fn from(error: AuthorityError) -> Self {
        Self::Authority(error)
    }
}
/// Principal credential addition refusal without credential data.
#[derive(Debug, PartialEq, Eq)]
pub enum AddCredentialError {
    /// Existing authority refusal.
    Authority(AuthorityError),
    /// Four active unexpired credentials already exist.
    CredentialLimit,
}
impl AddCredentialError {
    /// Extract the authority refusal, or none when the credential limit was reached.
    pub fn into_authority(self) -> Option<AuthorityError> {
        match self {
            Self::Authority(e) => Some(e),
            Self::CredentialLimit => None,
        }
    }
}
impl From<AuthorityError> for AddCredentialError {
    fn from(e: AuthorityError) -> Self {
        Self::Authority(e)
    }
}
impl From<StoreError> for AddCredentialError {
    fn from(_: StoreError) -> Self {
        Self::Authority(AuthorityError::Store)
    }
}
impl Authority {
    /// Salt and iterations of the first active, unexpired credential by identifier.
    pub fn session_scram_params(&self, id: SessionId) -> Option<(Vec<u8>, u32)> {
        let now = self.store.now_ms();
        self.view
            .read()
            .unwrap()
            .session_credentials
            .values()
            .filter(|c| c.session == id && c.state == CredentialState::Active && now < c.expires_ms)
            .min_by_key(|c| c.id)
            .map(|c| (c.verifier.0.salt.clone(), c.verifier.0.iterations))
    }

    /// Return the authority clock timestamp.
    pub fn now_ms(&self) -> u64 {
        self.store.now_ms()
    }

    /// Whether an acquisition already has an idempotency record.
    pub fn acquisition_recorded(&self, principal: PrincipalId, key: &str) -> bool {
        self.view
            .read()
            .unwrap()
            .idempotency
            .contains_key(&(principal, key.to_owned()))
    }

    /// Whether a renewal already has an idempotency record.
    pub fn renewal_recorded(&self, principal: PrincipalId, key: &str) -> bool {
        self.acquisition_recorded(principal, &format!("\0renew:{key}"))
    }

    /// Whether a revocation already has an idempotency record.
    pub fn revocation_recorded(&self, principal: PrincipalId, key: &str) -> bool {
        self.acquisition_recorded(principal, &format!("\0revoke:{key}"))
    }

    /// Whether a disablement already has an idempotency record.
    pub fn disablement_recorded(&self, principal: PrincipalId, key: &str) -> bool {
        self.acquisition_recorded(principal, &format!("\0disable:{key}"))
    }

    /// Delete a namespaced retry record from durable storage and the view.
    pub fn forget_retry(
        &self,
        owner: PrincipalId,
        namespaced_key: &str,
    ) -> Result<(), AuthorityError> {
        let _writer = self.writer.lock().unwrap();
        let mut tx = self.store.write()?;
        tx.delete(
            Table::Idempotency,
            &keys::idempotency(owner, namespaced_key),
        )?;
        tx.commit()?;
        self.view
            .write()
            .unwrap()
            .idempotency
            .remove(&(owner, namespaced_key.to_owned()));
        Ok(())
    }

    /// Set principal state with a caller-local retry record.
    pub fn set_principal_enabled_idempotent(
        &self,
        owner: PrincipalId,
        id: PrincipalId,
        enabled: bool,
        key: &str,
    ) -> Result<(), AuthorityError> {
        self.api_principal(id)?;
        let operation = if enabled { "enable" } else { "disable" };
        let key = format!("\0{operation}:{key}");
        let request_hash = hash(&(id, enabled));
        if self.result_retry(owner, &key, request_hash)?.is_some() {
            return Ok(());
        }
        self.set_principal_enabled(id, enabled)?;
        // Write after the mutation: state-setting replay is a no-op.
        self.record_result_retry(owner, key, request_hash, id.to_hex())
    }
    /// Revoke a credential with a caller-local retry record.
    pub fn revoke_principal_credential_idempotent(
        &self,
        owner: PrincipalId,
        id: PrincipalId,
        cid: CredentialId,
        key: &str,
    ) -> Result<CredentialSummary, AuthorityError> {
        self.api_principal(id)?;
        let key = format!("\0credential-revoke:{key}");
        let request_hash = hash(&(id, cid));
        if self.result_retry(owner, &key, request_hash)?.is_none() {
            self.revoke_principal_credential(id, cid)?;
            // Write after the mutation: state-setting replay is a no-op.
            self.record_result_retry(owner, key, request_hash, cid.to_hex())?;
        }
        self.credentials_of(id)
            .into_iter()
            .find(|c| c.id == cid)
            .ok_or(AuthorityError::NotFound)
    }
    /// Replace grants with a caller-local, order-independent retry record.
    pub fn set_grants_idempotent(
        &self,
        owner: PrincipalId,
        id: PrincipalId,
        mut grants: Vec<(RouteId, Mode)>,
        key: &str,
    ) -> Result<(), AuthorityError> {
        grants.sort_by(|a, b| a.0.cmp(&b.0));
        let key = format!("\0grants:{key}");
        let request_hash = hash(&(id, &grants));
        if self.result_retry(owner, &key, request_hash)?.is_some() {
            return Ok(());
        }
        self.set_grants(id, grants)?;
        // Write after the mutation: state-setting replay is a no-op.
        self.record_result_retry(owner, key, request_hash, id.to_hex())
    }
    /// Create a credential with a positive relative expiry and an atomic caller-local retry record.
    pub fn add_principal_credential_idempotent(
        &self,
        owner: PrincipalId,
        id: PrincipalId,
        verifier: Verifier,
        expires_in_ms: Option<u64>,
        key: &str,
    ) -> Result<CredentialSummary, AddCredentialError> {
        let _writer = self.writer.lock().unwrap();
        self.api_principal(id)?;
        let key = format!("\0credential:{key}");
        let request_hash = hash(&(id, &verifier, expires_in_ms));
        if let Some(result) = self.result_retry(owner, &key, request_hash)? {
            let cid = CredentialId::from_hex(&result).ok_or(AuthorityError::Store)?;
            return self
                .credentials_of(id)
                .into_iter()
                .find(|c| c.id == cid)
                .ok_or(AuthorityError::Store)
                .map_err(Into::into);
        }
        let now = self.store.now_ms();
        if self
            .credentials_of(id)
            .iter()
            .filter(|c| c.state == CredentialState::Active && c.expires_ms.is_none_or(|e| now < e))
            .count()
            >= 4
        {
            return Err(AddCredentialError::CredentialLimit);
        }
        let expires_ms = expires_in_ms
            .map(|duration| {
                if duration == 0 {
                    return Err(AuthorityError::LifetimeTooLong);
                }
                self.store
                    .now_ms()
                    .checked_add(duration)
                    .ok_or(AuthorityError::LifetimeTooLong)
            })
            .transpose()?;
        let c = PrincipalCredential {
            id: CredentialId::random(),
            principal: id,
            verifier,
            state: CredentialState::Active,
            expires_ms,
        };
        let record = IdempotencyRecord {
            principal: owner,
            key: key.clone(),
            request_hash,
            result: c.id.to_hex(),
        };
        let mut tx = self.store.write()?;
        tx.put(
            Table::PrincipalCredentials,
            &keys::principal_credential(id, c.id),
            &encode(&c),
        )?;
        tx.put(
            Table::Idempotency,
            &keys::idempotency(owner, &key),
            &encode(&record),
        )?;
        tx.commit()?;
        let summary = CredentialSummary {
            id: c.id,
            state: c.state,
            expires_ms: c.expires_ms,
        };
        let mut v = self.view.write().unwrap();
        v.credentials.insert(c.id, c);
        v.idempotency.insert((owner, key), record);
        Ok(summary)
    }
    fn result_retry(
        &self,
        owner: PrincipalId,
        key: &str,
        request_hash: [u8; 32],
    ) -> Result<Option<String>, AuthorityError> {
        let v = self.view.read().unwrap();
        let Some(record) = v.idempotency.get(&(owner, key.to_owned())) else {
            return Ok(None);
        };
        if record.request_hash != request_hash {
            return Err(AuthorityError::IdempotencyConflict);
        }
        Ok(Some(record.result.clone()))
    }
    /// Renew a session with a caller-local retry record and credential deduplication.
    pub fn renew_idempotent(
        &self,
        principal: PrincipalId,
        id: SessionId,
        mode: Option<Mode>,
        add_credential: Option<(Verifier, [u8; 32])>,
        key: &str,
    ) -> Result<AccessSession, RenewError> {
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
                Err(AuthorityError::SessionEnded.into())
            };
        }
        let session = self.owned_session(principal, id)?;
        if session.state != SessionState::Active || self.store.now_ms() >= session.expires_ms {
            return Err(AuthorityError::SessionEnded.into());
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
        if let Some((verifier, _)) = &add_credential
            && self.session_scram_params(id).as_ref()
                != Some(&(verifier.0.salt.clone(), verifier.0.iterations))
        {
            return Err(RenewError::RotationSaltMismatch);
        }
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
        self.record_result_retry(owner, key, request_hash, id.to_hex())
    }
    fn record_result_retry(
        &self,
        owner: PrincipalId,
        key: String,
        request_hash: [u8; 32],
        result: String,
    ) -> Result<(), AuthorityError> {
        let record = IdempotencyRecord {
            principal: owner,
            key: key.clone(),
            request_hash,
            result,
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
    fn principal_mutation_retries_survive_reload_and_scope_by_owner() {
        let f = Fixture::new();
        let p = f.principal("credential-owner", Mode::ReadWrite);
        let other = f.principal("other-owner", Mode::ReadWrite);
        let first =
            f.a.add_principal_credential_idempotent(p.id, p.id, verifier(), Some(5000), "add")
                .unwrap();
        assert_eq!(first.expires_ms, Some(6000));
        f.clock.store(2000, std::sync::atomic::Ordering::SeqCst);
        let a = Authority::load(f.store.clone(), SessionLimits::default()).unwrap();
        let again = a
            .add_principal_credential_idempotent(p.id, p.id, verifier(), Some(5000), "add")
            .unwrap();
        assert_eq!(first.id, again.id);
        assert_eq!(first.expires_ms, again.expires_ms);
        assert_eq!(a.credentials_of(p.id).len(), 1);
        assert_eq!(
            a.add_principal_credential_idempotent(p.id, p.id, verifier(), None, "add")
                .unwrap_err(),
            AddCredentialError::Authority(AuthorityError::IdempotencyConflict)
        );
        let independent = a
            .add_principal_credential_idempotent(other.id, p.id, verifier(), None, "add")
            .unwrap();
        assert_ne!(first.id, independent.id);
        a.revoke_principal_credential_idempotent(p.id, p.id, first.id, "revoke")
            .unwrap();
        a.set_principal_enabled_idempotent(other.id, p.id, false, "disable")
            .unwrap();
        a.set_grants_idempotent(other.id, p.id, vec![], "grants")
            .unwrap();
        let a = Authority::load(f.store.clone(), SessionLimits::default()).unwrap();
        assert_eq!(
            a.revoke_principal_credential_idempotent(p.id, p.id, first.id, "revoke")
                .unwrap()
                .state,
            CredentialState::Revoked
        );
        a.set_principal_enabled_idempotent(other.id, p.id, false, "disable")
            .unwrap();
        assert_eq!(a.principal(p.id).unwrap().revision, 2);
        a.set_grants_idempotent(other.id, p.id, vec![], "grants")
            .unwrap();
        assert!(a.grants_of(p.id).is_empty());
    }
    #[test]
    fn rotation_reuses_parameters_and_refuses_without_mutation() {
        let f = Fixture::new();
        let p = f.principal("rotation-owner", Mode::ReadWrite);
        let session = f.acquire(p.id, "acquire");
        let params = f.a.session_scram_params(session.id).unwrap();
        for (salt, iterations) in [
            (b"different".to_vec(), params.1),
            (params.0.clone(), params.1 + 1),
        ] {
            let mut v = verifier();
            v.0.salt = salt;
            v.0.iterations = iterations;
            assert_eq!(
                f.a.renew_idempotent(
                    p.id,
                    session.id,
                    Some(Mode::ReadOnly),
                    Some((v, [42; 32])),
                    "bad"
                )
                .unwrap_err(),
                RenewError::RotationSaltMismatch
            );
            assert_eq!(f.a.view.read().unwrap().session_credentials.len(), 1);
            assert_eq!(encode(&f.a.session(session.id).unwrap()), encode(&session));
        }
        f.a.renew_idempotent(p.id, session.id, None, Some((verifier(), [42; 32])), "good")
            .unwrap();
        {
            let view = f.a.view.read().unwrap();
            assert_eq!(view.session_credentials.len(), 2);
            assert!(
                view.session_credentials
                    .values()
                    .all(|c| (c.verifier.0.salt.clone(), c.verifier.0.iterations) == params)
            );
        }
        f.clock
            .store(session.expires_ms, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(f.a.session_scram_params(session.id), None);
        assert_eq!(
            f.a.renew_idempotent(
                p.id,
                session.id,
                None,
                Some((verifier(), [43; 32])),
                "expired"
            )
            .unwrap_err(),
            RenewError::Authority(AuthorityError::SessionEnded)
        );
        f.clock
            .store(session.created_ms, std::sync::atomic::Ordering::SeqCst);
        f.a.view.write().unwrap().session_credentials.clear();
        assert_eq!(f.a.session_scram_params(session.id), None);
        assert_eq!(
            f.a.renew_idempotent(
                p.id,
                session.id,
                None,
                Some((verifier(), [44; 32])),
                "missing-credential"
            )
            .unwrap_err(),
            RenewError::RotationSaltMismatch
        );
    }
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
            RenewError::Authority(AuthorityError::IdempotencyConflict)
        );
        loaded
            .revoke_idempotent(p.id, s.id, EndReason::Requested, "retry")
            .unwrap();
        assert_eq!(
            loaded
                .renew_idempotent(p.id, s.id, Some(Mode::ReadOnly), credential(), "retry")
                .unwrap_err(),
            RenewError::Authority(AuthorityError::SessionEnded)
        );
        let loaded = Authority::load(f.store.clone(), SessionLimits::default()).unwrap();
        let ended = loaded
            .revoke_idempotent(p.id, s.id, EndReason::Requested, "retry")
            .unwrap();
        assert_eq!(ended.revoke_reason, Some(EndReason::Requested));
    }
}
