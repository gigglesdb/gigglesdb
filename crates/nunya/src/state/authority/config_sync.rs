//! Atomic reconciliation of configuration-owned principals.
use super::*;

/// Configured standing identity, with redacted authentication material.
#[derive(Clone)]
pub struct ConfigPrincipal {
    /// Validated principal name.
    pub name: String,
    /// SCRAM verifier for the active credential.
    pub verifier: Verifier,
}
impl fmt::Debug for ConfigPrincipal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConfigPrincipal")
            .field("name", &"[REDACTED]")
            .field("verifier", &"[REDACTED]")
            .finish()
    }
}
/// Counts of changes committed by one configuration reconciliation.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct ConfigSync {
    /// Newly created principals.
    pub created: usize,
    /// Existing principals whose active credential changed.
    pub updated: usize,
    /// Disabled principals restored to enabled state.
    pub re_enabled: usize,
    /// Principals disabled because they disappeared from configuration.
    pub disabled: usize,
}
impl Authority {
    /// Validate and atomically reconcile all configuration-managed principals.
    pub fn sync_config_principals(
        &self,
        entries: &[ConfigPrincipal],
    ) -> Result<ConfigSync, AuthorityError> {
        let writer = self.writer.lock().unwrap();
        let mut names = HashSet::new();
        let mut principals;
        let mut credentials;
        {
            let v = self.view.read().unwrap();
            for e in entries {
                if !valid_name(&e.name) || !names.insert(e.name.clone()) {
                    return Err(AuthorityError::NameInvalid);
                }
            }
            for e in entries {
                if v.principals
                    .values()
                    .any(|p| p.name == e.name && p.source == PrincipalSource::Api)
                {
                    return Err(AuthorityError::NameTaken);
                }
            }
            principals = v.principals.clone();
            credentials = v.credentials.clone();
        }
        let mut result = ConfigSync::default();
        let mut changed = Vec::new();
        let mut changed_credentials = Vec::new();
        for entry in entries {
            let existing = principals.values().find(|p| p.name == entry.name).cloned();
            let mut p = existing.clone().unwrap_or_else(|| Principal {
                id: PrincipalId::random(),
                name: entry.name.clone(),
                kind: PrincipalKind::Service,
                state: PrincipalState::Enabled,
                source: PrincipalSource::Config,
                administrator: false,
                revision: 0,
            });
            let mut dirty = existing.is_none();
            if dirty {
                result.created += 1;
            }
            if p.state != PrincipalState::Enabled {
                p.state = PrincipalState::Enabled;
                result.re_enabled += 1;
                dirty = true;
            }
            let active: Vec<_> = credentials
                .values()
                .filter(|c| c.principal == p.id && c.state == CredentialState::Active)
                .cloned()
                .collect();
            if active.len() != 1
                || active[0].verifier.0.to_pg_string() != entry.verifier.0.to_pg_string()
            {
                for mut c in active {
                    c.state = CredentialState::Revoked;
                    changed_credentials.push(c);
                }
                changed_credentials.push(PrincipalCredential {
                    id: CredentialId::random(),
                    principal: p.id,
                    verifier: entry.verifier.clone(),
                    state: CredentialState::Active,
                    expires_ms: None,
                });
                if existing.is_some() {
                    result.updated += 1;
                }
                dirty = true;
            }
            if dirty {
                p.revision = p.revision.saturating_add(1);
                changed.push(p.clone());
                principals.insert(p.id, p);
            }
        }
        let removed: Vec<_> = principals
            .values()
            .filter(|p| p.source == PrincipalSource::Config && !names.contains(&p.name))
            .cloned()
            .collect();
        for mut p in removed {
            if p.state != PrincipalState::Disabled {
                p.state = PrincipalState::Disabled;
                p.revision = p.revision.saturating_add(1);
                result.disabled += 1;
                changed.push(p.clone());
                principals.insert(p.id, p);
            }
        }
        // Share the API's fail-closed ending primitive, including retries after failed commits.
        let rows = {
            let mut v = self.view.write().unwrap();
            for p in principals
                .values()
                .filter(|p| p.source == PrincipalSource::Config && !names.contains(&p.name))
            {
                v.principals.insert(p.id, p.clone());
                end_principal(&mut v, p.id, EndReason::ConfigRemoved, self.store.now_ms());
            }
            v.pending
                .iter()
                .filter_map(|id| v.sessions.get(id))
                .cloned()
                .collect::<Vec<_>>()
        };
        let mut tx = self.store.write()?;
        // Retry disabled principal rows even when memory already denied them on a failed commit.
        for p in changed.iter().chain(
            principals
                .values()
                .filter(|p| p.source == PrincipalSource::Config && !names.contains(&p.name)),
        ) {
            tx.put(Table::Principals, &keys::principal(p.id), &encode(p))?;
        }
        for c in &changed_credentials {
            tx.put(
                Table::PrincipalCredentials,
                &keys::principal_credential(c.principal, c.id),
                &encode(c),
            )?;
        }
        for s in &rows {
            let _: AccessSession = decode(
                &tx.get(Table::Sessions, &keys::session(s.id))?
                    .ok_or(AuthorityError::Store)?,
            )
            .map_err(|_| AuthorityError::Store)?;
            tx.put(Table::Sessions, &keys::session(s.id), &encode(s))?;
        }
        tx.commit()?;
        {
            let mut v = self.view.write().unwrap();
            v.principals = principals;
            for c in changed_credentials {
                credentials.insert(c.id, c);
            }
            v.credentials = credentials;
            for s in &rows {
                v.pending.remove(&s.id);
                v.flushed.insert(s.id, s.last_used_ms);
            }
        }
        drop(writer);
        self.notify(&rows);
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::*;
    use super::*;
    use crate::{pgwire::auth::ScramVerifier, state::store::RedbStore};
    use std::sync::atomic::Ordering;
    fn entry(name: &str) -> ConfigPrincipal {
        ConfigPrincipal {
            name: name.into(),
            verifier: verifier(),
        }
    }
    #[test]
    fn config_lifecycle_and_api_ownership() {
        let f = Fixture::new();
        assert_eq!(
            f.a.sync_config_principals(&[entry("standing")]).unwrap(),
            ConfigSync {
                created: 1,
                ..ConfigSync::default()
            }
        );
        let p = f.a.principal_by_name("standing").unwrap();
        assert_eq!(p.source, PrincipalSource::Config);
        assert_eq!(p.kind, PrincipalKind::Service);
        assert!(!p.administrator);
        assert_eq!(p.revision, 1);
        assert_eq!(
            f.a.sync_config_principals(&[entry("standing")]).unwrap(),
            ConfigSync::default()
        );
        assert_eq!(f.a.principal(p.id).unwrap().revision, 1);
        f.a.set_grants(p.id, vec![(route(), Mode::ReadWrite)])
            .unwrap();
        let s = f.acquire(p.id, "session");
        let old =
            f.a.view
                .read()
                .unwrap()
                .credentials
                .values()
                .find(|c| c.principal == p.id)
                .unwrap()
                .id;
        let changed = ConfigPrincipal {
            name: "standing".into(),
            verifier: Verifier(ScramVerifier::from_password(
                "new-test-password",
                b"salt",
                1,
            )),
        };
        let updated =
            f.a.sync_config_principals(std::slice::from_ref(&changed))
                .unwrap();
        assert_eq!(updated.updated, 1);
        assert_eq!(updated.created, 0);
        assert_eq!(
            f.a.view.read().unwrap().credentials[&old].state,
            CredentialState::Revoked
        );
        assert_eq!(f.a.principal(p.id).unwrap().revision, 2);
        assert_eq!(f.a.principal_credential_verifiers(p.id).len(), 1);
        assert_eq!(
            f.a.principal_credential_verifiers(p.id)[0].0.to_pg_string(),
            changed.verifier.0.to_pg_string()
        );
        f.a.admit(s.id, Operation::Write).unwrap();
        assert_eq!(f.a.session(s.id).unwrap().scope, s.scope);
        assert_eq!(
            f.a.set_principal_enabled(p.id, false),
            Err(AuthorityError::ConfigManaged)
        );
        assert_eq!(
            f.a.add_principal_credential(p.id, verifier(), None),
            Err(AuthorityError::ConfigManaged)
        );
        assert_eq!(
            f.a.revoke_principal_credential(p.id, old),
            Err(AuthorityError::ConfigManaged)
        );
        assert_eq!(
            f.a.create_principal("standing", PrincipalKind::Person, false, "collision")
                .unwrap_err(),
            AuthorityError::NameTaken
        );
        let removed = f.a.sync_config_principals(&[]).unwrap();
        assert_eq!(removed.disabled, 1);
        assert_eq!(f.a.principal(p.id).unwrap().revision, 3);
        assert_eq!(
            f.a.admit(s.id, Operation::Read).unwrap_err(),
            AdmitError::Ended
        );
        assert_eq!(f.stored(s.id).revoke_reason, Some(EndReason::ConfigRemoved));
        assert_eq!(EndReason::ConfigRemoved.as_str(), "config_removed");
        assert_eq!(
            *f.hook.0.lock().unwrap(),
            vec![(s.id, p.id, EndReason::ConfigRemoved)]
        );
        assert_eq!(
            f.a.sync_config_principals(&[]).unwrap(),
            ConfigSync::default()
        );
        assert_eq!(f.hook.0.lock().unwrap().len(), 1);
        assert_eq!(
            f.a.sync_config_principals(&[changed]).unwrap().re_enabled,
            1
        );
        assert_eq!(f.a.principal(p.id).unwrap().revision, 4);
        assert_eq!(f.a.principal(p.id).unwrap().state, PrincipalState::Enabled);
        assert_eq!(
            f.a.admit(s.id, Operation::Read).unwrap_err(),
            AdmitError::Ended
        );
        let active = f.acquire(p.id, "new-session");
        let Fixture {
            a,
            store,
            clock,
            hook,
            dir,
        } = f;
        drop(a);
        drop(store);
        let c = clock.clone();
        let store = Arc::new(
            RedbStore::with_clock(&dir.0, &root(), Arc::new(move || c.load(Ordering::SeqCst)))
                .unwrap(),
        );
        let a = Authority::load(store.clone(), SessionLimits::default()).unwrap();
        assert_eq!(a.principal_by_name("standing").unwrap().id, p.id);
        assert_eq!(a.principal(p.id).unwrap().revision, 4);
        assert_eq!(a.principal_credential_verifiers(p.id).len(), 1);
        a.admit(active.id, Operation::Write).unwrap();
        assert_eq!(a.session(s.id).unwrap().state, SessionState::Revoked);
        drop(a);
        drop(store);
        drop(hook);
        drop(dir);
    }
    type Snapshot = Vec<(Table, Vec<(Vec<u8>, Vec<u8>)>)>;
    fn snapshot(a: &Authority) -> Snapshot {
        let read = a.store.read().unwrap();
        Table::ALL
            .iter()
            .map(|t| (*t, read.scan_prefix(*t, b"").unwrap()))
            .collect()
    }
    #[test]
    fn validation_changes_nothing_and_debug_redacts() {
        let f = Fixture::new();
        f.principal("api", Mode::ReadOnly);
        f.a.sync_config_principals(&[entry("standing")]).unwrap();
        let before = snapshot(&f.a);
        for (entries, error) in [
            (vec![entry("new"), entry("api")], AuthorityError::NameTaken),
            (
                vec![entry("duplicate"), entry("duplicate")],
                AuthorityError::NameInvalid,
            ),
            (
                vec![entry("new"), entry("invalid name")],
                AuthorityError::NameInvalid,
            ),
            (vec![entry("")], AuthorityError::NameInvalid),
            (vec![entry(&"a".repeat(65))], AuthorityError::NameInvalid),
        ] {
            assert_eq!(f.a.sync_config_principals(&entries).unwrap_err(), error);
            assert_eq!(snapshot(&f.a), before);
            assert!(f.a.principal_by_name("new").is_none());
            assert_eq!(
                f.a.principal_by_name("standing").unwrap().state,
                PrincipalState::Enabled
            );
        }
        let e = entry("sensitive-name");
        let debug = format!("{e:?}");
        assert!(debug.contains("REDACTED"));
        assert!(!debug.contains(&e.name));
        assert!(!debug.contains(&e.verifier.0.to_pg_string()));
    }
    #[test]
    fn reappearing_config_persists_endings_from_failed_removal() {
        let f = Fixture::new();
        f.a.sync_config_principals(&[entry("standing")]).unwrap();
        let p = f.a.principal_by_name("standing").unwrap();
        f.a.set_grants(p.id, vec![(route(), Mode::ReadOnly)])
            .unwrap();
        let s = f.acquire(p.id, "one");
        f.store.fail_commits(true);
        assert_eq!(
            f.a.sync_config_principals(&[]).unwrap_err(),
            AuthorityError::Store
        );
        f.store.fail_commits(false);
        assert_eq!(
            f.a.sync_config_principals(&[entry("standing")])
                .unwrap()
                .re_enabled,
            1
        );
        assert_eq!(f.stored(s.id).revoke_reason, Some(EndReason::ConfigRemoved));
        assert_eq!(f.hook.0.lock().unwrap().len(), 1);
        let loaded = Authority::load(f.store.clone(), SessionLimits::default()).unwrap();
        assert_eq!(
            loaded.principal(p.id).unwrap().state,
            PrincipalState::Enabled
        );
        assert_eq!(
            loaded.admit(s.id, Operation::Read).unwrap_err(),
            AdmitError::Ended
        );
    }
    #[test]
    fn failed_sync_is_atomic_and_removal_retries_fail_closed() {
        let f = Fixture::new();
        f.a.sync_config_principals(&[entry("standing")]).unwrap();
        let p = f.a.principal_by_name("standing").unwrap();
        f.a.set_grants(p.id, vec![(route(), Mode::ReadOnly)])
            .unwrap();
        let s = f.acquire(p.id, "one");
        let before = snapshot(&f.a);
        f.store.fail_commits(true);
        assert_eq!(
            f.a.sync_config_principals(&[entry("standing"), entry("new")])
                .unwrap_err(),
            AuthorityError::Store
        );
        assert!(f.a.principal_by_name("new").is_none());
        assert_eq!(snapshot(&f.a), before);
        assert_eq!(
            f.a.sync_config_principals(&[]).unwrap_err(),
            AuthorityError::Store
        );
        assert_eq!(
            f.a.admit(s.id, Operation::Read).unwrap_err(),
            AdmitError::Ended
        );
        assert!(f.hook.0.lock().unwrap().is_empty());
        assert_eq!(snapshot(&f.a), before);
        f.store.fail_commits(false);
        f.a.sync_config_principals(&[]).unwrap();
        assert_eq!(f.stored(s.id).revoke_reason, Some(EndReason::ConfigRemoved));
        assert_eq!(f.hook.0.lock().unwrap().len(), 1);
        let loaded = Authority::load(f.store.clone(), SessionLimits::default()).unwrap();
        assert_eq!(
            loaded.principal(p.id).unwrap().state,
            PrincipalState::Disabled
        );
    }
}
