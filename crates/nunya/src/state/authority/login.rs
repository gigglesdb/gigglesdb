//! Read-only PostgreSQL session credential resolution.
use super::*;

/// Authentication material resolved from an authorized live session.
pub struct SessionLogin {
    /// Owning principal.
    pub principal: PrincipalId,
    /// Stored session token scope.
    pub scope: TokenScope,
    /// Selected active credential verifier.
    pub verifier: Verifier,
}
impl fmt::Debug for SessionLogin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SessionLogin")
            .field("principal", &self.principal)
            .field("scope", &"[REDACTED]")
            .field("verifier", &"[REDACTED]")
            .finish()
    }
}
impl Authority {
    /// Resolve login material without refreshing activity or mutating state.
    pub fn session_login(&self, id: SessionId, route: &RouteId) -> Option<SessionLogin> {
        let v = self.view.read().ok()?;
        let now = self.store.now_ms();
        let session = v.sessions.get(&id)?;
        if session.state != SessionState::Active
            || now >= session.expires_ms
            || session
                .idle_timeout_ms
                .is_some_and(|idle| now.saturating_sub(session.last_used_ms) > idle)
            || session.route != *route
            || v.principals.get(&session.principal)?.state != PrincipalState::Enabled
            || !v.grants.contains_key(&(session.principal, route.clone()))
        {
            return None;
        }
        let scope = v.scopes.get(&session.scope)?;
        if scope.route != *route || scope.derivation_version != 2 {
            return None;
        }
        // SCRAM can offer only one salt; credential-rotation overlap on this port
        // remains an open question raised by lead 4.
        let credential = v
            .session_credentials
            .values()
            .filter(|c| c.session == id && c.state == CredentialState::Active && now < c.expires_ms)
            .max_by_key(|c| (c.expires_ms, c.id))?;
        Some(SessionLogin {
            principal: session.principal,
            scope: scope.clone(),
            verifier: credential.verifier.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{Fixture, route};
    use super::*;
    use std::sync::atomic::Ordering;

    fn fixture() -> (Fixture, AccessSession) {
        let f = Fixture::new();
        let p = f.principal("agent", Mode::ReadWrite);
        let s = f.acquire(p.id, "login");
        (f, s)
    }
    #[test]
    fn unknown_id() {
        let (f, _) = fixture();
        assert!(f.a.session_login(SessionId::random(), &route()).is_none());
    }
    #[test]
    fn revoked() {
        let (f, s) = fixture();
        f.a.revoke(s.id, EndReason::Requested).unwrap();
        assert!(f.a.session_login(s.id, &route()).is_none());
    }
    #[test]
    fn absolutely_expired() {
        let (f, s) = fixture();
        f.clock.store(s.expires_ms, Ordering::SeqCst);
        assert!(f.a.session_login(s.id, &route()).is_none());
        assert_eq!(f.a.session(s.id).unwrap().state, SessionState::Active);
    }
    #[test]
    fn idle_expired() {
        let (f, s) = fixture();
        f.clock.store(
            s.last_used_ms + s.idle_timeout_ms.unwrap() + 1,
            Ordering::SeqCst,
        );
        assert!(f.a.session_login(s.id, &route()).is_none());
        assert_eq!(f.a.session(s.id).unwrap().state, SessionState::Active);
    }
    #[test]
    fn principal_disabled() {
        let (f, s) = fixture();
        f.a.view
            .write()
            .unwrap()
            .principals
            .get_mut(&s.principal)
            .unwrap()
            .state = PrincipalState::Disabled;
        assert!(f.a.session_login(s.id, &route()).is_none());
    }
    #[test]
    fn grant_removed() {
        let (f, s) = fixture();
        f.a.view.write().unwrap().grants.clear();
        assert!(f.a.session_login(s.id, &route()).is_none());
    }
    #[test]
    fn wrong_route() {
        let (f, s) = fixture();
        assert!(
            f.a.session_login(s.id, &RouteId::new("spanner-native").unwrap())
                .is_none()
        );
    }
    #[test]
    fn invalid_scope() {
        for bad_version in [false, true] {
            let (f, s) = fixture();
            let mut v = f.a.view.write().unwrap();
            let scope = v.scopes.get_mut(&s.scope).unwrap();
            if bad_version {
                scope.derivation_version = 1;
            } else {
                scope.route = RouteId::new("spanner-native").unwrap();
            }
            drop(v);
            assert!(f.a.session_login(s.id, &route()).is_none());
        }
    }
    #[test]
    fn no_active_credential() {
        for expired in [false, true] {
            let (f, s) = fixture();
            let mut v = f.a.view.write().unwrap();
            for c in v.session_credentials.values_mut() {
                if expired {
                    c.expires_ms = 1000;
                } else {
                    c.state = CredentialState::Revoked;
                }
            }
            drop(v);
            assert!(f.a.session_login(s.id, &route()).is_none());
        }
    }
    #[test]
    fn success() {
        let (f, s) = fixture();
        let login = f.a.session_login(s.id, &route()).unwrap();
        assert_eq!(login.principal, s.principal);
        assert_eq!(login.scope.id, s.scope);
        assert!(
            login.verifier.0.to_pg_string() == super::super::tests::verifier().0.to_pg_string()
        );
        let debug = format!("{login:?}");
        assert!(debug.contains("scope: \"[REDACTED]\""));
        assert!(!debug.contains(&login.verifier.0.to_pg_string()));
    }
    #[test]
    fn credential_selection() {
        let (f, s) = fixture();
        let mut v = f.a.view.write().unwrap();
        let mut c = v.session_credentials.values().next().unwrap().clone();
        c.expires_ms -= 1;
        v.session_credentials.clear();
        c.id = CredentialId([1; 16]);
        let first = c.clone();
        v.session_credentials.insert(c.id, c.clone());
        c.id = CredentialId([2; 16]);
        c.verifier = Verifier(crate::pgwire::auth::ScramVerifier::from_password(
            "other", b"other", 1,
        ));
        v.session_credentials.insert(c.id, c.clone());
        drop(v);
        assert!(
            f.a.session_login(s.id, &route())
                .unwrap()
                .verifier
                .0
                .to_pg_string()
                == c.verifier.0.to_pg_string()
        );
        let mut v = f.a.view.write().unwrap();
        v.session_credentials.get_mut(&first.id).unwrap().expires_ms += 1;
        drop(v);
        assert!(
            f.a.session_login(s.id, &route())
                .unwrap()
                .verifier
                .0
                .to_pg_string()
                == first.verifier.0.to_pg_string()
        );
    }
    #[test]
    fn does_not_refresh_last_used() {
        let (f, s) = fixture();
        f.clock.store(1001, Ordering::SeqCst);
        assert!(f.a.session_login(s.id, &route()).is_some());
        assert_eq!(f.a.session(s.id).unwrap().last_used_ms, s.last_used_ms);
    }
}
