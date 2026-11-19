//! Management bearer authentication with uniform failures.
use super::routes::MgmtContext;
use crate::{
    pgwire::auth::ScramVerifier, spanner::auth::verify_password, state::model::PrincipalState,
    token::PrincipalId,
};
use http::{HeaderMap, header::AUTHORIZATION};
use std::{fmt, sync::OnceLock};

/// Trust boundary of a management request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Origin {
    /// Locally authenticated Unix socket.
    Socket,
    /// Network connection requiring a bearer credential.
    Network,
}
/// Authenticated management identity.
pub struct Caller {
    /// Store principal, absent for configuration admins and socket callers.
    pub principal: Option<PrincipalId>,
    /// Authenticated name.
    pub name: String,
    /// Administrative privilege.
    pub admin: bool,
}
impl fmt::Debug for Caller {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Caller")
            .field("admin", &self.admin)
            .field("has_principal", &self.principal.is_some())
            .finish()
    }
}
/// Data-free authentication refusal.
#[derive(Debug, PartialEq, Eq)]
pub struct Unauthorized;
fn bearer(headers: &HeaderMap) -> Result<(&str, &str), Unauthorized> {
    if headers.get_all(AUTHORIZATION).iter().count() != 1 {
        return Err(Unauthorized);
    }
    let value = headers
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .ok_or(Unauthorized)?;
    let (name, secret) = value
        .strip_prefix("Bearer nunya.")
        .and_then(|v| v.split_once('.'))
        .ok_or(Unauthorized)?;
    if name.is_empty()
        || name.len() > 64
        || secret.is_empty()
        || !name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        return Err(Unauthorized);
    }
    Ok((name, secret))
}
const AUTH_CHECKS: usize = 4;
#[cfg(test)]
thread_local! { static CHECKS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }
fn check(v: &ScramVerifier, secret: &str) -> bool {
    #[cfg(test)]
    CHECKS.with(|n| n.set(n.get() + 1));
    verify_password(v, secret)
}
fn dummy(secret: &str) {
    static VERIFIER: OnceLock<ScramVerifier> = OnceLock::new();
    let v = VERIFIER.get_or_init(|| {
        ScramVerifier::from_password("management-dummy", b"management-dummy-salt", 4096)
    });
    let _ = check(v, secret);
}
/// Authenticate socket trust or a principal/configuration bearer credential.
pub fn authenticate(
    ctx: &MgmtContext,
    origin: Origin,
    headers: &HeaderMap,
) -> Result<Caller, Unauthorized> {
    if origin == Origin::Socket {
        return Ok(Caller {
            principal: None,
            name: "local-socket".into(),
            admin: true,
        });
    }
    let (name, secret) = bearer(headers)?;
    let principal = ctx.authority.principal_by_name(name);
    let admins = ctx.admins.read().unwrap_or_else(std::sync::PoisonError::into_inner);
    let admin = admins.iter().find(|(n, _)| n == name);
    let stored = principal
        .as_ref()
        .filter(|p| p.state == PrincipalState::Enabled)
        .map(|p| ctx.authority.principal_credential_verifiers(p.id))
        .unwrap_or_default();
    let mut verifiers: Vec<&ScramVerifier> = if let Some((_, verifier)) = admin {
        vec![verifier]
    } else {
        stored.iter().map(|v| &v.0).collect()
    };
    // Stable verifier ordering also handles legacy stores containing over four credentials.
    verifiers.sort_by_key(|v| v.to_pg_string());
    let mut matches = false;
    for index in 0..AUTH_CHECKS {
        if let Some(verifier) = verifiers.get(index) {
            matches |= check(verifier, secret);
        } else {
            dummy(secret);
        }
    }
    if !matches {
        return Err(Unauthorized);
    }
    if admin.is_some() {
        return Ok(Caller {
            principal: None,
            name: name.into(),
            admin: true,
        });
    }
    let p = principal.ok_or(Unauthorized)?;
    Ok(Caller {
        principal: Some(p.id),
        name: p.name,
        admin: p.administrator,
    })
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bearer_verification_count_is_uniform() {
        use crate::{
            mgmt::{
                audit::{AuditUnavailable, LifecycleRecord, LifecycleSink},
                routes::{ConnectionSource, ConnectionView, StatusSource},
            },
            state::{
                authority::{Authority, SessionLimits},
                model::{PrincipalKind, Verifier},
                store::RedbStore,
            },
            token::{SessionId, keys::RootKey},
        };
        use std::sync::Arc;
        struct Sources;
        impl ConnectionSource for Sources {
            fn list(&self) -> Vec<ConnectionView> {
                vec![]
            }
        }
        impl StatusSource for Sources {
            fn policy_revision(&self, _route: &str) -> Option<String> {
                None
            }
            fn config_revision(&self) -> Option<String> {
                None
            }
            fn route_ids(&self) -> Vec<String> {
                vec![]
            }
        }
        impl LifecycleSink for Sources {
            fn record(&self, _: &LifecycleRecord) -> Result<(), AuditUnavailable> {
                Ok(())
            }
        }
        let dir = std::env::temp_dir().join(format!("auth-count-{}", SessionId::random().to_hex()));
        std::fs::create_dir_all(&dir).unwrap();
        let store =
            Arc::new(RedbStore::open(&dir, &RootKey::from_hex(&"72".repeat(32)).unwrap()).unwrap());
        let authority = Authority::load(store, SessionLimits::default()).unwrap();
        let verifier = ScramVerifier::from_password("secret", b"auth-count-salt", 4096);
        for (name, count) in [("disabled", 1), ("zero", 0), ("one", 1), ("four", 4)] {
            let p = authority
                .create_principal(name, PrincipalKind::Agent, false, name)
                .unwrap();
            for _ in 0..count {
                authority
                    .add_principal_credential(
                        p.id,
                        Verifier::try_from(verifier.to_pg_string()).unwrap(),
                        None,
                    )
                    .unwrap();
            }
            if name == "disabled" {
                authority.set_principal_enabled(p.id, false).unwrap();
            }
        }
        let ctx = MgmtContext { leadership: None, policy: None,
            authority,
            admins: Arc::new(std::sync::RwLock::new(vec![("configured".into(), verifier)])),
            connections: Arc::new(Sources),
            status: Arc::new(Sources),
            lifecycle: Arc::new(Sources),
        };
        for (name, accepted) in [
            ("unknown", false),
            ("disabled", false),
            ("zero", false),
            ("one", true),
            ("four", true),
            ("configured", true),
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(
                AUTHORIZATION,
                format!("Bearer nunya.{name}.secret").parse().unwrap(),
            );
            CHECKS.with(|n| n.set(0));
            assert_eq!(
                authenticate(&ctx, Origin::Network, &headers).is_ok(),
                accepted
            );
            CHECKS.with(|n| assert_eq!(n.get(), AUTH_CHECKS));
        }
        drop(ctx);
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn header_parsing_rejects_malformed_credentials() {
        assert!(bearer(&HeaderMap::new()).is_err());
        for value in [
            "Basic nunya.a.b",
            "Bearer a.b",
            "Bearer nunya..b",
            "Bearer nunya.a.",
            "Bearer nunya.session:abcdef.secret",
            "Bearer nunya.a:b.secret",
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(AUTHORIZATION, value.parse().unwrap());
            assert!(bearer(&headers).is_err());
        }
    }
    #[test]
    fn secret_may_contain_dots() {
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            "Bearer nunya.agent.secret.with.dots".parse().unwrap(),
        );
        assert!(matches!(
            bearer(&headers),
            Ok(("agent", "secret.with.dots"))
        ));
    }
    #[test]
    fn caller_debug_hides_name() {
        let c = Caller {
            principal: None,
            name: "private-name".into(),
            admin: true,
        };
        assert!(!format!("{c:?}").contains("private-name"));
    }
}
