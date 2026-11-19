//! Native Spanner bearer authentication against durable access sessions.
use crate::{
    error::{NyCode, Refusal},
    pgwire::auth::{ScramVerifier, parse_session_user},
    state::{authority::Authority, model::RouteId},
    token::{
        PrincipalId, ScopeKeys, SessionId,
        keys::{RootKey, derive_scope_v2},
    },
};
use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;
use std::sync::Arc;
use tonic::metadata::MetadataMap;

fn denied() -> Refusal {
    Refusal::new(NyCode::TokenNotValid, "authentication failed")
}

/// Parse the first two separators, preserving every dot in the secret.
pub fn parse_authorization(header: &str) -> Option<(&str, &str)> {
    let credential = header.strip_prefix("Bearer nunya.")?;
    let (user, secret) = credential.split_once('.')?;
    if user.is_empty() || secret.is_empty() || user.chars().any(char::is_whitespace) {
        return None;
    }
    Some((user, secret))
}

/// Check a password against its SCRAM stored key using constant-time HMAC verification.
pub fn verify_password(verifier: &ScramVerifier, password: &str) -> bool {
    if verifier.iterations == 0 {
        return false;
    }
    // Reuse PostgreSQL's SASLprep, PBKDF2, and client-key derivation exactly.
    let candidate = ScramVerifier::from_password(password, &verifier.salt, verifier.iterations);
    let Ok(mut actual) = Hmac::<Sha256>::new_from_slice(&verifier.stored_key) else {
        return false;
    };
    actual.update(&candidate.stored_key);
    let digest = actual.finalize().into_bytes();
    let Ok(mut expected) = Hmac::<Sha256>::new_from_slice(&verifier.stored_key) else {
        return false;
    };
    expected.update(&verifier.stored_key);
    expected.verify_slice(&digest).is_ok()
}

/// An authenticated access session with redacted token keys.
#[derive(Clone)]
pub struct Authenticated {
    /// Durable access session identifier.
    pub session: SessionId,
    /// Owning principal identifier.
    pub principal: PrincipalId,
    /// Derived v2 token scope keys.
    pub keys: Arc<ScopeKeys>,
    /// Matched bearer credential identifier.
    pub credential: crate::state::model::CredentialId,
    /// Stored token scope identifier.
    pub scope: crate::token::ScopeId,
}
impl std::fmt::Debug for Authenticated {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Authenticated")
            .field("session", &self.session)
            .field("principal", &self.principal)
            .field("keys", &"[redacted]")
            .finish()
    }
}

/// Resolve exactly one bearer header to a currently authorized access session.
pub fn authenticate(
    metadata: &MetadataMap,
    authority: &Authority,
    route: &RouteId,
    root: &RootKey,
) -> Result<Authenticated, Refusal> {
    authenticate_state(metadata, authority, route, root)
}

/// Authenticate using the selected local or peer serving state.
pub fn authenticate_state(metadata: &MetadataMap, authority: &dyn crate::state::serving_state::ServingState, route: &RouteId, root: &RootKey) -> Result<Authenticated, Refusal> {
    if !authority.available() { return Err(Refusal::new(NyCode::ServingUnavailable, "serving unavailable")); }
    let mut headers = metadata.get_all("authorization").iter();
    let header = headers
        .next()
        .ok_or_else(denied)?
        .to_str()
        .map_err(|_| denied())?;
    if headers.next().is_some() {
        return Err(denied());
    }
    let (user, secret) = parse_authorization(header).ok_or_else(denied)?;
    let id = parse_session_user(user);
    let lookup = id.unwrap_or(SessionId([0; 16]));
    let login = id.and_then(|id| authority.session_login(id, route));
    let credential = authority.session_secret_credential(lookup, secret.as_bytes());
    let login = login.filter(|_| credential.is_some()).ok_or_else(denied)?;
    let keys = derive_scope_v2(
        root,
        &login.scope.salt,
        &login.scope.deployment_id,
        login.scope.route.as_str(),
    )
    .map_err(|_| denied())?;
    Ok(Authenticated {
        session: lookup,
        credential: credential.ok_or_else(denied)?,
        scope: login.scope.id,
        principal: login.principal,
        keys: Arc::new(keys),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{
        authority::AcquireRequest,
        model::{EndReason, Mode, PrincipalKind, Verifier},
        runtime::StateRuntime,
    };
    use sha2::Digest;
    struct Fixture {
        dir: std::path::PathBuf,
        runtime: Arc<StateRuntime>,
        root: RootKey,
        route: RouteId,
        principal: PrincipalId,
    }
    impl Fixture {
        fn new() -> Self {
            let dir =
                std::env::temp_dir().join(format!("spanner-auth-{}", SessionId::random().to_hex()));
            let root = RootKey::from_hex(&"00".repeat(32)).unwrap();
            let runtime =
                StateRuntime::open(&dir, &root, &crate::config::SessionSettings::default(), &[])
                    .unwrap();
            let route = RouteId::new("spanner-native").unwrap();
            let a = &runtime.authority;
            let principal = a
                .create_principal("alice", PrincipalKind::Agent, false, "test")
                .unwrap()
                .id;
            a.set_grants(
                principal,
                vec![
                    (route.clone(), Mode::ReadWrite),
                    (RouteId::new("postgres").unwrap(), Mode::ReadWrite),
                ],
            )
            .unwrap();
            a.add_principal_credential(
                principal,
                Verifier(ScramVerifier::from_password("principal-secret", b"salt", 2)),
                None,
            )
            .unwrap();
            Self {
                dir,
                runtime,
                root,
                route,
                principal,
            }
        }
        fn acquire(&self, route: &RouteId) -> SessionId {
            self.runtime
                .authority
                .acquire_session(
                    self.principal,
                    AcquireRequest {
                        route: route.clone(),
                        mode: None,
                        lifetime_ms: None,
                        isolated: true,
                        label: None,
                        idempotency_key: SessionId::random().to_hex(),
                        verifier: Verifier(ScramVerifier::from_password("p.a.ss", b"salt", 2)),
                        secret_sha256: Sha256::digest(b"p.a.ss").into(),
                    },
                )
                .unwrap()
                .id
        }
        fn auth(&self, metadata: &MetadataMap) -> Result<Authenticated, Refusal> {
            authenticate(metadata, &self.runtime.authority, &self.route, &self.root)
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }
    fn metadata(header: &str) -> MetadataMap {
        let mut metadata = MetadataMap::new();
        metadata.insert("authorization", header.parse().unwrap());
        metadata
    }
    fn bearer(id: SessionId) -> MetadataMap {
        metadata(&format!("Bearer nunya.session:{}.p.a.ss", id.to_hex()))
    }
    #[test]
    fn header_parsing_preserves_secret_dots_and_refuses_malformed() {
        assert_eq!(
            parse_authorization("Bearer nunya.alice.a.b.c"),
            Some(("alice", "a.b.c"))
        );
        for header in [
            "",
            "bearer nunya.a.b",
            "Bearer other.a.b",
            "Bearer nunya.a",
            "Bearer nunya..x",
            "Bearer nunya.a.",
            "Bearer nunya.a b.x",
        ] {
            assert!(parse_authorization(header).is_none());
        }
    }
    #[test]
    fn passwords_match_scram_including_saslprep_and_zero_iterations() {
        let mut verifier = ScramVerifier::from_password("IX", b"salt", 2);
        assert!(verify_password(&verifier, "I\u{00ad}X"));
        assert!(!verify_password(&verifier, "wrong"));
        verifier.iterations = 0;
        assert!(!verify_password(&verifier, "IX"));
        let verifier = ScramVerifier::from_password("a\u{0007}", b"salt", 1);
        assert!(verify_password(&verifier, "a\u{0007}"));
    }
    #[test]
    fn session_keys_are_stable_and_isolated() {
        use crate::{
            policy::DomainName,
            token::{StandardCodec, TokenCodec},
            value::{ColumnType, SqlValue},
        };
        let f = Fixture::new();
        let id = f.acquire(&f.route);
        let first = f.auth(&bearer(id)).unwrap();
        let later = f.auth(&bearer(id)).unwrap();
        let second = f.auth(&bearer(f.acquire(&f.route))).unwrap();
        let encode = |who: &Authenticated| {
            StandardCodec
                .encode(
                    &who.keys,
                    &DomainName("email".into()),
                    &ColumnType::Text,
                    None,
                    &SqlValue::Text("private@example.test".into()),
                )
                .unwrap()
        };
        assert_eq!(encode(&first), encode(&later));
        assert_ne!(encode(&first), encode(&second));
        assert_eq!(first.session, id);
        assert_eq!(first.principal, f.principal);
        assert!(format!("{first:?}").contains("[redacted]"));
    }
    #[test]
    fn revoked_session_is_refused_immediately() {
        let f = Fixture::new();
        let id = f.acquire(&f.route);
        assert!(f.auth(&bearer(id)).is_ok());
        f.runtime
            .authority
            .revoke(id, EndReason::AdminRevoked)
            .unwrap();
        assert_eq!(f.auth(&bearer(id)).unwrap_err(), denied());
    }
    #[test]
    fn replacement_authentication_observes_revocation() {
        let f = Fixture::new();
        let id = f.acquire(&f.route);
        let header = bearer(id);
        assert!(authenticate(&header, &f.runtime.authority, &f.route, &f.root).is_ok());
        f.runtime
            .authority
            .revoke(id, EndReason::AdminRevoked)
            .unwrap();
        assert_eq!(
            authenticate(&header, &f.runtime.authority, &f.route, &f.root).unwrap_err(),
            denied()
        );
    }
    #[test]
    fn failures_are_uniform() {
        let f = Fixture::new();
        let id = f.acquire(&f.route);
        let other_route = f.acquire(&RouteId::new("postgres").unwrap());
        let mut duplicate = bearer(id);
        duplicate.append(
            "authorization",
            "Bearer nunya.alice.principal-secret".parse().unwrap(),
        );
        let cases = [
            MetadataMap::new(),
            duplicate,
            metadata("wrong"),
            bearer(SessionId([0; 16])),
            metadata(&format!("Bearer nunya.session:{}.wrong", id.to_hex())),
            bearer(other_route),
            metadata(&format!("Bearer nunya.task:{}.p.a.ss", id.to_hex())),
            metadata("Bearer nunya.alice.principal-secret"),
        ];
        for header in cases {
            assert_eq!(f.auth(&header).unwrap_err(), denied());
            assert_eq!(f.auth(&header).unwrap_err(), denied());
        }
    }
}
