//! Bearer authentication and process-lifetime standing credential scopes.
use crate::{
    error::{NyCode, Refusal},
    pgwire::auth::ScramVerifier,
    task::TaskStore,
    token::TaskId,
};
use hmac::{Hmac, KeyInit, Mac};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashMap},
    sync::{Mutex, OnceLock},
};
use tonic::metadata::MetadataMap;

const CACHE_MS: u64 = 60_000;
static STANDING_SALTS: OnceLock<Mutex<HashMap<String, [u8; 32]>>> = OnceLock::new();

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

/// An authenticated principal and its token scope, without printable secret material.
#[derive(Clone)]
pub struct Authenticated {
    /// Credential name used for session ownership and audit records.
    pub principal: String,
    /// Persistent task identity, or none for standing credentials.
    pub task_id: Option<TaskId>,
    /// Task salt or process-lifetime standing-name salt.
    pub salt: [u8; 32],
}

struct Cached {
    authenticated: Authenticated,
    created_ms: u64,
}

/// Per-proxy success cache; only header hashes, never bearer secrets, are retained.
#[derive(Default)]
pub struct Authenticator {
    cache: Mutex<HashMap<[u8; 32], Cached>>,
}

impl Authenticator {
    /// Authenticate one metadata header, caching successful credentials for sixty seconds.
    pub fn authenticate(
        &self,
        metadata: &MetadataMap,
        tasks: &TaskStore,
        standing: &BTreeMap<String, ScramVerifier>,
        now_ms: u64,
    ) -> Result<Authenticated, Refusal> {
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
        let key: [u8; 32] = Sha256::digest(header.as_bytes()).into();
        let mut cache = self.cache.lock().map_err(|_| denied())?;
        cache.retain(|_, entry| {
            now_ms
                .checked_sub(entry.created_ms)
                .is_some_and(|age| age < CACHE_MS)
        });
        // Tasks are revalidated even on a cache hit: close and expiry revoke immediately.
        let authenticated = if let Some(id_text) = user.strip_prefix("task:") {
            let id = crate::task::parse_task_id(id_text).ok_or_else(denied)?;
            let record = tasks.authenticate(&id, secret).map_err(|_| denied())?;
            Authenticated {
                principal: user.into(),
                task_id: Some(id),
                salt: record.salt,
            }
        } else {
            if let Some(entry) = cache.get(&key) {
                return Ok(entry.authenticated.clone());
            }
            let verifier = standing.get(user).ok_or_else(denied)?;
            if !verify_password(verifier, secret) {
                return Err(denied());
            }
            // This is the gRPC equivalent of PostgreSQL's connection scope: standing
            // tokens remain stable across RPCs and proxy instances until process exit.
            let mut salts = STANDING_SALTS
                .get_or_init(Mutex::default)
                .lock()
                .map_err(|_| denied())?;
            let salt = if let Some(salt) = salts.get(user) {
                *salt
            } else {
                let mut salt = [0; 32];
                getrandom::fill(&mut salt).map_err(|_| denied())?;
                salts.insert(user.into(), salt);
                salt
            };
            Authenticated {
                principal: user.into(),
                task_id: None,
                salt,
            }
        };
        cache.insert(
            key,
            Cached {
                authenticated: authenticated.clone(),
                created_ms: now_ms,
            },
        );
        Ok(authenticated)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    struct Fixture {
        dir: std::path::PathBuf,
        tasks: TaskStore,
    }
    impl Fixture {
        fn new() -> Self {
            let mut random = [0; 16];
            getrandom::fill(&mut random).unwrap();
            let dir = std::env::temp_dir().join(format!(
                "spanner-auth-{}",
                data_encoding::HEXLOWER.encode(&random)
            ));
            let tasks = TaskStore::with_clock(&dir, std::sync::Arc::new(|| 100)).unwrap();
            Self { dir, tasks }
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.dir).unwrap();
        }
    }
    fn metadata(header: &str) -> MetadataMap {
        let mut metadata = MetadataMap::new();
        metadata.insert("authorization", header.parse().unwrap());
        metadata
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
    fn standing_salts_survive_cache_and_authenticator_replacement() {
        let fixture = Fixture::new();
        let standing = BTreeMap::from([(
            "auth-salt-test".into(),
            ScramVerifier::from_password("p.a.ss", b"salt", 2),
        )]);
        let header = metadata("Bearer nunya.auth-salt-test.p.a.ss");
        let auth = Authenticator::default();
        let first = auth
            .authenticate(&header, &fixture.tasks, &standing, 100)
            .unwrap();
        let later = auth
            .authenticate(&header, &fixture.tasks, &standing, 60_100)
            .unwrap();
        let other = Authenticator::default()
            .authenticate(&header, &fixture.tasks, &standing, 100)
            .unwrap();
        assert_eq!(first.salt, later.salt);
        assert_eq!(first.salt, other.salt);
        assert!(first.task_id.is_none());
    }
    #[test]
    fn successes_are_hashed_and_expire_at_sixty_seconds() {
        let fixture = Fixture::new();
        let mut standing = BTreeMap::from([(
            "auth-cache-test".into(),
            ScramVerifier::from_password("secret", b"salt", 2),
        )]);
        let header = metadata("Bearer nunya.auth-cache-test.secret");
        let auth = Authenticator::default();
        assert!(
            auth.authenticate(&header, &fixture.tasks, &standing, 100)
                .is_ok()
        );
        assert!(
            auth.cache
                .lock()
                .unwrap()
                .contains_key(&<[u8; 32]>::from(Sha256::digest(
                    b"Bearer nunya.auth-cache-test.secret"
                )))
        );
        standing.clear();
        assert!(
            auth.authenticate(&header, &fixture.tasks, &standing, 60_099)
                .is_ok()
        );
        assert!(
            auth.authenticate(&header, &fixture.tasks, &standing, 60_100)
                .is_err()
        );
        assert!(auth.cache.lock().unwrap().is_empty());
    }
    #[test]
    fn tasks_use_persistent_salt_and_close_revokes_cached_success() {
        let fixture = Fixture::new();
        let task = fixture.tasks.open(1000, None).unwrap();
        let header = metadata(&format!(
            "Bearer nunya.task:{}.{}",
            crate::task::format_task_id(&task.id),
            task.secret
        ));
        let auth = Authenticator::default();
        let principal = auth
            .authenticate(&header, &fixture.tasks, &BTreeMap::new(), 100)
            .unwrap();
        assert_eq!(principal.task_id, Some(task.id));
        assert_eq!(
            principal.salt,
            fixture
                .tasks
                .authenticate(&task.id, &task.secret)
                .unwrap()
                .salt
        );
        fixture.tasks.close(&task.id).unwrap();
        assert!(
            auth.authenticate(&header, &fixture.tasks, &BTreeMap::new(), 101)
                .is_err()
        );
    }
    #[test]
    fn failures_are_uniform_and_never_cached() {
        let fixture = Fixture::new();
        let auth = Authenticator::default();
        for header in [
            "Bearer nunya.unknown.secret",
            "Bearer nunya.task:invalid.secret",
            "wrong",
        ] {
            let error = auth
                .authenticate(&metadata(header), &fixture.tasks, &BTreeMap::new(), 100)
                .err()
                .unwrap();
            assert_eq!(error.code, NyCode::TokenNotValid);
            assert_eq!(error, denied());
        }
        assert!(
            auth.authenticate(&MetadataMap::new(), &fixture.tasks, &BTreeMap::new(), 100)
                .is_err()
        );
        let mut duplicated = metadata("Bearer nunya.a.b");
        duplicated.append("authorization", "Bearer nunya.a.b".parse().unwrap());
        assert!(
            auth.authenticate(&duplicated, &fixture.tasks, &BTreeMap::new(), 100)
                .is_err()
        );
        assert!(auth.cache.lock().unwrap().is_empty());
    }
}
