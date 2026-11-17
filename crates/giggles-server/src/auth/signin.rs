//! Shared role sign-in policy, independent of transport exchanges.
use super::scram::Verifier;
use crate::{Service, Session};
use std::{collections::BTreeMap, net::SocketAddr, sync::Arc};

pub(crate) struct SigninPolicy<'a> {
    pub certificates: &'a BTreeMap<String, String>,
    pub loopback_trust: bool,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Method {
    Certificate,
    LoopbackTrust,
    Password(Option<Verifier>),
}

/// Selects a sign-in method; password exchanges remain the transport's responsibility.
pub(crate) fn choose(
    policy: SigninPolicy<'_>,
    peer: SocketAddr,
    certificate: Option<&str>,
    user: &str,
    service: &Service,
) -> Method {
    if certificate
        .and_then(|fp| policy.certificates.get(fp))
        .is_some_and(|name| name == user)
    {
        return Method::Certificate;
    }
    if policy.loopback_trust && peer.ip().is_loopback() {
        return Method::LoopbackTrust;
    }
    let verifier = service
        .admin
        .roles()
        .get(user)
        .ok()
        .flatten()
        .filter(|role| role.login && !role.task_only)
        .and_then(|role| role.password)
        .and_then(|password| Verifier::parse(&password));
    Method::Password(verifier)
}

/// A role could not open a session, regardless of the selected sign-in method.
#[derive(Debug)]
pub(crate) struct Refusal;

pub(crate) fn open_role_session(service: &Arc<Service>, user: &str) -> Result<Session, Refusal> {
    service.role_session(user).map_err(|_| Refusal)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::Clock;
    use giggles_core::{Admin, Limits, roles::NewRole};
    use std::{
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    const FINGERPRINT: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    static SERIAL: AtomicU64 = AtomicU64::new(0);

    struct Fixture {
        service: Arc<Service>,
        certificates: BTreeMap<String, String>,
        verifier: Verifier,
    }

    impl Fixture {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "signin-{}-{}-{}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
                SERIAL.fetch_add(1, Ordering::Relaxed),
            ));
            std::fs::create_dir_all(&path).unwrap();
            #[cfg(feature = "engine-rocksdb")]
            let admin = Admin::create_engine(
                path.join("db.redb"),
                Limits::default(),
                giggles_core::Engine::V2,
            )
            .unwrap();
            #[cfg(not(feature = "engine-rocksdb"))]
            let admin = Admin::create(path.join("db.redb"), Limits::default()).unwrap();
            let verifier = Verifier::derive("password", b"signin-test", 4096);
            for (name, login, password) in [
                ("owner", true, Some(verifier.encode())),
                ("other", true, Some(verifier.encode())),
                ("task", true, Some(verifier.encode())),
                ("nologin", false, Some(verifier.encode())),
                ("no_password", true, None),
                ("malformed", true, Some("invalid verifier".into())),
            ] {
                admin
                    .roles()
                    .create(NewRole {
                        name: name.into(),
                        login,
                        password,
                        ..NewRole::default()
                    })
                    .unwrap();
            }
            #[cfg(feature = "engine-rocksdb")]
            admin.roles().set_task_only("task", true).unwrap();
            Self {
                service: Service::new(admin, vec![], Clock::new(0, 3600).unwrap()).unwrap(),
                certificates: BTreeMap::from([(FINGERPRINT.into(), "owner".into())]),
                verifier,
            }
        }

        fn choose(&self, trust: bool, peer: &str, certificate: Option<&str>, user: &str) -> Method {
            choose(
                SigninPolicy {
                    certificates: &self.certificates,
                    loopback_trust: trust,
                },
                peer.parse().unwrap(),
                certificate,
                user,
                &self.service,
            )
        }
    }

    #[test]
    fn matching_certificate_precedes_loopback_trust_and_password() {
        let f = Fixture::new();
        for (trust, peer) in [(false, "192.0.2.1:5432"), (true, "127.0.0.1:5432")] {
            assert_eq!(
                f.choose(trust, peer, Some(FINGERPRINT), "owner"),
                Method::Certificate
            );
        }
    }

    #[test]
    fn different_user_and_unmapped_certificates_fall_through() {
        let f = Fixture::new();
        assert_eq!(
            f.choose(false, "192.0.2.1:5432", Some(FINGERPRINT), "other"),
            Method::Password(Some(f.verifier.clone()))
        );
        assert_eq!(
            f.choose(true, "127.0.0.1:5432", Some(FINGERPRINT), "other"),
            Method::LoopbackTrust
        );
        assert_eq!(
            f.choose(false, "192.0.2.1:5432", Some("unmapped"), "owner"),
            Method::Password(Some(f.verifier.clone()))
        );
    }

    #[test]
    fn loopback_trust_requires_enabled_policy_and_loopback_peer() {
        let f = Fixture::new();
        for peer in ["127.0.0.1:5432", "[::1]:5432"] {
            assert_eq!(f.choose(true, peer, None, "owner"), Method::LoopbackTrust);
            assert_eq!(
                f.choose(false, peer, None, "owner"),
                Method::Password(Some(f.verifier.clone()))
            );
        }
        assert_eq!(
            f.choose(true, "192.0.2.1:5432", None, "owner"),
            Method::Password(Some(f.verifier.clone()))
        );
    }

    #[test]
    fn password_lookup_filters_ineligible_and_unknown_roles() {
        let f = Fixture::new();
        for user in ["nologin", "unknown", "no_password", "malformed"] {
            assert_eq!(
                f.choose(false, "127.0.0.1:5432", None, user),
                Method::Password(None)
            );
        }
        assert_eq!(
            f.choose(false, "127.0.0.1:5432", None, "owner"),
            Method::Password(Some(f.verifier.clone()))
        );
    }

    #[cfg(feature = "engine-rocksdb")]
    #[test]
    fn task_only_role_has_no_password_method_or_role_session() {
        let f = Fixture::new();
        assert_eq!(
            f.choose(false, "127.0.0.1:5432", None, "task"),
            Method::Password(None),
        );
        assert_eq!(
            f.choose(true, "127.0.0.1:5432", None, "task"),
            Method::LoopbackTrust,
        );
        assert!(open_role_session(&f.service, "task").is_err());
    }

    #[test]
    fn trusted_method_still_requires_eligible_role_session() {
        let f = Fixture::new();
        assert!(open_role_session(&f.service, "owner").is_ok());
        for user in ["nologin", "unknown"] {
            assert_eq!(
                f.choose(true, "127.0.0.1:5432", None, user),
                Method::LoopbackTrust
            );
            assert!(open_role_session(&f.service, user).is_err());
        }
    }
}
