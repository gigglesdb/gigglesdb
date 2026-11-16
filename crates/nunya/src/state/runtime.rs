//! Service-lifetime state ownership and connection revocation wiring.
use super::{
    authority::{Authority, ConfigPrincipal, RevocationHook, SessionLimits},
    model::{EndReason, Verifier},
    registry::{ConnectionRegistry, EndReason as ConnectionEndReason},
    store::{RedbStore, StoreError},
};
use crate::{
    config::{SessionSettings, StandingSection},
    token::{PrincipalId, SessionId, keys::RootKey},
};
use std::{fmt, path::Path, sync::Arc, time::Duration};

/// Delay between service state maintenance passes.
pub const MAINTENANCE_INTERVAL: Duration = Duration::from_secs(30);
/// Number of maintenance passes between tombstone purges.
pub const PURGE_EVERY: u32 = 120;
/// Minimum retention after a session's absolute expiry before tombstone removal.
pub const TOMBSTONE_RETENTION: Duration = Duration::from_secs(24 * 3600);

/// Data-free counts from one independent sequence of maintenance steps.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct MaintenanceOutcome {
    /// Session endings persisted by the sweep.
    pub swept: usize,
    /// Successful last-used flush passes.
    pub flushes: u32,
    /// Tombstones removed on a purge pass.
    pub purged: usize,
    /// Maintenance steps that failed.
    pub failures: u32,
}

/// Shared registry of access-session connections.
pub type Registry = ConnectionRegistry<SessionId, PrincipalId>;

struct RegistryHook(Registry);
impl RevocationHook for RegistryHook {
    fn session_ended(&self, session: SessionId, _: PrincipalId, reason: EndReason) {
        let reason = match reason {
            EndReason::Expired | EndReason::IdleTimeout => ConnectionEndReason::Expired,
            EndReason::Requested
            | EndReason::AdminRevoked
            | EndReason::PrincipalDisabled
            | EndReason::ConfigRemoved
            | EndReason::GrantRemoved => ConnectionEndReason::Revoked,
        };
        self.0.signal_session(&session, reason);
    }
}

/// Service-lifetime durable authority and live connection registry.
pub struct StateRuntime {
    /// Authoritative durable session state.
    pub authority: Arc<Authority>,
    /// Open connections shared by both routes.
    pub registry: Registry,
}
impl fmt::Debug for StateRuntime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StateRuntime").finish_non_exhaustive()
    }
}
fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}
fn store_error(error: StoreError) -> String {
    match error {
        StoreError::Owned | StoreError::ContinuityBreak | StoreError::Corrupt | StoreError::Io => {
            error.to_string()
        }
        StoreError::UnsupportedFormat { .. } => {
            "state store format is not supported by this build".into()
        }
    }
}
impl StateRuntime {
    /// Sweep expiry, flush activity and periodically purge retained tombstones.
    pub fn maintain(&self, tick: u64) -> MaintenanceOutcome {
        let mut outcome = MaintenanceOutcome::default();
        match self.authority.sweep() {
            Ok(count) => outcome.swept = count,
            Err(error) => {
                outcome.failures += 1;
                eprintln!("state maintenance sweep failed: {error}");
            }
        }
        match self.authority.flush_last_used() {
            Ok(()) => outcome.flushes = 1,
            Err(error) => {
                outcome.failures += 1;
                eprintln!("state maintenance flush_last_used failed: {error}");
            }
        }
        if tick.is_multiple_of(u64::from(PURGE_EVERY)) {
            match self.authority.purge_tombstones(millis(TOMBSTONE_RETENTION)) {
                Ok(count) => outcome.purged = count,
                Err(error) => {
                    outcome.failures += 1;
                    eprintln!("state maintenance purge_tombstones failed: {error}");
                }
            }
        }
        outcome
    }

    /// Run delayed maintenance passes off the async executor until aborted.
    pub async fn run_maintenance(self: Arc<Self>, interval: Duration) {
        let mut timer = tokio::time::interval(interval);
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        timer.tick().await;
        let mut tick = 0u64;
        loop {
            timer.tick().await;
            tick = tick.wrapping_add(1);
            let runtime = self.clone();
            if tokio::task::spawn_blocking(move || runtime.maintain(tick))
                .await
                .is_err()
            {
                eprintln!("state maintenance task failed");
            }
        }
    }

    #[cfg(test)]
    fn with_clock(
        state_dir: &Path,
        limits: SessionLimits,
        clock: super::store::Clock,
    ) -> Arc<Self> {
        let store = Arc::new(RedbStore::with_clock(state_dir, &tests::root(), clock).unwrap());
        let authority = Authority::load(store, limits).unwrap();
        let registry = Registry::new();
        authority.set_hook(Arc::new(RegistryHook(registry.clone())));
        Arc::new(Self {
            authority,
            registry,
        })
    }

    /// Open exclusive state ownership, attach revocation signals and reconcile configuration.
    pub fn open(
        state_dir: &Path,
        root_key: &RootKey,
        session: &SessionSettings,
        standing: &[StandingSection],
    ) -> Result<Arc<Self>, String> {
        if state_dir.join("tasks").is_dir() {
            eprintln!("serve: state_dir/tasks is from an earlier version and is ignored");
        }
        let store = Arc::new(RedbStore::open(state_dir, root_key).map_err(store_error)?);
        let authority = Authority::load(
            store,
            SessionLimits {
                max_lifetime_ms: millis(session.max_lifetime),
                idle_timeout_ms: session.idle_timeout.map(millis),
            },
        )
        .map_err(|e| format!("state authority load failed: {e}"))?;
        let registry = Registry::new();
        authority.set_hook(Arc::new(RegistryHook(registry.clone())));
        let principals: Vec<_> = standing
            .iter()
            .map(|entry| ConfigPrincipal {
                name: entry.name.clone(),
                verifier: Verifier(crate::pgwire::auth::ScramVerifier {
                    salt: entry.verifier.salt.clone(),
                    iterations: entry.verifier.iterations,
                    stored_key: entry.verifier.stored_key,
                    server_key: entry.verifier.server_key,
                }),
            })
            .collect();
        authority
            .sync_config_principals(&principals)
            .map_err(|e| format!("config principal sync failed: {e}"))?;
        Ok(Arc::new(Self {
            authority,
            registry,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{
        authority::AcquireRequest,
        model::{Mode, PrincipalKind, PrincipalSource, RouteId},
    };
    use std::path::PathBuf;

    struct TestDir(PathBuf);
    impl TestDir {
        fn new() -> Self {
            let path =
                std::env::temp_dir().join(format!("runtime-{}", SessionId::random().to_hex()));
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }
        fn open(&self, standing: &[StandingSection]) -> Arc<StateRuntime> {
            StateRuntime::open(&self.0, &root(), &SessionSettings::default(), standing).unwrap()
        }
    }
    impl Drop for TestDir {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).unwrap();
        }
    }
    pub(super) fn root() -> RootKey {
        RootKey::from_hex(&"42".repeat(32)).unwrap()
    }
    fn verifier() -> crate::pgwire::auth::ScramVerifier {
        crate::pgwire::auth::ScramVerifier::from_password("test", b"runtime-test", 4096)
    }
    fn acquire(runtime: &StateRuntime) -> crate::state::model::AccessSession {
        let principal = runtime
            .authority
            .create_principal("agent", PrincipalKind::Agent, false, "create")
            .unwrap();
        let route = RouteId::new("postgres").unwrap();
        runtime
            .authority
            .set_grants(principal.id, vec![(route.clone(), Mode::ReadOnly)])
            .unwrap();
        runtime
            .authority
            .acquire_session(
                principal.id,
                AcquireRequest {
                    route,
                    mode: None,
                    lifetime_ms: None,
                    isolated: false,
                    label: None,
                    idempotency_key: "acquire".into(),
                    verifier: Verifier(verifier()),
                    secret_sha256: [1; 32],
                },
            )
            .unwrap()
    }

    fn expiry_case(idle_timeout_ms: Option<u64>, advance_to: u64, reason: EndReason) {
        use std::sync::atomic::{AtomicU64, Ordering};
        let dir = TestDir::new();
        let clock = Arc::new(AtomicU64::new(1000));
        let c = clock.clone();
        let runtime = StateRuntime::with_clock(
            &dir.0,
            SessionLimits {
                max_lifetime_ms: 10_000,
                idle_timeout_ms,
            },
            Arc::new(move || c.load(Ordering::SeqCst)),
        );
        let session = acquire(&runtime);
        let (_guard, signal) =
            runtime
                .registry
                .register(session.id, session.principal, "postgres", None);
        assert_eq!(
            runtime.maintain(1),
            MaintenanceOutcome {
                flushes: 1,
                ..Default::default()
            }
        );
        assert_eq!(signal.try_ended(), None);
        clock.store(advance_to, Ordering::SeqCst);
        assert_eq!(
            runtime.maintain(2),
            MaintenanceOutcome {
                swept: 1,
                flushes: 1,
                ..Default::default()
            }
        );
        assert_eq!(signal.try_ended(), Some(ConnectionEndReason::Expired));
        assert_eq!(
            runtime.authority.session(session.id).unwrap().revoke_reason,
            Some(reason)
        );
    }

    #[test]
    fn maintenance_ends_absolute_expiry_and_signals_connection() {
        expiry_case(None, 11_001, EndReason::Expired);
    }

    #[test]
    fn maintenance_ends_idle_expiry_and_signals_connection() {
        expiry_case(Some(100), 1101, EndReason::IdleTimeout);
    }

    #[test]
    fn maintenance_purges_only_on_purge_ticks_after_retention() {
        use std::sync::atomic::{AtomicU64, Ordering};
        let dir = TestDir::new();
        let clock = Arc::new(AtomicU64::new(1000));
        let c = clock.clone();
        let runtime = StateRuntime::with_clock(
            &dir.0,
            SessionLimits::default(),
            Arc::new(move || c.load(Ordering::SeqCst)),
        );
        let session = acquire(&runtime);
        runtime
            .authority
            .revoke(session.id, EndReason::Requested)
            .unwrap();
        clock.store(
            session.expires_ms + millis(TOMBSTONE_RETENTION),
            Ordering::SeqCst,
        );
        assert_eq!(runtime.maintain(u64::from(PURGE_EVERY)).purged, 0);
        assert!(runtime.authority.session(session.id).is_some());
        clock.fetch_add(1, Ordering::SeqCst);
        assert_eq!(runtime.maintain(u64::from(PURGE_EVERY) + 1).purged, 0);
        assert!(runtime.authority.session(session.id).is_some());
        assert_eq!(
            runtime.maintain(2 * u64::from(PURGE_EVERY)),
            MaintenanceOutcome {
                purged: 1,
                flushes: 1,
                ..Default::default()
            }
        );
        assert!(runtime.authority.session(session.id).is_none());
    }

    #[test]
    fn fresh_open_and_exclusive_ownership() {
        let dir = TestDir::new();
        let runtime = dir.open(&[]);
        assert!(runtime.registry.is_empty());
        let error =
            StateRuntime::open(&dir.0, &root(), &SessionSettings::default(), &[]).unwrap_err();
        assert_eq!(error, "state is owned by a running service");
        drop(runtime);
        drop(dir.open(&[]));
    }
    #[test]
    fn standing_entries_become_config_principals() {
        let dir = TestDir::new();
        let runtime = dir.open(&[StandingSection {
            name: "agent".into(),
            verifier: verifier(),
        }]);
        let principal = runtime.authority.principal_by_name("agent").unwrap();
        assert_eq!(principal.source, PrincipalSource::Config);
    }
    #[test]
    fn revocation_signals_registered_connection() {
        let dir = TestDir::new();
        let runtime = dir.open(&[]);
        let authority = &runtime.authority;
        let principal = authority
            .create_principal("agent", PrincipalKind::Agent, false, "create")
            .unwrap();
        let route = RouteId::new("postgres").unwrap();
        authority
            .set_grants(principal.id, vec![(route.clone(), Mode::ReadOnly)])
            .unwrap();
        let session = authority
            .acquire_session(
                principal.id,
                AcquireRequest {
                    route,
                    mode: None,
                    lifetime_ms: None,
                    isolated: false,
                    label: None,
                    idempotency_key: "acquire".into(),
                    verifier: Verifier(verifier()),
                    secret_sha256: [1; 32],
                },
            )
            .unwrap();
        let (_guard, signal) =
            runtime
                .registry
                .register(session.id, principal.id, "postgres", None);
        authority.revoke(session.id, EndReason::Requested).unwrap();
        assert_eq!(signal.try_ended(), Some(ConnectionEndReason::Revoked));
    }
    #[test]
    fn hook_maps_all_end_reasons() {
        for reason in [
            EndReason::Expired,
            EndReason::IdleTimeout,
            EndReason::Requested,
            EndReason::AdminRevoked,
            EndReason::PrincipalDisabled,
            EndReason::ConfigRemoved,
            EndReason::GrantRemoved,
        ] {
            let registry = Registry::new();
            let session = SessionId::random();
            let principal = PrincipalId::random();
            let (_guard, signal) = registry.register(session, principal, "postgres", None);
            RegistryHook(registry).session_ended(session, principal, reason);
            let expected = match reason {
                EndReason::Expired | EndReason::IdleTimeout => ConnectionEndReason::Expired,
                EndReason::Requested
                | EndReason::AdminRevoked
                | EndReason::PrincipalDisabled
                | EndReason::ConfigRemoved
                | EndReason::GrantRemoved => ConnectionEndReason::Revoked,
            };
            assert_eq!(signal.try_ended(), Some(expected));
        }
    }
    #[test]
    fn legacy_tasks_directory_is_ignored() {
        let dir = TestDir::new();
        std::fs::create_dir(dir.0.join("tasks")).unwrap();
        drop(dir.open(&[]));
    }
    #[test]
    fn milliseconds_saturate() {
        assert_eq!(millis(Duration::MAX), u64::MAX);
        assert_eq!(millis(Duration::from_secs(2)), 2000);
    }
}
