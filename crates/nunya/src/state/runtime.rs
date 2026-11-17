//! Service-lifetime state ownership and connection revocation wiring.
use super::{
    authority::{Authority, ConfigPrincipal, RevocationHook, SessionLimits},
    model::{EndReason, Verifier},
    registry::{ConnectionRegistry, EndReason as ConnectionEndReason},
    store::{RedbStore, StoreError, Store, StoreMeta, ReadTxn, WriteTxn, ScanRows},
};
use crate::{
    config::{SessionSettings, StandingSection},
    token::{PrincipalId, SessionId, keys::RootKey},
};
use std::{fmt, path::Path, sync::{Arc, OnceLock, Weak, atomic::{AtomicBool, AtomicU64, Ordering}}, time::Duration};
use crate::readlog::{ReadLog, LifecycleRecord, LifecycleKind, LifecycleActor};

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

struct StoreHealth {
    failures: AtomicU64,
    last_write_ok: AtomicBool,
}
impl Default for StoreHealth {
    fn default() -> Self {
        Self { failures: AtomicU64::new(0), last_write_ok: AtomicBool::new(true) }
    }
}
impl StoreHealth {
    fn record(&self, ok: bool) {
        if !ok { self.failures.fetch_add(1, Ordering::Relaxed); }
        self.last_write_ok.store(ok, Ordering::Relaxed);
    }
}
struct CountingStore {
    inner: Arc<dyn Store>,
    health: Arc<StoreHealth>,
}
struct CountingWrite<'a> {
    inner: Box<dyn WriteTxn + 'a>,
    health: &'a StoreHealth,
}
impl Store for CountingStore {
    fn meta(&self) -> &StoreMeta { self.inner.meta() }
    fn now_ms(&self) -> u64 { self.inner.now_ms() }
    fn read(&self) -> Result<Box<dyn ReadTxn + '_>, StoreError> { self.inner.read() }
    fn write(&self) -> Result<Box<dyn WriteTxn + '_>, StoreError> {
        match self.inner.write() {
            Ok(inner) => Ok(Box::new(CountingWrite { inner, health: &self.health })),
            Err(error) => { self.health.record(false); Err(error) }
        }
    }
}
impl ReadTxn for CountingWrite<'_> {
    fn get(&self, t: super::schema::Table, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        self.inner.get(t, key)
    }
    fn scan_prefix(&self, t: super::schema::Table, prefix: &[u8]) -> Result<ScanRows, StoreError> {
        self.inner.scan_prefix(t, prefix)
    }
}
impl WriteTxn for CountingWrite<'_> {
    fn put(&mut self, t: super::schema::Table, key: &[u8], value: &[u8]) -> Result<(), StoreError> {
        self.inner.put(t, key, value)
    }
    fn delete(&mut self, t: super::schema::Table, key: &[u8]) -> Result<bool, StoreError> {
        self.inner.delete(t, key)
    }
    fn commit(self: Box<Self>) -> Result<(), StoreError> {
        let result = self.inner.commit();
        self.health.record(result.is_ok());
        result
    }
}
#[derive(Default)]
struct EndCounters {
    revoked: AtomicU64,
    expired: AtomicU64,
}
struct RegistryHook {
    registry: Registry,
    authority: Weak<Authority>,
    log: Arc<OnceLock<Arc<dyn ReadLog>>>,
    ends: Arc<EndCounters>,
}
impl RevocationHook for RegistryHook {
    fn session_ended(&self, session: SessionId, principal: PrincipalId, reason: EndReason) {
        let connection_reason = match reason {
            EndReason::Expired | EndReason::IdleTimeout => {
                self.ends.expired.fetch_add(1, Ordering::Relaxed);
                ConnectionEndReason::Expired
            },
            EndReason::Requested
            | EndReason::AdminRevoked
            | EndReason::PrincipalDisabled
            | EndReason::ConfigRemoved
            | EndReason::GrantRemoved => {
                self.ends.revoked.fetch_add(1, Ordering::Relaxed);
                ConnectionEndReason::Revoked
            },
        };
        self.registry.signal_session(&session, connection_reason);
        let route = self.authority.upgrade()
            .and_then(|authority| authority.session(session))
            .map(|row| row.route.as_str().to_owned());
        let record = LifecycleRecord {
            kind: LifecycleKind::SessionEnded,
            at_ms: crate::serve::now(),
            principal: principal.to_hex(),
            session: Some(session.to_hex()),
            route,
            connection: None,
            actor: LifecycleActor::System,
            reason: reason.as_str(),
        };
        if self.log.get().is_none_or(|log| log.write_lifecycle(&record).is_err()) {
            eprintln!("audit: lifecycle record unavailable");
        }
    }
}

/// Service-lifetime durable authority and live connection registry.
pub struct StateRuntime {
    /// Authoritative durable session state.
    pub authority: Arc<Authority>,
    /// Open connections shared by both routes.
    pub registry: Registry,
    lifecycle_log: Arc<OnceLock<Arc<dyn ReadLog>>>,
    store_health: Arc<StoreHealth>,
    ends: Arc<EndCounters>,
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
    /// Whether the most recent store write attempt succeeded, initially true.
    pub fn store_last_write_ok(&self) -> bool {
        self.store_health.last_write_ok.load(Ordering::Relaxed)
    }
    /// Return data-free session, connection and storage metrics.
    pub fn metrics(&self) -> Vec<(String, u64)> {
        let counters = self.authority.counters();
        vec![
            ("nunya_sessions_active".into(), self.authority.active_session_count() as u64),
            ("nunya_connections_open".into(), self.registry.len() as u64),
            ("nunya_sessions_issued_total".into(), counters.sessions_issued),
            ("nunya_sessions_renewed_total".into(), counters.sessions_renewed),
            ("nunya_sessions_revoked_total".into(), self.ends.revoked.load(Ordering::Relaxed)),
            ("nunya_sessions_expired_total".into(), self.ends.expired.load(Ordering::Relaxed)),
            ("nunya_session_resumes_refused_total".into(), counters.resumes_refused),
            ("nunya_store_write_failures_total".into(), self.store_health.failures.load(Ordering::Relaxed)),
        ]
    }

    /// Attach the startup lifecycle audit sink once it is available.
    pub fn set_lifecycle_log(&self, log: Arc<dyn ReadLog>) {
        let _ = self.lifecycle_log.set(log);
    }

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
        let store_health = Arc::new(StoreHealth::default());
        let store = Arc::new(CountingStore { inner: store, health: store_health.clone() });
        let ends = Arc::new(EndCounters::default());
        let authority = Authority::load(store, limits).unwrap();
        let registry = Registry::new();
        let lifecycle_log = Arc::new(OnceLock::new());
        authority.set_hook(Arc::new(RegistryHook {
            registry: registry.clone(), authority: Arc::downgrade(&authority), log: lifecycle_log.clone(), ends: ends.clone(),
        }));
        Arc::new(Self {
            authority,
            registry,
            lifecycle_log,
            store_health,
            ends,
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
        let store_health = Arc::new(StoreHealth::default());
        let store = Arc::new(CountingStore { inner: store, health: store_health.clone() });
        let ends = Arc::new(EndCounters::default());
        let authority = Authority::load(
            store,
            session_limits(session),
        )
        .map_err(|e| format!("state authority load failed: {e}"))?;
        let registry = Registry::new();
        let lifecycle_log = Arc::new(OnceLock::new());
        authority.set_hook(Arc::new(RegistryHook {
            registry: registry.clone(), authority: Arc::downgrade(&authority), log: lifecycle_log.clone(), ends: ends.clone(),
        }));
        let principals = config_principals(standing);
        authority
            .sync_config_principals(&principals)
            .map_err(|e| format!("config principal sync failed: {e}"))?;
        Ok(Arc::new(Self {
            authority,
            registry,
            lifecycle_log,
            store_health,
            ends,
        }))
    }
}

/// Convert configured standing credentials identically at startup and reload.
pub(crate) fn config_principals(standing: &[StandingSection]) -> Vec<ConfigPrincipal> {
    standing
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
        .collect()
}

/// Convert configured session durations into authority bounds.
pub(crate) fn session_limits(session: &SessionSettings) -> SessionLimits {
    SessionLimits {
        max_lifetime_ms: millis(session.max_lifetime),
        idle_timeout_ms: session.idle_timeout.map(millis),
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

    #[test]
    fn failed_write_begin_counts_once() {
        struct Unavailable(Arc<RedbStore>);
        impl Store for Unavailable {
            fn meta(&self) -> &StoreMeta { self.0.meta() }
            fn now_ms(&self) -> u64 { self.0.now_ms() }
            fn read(&self) -> Result<Box<dyn ReadTxn + '_>, StoreError> { self.0.read() }
            fn write(&self) -> Result<Box<dyn WriteTxn + '_>, StoreError> { Err(StoreError::Io) }
        }
        let dir = TestDir::new();
        let health = Arc::new(StoreHealth::default());
        let store = CountingStore {
            inner: Arc::new(Unavailable(Arc::new(RedbStore::open(&dir.0, &root()).unwrap()))),
            health: health.clone(),
        };
        assert!(store.write().is_err());
        assert_eq!(health.failures.load(Ordering::Relaxed), 1);
        assert!(!health.last_write_ok.load(Ordering::Relaxed));
    }
    #[test]
    fn store_failures_count_and_recover_on_commit() {
        let dir = TestDir::new();
        let inner = Arc::new(RedbStore::open(&dir.0, &root()).unwrap());
        let health = Arc::new(StoreHealth::default());
        let store = CountingStore { inner: inner.clone(), health: health.clone() };
        assert!(health.last_write_ok.load(Ordering::Relaxed));
        inner.fail_commits(true);
        assert!(store.write().unwrap().commit().is_err());
        assert_eq!(health.failures.load(Ordering::Relaxed), 1);
        assert!(!health.last_write_ok.load(Ordering::Relaxed));
        inner.fail_commits(false);
        drop(store.write().unwrap());
        assert!(!health.last_write_ok.load(Ordering::Relaxed));
        store.write().unwrap().commit().unwrap();
        assert!(health.last_write_ok.load(Ordering::Relaxed));
        assert_eq!(health.failures.load(Ordering::Relaxed), 1);
    }
    #[test]
    fn metrics_count_session_events_and_connection_guards() {
        let dir = TestDir::new();
        let runtime = dir.open(&[]);
        let session = acquire(&runtime);
        let a = &runtime.authority;
        let replay = a.acquire_session(session.principal, AcquireRequest {
            route: session.route.clone(), mode: None, lifetime_ms: None, isolated: false,
            label: None, idempotency_key: "acquire".into(), verifier: Verifier(verifier()), secret_sha256: [1; 32],
        }).unwrap();
        assert_eq!(replay.id, session.id);
        assert_eq!(a.counters().sessions_issued, 1);
        assert_eq!(a.active_session_count(), 1);
        a.renew(session.principal, session.id, None, None).unwrap();
        assert_eq!(a.counters().sessions_renewed, 1);
        a.resume(session.principal, session.id).unwrap();
        assert_eq!(a.counters().resumes_refused, 0);
        let (guard, _) = runtime.registry.register(session.id, session.principal, "postgres", None);
        let values = runtime.metrics();
        assert_eq!(values, vec![
            ("nunya_sessions_active".into(), 1), ("nunya_connections_open".into(), 1),
            ("nunya_sessions_issued_total".into(), 1), ("nunya_sessions_renewed_total".into(), 1),
            ("nunya_sessions_revoked_total".into(), 0), ("nunya_sessions_expired_total".into(), 0),
            ("nunya_session_resumes_refused_total".into(), 0), ("nunya_store_write_failures_total".into(), 0),
        ]);
        a.revoke(session.id, EndReason::Requested).unwrap();
        a.revoke(session.id, EndReason::Requested).unwrap();
        assert!(a.resume(session.principal, session.id).is_err());
        assert!(a.renew(session.principal, session.id, None, None).is_err());
        drop(guard);
        let values = runtime.metrics();
        assert_eq!(values[0].1, 0);
        assert_eq!(values[1].1, 0);
        assert_eq!(values[3].1, 1);
        assert_eq!(values[4].1, 1);
        assert_eq!(values[6].1, 1);
        assert!(runtime.store_last_write_ok());
    }
    #[derive(Default)]
    struct Audit(std::sync::Mutex<Vec<LifecycleRecord>>);
    impl ReadLog for Audit {
        fn write(&self, _: &crate::readlog::ReadLogRecord) -> Result<(), crate::error::Refusal> { Ok(()) }
        fn write_lifecycle(&self, record: &LifecycleRecord) -> Result<(), crate::error::Refusal> {
            self.0.lock().unwrap().push(record.clone());
            Ok(())
        }
    }
    #[test]
    fn failed_lifecycle_sink_does_not_prevent_revocation() {
        struct Unavailable;
        impl ReadLog for Unavailable {
            fn write(&self, _: &crate::readlog::ReadLogRecord) -> Result<(), crate::error::Refusal> {
                Err(crate::error::Refusal::new(crate::error::NyCode::ReadLogUnavailable, "read log unavailable"))
            }
            fn write_lifecycle(&self, _: &LifecycleRecord) -> Result<(), crate::error::Refusal> {
                Err(crate::error::Refusal::new(crate::error::NyCode::ReadLogUnavailable, "read log unavailable"))
            }
        }
        let dir = TestDir::new();
        let runtime = dir.open(&[]);
        let session = acquire(&runtime);
        runtime.set_lifecycle_log(Arc::new(Unavailable));
        let (_guard, signal) = runtime.registry.register(session.id, session.principal, "postgres", None);
        runtime.authority.revoke(session.id, EndReason::Requested).unwrap();
        assert_eq!(signal.try_ended(), Some(ConnectionEndReason::Revoked));
        assert!(runtime.authority.admit(session.id, crate::state::authority::Operation::Read).is_err());
    }
    #[test]
    fn session_end_records_requested_and_grant_removed() {
        for reason in [EndReason::Requested, EndReason::GrantRemoved] {
            let dir = TestDir::new();
            let runtime = dir.open(&[]);
            let log = Arc::new(Audit::default());
            runtime.set_lifecycle_log(log.clone());
            let session = acquire(&runtime);
            match reason {
                EndReason::Requested => { runtime.authority.revoke(session.id, reason).unwrap(); }
                EndReason::GrantRemoved => { runtime.authority.set_grants(session.principal, vec![]).unwrap(); }
                EndReason::AdminRevoked | EndReason::PrincipalDisabled | EndReason::ConfigRemoved | EndReason::Expired | EndReason::IdleTimeout => panic!("unexpected test reason"),
            }
            let records = log.0.lock().unwrap();
            assert_eq!(records.len(), 1);
            let record = &records[0];
            assert_eq!(record.kind, LifecycleKind::SessionEnded);
            assert_eq!(record.actor, LifecycleActor::System);
            assert_eq!(record.reason, reason.as_str());
            assert_eq!(record.principal, session.principal.to_hex());
            assert_eq!(record.session, Some(session.id.to_hex()));
            assert_eq!(record.route.as_deref(), Some("postgres"));
            assert_eq!(record.connection, None);
        }
    }
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
        let log = Arc::new(Audit::default());
        runtime.set_lifecycle_log(log.clone());
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
        assert_eq!(runtime.metrics()[0].1, 0);
        assert_eq!(runtime.metrics()[5].1, 1);
        assert_eq!(runtime.authority.sweep().unwrap(), 0);
        assert_eq!(runtime.metrics()[5].1, 1);
        let records = log.0.lock().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].kind, LifecycleKind::SessionEnded);
        assert_eq!(records[0].reason, reason.as_str());
        assert_eq!(records[0].session, Some(session.id.to_hex()));
        assert_eq!(records[0].principal, session.principal.to_hex());
        assert_eq!(records[0].route.as_deref(), Some("postgres"));
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
            let ends = Arc::new(EndCounters::default());
            RegistryHook { registry, authority: Weak::new(), log: Arc::new(OnceLock::new()), ends: ends.clone() }.session_ended(session, principal, reason);
            let expired = matches!(reason, EndReason::Expired | EndReason::IdleTimeout);
            assert_eq!(ends.expired.load(Ordering::Relaxed), u64::from(expired));
            assert_eq!(ends.revoked.load(Ordering::Relaxed), u64::from(!expired));
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
