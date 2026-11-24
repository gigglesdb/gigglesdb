//! Process-local leases for the published engine generation.

use super::{CoreError, V2Db, engine::kv::FenceCause, native_once::OnceOwners};
#[cfg(feature = "engine-rocksdb")]
use super::{
    Inner,
    engine::kv::{FenceClass, FenceSite, FenceState},
    fence_supervisor::{FenceRecovery, Wake},
    v2::OpenFailure,
};
#[cfg(feature = "engine-rocksdb")]
use std::sync::OnceLock;
use std::{
    num::NonZeroU64,
    ops::Deref,
    sync::{
        Arc, Condvar, Mutex, Weak,
        atomic::{AtomicBool, AtomicPtr, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Holder {
    Admin,
    Sql,
    Transaction,
    Cursor,
    Export,
    Sequence,
    Xact,
    NativeOnce,
    Role,
    TaskCatalog,
    TaskRun,
    Readlog,
}
impl Holder {
    const ALL: [Self; 12] = [
        Self::Admin,
        Self::Sql,
        Self::Transaction,
        Self::Cursor,
        Self::Export,
        Self::Sequence,
        Self::Xact,
        Self::NativeOnce,
        Self::Role,
        Self::TaskCatalog,
        Self::TaskRun,
        Self::Readlog,
    ];
}

type Counts = Vec<(Holder, usize)>;
static NEXT_GENERATION_STAMP: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug)]
pub(crate) struct PreparedGeneration {
    pub(super) slot: NonZeroU64,
    pub(super) stamp: u64,
}
const CLOSED: usize = 1 << (usize::BITS - 1);
const STOPPING: usize = CLOSED >> 1;
const OWNER_DROPPED: usize = STOPPING >> 1;
const COUNT: usize = OWNER_DROPPED - 1;

#[derive(Default)]
#[repr(align(64))]
struct Active(AtomicUsize);

struct Registry {
    active: Active,
    generation: Weak<Generation>,
    prepared: PreparedGeneration,
    catalog: Option<super::engine::catalog::CatalogVersion>,
    kv_fenced: Arc<std::sync::atomic::AtomicBool>,
    kinds: [AtomicUsize; Holder::ALL.len()],
    drain: Mutex<()>,
    changed: Condvar,
    #[cfg(feature = "engine-rocksdb")]
    recovery_wake: OnceLock<Weak<Wake>>,
}
impl Registry {
    fn check(&self) -> Result<(), CoreError> {
        if self.active.0.load(Ordering::Acquire) & CLOSED != 0 {
            return Err(CoreError::Fenced);
        }
        Ok(())
    }

    fn new(generation: Weak<Generation>, db: &V2Db, slot: Option<NonZeroU64>) -> Self {
        let stamp = NEXT_GENERATION_STAMP
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
            .expect("generation identity exhausted");
        Self {
            active: Active::default(),
            generation,
            prepared: PreparedGeneration {
                slot: slot.unwrap_or_else(|| NonZeroU64::new(stamp).expect("nonzero generation stamp")),
                stamp,
            },
            catalog: db.catalog().version_observer(),
            kv_fenced: db.kv.fence_flag(),
            kinds: Default::default(),
            drain: Mutex::new(()),
            changed: Condvar::new(),
            #[cfg(feature = "engine-rocksdb")]
            recovery_wake: OnceLock::new(),
        }
    }

    fn register(&self, before_cas: impl FnOnce()) -> Result<(), CoreError> {
        // All registrations and close share this word's modification order.
        // If the CAS wins, drain includes it. If close wins, a CAS against
        // the pre-close word fails and a retry observes CLOSED.
        let mut before_cas = Some(before_cas);
        self.active
            .0
            .try_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                if n >= COUNT {
                    return None;
                }
                if let Some(hook) = before_cas.take() {
                    hook();
                }
                Some(n + 1)
            })
            .map(|_| ())
            .map_err(|_| CoreError::Fenced)
    }

    fn release(&self) {
        let old = self.active.0.fetch_sub(1, Ordering::AcqRel);
        if old & (CLOSED | COUNT) == CLOSED | 1 {
            self.notify();
        }
    }

    fn lease(&self, kind: Holder, before_cas: impl FnOnce()) -> Result<EngineLease, CoreError> {
        self.register(before_cas)?;
        if let Some(generation) = self.generation.upgrade() {
            Ok(EngineLease::registered(generation, kind))
        } else {
            self.release();
            Err(CoreError::Fenced)
        }
    }

    fn counts(&self) -> Counts {
        Holder::ALL
            .into_iter()
            .filter_map(|kind| {
                let n = self.kinds[kind as usize].load(Ordering::Relaxed);
                (n != 0).then_some((kind, n))
            })
            .collect()
    }

    fn notify(&self) {
        // Pair with the waiter's predicate check and release of the wait mutex.
        let _guard = self.drain.lock().unwrap_or_else(|p| p.into_inner());
        self.changed.notify_all();
        #[cfg(feature = "engine-rocksdb")]
        if let Some(wake) = self.recovery_wake.get().and_then(Weak::upgrade) {
            wake.signal();
        }
    }

    #[cfg(feature = "engine-rocksdb")]
    fn close(&self) {
        self.active.0.fetch_or(CLOSED, Ordering::AcqRel);
    }

    fn stop(&self) {
        self.active.0.fetch_or(CLOSED | STOPPING, Ordering::AcqRel);
        self.notify();
    }
}

pub(crate) struct Generation {
    pub(crate) id: u64,
    db: Arc<V2Db>,
    once_owners: OnceOwners,
    registry: Arc<Registry>,
}

impl Generation {
    fn new(id: u64, db: V2Db, slot: Option<NonZeroU64>) -> Arc<Self> {
        let db = Arc::new(db);
        db.attach_retention();
        Arc::new_cyclic(|generation| Self {
            registry: Arc::new(Registry::new(generation.clone(), &db, slot)),
            id,
            db,
            once_owners: Default::default(),
        })
    }

    #[cfg_attr(not(feature = "engine-rocksdb"), allow(dead_code))]
    pub(crate) fn drain(&self, timeout: Duration) -> Result<(), DrainError> {
        self.drain_with(timeout, || {})
    }

    fn drain_with(
        &self,
        timeout: Duration,
        mut before_wait: impl FnMut(),
    ) -> Result<(), DrainError> {
        let registry = &self.registry;
        let start = Instant::now();
        let mut guard = registry.drain.lock().unwrap_or_else(|p| p.into_inner());
        loop {
            let active = registry.active.0.load(Ordering::Acquire);
            if active & STOPPING != 0 {
                return Err(DrainError::Stopping);
            }
            if active & COUNT == 0 {
                return Ok(());
            }
            let Some(left) = timeout.checked_sub(start.elapsed()) else {
                return Err(DrainError::TimedOut {
                    active: active & COUNT,
                    holders: registry.counts(),
                });
            };
            before_wait();
            (guard, _) = registry
                .changed
                .wait_timeout(guard, left)
                .unwrap_or_else(|p| p.into_inner());
        }
    }

    /// Consume the drained owner and join the engine workers. The latch
    /// remains usable across timeouts while a residual native snapshot closes.
    #[cfg(feature = "engine-rocksdb")]
    pub(crate) fn close_drained(self: Arc<Self>) -> Result<ClosingGeneration, CoreError> {
        let active = self.registry.active.0.load(Ordering::Acquire);
        if active & (CLOSED | COUNT) != CLOSED {
            return Err(CoreError::Fenced);
        }
        // A concurrent terminal stop can refuse publication, but the drained
        // engine still needs explicit shutdown and joins on this thread.
        let report = self.db.shutdown();
        if self.registry.active.0.load(Ordering::Acquire) & STOPPING != 0 {
            return Err(CoreError::Fenced);
        }
        let latch = self.db.close_latch().ok_or(CoreError::Unsupported)?;
        let registry = Arc::clone(&self.registry);
        let domain = self.db.meta.domain;
        drop(self);
        Ok(ClosingGeneration {
            latch,
            registry,
            domain,
            report,
        })
    }
}

#[cfg(feature = "engine-rocksdb")]
#[allow(dead_code, reason = "Used by the recovery supervisor")]
pub(crate) struct ClosingGeneration {
    latch: super::engine::kv_rocksdb::CloseLatch,
    registry: Arc<Registry>,
    domain: [u8; 16],
    report: super::v2::CloseReport,
}

#[cfg(feature = "engine-rocksdb")]
impl ClosingGeneration {
    pub(super) fn notify_recovery(&self, wake: &Arc<Wake>) {
        self.latch.notify_recovery(wake);
    }

    pub(crate) fn wait(
        &self,
        timeout: Duration,
    ) -> Result<ClosedProof, super::engine::kv_rocksdb::StillHeld> {
        self.latch.wait(timeout)?;
        Ok(ClosedProof {
            registry: Arc::clone(&self.registry),
            domain: self.domain,
            ack_hw: self.report.ack_hw,
        })
    }
}

/// A proof is specific to a slot and generation.
#[cfg(feature = "engine-rocksdb")]
#[derive(Clone)]
pub(crate) struct ClosedProof {
    registry: Arc<Registry>,
    domain: [u8; 16],
    ack_hw: super::engine::hlc::Ts,
}

/// Only a successful recovery open against the closed generation's original
/// source can construct this value.
#[cfg(feature = "engine-rocksdb")]
pub(crate) struct RecoveryCandidate {
    registry: Arc<Registry>,
    db: Option<V2Db>,
}

#[cfg(all(test, feature = "engine-rocksdb"))]
impl RecoveryCandidate {
    pub(super) fn db(&self) -> &V2Db {
        self.db.as_ref().expect("unpublished recovery candidate")
    }
}

#[cfg(feature = "engine-rocksdb")]
impl Drop for RecoveryCandidate {
    fn drop(&mut self) {
        if let Some(db) = &self.db {
            db.shutdown();
        }
    }
}

#[cfg(feature = "engine-rocksdb")]
struct RecoverySource {
    path: std::path::PathBuf,
    identity: super::directory_identity::DirectoryIdentity,
    token: Option<Vec<u8>>,
}

/// Owns only the verified source and close proof, so the native open does
/// not keep an Admin or slot alive.
#[cfg(feature = "engine-rocksdb")]
pub(super) struct RecoveryOpenRequest {
    source: Arc<RecoverySource>,
    proof: ClosedProof,
    gc_retention: Option<Duration>,
}

#[cfg(feature = "engine-rocksdb")]
impl RecoveryOpenRequest {
    pub(super) fn open(&self) -> Result<RecoveryCandidate, OpenFailure> {
        let db = V2Db::open_recovery(
            &self.source.path,
            self.source.token.as_deref(),
            super::v2::RecoveryOpen {
                ack_hw: self.proof.ack_hw,
                gc_retention: self.gc_retention,
                defer_maintenance: true,
                identity: &self.source.identity,
                expected_domain: Some(self.proof.domain),
            },
        )?;
        Ok(RecoveryCandidate {
            registry: Arc::clone(&self.proof.registry),
            db: Some(db),
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum DrainError {
    TimedOut { active: usize, holders: Counts },
    Stopping,
}

/// A tag can outlive the generation without extending the drain. It is useful
/// for decoded state that can do work only by registering a new continuation.
#[derive(Clone)]
pub(crate) struct GenerationTag {
    registry: Arc<Registry>,
}
impl GenerationTag {
    pub(crate) fn check(&self) -> Result<(), CoreError> {
        self.registry.check()
    }

    /// Materialized cursors may keep returning rows after ordinary owner
    /// drop. Recovery and explicit stopping still invalidate the results.
    pub(super) fn check_materialized(&self) -> Result<(), CoreError> {
        if self.registry.active.0.load(Ordering::Acquire) & (CLOSED | OWNER_DROPPED) == CLOSED {
            return Err(CoreError::Fenced);
        }
        Ok(())
    }

    pub(crate) fn stamp(&self) -> u64 {
        self.registry.prepared.stamp
    }

    pub(super) fn prepared_generation(&self) -> PreparedGeneration {
        self.registry.prepared
    }

    pub(crate) fn lease(&self, kind: Holder) -> Result<EngineLease, CoreError> {
        self.registry.lease(kind, || {})
    }
}

pub(crate) struct EngineLease {
    generation: Arc<Generation>,
    kind: Holder,
}
impl std::fmt::Debug for EngineLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineLease")
            .field("generation", &self.id())
            .field("kind", &self.kind)
            .finish()
    }
}
impl EngineLease {
    pub(super) fn fence_engine_error(
        &self,
        site: super::engine::kv::FenceSite,
        error: &CoreError,
        flag: &AtomicBool,
    ) {
        self.kv.causes.record_fence(FenceCause::for_core(site, error), flag, false, *error == CoreError::Fenced);
    }

    pub(super) fn fence_native_commit(&self, flag: &AtomicBool) {
        use super::engine::kv::{FenceClass, FenceSite};
        self.kv.causes.record_fence(
            FenceCause::new(
                FenceSite::NativeOnceCommit,
                FenceClass::NotRecoverable,
                &"engine commit outcome unknown",
            ),
            flag,
            true,
            false,
        );
    }

    pub(super) fn once_owners(&self) -> &OnceOwners {
        &self.generation.once_owners
    }

    pub(crate) fn id(&self) -> u64 {
        self.generation.id
    }

    pub(crate) fn stamp(&self) -> u64 {
        self.generation.registry.prepared.stamp
    }

    pub(super) fn prepared_generation(&self) -> PreparedGeneration {
        self.generation.registry.prepared
    }

    pub(crate) fn begin_leased(
        &self,
        isolation: super::Isolation,
        kind: Holder,
    ) -> Result<super::Transaction, CoreError> {
        let lease = self.child(kind)?;
        let mut txn = self.begin(isolation);
        txn.attach_lease(lease);
        Ok(txn)
    }

    pub(crate) fn tag(&self) -> GenerationTag {
        GenerationTag {
            registry: Arc::clone(&self.generation.registry),
        }
    }

    /// A borrowed continuation keeps its existing registration. Close makes
    /// this flag permanent for this generation, even after another is installed.
    pub(crate) fn check(&self) -> Result<(), CoreError> {
        self.generation.registry.check()
    }

    #[cfg(feature = "engine-rocksdb")]
    pub(crate) fn same_generation(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.generation, &other.generation)
    }

    pub(crate) fn child(&self, kind: Holder) -> Result<Self, CoreError> {
        self.generation.registry.register(|| {})?;
        Ok(Self::registered(Arc::clone(&self.generation), kind))
    }

    fn registered(generation: Arc<Generation>, kind: Holder) -> Self {
        // Ordinary statement counts are included in active. Long-lived pins
        // and other cold holders have separate diagnostic counters.
        if kind != Holder::Sql {
            generation.registry.kinds[kind as usize].fetch_add(1, Ordering::Relaxed);
        }
        Self { generation, kind }
    }
}

impl Deref for EngineLease {
    type Target = V2Db;
    fn deref(&self) -> &V2Db {
        &self.generation.db
    }
}
impl Drop for EngineLease {
    fn drop(&mut self) {
        let registry = &self.generation.registry;
        if self.kind != Holder::Sql {
            registry.kinds[self.kind as usize].fetch_sub(1, Ordering::Relaxed);
        }
        registry.release();
    }
}

#[cfg_attr(not(feature = "engine-rocksdb"), allow(dead_code))]
enum State {
    Serving(Arc<Generation>),
    Recovering {
        generation: u64,
        registry: Arc<Registry>,
        #[cfg(feature = "engine-rocksdb")]
        causes: Arc<FenceState>,
        since: Instant,
        attempt: u32,
    },
    GaveUp {
        generation: u64,
        cause: FenceCause,
        registry: Arc<Registry>,
        since: Instant,
        attempts: u32,
    },
    Stopping {
        // Keep the stopped engine alive through the service drain.
        _generation: Option<Arc<Generation>>,
    },
}

#[derive(Debug, PartialEq, Eq)]
pub enum StorageStatus {
    Legacy,
    Serving {
        generation: u64,
        fenced: bool,
        cause: Option<FenceCause>,
        holders: Counts,
    },
    Recovering {
        generation: u64,
        since: Instant,
        attempt: u32,
        cause: Option<FenceCause>,
        holders: Counts,
    },
    GaveUp {
        generation: u64,
        since: Instant,
        attempts: u32,
        cause: FenceCause,
        holders: Counts,
    },
    Stopping,
}

#[derive(Default)]
struct Settings {
    gc_retention: Option<Duration>,
    task_retention: Option<crate::task::RetentionConfig>,
    task_wall_ms: Option<u64>,
    #[cfg(any(test, feature = "test-commit-sync"))]
    sync_hook: Option<Arc<crate::CommitSyncHook>>,
}

pub(crate) struct EngineSlot {
    state: Mutex<State>,
    changed: Condvar,
    current: AtomicPtr<Registry>,
    // Registries can be sampled lock-free before their CAS. Keep their small
    // allocations alive until slot drop; their Weak does not own an engine.
    // This retains one small registry per successful recovery.
    #[cfg_attr(not(feature = "engine-rocksdb"), allow(dead_code, reason = "Owns the initial registry for the slot lifetime"))]
    publications: Mutex<Vec<Arc<Registry>>>,
    #[cfg(feature = "engine-rocksdb")]
    recovery_source: Option<Arc<RecoverySource>>,
    #[cfg(feature = "engine-rocksdb")]
    recovery_wake: OnceLock<Arc<Wake>>,
    settings: Mutex<Box<Settings>>,
    cluster: bool,
    pub(super) native_fenced: Arc<AtomicBool>,
}

impl EngineSlot {
    pub(crate) fn new(db: V2Db) -> Self {
        let cluster = db.cluster_services().is_some();
        let generation = Generation::new(1, db, None);
        let registry = Arc::clone(&generation.registry);
        Self {
            state: Mutex::new(State::Serving(generation)),
            changed: Condvar::new(),
            current: AtomicPtr::new(Arc::as_ptr(&registry).cast_mut()),
            publications: Mutex::new(vec![registry]),
            #[cfg(feature = "engine-rocksdb")]
            recovery_source: None,
            #[cfg(feature = "engine-rocksdb")]
            recovery_wake: OnceLock::new(),
            settings: Mutex::default(),
            cluster,
            native_fenced: Arc::default(),
        }
    }

    #[cfg(feature = "engine-rocksdb")]
    pub(super) fn for_directory(
        mut db: V2Db,
        path: &std::path::Path,
        token: Option<&[u8]>,
    ) -> Self {
        let identity = db.directory_identity.take();
        let mut slot = Self::new(db);
        slot.recovery_source = identity.map(|identity| Arc::new(RecoverySource {
            path: path.to_path_buf(),
            identity,
            token: token.map(<[u8]>::to_vec),
        }));
        slot
    }

    #[cfg(feature = "engine-rocksdb")]
    pub(super) fn set_fence_recovery(
        &self,
        owner: Weak<Inner>,
        settings: FenceRecovery,
    ) -> Result<(), CoreError> {
        self.start_recovery(settings, |wake| super::fence_supervisor::spawn(owner, wake, settings))
    }

    #[cfg(feature = "engine-rocksdb")]
    pub(super) fn start_recovery(
        &self,
        settings: FenceRecovery,
        spawn: impl FnOnce(Arc<Wake>) -> std::io::Result<()>,
    ) -> Result<(), CoreError> {
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if !settings.enabled && self.recovery_wake.get().is_none() {
            return Ok(());
        }
        let State::Serving(generation) = &*state else {
            return Err(CoreError::Fenced);
        };
        if generation.db.sequencer.stopped() {
            return Err(CoreError::Fenced);
        }
        if self.recovery_wake.get().is_some() {
            return Err(CoreError::Storage("fence recovery is already configured".into()));
        }
        settings.validate()?;
        if self.recovery_source.is_none() || generation.db.task_kernel_unsupported()
            || !cfg!(any(target_os = "linux", windows))
        {
            return Err(CoreError::Unsupported);
        }
        let wake = Arc::new(Wake::default());
        spawn(Arc::clone(&wake)).map_err(|e| CoreError::Storage(e.to_string()))?;
        self.watch_generation(generation, &wake);
        assert!(self.recovery_wake.set(wake).is_ok());
        Ok(())
    }

    #[cfg(feature = "engine-rocksdb")]
    fn watch_generation(&self, generation: &Generation, wake: &Arc<Wake>) {
        assert!(generation.registry.recovery_wake.set(Arc::downgrade(wake)).is_ok());
        generation.db.kv.causes.notify_recovery(generation.id, wake);
    }

    #[cfg(all(test, feature = "engine-rocksdb"))]
    pub(super) fn recovery_enabled(&self) -> bool {
        self.recovery_wake.get().is_some()
    }

    pub(crate) fn is_cluster(&self) -> bool {
        self.cluster
    }

    pub(crate) fn fresh(&self, kind: Holder) -> Result<EngineLease, CoreError> {
        self.register(kind, || {})
    }

    fn register(&self, kind: Holder, before_cas: impl FnOnce()) -> Result<EngineLease, CoreError> {
        self.registry().lease(kind, before_cas)
    }

    fn registry(&self) -> &Registry {
        let registry = self.current.load(Ordering::Acquire);
        // SAFETY: initialization and publish store only pointers into Arcs
        // owned by publications. Entries are never removed, and &self keeps
        // them alive for registration and read-only metadata observations.
        // The old registry stays CLOSED forever after the pointer changes.
        unsafe { &*registry }
    }

    pub(crate) fn serves_stamp(&self, stamp: u64) -> bool {
        let registry = self.registry();
        registry.prepared.stamp == stamp && registry.check().is_ok()
    }

    pub(crate) fn catalog_version(&self) -> Option<u64> {
        let registry = self.registry();
        registry.check().ok()?;
        registry.catalog.as_ref()?.read()
    }

    pub(super) fn authorization_versions(
        &self,
        statement_version: impl FnOnce(u64, &super::engine::catalog::CatalogVersion) -> Option<u64>,
    ) -> Option<(u64, u64, u64)> {
        if self.is_cluster() { return None; }
        let registry = self.registry();
        if registry.kv_fenced.load(Ordering::Acquire) { return None; }
        let catalog = registry.catalog.as_ref()?;
        let latest = catalog.read()?;
        let statement = statement_version(registry.prepared.stamp, catalog)?;
        if catalog.read() != Some(latest) { return None; }
        // Publication can supersede this registry only after permanently
        // closing it. This check revalidates the sampled generation even
        // while recovery is still between closing it and publishing the next.
        registry.check().ok()?;
        Some((latest, statement, registry.prepared.stamp))
    }

    // The state mutex excludes cold observers while the Release store
    // publishes the new generation. The prior registry is permanently closed.
    #[cfg(feature = "engine-rocksdb")]
    fn publish(&self, state: &mut State, generation: Arc<Generation>) {
        let registry = Arc::clone(&generation.registry);
        let ptr = Arc::as_ptr(&registry).cast_mut();
        self.publications
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(registry);
        *state = State::Serving(generation);
        self.current.store(ptr, Ordering::Release);
        self.changed.notify_all();
    }

    pub(crate) fn stop_submissions(&self) {
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        #[cfg(feature = "engine-rocksdb")]
        let enabled = self.recovery_wake.get().is_some();
        #[cfg(not(feature = "engine-rocksdb"))]
        let enabled = false;
        if !enabled && let State::Serving(generation) = &*state {
            generation.db.sequencer.stop_submissions();
            return;
        }
        self.stop_locked(state, || {});
    }

    pub(crate) fn set_gc_retention(&self, retention: Duration) -> Result<(), CoreError> {
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let State::Serving(generation) = &*state else {
            return Err(CoreError::Fenced);
        };
        generation.db.set_gc_retention(retention);
        self.settings
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .gc_retention = Some(retention);
        Ok(())
    }

    pub(crate) fn set_task_retention(
        &self,
        config: crate::task::RetentionConfig,
    ) -> Result<(), CoreError> {
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let State::Serving(generation) = &*state else {
            return Err(CoreError::Fenced);
        };
        generation.db.set_task_retention(config);
        self.settings
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .task_retention = Some(config);
        Ok(())
    }

    pub(crate) fn set_task_wall_clock(&self, ms: Option<u64>) -> Result<(), CoreError> {
        use super::engine::commit::{Clock, ManualClock, SystemClock};
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let State::Serving(generation) = &*state else {
            return Err(CoreError::Fenced);
        };
        let clock: Arc<dyn Clock> = match ms {
            Some(ms) => Arc::new(ManualClock::new(ms)),
            None => Arc::new(SystemClock),
        };
        generation.db.set_wall_clock(clock);
        self.settings
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .task_wall_ms = ms;
        Ok(())
    }

    #[cfg(any(test, feature = "test-commit-sync"))]
    pub(crate) fn set_sync_hook(&self, hook: Option<Arc<crate::CommitSyncHook>>) {
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let generation = match &*state {
            State::Serving(generation) => Some(Arc::clone(generation)),
            State::Recovering { registry, .. } | State::GaveUp { registry, .. } => {
                registry.generation.upgrade()
            }
            State::Stopping { .. } => self.registry().generation.upgrade(),
        };
        if let Some(generation) = &generation {
            generation.db.sequencer.set_sync_hook(hook.clone());
        }
        self.settings
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .sync_hook = hook;
        drop(state);
        drop(generation);
    }

    pub(crate) fn fence_cause(&self) -> Option<FenceCause> {
        match &*self.state.lock().unwrap_or_else(|p| p.into_inner()) {
            State::Serving(generation) => generation.db.fence_cause(),
            State::GaveUp { cause, .. } => Some(cause.clone()),
            _ => None,
        }
    }

    #[cfg(feature = "engine-rocksdb")]
    pub(crate) fn tag(&self) -> Option<GenerationTag> {
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        match &*state {
            State::Serving(generation) => Some(GenerationTag {
                registry: Arc::clone(&generation.registry),
            }),
            _ => None,
        }
    }

    pub(crate) fn status(&self) -> StorageStatus {
        match &*self.state.lock().unwrap_or_else(|p| p.into_inner()) {
            State::Serving(g) => StorageStatus::Serving {
                generation: g.id,
                fenced: g.db.is_fenced() || self.native_fenced.load(Ordering::Acquire),
                cause: g.db.fence_cause(),
                holders: g.registry.counts(),
            },
            State::Recovering {
                generation,
                registry,
                since,
                attempt,
                #[cfg(feature = "engine-rocksdb")]
                causes,
            } => StorageStatus::Recovering {
                generation: *generation,
                since: *since,
                attempt: *attempt,
                #[cfg(feature = "engine-rocksdb")]
                cause: causes.cause(),
                #[cfg(not(feature = "engine-rocksdb"))]
                cause: None,
                holders: registry.counts(),
            },
            State::GaveUp {
                generation,
                cause,
                registry,
                since,
                attempts,
            } => StorageStatus::GaveUp {
                generation: *generation,
                since: *since,
                attempts: *attempts,
                cause: cause.clone(),
                holders: registry.counts(),
            },
            State::Stopping { .. } => StorageStatus::Stopping,
        }
    }

    #[cfg(all(test, feature = "engine-rocksdb"))]
    pub(crate) fn stop_admission(&self, expected: u64) -> Option<Arc<Generation>> {
        self.admit_recovery(expected, false).map(|(generation, _)| generation)
    }

    #[cfg(feature = "engine-rocksdb")]
    pub(super) fn admit_recovery(
        &self,
        expected: u64,
        require_recoverable: bool,
    ) -> Option<(Arc<Generation>, Arc<FenceState>)> {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let State::Serving(generation) = &*state else {
            return None;
        };
        if generation.id != expected || generation.db.sequencer.stopped()
            || generation.db.task_kernel_unsupported()
        {
            return None;
        }
        let generation = Arc::clone(generation);
        let causes = Arc::clone(&generation.db.kv.causes);
        causes.with_cause(|cause| {
            if require_recoverable
                && (!(generation.db.is_fenced() || self.native_fenced.load(Ordering::Acquire))
                    || !cause.is_some_and(|cause| {
                        cause.class == FenceClass::Recoverable && cause.generation == expected
                    }))
            {
                return None;
            }
            generation.registry.close();
            *state = State::Recovering {
                generation: expected,
                registry: Arc::clone(&generation.registry),
                causes: Arc::clone(&causes),
                since: Instant::now(),
                attempt: 0,
            };
            self.changed.notify_all();
            Some((generation, Arc::clone(&causes)))
        })
    }

    #[cfg(feature = "engine-rocksdb")]
    pub(super) fn give_up(&self, expected: u64, mut cause: FenceCause) -> Option<(FenceCause, u32)> {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let State::Recovering { generation, registry, causes, attempt, .. } = &*state else {
            return None;
        };
        if *generation != expected { return None; }
        let causes = Arc::clone(causes);
        let registry = Arc::clone(registry);
        let attempts = *attempt;
        causes.with_cause(|latest| {
            if let Some(latest) = latest.filter(|c| c.class != FenceClass::Recoverable) {
                cause = latest.clone();
            }
            cause.generation = expected;
            *state = State::GaveUp {
                generation: expected,
                cause: cause.clone(),
                registry,
                since: Instant::now(),
                attempts,
            };
            self.changed.notify_all();
            Some((cause, attempts))
        })
    }

    #[cfg(feature = "engine-rocksdb")]
    pub(super) fn reopen_request(
        &self,
        proof: &ClosedProof,
        attempt: u32,
    ) -> Result<RecoveryOpenRequest, OpenFailure> {
        let source = self.recovery_source.as_ref().ok_or(CoreError::Unsupported)?;
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let State::Recovering { registry, causes, attempt: current, .. } = &mut *state else {
            return Err(CoreError::Fenced.into());
        };
        if !Arc::ptr_eq(registry, &proof.registry) {
            return Err(CoreError::Fenced.into());
        }
        causes.with_cause(|cause| {
            if let Some(cause) = cause.filter(|c| c.class != FenceClass::Recoverable) {
                return Err(OpenFailure { error: CoreError::Fenced, cause: cause.clone() });
            }
            *current = attempt;
            let gc_retention = self.settings.lock().unwrap_or_else(|p| p.into_inner()).gc_retention;
            Ok(RecoveryOpenRequest {
                source: Arc::clone(source),
                proof: proof.clone(),
                gc_retention,
            })
        })
    }

    #[cfg(all(test, feature = "engine-rocksdb"))]
    pub(crate) fn reopen_candidate(&self, proof: &ClosedProof) -> Result<RecoveryCandidate, OpenFailure> {
        self.reopen_request(proof, 1)?.open()
    }

    #[cfg(all(test, feature = "engine-rocksdb"))]
    pub(crate) fn install(&self, mut candidate: RecoveryCandidate) -> Result<(), CoreError> {
        self.install_recovery(&mut candidate).map_err(|_| CoreError::Fenced)
    }

    #[cfg(feature = "engine-rocksdb")]
    pub(super) fn install_recovery(&self, candidate: &mut RecoveryCandidate) -> Result<(), FenceCause> {
        self.install_with(candidate, |_| {}, || {})
    }

    #[cfg(feature = "engine-rocksdb")]
    fn install_with(
        &self,
        candidate: &mut RecoveryCandidate,
        before_publish: impl FnOnce(&V2Db),
        after_publish: impl FnOnce(),
    ) -> Result<(), FenceCause> {
        let refused = |text: &str| FenceCause::new(FenceSite::Open, FenceClass::NotRecoverable, &text);
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let State::Recovering { generation, registry, causes, .. } = &*state else {
            return Err(refused("storage is no longer recovering"));
        };
        if !Arc::ptr_eq(registry, &candidate.registry) {
            return Err(refused("recovery candidate belongs to another generation"));
        }
        let next = generation.checked_add(1).ok_or_else(|| refused("storage generation exhausted"))?;
        let slot_id = registry.prepared.slot;
        let causes = Arc::clone(causes);
        let db = candidate.db.as_ref().expect("unpublished recovery candidate");
        let settings = self.settings.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(retention) = settings.gc_retention {
            db.set_gc_retention(retention);
        }
        if let Some(config) = &settings.task_retention {
            db.set_task_retention(*config);
        }
        if let Some(ms) = settings.task_wall_ms {
            db.set_wall_clock(Arc::new(super::engine::commit::ManualClock::new(ms)));
        }
        #[cfg(any(test, feature = "test-commit-sync"))]
        db.sequencer.set_sync_hook(settings.sync_hook.clone());
        // Serialize publication with a terminal fence report from the old
        // generation. The candidate's maintenance is still paused.
        causes.with_cause(|old| {
            if let Some(cause) = old.filter(|cause| cause.class != FenceClass::Recoverable) {
                return Err(cause.clone());
            }
            let db = candidate.db.as_ref().expect("unpublished recovery candidate");
            if let Some(cause) = db.fence_cause() {
                return Err(cause);
            }
            if db.is_fenced() || !db.sequencer.is_accepting() {
                return Err(refused("recovery candidate is fenced or not accepting commits"));
            }
            before_publish(db);
            let db = candidate.db.take().expect("healthy recovery candidate");
            let generation = Generation::new(next, db, Some(slot_id));
            if let Some(wake) = self.recovery_wake.get() {
                self.watch_generation(&generation, wake);
            }
            self.native_fenced.store(false, Ordering::Release);
            self.publish(&mut state, Arc::clone(&generation));
            after_publish();
            generation.db.start_recovery_maintenance();
            Ok(())
        })
    }

    #[allow(dead_code, reason = "Used by the recovery supervisor")]
    pub(crate) fn begin_stopping(&self) {
        self.stop_with(|| {});
    }

    fn stop_with(&self, after_publish: impl FnOnce()) {
        self.stop_locked(self.state.lock().unwrap_or_else(|p| p.into_inner()), after_publish);
    }

    fn stop_locked(&self, mut state: std::sync::MutexGuard<'_, State>, after_publish: impl FnOnce()) {
        let (registry, generation) = match &*state {
            State::Serving(generation) => (&generation.registry, Some(Arc::clone(generation))),
            State::Recovering { registry, .. } | State::GaveUp { registry, .. } => {
                (registry, registry.generation.upgrade())
            }
            State::Stopping { .. } => return,
        };
        if let Some(generation) = &generation {
            generation.db.sequencer.stop_submissions();
        }
        registry.stop();
        let old = std::mem::replace(&mut *state, State::Stopping { _generation: generation });
        self.changed.notify_all();
        #[cfg(feature = "engine-rocksdb")]
        if let Some(wake) = self.recovery_wake.get() {
            wake.stop();
        }
        drop(state);
        after_publish();
        drop(old);
    }
}

impl Drop for EngineSlot {
    fn drop(&mut self) {
        #[cfg(feature = "engine-rocksdb")]
        if let Some(wake) = self.recovery_wake.get() {
            wake.stop();
        }
        // Release the identity handle before stopping or dropping a generation
        // can close RocksDB and signal its latch.
        #[cfg(feature = "engine-rocksdb")]
        drop(self.recovery_source.take());
        // Ordinary owner drop preserves already materialized results. A
        // streaming cursor keeps a slot owner alongside its counted pin.
        let state = self.state.get_mut().unwrap_or_else(|p| p.into_inner());
        if let State::Serving(generation) = state {
            generation.db.sequencer.stop_submissions();
            generation.registry.active.0.fetch_or(CLOSED | STOPPING | OWNER_DROPPED, Ordering::AcqRel);
        } else {
            self.begin_stopping();
        }
    }
}

#[cfg(all(test, feature = "engine-rocksdb"))]
#[path = "engine_slot_tests.rs"]
mod tests;
