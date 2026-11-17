//! Track A owns this module.
//! `RangeEngine`: the leader-side engine of one range (GD-0075 R5-32..R5-37; design.md 4, 4a).
//!
//! An engine is opened by the range group's role manager after the leader's read barrier and
//! closed (fenced) on leadership loss. It runs the existing `Sequencer` with oracle timestamps
//! (`TsSource::Oracle`) and a `LockManager` over the range's `RangeKv`: every write goes
//! through the group's sink and is applied on every replica by `RangeStore::apply`.
//!
//! - Reads (read.rs) follow O6: raise the served floor to T, wait for reservations and
//!   prepared records at or below T, then read MVCC at T.
//! - Locks (locking.rs) are held in memory and die with the engine; each transaction holding
//!   locks here has a lease renewed by `heartbeat` (and by every lock call); an expired lease
//!   releases the locks of a transaction that has not prepared.
//! - Commit, prepare and finalize (txns.rs) check that every write is inside the range and
//!   that the transaction took its locks in this term; a prepared record (prepared.rs) is
//!   range-local data, so a new engine re-installs its locks at open.
//! - Lock generation (design.md Changes 2026-11-16T10:40Z): every grant carries the
//!   generation the engine was opened with; commit and prepare carrying another generation
//!   are refused before anything changes (locks were lost to a leader change or reopen).
//!   Finalize never checks it.
use std::{
    collections::HashMap,
    sync::{
        Arc, Condvar, Mutex, MutexGuard, OnceLock, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

use super::home::{HomeRecords, RecoveryConfig};
use super::kv::RangeKv;
use super::local::local_interval;
use super::oracle::SystemRange;
use super::store::{RangeStore, child_route, epoch_refusal, stale_descriptor};
use super::txn_record::resolve_prepared;
use crate::storage::engine::{
    commit::{Sequencer, SequencerConfig, ServedGate, SystemClock, TsSource},
    hlc::Ts,
    kv::{Direction, Keyspace, Kv, KvBackend, KvError, KvRead},
    kv_rocksdb::RocksKv,
    locks::{LockConfig, LockError, LockManager, TxnId},
    meta::{RangeLocalKey, parse_range_local_key},
    txn::TxnError,
};
use crate::storage::range_api::{
    ClusterTxnId, Epoch, HomeOp, HomeReply, LockGrant, LockRequest, Outcome, RangeClient,
    RangeDescriptor, RangeEngineOpener, RangeError, RangeId, RangeService, RangeWrite, ReadPage,
    ReadRequest, SYSTEM_RANGE, Timestamp, TsOracle,
};
use crate::storage::replicated::BatchSink;

mod home_log;
mod locking;
pub(super) mod prepared;
mod read;
pub(crate) mod reclaim;
mod txns;

#[cfg(test)]
mod client_tests;
#[cfg(test)]
mod split_tests;
#[cfg(test)]
mod tests;

/// Engine tunables.
#[derive(Clone, Debug)]
pub(crate) struct EngineConfig {
    /// Lock lease of a transaction that has not prepared (renewed by heartbeat).
    pub(crate) lease: Duration,
    /// How long a read waits for in-flight commits and prepared records at or below its ts.
    pub(crate) read_wait: Duration,
    /// Lease sweep period.
    pub(crate) sweep: Duration,
    /// How long a read or lock request waits on a prepared transaction before asking its home
    /// (R5-53), and how often it asks again.
    pub(crate) resolve_after: Duration,
    /// Bounds of the home-record recovery worker (R5-52).
    pub(crate) recovery: RecoveryConfig,
    /// Period of the recovery worker.
    pub(crate) recovery_tick: Duration,
    /// R5-101: reads older than now - gc_ttl are refused, and dropped tables' and indexes'
    /// key runs whose drop is older than that are reclaimed (as CockroachDB's gc.ttl).
    pub(crate) gc_ttl: Duration,
    /// How often the reclaim worker runs a pass on every open engine.
    pub(crate) reclaim_every: Duration,
}
impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            lease: Duration::from_secs(10),
            read_wait: Duration::from_secs(5),
            sweep: Duration::from_millis(500),
            resolve_after: Duration::from_millis(250),
            recovery: RecoveryConfig::default(),
            recovery_tick: Duration::from_secs(1),
            gc_ttl: Duration::from_secs(4 * 60 * 60),
            reclaim_every: Duration::from_secs(5 * 60),
        }
    }
}

/// A transaction known to this engine in this term.
struct TxnEntry {
    expires: Instant,
    prepared: bool,
    /// A commit or prepare of this transaction is running: the sweeper, release and finalize
    /// leave it alone, and a second commit or prepare waits for it.
    busy: bool,
}

#[derive(Default)]
struct EngineState {
    txns: HashMap<ClusterTxnId, TxnEntry>,
    prepared: HashMap<ClusterTxnId, prepared::Prepared>,
    /// Transactions whose lease expired in this term: later calls get `Aborted` instead of a
    /// fresh lease, until the gateway's release or finalize forgets them.
    expired: std::collections::HashSet<ClusterTxnId>,
}

pub(crate) struct RangeEngine {
    descriptor: RangeDescriptor,
    kv: Arc<Kv>,
    /// The node's store, for the stored descriptor and split lineage (R5-80).
    stored: Arc<RocksKv>,
    seq: Arc<Sequencer>,
    gate: Arc<ServedGate>,
    locks: LockManager,
    oracle: Arc<dyn TsOracle>,
    config: EngineConfig,
    state: Mutex<EngineState>,
    changed: Condvar,
    closed: AtomicBool,
    /// Lock-table generation from the opener, stamped on every grant.
    generation: u64,
    /// Track D's home records of transactions homed here (R5-50), over `home_log`.
    home: OnceLock<HomeRecords>,
    /// Other ranges, for resolving prepared records through their homes (R5-53). Weak: the
    /// client normally owns the engines. Without one, readers and lockers only wait.
    client: OnceLock<Weak<dyn RangeClient>>,
}

fn txn_id(txn: ClusterTxnId) -> TxnId {
    TxnId(txn.0)
}

pub(super) fn ts(at: Timestamp) -> Ts {
    Ts(at.0)
}

fn lock_error(error: LockError) -> RangeError {
    match error {
        LockError::Deadlock => RangeError::Deadlock,
        LockError::Timeout => RangeError::LockTimeout,
        LockError::Cancelled => RangeError::Cancelled,
        LockError::Limit => RangeError::Unavailable("lock limit".into()),
        LockError::Misuse => RangeError::Corrupt("lock misuse".into()),
    }
}

/// A sequencer or KV failure. Fenced = nothing was written in this term; a KV write error
/// = the replication outcome is unknown, except a fenced KV (RangeKv: the sink said
/// NotLeader, or the handle was already fenced), which wrote nothing (R5-81).
fn txn_error(error: TxnError) -> RangeError {
    match error {
        TxnError::Fenced => RangeError::Aborted("range engine fenced".into()),
        TxnError::Kv(KvError::Fenced) => RangeError::NotLeader { hint: None },
        TxnError::Kv(error) => RangeError::Unknown(error.to_string()),
        TxnError::CommitAmbiguous => RangeError::Unknown("commit outcome unknown".into()),
        TxnError::Deadlock => RangeError::Deadlock,
        TxnError::LockTimeout => RangeError::LockTimeout,
        TxnError::Cancelled => RangeError::Cancelled,
        TxnError::LockLimit => RangeError::Unavailable("lock limit".into()),
        TxnError::Codec(crate::storage::engine::codec::CodecError::Clock) => {
            RangeError::Unavailable("timestamp oracle".into())
        }
        other => RangeError::Corrupt(format!("{other:?}")),
    }
}

impl RangeEngine {
    /// Opens the engine of `descriptor`'s range over `store`. The caller has passed the
    /// leader's read barrier, so the store holds every committed entry of earlier terms.
    pub(crate) fn open(
        store: &RangeStore,
        descriptor: RangeDescriptor,
        sink: Arc<dyn BatchSink>,
        oracle: Arc<dyn TsOracle>,
        config: EngineConfig,
        generation: u64,
    ) -> Result<Arc<Self>, RangeError> {
        let corrupt = |e: crate::CoreError| RangeError::Corrupt(format!("{e:?}"));
        let install =
            crate::storage::engine::meta::range_local_key(descriptor.id, &RangeLocalKey::Install);
        if store
            .kv()
            .get(Keyspace::Meta, &install)
            .map_err(|e| RangeError::Corrupt(e.to_string()))?
            .is_some()
        {
            return Err(RangeError::Unavailable("range needs a snapshot".into()));
        }
        match store.descriptor(descriptor.id).map_err(corrupt)? {
            Some(stored) if stored == descriptor => {}
            Some(stored) => return Err(RangeError::StaleEpoch { descriptor: stored }),
            None => {
                return Err(RangeError::Corrupt(format!(
                    "unknown range {:?}",
                    descriptor.id
                )));
            }
        }
        let backend: Arc<dyn KvBackend> = store.kv().clone();
        let kv = Arc::new(Kv::new(Arc::new(RangeKv::new(
            backend,
            descriptor.clone(),
            sink,
        ))));
        let seq = Sequencer::open(
            kv.clone(),
            Arc::new(SystemClock),
            SequencerConfig {
                ts_source: TsSource::Oracle(oracle.clone()),
                ..SequencerConfig::default()
            },
        )
        .map_err(txn_error)?;
        let gate = seq.served_gate();
        // O7: nothing reserved in an earlier term is used; the floor starts at a fresh ts.
        gate.raise(ts(oracle.next(1)?));
        if descriptor.id == SYSTEM_RANGE {
            check_role_bootstrap(kv.as_ref())?;
        }
        let engine = Arc::new(Self {
            descriptor,
            kv,
            stored: store.kv().clone(),
            seq,
            gate,
            locks: LockManager::new(LockConfig::default()),
            oracle,
            config,
            state: Mutex::new(EngineState::default()),
            changed: Condvar::new(),
            closed: AtomicBool::new(false),
            generation,
            home: OnceLock::new(),
            client: OnceLock::new(),
        });
        engine.reinstall_prepared(store)?;
        let log = Arc::new(home_log::EngineHomeLog(Arc::downgrade(&engine)));
        let _ = engine.home.set(HomeRecords::new(log));
        spawn_sweeper(&engine);
        Ok(engine)
    }

    /// Fences the engine: every later call fails with NotLeader and waiting locks give up.
    /// Then waits up to `drain` for writes already handed to the sink, so a new engine of
    /// the range on this node never starts below an old-term write still in flight (O7).
    /// False if some write was still in flight at the deadline.
    pub(crate) fn close_and_drain(&self, drain: Duration) -> bool {
        self.closed.store(true, Ordering::SeqCst);
        self.kv.fence();
        self.changed.notify_all();
        self.gate.drain(Instant::now() + drain)
    }

    /// Fences the engine without waiting.
    #[cfg(test)]
    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        self.kv.fence();
        self.changed.notify_all();
    }

    /// Lets the engine ask other ranges (R5-53). Set once; later calls are ignored.
    pub(crate) fn set_client(&self, client: Weak<dyn RangeClient>) {
        let _ = self.client.set(client);
    }

    fn client(&self) -> Option<Arc<dyn RangeClient>> {
        self.client.get().and_then(Weak::upgrade)
    }

    /// R5-53 (O6): resolves each blocking prepared transaction through its home record. Never
    /// called under `self.state` (finalize takes it). Pending or an error leaves the record:
    /// the caller keeps waiting and asks again later.
    fn resolve_blockers(&self, client: &dyn RangeClient, blockers: &[(ClusterTxnId, RangeId)]) {
        for &(txn, home) in blockers {
            let _ = resolve_prepared(client, home, txn, &|outcome| {
                self.finalize_txn(txn, outcome)
            });
        }
    }

    /// R5-82: finalizes `txn` on every range that took a share of its prepared record at a
    /// split of this one, before this range's own share goes: the coordinator Forgets the
    /// home record after this range acks, and a child asking a forgotten home would presume
    /// abort (Track D's B2). Without a client nothing is forwarded: `Unavailable`.
    /// The client is first taught each child's descriptor (review B1): a routed client reaches
    /// a range by id only through its cache, and nothing else puts a fresh child there.
    fn finalize_children(
        &self,
        txn: ClusterTxnId,
        outcome: &Outcome,
        children: &[RangeId],
    ) -> Result<(), RangeError> {
        if children.is_empty() {
            return Ok(());
        }
        let client = self.client().ok_or_else(|| {
            RangeError::Unavailable("no range client to finalize split children".into())
        })?;
        for &child in children {
            match child_route(self.stored.as_ref(), self.descriptor.id, child) {
                Ok(Some(descriptor)) => client.learn(&descriptor),
                // No record of it here: the client may still know a route.
                Ok(None) => {}
                Err(e) => return Err(RangeError::Corrupt(format!("{e:?}"))),
            }
            client.finalize(child, txn, outcome.clone())?;
        }
        Ok(())
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    fn check_open(&self) -> Result<(), RangeError> {
        if self.is_closed() || self.kv.is_fenced() {
            return Err(RangeError::NotLeader { hint: None });
        }
        Ok(())
    }

    /// A request at another epoch than the engine's: `Unavailable` while the engine is about
    /// to be reopened at that epoch, else `StaleEpoch` naming the range holding `key`
    /// (R5-81, split.rs `epoch_refusal`).
    fn check_epoch(&self, epoch: Epoch, key: Option<&[u8]>) -> Result<(), RangeError> {
        if epoch == self.descriptor.epoch {
            return Ok(());
        }
        Err(
            epoch_refusal(self.stored.as_ref(), &self.descriptor, epoch, key)
                .unwrap_or_else(|e| RangeError::Corrupt(format!("{e:?}"))),
        )
    }

    /// R5-80: once the stored descriptor's epoch moved (a split applied under this engine), a
    /// read or lock gets StaleEpoch with the descriptor of the range now holding `key`.
    fn check_stored(&self, key: Option<&[u8]>) -> Result<(), RangeError> {
        match stale_descriptor(self.stored.as_ref(), &self.descriptor, key) {
            Ok(None) => Ok(()),
            Ok(Some(descriptor)) => Err(RangeError::StaleEpoch { descriptor }),
            Err(e) => Err(RangeError::Corrupt(format!("{e:?}"))),
        }
    }

    /// Design.md Changes 10:40Z (c): a transaction whose grants came from another lock table
    /// lost them; None (no lock taken here) is not checked.
    fn check_generation(&self, lock_generation: Option<u64>) -> Result<(), RangeError> {
        match lock_generation {
            Some(g) if g != self.generation => Err(RangeError::Aborted(format!(
                "locks lost: granted under lock generation {g}, the range is at {}",
                self.generation
            ))),
            _ => Ok(()),
        }
    }

    fn state(&self) -> MutexGuard<'_, EngineState> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// R5-36: every prepared record of the range takes its locks back before anything is
    /// served; readers at T >= p wait for its outcome.
    fn reinstall_prepared(&self, store: &RangeStore) -> Result<(), RangeError> {
        let (lo, hi) = local_interval(self.descriptor.id);
        let mut records = Vec::new();
        for item in store
            .kv()
            .range(
                Keyspace::Meta,
                std::ops::Bound::Included(lo.as_slice()),
                std::ops::Bound::Excluded(hi.as_slice()),
                Direction::Forward,
            )
            .map_err(|e| RangeError::Corrupt(e.to_string()))?
        {
            let (key, value) = item.map_err(|e| RangeError::Corrupt(e.to_string()))?;
            if let Some((_, RangeLocalKey::Prepared(txn))) = parse_range_local_key(&key) {
                records.push((ClusterTxnId(txn), prepared::Prepared::decode(&value)?));
            }
        }
        let now = Instant::now();
        let mut state = self.state();
        for (txn, record) in records {
            for (key, mode) in &record.locks {
                self.locks
                    .acquire(txn_id(txn), key.clone(), *mode, Some(now), &|| false)
                    .map_err(|e| {
                        RangeError::Corrupt(format!("prepared locks of {txn:?} conflict: {e:?}"))
                    })?;
            }
            state.txns.insert(
                txn,
                TxnEntry {
                    expires: now,
                    prepared: true,
                    busy: false,
                },
            );
            state.prepared.insert(txn, record);
        }
        Ok(())
    }
}

/// The role-bootstrap invariant validate_meta skips in range mode (DD-0068 entry 1322): the
/// system range's `features` bit 512 is set exactly when its catalog has a live kind-7 entry.
fn check_role_bootstrap(kv: &Kv) -> Result<(), RangeError> {
    let features = crate::storage::engine::kv::KvRead::get(kv, Keyspace::Meta, b"features")
        .map_err(|e| RangeError::Corrupt(e.to_string()))?
        .and_then(|v| <[u8; 8]>::try_from(v.as_slice()).ok())
        .map_or(0, u64::from_be_bytes);
    let live = crate::storage::engine::meta::live_role_bootstrap_entries(kv)
        .map_err(|e| RangeError::Corrupt(e.to_string()))?;
    if (features & 512 != 0) != (live == 1) || live > 1 {
        return Err(RangeError::Corrupt(
            "role bootstrap feature/catalog mismatch".into(),
        ));
    }
    Ok(())
}

/// Expires leases until the engine closes or is dropped.
fn spawn_sweeper(engine: &Arc<RangeEngine>) {
    let weak: Weak<RangeEngine> = Arc::downgrade(engine);
    let period = engine.config.sweep;
    let _ = thread::Builder::new()
        .name(format!("range-{}-leases", engine.descriptor.id.0))
        .spawn(move || {
            loop {
                thread::sleep(period);
                let Some(engine) = weak.upgrade() else { return };
                if engine.is_closed() {
                    return;
                }
                engine.expire_leases();
            }
        });
}

impl RangeService for RangeEngine {
    fn descriptor(&self) -> RangeDescriptor {
        self.descriptor.clone()
    }
    fn read(&self, req: ReadRequest) -> Result<ReadPage, RangeError> {
        self.read_page(req)
    }
    fn lock(&self, req: LockRequest) -> Result<LockGrant, RangeError> {
        self.lock_target(req)
    }
    fn blockers(&self, txn: ClusterTxnId) -> Result<Vec<ClusterTxnId>, RangeError> {
        self.check_open()?;
        Ok(self
            .locks
            .blockers(txn_id(txn))
            .into_iter()
            .map(|t| ClusterTxnId(t.0))
            .collect())
    }
    fn cancel_wait(&self, txn: ClusterTxnId) -> Result<(), RangeError> {
        self.check_open()?;
        self.locks.cancel_wait(txn_id(txn));
        Ok(())
    }
    fn release(&self, txn: ClusterTxnId) -> Result<(), RangeError> {
        self.release_unprepared(txn)
    }
    fn heartbeat(&self, txn: ClusterTxnId) -> Result<(), RangeError> {
        self.renew(txn)
    }
    fn commit(
        &self,
        epoch: Epoch,
        txn: ClusterTxnId,
        writes: Vec<RangeWrite>,
        marker_xid: Option<u64>,
        lock_generation: Option<u64>,
    ) -> Result<Timestamp, RangeError> {
        self.commit_writes(epoch, txn, writes, marker_xid, lock_generation)
    }
    fn prepare(
        &self,
        epoch: Epoch,
        txn: ClusterTxnId,
        home: RangeId,
        writes: Vec<RangeWrite>,
        lock_generation: Option<u64>,
    ) -> Result<Timestamp, RangeError> {
        self.prepare_writes(epoch, txn, home, writes, lock_generation)
    }
    fn finalize(&self, txn: ClusterTxnId, outcome: Outcome) -> Result<(), RangeError> {
        self.finalize_txn(txn, outcome)
    }
    fn home(&self, epoch: Epoch, op: HomeOp) -> Result<HomeReply, RangeError> {
        self.check_open()?;
        self.check_epoch(epoch, None)?;
        self.home
            .get()
            .ok_or_else(|| RangeError::Unavailable("home records not open".into()))?
            .apply(op)
    }
}

/// R5-52: one home-record recovery pass on every open engine each `tick`, until the sender of
/// `stop` (the `RangeEngines`) is dropped.
/// Holds only weak references; errors wait for the next tick.
fn spawn_recovery(
    open: Weak<OpenEngines>,
    client: Weak<dyn RangeClient>,
    stop: mpsc::Receiver<()>,
    config: RecoveryConfig,
    tick: Duration,
) {
    let _ = thread::Builder::new()
        .name("range-recovery".into())
        .spawn(move || {
            while let Err(mpsc::RecvTimeoutError::Timeout) = stop.recv_timeout(tick) {
                let (Some(open), Some(client)) = (open.upgrade(), client.upgrade()) else {
                    return;
                };
                let engines: Vec<Arc<RangeEngine>> = open
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .values()
                    .cloned()
                    .collect();
                drop(open);
                for engine in engines {
                    if let (Ok(()), Some(home)) = (engine.check_open(), engine.home.get()) {
                        let _ = home.recover_once(&*client, config, Instant::now());
                    }
                }
            }
        });
}

type OpenEngines = Mutex<HashMap<RangeId, Arc<RangeEngine>>>;

/// The node's engines, opened by each range group's role manager (R5-16).
pub(crate) struct RangeEngines {
    store: Arc<RangeStore>,
    oracle: Arc<dyn TsOracle>,
    config: EngineConfig,
    open: Arc<OpenEngines>,
    /// Closed engines whose last writes were still in flight; a new engine of the range waits
    /// for them before it opens.
    draining: Mutex<HashMap<RangeId, Arc<RangeEngine>>>,
    /// The system range's oracle and leases while this node leads it (R5-31, R5-38).
    system: Mutex<Option<Arc<SystemRange>>>,
    client: OnceLock<Weak<dyn RangeClient>>,
    /// Dropped with `self`, which stops the recovery worker.
    recovery_stop: OnceLock<mpsc::Sender<()>>,
    reclaim_stop: OnceLock<mpsc::Sender<()>>,
    pub(crate) reclaim_errors: Arc<AtomicU64>,
}

impl RangeEngines {
    pub(crate) fn new(
        store: Arc<RangeStore>,
        oracle: Arc<dyn TsOracle>,
        config: EngineConfig,
    ) -> Self {
        Self {
            store,
            oracle,
            config,
            open: Arc::new(Mutex::new(HashMap::new())),
            draining: Mutex::new(HashMap::new()),
            system: Mutex::new(None),
            client: OnceLock::new(),
            recovery_stop: OnceLock::new(),
            reclaim_stop: OnceLock::new(),
            reclaim_errors: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Gives the node's engines a client for other ranges: engines opened from now on resolve
    /// prepared records through their homes (R5-53), and one worker thread runs home-record
    /// recovery (R5-52) on every open engine each `recovery_tick`, and a separate worker reclaims
    /// every `reclaim_every`, until `self` is dropped.
    /// Weak, because the client normally owns the engines. Set once; later calls are ignored.
    pub(crate) fn set_client(&self, client: Weak<dyn RangeClient>) {
        if self.client.set(client.clone()).is_err() {
            return;
        }
        let (stop, stopped) = mpsc::channel();
        let _ = self.recovery_stop.set(stop);
        #[cfg(test)]
        reclaim::hooks::register(&client, &self.open, &self.store, &self.reclaim_errors);
        spawn_recovery(
            Arc::downgrade(&self.open),
            client.clone(),
            stopped,
            self.config.recovery,
            self.config.recovery_tick,
        );
        let (stop, stopped) = mpsc::channel();
        let _ = self.reclaim_stop.set(stop);
        reclaim::spawn(
            Arc::downgrade(&self.open),
            client,
            stopped,
            self.config.reclaim_every,
            self.reclaim_errors.clone(),
        );
    }

    /// The system range's services, while this node leads the system range.
    pub(crate) fn system(&self) -> Option<Arc<SystemRange>> {
        self.system
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }
}

impl RangeEngineOpener for RangeEngines {
    fn open(
        &self,
        descriptor: RangeDescriptor,
        sink: Arc<dyn BatchSink>,
        generation: u64,
    ) -> Result<Arc<dyn RangeService>, RangeError> {
        let id = descriptor.id;
        self.close(id);
        let old = self
            .draining
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&id);
        if let Some(old) = old
            && !old.close_and_drain(self.config.read_wait)
        {
            self.draining
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .insert(id, old);
            return Err(RangeError::Unavailable(
                "the previous engine's writes are still in flight".into(),
            ));
        }
        if id == SYSTEM_RANGE {
            // The oracle and leases open first: the engine's own open takes a timestamp.
            let system = Arc::new(SystemRange::open(
                &self.store,
                descriptor.clone(),
                sink.clone(),
            )?);
            *self.system.lock().unwrap_or_else(|p| p.into_inner()) = Some(system);
        }
        let opened = match RangeEngine::open(
            &self.store,
            descriptor,
            sink,
            self.oracle.clone(),
            self.config.clone(),
            generation,
        ) {
            Ok(engine) => engine,
            Err(error) => {
                self.close(id);
                return Err(error);
            }
        };
        let engine = opened;
        if id == SYSTEM_RANGE
            && let Some(system) = self.system()
        {
            system.attach_sequencer(engine.seq.clone());
        }
        if let Some(client) = self.client.get() {
            engine.set_client(client.clone());
        }
        self.open
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(id, engine.clone());
        Ok(engine)
    }

    fn close(&self, range: RangeId) {
        if range == SYSTEM_RANGE
            && let Some(system) = self.system.lock().unwrap_or_else(|p| p.into_inner()).take()
        {
            system.close();
        }
        if let Some(engine) = self
            .open
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&range)
            && !engine.close_and_drain(Duration::ZERO)
        {
            self.draining
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .insert(range, engine);
        }
    }
}
