//! Gateway transaction services (GD-0075 R5-42): oracle statement timestamps, range locks,
//! the per-table writer sentinel (design.md 3.1), buffered single-range commit, release and
//! lock-lease heartbeats. Multi-range commit is R5-51 (Track D).
use super::coordinator::{
    CommitError, CommitPlan, Participant, commit_cross_range,
    deadlock::{DeadlockConfig, DeadlockDetector, DetectorHandle, LocalLocks},
};
use super::read::{RoutedKvRead, range_space};
use crate::range_api::{
    ClusterTxnId, GatewayPeer, GatewayService, LeaseKind, LockRequest, LockTarget, RangeClient,
    RangeDescriptor, RangeError, RangeId, RangeLockMode, RangeSpace, RangeWrite, SequenceOp,
    Timestamp, WaitSite, range_key,
};
use crate::storage::engine::{
    commit::RawOp,
    hlc::Ts,
    kv::{Keyspace, KvError, KvRead},
    locks::{LockConfig, LockManager, LockMode, TxnId},
    txn::{
        ClusterCommit, ClusterServices, Isolation, LatestVersion, SerializationReason, TxnError,
    },
};
use std::{
    collections::{BTreeSet, HashMap},
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

#[cfg(test)]
mod tests;

/// A lock grant's newest committed version (`LockGrant::latest`).
type RangeLatest = (Timestamp, Option<Vec<u8>>);

/// Lease id namespace of gateway transaction-id blocks: `lease(Xid, TXN_ID_SPACE | node, n)`
/// keeps a per-node high-water `Seq/[4] || id` in the system range, apart from the xid
/// allocator (id 0). design.md Changes 2026-11-16 Track C.
const TXN_ID_SPACE: u64 = 1 << 32;
/// Transaction ids leased at a time.
const TXN_ID_BLOCK: u64 = 1 << 20;

/// Gateway settings. Defaults follow design.md 3.2 (`lock_wait_backstop` 5 min).
#[derive(Clone, Debug)]
pub(crate) struct ClusterConfig {
    /// This gateway's node id (< 2^16): the high bits of every ClusterTxnId it issues.
    pub(crate) node: u16,
    pub(crate) write_set_bytes: usize,
    /// Byte budget of one range read page.
    pub(crate) page_bytes: usize,
    /// Lock-lease heartbeat period for transactions holding range locks.
    pub(crate) heartbeat: Duration,
    /// A blocking range lock is requested in slices this long so cancellation is seen.
    pub(crate) lock_slice: Duration,
    /// A lock wait with no deadline ends with 55P03 after this long.
    pub(crate) lock_backstop: Duration,
    /// Attempts for NotLeader / Unavailable / StaleEpoch before giving up.
    pub(crate) retries: u32,
}
impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            node: 1,
            write_set_bytes: 1024 * 1024 * 1024,
            page_bytes: 1024 * 1024,
            heartbeat: Duration::from_secs(1),
            lock_slice: Duration::from_millis(500),
            lock_backstop: Duration::from_secs(300),
            retries: 8,
        }
    }
}

#[derive(Default)]
struct TxnState {
    priority: Option<Timestamp>,
    /// Ranges in which the transaction may hold locks (released at the end).
    ranges: BTreeSet<RangeId>,
    /// Lock generation of the grants held in each range (design.md Changes 10:40Z): passed to
    /// commit and prepare, which refuse it once the range's lock table changed.
    generations: HashMap<RangeId, u64>,
    /// Tables with a Shared writer sentinel: taken lazily in each range written.
    shared_sentinels: BTreeSet<u64>,
    /// (table, range) pairs where a sentinel is held (either mode).
    sentinel_ranges: BTreeSet<(u64, RangeId)>,
    /// Tables whose Exclusive sentinel covers every range of the row span.
    exclusive_tables: BTreeSet<u64>,
    /// Set when a heartbeat learns the locks were lost (leader change, lease expiry).
    aborted: bool,
    /// Where the transaction is waiting now (for R5-56's deadlock detector).
    waiting: Option<WaitSite>,
}

/// One per gateway database. Transactions are keyed by TxnId (= ClusterTxnId.0).
pub(crate) struct ClusterTxnServices {
    client: Arc<dyn RangeClient>,
    gateway: Arc<LockManager>,
    config: ClusterConfig,
    /// Unused transaction ids of the current leased block (`next_txn_id`).
    txn_ids: Mutex<std::ops::Range<u64>>,
    txns: Mutex<HashMap<TxnId, TxnState>>,
    stop: Arc<AtomicBool>,
    /// Cross-range and gateway deadlock detection (R5-56), when the gateway has peers.
    detector: Option<Arc<DeadlockDetector>>,
    _detector_thread: Option<DetectorHandle>,
}

/// The gateway's own lock manager as the detector sees it (`WaitSite::Gateway`).
struct GatewayLocks(Arc<LockManager>);
impl LocalLocks for GatewayLocks {
    fn blockers(&self, txn: ClusterTxnId) -> Vec<ClusterTxnId> {
        self.0
            .blockers(TxnId(txn.0))
            .into_iter()
            .map(|t| ClusterTxnId(t.0))
            .collect()
    }
    fn cancel_wait(&self, txn: ClusterTxnId) {
        self.0.cancel_wait(TxnId(txn.0));
    }
}

fn now_ms(deadline: Instant) -> u64 {
    deadline
        .saturating_duration_since(Instant::now())
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

/// RangeError -> TxnError for locks and commits. Lost locks are a retryable 40001.
pub(crate) fn txn_error(error: RangeError) -> TxnError {
    match error {
        RangeError::Deadlock => TxnError::Deadlock,
        RangeError::LockTimeout => TxnError::LockTimeout,
        RangeError::Cancelled => TxnError::Cancelled,
        RangeError::Aborted(_) | RangeError::StaleEpoch { .. } => {
            TxnError::Serialization(SerializationReason::WriteConflict)
        }
        RangeError::Conflict(_) => TxnError::Serialization(SerializationReason::WriteConflict),
        RangeError::Unknown(_) => TxnError::CommitAmbiguous,
        RangeError::Corrupt(message) => TxnError::Kv(KvError::Corrupt(message)),
        RangeError::NotLeader { .. } | RangeError::Unavailable(_) => {
            TxnError::Kv(KvError::Io(error.to_string()))
        }
    }
}

impl ClusterTxnServices {
    /// A gateway without peers: no cross-range deadlock detection (each range still breaks
    /// its own cycles, and every wait ends at `lock_backstop`).
    pub(crate) fn new(client: Arc<dyn RangeClient>, config: ClusterConfig) -> Arc<Self> {
        Self::build(client, config, None)
    }
    /// A gateway that detects deadlocks across ranges and gateways with `peer` (design.md 3.2).
    pub(crate) fn with_peer(
        client: Arc<dyn RangeClient>,
        config: ClusterConfig,
        peer: Arc<dyn GatewayPeer>,
        deadlock: DeadlockConfig,
    ) -> Arc<Self> {
        Self::build(client, config, Some((peer, deadlock)))
    }
    fn build(
        client: Arc<dyn RangeClient>,
        config: ClusterConfig,
        peer: Option<(Arc<dyn GatewayPeer>, DeadlockConfig)>,
    ) -> Arc<Self> {
        let gateway = Arc::new(LockManager::new(LockConfig::default()));
        let detector = peer.map(|(peer, deadlock)| {
            DeadlockDetector::new(
                config.node,
                deadlock,
                client.clone(),
                peer,
                Arc::new(GatewayLocks(gateway.clone())),
            )
        });
        let thread = detector.as_ref().map(DeadlockDetector::spawn);
        let services = Arc::new(Self {
            client,
            gateway,
            config,
            txn_ids: Mutex::new(0..0),
            txns: Mutex::new(HashMap::new()),
            stop: Arc::new(AtomicBool::new(false)),
            detector,
            _detector_thread: thread,
        });
        let weak = Arc::downgrade(&services);
        let stop = services.stop.clone();
        let period = services.config.heartbeat;
        thread::Builder::new()
            .name("gateway-heartbeat".into())
            .spawn(move || heartbeat_loop(weak, stop, period))
            .expect("spawn gateway heartbeat thread");
        services
    }
    /// This gateway's deadlock service, for the peer dispatcher (Track B) to deliver probes.
    #[cfg_attr(
        not(test),
        allow(dead_code, reason = "Phase 7 wires the peer dispatcher")
    )]
    pub(crate) fn gateway_service(&self) -> Option<Arc<dyn GatewayService>> {
        self.detector
            .clone()
            .map(|detector| detector as Arc<dyn GatewayService>)
    }
    /// A new transaction id: this node's id in the high 16 bits.
    /// A new transaction id: this node's id in the high 16 bits and a counter that never
    /// repeats on this node, even across gateway restarts: counters come in blocks leased
    /// from the system range (its persisted per-node high-water), so a restarted gateway
    /// never reuses the id of a transaction whose home or prepared records still exist.
    /// Waits for the system range when a block runs out (once per `TXN_ID_BLOCK` ids).
    pub(crate) fn next_txn_id(&self) -> TxnId {
        let mut ids = self.txn_ids.lock().unwrap_or_else(|p| p.into_inner());
        if ids.is_empty() {
            *ids = self.lease_txn_ids();
        }
        let counter = ids.start;
        ids.start += 1;
        TxnId(ClusterTxnId::new(self.config.node, counter).0)
    }
    fn lease_txn_ids(&self) -> std::ops::Range<u64> {
        let id = TXN_ID_SPACE | u64::from(self.config.node);
        let mut delay = Duration::from_millis(1);
        loop {
            match self.client.lease(LeaseKind::Xid, id, TXN_ID_BLOCK) {
                Ok(block) if block.end <= 1 << 48 && !block.is_empty() => return block,
                Ok(block) => panic!("gateway transaction ids exhausted: {block:?}"),
                Err(RangeError::Corrupt(message)) => {
                    panic!("gateway transaction id allocator is corrupt: {message}")
                }
                Err(_) => {
                    thread::sleep(delay);
                    delay = (delay * 2).min(Duration::from_millis(100));
                }
            }
        }
    }
    /// The site a transaction is blocked at now, if any (deadlock detection, R5-56).
    #[allow(dead_code)]
    pub(crate) fn wait_site(&self, txn: TxnId) -> Option<WaitSite> {
        self.state(txn, |state| state.waiting)
    }
    fn state<T>(&self, txn: TxnId, f: impl FnOnce(&mut TxnState) -> T) -> T {
        let mut txns = self.txns.lock().unwrap_or_else(|p| p.into_inner());
        f(txns.entry(txn).or_default())
    }
    fn check_live(&self, txn: TxnId) -> Result<(), TxnError> {
        if self.state(txn, |state| state.aborted) {
            return Err(TxnError::Serialization(SerializationReason::WriteConflict));
        }
        Ok(())
    }
    fn priority(&self, txn: TxnId) -> Result<Timestamp, TxnError> {
        if let Some(priority) = self.state(txn, |state| state.priority) {
            return Ok(priority);
        }
        let ts = self.oracle()?;
        Ok(self.state(txn, |state| *state.priority.get_or_insert(ts)))
    }
    fn oracle(&self) -> Result<Timestamp, TxnError> {
        self.retry(|| self.client.oracle().next(1))
    }
    /// Retries NotLeader and Unavailable with a short doubling backoff.
    fn retry<T>(&self, mut call: impl FnMut() -> Result<T, RangeError>) -> Result<T, TxnError> {
        let mut delay = Duration::from_millis(1);
        let mut attempt = 0;
        loop {
            match call() {
                Err(RangeError::NotLeader { .. } | RangeError::Unavailable(_))
                    if attempt + 1 < self.config.retries =>
                {
                    attempt += 1;
                    thread::sleep(delay);
                    delay = (delay * 2).min(Duration::from_millis(50));
                }
                result => return result.map_err(txn_error),
            }
        }
    }
    fn descriptor(&self, range_key: &[u8]) -> Result<RangeDescriptor, TxnError> {
        self.retry(|| self.client.descriptor_for(range_key))
    }

    /// One lock request in the range containing `rk`, in slices so `cancel` is observed.
    /// Re-resolves the range on StaleEpoch; records the range before asking.
    fn lock_in_range(
        &self,
        txn: TxnId,
        rk: &[u8],
        target: LockTarget,
        mode: RangeLockMode,
        deadline: Option<Instant>,
        cancel: &dyn Fn() -> bool,
    ) -> Result<(RangeDescriptor, Option<RangeLatest>), TxnError> {
        let priority = self.priority(txn)?;
        let deadline = deadline.unwrap_or_else(|| Instant::now() + self.config.lock_backstop);
        let mut stale = 0;
        let mut unavailable = 0;
        let mut delay = Duration::from_millis(1);
        // One registered wait spans every slice, so a wait older than deadlock_timeout is
        // probed even though each range request lasts only one slice.
        let mut wait: Option<(RangeId, super::coordinator::deadlock::WaitGuard)> = None;
        loop {
            self.check_live(txn)?;
            if cancel() {
                return Err(TxnError::Cancelled);
            }
            if wait.as_ref().is_some_and(|(_, guard)| guard.was_victim()) {
                return Err(TxnError::Deadlock);
            }
            let descriptor = self.descriptor(rk)?;
            if let Some(detector) = &self.detector
                && wait
                    .as_ref()
                    .is_none_or(|(range, _)| *range != descriptor.id)
            {
                wait = Some((
                    descriptor.id,
                    detector.begin_wait(
                        ClusterTxnId(txn.0),
                        priority,
                        vec![WaitSite::Range(descriptor.id)],
                    ),
                ));
            }
            self.state(txn, |state| {
                state.ranges.insert(descriptor.id);
                state.waiting = Some(WaitSite::Range(descriptor.id));
            });
            let slice = now_ms(deadline).min(self.config.lock_slice.as_millis() as u64);
            let result = self.client.lock(LockRequest {
                range: descriptor.id,
                epoch: descriptor.epoch,
                txn: ClusterTxnId(txn.0),
                priority,
                target: target.clone(),
                mode,
                timeout_ms: Some(slice),
            });
            self.state(txn, |state| state.waiting = None);
            match result {
                Ok(grant) => {
                    // A second generation from one range means earlier locks there were lost:
                    // the transaction aborts without trying to commit (design.md 10:40Z (b)).
                    let lost = self.state(txn, |state| {
                        let held = *state
                            .generations
                            .entry(descriptor.id)
                            .or_insert(grant.generation);
                        if held != grant.generation {
                            state.aborted = true;
                        }
                        held != grant.generation
                    });
                    if lost {
                        return Err(TxnError::Serialization(SerializationReason::WriteConflict));
                    }
                    return Ok((descriptor, grant.latest));
                }
                Err(RangeError::Deadlock) => return Err(TxnError::Deadlock),
                Err(RangeError::LockTimeout) => {
                    if Instant::now() >= deadline {
                        return Err(TxnError::LockTimeout);
                    }
                }
                Err(RangeError::StaleEpoch { .. }) if stale < self.config.retries => {
                    stale += 1;
                    self.client.invalidate(&descriptor);
                }
                Err(RangeError::NotLeader { .. } | RangeError::Unavailable(_))
                    if unavailable + 1 < self.config.retries =>
                {
                    unavailable += 1;
                    self.client.invalidate(&descriptor);
                    thread::sleep(delay);
                    delay = (delay * 2).min(Duration::from_millis(50));
                }
                Err(error) => return Err(txn_error(error)),
            }
        }
    }

    /// Takes the Shared writer sentinel of `table` in the range holding `rk`, once.
    fn shared_sentinel_for(
        &self,
        txn: TxnId,
        table: u64,
        rk: &[u8],
        deadline: Option<Instant>,
        cancel: &dyn Fn() -> bool,
    ) -> Result<(), TxnError> {
        let wanted = self.state(txn, |state| {
            state.shared_sentinels.contains(&table) && !state.exclusive_tables.contains(&table)
        });
        if !wanted {
            return Ok(());
        }
        let range = self.descriptor(rk)?.id;
        if self.state(txn, |state| state.sentinel_ranges.contains(&(table, range))) {
            return Ok(());
        }
        let (descriptor, _) = self.lock_in_range(
            txn,
            rk,
            LockTarget::TableSentinel { table },
            RangeLockMode::Shared,
            deadline,
            cancel,
        )?;
        self.state(txn, |state| {
            state.sentinel_ranges.insert((table, descriptor.id));
        });
        Ok(())
    }

    /// Exclusive sentinel on every range intersecting the table's row span, in order.
    fn exclusive_sentinel(
        &self,
        txn: TxnId,
        table: u64,
        cancel: &dyn Fn() -> bool,
    ) -> Result<(), TxnError> {
        let mut at = range_key(RangeSpace::Rows, &table.to_be_bytes());
        let end = match table.checked_add(1) {
            Some(next) => range_key(RangeSpace::Rows, &next.to_be_bytes()),
            None => vec![RangeSpace::Rows.tag() + 1],
        };
        loop {
            let (descriptor, _) = self.lock_in_range(
                txn,
                &at,
                LockTarget::TableSentinel { table },
                RangeLockMode::Exclusive,
                None,
                cancel,
            )?;
            self.state(txn, |state| {
                state.sentinel_ranges.insert((table, descriptor.id));
            });
            match descriptor.end {
                Some(next) if next < end => at = next,
                _ => break,
            }
        }
        self.state(txn, |state| {
            state.exclusive_tables.insert(table);
        });
        Ok(())
    }

    /// Locks held on ranges the commit does not write (FOR UPDATE / FOR SHARE rows, table
    /// sentinels) are confirmed before committing: commit and prepare check the lock
    /// generation only where they write, so a leader change on a lock-only range would
    /// otherwise go unseen. A range that no longer knows the transaction, or cannot be
    /// reached, aborts it (40001); its locks may be gone.
    fn check_lock_only(&self, txn: TxnId, written: &[Participant]) -> Result<(), TxnError> {
        let ranges: Vec<RangeId> = self.state(txn, |state| {
            state
                .generations
                .keys()
                .copied()
                .filter(|range| !written.iter().any(|p| p.range == *range))
                .collect()
        });
        for range in ranges {
            let alive = self.retry(|| self.client.heartbeat(range, ClusterTxnId(txn.0)));
            if alive.is_err() {
                self.state(txn, |state| state.aborted = true);
                return Err(TxnError::Serialization(SerializationReason::WriteConflict));
            }
        }
        Ok(())
    }

    fn heartbeat_all(&self) {
        // Only ranges that granted a lock: a request that failed before the leader saw it
        // left no lease there, and "holds no locks" from it must not abort the transaction.
        let work: Vec<(TxnId, Vec<RangeId>)> = {
            let txns = self.txns.lock().unwrap_or_else(|p| p.into_inner());
            txns.iter()
                .filter(|(_, state)| !state.generations.is_empty() && !state.aborted)
                .map(|(txn, state)| (*txn, state.generations.keys().copied().collect()))
                .collect()
        };
        for (txn, ranges) in work {
            for range in ranges {
                if let Err(RangeError::Aborted(_)) =
                    self.client.heartbeat(range, ClusterTxnId(txn.0))
                {
                    let mut txns = self.txns.lock().unwrap_or_else(|p| p.into_inner());
                    if let Some(state) = txns.get_mut(&txn) {
                        state.aborted = true;
                    }
                }
            }
        }
    }
}

impl Drop for ClusterTxnServices {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
    }
}

fn heartbeat_loop(services: Weak<ClusterTxnServices>, stop: Arc<AtomicBool>, period: Duration) {
    let tick = period
        .min(Duration::from_millis(50))
        .max(Duration::from_millis(1));
    let mut waited = Duration::ZERO;
    while !stop.load(Ordering::Acquire) {
        thread::sleep(tick);
        waited += tick;
        if waited < period {
            continue;
        }
        waited = Duration::ZERO;
        let Some(services) = services.upgrade() else {
            return;
        };
        services.heartbeat_all();
    }
}

impl ClusterServices for ClusterTxnServices {
    fn snapshot(&self, txn: TxnId) -> Result<Ts, TxnError> {
        let ts = self.oracle()?;
        self.state(txn, |state| {
            state.priority.get_or_insert(ts);
        });
        Ok(Ts(ts.0))
    }
    fn now(&self) -> Result<Ts, TxnError> {
        Ok(Ts(self.oracle()?.0))
    }
    fn reader(&self, ts: Ts) -> Arc<dyn KvRead> {
        Arc::new(RoutedKvRead::new(
            self.client.clone(),
            ts,
            self.config.page_bytes,
        ))
    }
    fn gateway_locks(&self) -> &LockManager {
        &self.gateway
    }
    fn lock_key(
        &self,
        txn: TxnId,
        ks: Keyspace,
        key: &[u8],
        deadline: Option<Instant>,
        cancel: &dyn Fn() -> bool,
    ) -> Result<Option<LatestVersion>, TxnError> {
        let space = match ks {
            Keyspace::Rows => RangeSpace::Rows,
            Keyspace::Index => RangeSpace::Index,
            _ => return Err(TxnError::Misuse),
        };
        if key.len() < 8 {
            return Err(TxnError::Misuse);
        }
        let rk = range_key(space, key);
        if ks == Keyspace::Rows {
            let table = u64::from_be_bytes(key[..8].try_into().unwrap());
            self.shared_sentinel_for(txn, table, &rk, deadline, cancel)?;
        }
        let (_, latest) = self.lock_in_range(
            txn,
            &rk,
            LockTarget::Key {
                space,
                key: key.to_vec(),
            },
            RangeLockMode::Exclusive,
            deadline,
            cancel,
        )?;
        Ok(latest.map(|(ts, value)| (Ts(ts.0), value)))
    }
    fn lock_sentinel(
        &self,
        txn: TxnId,
        table: u64,
        mode: LockMode,
        cancel: &dyn Fn() -> bool,
    ) -> Result<(), TxnError> {
        self.check_live(txn)?;
        match mode {
            // Taken lazily in every range the statement writes rows of `table`.
            LockMode::Shared => {
                self.state(txn, |state| {
                    state.shared_sentinels.insert(table);
                });
                Ok(())
            }
            LockMode::Exclusive => {
                if self.state(txn, |state| state.exclusive_tables.contains(&table)) {
                    return Ok(());
                }
                self.exclusive_sentinel(txn, table, cancel)
            }
            _ => Err(TxnError::Misuse),
        }
    }
    fn write_set_bytes(&self) -> usize {
        self.config.write_set_bytes
    }
    fn commit(&self, txn: TxnId, commit: ClusterCommit) -> Result<Ts, TxnError> {
        self.check_live(txn)?;
        if commit.isolation == Isolation::Serializable {
            // SERIALIZABLE in a cluster is Phase 8 (shared and gap locks).
            return Err(TxnError::Unsupported);
        }
        let mut marker_xid = None;
        let mut dropped_counters = Vec::new();
        for op in commit.raw {
            match op {
                RawOp::XactCommitted { xid } => marker_xid = Some(xid),
                // Retired with the definition; deleted on the system range after commit.
                RawOp::DeleteCounter { key } => dropped_counters.push(key),
                // Counters are written only by the system range leader (SequenceOp).
                RawOp::PutCounter { .. } => return Err(TxnError::Unsupported),
            }
        }
        // Feature bits are range-local (deviation E9); range leaders own them.
        let _ = commit.features_or;
        let mut groups: Vec<Participant> = Vec::new();
        let mut current: Option<RangeDescriptor> = None;
        for ((ks, key), value) in commit.writes {
            let space = range_space(ks).ok_or(TxnError::Misuse)?;
            let rk = range_key(space, &key);
            if !current.as_ref().is_some_and(|d| d.contains(&rk)) {
                current = Some(self.descriptor(&rk)?);
            }
            let descriptor = current.as_ref().expect("resolved above");
            let write = RangeWrite { space, key, value };
            match groups.iter_mut().find(|p| p.range == descriptor.id) {
                Some(group) => group.writes.push(write),
                None => groups.push(Participant {
                    range: descriptor.id,
                    epoch: descriptor.epoch,
                    writes: vec![write],
                    lock_generation: self
                        .state(txn, |state| state.generations.get(&descriptor.id).copied()),
                }),
            }
        }
        self.check_lock_only(txn, &groups)?;
        let ts = match groups.len() {
            // Nothing to write (an xid with no writes, or only retired counters): the
            // transaction ends at a fresh timestamp. An xid's status is R5-55.
            0 => self.oracle()?,
            1 => {
                let group = groups.pop().expect("one group");
                self.retry(|| {
                    self.client.commit(
                        group.range,
                        group.epoch,
                        ClusterTxnId(txn.0),
                        group.writes.clone(),
                        marker_xid,
                        group.lock_generation,
                    )
                })?
            }
            _ => {
                // Several ranges: Track D's coordinator, with the home in the first range
                // written in key order. The commit marker of an xid is R5-55.
                let lock_only: Vec<RangeId> = self.state(txn, |state| {
                    state
                        .ranges
                        .iter()
                        .copied()
                        .filter(|range| !groups.iter().any(|p| p.range == *range))
                        .collect()
                });
                let plan = CommitPlan {
                    txn: ClusterTxnId(txn.0),
                    participants: groups,
                    lock_only,
                };
                commit_cross_range(self.client.as_ref(), plan).map_err(|e| match e {
                    CommitError::Aborted(cause) => match txn_error(cause) {
                        // A definite abort is never reported as an unknown outcome.
                        TxnError::CommitAmbiguous => {
                            TxnError::Serialization(SerializationReason::WriteConflict)
                        }
                        other => other,
                    },
                    CommitError::Unknown(_) => TxnError::CommitAmbiguous,
                })?
            }
        };
        for key in dropped_counters {
            // Best effort: a counter left behind belongs to a retired incarnation and is
            // never read again (counter keys carry the incarnation).
            if key.len() == 25 {
                let id = u64::from_be_bytes(key[1..9].try_into().expect("checked length"));
                let incarnation =
                    u128::from_be_bytes(key[9..25].try_into().expect("checked length"));
                let _ = self.client.sequence(id, incarnation, SequenceOp::Forget);
            }
        }
        Ok(Ts(ts.0))
    }
    fn release_all(&self, txn: TxnId) {
        let state = self
            .txns
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&txn);
        if let Some(state) = state {
            for range in state.ranges {
                // Unreleased locks die with their lease; nothing to retry here.
                let _ = self.client.release(range, ClusterTxnId(txn.0));
            }
        }
        self.gateway.release_all(txn);
    }
    fn next_txn_id(&self) -> TxnId {
        ClusterTxnServices::next_txn_id(self)
    }
    fn client(&self) -> Arc<dyn RangeClient> {
        self.client.clone()
    }
}
