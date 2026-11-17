//! Gateway transaction services (GD-0075 R5-42): oracle statement timestamps, range locks,
//! the per-table writer sentinel (design.md 3.1), buffered single-range commit, release and
//! lock-lease heartbeats. Multi-range commit is R5-51 (Track D).
use super::coordinator::{
    CommitError, CommitPlan, Participant, commit_cross_range_gated,
    deadlock::{DeadlockConfig, DeadlockDetector, DetectorHandle, LocalLocks},
};
use super::read::{RoutedKvRead, past_end, range_space};
use super::xact::{Lookup, XactRecord, read_record, xact_key};
use crate::range_api::{
    ClusterTxnId, DeadlockProbe, Epoch, GatewayPeer, GatewayService, LeaseKind, LockBatchRequest,
    LockRequest, LockTarget, RangeClient, RangeDescriptor, RangeError, RangeId, RangeLockMode,
    RangeSpace, RangeWrite, SequenceOp, Timestamp, WaitSite, range_key,
};
use crate::storage::engine::{
    commit::RawOp,
    hlc::Ts,
    kv::{Keyspace, KvError, KvRead},
    locks::{LockConfig, LockManager, LockMode, TxnId},
    ssi_validate::{ValidationConfig, Validator},
    txn::{
        ClusterCommit, ClusterServices, Isolation, LatestVersion, ReadSet, SerializationReason,
        TxnError,
    },
    xact::XactStatus,
};
use std::{
    collections::{BTreeSet, HashMap, HashSet},
    ops::Bound,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

// Gateway membership and DDL across gateways (GD-0075 Phase 11), in cluster/ddl.rs.
#[path = "ddl.rs"]
pub(crate) mod ddl;
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
/// How long a range call keeps being retried on NotLeader/Unavailable: past liveness declaring
/// a leader's node dead (1 s) plus an election.
pub(super) const FAILOVER_WINDOW: Duration = Duration::from_secs(5);

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
    /// Every gateway of the cluster, this one included (Phase 11). Empty: single-gateway
    /// mode (no membership, no fencing).
    pub(crate) gateways: Vec<u16>,
    /// How long a granted liveness lock lets this gateway serve (`ddl` module); below the
    /// range lock lease.
    pub(crate) gateway_lease: Duration,
    /// Lease of transaction locks at the ranges.
    pub(crate) lock_lease: Duration,
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
            gateways: Vec::new(),
            gateway_lease: Duration::from_secs(6),
            lock_lease: Duration::from_secs(10),
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
    /// The epoch of the descriptor each range granted its first lock under (a SERIALIZABLE
    /// commit prepares lock-only ranges at it, R5-71).
    epochs: HashMap<RangeId, Epoch>,
    /// Tables with a Shared writer sentinel: taken lazily in each range written.
    shared_sentinels: BTreeSet<u64>,
    /// (table, range) pairs where a sentinel is held (either mode).
    sentinel_ranges: BTreeSet<(u64, RangeId)>,
    /// Tables whose Exclusive sentinel covers every range of the row span.
    exclusive_tables: BTreeSet<u64>,
    /// Set when a heartbeat learns the locks were lost (leader change, lease expiry).
    aborted: bool,
    /// Set when the gateway fenced while the transaction was live: it never runs again.
    fenced: bool,
    /// A DDL transaction across gateways (Phase 11): the members it closed or evicted.
    ddl: Option<ddl::DdlTxn>,
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
    /// The other gateways, asked whether an xid's owner is live (R5-55).
    peer: Option<Arc<dyn GatewayPeer>>,
    /// What the peer dispatcher serves for this gateway: deadlock calls and `txn_live`.
    endpoint: Arc<GatewayEndpoint>,
    /// Gateway membership (Phase 11): whether this gateway may serve.
    membership: ddl::Membership,
}

/// This gateway's `GatewayService`: deadlock calls go to the detector (none without peers),
/// `txn_live` answers from the transactions that registered an xid here and have not ended.
struct GatewayEndpoint {
    detector: Option<Arc<DeadlockDetector>>,
    xact_owners: Mutex<HashSet<TxnId>>,
    /// The gateway's services, for the DDL calls (Phase 11).
    services: std::sync::OnceLock<Weak<ClusterTxnServices>>,
}
impl GatewayEndpoint {
    fn owners(&self) -> std::sync::MutexGuard<'_, HashSet<TxnId>> {
        self.xact_owners.lock().unwrap_or_else(|p| p.into_inner())
    }
    fn services(&self) -> Option<Arc<ClusterTxnServices>> {
        self.services.get().and_then(Weak::upgrade)
    }
}
impl GatewayService for GatewayEndpoint {
    fn probe(&self, probe: DeadlockProbe) {
        if let Some(detector) = &self.detector {
            detector.probe(probe);
        }
    }
    fn abort(&self, victim: ClusterTxnId) {
        if let Some(detector) = &self.detector {
            detector.abort(victim);
        }
    }
    fn blockers(&self, txn: ClusterTxnId) -> Vec<ClusterTxnId> {
        self.detector
            .as_ref()
            .map_or_else(Vec::new, |detector| detector.blockers(txn))
    }
    fn cancel_wait(&self, txn: ClusterTxnId) {
        if let Some(detector) = &self.detector {
            detector.cancel_wait(txn);
        }
    }
    fn txn_live(&self, txn: ClusterTxnId) -> bool {
        self.owners().contains(&TxnId(txn.0))
    }
    fn ddl_close(
        &self,
        txn: ClusterTxnId,
        _priority: Timestamp,
        tables: &[u64],
    ) -> Result<bool, RangeError> {
        match self.services() {
            Some(services) => services.proxy_close(txn, tables),
            None => Err(RangeError::Unavailable("gateway closed".into())),
        }
    }
    fn ddl_open(&self, txn: ClusterTxnId) {
        if let Some(services) = self.services() {
            services.proxy_open(txn);
        }
    }
    fn ddl_live(&self, txn: ClusterTxnId) -> bool {
        self.services()
            .is_some_and(|services| services.ddl_live(txn))
    }
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
        let membership = !config.gateways.is_empty();
        let peer_handle = peer.as_ref().map(|(peer, _)| peer.clone());
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
        let endpoint = Arc::new(GatewayEndpoint {
            detector: detector.clone(),
            xact_owners: Mutex::new(HashSet::new()),
            services: std::sync::OnceLock::new(),
        });
        let services = Arc::new(Self {
            client,
            gateway,
            config,
            txn_ids: Mutex::new(0..0),
            txns: Mutex::new(HashMap::new()),
            stop: Arc::new(AtomicBool::new(false)),
            detector,
            _detector_thread: thread,
            peer: peer_handle,
            endpoint,
            membership: ddl::Membership::new(membership),
        });
        let weak = Arc::downgrade(&services);
        let _ = services.endpoint.services.set(weak.clone());
        let stop = services.stop.clone();
        let period = services.config.heartbeat;
        thread::Builder::new()
            .name("gateway-heartbeat".into())
            .spawn(move || heartbeat_loop(weak, stop, period))
            .expect("spawn gateway heartbeat thread");
        services
    }
    /// This gateway's service for the peer dispatcher (Track B): deadlock probes (when the
    /// gateway has peers) and `txn_live` (always, R5-55).
    pub(crate) fn gateway_service(&self) -> Arc<dyn GatewayService> {
        self.endpoint.clone()
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
    /// Retries NotLeader and Unavailable with a short doubling backoff: at least
    /// `config.retries` attempts, and for at least `FAILOVER_WINDOW`, so a statement outlasts a
    /// leader's node being declared dead and a new leader being elected (delta review D6).
    fn retry<T>(&self, mut call: impl FnMut() -> Result<T, RangeError>) -> Result<T, TxnError> {
        let mut delay = Duration::from_millis(1);
        let mut attempt = 0;
        let started = Instant::now();
        loop {
            match call() {
                Err(RangeError::NotLeader { .. } | RangeError::Unavailable(_))
                    if self.may_retry(attempt, started) =>
                {
                    attempt += 1;
                    thread::sleep(delay);
                    delay = (delay * 2).min(Duration::from_millis(50));
                }
                result => return result.map_err(txn_error),
            }
        }
    }
    /// Whether a call failing NotLeader / Unavailable after `failures` earlier such failures
    /// since `since` is tried again: under `config.retries` attempts, or inside
    /// `FAILOVER_WINDOW` (a new split child elects its first leader in 300-530 ms).
    fn may_retry(&self, failures: u32, since: Instant) -> bool {
        failures + 1 < self.config.retries || since.elapsed() < FAILOVER_WINDOW
    }
    fn descriptor(&self, range_key: &[u8]) -> Result<RangeDescriptor, TxnError> {
        self.retry(|| self.client.descriptor_for(range_key))
    }
    /// After a NotLeader / Unavailable backoff on a call addressed to `desc` (holding `key`),
    /// looks the range up again (GD-0075 P9-9, as read.rs `send`): a routed client picks
    /// replicas from its cached descriptor, and a new split child may simply have no leader
    /// yet. Returns a newer epoch holding the key (a route change: the range would refuse the
    /// call as StaleEpoch). An older or unknown record there (a split not yet published to the
    /// directory) is not one: `desc` is learned back so the by-id call still routes.
    fn reroute(&self, key: &[u8], desc: &RangeDescriptor) -> Option<RangeDescriptor> {
        self.client.invalidate(desc);
        match self.client.descriptor_for(key) {
            Ok(current) if current.epoch > desc.epoch => return Some(current),
            Ok(current) if current == *desc => {}
            _ => self.client.learn(desc),
        }
        None
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
        // The (id, epoch) last asked: each change (a split or move seen through the directory
        // or a StaleEpoch) restarts the NotLeader / Unavailable budget and window, at most
        // `retries` times (GD-0075 P9-9). The window opens at the first such failure.
        let mut route: Option<(RangeId, Epoch)> = None;
        let mut route_changes = 0;
        let mut unavailable_since: Option<Instant> = None;
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
            let current = (descriptor.id, descriptor.epoch);
            if route.is_some_and(|last| last != current) && route_changes < self.config.retries {
                route_changes += 1;
                unavailable = 0;
                unavailable_since = None;
                delay = Duration::from_millis(1);
            }
            route = Some(current);
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
            // A gap lock asks each range for the part of its interval that range holds.
            let target = match &target {
                LockTarget::Interval { space, start, end } => {
                    let (start, end) =
                        clip_interval(&descriptor, *space, start, end).ok_or(TxnError::Misuse)?;
                    LockTarget::Interval {
                        space: *space,
                        start,
                        end,
                    }
                }
                other => other.clone(),
            };
            let result = self.client.lock(LockRequest {
                range: descriptor.id,
                epoch: descriptor.epoch,
                txn: ClusterTxnId(txn.0),
                priority,
                target,
                mode,
                timeout_ms: Some(slice),
            });
            self.state(txn, |state| state.waiting = None);
            match result {
                Ok(grant) => {
                    self.record_lock_grant(txn, &descriptor, grant.generation)?;
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
                    if self.may_retry(
                        unavailable,
                        *unavailable_since.get_or_insert_with(Instant::now),
                    ) =>
                {
                    unavailable += 1;
                    thread::sleep(delay);
                    delay = (delay * 2).min(Duration::from_millis(50));
                    // A newer epoch is picked up by the next lookup (a route change).
                    let _ = self.reroute(rk, &descriptor);
                }
                Err(error) => return Err(txn_error(error)),
            }
        }
    }

    /// Remembers the epoch and refuses grants from a replacement lock table.
    fn record_lock_grant(
        &self,
        txn: TxnId,
        descriptor: &RangeDescriptor,
        generation: u64,
    ) -> Result<(), TxnError> {
        self.state(txn, |state| {
            state.ranges.insert(descriptor.id);
        });
        // A second generation from one range means earlier locks there were lost:
        // the transaction aborts without trying to commit (design.md 10:40Z (b)).
        let lost = self.state(txn, |state| {
            state
                .epochs
                .entry(descriptor.id)
                .or_insert(descriptor.epoch);
            let held = *state.generations.entry(descriptor.id).or_insert(generation);
            if held != generation {
                state.aborted = true;
            }
            held != generation
        });
        if lost {
            return Err(TxnError::Serialization(SerializationReason::WriteConflict));
        }
        Ok(())
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

    /// R5-71/R5-72: a SERIALIZABLE writer read at its snapshot. At commit it takes Shared locks
    /// over what it read of Rows and Index, range by range in key order. Point reads validate
    /// from each lock grant's latest committed version; intervals are read again at a timestamp
    /// taken after the last grant. A version newer than the snapshot fails 40001. Points and
    /// scanned versions share one budget; point-only validation needs no reader or oracle call.
    /// Under those locks no other commit changes what it read until it ends, so its reads hold
    /// at its commit timestamp and committed SERIALIZABLE writers serialize in commit-timestamp
    /// order. A wait that ends in a deadlock or at the lock backstop is a serialization failure
    /// (40001) here; a cancelled statement stays cancelled.
    fn validate_reads(
        &self,
        txn: TxnId,
        reads: ReadSet,
        snapshot: Ts,
        mut validation: ValidationConfig,
    ) -> Result<(), TxnError> {
        let data = |ks: &Keyspace| matches!(ks, Keyspace::Rows | Keyspace::Index);
        let reads = ReadSet {
            points: reads
                .points
                .into_iter()
                .filter(|(ks, _)| data(ks))
                .collect(),
            ranges: reads.ranges.into_iter().filter(|r| data(&r.ks)).collect(),
        };
        let mut targets: Vec<(Vec<u8>, LockTarget)> = Vec::new();
        for (ks, key) in &reads.points {
            let space = range_space(*ks).ok_or(TxnError::Misuse)?;
            targets.push((
                range_key(space, key),
                LockTarget::Key {
                    space,
                    key: key.clone(),
                },
            ));
        }
        targets.sort_by(|a, b| a.0.cmp(&b.0));
        let cancel = || crate::cancellation::check().is_err();
        let shared = |rk: &[u8], target: LockTarget| {
            self.lock_in_range(txn, rk, target, RangeLockMode::Shared, None, &cancel)
                .map_err(|error| match error {
                    // PostgreSQL reports neither at COMMIT: the read set could not be kept.
                    TxnError::Deadlock | TxnError::LockTimeout => {
                        TxnError::Serialization(SerializationReason::ReadWriteConflict)
                    }
                    other => other,
                })
        };
        let mut at = 0;
        while at < targets.len() {
            self.check_live(txn)?;
            if cancel() {
                return Err(TxnError::Cancelled);
            }
            let descriptor = self.descriptor(&targets[at].0)?;
            let mut end = at + 1;
            while end < targets.len() && descriptor.contains(&targets[end].0) {
                end += 1;
            }
            let group = &targets[at..end];
            let grants = if group.len() >= 2 {
                // Even a partial batch can hold locks: remember the range for release.
                self.state(txn, |state| {
                    state.ranges.insert(descriptor.id);
                });
                self.client
                    .lock_batch(LockBatchRequest {
                        range: descriptor.id,
                        epoch: descriptor.epoch,
                        txn: ClusterTxnId(txn.0),
                        priority: self.priority(txn)?,
                        mode: RangeLockMode::Shared,
                        targets: group.iter().map(|(_, target)| target.clone()).collect(),
                    })
                    .ok()
                    .filter(|grants| grants.len() == group.len())
            } else {
                None
            };
            if let Some(grants) = grants {
                for grant in grants {
                    self.record_lock_grant(txn, &descriptor, grant.generation)?;
                    validation
                        .validate_locked_point(grant.latest.map(|(ts, _)| Ts(ts.0)), snapshot)?;
                }
            } else {
                for (rk, target) in group {
                    let (_, latest) = shared(rk, target.clone())?;
                    validation.validate_locked_point(latest.map(|(ts, _)| Ts(ts.0)), snapshot)?;
                }
            }
            at = end;
        }
        // R5-72: each scanned interval (to its last key read, or to its end bound when the
        // scan ran out, which covers the gap after the last key) is gap-locked in every range
        // it spans, so no insert, update or delete can land inside it before this ends.
        for range in reads.ranges.iter().filter(|r| !empty(&r.start, &r.end)) {
            let space = range_space(range.ks).ok_or(TxnError::Misuse)?;
            let mut route = match &range.start {
                Bound::Included(key) | Bound::Excluded(key) => range_key(space, key),
                Bound::Unbounded => range_key(space, &[]),
            };
            loop {
                let target = LockTarget::Interval {
                    space,
                    start: range.start.clone(),
                    end: range.end.clone(),
                };
                let (descriptor, _) = shared(&route, target)?;
                match descriptor.end {
                    Some(next) if !past_end(space, &next, &range.end) => route = next,
                    _ => break,
                }
            }
        }
        if !reads.ranges.iter().any(|r| !empty(&r.start, &r.end)) {
            return Ok(());
        }
        let at = Ts(self.oracle()?.0);
        let reader = RoutedKvRead::new(self.client.clone(), at, self.config.page_bytes);
        let intervals = ReadSet {
            points: BTreeSet::new(),
            ranges: reads.ranges,
        };
        Validator::new(intervals, validation).validate_kv(&reader, snapshot)
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

    /// Normal release, also used by the ambiguous-DDL settle thread.
    fn finish_release(&self, txn: TxnId) {
        let state = self
            .txns
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&txn);
        if let Some(mut state) = state {
            // A DDL reopens the gateways it closed before its range locks go (Phase 11).
            if let Some(ddl) = state.ddl.take() {
                self.end_ddl(txn, ddl);
            }
            for range in state.ranges {
                // Unreleased locks die with their lease; nothing to retry here.
                let _ = self.client.release(range, ClusterTxnId(txn.0));
            }
        }
        self.gateway.release_all(txn);
        // The transaction has ended (after its commit returned, or rolled back): an xid it
        // registered is no longer live here (R5-55 rule 3c).
        self.endpoint.owners().remove(&txn);
    }

    fn heartbeat_all(&self) {
        if self.gate().is_err() {
            return;
        }
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
                if self.gate().is_err() {
                    return;
                }
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

/// A user-key interval [start, end] of one keyspace.
type UserInterval = (Bound<Vec<u8>>, Bound<Vec<u8>>);

/// The part of the user-key interval [start, end] of `space` inside `desc`, if they meet.
/// Range bounds are whole user keys or id prefixes (design.md Changes, Track A (g)).
fn clip_interval(
    desc: &RangeDescriptor,
    space: RangeSpace,
    start: &Bound<Vec<u8>>,
    end: &Bound<Vec<u8>>,
) -> Option<UserInterval> {
    let tag = space.tag();
    let lo = match desc.start.split_first() {
        None => Bound::Unbounded,
        Some((&t, _)) if t < tag => Bound::Unbounded,
        Some((&t, _)) if t > tag => return None,
        Some((_, [])) => Bound::Unbounded,
        Some((_, user)) => Bound::Included(user.to_vec()),
    };
    let hi = match desc.end.as_deref().map(<[u8]>::split_first) {
        None => Bound::Unbounded,
        Some(None) => return None,
        Some(Some((&t, _))) if t < tag => return None,
        Some(Some((&t, _))) if t > tag => Bound::Unbounded,
        Some(Some((_, []))) => return None,
        Some(Some((_, user))) => Bound::Excluded(user.to_vec()),
    };
    let key = |b: &Bound<Vec<u8>>| match b {
        Bound::Included(k) | Bound::Excluded(k) => Some(k.clone()),
        Bound::Unbounded => None,
    };
    // The tighter lower bound: the larger key; Excluded wins a tie.
    let start = match (key(start), key(&lo)) {
        (None, _) => lo,
        (_, None) => start.clone(),
        (Some(a), Some(b)) if a > b || (a == b && matches!(start, Bound::Excluded(_))) => {
            start.clone()
        }
        _ => lo,
    };
    // The tighter upper bound: the smaller key; Excluded wins a tie.
    let end = match (key(end), key(&hi)) {
        (None, _) => hi,
        (_, None) => end.clone(),
        (Some(a), Some(b)) if a < b || (a == b && matches!(end, Bound::Excluded(_))) => end.clone(),
        _ => hi,
    };
    (!empty(&start, &end)).then_some((start, end))
}

/// Whether the interval [start, end] holds no key.
fn empty(start: &Bound<Vec<u8>>, end: &Bound<Vec<u8>>) -> bool {
    match (start, end) {
        (Bound::Included(a), Bound::Included(b)) => a > b,
        (Bound::Included(a) | Bound::Excluded(a), Bound::Included(b) | Bound::Excluded(b)) => {
            a >= b
        }
        _ => false,
    }
}

impl Drop for ClusterTxnServices {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.leave();
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
        // A fenced gateway serves no statement; a transaction live across a fence never
        // runs again (ddl module).
        let joined = self.check_serving()?;
        let ts = self.oracle()?;
        self.admit(txn, joined, |state| {
            state.priority.get_or_insert(ts);
        })?;
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
    fn lock_key_shared(
        &self,
        txn: TxnId,
        ks: Keyspace,
        key: &[u8],
        deadline: Option<Instant>,
        cancel: &dyn Fn() -> bool,
    ) -> Result<(), TxnError> {
        let space = match ks {
            Keyspace::Rows => RangeSpace::Rows,
            Keyspace::Index => RangeSpace::Index,
            _ => return Err(TxnError::Misuse),
        };
        if key.len() < 8 {
            return Err(TxnError::Misuse);
        }
        self.lock_in_range(
            txn,
            &range_key(space, key),
            LockTarget::Key {
                space,
                key: key.to_vec(),
            },
            RangeLockMode::Shared,
            deadline,
            cancel,
        )?;
        Ok(())
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
        // Counted until it returns, and before the transaction's own checks: a gateway rejoins,
        // and answers a DDL's close without a proxy, only once no commit call is in flight.
        let _commit = self.begin_commit()?;
        self.check_live(txn)?;
        let result = self.commit_writes(txn, commit);
        if result == Err(TxnError::CommitAmbiguous) {
            // A DDL whose outcome is unknown keeps the other gateways closed until it settled.
            self.state(txn, |state| {
                if let Some(ddl) = state.ddl.as_mut() {
                    ddl.ambiguous = true;
                }
            });
        }
        result
    }
    fn release_all(&self, txn: TxnId) {
        if !self.defer_ddl_release(txn) {
            self.finish_release(txn);
        }
    }
    fn next_txn_id(&self) -> TxnId {
        ClusterTxnServices::next_txn_id(self)
    }
    fn ddl_begin(&self, txn: TxnId, cancel: &dyn Fn() -> bool) -> Result<(), TxnError> {
        self.begin_ddl(txn, cancel)
    }
    fn ddl_close(
        &self,
        txn: TxnId,
        tables: &[u64],
        cancel: &dyn Fn() -> bool,
    ) -> Result<(), TxnError> {
        self.close_ddl(txn, tables, cancel)
    }
    fn client(&self) -> Arc<dyn RangeClient> {
        self.client.clone()
    }
    fn register_xact(&self, txn: TxnId, xid: u64) -> Result<(), TxnError> {
        // Live before the record can be read: whoever finds Owner and asks hears "live".
        self.endpoint.owners().insert(txn);
        let result = self.write_owner(txn, xid);
        if result.is_err() {
            self.endpoint.owners().remove(&txn);
        }
        result
    }
    fn xact_status(&self, xid: u64) -> Result<Option<XactStatus>, TxnError> {
        self.xact_status_of(xid)
    }
}

impl ClusterTxnServices {
    /// The commit itself (`ClusterServices::commit` counts it and checks the transaction).
    fn commit_writes(&self, txn: TxnId, mut commit: ClusterCommit) -> Result<Ts, TxnError> {
        let serializable = commit.isolation == Isolation::Serializable;
        if serializable {
            let reads = commit.reads.take().unwrap_or_default();
            // R5-70: a read-only SERIALIZABLE transaction read one snapshot. Every SERIALIZABLE
            // writer validates its reads under shared locks (below), so committed writers are
            // serializable in commit-timestamp order and a snapshot sees a prefix of that order.
            if commit.writes.is_empty() && commit.raw.is_empty() {
                return Ok(commit.snapshot);
            }
            self.validate_reads(txn, reads, commit.snapshot, commit.validation.clone())?;
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
        // R5-55 rule 2: an xid's Committed record is one more write of the transaction, in
        // the system range, so it is committed iff the transaction is.
        if let Some(xid) = marker_xid {
            commit.writes.insert(
                (Keyspace::Catalog, xact_key(xid)),
                Some(XactRecord::Committed.encode()),
            );
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
        if serializable {
            // R5-71: ranges where the transaction holds only shared locks prepare too, so those
            // locks are replicated with the prepared record and held until the decision; a
            // lost lock table refuses the prepare (lock generation).
            let held: Vec<(RangeId, Epoch, u64)> = self.state(txn, |state| {
                let mut held: Vec<_> = state
                    .generations
                    .iter()
                    .filter_map(|(range, generation)| {
                        state
                            .epochs
                            .get(range)
                            .map(|epoch| (*range, *epoch, *generation))
                    })
                    .collect();
                held.sort();
                held
            });
            for (range, epoch, generation) in held {
                if !groups.iter().any(|p| p.range == range) {
                    groups.push(Participant {
                        range,
                        epoch,
                        writes: Vec::new(),
                        lock_generation: Some(generation),
                    });
                }
            }
        }
        self.check_lock_only(txn, &groups)?;
        let ts = match groups.len() {
            // Nothing to write (no xid, only retired counters): the transaction ends at a
            // fresh timestamp.
            0 => self.oracle()?,
            // One range and no xid: one single-range commit.
            1 if marker_xid.is_none() => {
                let group = groups.pop().expect("one group");
                // The commit re-resolves its range after each backoff (GD-0075 P9-9).
                let first = group.writes.first().ok_or(TxnError::Misuse)?;
                let key = range_key(first.space, &first.key);
                let desc = current.filter(|d| (d.id, d.epoch) == (group.range, group.epoch));
                let route = desc.as_ref().map(|d| (key.as_slice(), d));
                self.commit_one(txn, &group, route)?
            }
            _ => {
                // Several ranges, or any transaction holding an xid (even when the system
                // range is its only participant): Track D's coordinator, with the home in the
                // first range written in key order (the system range when an xid's record is
                // written). An xid's Committed record is then prepared before any decision, so
                // a late request can only commit through a Decide this coordinator sent after
                // every vote, and a reader that finds the owner ended waits on the prepared
                // write; a plain single-range commit could land after a reader reported the xid
                // "aborted" (review S1).
                //
                // A Rows/Index participant that granted this transaction no lock cannot
                // prepare: its writes must be covered by locks taken there in its current term,
                // and it is a split child whose split dropped them (review B N4). Fail at once,
                // before any prepare. Catalog-only participants need no lock.
                if groups.iter().any(|p| {
                    p.lock_generation.is_none()
                        && p.writes
                            .iter()
                            .any(|w| matches!(w.space, RangeSpace::Rows | RangeSpace::Index))
                }) {
                    return Err(TxnError::Serialization(SerializationReason::WriteConflict));
                }
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
                commit_cross_range_gated(self.client.as_ref(), plan, &|| self.gate()).map_err(
                    |e| match e {
                        // A participant without a leader (a split child reopening or electing)
                        // aborted the transaction before any decision: retryable, not an I/O error.
                        CommitError::Aborted(
                            RangeError::NotLeader { .. } | RangeError::Unavailable(_),
                        ) => TxnError::Serialization(SerializationReason::WriteConflict),
                        CommitError::Aborted(cause) => match txn_error(cause) {
                            // A definite abort is never reported as an unknown outcome.
                            TxnError::CommitAmbiguous => {
                                TxnError::Serialization(SerializationReason::WriteConflict)
                            }
                            other => other,
                        },
                        CommitError::Unknown(_) => TxnError::CommitAmbiguous,
                    },
                )?
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
    fn xact_status_of(&self, xid: u64) -> Result<Option<XactStatus>, TxnError> {
        let (node, owner) = match read_record(self.client.as_ref(), xid)? {
            Lookup::Pending => return Ok(Some(XactStatus::InProgress)),
            // Never assigned, or the assignment failed before anyone saw the xid.
            Lookup::Found(None) => return Ok(None),
            Lookup::Found(Some(XactRecord::Committed)) => return Ok(Some(XactStatus::Committed)),
            Lookup::Found(Some(XactRecord::Owner { node, txn })) => (node, txn),
        };
        let live = if node == u64::from(self.config.node) {
            Ok(self.endpoint.txn_live(owner))
        } else {
            match &self.peer {
                Some(peer) => peer.txn_live(node, owner),
                None => Err(RangeError::Unavailable("no gateway peers".into())),
            }
        };
        Ok(Some(match live {
            // Live, or the owner cannot be asked: it may still commit if only partitioned.
            Ok(true) | Err(_) => XactStatus::InProgress,
            // Ended: its commit returned (the decision is durable) or it rolled back. A read
            // at a new timestamp sees its Committed record or resolves the prepared write.
            Ok(false) => match read_record(self.client.as_ref(), xid)? {
                Lookup::Pending => XactStatus::InProgress,
                Lookup::Found(Some(XactRecord::Committed)) => XactStatus::Committed,
                Lookup::Found(_) => XactStatus::Aborted,
            },
        }))
    }
}

impl ClusterTxnServices {
    /// One single-range commit of a transaction without an xid. NotLeader and Unavailable are
    /// retried like `retry` does, but once an attempt may have reached a leader (Unavailable:
    /// a transport timeout, or a request still queued there) the outcome is unknown: a later
    /// failure of any kind, or running out of attempts, is `CommitAmbiguous` (08007), never an
    /// I/O error or an abort (review S1). Running out of attempts on NotLeader alone is
    /// reported as unknown too. Before any such attempt a definite error keeps its meaning
    /// (an abort stays 40001). With `route` = (a key the group writes, its descriptor), the
    /// range is looked up again after each backoff (`reroute`): a newer epoch there ends the
    /// commit as StaleEpoch (40001), or `CommitAmbiguous` once an attempt may have applied.
    /// A group holding no lock on the range (`lock_generation` None) is not resent at all once
    /// an attempt may have applied (review S3).
    fn commit_one(
        &self,
        txn: TxnId,
        group: &Participant,
        route: Option<(&[u8], &RangeDescriptor)>,
    ) -> Result<Timestamp, TxnError> {
        let mut delay = Duration::from_millis(1);
        let mut attempt = 0;
        let mut maybe_applied = false;
        let started = Instant::now();
        loop {
            attempt += 1;
            // Phase 11: no commit request once this gateway's membership lapsed.
            if self.gate().is_err() {
                return Err(if maybe_applied {
                    TxnError::CommitAmbiguous
                } else {
                    TxnError::Serialization(SerializationReason::GatewayFenced)
                });
            }
            match self.client.commit(
                group.range,
                group.epoch,
                ClusterTxnId(txn.0),
                group.writes.clone(),
                None,
                group.lock_generation,
            ) {
                Ok(ts) => return Ok(ts),
                Err(e @ (RangeError::NotLeader { .. } | RangeError::Unavailable(_))) => {
                    maybe_applied |= matches!(e, RangeError::Unavailable(_));
                    // Review S3, design.md Changes (g): with no lock on the range, nothing
                    // there refuses a resend (a catalog-only commit would apply again at a
                    // later timestamp), so a possibly applied attempt is never resent.
                    if (maybe_applied && group.lock_generation.is_none())
                        || !self.may_retry(attempt - 1, started)
                    {
                        return Err(TxnError::CommitAmbiguous);
                    }
                    thread::sleep(delay);
                    delay = (delay * 2).min(Duration::from_millis(50));
                    if let Some((key, desc)) = route
                        && let Some(descriptor) = self.reroute(key, desc)
                    {
                        if maybe_applied {
                            return Err(TxnError::CommitAmbiguous);
                        }
                        return Err(txn_error(RangeError::StaleEpoch { descriptor }));
                    }
                }
                Err(_) if maybe_applied => return Err(TxnError::CommitAmbiguous),
                Err(e) => return Err(txn_error(e)),
            }
        }
    }

    /// R5-55 rule 1: `XACT/<xid> = Owner{node, txn}` as one single-range commit on the system
    /// range. It runs under a transaction id of its own: a commit ends its transaction on the
    /// range (releasing its locks there), and `txn` is still running. Each attempt takes a
    /// fresh id: a range commit is never retried under an id whose earlier attempt failed
    /// (design.md Changes (g)).
    fn write_owner(&self, txn: TxnId, xid: u64) -> Result<(), TxnError> {
        let record = XactRecord::Owner {
            node: u64::from(self.config.node),
            txn: ClusterTxnId(txn.0),
        };
        let route = range_key(RangeSpace::Catalog, &xact_key(xid));
        let mut delay = Duration::from_millis(1);
        let mut last = RangeError::Unavailable("no attempt".into());
        for _ in 0..self.config.retries {
            let writer = ClusterTxnId(self.next_txn_id().0);
            let descriptor = self.descriptor(&route)?;
            // Catalog writes on the system range need no lock lease.
            match self.client.commit(
                descriptor.id,
                descriptor.epoch,
                writer,
                vec![record.write(xid)],
                None,
                None,
            ) {
                Ok(_) => return Ok(()),
                // Refused before anything was proposed: safe to try again.
                Err(e @ RangeError::StaleEpoch { .. }) => {
                    self.client.invalidate(&descriptor);
                    last = e;
                    thread::sleep(delay);
                    delay = (delay * 2).min(Duration::from_millis(50));
                }
                // Anything else may still apply later. A retry could then land an Owner record
                // over a later Committed one, and the xid would read as aborted; instead the
                // assignment fails and the xid is never shown (its late record names an xid
                // nobody holds).
                Err(e) => {
                    self.client.invalidate(&descriptor);
                    return Err(txn_error(e));
                }
            }
        }
        Err(txn_error(last))
    }
}
