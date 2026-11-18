//! Single durable commit sequencer and bounded conflict history.
use super::{
    codec::CodecError,
    hlc::{HlcClock, Ts},
    kv::{Durability, Keyspace, Kv, KvRead, WriteBatch},
    once_guard::CommitGuard,
    txn::TxnError,
    write_fence::{FenceState, WriteFenceRecord, holder_digest},
};
use crate::storage::range_api::TsOracle;
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
        mpsc,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

pub(crate) trait Clock: Send + Sync {
    fn now_ms(&self) -> u64;
}
pub(crate) struct SystemClock;
impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(u64::MAX as u128) as u64
    }
}
/// A wall clock that reads a set instant (tests and the hidden
/// `Admin::set_task_wall_clock`).
pub(crate) struct ManualClock(std::sync::atomic::AtomicU64);
impl ManualClock {
    pub(crate) fn new(ms: u64) -> Self {
        Self(std::sync::atomic::AtomicU64::new(ms))
    }
    #[cfg(test)]
    pub(crate) fn set(&self, ms: u64) {
        self.0.store(ms, std::sync::atomic::Ordering::SeqCst);
    }
}
impl Clock for ManualClock {
    fn now_ms(&self) -> u64 {
        self.0.load(std::sync::atomic::Ordering::SeqCst)
    }
}

/// Where commit timestamps come from (GD-0075 design section 4, rules O1/O4/O7).
#[derive(Clone, Default)]
pub(crate) enum TsSource {
    /// The local hybrid logical clock; the behaviour before ranges.
    #[default]
    Hlc,
    /// A range leader: one oracle timestamp per commit group, refetched while it
    /// is not above the served-read floor and the last commit timestamp (O4).
    Oracle(Arc<dyn TsOracle>),
}
impl std::fmt::Debug for TsSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Hlc => f.write_str("Hlc"),
            Self::Oracle(_) => f.write_str("Oracle(..)"),
        }
    }
}
/// Refetches allowed before an oracle that keeps returning stale values fails the group.
const ORACLE_REFETCH_LIMIT: usize = 1000;
/// The served-read floor and in-flight timestamp reservations of one range leader (GD-0075
/// design section 4a, O3/O4/O6): every commit or prepare timestamp is reserved above the floor
/// and stays registered until its write is applied; a read at T raises the floor to T and waits
/// for every reservation at or below T. Check-and-register and raise-and-collect take one mutex,
/// so a reservation either sees the raised floor (and refetches) or is waited for.
pub(crate) struct ServedGate {
    state: Mutex<GateState>,
    changed: Condvar,
}
struct GateState {
    served: Ts,
    /// First timestamp of each live reservation, with a count.
    in_flight: BTreeMap<Ts, usize>,
}
impl ServedGate {
    pub(crate) fn new(floor: Ts) -> Self {
        Self {
            state: Mutex::new(GateState {
                served: floor,
                in_flight: BTreeMap::new(),
            }),
            changed: Condvar::new(),
        }
    }
    fn lock(&self) -> std::sync::MutexGuard<'_, GateState> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }
    pub(crate) fn served(&self) -> Ts {
        self.lock().served
    }
    /// Raises the floor without waiting (leader open, O7).
    pub(crate) fn raise(&self, ts: Ts) {
        let mut state = self.lock();
        state.served = state.served.max(ts);
    }
    /// Reserves `n` consecutive oracle timestamps whose first is above max(floor, `last`) and
    /// registers the reservation in flight. The oracle is called outside the mutex.
    pub(crate) fn reserve(&self, oracle: &dyn TsOracle, n: u32, last: Ts) -> Result<Ts, TxnError> {
        for _ in 0..ORACLE_REFETCH_LIMIT {
            // Timestamp and Ts share the ms << 16 | logical layout.
            let first = Ts(oracle
                .next(n)
                .map_err(|_| TxnError::Codec(CodecError::Clock))?
                .0);
            let mut state = self.lock();
            if first > state.served.max(last) {
                *state.in_flight.entry(first).or_default() += 1;
                return Ok(first);
            }
        }
        Err(TxnError::Codec(CodecError::Clock))
    }
    /// Waits until no reservation is in flight (an old leader's last writes). False when
    /// `deadline` passes first.
    pub(crate) fn drain(&self, deadline: Instant) -> bool {
        let mut state = self.lock();
        while !state.in_flight.is_empty() {
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            state = self
                .changed
                .wait_timeout(state, deadline - now)
                .unwrap_or_else(|p| p.into_inner())
                .0;
        }
        true
    }
    /// Ends a reservation made by `reserve` (applied, failed or abandoned).
    pub(crate) fn finish(&self, first: Ts) {
        let mut state = self.lock();
        if let Some(count) = state.in_flight.get_mut(&first) {
            *count -= 1;
            if *count == 0 {
                state.in_flight.remove(&first);
            }
        }
        drop(state);
        self.changed.notify_all();
    }
    /// O6: raises the floor to `ts`, then waits until no reservation at or below `ts` is in
    /// flight. False when `deadline` passes first.
    pub(crate) fn serve(&self, ts: Ts, deadline: Option<Instant>) -> bool {
        let mut state = self.lock();
        state.served = state.served.max(ts);
        while state.in_flight.range(..=ts).next().is_some() {
            match deadline {
                None => state = self.changed.wait(state).unwrap_or_else(|p| p.into_inner()),
                Some(deadline) => {
                    let now = Instant::now();
                    if now >= deadline {
                        return false;
                    }
                    state = self
                        .changed
                        .wait_timeout(state, deadline - now)
                        .unwrap_or_else(|p| p.into_inner())
                        .0;
                }
            }
        }
        true
    }
}
/// Finishes a reservation when dropped.
struct Reservation<'a> {
    gate: &'a ServedGate,
    first: Ts,
}
impl Drop for Reservation<'_> {
    fn drop(&mut self) {
        self.gate.finish(self.first);
    }
}

#[derive(Clone, Debug)]
pub(crate) struct SequencerConfig {
    /// Maximum additional requests gathered after the first; zero means singleton.
    pub(crate) group_max: usize,
    /// Optional key/value byte limit; the first request is always accepted.
    pub(crate) group_max_bytes: Option<usize>,
    /// Zero or sub-millisecond waits only drain requests already queued.
    pub(crate) group_wait: Duration,
    pub(crate) recent_log_bytes: usize,
    pub(crate) write_set_bytes: usize,
    pub(crate) ts_source: TsSource,
}
impl Default for SequencerConfig {
    fn default() -> Self {
        Self {
            group_max: 64,
            group_max_bytes: None,
            group_wait: Duration::ZERO,
            recent_log_bytes: 256 * 1024 * 1024,
            write_set_bytes: 256 * 1024 * 1024,
            ts_source: TsSource::Hlc,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct LogOverflow;
type Keys = Arc<[(Keyspace, Vec<u8>)]>;
#[derive(Debug)]
pub(crate) struct RecentLog {
    entries: VecDeque<(Ts, Keys)>,
    bytes: usize,
    budget: usize,
    horizon: Ts,
}
impl RecentLog {
    pub(crate) fn new(budget: usize) -> Self {
        Self {
            entries: VecDeque::new(),
            bytes: 0,
            budget,
            horizon: Ts(0),
        }
    }
    pub(crate) fn horizon(&self) -> Ts {
        self.horizon
    }
    pub(crate) fn byte_size(&self) -> usize {
        self.bytes
    }
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }
    pub(crate) fn append(&mut self, ts: Ts, keys: Keys) {
        self.bytes = self.bytes.saturating_add(Self::entry_bytes(&keys));
        self.entries.push_back((ts, keys));
    }
    // Charge timestamp (8), keyspace tag (1 per key), and key bytes; exclude allocator overhead.
    fn entry_bytes(keys: &[(Keyspace, Vec<u8>)]) -> usize {
        8usize.saturating_add(
            keys.iter()
                .map(|(_, k)| 1usize.saturating_add(k.len()))
                .sum::<usize>(),
        )
    }
    pub(crate) fn trim(&mut self, oldest: Option<Ts>) {
        while self.entries.front().is_some_and(|(ts, _)| {
            oldest.is_none_or(|oldest| *ts <= oldest) || self.bytes > self.budget
        }) {
            let (ts, keys) = self.entries.pop_front().unwrap();
            self.bytes -= Self::entry_bytes(&keys);
            if oldest.is_some_and(|oldest| oldest < ts) {
                self.horizon = self.horizon.max(ts);
            }
        }
    }
    pub(crate) fn writes_after(
        &self,
        snapshot: Ts,
        mut f: impl FnMut(Ts, Keyspace, &[u8]),
    ) -> Result<(), LogOverflow> {
        if snapshot < self.horizon {
            return Err(LogOverflow);
        }
        for (ts, keys) in &self.entries {
            if *ts > snapshot {
                for (ks, key) in keys.iter() {
                    f(*ts, *ks, key);
                }
            }
        }
        Ok(())
    }
}

type Validation = Box<dyn Fn(&RecentLog, &dyn KvRead, Ts, Ts) -> Result<(), TxnError> + Send>;

pub(crate) struct CommitRequest {
    pub(crate) snapshot: Ts,
    pub(crate) writes: BTreeMap<(Keyspace, Vec<u8>), Option<Vec<u8>>>,
    pub(crate) features_or: u64,
    pub(crate) minimum_subversion: u32,
    pub(crate) raw: Vec<RawOp>,
    // Hooks inspect the worker-owned log; no snapshot mutex is held during validation.
    pub(crate) validation: Option<Validation>,
}
#[derive(Clone, Debug)]
pub(crate) enum RawOp {
    XactCommitted { xid: u64 },
    DeleteCounter { key: Vec<u8> },
    PutCounter { key: Vec<u8>, value: Vec<u8> },
}
pub(crate) fn valid_counter_key(key: &[u8]) -> bool {
    key.len() == 25 && key.first() == Some(&2)
}
const SNAPSHOT_SHARDS: usize = 16;
static NEXT_SNAPSHOT_SHARD: AtomicUsize = AtomicUsize::new(0);
thread_local! {
    static SNAPSHOT_SHARD: usize = NEXT_SNAPSHOT_SHARD.fetch_add(1, Ordering::Relaxed) % SNAPSHOT_SHARDS;
}
#[cfg(test)]
type HeldCounterReplies = Option<Vec<mpsc::Sender<Result<(), TxnError>>>>;
struct Shared {
    #[cfg(test)]
    counter_gate: Mutex<HeldCounterReplies>,
    #[cfg(test)]
    max_group: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    max_mixed_group: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    sync_writes: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    highwater_puts: std::sync::atomic::AtomicUsize,
    visible: AtomicU64,
    active: [Mutex<BTreeMap<Ts, usize>>; SNAPSHOT_SHARDS],
}
pub(crate) struct SnapshotHandle {
    shared: Arc<Shared>,
    shard: usize,
    ts: Ts,
}
impl SnapshotHandle {
    pub(crate) fn ts(&self) -> Ts {
        self.ts
    }
}
impl Drop for SnapshotHandle {
    fn drop(&mut self) {
        let mut active = self.shared.active[self.shard].lock().unwrap_or_else(|p| p.into_inner());
        if let Some(count) = active.get_mut(&self.ts) {
            *count -= 1;
            if *count == 0 {
                active.remove(&self.ts);
            }
        }
    }
}
fn oldest_active(shared: &Shared) -> Option<Ts> {
    shared
        .active
        .iter()
        .filter_map(|shard| {
            shard.lock().unwrap_or_else(|p| p.into_inner()).keys().next().copied()
        })
        .min()
}

/// Called only by the single sequencer publisher, with monotonic timestamps.
fn publish_visible(shared: &Shared, max: Ts) -> Ts {
    // Store visible (Release) FIRST, then lock each shard to scan registrations.
    // snapshot() locks its shard BEFORE loading visible (Acquire). Any snapshot
    // missed by this scan registered after its shard was released and therefore
    // read visible >= max, so min(max, oldest) never passes a live snapshot.
    shared.visible.store(max.0, Ordering::Release);
    max.min(oldest_active(shared).unwrap_or(max))
}

struct Pending {
    request: CommitRequest,
    guard: Option<CommitGuard>,
    reply: mpsc::Sender<Result<Ts, TxnError>>,
}
enum Message {
    Commit(Pending),
    Counter {
        key: Vec<u8>,
        value: Vec<u8>,
        features_or: u64,
        reply: mpsc::Sender<Result<(), TxnError>>,
    },
    Highwater {
        key: Vec<u8>,
        value: Vec<u8>,
        reply: mpsc::Sender<Result<(), TxnError>>,
    },
    Fence {
        action: FenceAction,
        reply: mpsc::Sender<Result<Option<WriteFenceRecord>, TxnError>>,
    },
    Shutdown,
}
impl From<Pending> for Message {
    fn from(pending: Pending) -> Self { Self::Commit(pending) }
}
impl Message {
    fn bytes(&self) -> usize {
        match self {
            Self::Commit(p) => request_bytes(&p.request),
            Self::Counter { key, value, .. } | Self::Highwater { key, value, .. } => key.len() + value.len(),
            Self::Fence { .. } | Self::Shutdown => unreachable!("fence actions and shutdown are never grouped"),
        }
    }
    fn reject(self, error: TxnError) {
        match self {
            Self::Commit(p) => { let _ = p.reply.send(Err(error)); }
            Self::Counter { reply, .. } | Self::Highwater { reply, .. } => { let _ = reply.send(Err(error)); }
            Self::Fence { .. } | Self::Shutdown => unreachable!("fence actions and shutdown are never grouped"),
        }
    }
}

// Only digests enter the queue; holder tokens are never retained or formatted.
enum FenceAction {
    Install { digest: [u8; 32], label: String },
    Release { digest: [u8; 32], epoch: u64 },
    Status,
}
#[derive(Default)]
struct FenceAccess {
    record: Option<WriteFenceRecord>,
    holder: bool,
}
impl FenceAccess {
    fn check(&self) -> Result<(), TxnError> {
        if let Some(record) = &self.record
            && record.state == FenceState::Fenced
            && !self.holder
        {
            return Err(TxnError::WriteFenced {
                epoch: record.epoch,
            });
        }
        Ok(())
    }
}
pub(crate) struct CounterAck(pub(crate) mpsc::Receiver<Result<(), TxnError>>);
impl CounterAck {
    pub(crate) fn wait(self) -> Result<(), TxnError> {
        self.0.recv().unwrap_or(Err(TxnError::Fenced))
    }
}

pub(crate) struct Sequencer {
    kv: Arc<Kv>,
    config: SequencerConfig,
    shared: Arc<Shared>,
    /// Served-read floor and in-flight reservations (O3/O4/O6); used in oracle mode.
    served: Arc<ServedGate>,
    sender: mpsc::Sender<Message>,
    worker: Mutex<Option<JoinHandle<()>>>,
}
impl Sequencer {
    #[cfg(test)]
    pub(crate) fn gate_counter_acks(&self, armed: bool) {
        let mut gate = self.shared.counter_gate.lock().unwrap();
        if armed { assert!(gate.is_none()); *gate = Some(Vec::new()); }
        else if let Some(replies) = gate.take() {
            for reply in replies { let _ = reply.send(Ok(())); }
        }
    }
    #[cfg(test)]
    pub(crate) fn held_counter_acks(&self) -> usize {
        self.shared.counter_gate.lock().unwrap().as_ref().map_or(0, Vec::len)
    }

    #[cfg(test)]
    pub(crate) fn max_group_size(&self) -> usize {
        self.shared.max_group.load(std::sync::atomic::Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn highwater_puts(&self) -> usize {
        self.shared.highwater_puts.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub(crate) fn open(
        kv: Arc<Kv>,
        clock: Arc<dyn Clock>,
        config: SequencerConfig,
    ) -> Result<Arc<Self>, TxnError> {
        Self::open_with_holder(kv, clock, config, None)
    }
    pub(crate) fn open_with_holder(
        kv: Arc<Kv>,
        clock: Arc<dyn Clock>,
        config: SequencerConfig,
        token: Option<&[u8]>,
    ) -> Result<Arc<Self>, TxnError> {
        let record = kv
            .get(Keyspace::Meta, b"write_fence")?
            .map(|bytes| WriteFenceRecord::decode(&bytes))
            .transpose()
            .map_err(|e| TxnError::Kv(super::kv::KvError::Corrupt(e.to_string())))?;
        let holder = record
            .as_ref()
            .is_some_and(|r| token.is_some_and(|t| holder_digest(t) == r.holder_digest));
        let fence = FenceAccess { record, holder };
        let features = load_features(kv.as_ref())?;
        let subversion = load_subversion(kv.as_ref())?;
        let seed = match kv
            .get(Keyspace::Meta, b"last_commit_ts")
            .map_err(TxnError::Kv)?
        {
            Some(bytes) if bytes.len() == 8 => Ts(u64::from_be_bytes(bytes.try_into().unwrap())),
            Some(_) => {
                return Err(TxnError::Kv(super::kv::KvError::Corrupt(
                    "invalid last_commit_ts".into(),
                )));
            }
            None => Ts(0),
        };
        let shared = Arc::new(Shared {
            #[cfg(test)]
            counter_gate: Mutex::new(None),
            #[cfg(test)]
            max_group: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            max_mixed_group: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            sync_writes: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            highwater_puts: std::sync::atomic::AtomicUsize::new(0),
            visible: AtomicU64::new(seed.0),
            active: std::array::from_fn(|_| Mutex::new(BTreeMap::new())),
        });
        let served = Arc::new(ServedGate::new(Ts(0)));
        let worker_served = served.clone();
        let (sender, receiver) = mpsc::channel();
        let worker_shared = shared.clone();
        let worker_kv = kv.clone();
        let worker_config = config.clone();
        let worker = thread::spawn(move || {
            run(
                receiver,
                worker_kv,
                clock,
                worker_config,
                worker_shared,
                worker_served,
                seed,
                features,
                subversion,
                fence,
            )
        });
        Ok(Arc::new(Self {
            kv,
            config,
            shared,
            served,
            sender,
            worker: Mutex::new(Some(worker)),
        }))
    }
    fn fence_action(&self, action: FenceAction) -> Result<Option<WriteFenceRecord>, TxnError> {
        let (reply, receive) = mpsc::channel();
        self.sender
            .send(Message::Fence { action, reply })
            .map_err(|_| TxnError::Fenced)?;
        receive.recv().unwrap_or(Err(TxnError::Fenced))
    }
    pub(crate) fn install_write_fence(&self, token: &[u8], label: &str) -> Result<u64, TxnError> {
        if label.len() > 256 {
            return Err(TxnError::WriteSetLimit);
        }
        self.fence_action(FenceAction::Install {
            digest: holder_digest(token),
            label: label.into(),
        })?
        .map(|r| r.epoch)
        .ok_or(TxnError::Misuse)
    }
    pub(crate) fn release_write_fence(&self, token: &[u8], epoch: u64) -> Result<(), TxnError> {
        self.fence_action(FenceAction::Release {
            digest: holder_digest(token),
            epoch,
        })?;
        Ok(())
    }
    pub(crate) fn write_fence_status(&self) -> Result<Option<WriteFenceRecord>, TxnError> {
        self.fence_action(FenceAction::Status)
    }
    pub(crate) fn kv(&self) -> &Kv {
        &self.kv
    }
    #[cfg(all(test, feature = "engine-rocksdb"))]
    pub(crate) fn group_max_bytes(&self) -> Option<usize> {
        self.config.group_max_bytes
    }

    /// The served-read gate shared with the range engine's reads and prepares (R5-33, R5-36).
    pub(crate) fn served_gate(&self) -> Arc<ServedGate> {
        self.served.clone()
    }
    /// Raises the served-read floor to at least `ts`.
    #[cfg(test)]
    pub(crate) fn note_served(&self, ts: Ts) {
        self.served.raise(ts);
    }
    pub(crate) fn write_set_bytes(&self) -> usize {
        self.config.write_set_bytes
    }
    pub(crate) fn visible_ts(&self) -> Ts {
        Ts(self.shared.visible.load(Ordering::Acquire))
    }
    pub(crate) fn oldest_active(&self) -> Option<Ts> {
        oldest_active(&self.shared)
    }
    pub(crate) fn snapshot(&self) -> SnapshotHandle {
        let shard = SNAPSHOT_SHARD.with(|shard| *shard);
        let mut active = self.shared.active[shard].lock().unwrap_or_else(|p| p.into_inner());
        // Lock the shard BEFORE loading visible (Acquire) and registering. The
        // publisher stores visible FIRST, then scans the shards. A snapshot the
        // scan misses registered after its shard was released, so reads at least
        // the published timestamp and cannot be passed by that trim bound.
        let ts = Ts(self.shared.visible.load(Ordering::Acquire));
        *active.entry(ts).or_default() += 1;
        SnapshotHandle {
            shared: self.shared.clone(),
            shard,
            ts,
        }
    }
    pub(crate) fn submit(&self, request: CommitRequest) -> Result<Ts, TxnError> {
        if self.kv.is_fenced() {
            return Err(TxnError::Fenced);
        }
        let (reply, receive) = mpsc::channel();
        self.sender
            .send(Message::Commit(Pending { request, guard: None, reply }))
            .map_err(|_| TxnError::Fenced)?;
        receive.recv().unwrap_or(Err(TxnError::Fenced))
    }
    pub(crate) fn submit_guarded(&self, request: CommitRequest, guard: CommitGuard) -> Result<Ts, TxnError> {
        if self.kv.is_fenced() { return Err(TxnError::Fenced); }
        let (reply, receive) = mpsc::channel();
        self.sender.send(Message::Commit(Pending { request, guard: Some(guard), reply }))
            .map_err(|_| TxnError::Fenced)?;
        receive.recv().unwrap_or(Err(TxnError::Fenced))
    }
    /// Queue an unversioned row-version highwater, acknowledged only after Sync.
    pub(crate) fn write_highwater_async(
        &self, key: Vec<u8>, value: Vec<u8>,
    ) -> Result<mpsc::Receiver<Result<(), TxnError>>, TxnError> {
        if key.len() != 9 || key[0] != 3 || value.len() != 8
            || u64::from_be_bytes(value.as_slice().try_into().unwrap()) == 0 {
            return Err(TxnError::Misuse);
        }
        if self.kv.is_fenced() { return Err(TxnError::Fenced); }
        let (reply, receive) = mpsc::channel();
        self.sender.send(Message::Highwater { key, value, reply })
            .map_err(|_| TxnError::Fenced)?;
        Ok(receive)
    }
    /// Queue a reservation without waiting for its durability acknowledgment.
    pub(crate) fn send_counter(
        &self, key: Vec<u8>, value: Vec<u8>, features_or: u64,
    ) -> Result<CounterAck, TxnError> {
        if !valid_counter_key(&key) { return Err(TxnError::Misuse); }
        if self.kv.is_fenced() { return Err(TxnError::Fenced); }
        let (reply, receive) = mpsc::channel();
        self.sender.send(Message::Counter { key, value, features_or, reply })
            .map_err(|_| TxnError::Fenced)?;
        Ok(CounterAck(receive))
    }
    /// A counter reservation is durable independently of any SQL transaction.
    pub(crate) fn write_counter(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        features_or: u64,
    ) -> Result<(), TxnError> {
        self.send_counter(key, value, features_or)?.wait()
    }
}
fn load_features(kv: &dyn KvRead) -> Result<u64, TxnError> {
    // Bare engine fixtures predate format metadata. Format-aware callers must
    // run open_checks first, which requires the features record to be present.
    let bits = match kv.get(Keyspace::Meta, b"features")? {
        None => 0,
        Some(bytes) => u64::from_be_bytes(bytes.try_into().map_err(|_| {
            TxnError::Kv(super::kv::KvError::Corrupt(
                "invalid features length".into(),
            ))
        })?),
    };
    super::meta::check_features(bits)
        .map_err(|e| TxnError::Kv(super::kv::KvError::Corrupt(e.to_string())))?;
    Ok(bits)
}
fn load_subversion(kv: &dyn KvRead) -> Result<u32, TxnError> {
    let version = match kv.get(Keyspace::Meta, b"subversion")? {
        None => super::meta::INITIAL_SUBVERSION, // bare engine fixtures
        Some(bytes) => u32::from_be_bytes(bytes.try_into().map_err(|_| TxnError::Misuse)?),
    };
    super::meta::check_subversion(version, super::meta::CURRENT_SUBVERSION)
        .map_err(|_| TxnError::Misuse)?;
    Ok(version)
}
impl Drop for Sequencer {
    fn drop(&mut self) {
        let _ = self.sender.send(Message::Shutdown);
        if let Some(worker) = self.worker.lock().unwrap_or_else(|p| p.into_inner()).take() {
            let _ = worker.join();
        }
    }
}

fn request_bytes(request: &CommitRequest) -> usize {
    let writes = request
        .writes
        .iter()
        .map(|((_, key), value)| key.len() + value.as_ref().map_or(0, Vec::len))
        .sum::<usize>();
    writes
        + request
            .raw
            .iter()
            .map(|op| match op {
                RawOp::XactCommitted { .. } => 17,
                RawOp::DeleteCounter { key } => key.len(),
                RawOp::PutCounter { key, value } => key.len() + value.len(),
            })
            .sum::<usize>()
}

fn gather(
    first: impl Into<Message>,
    receiver: &mpsc::Receiver<Message>,
    config: &SequencerConfig,
) -> (Vec<Message>, Option<Message>, bool) {
    let first = first.into();
    if matches!(&first, Message::Commit(Pending { guard: Some(_), .. })) {
        return (vec![first], None, false);
    }
    let mut bytes = config
        .group_max_bytes
        .map(|_| first.bytes());
    let mut group = vec![first];
    // Windows timed receives can round a sub-millisecond wait up to a roughly
    // 15 ms timer tick. Drain without waiting by default, and only opt into a
    // timed gather for windows of at least 1 ms, including the remaining time.
    const MIN_TIMED_WAIT: Duration = Duration::from_millis(1);
    let deadline = (config.group_wait >= MIN_TIMED_WAIT).then(|| {
        Instant::now()
            .checked_add(config.group_wait)
            .unwrap_or_else(Instant::now)
    });
    while group.len() - 1 < config.group_max {
        let message = match receiver.try_recv() {
            Ok(message) => message,
            Err(mpsc::TryRecvError::Disconnected) => break,
            Err(mpsc::TryRecvError::Empty) => {
                let Some(deadline) = deadline else {
                    break;
                };
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining < MIN_TIMED_WAIT {
                    break;
                }
                match receiver.recv_timeout(remaining) {
                    Ok(message) => message,
                    Err(_) => break,
                }
            }
        };
        match message {
            guarded @ Message::Commit(Pending { guard: Some(_), .. }) => {
                return (group, Some(guarded), false);
            }
            item @ (Message::Commit(_) | Message::Counter { .. } | Message::Highwater { .. }) => {
                if let (Some(cap), Some(total)) = (config.group_max_bytes, bytes.as_mut()) {
                    let size = item.bytes();
                    if *total > cap || size > cap - *total {
                        return (group, Some(item), false);
                    }
                    *total += size;
                }
                group.push(item);
            }
            fence @ Message::Fence { .. } => return (group, Some(fence), false),
            Message::Shutdown => return (group, None, true),
        }
    }
    (group, None, false)
}

#[allow(clippy::too_many_arguments)]
fn run(
    receiver: mpsc::Receiver<Message>,
    kv: Arc<Kv>,
    clock: Arc<dyn Clock>,
    config: SequencerConfig,
    shared: Arc<Shared>,
    served: Arc<ServedGate>,
    seed: Ts,
    mut features: u64,
    mut subversion: u32,
    mut fence: FenceAccess,
) {
    let mut hlc = HlcClock::new(seed);
    let mut log = RecentLog::new(config.recent_log_bytes);
    let mut fenced = false;
    let mut deferred = None;
    while let Some(message) = deferred.take().or_else(|| receiver.recv().ok()) {
        let first = match message {
            Message::Fence { action, reply } => {
                let result = if fenced || kv.is_fenced() {
                    Err(TxnError::Fenced)
                } else {
                    process_fence(
                        &kv,
                        action,
                        &mut fence,
                        &mut features,
                        &mut hlc,
                        clock.as_ref(),
                    )
                };
                fenced |= matches!(result, Err(TxnError::CommitAmbiguous | TxnError::Fenced));
                let _ = reply.send(result);
                continue;
            }
            Message::Shutdown => break,
            item => item,
        };
        let (group, next, shutdown) = gather(first, &receiver, &config);
        deferred = next;
        if shutdown || fenced || kv.is_fenced() {
            for p in group {
                p.reject(TxnError::Fenced);
            }
            if shutdown {
                break;
            }
            fenced = true;
            continue;
        }
        fenced = process_group(
            group,
            &kv,
            clock.as_ref(),
            &shared,
            &mut hlc,
            &mut log,
            &mut features,
            &mut subversion,
            &config.ts_source,
            &served,
            &fence,
        );
    }
    for message in receiver.try_iter() {
        match message {
            Message::Commit(p) => {
                let _ = p.reply.send(Err(TxnError::Fenced));
            }
            Message::Counter { reply, .. } | Message::Highwater { reply, .. } => {
                let _ = reply.send(Err(TxnError::Fenced));
            }
            Message::Fence { reply, .. } => {
                let _ = reply.send(Err(TxnError::Fenced));
            }
            Message::Shutdown => {}
        }
    }
}
fn process_fence(
    kv: &Kv,
    action: FenceAction,
    fence: &mut FenceAccess,
    features: &mut u64,
    hlc: &mut HlcClock,
    clock: &dyn Clock,
) -> Result<Option<WriteFenceRecord>, TxnError> {
    if matches!(action, FenceAction::Status) {
        return Ok(fence.record.clone());
    }
    if *features & super::meta::REPLICATED != 0 {
        return Err(TxnError::Unsupported);
    }
    let current_epoch = fence.record.as_ref().map_or(0, |r| r.epoch);
    let rejected = || TxnError::WriteFenced {
        epoch: current_epoch,
    };
    let mut record = match action {
        FenceAction::Install { digest, label } => {
            if label.len() > 256 {
                return Err(TxnError::WriteSetLimit);
            }
            if fence
                .record
                .as_ref()
                .is_some_and(|r| r.state == FenceState::Fenced && r.holder_digest != digest)
            {
                return Err(rejected());
            }
            WriteFenceRecord {
                state: FenceState::Fenced,
                epoch: current_epoch
                    .checked_add(1)
                    .ok_or(TxnError::WriteSetLimit)?,
                holder_digest: digest,
                changed_at: 0,
                label,
            }
        }
        FenceAction::Release { digest, epoch } => {
            let r = fence.record.as_ref().ok_or_else(rejected)?;
            if r.state != FenceState::Fenced || r.holder_digest != digest || r.epoch != epoch {
                return Err(rejected());
            }
            let mut r = r.clone();
            r.state = FenceState::Released;
            r
        }
        FenceAction::Status => unreachable!(),
    };
    // Publish the record, feature bit and clock seed in one durable batch.
    let mut next_clock = HlcClock::new(hlc.last);
    record.changed_at = next_clock.next(clock.now_ms())?.0;
    let next_features = *features | super::meta::WRITE_FENCE;
    let mut batch = WriteBatch::default();
    batch.put(Keyspace::Meta, b"write_fence".to_vec(), record.encode());
    super::meta::put_features(&mut batch, next_features).map_err(|_| TxnError::Misuse)?;
    batch.put(
        Keyspace::Meta,
        b"last_commit_ts".to_vec(),
        record.changed_at.to_be_bytes().to_vec(),
    );
    kv.write(batch, Durability::Sync)
        .map_err(|e| write_error(&e))?;
    // A failed Sync leaves all worker-owned fence state unchanged.
    *hlc = next_clock;
    *features = next_features;
    fence.holder = record.state == FenceState::Fenced;
    fence.record = Some(record.clone());
    Ok(Some(record))
}
#[allow(clippy::too_many_arguments)]
fn validate_counter(
    kv: &Kv,
    key: &[u8],
    value: &[u8],
    features_or: u64,
    features: u64,
    subversion: u32,
    native: &super::native_content::NativeContent,
    catalog: &BTreeMap<Vec<u8>, Option<Vec<u8>>>,
) -> Result<u64, TxnError> {
    let next = features | features_or | 256;
    if !valid_counter_key(key) || super::meta::check_features(next).is_err() {
        return Err(TxnError::Misuse);
    }
    let seq_id = u64::from_be_bytes(key[1..9].try_into().expect("validated counter key"));
    let incarnation = u128::from_be_bytes(key[9..25].try_into().expect("validated counter key"));
    // Accepted predecessors are not yet visible in KV. A staged tombstone
    // must hide the persisted definition just as a committed tombstone does.
    let catalog_key = super::catalog_codec::sequence_key(seq_id);
    let definition = match catalog.get(&catalog_key) {
        Some(value) => value.clone(),
        None => super::mvcc::latest_version(kv, Keyspace::Catalog, &catalog_key)?
            .and_then(|(_, value)| value),
    }.ok_or(TxnError::SequenceGone)?;
    let definition = super::catalog_codec::decode_sequence(&definition)
        .map_err(|error| TxnError::Kv(super::kv::KvError::Corrupt(error.to_string())))?;
    if definition.incarnation != incarnation {
        return Err(TxnError::SequenceGone);
    }
    super::catalog_codec::decode_counter(key, value, &definition)
        .map_err(|_| TxnError::Misuse)?;
    native.validate(next, subversion).map_err(|_| TxnError::Misuse)?;
    Ok(next)
}

/// The native-content record. Its read fails on a KV fenced after the worker's fence check
/// (a range engine closed by a leader change or a split, GD-0075 P9-5b): nothing of the
/// group was written, so that is `Fenced`, never `Misuse`.
fn load_native(kv: &Kv) -> Result<super::native_content::NativeContent, TxnError> {
    super::native_content::NativeContent::load(kv).map_err(|_| {
        if kv.is_fenced() { TxnError::Fenced } else { TxnError::Misuse }
    })
}

fn validate_highwater(
    kv: &Kv, key: &[u8], value: &[u8], staged: &BTreeMap<Vec<u8>, u64>,
) -> Result<u64, TxnError> {
    if key.len() != 9 || key[0] != 3 || value.len() != 8 {
        return Err(TxnError::Misuse);
    }
    let end = u64::from_be_bytes(value.try_into().unwrap());
    let current = match staged.get(key) {
        Some(current) => *current,
        None => match kv.get(Keyspace::Seq, key)? {
            None => 1,
            Some(bytes) => {
                let bytes = bytes.try_into().map_err(|_| TxnError::Misuse)?;
                let current = u64::from_be_bytes(bytes);
                if current == 0 { return Err(TxnError::Misuse); }
                current
            }
        },
    };
    if end <= current { return Err(TxnError::Misuse); }
    Ok(end)
}

/// A failed sequencer write: ambiguous, except a fenced KV, which wrote nothing (a range
/// backend reports a sink's definite NotLeader that way, GD-0075 R5-81).
fn write_error(error: &super::kv::KvError) -> TxnError {
    match error {
        super::kv::KvError::Fenced => TxnError::Fenced,
        _ => TxnError::CommitAmbiguous,
    }
}

// Returns whether an ambiguous backend result requires fencing subsequent groups.
#[allow(clippy::too_many_arguments)]
fn process_group(
    group: Vec<Message>,
    kv: &Kv,
    clock: &dyn Clock,
    shared: &Shared,
    hlc: &mut HlcClock,
    log: &mut RecentLog,
    features: &mut u64,
    subversion: &mut u32,
    ts_source: &TsSource,
    served: &ServedGate,
    fence: &FenceAccess,
) -> bool {
    #[cfg(test)]
    shared.max_group.fetch_max(group.len(), std::sync::atomic::Ordering::Relaxed);
    // Reject each writer before timestamp reservation, validation or staging;
    // read-only requests from the same group can still complete. Counter
    // reservations now share this group path instead of process_counter.
    // Highwater only advances the row-version allocator's recovery floor; it
    // publishes no user data. The statement using that range is fenced here
    // when its Commit arrives, even if its allocator refill has already synced.
    let group: Vec<_> = group
        .into_iter()
        .filter_map(|item| {
            let writes = match &item {
                Message::Commit(p) => !p.request.writes.is_empty() || !p.request.raw.is_empty(),
                Message::Counter { .. } => true,
                _ => false,
            };
            if writes && let Err(error) = fence.check() {
                item.reject(error);
                return None;
            }
            Some(item)
        })
        .collect();
    if group.is_empty() {
        return false;
    }
    // Oracle mode reserves the group's timestamps before anything is staged, so an
    // oracle failure fails every request and writes nothing.
    // Each write request gets its own timestamp of one `next(n)` reservation (first + i).
    #[cfg(test)]
    if group.iter().any(|p| matches!(p, Message::Counter { .. } | Message::Highwater { .. }))
        && group.iter().any(|p| matches!(p, Message::Commit(_))) {
        shared.max_mixed_group.fetch_max(group.len(), std::sync::atomic::Ordering::Relaxed);
    }
    let writers = group.iter().filter(|p| matches!(p, Message::Commit(p) if !p.request.writes.is_empty())).count();
    // The reservation stays in flight (readers at or above it wait) until this group ends.
    let mut _reservation = None;
    let mut group_ts = match ts_source {
        TsSource::Oracle(oracle) if writers > 0 => {
            let n = u32::try_from(writers).unwrap_or(u32::MAX);
            match served.reserve(oracle.as_ref(), n, hlc.last) {
                Ok(ts) => {
                    _reservation = Some(Reservation {
                        gate: served,
                        first: ts,
                    });
                    Some(ts)
                }
                Err(error) => {
                    for p in group {
                        p.reject(error.clone());
                    }
                    return false;
                }
            }
        }
        _ => None,
    };
    let visible = Ts(shared.visible.load(Ordering::Acquire));
    let mut max = visible;
    let mut batch = WriteBatch::default();
    let mut accepted = Vec::new();
    let mut counters = Vec::new();
    let mut highwaters = BTreeMap::new();
    let mut catalog = BTreeMap::new();
    let mut decision = None;
    let mut deferred_log = None;
    let mut native = match load_native(kv) {
        Ok(native) => native,
        Err(error) => {
            let fenced = error == TxnError::Fenced;
            for pending in group { pending.reject(error.clone()); }
            return fenced;
        }
    };
    let mut pending_features = *features;
    let mut pending_subversion = *subversion;
    for item in group {
        let pending = match item {
            Message::Commit(pending) => pending,
            Message::Highwater { key, value, reply } => {
                match validate_highwater(kv, &key, &value, &highwaters) {
                    Ok(end) => {
                        highwaters.insert(key.clone(), end);
                        batch.put(Keyspace::Seq, key, value);
                        counters.push(reply);
                        #[cfg(test)]
                        shared.highwater_puts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    Err(error) => { let _ = reply.send(Err(error)); }
                }
                continue;
            }
            Message::Counter { key, value, features_or, reply } => {
                match validate_counter(kv, &key, &value, features_or, pending_features,
                    pending_subversion, &native, &catalog) {
                    Ok(next) => {
                        batch.put(Keyspace::Seq, key, value);
                        pending_features = next;
                        counters.push(reply);
                    }
                    Err(error) => { let _ = reply.send(Err(error)); }
                }
                continue;
            }
            Message::Fence { .. } | Message::Shutdown => {
                unreachable!("fence actions and shutdown are never grouped")
            }
        };
        let request = &pending.request;
        let next_features = pending_features | request.features_or;
        let next_subversion = pending_subversion.max(request.minimum_subversion);
        let mut next_native = native.clone();
        if super::meta::check_subversion(next_subversion, super::meta::CURRENT_SUBVERSION).is_err()
            || super::meta::check_features(next_features).is_err()
            || next_native.apply(&request.writes).is_err()
            || next_native.validate(next_features, next_subversion).is_err()
            || request.writes.iter().any(|((space, key), value)| {
                // Defense in depth: versioned writes never target unversioned
                // keyspaces, so counters cannot bypass write_counter's bit 8.
                if matches!(space, Keyspace::Meta | Keyspace::Seq | Keyspace::Gc) {
                    return true;
                }
                // Protected task records (Kernel 0x20..=0x2B) need their bit and
                // well-formed bytes; other Kernel keys keep the legacy, ungated path.
                if *space == Keyspace::Kernel {
                    return super::content_check::required_bits_for_kernel_write(
                        key,
                        value.as_deref(),
                        next_features,
                    )
                    .map_or(true, |required| required & !next_features != 0);
                }
                if *space != Keyspace::Catalog {
                    return false;
                }
                if super::content_check::required_bits_for_catalog_key(key)
                    .map_or(true, |required| required & !next_features != 0)
                {
                    return true;
                }
                let Some(value) = value else {
                    return false;
                };
                super::content_check::required_bits_for_catalog_value(key, value)
                    .map_or(true, |required| required & !next_features != 0)
            })
            || request.raw.iter().any(|op| match op {
                RawOp::XactCommitted { .. } => false,
                RawOp::DeleteCounter { key } => !valid_counter_key(key),
                RawOp::PutCounter { key, value } => {
                    if !valid_counter_key(key) || next_features & 256 == 0 {
                        return true;
                    }
                    let id = u64::from_be_bytes(key[1..9].try_into().unwrap());
                    let definition = request
                        .writes
                        .get(&(Keyspace::Catalog, super::catalog_codec::sequence_key(id)));
                    // Transactional counters accompany a new/replaced definition.
                    let Some(Some(definition)) = definition else {
                        return true;
                    };
                    super::catalog_codec::decode_sequence(definition)
                        .and_then(|def| super::catalog_codec::decode_counter(key, value, &def))
                        .is_err()
                }
            })
        {
            let _ = pending.reply.send(Err(TxnError::Misuse));
            continue;
        }
        if let Some(guard) = &pending.guard
            && let Err(error) = guard.check_fence(visible)
        {
            let _ = pending.reply.send(Err(TxnError::Guard(error)));
            continue;
        }
        let ts = if request.writes.is_empty() {
            visible
        } else if let Some(ts) = group_ts.as_mut() {
            let this = *ts;
            *ts = Ts(ts.0 + 1);
            this
        } else {
            match HlcClock::new(hlc.last).next(clock.now_ms()) {
                Ok(ts) => ts,
                Err(error) => {
                    let _ = pending.reply.send(Err(TxnError::Codec(error)));
                    continue;
                }
            }
        };
        if let Some(validate) = &request.validation
            && let Err(error) = validate(log, kv, request.snapshot, ts)
        {
            let _ = pending.reply.send(Err(error));
            continue;
        }
        if !request.writes.is_empty() {
            let keys: Keys = request.writes.keys().cloned().collect::<Vec<_>>().into();
            for ((ks, key), value) in &request.writes {
                let versioned = super::codec::user_key_seek_at(key, ts);
                let tagged = match value {
                    Some(value) => super::mvcc::encode_live(value),
                    None => super::mvcc::encode_tombstone(),
                };
                batch.put(*ks, versioned, tagged);
                if *ks == Keyspace::Catalog {
                    catalog.insert(key.clone(), value.clone());
                }
            }
            // Later requests in this group validate against accepted predecessors.
            // Unpublished entries on Sync failure are harmless: the permanent
            // fence prevents every subsequent group from consulting this log.
            if pending.guard.is_some() {
                deferred_log = Some((ts, keys));
            } else {
                hlc.last = ts;
                log.append(ts, keys);
            }
            max = ts;
        }
        for op in &request.raw {
            match op {
                RawOp::XactCommitted { xid } => batch.put(
                    Keyspace::Seq,
                    super::xact::marker_key(*xid),
                    ts.0.to_be_bytes().to_vec(),
                ),
                RawOp::DeleteCounter { key } => batch.delete(Keyspace::Seq, key.clone()),
                RawOp::PutCounter { key, value } => {
                    batch.put(Keyspace::Seq, key.clone(), value.clone())
                }
            }
        }
        pending_features = next_features;
        pending_subversion = next_subversion;
        native = next_native;
        decision = pending.guard;
        accepted.push((pending.reply, ts));
    }
    if pending_features != *features {
        super::meta::put_features(&mut batch, pending_features)
            .expect("validated pending features");
    }
    if pending_subversion != *subversion {
        batch.put(Keyspace::Meta, b"subversion".to_vec(), pending_subversion.to_be_bytes().to_vec());
    }
    if max != visible {
        batch.put(
            Keyspace::Meta,
            b"last_commit_ts".to_vec(),
            max.0.to_be_bytes().to_vec(),
        );
    }
    if let Some(guard) = decision
        && let Err(error) = guard.decide()
    {
        for (reply, _) in accepted {
            let _ = reply.send(Err(TxnError::Guard(error)));
        }
        return false;
    }
    if let Some((ts, keys)) = deferred_log {
        hlc.last = ts;
        log.append(ts, keys);
    }
    #[cfg(test)]
    if !batch.is_empty() {
        shared.sync_writes.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    if !batch.is_empty()
        && let Err(error) = kv.write(batch, Durability::Sync)
    {
        let error = write_error(&error);
        for (reply, _) in accepted {
            let _ = reply.send(Err(error.clone()));
        }
        for reply in counters { let _ = reply.send(Err(error.clone())); }
        return true;
    }
    *features = pending_features;
    *subversion = pending_subversion;
    // Publish only after Sync succeeds, then trim and acknowledge.
    let trim_bound = publish_visible(shared, max);
    if !accepted.is_empty() {
        log.trim(Some(trim_bound));
    }
    for reply in counters {
        #[cfg(test)]
        {
            let mut gate = shared.counter_gate.lock().unwrap();
            if let Some(replies) = gate.as_mut() { replies.push(reply); continue; }
        }
        let _ = reply.send(Ok(()));
    }
    for (reply, ts) in accepted {
        let _ = reply.send(Ok(ts));
    }
    false
}

#[cfg(test)]
impl Sequencer {
    /// Exercise the exact worker publication path with a predetermined group.
    /// Tests must not submit worker work concurrently with or after helper calls,
    /// because the idle worker HLC is not updated by this test harness.
    pub(crate) fn process_group_for_test(
        &self,
        requests: Vec<CommitRequest>,
        clock: &dyn Clock,
        log: &mut RecentLog,
    ) -> Vec<Result<Ts, TxnError>> {
        let mut receives = Vec::new();
        let group = requests
            .into_iter()
            .map(|request| {
                let (reply, receive) = mpsc::channel();
                receives.push(receive);
                Message::Commit(Pending { request, guard: None, reply })
            })
            .collect();
        let mut hlc = HlcClock::new(self.visible_ts());
        if self.kv.is_fenced() {
            return receives
                .into_iter()
                .map(|_| Err(TxnError::Fenced))
                .collect();
        }
        let mut features = load_features(self.kv.as_ref()).unwrap();
        let mut subversion = load_subversion(self.kv.as_ref()).unwrap();
        process_group(
            group,
            &self.kv,
            clock,
            &self.shared,
            &mut hlc,
            log,
            &mut features,
            &mut subversion,
            &self.config.ts_source,
            &self.served,
            &FenceAccess::default(),
        );
        receives
            .into_iter()
            .map(|receive| receive.recv().unwrap_or(Err(TxnError::Fenced)))
            .collect()
    }
}

#[cfg(test)]
#[path = "commit_guard_tests.rs"]
mod guard_tests;

#[cfg(test)]
#[path = "commit_decision_tests.rs"]
mod decision_tests;

#[cfg(test)]
#[path = "commit_batch_tests.rs"]
mod batch_tests;

#[cfg(test)]
#[path = "commit_cap_tests.rs"]
mod cap_tests;

#[cfg(test)]
#[path = "native_format_tests.rs"]
mod native_format_tests;

#[cfg(test)]
#[path = "commit_tssource_tests.rs"]
mod tssource_tests;

#[cfg(test)]
#[path = "write_fence_commit_tests.rs"]
mod write_fence_tests;

#[cfg(test)]
#[path = "commit_snapshot_tests.rs"]
mod snapshot_tests;
