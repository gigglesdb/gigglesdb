//! Single durable commit sequencer and bounded conflict history.
use super::{
    hlc::{HlcClock, Ts},
    kv::{Durability, Keyspace, Kv, KvRead, WriteBatch},
    txn::TxnError,
};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex, mpsc},
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
#[cfg(test)]
pub(crate) struct ManualClock(std::sync::atomic::AtomicU64);
#[cfg(test)]
impl ManualClock {
    pub(crate) fn new(ms: u64) -> Self {
        Self(std::sync::atomic::AtomicU64::new(ms))
    }
    pub(crate) fn set(&self, ms: u64) {
        self.0.store(ms, std::sync::atomic::Ordering::SeqCst);
    }
}
#[cfg(test)]
impl Clock for ManualClock {
    fn now_ms(&self) -> u64 {
        self.0.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct SequencerConfig {
    /// Maximum additional requests gathered after the first; zero means singleton.
    pub(crate) group_max: usize,
    pub(crate) group_wait: Duration,
    pub(crate) recent_log_bytes: usize,
    pub(crate) write_set_bytes: usize,
}
impl Default for SequencerConfig {
    fn default() -> Self {
        Self {
            group_max: 64,
            group_wait: Duration::from_micros(200),
            recent_log_bytes: 256 * 1024 * 1024,
            write_set_bytes: 256 * 1024 * 1024,
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
    pub(crate) raw: Vec<RawOp>,
    // Hooks inspect the worker-owned log; no snapshot mutex is held during validation.
    pub(crate) validation: Option<Validation>,
}
#[derive(Clone, Debug)]
pub(crate) enum RawOp {
    DeleteCounter { key: Vec<u8> },
    PutCounter { key: Vec<u8>, value: Vec<u8> },
}
pub(crate) fn valid_counter_key(key: &[u8]) -> bool {
    key.len() == 25 && key.first() == Some(&2)
}
struct SnapState {
    visible: Ts,
    active: BTreeMap<Ts, usize>,
}
struct Shared {
    snap: Mutex<SnapState>,
}
pub(crate) struct SnapshotHandle {
    shared: Arc<Shared>,
    ts: Ts,
}
impl SnapshotHandle {
    pub(crate) fn ts(&self) -> Ts {
        self.ts
    }
}
impl Drop for SnapshotHandle {
    fn drop(&mut self) {
        let mut state = self.shared.snap.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(count) = state.active.get_mut(&self.ts) {
            *count -= 1;
            if *count == 0 {
                state.active.remove(&self.ts);
            }
        }
    }
}
struct Pending {
    request: CommitRequest,
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
    Shutdown,
}
pub(crate) struct Sequencer {
    kv: Arc<Kv>,
    config: SequencerConfig,
    shared: Arc<Shared>,
    sender: mpsc::Sender<Message>,
    worker: Mutex<Option<JoinHandle<()>>>,
}
impl Sequencer {
    pub(crate) fn open(
        kv: Arc<Kv>,
        clock: Arc<dyn Clock>,
        config: SequencerConfig,
    ) -> Result<Arc<Self>, TxnError> {
        let features = load_features(kv.as_ref())?;
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
            snap: Mutex::new(SnapState {
                visible: seed,
                active: BTreeMap::new(),
            }),
        });
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
                seed,
                features,
            )
        });
        Ok(Arc::new(Self {
            kv,
            config,
            shared,
            sender,
            worker: Mutex::new(Some(worker)),
        }))
    }
    pub(crate) fn kv(&self) -> &Kv {
        &self.kv
    }
    pub(crate) fn write_set_bytes(&self) -> usize {
        self.config.write_set_bytes
    }
    pub(crate) fn visible_ts(&self) -> Ts {
        self.shared
            .snap
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .visible
    }
    pub(crate) fn oldest_active(&self) -> Option<Ts> {
        self.shared
            .snap
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .active
            .keys()
            .next()
            .copied()
    }
    pub(crate) fn snapshot(&self) -> SnapshotHandle {
        let mut state = self.shared.snap.lock().unwrap_or_else(|p| p.into_inner());
        let ts = state.visible;
        *state.active.entry(ts).or_default() += 1;
        SnapshotHandle {
            shared: self.shared.clone(),
            ts,
        }
    }
    pub(crate) fn submit(&self, request: CommitRequest) -> Result<Ts, TxnError> {
        if self.kv.is_fenced() {
            return Err(TxnError::Fenced);
        }
        let (reply, receive) = mpsc::channel();
        self.sender
            .send(Message::Commit(Pending { request, reply }))
            .map_err(|_| TxnError::Fenced)?;
        receive.recv().unwrap_or(Err(TxnError::Fenced))
    }
    /// A counter reservation is durable independently of any SQL transaction.
    pub(crate) fn write_counter(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        features_or: u64,
    ) -> Result<(), TxnError> {
        if !valid_counter_key(&key) {
            return Err(TxnError::Misuse);
        }
        if self.kv.is_fenced() {
            return Err(TxnError::Fenced);
        }
        let (reply, receive) = mpsc::channel();
        self.sender
            .send(Message::Counter {
                key,
                value,
                features_or,
                reply,
            })
            .map_err(|_| TxnError::Fenced)?;
        receive.recv().unwrap_or(Err(TxnError::Fenced))
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
impl Drop for Sequencer {
    fn drop(&mut self) {
        let _ = self.sender.send(Message::Shutdown);
        if let Some(worker) = self.worker.lock().unwrap_or_else(|p| p.into_inner()).take() {
            let _ = worker.join();
        }
    }
}

fn run(
    receiver: mpsc::Receiver<Message>,
    kv: Arc<Kv>,
    clock: Arc<dyn Clock>,
    config: SequencerConfig,
    shared: Arc<Shared>,
    seed: Ts,
    mut features: u64,
) {
    let mut hlc = HlcClock::new(seed);
    let mut log = RecentLog::new(config.recent_log_bytes);
    let mut fenced = false;
    let mut deferred = None;
    while let Some(message) = deferred.take().or_else(|| receiver.recv().ok()) {
        let first = match message {
            Message::Commit(first) => first,
            Message::Counter {
                key,
                value,
                features_or,
                reply,
            } => {
                let result = if fenced || kv.is_fenced() {
                    Err(TxnError::Fenced)
                } else {
                    process_counter(&kv, key, value, features_or, &mut features)
                };
                fenced |= matches!(result, Err(TxnError::CommitAmbiguous | TxnError::Fenced));
                let _ = reply.send(result);
                continue;
            }
            Message::Shutdown => break,
        };
        let mut group = vec![first];
        let deadline = Instant::now()
            .checked_add(config.group_wait)
            .unwrap_or_else(Instant::now);
        let mut shutdown = false;
        while group.len() - 1 < config.group_max && Instant::now() < deadline {
            match receiver.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
                Ok(Message::Commit(p)) => group.push(p),
                Ok(counter @ Message::Counter { .. }) => {
                    deferred = Some(counter);
                    break;
                }
                Ok(Message::Shutdown) => {
                    shutdown = true;
                    break;
                }
                Err(_) => break,
            }
        }
        if shutdown || fenced || kv.is_fenced() {
            for p in group {
                let _ = p.reply.send(Err(TxnError::Fenced));
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
        );
    }
    for message in receiver.try_iter() {
        match message {
            Message::Commit(p) => {
                let _ = p.reply.send(Err(TxnError::Fenced));
            }
            Message::Counter { reply, .. } => {
                let _ = reply.send(Err(TxnError::Fenced));
            }
            Message::Shutdown => {}
        }
    }
}
fn process_counter(
    kv: &Kv,
    key: Vec<u8>,
    value: Vec<u8>,
    features_or: u64,
    features: &mut u64,
) -> Result<(), TxnError> {
    let next = *features | features_or | 256;
    if !valid_counter_key(&key) || super::meta::check_features(next).is_err() {
        return Err(TxnError::Misuse);
    }
    let seq_id = u64::from_be_bytes(key[1..9].try_into().expect("validated counter key"));
    let incarnation = u128::from_be_bytes(key[9..25].try_into().expect("validated counter key"));
    // This worker serializes commit groups and counter messages, so no DROP
    // can interleave between this latest-physical-version check and the write.
    // A latest tombstone hides every older live definition.
    let definition = super::mvcc::latest_version(
        kv,
        Keyspace::Catalog,
        &super::catalog_codec::sequence_key(seq_id),
    )?
    .and_then(|(_, value)| value)
    .ok_or(TxnError::SequenceGone)?;
    let definition = super::catalog_codec::decode_sequence(&definition)
        .map_err(|error| TxnError::Kv(super::kv::KvError::Corrupt(error.to_string())))?;
    if definition.incarnation != incarnation {
        return Err(TxnError::SequenceGone);
    }
    super::catalog_codec::decode_counter(&key, &value, &definition)
        .map_err(|_| TxnError::Misuse)?;
    let mut batch = WriteBatch::default();
    batch.put(Keyspace::Seq, key, value);
    if next != *features {
        super::meta::put_features(&mut batch, next).map_err(|_| TxnError::Misuse)?;
    }
    kv.write(batch, Durability::Sync)
        .map_err(|_| TxnError::CommitAmbiguous)?;
    *features = next;
    Ok(())
}

// Returns whether an ambiguous backend result requires fencing subsequent groups.
fn process_group(
    group: Vec<Pending>,
    kv: &Kv,
    clock: &dyn Clock,
    shared: &Shared,
    hlc: &mut HlcClock,
    log: &mut RecentLog,
    features: &mut u64,
) -> bool {
    let visible = shared
        .snap
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .visible;
    let mut max = visible;
    let mut batch = WriteBatch::default();
    let mut accepted = Vec::new();
    let mut pending_features = *features;
    for pending in group {
        let request = &pending.request;
        let next_features = pending_features | request.features_or;
        if super::meta::check_features(next_features).is_err()
            || request.writes.iter().any(|((space, key), value)| {
                // Defense in depth: versioned writes never target unversioned
                // keyspaces, so counters cannot bypass write_counter's bit 8.
                if matches!(space, Keyspace::Meta | Keyspace::Seq | Keyspace::Gc) {
                    return true;
                }
                if *space != Keyspace::Catalog {
                    return false;
                }
                let Some(value) = value else {
                    return false;
                };
                super::content_check::required_bits_for_catalog_value(key, value)
                    .map_or(true, |required| required & !next_features != 0)
            })
            || request.raw.iter().any(|op| match op {
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
        let ts = if request.writes.is_empty() {
            visible
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
            hlc.last = ts;
            let keys: Keys = request.writes.keys().cloned().collect::<Vec<_>>().into();
            for ((ks, key), value) in &request.writes {
                let versioned = super::codec::user_key_seek_at(key, ts);
                let tagged = match value {
                    Some(value) => super::mvcc::encode_live(value),
                    None => super::mvcc::encode_tombstone(),
                };
                batch.put(*ks, versioned, tagged);
            }
            // Later requests in this group validate against accepted predecessors.
            // Unpublished entries on Sync failure are harmless: the permanent
            // fence prevents every subsequent group from consulting this log.
            log.append(ts, keys);
            max = ts;
        }
        for op in &request.raw {
            match op {
                RawOp::DeleteCounter { key } => batch.delete(Keyspace::Seq, key.clone()),
                RawOp::PutCounter { key, value } => {
                    batch.put(Keyspace::Seq, key.clone(), value.clone())
                }
            }
        }
        pending_features = next_features;
        accepted.push((pending.reply, ts));
    }
    if pending_features != *features {
        super::meta::put_features(&mut batch, pending_features)
            .expect("validated pending features");
    }
    if !batch.is_empty() {
        if max != visible {
            batch.put(
                Keyspace::Meta,
                b"last_commit_ts".to_vec(),
                max.0.to_be_bytes().to_vec(),
            );
        }
        if kv.write(batch, Durability::Sync).is_err() {
            for (reply, _) in accepted {
                let _ = reply.send(Err(TxnError::CommitAmbiguous));
            }
            return true;
        }
    }
    *features = pending_features;
    // Publish only after Sync succeeds; capture retention state in the same
    // short critical section, then trim and acknowledge without the mutex.
    let (visible, oldest) = {
        let mut snap = shared.snap.lock().unwrap_or_else(|p| p.into_inner());
        snap.visible = max;
        (snap.visible, snap.active.keys().next().copied())
    };
    // A live snapshot S needs only writes > S. Visibility is monotonic, and
    // snapshots registered after this capture are >= captured visible. Normal
    // trim removes only <= min(visible, oldest), preserving every live reader's
    // needed history; forced budget eviction instead records the lost horizon.
    log.trim(Some(visible.min(oldest.unwrap_or(visible))));
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
                Pending { request, reply }
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
        process_group(
            group,
            &self.kv,
            clock,
            &self.shared,
            &mut hlc,
            log,
            &mut features,
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
