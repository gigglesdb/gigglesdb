//! In-process fake cluster for Track C tests (GD-0075 R5-41..46; design.md sections 3 and 4a):
//! one shared raw store in the Sequencer's physical layout, ranges with epochs, the timestamp
//! oracle with the O3/O4 rules, Seq leases, and one `LockManager` per range. Every `FakeClient`
//! is a gateway with its own route cache, which goes stale on `split`.
#![allow(dead_code)]
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    ops::{Bound, Range},
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicU32, AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use crate::storage::{
    engine::{
        codec,
        hlc::Ts,
        kv::{Direction, Durability, Keyspace, Kv, KvPair, WriteBatch},
        kv_mem::MemKv,
        locks::{LockConfig, LockError, LockKey, LockManager, LockMode, TxnId},
        mvcc,
    },
    range_api::{
        ClusterTxnId, Epoch, HomeOp, HomeReply, LeaseKind, LockGrant, LockRequest, LockTarget,
        Outcome, RangeClient, RangeDescriptor, RangeError, RangeId, RangeLockMode, RangeSpace,
        RangeWrite, ReadDirection, ReadPage, ReadRequest, ReplicaDesc, ReplicaRole, SYSTEM_RANGE,
        Timestamp, TsOracle, first_user_key, range_key,
    },
};

/// First timestamp the oracle hands out.
pub(crate) const ORACLE_START: u64 = 1 << 16;

pub(crate) fn keyspace(space: RangeSpace) -> Keyspace {
    match space {
        RangeSpace::Catalog => Keyspace::Catalog,
        RangeSpace::Kernel => Keyspace::Kernel,
        RangeSpace::Seq => Keyspace::Seq,
        RangeSpace::Gc => Keyspace::Gc,
        RangeSpace::Rows => Keyspace::Rows,
        RangeSpace::Index => Keyspace::Index,
    }
}

/// Seq and Gc are stored raw; every other range keyspace is MVCC-versioned.
pub(crate) fn versioned(space: RangeSpace) -> bool {
    !matches!(space, RangeSpace::Seq | RangeSpace::Gc)
}

/// Strictly increasing counter; `failures` makes the next calls return Unavailable.
pub(crate) struct FakeOracle(AtomicU64, AtomicU32);

impl TsOracle for FakeOracle {
    fn next(&self, n: u32) -> Result<Timestamp, RangeError> {
        if n == 0 {
            return Err(RangeError::Corrupt("oracle: n must be at least 1".into()));
        }
        let mut left = self.1.load(Ordering::SeqCst);
        while left > 0 {
            match self
                .1
                .compare_exchange(left, left - 1, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => break,
                Err(now) => left = now,
            }
        }
        if left > 0 {
            return Err(RangeError::Unavailable("fake: oracle unavailable".into()));
        }
        Ok(Timestamp(self.0.fetch_add(u64::from(n), Ordering::SeqCst)))
    }
}

struct FakeRange {
    desc: RangeDescriptor,
    /// O3: highest read timestamp served here.
    max_served: Timestamp,
    locks: Arc<LockManager>,
    commits: u64,
    heartbeats: u64,
    releases: u64,
}

impl FakeRange {
    fn new(desc: RangeDescriptor, max_served: Timestamp) -> Self {
        Self {
            desc,
            max_served,
            locks: Arc::new(LockManager::new(LockConfig::default())),
            commits: 0,
            heartbeats: 0,
            releases: 0,
        }
    }

    fn stale(&self) -> RangeError {
        RangeError::StaleEpoch {
            descriptor: self.desc.clone(),
        }
    }
}

#[derive(Default)]
struct State {
    ranges: BTreeMap<RangeId, FakeRange>,
    next_id: u64,
    failures: HashMap<RangeId, VecDeque<RangeError>>,
    markers: HashMap<u64, Timestamp>,
    leases: HashMap<(u8, u64), u64>,
}

impl State {
    /// Injected failure first, then the range id, then the epoch when one is given.
    fn range(&mut self, id: RangeId, epoch: Option<Epoch>) -> Result<&mut FakeRange, RangeError> {
        if let Some(error) = self.failures.get_mut(&id).and_then(VecDeque::pop_front) {
            return Err(error);
        }
        let range = self
            .ranges
            .get_mut(&id)
            .ok_or_else(|| RangeError::Unavailable(format!("fake: unknown range {id:?}")))?;
        if epoch.is_some_and(|epoch| epoch != range.desc.epoch) {
            return Err(range.stale());
        }
        Ok(range)
    }
}

pub(crate) struct FakeCluster {
    kv: Arc<Kv>,
    oracle: FakeOracle,
    state: Mutex<State>,
    /// Microseconds every read waits, outside the cluster mutex.
    read_delay: AtomicU64,
}

impl FakeCluster {
    /// Range 1 (SYSTEM_RANGE) = [empty, first_user_key()); user ranges [first_user_key(), s1),
    /// [s1, s2), ..., [sn, None). Splits are range keys, sorted, above first_user_key().
    pub(crate) fn new(user_splits: &[Vec<u8>]) -> Arc<Self> {
        let mut starts = vec![Vec::new(), first_user_key()];
        starts.extend(user_splits.iter().cloned());
        assert!(
            starts.windows(2).all(|pair| pair[0] < pair[1]),
            "user splits must be sorted, unique and above first_user_key()"
        );
        let mut state = State {
            next_id: starts.len() as u64 + 1,
            ..State::default()
        };
        for (index, start) in starts.iter().enumerate() {
            let desc = RangeDescriptor {
                id: RangeId(index as u64 + 1),
                epoch: Epoch(1),
                start: start.clone(),
                end: starts.get(index + 1).cloned(),
                replicas: vec![ReplicaDesc {
                    node: 1,
                    role: ReplicaRole::Voter,
                }],
            };
            state
                .ranges
                .insert(desc.id, FakeRange::new(desc, Timestamp(0)));
        }
        debug_assert!(state.ranges.contains_key(&SYSTEM_RANGE));
        Arc::new(Self {
            kv: Arc::new(Kv::new(Arc::new(MemKv::new()))),
            oracle: FakeOracle(AtomicU64::new(ORACLE_START), AtomicU32::new(0)),
            state: Mutex::new(state),
            read_delay: AtomicU64::new(0),
        })
    }

    /// A gateway with its own (possibly stale) route cache.
    pub(crate) fn client(self: &Arc<Self>) -> Arc<FakeClient> {
        Arc::new(FakeClient {
            cluster: Arc::clone(self),
            cache: Mutex::new(Vec::new()),
            stale_epochs: AtomicU64::new(0),
            reads: AtomicU64::new(0),
            in_flight: AtomicU64::new(0),
            max_in_flight: AtomicU64::new(0),
        })
    }

    /// Every later read takes at least `delay` (a leader round trip); reads overlap.
    pub(crate) fn set_read_delay(&self, delay: Duration) {
        self.read_delay
            .store(delay.as_micros() as u64, Ordering::SeqCst);
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().expect("fake cluster mutex poisoned")
    }

    /// Splits the range containing range key `at`; the parent keeps its id and the left half,
    /// the new id the right half, both at the parent's epoch + 1 and max_served (O8).
    pub(crate) fn split(&self, at: &[u8]) -> RangeId {
        let mut state = self.state();
        let parent = state
            .ranges
            .values()
            .find(|range| range.desc.contains(at))
            .map(|range| range.desc.id)
            .expect("ranges cover every key");
        let id = RangeId(state.next_id);
        state.next_id += 1;
        let parent = state.ranges.get_mut(&parent).expect("parent range");
        assert!(parent.desc.start.as_slice() < at, "split at a range start");
        let epoch = Epoch(parent.desc.epoch.0 + 1);
        let right = RangeDescriptor {
            id,
            epoch,
            start: at.to_vec(),
            end: parent.desc.end.replace(at.to_vec()),
            replicas: parent.desc.replicas.clone(),
        };
        parent.desc.epoch = epoch;
        let max_served = parent.max_served;
        state.ranges.insert(id, FakeRange::new(right, max_served));
        id
    }

    /// Current descriptors in key order.
    pub(crate) fn descriptors(&self) -> Vec<RangeDescriptor> {
        let mut out: Vec<_> = self
            .state()
            .ranges
            .values()
            .map(|range| range.desc.clone())
            .collect();
        out.sort_by(|a, b| a.start.cmp(&b.start));
        out
    }

    /// The next `n` oracle calls return Unavailable.
    pub(crate) fn fail_oracle(&self, n: u32) {
        self.oracle.1.store(n, Ordering::SeqCst);
    }

    pub(crate) fn kv(&self) -> Arc<Kv> {
        Arc::clone(&self.kv)
    }

    /// Queues `error`; the next call naming `range` returns it before any other check.
    pub(crate) fn fail_next(&self, range: RangeId, error: RangeError) {
        self.state()
            .failures
            .entry(range)
            .or_default()
            .push_back(error);
    }

    fn counter(&self, range: RangeId, read: impl Fn(&FakeRange) -> u64) -> u64 {
        self.state().ranges.get(&range).map_or(0, read)
    }
    /// Single-range commits applied at `range`.
    pub(crate) fn commits(&self, range: RangeId) -> u64 {
        self.counter(range, |range| range.commits)
    }
    pub(crate) fn heartbeats(&self, range: RangeId) -> u64 {
        self.counter(range, |range| range.heartbeats)
    }
    pub(crate) fn releases(&self, range: RangeId) -> u64 {
        self.counter(range, |range| range.releases)
    }
    pub(crate) fn marker(&self, xid: u64) -> Option<Timestamp> {
        self.state().markers.get(&xid).copied()
    }

    fn current(&self, range_key: &[u8]) -> Result<RangeDescriptor, RangeError> {
        self.state()
            .ranges
            .values()
            .find(|range| range.desc.contains(range_key))
            .map(|range| range.desc.clone())
            .ok_or_else(|| RangeError::Unavailable("fake: no range for key".into()))
    }

    fn read(&self, req: ReadRequest) -> Result<ReadPage, RangeError> {
        let mut state = self.state();
        let range = state.range(req.range, Some(req.epoch))?;
        if !position_inside(&range.desc, req.space, &req.start, &req.end, req.dir) {
            return Err(range.stale());
        }
        // O3/O6: one mutex, so no commit is in flight while this read runs.
        range.max_served = range.max_served.max(req.ts);
        let desc = range.desc.clone();
        let rows = self.visible(&desc, &req).map_err(corrupt)?;
        let versioned = versioned(req.space);
        let mut pairs = Vec::new();
        let mut bytes = 0;
        let mut rows = rows.into_iter();
        for (key, value) in rows.by_ref() {
            bytes += key.len() + value.len();
            pairs.push((key, value));
            if bytes >= req.page_bytes {
                break;
            }
        }
        let resume = match (rows.next(), pairs.last()) {
            (Some(_), Some((last, _))) if versioned => {
                let (user, _) =
                    codec::split_versioned(last).map_err(|e| corrupt(format!("{e:?}")))?;
                Some(match req.dir {
                    ReadDirection::Forward => Bound::Included(codec::user_key_upper_bound(user)),
                    ReadDirection::Reverse => {
                        Bound::Excluded(codec::user_key_seek_at(user, Ts(u64::MAX)))
                    }
                })
            }
            (Some(_), Some((last, _))) => Some(Bound::Excluded(last.clone())),
            _ => None,
        };
        Ok(ReadPage {
            pairs,
            resume,
            range_end: match req.dir {
                ReadDirection::Forward => desc.end,
                ReadDirection::Reverse => Some(desc.start),
            },
        })
    }

    /// Every pair `req` sees inside `desc`, in `req.dir` order.
    fn visible(&self, desc: &RangeDescriptor, req: &ReadRequest) -> Result<Vec<KvPair>, String> {
        let Some((low, high)) = descriptor_bounds(desc, req.space) else {
            return Ok(Vec::new());
        };
        let low = max_lower(low, req.start.clone());
        let high = min_upper(high, req.end.clone());
        let raw = self
            .kv
            .range(
                keyspace(req.space),
                low.as_ref().map(Vec::as_slice),
                high.as_ref().map(Vec::as_slice),
                Direction::Forward,
            )
            .map_err(|e| e.to_string())?;
        let mut rows = Vec::new();
        let mut last_user: Option<Vec<u8>> = None;
        for item in raw {
            let (key, value) = item.map_err(|e| e.to_string())?;
            if versioned(req.space) {
                // Forward raw order is newest first within a user key.
                let (user, ts) = codec::split_versioned(&key).map_err(|e| format!("{e:?}"))?;
                if last_user.as_deref() == Some(user) || ts.0 > req.ts.0 {
                    continue;
                }
                last_user = Some(user.to_vec());
            }
            rows.push((key, value));
        }
        if req.dir == ReadDirection::Reverse {
            rows.reverse();
        }
        Ok(rows)
    }

    fn lock(&self, req: LockRequest) -> Result<LockGrant, RangeError> {
        let locks = {
            let mut state = self.state();
            let range = state.range(req.range, Some(req.epoch))?;
            let inside = match &req.target {
                LockTarget::Key { space, key } => range.desc.contains(&range_key(*space, key)),
                LockTarget::TableSentinel { table } => table_overlaps(&range.desc, *table),
                LockTarget::Interval { .. } => {
                    return Err(RangeError::Corrupt("fake: no gap locks".into()));
                }
            };
            if !inside {
                return Err(range.stale());
            }
            Arc::clone(&range.locks)
        };
        let (key, latest_of) = match req.target {
            LockTarget::Key { space, key } => {
                let Some(id) = key.get(..8) else {
                    return Err(RangeError::Corrupt("fake: short lock key".into()));
                };
                let id = u64::from_be_bytes(id.try_into().expect("eight bytes"));
                let rest = key[8..].to_vec();
                let lock_key = match space {
                    RangeSpace::Rows => LockKey::Row {
                        table: id,
                        pk: rest,
                    },
                    RangeSpace::Index => LockKey::Unique {
                        index: id,
                        tuple: rest,
                    },
                    other => {
                        return Err(RangeError::Corrupt(format!(
                            "fake: no key locks in {other:?}"
                        )));
                    }
                };
                (lock_key, Some((space, key)))
            }
            LockTarget::TableSentinel { table } => (
                LockKey::Row {
                    table,
                    pk: Vec::new(),
                },
                None,
            ),
            LockTarget::Interval { .. } => unreachable!("refused above"),
        };
        let mode = match req.mode {
            RangeLockMode::Shared => LockMode::Shared,
            RangeLockMode::Exclusive => LockMode::Exclusive,
        };
        let deadline = req
            .timeout_ms
            .map(|ms| Instant::now() + Duration::from_millis(ms));
        // No cluster mutex held while waiting.
        locks
            .acquire(TxnId(req.txn.0), key, mode, deadline, &|| false)
            .map_err(|e| match e {
                LockError::Deadlock => RangeError::Deadlock,
                LockError::Timeout => RangeError::LockTimeout,
                LockError::Cancelled => RangeError::Cancelled,
                other => RangeError::Aborted(format!("fake: lock {other:?}")),
            })?;
        let latest = match latest_of {
            Some((space, key)) => mvcc::latest_version(&*self.kv, keyspace(space), &key)
                .map_err(|e| RangeError::Corrupt(format!("{e:?}")))?
                .map(|(ts, value)| (Timestamp(ts.0), value)),
            None => None,
        };
        Ok(LockGrant {
            latest,
            generation: 1,
        })
    }

    fn release(&self, range: RangeId, txn: ClusterTxnId) -> Result<(), RangeError> {
        let locks = {
            let mut state = self.state();
            let range = state.range(range, None)?;
            range.releases += 1;
            Arc::clone(&range.locks)
        };
        locks.release_all(TxnId(txn.0));
        Ok(())
    }

    fn commit(
        &self,
        range: RangeId,
        epoch: Epoch,
        writes: Vec<RangeWrite>,
        marker_xid: Option<u64>,
    ) -> Result<Timestamp, RangeError> {
        let mut state = self.state();
        let range = state.range(range, Some(epoch))?;
        if writes
            .iter()
            .any(|write| !range.desc.contains(&range_key(write.space, &write.key)))
        {
            return Err(range.stale());
        }
        // O4: refetch until above every read timestamp served here.
        let commit = loop {
            let ts = self.oracle.next(1)?;
            if ts > range.max_served {
                break ts;
            }
        };
        let mut batch = WriteBatch::default();
        for write in writes {
            let ks = keyspace(write.space);
            match (versioned(write.space), write.value) {
                (true, value) => batch.put(
                    ks,
                    codec::user_key_seek_at(&write.key, Ts(commit.0)),
                    value.map_or_else(mvcc::encode_tombstone, |v| mvcc::encode_live(&v)),
                ),
                (false, Some(value)) => batch.put(ks, write.key, value),
                (false, None) => batch.delete(ks, write.key),
            }
        }
        self.kv
            .write(batch, Durability::Sync)
            .map_err(|e| RangeError::Unknown(e.to_string()))?;
        range.commits += 1;
        if let Some(xid) = marker_xid {
            state.markers.insert(xid, commit);
        }
        Ok(commit)
    }

    fn lease(&self, kind: LeaseKind, id: u64, count: u64) -> Result<Range<u64>, RangeError> {
        let mut state = self.state();
        let high = state.leases.entry((kind as u8, id)).or_insert(1);
        let start = *high;
        *high = start
            .checked_add(count)
            .ok_or_else(|| RangeError::Unavailable("fake: lease exhausted".into()))?;
        Ok(start..*high)
    }
}

fn corrupt(e: impl std::fmt::Display) -> RangeError {
    RangeError::Corrupt(e.to_string())
}

/// Whether the bound a read starts from (Forward: start, Reverse: end) lies in `desc`. The far
/// bound may lie outside; the leader clips it.
fn position_inside(
    desc: &RangeDescriptor,
    space: RangeSpace,
    start: &Bound<Vec<u8>>,
    end: &Bound<Vec<u8>>,
    dir: ReadDirection,
) -> bool {
    let space_end = [space.tag() + 1];
    match (dir, start, end) {
        (ReadDirection::Forward, Bound::Unbounded, _) => desc.contains(&range_key(space, &[])),
        (ReadDirection::Forward, Bound::Included(key) | Bound::Excluded(key), _)
        | (ReadDirection::Reverse, _, Bound::Included(key)) => {
            desc.contains(&range_key(space, key))
        }
        (ReadDirection::Reverse, _, Bound::Excluded(key)) => {
            let key = range_key(space, key);
            desc.start < key && desc.end.as_ref().is_none_or(|end| key <= *end)
        }
        (ReadDirection::Reverse, _, Bound::Unbounded) => {
            desc.start.as_slice() < space_end.as_slice()
                && desc
                    .end
                    .as_ref()
                    .is_none_or(|end| end.as_slice() >= space_end.as_slice())
        }
    }
}

/// Lower and upper bound of raw keys.
type RawBounds = (Bound<Vec<u8>>, Bound<Vec<u8>>);

/// `desc`'s bounds as raw keys of `space`; None when it holds no key of `space`.
fn descriptor_bounds(desc: &RangeDescriptor, space: RangeSpace) -> Option<RawBounds> {
    let tag = space.tag();
    let low = match desc.start.split_first() {
        None => Bound::Unbounded,
        Some((&first, _)) if first < tag => Bound::Unbounded,
        Some((&first, rest)) if first == tag => Bound::Included(rest.to_vec()),
        Some(_) => return None,
    };
    let high = match desc.end.as_deref().map(<[u8]>::split_first) {
        None => Bound::Unbounded,
        Some(None) => return None,
        Some(Some((&first, _))) if first > tag => Bound::Unbounded,
        Some(Some((&first, rest))) if first == tag => Bound::Excluded(rest.to_vec()),
        Some(Some(_)) => return None,
    };
    Some((low, high))
}

fn max_lower(a: Bound<Vec<u8>>, b: Bound<Vec<u8>>) -> Bound<Vec<u8>> {
    match (&a, &b) {
        (Bound::Unbounded, _) => b,
        (_, Bound::Unbounded) => a,
        (Bound::Included(x) | Bound::Excluded(x), Bound::Included(y) | Bound::Excluded(y)) => {
            if x > y || (x == y && matches!(a, Bound::Excluded(_))) {
                a
            } else {
                b
            }
        }
    }
}

fn min_upper(a: Bound<Vec<u8>>, b: Bound<Vec<u8>>) -> Bound<Vec<u8>> {
    match (&a, &b) {
        (Bound::Unbounded, _) => b,
        (_, Bound::Unbounded) => a,
        (Bound::Included(x) | Bound::Excluded(x), Bound::Included(y) | Bound::Excluded(y)) => {
            if x < y || (x == y && matches!(a, Bound::Excluded(_))) {
                a
            } else {
                b
            }
        }
    }
}

/// Whether `desc` intersects the row span of `table` (section 3.1).
fn table_overlaps(desc: &RangeDescriptor, table: u64) -> bool {
    let low = range_key(RangeSpace::Rows, &table.to_be_bytes());
    let high = match table.checked_add(1) {
        Some(next) => range_key(RangeSpace::Rows, &next.to_be_bytes()),
        None => vec![RangeSpace::Rows.tag() + 1],
    };
    desc.start < high && desc.end.as_ref().is_none_or(|end| *end > low)
}

fn overlaps(a: &RangeDescriptor, b: &RangeDescriptor) -> bool {
    a.end.as_ref().is_none_or(|end| b.start < *end)
        && b.end.as_ref().is_none_or(|end| a.start < *end)
}

/// A gateway over the fake cluster with its own route cache.
pub(crate) struct FakeClient {
    cluster: Arc<FakeCluster>,
    cache: Mutex<Vec<RangeDescriptor>>,
    stale_epochs: AtomicU64,
    reads: AtomicU64,
    in_flight: AtomicU64,
    max_in_flight: AtomicU64,
}

impl FakeClient {
    /// StaleEpoch replies this gateway has received from reads.
    pub(crate) fn stale_epochs(&self) -> u64 {
        self.stale_epochs.load(Ordering::SeqCst)
    }

    /// Reads this gateway has started (counted on entry, before the delay).
    pub(crate) fn reads(&self) -> u64 {
        self.reads.load(Ordering::SeqCst)
    }

    /// Reads of this gateway running now.
    pub(crate) fn in_flight(&self) -> u64 {
        self.in_flight.load(Ordering::SeqCst)
    }

    /// Most reads of this gateway that have run at once.
    pub(crate) fn max_in_flight(&self) -> u64 {
        self.max_in_flight.load(Ordering::SeqCst)
    }

    fn cache(&self) -> MutexGuard<'_, Vec<RangeDescriptor>> {
        self.cache.lock().expect("route cache mutex poisoned")
    }
}

impl RangeClient for FakeClient {
    fn descriptor_for(&self, range_key: &[u8]) -> Result<RangeDescriptor, RangeError> {
        let mut cache = self.cache();
        if let Some(desc) = cache.iter().find(|desc| desc.contains(range_key)) {
            return Ok(desc.clone());
        }
        let desc = self.cluster.current(range_key)?;
        cache.retain(|cached| !overlaps(cached, &desc));
        cache.push(desc.clone());
        Ok(desc)
    }

    fn invalidate(&self, stale: &RangeDescriptor) {
        self.cache()
            .retain(|cached| (cached.id, cached.epoch) != (stale.id, stale.epoch));
    }

    fn read(&self, req: ReadRequest) -> Result<ReadPage, RangeError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        let running = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_in_flight.fetch_max(running, Ordering::SeqCst);
        let delay = self.cluster.read_delay.load(Ordering::SeqCst);
        if delay > 0 {
            thread::sleep(Duration::from_micros(delay));
        }
        let result = self.cluster.read(req);
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        if matches!(result, Err(RangeError::StaleEpoch { .. })) {
            self.stale_epochs.fetch_add(1, Ordering::SeqCst);
        }
        result
    }

    fn lock(&self, req: LockRequest) -> Result<LockGrant, RangeError> {
        self.cluster.lock(req)
    }

    fn blockers(&self, range: RangeId, _: ClusterTxnId) -> Result<Vec<ClusterTxnId>, RangeError> {
        self.cluster.state().range(range, None)?;
        Ok(Vec::new())
    }

    fn cancel_wait(&self, range: RangeId, _: ClusterTxnId) -> Result<(), RangeError> {
        self.cluster.state().range(range, None)?;
        Ok(())
    }

    fn release(&self, range: RangeId, txn: ClusterTxnId) -> Result<(), RangeError> {
        self.cluster.release(range, txn)
    }

    fn heartbeat(&self, range: RangeId, _: ClusterTxnId) -> Result<(), RangeError> {
        self.cluster.state().range(range, None)?.heartbeats += 1;
        Ok(())
    }

    fn commit(
        &self,
        range: RangeId,
        epoch: Epoch,
        _: ClusterTxnId,
        writes: Vec<RangeWrite>,
        marker_xid: Option<u64>,
        _: Option<u64>,
    ) -> Result<Timestamp, RangeError> {
        self.cluster.commit(range, epoch, writes, marker_xid)
    }

    fn prepare(
        &self,
        range: RangeId,
        _: Epoch,
        _: ClusterTxnId,
        _: RangeId,
        _: Vec<RangeWrite>,
        _: Option<u64>,
    ) -> Result<Timestamp, RangeError> {
        self.cluster.state().range(range, None)?;
        Err(track_d())
    }

    fn finalize(&self, range: RangeId, _: ClusterTxnId, _: Outcome) -> Result<(), RangeError> {
        self.cluster.state().range(range, None)?;
        Err(track_d())
    }

    fn home(&self, range: RangeId, _: Epoch, _: HomeOp) -> Result<HomeReply, RangeError> {
        self.cluster.state().range(range, None)?;
        Err(track_d())
    }

    fn oracle(&self) -> &dyn TsOracle {
        &self.cluster.oracle
    }

    fn lease(&self, kind: LeaseKind, id: u64, count: u64) -> Result<Range<u64>, RangeError> {
        self.cluster.lease(kind, id, count)
    }
}

fn track_d() -> RangeError {
    RangeError::Unavailable("fake: Track D".into())
}
