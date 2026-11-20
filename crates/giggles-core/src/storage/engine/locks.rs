//! In-memory transaction locks with shard-local immediate grants and global queueing.
//!
//! Each queued request has its own condition variable and checks its terminal result
//! under the mutex, so completion cannot be missed or bypass FIFO. Only completed
//! or cancelled requests are woken, except when a grant to a transaction with another
//! pending request can close a cycle: its requests and the granted key's queue and
//! overlapping queues are woken too. A new cycle includes either a newly queued
//! request (checked at queue time) or such a grant, so no cycle notification is lost.
//! Optional deferred detection waits until `deadlock_timeout`; callbacks still run
//! outside the mutex and are polled at `deadlock_recheck`.
//!
//! Interval (gap) locks: `LockKey::Interval` is a Shared lock over a byte range of
//! one `LockSpace`. It conflicts with other transactions' Exclusive point locks
//! whose byte form lies inside it, in either order. Two indices keep that cheap:
//! the live interval keys per space, and the ordered byte forms of point keys that
//! have an Exclusive holder or a queued Exclusive request. A point request checks
//! intervals only when its space has one (an O(1) emptiness check otherwise).
//! FIFO is per key: a request on one key never waits behind queued requests on
//! another key, so a stream of overlapping intervals can delay a queued writer.

// The same suites intentionally instantiate managers with three shard counts.
#![cfg_attr(test, allow(clippy::duplicate_mod))]

use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::Bound;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::hash::{BuildHasher, BuildHasherDefault, Hash, Hasher};
use std::collections::hash_map::RandomState;
use std::sync::atomic::{AtomicBool, AtomicU64};
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// Production shard count; explicit 1/2/64 suites exercise the same protocol.
pub(crate) const DEFAULT_LOCK_SHARDS: usize = 64;

/// `Unique { index: CATALOG_NAME_INDEX, tuple: name }` in `Exclusive` mode
/// serializes concurrent DDL on the same relation name.
pub(crate) const CATALOG_NAME_INDEX: u64 = u64::MAX;

/// Reserved table lock serializing kernel image read/modify/write transactions.
pub(crate) const KERNEL_LOCK_TABLE: u64 = u64::MAX - 1;

/// Reserved row-lock table for the GD-0061 task authority hold: `Row { table:
/// TASK_AUTHORITY_LOCK_TABLE, pk: kernel_key }` in `Exclusive`. `MAX - 2` is the
/// roles lock (`roles_engine.rs`).
pub(crate) const TASK_AUTHORITY_LOCK_TABLE: u64 = u64::MAX - 3;

/// Reserved table lock serializing GD-0061 task catalog writers (CREATE TASK,
/// REVOKE TASK, task grants, purge): `Table(TASK_CATALOG_LOCK_TABLE)` in
/// `AccessExclusive`, taken after the table and the shared role lock.
pub(crate) const TASK_CATALOG_LOCK_TABLE: u64 = u64::MAX - 4;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct TxnId(pub u64);

/// The byte space of point keys: `Row { table, pk }` is `table BE8 || pk`,
/// `Unique { index, tuple }` is `index BE8 || tuple`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum LockSpace {
    Row,
    Unique,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum LockKey {
    Table(u64),
    Row {
        table: u64,
        pk: Vec<u8>,
    },
    /// FK key-share / FOR UPDATE; independent of row and interval locks.
    RowKey {
        table: u64,
        pk: Vec<u8>,
    },
    Unique {
        index: u64,
        tuple: Vec<u8>,
    },
    /// Shared-only lock over the point keys of `space` whose byte form lies
    /// within the bounds. The per-table writer sentinel `Row { pk: [] }` and
    /// table keys are never inside an interval.
    Interval {
        space: LockSpace,
        start: Bound<Vec<u8>>,
        end: Bound<Vec<u8>>,
    },
}

impl LockKey {
    /// A point key's space, id and tail; `None` for tables, the writer sentinel
    /// and intervals.
    fn point(&self) -> Option<(LockSpace, u64, &[u8])> {
        match self {
            Self::Row { table, pk } if !pk.is_empty() => Some((LockSpace::Row, *table, pk)),
            Self::Unique { index, tuple } => Some((LockSpace::Unique, *index, tuple)),
            _ => None,
        }
    }
}

fn point_bytes(id: u64, tail: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(8 + tail.len());
    bytes.extend_from_slice(&id.to_be_bytes());
    bytes.extend_from_slice(tail);
    bytes
}

#[cfg(test)]
fn point_key(space: LockSpace, bytes: &[u8]) -> LockKey {
    let (id, tail) = bytes.split_at(8);
    let id = u64::from_be_bytes(id.try_into().expect("eight id bytes"));
    match space {
        LockSpace::Row => LockKey::Row {
            table: id,
            pk: tail.to_vec(),
        },
        LockSpace::Unique => LockKey::Unique {
            index: id,
            tuple: tail.to_vec(),
        },
    }
}

fn borrowed(bound: &Bound<Vec<u8>>) -> Bound<&[u8]> {
    match bound {
        Bound::Included(bytes) => Bound::Included(bytes),
        Bound::Excluded(bytes) => Bound::Excluded(bytes),
        Bound::Unbounded => Bound::Unbounded,
    }
}

fn interval_contains(start: &Bound<Vec<u8>>, end: &Bound<Vec<u8>>, bytes: &[u8]) -> bool {
    (match start {
        Bound::Included(start) => start.as_slice() <= bytes,
        Bound::Excluded(start) => start.as_slice() < bytes,
        Bound::Unbounded => true,
    }) && match end {
        Bound::Included(end) => bytes <= end.as_slice(),
        Bound::Excluded(end) => bytes < end.as_slice(),
        Bound::Unbounded => true,
    }
}

/// Whether `a` held in `a_mode` and `b` held in `b_mode` by two different transactions
/// conflict: the same key in conflicting modes, or a gap lock and an Exclusive point lock
/// inside it (GD-0075 R5-72). The range engine uses it to find prepared holders.
pub(crate) fn locks_conflict(a: &LockKey, a_mode: LockMode, b: &LockKey, b_mode: LockMode) -> bool {
    if a == b {
        return a_mode.conflicts(b_mode);
    }
    let gap_and_point = |gap: &LockKey, point: &LockKey, point_mode: LockMode| match (gap, point.point()) {
        (LockKey::Interval { space, start, end }, Some((point_space, id, tail))) => {
            *space == point_space
                && point_mode == LockMode::Exclusive
                && interval_contains(start, end, &point_bytes(id, tail))
        }
        _ => false,
    };
    gap_and_point(a, b, b_mode) || gap_and_point(b, a, a_mode)
}

/// `start >= end` per bounds. A non-empty interval is a valid `BTreeMap::range`.
fn interval_empty(start: &Bound<Vec<u8>>, end: &Bound<Vec<u8>>) -> bool {
    match (start, end) {
        (Bound::Included(start), Bound::Included(end)) => start > end,
        (
            Bound::Included(start) | Bound::Excluded(start),
            Bound::Included(end) | Bound::Excluded(end),
        ) => start >= end,
        _ => false,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum LockMode {
    AccessShare,
    RowShare,
    RowExclusive,
    ShareUpdateExclusive,
    Share,
    ShareRowExclusive,
    ExclusiveTable,
    AccessExclusive,
    Shared,
    Exclusive,
}

impl LockMode {
    // Table bits follow the first eight enum variants in PostgreSQL mode order.
    fn conflict_set(self) -> u16 {
        match self {
            Self::AccessShare => 0b10000000,
            Self::RowShare => 0b11000000,
            Self::RowExclusive => 0b11110000,
            Self::ShareUpdateExclusive => 0b11111000,
            Self::Share => 0b11101100,
            Self::ShareRowExclusive => 0b11111100,
            Self::ExclusiveTable => 0b11111110,
            Self::AccessExclusive => 0b11111111,
            Self::Shared => 0b10,
            Self::Exclusive => 0b11,
        }
    }

    pub(crate) fn is_table(self) -> bool {
        !matches!(self, Self::Shared | Self::Exclusive)
    }

    pub(crate) fn covers(self, requested: Self) -> bool {
        self.is_table() == requested.is_table()
            && self.conflict_set() & requested.conflict_set() == requested.conflict_set()
    }

    fn conflicts(self, other: Self) -> bool {
        if self.is_table() && other.is_table() {
            self.conflict_set() & (1 << other as u8) != 0
        } else {
            match self {
                Self::Shared => matches!(other, Self::Exclusive),
                Self::Exclusive | Self::AccessExclusive => true,
                _ => false,
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct HeldModes(u16);

impl HeldModes {
    pub(crate) fn is_subset(self, other: Self) -> bool {
        self.intersection(other) == self
    }

    fn intersection(self, other: Self) -> Self {
        Self(self.0 & other.0)
    }

    fn insert(&mut self, mode: LockMode) {
        self.0 |= 1 << mode as u8;
    }

    fn iter(self) -> impl Iterator<Item = LockMode> {
        use LockMode::*;
        [
            AccessShare,
            RowShare,
            RowExclusive,
            ShareUpdateExclusive,
            Share,
            ShareRowExclusive,
            ExclusiveTable,
            AccessExclusive,
            Shared,
            Exclusive,
        ]
        .into_iter()
        .filter(move |mode| self.0 & (1 << *mode as u8) != 0)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct LockConfig {
    pub(crate) shards: usize,
    #[cfg(test)]
    pub(crate) placement: Option<fn(&LockKey) -> usize>,
    pub(crate) max_row_locks_per_txn: usize,
    pub(crate) deadlock_recheck: Duration,
    pub(crate) deadlock_timeout: Duration,
}

impl Default for LockConfig {
    fn default() -> Self {
        Self {
            shards: DEFAULT_LOCK_SHARDS,
            #[cfg(test)]
            placement: None,
            max_row_locks_per_txn: 1_000_000,
            deadlock_recheck: Duration::from_millis(100),
            deadlock_timeout: Duration::ZERO,
        }
    }
}

impl LockConfig {
    fn sleep_interval(&self, queued_at: Instant, now: Instant, waited: Duration, deadline: Option<Instant>) -> Duration {
        let mut interval = self.deadlock_recheck;
        if waited < self.deadlock_timeout {
            interval = interval.min(self.deadlock_timeout.saturating_sub(now.duration_since(queued_at)));
        }
        if let Some(deadline) = deadline { interval = interval.min(deadline.saturating_duration_since(now)); }
        interval
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LockError {
    Deadlock,
    Timeout,
    Cancelled,
    Limit,
    Misuse,
}

impl LockError {
    pub(crate) fn sqlstate(&self) -> &'static str {
        match self {
            Self::Deadlock => "40P01",
            Self::Timeout => "55P03",
            Self::Cancelled => "57014",
            Self::Limit => "53200",
            Self::Misuse => "XX000",
        }
    }
}

#[derive(Debug, Default)]
struct KeyState {
    holders: HashMap<TxnId, HeldModes>,
    queue: Vec<u64>,
    slow: HashSet<TxnId>,
}

// Key payload allocations are prepared before taking shard locks. The map
// copy is consumed only for a new key; the holding copy only for a new owner.
#[derive(Debug, Default)]
struct PreparedKey { map: Option<HashedKey>, held: Option<HashedKey> }
impl PreparedKey {
    fn new(key: &HashedKey) -> Self { Self { map: Some(key.clone()), held: Some(key.clone()) } }
}

#[derive(Debug)]
struct Waiter {
    txn: TxnId,
    key: HashedKey,
    mode: LockMode,
    upgrade: bool,
    group: Vec<(HashedKey, LockMode)>,
    // Set by `cancel_wait`: the request ends `Deadlock` instead of being granted.
    cancelled: bool,
    slot: Arc<Slot>,
    prepared: PreparedKey,
}

#[derive(Debug, Default)]
struct SlotState {
    result: Option<Result<(), LockError>>,
    recheck: bool,
    #[cfg(test)]
    sleeping: bool,
    #[cfg(test)]
    ever_slept: bool,
    #[cfg(test)]
    notified: bool,
}
#[derive(Debug, Default)]
struct Slot { m: Mutex<SlotState>, cv: Condvar }
impl Slot {
    fn result(&self) -> Option<Result<(), LockError>> {
        let _order = OrderGuard::leaf();
        self.m.lock().unwrap_or_else(|p| p.into_inner()).result
    }
    fn recheck(&self) {
        let _order = OrderGuard::leaf();
        let mut state = self.m.lock().unwrap_or_else(|p| p.into_inner());
        state.recheck = true;
        #[cfg(test)]
        { state.notified = true; }
        self.cv.notify_one();
    }
    fn complete(&self, result: Result<(), LockError>) {
        let _order = OrderGuard::leaf();
        let mut state = self.m.lock().unwrap_or_else(|p| p.into_inner());
        state.result = Some(result);
        #[cfg(test)]
        { state.notified = true; }
        self.cv.notify_one();
    }
}
#[derive(Debug, Default)]
struct TxnLocal { rows: usize, shard_mask: ShardMask, pending: usize }
#[derive(Debug, Default)]
struct ShardMask { first: u64, rest: Vec<u64> }
impl ShardMask {
    fn insert(&mut self, index: usize) {
        if index < 64 { self.first |= 1 << index; }
        else {
            let word = index / 64 - 1;
            if self.rest.len() <= word { self.rest.resize(word + 1, 0); }
            self.rest[word] |= 1 << (index % 64);
        }
    }
    fn contains(&self, index: usize) -> bool {
        let word = if index < 64 { self.first } else { self.rest.get(index / 64 - 1).copied().unwrap_or(0) };
        word & (1 << (index % 64)) != 0
    }
}
#[derive(Debug)]
struct TxnRegistry { stripes: Box<[Mutex<HashMap<TxnId, TxnLocal>>]> }
impl Default for TxnRegistry {
    fn default() -> Self {
        Self { stripes: (0..64).map(|_| Mutex::default()).collect() }
    }
}
impl TxnRegistry {
    fn stripe(&self, txn: TxnId) -> LeafGuard<'_, HashMap<TxnId, TxnLocal>> {
        LeafGuard { guard: self.stripes[txn.0 as usize & 63].lock().unwrap_or_else(|p| p.into_inner()), _order: OrderGuard::leaf() }
    }
    fn rows(&self, txn: TxnId) -> usize { self.stripe(txn).get(&txn).map_or(0, |t| t.rows) }
    fn pending(&self, txn: TxnId, add: bool) {
        let mut stripe = self.stripe(txn);
        let local = stripe.entry(txn).or_default();
        if add { local.pending += 1; } else { local.pending -= 1; }
    }
}
#[derive(Debug, Default)]
struct GapState {
    intervals: [KeySet; 2],
    points: [BTreeMap<Vec<u8>, HashedKey>; 2],
    indexed: [bool; 2],
}
// The keyed payload hash is prepared before locking. Bucket probes and rehashes
// write only this u64, avoiding repeated SipHash over user-sized byte strings.
// Shard selection mixes it separately so bucket bits retain their entropy.
#[derive(Clone, Debug, PartialEq, Eq)]
struct HashedKey { hash: u64, key: LockKey }
impl Hash for HashedKey {
    fn hash<H: Hasher>(&self, h: &mut H) { h.write_u64(self.hash); }
}
#[derive(Default, Debug)]
struct PassHasher(u64);
impl Hasher for PassHasher {
    fn finish(&self) -> u64 { self.0 }
    fn write(&mut self, _: &[u8]) { unreachable!("prehashed keys only") }
    fn write_u64(&mut self, h: u64) { self.0 = h; }
}
impl std::ops::Deref for HashedKey {
    type Target = LockKey;
    fn deref(&self) -> &LockKey { &self.key }
}
type KeyMap<V> = HashMap<HashedKey, V, BuildHasherDefault<PassHasher>>;
type KeySet = HashSet<HashedKey, BuildHasherDefault<PassHasher>>;

#[derive(Debug, Default)]
struct ShardState {
    keys: KeyMap<KeyState>,
    waiters: HashMap<u64, Waiter>,
    by_txn: HashMap<TxnId, Vec<u64>>,
    held: HashMap<TxnId, KeySet>,
    provisional: HashMap<TxnId, KeyMap<HeldModes>>,
    // Only shard zero's gap state is used, under all shard locks.
    gap: GapState,
    #[cfg(test)]
    deadlock_victims: HashSet<TxnId>,
}
#[cfg(test)]
thread_local! {
    static LOCK_ORDER: std::cell::RefCell<(Vec<usize>, usize)> = const { std::cell::RefCell::new((Vec::new(), 0)) };
}
#[derive(Debug)]
struct OrderGuard { #[cfg(test)] shard: Option<usize> }
impl OrderGuard {
    fn shard(index: usize) -> Self {
        #[cfg(test)]
        LOCK_ORDER.with(|order| {
            let mut order = order.borrow_mut();
            assert_eq!(order.1, 0, "shard acquired while holding a leaf lock");
            assert!(order.0.iter().all(|&i| i < index), "shards acquired out of order");
            order.0.push(index);
        });
        #[cfg(not(test))]
        let _ = index;
        Self { #[cfg(test)] shard: Some(index) }
    }
    fn leaf() -> Self {
        #[cfg(test)]
        LOCK_ORDER.with(|order| order.borrow_mut().1 += 1);
        Self { #[cfg(test)] shard: None }
    }
}
impl Drop for OrderGuard {
    fn drop(&mut self) {
        #[cfg(test)]
        LOCK_ORDER.with(|order| {
            let mut order = order.borrow_mut();
            if let Some(i) = self.shard { order.0.retain(|&s| s != i); } else { order.1 -= 1; }
        });
    }
}
struct ShardGuard<'a> { guard: MutexGuard<'a, ShardState>, _order: OrderGuard }
impl std::ops::Deref for ShardGuard<'_> {
    type Target = ShardState;
    fn deref(&self) -> &ShardState { &self.guard }
}
impl std::ops::DerefMut for ShardGuard<'_> {
    fn deref_mut(&mut self) -> &mut ShardState { &mut self.guard }
}
struct LeafGuard<'a, T> { guard: MutexGuard<'a, T>, _order: OrderGuard }
impl<T> std::ops::Deref for LeafGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T { &self.guard }
}
impl<T> std::ops::DerefMut for LeafGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T { &mut self.guard }
}
// Inline one guard so shard mode acquires one mutex without a guard-vector allocation.
enum Guards<'a> { One([ShardGuard<'a>; 1]), Many(Vec<ShardGuard<'a>>) }
impl<'a> Guards<'a> {
    fn slice(&self) -> &[ShardGuard<'a>] {
        match self { Self::One(x) => x, Self::Many(x) => x }
    }
    fn slice_mut(&mut self) -> &mut [ShardGuard<'a>] {
        match self { Self::One(x) => x, Self::Many(x) => x }
    }
}
struct State<'a> { guards: Guards<'a>, manager: &'a LockManager, local: Option<usize>, reserved: Option<(TxnId, usize)> }
impl std::ops::Deref for State<'_> {
    type Target = GapState;
    fn deref(&self) -> &GapState { &self.guards.slice()[0].gap }
}
impl std::ops::DerefMut for State<'_> {
    fn deref_mut(&mut self) -> &mut GapState { &mut self.guards.slice_mut()[0].gap }
}
impl State<'_> {
    fn position(&self, key: &HashedKey) -> usize {
        let index = self.manager.hashed_shard(key);
        if let Some(local) = self.local { debug_assert_eq!(local, index); 0 } else { index }
    }
    fn key(&self, key: &HashedKey) -> Option<&KeyState> { self.guards.slice()[self.position(key)].keys.get(key) }
    fn key_mut(&mut self, key: &HashedKey) -> Option<&mut KeyState> {
        let i = self.position(key); self.guards.slice_mut()[i].keys.get_mut(key)
    }
    fn key_entry(&mut self, key: HashedKey) -> std::collections::hash_map::Entry<'_, HashedKey, KeyState> {
        let i = self.position(&key); self.guards.slice_mut()[i].keys.entry(key)
    }
    fn key_remove(&mut self, key: &HashedKey) { let i = self.position(key); self.guards.slice_mut()[i].keys.remove(key); }
    fn keys(&self) -> impl Iterator<Item=(&HashedKey, &KeyState)> { self.guards.slice().iter().flat_map(|s| s.keys.iter()) }
    fn waiter(&self, id: &u64) -> Option<&Waiter> { self.guards.slice().iter().find_map(|s| s.waiters.get(id)) }
    fn waiter_mut(&mut self, id: &u64) -> Option<&mut Waiter> { self.guards.slice_mut().iter_mut().find_map(|s| s.waiters.get_mut(id)) }
    fn pending(&self, txn: TxnId) -> Vec<u64> { self.guards.slice().iter().flat_map(|s| s.by_txn.get(&txn).into_iter().flatten().copied()).collect() }
    fn provisional_modes(&self, txn: TxnId, key: &HashedKey) -> Option<HeldModes> {
        self.guards.slice()[self.position(key)].provisional.get(&txn)?.get(key).copied()
    }
    fn provisional_empty(&self) -> bool { self.guards.slice().iter().all(|s| s.provisional.is_empty()) }
    fn provisional_insert(&mut self, txn: TxnId, key: HashedKey, modes: HeldModes) {
        let i = self.position(&key); self.guards.slice_mut()[i].provisional.entry(txn).or_default().insert(key, modes);
    }
    fn provisional_remove(&mut self, txn: TxnId, key: &HashedKey) -> Option<HeldModes> {
        let i = self.position(key); let map = &mut self.guards.slice_mut()[i].provisional;
        let modes = map.get_mut(&txn)?.remove(key);
        if map[&txn].is_empty() { map.remove(&txn); } modes
    }
    fn forget_holding(&mut self, txn: TxnId, key: &HashedKey) {
        let i = self.position(key);
        let held = &mut self.guards.slice_mut()[i].held;
        if let Some(keys) = held.get_mut(&txn) { keys.remove(key); if keys.is_empty() { held.remove(&txn); } }
        let mut stripe = self.manager.registry.stripe(txn);
        if !matches!(key.key, LockKey::Table(_)) && let Some(local) = stripe.get_mut(&txn) {
            debug_assert!(local.rows > 0, "removed counted holding has no row count");
            local.rows = local.rows.saturating_sub(1);
        }
    }
    #[cfg(test)]
    fn victim(&mut self, txn: TxnId) { self.guards.slice_mut()[0].deadlock_victims.insert(txn); }
}

struct GroupContext<'a> {
    requested: &'a [(HashedKey, LockMode)],
}

#[cfg(test)]
type RaceHook = Arc<dyn Fn() + Send + Sync>;
#[cfg(test)]
#[derive(Default)]
struct TestHooks { h1: Mutex<Option<RaceHook>>, point: Mutex<Option<RaceHook>>, h2: Mutex<Option<RaceHook>>, h3: Mutex<Option<RaceHook>> }
#[cfg(test)]
impl std::fmt::Debug for TestHooks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str("TestHooks") }
}
#[cfg(test)]
fn run_hook(hook: &Mutex<Option<RaceHook>>) {
    let callback = hook.lock().unwrap().clone();
    if let Some(callback) = callback { callback(); }
}

#[derive(Debug)]
pub(crate) struct LockManager {
    config: LockConfig,
    hash_builder: RandomState,
    shards: Box<[Mutex<ShardState>]>,
    registry: TxnRegistry,
    next_waiter: AtomicU64,
    gap_mode: [AtomicBool; 2],
    #[cfg(test)]
    hooks: TestHooks,
    #[cfg(test)]
    waiter_wakeups: AtomicUsize,
    #[cfg(test)]
    waiter_spurious_wakeups: AtomicUsize,
    #[cfg(test)]
    release_all_calls: AtomicUsize,
}

impl LockManager {
    pub(crate) fn new(config: LockConfig) -> Self {
        assert!(config.shards.is_power_of_two());
        Self {
            shards: (0..config.shards).map(|_| Mutex::default()).collect(),
            hash_builder: RandomState::new(),
            registry: TxnRegistry::default(),
            next_waiter: AtomicU64::new(0),
            gap_mode: [AtomicBool::new(false), AtomicBool::new(false)],
            #[cfg(test)]
            hooks: TestHooks::default(),
            config,
            #[cfg(test)]
            waiter_wakeups: AtomicUsize::new(0),
            #[cfg(test)]
            waiter_spurious_wakeups: AtomicUsize::new(0),
            #[cfg(test)]
            release_all_calls: AtomicUsize::new(0),
        }
    }

    pub(crate) fn acquire(
        &self,
        txn: TxnId,
        key: LockKey,
        mode: LockMode,
        deadline: Option<Instant>,
        cancel: &dyn Fn() -> bool,
    ) -> Result<(), LockError> {
        let key = self.prepare(key);
        self.acquire_group_member(txn, key.clone(), mode, deadline, cancel, GroupContext { requested: &[(key, mode)] })
    }

    // Execute precisely one member's immediate protocol, including its notifications.
    fn try_member_locked(
        &self, state: &mut State<'_>, txn: TxnId, key: &HashedKey, mode: LockMode,
        group: &[(HashedKey, LockMode)], prepared: &mut PreparedKey,
    ) -> Result<Option<()>, LockError> {
        let held = state
            .key(key)
            .and_then(|k| k.holders.get(&txn))
            .copied();
        if (state.provisional_empty() || state.provisional_modes(txn, key).is_none())
            && held.is_some_and(|held| held.iter().any(|held| held.covers(mode))) {
            if mode.is_table() {
                state.key_mut(key).unwrap().holders.get_mut(&txn).unwrap().insert(mode);
            }
            return Ok(Some(()));
        }
        let immediate = state.key(key).is_none_or(|locks| {
            (locks.queue.is_empty() || held.is_some() || state.row_locker(txn, key) || state.compatible_bypass(txn, key, group))
                && !locks.holders.iter().any(|(&owner, held)| {
                    state.holder_conflicts(txn, key, mode, owner, *held, group)
                })
        }) && !state.cross_blocked(txn, key, mode);
        // A zero deadline is a nonblocking attempt: a compatible immediate
        // acquisition, including reentrance and upgrades, still succeeds.
        if immediate {
            state.grant(txn, key, mode, self.config.max_row_locks_per_txn, prepared)?;
            state.finish_group(txn, group);
            state.drain_queue(key, self.config.max_row_locks_per_txn);
            state.notify_grant(txn, key);
            return Ok(Some(()));
        }

        Ok(None)
    }

    // S1-S3 are checked without changing any shard or registry state. Enqueue
    // requires every shard, so pending cannot rise while this guard is held.
    fn shard_safe(&self, state: &State<'_>, txn: TxnId, group: &[(HashedKey, LockMode)]) -> bool {
        let index = state.local.expect("shard preflight");
        if self.registry.stripe(txn).get(&txn).is_some_and(|t| t.pending != 0) { return false; } // S1
        for (key, _) in group {
            if matches!(&key.key, LockKey::Interval { .. }) || self.hashed_shard(key) != index { return false; }
            // S2 must be read under the point's shard lock, after the test hook.
            if key.point().is_some_and(|(space, _, _)| self.gap_mode[space as usize].load(std::sync::atomic::Ordering::SeqCst)) { return false; }
            if let Some(locks) = state.key(key) {
                for id in &locks.queue {
                    let waiter = state.waiter(id).expect("local waiter");
                    // Conservatively inspect every waiter, including ones that
                    // become grantable after this operation changes ownership.
                    if self.registry.stripe(waiter.txn).get(&waiter.txn).is_some_and(|t| t.pending > 1)
                        || waiter.group.iter().any(|(k, _)| self.hashed_shard(k) != index
                            || k.point().is_some_and(|(space, _, _)| self.gap_mode[space as usize].load(std::sync::atomic::Ordering::SeqCst))) {
                        return false; // S3: drop the shard before any mutation.
                    }
                }
            }
        }
        true
    }

    fn preflight_group(&self, state: &mut State<'_>, txn: TxnId, requested: &[(HashedKey, LockMode)], prepared: &mut [PreparedKey], blocking: bool) -> Result<bool, LockError> {
        let blocked = requested.iter().any(|(key, mode)| {
            let held = state.key(key).and_then(|k| k.holders.get(&txn));
            if (state.provisional_empty() || state.provisional_modes(txn, key).is_none())
                && held.is_some_and(|h| h.iter().any(|h| h.covers(*mode))) { return false; }
            state.key(key).is_some_and(|locks| {
                (!locks.queue.is_empty() && held.is_none() && !state.row_locker(txn, key) && !state.compatible_bypass(txn, key, requested))
                    || locks.holders.iter().any(|(&owner, held)| state.holder_conflicts(txn, key, *mode, owner, *held, requested))
            }) || state.cross_blocked(txn, key, *mode)
        });
        if blocked { return Ok(false); }
        let extra = requested.iter().enumerate().filter(|(i, (key, _))|
            !requested[..*i].iter().any(|(earlier, _)| earlier == key)
                && state.key(key).is_none_or(|k| !k.holders.contains_key(&txn))).count();
        // Reserve the complete group's count in one stripe critical section:
        // two independent shard grants to one transaction cannot overbook it.
        {
            let mut stripe = self.registry.stripe(txn);
            let local = stripe.entry(txn).or_default();
            if local.rows.saturating_add(extra) > self.config.max_row_locks_per_txn { return Err(LockError::Limit); }
            local.rows += extra;
        }
        state.reserved = Some((txn, extra));
        if blocking {
            // Preserve the existing member protocol exactly: the first member
            // may finish the whole provisional group, making later covered
            // members return without adding a mode or draining their queue.
            for (key, _) in requested {
                let previous = state.key(key).and_then(|k| k.holders.get(&txn)).copied().unwrap_or_default();
                state.provisional_insert(txn, key.clone(), previous);
            }
            for ((key, mode), prepared) in requested.iter().zip(prepared) {
                assert!(self.try_member_locked(state, txn, key, *mode, requested, prepared)?.is_some());
            }
            for (key, _) in requested { state.provisional_remove(txn, key); }
        } else {
            for ((key, mode), prepared) in requested.iter().zip(prepared) {
                state.grant(txn, key, *mode, self.config.max_row_locks_per_txn, prepared)?;
            }
            debug_assert_eq!(state.reserved, Some((txn, 0)));
            state.reserved = None;
            for (key, _) in requested {
                state.drain_queue(key, self.config.max_row_locks_per_txn);
                state.notify_grant(txn, key);
            }
        }
        if blocking {
            debug_assert_eq!(state.reserved, Some((txn, 0)));
            state.reserved = None;
        }
        Ok(true)
    }

    fn acquire_group_member(
        &self, txn: TxnId, key: HashedKey, mode: LockMode,
        deadline: Option<Instant>, cancel: &dyn Fn() -> bool,
        GroupContext { requested: group }: GroupContext<'_>,
    ) -> Result<(), LockError> {
        let deadline = match (deadline, crate::Cancellation::lock_deadline()) {
            (Some(a), Some(b)) => Some(a.min(b)), (a, b) => a.or(b),
        };
        if matches!(&key.key, LockKey::Table(_)) != mode.is_table() {
            return Err(LockError::Misuse);
        }
        if let LockKey::Interval { start, end, .. } = &key.key {
            if mode != LockMode::Shared {
                return Err(LockError::Misuse);
            }
            // An empty interval protects nothing: granted without being recorded.
            if interval_empty(start, end) {
                return Ok(());
            }
        }
        if let LockKey::Interval { space, .. } = &key.key {
            self.gap_mode[*space as usize].store(true, std::sync::atomic::Ordering::SeqCst);
        }
        let index = self.hashed_shard(&key);
        let mut prepared = PreparedKey::new(&key);
        if !matches!(&key.key, LockKey::Interval { .. }) {
            let mut state = self.single(index).map_err(|_| LockError::Misuse)?;
            #[cfg(test)]
            run_hook(&self.hooks.point);
            if self.shard_safe(&state, txn, group) {
                if self.try_member_locked(&mut state, txn, &key, mode, group, &mut prepared)?.is_some() { return Ok(()); }
                if deadline.is_some_and(|d| Instant::now() >= d) { return Err(LockError::Timeout); }
            }
            drop(state);
            #[cfg(test)]
            run_hook(&self.hooks.h1);
        }
        // Allocate waiter ownership before entering the global critical section.
        let slot = Arc::new(Slot::default());
        let owned_group = group.to_vec();
        let waiter_key = key.clone();
        let mut state = self.global().map_err(|_| LockError::Misuse)?;
        if let LockKey::Interval { space, .. } = &key.key {
            state.ensure_indexed(*space);
        }
        if self.try_member_locked(&mut state, txn, &key, mode, group, &mut prepared)?.is_some() {
            return Ok(());
        }
        let held = state.key(&key).and_then(|k| k.holders.get(&txn)).copied();

        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            return Err(LockError::Timeout);
        }
        let id = self.next_waiter.try_update(std::sync::atomic::Ordering::Relaxed, std::sync::atomic::Ordering::Relaxed, |n| n.checked_add(1)).map_err(|_| LockError::Misuse)?;
        let upgrade = held.is_some() || state.row_locker(txn, &key);
        // A request blocked only across keys queues on a fresh entry.
        if state.key(&key).is_none() { state.key_entry(prepared.map.take().expect("prepared key")).or_default(); }
        let position = if upgrade {
            state.key(&key).unwrap()
                .queue
                .iter()
                .position(|id| !state.waiter(id).unwrap().upgrade)
                .unwrap_or(state.key(&key).unwrap().queue.len())
        } else {
            state.key(&key).unwrap().queue.len()
        };
        let queued_at = Instant::now();
        let i = index;
        self.registry.pending(txn, true);
        state.guards.slice_mut()[i].by_txn.entry(txn).or_default().push(id);
        state.guards.slice_mut()[i].waiters.insert(
            id,
            Waiter {
                txn,
                key: waiter_key,
                mode,
                upgrade,
                group: owned_group,
                cancelled: false,
                slot: Arc::clone(&slot),
                prepared,
            },
        );
        state
            .key_mut(&key)
            .expect("blocked key exists")
            .queue
            .insert(position, id);
        state.index(&key, mode);
        if self.config.deadlock_timeout.is_zero() && state.has_cycle(txn) {
            #[cfg(test)]
            state.victim(txn);
            state.remove_waiter(id, Err(LockError::Deadlock));
            state.drain_queue(&key, self.config.max_row_locks_per_txn);
        }

        drop(state);
        loop {
            if let Some(result) = self.observe_result(&slot) {
                return result;
            }
            // User callbacks may inspect this manager without deadlocking it.
            #[cfg(test)]
            run_hook(&self.hooks.h3);
            let cancelled = cancel();
            let mut state = self.global().map_err(|_| LockError::Misuse)?;
            if let Some(result) = self.observe_result(&slot) {
                return result;
            }
            let waited = queued_at.elapsed();
            let error = if state
                .waiter(&id)
                .is_some_and(|waiter| waiter.cancelled)
            {
                Some(LockError::Deadlock)
            } else if cancelled {
                Some(LockError::Cancelled)
            } else if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                Some(LockError::Timeout)
            } else if waited >= self.config.deadlock_timeout && state.has_cycle(txn) {
                #[cfg(test)]
                state.victim(txn);
                Some(LockError::Deadlock)
            } else {
                None
            };
            if let Some(error) = error {
                state.remove_waiter(id, Err(error));
                state.drain_queue(&key, self.config.max_row_locks_per_txn);
                return slot.result().expect("removed waiter has result");
            }
            let now = Instant::now();
            let interval = self.config.sleep_interval(queued_at, now, waited, deadline);
            drop(state);
            #[cfg(test)]
            run_hook(&self.hooks.h2);
            let slot_order = OrderGuard::leaf();
            let mut parked = slot.m.lock().map_err(|_| LockError::Misuse)?;
            if parked.result.is_none() && !parked.recheck {
                #[cfg(test)]
                { parked.sleeping = true; parked.ever_slept = true; }
                let (next, _) = slot.cv.wait_timeout(parked, interval).map_err(|_| LockError::Misuse)?;
                parked = next;
                #[cfg(test)]
                {
                    if parked.result.is_some() || parked.recheck {
                        self.record_explained(&mut parked);
                    } else { self.waiter_spurious_wakeups.fetch_add(1, Ordering::Relaxed); }
                    parked.sleeping = false;
                }
            }
            #[cfg(test)]
            self.record_explained(&mut parked);
            parked.recheck = false;
            drop(parked);
            drop(slot_order);
        }
    }

    /// Acquire a group of point locks in K-then-D order for blocking calls.
    /// Zero-deadline calls preflight the complete group atomically. The caller serializes requests made by the same transaction.
    pub(crate) fn acquire_points(
        &self, txn: TxnId, requested: &[(LockKey, LockMode)],
        deadline: Option<Instant>, cancel: &dyn Fn() -> bool,
    ) -> Result<(), LockError> {
        let requested = requested.iter().map(|(key, mode)| (self.prepare(key.clone()), *mode)).collect::<Vec<_>>();
        self.acquire_points_hashed(txn, &requested, deadline, cancel)
    }
    fn acquire_points_hashed(&self, txn: TxnId, requested: &[(HashedKey, LockMode)], deadline: Option<Instant>, cancel: &dyn Fn() -> bool) -> Result<(), LockError> {
        let deadline = match (deadline, crate::Cancellation::lock_deadline()) {
            (Some(a), Some(b)) => Some(a.min(b)), (a, b) => a.or(b),
        };
        if requested.iter().any(|(key, mode)| (key.point().is_none() && !matches!(&key.key, LockKey::RowKey { .. })) || mode.is_table()) {
            return Err(LockError::Misuse);
        }
        let blocking = deadline.is_none_or(|d| d > Instant::now());
        let mut atomic_order = requested.to_vec();
        if blocking { atomic_order.sort_by_key(|(key, _)| !matches!(&key.key, LockKey::RowKey { .. })); }
        if let Some((first, _)) = requested.first() {
            let index = self.hashed_shard(first);
            let mut prepared = atomic_order.iter().map(|(key, _)| PreparedKey::new(key)).collect::<Vec<_>>();
            let mut state = self.single(index).map_err(|_| LockError::Misuse)?;
            #[cfg(test)]
            run_hook(&self.hooks.point);
            // An existing provisional group can become final during a member's
            // drain and invalidate a later member's preflight. Keep that case
            // on the original global provisional protocol.
            if self.shard_safe(&state, txn, requested) && (!blocking || state.provisional_empty()) {
                if self.preflight_group(&mut state, txn, &atomic_order, &mut prepared, blocking)? { return Ok(()); }
                if deadline.is_some_and(|d| Instant::now() >= d) { return Err(LockError::Timeout); }
            }
            drop(state);
            #[cfg(test)]
            run_hook(&self.hooks.h1);
        } else { return Ok(()); }
        if deadline.is_none_or(|d| d > Instant::now()) {
            let mut ordered = requested.iter().collect::<Vec<_>>();
            ordered.sort_by_key(|(key, _)| !matches!(&key.key, LockKey::RowKey { .. }));
            let mut prepared = ordered.iter().map(|(key, _)| PreparedKey::new(key)).collect::<Vec<_>>();
            let provisional_keys = ordered.iter().map(|(key, _)| (txn, key.clone())).collect::<Vec<_>>();
            let mut state = self.global().map_err(|_| LockError::Misuse)?;
            for ((key, _), provisional_key) in ordered.iter().zip(provisional_keys) {
                let previous = state.key(key).and_then(|s| s.holders.get(&txn))
                    .copied().unwrap_or_default();
                state.provisional_insert(provisional_key.0, provisional_key.1, previous);
            }
            let result = (|| {
                // Point members have already passed acquire_group_member's type checks.
                // It merges the thread deadline before locking, but checks expiration
                // and calls cancel only after an immediate attempt fails. Neither can
                // reject an immediate grant. Keep the same per-member side effects.
                let mut start = 0;
                for (i, (key, mode)) in ordered.iter().enumerate() {
                    if self.try_member_locked(&mut state, txn, key, *mode, requested, &mut prepared[i])?.is_none() {
                        start = i;
                        break;
                    }
                    start = i + 1;
                }
                loop {
                    if start < ordered.len() {
                        drop(state);
                        for (key, mode) in &ordered[start..] {
                            self.acquire_group_member(txn, key.clone(), *mode, deadline, cancel,
                                GroupContext { requested })?;
                        }
                        state = self.global().map_err(|_| LockError::Misuse)?;
                    }
                    // Subsequent passes recheck the whole group after any wait.
                    start = 0;
                    if ordered.iter().any(|(key, mode)| {
                        state.key(key).is_some_and(|locks| locks.holders.iter().any(|(&owner, held)|
                            state.holder_conflicts(txn, key, *mode, owner, *held, requested)))
                            || state.cross_blocked(txn, key, *mode)
                    }) { continue; }
                    for (key, _) in &ordered {
                        state.provisional_remove(txn, key);
                    }
                    return Ok(());
                }
            })();
            if result.is_err() {
                // Restore precisely the pre-group modes, including upgrades of
                // existing grants. release_all may already have removed them.
                let mut state = self.global().unwrap_or_else(|p| p.into_inner());
                let mut affected = HashSet::new();
                for (key, _) in &ordered {
                    if let Some(previous) = state.provisional_remove(txn, key) {
                        if previous.iter().next().is_some() {
                            state.key_entry(key.clone()).or_default().holders.insert(txn, previous);
                        } else if state.key_mut(key).is_some_and(|s| s.holders.remove(&txn).is_some()) {
                            state.forget_holding(txn, key);
                        }
                        affected.insert(key.clone());
                        let mut cross = Vec::new();
                        state.cross_queued(key, &mut cross);
                        affected.extend(cross);
                    }
                }
                for key in affected {
                    state.drain_queue(&key, self.config.max_row_locks_per_txn);
                    state.settle(&key);
                }
            }
            return result;
        }
        {
            let mut prepared = requested.iter().map(|(key, _)| PreparedKey::new(key)).collect::<Vec<_>>();
            let mut state = self.global().map_err(|_| LockError::Misuse)?;
            if self.preflight_group(&mut state, txn, requested, &mut prepared, false)? { return Ok(()); }
        }
        Err(LockError::Timeout)
    }

    #[cfg(test)]
    pub(crate) fn release_all_calls(&self) -> usize {
        self.release_all_calls.load(Ordering::Relaxed)
    }

    /// Snapshot just the requested keys; callers serialize acquisitions per transaction.
    pub(crate) fn modes_on(&self, txn: TxnId, keys: &[(LockKey, LockMode)]) -> Vec<(LockKey, HeldModes)> {
        keys.iter().map(|(key, _)| {
            let hashed = self.prepare(key.clone());
            let index = self.hashed_shard(&hashed);
            let state = self.single(index).unwrap_or_else(|p| p.into_inner());
            (key.clone(), state.key(&hashed).and_then(|s| s.holders.get(&txn)).copied().unwrap_or_default())
        }).collect()
    }

    /// Undo acquisitions in reverse journal order, then wake queues only after
    /// every key has reached its final state (including both halves of K/D).
    pub(crate) fn restore_modes(&self, txn: TxnId, undo: &[(LockKey, HeldModes)]) {
        let undo = undo.iter().map(|(key, modes)| (self.prepare(key.clone()), *modes)).collect::<Vec<_>>();
        let mut state = self.global().unwrap_or_else(|p| p.into_inner());
        let mut affected = HashSet::new();
        for (key, previous) in undo.iter().rev() {
            let Some(locks) = state.key_mut(key) else { continue; };
            let Some(current) = locks.holders.get_mut(&txn) else { continue; };
            // Cleanup may have removed modes since the journal snapshot.
            // Rollback can only release ownership, never recreate it.
            let target = previous.intersection(*current);
            if target.iter().next().is_some() {
                *current = target;
            } else {
                locks.holders.remove(&txn);
                locks.slow.remove(&txn);
                state.forget_holding(txn, key);
            }
            state.provisional_remove(txn, key);
            affected.insert(key.clone());
            let mut unblocked = Vec::new();
            state.cross_queued(key, &mut unblocked);
            affected.extend(unblocked);
            if let LockKey::Row { table, pk } = &key.key {
                affected.insert(HashedKey { hash: key.hash, key: LockKey::RowKey { table: *table, pk: pk.clone() } });
            }
        }
        for key in affected {
            state.drain_queue(&key, self.config.max_row_locks_per_txn);
            state.settle(&key);
        }
    }

    /// Release held locks and terminate this transaction's pending calls with
    /// `Cancelled`. Cleanup still operates if the bookkeeping mutex is poisoned.
    pub(crate) fn release_all(&self, txn: TxnId) {
        #[cfg(test)]
        self.release_all_calls.fetch_add(1, Ordering::Relaxed);
        // Cleanup/inspection remain useful after poisoning; acquire reports Misuse.
        let mut state = self
            .global()
            .unwrap_or_else(|poison| poison.into_inner());
        // Metadata transactions hold no locks. In particular, dropping one must
        // not scan all row/index locks owned by an in-progress COPY statement.
        let local = self.registry.stripe(txn).remove(&txn);
        // A group can have provisional snapshots before its first grant creates
        // a registry entry. Keep cleanup for that case (and any indexed state).
        if local.is_none() && state.guards.slice().iter().all(|s|
            !s.provisional.contains_key(&txn) && !s.by_txn.contains_key(&txn)
                && !s.held.contains_key(&txn)) {
            return;
        }
        let local = local.unwrap_or_default();
        let mut affected = HashSet::new();
        let mut released_hashes = Vec::new();
        for (i, shard) in state.guards.slice_mut().iter_mut().enumerate() {
            if local.shard_mask.contains(i) { released_hashes.extend(shard.held.remove(&txn).unwrap_or_default()); }
        }
        affected.extend(released_hashes);
        // Pending removal updates the registry; retain its count until removal.
        self.registry.stripe(txn).entry(txn).or_default().pending = local.pending;
        let mut unblocked = Vec::new();
        for key in &affected {
            if let Some(locks) = state.key_mut(key) {
                locks.holders.remove(&txn);
                locks.slow.remove(&txn);
            }
            state.provisional_remove(txn, key);
            state.cross_queued(key, &mut unblocked);
            // D grants also determine whether queued K groups may bypass.
            if let LockKey::Row { table, pk } = &key.key {
                let row_key = HashedKey { hash: key.hash, key: LockKey::RowKey { table: *table, pk: pk.clone() } };
                if state.key(&row_key).is_some_and(|locks| !locks.queue.is_empty()) {
                    unblocked.push(row_key);
                }
            }
        }
        affected.extend(unblocked);

        let pending = state.pending(txn);
        for id in pending {
            affected.insert(state.waiter(&id).unwrap().key.clone());
            state.remove_waiter(id, Err(LockError::Cancelled));
        }
        self.registry.stripe(txn).remove(&txn);
        for shard in state.guards.slice_mut() { shard.provisional.remove(&txn); }
        // Only releasing a holder or cancelling a queued request can unblock
        // this key's waiters, or (across keys) requests queued on a key that a
        // released lock overlapped. Leave unrelated queues and holders untouched.
        for key in affected {
            state.drain_queue(&key, self.config.max_row_locks_per_txn);
            state.settle(&key);
        }
    }

    /// Transactions blocking `txn`'s pending request(s): conflicting holders plus
    /// conflicting waiters queued ahead of it. Sorted and deduplicated; empty
    /// when `txn` is not waiting. Feeds the cross-range deadlock detector.
    pub(crate) fn blockers(&self, txn: TxnId) -> Vec<TxnId> {
        let state = self
            .global()
            .unwrap_or_else(|poison| poison.into_inner());
        let mut blockers = Vec::new();
        state.txn_blockers(txn, &mut blockers);
        blockers.sort_unstable();
        blockers.dedup();
        blockers
    }

    /// End `txn`'s pending acquire(s) with `Deadlock` (a cross-range victim).
    /// Sets the per-request cancel flag and wakes the waiter, which removes its
    /// own queue entry. No-op when `txn` is not waiting.
    pub(crate) fn cancel_wait(&self, txn: TxnId) {
        let mut state = self
            .global()
            .unwrap_or_else(|poison| poison.into_inner());
        for id in state.pending(txn) {
            let waiter = state.waiter_mut(&id).expect("indexed waiter");
            waiter.cancelled = true;
            waiter.slot.recheck();
        }
    }

    /// Snapshot held locks, recovering the state for inspection after poisoning.
    pub(crate) fn held(&self, txn: TxnId) -> Vec<(LockKey, LockMode)> {
        let state = self.global().unwrap_or_else(|p| p.into_inner());
        state.keys().flat_map(|(key, locks)| locks.holders.get(&txn).into_iter()
            .flat_map(move |held| held.iter().map(move |mode| (key.key.clone(), mode)))).collect()
    }
    pub(crate) fn fastpath(&self, txn: TxnId, table: u64, mode: LockMode) -> bool {
        let key = LockKey::Table(table);
        let hashed = self.prepare(key);
        let index = self.hashed_shard(&hashed);
        matches!(mode, LockMode::AccessShare | LockMode::RowShare | LockMode::RowExclusive)
            && self.single(index).unwrap_or_else(|p| p.into_inner()).key(&hashed)
                .is_some_and(|locks| !locks.slow.contains(&txn))
    }
    pub(crate) fn waiting_count(&self) -> usize {
        self.global().unwrap_or_else(|p| p.into_inner()).guards.slice().iter().map(|s| s.waiters.len()).sum()
    }
    // Count a product notification when observed, including one arriving
    // during an unexplained condvar return before the thread can sleep again.
    // A coalesced predicate is observed once; self-completion uses Slot::result.
    fn observe_result(&self, slot: &Slot) -> Option<Result<(), LockError>> {
        let _order = OrderGuard::leaf();
        #[allow(unused_mut)]
        let mut state = slot.m.lock().unwrap_or_else(|p| p.into_inner());
        #[cfg(test)]
        self.record_explained(&mut state);
        state.result
    }
    #[cfg(test)]
    fn record_explained(&self, state: &mut SlotState) {
        if state.notified && state.ever_slept && (state.result.is_some() || state.recheck) {
            self.waiter_wakeups.fetch_add(1, Ordering::Relaxed);
        }
        state.notified = false;
    }
    fn hash(&self, key: &LockKey) -> u64 {
        let mut h = self.hash_builder.build_hasher();
        match key {
            LockKey::Row { table, pk } | LockKey::RowKey { table, pk } => (0u8, table, pk).hash(&mut h),
            LockKey::Unique { index, tuple } => (1u8, index, tuple).hash(&mut h),
            LockKey::Table(t) => (2u8, t).hash(&mut h),
            LockKey::Interval { .. } => key.hash(&mut h),
        }
        h.finish()
    }
    fn shard_hash(&self, key: &LockKey, hash: u64) -> usize {
        if self.shards.len() == 1 { return 0; }
        #[cfg(test)]
        if let Some(place) = self.config.placement { return place(key) & (self.shards.len() - 1); }
        if matches!(key, LockKey::Interval { .. }) { return 0; }
        (hash.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> (64 - self.shards.len().trailing_zeros())) as usize
    }
    fn prepare(&self, key: LockKey) -> HashedKey { HashedKey { hash: self.hash(&key), key } }
    fn hashed_shard(&self, key: &HashedKey) -> usize { self.shard_hash(&key.key, key.hash) }
    #[cfg(test)]
    fn shard(&self, key: &LockKey) -> usize { self.shard_hash(key, self.hash(key)) }
    fn single(&self, index: usize) -> std::sync::LockResult<State<'_>> {
        let order = OrderGuard::shard(index);
        let (guard, poisoned) = match self.shards[index].lock() {
            Ok(g) => (g, false), Err(p) => (p.into_inner(), true),
        };
        let state = State { guards: Guards::One([ShardGuard { guard, _order: order }]), manager: self,
            local: Some(index), reserved: None };
        if poisoned { Err(std::sync::PoisonError::new(state)) } else { Ok(state) }
    }
    fn global<'a>(&'a self) -> std::sync::LockResult<State<'a>> {
        let mut poisoned = false;
        let mut take = |(i, s): (usize, &'a Mutex<ShardState>)| {
            let order = OrderGuard::shard(i);
            let guard = match s.lock() {
                Ok(g) => g,
                Err(p) => { poisoned = true; p.into_inner() }
            };
            ShardGuard { guard, _order: order }
        };
        let guards = if self.shards.len() == 1 { Guards::One([take((0, &self.shards[0]))]) }
            else { Guards::Many(self.shards.iter().enumerate().map(&mut take).collect()) };
        let state = State { guards, manager: self, local: None, reserved: None };
        if poisoned { Err(std::sync::PoisonError::new(state)) } else { Ok(state) }
    }

}

impl State<'_> {
    // Complete a ready group under the granting mutex, before another group
    // can overtake it while the acquiring thread is waking. A retained bypasser
    // keeps every addition provisional, including D, until revalidation succeeds.
    fn finish_group(&mut self, txn: TxnId, group: &[(HashedKey, LockMode)]) {
        if self.provisional_empty() || !self.pending(txn).is_empty() { return; }
        if group.iter().any(|(key, mode)| {
            self.key(key).is_none_or(|locks|
                !locks.holders.get(&txn).is_some_and(|h| h.iter().any(|h| h.covers(*mode)))
                    || locks.holders.iter().any(|(&owner, held)|
                        self.holder_conflicts(txn, key, *mode, owner, *held, group)))
                || self.cross_blocked(txn, key, *mode)
        }) { return; }
        for (key, _) in group { self.provisional_remove(txn, key); }
    }

    fn row_locker(&self, txn: TxnId, key: &HashedKey) -> bool {
        let (table, pk) = match &key.key {
            LockKey::RowKey { table, pk } | LockKey::Row { table, pk } => (*table, pk),
            _ => return false,
        };
        [HashedKey { hash: key.hash, key: LockKey::RowKey { table, pk: pk.clone() } },
         HashedKey { hash: key.hash, key: LockKey::Row { table, pk: pk.clone() } }].iter()
            .any(|k| self.key(k).and_then(|s| s.holders.get(&txn)).is_some_and(|held|
                self.provisional_modes(txn, k).unwrap_or(*held).iter().next().is_some()))
    }

    // Only a whole group compatible with all actual grants can bypass tuple
    // arbitration. Keep pre-group K modes: they are real grants too.
    fn compatible_bypass(&self, txn: TxnId, key: &HashedKey,
        group: &[(HashedKey, LockMode)]) -> bool {
        let row_group = match &key.key {
            LockKey::RowKey { .. } => true,
            LockKey::Row { table, pk } => group.iter().any(|(key, _)|
                matches!(&key.key, LockKey::RowKey { table: t, pk: p } if t == table && p == pk)),
            _ => false,
        };
        row_group && group.iter().all(|(key, mode)| {
            !self.cross_blocked(txn, key, *mode) && self.key(key).is_none_or(|locks|
                locks.holders.iter().all(|(&owner, held)| owner == txn ||
                    !self.provisional_modes(owner, key).unwrap_or(*held).iter()
                        .any(|h| mode.conflicts(h))))
        })
    }

    fn holder_conflicts(&self, txn: TxnId, key: &HashedKey, mode: LockMode,
        owner: TxnId, held: HeldModes, group: &[(HashedKey, LockMode)]) -> bool {
        if owner == txn { return false; }
        let effective = match self.provisional_modes(owner, key) {
            Some(previous) if self.row_locker(txn, key) || self.compatible_bypass(txn, key, group) => previous,
            _ => held,
        };
        effective.iter().any(|h| mode.conflicts(h))
    }

    fn grant(
        &mut self,
        txn: TxnId,
        key: &HashedKey,
        mode: LockMode,
        limit: usize,
        prepared: &mut PreparedKey,
    ) -> Result<(), LockError> {
        let previous = self
            .key(key)
            .and_then(|locks| locks.holders.get(&txn))
            .copied();
        if previous.is_none() {
            let i = self.position(key);
            let actual_shard = self.local.unwrap_or(i);
            let mut stripe = self.manager.registry.stripe(txn);
            let local = stripe.entry(txn).or_default();
            if !matches!(&key.key, LockKey::Table(_)) {
                if let Some((owner, remaining)) = &mut self.reserved && *owner == txn {
                    debug_assert!(*remaining > 0); *remaining -= 1;
                } else {
                    if local.rows >= limit { return Err(LockError::Limit); }
                    local.rows += 1;
                }
            }
            local.shard_mask.insert(actual_shard);
            self.guards.slice_mut()[i].held.entry(txn).or_default().insert(prepared.held.take().expect("prepared holding"));
        }
        if self.key(key).is_none() { self.key_entry(prepared.map.take().expect("prepared key")).or_default(); }
        let locks = self.key_mut(key).expect("granted key");
        if matches!(&key.key, LockKey::Table(_)) {
            if !matches!(mode, LockMode::AccessShare | LockMode::RowShare | LockMode::RowExclusive) {
                locks.slow.extend(locks.holders.keys().copied());
                locks.slow.insert(txn);
            } else if locks.holders.values().any(|held| held.iter().any(|mode|
                !matches!(mode, LockMode::AccessShare | LockMode::RowShare | LockMode::RowExclusive))) {
                locks.slow.insert(txn);
            }
        }
        locks.holders
            .entry(txn)
            .or_default()
            .insert(mode);
        self.index(key, mode);
        Ok(())
    }

    fn remove_waiter(&mut self, id: u64, result: Result<(), LockError>) {
        let Some(i) = self.guards.slice().iter().position(|s| s.waiters.contains_key(&id)) else { return; };
        let shard = &mut self.guards.slice_mut()[i];
        let waiter = shard.waiters.remove(&id).unwrap();
        let pending = shard.by_txn.get_mut(&waiter.txn).expect("indexed waiter");
        pending.retain(|&pending| pending != id);
        if pending.is_empty() { shard.by_txn.remove(&waiter.txn); }
        if let Some(locks) = shard.keys.get_mut(&waiter.key) { locks.queue.retain(|&queued| queued != id); }
        self.manager.registry.pending(waiter.txn, false);
        self.settle(&waiter.key);
        waiter.slot.complete(result);
    }

    /// Record a granted or queued `mode` on `key` in the interval indices.
    fn index(&mut self, key: &HashedKey, mode: LockMode) {
        if self.local.is_some() { return; }
        if let LockKey::Interval { space, .. } = &key.key {
            let live = &mut self.intervals[*space as usize];
            if !live.contains(key) {
                live.insert(key.clone());
            }
        } else if let Some((space, id, tail)) = key.point()
            && mode == LockMode::Exclusive
            && self.indexed[space as usize]
        {
            self.points[space as usize].insert(point_bytes(id, tail), key.clone());
        }
    }

    /// Starts indexing `space`'s Exclusive points (held or queued), once, before
    /// its first interval is checked or recorded.
    fn ensure_indexed(&mut self, space: LockSpace) {
        if self.indexed[space as usize] {
            return;
        }
        let mut points = BTreeMap::new();
        for (key, locks) in self.keys() {
            let Some((point_space, id, tail)) = key.point() else {
                continue;
            };
            let exclusive = locks
                .holders
                .values()
                .any(|held| held.iter().any(|mode| mode == LockMode::Exclusive))
                || locks
                    .queue
                    .iter()
                    .any(|id| self.waiter(id).unwrap().mode == LockMode::Exclusive);
            if point_space == space && exclusive {
                points.insert(point_bytes(id, tail), key.clone());
            }
        }
        self.points[space as usize] = points;
        self.indexed[space as usize] = true;
    }

    /// Drop `key` once it has no holders or queue, and its index entry once it
    /// no longer applies.
    fn settle(&mut self, key: &HashedKey) {
        if self.local.is_some() {
            if self.key(key).is_some_and(|k| k.holders.is_empty() && k.queue.is_empty()) { self.key_remove(key); }
            return;
        }
        if self
            .key(key)
            .is_some_and(|locks| locks.holders.is_empty() && locks.queue.is_empty())
        {
            self.key_remove(key);
        }
        if let LockKey::Interval { space, .. } = &key.key {
            if self.key(key).is_none() {
                self.intervals[*space as usize].remove(key);
            }
        } else if let Some((space, id, tail)) = key.point()
            && !self.points[space as usize].is_empty()
        {
            let exclusive = self.key(key).is_some_and(|locks| {
                locks
                    .holders
                    .values()
                    .any(|held| held.iter().any(|mode| mode == LockMode::Exclusive))
                    || locks
                        .queue
                        .iter()
                        .any(|id| self.waiter(id).unwrap().mode == LockMode::Exclusive)
            });
            if !exclusive {
                self.points[space as usize].remove(point_bytes(id, tail).as_slice());
            }
        }
    }

    /// Other transactions holding a lock on a different key that conflicts with
    /// `mode` on `key`: interval holders containing an Exclusive point request,
    /// or Exclusive point holders inside an interval request.
    fn cross_holders(&self, txn: TxnId, key: &HashedKey, mode: LockMode, out: &mut Vec<TxnId>) {
        if self.local.is_some() { return; }
        let mut push = |key: &HashedKey| {
            if let Some(locks) = self.key(key) {
                for (&owner, held) in &locks.holders {
                    if owner != txn && held.iter().any(|held| mode.conflicts(held)) {
                        out.push(owner);
                    }
                }
            }
        };
        if let LockKey::Interval { space, start, end } = &key.key {
            let points = &self.points[*space as usize];
            if !points.is_empty() {
                for (_, point) in points.range::<[u8], _>((borrowed(start), borrowed(end))) {
                    push(point);
                }
            }
        } else if let Some((space, id, tail)) = key.point()
            && mode.conflicts(LockMode::Shared)
            && !self.intervals[space as usize].is_empty()
        {
            let bytes = point_bytes(id, tail);
            for interval in &self.intervals[space as usize] {
                if let LockKey::Interval { start, end, .. } = &interval.key
                    && interval_contains(start, end, &bytes)
                {
                    push(interval);
                }
            }
        }
    }

    fn cross_blocked(&self, txn: TxnId, key: &HashedKey, mode: LockMode) -> bool {
        let mut holders = Vec::new();
        self.cross_holders(txn, key, mode, &mut holders);
        !holders.is_empty()
    }

    /// Keys across the interval relation from `key` with queued requests, which
    /// releasing `key` may unblock.
    fn cross_queued(&self, key: &HashedKey, out: &mut Vec<HashedKey>) {
        let queued = |key: &HashedKey| {
            self.key(key)
                .is_some_and(|locks| !locks.queue.is_empty())
        };
        if let LockKey::Interval { space, start, end } = &key.key {
            let points = &self.points[*space as usize];
            if !points.is_empty() {
                for (_, point) in points.range::<[u8], _>((borrowed(start), borrowed(end))) {
                    if queued(point) {
                        out.push(point.clone());
                    }
                }
            }
        } else if let Some((space, id, tail)) = key.point()
            && !self.intervals[space as usize].is_empty()
        {
            let bytes = point_bytes(id, tail);
            for interval in &self.intervals[space as usize] {
                if let LockKey::Interval { start, end, .. } = &interval.key
                    && interval_contains(start, end, &bytes)
                    && queued(interval)
                {
                    out.push(interval.clone());
                }
            }
        }
    }

    fn drain_queue(&mut self, key: &HashedKey, limit: usize) {
        if self.key(key).is_none_or(|locks| locks.queue.is_empty()) { return; }
        while let Some(id) = self.key(key).and_then(|locks| {
            locks.queue.iter().copied().find(|id| {
                let waiter = self.waiter(id).unwrap();
                (locks.queue.first() == Some(id)
                    || self.compatible_bypass(waiter.txn, key, &waiter.group))
                    && (waiter.cancelled || (!locks.holders.iter().any(|(&owner, held)|
                        self.holder_conflicts(waiter.txn, key, waiter.mode, owner, *held, &waiter.group))
                        && !self.cross_blocked(waiter.txn, key, waiter.mode)))
            })
        }) {
            let waiter = self.waiter(&id).unwrap();
            let txn = waiter.txn;
            let mode = waiter.mode;
            // A cancel_wait victim is never granted, even if its thread has not
            // observed the flag yet.
            if waiter.cancelled {
                self.remove_waiter(id, Err(LockError::Deadlock));
                continue;
            }
            let group = waiter.group.clone();
            let mut prepared = std::mem::take(&mut self.waiter_mut(&id).unwrap().prepared);
            let result = self.grant(txn, key, mode, limit, &mut prepared);
            self.remove_waiter(id, result);
            if result.is_ok() {
                self.finish_group(txn, &group);
                self.notify_grant(txn, key);
            }
        }
    }

    /// Wait-for edges of the waiter at `position` in `locks.queue`: other
    /// transactions holding a conflicting mode, then conflicting earlier waiters,
    /// then holders of conflicting locks on other keys (interval vs point).
    /// Edges to requests queued on other, overlapping keys are omitted: FIFO is
    /// per key, so a request never waits behind them.
    fn queue_blockers(&self, locks: &KeyState, position: usize, blockers: &mut Vec<TxnId>) {
        let waiter = self.waiter(&locks.queue[position]).unwrap();
        for (&owner, held) in &locks.holders {
            if self.holder_conflicts(waiter.txn, &waiter.key, waiter.mode, owner, *held, &waiter.group) {
                blockers.push(owner);
            }
        }
        for preceding in &locks.queue[..position] {
            let preceding = self.waiter(preceding).unwrap();
            if preceding.txn != waiter.txn && waiter.mode.conflicts(preceding.mode)
                && !self.compatible_bypass(waiter.txn, &waiter.key, &waiter.group) {
                blockers.push(preceding.txn);
            }
        }
        self.cross_holders(waiter.txn, &waiter.key, waiter.mode, blockers);
    }

    /// A grant can add wait-for edges to its owner. If the owner is itself
    /// waiting, wake both ends of every potentially new edge to recheck cycles.
    fn notify_grant(&self, txn: TxnId, key: &HashedKey) {
        if self.local.is_some() { return; } // S1 excludes every other pending request.
        let pending = self.pending(txn);
        if pending.is_empty() { return; }
        let mut notify: HashSet<u64> = pending.iter().copied().collect();
        let mut keys = vec![key.clone()];
        self.cross_queued(key, &mut keys);
        for key in keys {
            if let Some(locks) = self.key(&key) {
                notify.extend(locks.queue.iter().copied());
            }
        }
        for id in notify {
            self.waiter(&id).unwrap().slot.recheck();
        }
    }

    fn txn_blockers(&self, txn: TxnId, blockers: &mut Vec<TxnId>) {
        for id in &self.pending(txn) {
            let waiter = self.waiter(id).unwrap();
            let locks = self.key(&waiter.key).unwrap();
            if let Some(position) = locks.queue.iter().position(|queued| queued == id) {
                self.queue_blockers(locks, position, blockers);
            }
        }
    }

    fn has_cycle(&self, requester: TxnId) -> bool {
        let mut visited = HashSet::new();
        let mut stack = vec![requester];
        let mut blockers = Vec::new();
        while let Some(txn) = stack.pop() {
            if !visited.insert(txn) {
                continue;
            }
            // Compute edges only for reachable transactions, never all held keys.
            blockers.clear();
            self.txn_blockers(txn, &mut blockers);
            for &blocker in &blockers {
                if blocker == requester {
                    return true;
                }
                if !visited.contains(&blocker) {
                    stack.push(blocker);
                }
            }
        }
        false
    }
}

#[cfg(test)]
#[path = "locks_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "locks_deadlock_tests.rs"]
mod deadlock_tests;

#[cfg(test)]
#[path = "locks_interval_tests.rs"]
mod interval_tests;

#[cfg(test)]
#[path = "locks_wakeup_tests.rs"]
mod wakeup_tests;

#[cfg(test)]
#[path = "locks_row_tests.rs"]
mod row_tests;

fn restore_modes_never_recreates_released_ownership_at(shards: usize) {
    let locks = LockManager::new(LockConfig { shards, ..Default::default() });
    let txn = TxnId(1);
    let other = TxnId(2);
    let key = LockKey::Row { table: 1, pk: vec![1] };
    locks.acquire(txn, key.clone(), LockMode::Shared, None, &|| false).unwrap();
    locks.acquire(other, key.clone(), LockMode::Shared, None, &|| false).unwrap();
    let previous = locks.modes_on(txn, &[(key.clone(), LockMode::Shared)]);
    locks.release_all(txn);
    locks.restore_modes(txn, &previous);
    assert!(locks.held(txn).is_empty());
    {
        let state = locks.global().unwrap();
        assert!(state.guards.slice().iter().all(|s| !s.held.contains_key(&txn)));
        assert_eq!(locks.registry.rows(txn), 0);
        assert!(!state.key(&locks.prepare(key.clone())).unwrap().holders.contains_key(&txn));
    }
    // Even when the key is held again, missing historical modes stay absent.
    locks.release_all(other);
    locks.acquire(txn, key.clone(), LockMode::Exclusive, None, &|| false).unwrap();
    locks.restore_modes(txn, &previous);
    assert!(locks.held(txn).is_empty());
    let state = locks.global().unwrap();
    assert!(state.guards.slice().iter().all(|s| !s.held.contains_key(&txn)));
    assert_eq!(locks.registry.rows(txn), 0);
}

#[cfg(test)]
#[path = "locks_reference.rs"]
mod reference;

#[cfg(test)]
#[path = "locks_shards2_tests.rs"]
mod shards2;

#[cfg(test)]
#[path = "locks_shards64_tests.rs"]
mod shards64;

#[cfg(test)]
#[path = "locks_sharding_tests.rs"]
mod sharding_tests;

#[test]
fn restore_modes_never_recreates_released_ownership() { restore_modes_never_recreates_released_ownership_at(1); }

#[test]
fn restore_modes_never_recreates_released_ownership_shards2() { restore_modes_never_recreates_released_ownership_at(2); }

#[test]
fn restore_modes_never_recreates_released_ownership_shards64() { restore_modes_never_recreates_released_ownership_at(64); }

// `grant` counts every holding except `Table` keys toward
// `max_row_locks_per_txn`, so restoring a Table holding must not uncount one.
// Before the fix, restoring a row and a table holding to empty decremented the
// count twice: a debug build panicked and a release build wrapped to
// usize::MAX, so the transaction's next row lock failed with `Limit`.
#[cfg(test)]
fn restore_modes_does_not_uncount_table_holdings_at(shards: usize) {
    let locks = LockManager::new(LockConfig { shards, ..LockConfig::default() });
    let txn = TxnId(1);
    let table = LockKey::Table(7);
    let first = LockKey::Row { table: 7, pk: vec![1] };
    let second = LockKey::Row { table: 7, pk: vec![2] };
    let keys = [(table.clone(), LockMode::RowExclusive), (first.clone(), LockMode::Exclusive)];
    let before = locks.modes_on(txn, &keys);
    locks.acquire(txn, second.clone(), LockMode::Exclusive, None, &|| false).unwrap();
    locks.acquire(txn, table.clone(), LockMode::RowExclusive, None, &|| false).unwrap();
    locks.acquire(txn, first.clone(), LockMode::Exclusive, None, &|| false).unwrap();
    assert_eq!(locks.registry.rows(txn), 2);
    // Restoring only the table holding leaves both row holdings counted.
    locks.restore_modes(txn, &before[..1]);
    assert_eq!(locks.registry.rows(txn), 2);
    locks.acquire(txn, table.clone(), LockMode::RowExclusive, None, &|| false).unwrap();
    // Restoring the table and one row holding uncounts exactly that row.
    locks.restore_modes(txn, &before);
    assert_eq!(locks.registry.rows(txn), 1);
    locks.restore_modes(txn, &locks.modes_on(TxnId(99), &[(second.clone(), LockMode::Exclusive)]));
    {
        let state = locks.global().unwrap();
        assert_eq!(locks.registry.rows(txn), 0);
        assert!(state.guards.slice().iter().all(|s| !s.held.contains_key(&txn)));
    }
    // The count stays exact, so the limit still admits the next row lock.
    let limited = LockManager::new(LockConfig { shards, max_row_locks_per_txn: 1, ..LockConfig::default() });
    let before = limited.modes_on(txn, &keys);
    limited.acquire(txn, table.clone(), LockMode::RowExclusive, None, &|| false).unwrap();
    limited.acquire(txn, first.clone(), LockMode::Exclusive, None, &|| false).unwrap();
    limited.restore_modes(txn, &before);
    assert!(limited.held(txn).is_empty());
    assert_eq!(limited.acquire(txn, second, LockMode::Exclusive, None, &|| false), Ok(()));
    assert_eq!(limited.acquire(txn, first, LockMode::Exclusive, None, &|| false), Err(LockError::Limit));
}

#[test]
fn restore_modes_does_not_uncount_table_holdings() { restore_modes_does_not_uncount_table_holdings_at(1); }

#[test]
fn restore_modes_does_not_uncount_table_holdings_shards2() { restore_modes_does_not_uncount_table_holdings_at(2); }

#[test]
fn restore_modes_does_not_uncount_table_holdings_shards64() { restore_modes_does_not_uncount_table_holdings_at(64); }

#[cfg(test)]
#[path = "locks_shards1_tests.rs"]
mod shards1_tests;
