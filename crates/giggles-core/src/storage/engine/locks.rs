//! In-memory transaction locks. All bookkeeping is protected by one mutex.
//!
//! A shared condition variable deliberately wakes every waiter when a queue changes:
//! each waiter checks its own result under the mutex, so notifications cannot grant
//! a lock twice or bypass FIFO. Cancellation callbacks run outside that mutex and
//! are polled at `deadlock_recheck`, since the callback has no notification channel.
//!
//! Interval (gap) locks: `LockKey::Interval` is a Shared lock over a byte range of
//! one `LockSpace`. It conflicts with other transactions' Exclusive point locks
//! whose byte form lies inside it, in either order. Two indices keep that cheap:
//! the live interval keys per space, and the ordered byte forms of point keys that
//! have an Exclusive holder or a queued Exclusive request. A point request checks
//! intervals only when its space has one (an O(1) emptiness check otherwise).
//! FIFO is per key: a request on one key never waits behind queued requests on
//! another key, so a stream of overlapping intervals can delay a queued writer.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::ops::Bound;
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

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

/// `start >= end` per bounds. A non-empty interval is a valid `BTreeSet::range`.
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

#[derive(Clone, Copy, Debug, Default)]
struct HeldModes(u16);

impl HeldModes {
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
    pub(crate) max_row_locks_per_txn: usize,
    pub(crate) deadlock_recheck: Duration,
}

impl Default for LockConfig {
    fn default() -> Self {
        Self {
            max_row_locks_per_txn: 1_000_000,
            deadlock_recheck: Duration::from_millis(100),
        }
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

#[derive(Debug)]
struct Waiter {
    txn: TxnId,
    key: LockKey,
    mode: LockMode,
    upgrade: bool,
    // Set by `cancel_wait`: the request ends `Deadlock` instead of being granted.
    cancelled: bool,
}

#[derive(Debug, Default)]
struct State {
    keys: HashMap<LockKey, KeyState>,
    waiters: HashMap<u64, Waiter>,
    // A removed waiter keeps its terminal result until its acquiring thread
    // observes it. In particular release_all terminates pending calls Cancelled.
    completed: HashMap<u64, Result<(), LockError>>,
    row_counts: HashMap<TxnId, usize>,
    held_keys: HashMap<TxnId, HashSet<LockKey>>,
    next_waiter: u64,
    // Per `LockSpace`: interval keys with holders or queued requests.
    intervals: [HashSet<LockKey>; 2],
    // Per `LockSpace`: byte forms of point keys with an Exclusive holder or a
    // queued Exclusive request. Maintained only once the space has seen an
    // interval (`indexed`), so a manager that never takes a gap lock (every
    // single-node one) pays nothing for it.
    points: [BTreeSet<Vec<u8>>; 2],
    indexed: [bool; 2],
}

#[derive(Debug)]
pub(crate) struct LockManager {
    config: LockConfig,
    state: Mutex<State>,
    changed: Condvar,
}

impl LockManager {
    pub(crate) fn new(config: LockConfig) -> Self {
        Self {
            config,
            state: Mutex::new(State::default()),
            changed: Condvar::new(),
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
        if matches!(key, LockKey::Table(_)) != mode.is_table() {
            return Err(LockError::Misuse);
        }
        if let LockKey::Interval { start, end, .. } = &key {
            if mode != LockMode::Shared {
                return Err(LockError::Misuse);
            }
            // An empty interval protects nothing: granted without being recorded.
            if interval_empty(start, end) {
                return Ok(());
            }
        }
        let mut state = self.state.lock().map_err(|_| LockError::Misuse)?;
        if let LockKey::Interval { space, .. } = &key {
            state.ensure_indexed(*space);
        }
        let held = state
            .keys
            .get(&key)
            .and_then(|k| k.holders.get(&txn))
            .copied();
        if held.is_some_and(|held| held.iter().any(|held| held.covers(mode))) {
            if mode.is_table() {
                state.keys.get_mut(&key).unwrap().holders.get_mut(&txn).unwrap().insert(mode);
            }
            return Ok(());
        }
        let immediate = state.keys.get(&key).is_none_or(|locks| {
            (held.is_some() || locks.queue.is_empty())
                && !locks.holders.iter().any(|(&owner, held)| {
                    owner != txn && held.iter().any(|held| mode.conflicts(held))
                })
        }) && !state.cross_blocked(txn, &key, mode);
        // A zero deadline is a nonblocking attempt: a compatible immediate
        // acquisition, including reentrance and upgrades, still succeeds.
        if immediate {
            state.grant(txn, &key, mode, self.config.max_row_locks_per_txn)?;
            state.drain_queue(&key, self.config.max_row_locks_per_txn);
            self.changed.notify_all();
            return Ok(());
        }

        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            return Err(LockError::Timeout);
        }
        let id = state.next_waiter;
        state.next_waiter = state.next_waiter.checked_add(1).ok_or(LockError::Misuse)?;
        let upgrade = held.is_some();
        // A request blocked only across keys queues on a fresh entry.
        state.keys.entry(key.clone()).or_default();
        let position = if upgrade {
            state.keys[&key]
                .queue
                .iter()
                .position(|id| !state.waiters[id].upgrade)
                .unwrap_or(state.keys[&key].queue.len())
        } else {
            state.keys[&key].queue.len()
        };
        state.waiters.insert(
            id,
            Waiter {
                txn,
                key: key.clone(),
                mode,
                upgrade,
                cancelled: false,
            },
        );
        state
            .keys
            .get_mut(&key)
            .expect("blocked key exists")
            .queue
            .insert(position, id);
        state.index(&key, mode);
        if state.has_cycle(txn) {
            state.remove_waiter(id, Err(LockError::Deadlock));
            state.drain_queue(&key, self.config.max_row_locks_per_txn);
            self.changed.notify_all();
        }

        loop {
            if let Some(result) = state.completed.remove(&id) {
                return result;
            }
            // User callbacks may inspect this manager without deadlocking it.
            drop(state);
            let cancelled = cancel();
            state = self.state.lock().map_err(|_| LockError::Misuse)?;
            if let Some(result) = state.completed.remove(&id) {
                return result;
            }
            let error = if state
                .waiters
                .get(&id)
                .is_some_and(|waiter| waiter.cancelled)
            {
                Some(LockError::Deadlock)
            } else if cancelled {
                Some(LockError::Cancelled)
            } else if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                Some(LockError::Timeout)
            } else if state.has_cycle(txn) {
                Some(LockError::Deadlock)
            } else {
                None
            };
            if let Some(error) = error {
                state.remove_waiter(id, Err(error));
                state.drain_queue(&key, self.config.max_row_locks_per_txn);
                self.changed.notify_all();
                return state
                    .completed
                    .remove(&id)
                    .expect("removed waiter has result");
            }
            let interval = deadline.map_or(self.config.deadlock_recheck, |deadline| {
                self.config
                    .deadlock_recheck
                    .min(deadline.saturating_duration_since(Instant::now()))
            });
            let (next, _) = self
                .changed
                .wait_timeout(state, interval)
                .map_err(|_| LockError::Misuse)?;
            state = next;
        }
    }

    /// Release held locks and terminate this transaction's pending calls with
    /// `Cancelled`. Cleanup still operates if the bookkeeping mutex is poisoned.
    pub(crate) fn release_all(&self, txn: TxnId) {
        // Cleanup/inspection remain useful after poisoning; acquire reports Misuse.
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        // Metadata transactions hold no locks. In particular, dropping one must
        // not scan all row/index locks owned by an in-progress COPY statement.
        let mut affected = state.held_keys.remove(&txn).unwrap_or_default();
        let mut unblocked = Vec::new();
        for key in &affected {
            if let Some(locks) = state.keys.get_mut(key) {
                locks.holders.remove(&txn);
                locks.slow.remove(&txn);
            }
            state.cross_queued(key, &mut unblocked);
        }
        affected.extend(unblocked);
        state.row_counts.remove(&txn);
        let pending: Vec<_> = state
            .waiters
            .iter()
            .filter_map(|(&id, waiter)| (waiter.txn == txn).then_some(id))
            .collect();
        for id in pending {
            affected.insert(state.waiters[&id].key.clone());
            state.remove_waiter(id, Err(LockError::Cancelled));
        }
        // Only releasing a holder or cancelling a queued request can unblock
        // this key's waiters, or (across keys) requests queued on a key that a
        // released lock overlapped. Leave unrelated queues and holders untouched.
        for key in affected {
            state.drain_queue(&key, self.config.max_row_locks_per_txn);
            state.settle(&key);
        }
        self.changed.notify_all();
    }

    /// Transactions blocking `txn`'s pending request(s): conflicting holders plus
    /// conflicting waiters queued ahead of it. Sorted and deduplicated; empty
    /// when `txn` is not waiting. Feeds the cross-range deadlock detector.
    pub(crate) fn blockers(&self, txn: TxnId) -> Vec<TxnId> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let mut blockers = Vec::new();
        for (&id, waiter) in state.waiters.iter().filter(|(_, waiter)| waiter.txn == txn) {
            let Some(locks) = state.keys.get(&waiter.key) else {
                continue;
            };
            if let Some(position) = locks.queue.iter().position(|&queued| queued == id) {
                state.queue_blockers(locks, position, &mut blockers);
            }
        }
        blockers.sort_unstable();
        blockers.dedup();
        blockers
    }

    /// End `txn`'s pending acquire(s) with `Deadlock` (a cross-range victim).
    /// Sets the per-request cancel flag and wakes the waiter, which removes its
    /// own queue entry. No-op when `txn` is not waiting.
    pub(crate) fn cancel_wait(&self, txn: TxnId) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let mut any = false;
        for waiter in state
            .waiters
            .values_mut()
            .filter(|waiter| waiter.txn == txn)
        {
            waiter.cancelled = true;
            any = true;
        }
        if any {
            self.changed.notify_all();
        }
    }

    /// Snapshot held locks, recovering the state for inspection after poisoning.
    pub(crate) fn held(&self, txn: TxnId) -> Vec<(LockKey, LockMode)> {
        self.state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .keys
            .iter()
            .flat_map(|(key, locks)| {
                locks
                    .holders
                    .get(&txn)
                    .into_iter()
                    .flat_map(move |held| held.iter().map(move |mode| (key.clone(), mode)))
            })
            .collect()
    }

    pub(crate) fn fastpath(&self, txn: TxnId, table: u64, mode: LockMode) -> bool {
        matches!(mode, LockMode::AccessShare | LockMode::RowShare | LockMode::RowExclusive)
            && self.state.lock().unwrap_or_else(|p| p.into_inner()).keys.get(&LockKey::Table(table))
                .is_some_and(|locks| !locks.slow.contains(&txn))
    }

    /// Count live queue entries, recovering state for inspection after poisoning.
    pub(crate) fn waiting_count(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .waiters
            .len()
    }
}

impl State {
    fn grant(
        &mut self,
        txn: TxnId,
        key: &LockKey,
        mode: LockMode,
        limit: usize,
    ) -> Result<(), LockError> {
        let previous = self
            .keys
            .get(key)
            .and_then(|locks| locks.holders.get(&txn))
            .copied();
        if previous.is_none() && !matches!(key, LockKey::Table(_)) {
            let count = self.row_counts.entry(txn).or_default();
            if *count >= limit {
                return Err(LockError::Limit);
            }
            *count += 1;
        }
        if previous.is_none() {
            self.held_keys.entry(txn).or_default().insert(key.clone());
        }
        if matches!(key, LockKey::Table(_)) {
            let locks = self.keys.entry(key.clone()).or_default();
            if !matches!(mode, LockMode::AccessShare | LockMode::RowShare | LockMode::RowExclusive) {
                locks.slow.extend(locks.holders.keys().copied());
                locks.slow.insert(txn);
            } else if locks.holders.values().any(|held| held.iter().any(|mode|
                !matches!(mode, LockMode::AccessShare | LockMode::RowShare | LockMode::RowExclusive))) {
                locks.slow.insert(txn);
            }
        }
        self.keys
            .entry(key.clone())
            .or_default()
            .holders
            .entry(txn)
            .or_default()
            .insert(mode);
        self.index(key, mode);
        Ok(())
    }

    fn remove_waiter(&mut self, id: u64, result: Result<(), LockError>) {
        if let Some(waiter) = self.waiters.remove(&id) {
            if let Some(locks) = self.keys.get_mut(&waiter.key) {
                locks.queue.retain(|&queued| queued != id);
            }
            self.settle(&waiter.key);
            self.completed.insert(id, result);
        }
    }

    /// Record a granted or queued `mode` on `key` in the interval indices.
    fn index(&mut self, key: &LockKey, mode: LockMode) {
        if let LockKey::Interval { space, .. } = key {
            let live = &mut self.intervals[*space as usize];
            if !live.contains(key) {
                live.insert(key.clone());
            }
        } else if let Some((space, id, tail)) = key.point()
            && mode == LockMode::Exclusive
            && self.indexed[space as usize]
        {
            self.points[space as usize].insert(point_bytes(id, tail));
        }
    }

    /// Starts indexing `space`'s Exclusive points (held or queued), once, before
    /// its first interval is checked or recorded.
    fn ensure_indexed(&mut self, space: LockSpace) {
        if self.indexed[space as usize] {
            return;
        }
        let mut points = BTreeSet::new();
        for (key, locks) in &self.keys {
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
                    .any(|id| self.waiters[id].mode == LockMode::Exclusive);
            if point_space == space && exclusive {
                points.insert(point_bytes(id, tail));
            }
        }
        self.points[space as usize] = points;
        self.indexed[space as usize] = true;
    }

    /// Drop `key` once it has no holders or queue, and its index entry once it
    /// no longer applies.
    fn settle(&mut self, key: &LockKey) {
        if self
            .keys
            .get(key)
            .is_some_and(|locks| locks.holders.is_empty() && locks.queue.is_empty())
        {
            self.keys.remove(key);
        }
        if let LockKey::Interval { space, .. } = key {
            if !self.keys.contains_key(key) {
                self.intervals[*space as usize].remove(key);
            }
        } else if let Some((space, id, tail)) = key.point()
            && !self.points[space as usize].is_empty()
        {
            let exclusive = self.keys.get(key).is_some_and(|locks| {
                locks
                    .holders
                    .values()
                    .any(|held| held.iter().any(|mode| mode == LockMode::Exclusive))
                    || locks
                        .queue
                        .iter()
                        .any(|id| self.waiters[id].mode == LockMode::Exclusive)
            });
            if !exclusive {
                self.points[space as usize].remove(point_bytes(id, tail).as_slice());
            }
        }
    }

    /// Other transactions holding a lock on a different key that conflicts with
    /// `mode` on `key`: interval holders containing an Exclusive point request,
    /// or Exclusive point holders inside an interval request.
    fn cross_holders(&self, txn: TxnId, key: &LockKey, mode: LockMode, out: &mut Vec<TxnId>) {
        let mut push = |key: &LockKey| {
            if let Some(locks) = self.keys.get(key) {
                for (&owner, held) in &locks.holders {
                    if owner != txn && held.iter().any(|held| mode.conflicts(held)) {
                        out.push(owner);
                    }
                }
            }
        };
        if let LockKey::Interval { space, start, end } = key {
            let points = &self.points[*space as usize];
            if !points.is_empty() {
                for bytes in points.range::<[u8], _>((borrowed(start), borrowed(end))) {
                    push(&point_key(*space, bytes));
                }
            }
        } else if let Some((space, id, tail)) = key.point()
            && mode.conflicts(LockMode::Shared)
            && !self.intervals[space as usize].is_empty()
        {
            let bytes = point_bytes(id, tail);
            for interval in &self.intervals[space as usize] {
                if let LockKey::Interval { start, end, .. } = interval
                    && interval_contains(start, end, &bytes)
                {
                    push(interval);
                }
            }
        }
    }

    fn cross_blocked(&self, txn: TxnId, key: &LockKey, mode: LockMode) -> bool {
        let mut holders = Vec::new();
        self.cross_holders(txn, key, mode, &mut holders);
        !holders.is_empty()
    }

    /// Keys across the interval relation from `key` with queued requests, which
    /// releasing `key` may unblock.
    fn cross_queued(&self, key: &LockKey, out: &mut Vec<LockKey>) {
        let queued = |key: &LockKey| {
            self.keys
                .get(key)
                .is_some_and(|locks| !locks.queue.is_empty())
        };
        if let LockKey::Interval { space, start, end } = key {
            let points = &self.points[*space as usize];
            if !points.is_empty() {
                for bytes in points.range::<[u8], _>((borrowed(start), borrowed(end))) {
                    let point = point_key(*space, bytes);
                    if queued(&point) {
                        out.push(point);
                    }
                }
            }
        } else if let Some((space, id, tail)) = key.point()
            && !self.intervals[space as usize].is_empty()
        {
            let bytes = point_bytes(id, tail);
            for interval in &self.intervals[space as usize] {
                if let LockKey::Interval { start, end, .. } = interval
                    && interval_contains(start, end, &bytes)
                    && queued(interval)
                {
                    out.push(interval.clone());
                }
            }
        }
    }

    fn drain_queue(&mut self, key: &LockKey, limit: usize) {
        while let Some(&id) = self.keys.get(key).and_then(|locks| locks.queue.first()) {
            let waiter = &self.waiters[&id];
            let txn = waiter.txn;
            let mode = waiter.mode;
            // A cancel_wait victim is never granted, even if its thread has not
            // observed the flag yet.
            if waiter.cancelled {
                self.remove_waiter(id, Err(LockError::Deadlock));
                continue;
            }
            if self.keys[key]
                .holders
                .iter()
                .any(|(&owner, held)| owner != txn && held.iter().any(|held| mode.conflicts(held)))
                || self.cross_blocked(txn, key, mode)
            {
                break;
            }
            let result = self.grant(txn, key, mode, limit);
            self.remove_waiter(id, result);
        }
    }

    /// Wait-for edges of the waiter at `position` in `locks.queue`: other
    /// transactions holding a conflicting mode, then conflicting earlier waiters,
    /// then holders of conflicting locks on other keys (interval vs point).
    /// Edges to requests queued on other, overlapping keys are omitted: FIFO is
    /// per key, so a request never waits behind them.
    fn queue_blockers(&self, locks: &KeyState, position: usize, blockers: &mut Vec<TxnId>) {
        let waiter = &self.waiters[&locks.queue[position]];
        for (&owner, held) in &locks.holders {
            if owner != waiter.txn && held.iter().any(|mode| waiter.mode.conflicts(mode)) {
                blockers.push(owner);
            }
        }
        for preceding in &locks.queue[..position] {
            let preceding = &self.waiters[preceding];
            if preceding.txn != waiter.txn && waiter.mode.conflicts(preceding.mode) {
                blockers.push(preceding.txn);
            }
        }
        self.cross_holders(waiter.txn, &waiter.key, waiter.mode, blockers);
    }

    fn has_cycle(&self, requester: TxnId) -> bool {
        let mut edges: HashMap<TxnId, Vec<TxnId>> = HashMap::new();
        for locks in self.keys.values() {
            for (position, id) in locks.queue.iter().enumerate() {
                let blockers = edges.entry(self.waiters[id].txn).or_default();
                self.queue_blockers(locks, position, blockers);
            }
        }
        let mut visited = HashMap::new();
        let mut stack = edges.get(&requester).cloned().unwrap_or_default();
        while let Some(txn) = stack.pop() {
            if txn == requester {
                return true;
            }
            if visited.insert(txn, ()).is_none() {
                stack.extend(edges.get(&txn).into_iter().flatten().copied());
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
