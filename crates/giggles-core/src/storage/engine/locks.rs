//! In-memory transaction locks. All bookkeeping is protected by one mutex.
//!
//! A shared condition variable deliberately wakes every waiter when a queue changes:
//! each waiter checks its own result under the mutex, so notifications cannot grant
//! a lock twice or bypass FIFO. Cancellation callbacks run outside that mutex and
//! are polled at `deadlock_recheck`, since the callback has no notification channel.

use std::collections::{HashMap, HashSet};
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

/// `Unique { index: CATALOG_NAME_INDEX, tuple: name }` in `Exclusive` mode
/// serializes concurrent DDL on the same relation name.
pub(crate) const CATALOG_NAME_INDEX: u64 = u64::MAX;

/// Reserved table lock serializing kernel image read/modify/write transactions.
pub(crate) const KERNEL_LOCK_TABLE: u64 = u64::MAX - 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct TxnId(pub u64);

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum LockKey {
    Table(u64),
    Row { table: u64, pk: Vec<u8> },
    Unique { index: u64, tuple: Vec<u8> },
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
        let mut state = self.state.lock().map_err(|_| LockError::Misuse)?;
        let held = state
            .keys
            .get(&key)
            .and_then(|k| k.holders.get(&txn))
            .copied();
        if held.is_some_and(|held| held.iter().any(|held| held.covers(mode))) {
            return Ok(());
        }
        let immediate = state.keys.get(&key).is_none_or(|locks| {
            (held.is_some() || locks.queue.is_empty())
                && !locks.holders.iter().any(|(&owner, held)| {
                    owner != txn && held.iter().any(|held| mode.conflicts(held))
                })
        });
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
            let error = if state.waiters.get(&id).is_some_and(|waiter| waiter.cancelled) {
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
        for key in &affected {
            if let Some(locks) = state.keys.get_mut(key) {
                locks.holders.remove(&txn);
            }
        }
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
        // this key's waiters. Leave unrelated queues and holders untouched.
        for key in affected {
            state.drain_queue(&key, self.config.max_row_locks_per_txn);
            if state.keys.get(&key).is_some_and(|locks|
                locks.holders.is_empty() && locks.queue.is_empty()) {
                state.keys.remove(&key);
            }
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
        for waiter in state.waiters.values_mut().filter(|waiter| waiter.txn == txn) {
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
        self.keys
            .entry(key.clone())
            .or_default()
            .holders
            .entry(txn)
            .or_default()
            .insert(mode);
        Ok(())
    }

    fn remove_waiter(&mut self, id: u64, result: Result<(), LockError>) {
        if let Some(waiter) = self.waiters.remove(&id) {
            if let Some(locks) = self.keys.get_mut(&waiter.key) {
                locks.queue.retain(|&queued| queued != id);
            }
            self.completed.insert(id, result);
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
            {
                break;
            }
            let result = self.grant(txn, key, mode, limit);
            self.remove_waiter(id, result);
        }
    }

    /// Wait-for edges of the waiter at `position` in `locks.queue`: other
    /// transactions holding a conflicting mode, then conflicting earlier waiters.
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
