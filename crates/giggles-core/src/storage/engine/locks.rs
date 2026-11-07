//! In-memory transaction locks. All bookkeeping is protected by one mutex.
//!
//! A shared condition variable deliberately wakes every waiter when a queue changes:
//! each waiter checks its own result under the mutex, so notifications cannot grant
//! a lock twice or bypass FIFO. Cancellation callbacks run outside that mutex and
//! are polled at `deadlock_recheck`, since the callback has no notification channel.

use std::collections::HashMap;
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

/// `Unique { index: CATALOG_NAME_INDEX, tuple: name }` in `Exclusive` mode
/// serializes concurrent DDL on the same relation name.
pub(crate) const CATALOG_NAME_INDEX: u64 = u64::MAX;

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
    RowExclusive,
    Share,
    AccessExclusive,
    Shared,
    Exclusive,
}

impl LockMode {
    fn conflict_set(self) -> u8 {
        match self {
            Self::AccessShare => 0b1000,
            Self::RowExclusive => 0b1100,
            Self::Share => 0b1010,
            Self::AccessExclusive => 0b1111,
            Self::Shared => 0b10,
            Self::Exclusive => 0b11,
        }
    }

    fn is_table(self) -> bool {
        matches!(
            self,
            Self::AccessShare | Self::RowExclusive | Self::Share | Self::AccessExclusive
        )
    }

    pub(crate) fn covers(self, requested: Self) -> bool {
        self.is_table() == requested.is_table()
            && self.conflict_set() & requested.conflict_set() == requested.conflict_set()
    }

    fn conflicts(self, other: Self) -> bool {
        match self {
            Self::AccessShare => matches!(other, Self::AccessExclusive),
            Self::RowExclusive => matches!(other, Self::Share | Self::AccessExclusive),
            Self::Share => matches!(other, Self::RowExclusive | Self::AccessExclusive),
            Self::AccessExclusive => true,
            Self::Shared => matches!(other, Self::Exclusive),
            Self::Exclusive => true,
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct HeldModes(u8);

impl HeldModes {
    fn insert(&mut self, mode: LockMode) {
        self.0 |= 1 << mode as u8;
    }

    fn iter(self) -> impl Iterator<Item = LockMode> {
        use LockMode::*;
        [
            AccessShare,
            RowExclusive,
            Share,
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
}

#[derive(Debug, Default)]
struct State {
    keys: HashMap<LockKey, KeyState>,
    waiters: HashMap<u64, Waiter>,
    // A removed waiter keeps its terminal result until its acquiring thread
    // observes it. In particular release_all terminates pending calls Cancelled.
    completed: HashMap<u64, Result<(), LockError>>,
    row_counts: HashMap<TxnId, usize>,
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
            let error = if cancelled {
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
        for locks in state.keys.values_mut() {
            locks.holders.remove(&txn);
        }
        state.row_counts.remove(&txn);
        let pending: Vec<_> = state
            .waiters
            .iter()
            .filter_map(|(&id, waiter)| (waiter.txn == txn).then_some(id))
            .collect();
        for id in pending {
            state.remove_waiter(id, Err(LockError::Cancelled));
        }
        let keys: Vec<_> = state.keys.keys().cloned().collect();
        for key in keys {
            state.drain_queue(&key, self.config.max_row_locks_per_txn);
        }
        state
            .keys
            .retain(|_, locks| !locks.holders.is_empty() || !locks.queue.is_empty());
        self.changed.notify_all();
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

    fn has_cycle(&self, requester: TxnId) -> bool {
        let mut edges: HashMap<TxnId, Vec<TxnId>> = HashMap::new();
        for locks in self.keys.values() {
            for (position, id) in locks.queue.iter().enumerate() {
                let waiter = &self.waiters[id];
                let blockers = edges.entry(waiter.txn).or_default();
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
