#![allow(dead_code)]
// In-memory transaction locks. All bookkeeping is protected by one mutex.
//
// Each queued request has its own condition variable and checks its terminal result
// under the mutex, so completion cannot be missed or bypass FIFO. Only completed
// or cancelled requests are woken, except when a grant to a transaction with another
// pending request can close a cycle: its requests and the granted key's queue and
// overlapping queues are woken too. A new cycle includes either a newly queued
// request (checked at queue time) or such a grant, so no cycle notification is lost.
// Optional deferred detection waits until `deadlock_timeout`; callbacks still run
// outside the mutex and are polled at `deadlock_recheck`.
//
// Interval (gap) locks: `LockKey::Interval` is a Shared lock over a byte range of
// one `LockSpace`. It conflicts with other transactions' Exclusive point locks
// whose byte form lies inside it, in either order. Two indices keep that cheap:
// the live interval keys per space, and the ordered byte forms of point keys that
// have an Exclusive holder or a queued Exclusive request. A point request checks
// intervals only when its space has one (an O(1) emptiness check otherwise).
// FIFO is per key: a request on one key never waits behind queued requests on
// another key, so a stream of overlapping intervals can delay a queued writer.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Condvar, Mutex};
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use super::{TxnId, LockSpace, LockKey, LockMode, HeldModes, LockConfig, LockError, point_bytes, point_key, borrowed, interval_contains, interval_empty};

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
    group: Vec<(LockKey, LockMode)>,
    // Set by `cancel_wait`: the request ends `Deadlock` instead of being granted.
    cancelled: bool,
    changed: Arc<Condvar>,
    #[cfg(test)]
    sleeping: bool,
}

#[derive(Debug, Default)]
struct State {
    keys: HashMap<LockKey, KeyState>,
    waiters: HashMap<u64, Waiter>,
    by_txn: HashMap<TxnId, Vec<u64>>,
    // A removed waiter keeps its terminal result until its acquiring thread
    // observes it. In particular release_all terminates pending calls Cancelled.
    completed: HashMap<u64, Result<(), LockError>>,
    row_counts: HashMap<TxnId, usize>,
    held_keys: HashMap<TxnId, HashSet<LockKey>>,
    next_waiter: u64,
    #[cfg(test)]
    deadlock_victims: HashSet<TxnId>,
    // Group grants stay provisional until every member validates atomically.
    provisional: HashMap<(TxnId, LockKey), HeldModes>,
    // Per `LockSpace`: interval keys with holders or queued requests.
    intervals: [HashSet<LockKey>; 2],
    // Per `LockSpace`: byte forms of point keys with an Exclusive holder or a
    // queued Exclusive request. Maintained only once the space has seen an
    // interval (`indexed`), so a manager that never takes a gap lock (every
    // single-node one) pays nothing for it.
    points: [BTreeSet<Vec<u8>>; 2],
    indexed: [bool; 2],
}

struct GroupContext<'a> {
    requested: &'a [(LockKey, LockMode)],
}

#[derive(Debug)]
pub(crate) struct ReferenceLockManager {
    config: LockConfig,
    state: Mutex<State>,
    #[cfg(test)]
    waiter_wakeups: AtomicUsize,
    #[cfg(test)]
    release_all_calls: AtomicUsize,
}

impl ReferenceLockManager {
    pub(crate) fn new(config: LockConfig) -> Self {
        Self {
            config,
            state: Mutex::new(State::default()),
            #[cfg(test)]
            waiter_wakeups: AtomicUsize::new(0),
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
        self.acquire_group_member(txn, key.clone(), mode, deadline, cancel, GroupContext { requested: &[(key, mode)] })
    }

    // Execute precisely one member's immediate protocol, including its notifications.
    fn try_member_locked(
        &self, state: &mut State, txn: TxnId, key: &LockKey, mode: LockMode,
        group: &[(LockKey, LockMode)],
    ) -> Result<Option<()>, LockError> {
        let held = state
            .keys
            .get(key)
            .and_then(|k| k.holders.get(&txn))
            .copied();
        if (state.provisional.is_empty() || !state.provisional.contains_key(&(txn, key.clone())))
            && held.is_some_and(|held| held.iter().any(|held| held.covers(mode))) {
            if mode.is_table() {
                state.keys.get_mut(key).unwrap().holders.get_mut(&txn).unwrap().insert(mode);
            }
            return Ok(Some(()));
        }
        let immediate = state.keys.get(key).is_none_or(|locks| {
            (locks.queue.is_empty() || held.is_some() || state.row_locker(txn, key) || state.compatible_bypass(txn, key, group))
                && !locks.holders.iter().any(|(&owner, held)| {
                    state.holder_conflicts(txn, key, mode, owner, *held, group)
                })
        }) && !state.cross_blocked(txn, key, mode);
        // A zero deadline is a nonblocking attempt: a compatible immediate
        // acquisition, including reentrance and upgrades, still succeeds.
        if immediate {
            state.grant(txn, key, mode, self.config.max_row_locks_per_txn)?;
            state.finish_group(txn, group);
            state.drain_queue(key, self.config.max_row_locks_per_txn);
            state.notify_grant(txn, key);
            return Ok(Some(()));
        }

        Ok(None)
    }

    fn acquire_group_member(
        &self, txn: TxnId, key: LockKey, mode: LockMode,
        deadline: Option<Instant>, cancel: &dyn Fn() -> bool,
        GroupContext { requested: group }: GroupContext<'_>,
    ) -> Result<(), LockError> {
        let deadline = match (deadline, crate::Cancellation::lock_deadline()) {
            (Some(a), Some(b)) => Some(a.min(b)), (a, b) => a.or(b),
        };
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
        if self.try_member_locked(&mut state, txn, &key, mode, group)?.is_some() {
            return Ok(());
        }
        let held = state.keys.get(&key).and_then(|k| k.holders.get(&txn)).copied();

        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            return Err(LockError::Timeout);
        }
        let id = state.next_waiter;
        state.next_waiter = state.next_waiter.checked_add(1).ok_or(LockError::Misuse)?;
        let upgrade = held.is_some() || state.row_locker(txn, &key);
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
        let queued_at = Instant::now();
        let changed = Arc::new(Condvar::new());
        state.by_txn.entry(txn).or_default().push(id);
        state.waiters.insert(
            id,
            Waiter {
                txn,
                key: key.clone(),
                mode,
                upgrade,
                group: group.to_vec(),
                cancelled: false,
                changed: Arc::clone(&changed),
                #[cfg(test)]
                sleeping: false,
            },
        );
        state
            .keys
            .get_mut(&key)
            .expect("blocked key exists")
            .queue
            .insert(position, id);
        state.index(&key, mode);
        if self.config.deadlock_timeout.is_zero() && state.has_cycle(txn) {
            #[cfg(test)]
            state.deadlock_victims.insert(txn);
            state.remove_waiter(id, Err(LockError::Deadlock));
            state.drain_queue(&key, self.config.max_row_locks_per_txn);
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
            let waited = queued_at.elapsed();
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
            } else if waited >= self.config.deadlock_timeout && state.has_cycle(txn) {
                #[cfg(test)]
                state.deadlock_victims.insert(txn);
                Some(LockError::Deadlock)
            } else {
                None
            };
            if let Some(error) = error {
                state.remove_waiter(id, Err(error));
                state.drain_queue(&key, self.config.max_row_locks_per_txn);
                return state
                    .completed
                    .remove(&id)
                    .expect("removed waiter has result");
            }
            let now = Instant::now();
            let mut interval = self.config.deadlock_recheck;
            if waited < self.config.deadlock_timeout {
                interval = interval.min(
                    self.config.deadlock_timeout.saturating_sub(now.duration_since(queued_at)),
                );
            }
            if let Some(deadline) = deadline {
                interval = interval.min(deadline.saturating_duration_since(now));
            }
            #[cfg(test)]
            {
                state.waiters.get_mut(&id).expect("pending waiter").sleeping = true;
            }
            let (next, _) = changed
                .wait_timeout(state, interval)
                .map_err(|_| LockError::Misuse)?;
            #[cfg(test)]
            {
                self.waiter_wakeups.fetch_add(1, Ordering::Relaxed);
            }
            state = next;
            #[cfg(test)]
            if let Some(waiter) = state.waiters.get_mut(&id) {
                waiter.sleeping = false;
            }
        }
    }

    /// Acquire a group of point locks in K-then-D order for blocking calls.
    /// Zero-deadline calls preflight the complete group atomically. The caller serializes requests made by the same transaction.
    pub(crate) fn acquire_points(
        &self, txn: TxnId, requested: &[(LockKey, LockMode)],
        deadline: Option<Instant>, cancel: &dyn Fn() -> bool,
    ) -> Result<(), LockError> {
        let deadline = match (deadline, crate::Cancellation::lock_deadline()) {
            (Some(a), Some(b)) => Some(a.min(b)), (a, b) => a.or(b),
        };
        if requested.iter().any(|(key, mode)| (key.point().is_none() && !matches!(key, LockKey::RowKey { .. })) || mode.is_table()) {
            return Err(LockError::Misuse);
        }
        if deadline.is_none_or(|d| d > Instant::now()) {
            let mut ordered = requested.iter().collect::<Vec<_>>();
            ordered.sort_by_key(|(key, _)| !matches!(key, LockKey::RowKey { .. }));
            let provisional_keys = ordered.iter().map(|(key, _)| (txn, key.clone())).collect::<Vec<_>>();
            let mut state = self.state.lock().map_err(|_| LockError::Misuse)?;
            for ((key, _), provisional_key) in ordered.iter().zip(provisional_keys) {
                let previous = state.keys.get(key).and_then(|s| s.holders.get(&txn))
                    .copied().unwrap_or_default();
                state.provisional.insert(provisional_key, previous);
            }
            let result = (|| {
                // Point members have already passed acquire_group_member's type checks.
                // It merges the thread deadline before locking, but checks expiration
                // and calls cancel only after an immediate attempt fails. Neither can
                // reject an immediate grant. Keep the same per-member side effects.
                let mut start = 0;
                for (i, (key, mode)) in ordered.iter().enumerate() {
                    if self.try_member_locked(&mut state, txn, key, *mode, requested)?.is_none() {
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
                        state = self.state.lock().map_err(|_| LockError::Misuse)?;
                    }
                    // Subsequent passes recheck the whole group after any wait.
                    start = 0;
                    if ordered.iter().any(|(key, mode)| {
                        state.keys.get(key).is_some_and(|locks| locks.holders.iter().any(|(&owner, held)|
                            state.holder_conflicts(txn, key, *mode, owner, *held, requested)))
                            || state.cross_blocked(txn, key, *mode)
                    }) { continue; }
                    for (key, _) in &ordered {
                        state.provisional.remove(&(txn, key.clone()));
                    }
                    return Ok(());
                }
            })();
            if result.is_err() {
                // Restore precisely the pre-group modes, including upgrades of
                // existing grants. release_all may already have removed them.
                let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
                let mut affected = HashSet::new();
                for (key, _) in &ordered {
                    if let Some(previous) = state.provisional.remove(&(txn, key.clone())) {
                        if previous.iter().next().is_some() {
                            state.keys.entry(key.clone()).or_default().holders.insert(txn, previous);
                        } else if state.keys.get_mut(key).is_some_and(|s| s.holders.remove(&txn).is_some()) {
                            if let Some(keys) = state.held_keys.get_mut(&txn) { keys.remove(key); }
                            if let Some(count) = state.row_counts.get_mut(&txn) { *count -= 1; }
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
            let mut state = self.state.lock().map_err(|_| LockError::Misuse)?;
            let blocked = requested.iter().any(|(key, mode)| {
                let held = state.keys.get(key).and_then(|k| k.holders.get(&txn));
                if (state.provisional.is_empty() || !state.provisional.contains_key(&(txn, key.clone())))
                    && held.is_some_and(|h| h.iter().any(|h| h.covers(*mode))) { return false; }
                state.keys.get(key).is_some_and(|locks| {
                    (!locks.queue.is_empty() && held.is_none() && !state.row_locker(txn, key) && !state.compatible_bypass(txn, key, requested)) || locks.holders.iter()
                        .any(|(&owner, held)| state.holder_conflicts(txn, key, *mode, owner, *held, requested))
                }) || state.cross_blocked(txn, key, *mode)
            });
            if !blocked {
                // Count distinct new keys without allocating under the mutex.
                let extra = requested.iter().enumerate().filter(|(i, (key, _))|
                    !requested[..*i].iter().any(|(earlier, _)| earlier == key)
                        && state.keys.get(key).is_none_or(|k| !k.holders.contains_key(&txn))).count();
                if state.row_counts.get(&txn).copied().unwrap_or(0)
                    .saturating_add(extra) > self.config.max_row_locks_per_txn {
                    return Err(LockError::Limit);
                }
                for (key, mode) in requested {
                    state.grant(txn, key, *mode, self.config.max_row_locks_per_txn)?;
                }
                for (key, _) in requested {
                    state.drain_queue(key, self.config.max_row_locks_per_txn);
                    state.notify_grant(txn, key);
                }
                return Ok(());
            }
        }
        Err(LockError::Timeout)
    }

    #[cfg(test)]
    pub(crate) fn release_all_calls(&self) -> usize {
        self.release_all_calls.load(Ordering::Relaxed)
    }

    /// Snapshot just the requested keys; callers serialize acquisitions per transaction.
    pub(crate) fn modes_on(&self, txn: TxnId, keys: &[(LockKey, LockMode)]) -> Vec<(LockKey, HeldModes)> {
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        keys.iter().map(|(key, _)| (key.clone(), state.keys.get(key)
            .and_then(|s| s.holders.get(&txn)).copied().unwrap_or_default())).collect()
    }

    /// Undo acquisitions in reverse journal order, then wake queues only after
    /// every key has reached its final state (including both halves of K/D).
    pub(crate) fn restore_modes(&self, txn: TxnId, undo: &[(LockKey, HeldModes)]) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let mut affected = HashSet::new();
        for (key, previous) in undo.iter().rev() {
            let Some(locks) = state.keys.get_mut(key) else { continue; };
            let Some(current) = locks.holders.get_mut(&txn) else { continue; };
            // Cleanup may have removed modes since the journal snapshot.
            // Rollback can only release ownership, never recreate it.
            let target = previous.intersection(*current);
            if target.iter().next().is_some() {
                *current = target;
            } else {
                locks.holders.remove(&txn);
                locks.slow.remove(&txn);
                if let Some(keys) = state.held_keys.get_mut(&txn) { keys.remove(key); }
                // GD-0084 Phase 1: row-count fix applied to the oracle too (restore of a Table holding no longer decrements rows); see the work log.
                if !matches!(key, LockKey::Table(_)) && let Some(count) = state.row_counts.get_mut(&txn) { *count -= 1; }
            }
            state.provisional.remove(&(txn, key.clone()));
            affected.insert(key.clone());
            let mut unblocked = Vec::new();
            state.cross_queued(key, &mut unblocked);
            affected.extend(unblocked);
            if let LockKey::Row { table, pk } = key {
                affected.insert(LockKey::RowKey { table: *table, pk: pk.clone() });
            }
        }
        if state.held_keys.get(&txn).is_some_and(HashSet::is_empty) {
            state.held_keys.remove(&txn);
        }
        if state.row_counts.get(&txn) == Some(&0) {
            state.row_counts.remove(&txn);
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
            state.provisional.remove(&(txn, key.clone()));
            state.cross_queued(key, &mut unblocked);
            // D grants also determine whether queued K groups may bypass.
            if let LockKey::Row { table, pk } = key {
                let row_key = LockKey::RowKey { table: *table, pk: pk.clone() };
                if state.keys.get(&row_key).is_some_and(|locks| !locks.queue.is_empty()) {
                    unblocked.push(row_key);
                }
            }
        }
        affected.extend(unblocked);
        state.row_counts.remove(&txn);
        let pending = state.by_txn.get(&txn).cloned().unwrap_or_default();
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
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        for id in state.by_txn.get(&txn).cloned().unwrap_or_default() {
            let waiter = state.waiters.get_mut(&id).expect("indexed waiter");
            waiter.cancelled = true;
            waiter.changed.notify_one();
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
    // Complete a ready group under the granting mutex, before another group
    // can overtake it while the acquiring thread is waking. A retained bypasser
    // keeps every addition provisional, including D, until revalidation succeeds.
    fn finish_group(&mut self, txn: TxnId, group: &[(LockKey, LockMode)]) {
        if self.provisional.is_empty() || self.by_txn.get(&txn).is_some_and(|ids| !ids.is_empty()) { return; }
        if group.iter().any(|(key, mode)| {
            self.keys.get(key).is_none_or(|locks|
                !locks.holders.get(&txn).is_some_and(|h| h.iter().any(|h| h.covers(*mode)))
                    || locks.holders.iter().any(|(&owner, held)|
                        self.holder_conflicts(txn, key, *mode, owner, *held, group)))
                || self.cross_blocked(txn, key, *mode)
        }) { return; }
        for (key, _) in group { self.provisional.remove(&(txn, key.clone())); }
    }

    fn row_locker(&self, txn: TxnId, key: &LockKey) -> bool {
        let (table, pk) = match key {
            LockKey::RowKey { table, pk } | LockKey::Row { table, pk } => (*table, pk),
            _ => return false,
        };
        [LockKey::RowKey { table, pk: pk.clone() }, LockKey::Row { table, pk: pk.clone() }].iter()
            .any(|k| self.keys.get(k).and_then(|s| s.holders.get(&txn)).is_some_and(|held|
                self.provisional_modes(txn, k).unwrap_or(*held).iter().next().is_some()))
    }

    fn provisional_modes(&self, owner: TxnId, key: &LockKey) -> Option<HeldModes> {
        if self.provisional.is_empty() { return None; }
        self.provisional.get(&(owner, key.clone())).copied()
    }

    // Only a whole group compatible with all actual grants can bypass tuple
    // arbitration. Keep pre-group K modes: they are real grants too.
    fn compatible_bypass(&self, txn: TxnId, key: &LockKey,
        group: &[(LockKey, LockMode)]) -> bool {
        let row_group = match key {
            LockKey::RowKey { .. } => true,
            LockKey::Row { table, pk } => group.iter().any(|(key, _)|
                matches!(key, LockKey::RowKey { table: t, pk: p } if t == table && p == pk)),
            _ => false,
        };
        row_group && group.iter().all(|(key, mode)| {
            !self.cross_blocked(txn, key, *mode) && self.keys.get(key).is_none_or(|locks|
                locks.holders.iter().all(|(&owner, held)| owner == txn ||
                    !self.provisional_modes(owner, key).unwrap_or(*held).iter()
                        .any(|h| mode.conflicts(h))))
        })
    }

    fn holder_conflicts(&self, txn: TxnId, key: &LockKey, mode: LockMode,
        owner: TxnId, held: HeldModes, group: &[(LockKey, LockMode)]) -> bool {
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
            let pending = self.by_txn.get_mut(&waiter.txn).expect("indexed waiter");
            pending.retain(|&pending| pending != id);
            if pending.is_empty() {
                self.by_txn.remove(&waiter.txn);
            }
            if let Some(locks) = self.keys.get_mut(&waiter.key) {
                locks.queue.retain(|&queued| queued != id);
            }
            self.settle(&waiter.key);
            self.completed.insert(id, result);
            waiter.changed.notify_one();
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
        if self.keys.get(key).is_none_or(|locks| locks.queue.is_empty()) { return; }
        while let Some(id) = self.keys.get(key).and_then(|locks| {
            locks.queue.iter().copied().find(|id| {
                let waiter = &self.waiters[id];
                (locks.queue.first() == Some(id)
                    || self.compatible_bypass(waiter.txn, key, &waiter.group))
                    && (waiter.cancelled || (!locks.holders.iter().any(|(&owner, held)|
                        self.holder_conflicts(waiter.txn, key, waiter.mode, owner, *held, &waiter.group))
                        && !self.cross_blocked(waiter.txn, key, waiter.mode)))
            })
        }) {
            let waiter = &self.waiters[&id];
            let txn = waiter.txn;
            let mode = waiter.mode;
            // A cancel_wait victim is never granted, even if its thread has not
            // observed the flag yet.
            if waiter.cancelled {
                self.remove_waiter(id, Err(LockError::Deadlock));
                continue;
            }
            let group = waiter.group.clone();
            let result = self.grant(txn, key, mode, limit);
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
        let waiter = &self.waiters[&locks.queue[position]];
        for (&owner, held) in &locks.holders {
            if self.holder_conflicts(waiter.txn, &waiter.key, waiter.mode, owner, *held, &waiter.group) {
                blockers.push(owner);
            }
        }
        for preceding in &locks.queue[..position] {
            let preceding = &self.waiters[preceding];
            if preceding.txn != waiter.txn && waiter.mode.conflicts(preceding.mode)
                && !self.compatible_bypass(waiter.txn, &waiter.key, &waiter.group) {
                blockers.push(preceding.txn);
            }
        }
        self.cross_holders(waiter.txn, &waiter.key, waiter.mode, blockers);
    }

    /// A grant can add wait-for edges to its owner. If the owner is itself
    /// waiting, wake both ends of every potentially new edge to recheck cycles.
    fn notify_grant(&self, txn: TxnId, key: &LockKey) {
        let Some(pending) = self.by_txn.get(&txn) else {
            return;
        };
        let mut notify: HashSet<u64> = pending.iter().copied().collect();
        let mut keys = vec![key.clone()];
        self.cross_queued(key, &mut keys);
        for key in keys {
            if let Some(locks) = self.keys.get(&key) {
                notify.extend(locks.queue.iter().copied());
            }
        }
        for id in notify {
            self.waiters[&id].changed.notify_one();
        }
    }

    fn txn_blockers(&self, txn: TxnId, blockers: &mut Vec<TxnId>) {
        for id in self.by_txn.get(&txn).into_iter().flatten() {
            let waiter = &self.waiters[id];
            let locks = &self.keys[&waiter.key];
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

