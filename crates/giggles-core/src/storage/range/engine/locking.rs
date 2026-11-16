//! Lock service and per-transaction lock leases (GD-0075 R5-34; design.md 3, 3.1, 3.2).
use std::time::{Duration, Instant};

use super::{RangeEngine, TxnEntry, lock_error, txn_id};
use crate::storage::engine::{
    locks::{LockError, LockKey, LockMode},
    mvcc,
};
use crate::storage::range::keys::keyspace_of;
use crate::storage::range_api::{
    ClusterTxnId, LockGrant, LockRequest, LockTarget, RangeError, RangeId, RangeLockMode,
    RangeSpace, Timestamp, range_key,
};

impl RangeEngine {
    /// The `LockManager` key of a target, if this range owns it.
    pub(super) fn lock_key(&self, target: &LockTarget) -> Result<LockKey, RangeError> {
        let stale = || RangeError::StaleEpoch {
            descriptor: self.descriptor.clone(),
        };
        match target {
            LockTarget::Key { space, key } => {
                if key.len() < 8 {
                    return Err(RangeError::Corrupt("lock key without an id".into()));
                }
                if !self.descriptor.contains(&range_key(*space, key)) {
                    return Err(stale());
                }
                let id = u64::from_be_bytes(key[..8].try_into().unwrap());
                match space {
                    RangeSpace::Rows => Ok(LockKey::Row {
                        table: id,
                        pk: key[8..].to_vec(),
                    }),
                    RangeSpace::Index => Ok(LockKey::Unique {
                        index: id,
                        tuple: key[8..].to_vec(),
                    }),
                    _ => Err(RangeError::Corrupt(format!("no locks in {space:?}"))),
                }
            }
            LockTarget::TableSentinel { table } => {
                // Design 3.1: a sentinel lives on every range intersecting the table's rows.
                let lo = range_key(RangeSpace::Rows, &table.to_be_bytes());
                let hi = table
                    .checked_add(1)
                    .map(|next| range_key(RangeSpace::Rows, &next.to_be_bytes()));
                let below_end = self.descriptor.end.as_ref().is_none_or(|end| lo < *end);
                let above_start = hi.as_ref().is_none_or(|hi| self.descriptor.start < *hi);
                if !(below_end && above_start) {
                    return Err(stale());
                }
                Ok(LockKey::Row {
                    table: *table,
                    pk: Vec::new(),
                })
            }
        }
    }

    pub(super) fn lock_target(&self, req: LockRequest) -> Result<LockGrant, RangeError> {
        self.check_open()?;
        self.check_epoch(req.epoch)?;
        let key = self.lock_key(&req.target)?;
        {
            let mut state = self.state();
            if state.expired.contains(&req.txn) {
                // Its earlier locks here are gone: a fresh lease would hide that (S1, E67).
                return Err(RangeError::Aborted("lock lease expired".into()));
            }
            let lease = self.config.lease;
            let entry = state.txns.entry(req.txn).or_insert(TxnEntry {
                expires: Instant::now() + lease,
                prepared: false,
                busy: false,
            });
            if entry.prepared {
                // Design 3.2 step 5: prepare is a transaction's last lock step, so a prepared
                // transaction never waits and is never a deadlock victim. Refused in every build.
                return Err(RangeError::Aborted("transaction is prepared".into()));
            }
            entry.expires = Instant::now() + lease;
        }
        let mode = match req.mode {
            RangeLockMode::Shared => LockMode::Shared,
            RangeLockMode::Exclusive => LockMode::Exclusive,
        };
        let deadline = req
            .timeout_ms
            .map(|ms| Instant::now() + Duration::from_millis(ms));
        let cancel = || self.is_closed() || self.lease_lost(req.txn);
        loop {
            // R5-53: with a client, wait at most `resolve_after` at a time and resolve the
            // prepared holders of the key between attempts.
            let client = self.client();
            let attempt = match client {
                Some(_) => {
                    let step = Instant::now() + self.config.resolve_after;
                    Some(deadline.map_or(step, |deadline| deadline.min(step)))
                }
                None => deadline,
            };
            let error =
                match self
                    .locks
                    .acquire(txn_id(req.txn), key.clone(), mode, attempt, &cancel)
                {
                    Ok(()) => break,
                    Err(error) => error,
                };
            if self.is_closed() || self.lease_lost(req.txn) {
                return Err(RangeError::Aborted("locks lost while waiting".into()));
            }
            match client {
                Some(client)
                    if error == LockError::Timeout
                        && deadline.is_none_or(|deadline| Instant::now() < deadline) =>
                {
                    let holders = self.prepared_holders(&key, mode);
                    self.resolve_blockers(&*client, &holders);
                }
                _ => return Err(lock_error(error)),
            }
        }
        if self.lease_lost(req.txn) {
            // The sweep expired the lease while this request waited: give the lock back.
            self.locks.release_all(txn_id(req.txn));
            return Err(RangeError::Aborted("lock lease expired".into()));
        }
        self.touch(req.txn);
        let latest = match &req.target {
            LockTarget::TableSentinel { .. } => None,
            LockTarget::Key { space, key } => {
                mvcc::latest_version(self.kv.as_ref(), keyspace_of(*space), key)
                    .map_err(super::txn_error)?
                    .map(|(at, value)| (Timestamp(at.0), value))
            }
        };
        Ok(LockGrant {
            latest,
            generation: self.generation,
        })
    }

    /// Prepared transactions holding `key` in a mode that conflicts with `mode`. A prepared
    /// transaction never waits, so these are the prepared ones among the request's blockers
    /// (`LockManager::blockers` is empty once a timed-out request has left the queue).
    fn prepared_holders(&self, key: &LockKey, mode: LockMode) -> Vec<(ClusterTxnId, RangeId)> {
        self.state()
            .prepared
            .iter()
            .filter(|(_, record)| {
                record.locks.iter().any(|(held, held_mode)| {
                    held == key
                        && (mode == LockMode::Exclusive || *held_mode == LockMode::Exclusive)
                })
            })
            .map(|(txn, record)| (*txn, record.home))
            .collect()
    }

    /// The state, once no commit or prepare of `txn` is running.
    pub(super) fn idle(&self, txn: ClusterTxnId) -> std::sync::MutexGuard<'_, super::EngineState> {
        let mut state = self.state();
        while state.txns.get(&txn).is_some_and(|entry| entry.busy) {
            state = self
                .changed
                .wait_timeout(state, Duration::from_millis(100))
                .unwrap_or_else(|p| p.into_inner())
                .0;
        }
        state
    }

    /// Whether `txn` no longer has a lease here (expired, released or never locked).
    fn lease_lost(&self, txn: ClusterTxnId) -> bool {
        !self.state().txns.contains_key(&txn)
    }

    fn touch(&self, txn: ClusterTxnId) {
        if let Some(entry) = self.state().txns.get_mut(&txn) {
            entry.expires = Instant::now() + self.config.lease;
        }
    }

    pub(super) fn renew(&self, txn: ClusterTxnId) -> Result<(), RangeError> {
        self.check_open()?;
        let mut state = self.state();
        match state.txns.get_mut(&txn) {
            Some(entry) => {
                entry.expires = Instant::now() + self.config.lease;
                Ok(())
            }
            None => Err(RangeError::Aborted(
                "transaction holds no locks here in this term".into(),
            )),
        }
    }

    /// Releases the locks of a transaction that has not prepared; a prepared transaction's
    /// locks stay until finalize (the coordinator calls release on every exit path, E43).
    pub(super) fn release_unprepared(&self, txn: ClusterTxnId) -> Result<(), RangeError> {
        self.check_open()?;
        let mut state = self.idle(txn);
        state.expired.remove(&txn);
        if state.txns.get(&txn).is_some_and(|entry| entry.prepared) {
            return Ok(());
        }
        state.txns.remove(&txn);
        drop(state);
        self.locks.release_all(txn_id(txn));
        Ok(())
    }

    /// Releases the locks of every unprepared transaction whose lease has expired (the
    /// gateway died or lost contact; E43's coordinator-dies-before-Seal case).
    pub(super) fn expire_leases(&self) {
        let now = Instant::now();
        let expired: Vec<ClusterTxnId> = {
            let mut state = self.state();
            let expired: Vec<_> = state
                .txns
                .iter()
                .filter(|(_, entry)| !entry.prepared && !entry.busy && entry.expires <= now)
                .map(|(txn, _)| *txn)
                .collect();
            for txn in &expired {
                state.txns.remove(txn);
                state.expired.insert(*txn);
            }
            expired
        };
        for txn in expired {
            self.locks.release_all(txn_id(txn));
        }
    }
}
