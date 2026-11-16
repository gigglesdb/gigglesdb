//! Single-range commit, prepare and finalize (GD-0075 R5-35, R5-36, R5-37; design.md 4a O4/O5).
use std::collections::HashSet;

use super::prepared::{Prepared, Writes};
use super::{RangeEngine, txn_error, txn_id};
use crate::storage::engine::{
    codec,
    commit::{CommitRequest, RawOp},
    hlc::Ts,
    kv::{Durability, Keyspace, WriteBatch},
    locks::{LockKey, LockMode},
    meta::{RangeLocalKey, range_local_key},
    mvcc,
};
use crate::storage::range::keys::{is_versioned, keyspace_of};
use crate::storage::range_api::{
    ClusterTxnId, Epoch, Outcome, RangeError, RangeId, RangeSpace, RangeWrite, Timestamp, range_key,
};

/// Clears a transaction's busy mark when dropped.
struct Busy<'a>(&'a RangeEngine, ClusterTxnId);
impl Drop for Busy<'_> {
    fn drop(&mut self) {
        if let Some(entry) = self.0.state().txns.get_mut(&self.1) {
            entry.busy = false;
        }
        self.0.changed.notify_all();
    }
}

/// How a commit or prepare of a transaction may proceed.
enum Begun<'a> {
    Ready(Busy<'a>),
    Prepared(Ts),
}

/// Ends a gate reservation when dropped.
struct Reserved<'a>(&'a RangeEngine, Ts);
impl Drop for Reserved<'_> {
    fn drop(&mut self) {
        self.0.gate.finish(self.1);
    }
}

impl RangeEngine {
    /// Marks `txn` busy (one commit or prepare at a time), after any running one ends. The
    /// transaction must hold a lease here in this term.
    fn begin(&self, txn: ClusterTxnId) -> Result<Begun<'_>, RangeError> {
        let mut state = self.idle(txn);
        self.check_open()?;
        if let Some(record) = state.prepared.get(&txn) {
            return Ok(Begun::Prepared(record.at));
        }
        let expired = state.expired.contains(&txn);
        match state.txns.get_mut(&txn) {
            Some(entry) => {
                entry.busy = true;
                Ok(Begun::Ready(Busy(self, txn)))
            }
            None if expired => Err(RangeError::Aborted("lock lease expired".into())),
            None => Err(RangeError::Aborted(
                "transaction holds no locks here in this term".into(),
            )),
        }
    }

    /// The writes as the sequencer takes them, after checking that each is inside the range and
    /// that `txn` holds an exclusive lock on every row it writes.
    fn checked_writes(
        &self,
        txn: ClusterTxnId,
        writes: Vec<RangeWrite>,
    ) -> Result<Writes, RangeError> {
        let held: HashSet<LockKey> = self
            .locks
            .held(txn_id(txn))
            .into_iter()
            .filter(|(_, mode)| *mode == LockMode::Exclusive)
            .map(|(key, _)| key)
            .collect();
        let mut out = Writes::new();
        for write in writes {
            if !self
                .descriptor
                .contains(&range_key(write.space, &write.key))
            {
                return Err(RangeError::StaleEpoch {
                    descriptor: self.descriptor.clone(),
                });
            }
            if !is_versioned(write.space) {
                // Seq and Gc are written raw by allocators, never by transactions (txn.rs).
                return Err(RangeError::Corrupt(format!(
                    "transactional write to {:?}",
                    write.space
                )));
            }
            if write.space == RangeSpace::Rows {
                let locked = write.key.len() >= 8
                    && held.contains(&LockKey::Row {
                        table: u64::from_be_bytes(write.key[..8].try_into().unwrap()),
                        pk: write.key[8..].to_vec(),
                    });
                if !locked {
                    return Err(RangeError::Aborted("row write without its lock".into()));
                }
            }
            out.insert((keyspace_of(write.space), write.key), write.value);
        }
        Ok(out)
    }

    /// Forgets `txn` here and releases all its locks.
    fn end_txn(&self, txn: ClusterTxnId) {
        {
            let mut state = self.state();
            state.txns.remove(&txn);
            state.prepared.remove(&txn);
        }
        self.locks.release_all(txn_id(txn));
        self.changed.notify_all();
    }

    /// R5-35: one-range commit through the sequencer (oracle ts, O4), with the transaction's
    /// commit marker range-local (RangeKv maps `Seq/[5]||xid`), then release.
    pub(super) fn commit_writes(
        &self,
        epoch: Epoch,
        txn: ClusterTxnId,
        writes: Vec<RangeWrite>,
        marker_xid: Option<u64>,
        lock_generation: Option<u64>,
    ) -> Result<Timestamp, RangeError> {
        self.check_open()?;
        self.check_epoch(epoch)?;
        self.check_generation(lock_generation)?;
        let _busy = match self.begin(txn)? {
            Begun::Ready(busy) => busy,
            Begun::Prepared(_) => {
                return Err(RangeError::Aborted("transaction is prepared".into()));
            }
        };
        let writes = self.checked_writes(txn, writes)?;
        let result = self.seq.submit(CommitRequest {
            snapshot: self.seq.visible_ts(),
            writes,
            features_or: 0,
            minimum_subversion: 0,
            raw: marker_xid
                .map(|xid| RawOp::XactCommitted { xid })
                .into_iter()
                .collect(),
            validation: None,
        });
        // Applied or failed, the commit is over: the locks go either way.
        self.end_txn(txn);
        result.map(|at| Timestamp(at.0)).map_err(txn_error)
    }

    /// R5-36: reserve p above the served floor, write the prepared record (writes and locks)
    /// in one replicated batch, keep the locks. A retry of a prepared transaction returns p.
    pub(super) fn prepare_writes(
        &self,
        epoch: Epoch,
        txn: ClusterTxnId,
        home: RangeId,
        writes: Vec<RangeWrite>,
        lock_generation: Option<u64>,
    ) -> Result<Timestamp, RangeError> {
        self.check_open()?;
        self.check_epoch(epoch)?;
        // A retry of a prepared transaction returns its p whatever its lock generation: the
        // prepare was accepted under a valid one and a new leader re-took its locks (Track D
        // rule (a)). A concurrent duplicate waits.
        let _busy = match self.begin(txn) {
            Ok(Begun::Prepared(at)) => return Ok(Timestamp(at.0)),
            Ok(Begun::Ready(busy)) => {
                self.check_generation(lock_generation)?;
                busy
            }
            Err(error) => {
                self.check_generation(lock_generation)?;
                return Err(error);
            }
        };
        let writes = self.checked_writes(txn, writes)?;
        let locks = self.locks.held(txn_id(txn));
        let at = self
            .gate
            .reserve(self.oracle.as_ref(), 1, self.seq.visible_ts())
            .map_err(txn_error)?;
        let _reserved = Reserved(self, at);
        let record = Prepared {
            home,
            at,
            writes,
            locks,
        };
        let mut batch = WriteBatch::default();
        batch.put(
            Keyspace::Meta,
            range_local_key(self.descriptor.id, &RangeLocalKey::Prepared(txn.0)),
            record.encode()?,
        );
        self.kv
            .write(batch, Durability::Sync)
            .map_err(|e| txn_error(e.into()))?;
        let mut state = self.state();
        if let Some(entry) = state.txns.get_mut(&txn) {
            entry.prepared = true;
        }
        state.prepared.insert(txn, record);
        drop(state);
        self.changed.notify_all();
        Ok(Timestamp(at.0))
    }

    /// R5-37: Commit(c) writes the prepared versions at c and deletes the record in one batch;
    /// Abort deletes the record if there is one. Either way every lock of `txn` here is
    /// released (E43: even with no prepared record). Repeating it changes nothing.
    pub(super) fn finalize_txn(
        &self,
        txn: ClusterTxnId,
        outcome: Outcome,
    ) -> Result<(), RangeError> {
        self.check_open()?;
        // Never race a running prepare: it would leave an orphan record after an Abort.
        let record = {
            let mut state = self.idle(txn);
            state.expired.remove(&txn);
            state.prepared.get(&txn).cloned()
        };
        let key = range_local_key(self.descriptor.id, &RangeLocalKey::Prepared(txn.0));
        let batch = match (&outcome, record) {
            (Outcome::Commit(c), Some(record)) => {
                let c = Ts(c.0);
                if c <= record.at {
                    return Err(RangeError::Corrupt(
                        "commit timestamp not above the prepared timestamp".into(),
                    ));
                }
                let mut batch = WriteBatch::default();
                for ((ks, key), value) in &record.writes {
                    let value = match value {
                        Some(value) => mvcc::encode_live(value),
                        None => mvcc::encode_tombstone(),
                    };
                    batch.put(*ks, codec::user_key_seek_at(key, c), value);
                }
                batch.delete(Keyspace::Meta, key);
                Some(batch)
            }
            (Outcome::Abort, Some(_)) => {
                let mut batch = WriteBatch::default();
                batch.delete(Keyspace::Meta, key);
                Some(batch)
            }
            // Already finalized, or (Abort) never prepared here.
            (_, None) => None,
        };
        if let Some(batch) = batch {
            self.kv
                .write(batch, Durability::Sync)
                .map_err(|e| txn_error(e.into()))?;
        }
        self.end_txn(txn);
        Ok(())
    }
}
