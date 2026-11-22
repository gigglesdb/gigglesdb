//! Snapshot-safe, resumable MVCC garbage collection.
//!
//! Open one collector per engine and share it (normally through `Arc<Gc>`): its
//! pin registry and admission mutex belong to that engine's watermark.
use super::{
    codec,
    commit::Sequencer,
    hlc::Ts,
    kv::{Direction, Durability, Keyspace, Kv, KvError, WriteBatch},
    mvcc,
    txn::TxnError,
};
use std::{
    collections::BTreeMap,
    ops::Bound,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

const WATERMARK_KEY: &[u8] = b"gc_watermark";
/// Deletes per write batch. A hot key's history is removed in chunks of this
/// size rather than refused (R3-620).
pub(crate) const DELETE_BATCH: usize = 10_000;

fn duration_ms(duration: Duration) -> u64 {
    duration.as_millis().min(u64::MAX as u128) as u64
}

#[derive(Clone, Debug, Default)]
pub(crate) struct GcConfig {
    pub(crate) retention: Duration,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct StepStats {
    pub(crate) examined: usize,
    pub(crate) deleted: usize,
    pub(crate) keys: usize,
    pub(crate) wrapped: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct GcStats {
    pub(crate) watermark: Ts,
    pub(crate) visible: Ts,
    pub(crate) lag_ms: u64,
    pub(crate) pins: usize,
    pub(crate) versions_examined_total: u64,
    pub(crate) versions_deleted_total: u64,
}

struct State {
    watermark: Ts,
    next_pin: u64,
    pins: BTreeMap<u64, (Ts, String)>,
    versions_examined_total: u64,
    versions_deleted_total: u64,
}

/// Owning, non-cloneable registration; dropping it releases exactly one pin.
pub(crate) struct PinGuard {
    state: Arc<Mutex<State>>,
    id: u64,
}
impl Drop for PinGuard {
    fn drop(&mut self) {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .pins
            .remove(&self.id);
    }
}

pub(crate) struct Gc {
    kv: Arc<Kv>,
    seq: Arc<Sequencer>,
    // `GcConfig.retention` in milliseconds; adjustable while running.
    retention_ms: AtomicU64,
    // Orders pin admission, computation, durable advancement and publication.
    state: Arc<Mutex<State>>,
    // A step's cursor read/write and table reclamation must not race another
    // collector operation on this instance. Commits need not take this mutex.
    work: Mutex<()>,
}

impl Gc {
    pub(crate) fn open(
        kv: Arc<Kv>,
        seq: Arc<Sequencer>,
        config: GcConfig,
    ) -> Result<Self, TxnError> {
        let watermark = match kv.get(Keyspace::Meta, WATERMARK_KEY)? {
            None => Ts(0),
            Some(bytes) => Ts(u64::from_be_bytes(bytes.try_into().map_err(|_| {
                KvError::Corrupt("invalid gc_watermark: expected eight bytes".into())
            })?)),
        };
        Ok(Self {
            kv,
            seq,
            retention_ms: AtomicU64::new(duration_ms(config.retention)),
            state: Arc::new(Mutex::new(State {
                watermark,
                next_pin: 0,
                pins: BTreeMap::new(),
                versions_examined_total: 0,
                versions_deleted_total: 0,
            })),
            work: Mutex::new(()),
        })
    }

    /// Changes the retention window for later watermark computations. A
    /// longer window never rewinds the durable watermark.
    #[cfg(all(test, feature = "engine-rocksdb"))]
    pub(crate) fn test_retention_ms(&self) -> u64 {
        self.retention_ms.load(Ordering::Relaxed)
    }

    pub(crate) fn set_retention(&self, retention: Duration) {
        self.retention_ms
            .store(duration_ms(retention), Ordering::Relaxed);
    }

    pub(crate) fn pin(&self, ts: Ts, owner: String) -> Result<PinGuard, TxnError> {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if ts < state.watermark {
            return Err(TxnError::SnapshotTooOld);
        }
        let id = state.next_pin;
        state.next_pin = id.checked_add(1).ok_or(TxnError::Misuse)?;
        state.pins.insert(id, (ts, owner));
        Ok(PinGuard {
            state: self.state.clone(),
            id,
        })
    }

    pub(crate) fn pins(&self) -> Vec<(Ts, String)> {
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let mut pins: Vec<_> = state.pins.values().cloned().collect();
        pins.sort();
        pins
    }

    pub(crate) fn watermark(&self) -> Ts {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .watermark
    }

    pub(crate) fn check_snapshot(&self, ts: Ts) -> Result<(), TxnError> {
        if ts < self.watermark() {
            Err(TxnError::SnapshotTooOld)
        } else {
            Ok(())
        }
    }

    pub(crate) fn compute(&self) -> Ts {
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        self.compute_locked(&state)
    }

    fn compute_locked(&self, state: &State) -> Ts {
        // Capture visible FIRST. Reading oldest first could miss a snapshot
        // admitted at that older visible value before visibility advances.
        let visible = self.seq.visible_ts();
        let oldest = self.seq.oldest_active().unwrap_or(visible);
        let retention_ms = self.retention_ms.load(Ordering::Relaxed);
        let retained = Ts((visible.0 >> 16).saturating_sub(retention_ms) << 16);
        let mut next = visible.min(oldest).min(retained);
        for (ts, _) in state.pins.values() {
            next = next.min(*ts);
        }
        next.max(state.watermark)
    }

    pub(crate) fn advance(&self) -> Result<Ts, TxnError> {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let next = self.compute_locked(&state);
        if next > state.watermark {
            let mut batch = WriteBatch::default();
            batch.put(
                Keyspace::Meta,
                WATERMARK_KEY.to_vec(),
                next.0.to_be_bytes().to_vec(),
            );
            // Sync must succeed BEFORE publishing W or using it for deletes.
            // Kv fences an ambiguous write; we leave the published W unchanged.
            self.kv.write(batch, Durability::Sync)?;
            state.watermark = next;
        }
        // Safety: every live snapshot S >= W. W <= oldest active at compute
        // time; snapshots admitted after the captured visible have S = visible
        // >= that earlier W because visible is monotonic and W <= visible.
        // Holding state through Sync also prevents admitting a pin below next.
        // Existing pins bounded next, and every later pin must be >= published W.
        Ok(state.watermark)
    }

    /// Process complete user-key groups, stopping near `budget` raw versions.
    pub(crate) fn step(&self, ks: Keyspace, budget: usize) -> Result<StepStats, TxnError> {
        if matches!(ks, Keyspace::Meta | Keyspace::Seq | Keyspace::Gc) {
            return Err(TxnError::Misuse);
        }
        let _work = self.work.lock().unwrap_or_else(|p| p.into_inner());
        let watermark = self.advance()?;
        let mut stats = StepStats::default();
        if budget == 0 {
            return Ok(stats);
        }
        let cursor_key = vec![ks as u8];
        let cursor = self.kv.get(Keyspace::Gc, &cursor_key)?;
        let lower = match cursor.as_deref() {
            Some(value) if !value.is_empty() => Bound::Included(value),
            _ => Bound::Unbounded,
        };
        // Read raw versions, never VersionedRange, which hides collectible data.
        let mut iter = self
            .kv
            .range(ks, lower, Bound::Unbounded, Direction::Forward)?;
        let mut pending = iter.next().transpose()?;
        let mut batch = WriteBatch::default();
        // Deletes in `batch` not yet counted into the shared totals.
        let mut unflushed = 0usize;
        let mut next_cursor = Vec::new();
        while let Some((first_key, first_value)) = pending.take() {
            let (user, _) = codec::split_versioned(&first_key)?;
            if matches!(ks, Keyspace::Rows | Keyspace::Index) && user.len() < 8 {
                return Err(codec::CodecError::Corrupt("short GC row/index user key").into());
            }
            let user = user.to_vec();
            let mut current = Some((first_key, first_value));
            let mut anchor_seen = false;
            // A tombstone anchor hides every older version from snapshots
            // >= W. It is deleted only after all of them, in the group's
            // final batch, so no crash image can resurrect an older value.
            let mut tombstone_anchor = None;
            while let Some((key, value)) = current.take() {
                stats.examined += 1;
                let (_, ts) = codec::split_versioned(&key)?;
                let mut resume_after = None;
                if ts <= watermark {
                    if anchor_seen {
                        // Older than the anchor: invisible to every live
                        // snapshot, so any number of batches may remove them.
                        stats.deleted += 1;
                        unflushed += 1;
                        if unflushed >= DELETE_BATCH {
                            resume_after = Some(key.clone());
                        }
                        batch.delete(ks, key);
                    } else {
                        anchor_seen = true;
                        if mvcc::decode(&value)?.is_none() {
                            tombstone_anchor = Some(key);
                        }
                    }
                }
                if let Some(last) = resume_after {
                    // Flush a chunk without the cursor. Reopen the scan after
                    // the last deleted key so the backend iterator does not
                    // pin the pre-delete view of the whole chunk.
                    drop(iter);
                    self.flush_deletes(std::mem::take(&mut batch), unflushed)?;
                    unflushed = 0;
                    iter = self.kv.range(
                        ks,
                        Bound::Excluded(&last),
                        Bound::Unbounded,
                        Direction::Forward,
                    )?;
                }
                if let Some((key, value)) = iter.next().transpose()? {
                    let (next_user, _) = codec::split_versioned(&key)?;
                    if next_user == user {
                        current = Some((key, value));
                    } else {
                        pending = Some((key, value));
                    }
                }
            }
            if let Some(key) = tombstone_anchor {
                batch.delete(ks, key);
                stats.deleted += 1;
                unflushed += 1;
            }
            stats.keys += 1;
            next_cursor = codec::user_key_upper_bound(&user);
            if pending.is_none() || stats.examined >= budget {
                break;
            }
        }
        stats.wrapped = pending.is_none();
        if stats.wrapped {
            next_cursor.clear();
        }
        drop(iter);
        batch.put(Keyspace::Gc, cursor_key, next_cursor);
        // Cursor advancement shares the step's last batch, and every tombstone
        // anchor shares a batch with or follows all its older versions. Each
        // batch is NoSync and applied in order, so a crash keeps a prefix of
        // them: reprocessing is idempotent, while the watermark authorizing
        // the deletes was already Sync-durable.
        // Direct Kv deletes are safe: commits only add ts > visible >= W and
        // never change <= W; GC only deletes <= W.
        self.flush_deletes(batch, unflushed)?;
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state.versions_examined_total = state
            .versions_examined_total
            .saturating_add(stats.examined as u64);
        Ok(stats)
    }

    fn flush_deletes(&self, batch: WriteBatch, deleted: usize) -> Result<(), TxnError> {
        self.kv.write(batch, Durability::NoSync)?;
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state.versions_deleted_total = state.versions_deleted_total.saturating_add(deleted as u64);
        Ok(())
    }

    /// The caller supplies the committed drop timestamp for a non-reused ID.
    /// Table locking has already excluded writers before that drop commits.
    pub(crate) fn reclaim_table(&self, table_id: u64, dropped_at: Ts) -> Result<bool, TxnError> {
        let _work = self.work.lock().unwrap_or_else(|p| p.into_inner());
        if self.watermark() < dropped_at {
            return Ok(false);
        }
        let start = table_id.to_be_bytes().to_vec();
        let end = match table_id.checked_add(1) {
            Some(next) => next.to_be_bytes().to_vec(),
            None => {
                // The seam accepts only finite DeleteRange bounds. Discover
                // the last current raw row in this logically unbounded prefix
                // with one reverse seek, then use last_key || 0 as its exact
                // exclusive successor. No arbitrary maximum sentinel or full
                // scan is needed. A dropped, non-reused table admits no later
                // writes, so this finite bound covers all its rows.
                let last = self
                    .kv
                    .range(
                        Keyspace::Rows,
                        Bound::Included(&start),
                        Bound::Unbounded,
                        Direction::Reverse,
                    )?
                    .next()
                    .transpose()?;
                let Some((mut key, _)) = last else {
                    return Ok(true);
                };
                if !key.starts_with(&start) {
                    return Err(KvError::Corrupt("GC max-table prefix mismatch".into()).into());
                }
                key.push(0);
                key
            }
        };
        let mut batch = WriteBatch::default();
        batch.delete_range(Keyspace::Rows, start, end);
        self.kv.write(batch, Durability::NoSync)?;
        Ok(true)
    }

    /// Version totals count successful steps. Range reclamation does not
    /// enumerate versions and therefore does not contribute to those totals.
    pub(crate) fn stats(&self) -> GcStats {
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let visible = self.seq.visible_ts();
        GcStats {
            watermark: state.watermark,
            visible,
            lag_ms: (visible.0 >> 16).saturating_sub(state.watermark.0 >> 16),
            pins: state.pins.len(),
            versions_examined_total: state.versions_examined_total,
            versions_deleted_total: state.versions_deleted_total,
        }
    }
}

#[cfg(test)]
#[path = "gc_tests.rs"]
mod tests;
