//! Retention sweeper (GD-0061 D5, design D-I, plan R3-622).
//!
//! One pass, run by the maintenance thread every `interval_ms` with the clock
//! from `V2Db::wall_now_ms()`:
//! 1. Read log `2A` and debits `2B`: per task prefix, delete entries whose
//!    `wall_ms < now - read_log_ms`, stopping at the task's first young entry.
//! 2. Change records `24`/`25`: for headers decided before
//!    `now - before_values_ms`, rewrite every row with an empty `before` and
//!    flag bit1, then set header bit0. The per-row hashes stay, so the effects
//!    digest still verifies.
//! 3. Outcomes `22` of kinds 2-4: delete when `now >= retain_until_ms` and the
//!    task is absent, revoked, or `now >= expires_at + outcome_ms`.
//! 4. Never touches kind 1 (`once`), or `20`, `21`, `23`.
//!
//! Every write goes through read-committed transactions of at most `batch`
//! records, without the authority hold. The task catalog record is never
//! purged here: a task keeps its `20`/`21`/`23` records forever, so the
//! "no Kernel records left" condition for `TaskCatalog::purge` never holds.
use super::records::{
    ChangeHeader, ChangeRow, DebitRecord, NS_CHANGE_HEADER, NS_CHANGE_ROW, NS_DEBIT,
    NS_OUTCOME, NS_READ_LOG, OutcomePayload, OutcomeRecord, ReadLogRecord, TaskObjectId,
    CHANGE_BEFORE_BLANKED, ROW_BEFORE_BLANKED, change_row_key, task_prefix,
};
use crate::CoreError;
use crate::storage::engine::kv::Direction;
use crate::storage::{Isolation, Keyspace, PROTECTED_RECORDS, Transaction, V2Db};
use std::ops::Bound;

const DAY_MS: u64 = 24 * 60 * 60 * 1000;

/// Retention settings (D5 defaults: read log 90 days, before-values 30 days,
/// outcomes 30 days after the later of decision and task end).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetentionConfig {
    /// Read-log (`2A`) and debit (`2B`) entries older than this are deleted.
    pub read_log_ms: u64,
    /// Change records decided longer ago than this lose their before-values.
    pub before_values_ms: u64,
    /// An outcome is kept this long after its live task's expiry.
    pub outcome_ms: u64,
    /// Time between background passes.
    pub interval_ms: u64,
    /// Most records written by one transaction (at least 1).
    pub batch: usize,
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            read_log_ms: 90 * DAY_MS,
            before_values_ms: 30 * DAY_MS,
            outcome_ms: 30 * DAY_MS,
            interval_ms: 10 * 60 * 1000,
            batch: 500,
        }
    }
}

/// What one pass changed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RetentionStats {
    pub read_logs_deleted: u64,
    pub debits_deleted: u64,
    pub rows_blanked: u64,
    pub headers_blanked: u64,
    pub outcomes_deleted: u64,
    /// Committed transactions that wrote at least one record.
    pub transactions: u64,
    /// Most records written by any one of those transactions.
    pub largest_transaction: usize,
}

/// Committed `(key, value)` pairs of one scan.
type Entries = Vec<(Vec<u8>, Vec<u8>)>;

/// A read-committed transaction that commits every `batch` writes.
struct Batch<'a> {
    db: &'a V2Db,
    /// `None` only between a commit and the next read or write.
    txn: Option<Transaction>,
    writes: usize,
    limit: usize,
    stats: &'a mut RetentionStats,
}

/// Where a scan resumes. Bounds are user keys: the versioned range compares
/// raw `key ‖ ts` keys, so "after k" must be `Excluded(k)`; `Included(k ‖ 00)`
/// would still match `k`'s own versions.
enum Cursor {
    At(Vec<u8>),
    After(Vec<u8>),
}

impl Cursor {
    /// Whether nothing below `end` is left to scan.
    fn reached(&self, end: &[u8]) -> bool {
        match self {
            Self::At(key) | Self::After(key) => key.as_slice() >= end,
        }
    }
}

impl<'a> Batch<'a> {
    fn new(db: &'a V2Db, limit: usize, stats: &'a mut RetentionStats) -> Self {
        Self { db, txn: None, writes: 0, limit: limit.max(1), stats }
    }

    /// The open transaction, begun after the previous batch committed.
    fn txn(&mut self) -> &mut Transaction {
        let db = self.db;
        self.txn.get_or_insert_with(|| db.begin(Isolation::ReadCommitted))
    }

    /// Records this transaction may still write.
    fn room(&self) -> usize {
        self.limit - self.writes
    }

    fn write(&mut self, key: Vec<u8>, value: Option<Vec<u8>>) -> Result<(), CoreError> {
        debug_assert!(self.writes < self.limit);
        let txn = self.txn();
        match value {
            Some(value) => txn.put(Keyspace::Kernel, key, value)?,
            None => txn.delete(Keyspace::Kernel, key)?,
        }
        txn.require_features(PROTECTED_RECORDS);
        self.writes += 1;
        if self.writes == self.limit {
            self.flush()?;
        }
        Ok(())
    }

    /// Commits the writes so far; the next read starts a new transaction.
    fn flush(&mut self) -> Result<(), CoreError> {
        let Some(txn) = self.txn.take() else {
            return Ok(());
        };
        if self.writes > 0 {
            txn.commit()?;
            self.stats.transactions += 1;
            self.stats.largest_transaction = self.stats.largest_transaction.max(self.writes);
        }
        self.writes = 0;
        Ok(())
    }

    /// Up to `limit` committed entries from `from` to `end` (exclusive).
    fn scan(&mut self, from: &Cursor, end: &[u8], limit: usize) -> Result<Entries, CoreError> {
        let start = match from {
            Cursor::At(key) => Bound::Included(key.as_slice()),
            Cursor::After(key) => Bound::Excluded(key.as_slice()),
        };
        self.txn()
            .scan(Keyspace::Kernel, start, Bound::Excluded(end), Direction::Forward, Some(limit))?
            .map(|entry| entry.map_err(CoreError::from))
            .collect()
    }
}

fn task_of(key: &[u8]) -> Result<TaskObjectId, CoreError> {
    key.get(1..9)
        .and_then(|bytes| bytes.try_into().ok())
        .map(|bytes| TaskObjectId(u64::from_be_bytes(bytes)))
        .ok_or_else(|| CoreError::Corrupt("retention: short task key".into()))
}

fn seq_of(key: &[u8]) -> Result<u64, CoreError> {
    key.get(9..17)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u64::from_be_bytes)
        .ok_or_else(|| CoreError::Corrupt("retention: short change key".into()))
}

/// One retention pass at `db.wall_now_ms()`. A replicated database holds no
/// protected records and is skipped; a cluster gateway is Unsupported.
pub(crate) fn sweep(db: &V2Db, config: &RetentionConfig) -> Result<RetentionStats, CoreError> {
    db.require_local()?;
    let mut stats = RetentionStats::default();
    if db.meta.features & crate::storage::engine::meta::REPLICATED != 0 {
        return Ok(stats);
    }
    let now = db.wall_now_ms();
    let mut batch = Batch::new(db, config.batch, &mut stats);
    if let Some(cutoff) = now.checked_sub(config.read_log_ms) {
        expire_reads(&mut batch, NS_READ_LOG, cutoff)?;
        expire_reads(&mut batch, NS_DEBIT, cutoff)?;
    }
    if let Some(cutoff) = now.checked_sub(config.before_values_ms) {
        blank_before_values(&mut batch, cutoff)?;
    }
    expire_outcomes(&mut batch, now, config.outcome_ms)?;
    batch.flush()?;
    Ok(stats)
}

/// Deletes `2A`/`2B` entries with `wall_ms < cutoff`, per task in key order,
/// stopping at each task's first young entry.
fn expire_reads(batch: &mut Batch<'_>, ns: u8, cutoff: u64) -> Result<(), CoreError> {
    let end = vec![ns + 1];
    let mut from = Cursor::At(vec![ns]);
    while !from.reached(&end) {
        let entries = batch.scan(&from, &end, batch.room())?;
        if entries.is_empty() {
            break;
        }
        for (key, value) in entries {
            let wall_ms = if ns == NS_READ_LOG {
                ReadLogRecord::decode(&value)?.wall_ms
            } else {
                DebitRecord::decode(&value)?.wall_ms
            };
            if wall_ms >= cutoff {
                from = Cursor::At(task_prefix(ns, task_of(&key)?).1);
                break;
            }
            from = Cursor::After(key.clone());
            batch.write(key, None)?;
            if ns == NS_READ_LOG {
                batch.stats.read_logs_deleted += 1;
            } else {
                batch.stats.debits_deleted += 1;
            }
        }
    }
    Ok(())
}

/// Blanks the before-values of change records decided before `cutoff`.
fn blank_before_values(batch: &mut Batch<'_>, cutoff: u64) -> Result<(), CoreError> {
    let end = vec![NS_CHANGE_HEADER + 1];
    let mut from = Cursor::At(vec![NS_CHANGE_HEADER]);
    while !from.reached(&end) {
        let Some((key, value)) = batch.scan(&from, &end, 1)?.pop() else {
            break;
        };
        let mut header = ChangeHeader::decode(&value)?;
        if header.flags & CHANGE_BEFORE_BLANKED != 0 {
            from = Cursor::After(key);
            continue;
        }
        let task = task_of(&key)?;
        if header.decided_ms >= cutoff {
            from = Cursor::At(task_prefix(NS_CHANGE_HEADER, task).1);
            continue;
        }
        let seq = seq_of(&key)?;
        let mut row_from = Cursor::At(change_row_key(task, seq, 0));
        let row_end = match seq.checked_add(1) {
            Some(next) => change_row_key(task, next, 0),
            None => task_prefix(NS_CHANGE_ROW, task).1,
        };
        loop {
            let room = batch.room();
            let rows = batch.scan(&row_from, &row_end, room)?;
            let done = rows.len() < room;
            for (row_key, row_value) in rows {
                row_from = Cursor::After(row_key.clone());
                let mut row = ChangeRow::decode(&row_value)?;
                if row.flags & ROW_BEFORE_BLANKED == 0 {
                    row.flags |= ROW_BEFORE_BLANKED;
                    row.before.clear();
                    batch.write(row_key, Some(row.encode()))?;
                    batch.stats.rows_blanked += 1;
                }
            }
            if done {
                break;
            }
            // Every scanned slot was used; give the next scan a full budget.
            batch.flush()?;
        }
        header.flags |= CHANGE_BEFORE_BLANKED;
        from = Cursor::After(key.clone());
        batch.write(key, Some(header.encode()))?;
        batch.stats.headers_blanked += 1;
    }
    Ok(())
}

/// Deletes kind 2-4 outcomes past retention whose task can no longer use them.
fn expire_outcomes(batch: &mut Batch<'_>, now: u64, outcome_ms: u64) -> Result<(), CoreError> {
    let db = batch.db;
    let view = db.catalog().committed_view(batch.txn())?;
    let end = vec![NS_OUTCOME + 1];
    let mut from = Cursor::At(vec![NS_OUTCOME]);
    loop {
        let entries = batch.scan(&from, &end, batch.room())?;
        if entries.is_empty() {
            break;
        }
        for (key, value) in entries {
            from = Cursor::After(key.clone());
            let record = OutcomeRecord::decode(&value)?;
            if now < record.retain_until_ms {
                continue;
            }
            let task = match record.payload {
                OutcomePayload::Once(_) => continue,
                OutcomePayload::Committed { task, .. }
                | OutcomePayload::Denied { task, .. }
                | OutcomePayload::BudgetExceeded { task, .. } => task,
            };
            let finished = match view.tasks.get(&task.0) {
                None => true,
                Some(def) => {
                    def.revoked
                        || now >= u64::try_from(def.expires_at.0).unwrap_or(0).saturating_add(outcome_ms)
                }
            };
            if finished {
                batch.write(key, None)?;
                batch.stats.outcomes_deleted += 1;
            }
        }
    }
    Ok(())
}

#[cfg(all(test, feature = "engine-rocksdb"))]
#[path = "retention_tests.rs"]
mod retention_tests;
