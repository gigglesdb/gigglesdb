//! One-time move of the V2ONCE `[07]` ledger blob into per-identity `22`
//! outcome records (GD-0061 R3-624, design section 6 D2/D-G).
//!
//! One transaction under `Kernel::lock`:
//! 1. one kind-1 `22` record per ledger entry, keyed by
//!    `sha256(entry domain ‖ identity)`; times are the ledger's seconds x 1000,
//!    and a compacted entry (no result) carries the payload-blanked flag;
//! 2. the ledger header `26 00` with the ledger's upper watermark, the entry
//!    count and `migrated_v1 = 1`;
//! 3. `[07]` rewritten as the empty v1 ledger (never a tombstone, so a reader
//!    of `[07]` and the native content check keep working);
//! 4. `require_features(PROTECTED_RECORDS)`.
//!
//! Readers use `[07]` while `26 00` is absent and the `22` records once it is
//! present ([`lookup_v2`]). A crash before the commit leaves the blob exactly
//! as it was; a second run finds `26 00` and writes nothing.
// R3-625 wires publication, compaction and lookups to these entry points.
#![cfg_attr(not(all(test, feature = "engine-rocksdb")), allow(dead_code))]
use super::*;
use crate::storage::engine::{
    kernel::{Kernel, KernelGuard},
    kv::Keyspace,
    meta::PROTECTED_RECORDS,
    txn::{Isolation, Transaction, TxnError},
};
use crate::task::records::{
    LEDGER_HEADER_KEY, LedgerHeader, OUTCOME_PAYLOAD_BLANKED, OutcomePayload, OutcomeRecord,
    outcome_key,
};

/// What one migration attempt found.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Migration {
    /// No `[07]` ledger: nothing to move and nothing written.
    NoLedger,
    /// `26 00` already exists: nothing written.
    AlreadyMigrated(LedgerHeader),
    /// The ledger was moved; the header that was written.
    Migrated(LedgerHeader),
}

impl NativeOutcomes {
    /// No identities (the shape `[07]` keeps after migration).
    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

fn millis(seconds: u64) -> Result<u64, CoreError> {
    seconds
        .checked_mul(1000)
        .ok_or_else(|| CoreError::Corrupt("native ledger time exceeds milliseconds".into()))
}

/// The `22` key and kind-1 record of one v1 ledger entry. The key uses the
/// entry's own domain, so entries kept from an earlier domain stay distinct.
pub(super) fn outcome_entry(key: &NativeOnceKey, entry: &Entry) -> Result<(Vec<u8>, OutcomeRecord), CoreError> {
    let (flags, payload) = match &entry.record.result {
        Some(result) if result.is_empty() => {
            return Err(CoreError::Corrupt("native ledger: empty retained result".into()));
        }
        Some(result) => (0, result.clone()),
        None => (OUTCOME_PAYLOAD_BLANKED, Vec::new()),
    };
    let record = OutcomeRecord {
        flags,
        identity: key.identity.clone(),
        request_fp: entry.record.submitted_fingerprint,
        accepted_fp: entry.record.accepted_fingerprint,
        decided_lower_ms: millis(entry.decided_lower_unix_seconds)?,
        decided_upper_ms: millis(entry.decided_upper_unix_seconds)?,
        retain_until_ms: millis(entry.record.retry_until_seconds)?,
        payload: OutcomePayload::Once(payload),
    };
    Ok((outcome_key(key.database_domain.to_be_bytes(), &key.identity), record))
}

/// The native record a kind-1 `22` record stands for.
pub(super) fn native_record(record: &OutcomeRecord) -> Result<NativeOnceRecord, CoreError> {
    let OutcomePayload::Once(payload) = &record.payload else {
        return Err(CoreError::Corrupt("native once key holds a task outcome".into()));
    };
    Ok(NativeOnceRecord {
        submitted_fingerprint: record.request_fp,
        accepted_fingerprint: record.accepted_fp,
        retry_until_seconds: record.retain_until_ms / 1000,
        result: (record.flags & OUTCOME_PAYLOAD_BLANKED == 0).then(|| payload.clone()),
    })
}

/// The ledger header at the transaction's snapshot.
pub(crate) fn ledger_header(txn: &mut Transaction) -> Result<Option<LedgerHeader>, CoreError> {
    txn.get(Keyspace::Kernel, &LEDGER_HEADER_KEY)?
        .map(|value| LedgerHeader::decode(&value))
        .transpose()
}

/// Latest committed record for `key`: from `[07]` while `26 00` is absent,
/// from its `22` record once the ledger has been migrated.
pub(super) fn lookup_v2(db: &crate::storage::v2::V2Db, key: &NativeOnceKey) -> Result<Option<NativeOnceRecord>, CoreError> {
    let mut txn = db.begin(Isolation::RepeatableRead);
    if ledger_header(&mut txn)?.is_none() {
        drop(txn);
        return Ok(v2::read_v2_ledger(db)?.and_then(|ledger| ledger.retained(key).cloned()));
    }
    let value = txn.get(Keyspace::Kernel, &outcome_key(key.database_domain.to_be_bytes(), &key.identity))?;
    drop(txn);
    match value {
        None => Ok(None),
        Some(value) => {
            let record = OutcomeRecord::decode(&value)?;
            if record.identity != key.identity {
                return Err(CoreError::Corrupt("outcome identity does not match its key".into()));
            }
            native_record(&record).map(Some)
        }
    }
}

/// Stage the migration in the guarded transaction (the caller commits it).
/// Publication and compaction call this first, under the kernel lock they
/// already hold, so the move commits atomically with their own write.
pub(crate) fn stage_migration(guard: &mut KernelGuard<'_>) -> Result<Migration, CoreError> {
    if let Some(header) = ledger_header(guard.transaction())? {
        return Ok(Migration::AlreadyMigrated(header));
    }
    let presence = Kernel::probe(guard.transaction())?;
    if presence.ledger && !presence.metadata {
        return Err(CoreError::Corrupt("native ledger without kernel metadata".into()));
    }
    if !presence.ledger {
        return Ok(Migration::NoLedger);
    }
    let before = guard.load()?;
    let ledger = before.0.native_outcomes.as_ref()
        .ok_or_else(|| CoreError::Corrupt("native ledger missing from kernel image".into()))?;
    let txn = guard.transaction();
    for (key, entry) in &ledger.entries {
        let (outcome, record) = outcome_entry(key, entry)?;
        txn.put(Keyspace::Kernel, outcome, record.encode())?;
    }
    let header = LedgerHeader {
        once_upper_watermark_s: ledger.upper_watermark_unix_seconds,
        once_identities: ledger.entries.len() as u64,
        migrated_v1: true,
    };
    txn.put(Keyspace::Kernel, LEDGER_HEADER_KEY.to_vec(), header.encode())?;
    txn.require_features(PROTECTED_RECORDS);
    let mut after = before.clone();
    after.0.native_outcomes = Some(NativeOutcomes::default());
    Kernel::stage_from(guard, Some(&before), &after)?;
    Ok(Migration::Migrated(header))
}

impl Admin {
    /// Run the migration in its own transaction. Refused (`Unsupported`) on a
    /// replicated store or a cluster gateway, like every protected record.
    pub(crate) fn migrate_native_once_v2(&self) -> Result<Migration, CoreError> {
        let inner = &self.inner;
        let Backend::V2(db) = &inner.backend else { return Err(CoreError::Unsupported) };
        let _ticket = inner.enter()?;
        if db.task_kernel_unsupported() {
            return Err(CoreError::Unsupported);
        }
        let mut txn = db.begin(Isolation::RepeatableRead);
        let migration = {
            let mut kernel = Kernel::lock(&mut txn, &db.kv, None, &|| crate::cancellation::check().is_err())?;
            stage_migration(&mut kernel)?
        };
        if !matches!(migration, Migration::Migrated(_)) {
            return Ok(migration);
        }
        let _gate = crate::cancellation::lock_notified(&inner.gate, || inner.decision_point(DecisionPoint::WaitingLock))?;
        if inner.fenced.load(Ordering::Acquire) {
            return Err(CoreError::Fenced);
        }
        let point = *inner.crash.lock().map_err(|_| CoreError::Fenced)?;
        if point == CrashPoint::BeforeCommit { crash_now(); }
        if point == CrashPoint::ErrorBeforeCommit {
            return Err(CoreError::Storage("injected before commit".into()));
        }
        db.catalog().commit_ddl(txn).map_err(|error| match error {
            TxnError::CommitAmbiguous => {
                inner.fenced.store(true, Ordering::Release);
                CoreError::Storage("engine commit outcome unknown".into())
            }
            other => CoreError::from(other),
        })?;
        Ok(migration)
    }
}

#[cfg(all(test, feature = "engine-rocksdb"))]
#[path = "v2_migrate_tests.rs"]
mod tests;
