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
//! present. A crash before the commit leaves the blob exactly as it was; a
//! second run finds `26 00` and writes nothing.
//!
//! R3-625: the first `once` publication or compaction on a local store stages
//! the migration under the kernel lock it already holds, then writes its own
//! outcome as a `22` record and updates `26 00` ([`stage_outcome`],
//! [`stage_compaction`]). Admission and lookup read the `22` records once
//! `26 00` exists ([`shared_record`]).
// Publication and compaction (the callers) are engine-rocksdb only.
#![cfg_attr(not(feature = "engine-rocksdb"), allow(dead_code))]
use super::*;
use crate::storage::engine::{
    kernel::{Kernel, KernelGuard},
    kv::Keyspace,
    meta::PROTECTED_RECORDS,
    txn::{Isolation, Transaction, TxnError},
};
use crate::task::records::{
    LEDGER_HEADER_KEY, LedgerHeader, NS_OUTCOME, OUTCOME_PAYLOAD_BLANKED, OutcomePayload,
    OutcomeRecord, outcome_key,
};
use std::ops::Bound;

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

/// True for identities that carry a task (`02`) or undo (`03`) tag: native
/// `once` identities start with `01` (`operations/once.rs` `key`).
pub(super) fn reserved_identity(identity: &[u8]) -> bool {
    matches!(
        identity.first(),
        Some(&crate::task::ledger::TASK_IDENTITY_TAG | &crate::task::ledger::UNDO_IDENTITY_TAG)
    )
}

/// The `22` key of a native identity, in the identity's own domain.
fn native_outcome_key(key: &NativeOnceKey) -> Vec<u8> {
    outcome_key(key.database_domain.to_be_bytes(), &key.identity)
}

/// Decode the `22` value at `key`'s outcome key as a native record.
fn decode_native(key: &NativeOnceKey, value: &[u8]) -> Result<NativeOnceRecord, CoreError> {
    let record = OutcomeRecord::decode(value)?;
    if record.identity != key.identity {
        return Err(CoreError::Corrupt("outcome identity does not match its key".into()));
    }
    native_record(&record)
}

/// Latest committed `22` record for `key` (the shared ledger; `26 00` exists).
pub(super) fn shared_record(db: &crate::storage::v2::V2Db, key: &NativeOnceKey) -> Result<Option<NativeOnceRecord>, CoreError> {
    let mut txn = db.begin(Isolation::RepeatableRead);
    let value = txn.get(Keyspace::Kernel, &native_outcome_key(key))?;
    drop(txn);
    value.map(|value| decode_native(key, &value)).transpose()
}

/// Latest committed record for `key`: from `[07]` while `26 00` is absent,
/// from its `22` record once the ledger has been migrated.
#[cfg(test)]
pub(super) fn lookup_v2(db: &crate::storage::v2::V2Db, key: &NativeOnceKey) -> Result<Option<NativeOnceRecord>, CoreError> {
    let (ledger, header) = v2::read_v2_view(db)?;
    match header {
        Some(_) => shared_record(db, key),
        None => Ok(ledger.and_then(|ledger| ledger.retained(key).cloned())),
    }
}

/// The header a shared-ledger write starts from: migrate `[07]` first (under
/// the caller's kernel lock); a store that never had a ledger starts empty.
/// The flag is true when this call staged the migration.
pub(super) fn shared_header(guard: &mut KernelGuard<'_>) -> Result<(Option<LedgerHeader>, bool), CoreError> {
    Ok(match stage_migration(guard)? {
        Migration::NoLedger => (None, false),
        Migration::AlreadyMigrated(header) => (Some(header), false),
        Migration::Migrated(header) => (Some(header), true),
    })
}

fn put_header(guard: &mut KernelGuard<'_>, header: &LedgerHeader) -> Result<(), CoreError> {
    let txn = guard.transaction();
    txn.put(Keyspace::Kernel, LEDGER_HEADER_KEY.to_vec(), header.encode())?;
    txn.require_features(PROTECTED_RECORDS);
    Kernel::touch(guard)
}

/// Stage one new outcome as a kind-1 `22` record and count it in `26 00`.
/// Same rules as the v1 `insert_outcome`: the 1,024-identity cap (blanked
/// identities count), retention 1..=7 days, payload <= 2 MiB and a valid
/// GDOU receipt, and `retry_until = decided_upper + retention`. Returns the
/// record the caller answers with.
#[allow(clippy::too_many_arguments)]
pub(super) fn stage_outcome(
    guard: &mut KernelGuard<'_>,
    header: &LedgerHeader,
    key: &NativeOnceKey,
    submitted: [u8; 32],
    accepted: [u8; 32],
    result: Vec<u8>,
    retention_seconds: u64,
    lease: NativeClockLease,
) -> NativeResult<NativeOnceRecord> {
    if key.identity.is_empty() || key.identity.len() > MAX_NATIVE_IDENTITY_BYTES || reserved_identity(&key.identity) {
        return Err(NativeOnceError::RetentionUnavailable);
    }
    let raw_key = native_outcome_key(key);
    if guard.transaction().get(Keyspace::Kernel, &raw_key).map_err(CoreError::from)?.is_some() {
        return Err(NativeOnceError::IdempotencyConflict);
    }
    if !(1..=MAX_NATIVE_RETENTION_SECONDS).contains(&retention_seconds)
        || result.is_empty()
        || result.len() > MAX_NATIVE_RESULT_BYTES
        || header.once_identities >= MAX_NATIVE_RECORDS as u64
    {
        return Err(NativeOnceError::RetentionUnavailable);
    }
    let retry_until_seconds = lease.upper_unix_seconds.checked_add(retention_seconds)
        .ok_or(NativeOnceError::ClockUncertain)?;
    let ms = |seconds: u64| seconds.checked_mul(1000).ok_or(NativeOnceError::ClockUncertain);
    crate::operations::once::outcome::validate(&result)?;
    let wide = crate::operations::once::outcome::is_wide(&result);
    let record = OutcomeRecord {
        flags: 0,
        identity: key.identity.clone(),
        request_fp: submitted,
        accepted_fp: accepted,
        decided_lower_ms: ms(lease.lower_unix_seconds)?,
        decided_upper_ms: ms(lease.upper_unix_seconds)?,
        retain_until_ms: ms(retry_until_seconds)?,
        payload: OutcomePayload::Once(result),
    };
    let response = native_record(&record)?;
    let txn = guard.transaction();
    txn.put(Keyspace::Kernel, raw_key, record.encode()).map_err(CoreError::from)?;
    if wide {
        txn.require_features(crate::storage::engine::meta::WIDE_ONCE_RECEIPTS);
    }
    put_header(guard, &LedgerHeader {
        once_upper_watermark_s: lease.upper_unix_seconds,
        once_identities: header.once_identities + 1,
        migrated_v1: header.migrated_v1,
    })?;
    Ok(response)
}

/// Blank every kind-1 `22` payload whose retry window ended before
/// `lease.lower` (flags bit 0 set, payload emptied; identity, fingerprints
/// and times kept, so the identity still counts and still answers Expired).
/// Task outcomes (kinds 2-4) are never touched. Returns the count; on a
/// nonzero count `26 00` takes the lease's upper bound as its watermark.
pub(super) fn stage_compaction(
    guard: &mut KernelGuard<'_>,
    header: &LedgerHeader,
    lease: NativeClockLease,
) -> Result<usize, CoreError> {
    let txn = guard.transaction();
    let mut blanked = Vec::new();
    for item in txn.scan(
        Keyspace::Kernel,
        Bound::Included(&[NS_OUTCOME][..]),
        Bound::Excluded(&[NS_OUTCOME + 1][..]),
        crate::storage::engine::kv::Direction::Forward,
        None,
    )? {
        let (key, value) = item?;
        let mut record = OutcomeRecord::decode(&value)?;
        if !matches!(record.payload, OutcomePayload::Once(_))
            || record.flags & OUTCOME_PAYLOAD_BLANKED != 0
            || lease.lower_unix_seconds <= record.retain_until_ms / 1000
        {
            continue;
        }
        record.flags |= OUTCOME_PAYLOAD_BLANKED;
        record.payload = OutcomePayload::Once(Vec::new());
        blanked.push((key, record));
    }
    if blanked.is_empty() {
        return Ok(0);
    }
    for (key, record) in &blanked {
        txn.put(Keyspace::Kernel, key.clone(), record.encode())?;
    }
    put_header(guard, &LedgerHeader {
        once_upper_watermark_s: lease.upper_unix_seconds,
        ..header.clone()
    })?;
    Ok(blanked.len())
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
    // Standalone admin entry point; publication and compaction migrate inline.
    #[cfg_attr(not(test), allow(dead_code))]
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
