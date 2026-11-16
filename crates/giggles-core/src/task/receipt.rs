//! Receipt and change record builders, effects digest (GD-0061 design
//! section 6, D-H).
//!
//! - Per-row hashes: `before_hash = sha256(salt ‖ before payload)` and
//!   `after_hash = sha256(salt ‖ after payload)`, where a payload is the row
//!   payload encoding of the full row values. The 16-byte salt lives in the
//!   `24` header (inspection privilege only), so an executor cannot guess
//!   low-entropy hidden values from the digest.
//! - Effects digest: `sha256("GDTASKCHG\x01" ‖ task u64 ‖ seq u64 ‖ table u64 ‖
//!   row_count u32 ‖ for each row in physical-key order: pk_len u16 ‖ pk ‖
//!   version_before u64 ‖ version_written u64 ‖ flags u8 (bit0 only) ‖
//!   before_hash ‖ after_hash)`.
//! - The digest uses only the hashes, so it stays checkable after retention
//!   blanks the before-values.
use super::effects::Effects;
use super::error::{DenyRule, TaskError};
use super::records::{
    ChangeHeader, ChangeRow, ReceiptRecord, TaskObjectId, ROW_BEFORE_BLANKED, ROW_NOOP,
};
use crate::relational::Value;
use crate::CoreError;
use sha2::{Digest, Sha256};

/// Domain prefix of the effects digest.
pub(crate) const DIGEST_DOMAIN: &[u8; 10] = b"GDTASKCHG\x01";
/// Change-record header `kind` of an ordinary change (02 is an undo).
pub(crate) const KIND_CHANGE: u8 = 1;
/// Receipt `decision` of a committed operation (the only decision that writes a receipt).
pub(crate) const DECISION_COMMITTED: u8 = 1;

/// `sha256(salt ‖ payload)`.
pub(crate) fn row_hash(salt: &[u8; 16], payload: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(salt);
    h.update(payload);
    h.finalize().into()
}

fn payload(values: &[Value]) -> Result<Vec<u8>, CoreError> {
    crate::storage::task_row_payload(values)
}

/// The exact bytes hashed by [`effects_digest`]. `rows` are taken in the
/// order given, which for a built change record is physical-key order.
pub(crate) fn digest_preimage(task: TaskObjectId, seq: u64, table: u64, rows: &[ChangeRow]) -> Vec<u8> {
    let mut out = Vec::with_capacity(38 + rows.len() * 120);
    out.extend_from_slice(DIGEST_DOMAIN);
    out.extend_from_slice(&task.0.to_be_bytes());
    out.extend_from_slice(&seq.to_be_bytes());
    out.extend_from_slice(&table.to_be_bytes());
    let count = u32::try_from(rows.len()).expect("change record row count exceeds u32");
    out.extend_from_slice(&count.to_be_bytes());
    for row in rows {
        let pk_len = u16::try_from(row.pk.len()).expect("primary key exceeds u16");
        out.extend_from_slice(&pk_len.to_be_bytes());
        out.extend_from_slice(&row.pk);
        out.extend_from_slice(&row.version_before.to_be_bytes());
        out.extend_from_slice(&row.version_written.to_be_bytes());
        out.push(row.flags & ROW_NOOP);
        out.extend_from_slice(&row.before_hash);
        out.extend_from_slice(&row.after_hash);
    }
    out
}

/// The effects digest of a change record.
pub(crate) fn effects_digest(task: TaskObjectId, seq: u64, table: u64, rows: &[ChangeRow]) -> [u8; 32] {
    Sha256::digest(digest_preimage(task, seq, table, rows)).into()
}

/// Build the change record (`24` header, `25` rows) and its effects digest.
///
/// Rows come out in physical-key order with `row_no` = index. The header has
/// `kind` 1 (change) and a zero `outcome_key`; the caller sets the operation's
/// outcome key hash (and `kind` 2 for an undo) before staging. An effect on
/// another table is `Denied(OtherTable)`; one without an old or a new row is
/// `Denied(PrimaryKeyChange)`.
pub(crate) fn build_change(
    task: TaskObjectId,
    seq: u64,
    table: u64,
    effects: &Effects,
    salt: [u8; 16],
    decided_ms: u64,
) -> Result<(ChangeHeader, Vec<ChangeRow>, [u8; 32]), TaskError> {
    let mut rows = Vec::with_capacity(effects.len());
    for effect in effects.iter() {
        if effect.table != table {
            return Err(TaskError::Denied(DenyRule::OtherTable));
        }
        let (Some(old), Some(new)) = (&effect.old, &effect.new) else {
            return Err(TaskError::Denied(DenyRule::PrimaryKeyChange));
        };
        if effect.pk.is_empty() || effect.pk.len() > usize::from(u16::MAX) {
            return Err(CoreError::Corrupt("change record: primary key length".into()).into());
        }
        let before = payload(&old.values)?;
        let after = payload(&new.values)?;
        rows.push(ChangeRow {
            flags: if Effects::is_noop(effect) { ROW_NOOP } else { 0 },
            table,
            pk: effect.pk.clone(),
            version_before: old.version,
            version_written: new.version,
            before_hash: row_hash(&salt, &before),
            after_hash: row_hash(&salt, &after),
            before,
        });
    }
    let row_count = u32::try_from(rows.len()).map_err(|_| CoreError::Limit)?;
    let digest = effects_digest(task, seq, table, &rows);
    let header = ChangeHeader {
        flags: 0,
        kind: KIND_CHANGE,
        outcome_key: [0; 32],
        table,
        row_count,
        salt,
        effects_digest: digest,
        decided_ms,
    };
    Ok((header, rows, digest))
}

/// Whether `rows` (in `row_no` order) match `header`: the row count, the
/// table, the before hash of every row that still has its before-values,
/// and the effects digest. Blanked rows are checked through their hashes.
pub(crate) fn verify_digest(task: TaskObjectId, seq: u64, header: &ChangeHeader, rows: &[ChangeRow]) -> bool {
    if usize::try_from(header.row_count).ok() != Some(rows.len()) {
        return false;
    }
    let rows_ok = rows.iter().all(|row| {
        row.table == header.table
            && (row.flags & ROW_BEFORE_BLANKED != 0 || row_hash(&header.salt, &row.before) == row.before_hash)
    });
    rows_ok && effects_digest(task, seq, header.table, rows) == header.effects_digest
}

/// The `24` header and `25` rows of one committed operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ChangeRecord {
    pub(crate) header: ChangeHeader,
    pub(crate) rows: Vec<ChangeRow>,
}

/// What only the full (inspection) view of a receipt carries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ReceiptDetail {
    /// The stored `23` record: roles, versions and after-counters.
    pub(crate) record: ReceiptRecord,
    /// The change record, when the caller loaded it.
    pub(crate) change: Option<ChangeRecord>,
}

/// The receipt of a committed protected operation.
///
/// The public fields are the minimal view an executor gets. The full view
/// ([`Receipt::from_record`]) adds the roles, versions, after-counters and
/// the change record; [`Receipt::minimal`] drops them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Receipt {
    pub operation_id: Vec<u8>,
    pub decision: u8,
    pub seq: u64,
    pub rows_affected: u32,
    pub distinct_charged: u32,
    pub transitions_charged: u32,
    pub noop_rows: u32,
    pub effects_digest: [u8; 32],
    pub request_fp: [u8; 32],
    pub decided_ms: u64,
    pub(crate) detail: Option<Box<ReceiptDetail>>,
}

impl Receipt {
    /// The full view of a stored receipt, with its change record if loaded.
    pub(crate) fn from_record(record: &ReceiptRecord, change: Option<ChangeRecord>) -> Self {
        Self {
            operation_id: record.op_id.clone(),
            decision: record.decision,
            seq: record.seq,
            rows_affected: record.rows_affected,
            distinct_charged: record.distinct_charged,
            transitions_charged: record.transitions_charged,
            noop_rows: record.noop_rows,
            effects_digest: record.effects_digest,
            request_fp: record.request_fp,
            decided_ms: record.decided_ms,
            detail: Some(Box::new(ReceiptDetail { record: record.clone(), change })),
        }
    }

    /// The executor's view: no roles, versions, after-counters or change record.
    pub fn minimal(&self) -> Self {
        Self { detail: None, ..self.clone() }
    }

    pub fn is_minimal(&self) -> bool {
        self.detail.is_none()
    }

    pub(crate) fn record(&self) -> Option<&ReceiptRecord> {
        self.detail.as_ref().map(|d| &d.record)
    }

    pub(crate) fn change(&self) -> Option<&ChangeRecord> {
        self.detail.as_ref().and_then(|d| d.change.as_ref())
    }

    pub fn issuer(&self) -> Option<&str> {
        self.record().map(|r| r.issuer.as_str())
    }

    pub fn executor(&self) -> Option<&str> {
        self.record().map(|r| r.executor.as_str())
    }

    pub fn approver(&self) -> Option<&str> {
        self.record().and_then(|r| r.approver.as_deref())
    }
}

#[cfg(test)]
#[path = "receipt_tests.rs"]
mod receipt_tests;
