//! Operation ledger: task operation identity, request fingerprint, outcome
//! lookup and staging (GD-0061 design section 6, D-G).
//!
//! - Identity: `02 ‖ task u64 ‖ len u16 ‖ op bytes`. Native `once` keeps its
//!   own `01 ‖ tenant ‖ ns ‖ key` identity, so the two never collide.
//! - Fingerprint: `sha256("GDTASKREQ\x01" ‖ task u64 ‖ kind u8 ‖ body)`, with
//!   kind 1 Direct (`text B32 ‖ param_count u16 ‖ each param B32`), kind 2
//!   Apply (`preview u64 ‖ digest [32]`) and kind 3 Undo
//!   (`preview u64 ‖ digest [32] ‖ mode u8`). The definition version and the
//!   executor are not hashed.
//! - Lookup never writes and takes no hold; staging only buffers the record
//!   in the caller's transaction.
use super::records::{
    MAX_IDENTITY, MAX_OPERATION_ID, MAX_STATEMENT_TEXT, OutcomeRecord, TaskObjectId, outcome_key,
};
use super::error::{Budget, DenyRule};
use super::receipt::Receipt;
use crate::CoreError;
use crate::storage::{Isolation, Keyspace, PROTECTED_RECORDS, Transaction, V2Db};
use sha2::{Digest, Sha256};

/// Identity tag of a task operation (native `once` uses `01`).
pub(crate) const TASK_IDENTITY_TAG: u8 = 2;
/// Domain prefix of the request fingerprint.
pub(crate) const FINGERPRINT_DOMAIN: &[u8; 10] = b"GDTASKREQ\x01";

pub(crate) const KIND_DIRECT: u8 = 1;
pub(crate) const KIND_APPLY: u8 = 2;
pub(crate) const KIND_UNDO: u8 = 3;

/// What an operation ID's stored outcome says (D-G). Only these three
/// decisions are recorded; every other refusal leaves the ID free.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// The operation committed: its receipt (minimal for an executor, full
    /// with the change record for an inspector).
    Committed(Receipt),
    Denied(DenyRule),
    BudgetExceeded(Budget),
}

/// Client-chosen operation ID of a protected write, 1..=128 bytes.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct OperationId(Vec<u8>);

impl OperationId {
    /// `None` unless `bytes` is 1..=128 bytes long (`OperationIdRequired`).
    pub fn new(bytes: impl Into<Vec<u8>>) -> Option<Self> {
        let bytes = bytes.into();
        (1..=MAX_OPERATION_ID).contains(&bytes.len()).then_some(Self(bytes))
    }
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// The fingerprinted shape of a protected request. `ProtectedRequest`
/// implements [`FingerprintRequest`] by producing one of these; tests build
/// it directly, so they need no `Prepared`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RequestBody<'a> {
    /// `text` is the statement bytes exactly as the entry point received them;
    /// each param is the storage encoding of its `Value`.
    Direct { text: &'a [u8], params: Vec<Vec<u8>> },
    Apply { preview: u64, digest: [u8; 32] },
    Undo { preview: u64, digest: [u8; 32], mode: u8 },
}

impl RequestBody<'_> {
    pub(crate) fn kind(&self) -> u8 {
        match self {
            Self::Direct { .. } => KIND_DIRECT,
            Self::Apply { .. } => KIND_APPLY,
            Self::Undo { .. } => KIND_UNDO,
        }
    }
}

/// A request that can be fingerprinted.
pub(crate) trait FingerprintRequest {
    fn fingerprint_body(&self) -> RequestBody<'_>;
}

impl FingerprintRequest for RequestBody<'_> {
    fn fingerprint_body(&self) -> RequestBody<'_> {
        self.clone()
    }
}

fn b32(out: &mut Vec<u8>, bytes: &[u8]) {
    let len = u32::try_from(bytes.len()).expect("fingerprint field exceeds u32");
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(bytes);
}

/// The exact bytes hashed by [`request_fingerprint`].
pub(crate) fn fingerprint_preimage(task: TaskObjectId, request: &dyn FingerprintRequest) -> Vec<u8> {
    let body = request.fingerprint_body();
    let mut out = Vec::with_capacity(64);
    out.extend_from_slice(FINGERPRINT_DOMAIN);
    out.extend_from_slice(&task.0.to_be_bytes());
    out.push(body.kind());
    match &body {
        RequestBody::Direct { text, params } => {
            assert!(text.len() <= MAX_STATEMENT_TEXT, "statement text length");
            b32(&mut out, text);
            let count = u16::try_from(params.len()).expect("parameter count exceeds u16");
            out.extend_from_slice(&count.to_be_bytes());
            for param in params {
                b32(&mut out, param);
            }
        }
        RequestBody::Apply { preview, digest } => {
            out.extend_from_slice(&preview.to_be_bytes());
            out.extend_from_slice(digest);
        }
        RequestBody::Undo { preview, digest, mode } => {
            out.extend_from_slice(&preview.to_be_bytes());
            out.extend_from_slice(digest);
            out.push(*mode);
        }
    }
    out
}

/// `sha256("GDTASKREQ\x01" ‖ task u64 ‖ kind u8 ‖ body)`.
pub(crate) fn request_fingerprint(task: TaskObjectId, request: &dyn FingerprintRequest) -> [u8; 32] {
    Sha256::digest(fingerprint_preimage(task, request)).into()
}

/// `02 ‖ task u64 ‖ len u16 ‖ op bytes`.
pub(crate) fn task_identity(task: TaskObjectId, op: &OperationId) -> Vec<u8> {
    let bytes = op.as_bytes();
    let mut identity = Vec::with_capacity(11 + bytes.len());
    identity.push(TASK_IDENTITY_TAG);
    identity.extend_from_slice(&task.0.to_be_bytes());
    identity.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
    identity.extend_from_slice(bytes);
    debug_assert!(identity.len() <= MAX_IDENTITY);
    identity
}

/// The `22` key of a task operation in this database's domain.
pub(crate) fn task_outcome_key(db: &V2Db, task: TaskObjectId, op: &OperationId) -> Vec<u8> {
    outcome_key(db.meta.domain, &task_identity(task, op))
}

/// Latest committed outcome at `key`. Never writes and takes no hold: the
/// read-committed transaction is dropped without committing.
pub(crate) fn lookup(db: &V2Db, key: &[u8]) -> Result<Option<OutcomeRecord>, CoreError> {
    let txn = db.begin(Isolation::ReadCommitted);
    let value = txn.kernel_latest_committed(key)?;
    drop(txn);
    value.map(|value| OutcomeRecord::decode(&value)).transpose()
}

/// Buffer `record` at `key` in `txn`; it becomes visible only when `txn` commits.
/// Declares `PROTECTED_RECORDS`, without which the commit gate refuses the key.
pub(crate) fn stage(txn: &mut Transaction, key: Vec<u8>, record: &OutcomeRecord) -> Result<(), CoreError> {
    if super::records::classify_key(&key)? != super::records::Namespace::Outcome {
        return Err(CoreError::Corrupt("task ledger: not an outcome key".into()));
    }
    txn.put(Keyspace::Kernel, key, record.encode())?;
    txn.require_features(PROTECTED_RECORDS);
    Ok(())
}

#[cfg(test)]
#[path = "ledger_tests.rs"]
mod ledger_tests;
