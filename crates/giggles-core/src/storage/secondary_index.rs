//! Coordinated namespace5/8 storage path. No standalone indexed base escapes.
use super::*;
use crate::relational::secondary_index::codec::{self as index_codec, PendingDecode};

pub(super) fn family(version: u32) -> Result<(format_policy::Features, bool, bool), CoreError> {
    match version {
        1..=6 => format_policy::Features::read(version).map(|f| (f, false, false)),
        7..=10 => format_policy::Features::read_with(version).map(|(f, i)| (f, i, false)),
        11..=14 => format_policy::Features::read_application(version).map(|(f, a)| (f, false, a)),
        _ => None,
    }
    .ok_or(CoreError::Unsupported)
}
pub(super) fn gate(
    records: &BTreeMap<Vec<u8>, Vec<u8>>,
    indexed: bool,
    applications: bool,
) -> Result<(), CoreError> {
    // Reject an unqualified combined family before touching either payload.
    let sidecar = records.keys().any(|k| k.first() == Some(&8));
    let ledger = records.keys().any(|k| k.first() == Some(&9));
    if (indexed && ledger) || (applications && sidecar) || (sidecar && ledger) {
        return Err(CoreError::Unsupported);
    }
    if indexed != records.contains_key(&vec![8]) || (!indexed && sidecar) {
        return Err(CoreError::Corrupt("index format/namespace mismatch".into()));
    }
    Ok(())
}
pub(super) fn sql_error(e: relational::SqlError) -> CoreError {
    if crate::cancellation::check().is_err() {
        return CoreError::Cancelled;
    }
    match e {
        relational::SqlError::Limit => CoreError::Limit,
        _ => CoreError::Corrupt(format!("secondary index invariant: {e:?}")),
    }
}
pub(super) fn sql_error_with_cancel(
    error: relational::SqlError,
    cancel: &mut impl FnMut() -> bool,
) -> CoreError {
    if cancel() || matches!(error, relational::SqlError::Aborted) {
        return CoreError::Cancelled;
    }
    match error {
        relational::SqlError::Limit => CoreError::Limit,
        _ => CoreError::Corrupt(format!("secondary index invariant: {error:?}")),
    }
}
pub(super) fn pending(
    bytes: &[u8],
    cancel: &mut impl FnMut() -> bool,
) -> Result<PendingDecode, CoreError> {
    index_codec::decode_pending(bytes, cancel).map_err(|e| match e {
        crate::relational::secondary_index::Error::Cancelled => CoreError::Cancelled,
        crate::relational::secondary_index::Error::Limit => CoreError::Limit,
        _ => CoreError::Corrupt(format!("secondary index payload: {e:?}")),
    })
}
pub(super) fn persist(sql: &relational::Relational) -> Result<(Vec<u8>, Vec<u8>), CoreError> {
    let (base, payload) = sql
        .secondary_persistence_engine(|| crate::cancellation::check().is_err())
        .map_err(sql_error)?;
    // The bridge uses fixed little-endian bincode; storage namespace5 has always
    // used fixed big-endian bincode. Re-encode only its index-free paired base.
    let base: relational::Relational = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(MAX_BYTES)
        .reject_trailing_bytes()
        .deserialize(&base)
        .map_err(|e| CoreError::Corrupt(e.to_string()))?;
    let size = codec()
        .serialized_size(&base)
        .map_err(|e| CoreError::Corrupt(e.to_string()))?;
    if size
        .checked_add(payload.len() as u64)
        .and_then(|n| n.checked_add(42))
        .is_none_or(|n| n > MAX_BYTES)
    {
        return Err(CoreError::Limit);
    }
    crate::cancellation::check()?;
    Ok((encode(&base)?, payload))
}

#[cfg(test)]
mod tests;

#[cfg(test)]
#[path = "../operations/catalog/unique/writer_tests.rs"]
mod public_unique_writer_tests;
