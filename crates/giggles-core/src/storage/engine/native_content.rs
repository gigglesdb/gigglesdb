//! Bounded native ledger witnesses shared by open and the commit boundary.
use super::{
    codec, kernel,
    kv::{Direction, Keyspace, KvRead},
    meta, mvcc,
};
use crate::storage::{self, native_once::NativeOutcomes};
use crate::task::records;
use meta::FormatError;
use std::{collections::BTreeMap, ops::Bound};

fn corrupt(error: impl std::fmt::Debug) -> FormatError {
    FormatError::Corrupt(format!("{error:?}"))
}

#[derive(Clone, Default)]
pub(super) struct NativeContent {
    metadata: Option<Vec<u8>>,
    ledger: Option<Vec<u8>>,
    /// The ledger header `26 00` (GD-0061 R3-624). Once it exists, `[07]`
    /// must be the empty v1 ledger: the identities live in `22` records.
    header: Option<Vec<u8>>,
}
impl NativeContent {
    pub(super) fn load(kv: &dyn KvRead) -> Result<Self, FormatError> {
        let latest = |key: &[u8]| -> Result<Option<Vec<u8>>, FormatError> {
            match mvcc::latest_version(kv, Keyspace::Kernel, key).map_err(corrupt)? {
                Some((_, None)) if key == [7] || key == records::LEDGER_HEADER_KEY => {
                    Err(corrupt("native MVCC tombstone"))
                }
                version => Ok(version.and_then(|(_, value)| value)),
            }
        };
        Ok(Self {
            metadata: latest(&[0])?,
            ledger: latest(&[7])?,
            header: latest(&records::LEDGER_HEADER_KEY)?,
        })
    }
    pub(super) fn apply(
        &mut self,
        writes: &BTreeMap<(Keyspace, Vec<u8>), Option<Vec<u8>>>,
    ) -> Result<(), FormatError> {
        for ((space, key), value) in writes {
            if *space != Keyspace::Kernel {
                continue;
            }
            if key.as_slice() == records::LEDGER_HEADER_KEY {
                self.header = Some(
                    value
                        .clone()
                        .ok_or_else(|| corrupt("native MVCC tombstone"))?,
                );
                continue;
            }
            // Protected task records (0x20..=0x2B) are gated by content_check.
            if super::content_check::is_protected_kernel_key(key) {
                continue;
            }
            if !kernel::valid_key(key) && key.as_slice() != [255] {
                return Err(corrupt("kernel namespace or key length"));
            }
            match key.as_slice() {
                [0] => self.metadata = value.clone(),
                [7] => {
                    self.ledger = Some(
                        value
                            .clone()
                            .ok_or_else(|| corrupt("native MVCC tombstone"))?,
                    )
                }
                [255] if value.as_deref() != Some(&[]) => {
                    return Err(corrupt("kernel mutation stamp"));
                }
                _ => {}
            }
        }
        Ok(())
    }
    pub(super) fn validate(&self, features: u64, version: u32) -> Result<(), FormatError> {
        if let Some(header) = &self.header {
            records::LedgerHeader::decode(header).map_err(corrupt)?;
            // A migrated store keeps `[07]` only as the empty v1 ledger.
            if let Some(ledger) = &self.ledger
                && !NativeOutcomes::from_bytes(ledger).map_err(corrupt)?.is_empty()
            {
                return Err(corrupt("native ledger entries beside the ledger header"));
            }
        }
        if self.ledger.is_none() && features & meta::NATIVE_OUTCOMES == 0 {
            return Ok(());
        }
        if features & meta::NATIVE_OUTCOMES == 0 || version < meta::NATIVE_SUBVERSION {
            return Err(corrupt("native content requires feature/subversion"));
        }
        let ledger = NativeOutcomes::from_bytes(
            self.ledger
                .as_deref()
                .ok_or_else(|| corrupt("native feature without ledger"))?,
        )
        .map_err(corrupt)?;
        let metadata: storage::Metadata = storage::decode(
            self.metadata
                .as_deref()
                .ok_or_else(|| corrupt("native ledger without metadata"))?,
        )
        .map_err(corrupt)?;
        let (family, _, _, temporal, _) =
            storage::envelope_family(metadata.format).map_err(corrupt)?;
        let typed = ledger.contains_typed().map_err(corrupt)?;
        let timed = ledger.contains_temporal().map_err(corrupt)?;
        if ledger.contains_wide() && features & meta::WIDE_ONCE_RECEIPTS == 0 {
            return Err(corrupt("wide once receipt without its feature bit"));
        }
        if !family.native_outcomes
            || (typed && !family.typed_scalars)
            || (timed && !temporal)
            || (family.typed_scalars && features & 1 == 0)
            || (temporal && features & 16 == 0)
        {
            return Err(corrupt("native ledger/metadata format mismatch"));
        }
        Ok(())
    }
}

/// Kernel ranges this check owns: the legacy namespaces below 0x0C and the
/// mutation stamp 0xFF. The protected namespaces 0x20..=0x2B (bounded scan) and
/// the unassigned gaps 0x0C..0x20 and 0x2C..0xFF (one seek each) belong to
/// `content_check`, so open stays constant-cost in protected records.
pub(super) const LEGACY_KERNEL_END: u8 = 0x0C;
pub(super) const STAMP_NAMESPACE: u8 = 0xFF;

pub(super) fn check(kv: &dyn KvRead, features: u64, version: u32) -> Result<(), FormatError> {
    let mut previous = None;
    let legacy = kv
        .range(
            Keyspace::Kernel,
            Bound::Unbounded,
            Bound::Excluded(&[LEGACY_KERNEL_END]),
            Direction::Forward,
        )
        .map_err(corrupt)?;
    let stamp = kv
        .range(
            Keyspace::Kernel,
            Bound::Included(&[STAMP_NAMESPACE]),
            Bound::Unbounded,
            Direction::Forward,
        )
        .map_err(corrupt)?;
    for item in legacy.chain(stamp) {
        let (key, value) = item.map_err(corrupt)?;
        let (user, _) = codec::split_versioned(&key).map_err(|e| corrupt(format!("{e:?}")))?;
        if !kernel::valid_key(user) && user != [255] {
            return Err(corrupt("kernel namespace or key length"));
        }
        if previous.as_deref() == Some(user) {
            continue;
        }
        previous = Some(user.to_vec());
        let live = mvcc::decode(&value).map_err(corrupt)?;
        if user == [7] && live.is_none() {
            return Err(corrupt("native MVCC tombstone"));
        }
        if user == [255] && live != Some(&[]) {
            return Err(corrupt("kernel mutation stamp"));
        }
    }
    NativeContent::load(kv)?.validate(features, version)
}
