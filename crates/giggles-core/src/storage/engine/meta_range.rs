//! Range-local Meta keys (GD-0075 design section 6): `range/` || id BE8 || `/` || sub.
//! Owned by meta.rs so `validate_meta` checks them without the RocksDB engine.
use super::{FormatError, check_features, corrupt};
use crate::storage::range_api::{RangeDescriptor, RangeId, SYSTEM_RANGE};

const RANGE_PREFIX: &[u8] = b"range/";

/// The sub-key under a range's Meta prefix.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RangeLocalKey {
    /// Raft index BE8 || term BE8.
    Applied,
    /// BE8.
    LastCommitTs,
    /// `RangeDescriptor::encode`.
    Descriptor,
    /// u64 BE feature bits; a subset of the node's features.
    Features,
    /// BE8; system range only.
    OracleHw,
    /// BE8; the range's GC watermark (node key `gc_watermark` in single-node mode).
    GcWatermark,
    /// Pending-install marker (opaque here).
    Install,
    /// `prepared/` || txn BE8 (opaque value).
    Prepared(u64),
    /// `home/` || txn BE8 (opaque value).
    Home(u64),
    /// `marker/` || xid BE8; value is a commit ts BE8.
    Marker(u64),
}

/// `range/` || id BE8 || `/`.
#[cfg_attr(not(feature = "engine-rocksdb"), allow(dead_code))]
pub(crate) fn range_local_prefix(id: RangeId) -> Vec<u8> {
    let mut key = Vec::with_capacity(RANGE_PREFIX.len() + 9 + 17);
    key.extend_from_slice(RANGE_PREFIX);
    key.extend_from_slice(&id.0.to_be_bytes());
    key.push(b'/');
    key
}

#[cfg_attr(not(feature = "engine-rocksdb"), allow(dead_code))]
pub(crate) fn range_local_key(id: RangeId, sub: &RangeLocalKey) -> Vec<u8> {
    debug_assert_ne!(id.0, 0, "range id 0 is never valid");
    let (name, arg): (&[u8], Option<u64>) = match *sub {
        RangeLocalKey::Applied => (b"applied", None),
        RangeLocalKey::LastCommitTs => (b"last_commit_ts", None),
        RangeLocalKey::Descriptor => (b"descriptor", None),
        RangeLocalKey::Features => (b"features", None),
        RangeLocalKey::OracleHw => (b"oracle_hw", None),
        RangeLocalKey::GcWatermark => (b"gc_watermark", None),
        RangeLocalKey::Install => (b"install", None),
        RangeLocalKey::Prepared(txn) => (b"prepared/", Some(txn)),
        RangeLocalKey::Home(txn) => (b"home/", Some(txn)),
        RangeLocalKey::Marker(xid) => (b"marker/", Some(xid)),
    };
    let mut key = range_local_prefix(id);
    key.extend_from_slice(name);
    if let Some(arg) = arg {
        key.extend_from_slice(&arg.to_be_bytes());
    }
    key
}

/// None for any key that is not exactly a well-formed range-local key.
pub(crate) fn parse_range_local_key(key: &[u8]) -> Option<(RangeId, RangeLocalKey)> {
    let (id, rest) = key.strip_prefix(RANGE_PREFIX)?.split_first_chunk::<8>()?;
    let id = u64::from_be_bytes(*id);
    if id == 0 {
        return None;
    }
    let sub = rest.strip_prefix(b"/")?;
    let keyed = |name: &[u8], make: fn(u64) -> RangeLocalKey| {
        let arg: [u8; 8] = sub.strip_prefix(name)?.try_into().ok()?;
        Some(make(u64::from_be_bytes(arg)))
    };
    let sub = match sub {
        b"applied" => RangeLocalKey::Applied,
        b"last_commit_ts" => RangeLocalKey::LastCommitTs,
        b"descriptor" => RangeLocalKey::Descriptor,
        b"features" => RangeLocalKey::Features,
        b"oracle_hw" => RangeLocalKey::OracleHw,
        b"gc_watermark" => RangeLocalKey::GcWatermark,
        b"install" => RangeLocalKey::Install,
        _ => keyed(b"prepared/", RangeLocalKey::Prepared)
            .or_else(|| keyed(b"home/", RangeLocalKey::Home))
            .or_else(|| keyed(b"marker/", RangeLocalKey::Marker))?,
    };
    Some((RangeId(id), sub))
}

/// Fail-closed value check for one range-local key; `node` is the node's features.
pub(crate) fn check_range_value(
    id: RangeId,
    sub: &RangeLocalKey,
    value: &[u8],
    node: u64,
) -> Result<(), FormatError> {
    let eight = || -> Result<u64, FormatError> {
        Ok(u64::from_be_bytes(
            value
                .try_into()
                .map_err(|_| corrupt("wrong range metadata length"))?,
        ))
    };
    match sub {
        RangeLocalKey::Applied if value.len() != 16 => Err(corrupt("wrong range applied length")),
        RangeLocalKey::LastCommitTs | RangeLocalKey::GcWatermark | RangeLocalKey::Marker(_) => {
            eight().map(drop)
        }
        RangeLocalKey::OracleHw => {
            eight()?;
            if id != SYSTEM_RANGE {
                return Err(corrupt("oracle_hw outside the system range"));
            }
            Ok(())
        }
        RangeLocalKey::Features => {
            let bits = eight()?;
            check_features(bits)?;
            if bits & !node != 0 {
                return Err(corrupt("range features exceed node features"));
            }
            Ok(())
        }
        RangeLocalKey::Descriptor => {
            let descriptor =
                RangeDescriptor::decode(value).map_err(|e| corrupt(format!("range {e}")))?;
            if descriptor.id != id {
                return Err(corrupt("range descriptor id mismatch"));
            }
            Ok(())
        }
        RangeLocalKey::Applied
        | RangeLocalKey::Install
        | RangeLocalKey::Prepared(_)
        | RangeLocalKey::Home(_) => Ok(()),
    }
}
