//! Track A owns this module.
//! Many ranges over one shared RocksDB (GD-0075 R5-03).
//!
//! Each range applies its committed batches independently, with R4's apply contract
//! (replicated.rs `ReplicaStore::apply_entry`): positions apply in log order, an index at or
//! below the applied one is skipped, a gap is an error. The applied position lives under the
//! range-local key `Meta/range/<id>/applied`, written in the same RocksDB batch as the data.
//!
//! Every op of a batch must lie inside the range's applied descriptor (keys.rs) or be one of the
//! range's own local Meta keys with a valid value. Otherwise the whole batch is refused: it
//! changes nothing but still advances the applied position, so every replica, which holds the
//! same descriptor at that index, refuses it identically.
use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, Mutex},
};

use super::keys::{contains_raw, interval_in_range};
use super::local::{applied_key, decode_applied, encode_applied, local_interval};
use crate::CoreError;
use crate::storage::engine::{
    kv::{Durability, Keyspace, KvBackend, KvRead, Op, WriteBatch},
    kv_rocksdb::{RocksKv, WriterLock},
    meta::{self, RangeLocalKey, check_range_value, parse_range_local_key, range_local_key},
};
use crate::storage::range_api::{RangeDescriptor, RangeId};
use crate::storage::replicated::AppliedPosition;
use crate::storage::{io, v2};

/// Whether a committed entry changed this range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeApplyOutcome {
    Applied,
    /// At or below the applied position; nothing changed.
    Skipped,
    /// A batch op was outside the range: no data changed, the position advanced.
    Refused,
}

/// A replicated v2 directory holding many ranges (features `REPLICATED | RANGES`).
pub struct RangeStore {
    kv: Arc<RocksKv>,
    domain: [u8; 16],
    /// One apply lock per range: applies (and installs) of one range are serialized.
    locks: Mutex<HashMap<RangeId, Arc<Mutex<()>>>>,
    /// Serializes read-modify-write of the node's `Meta/features` union.
    features: Mutex<()>,
    /// Serializes installs with each other and with descriptor changes, so the overlap fence
    /// of one install sees every other range's bounds.
    installs: Mutex<()>,
}

impl RangeStore {
    /// Create an empty range-mode directory.
    pub fn create(path: impl AsRef<Path>, domain: [u8; 16]) -> Result<Self, CoreError> {
        let path = path.as_ref();
        std::fs::create_dir_all(path).map_err(io)?;
        let lock = WriterLock::acquire(path)?;
        let mut concrete = None;
        meta::create_with_features(path, domain, meta::REPLICATED | meta::RANGES, |p| {
            let kv = Arc::new(RocksKv::open_locked(p, 64 << 20, lock)?);
            concrete = Some(kv.clone());
            Ok(kv)
        })
        .map_err(v2::format_error)?;
        Ok(Self::new(concrete.expect("create opened backend"), domain))
    }

    /// Open a range-mode directory and finish or roll back any interrupted install (R5-05).
    pub fn open(path: impl AsRef<Path>) -> Result<Self, CoreError> {
        let path = path.as_ref();
        if meta::classify_dir(path).map_err(v2::format_error)? != meta::DirClass::V2 {
            return Err(CoreError::Corrupt("not a v2 directory".into()));
        }
        let kv = Arc::new(RocksKv::open(path, 64 << 20)?);
        let info = meta::validate_meta(kv.as_ref()).map_err(v2::format_error)?;
        if info.features & meta::RANGES == 0 {
            return Err(CoreError::Unsupported);
        }
        let store = Self::new(kv, info.domain);
        store.recover_installs()?;
        Ok(store)
    }

    fn new(kv: Arc<RocksKv>, domain: [u8; 16]) -> Self {
        Self {
            kv,
            domain,
            locks: Mutex::new(HashMap::new()),
            features: Mutex::new(()),
            installs: Mutex::new(()),
        }
    }

    pub fn domain(&self) -> [u8; 16] {
        self.domain
    }

    pub(crate) fn kv(&self) -> &Arc<RocksKv> {
        &self.kv
    }

    /// The apply lock of one range (shared by apply, apply_noop, install).
    pub(super) fn range_lock(&self, range: RangeId) -> Result<Arc<Mutex<()>>, CoreError> {
        let mut locks = self
            .locks
            .lock()
            .map_err(|_| CoreError::Corrupt("range lock table poisoned".into()))?;
        Ok(locks.entry(range).or_default().clone())
    }

    /// Ranges with a stored descriptor, in id order.
    pub fn ranges(&self) -> Result<Vec<RangeId>, CoreError> {
        let mut out = Vec::new();
        for item in self.kv.range(
            Keyspace::Meta,
            std::ops::Bound::Included(b"range/".as_slice()),
            std::ops::Bound::Excluded(b"range0".as_slice()),
            crate::storage::engine::kv::Direction::Forward,
        )? {
            let (key, _) = item?;
            if let Some((id, RangeLocalKey::Descriptor)) = parse_range_local_key(&key) {
                out.push(id);
            }
        }
        Ok(out)
    }

    /// Store a new range's first descriptor. Repeating it with the same descriptor is a no-op;
    /// a different one for an existing range is `Exists`.
    pub fn create_range(&self, descriptor: &RangeDescriptor) -> Result<(), CoreError> {
        let bytes = descriptor
            .encode()
            .map_err(|e| CoreError::Corrupt(format!("range descriptor: {e}")))?;
        let lock = self.range_lock(descriptor.id)?;
        let _guard = lock
            .lock()
            .map_err(|_| CoreError::Corrupt("range apply lock poisoned".into()))?;
        let key = range_local_key(descriptor.id, &RangeLocalKey::Descriptor);
        match self.kv.get(Keyspace::Meta, &key)? {
            Some(existing) if existing == bytes => Ok(()),
            Some(_) => Err(CoreError::Exists),
            None => {
                let mut batch = WriteBatch::default();
                batch.put(Keyspace::Meta, key, bytes);
                Ok(self.kv.write(batch, Durability::Sync)?)
            }
        }
    }

    /// The range's applied descriptor.
    pub fn descriptor(&self, range: RangeId) -> Result<Option<RangeDescriptor>, CoreError> {
        read_descriptor(self.kv.as_ref(), range)
    }

    /// The range's last applied log position.
    pub fn applied(&self, range: RangeId) -> Result<Option<AppliedPosition>, CoreError> {
        read_applied(self.kv.as_ref(), range)
    }

    /// Apply a committed encoded batch of one range and its position atomically, in log order.
    pub fn apply(
        &self,
        range: RangeId,
        encoded_batch: &[u8],
        at: AppliedPosition,
    ) -> Result<RangeApplyOutcome, CoreError> {
        self.apply_entry(range, Some(encoded_batch), at)
    }

    /// Advance the range's applied position for a committed entry without data changes.
    pub fn apply_noop(
        &self,
        range: RangeId,
        at: AppliedPosition,
    ) -> Result<RangeApplyOutcome, CoreError> {
        self.apply_entry(range, None, at)
    }

    fn apply_entry(
        &self,
        range: RangeId,
        encoded: Option<&[u8]>,
        at: AppliedPosition,
    ) -> Result<RangeApplyOutcome, CoreError> {
        let lock = self.range_lock(range)?;
        let _guard = lock
            .lock()
            .map_err(|_| CoreError::Corrupt("range apply lock poisoned".into()))?;
        if self
            .kv
            .get(
                Keyspace::Meta,
                &range_local_key(range, &RangeLocalKey::Install),
            )?
            .is_some()
        {
            return Err(CoreError::Corrupt(format!(
                "range {} has an unfinished install",
                range.0
            )));
        }
        let descriptor = self
            .descriptor(range)?
            .ok_or_else(|| CoreError::Corrupt(format!("unknown range {}", range.0)))?;
        let cur = self.applied(range)?;
        if cur.is_some_and(|c| at.index <= c.index) {
            return Ok(RangeApplyOutcome::Skipped);
        }
        let expected = cur.map_or(1, |c| c.index + 1);
        if at.index != expected {
            return Err(CoreError::Corrupt(format!(
                "range {} replication gap: applied {}, got {}",
                range.0,
                cur.map_or(0, |c| c.index),
                at.index
            )));
        }
        let batch = match encoded {
            Some(bytes) => {
                WriteBatch::decode(bytes).map_err(|e| CoreError::Corrupt(e.to_string()))?
            }
            None => WriteBatch::default(),
        };
        let (mut batch, outcome) = match check_batch(&descriptor, &batch) {
            Ok(check) => (batch, Some(check)),
            Err(BatchError::Outside(_)) => (WriteBatch::default(), None),
            // A value this build cannot validate (for example a feature bit from a newer
            // build) stops the range instead of being refused: refusing would drop data that
            // a newer replica applies (R4 fails the same way, on the next open).
            Err(BatchError::Invalid(reason)) => {
                return Err(CoreError::Corrupt(format!(
                    "range {} batch at {}: {reason}",
                    range.0, at.index
                )));
            }
        };
        let _installs = match outcome {
            Some(BatchCheck { descriptor: true, .. }) => Some(self.installs_lock()?),
            _ => None,
        };
        batch.put(
            Keyspace::Meta,
            applied_key(range),
            encode_applied(at).to_vec(),
        );
        match outcome {
            None => {
                self.kv.write(batch, Durability::NoSync)?;
                Ok(RangeApplyOutcome::Refused)
            }
            Some(BatchCheck { features: 0, .. }) => {
                self.kv.write(batch, Durability::NoSync)?;
                Ok(RangeApplyOutcome::Applied)
            }
            Some(BatchCheck { features: bits, .. }) => {
                let _features = self
                    .features
                    .lock()
                    .map_err(|_| CoreError::Corrupt("features lock poisoned".into()))?;
                union_node_features(self.kv.as_ref(), &mut batch, bits)?;
                self.kv.write(batch, Durability::NoSync)?;
                Ok(RangeApplyOutcome::Applied)
            }
        }
    }

    /// Flush all applied batches to durable storage.
    pub fn flush(&self) -> Result<(), CoreError> {
        Ok(self.kv.flush_barrier()?)
    }

    /// Serializes installs and descriptor changes across ranges.
    pub(super) fn installs_lock(&self) -> Result<std::sync::MutexGuard<'_, ()>, CoreError> {
        self.installs
            .lock()
            .map_err(|_| CoreError::Corrupt("install lock poisoned".into()))
    }

    /// Serializes a `Meta/features` read-modify-write against concurrent applies.
    pub(super) fn features_lock(&self) -> Result<std::sync::MutexGuard<'_, ()>, CoreError> {
        self.features
            .lock()
            .map_err(|_| CoreError::Corrupt("features lock poisoned".into()))
    }
}

pub(super) fn read_descriptor(
    kv: &dyn KvRead,
    range: RangeId,
) -> Result<Option<RangeDescriptor>, CoreError> {
    kv.get(
        Keyspace::Meta,
        &range_local_key(range, &RangeLocalKey::Descriptor),
    )?
    .map(|bytes| {
        RangeDescriptor::decode(&bytes)
            .map_err(|e| CoreError::Corrupt(format!("range {} descriptor: {e}", range.0)))
    })
    .transpose()
}

pub(super) fn read_applied(
    kv: &dyn KvRead,
    range: RangeId,
) -> Result<Option<AppliedPosition>, CoreError> {
    kv.get(Keyspace::Meta, &applied_key(range))?
        .map(|bytes| {
            decode_applied(&bytes)
                .ok_or_else(|| CoreError::Corrupt(format!("range {} applied length", range.0)))
        })
        .transpose()
}

/// Add `bits` to the node's `Meta/features` in `batch` when they are not all set yet.
/// The caller holds the store's features lock.
pub(super) fn union_node_features(
    kv: &dyn KvRead,
    batch: &mut WriteBatch,
    bits: u64,
) -> Result<(), CoreError> {
    let node = kv
        .get(Keyspace::Meta, b"features")?
        .and_then(|v| <[u8; 8]>::try_from(v.as_slice()).ok())
        .map(u64::from_be_bytes)
        .ok_or_else(|| CoreError::Corrupt("node features missing".into()))?;
    if bits & !node != 0 {
        meta::put_features(batch, node | bits).map_err(v2::format_error)?;
    }
    Ok(())
}

/// Checks every op of `batch` against the range: `Err(Outside)` names the first op outside it,
/// `Err(Invalid)` a range-local value this build rejects.
///
/// Allowed: non-Meta keys and delete-ranges inside the descriptor (keys.rs, exact for keys);
/// Meta puts and deletes of this range's local keys other than `applied` and `install` (the
/// store writes those), with values `validate_meta` accepts, and no delete of `descriptor`; a
/// Meta delete-range inside the range's local interval that covers none of those three.
/// Why `check_batch` rejected a batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BatchError {
    /// An op outside the range (a pure function of the descriptor and the batch bytes, the
    /// same in every build): the batch is refused.
    Outside(String),
    /// A range-local value this build does not accept: applying stops with an error.
    Invalid(String),
}
impl std::fmt::Display for BatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Outside(reason) | Self::Invalid(reason) => f.write_str(reason),
        }
    }
}

/// What an accepted batch does to range-wide state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BatchCheck {
    /// Feature bits written into the range's `features` key (0 if none).
    pub(crate) features: u64,
    /// Whether the batch replaces the range's descriptor.
    pub(crate) descriptor: bool,
}

pub(crate) fn check_batch(
    descriptor: &RangeDescriptor,
    batch: &WriteBatch,
) -> Result<BatchCheck, BatchError> {
    let outside = BatchError::Outside;
    let id = descriptor.id;
    // `applied` and `install` belong to the store; the descriptor may be replaced (split) but
    // never removed, or every later apply of the range would fail on every replica.
    let own_key = |key: &[u8], delete: bool| -> Result<RangeLocalKey, BatchError> {
        match parse_range_local_key(key) {
            Some((owner, sub)) if owner == id => match sub {
                RangeLocalKey::Applied | RangeLocalKey::Install => {
                    Err(outside(format!("batch writes range {} {sub:?}", id.0)))
                }
                RangeLocalKey::Descriptor if delete => {
                    Err(outside(format!("batch deletes range {} descriptor", id.0)))
                }
                sub => Ok(sub),
            },
            _ => Err(outside(format!("meta key outside range {}", id.0))),
        }
    };
    let mut features = 0;
    let mut replaces_descriptor = false;
    for op in batch.ops() {
        match op {
            Op::Put {
                ks: Keyspace::Meta,
                key,
                value,
            } => {
                let sub = own_key(key, false)?;
                check_range_value(id, &sub, value, u64::MAX)
                    .map_err(|e| BatchError::Invalid(e.to_string()))?;
                replaces_descriptor |= sub == RangeLocalKey::Descriptor;
                if sub == RangeLocalKey::Features {
                    features |= u64::from_be_bytes(value.as_slice().try_into().expect("checked"));
                }
            }
            Op::Delete {
                ks: Keyspace::Meta,
                key,
            } => {
                own_key(key, true)?;
            }
            Op::DeleteRange {
                ks: Keyspace::Meta,
                start,
                end,
            } => {
                if start >= end {
                    continue;
                }
                let (lo, hi) = local_interval(id);
                if start < &lo || end > &hi {
                    return Err(outside(format!("meta delete-range outside range {}", id.0)));
                }
                for protected in [
                    RangeLocalKey::Applied,
                    RangeLocalKey::Install,
                    RangeLocalKey::Descriptor,
                ] {
                    let key = range_local_key(id, &protected);
                    if start <= &key && &key < end {
                        return Err(outside(format!(
                            "meta delete-range covers range {} {protected:?}",
                            id.0
                        )));
                    }
                }
            }
            Op::Put { ks, key, .. } | Op::Delete { ks, key } => {
                if !contains_raw(descriptor, *ks, key).map_err(|e| outside(format!("{e:?}")))? {
                    return Err(outside(format!("{ks:?} key outside range {}", id.0)));
                }
            }
            Op::DeleteRange { ks, start, end } => {
                if !interval_in_range(descriptor, *ks, start, end)
                    .map_err(|e| outside(format!("{e:?}")))?
                {
                    return Err(outside(format!("{ks:?} delete-range outside range {}", id.0)));
                }
            }
        }
    }
    Ok(BatchCheck {
        features,
        descriptor: replaces_descriptor,
    })
}

#[cfg(test)]
#[path = "store_tests.rs"]
pub(crate) mod tests;
