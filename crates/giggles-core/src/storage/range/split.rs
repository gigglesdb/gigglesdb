//! Track A owns this module.
//! Range split apply (GD-0075 R5-80; r5/p9/split-design.md "Shape").
//!
//! A split is one entry of the parent's log. Every replica applies it with `RangeStore::split`
//! in ONE RocksDB batch that only rewrites range-local Meta keys (data keys never move):
//! parent descriptor -> [start, at) epoch+1, parent `applied` -> the entry, parent
//! `child/<right>` = the right descriptor (lineage); right descriptor -> [at, end) epoch+1 with
//! the same replicas, right `applied` = the entry, and the parent's `features`,
//! `last_commit_ts`, `gc_watermark` and `marker/*` copied. Home records stay on the parent.
//!
//! Prepared records are divided (R5-82): writes and row/unique locks at or above `at_key` go
//! to a record of the right range (same home and p), and the parent's record keeps its own
//! share and names the right range as a child (prepared record v2); the parent's finalize
//! reaches the children before its own share (engine/txns.rs), so the home record is never
//! forgotten while a child still holds a share. Table sentinels stay with the parent.
//!
//! A split is refused (nothing changes but the parent's position) on a decision made only
//! from the parent's descriptor and the entry: stale epoch; `at_key` not strictly inside the
//! parent, not a range key, or a Rows/Index key with a partial 8-byte id (a bare space tag such
//! as `first_user_key()` is allowed); a split of the system range below `first_user_key()`;
//! a half whose descriptor fails `validate()` (a bound over 64 KiB); a right id of 0, 1,
//! u64::MAX or the parent's; a parent with a Learner replica (the right group opens with the
//! voters only, so a learner would host it outside its configuration). A right id already
//! stored on this node is a fatal apply error instead: that is local state the parent's log
//! does not replicate, so refusing on it could diverge the replicas.
use std::ops::Bound;

use super::{RangeStore, read_descriptor};
use crate::CoreError;
use crate::storage::engine::{
    codec,
    hlc::Ts,
    kv::{Direction, Durability, Keyspace, KvBackend, KvRead, WriteBatch},
    meta::{RangeLocalKey, parse_range_local_key, range_local_key, range_local_prefix},
};
use crate::storage::range::engine::prepared::Prepared;
use crate::storage::range::keys::keyspace_of;
use crate::storage::range::local::{applied_key, encode_applied, local_interval};
use crate::storage::range_api::{
    Epoch, RangeDescriptor, RangeError, RangeId, RangeSpace, ReplicaRole, SYSTEM_RANGE,
    first_user_key, split_range_key,
};
use crate::storage::replicated::AppliedPosition;

/// What a split entry did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SplitOutcome {
    /// The parent is now `left`; `right` is the new range.
    Applied {
        left: RangeDescriptor,
        right: RangeDescriptor,
    },
    /// At or below the parent's applied position; nothing changed.
    Skipped,
    /// Refused deterministically: nothing changed, the parent's position advanced.
    Refused(String),
}

/// Meta interval of a range's `child/*` lineage keys.
pub(super) fn child_interval(id: RangeId) -> (Vec<u8>, Vec<u8>) {
    let mut lo = range_local_prefix(id);
    lo.extend_from_slice(b"child/");
    let mut hi = lo.clone();
    *hi.last_mut().expect("non-empty") = b'0';
    (lo, hi)
}

/// Meta (key, value) pairs.
type Pairs = Vec<(Vec<u8>, Vec<u8>)>;

fn local_keys(kv: &dyn KvRead, lo: &[u8], hi: &[u8]) -> Result<Pairs, CoreError> {
    Ok(kv
        .range(
            Keyspace::Meta,
            Bound::Included(lo),
            Bound::Excluded(hi),
            Direction::Forward,
        )?
        .collect::<Result<_, _>>()?)
}

fn read_children(kv: &dyn KvRead, range: RangeId) -> Result<Vec<RangeDescriptor>, CoreError> {
    let (lo, hi) = child_interval(range);
    local_keys(kv, &lo, &hi)?
        .into_iter()
        .map(|(_, value)| {
            RangeDescriptor::decode(&value)
                .map_err(|e| CoreError::Corrupt(format!("range {} child: {e}", range.0)))
        })
        .collect()
}

/// Lineage steps `holder` follows (each one narrows the descriptor).
const MAX_LINEAGE_HOPS: usize = 16;

fn stored_descriptor(kv: &dyn KvRead, range: RangeId) -> Result<RangeDescriptor, CoreError> {
    read_descriptor(kv, range)?
        .ok_or_else(|| CoreError::Corrupt(format!("unknown range {}", range.0)))
}

/// The range holding `key` on this store, from `descriptor` (a stored one): while it does not
/// contain `key`, the recorded child that does, as stored now (it may have split again).
fn holder(
    kv: &dyn KvRead,
    mut descriptor: RangeDescriptor,
    key: Option<&[u8]>,
) -> Result<RangeDescriptor, CoreError> {
    let Some(key) = key else {
        return Ok(descriptor);
    };
    for _ in 0..MAX_LINEAGE_HOPS {
        if descriptor.contains(key) {
            break;
        }
        let children = read_children(kv, descriptor.id)?;
        let Some(child) = children.into_iter().find(|c| c.contains(key)) else {
            break;
        };
        descriptor = read_descriptor(kv, child.id)?.unwrap_or(child);
    }
    Ok(descriptor)
}

/// R5-82 (review B1): where to route a call to `child`, a range that took a share of a
/// prepared record at a split of `parent`: its descriptor stored on this node if any (it may
/// have split again), else `parent`'s lineage record of it.
pub(crate) fn child_route(
    kv: &dyn KvRead,
    parent: RangeId,
    child: RangeId,
) -> Result<Option<RangeDescriptor>, CoreError> {
    if let Some(stored) = read_descriptor(kv, child)? {
        return Ok(Some(stored));
    }
    kv.get(
        Keyspace::Meta,
        &range_local_key(parent, &RangeLocalKey::Child(child.0)),
    )?
    .map(|bytes| {
        RangeDescriptor::decode(&bytes)
            .map_err(|e| CoreError::Corrupt(format!("range {} child: {e}", parent.0)))
    })
    .transpose()
}

/// R5-80: None while the stored descriptor of the engine's range has the engine's epoch; else
/// the descriptor of the range now holding `key` (lineage children followed transitively), or
/// the stored one.
pub(crate) fn stale_descriptor(
    kv: &dyn KvRead,
    engine: &RangeDescriptor,
    key: Option<&[u8]>,
) -> Result<Option<RangeDescriptor>, CoreError> {
    let stored = stored_descriptor(kv, engine.id)?;
    if stored.epoch == engine.epoch {
        return Ok(None);
    }
    holder(kv, stored, key).map(Some)
}

/// R5-81: the refusal of a request at `epoch`, not the engine's. Newer than the engine's
/// while the stored descriptor already has it: the engine is about to be reopened over that
/// descriptor, so the caller retries (`Unavailable`). Newer than the stored one as well: this
/// node has not applied that far yet, so the caller retries too rather than being handed an
/// older descriptor (review N4). Otherwise `StaleEpoch` with the descriptor of the range now
/// holding `key` (the stored one when nothing moved).
pub(crate) fn epoch_refusal(
    kv: &dyn KvRead,
    engine: &RangeDescriptor,
    epoch: Epoch,
    key: Option<&[u8]>,
) -> Result<RangeError, CoreError> {
    let stored = stored_descriptor(kv, engine.id)?;
    if epoch > engine.epoch && epoch == stored.epoch {
        return Ok(RangeError::Unavailable(
            "range engine reopening after a descriptor change".into(),
        ));
    }
    if epoch > engine.epoch && epoch > stored.epoch {
        return Ok(RangeError::Unavailable(format!(
            "range {} has not applied epoch {} here yet",
            engine.id.0, epoch.0
        )));
    }
    Ok(RangeError::StaleEpoch {
        descriptor: holder(kv, stored, key)?,
    })
}

/// The parent's and the right range's descriptors after a split (epoch + 1, same replicas).
fn halves(
    parent: &RangeDescriptor,
    at_key: &[u8],
    right: RangeId,
) -> Result<(RangeDescriptor, RangeDescriptor), CoreError> {
    let next = parent
        .epoch
        .0
        .checked_add(1)
        .ok_or_else(|| CoreError::Corrupt(format!("range {} epoch", parent.id.0)))?;
    let left = RangeDescriptor {
        epoch: Epoch(next),
        end: Some(at_key.to_vec()),
        ..parent.clone()
    };
    let child = RangeDescriptor {
        id: right,
        epoch: Epoch(next),
        start: at_key.to_vec(),
        ..parent.clone()
    };
    Ok((left, child))
}

/// Why `at_key` cannot split `parent`, from the descriptor and the key alone.
fn key_refusal(parent: &RangeDescriptor, at_key: &[u8]) -> Result<Option<String>, CoreError> {
    if at_key <= parent.start.as_slice() || !parent.contains(at_key) {
        return Ok(Some("split key not strictly inside the range".into()));
    }
    match split_range_key(at_key) {
        None => return Ok(Some("split key is not a range key".into())),
        // A bare tag (a space boundary such as first_user_key()) is a whole-space bound.
        Some((RangeSpace::Rows | RangeSpace::Index, user)) if (1..8).contains(&user.len()) => {
            return Ok(Some("split key has no table or index id".into()));
        }
        Some(_) => {}
    }
    if parent.id == SYSTEM_RANGE && at_key < first_user_key().as_slice() {
        return Ok(Some("split key inside the system key space".into()));
    }
    // Review S1: a half that cannot be encoded would fail the apply on every replica.
    let (left, right) = halves(parent, at_key, parent.id)?;
    for half in [&left, &right] {
        if let Err(e) = half.validate() {
            return Ok(Some(format!("split descriptor invalid: {e}")));
        }
    }
    Ok(None)
}

/// Why `split` refuses, from the parent's descriptor and the entry only: never from local
/// state another log writes, so every replica decides the same.
fn refusal(
    parent: &RangeDescriptor,
    epoch: Epoch,
    at_key: &[u8],
    right: RangeId,
) -> Result<Option<String>, CoreError> {
    if epoch != parent.epoch {
        return Ok(Some(format!(
            "stale epoch {} (range {} is at {})",
            epoch.0, parent.id.0, parent.epoch.0
        )));
    }
    if let Some(why) = key_refusal(parent, at_key)? {
        return Ok(Some(why));
    }
    if [0, SYSTEM_RANGE.0, u64::MAX, parent.id.0].contains(&right.0) {
        return Ok(Some(format!("invalid right range id {}", right.0)));
    }
    // Review S5: a reconfiguration is in progress.
    if parent
        .replicas
        .iter()
        .any(|r| r.role == ReplicaRole::Learner)
    {
        return Ok(Some(format!("range {} has a learner replica", parent.id.0)));
    }
    Ok(None)
}

impl RangeStore {
    /// Apply a committed split entry of `parent` at `at` (module doc). Same apply contract as
    /// `apply`: an unfinished install or a gap is an error, at or below applied is Skipped.
    pub fn split(
        &self,
        parent: RangeId,
        epoch: Epoch,
        at_key: &[u8],
        right: RangeId,
        at: AppliedPosition,
    ) -> Result<SplitOutcome, CoreError> {
        let lock = self.range_lock(parent)?;
        let _guard = lock
            .lock()
            .map_err(|_| CoreError::Corrupt("range apply lock poisoned".into()))?;
        let Some(descriptor) = self.next_entry(parent, at)? else {
            return Ok(SplitOutcome::Skipped);
        };
        // A descriptor change: serialized with installs (their overlap fence).
        let _installs = self.installs_lock()?;
        let kv: &dyn KvRead = self.kv.as_ref();
        let (lo, hi) = local_interval(parent);
        let local = local_keys(kv, &lo, &hi)?;
        let mut batch = WriteBatch::default();
        batch.put(
            Keyspace::Meta,
            applied_key(parent),
            encode_applied(at).to_vec(),
        );
        if let Some(reason) = refusal(&descriptor, epoch, at_key, right)? {
            self.kv.write(batch, Durability::NoSync)?;
            return Ok(SplitOutcome::Refused(reason));
        }
        // Review S2: fatal (the replica halts visibly), never a refusal.
        let (lo, hi) = local_interval(right);
        if !local_keys(kv, &lo, &hi)?.is_empty() {
            return Err(CoreError::Corrupt(format!(
                "split of range {} at index {}: right range {} is already stored here",
                parent.0, at.index, right.0
            )));
        }
        let (left, child) = halves(&descriptor, at_key, right)?;
        let encode = |d: &RangeDescriptor| {
            d.encode()
                .map_err(|e| CoreError::Corrupt(format!("range {} descriptor: {e}", d.id.0)))
        };
        let child_bytes = encode(&child)?;
        let put = |batch: &mut WriteBatch, id: RangeId, sub: RangeLocalKey, value: Vec<u8>| {
            batch.put(Keyspace::Meta, range_local_key(id, &sub), value);
        };
        put(
            &mut batch,
            parent,
            RangeLocalKey::Descriptor,
            encode(&left)?,
        );
        put(
            &mut batch,
            parent,
            RangeLocalKey::Child(right.0),
            child_bytes.clone(),
        );
        put(&mut batch, right, RangeLocalKey::Descriptor, child_bytes);
        batch.put(
            Keyspace::Meta,
            applied_key(right),
            encode_applied(at).to_vec(),
        );
        for (key, value) in local {
            match parse_range_local_key(&key) {
                Some((
                    _,
                    sub @ (RangeLocalKey::Features
                    | RangeLocalKey::LastCommitTs
                    | RangeLocalKey::GcWatermark
                    | RangeLocalKey::Marker(_)),
                )) => put(&mut batch, right, sub, value),
                Some((_, sub @ RangeLocalKey::Prepared(_))) => {
                    let corrupt = |e| CoreError::Corrupt(format!("range {}: {e:?}", parent.0));
                    let mut record = Prepared::decode(&value).map_err(corrupt)?;
                    if let Some(share) = record.split_off(at_key, right) {
                        put(&mut batch, right, sub, share.encode().map_err(corrupt)?);
                        put(&mut batch, parent, sub, record.encode().map_err(corrupt)?);
                    }
                }
                _ => {}
            }
        }
        self.kv.write(batch, Durability::NoSync)?;
        Ok(SplitOutcome::Applied { left, right: child })
    }

    /// For a split PROPOSER (review S3): Ok when `at_key` may split `range` as stored here, so
    /// the bound is a whole user key (design.md Changes, Track A (g)): strictly inside the
    /// range, with a bounded descriptor, and in Rows or Index either a bare space tag, a table
    /// or index id (an 8-byte user part), or a user key stored in that keyspace (any version).
    /// A bound inside a user key would put that key's user form and its versions on different
    /// sides (`contains_raw` against `cf_interval`). The apply cannot check this: stored data
    /// is not a deterministic input of an entry (one shared store per node can hold keys the
    /// range's log never wrote there, such as orphans of a range that left the node, and GC
    /// runs per node), so replicas could decide differently. Only the proposer checks it.
    pub fn check_split_key(&self, range: RangeId, at_key: &[u8]) -> Result<(), CoreError> {
        let kv: &dyn KvRead = self.kv.as_ref();
        let descriptor = stored_descriptor(kv, range)?;
        let invalid =
            |why: &str| CoreError::Storage(format!("split key of range {}: {why}", range.0));
        if let Some(why) = key_refusal(&descriptor, at_key)? {
            return Err(invalid(&why));
        }
        let (space, user) = match split_range_key(at_key) {
            Some((space @ (RangeSpace::Rows | RangeSpace::Index), user)) => (space, user),
            _ => return Err(invalid("not a Rows or Index key")),
        };
        if user.is_empty() || user.len() == 8 {
            return Ok(());
        }
        // The newest version sorts first: `user || !ts`, lowest at ts = u64::MAX.
        let first = codec::user_key_seek_at(user, Ts(u64::MAX));
        let stored = kv
            .seek_ge(keyspace_of(space), &first)?
            .is_some_and(|(key, _)| key.len() == user.len() + 8 && key.starts_with(user));
        if !stored {
            return Err(invalid("not a stored user key, table or index boundary"));
        }
        Ok(())
    }

    /// Lineage: descriptors (as at split time) of every child recorded under `range`, id order.
    pub fn children(&self, range: RangeId) -> Result<Vec<RangeDescriptor>, CoreError> {
        read_children(self.kv.as_ref(), range)
    }
}

#[cfg(test)]
#[path = "split_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "change_dump_tests.rs"]
mod change_dump_tests;
