//! Prepared records (GD-0075 R5-36; design.md sections 5 and 6): the value of
//! `Meta/range/<id>/prepared/<txn BE8>`, written by the participant's prepare and deleted by
//! its finalize, in the same batches as their effects.
//!
//! Version 1, strict: `[ver=1][home range BE8][prepared ts BE8]
//! [n writes BE4][(keyspace tag u8, key: len BE4 + bytes, value flag u8 (0 delete, 1 put),
//! value: len BE4 + bytes if put)]*n [n locks BE4][(lock tag u8 (1 row, 2 unique), id BE8,
//! key bytes: len BE4 + bytes, mode u8 (0 shared, 1 exclusive))]*n`.
//! Writes are user keys of the range's spaces (keyspace tag = RangeSpace tag); locks are the
//! transaction's row and unique locks in this range at prepare.
//!
//! Version 2 (R5-82) = the v1 layout with version byte 2, then `[n children BE4][range BE8]*n`,
//! n >= 1, distinct, none 0, 1 or u64::MAX: the ranges that took a share of the record at a
//! split of this range, finalized before this range's own share. A record without children
//! is always written as v1.
use std::collections::BTreeMap;

use super::super::keys::{is_versioned, keyspace_of, space_of};
use crate::storage::engine::{
    hlc::Ts,
    kv::Keyspace,
    locks::{LockKey, LockMode},
};
use crate::storage::range_api::{RangeError, RangeId, RangeSpace, SYSTEM_RANGE, range_key};

const V1: u8 = 1;
const V2: u8 = 2;

pub(crate) type Writes = BTreeMap<(Keyspace, Vec<u8>), Option<Vec<u8>>>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Prepared {
    pub(crate) home: RangeId,
    pub(crate) at: Ts,
    pub(crate) writes: Writes,
    pub(crate) locks: Vec<(LockKey, LockMode)>,
    /// Ranges holding a share of this transaction since a split of this range (v2).
    pub(crate) children: Vec<RangeId>,
}

/// Range key a lock's requests are routed by (engine/locking.rs `lock_key`). None for a
/// table sentinel (`Row` with an empty pk, design.md 3.1): it lives on every range holding
/// the table's rows, not at one key.
fn lock_position(key: &LockKey) -> Option<Vec<u8>> {
    let at = |space, id: u64, rest: &[u8]| {
        let mut user = id.to_be_bytes().to_vec();
        user.extend_from_slice(rest);
        Some(range_key(space, &user))
    };
    match key {
        LockKey::Row { table, pk } if !pk.is_empty() => at(RangeSpace::Rows, *table, pk),
        LockKey::Unique { index, tuple } => at(RangeSpace::Index, *index, tuple),
        LockKey::Row { .. } | LockKey::Table(_) => None,
    }
}

fn corrupt(message: &str) -> RangeError {
    RangeError::Corrupt(format!("prepared record: {message}"))
}

fn put_len(out: &mut Vec<u8>, len: usize) -> Result<(), RangeError> {
    let len = u32::try_from(len).map_err(|_| corrupt("too large"))?;
    out.extend_from_slice(&len.to_be_bytes());
    Ok(())
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) -> Result<(), RangeError> {
    put_len(out, bytes.len())?;
    out.extend_from_slice(bytes);
    Ok(())
}

struct Reader<'a>(&'a [u8]);
impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], RangeError> {
        if self.0.len() < n {
            return Err(corrupt("truncated"));
        }
        let (head, rest) = self.0.split_at(n);
        self.0 = rest;
        Ok(head)
    }
    fn u8(&mut self) -> Result<u8, RangeError> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> Result<u32, RangeError> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64, RangeError> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn bytes(&mut self) -> Result<Vec<u8>, RangeError> {
        let n = self.u32()? as usize;
        Ok(self.take(n)?.to_vec())
    }
}

impl Prepared {
    /// R5-82: moves the writes and locks at or above `at_key` into a record for `right` (same
    /// home and p, no children) and names `right` as a child of this one. None, and this
    /// record unchanged, when nothing moves. Sentinels stay here (`lock_position`).
    pub(crate) fn split_off(&mut self, at_key: &[u8], right: RangeId) -> Option<Prepared> {
        let moves =
            |position: Option<Vec<u8>>| position.is_some_and(|key| key.as_slice() >= at_key);
        let (moved, kept): (Writes, Writes) = std::mem::take(&mut self.writes)
            .into_iter()
            .partition(|((ks, key), _)| moves(space_of(*ks).map(|space| range_key(space, key))));
        let (moved_locks, kept_locks): (Vec<_>, Vec<_>) = std::mem::take(&mut self.locks)
            .into_iter()
            .partition(|(key, _)| moves(lock_position(key)));
        self.writes = kept;
        self.locks = kept_locks;
        if moved.is_empty() && moved_locks.is_empty() {
            return None;
        }
        self.children.push(right);
        Some(Prepared {
            home: self.home,
            at: self.at,
            writes: moved,
            locks: moved_locks,
            children: Vec::new(),
        })
    }

    pub(crate) fn encode(&self) -> Result<Vec<u8>, RangeError> {
        let version = if self.children.is_empty() { V1 } else { V2 };
        let mut out = vec![version];
        out.extend_from_slice(&self.home.0.to_be_bytes());
        out.extend_from_slice(&self.at.0.to_be_bytes());
        put_len(&mut out, self.writes.len())?;
        for ((ks, key), value) in &self.writes {
            let space = space_of(*ks)
                .filter(|space| is_versioned(*space))
                .ok_or_else(|| corrupt("write outside a versioned space"))?;
            out.push(space.tag());
            put_bytes(&mut out, key)?;
            match value {
                None => out.push(0),
                Some(value) => {
                    out.push(1);
                    put_bytes(&mut out, value)?;
                }
            }
        }
        // Locks in byte order, so one record has one encoding.
        let mut locks = Vec::with_capacity(self.locks.len());
        for (key, mode) in &self.locks {
            let (tag, id, bytes) = match key {
                LockKey::Row { table, pk } => (1u8, *table, pk),
                LockKey::Unique { index, tuple } => (2u8, *index, tuple),
                LockKey::Table(_) => return Err(corrupt("table lock")),
            };
            let mut lock = vec![tag];
            lock.extend_from_slice(&id.to_be_bytes());
            put_bytes(&mut lock, bytes)?;
            lock.push(match mode {
                LockMode::Shared => 0,
                LockMode::Exclusive => 1,
                _ => return Err(corrupt("table lock mode")),
            });
            locks.push(lock);
        }
        locks.sort_unstable();
        locks.dedup();
        put_len(&mut out, locks.len())?;
        for lock in locks {
            out.extend_from_slice(&lock);
        }
        if version == V2 {
            put_len(&mut out, self.children.len())?;
            for child in &self.children {
                out.extend_from_slice(&child.0.to_be_bytes());
            }
        }
        Ok(out)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, RangeError> {
        let mut r = Reader(bytes);
        let version = r.u8()?;
        if version != V1 && version != V2 {
            return Err(corrupt("unknown version"));
        }
        let home = RangeId(r.u64()?);
        let at = Ts(r.u64()?);
        if home.0 == 0 || at.0 == 0 {
            return Err(corrupt("zero home or timestamp"));
        }
        let mut writes = BTreeMap::new();
        for _ in 0..r.u32()? {
            let space = RangeSpace::from_tag(r.u8()?)
                .filter(|space| is_versioned(*space))
                .ok_or_else(|| corrupt("unknown or unversioned space"))?;
            let key = r.bytes()?;
            let value = match r.u8()? {
                0 => None,
                1 => Some(r.bytes()?),
                _ => return Err(corrupt("bad value flag")),
            };
            if writes.insert((keyspace_of(space), key), value).is_some() {
                return Err(corrupt("duplicate write"));
            }
        }
        let mut locks: Vec<(LockKey, LockMode)> = Vec::new();
        for _ in 0..r.u32()? {
            let tag = r.u8()?;
            let id = r.u64()?;
            let key = r.bytes()?;
            let key = match tag {
                1 => LockKey::Row { table: id, pk: key },
                2 => LockKey::Unique {
                    index: id,
                    tuple: key,
                },
                _ => return Err(corrupt("bad lock tag")),
            };
            let mode = match r.u8()? {
                0 => LockMode::Shared,
                1 => LockMode::Exclusive,
                _ => return Err(corrupt("bad lock mode")),
            };
            if locks.iter().any(|(held, _)| *held == key) {
                return Err(corrupt("duplicate lock"));
            }
            locks.push((key, mode));
        }
        let mut children = Vec::new();
        if version == V2 {
            let n = r.u32()?;
            if n == 0 {
                return Err(corrupt("v2 without children"));
            }
            for _ in 0..n {
                let child = RangeId(r.u64()?);
                if [0, SYSTEM_RANGE.0, u64::MAX].contains(&child.0) || children.contains(&child) {
                    return Err(corrupt("bad or duplicate child range"));
                }
                children.push(child);
            }
        }
        if !r.0.is_empty() {
            return Err(corrupt("trailing bytes"));
        }
        Ok(Self {
            home,
            at,
            writes,
            locks,
            children,
        })
    }
}
