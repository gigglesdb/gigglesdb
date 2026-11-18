//! Prepared records (GD-0075 R5-36; design.md sections 5 and 6): the value of
//! `Meta/range/<id>/prepared/<txn BE8>`, written by the participant's prepare and deleted by
//! its finalize, in the same batches as their effects.
//!
//! Version 1, strict: `[ver=1][home range BE8][prepared ts BE8]
//! [n writes BE4][(keyspace tag u8, key: len BE4 + bytes, value flag u8 (0 delete, 1 put),
//! value: len BE4 + bytes if put)]*n [n locks BE4][(lock tag u8 (1 row, 2 unique), id BE8,
//! key bytes: len BE4 + bytes, mode u8 (0 shared, 1 exclusive))]*n`.
//! Writes are user keys of the range's spaces (keyspace tag = RangeSpace tag); locks are the
//! transaction's row and unique locks in this range at prepare, one per key at its strongest
//! mode.
//!
//! Version 2 (R5-82) = the v1 layout with version byte 2, then `[n children BE4][range BE8]*n`,
//! n >= 1, distinct, none 0, 1 or u64::MAX: the ranges that took a share of the record at a
//! split of this range, finalized before this range's own share. A record without children
//! is always written as v1.
//!
//! GD-0075 R5-73 adds lock tag 3, a SERIALIZABLE gap lock: `[3][space u8 (1 row, 2 unique)]
//! [start bound][end bound][mode u8 = 0 shared]`, a bound being `[0]` (unbounded), `[1]` +
//! len BE4 + bytes (included) or `[2]` + len BE4 + bytes (excluded), over `id BE8 || tail`.
//! Only a transaction holding gap locks writes it; a build without R5-73 refuses such a record,
//! so every node must run R5-73 before SERIALIZABLE transactions run in a cluster.
use std::collections::BTreeMap;

use super::super::keys::{is_versioned, keyspace_of, space_of};
use crate::storage::engine::{
    hlc::Ts,
    kv::Keyspace,
    locks::{LockKey, LockMode, LockSpace},
};
use crate::storage::range_api::{RangeError, RangeId, RangeSpace, SYSTEM_RANGE, range_key};
use std::ops::Bound;

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
        LockKey::Row { .. } | LockKey::Table(_) | LockKey::RowKey { .. } => None,
        // Intervals have no single position; split_off cuts them at the split key.
        LockKey::Interval { .. } => None,
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

fn put_bound(out: &mut Vec<u8>, bound: &Bound<Vec<u8>>) -> Result<(), RangeError> {
    match bound {
        Bound::Unbounded => out.push(0),
        Bound::Included(bytes) => {
            out.push(1);
            put_bytes(out, bytes)?;
        }
        Bound::Excluded(bytes) => {
            out.push(2);
            put_bytes(out, bytes)?;
        }
    }
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
    fn bound(&mut self) -> Result<Bound<Vec<u8>>, RangeError> {
        Ok(match self.u8()? {
            0 => Bound::Unbounded,
            1 => Bound::Included(self.bytes()?),
            2 => Bound::Excluded(self.bytes()?),
            _ => return Err(corrupt("bad bound flag")),
        })
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
        let mut moved_locks = Vec::new();
        let mut kept_locks = Vec::new();
        for (key, mode) in std::mem::take(&mut self.locks) {
            let LockKey::Interval { space, start, end } = &key else {
                if moves(lock_position(&key)) {
                    moved_locks.push((key, mode));
                } else {
                    kept_locks.push((key, mode));
                }
                continue;
            };
            let range_space = match space {
                LockSpace::Row => RangeSpace::Rows,
                LockSpace::Unique => RangeSpace::Index,
            };
            // Unbounded endpoints stop at the keyspace edges, not the range edges.
            if range_key(range_space, &[]).as_slice() >= at_key {
                moved_locks.push((key, mode));
                continue;
            }
            if range_space.tag() < at_key[0] {
                kept_locks.push((key, mode));
                continue;
            }
            let at_user = &at_key[1..];
            if matches!(start, Bound::Included(s) | Bound::Excluded(s) if s.as_slice() >= at_user) {
                moved_locks.push((key, mode));
            } else if match end {
                Bound::Included(e) => e.as_slice() < at_user,
                Bound::Excluded(e) => e.as_slice() <= at_user,
                Bound::Unbounded => false,
            } {
                kept_locks.push((key, mode));
            } else {
                // The cut belongs to the right child; neither half is empty.
                kept_locks.push((
                    LockKey::Interval {
                        space: *space,
                        start: start.clone(),
                        end: Bound::Excluded(at_user.to_vec()),
                    },
                    mode,
                ));
                moved_locks.push((
                    LockKey::Interval {
                        space: *space,
                        start: Bound::Included(at_user.to_vec()),
                        end: end.clone(),
                    },
                    mode,
                ));
            }
        }
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
        // One lock per key at its strongest mode (a key held Shared, then upgraded, is held in
        // both modes), in byte order, so one record has one encoding.
        let mut strongest: std::collections::HashMap<&LockKey, LockMode> =
            std::collections::HashMap::with_capacity(self.locks.len());
        for (key, mode) in &self.locks {
            let held = strongest.entry(key).or_insert(*mode);
            if *mode == LockMode::Exclusive {
                *held = LockMode::Exclusive;
            }
        }
        let mut locks = Vec::with_capacity(strongest.len());
        for (key, mode) in strongest {
            let mut lock = Vec::new();
            match key {
                LockKey::Row { table, pk } => {
                    lock.push(1u8);
                    lock.extend_from_slice(&table.to_be_bytes());
                    put_bytes(&mut lock, pk)?;
                }
                LockKey::Unique { index, tuple } => {
                    lock.push(2u8);
                    lock.extend_from_slice(&index.to_be_bytes());
                    put_bytes(&mut lock, tuple)?;
                }
                LockKey::Interval { space, start, end } => {
                    if mode != LockMode::Shared {
                        return Err(corrupt("gap lock mode"));
                    }
                    lock.push(3u8);
                    lock.push(match space {
                        LockSpace::Row => 1,
                        LockSpace::Unique => 2,
                    });
                    put_bound(&mut lock, start)?;
                    put_bound(&mut lock, end)?;
                }
                LockKey::Table(_) => return Err(corrupt("table lock")),
                LockKey::RowKey { .. } => return Err(corrupt("local row-key lock")),
            }
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
        let mut seen = std::collections::HashSet::new();
        for _ in 0..r.u32()? {
            let key = match r.u8()? {
                1 => LockKey::Row {
                    table: r.u64()?,
                    pk: r.bytes()?,
                },
                2 => LockKey::Unique {
                    index: r.u64()?,
                    tuple: r.bytes()?,
                },
                3 => LockKey::Interval {
                    space: match r.u8()? {
                        1 => LockSpace::Row,
                        2 => LockSpace::Unique,
                        _ => return Err(corrupt("bad gap lock space")),
                    },
                    start: r.bound()?,
                    end: r.bound()?,
                },
                _ => return Err(corrupt("bad lock tag")),
            };
            let mode = match r.u8()? {
                0 => LockMode::Shared,
                1 => LockMode::Exclusive,
                _ => return Err(corrupt("bad lock mode")),
            };
            if matches!(key, LockKey::Interval { .. }) && mode != LockMode::Shared {
                return Err(corrupt("gap lock mode"));
            }
            if !seen.insert(key.clone()) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use Bound::{Excluded, Included, Unbounded};

    fn record(locks: Vec<(LockKey, LockMode)>) -> Prepared {
        Prepared {
            home: RangeId(2),
            at: Ts(10),
            writes: BTreeMap::new(),
            locks,
            children: Vec::new(),
        }
    }

    fn user(n: u64) -> Vec<u8> {
        n.to_be_bytes().to_vec()
    }

    #[test]
    fn gap_locks_split_at_bounds_and_keyspace_edges() {
        // Each case specifies the original bounds and the expected (left, right) bounds.
        let cases = [
            (Included(user(1)), Included(user(4)), true, false),
            (Included(user(6)), Included(user(9)), false, true),
            (Included(user(1)), Included(user(9)), true, true),
            (Included(user(1)), Unbounded, true, true),
            (Included(user(5)), Included(user(9)), false, true),
            (Excluded(user(5)), Included(user(9)), false, true),
            (Included(user(1)), Excluded(user(5)), true, false),
            (Included(user(1)), Included(user(5)), true, true),
            (Unbounded, Included(user(9)), true, true),
            (Unbounded, Unbounded, true, true),
            (Included(user(5)), Included(user(5)), false, true),
        ];
        for (space, range_space) in [
            (LockSpace::Row, RangeSpace::Rows),
            (LockSpace::Unique, RangeSpace::Index),
        ] {
            let gap = |start, end| (LockKey::Interval { space, start, end }, LockMode::Shared);
            for (start, end, left, right) in &cases {
                let original = gap(start.clone(), end.clone());
                let mut parent = record(vec![original.clone()]);
                let child = parent.split_off(&range_key(range_space, &user(5)), RangeId(3));
                let expected_left = if *left {
                    vec![if *right {
                        gap(start.clone(), Excluded(user(5)))
                    } else {
                        original.clone()
                    }]
                } else {
                    vec![]
                };
                let expected_right = if *right {
                    vec![if *left {
                        gap(Included(user(5)), end.clone())
                    } else {
                        original.clone()
                    }]
                } else {
                    vec![]
                };
                assert_eq!(parent.locks, expected_left, "{space:?} {start:?} {end:?}");
                assert_eq!(child.is_some(), *right);
                assert_eq!(
                    parent.children,
                    if *right { vec![RangeId(3)] } else { vec![] }
                );
                if let Some(child) = child {
                    assert_eq!(child.locks, expected_right);
                    assert_eq!((child.home, child.at), (parent.home, parent.at));
                    assert!(child.children.is_empty());
                    assert_eq!(Prepared::decode(&child.encode().unwrap()).unwrap(), child);
                }
                assert_eq!(Prepared::decode(&parent.encode().unwrap()).unwrap(), parent);
            }
            // Even unbounded gaps stay within their own keyspace.
            for (split_space, moves) in [
                (RangeSpace::Catalog, true),
                (RangeSpace::Rows, space == LockSpace::Unique),
                (RangeSpace::Index, false),
            ] {
                if split_space == range_space {
                    continue;
                }
                let original = gap(Unbounded, Unbounded);
                let mut parent = record(vec![original.clone()]);
                let child = parent.split_off(&range_key(split_space, &user(5)), RangeId(3));
                assert_eq!(child.is_some(), moves);
                if moves {
                    assert!(parent.locks.is_empty());
                    assert_eq!(child.unwrap().locks, vec![original]);
                } else {
                    assert_eq!(parent.locks, vec![original]);
                }
            }
            let original = gap(Unbounded, Unbounded);
            let mut parent = record(vec![original.clone()]);
            let child = parent
                .split_off(&range_key(range_space, &[]), RangeId(3))
                .unwrap();
            assert!(parent.locks.is_empty());
            assert_eq!(child.locks, vec![original]);
        }
    }

    #[test]
    fn writes_point_locks_and_sentinels_keep_their_split_routing() {
        let row = |table, pk| (LockKey::Row { table, pk }, LockMode::Exclusive);
        let kept = vec![
            row(4, vec![1]),
            row(6, vec![]),
            (LockKey::Table(6), LockMode::AccessShare),
        ];
        let moved = vec![
            row(5, vec![1]),
            (
                LockKey::Unique {
                    index: 1,
                    tuple: vec![1],
                },
                LockMode::Exclusive,
            ),
        ];
        let mut parent = record([kept.clone(), moved.clone()].concat());
        parent
            .writes
            .insert((Keyspace::Rows, user(4)), Some(vec![4]));
        parent.writes.insert((Keyspace::Rows, user(5)), None);
        let child = parent
            .split_off(&range_key(RangeSpace::Rows, &user(5)), RangeId(3))
            .unwrap();
        assert_eq!(parent.locks, kept);
        assert_eq!(child.locks, moved);
        assert_eq!(
            parent.writes,
            BTreeMap::from([((Keyspace::Rows, user(4)), Some(vec![4]))])
        );
        assert_eq!(
            child.writes,
            BTreeMap::from([((Keyspace::Rows, user(5)), None)])
        );
    }
}
