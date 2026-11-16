//! Track A owns this module.
//! Range size and a midpoint split key (GD-0075 R5-83).
//!
//! Both are read-only. `approximate_size` asks RocksDB for the approximate stored bytes of the
//! range's per-keyspace raw intervals (keys.rs `cf_interval`); range-local Meta keys are not in
//! those intervals. `split_key` walks the range's raw keys on one snapshot, in range-key order
//! (keyspaces in tag order, then raw key order), and returns the range key of the first whole
//! user key that starts at or after half of the range's exact key+value bytes. Versions share a
//! user key, so all versions of the returned key fall right of the split (design.md Changes,
//! Track A Phase 1 (g): a split bound's user part is a whole user key of its keyspace).
use std::ops::Bound;

use super::super::export::intervals;
use super::super::keys::{RawInterval, keyspace_of, user_key};
use super::{RangeStore, read_descriptor};
use crate::CoreError;
use crate::storage::engine::kv::{Direction, KvRead};
use crate::storage::range_api::{RangeId, RangeSpace, SYSTEM_RANGE, range_key};

fn unknown(range: RangeId) -> CoreError {
    CoreError::Corrupt(format!("unknown range {}", range.0))
}

fn as_slice(bound: &Bound<Vec<u8>>) -> Bound<&[u8]> {
    bound.as_ref().map(Vec::as_slice)
}

impl RangeStore {
    /// Approximate bytes of the range's data in every keyspace (RocksDB approximate sizes of its
    /// cf intervals), not counting range-local Meta keys. rocksdb 0.25 counts SST files only, so
    /// data still in memtables is not counted until it is flushed.
    pub fn approximate_size(&self, range: RangeId) -> Result<u64, CoreError> {
        let descriptor = self.descriptor(range)?.ok_or_else(|| unknown(range))?;
        Ok(intervals(&descriptor)
            .iter()
            .map(|(space, (start, end))| {
                self.kv()
                    .approximate_size(keyspace_of(*space), as_slice(start), as_slice(end))
            })
            .sum())
    }

    /// Flush memtables so approximate_size counts recent writes. The size policy does not call
    /// this (RocksDB flushes on its own in production); tests call it before a policy pass.
    pub fn flush_memtables(&self) -> Result<(), CoreError> {
        Ok(self.kv().flush_memtables()?)
    }

    /// A range key strictly inside the range (start < key < end) at a whole stored user key,
    /// chosen so that about half of the range's bytes lie below it; None when the range holds
    /// fewer than two distinct user keys, or is the system range (SYSTEM_RANGE never splits by
    /// size; its only split is at first_user_key(), done by the policy, not here).
    pub fn split_key(&self, range: RangeId) -> Result<Option<Vec<u8>>, CoreError> {
        if range == SYSTEM_RANGE {
            return Ok(None);
        }
        self.kv()
            .with_snapshot(|snapshot| Ok(midpoint(snapshot, range)))?
    }
}

fn midpoint(kv: &dyn KvRead, range: RangeId) -> Result<Option<Vec<u8>>, CoreError> {
    let descriptor = read_descriptor(kv, range)?.ok_or_else(|| unknown(range))?;
    let parts = intervals(&descriptor);
    let mut total = 0u64;
    walk(kv, &parts, |_, key, value| {
        total += (key.len() + value.len()) as u64;
        Ok(true)
    })?;
    let mut below = 0u64;
    let mut last: Option<(RangeSpace, Vec<u8>)> = None;
    let mut found = None;
    walk(kv, &parts, |space, key, value| {
        let user = user_key(space, key).map_err(|e| CoreError::Corrupt(format!("{e:?}")))?;
        if last
            .as_ref()
            .is_none_or(|(s, u)| *s != space || u.as_slice() != user)
        {
            // Only Rows/Index keys are split bounds (split.rs refuses other spaces); a range
            // other than the system range holds no other space anyway.
            if below * 2 >= total && matches!(space, RangeSpace::Rows | RangeSpace::Index) {
                let at = range_key(space, user);
                if at > descriptor.start {
                    found = Some(at);
                    return Ok(false);
                }
            }
            last = Some((space, user.to_vec()));
        }
        below += (key.len() + value.len()) as u64;
        Ok(true)
    })?;
    // `below * 2 >= total` never holds at the first stored key (Rows/Index keys have bytes),
    // so a range with one user key, or none, yields None.
    Ok(found)
}

/// Visits the range's raw keys in range-key order until `visit` returns false.
fn walk(
    kv: &dyn KvRead,
    parts: &[(RangeSpace, RawInterval)],
    mut visit: impl FnMut(RangeSpace, &[u8], &[u8]) -> Result<bool, CoreError>,
) -> Result<(), CoreError> {
    for (space, (start, end)) in parts {
        let ks = keyspace_of(*space);
        for item in kv.range(ks, as_slice(start), as_slice(end), Direction::Forward)? {
            let (key, value) = item?;
            if !visit(*space, &key, &value)? {
                return Ok(());
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::tests::{Dir, SPLIT_TABLE, descriptors, row};
    use super::*;
    use crate::storage::engine::{
        codec,
        kv::{Durability, Keyspace, KvBackend, WriteBatch},
    };
    use crate::storage::range::keys::contains_raw;
    use crate::storage::range_api::RangeDescriptor;

    const RIGHT: RangeId = RangeId(2);

    /// SYSTEM_RANGE = [MIN, start) and range 2 = [start, MAX).
    fn store_split_at(dir: &Dir, start: Vec<u8>) -> RangeStore {
        let [mut left, mut right] = descriptors();
        left.end = Some(start.clone());
        right.start = start;
        let store = RangeStore::create(dir.sub("db"), [7; 16]).unwrap();
        store.create_range(&left).unwrap();
        store.create_range(&right).unwrap();
        store
    }
    fn table_start() -> Vec<u8> {
        range_key(RangeSpace::Rows, &SPLIT_TABLE.to_be_bytes())
    }
    fn user(table: u64, n: u64) -> Vec<u8> {
        codec::split_versioned(&row(table, n, 0))
            .unwrap()
            .0
            .to_vec()
    }
    /// Rows `n` of `table` with versions 1..=versions, each holding `len` random bytes.
    fn put(store: &RangeStore, ks: Keyspace, table: u64, ns: &[u64], versions: u64, len: usize) {
        let mut batch = WriteBatch::default();
        for &n in ns {
            for ts in 1..=versions {
                let mut value = vec![0; len];
                getrandom::fill(&mut value).unwrap();
                batch.put(ks, row(table, n, ts), value);
            }
        }
        store.kv().write(batch, Durability::NoSync).unwrap();
    }

    #[test]
    fn empty_and_single_key_ranges_have_no_split_key() {
        let dir = Dir::new("size-empty");
        let store = store_split_at(&dir, table_start());
        assert!(store.approximate_size(RIGHT).unwrap() < 4096);
        assert_eq!(store.split_key(RIGHT).unwrap(), None);
        put(&store, Keyspace::Rows, SPLIT_TABLE, &[1], 50, 100);
        assert_eq!(store.split_key(RIGHT).unwrap(), None);
        assert!(store.split_key(RangeId(9)).is_err());
        assert!(store.approximate_size(RangeId(9)).is_err());
    }

    #[test]
    fn equal_rows_split_near_the_middle_with_versions_right() {
        let dir = Dir::new("size-middle");
        let store = store_split_at(&dir, table_start());
        let ns: Vec<u64> = (0..200).collect();
        put(&store, Keyspace::Rows, SPLIT_TABLE, &ns, 3, 64);
        let key = store.split_key(RIGHT).unwrap().unwrap();
        let n = ns
            .iter()
            .copied()
            .find(|&n| range_key(RangeSpace::Rows, &user(SPLIT_TABLE, n)) == key)
            .expect("split key is a stored user key");
        assert!((80..=120).contains(&n), "{n}");
        assert!(key > table_start());
        let right = RangeDescriptor {
            start: key,
            ..store.descriptor(RIGHT).unwrap().unwrap()
        };
        for ts in 1..=3 {
            assert!(contains_raw(&right, Keyspace::Rows, &row(SPLIT_TABLE, n, ts)).unwrap());
            assert!(!contains_raw(&right, Keyspace::Rows, &row(SPLIT_TABLE, n - 1, ts)).unwrap());
        }
    }

    #[test]
    fn midpoint_lands_in_the_keyspace_holding_it() {
        let ns: Vec<u64> = (0..100).collect();
        for (rows_len, index_len, space) in
            [(16, 400, RangeSpace::Index), (400, 16, RangeSpace::Rows)]
        {
            let dir = Dir::new("size-spaces");
            let store = store_split_at(&dir, table_start());
            put(&store, Keyspace::Rows, SPLIT_TABLE, &ns, 1, rows_len);
            put(&store, Keyspace::Index, SPLIT_TABLE + 5, &ns, 1, index_len);
            let key = store.split_key(RIGHT).unwrap().unwrap();
            assert_eq!(key[0], space.tag(), "{key:?}");
        }
    }

    #[test]
    fn never_returns_a_row_key_start() {
        let dir = Dir::new("size-rowstart");
        let start = range_key(RangeSpace::Rows, &user(SPLIT_TABLE, 5));
        let store = store_split_at(&dir, start.clone());
        put(&store, Keyspace::Rows, SPLIT_TABLE, &[5], 20, 1000);
        assert_eq!(store.split_key(RIGHT).unwrap(), None);
        put(&store, Keyspace::Rows, SPLIT_TABLE, &[6], 1, 10);
        let key = store.split_key(RIGHT).unwrap().unwrap();
        assert_ne!(key, start);
        assert_eq!(key, range_key(RangeSpace::Rows, &user(SPLIT_TABLE, 6)));
    }

    #[test]
    fn system_range_never_splits_by_size() {
        let dir = Dir::new("size-system");
        let store = store_split_at(&dir, table_start());
        let ns: Vec<u64> = (0..50).collect();
        put(&store, Keyspace::Rows, SPLIT_TABLE - 1, &ns, 1, 100);
        put(&store, Keyspace::Catalog, 3, &ns, 1, 100);
        assert_eq!(store.split_key(SYSTEM_RANGE).unwrap(), None);
    }

    #[test]
    fn approximate_size_counts_flushed_bytes_of_the_range_only() {
        let dir = Dir::new("size-approx");
        let store = store_split_at(&dir, table_start());
        let before = store.approximate_size(RIGHT).unwrap();
        let ns: Vec<u64> = (0..1024).collect();
        put(&store, Keyspace::Rows, SPLIT_TABLE, &ns, 1, 4096);
        store.flush_memtables().unwrap();
        let after = store.approximate_size(RIGHT).unwrap();
        let written = 1024 * 4096;
        assert!(after > before && after > written / 2, "{before} -> {after}");
        assert!(store.approximate_size(SYSTEM_RANGE).unwrap() < 256 << 10);
    }
}
