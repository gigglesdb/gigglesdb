//! R5-104: a committed change is (commit timestamp, range key), independent of range id.
use crate::storage::engine::{
    codec,
    kv::{Keyspace, WriteBatch},
    mvcc,
};
use crate::storage::range::keys::{contains_raw, space_of};
use crate::storage::range::store::tests::{Dir, SPLIT_TABLE, pos, row, rows, two_ranges};
use crate::storage::range::{RangeApplyOutcome, RangeStore, SplitOutcome};
use crate::storage::range_api::{RangeDescriptor, RangeId, RangeSpace, range_key};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

type Change = (u64, Vec<u8>, Option<[u8; 32]>);
const PARENT: RangeId = RangeId(2);
const CHILD: RangeId = RangeId(5);
const TIMES: [u64; 4] = [0, 7, 42, u64::MAX];

/// All committed versions owned by `range`, including versions hidden by later tombstones.
/// Hash the live payload; a tombstone is None (distinct from a live empty value).
/// Seq, Gc and range-local Meta records are not committed change identities.
pub(crate) fn change_dump(store: &RangeStore, range: RangeId) -> Vec<Change> {
    let desc = store.descriptor(range).unwrap().expect("range exists");
    let mut dump = Vec::new();
    for ks in [
        Keyspace::Rows,
        Keyspace::Index,
        Keyspace::Catalog,
        Keyspace::Kernel,
    ] {
        let space = space_of(ks).unwrap();
        for (raw, value) in rows(store, ks) {
            if !contains_raw(&desc, ks, &raw).unwrap() {
                continue;
            }
            let (user, ts) = codec::split_versioned(&raw).unwrap();
            let hash = mvcc::decode(&value)
                .unwrap()
                .map(|v| Sha256::digest(v).into());
            dump.push((ts.0, range_key(space, user), hash));
        }
    }
    dump.sort();
    dump
}

/// Three tables/indexes, four keys each, four versions each; same timestamps in both spaces.
/// The latest version deletes odd keys and leaves even keys live with an empty payload.
fn fixture(dir: &Dir) -> (RangeStore, Vec<Change>, Vec<Change>) {
    let store = two_ranges(dir);
    let mut expected = Vec::new();
    let mut system_expected = Vec::new();
    for (version, ts) in TIMES.into_iter().enumerate() {
        let mut data = WriteBatch::default();
        let mut system = WriteBatch::default();
        for (ks, space) in [
            (Keyspace::Rows, RangeSpace::Rows),
            (Keyspace::Index, RangeSpace::Index),
            (Keyspace::Catalog, RangeSpace::Catalog),
            (Keyspace::Kernel, RangeSpace::Kernel),
        ] {
            for table in SPLIT_TABLE..SPLIT_TABLE + 3 {
                for n in 1..=4 {
                    let raw = row(table, n, ts);
                    let (user, _) = codec::split_versioned(&raw).unwrap();
                    let key = range_key(space, user);
                    let payload = if version == 3 && n % 2 == 1 {
                        None
                    } else if version == 3 {
                        Some(Vec::new())
                    } else {
                        Some(format!("{space:?}/{table}/{n}/{version}").into_bytes())
                    };
                    let value = match &payload {
                        Some(v) => mvcc::encode_live(v),
                        None => mvcc::encode_tombstone(),
                    };
                    let change = (ts, key, payload.map(|v| Sha256::digest(v).into()));
                    if matches!(ks, Keyspace::Catalog | Keyspace::Kernel) {
                        system.put(ks, raw, value);
                        system_expected.push(change);
                    } else {
                        data.put(ks, raw, value);
                        expected.push(change);
                    }
                }
            }
        }
        // Unversioned keys must never appear in the dump, nor be decoded as versions.
        system.put(Keyspace::Seq, vec![1], vec![2]);
        system.put(Keyspace::Gc, vec![3], vec![4]);
        for (range, batch) in [(PARENT, data), (RangeId(1), system)] {
            assert_eq!(
                store
                    .apply(range, &batch.encode(), pos(version as u64 + 1, 1))
                    .unwrap(),
                RangeApplyOutcome::Applied
            );
        }
    }
    expected.sort();
    system_expected.sort();
    (store, expected, system_expected)
}

fn assert_unique(dump: &[Change]) {
    let identities: BTreeSet<_> = dump.iter().map(|(ts, key, _)| (*ts, key)).collect();
    assert_eq!(identities.len(), dump.len(), "duplicate change identity");
}

fn assert_half(desc: &RangeDescriptor, dump: &[Change]) {
    assert!(!dump.is_empty());
    assert!(dump.iter().all(|(_, key, _)| desc.contains(key)));
    assert_unique(dump);
}

fn assert_split_preserves_changes(at: Vec<u8>, tag: &str) {
    let dir = Dir::new(tag);
    let (store, expected, system_expected) = fixture(&dir);
    let before = change_dump(&store, PARENT);
    assert_eq!(before, expected);
    assert_eq!(before.len(), 96);
    assert_eq!(
        before.iter().filter(|(_, _, hash)| hash.is_none()).count(),
        12
    );
    assert_unique(&before);
    // Catalog/Kernel versions belong to the system range, not either data half.
    assert_eq!(change_dump(&store, RangeId(1)), system_expected);
    let old = store.descriptor(PARENT).unwrap().unwrap();
    let result = store
        .split(PARENT, old.epoch, &at, CHILD, pos(5, 1))
        .unwrap();
    let SplitOutcome::Applied { left, right } = result else {
        panic!("{result:?}");
    };
    assert_eq!(left.end.as_ref(), Some(&at));
    assert_eq!(right.start, at);
    let left_dump = change_dump(&store, PARENT);
    let right_dump = change_dump(&store, CHILD);
    assert_half(&left, &left_dump);
    assert_half(&right, &right_dump);
    // Every version of the split key is on the right, even timestamp 0 / MAX.
    assert_eq!(
        right_dump.iter().filter(|(_, key, _)| key == &at).count(),
        if at.len() > 9 { TIMES.len() } else { 0 }
    );
    let mut union = left_dump;
    union.extend(right_dump);
    union.sort();
    assert_unique(&union);
    assert_eq!(union, before);
    assert_eq!(change_dump(&store, RangeId(1)), system_expected);
    drop(store);
    let reopened = RangeStore::open(dir.sub("db")).unwrap();
    let mut union = change_dump(&reopened, PARENT);
    union.extend(change_dump(&reopened, CHILD));
    union.sort();
    assert_eq!(union, before);
}

#[test]
fn a_split_preserves_every_change_identity() {
    let raw = row(SPLIT_TABLE + 1, 2, 0);
    let (user, _) = codec::split_versioned(&raw).unwrap();
    assert_split_preserves_changes(range_key(RangeSpace::Rows, user), "change-dump-user-split");
}

#[test]
fn a_table_boundary_split_preserves_every_change_identity() {
    assert_split_preserves_changes(
        range_key(RangeSpace::Rows, &(SPLIT_TABLE + 1).to_be_bytes()),
        "change-dump-table-split",
    );
}

#[test]
#[ignore = "R5-104 move: Phase 10"]
fn a_move_preserves_every_change_identity() {
    // Before moving, dump every committed version of the range on its old replicas.
    // After the move completes, dump that range on each new replica and require exact
    // equality (timestamp, range key, live-value hash or tombstone), with no duplicates.
    // Assert descriptor ownership and that historical versions and tombstones survive;
    // range/replica identities and local Meta must not enter the change identity.
}
