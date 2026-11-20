use super::*;
use crate::storage::engine::{codec, hlc::Ts, kv::Direction};
use crate::storage::range_api::{Epoch, RangeSpace, ReplicaDesc, ReplicaRole, range_key};
use std::{ops::Bound, path::PathBuf};

pub(crate) struct Dir(pub(crate) PathBuf);
impl Dir {
    pub(crate) fn new(tag: &str) -> Self {
        let mut nonce = [0; 16];
        getrandom::fill(&mut nonce).unwrap();
        let path = std::env::temp_dir().join(format!("{tag}-{:032x}", u128::from_be_bytes(nonce)));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
    pub(crate) fn sub(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub(crate) const SPLIT_TABLE: u64 = 10;
pub(crate) fn pos(index: u64, term: u64) -> AppliedPosition {
    AppliedPosition { index, term }
}
/// Range 1 = [MIN, Rows table 10); range 2 = [Rows table 10, MAX).
pub(crate) fn descriptors() -> [RangeDescriptor; 2] {
    let split = range_key(RangeSpace::Rows, &SPLIT_TABLE.to_be_bytes());
    let replicas = vec![ReplicaDesc {
        node: 1,
        role: ReplicaRole::Voter,
    }];
    [
        RangeDescriptor {
            id: RangeId(1),
            epoch: Epoch(1),
            start: vec![],
            end: Some(split.clone()),
            replicas: replicas.clone(),
        },
        RangeDescriptor {
            id: RangeId(2),
            epoch: Epoch(1),
            start: split,
            end: None,
            replicas,
        },
    ]
}
pub(crate) fn row(table: u64, n: u64, ts: u64) -> Vec<u8> {
    codec::row_key(table, &n.to_be_bytes(), Ts(ts))
}
pub(crate) fn two_ranges(dir: &Dir) -> RangeStore {
    let store = RangeStore::create(dir.sub("db"), [7; 16]).unwrap();
    for d in descriptors() {
        store.create_range(&d).unwrap();
    }
    store
}
pub(crate) fn rows(store: &RangeStore, ks: Keyspace) -> Vec<(Vec<u8>, Vec<u8>)> {
    store
        .kv()
        .range(ks, Bound::Unbounded, Bound::Unbounded, Direction::Forward)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}
fn put(ks: Keyspace, key: Vec<u8>, value: &[u8]) -> Vec<u8> {
    let mut batch = WriteBatch::default();
    batch.put(ks, key, value.to_vec());
    batch.encode()
}
fn node_features(store: &RangeStore) -> u64 {
    u64::from_be_bytes(
        store
            .kv()
            .get(Keyspace::Meta, b"features")
            .unwrap()
            .unwrap()
            .try_into()
            .unwrap(),
    )
}

#[test]
fn ranges_apply_independently_with_skip_and_gap_rules() {
    let dir = Dir::new("range-store");
    let store = two_ranges(&dir);
    assert_eq!(store.ranges().unwrap(), vec![RangeId(1), RangeId(2)]);
    let (a, b) = (RangeId(1), RangeId(2));
    assert_eq!(store.applied(a).unwrap(), None);
    let low = put(Keyspace::Rows, row(3, 1, 1), b"a");
    let high = put(Keyspace::Rows, row(11, 1, 1), b"b");
    assert_eq!(
        store.apply(a, &low, pos(1, 1)).unwrap(),
        RangeApplyOutcome::Applied
    );
    assert_eq!(
        store.apply(b, &high, pos(1, 1)).unwrap(),
        RangeApplyOutcome::Applied
    );
    assert_eq!(
        store.apply(b, &high, pos(2, 1)).unwrap(),
        RangeApplyOutcome::Applied
    );
    assert_eq!(
        store.apply(a, &low, pos(1, 1)).unwrap(),
        RangeApplyOutcome::Skipped
    );
    assert!(matches!(
        store.apply(a, &low, pos(3, 1)),
        Err(CoreError::Corrupt(_))
    ));
    assert_eq!(
        store.apply_noop(a, pos(2, 2)).unwrap(),
        RangeApplyOutcome::Applied
    );
    assert_eq!(store.applied(a).unwrap(), Some(pos(2, 2)));
    assert_eq!(store.applied(b).unwrap(), Some(pos(2, 1)));
    assert_eq!(rows(&store, Keyspace::Rows).len(), 2);
    assert!(matches!(
        store.apply(RangeId(9), &low, pos(1, 1)),
        Err(CoreError::Corrupt(_))
    ));
    // Reopen keeps positions; the directory validates with range-local keys present.
    drop(store);
    let store = RangeStore::open(dir.sub("db")).unwrap();
    assert_eq!(store.applied(b).unwrap(), Some(pos(2, 1)));
    assert_eq!(store.descriptor(a).unwrap(), Some(descriptors()[0].clone()));
}

#[test]
fn out_of_range_batches_are_refused_identically() {
    let dir = Dir::new("range-store");
    let dir_a = Dir::new("range-store-a");
    let stores = [two_ranges(&dir_a), two_ranges(&dir)];
    let a = RangeId(1);
    let mut mixed = WriteBatch::default();
    mixed.put(Keyspace::Rows, row(3, 1, 1), b"in".to_vec());
    mixed.put(Keyspace::Rows, row(SPLIT_TABLE, 0, 1), b"out".to_vec());
    let mut wide = WriteBatch::default();
    wide.delete_range(Keyspace::Rows, row(3, 0, 0), row(SPLIT_TABLE, 1, 0));
    let refused: Vec<Vec<u8>> = vec![
        mixed.encode(),
        wide.encode(),
        put(Keyspace::Rows, vec![1, 2, 3], b"short versioned key"),
        put(Keyspace::Meta, b"raft_applied".to_vec(), &[0; 16]),
        put(Keyspace::Meta, applied_key(a), &[0; 16]),
        put(
            Keyspace::Meta,
            range_local_key(a, &RangeLocalKey::Install),
            b"x",
        ),
        put(
            Keyspace::Meta,
            range_local_key(RangeId(2), &RangeLocalKey::LastCommitTs),
            &[0; 8],
        ),
        put(
            Keyspace::Meta,
            range_local_key(RangeId(2), &RangeLocalKey::OracleHw),
            &[0; 8],
        ),
    ];
    for store in &stores {
        for (i, batch) in refused.iter().enumerate() {
            let at = pos(i as u64 + 1, 1);
            assert_eq!(
                store.apply(a, batch, at).unwrap(),
                RangeApplyOutcome::Refused,
                "batch {i}"
            );
            assert_eq!(store.applied(a).unwrap(), Some(at));
        }
        assert!(rows(store, Keyspace::Rows).is_empty());
    }
    let contents = |s: &RangeStore| Keyspace::ALL.map(|ks| rows(s, ks));
    assert_eq!(contents(&stores[0]), contents(&stores[1]));
    // A Meta delete-range over the range's own keys must spare applied and install.
    let (lo, hi) = super::local_interval(a);
    let mut sweep = WriteBatch::default();
    sweep.delete_range(Keyspace::Meta, lo, hi);
    assert_eq!(
        stores[1]
            .apply(a, &sweep.encode(), pos(refused.len() as u64 + 1, 1))
            .unwrap(),
        RangeApplyOutcome::Refused
    );
}

#[test]
fn range_local_meta_writes_and_feature_union() {
    let dir = Dir::new("range-store");
    let store = two_ranges(&dir);
    let b = RangeId(2);
    let before = node_features(&store);
    let bits = meta::REPLICATED | 1;
    let mut batch = WriteBatch::default();
    batch.put(
        Keyspace::Meta,
        range_local_key(b, &RangeLocalKey::LastCommitTs),
        9u64.to_be_bytes().to_vec(),
    );
    batch.put(
        Keyspace::Meta,
        range_local_key(b, &RangeLocalKey::Features),
        bits.to_be_bytes().to_vec(),
    );
    batch.put(
        Keyspace::Meta,
        range_local_key(b, &RangeLocalKey::Marker(5)),
        9u64.to_be_bytes().to_vec(),
    );
    batch.put(
        Keyspace::Meta,
        range_local_key(b, &RangeLocalKey::Prepared(4)),
        crate::storage::range::engine::prepared::Prepared {
            home: RangeId(2),
            at: crate::storage::engine::hlc::Ts(1),
            writes: Default::default(),
            locks: vec![],
            children: vec![],
        }
        .encode()
        .unwrap(),
    );
    batch.put(Keyspace::Rows, row(SPLIT_TABLE, 1, 1), b"v".to_vec());
    assert_eq!(
        store.apply(b, &batch.encode(), pos(1, 1)).unwrap(),
        RangeApplyOutcome::Applied
    );
    assert_eq!(node_features(&store), before | bits);
    let mut gone = WriteBatch::default();
    gone.delete(
        Keyspace::Meta,
        range_local_key(b, &RangeLocalKey::Prepared(4)),
    );
    assert_eq!(
        store.apply(b, &gone.encode(), pos(2, 1)).unwrap(),
        RangeApplyOutcome::Applied
    );
    assert_eq!(
        store
            .kv()
            .get(
                Keyspace::Meta,
                &range_local_key(b, &RangeLocalKey::Prepared(4))
            )
            .unwrap(),
        None
    );
    drop(store);
    RangeStore::open(dir.sub("db")).unwrap();
}

#[test]
fn create_range_is_idempotent_and_needs_the_ranges_feature() {
    let dir = Dir::new("range-store");
    let store = two_ranges(&dir);
    let [first, _] = descriptors();
    store.create_range(&first).unwrap();
    let mut other = first.clone();
    other.epoch = Epoch(2);
    assert!(matches!(store.create_range(&other), Err(CoreError::Exists)));
    drop(store);
    let plain = crate::storage::replicated::ReplicaStore::create(dir.sub("r4"), [1; 16]).unwrap();
    drop(plain);
    assert!(matches!(
        RangeStore::open(dir.sub("r4")),
        Err(CoreError::Unsupported)
    ));
    // A single-group store refuses a range-mode directory.
    assert!(matches!(
        crate::storage::replicated::ReplicaStore::open(dir.sub("db")),
        Err(CoreError::Unsupported)
    ));
}

#[test]
fn invalid_range_values_stop_the_range_instead_of_being_refused() {
    let dir = Dir::new("range-store");
    let store = two_ranges(&dir);
    let a = RangeId(1);
    // A feature bit this build does not know: an error, and the position does not move.
    let unknown = put(
        Keyspace::Meta,
        range_local_key(a, &RangeLocalKey::Features),
        &(1u64 << 60).to_be_bytes(),
    );
    assert!(matches!(
        store.apply(a, &unknown, pos(1, 1)),
        Err(CoreError::Corrupt(_))
    ));
    assert_eq!(store.applied(a).unwrap(), None);
    let short = put(
        Keyspace::Meta,
        range_local_key(a, &RangeLocalKey::LastCommitTs),
        &[0; 7],
    );
    assert!(matches!(
        store.apply(a, &short, pos(1, 1)),
        Err(CoreError::Corrupt(_))
    ));
    // An op outside the range is refused and advances the position.
    let outside = put(Keyspace::Rows, row(SPLIT_TABLE, 1, 1), b"x");
    assert_eq!(
        store.apply(a, &outside, pos(1, 1)).unwrap(),
        RangeApplyOutcome::Refused
    );
    assert_eq!(store.applied(a).unwrap(), Some(pos(1, 1)));
}

/// Distinct commit timestamps model two publishers that read before either commits.
fn directory_value(desc: &RangeDescriptor) -> Vec<u8> {
    let mut value = vec![0];
    value.extend(desc.encode().unwrap());
    value
}
fn directory_put(value: &[u8], ts: u64) -> Vec<u8> {
    let key = codec::user_key_seek_at(&[DIRECTORY_TAG, 0xFF, 0xFF], Ts(ts));
    put(Keyspace::Catalog, key, value)
}

#[test]
fn directory_publish_epochs_are_monotonic_at_apply() {
    let dir = Dir::new("directory-epoch");
    let store = two_ranges(&dir);
    let mut older = descriptors()[1].clone();
    older.epoch = Epoch(5);
    let old = directory_value(&older);
    let mut newer = older.clone();
    newer.epoch = Epoch(6);
    let new = directory_value(&newer);
    let apply = |value: &[u8], index| {
        store
            .apply(SYSTEM_RANGE, &directory_put(value, index), pos(index, 1))
            .unwrap()
    };
    assert_eq!(apply(&old, 1), RangeApplyOutcome::Applied);
    assert_eq!(apply(&new, 2), RangeApplyOutcome::Applied);
    let before = rows(&store, Keyspace::Catalog);
    // The stale publisher commits last, with a newer MVCC timestamp but an older epoch.
    let mut stale = WriteBatch::decode(&directory_put(&old, 3)).unwrap();
    stale.put(
        Keyspace::Catalog,
        codec::user_key_seek_at(b"other", Ts(3)),
        vec![0, 7],
    );
    assert_eq!(
        store
            .apply(SYSTEM_RANGE, &stale.encode(), pos(3, 1))
            .unwrap(),
        RangeApplyOutcome::Refused
    );
    assert_eq!(rows(&store, Keyspace::Catalog), before);
    assert_eq!(store.applied(SYSTEM_RANGE).unwrap(), Some(pos(3, 1)));
    assert_eq!(apply(&new, 4), RangeApplyOutcome::Applied);
    let before = rows(&store, Keyspace::Catalog);
    newer.replicas[0].node = 2;
    assert_eq!(
        apply(&directory_value(&newer), 5),
        RangeApplyOutcome::Refused
    );
    assert_eq!(rows(&store, Keyspace::Catalog), before);
    assert_eq!(store.applied(SYSTEM_RANGE).unwrap(), Some(pos(5, 1)));
    // Same epoch with different bounds is also refused (byte equality, not replica equality).
    newer.replicas[0].node = 1;
    newer.start.push(1);
    assert_eq!(
        apply(&directory_value(&newer), 6),
        RangeApplyOutcome::Refused
    );
    drop(store);
    let reopened = RangeStore::open(dir.sub("db")).unwrap();
    assert_eq!(reopened.applied(SYSTEM_RANGE).unwrap(), Some(pos(6, 1)));
    assert_eq!(rows(&reopened, Keyspace::Catalog), before);
}

#[test]
fn directory_guard_uses_only_newest_live_decodable_value() {
    for replacement in [vec![1], vec![0, 42]] {
        let dir = Dir::new("directory-tombstone");
        let store = two_ranges(&dir);
        let mut desc = descriptors()[1].clone();
        desc.epoch = Epoch(9);
        let values = [directory_value(&desc), replacement, {
            desc.epoch = Epoch(1);
            directory_value(&desc)
        }];
        for (i, value) in values.iter().enumerate() {
            let index = i as u64 + 1;
            assert_eq!(
                store
                    .apply(SYSTEM_RANGE, &directory_put(value, index), pos(index, 1))
                    .unwrap(),
                RangeApplyOutcome::Applied
            );
        }
    }
}

#[test]
fn directory_looking_keys_outside_system_range_are_unaffected() {
    let dir = Dir::new("directory-user-range");
    let store = RangeStore::create(dir.sub("db"), [7; 16]).unwrap();
    let mut desc = descriptors()[1].clone();
    // This non-system test range owns Catalog as well as user data.
    desc.start.clear();
    store.create_range(&desc).unwrap();
    for (index, epoch) in [(1, 9), (2, 1)] {
        desc.epoch = Epoch(epoch);
        assert_eq!(
            store
                .apply(
                    desc.id,
                    &directory_put(&directory_value(&desc), index),
                    pos(index, 1)
                )
                .unwrap(),
            RangeApplyOutcome::Applied
        );
    }
    assert_eq!(rows(&store, Keyspace::Catalog).len(), 2);
}

#[test]
fn receiving_batches_refuse_wide_rows_and_prepared_values_atomically() {
    use crate::storage::engine::{meta::WIDE_ROWS, mvcc};
    use crate::storage::range::engine::prepared::Prepared;
    let dir = Dir::new("range-receiving-bound");
    let store = two_ranges(&dir);
    let id = RangeId(2);
    let before = rows(&store, Keyspace::Meta);
    let _budget =
        crate::relational::ValueBudget::enter(crate::relational::DEFAULT_MAX_VALUE_BYTES, true);
    let wide = codec::encode_row(
        b"k",
        1,
        &[crate::relational::Value::Text("x".repeat(70_000))],
    )
    .unwrap();
    let mut prepared = Prepared {
        home: id,
        at: Ts(1),
        writes: Default::default(),
        locks: vec![],
        children: vec![],
    };
    prepared
        .writes
        .insert((Keyspace::Rows, b"k".to_vec()), Some(wide.clone()));
    let encoded = prepared.encode().unwrap();
    assert!(matches!(
        Prepared::decode(&encoded),
        Err(crate::storage::range_api::RangeError::Corrupt(_))
    ));
    for (ks, key, value) in [
        (
            Keyspace::Rows,
            row(SPLIT_TABLE, 1, 1),
            mvcc::encode_live(&wide),
        ),
        (
            Keyspace::Meta,
            range_local_key(id, &RangeLocalKey::Prepared(1)),
            encoded,
        ),
    ] {
        let mut batch = WriteBatch::default();
        batch.put(Keyspace::Rows, row(SPLIT_TABLE, 2, 1), b"small".to_vec());
        batch.put(
            Keyspace::Meta,
            range_local_key(id, &RangeLocalKey::Features),
            (node_features(&store) | WIDE_ROWS).to_be_bytes().to_vec(),
        );
        batch.put(ks, key, value);
        assert!(matches!(
            store.apply(id, &batch.encode(), pos(1, 1)),
            Err(CoreError::Corrupt(_))
        ));
        assert_eq!(rows(&store, Keyspace::Meta), before);
        assert!(rows(&store, Keyspace::Rows).is_empty());
        assert_eq!(store.applied(id).unwrap(), None);
    }
    for owner in [id, RangeId(1)] {
        let mut batch = WriteBatch::default();
        // An earlier routing refusal must not hide the embedded wide row.
        batch.put(Keyspace::Rows, row(1, 1, 1), b"small".to_vec());
        batch.put(Keyspace::Meta,
            range_local_key(owner, &RangeLocalKey::Prepared(1)),
            prepared.encode().unwrap());
        assert!(matches!(store.apply(id, &batch.encode(), pos(1, 1)),
            Err(CoreError::Corrupt(_))));
        assert_eq!(rows(&store, Keyspace::Meta), before);
        assert!(rows(&store, Keyspace::Rows).is_empty());
        assert_eq!(store.applied(id).unwrap(), None);
    }
    let small = codec::encode_row(
        b"k",
        1,
        &[crate::relational::Value::Text("x".repeat(60_000))],
    )
    .unwrap();
    store
        .apply(
            id,
            &put(
                Keyspace::Rows,
                row(SPLIT_TABLE, 1, 1),
                &mvcc::encode_live(&small),
            ),
            pos(1, 1),
        )
        .unwrap();
    assert_eq!(rows(&store, Keyspace::Rows).len(), 1);
    assert_eq!(node_features(&store) & WIDE_ROWS, 0);
}
