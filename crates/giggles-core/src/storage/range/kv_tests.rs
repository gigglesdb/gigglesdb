use super::*;
use crate::storage::range::store::{
    RangeApplyOutcome, RangeStore,
    tests::{Dir, SPLIT_TABLE, descriptors, pos, row, two_ranges},
};
use crate::storage::replicated::{AppliedPosition, ReplicateError};
use std::sync::Mutex;

/// Applies proposals to the range in log order, as the range's Raft group would.
struct Sink {
    store: Arc<RangeStore>,
    range: RangeId,
    next: Mutex<u64>,
    proposals: Mutex<usize>,
}
impl BatchSink for Sink {
    fn replicate(&self, encoded_batch: Vec<u8>) -> Result<(), ReplicateError> {
        *self.proposals.lock().unwrap() += 1;
        let mut next = self.next.lock().unwrap();
        *next += 1;
        let outcome = self
            .store
            .apply(
                self.range,
                &encoded_batch,
                AppliedPosition {
                    index: *next,
                    term: 1,
                },
            )
            .map_err(|e| ReplicateError::Unavailable(format!("{e:?}")))?;
        assert_eq!(outcome, RangeApplyOutcome::Applied);
        Ok(())
    }
}

fn range_kv(dir: &Dir, which: usize) -> (Arc<RangeStore>, Arc<Sink>, RangeKv) {
    let store = Arc::new(two_ranges(dir));
    let descriptor = descriptors()[which].clone();
    let sink = Arc::new(Sink {
        store: store.clone(),
        range: descriptor.id,
        next: Mutex::new(0),
        proposals: Mutex::new(0),
    });
    let kv = RangeKv::new(store.kv().clone(), descriptor, sink.clone());
    (store, sink, kv)
}

#[test]
fn in_range_writes_reach_the_sink_and_out_of_range_writes_are_never_proposed() {
    let dir = Dir::new("range-kv");
    let (store, sink, kv) = range_kv(&dir, 1);
    let mut batch = WriteBatch::default();
    batch.put(Keyspace::Rows, row(SPLIT_TABLE, 1, 1), b"v".to_vec());
    kv.write(batch, Durability::Sync).unwrap();
    assert_eq!(*sink.proposals.lock().unwrap(), 1);
    assert_eq!(
        kv.get(Keyspace::Rows, &row(SPLIT_TABLE, 1, 1)).unwrap(),
        Some(b"v".to_vec())
    );
    assert_eq!(store.applied(RangeId(2)).unwrap(), Some(pos(1, 1)));
    for (ks, key) in [
        (Keyspace::Rows, row(SPLIT_TABLE - 1, 1, 1)),
        (Keyspace::Catalog, row(1, 1, 1)),
        (Keyspace::Meta, b"subversion".to_vec()),
        (Keyspace::Meta, b"raft_applied".to_vec()),
    ] {
        let mut batch = WriteBatch::default();
        batch.put(Keyspace::Rows, row(SPLIT_TABLE, 2, 1), b"in".to_vec());
        batch.put(ks, key, vec![0; 8]);
        assert!(matches!(
            kv.write(batch, Durability::Sync),
            Err(KvError::Corrupt(_))
        ));
    }
    assert_eq!(*sink.proposals.lock().unwrap(), 1);
    assert_eq!(
        kv.get(Keyspace::Rows, &row(SPLIT_TABLE, 2, 1)).unwrap(),
        None
    );
}

#[test]
fn sequencer_meta_keys_are_range_local() {
    let dir = Dir::new("range-kv");
    let (store, _sink, kv) = range_kv(&dir, 0);
    let node_ts = store.kv().get(Keyspace::Meta, b"last_commit_ts").unwrap();
    let mut batch = WriteBatch::default();
    batch.put(
        Keyspace::Meta,
        b"last_commit_ts".to_vec(),
        77u64.to_be_bytes().to_vec(),
    );
    batch.put(
        Keyspace::Meta,
        b"features".to_vec(),
        1u64.to_be_bytes().to_vec(),
    );
    batch.put(Keyspace::Seq, b"counter".to_vec(), b"1".to_vec());
    kv.write(batch, Durability::Sync).unwrap();
    assert_eq!(
        kv.get(Keyspace::Meta, b"last_commit_ts").unwrap(),
        Some(77u64.to_be_bytes().to_vec())
    );
    assert_eq!(
        kv.snapshot()
            .unwrap()
            .get(Keyspace::Meta, b"features")
            .unwrap(),
        Some(1u64.to_be_bytes().to_vec())
    );
    assert_eq!(
        store.kv().get(Keyspace::Meta, b"last_commit_ts").unwrap(),
        node_ts
    );
    let local = range_local_key(RangeId(1), &RangeLocalKey::LastCommitTs);
    assert_eq!(
        store.kv().get(Keyspace::Meta, &local).unwrap(),
        Some(77u64.to_be_bytes().to_vec())
    );
    // Range 2 sees its own (absent) value.
    let other = RangeKv::new(store.kv().clone(), descriptors()[1].clone(), _sink.clone());
    assert_eq!(other.get(Keyspace::Meta, b"last_commit_ts").unwrap(), None);
}

#[test]
fn system_range_marks_only_the_committed_wide_catalog() {
    use crate::relational::{ColumnDef, DataType, TableDef, secondary_index::Descriptor};
    use crate::storage::engine::{catalog, codec, hlc::Ts, indexes, mvcc};
    let dir = Dir::new("range-wide-catalog");
    let (store, _sink, kv) = range_kv(&dir, 0);
    let feature_word = |read: &dyn KvRead| {
        let bytes = read.get(Keyspace::Meta, b"features").unwrap().unwrap();
        u64::from_be_bytes(bytes.try_into().unwrap())
    };
    let mut initial = WriteBatch::default();
    let table = catalog::TableEntry {
        def: TableDef {
            id: 1,
            name: "wide".into(),
            columns: vec![ColumnDef {
                id: 2,
                name: "id".into(),
                data_type: DataType::Integer,
                not_null: true,
            }],
            primary_key: vec![0],
        },
        heap: false,
        columns_ext: Default::default(),
        primary_index_birth: None,
    };
    let put = |batch: &mut WriteBatch, key: Vec<u8>, value: Vec<u8>, ts| {
        batch.put(
            Keyspace::Catalog,
            codec::user_key_seek_at(&key, Ts(ts)),
            mvcc::encode_live(&value),
        );
    };
    let add_index = |batch: &mut WriteBatch, id, ts| {
        let desc = Descriptor {
            id,
            table: 1,
            name: format!("wc_{id}"),
            columns: vec![2],
            key_encoding: 1,
            unique: false,
        };
        put(
            batch,
            indexes::descriptor_key(id),
            indexes::encode_descriptor(&desc).unwrap(),
            ts,
        );
        put(
            batch,
            catalog::name_key(&desc.name),
            id.to_be_bytes().to_vec(),
            ts,
        );
    };
    put(
        &mut initial,
        catalog::table_key(1),
        catalog::encode_entry(&table).unwrap(),
        1,
    );
    put(
        &mut initial,
        catalog::name_key(&table.def.name),
        1u64.to_be_bytes().to_vec(),
        1,
    );
    for id in 3..=10 {
        add_index(&mut initial, id, 1);
    }
    meta::put_features(&mut initial, meta::SECONDARY_INDEXES).unwrap();
    kv.write(initial, Durability::Sync).unwrap();
    assert_eq!(feature_word(&kv), meta::SECONDARY_INDEXES);

    let mut finalization = WriteBatch::default();
    add_index(&mut finalization, 11, 2);
    kv.write(finalization, Durability::Sync).unwrap();
    let wide = meta::SECONDARY_INDEXES | meta::WIDE_CATALOG;
    assert_eq!(feature_word(&kv), wide);
    assert_ne!(feature_word(store.kv().as_ref()) & meta::WIDE_CATALOG, 0);

    let mut stale_features = WriteBatch::default();
    let key = range_local_key(SYSTEM_RANGE, &RangeLocalKey::Features);
    stale_features.put(
        Keyspace::Meta,
        key,
        meta::SECONDARY_INDEXES.to_be_bytes().to_vec(),
    );
    store
        .apply(SYSTEM_RANGE, &stale_features.encode(), pos(3, 1))
        .unwrap();
    assert_eq!(feature_word(&kv), wide);
}
