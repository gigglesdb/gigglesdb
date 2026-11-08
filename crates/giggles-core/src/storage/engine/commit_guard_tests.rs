use super::*;
use crate::{
    relational::{ColumnDef, DataType, TableDef, catalog_ext::ColumnExt},
    storage::engine::{
        catalog::TableEntry,
        catalog_codec,
        kv::{Direction, KvBackend},
        kv_mem::MemKv,
        meta,
    },
};
use std::ops::Bound;

fn fixture() -> (Arc<MemKv>, Arc<Sequencer>) {
    let mem = Arc::new(MemKv::new());
    let mut batch = WriteBatch::default();
    for (key, value) in [
        (b"family".as_slice(), 2u32.to_be_bytes().to_vec()),
        (b"subversion", 1u32.to_be_bytes().to_vec()),
        (b"features", 0u64.to_be_bytes().to_vec()),
        (b"domain", vec![0; 16]),
        (b"engine", b"rocksdb".to_vec()),
    ] {
        batch.put(Keyspace::Meta, key.to_vec(), value);
    }
    mem.write(batch, Durability::Sync).unwrap();
    let seq = reopen(mem.clone());
    (mem, seq)
}
fn reopen(mem: Arc<MemKv>) -> Arc<Sequencer> {
    Sequencer::open(
        Arc::new(Kv::new(mem)),
        Arc::new(ManualClock::new(10)),
        SequencerConfig::default(),
    )
    .unwrap()
}
fn request(bits: u64) -> CommitRequest {
    CommitRequest {
        snapshot: Ts(0),
        writes: BTreeMap::new(),
        features_or: bits,
        raw: Vec::new(),
        validation: None,
    }
}
fn table(typmod: bool) -> Vec<u8> {
    let entry = TableEntry {
        def: TableDef {
            id: 1,
            name: "t".into(),
            columns: vec![ColumnDef {
                id: 2,
                name: "c".into(),
                data_type: DataType::Numeric,
                not_null: true,
            }],
            primary_key: vec![0],
        },
        heap: false,
        columns_ext: BTreeMap::new(),
    };
    let ext = BTreeMap::from([(
        2,
        ColumnExt {
            typmod: typmod.then_some(65540),
            default: Some("1".into()),
            identity: None,
            text_kind: None,
        },
    )]);
    catalog_codec::encode_table(&entry, &ext).unwrap()
}
fn dump(kv: &dyn KvRead) -> Vec<(Keyspace, Vec<u8>, Vec<u8>)> {
    Keyspace::ALL
        .into_iter()
        .flat_map(|space| {
            kv.range(
                space,
                Bound::Unbounded,
                Bound::Unbounded,
                Direction::Forward,
            )
            .unwrap()
            .map(move |item| {
                let (key, value) = item.unwrap();
                (space, key, value)
            })
        })
        .collect()
}
fn raw_witness(mem: &MemKv) -> Vec<u8> {
    let key = catalog_codec::counter_key(99, 7);
    let mut batch = WriteBatch::default();
    // This raw-operation witness is independent of catalog contents; the mixed
    // group test checks byte-for-byte recovery rather than open_checks.
    batch.put(Keyspace::Seq, key.clone(), vec![42]);
    mem.write(batch, Durability::Sync).unwrap();
    key
}

#[test]
fn refused_catalog_requests_contribute_nothing_even_after_crash() {
    for (key, value, bits) in [
        (catalog_codec::constraint_key(1, 4), vec![0], 0),
        (catalog_codec::sequence_key(1), vec![0], 0),
        (catalog_codec::type_key(1), vec![0], 0),
        (catalog_codec::table_key(1), table(false), 0),
        (catalog_codec::table_key(1), table(true), 64),
        (catalog_codec::table_key(1), vec![255], 64),
        // A role-bootstrap marker (kind 7) needs bit 9.
        ([&[7u8][..], &1u64.to_be_bytes()].concat(), vec![1], 64),
    ] {
        let (mem, seq) = fixture();
        let counter = catalog_codec::counter_key(99, 7);
        let before = dump(mem.as_ref());
        let mut refused = request(bits | 32);
        refused.writes.insert((Keyspace::Catalog, key), Some(value));
        refused
            .writes
            .insert((Keyspace::Rows, b"refused".to_vec()), Some(vec![1]));
        refused.raw.push(RawOp::DeleteCounter {
            key: counter.clone(),
        });
        assert_eq!(seq.submit(refused), Err(TxnError::Misuse));
        assert_eq!(seq.visible_ts(), Ts(0));
        assert_eq!(dump(mem.as_ref()), before);
        let durable = Arc::new(mem.crash());
        assert_eq!(dump(durable.as_ref()), before);
        let recovered = reopen(durable.clone());
        assert_eq!(recovered.visible_ts(), Ts(0));
        assert_eq!(meta::open_checks(durable.as_ref()).unwrap().features, 0);
    }
}

#[test]
fn mixed_group_uses_only_accepted_features_raw_ops_and_timestamps() {
    let (mem, seq) = fixture();
    let refused_counter = raw_witness(&mem);
    let accepted_counter = catalog_codec::counter_key(98, 7);
    let mut batch = WriteBatch::default();
    batch.put(Keyspace::Seq, accepted_counter.clone(), vec![43]);
    mem.write(batch, Durability::Sync).unwrap();
    let mut refused = request(32);
    refused.writes.insert(
        (Keyspace::Catalog, catalog_codec::table_key(9)),
        Some(table(false)),
    );
    refused
        .writes
        .insert((Keyspace::Rows, b"refused".to_vec()), Some(vec![1]));
    refused.raw.push(RawOp::DeleteCounter {
        key: refused_counter.clone(),
    });
    let mut first = request(64);
    first.writes.insert(
        (Keyspace::Catalog, catalog_codec::table_key(1)),
        Some(table(false)),
    );
    first.raw.push(RawOp::DeleteCounter {
        key: accepted_counter.clone(),
    });
    let mut second = request(128);
    second.writes.insert(
        (Keyspace::Catalog, catalog_codec::table_key(1)),
        Some(table(true)),
    );
    let results = seq.process_group_for_test(
        vec![refused, first, second],
        &ManualClock::new(10),
        &mut RecentLog::new(1024),
    );
    assert_eq!(results[0], Err(TxnError::Misuse));
    let first_ts = results[1].as_ref().unwrap();
    let last_ts = results[2].as_ref().unwrap();
    assert_eq!(first_ts.0, 10 << 16);
    assert_eq!(last_ts.0, first_ts.0 + 1);
    assert_eq!(load_features(seq.kv()).unwrap(), 192);
    assert_eq!(seq.visible_ts(), *last_ts);
    assert_eq!(
        mem.get(Keyspace::Meta, b"last_commit_ts").unwrap(),
        Some(last_ts.0.to_be_bytes().to_vec())
    );
    assert!(mem.get(Keyspace::Seq, &refused_counter).unwrap().is_some());
    assert!(mem.get(Keyspace::Seq, &accepted_counter).unwrap().is_none());
    assert!(
        !dump(mem.as_ref())
            .iter()
            .any(|(space, _, _)| *space == Keyspace::Rows)
    );
    let durable = Arc::new(mem.crash());
    assert_eq!(dump(durable.as_ref()), dump(mem.as_ref()));
    assert_eq!(reopen(durable.clone()).visible_ts(), *last_ts);
    assert!(
        crate::storage::engine::mvcc::latest_version(
            durable.as_ref(),
            Keyspace::Catalog,
            &catalog_codec::table_key(9)
        )
        .unwrap()
        .is_none()
    );
}

#[test]
fn correct_bits_and_catalog_tombstones_are_accepted() {
    for typmod in [false, true] {
        let (mem, seq) = fixture();
        let mut valid = request(if typmod { 192 } else { 64 });
        valid.writes.insert(
            (Keyspace::Catalog, catalog_codec::table_key(1)),
            Some(table(typmod)),
        );
        seq.submit(valid).unwrap();
        meta::open_checks(&mem.crash()).unwrap();
    }
    let (mem, seq) = fixture();
    let mut tombstone = request(0);
    tombstone.writes.insert(
        (Keyspace::Catalog, catalog_codec::constraint_key(1, 4)),
        None,
    );
    seq.submit(tombstone).unwrap();
    assert_eq!(meta::open_checks(&mem.crash()).unwrap().features, 0);
}

#[test]
fn counters_only_through_write_counter_which_always_sets_bit_8() {
    // DD-0068 X1 condition 1(a): a versioned request write into an unversioned
    // keyspace (a counter put that would bypass bit 8) is refused untouched.
    for space in [Keyspace::Seq, Keyspace::Meta, Keyspace::Gc] {
        let (mem, seq) = fixture();
        let before = dump(mem.as_ref());
        let mut bypass = request(64);
        bypass
            .writes
            .insert((space, catalog_codec::counter_key(1, 7)), Some(vec![0; 25]));
        assert_eq!(seq.submit(bypass), Err(TxnError::Misuse));
        assert_eq!(dump(mem.as_ref()), before);
    }
    // 1(b): the only counter writer ORs bit 8 into Meta in the same Sync batch,
    // even when the caller passes no bits; a crash right after keeps both or neither.
    let (mem, seq) = fixture();
    seq.submit({
        let mut def = request(64);
        def.writes.insert(
            (Keyspace::Catalog, catalog_codec::sequence_key(1)),
            Some(
                catalog_codec::encode_sequence(&crate::relational::catalog_ext::SequenceDef {
                    name: "s".into(),
                    data_type: DataType::BigInt,
                    start: 1,
                    increment: 1,
                    min: 1,
                    max: 99,
                    cache: 1,
                    cycle: false,
                    owned_by: None,
                    identity: false,
                    incarnation: 7,
                })
                .unwrap(),
            ),
        );
        def
    })
    .unwrap();
    let key = catalog_codec::counter_key(1, 7);
    let value = crate::storage::encode(&(1i64, false, 1i64, 1u64)).unwrap();
    seq.write_counter(key.clone(), value, 0).unwrap();
    let durable = mem.crash();
    assert!(durable.get(Keyspace::Seq, &key).unwrap().is_some());
    assert_eq!(meta::open_checks(&durable).unwrap().features & 256, 256);
}
