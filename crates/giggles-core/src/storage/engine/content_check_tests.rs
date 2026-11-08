use super::*;
use crate::{
    relational::{
        ColumnDef, DataType, TableDef,
        catalog_ext::{ColumnExt, Constraint, ConstraintKind, SequenceDef},
    },
    storage::engine::{
        catalog::TableEntry,
        hlc::Ts,
        kv::{Durability, KvBackend, WriteBatch},
        kv_mem::MemKv,
        meta,
    },
};

fn put(kv: &MemKv, space: Keyspace, key: Vec<u8>, value: Vec<u8>) {
    let mut batch = WriteBatch::default();
    batch.put(space, key, value);
    kv.write(batch, Durability::Sync).unwrap();
}
fn catalog(kv: &MemKv, key: Vec<u8>, ts: u64, value: Option<Vec<u8>>) {
    put(
        kv,
        Keyspace::Catalog,
        codec::user_key_seek_at(&key, Ts(ts)),
        value.map_or_else(mvcc::encode_tombstone, |v| mvcc::encode_live(&v)),
    );
}
fn table(ext: bool, typmod: bool) -> Vec<u8> {
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
        columns_ext: Default::default(),
    };
    let mut extensions = BTreeMap::new();
    if ext {
        extensions.insert(
            2,
            ColumnExt {
                typmod: typmod.then_some(65540),
                default: Some("1".into()),
                identity: None,
                text_kind: None,
            },
        );
    }
    catalog_codec::encode_table(&entry, &extensions).unwrap()
}
fn sequence(incarnation: u128) -> SequenceDef {
    SequenceDef {
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
        incarnation,
    }
}
fn counter(kv: &MemKv, incarnation: u128) {
    put(
        kv,
        Keyspace::Seq,
        catalog_codec::counter_key(3, incarnation),
        crate::storage::encode(&(8i64, true, 8i64, 1u64)).unwrap(),
    );
}
fn corrupt_check(kv: &MemKv, bits: u64) {
    assert!(matches!(check(kv, bits), Err(FormatError::Corrupt(_))));
}

#[test]
fn extensions_and_typmods_require_their_feature_bits() {
    for typmod in [false, true] {
        let kv = MemKv::new();
        catalog(
            &kv,
            catalog_codec::table_key(1),
            1,
            Some(table(true, typmod)),
        );
        corrupt_check(&kv, 0);
        if typmod {
            corrupt_check(&kv, 64);
        }
        check(&kv, 192).unwrap();
    }
}

#[test]
fn constraints_and_sequences_require_catalog_ext() {
    let kv = MemKv::new();
    catalog(
        &kv,
        catalog_codec::table_key(1),
        1,
        Some(table(false, false)),
    );
    let constraint = Constraint {
        id: 4,
        name: "ck".into(),
        kind: ConstraintKind::Check {
            expr: "c>0".into(),
            not_valid: false,
        },
    };
    catalog(
        &kv,
        catalog_codec::constraint_key(1, 4),
        1,
        Some(catalog_codec::encode_constraint(&constraint, "t").unwrap()),
    );
    corrupt_check(&kv, 0);
    check(&kv, 64).unwrap();
    let kv = MemKv::new();
    catalog(
        &kv,
        catalog_codec::sequence_key(3),
        1,
        Some(catalog_codec::encode_sequence(&sequence(7)).unwrap()),
    );
    corrupt_check(&kv, 0);
    check(&kv, 64).unwrap();
}

#[test]
fn counter_requires_bit_and_live_matching_sequence() {
    let kv = MemKv::new();
    counter(&kv, 7);
    corrupt_check(&kv, 320);
    catalog(
        &kv,
        catalog_codec::sequence_key(3),
        1,
        Some(catalog_codec::encode_sequence(&sequence(8)).unwrap()),
    );
    corrupt_check(&kv, 320);
    catalog(
        &kv,
        catalog_codec::sequence_key(3),
        2,
        Some(catalog_codec::encode_sequence(&sequence(7)).unwrap()),
    );
    corrupt_check(&kv, 64);
    check(&kv, 320).unwrap();
    catalog(&kv, catalog_codec::sequence_key(3), 3, None);
    corrupt_check(&kv, 320);
}

#[test]
fn tombstones_and_latest_versions_determine_witnesses() {
    let kv = MemKv::new();
    catalog(&kv, catalog_codec::constraint_key(1, 4), 1, Some(vec![255]));
    catalog(&kv, catalog_codec::constraint_key(1, 4), 2, None);
    catalog(&kv, catalog_codec::table_key(1), 1, Some(table(true, true)));
    catalog(
        &kv,
        catalog_codec::table_key(1),
        2,
        Some(table(false, false)),
    );
    check(&kv, 0).unwrap();
    check(&kv, 448).unwrap();
    check(&MemKv::new(), 448).unwrap();
}

#[test]
fn malformed_catalog_and_counter_payloads_fail_closed() {
    for (key, value) in [
        (vec![9], None),
        (vec![1], None),
        (catalog_codec::table_key(1), Some(vec![255])),
        (catalog_codec::name_key("t"), Some(vec![0; 7])),
        (catalog_codec::type_key(3), Some(vec![1, 0, 0, 0, 0])),
        (catalog_codec::sequence_key(3), Some(vec![255])),
    ] {
        let kv = MemKv::new();
        catalog(&kv, key, 1, value);
        corrupt_check(&kv, 448);
    }
    let kv = MemKv::new();
    put(&kv, Keyspace::Seq, vec![2; 24], vec![0; 25]);
    corrupt_check(&kv, 320);
    let kv = MemKv::new();
    catalog(
        &kv,
        catalog_codec::sequence_key(3),
        1,
        Some(catalog_codec::encode_sequence(&sequence(7)).unwrap()),
    );
    put(
        &kv,
        Keyspace::Seq,
        catalog_codec::counter_key(3, 7),
        vec![0; 24],
    );
    corrupt_check(&kv, 320);
}

#[test]
fn open_checks_runs_meta_gate_and_preserves_role_fixture() {
    let kv = MemKv::new();
    assert!(meta::open_checks(&kv).is_err());
    for (key, value) in [
        (b"family".as_slice(), 2u32.to_be_bytes().to_vec()),
        (b"subversion", 1u32.to_be_bytes().to_vec()),
        (b"domain", vec![0; 16]),
        (b"engine", b"rocksdb".to_vec()),
        (b"features", 512u64.to_be_bytes().to_vec()),
    ] {
        put(&kv, Keyspace::Meta, key.to_vec(), value);
    }
    let mut role = vec![7];
    role.extend(1u64.to_be_bytes());
    catalog(&kv, role, 1, Some(b"role".to_vec()));
    assert_eq!(meta::open_checks(&kv).unwrap().features, 512);
    catalog(
        &kv,
        catalog_codec::sequence_key(3),
        1,
        Some(catalog_codec::encode_sequence(&sequence(7)).unwrap()),
    );
    assert!(matches!(
        meta::open_checks(&kv),
        Err(FormatError::Corrupt(_))
    ));
}
