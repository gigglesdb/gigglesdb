use super::super::{
    catalog_codec,
    commit::{ManualClock, SequencerConfig},
    content_check,
    kv::{Keyspace, KvRead},
    kv_mem::MemKv,
    locks::{LockConfig, TxnId},
};
use super::*;
use crate::relational::{
    DataType,
    catalog_ext::{CatalogExt, ColumnExt, Constraint, ConstraintKind, Identity, SequenceDef},
};
use std::{ops::Range, time::Duration};

#[path = "catalog_sql_view_tests.rs"]
mod sql_view_tests;
#[path = "catalog_comment_tests.rs"]
mod comment_tests;
#[path = "catalog_comment_ddl_tests.rs"]
mod comment_ddl_tests;

fn setup() -> (Arc<MemKv>, Arc<Sequencer>, Arc<LockManager>, Catalog) {
    let mem = Arc::new(MemKv::new());
    let kv = Arc::new(Kv::new(mem.clone()));
    let seq = Sequencer::open(
        kv.clone(),
        Arc::new(ManualClock::new(10)),
        SequencerConfig {
            group_max: 0,
            group_wait: Duration::ZERO,
            ..SequencerConfig::default()
        },
    )
    .unwrap();
    let locks = Arc::new(LockManager::new(LockConfig::default()));
    let catalog = Catalog::open(
        seq.clone(),
        locks.clone(),
        Arc::new(SequenceAllocator::new(kv, 64)),
    )
    .unwrap();
    (mem, seq, locks, catalog)
}
fn txn(seq: &Arc<Sequencer>, locks: &Arc<LockManager>, id: u64) -> Transaction {
    Transaction::begin(seq, locks, TxnId(id), Isolation::ReadCommitted)
}
fn sequence_ddl_txn(seq: &Arc<Sequencer>, locks: &Arc<LockManager>, id: u64) -> Transaction {
    let txn = txn(seq, locks, id);
    txn.acquire_lock(
        LockKey::Table(0),
        LockMode::AccessExclusive,
        Some(Instant::now() + Duration::from_secs(5)),
        &|| false,
    )
    .unwrap();
    txn
}
fn empty() -> CatalogParts {
    CatalogParts {
        tables: BTreeMap::new(),
        heap: BTreeSet::new(),
        indexes: Vec::new(),
        ext: CatalogExt::default(),
        sql_views: BTreeMap::new(), comments: BTreeMap::new(), column_labels: BTreeMap::new(),
    }
}
fn serial_parts(ids: &Range<u64>) -> CatalogParts {
    let t = ids.start;
    let c = t + 1;
    let s = t + 2;
    let mut parts = empty();
    parts.tables.insert(
        "serial_table".into(),
        TableDef {
            id: t,
            name: "serial_table".into(),
            columns: vec![ColumnDef {
                id: c,
                name: "id".into(),
                data_type: DataType::BigInt,
                not_null: true,
            }],
            primary_key: vec![0],
        },
    );
    parts.ext.columns.insert(
        c,
        ColumnExt {
            identity: Some(Identity {
                always: false,
                sequence_id: s,
            }),
            ..ColumnExt::default()
        },
    );
    parts.ext.sequences.insert(
        s,
        SequenceDef {
            name: "serial_table_id_seq".into(),
            data_type: DataType::BigInt,
            start: 1,
            increment: 1,
            min: 1,
            max: i64::MAX,
            cache: 1,
            cycle: false,
            owned_by: Some((t, c)),
            identity: true,
            incarnation: 17,
        },
    );
    parts.ext.constraints.insert(
        t,
        vec![
            Constraint {
                id: t + 3,
                name: "serial_table_pkey".into(),
                kind: ConstraintKind::PrimaryKey,
            },
            Constraint {
                id: t + 4,
                name: "positive_id".into(),
                kind: ConstraintKind::Check {
                    expr: "id > 0".into(),
                    not_valid: false,
                },
            },
        ],
    );
    parts
}
fn parts(catalog: &Catalog, seq: &Sequencer) -> CatalogParts {
    catalog.view_at(seq.visible_ts()).unwrap().parts()
}
fn features(kv: &dyn KvRead) -> u64 {
    kv.get(Keyspace::Meta, b"features")
        .unwrap()
        .map_or(0, |value| u64::from_be_bytes(value.try_into().unwrap()))
}
fn assert_parts(actual: &CatalogParts, expected: &CatalogParts) {
    assert_eq!(actual.tables, expected.tables);
    assert_eq!(actual.heap, expected.heap);
    assert_eq!(actual.ext, expected.ext);
}

#[test]
fn diff_roundtrips_owned_sequence_identity_and_check_after_crash() {
    let (mem, seq, locks, catalog) = setup();
    let ids = catalog.reserve_ids(7).unwrap();
    let mut wanted = serial_parts(&ids);
    wanted.tables.insert(
        "heap_table".into(),
        TableDef {
            id: ids.start + 5,
            name: "heap_table".into(),
            columns: vec![ColumnDef {
                id: ids.start + 6,
                name: "value".into(),
                data_type: DataType::Text,
                not_null: false,
            }],
            primary_key: vec![],
        },
    );
    wanted.heap.insert(ids.start + 5);
    let mut tx = sequence_ddl_txn(&seq, &locks, 1);
    catalog
        .apply_diff(&mut tx, &empty(), &wanted, &ids, None, &|| false)
        .unwrap();
    catalog.commit_ddl(tx).unwrap();
    assert_parts(&parts(&catalog, &seq), &wanted);
    let mut reader = txn(&seq, &locks, 2);
    assert_parts(&catalog.parts(&mut reader).unwrap(), &wanted);
    reader.rollback();
    let kv = Arc::new(Kv::new(Arc::new(mem.crash())));
    let reopened_seq = Sequencer::open(
        kv.clone(),
        Arc::new(ManualClock::new(1)),
        SequencerConfig::default(),
    )
    .unwrap();
    let reopened = Catalog::open(
        reopened_seq.clone(),
        locks,
        Arc::new(SequenceAllocator::new(kv.clone(), 64)),
    )
    .unwrap();
    assert_parts(&parts(&reopened, &reopened_seq), &wanted);
    assert_eq!(features(kv.as_ref()), 1 << 6);
    content_check::check(kv.as_ref(), features(kv.as_ref())).unwrap();
    assert!(content_check::check(kv.as_ref(), 0).is_err());
}

#[test]
fn diff_sets_exact_features_for_plain_extended_and_typmod_content() {
    for (mode, expected) in [(0, 0), (1, 1 << 6), (2, (1 << 6) | (1 << 7))] {
        let (mem, seq, locks, catalog) = setup();
        let ids = catalog.reserve_ids(2).unwrap();
        let mut wanted = serial_parts(&ids);
        wanted.ext = CatalogExt::default();
        if mode != 0 {
            wanted.ext.columns.insert(
                ids.start + 1,
                ColumnExt {
                    default: Some("1".into()),
                    typmod: (mode == 2).then_some(65540),
                    identity: None,
                    text_kind: None,
                    type_id: None,
                },
            );
        }
        if mode == 2 {
            wanted.tables.get_mut("serial_table").unwrap().columns[0].data_type = DataType::Numeric;
        }
        let mut tx = txn(&seq, &locks, 1);
        catalog
            .apply_diff(&mut tx, &empty(), &wanted, &ids, None, &|| false)
            .unwrap();
        catalog.commit_ddl(tx).unwrap();
        let crashed = Kv::new(Arc::new(mem.crash()));
        assert_eq!(features(&crashed), expected);
        content_check::check(&crashed, expected).unwrap();
        if mode != 0 {
            assert!(content_check::check(&crashed, expected & !(1 << 6)).is_err());
        }
        if mode == 2 {
            assert!(content_check::check(&crashed, 1 << 6).is_err());
        }
    }
}

#[test]
fn diff_concurrent_same_name_serializes_and_rejects_stale_creator() {
    use std::{sync::Barrier, thread};
    let (_, seq, locks, catalog) = setup();
    let catalog = Arc::new(catalog);
    let barrier = Arc::new(Barrier::new(2));
    let reservations = [
        catalog.reserve_ids(5).unwrap(),
        catalog.reserve_ids(5).unwrap(),
    ];
    let workers: Vec<_> = reservations
        .into_iter()
        .enumerate()
        .map(|(index, ids)| {
            let (catalog, seq, locks, barrier) =
                (catalog.clone(), seq.clone(), locks.clone(), barrier.clone());
            thread::spawn(move || {
                // Both transactions begin before the barrier so the loser keeps a
                // stale snapshot; Table(0) is taken only after it, as SQL DDL does.
                let mut tx = txn(&seq, &locks, index as u64 + 1);
                let wanted = serial_parts(&ids);
                barrier.wait();
                tx.acquire_lock(
                    LockKey::Table(0),
                    LockMode::AccessExclusive,
                    Some(Instant::now() + Duration::from_secs(5)),
                    &|| false,
                )
                .unwrap();
                match catalog.apply_diff(
                    &mut tx,
                    &empty(),
                    &wanted,
                    &ids,
                    Some(Instant::now() + Duration::from_secs(5)),
                    &|| false,
                ) {
                    Ok(()) => {
                        catalog.commit_ddl(tx).unwrap();
                        true
                    }
                    Err(CoreError::Sql(SqlError::Conflict)) => {
                        assert!(!tx.writes_keyspace(Keyspace::Catalog));
                        tx.rollback();
                        false
                    }
                    Err(error) => panic!("unexpected same-name result: {error:?}"),
                }
            })
        })
        .collect();
    assert_eq!(
        workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .filter(|won| *won)
            .count(),
        1
    );
    assert_eq!(parts(&catalog, &seq).tables.len(), 1);
    assert_eq!(parts(&catalog, &seq).ext.sequences.len(), 1);
}

#[test]
fn diff_rejects_each_new_object_id_outside_reservation() {
    let (_, seq, locks, catalog) = setup();
    let ids = catalog.reserve_ids(5).unwrap();
    let before = serial_parts(&ids);
    let mut tx = sequence_ddl_txn(&seq, &locks, 1);
    catalog
        .apply_diff(&mut tx, &empty(), &before, &ids, None, &|| false)
        .unwrap();
    catalog.commit_ddl(tx).unwrap();
    let outside = ids.end + 100;
    for kind in [1, 2, 4, 5] {
        let mut after = before.clone();
        match kind {
            1 => {
                after.tables.insert(
                    "new_table".into(),
                    TableDef {
                        id: outside,
                        name: "new_table".into(),
                        columns: vec![ColumnDef {
                            id: outside + 1,
                            name: "c".into(),
                            data_type: DataType::Text,
                            not_null: false,
                        }],
                        primary_key: vec![],
                    },
                );
            }
            2 => after
                .tables
                .get_mut("serial_table")
                .unwrap()
                .columns
                .push(ColumnDef {
                    id: outside,
                    name: "new_column".into(),
                    data_type: DataType::Text,
                    not_null: false,
                }),
            4 => after
                .ext
                .constraints
                .get_mut(&ids.start)
                .unwrap()
                .push(Constraint {
                    id: outside,
                    name: "new_check".into(),
                    kind: ConstraintKind::Check {
                        expr: "true".into(),
                        not_valid: false,
                    },
                }),
            5 => {
                let mut def = before.ext.sequences[&(ids.start + 2)].clone();
                def.name = "new_sequence".into();
                def.owned_by = None;
                def.identity = false;
                after.ext.sequences.insert(outside, def);
            }
            _ => unreachable!(),
        }
        let mut tx = txn(&seq, &locks, 2);
        assert!(
            matches!(
                catalog.apply_diff(&mut tx, &before, &after, &ids, None, &|| false),
                Err(CoreError::Corrupt(_))
            ),
            "kind {kind}"
        );
        assert!(!tx.writes_keyspace(Keyspace::Catalog));
        assert!(!tx.writes_keyspace(Keyspace::Seq));
        assert_eq!(catalog.commit_ddl(tx).unwrap(), seq.visible_ts());
    }
}

#[test]
fn diff_rename_preserves_explicit_primary_constraint_name() {
    let (mem, seq, locks, catalog) = setup();
    let ids = catalog.reserve_ids(5).unwrap();
    let before = serial_parts(&ids);
    let mut tx = sequence_ddl_txn(&seq, &locks, 1);
    catalog
        .apply_diff(&mut tx, &empty(), &before, &ids, None, &|| false)
        .unwrap();
    catalog.commit_ddl(tx).unwrap();
    let mut after = before.clone();
    let mut table = after.tables.remove("serial_table").unwrap();
    table.name = "renamed".into();
    after.tables.insert("renamed".into(), table);
    let mut tx = txn(&seq, &locks, 2);
    catalog
        .apply_diff(&mut tx, &before, &after, &(0..0), None, &|| false)
        .unwrap();
    catalog.commit_ddl(tx).unwrap();
    assert_parts(&parts(&catalog, &seq), &after);
    assert_eq!(
        parts(&catalog, &seq).ext.constraints[&ids.start][0].name,
        "serial_table_pkey"
    );
    let kv = Arc::new(Kv::new(Arc::new(mem.crash())));
    let reopened_seq = Sequencer::open(
        kv.clone(),
        Arc::new(ManualClock::new(1)),
        SequencerConfig::default(),
    )
    .unwrap();
    let reopened = Catalog::open(
        reopened_seq.clone(),
        locks,
        Arc::new(SequenceAllocator::new(kv, 64)),
    )
    .unwrap();
    assert_parts(&parts(&reopened, &reopened_seq), &after);
    assert_eq!(
        parts(&reopened, &reopened_seq).ext.constraints[&ids.start][0].name,
        "serial_table_pkey"
    );
}

#[test]
fn diff_drop_owned_sequence_removes_counter_with_catalog_commit() {
    let (mem, seq, locks, catalog) = setup();
    let ids = catalog.reserve_ids(5).unwrap();
    let before = serial_parts(&ids);
    let key = catalog_codec::counter_key(ids.start + 2, 17);
    let mut tx = sequence_ddl_txn(&seq, &locks, 1);
    catalog
        .apply_diff(&mut tx, &empty(), &before, &ids, None, &|| false)
        .unwrap();
    catalog.commit_ddl(tx).unwrap();
    let def = &before.ext.sequences[&(ids.start + 2)];
    let counter = crate::storage::sequences::SeqCounter {
        incarnation: def.incarnation,
        last_value: 10,
        is_called: true,
        log_hw: 10,
        stamp: 1,
    };
    seq.write_counter(
        key.clone(),
        catalog_codec::encode_counter(&counter, def).unwrap(),
        1 << 8,
    )
    .unwrap();
    assert!(seq.kv().get(Keyspace::Seq, &key).unwrap().is_some());
    let mut tx = sequence_ddl_txn(&seq, &locks, 2);
    catalog
        .apply_diff(&mut tx, &before, &empty(), &(0..0), None, &|| false)
        .unwrap();
    // A crash before this commit must retain both definition and counter.
    let precommit = Kv::new(Arc::new(mem.crash()));
    assert!(precommit.get(Keyspace::Seq, &key).unwrap().is_some());
    assert!(
        mvcc::latest_version(
            &precommit,
            Keyspace::Catalog,
            &catalog_codec::sequence_key(ids.start + 2)
        )
        .unwrap()
        .unwrap()
        .1
        .is_some()
    );
    content_check::check(&precommit, features(&precommit)).unwrap();
    catalog.commit_ddl(tx).unwrap();
    let durable = Kv::new(Arc::new(mem.crash()));
    assert!(durable.get(Keyspace::Seq, &key).unwrap().is_none());
    assert!(
        mvcc::latest_version(
            &durable,
            Keyspace::Catalog,
            &catalog_codec::sequence_key(ids.start + 2)
        )
        .unwrap()
        .unwrap()
        .1
        .is_none()
    );
    content_check::check(&durable, features(&durable)).unwrap();
    assert_parts(&parts(&catalog, &seq), &empty());
}

#[test]
fn diff_identical_parts_buffer_no_catalog_or_counter_writes() {
    let (_, seq, locks, catalog) = setup();
    let ids = catalog.reserve_ids(5).unwrap();
    let before = serial_parts(&ids);
    let mut tx = sequence_ddl_txn(&seq, &locks, 1);
    catalog
        .apply_diff(&mut tx, &empty(), &before, &ids, None, &|| false)
        .unwrap();
    catalog.commit_ddl(tx).unwrap();
    let mut reordered = before.clone();
    reordered
        .ext
        .constraints
        .get_mut(&ids.start)
        .unwrap()
        .reverse();
    for after in [&before, &reordered] {
        let visible = seq.visible_ts();
        let mut tx = txn(&seq, &locks, 2);
        catalog
            .apply_diff(&mut tx, &before, after, &(0..0), None, &|| false)
            .unwrap();
        assert!(!tx.writes_keyspace(Keyspace::Catalog));
        assert!(!tx.writes_keyspace(Keyspace::Seq));
        assert_eq!(catalog.commit_ddl(tx).unwrap(), visible);
        assert_eq!(seq.visible_ts(), visible);
    }
}

#[test]
fn diff_rejects_ids_outside_reservation_without_buffering_writes() {
    let (_, seq, locks, catalog) = setup();
    let ids = catalog.reserve_ids(5).unwrap();
    let wanted = serial_parts(&ids);
    for invalid in [
        ids.start..ids.start,
        (ids.start + 1)..ids.end,
        ids.start..(ids.end - 1),
    ] {
        let mut tx = txn(&seq, &locks, 1);
        assert!(matches!(
            catalog.apply_diff(&mut tx, &empty(), &wanted, &invalid, None, &|| false),
            Err(CoreError::Corrupt(_))
        ));
        assert!(!tx.writes_keyspace(Keyspace::Catalog));
        assert!(!tx.writes_keyspace(Keyspace::Seq));
        tx.rollback();
    }
}

#[test]
fn diff_rejects_a_tampered_unchanged_catalog_part() {
    let (_, seq, locks, catalog) = setup();
    let ids = catalog.reserve_ids(5).unwrap();
    let before = Arc::new(serial_parts(&ids));
    let checked = relational::Relational::catalog_image(
        before.tables.clone(),
        before.ext.clone(),
        ids.end,
        1,
    )
    .unwrap();
    let mut create = sequence_ddl_txn(&seq, &locks, 1);
    catalog
        .apply_sql_diff(
            &mut create,
            &empty(),
            before.clone(),
            checked.clone(),
            &ids,
            &|| false,
        )
        .unwrap();
    let visible = catalog.commit_sql_ddl(create, None).unwrap();

    let mut orphan = before.as_ref().clone();
    orphan.ext.columns.insert(ids.end, ColumnExt::default());
    let mut collision = before.as_ref().clone();
    // The unchanged table record's column ID now collides with a sequence ID.
    let mut sequence = before.ext.sequences[&(ids.start + 2)].clone();
    sequence.name = "other_sequence".into();
    sequence.owned_by = None;
    sequence.identity = false;
    collision.ext.sequences.insert(ids.start + 1, sequence);

    for (txn_id, invalid, message) in [
        (2, orphan, "catalog extension owner absent"),
        (3, collision, "duplicate or zero catalog object id"),
    ] {
        assert_eq!(
            catalog.checked_parts.lock().unwrap().as_ref().unwrap().parts,
            before
        );
        let mut ddl = sequence_ddl_txn(&seq, &locks, txn_id);
        let skip = UnchangedRecords::between(&before, &invalid, &BTreeSet::new());
        assert!(skip.tables.contains(&ids.start));
        assert_eq!(
            catalog.apply_sql_diff(
                &mut ddl,
                &before,
                Arc::new(invalid),
                checked.clone(),
                &(0..0),
                &|| false,
            ),
            Err(corrupt(message))
        );
        assert!(!ddl.writes_keyspace(Keyspace::Catalog));
        assert_eq!(catalog.commit_sql_ddl(ddl, None).unwrap(), visible);
    }
}
