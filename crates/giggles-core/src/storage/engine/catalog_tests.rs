use super::super::{
    commit::{ManualClock, Sequencer, SequencerConfig},
    kv::{Durability, Keyspace, Kv, KvBackend, KvError, KvIter, KvRead, Op, WriteBatch},
    kv_mem::MemKv,
    locks::{LockConfig, LockKey, LockManager, LockMode, TxnId},
    seq::SequenceAllocator,
    txn::{Isolation, SerializationReason, Transaction, TxnError},
};
use super::*;
use crate::{
    CoreError,
    relational::{ColumnDef, DataType, SqlError, TableDef},
};
use std::{
    collections::BTreeSet,
    sync::{
        Arc, Barrier,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

fn catalog_setup() -> (Arc<MemKv>, Arc<Sequencer>, Arc<LockManager>, Arc<Catalog>) {
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
    let catalog = Arc::new(
        Catalog::open(
            seq.clone(),
            locks.clone(),
            Arc::new(SequenceAllocator::new(kv, 64)),
        )
        .unwrap(),
    );
    (mem, seq, locks, catalog)
}

fn transaction(
    seq: &Arc<Sequencer>,
    locks: &Arc<LockManager>,
    id: u64,
    isolation: Isolation,
) -> Transaction {
    Transaction::begin(seq, locks, TxnId(id), isolation)
}

fn column(name: &str) -> ColumnDef {
    ColumnDef {
        id: 0,
        name: name.into(),
        data_type: DataType::BigInt,
        not_null: false,
    }
}

fn table(name: &str) -> TableDef {
    let mut key = column("a");
    key.not_null = true;
    TableDef {
        id: 0,
        name: name.into(),
        columns: vec![key, column("b")],
        primary_key: vec![0],
    }
}

fn create(
    catalog: &Catalog,
    seq: &Arc<Sequencer>,
    locks: &Arc<LockManager>,
    id: u64,
    name: &str,
) -> Arc<TableEntry> {
    let mut txn = transaction(seq, locks, id, Isolation::ReadCommitted);
    let entry = catalog
        .create_table(&mut txn, table(name), false, None, &|| false)
        .unwrap();
    catalog.commit_ddl(txn).unwrap();
    entry
}

fn sequences(block: u64) -> (Arc<MemKv>, Arc<Kv>, Arc<SequenceAllocator>) {
    let mem = Arc::new(MemKv::new());
    let kv = Arc::new(Kv::new(mem.clone()));
    let seq = Arc::new(SequenceAllocator::new(kv.clone(), block));
    (mem, kv, seq)
}

#[test]
fn sequence_concurrent_blocks_are_unique_and_monotonic_per_caller() {
    let (_, _, seq) = sequences(127);
    let start = Arc::new(Barrier::new(8));
    let workers: Vec<_> = (0..8)
        .map(|_| {
            let seq = seq.clone();
            let start = start.clone();
            thread::spawn(move || {
                start.wait();
                let values: Vec<_> = (0..5_000).map(|_| seq.next(1, 7).unwrap()).collect();
                assert!(values.windows(2).all(|w| w[0] < w[1]));
                values
            })
        })
        .collect();
    let values: BTreeSet<_> = workers
        .into_iter()
        .flat_map(|worker| worker.join().unwrap())
        .collect();
    assert_eq!(values.len(), 40_000);
    assert!(values.first().copied().unwrap() > 0);
}

#[test]
fn sequence_keys_and_kinds_have_independent_counters() {
    let (_, _, seq) = sequences(4);
    assert_eq!(seq.next(1, 1).unwrap(), 1);
    assert_eq!(seq.next(1, 1).unwrap(), 2);
    assert_eq!(seq.next(1, 2).unwrap(), 1);
    assert_eq!(seq.next(2, 1).unwrap(), 1);
    seq.ensure_at_least(1, 1, 100).unwrap();
    assert!(seq.next(1, 1).unwrap() >= 100);
    assert_eq!(seq.next(1, 2).unwrap(), 2);
    assert_eq!(seq.next(2, 1).unwrap(), 2);
}

#[test]
fn sequence_crash_discards_unused_block_without_reusing_ids() {
    let (mem, _, seq) = sequences(32);
    let prior: Vec<_> = (0..3).map(|_| seq.next(1, 9).unwrap()).collect();
    let reopened = SequenceAllocator::new(Arc::new(Kv::new(Arc::new(mem.crash()))), 32);
    let next = reopened.next(1, 9).unwrap();
    assert!(next > *prior.last().unwrap());
    assert_eq!(next, 33);
}

#[test]
fn sequence_default_block_and_floor_survive_crash() {
    let (mem, _, seq) = sequences(0);
    assert_eq!(seq.next(1, 1).unwrap(), 1);
    let reopened = SequenceAllocator::new(Arc::new(Kv::new(Arc::new(mem.crash()))), 0);
    assert_eq!(reopened.next(1, 1).unwrap(), 1025);
    seq.ensure_at_least(1, 9, 10_000).unwrap();
    let reopened = SequenceAllocator::new(Arc::new(Kv::new(Arc::new(mem.crash()))), 8);
    assert!(reopened.next(1, 9).unwrap() >= 10_000);
}

#[test]
fn sequence_floor_never_rewinds_an_active_or_reopened_counter() {
    let (mem, _, seq) = sequences(8);
    assert_eq!(seq.next(1, 1).unwrap(), 1);
    seq.ensure_at_least(1, 1, 0).unwrap();
    seq.ensure_at_least(1, 1, 1).unwrap();
    assert_eq!(seq.next(1, 1).unwrap(), 2);
    seq.ensure_at_least(1, 1, 7).unwrap();
    let raised = seq.next(1, 1).unwrap();
    assert!(raised >= 7);
    seq.ensure_at_least(1, 1, 3).unwrap();
    assert!(seq.next(1, 1).unwrap() > raised);
    seq.ensure_at_least(1, 1, 500).unwrap();
    let reopened = SequenceAllocator::new(Arc::new(Kv::new(Arc::new(mem.crash()))), 8);
    assert!(reopened.next(1, 1).unwrap() >= 500);
}

#[test]
fn sequence_write_failure_fences_refills_and_cached_ids() {
    let (mem, kv, seq) = sequences(8);
    assert_eq!(seq.next(1, 1).unwrap(), 1);
    let failure = KvError::Io("sequence reservation failed".into());
    mem.fail_next_write(failure.clone());
    assert_eq!(seq.next(1, 2), Err(TxnError::Kv(failure)));
    assert!(kv.is_fenced());
    assert_eq!(seq.next(1, 1), Err(TxnError::Kv(KvError::Fenced)));
    assert_eq!(
        seq.ensure_at_least(1, 1, 100),
        Err(TxnError::Kv(KvError::Fenced))
    );
}

#[test]
fn sequence_rejects_corrupt_highwater_and_overflow() {
    for value in [vec![], vec![0; 8], vec![1; 7]] {
        let (_, kv, seq) = sequences(8);
        let mut key = vec![1];
        key.extend_from_slice(&1u64.to_be_bytes());
        let mut batch = WriteBatch::default();
        batch.put(Keyspace::Seq, key, value);
        kv.write(batch, Durability::Sync).unwrap();
        assert!(matches!(
            seq.next(1, 1),
            Err(TxnError::Kv(KvError::Corrupt(_)))
        ));
    }
    let (_, _, seq) = sequences(8);
    seq.ensure_at_least(1, 1, u64::MAX).unwrap();
    assert_eq!(seq.next(1, 1), Err(TxnError::Misuse));
}

#[test]
fn catalog_create_resolve_drop_recreate_allocates_fresh_object_and_column_ids() {
    let (_, seq, locks, catalog) = catalog_setup();
    let first = create(&catalog, &seq, &locks, 1, "t");
    assert!(!first.heap);
    let initial: BTreeSet<_> = std::iter::once(first.def.id)
        .chain(first.def.columns.iter().map(|column| column.id))
        .collect();
    assert_eq!(initial.len(), 3);
    assert!(initial.iter().all(|id| *id > 0));
    let mut reader = transaction(&seq, &locks, 2, Isolation::ReadCommitted);
    assert_eq!(
        catalog
            .resolve(&mut reader, "t", LockMode::AccessShare, None, &|| false)
            .unwrap(),
        first
    );
    reader.rollback();
    let mut ddl = transaction(&seq, &locks, 3, Isolation::ReadCommitted);
    catalog.drop_table(&mut ddl, "t", None, &|| false).unwrap();
    catalog.commit_ddl(ddl).unwrap();
    let mut reader = transaction(&seq, &locks, 4, Isolation::ReadCommitted);
    assert!(matches!(
        catalog.resolve(&mut reader, "t", LockMode::AccessShare, None, &|| false),
        Err(CatalogError::Core(CoreError::Sql(SqlError::UnknownTable)))
    ));
    reader.rollback();
    let mut ddl = transaction(&seq, &locks, 5, Isolation::ReadCommitted);
    let mut heap = table("t");
    heap.primary_key.clear();
    let recreated = catalog
        .create_table(&mut ddl, heap, true, None, &|| false)
        .unwrap();
    assert!(recreated.heap);
    assert!(
        std::iter::once(recreated.def.id)
            .chain(recreated.def.columns.iter().map(|c| c.id))
            .all(|id| id > *initial.last().unwrap())
    );
    catalog.commit_ddl(ddl).unwrap();
}

#[test]
fn catalog_repeatable_read_absence_does_not_bind_a_concurrent_create() {
    let (_, seq, locks, catalog) = catalog_setup();
    let mut old = transaction(&seq, &locks, 1, Isolation::RepeatableRead);
    assert_eq!(old.get(Keyspace::Catalog, &[0; 8]).unwrap(), None);
    let created = create(&catalog, &seq, &locks, 2, "new_table");
    assert!(matches!(
        catalog.resolve(&mut old, "new_table", LockMode::AccessShare, None, &|| {
            false
        }),
        Err(CatalogError::Core(CoreError::Sql(SqlError::UnknownTable)))
    ));
    old.rollback();
    let mut fresh = transaction(&seq, &locks, 3, Isolation::ReadCommitted);
    assert_eq!(
        catalog
            .resolve(
                &mut fresh,
                "new_table",
                LockMode::AccessShare,
                None,
                &|| false
            )
            .unwrap(),
        created
    );
    fresh.rollback();
}

#[test]
fn catalog_concurrent_same_name_create_has_one_winner_and_rollback_releases_name() {
    let (_, seq, locks, catalog) = catalog_setup();
    let start = Arc::new(Barrier::new(2));
    let workers: Vec<_> = (1..=2)
        .map(|id| {
            let (seq, locks, catalog, start) =
                (seq.clone(), locks.clone(), catalog.clone(), start.clone());
            thread::spawn(move || {
                let mut txn = transaction(&seq, &locks, id, Isolation::ReadCommitted);
                start.wait();
                match catalog.create_table(&mut txn, table("contended"), false, None, &|| false) {
                    Ok(_) => {
                        catalog.commit_ddl(txn).unwrap();
                        true
                    }
                    Err(CatalogError::Core(CoreError::Sql(SqlError::Duplicate))) => {
                        txn.rollback();
                        false
                    }
                    Err(error) => panic!("unexpected create error: {error:?}"),
                }
            })
        })
        .collect();
    assert_eq!(
        workers
            .into_iter()
            .map(|worker| usize::from(worker.join().unwrap()))
            .sum::<usize>(),
        1
    );
    let mut first = transaction(&seq, &locks, 3, Isolation::ReadCommitted);
    let abandoned = catalog
        .create_table(&mut first, table("rolled_back"), false, None, &|| false)
        .unwrap();
    let (queued_tx, queued_rx) = mpsc::channel();
    let replacement = {
        let (seq, locks, catalog) = (seq.clone(), locks.clone(), catalog.clone());
        thread::spawn(move || {
            let mut txn = transaction(&seq, &locks, 4, Isolation::ReadCommitted);
            let accepted = catalog
                .create_table(&mut txn, table("rolled_back"), false, None, &|| {
                    let _ = queued_tx.send(());
                    false
                })
                .unwrap();
            catalog.commit_ddl(txn).unwrap();
            accepted
        })
    };
    queued_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    assert_eq!(locks.waiting_count(), 1);
    first.rollback();
    let accepted = replacement.join().unwrap();
    assert!(accepted.def.id > abandoned.def.id);
}

#[test]
fn catalog_drop_waits_for_access_share_and_resumes_after_channel_release() {
    let (_, seq, locks, catalog) = catalog_setup();
    create(&catalog, &seq, &locks, 1, "held");
    let mut reader = transaction(&seq, &locks, 2, Isolation::ReadCommitted);
    catalog
        .resolve(&mut reader, "held", LockMode::AccessShare, None, &|| false)
        .unwrap();
    let mut timed_out = transaction(&seq, &locks, 3, Isolation::ReadCommitted);
    assert!(matches!(
        catalog.drop_table(&mut timed_out, "held", Some(Instant::now()), &|| false),
        Err(CatalogError::Txn(TxnError::LockTimeout))
    ));
    timed_out.rollback();
    let (queued_tx, queued_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let worker = {
        let (seq, locks, catalog) = (seq.clone(), locks.clone(), catalog.clone());
        thread::spawn(move || {
            let mut txn = transaction(&seq, &locks, 4, Isolation::ReadCommitted);
            catalog
                .drop_table(&mut txn, "held", None, &|| {
                    queued_tx.send(()).unwrap();
                    false
                })
                .unwrap();
            catalog.commit_ddl(txn).unwrap();
            done_tx.send(()).unwrap();
        })
    };
    queued_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    assert_eq!(locks.waiting_count(), 1);
    assert!(matches!(done_rx.try_recv(), Err(mpsc::TryRecvError::Empty)));
    reader.rollback();
    done_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    worker.join().unwrap();
    assert_eq!(locks.waiting_count(), 0);
}

#[test]
fn catalog_ddl_publication_precedes_unlock_and_read_committed_rebinds_after_wait() {
    let (_, seq, locks, catalog) = catalog_setup();
    let mut next_id = 1;
    for round in 0..200 {
        let name = format!("publication_{}", round / 60);
        if round % 60 == 0 {
            create(&catalog, &seq, &locks, next_id, &name);
            next_id += 1;
        }
        let mut ddl = transaction(&seq, &locks, next_id, Isolation::ReadCommitted);
        next_id += 1;
        let updated = catalog
            .add_column(&mut ddl, &name, column(&format!("c{round}")), None, &|| {
                false
            })
            .unwrap();
        let (queued_tx, queued_rx) = mpsc::channel();
        let reader = {
            let (seq, locks, catalog, name) =
                (seq.clone(), locks.clone(), catalog.clone(), name.clone());
            let id = next_id;
            next_id += 1;
            thread::spawn(move || {
                let mut txn = transaction(&seq, &locks, id, Isolation::ReadCommitted);
                let found = catalog
                    .resolve(&mut txn, &name, LockMode::AccessShare, None, &|| {
                        queued_tx.send(()).unwrap();
                        false
                    })
                    .unwrap();
                txn.rollback();
                found
            })
        };
        queued_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        catalog.commit_ddl(ddl).unwrap();
        assert_eq!(
            reader.join().unwrap(),
            updated,
            "stale definition in round {round}"
        );
    }
}

#[test]
fn catalog_reopen_from_crash_preserves_definitions_heap_and_ids() {
    let (mem, seq, locks, catalog) = catalog_setup();
    let first = create(&catalog, &seq, &locks, 1, "persisted");
    let mut ddl = transaction(&seq, &locks, 2, Isolation::ReadCommitted);
    let added = catalog
        .add_column(&mut ddl, "persisted", column("extra"), None, &|| false)
        .unwrap();
    catalog.commit_ddl(ddl).unwrap();
    let mut heap = table("persisted_heap");
    heap.primary_key.clear();
    let mut heap_txn = transaction(&seq, &locks, 3, Isolation::ReadCommitted);
    let heap_entry = catalog
        .create_table(&mut heap_txn, heap, true, None, &|| false)
        .unwrap();
    catalog.commit_ddl(heap_txn).unwrap();
    let original_view = catalog.view_at(seq.visible_ts()).unwrap();
    assert_eq!(original_view.by_name.len(), 2);
    assert_eq!(
        original_view.by_name.get("persisted_heap"),
        Some(&heap_entry)
    );
    let crash = Arc::new(mem.crash());
    let kv = Arc::new(Kv::new(crash));
    let reopened_seq = Sequencer::open(
        kv.clone(),
        Arc::new(ManualClock::new(1)),
        SequencerConfig::default(),
    )
    .unwrap();
    let reopened_locks = Arc::new(LockManager::new(LockConfig::default()));
    let reopened = Catalog::open(
        reopened_seq.clone(),
        reopened_locks.clone(),
        Arc::new(SequenceAllocator::new(kv, 64)),
    )
    .unwrap();
    assert_eq!(
        reopened.view_at(reopened_seq.visible_ts()).unwrap(),
        original_view
    );
    let mut txn = transaction(&reopened_seq, &reopened_locks, 4, Isolation::ReadCommitted);
    assert_eq!(
        reopened
            .resolve(&mut txn, "persisted", LockMode::AccessShare, None, &|| {
                false
            })
            .unwrap(),
        added
    );
    txn.rollback();
    let later = create(&reopened, &reopened_seq, &reopened_locks, 5, "later");
    assert!(later.def.id > first.def.id);
    assert!(
        later.def.id
            > added
                .def
                .columns
                .iter()
                .map(|column| column.id)
                .max()
                .unwrap()
    );
}

#[test]
fn catalog_cache_publishes_current_view_and_prunes_at_the_oldest_snapshot() {
    let (_, seq, locks, catalog) = catalog_setup();
    let alpha = create(&catalog, &seq, &locks, 1, "alpha");
    let first_ts = seq.visible_ts();
    let first_view = catalog.view_at(first_ts).unwrap();
    assert_eq!(first_view.by_name.get("alpha"), Some(&alpha));
    let mut pinned = transaction(&seq, &locks, 2, Isolation::RepeatableRead);
    pinned.get(Keyspace::Catalog, &[0; 8]).unwrap();
    let beta = create(&catalog, &seq, &locks, 3, "beta");
    let second_ts = seq.visible_ts();
    let mut ddl = transaction(&seq, &locks, 4, Isolation::ReadCommitted);
    let changed = catalog
        .add_column(&mut ddl, "alpha", column("added"), None, &|| false)
        .unwrap();
    let last_ts = catalog.commit_ddl(ddl).unwrap();
    let latest = catalog.view_at(seq.visible_ts()).unwrap();
    assert_eq!(latest.by_name.get("alpha"), Some(&changed));
    assert_eq!(latest.by_name.get("beta"), Some(&beta));
    assert_eq!(latest.by_id.get(&changed.def.id), Some(&changed));
    assert_eq!(catalog.view_at(first_ts).unwrap(), first_view);
    assert_eq!(
        catalog.view_at(second_ts).unwrap().by_name.get("alpha"),
        Some(&alpha)
    );
    catalog.prune().unwrap();
    assert_eq!(catalog.cache_versions(), vec![first_ts, second_ts, last_ts]);
    pinned.rollback();
    catalog.prune().unwrap();
    assert_eq!(catalog.cache_versions(), vec![last_ts]);
    assert_eq!(catalog.view_at(super::super::hlc::Ts(0)), None);
    assert_eq!(
        catalog.view_at(super::super::hlc::Ts(u64::MAX)).unwrap(),
        latest
    );
}

#[test]
fn catalog_writes_require_commit_ddl_and_own_ddl_is_visible() {
    let (_, seq, locks, catalog) = catalog_setup();
    let mut txn = transaction(&seq, &locks, 1, Isolation::ReadCommitted);
    let created = catalog
        .create_table(&mut txn, table("own"), false, None, &|| false)
        .unwrap();
    assert_eq!(
        catalog
            .resolve(&mut txn, "own", LockMode::AccessShare, None, &|| false)
            .unwrap(),
        created
    );
    let added = catalog
        .add_column(&mut txn, "own", column("own_added"), None, &|| false)
        .unwrap();
    assert_eq!(
        catalog
            .resolve(&mut txn, "own", LockMode::AccessShare, None, &|| false)
            .unwrap(),
        added
    );
    assert_eq!(txn.commit(), Err(TxnError::Misuse));
    let mut other = transaction(&seq, &locks, 2, Isolation::ReadCommitted);
    assert!(matches!(
        catalog.resolve(&mut other, "own", LockMode::AccessShare, None, &|| false),
        Err(CatalogError::Core(CoreError::Sql(SqlError::UnknownTable)))
    ));
    other.rollback();
}

#[test]
fn catalog_old_definition_changes_are_serialization_errors_for_rr_and_serializable() {
    for isolation in [Isolation::RepeatableRead, Isolation::Serializable] {
        let (_, seq, locks, catalog) = catalog_setup();
        create(&catalog, &seq, &locks, 1, "changed");
        let mut old = transaction(&seq, &locks, 2, isolation);
        old.get(Keyspace::Catalog, &[0; 8]).unwrap();
        let mut ddl = transaction(&seq, &locks, 3, Isolation::ReadCommitted);
        catalog
            .add_column(&mut ddl, "changed", column("new_column"), None, &|| false)
            .unwrap();
        catalog.commit_ddl(ddl).unwrap();
        assert!(matches!(
            catalog.resolve(&mut old, "changed", LockMode::AccessShare, None, &|| false),
            Err(CatalogError::Txn(TxnError::Serialization(
                SerializationReason::WriteConflict
            )))
        ));
        old.rollback();
    }
}

#[test]
fn catalog_own_drop_hides_table_and_rollback_restores_committed_definition() {
    let (_, seq, locks, catalog) = catalog_setup();
    let committed = create(&catalog, &seq, &locks, 1, "own_drop");
    let mut ddl = transaction(&seq, &locks, 2, Isolation::ReadCommitted);
    catalog
        .drop_table(&mut ddl, "own_drop", None, &|| false)
        .unwrap();
    assert!(matches!(
        catalog.resolve(&mut ddl, "own_drop", LockMode::AccessShare, None, &|| false),
        Err(CatalogError::Core(CoreError::Sql(SqlError::UnknownTable)))
    ));
    ddl.rollback();
    let mut reader = transaction(&seq, &locks, 3, Isolation::ReadCommitted);
    assert_eq!(
        catalog
            .resolve(
                &mut reader,
                "own_drop",
                LockMode::AccessShare,
                None,
                &|| false
            )
            .unwrap(),
        committed
    );
    reader.rollback();
}

#[test]
fn catalog_short_name_has_exact_wire_keys_and_input_ids_are_ignored() {
    let (_, seq, locks, catalog) = catalog_setup();
    let mut definition = table("x");
    definition.id = 900_001;
    definition.columns[0].id = 900_002;
    definition.columns[1].id = 900_003;
    let mut ddl = transaction(&seq, &locks, 1, Isolation::ReadCommitted);
    let entry = catalog
        .create_table(&mut ddl, definition, false, None, &|| false)
        .unwrap();
    assert_ne!(entry.def.id, 900_001);
    assert_ne!(entry.def.columns[0].id, 900_002);
    assert_ne!(entry.def.columns[1].id, 900_003);
    let committed = catalog.commit_ddl(ddl).unwrap();
    let mut table_key = vec![1];
    table_key.extend_from_slice(&entry.def.id.to_be_bytes());
    table_key.extend_from_slice(&committed.inverted_be());
    let mut name_key = vec![3, b'x', 0];
    name_key.extend_from_slice(&committed.inverted_be());
    let mut table_value = vec![0, 2];
    table_value.extend(crate::storage::encode(&*entry).unwrap());
    table_value.extend_from_slice(&[0; 8]); // frozen empty columns_ext map
    let mut name_value = vec![0];
    name_value.extend_from_slice(&entry.def.id.to_be_bytes());
    let actual = seq
        .kv()
        .range(
            Keyspace::Catalog,
            std::ops::Bound::Unbounded,
            std::ops::Bound::Unbounded,
            super::super::kv::Direction::Forward,
        )
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        actual,
        vec![(table_key, table_value), (name_key, name_value)]
    );
    let mut reader = transaction(&seq, &locks, 2, Isolation::ReadCommitted);
    assert_eq!(
        catalog
            .resolve(&mut reader, "x", LockMode::AccessShare, None, &|| false)
            .unwrap(),
        entry
    );
    reader.rollback();
    let mut invalid = transaction(&seq, &locks, 3, Isolation::ReadCommitted);
    assert!(
        catalog
            .create_table(&mut invalid, table("bad\0name"), false, None, &|| false)
            .is_err()
    );
    invalid.rollback();
}

fn externally_lock_table_and_row(locks: &LockManager, id: u64) {
    locks
        .acquire(
            TxnId(id),
            LockKey::Table(91),
            LockMode::AccessExclusive,
            None,
            &|| false,
        )
        .unwrap();
    locks
        .acquire(
            TxnId(id),
            LockKey::Row {
                table: 91,
                pk: vec![1],
            },
            LockMode::Exclusive,
            None,
            &|| false,
        )
        .unwrap();
    assert_eq!(locks.held(TxnId(id)).len(), 2);
}

#[test]
fn transaction_catalog_tombstones_require_commit_then_and_release_locks_on_rejection() {
    let (_, seq, locks, _) = catalog_setup();
    let mut txn = transaction(&seq, &locks, 1, Isolation::ReadCommitted);
    assert!(!txn.writes_keyspace(Keyspace::Catalog));
    txn.put(Keyspace::Kernel, vec![4; 8], vec![9]).unwrap();
    assert!(txn.writes_keyspace(Keyspace::Kernel));
    assert!(!txn.writes_keyspace(Keyspace::Catalog));
    txn.delete(Keyspace::Catalog, vec![5; 8]).unwrap();
    assert!(txn.writes_keyspace(Keyspace::Catalog));
    externally_lock_table_and_row(&locks, 1);
    assert_eq!(txn.commit(), Err(TxnError::Misuse));
    assert!(locks.held(TxnId(1)).is_empty());
    assert_eq!(seq.visible_ts(), super::super::hlc::Ts(0));
    assert_eq!(
        super::super::mvcc::read_at(seq.kv(), Keyspace::Kernel, &[4; 8], seq.visible_ts()).unwrap(),
        None
    );
}

#[test]
fn transaction_commit_then_keeps_locks_through_callback_and_callback_errors_are_durable() {
    for callback_fails in [false, true] {
        let (mem, seq, locks, _) = catalog_setup();
        let mut txn = transaction(&seq, &locks, 1, Isolation::ReadCommitted);
        let key = vec![5; 8];
        txn.put(Keyspace::Catalog, key.clone(), vec![7]).unwrap();
        externally_lock_table_and_row(&locks, 1);
        let before = seq.visible_ts();
        let mut called = false;
        let mut callback_ts = None;
        let result = txn.commit_then(|ts| {
            called = true;
            callback_ts = Some(ts);
            assert!(ts > before);
            assert_eq!(seq.visible_ts(), ts);
            assert_eq!(locks.held(TxnId(1)).len(), 2);
            assert_eq!(
                super::super::mvcc::read_at(seq.kv(), Keyspace::Catalog, &key, ts).unwrap(),
                Some(vec![7])
            );
            if callback_fails {
                Err(TxnError::Cancelled)
            } else {
                Ok(())
            }
        });
        assert!(called);
        let ts = callback_ts.unwrap();
        assert_eq!(
            result,
            if callback_fails {
                Err(TxnError::Cancelled)
            } else {
                Ok(ts)
            }
        );
        assert!(locks.held(TxnId(1)).is_empty());
        assert_eq!(seq.oldest_active(), None);
        let durable = Kv::new(Arc::new(mem.crash()));
        assert_eq!(
            super::super::mvcc::read_at(&durable, Keyspace::Catalog, &key, ts).unwrap(),
            Some(vec![7])
        );
    }
}

#[test]
fn transaction_commit_then_skips_callback_on_commit_failure_and_releases_locks() {
    let (mem, seq, locks, _) = catalog_setup();
    let mut txn = transaction(&seq, &locks, 1, Isolation::ReadCommitted);
    txn.put(Keyspace::Catalog, vec![5; 8], vec![7]).unwrap();
    externally_lock_table_and_row(&locks, 1);
    mem.fail_next_write(KvError::Io("commit rejected".into()));
    let mut called = false;
    assert_eq!(
        txn.commit_then(|_| {
            called = true;
            Ok(())
        }),
        Err(TxnError::CommitAmbiguous)
    );
    assert!(!called);
    assert!(locks.held(TxnId(1)).is_empty());
    assert_eq!(seq.oldest_active(), None);
    assert_eq!(seq.visible_ts(), super::super::hlc::Ts(0));
}

#[test]
fn transaction_read_only_commit_then_still_invokes_callback_before_releasing_locks() {
    let (_, seq, locks, _) = catalog_setup();
    let txn = transaction(&seq, &locks, 1, Isolation::ReadCommitted);
    externally_lock_table_and_row(&locks, 1);
    let mut called = false;
    let visible = seq.visible_ts();
    assert_eq!(
        txn.commit_then(|ts| {
            called = true;
            assert_eq!(ts, visible);
            assert_eq!(locks.held(TxnId(1)).len(), 2);
            Ok(())
        }),
        Ok(visible)
    );
    assert!(called);
    assert!(locks.held(TxnId(1)).is_empty());
    assert_eq!(seq.oldest_active(), None);
}

/// Arms a read error only after a successful catalog Sync, so the failure is
/// precisely between durable commit and catalog view publication.
#[derive(Default)]
struct FailCatalogRebuild {
    mem: MemKv,
    arm: AtomicBool,
    fail_range: AtomicBool,
}
impl KvRead for FailCatalogRebuild {
    fn get(&self, ks: Keyspace, key: &[u8]) -> Result<Option<Vec<u8>>, KvError> {
        self.mem.get(ks, key)
    }
    fn range(
        &self,
        ks: Keyspace,
        start: std::ops::Bound<&[u8]>,
        end: std::ops::Bound<&[u8]>,
        dir: super::super::kv::Direction,
    ) -> Result<KvIter<'_>, KvError> {
        if ks == Keyspace::Catalog && self.fail_range.swap(false, Ordering::SeqCst) {
            return Err(KvError::Io(
                "catalog rebuild failed after durable commit".into(),
            ));
        }
        self.mem.range(ks, start, end, dir)
    }
}
impl KvBackend for FailCatalogRebuild {
    fn write(&self, batch: WriteBatch, durability: Durability) -> Result<(), KvError> {
        let ops = batch.into_ops();
        let catalog_write = ops.iter().any(|op| {
            matches!(
                op,
                Op::Put {
                    ks: Keyspace::Catalog,
                    ..
                }
            )
        });
        let mut batch = WriteBatch::default();
        for op in ops {
            match op {
                Op::Put { ks, key, value } => batch.put(ks, key, value),
                Op::Delete { ks, key } => batch.delete(ks, key),
                Op::DeleteRange { ks, start, end } => batch.delete_range(ks, start, end),
            }
        }
        self.mem.write(batch, durability)?;
        if durability == Durability::Sync && catalog_write && self.arm.swap(false, Ordering::SeqCst)
        {
            self.fail_range.store(true, Ordering::SeqCst);
        }
        Ok(())
    }
    fn flush_barrier(&self) -> Result<(), KvError> {
        self.mem.flush_barrier()
    }
    fn snapshot(&self) -> Result<Arc<dyn KvRead>, KvError> {
        self.mem.snapshot()
    }
    fn engine_name(&self) -> &'static str {
        "fail-catalog-rebuild"
    }
}

#[test]
fn catalog_post_durable_rebuild_failure_poison_rejects_reads_and_ddl_until_reopen() {
    let backend = Arc::new(FailCatalogRebuild::default());
    let kv = Arc::new(Kv::new(backend.clone()));
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
    create(&catalog, &seq, &locks, 1, "poisoned");
    let mut ddl = transaction(&seq, &locks, 2, Isolation::ReadCommitted);
    let changed = catalog
        .add_column(
            &mut ddl,
            "poisoned",
            column("durable_column"),
            None,
            &|| false,
        )
        .unwrap();
    let before = seq.visible_ts();
    backend.arm.store(true, Ordering::SeqCst);
    assert_eq!(catalog.commit_ddl(ddl), Err(TxnError::CommitAmbiguous));
    assert!(seq.visible_ts() > before);
    assert!(locks.held(TxnId(2)).is_empty());
    assert!(catalog.view_at(seq.visible_ts()).is_none());
    for operation in 0..4 {
        let mut txn = transaction(&seq, &locks, 3 + operation, Isolation::ReadCommitted);
        let error = match operation {
            0 => catalog
                .resolve(&mut txn, "poisoned", LockMode::AccessShare, None, &|| false)
                .err(),
            1 => catalog
                .create_table(&mut txn, table("another"), false, None, &|| false)
                .err(),
            2 => catalog
                .add_column(&mut txn, "poisoned", column("later"), None, &|| false)
                .err(),
            _ => catalog
                .drop_table(&mut txn, "poisoned", None, &|| false)
                .err(),
        };
        assert!(matches!(error, Some(CatalogError::Txn(TxnError::Fenced))));
        txn.rollback();
    }
    let txn = transaction(&seq, &locks, 7, Isolation::ReadCommitted);
    assert_eq!(catalog.commit_ddl(txn), Err(TxnError::Fenced));
    let durable_kv = Arc::new(Kv::new(Arc::new(backend.mem.crash())));
    let durable_seq = Sequencer::open(
        durable_kv.clone(),
        Arc::new(ManualClock::new(1)),
        SequencerConfig::default(),
    )
    .unwrap();
    let durable_locks = Arc::new(LockManager::new(LockConfig::default()));
    let reopened = Catalog::open(
        durable_seq.clone(),
        durable_locks.clone(),
        Arc::new(SequenceAllocator::new(durable_kv, 64)),
    )
    .unwrap();
    let mut reader = transaction(&durable_seq, &durable_locks, 8, Isolation::ReadCommitted);
    assert_eq!(
        reopened
            .resolve(
                &mut reader,
                "poisoned",
                LockMode::AccessShare,
                None,
                &|| false
            )
            .unwrap(),
        changed
    );
    reader.rollback();
}

#[test]
fn mvcc_one_byte_catalog_keys_preserve_snapshot_tombstone_and_range_boundaries() {
    use super::super::{codec, hlc::Ts, kv::Direction, mvcc};
    use std::ops::Bound::{Excluded, Included};
    let kv = Kv::new(Arc::new(MemKv::new()));
    let mut batch = WriteBatch::default();
    for (key, ts, payload) in [
        (b'a', 0, Some(b"a0".as_slice())),
        (b'a', 2, Some(b"a2".as_slice())),
        (b'a', 4, None),
        (b'a', 6, Some(b"a6".as_slice())),
        (b'b', 0, Some(b"b0".as_slice())),
        (b'b', 3, None),
        (b'b', 5, Some(b"b5".as_slice())),
        (b'c', 2, Some(b"c2".as_slice())),
        (b'c', 4, None),
    ] {
        batch.put(
            Keyspace::Catalog,
            codec::user_key_seek_at(&[key], Ts(ts)),
            payload.map_or_else(mvcc::encode_tombstone, mvcc::encode_live),
        );
    }
    kv.write(batch, Durability::Sync).unwrap();
    for (snapshot, a, b, c) in [
        (0, Some(b"a0".as_slice()), Some(b"b0".as_slice()), None),
        (1, Some(b"a0".as_slice()), Some(b"b0".as_slice()), None),
        (
            2,
            Some(b"a2".as_slice()),
            Some(b"b0".as_slice()),
            Some(b"c2".as_slice()),
        ),
        (3, Some(b"a2".as_slice()), None, Some(b"c2".as_slice())),
        (4, None, None, None),
        (5, None, Some(b"b5".as_slice()), None),
        (6, Some(b"a6".as_slice()), Some(b"b5".as_slice()), None),
    ] {
        for (key, value) in [(b'a', a), (b'b', b), (b'c', c)] {
            assert_eq!(
                mvcc::read_at(&kv, Keyspace::Catalog, &[key], Ts(snapshot)).unwrap(),
                value.map(<[u8]>::to_vec)
            );
        }
        for (start, end, include_a, include_b) in [
            (
                Included(b"a".as_slice()),
                Included(b"b".as_slice()),
                true,
                true,
            ),
            (
                Included(b"a".as_slice()),
                Excluded(b"b".as_slice()),
                true,
                false,
            ),
            (
                Excluded(b"a".as_slice()),
                Included(b"b".as_slice()),
                false,
                true,
            ),
            (
                Excluded(b"a".as_slice()),
                Excluded(b"b".as_slice()),
                false,
                false,
            ),
        ] {
            let mut expected = Vec::new();
            if include_a && let Some(value) = a {
                expected.push((b"a".to_vec(), value.to_vec()));
            }
            if include_b && let Some(value) = b {
                expected.push((b"b".to_vec(), value.to_vec()));
            }
            for direction in [Direction::Forward, Direction::Reverse] {
                let actual = mvcc::VersionedRange::new(
                    &kv,
                    Keyspace::Catalog,
                    start,
                    end,
                    direction,
                    Ts(snapshot),
                )
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
                let mut ordered = expected.clone();
                if direction == Direction::Reverse {
                    ordered.reverse();
                }
                assert_eq!(
                    actual, ordered,
                    "snapshot {snapshot}, {start:?}..{end:?}, {direction:?}"
                );
            }
        }
    }
    assert_eq!(
        mvcc::latest_version(&kv, Keyspace::Catalog, b"a").unwrap(),
        Some((Ts(6), Some(b"a6".to_vec())))
    );
    assert_eq!(
        mvcc::latest_version(&kv, Keyspace::Catalog, b"b").unwrap(),
        Some((Ts(5), Some(b"b5".to_vec())))
    );
    assert_eq!(
        mvcc::latest_version(&kv, Keyspace::Catalog, b"c").unwrap(),
        Some((Ts(4), None))
    );
    assert_eq!(
        mvcc::read_at(&kv, Keyspace::Catalog, b"d", Ts(u64::MAX)).unwrap(),
        None
    );
}

#[test]
fn mvcc_rejects_empty_catalog_user_keys_in_points_bounds_and_raw_scans() {
    use super::super::{codec::CodecError, hlc::Ts, kv::Direction, mvcc};
    use std::ops::Bound::{Excluded, Included, Unbounded};
    let kv = Kv::new(Arc::new(MemKv::new()));
    assert!(matches!(
        mvcc::read_at(&kv, Keyspace::Catalog, b"", Ts(0)),
        Err(TxnError::Codec(CodecError::Corrupt(_)))
    ));
    assert!(matches!(
        mvcc::latest_version(&kv, Keyspace::Catalog, b""),
        Err(TxnError::Codec(CodecError::Corrupt(_)))
    ));
    for direction in [Direction::Forward, Direction::Reverse] {
        for (start, end) in [
            (Included(b"".as_slice()), Unbounded),
            (Excluded(b"".as_slice()), Unbounded),
            (Unbounded, Included(b"".as_slice())),
            (Unbounded, Excluded(b"".as_slice())),
        ] {
            assert!(matches!(
                mvcc::VersionedRange::new(&kv, Keyspace::Catalog, start, end, direction, Ts(0)),
                Err(TxnError::Codec(CodecError::Corrupt(_)))
            ));
        }
    }
    let mut batch = WriteBatch::default();
    batch.put(
        Keyspace::Catalog,
        Ts(0).inverted_be().to_vec(),
        mvcc::encode_live(b"invalid"),
    );
    kv.write(batch, Durability::Sync).unwrap();
    for direction in [Direction::Forward, Direction::Reverse] {
        let mut scan = mvcc::VersionedRange::new(
            &kv,
            Keyspace::Catalog,
            Unbounded,
            Unbounded,
            direction,
            Ts(0),
        )
        .unwrap();
        assert!(matches!(
            scan.next(),
            Some(Err(TxnError::Codec(CodecError::Corrupt(_))))
        ));
        assert!(scan.next().is_none());
    }
}

#[test]
fn mvcc_unbounded_rows_and_index_scans_reject_user_keys_shorter_than_object_id() {
    use super::super::{codec, codec::CodecError, hlc::Ts, kv::Direction, mvcc};
    use std::ops::Bound::Unbounded;
    for keyspace in [Keyspace::Rows, Keyspace::Index] {
        for length in 0..8 {
            let kv = Kv::new(Arc::new(MemKv::new()));
            let user = vec![b'a'; length];
            let mut batch = WriteBatch::default();
            batch.put(
                keyspace,
                codec::user_key_seek_at(&user, Ts(0)),
                mvcc::encode_live(b"invalid"),
            );
            kv.write(batch, Durability::Sync).unwrap();
            for direction in [Direction::Forward, Direction::Reverse] {
                let mut scan = mvcc::VersionedRange::new(
                    &kv,
                    keyspace,
                    Unbounded,
                    Unbounded,
                    direction,
                    Ts(0),
                )
                .unwrap();
                assert!(
                    matches!(
                        scan.next(),
                        Some(Err(TxnError::Codec(CodecError::Corrupt(_))))
                    ),
                    "{keyspace:?}, length {length}, {direction:?}"
                );
                assert!(scan.next().is_none());
            }
        }
    }
}

#[test]
fn transaction_catalog_lock_accessor_uses_its_owner_and_rejects_other_lock_keys() {
    let (_, seq, locks, _) = catalog_setup();
    let other_manager = LockManager::new(LockConfig::default());
    let txn = transaction(&seq, &locks, 41, Isolation::ReadCommitted);
    let table = LockKey::Table(7);
    let name = LockKey::Unique {
        index: super::super::locks::CATALOG_NAME_INDEX,
        tuple: b"catalog_name".to_vec(),
    };
    txn.acquire_lock(table.clone(), LockMode::AccessShare, None, &|| false)
        .unwrap();
    txn.acquire_lock(name.clone(), LockMode::Exclusive, None, &|| false)
        .unwrap();
    let held = locks.held(TxnId(41));
    assert_eq!(held.len(), 2);
    assert!(held.contains(&(table.clone(), LockMode::AccessShare)));
    assert!(held.contains(&(name.clone(), LockMode::Exclusive)));
    assert!(locks.held(TxnId(42)).is_empty());
    assert!(other_manager.held(TxnId(41)).is_empty());
    for forbidden in [
        LockKey::Row {
            table: 7,
            pk: vec![1],
        },
        LockKey::Unique {
            index: 7,
            tuple: vec![1],
        },
    ] {
        assert_eq!(
            txn.acquire_lock(forbidden, LockMode::Exclusive, None, &|| false),
            Err(TxnError::Misuse)
        );
    }
    assert_eq!(
        txn.acquire_lock(table, LockMode::Exclusive, None, &|| false),
        Err(TxnError::Misuse)
    );
    assert_eq!(
        txn.acquire_lock(name, LockMode::AccessShare, None, &|| false),
        Err(TxnError::Misuse)
    );
    assert_eq!(locks.held(TxnId(41)).len(), 2);
    txn.rollback();
    assert!(locks.held(TxnId(41)).is_empty());
    assert_eq!(locks.waiting_count(), 0);
}
