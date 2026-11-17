use super::super::{
    commit::{ManualClock, SequencerConfig},
    kv_mem::MemKv,
    locks::{LockConfig, TxnId},
};
use super::*;
use crate::relational::DataType;
use std::{sync::Barrier, thread, time::Duration};

fn setup() -> (Arc<Sequencer>, Arc<LockManager>, Catalog) {
    let kv = Arc::new(Kv::new(Arc::new(MemKv::new())));
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
    (seq, locks, catalog)
}

fn txn(seq: &Arc<Sequencer>, locks: &Arc<LockManager>, id: u64) -> Transaction {
    Transaction::begin(seq, locks, TxnId(id), Isolation::ReadCommitted)
}

fn create(catalog: &Catalog, tx: &mut Transaction, name: &str) {
    catalog
        .create_table(
            tx,
            TableDef {
                id: 0,
                name: name.into(),
                columns: vec![ColumnDef {
                    id: 0,
                    name: "id".into(),
                    data_type: DataType::BigInt,
                    not_null: true,
                }],
                primary_key: vec![0],
            },
            false,
            None,
            &|| false,
        )
        .unwrap();
}

#[test]
fn latest_snapshot_reuses_mutex_arc_without_taking_cache_mutex() {
    let (seq, locks, catalog) = setup();
    let mut ddl = txn(&seq, &locks, 1);
    create(&catalog, &mut ddl, "base");
    let version = catalog.commit_ddl(ddl).unwrap();
    assert!(catalog.current.read().unwrap().is_none());
    let mut reader = txn(&seq, &locks, 2);
    let ts = reader.snapshot_ts();
    let first = {
        let mut cache = catalog.cache.lock().unwrap();
        let snapshot = catalog.snapshot_locked(&mut cache, &reader, ts).unwrap();
        assert!(Arc::ptr_eq(&snapshot, &cache.snapshots[&version]));
        snapshot
    };
    // All four entry points must bypass check() and the cache mutex, including
    // the periodic prune attempt: try_lock must not wait on this held mutex.
    let cache = catalog.cache.lock().unwrap();
    for _ in 0..=CURRENT_PRUNE_INTERVAL {
        assert!(Arc::ptr_eq(&first, &catalog.snapshot(&mut reader).unwrap()));
        assert!(Arc::ptr_eq(
            &first,
            &catalog.metadata_snapshot(&reader).unwrap()
        ));
        let (bound, bound_version) = catalog.binding_snapshot(&reader).unwrap();
        assert!(Arc::ptr_eq(&first, &bound));
        assert_eq!(bound_version, Some(version));
        assert_eq!(catalog.snapshot_version(&reader), Some(version));
    }
    drop(cache);
}

#[test]
fn own_catalog_writes_and_older_snapshots_bypass_current() {
    let (seq, locks, catalog) = setup();
    let mut old = Transaction::begin(&seq, &locks, TxnId(1), Isolation::RepeatableRead);
    let before = catalog.snapshot(&mut old).unwrap();
    let old_version = catalog.snapshot_version(&old);
    let mut ddl = txn(&seq, &locks, 2);
    create(&catalog, &mut ddl, "base");
    catalog.commit_ddl(ddl).unwrap();
    let committed = catalog.snapshot(&mut txn(&seq, &locks, 3)).unwrap();
    assert!(committed.parts.tables.contains_key("base"));
    let old_ts = old.snapshot_ts();
    assert!(catalog.current_for(&old, old_ts).is_none());
    assert!(Arc::ptr_eq(&before, &catalog.snapshot(&mut old).unwrap()));
    assert!(Arc::ptr_eq(
        &before,
        &catalog.metadata_snapshot(&old).unwrap()
    ));
    let (bound, version) = catalog.binding_snapshot(&old).unwrap();
    assert!(Arc::ptr_eq(&before, &bound));
    assert_eq!(version, old_version);
    assert_eq!(catalog.snapshot_version(&old), old_version);

    let mut own = txn(&seq, &locks, 4);
    create(&catalog, &mut own, "private");
    let own_ts = own.snapshot_ts();
    assert!(catalog.current_for(&own, own_ts).is_none());
    assert!(
        catalog
            .snapshot(&mut own)
            .unwrap()
            .parts
            .tables
            .contains_key("private")
    );
    assert!(
        catalog
            .metadata_snapshot(&own)
            .unwrap()
            .parts
            .tables
            .contains_key("private")
    );
    assert!(
        catalog
            .binding_snapshot(&own)
            .unwrap()
            .0
            .parts
            .tables
            .contains_key("private")
    );
    assert_eq!(catalog.snapshot_version(&own), None);
    assert!(!committed.parts.tables.contains_key("private"));
}

#[test]
fn periodic_fast_hits_reclaim_after_old_reader_leaves_without_ddl() {
    let (seq, locks, catalog) = setup();
    let mut old = Transaction::begin(&seq, &locks, TxnId(1), Isolation::RepeatableRead);
    let first = catalog.snapshot(&mut old).unwrap();
    let weak = Arc::downgrade(&first);
    drop(first);
    for id in 2..=3 {
        let mut ddl = txn(&seq, &locks, id);
        create(&catalog, &mut ddl, &format!("t{id}"));
        catalog.commit_ddl(ddl).unwrap();
    }
    let mut reader = txn(&seq, &locks, 4);
    let latest = catalog.snapshot(&mut reader).unwrap();
    assert_eq!(catalog.cache_versions().len(), 3);
    drop(old);
    // No mutex-path acquisition or DDL follows dropping the old reader.
    for _ in 0..=CURRENT_PRUNE_INTERVAL {
        assert!(Arc::ptr_eq(
            &latest,
            &catalog.snapshot(&mut reader).unwrap()
        ));
    }
    assert_eq!(catalog.cache_versions(), vec![seq.visible_ts()]);
    assert!(weak.upgrade().is_none());
}

#[test]
fn stress_ddl_vs_fast_catalog_snapshot() {
    let (seq, locks, catalog) = setup();
    catalog.snapshot(&mut txn(&seq, &locks, 1)).unwrap();
    let recorded = Mutex::new(Vec::<(Ts, String)>::new());
    let start = Barrier::new(32);
    thread::scope(|scope| {
        scope.spawn(|| {
            start.wait();
            for n in 0..24 {
                let name = format!("t{n}");
                let mut ddl = txn(&seq, &locks, 10 + n);
                create(&catalog, &mut ddl, &name);
                let committed = catalog.commit_ddl(ddl).unwrap();
                recorded.lock().unwrap().push((committed, name));
                thread::yield_now();
            }
        });
        for client in 0..31 {
            let (seq, locks, catalog, recorded, start) =
                (&seq, &locks, &catalog, &recorded, &start);
            scope.spawn(move || {
                start.wait();
                for n in 0..128 {
                    let mut reader = txn(seq, locks, 1000 + client * 1000 + n);
                    let snapshot = catalog.snapshot(&mut reader).unwrap();
                    let ts = reader.snapshot_ts();
                    for (committed, name) in recorded.lock().unwrap().iter() {
                        if *committed <= ts {
                            assert!(
                                snapshot.parts.tables.contains_key(name),
                                "snapshot {ts:?} missed {name} committed at {committed:?}"
                            );
                        }
                    }
                    thread::yield_now();
                }
            });
        }
    });
    assert_eq!(recorded.lock().unwrap().len(), 24);
    let final_snapshot = catalog.snapshot(&mut txn(&seq, &locks, 5000)).unwrap();
    for (_, name) in recorded.lock().unwrap().iter() {
        assert!(final_snapshot.parts.tables.contains_key(name));
    }
}
