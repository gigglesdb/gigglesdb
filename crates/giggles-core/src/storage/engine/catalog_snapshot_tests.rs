use super::super::{
    commit::{ManualClock, SequencerConfig},
    kv_mem::MemKv,
    locks::{LockConfig, TxnId},
};
use super::*;
use crate::relational::DataType;
use std::{sync::Barrier, thread};

fn setup() -> (Arc<Sequencer>, Arc<LockManager>, Catalog) {
    let kv = Arc::new(Kv::new(Arc::new(MemKv::new())));
    let seq = Sequencer::open(
        kv.clone(),
        Arc::new(ManualClock::new(10)),
        SequencerConfig::default(),
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
fn column(name: &str) -> ColumnDef {
    ColumnDef {
        id: 0,
        name: name.into(),
        data_type: DataType::BigInt,
        not_null: true,
    }
}
fn create(catalog: &Catalog, tx: &mut Transaction, name: &str) {
    catalog
        .create_table(
            tx,
            TableDef {
                id: 0,
                name: name.into(),
                columns: vec![column("id")],
                primary_key: vec![0],
            },
            false,
            None,
            &|| false,
        )
        .unwrap();
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn gateway_token_reads_leave_the_catalog_cache_unlocked() {
    use crate::storage::cluster::{
        db::{attach, local::LocalCluster},
        txn::ClusterConfig,
    };
    use crate::storage::{Backend, Limits};
    use std::{sync::mpsc, time::Duration};

    let cluster = LocalCluster::new(&[]);
    let gateway = attach(
        cluster,
        ClusterConfig::default(),
        None,
        [9; 16],
        Limits::default(),
    )
    .unwrap();
    let Backend::V2(db) = &gateway.admin.inner.backend else {
        unreachable!()
    };
    let catalog = db.catalog();
    for acquisition in 0..5 {
        let (entered, waiting) = mpsc::channel();
        let (release, resume) = mpsc::channel();
        let resume = Mutex::new(resume);
        *catalog.token_read_hook.lock().unwrap() = Some(Box::new(move || {
            entered.send(()).unwrap();
            resume.lock().unwrap().recv().unwrap();
        }));
        thread::scope(|scope| {
            let reader = scope.spawn(|| {
                let mut tx = db.begin(Isolation::ReadCommitted);
                match acquisition {
                    0 => {
                        catalog.snapshot(&mut tx).unwrap();
                    }
                    1 => {
                        catalog.metadata_snapshot(&tx).unwrap();
                    }
                    2 => {
                        catalog.binding_snapshot(&tx).unwrap();
                    }
                    3 => {
                        catalog.refresh_latest().unwrap();
                    }
                    _ => assert!(matches!(
                        catalog.resolve(&mut tx, "absent", LockMode::AccessShare, None, &|| false),
                        Err(CatalogError::Core(CoreError::Sql(SqlError::UnknownTable)))
                    )),
                }
            });
            let reached = waiting.recv_timeout(Duration::from_secs(10));
            let unlocked = catalog.cache.try_lock().is_ok();
            // Always release the worker before asserting, including on failure.
            release.send(()).ok();
            reader.join().unwrap();
            assert!(
                reached.is_ok(),
                "acquisition {acquisition} never reached its token read"
            );
            assert!(
                unlocked,
                "acquisition {acquisition} held the cache during its token read"
            );
        });
        *catalog.token_read_hook.lock().unwrap() = None;
    }
}

#[test]
fn token_recheck_uses_the_version_published_during_the_unlocked_read() {
    let (_, _, catalog) = setup();
    let mut cache = catalog.cache.lock().unwrap();
    let old = Ts(10);
    let published = Ts(20);
    let statement = Ts(30);
    cache.publish(old, CatalogView::default());
    cache.tokens.insert(old, (1, old));
    // Another statement publishes between the cheap check and the locked recheck.
    cache.publish(published, CatalogView::default());
    cache.tokens.insert(published, (2, published));
    catalog
        .refresh_locked(&mut cache, statement, Some(2))
        .unwrap();
    assert_eq!(cache.tokens[&published], (2, statement));
    assert_eq!(cache.tokens[&old], (1, old));
    catalog.refresh_locked(&mut cache, Ts(25), Some(2)).unwrap();
    assert_eq!(cache.tokens[&published], (2, statement));
}

#[test]
fn immutable_image_is_reused_across_data_timestamps_at_ten_and_fifty_tables() {
    for tables in [10, 50] {
        let (seq, locks, catalog) = setup();
        for n in 1..=tables {
            let mut ddl = txn(&seq, &locks, n);
            create(&catalog, &mut ddl, &format!("t{n}"));
            catalog.commit_ddl(ddl).unwrap();
        }
        let catalog_version = seq.visible_ts();
        let mut reader = txn(&seq, &locks, 100);
        let first = catalog.snapshot(&mut reader).unwrap();
        let image = first.image().unwrap();
        let cursor_image = first.cursor_image().unwrap();
        assert_eq!(image.tables.len(), tables as usize);
        let fork = image.catalog_image_for_statement(first.allocation_floor().unwrap(), 1);
        fork.validate().unwrap();
        for n in 0..20 {
            let mut data = txn(&seq, &locks, 200 + n);
            data.lock_for_write(Keyspace::Rows, &[1; 8], None, &|| false).unwrap();
            data.put(Keyspace::Rows, vec![1; 8], vec![n as u8]).unwrap();
            catalog.commit_ddl(data).unwrap();
            reader.begin_statement();
            assert!(reader.snapshot_ts() > catalog_version);
            let next = catalog.snapshot(&mut reader).unwrap();
            assert!(Arc::ptr_eq(&first, &next));
            assert!(Arc::ptr_eq(&image, &next.image().unwrap()));
            assert!(Arc::ptr_eq(&cursor_image, &next.cursor_image().unwrap()));
        }
        assert_eq!(
            catalog
                .cache
                .lock()
                .unwrap()
                .snapshots
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            vec![catalog_version]
        );
    }
}

#[test]
fn private_ddl_bypasses_cache_through_partial_and_complete_savepoint_rollback() {
    let (seq, locks, catalog) = setup();
    let mut ddl = txn(&seq, &locks, 1);
    create(&catalog, &mut ddl, "base");
    catalog.commit_ddl(ddl).unwrap();
    let mut own = txn(&seq, &locks, 2);
    let committed = catalog.snapshot(&mut own).unwrap();
    let committed_image = committed.image().unwrap();
    let initial = own.savepoint();
    create(&catalog, &mut own, "private");
    let after_create = own.savepoint();
    let private = catalog.snapshot(&mut own).unwrap();
    assert!(private.image().unwrap().tables.contains_key("private"));
    assert!(!committed_image.tables.contains_key("private"));
    catalog
        .add_column(&mut own, "private", column("extra"), None, &|| false)
        .unwrap();
    assert_eq!(
        catalog
            .metadata_snapshot(&own)
            .unwrap()
            .image()
            .unwrap()
            .tables["private"]
            .columns
            .len(),
        2
    );
    own.rollback_to(&after_create).unwrap();
    let rolled = catalog.snapshot(&mut own).unwrap();
    assert_eq!(rolled.image().unwrap().tables["private"].columns.len(), 1);
    assert!(!Arc::ptr_eq(&rolled, &private));
    assert!(!Arc::ptr_eq(&rolled, &catalog.snapshot(&mut own).unwrap()));
    let mut other = txn(&seq, &locks, 3);
    assert!(Arc::ptr_eq(
        &committed,
        &catalog.snapshot(&mut other).unwrap()
    ));
    own.rollback_to(&initial).unwrap();
    assert!(Arc::ptr_eq(
        &committed,
        &catalog.snapshot(&mut own).unwrap()
    ));
    assert!(Arc::ptr_eq(
        &committed_image,
        &catalog.metadata_snapshot(&own).unwrap().image().unwrap()
    ));
    catalog
        .drop_table(&mut own, "base", None, &|| false)
        .unwrap();
    assert!(
        !catalog
            .snapshot(&mut own)
            .unwrap()
            .image()
            .unwrap()
            .tables
            .contains_key("base")
    );
    assert!(
        catalog
            .snapshot(&mut other)
            .unwrap()
            .image()
            .unwrap()
            .tables
            .contains_key("base")
    );
    own.rollback_to(&initial).unwrap();
    assert!(Arc::ptr_eq(
        &committed,
        &catalog.snapshot(&mut own).unwrap()
    ));
}

#[test]
fn committed_versions_follow_rc_refresh_and_preserve_old_rr_images() {
    let (seq, locks, catalog) = setup();
    let mut ddl = txn(&seq, &locks, 1);
    create(&catalog, &mut ddl, "base");
    let first_ts = catalog.commit_ddl(ddl).unwrap();
    let mut rc = txn(&seq, &locks, 2);
    let mut rr = Transaction::begin(&seq, &locks, TxnId(3), Isolation::RepeatableRead);
    let first = catalog.snapshot(&mut rr).unwrap();
    assert!(Arc::ptr_eq(&first, &catalog.snapshot(&mut rc).unwrap()));
    let mut ddl = txn(&seq, &locks, 4);
    catalog
        .add_column(&mut ddl, "base", column("extra"), None, &|| false)
        .unwrap();
    create(&catalog, &mut ddl, "next");
    let second_ts = catalog.commit_ddl(ddl).unwrap();
    assert!(Arc::ptr_eq(&first, &catalog.snapshot(&mut rc).unwrap()));
    rc.begin_statement();
    rr.begin_statement();
    let next = catalog.snapshot(&mut rc).unwrap();
    assert_eq!(next.image().unwrap().tables["base"].columns.len(), 2);
    assert!(next.image().unwrap().tables.contains_key("next"));
    assert!(Arc::ptr_eq(&first, &catalog.snapshot(&mut rr).unwrap()));
    assert_eq!(first.image().unwrap().tables["base"].columns.len(), 1);
    assert!(!first.image().unwrap().tables.contains_key("next"));
    assert_eq!(
        catalog
            .cache
            .lock()
            .unwrap()
            .snapshots
            .keys()
            .copied()
            .collect::<Vec<_>>(),
        vec![first_ts, second_ts]
    );
}

#[test]
fn derived_images_are_evicted_with_unselectable_catalog_versions() {
    let (seq, locks, catalog) = setup();
    let mut pinned = Transaction::begin(&seq, &locks, TxnId(1), Isolation::RepeatableRead);
    let initial = catalog.snapshot(&mut pinned).unwrap();
    let weak = Arc::downgrade(&initial);
    let image_weak = Arc::downgrade(&initial.image().unwrap());
    let cursor_weak = Arc::downgrade(&initial.cursor_image().unwrap());
    drop(initial);
    for n in 0..12 {
        let mut ddl = txn(&seq, &locks, 10 + n);
        create(&catalog, &mut ddl, &format!("t{n}"));
        catalog.commit_ddl(ddl).unwrap();
        catalog
            .snapshot(&mut txn(&seq, &locks, 100 + n))
            .unwrap()
            .image()
            .unwrap();
    }
    assert!(weak.upgrade().is_some());
    assert!(image_weak.upgrade().is_some());
    assert!(catalog.cache.lock().unwrap().snapshots.len() > 1);
    pinned.rollback();
    // A metadata acquisition alone reclaims old versions; no new DDL is needed.
    catalog.snapshot(&mut txn(&seq, &locks, 200)).unwrap();
    assert_eq!(catalog.cache.lock().unwrap().snapshots.len(), 1);
    assert_eq!(catalog.cache_versions().len(), 1);
    assert!(weak.upgrade().is_none());
    assert!(image_weak.upgrade().is_none());
    assert!(cursor_weak.upgrade().is_none());
}

#[test]
fn equal_catalog_timestamps_in_different_engines_never_share_images() {
    let (left_seq, left_locks, left) = setup();
    let (right_seq, right_locks, right) = setup();
    let mut left_ddl = txn(&left_seq, &left_locks, 1);
    create(&left, &mut left_ddl, "left_table");
    let left_ts = left.commit_ddl(left_ddl).unwrap();
    let mut right_ddl = txn(&right_seq, &right_locks, 1);
    create(&right, &mut right_ddl, "right_table");
    let right_ts = right.commit_ddl(right_ddl).unwrap();
    assert_eq!(left_ts, right_ts);
    let left_image = left
        .snapshot(&mut txn(&left_seq, &left_locks, 2))
        .unwrap()
        .image()
        .unwrap();
    let right_image = right
        .snapshot(&mut txn(&right_seq, &right_locks, 2))
        .unwrap()
        .image()
        .unwrap();
    assert!(!Arc::ptr_eq(&left_image, &right_image));
    assert!(left_image.tables.contains_key("left_table"));
    assert!(!left_image.tables.contains_key("right_table"));
    assert!(right_image.tables.contains_key("right_table"));
    assert!(!right_image.tables.contains_key("left_table"));
}

#[test]
fn metadata_fallback_pins_only_the_acquisition_without_assigning_a_transaction_snapshot() {
    let (seq, locks, catalog) = setup();
    let unselected = Transaction::begin(&seq, &locks, TxnId(100), Isolation::RepeatableRead);
    assert_eq!(unselected.current_snapshot_ts(), None);
    let old = catalog.metadata_snapshot(&unselected).unwrap();
    assert!(old.parts.tables.is_empty());
    assert_eq!(unselected.current_snapshot_ts(), None);
    assert_eq!(seq.oldest_active(), None);
    for n in 0..12 {
        let mut ddl = txn(&seq, &locks, 1 + n);
        create(&catalog, &mut ddl, &format!("t{n}"));
        catalog.commit_ddl(ddl).unwrap();
        catalog.prune().unwrap();
        let current = catalog.metadata_snapshot(&unselected).unwrap();
        assert_eq!(current.image().unwrap().tables.len(), n as usize + 1);
        assert_eq!(unselected.current_snapshot_ts(), None);
        assert_eq!(seq.oldest_active(), None);
    }
    assert!(old.image().unwrap().tables.is_empty());
}

#[test]
fn concurrent_catalog_and_data_commits_share_only_complete_validated_images() {
    let (seq, locks, catalog) = setup();
    let start = Barrier::new(5);
    thread::scope(|scope| {
        scope.spawn(|| {
            start.wait();
            for n in 0..30 {
                let mut ddl = txn(&seq, &locks, 1 + n);
                create(&catalog, &mut ddl, &format!("t{n}"));
                catalog.commit_ddl(ddl).unwrap();
            }
        });
        for client in 0..4 {
            let (seq, locks, catalog, start) = (&seq, &locks, &catalog, &start);
            scope.spawn(move || {
                start.wait();
                for n in 0..80 {
                    let mut tx = txn(seq, locks, 1000 + client * 100 + n);
                    let snapshot = catalog.snapshot(&mut tx).unwrap();
                    let image = snapshot.image().unwrap();
                    assert_eq!(image.tables, snapshot.parts.tables);
                    image.validate().unwrap();
                    tx.lock_for_write(Keyspace::Rows, &[client as u8; 8], None, &|| false)
                        .unwrap();
                    tx.put(Keyspace::Rows, vec![client as u8; 8], vec![n as u8])
                        .unwrap();
                    catalog.commit_ddl(tx).unwrap();
                }
            });
        }
    });
}
