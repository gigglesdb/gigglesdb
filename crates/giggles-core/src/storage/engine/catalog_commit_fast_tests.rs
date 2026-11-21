//! No-catalog commits must neither rebuild metadata nor hold its publication mutex.
use super::super::{
    commit::{ManualClock, SequencerConfig},
    kv::{Durability, FenceClass, FenceSite, KvBackend, KvIter, KvRead, RocksStatus, WriteBatch},
    kv_mem::MemKv,
    locks::{LockConfig, TxnId},
};
use super::*;
use crate::relational::DataType;
use std::{
    sync::{
        Weak,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::Duration,
};

type CommitPause = (mpsc::Sender<()>, mpsc::Receiver<()>);

struct CatalogReadFailure {
    error: KvError,
    fence_during_read: Option<Weak<Sequencer>>,
}

fn status(code: u8) -> RocksStatus {
    RocksStatus { code, subcode: 0, severity: None }
}

#[derive(Default)]
struct ObservedKv {
    mem: MemKv,
    catalog_ranges: AtomicUsize,
    pause: Mutex<Option<CommitPause>>,
    catalog_read_error: Mutex<Option<CatalogReadFailure>>,
}
impl KvRead for ObservedKv {
    fn get(&self, ks: Keyspace, key: &[u8]) -> Result<Option<Vec<u8>>, KvError> {
        self.mem.get(ks, key)
    }
    fn range(
        &self,
        ks: Keyspace,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        dir: Direction,
    ) -> Result<KvIter<'_>, KvError> {
        if ks == Keyspace::Catalog {
            self.catalog_ranges.fetch_add(1, Ordering::Relaxed);
            if let Some(failure) = self.catalog_read_error.lock().unwrap().take() {
                if let Some(seq) = failure.fence_during_read {
                    // The reload has passed Kv's fence check. Simulate a writer
                    // fencing the Kv before this in-flight read returns.
                    let seq = seq.upgrade().unwrap();
                    let error = KvError::RocksIo {
                        message: "prior WAL error".into(),
                        status: status(RocksStatus::IO_ERROR),
                        site: FenceSite::WalWrite,
                    };
                    self.mem.fail_next_write(error.clone());
                    assert_eq!(seq.kv().write(WriteBatch::default(), Durability::Sync), Err(error));
                    assert!(seq.kv().is_fenced());
                    assert_eq!(seq.kv().causes.cause().unwrap().class, FenceClass::Recoverable);
                }
                return Ok(KvIter::new(std::iter::once(Err(failure.error))));
            }
        }
        self.mem.range(ks, start, end, dir)
    }
}
impl KvBackend for ObservedKv {
    fn write(&self, batch: WriteBatch, durability: Durability) -> Result<(), KvError> {
        if durability == Durability::Sync
            && let Some((entered, release)) = self.pause.lock().unwrap().take()
        {
            entered.send(()).unwrap();
            release.recv_timeout(Duration::from_secs(10)).unwrap();
        }
        self.mem.write(batch, durability)
    }
    fn flush_barrier(&self) -> Result<(), KvError> {
        self.mem.flush_barrier()
    }
    fn snapshot(&self) -> Result<Arc<dyn KvRead>, KvError> {
        self.mem.snapshot()
    }
    fn engine_name(&self) -> &'static str {
        "f7-observed"
    }
}

fn setup() -> (
    Arc<ObservedKv>,
    Arc<Sequencer>,
    Arc<LockManager>,
    Arc<Catalog>,
) {
    let backend = Arc::new(ObservedKv::default());
    let kv = Arc::new(Kv::new(backend.clone()));
    let seq = Sequencer::open(
        kv.clone(),
        Arc::new(ManualClock::new(10)),
        SequencerConfig::default(),
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
    (backend, seq, locks, catalog)
}
fn txn(seq: &Arc<Sequencer>, locks: &Arc<LockManager>, id: u64) -> Transaction {
    Transaction::begin(seq, locks, TxnId(id), Isolation::ReadCommitted)
}
fn table(name: &str) -> TableDef {
    TableDef {
        id: 0,
        name: name.into(),
        columns: vec![column("id")],
        primary_key: vec![0],
    }
}
fn column(name: &str) -> ColumnDef {
    ColumnDef {
        id: 0,
        name: name.into(),
        data_type: DataType::BigInt,
        not_null: true,
    }
}

#[test]
fn no_catalog_commits_do_not_scan_or_publish_at_ten_and_fifty_tables() {
    for count in [10, 50] {
        let (backend, seq, locks, catalog) = setup();
        for id in 1..=count {
            let mut ddl = txn(&seq, &locks, id);
            catalog
                .create_table(&mut ddl, table(&format!("t{id}")), false, None, &|| false)
                .unwrap();
            catalog.commit_ddl(ddl).unwrap();
        }
        let before = catalog.cache_versions();
        let view = catalog.view_at(seq.visible_ts()).unwrap();
        backend.catalog_ranges.store(0, Ordering::Relaxed);
        catalog.commit_ddl(txn(&seq, &locks, count + 1)).unwrap();
        let mut dml = txn(&seq, &locks, count + 2);
        dml.lock_for_write(Keyspace::Rows, &[1; 8], None, &|| false).unwrap();
        dml.put(Keyspace::Rows, vec![1; 8], vec![2]).unwrap();
        catalog.commit_ddl(dml).unwrap();
        assert_eq!(
            backend.catalog_ranges.load(Ordering::Relaxed),
            0,
            "{count} tables"
        );
        assert_eq!(catalog.cache_versions(), before);
        assert!(Arc::ptr_eq(
            &view,
            &catalog.view_at(seq.visible_ts()).unwrap()
        ));
    }
}

#[test]
fn data_commit_releases_catalog_mutex_while_waiting_for_sequencer() {
    let (backend, seq, locks, catalog) = setup();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    *backend.pause.lock().unwrap() = Some((entered_tx, release_rx));
    let mut dml = txn(&seq, &locks, 1);
    dml.lock_for_write(Keyspace::Rows, &[1; 8], None, &|| false).unwrap();
    dml.put(Keyspace::Rows, vec![1; 8], vec![2]).unwrap();
    let committing = catalog.clone();
    let worker = thread::spawn(move || committing.commit_ddl(dml));
    entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let available = catalog.cache.try_lock().is_ok();
    release_tx.send(()).unwrap();
    worker.join().unwrap().unwrap();
    assert!(
        available,
        "catalog readers were blocked behind a data commit"
    );
    assert_eq!(backend.catalog_ranges.load(Ordering::Relaxed), 1); // initial open only
}

#[test]
fn private_ddl_and_committed_versions_preserve_rc_and_repeatable_read_visibility() {
    let (_, seq, locks, catalog) = setup();
    let mut rc = txn(&seq, &locks, 1);
    let mut rr = Transaction::begin(&seq, &locks, TxnId(2), Isolation::RepeatableRead);
    assert!(catalog.parts(&mut rc).unwrap().tables.is_empty());
    assert!(catalog.parts(&mut rr).unwrap().tables.is_empty());
    let mut create = txn(&seq, &locks, 3);
    catalog
        .create_table(&mut create, table("visible"), false, None, &|| false)
        .unwrap();
    assert!(
        catalog
            .parts(&mut create)
            .unwrap()
            .tables
            .contains_key("visible")
    );
    assert!(
        !catalog
            .parts(&mut rc)
            .unwrap()
            .tables
            .contains_key("visible")
    );
    catalog.commit_ddl(create).unwrap();
    rc.begin_statement();
    rr.begin_statement();
    assert!(
        catalog
            .parts(&mut rc)
            .unwrap()
            .tables
            .contains_key("visible")
    );
    assert!(catalog.parts(&mut rr).unwrap().tables.is_empty());
    let mut rr_created = Transaction::begin(&seq, &locks, TxnId(6), Isolation::RepeatableRead);
    assert_eq!(
        catalog.parts(&mut rr_created).unwrap().tables["visible"]
            .columns
            .len(),
        1
    );
    let mut alter = txn(&seq, &locks, 4);
    catalog
        .add_column(&mut alter, "visible", column("extra"), None, &|| false)
        .unwrap();
    assert_eq!(
        catalog.parts(&mut alter).unwrap().tables["visible"]
            .columns
            .len(),
        2
    );
    assert_eq!(
        catalog.parts(&mut rc).unwrap().tables["visible"]
            .columns
            .len(),
        1
    );
    catalog.commit_ddl(alter).unwrap();
    rc.begin_statement();
    assert_eq!(
        catalog.parts(&mut rc).unwrap().tables["visible"]
            .columns
            .len(),
        2
    );
    rr_created.begin_statement();
    assert_eq!(
        catalog.parts(&mut rr_created).unwrap().tables["visible"]
            .columns
            .len(),
        1
    );
    let mut rr_altered = Transaction::begin(&seq, &locks, TxnId(7), Isolation::RepeatableRead);
    assert_eq!(
        catalog.parts(&mut rr_altered).unwrap().tables["visible"]
            .columns
            .len(),
        2
    );
    let mut drop = txn(&seq, &locks, 5);
    catalog
        .drop_table(&mut drop, "visible", None, &|| false)
        .unwrap();
    assert!(catalog.parts(&mut drop).unwrap().tables.is_empty());
    assert!(
        catalog
            .parts(&mut rc)
            .unwrap()
            .tables
            .contains_key("visible")
    );
    catalog.commit_ddl(drop).unwrap();
    rc.begin_statement();
    rr.begin_statement();
    assert!(catalog.parts(&mut rc).unwrap().tables.is_empty());
    assert!(catalog.parts(&mut rr).unwrap().tables.is_empty());
    rr_created.begin_statement();
    rr_altered.begin_statement();
    assert_eq!(
        catalog.parts(&mut rr_created).unwrap().tables["visible"]
            .columns
            .len(),
        1
    );
    assert_eq!(
        catalog.parts(&mut rr_altered).unwrap().tables["visible"]
            .columns
            .len(),
        2
    );
}

#[test]
fn concurrent_ddl_and_data_commits_keep_published_catalog_coherent() {
    let (_, seq, locks, catalog) = setup();
    thread::scope(|scope| {
        scope.spawn(|| {
            for i in 0..40 {
                let mut ddl = txn(&seq, &locks, 100 + i);
                catalog
                    .create_table(&mut ddl, table(&format!("t{i}")), false, None, &|| false)
                    .unwrap();
                catalog.commit_ddl(ddl).unwrap();
            }
        });
        for client in 0..4 {
            let seq = &seq;
            let locks = &locks;
            let catalog = &catalog;
            scope.spawn(move || {
                for i in 0..80 {
                    let mut data = txn(seq, locks, 1000 + client * 100 + i);
                    let parts = catalog.parts(&mut data).unwrap();
                    assert!(parts.tables.values().all(|t| t.columns.len() == 1));
                    data.lock_for_write(Keyspace::Rows, &[client as u8; 8], None, &|| false)
                        .unwrap();
                    data.put(Keyspace::Rows, vec![client as u8; 8], vec![i as u8])
                        .unwrap();
                    catalog.commit_ddl(data).unwrap();
                }
            });
        }
    });
    assert_eq!(
        catalog
            .parts(&mut txn(&seq, &locks, 2000))
            .unwrap()
            .tables
            .len(),
        40
    );
}

#[test]
fn durable_reload_failure_retains_cause_and_never_serves_stale_catalog() {
    let (backend, seq, locks, catalog) = setup();
    catalog.snapshot(&mut txn(&seq, &locks, 400)).unwrap();
    assert!(catalog.current.read().unwrap().is_some());
    let replaced = catalog.cache.lock().unwrap().versions.last().unwrap().0;
    let mut ddl = txn(&seq, &locks, 401);
    catalog.create_table(&mut ddl, table("durable_despite_reload"), false, None, &|| false).unwrap();
    *catalog.load_at_error.lock().unwrap() = Some(CoreError::Storage("injected reload I/O".into()));
    assert!(matches!(catalog.commit_ddl(ddl), Err(TxnError::CommitAmbiguous)));
    let committed = seq.visible_ts();
    assert_eq!(seq.kv().causes.cause().unwrap().class, FenceClass::NotRecoverable);
    {
        let cache = catalog.cache.lock().unwrap();
        let cause = cache.poison_cause.as_ref().unwrap();
        assert!(cause.contains("injected reload I/O"));
        assert!(cause.contains(&format!("commit_ts={}", committed.0)));
        assert!(cause.contains(&format!("replaced_version=Some({})", replaced.0)));
        assert!(!cache.poison_reported.load(Ordering::Relaxed));
        assert!(cache.view_at(committed).is_none());
    }
    let mut later = txn(&seq, &locks, 402);
    assert!(matches!(catalog.snapshot(&mut later), Err(CoreError::Fenced)));
    assert!(matches!(catalog.commit_ddl(later), Err(TxnError::Fenced)));
    assert!(catalog.cache.lock().unwrap().poison_reported.load(Ordering::Relaxed));
    assert!(catalog.current.read().unwrap().is_none());

    // MemKv crash discards every non-durable write: reopen only its durable image.
    let kv = Arc::new(Kv::new(Arc::new(backend.mem.crash())));
    let reopened_seq = Sequencer::open(kv.clone(), Arc::new(ManualClock::new(20)), SequencerConfig::default()).unwrap();
    let reopened = Catalog::open(reopened_seq.clone(), Arc::new(LockManager::new(LockConfig::default())), Arc::new(SequenceAllocator::new(kv, 64))).unwrap();
    assert!(reopened.view_at(reopened_seq.visible_ts()).unwrap().by_name.contains_key("durable_despite_reload"));
}

#[test]
fn durable_catalog_poison_preserves_native_read_status_and_precedence() {
    let native_read = |code| KvError::RocksIo {
        message: "typed backend read failure".into(),
        status: status(code),
        site: FenceSite::Read,
    };
    for (error, fence_during_read, site, class, code) in [
        (native_read(7), false, FenceSite::CatalogReload, FenceClass::NotRecoverable, Some(7)), // Incomplete
        (native_read(7), true, FenceSite::CatalogReload, FenceClass::NotRecoverable, Some(7)),
        (native_read(RocksStatus::IO_ERROR), false, FenceSite::CatalogReload, FenceClass::Recoverable, Some(RocksStatus::IO_ERROR)),
        (KvError::Closed, true, FenceSite::CatalogReload, FenceClass::NotRecoverable, None),
        (KvError::Fenced, true, FenceSite::WalWrite, FenceClass::Recoverable, Some(RocksStatus::IO_ERROR)),
    ] {
        let (backend, seq, locks, catalog) = setup();
        let before = seq.visible_ts();
        let mut ddl = txn(&seq, &locks, 401);
        catalog
            .create_table(
                &mut ddl,
                table("durable_despite_reload"),
                false,
                None,
                &|| false,
            )
            .unwrap();
        *backend.catalog_read_error.lock().unwrap() = Some(CatalogReadFailure {
            error,
            fence_during_read: fence_during_read.then(|| Arc::downgrade(&seq)),
        });
        assert!(matches!(
            catalog.commit_ddl(ddl),
            Err(TxnError::CommitAmbiguous)
        ));
        assert!(seq.visible_ts() > before);
        assert!(catalog.cache.lock().unwrap().poisoned);
        assert_eq!(seq.kv().is_fenced(), fence_during_read);
        let cause = seq.kv().causes.cause().unwrap();
        assert_eq!(
            (cause.site, cause.class, cause.status),
            (site, class, code.map(status))
        );
    }
}

#[test]
fn changed_table_revalidates_unchanged_legacy_fk_before_durability() {
    use crate::relational::catalog_ext::{Action, Constraint, ConstraintKind, Deferral, ReferencedKey};
    let (backend, seq, locks, catalog) = setup();
    let mut ddl = txn(&seq, &locks, 501);
    let parent = catalog.create_table(&mut ddl, table("parent"), false, None, &|| false).unwrap();
    let mut child_def = table("child");
    child_def.columns.push(column("parent_id"));
    let child = catalog.create_table(&mut ddl, child_def, false, None, &|| false).unwrap();
    let constraint = Constraint {
        id: 1000,
        name: "child_parent_fk".into(),
        kind: ConstraintKind::ForeignKey {
            columns: vec![child.def.columns[1].id],
            ref_table: parent.def.id,
            ref_columns: vec![parent.def.columns[0].id],
            on_delete: Action::NoAction,
            on_update: Action::NoAction,
            not_valid: false,
            deferral: Deferral::NotDeferrable,
            target: ReferencedKey::Unknown,
        },
    };
    let key = catalog_codec::constraint_key(child.def.id, constraint.id);
    let value = catalog_codec::encode_constraint(&constraint, "child").unwrap();
    assert_eq!(value[0], 1, "exercise the legacy FK path");
    ddl.require_features(super::super::content_check::required_bits_for_catalog_key(&key).unwrap()
        | super::super::content_check::required_bits_for_catalog_value(&key, &value).unwrap());
    ddl.put(Keyspace::Catalog, key, value).unwrap();
    catalog.commit_ddl(ddl).unwrap();
    catalog.snapshot(&mut txn(&seq, &locks, 502)).unwrap();
    assert!(catalog.current.read().unwrap().is_some());
    let before_ts = seq.visible_ts();
    let before_versions = catalog.cache_versions();
    let before_image: Vec<_> = backend.mem.range(Keyspace::Catalog, Bound::Unbounded, Bound::Unbounded, Direction::Forward).unwrap().collect();
    let mut ddl = txn(&seq, &locks, 503);
    let mut changed = (*child).clone();
    changed.def.columns.pop();
    let key = table_key(child.def.id);
    ddl.put(Keyspace::Catalog, key, encode_entry(&changed).unwrap()).unwrap();
    let result = catalog.commit_ddl(ddl);
    assert!(matches!(result, Err(TxnError::Kv(KvError::Corrupt(_)))), "{result:?}; poisoned={}", catalog.cache.lock().unwrap().poisoned);
    assert!(!catalog.cache.lock().unwrap().poisoned);
    assert_eq!(seq.visible_ts(), before_ts);
    assert_eq!(catalog.cache_versions(), before_versions);
    let after_image: Vec<_> = backend.mem.range(Keyspace::Catalog, Bound::Unbounded, Bound::Unbounded, Direction::Forward).unwrap().collect();
    assert_eq!(before_image, after_image);
    let parts = catalog.parts(&mut txn(&seq, &locks, 504)).unwrap();
    assert_eq!(parts.tables["child"].columns.len(), 2);
    assert!(catalog.current.read().unwrap().is_some());
}
