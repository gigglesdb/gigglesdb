//! No-catalog commits must neither rebuild metadata nor hold its publication mutex.
use super::super::{
    commit::{ManualClock, SequencerConfig},
    kv::{Durability, KvBackend, KvIter, KvRead, WriteBatch},
    kv_mem::MemKv,
    locks::{LockConfig, TxnId},
};
use super::*;
use crate::relational::DataType;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::Duration,
};

type CommitPause = (mpsc::Sender<()>, mpsc::Receiver<()>);

#[derive(Default)]
struct ObservedKv {
    mem: MemKv,
    catalog_ranges: AtomicUsize,
    pause: Mutex<Option<CommitPause>>,
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
        dml.put(Keyspace::Kernel, vec![1], vec![2]).unwrap();
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
    dml.put(Keyspace::Kernel, vec![1], vec![2]).unwrap();
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
                    data.put(Keyspace::Kernel, vec![client as u8], vec![i as u8])
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
