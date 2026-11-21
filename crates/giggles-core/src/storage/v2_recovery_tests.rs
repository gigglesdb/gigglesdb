use super::*;
use crate::storage::engine::{
    catalog_codec,
    commit::{CommitRequest, RawOp},
    hlc::Ts,
    kv::{Durability, Keyspace, Kv, KvError, KvRead, WriteBatch},
    kv_rocksdb::{
        CloseLatch, RECOVERY_FLUSH_MARKER, RECOVERY_FLUSH_MISSED, RECOVERY_OPEN_CRASH,
        RecoveryOpenCrash, RocksKv, WAL_ONLY_SHUTDOWN, WriterLock,
    },
    txn::TxnError,
    xact::marker_key,
};
use std::{
    fs::OpenOptions,
    path::PathBuf,
    process::Command,
    thread,
    time::{Duration, Instant},
};

const WAIT: Duration = Duration::from_secs(5);
const ORPHAN: [u8; 8] = 99u64.to_be_bytes();
const WRITER_LOCK_HELD: &str = "database already open by another writer";

pub(in crate::storage) trait WriterLockContention {
    fn is_writer_lock_held(&self) -> bool;
}

impl WriterLockContention for KvError {
    fn is_writer_lock_held(&self) -> bool {
        matches!(self, Self::Io(message) if message == WRITER_LOCK_HELD)
    }
}

impl WriterLockContention for CoreError {
    fn is_writer_lock_held(&self) -> bool {
        matches!(self, Self::Storage(message) if message == WRITER_LOCK_HELD)
    }
}

impl WriterLockContention for OpenFailure {
    fn is_writer_lock_held(&self) -> bool {
        self.error.is_writer_lock_held()
    }
}

// See writer_lock_is_inherited_until_child_exec and fence recovery design note
// section 2.2 ("writer lock still held: retry with backoff").
pub(in crate::storage) fn retry_writer_lock<T, E: WriterLockContention>(
    mut open: impl FnMut() -> Result<T, E>,
) -> Result<T, E> {
    let start = Instant::now();
    loop {
        let result = open();
        if !result
            .as_ref()
            .is_err_and(WriterLockContention::is_writer_lock_held)
            || start.elapsed() >= Duration::from_secs(10)
        {
            return result;
        }
        thread::sleep(Duration::from_millis(30));
    }
}

struct TestDir(PathBuf);
impl TestDir {
    fn new() -> Self {
        let mut nonce = [0; 16];
        getrandom::fill(&mut nonce).unwrap();
        Self(std::env::temp_dir().join(format!(
            "fence-recovery-{:032x}",
            u128::from_be_bytes(nonce)
        )))
    }
}
impl Drop for TestDir {
    fn drop(&mut self) {
        if self.0.exists() {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }
}

fn create_wal_only(path: &Path) -> V2Db {
    WAL_ONLY_SHUTDOWN.set(true);
    let db = V2Db::create(path);
    WAL_ONLY_SHUTDOWN.set(false);
    db.unwrap()
}

fn wal(path: &Path) -> PathBuf {
    fs::read_dir(path)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|file| file.extension().is_some_and(|ext| ext == "log"))
        .max()
        .unwrap()
}

fn wal_len(path: &Path) -> u64 {
    fs::metadata(wal(path)).unwrap().len()
}

fn reopen(path: &Path, ack_hw: Ts) -> Result<V2Db, OpenFailure> {
    retry_writer_lock(|| V2Db::open_recovery(path, None, RecoveryOpen { ack_hw }))
}

fn close(db: V2Db) -> Ts {
    let latch = db.close_latch().unwrap();
    let report = db.shutdown();
    assert!(!db.maintenance.running());
    assert!(!db.sequencer.is_accepting());
    assert_eq!(db.shutdown(), report);
    drop(db);
    latch.wait(WAIT).unwrap();
    report.ack_hw
}

fn submit(db: &V2Db, xids: std::ops::RangeInclusive<u64>) -> Result<Ts, TxnError> {
    db.sequencer.submit(CommitRequest {
        snapshot: db.sequencer.visible_ts(),
        writes: [((Keyspace::Kernel, vec![255]), Some(Vec::new()))].into(),
        features_or: 0,
        minimum_subversion: 0,
        raw: xids.map(|xid| RawOp::XactCommitted { xid }).collect(),
        validation: None,
    })
}

fn assert_marker(kv: &dyn KvRead, xid: u64, timestamp: Option<Ts>) {
    assert_eq!(
        kv.get(Keyspace::Seq, &marker_key(xid)).unwrap(),
        timestamp.map(|ts| ts.0.to_be_bytes().to_vec()),
        "xid {xid}",
    );
}

fn assert_acknowledged(kv: &dyn KvRead, commits: &[Ts]) {
    for (index, ts) in commits.iter().enumerate() {
        assert_marker(kv, index as u64 + 1, Some(*ts));
        let key = [&[255], ts.inverted_be().as_slice()].concat();
        assert_eq!(kv.get(Keyspace::Kernel, &key).unwrap(), Some(vec![0]));
    }
}

fn seed_orphan(db: &V2Db) {
    let mut batch = WriteBatch::default();
    batch.put(
        Keyspace::Rows,
        ORPHAN.to_vec(),
        b"reclaim tripwire".to_vec(),
    );
    db.kv.write(batch, Durability::Sync).unwrap();
}

fn assert_orphan(kv: &dyn KvRead) {
    assert_eq!(
        kv.get(Keyspace::Rows, &ORPHAN).unwrap(),
        Some(b"reclaim tripwire".to_vec())
    );
}

fn read_native(path: &Path) -> RocksKv {
    retry_writer_lock(|| RocksKv::open(path, 8 * 1024 * 1024)).unwrap()
}

fn seed_stale_counter(db: &V2Db) -> Vec<u8> {
    use crate::relational::{DataType, catalog_ext::SequenceDef};
    let definition = SequenceDef {
        name: "test_sequence".into(),
        data_type: DataType::BigInt,
        start: 1,
        increment: 1,
        min: 1,
        max: 99,
        cache: 1,
        cycle: false,
        owned_by: None,
        identity: false,
        incarnation: 2,
    };
    db.sequencer
        .submit(CommitRequest {
            snapshot: db.sequencer.visible_ts(),
            writes: [(
                (Keyspace::Catalog, catalog_codec::sequence_key(3)),
                Some(catalog_codec::encode_sequence(&definition).unwrap()),
            )]
            .into(),
            features_or: 64 | 256,
            minimum_subversion: 0,
            raw: Vec::new(),
            validation: None,
        })
        .unwrap();

    let key = catalog_codec::counter_key(3, 1);
    let mut batch = WriteBatch::default();
    batch.put(
        Keyspace::Seq,
        key.clone(),
        crate::storage::encode(&(8i64, true, 8i64, 1u64)).unwrap(),
    );
    db.kv.write(batch, Durability::Sync).unwrap();
    assert_eq!(
        engine::meta::open_checks(db.kv.as_ref())
            .unwrap()
            .stale_counters,
        vec![key.clone()]
    );
    key
}

#[test]
fn mid_wal_corruption_refuses_before_application_writes() {
    use engine::kv::FenceClass;
    let dir = TestDir::new();
    let db = create_wal_only(&dir.0);
    let counter = seed_stale_counter(&db);
    seed_orphan(&db);
    let first = submit(&db, 1..=1).unwrap();
    let middle_start = wal_len(&dir.0);
    let second = submit(&db, 2..=2).unwrap();
    let middle_end = wal_len(&dir.0);
    let third = submit(&db, 3..=3).unwrap();
    assert_eq!(close(db), third);

    let log = wal(&dir.0);
    let original = fs::read(&log).unwrap();
    let middle = (middle_start + (middle_end - middle_start) / 2) as usize;
    let mut damaged = original.clone();
    damaged[middle] ^= 1;
    fs::write(&log, &damaged).unwrap();

    let Err(error) = reopen(&dir.0, first) else {
        panic!("accepted mid-WAL corruption")
    };
    assert_eq!(error.cause.class, FenceClass::Data);
    assert!(matches!(error.error, CoreError::Corrupt(_)));
    assert_eq!(fs::read(&log).unwrap(), damaged);
    fs::write(&log, original).unwrap();

    let raw = read_native(&dir.0);
    assert_acknowledged(&raw, &[first, second, third]);
    assert_orphan(&raw);
    assert!(raw.get(Keyspace::Seq, &counter).unwrap().is_some());
}

#[test]
fn mid_wal_corruption_refuses_with_one_cf_flushed_and_one_empty() {
    use engine::{
        kv::{FenceClass, FenceSite},
        meta,
    };

    let dir = TestDir::new();
    fs::create_dir_all(&dir.0).unwrap();
    let mut native = None;
    WAL_ONLY_SHUTDOWN.set(true);
    let backend = meta::create(&dir.0, [42; 16], |path| {
        let rocks = Arc::new(RocksKv::open(path, 8 << 20)?);
        native = Some(rocks.clone());
        Ok(rocks)
    })
    .unwrap();
    WAL_ONLY_SHUTDOWN.set(false);
    let native = native.unwrap();
    let db = V2Db::from_kv(Arc::new(Kv::new_single_writer(backend))).unwrap();
    let counter = seed_stale_counter(&db);
    let first = submit(&db, 1..=1).unwrap();
    let middle_start = wal_len(&dir.0);
    let second = submit(&db, 2..=2).unwrap();
    let middle_end = wal_len(&dir.0);
    let log = wal(&dir.0);

    // Rows contains a later write in an SST while Index is completely empty.
    // Unflushed commit metadata keeps the damaged WAL in the replay set.
    seed_orphan(&db);
    let third = submit(&db, 3..=3).unwrap();
    native.flush_one_memtable(Keyspace::Rows).unwrap();
    assert!(native.has_sst(Keyspace::Rows));
    assert!(!native.has_sst(Keyspace::Index));
    assert!(db.kv.seek_ge(Keyspace::Index, b"").unwrap().is_none());
    drop(native);
    assert_eq!(close(db), third);

    let original = fs::read(&log).unwrap();
    let mut damaged = original.clone();
    damaged[(middle_start + (middle_end - middle_start) / 2) as usize] ^= 1;
    fs::write(&log, &damaged).unwrap();
    let Err(failure) = reopen(&dir.0, first) else {
        panic!("recovery accepted mid-WAL corruption with asymmetric CFs");
    };
    assert_eq!(
        (failure.cause.site, failure.cause.class),
        (FenceSite::Open, FenceClass::Data)
    );
    assert_eq!(fs::read(&log).unwrap(), damaged);
    fs::write(&log, original).unwrap();

    let raw = read_native(&dir.0);
    assert_acknowledged(&raw, &[first, second, third]);
    assert_orphan(&raw);
    assert!(raw.get(Keyspace::Seq, &counter).unwrap().is_some());
}

#[test]
fn torn_last_wal_record_recovers_all_earlier_groups() {
    let dir = TestDir::new();
    let db = create_wal_only(&dir.0);
    let commits = [submit(&db, 1..=1).unwrap(), submit(&db, 2..=2).unwrap()];
    let before = wal_len(&dir.0);
    submit(&db, 3..=3).unwrap();
    let after = wal_len(&dir.0);
    close(db);

    OpenOptions::new()
        .write(true)
        .open(wal(&dir.0))
        .unwrap()
        .set_len(before + (after - before) / 2)
        .unwrap();
    let recovered = reopen(&dir.0, commits[1]).unwrap();
    assert_eq!(recovered.sequencer.visible_ts(), commits[1]);
    assert_acknowledged(recovered.kv.as_ref(), &commits);
    assert_marker(recovered.kv.as_ref(), 3, None);
}

#[test]
fn missing_acknowledged_group_refuses_before_counter_cleanup() {
    use engine::kv::FenceClass;
    let dir = TestDir::new();
    let db = create_wal_only(&dir.0);
    let counter = seed_stale_counter(&db);
    seed_orphan(&db);
    let earlier = submit(&db, 1..=1).unwrap();
    let before = wal_len(&dir.0);
    let lost = submit(&db, 2..=2).unwrap();
    assert_eq!(close(db), lost);
    // Recreate the WAL image ending at the earlier synced group.
    OpenOptions::new()
        .write(true)
        .open(wal(&dir.0))
        .unwrap()
        .set_len(before)
        .unwrap();

    let Err(error) = reopen(&dir.0, lost) else {
        panic!("accepted missing acknowledged group")
    };
    assert_eq!(error.cause.class, FenceClass::Data);
    assert!(error.cause.error_text.contains("below acknowledged commit"));

    // Use the raw backend so application cleanup cannot hide a write from the failed open.
    let raw = read_native(&dir.0);
    assert_acknowledged(&raw, &[earlier]);
    assert_marker(&raw, 2, None);
    assert!(raw.get(Keyspace::Seq, &counter).unwrap().is_some());
    assert_orphan(&raw);
}

#[test]
fn recovery_flushes_replayed_wal_to_synced_ssts() {
    let dir = TestDir::new();
    let db = create_wal_only(&dir.0);
    let ts = submit(&db, 1..=1).unwrap();
    let log = wal(&dir.0);
    close(db);

    let count_ssts = || {
        fs::read_dir(&dir.0)
            .unwrap()
            .filter(|entry| {
                entry
                    .as_ref()
                    .unwrap()
                    .path()
                    .extension()
                    .is_some_and(|ext| ext == "sst")
            })
            .count()
    };
    assert_eq!(count_ssts(), 0);
    let db = reopen(&dir.0, ts).unwrap();
    assert_acknowledged(db.kv.as_ref(), &[ts]);
    assert!(count_ssts() >= 3);
    assert!(!log.exists(), "the replayed WAL is obsolete");
    assert!(
        fs::read_to_string(dir.0.join("LOG"))
            .unwrap()
            .contains("Recovering log #")
    );
    let options = saved_options(&dir.0);
    for pinned in [
        "avoid_flush_during_recovery=false",
        "recycle_log_file_num=0",
        "manual_wal_flush=false",
        "wal_recovery_mode=kTolerateCorruptedTailRecords",
    ] {
        assert!(options.contains(pinned), "missing RocksDB option {pinned}");
    }
}

fn saved_options(path: &Path) -> String {
    let options = fs::read_dir(path)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|file| {
            file.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("OPTIONS-")
        })
        .max()
        .unwrap();
    fs::read_to_string(options).unwrap()
}

#[test]
fn normal_open_keeps_point_in_time_wal_recovery() {
    let dir = TestDir::new();
    let db = create_wal_only(&dir.0);
    let ts = submit(&db, 1..=1).unwrap();
    close(db);
    let db = retry_writer_lock(|| V2Db::open(&dir.0)).unwrap();
    assert!(saved_options(&dir.0).contains("wal_recovery_mode=kPointInTimeRecovery"));
    assert_acknowledged(db.kv.as_ref(), &[ts]);
}

#[test]
fn partial_gc_batches_preserve_visible_rows_and_later_gc_converges() {
    use engine::{
        codec,
        commit::{ManualClock, Sequencer, SequencerConfig},
        gc::{Gc, GcConfig},
        kv::{Direction, KvBackend},
        mvcc::{self, VersionedRange},
    };
    use std::ops::Bound;

    let visible = Ts(100 << 16);
    let latest = Ts(50 << 16);
    let older = Ts(10 << 16);
    let row = |key| [7u64.to_be_bytes().as_slice(), &[key]].concat();
    let visible_rows = |kv: &dyn KvRead| {
        VersionedRange::new(
            kv,
            Keyspace::Rows,
            Bound::Unbounded,
            Bound::Unbounded,
            Direction::Forward,
            visible,
        )
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
    };
    let raw_rows = |kv: &dyn KvRead| {
        kv.range(
            Keyspace::Rows,
            Bound::Unbounded,
            Bound::Unbounded,
            Direction::Forward,
        )
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
    };
    let open_gc = |backend: Arc<RocksKv>| {
        let kv = Arc::new(Kv::new_single_writer(backend));
        let sequencer = Sequencer::open(
            kv.clone(),
            Arc::new(ManualClock::new(100)),
            SequencerConfig::default(),
        )
        .unwrap();
        let gc = Gc::open(
            kv.clone(),
            sequencer,
            GcConfig {
                retention: Duration::ZERO,
            },
        )
        .unwrap();
        (kv, gc)
    };
    let cursor_key = [Keyspace::Rows as u8];
    for cursor_only in [false, true] {
        let dir = TestDir::new();
        WAL_ONLY_SHUTDOWN.set(true);
        let backend = Arc::new(RocksKv::open(&dir.0, 8 << 20).unwrap());
        WAL_ONLY_SHUTDOWN.set(false);
        let (kv, gc) = {
            let mut seed = WriteBatch::default();
            seed.put(
                Keyspace::Meta,
                b"last_commit_ts".to_vec(),
                visible.0.to_be_bytes().to_vec(),
            );
            for key in [0, 1] {
                for at in [latest, older] {
                    seed.put(
                        Keyspace::Rows,
                        codec::user_key_seek_at(&row(key), at),
                        mvcc::encode_live(&[key]),
                    );
                }
            }
            backend.write(seed, Durability::Sync).unwrap();
            open_gc(backend.clone())
        };
        assert_eq!(gc.advance().unwrap(), visible);
        let expected = vec![(row(0), vec![0]), (row(1), vec![1])];
        assert_eq!(visible_rows(kv.as_ref()), expected);

        // Keep only one part of the first GC step and sync that synthetic
        // fragment to reproduce a partial persistence outcome at restart.
        let mut fragment = WriteBatch::default();
        if cursor_only {
            fragment.put(
                Keyspace::Gc,
                cursor_key.to_vec(),
                codec::user_key_upper_bound(&row(0)),
            );
        } else {
            fragment.delete(Keyspace::Rows, codec::user_key_seek_at(&row(0), older));
        }
        backend.write(fragment, Durability::Sync).unwrap();
        assert_eq!(visible_rows(kv.as_ref()), expected);
        drop((kv, gc, backend));

        let recovered = retry_writer_lock(|| RocksKv::open_recovery(&dir.0, 8 << 20)).unwrap();
        let (kv, gc) = open_gc(Arc::new(recovered));
        assert_eq!(gc.watermark(), visible);
        assert_eq!(
            kv.get(Keyspace::Gc, &cursor_key).unwrap().is_some(),
            cursor_only
        );
        assert_eq!(raw_rows(kv.as_ref()).len(), if cursor_only { 4 } else { 3 });
        assert_eq!(visible_rows(kv.as_ref()), expected);
        assert!(gc.step(Keyspace::Rows, usize::MAX).unwrap().wrapped);
        assert!(gc.step(Keyspace::Rows, usize::MAX).unwrap().wrapped);
        assert_eq!(raw_rows(kv.as_ref()).len(), 2);
        assert_eq!(visible_rows(kv.as_ref()), expected);
    }
}

thread_local! {
    static FAILED_OPEN_LATCH: std::cell::Cell<Option<CloseLatch>> = const { std::cell::Cell::new(None) };
}

#[test]
fn open_reports_recoverable_read_before_maintenance_starts() {
    use engine::kv::{FenceCause, FenceClass, FenceSite, RocksStatus};

    let dir = TestDir::new();
    let ack_hw = close(create_wal_only(&dir.0));
    TEST_OPEN_RECLAIM.set(Some(|db| {
        assert!(!db.maintenance.running());
        FAILED_OPEN_LATCH.set(db.close_latch());
        db.kv.causes.record(FenceCause::for_kv(
            FenceSite::Read,
            &KvError::RocksIo {
                message: "injected startup read IOError".into(),
                status: RocksStatus {
                    code: RocksStatus::IO_ERROR,
                    subcode: 0,
                    severity: None,
                },
                site: FenceSite::Read,
            },
        ));
        assert!(!db.kv.is_fenced());
        Ok(())
    }));
    let Err(failure) = reopen(&dir.0, ack_hw) else {
        panic!("recovery accepted a recorded read failure");
    };
    assert!(matches!(failure.error, CoreError::Storage(_)));
    assert_eq!(failure.cause.class, FenceClass::Recoverable);
    FAILED_OPEN_LATCH
        .take()
        .unwrap()
        .wait(Duration::ZERO)
        .unwrap();
}

#[test]
fn shutdown_joins_and_snapshot_holds_close_latch() {
    let dir = TestDir::new();
    let db = V2Db::create(&dir.0).unwrap();
    let snapshot = db.kv.snapshot().unwrap();
    let latch = db.close_latch().unwrap();
    let ts = submit(&db, 1..=1).unwrap();
    let report = db.shutdown();
    assert_eq!(report.ack_hw, ts);
    assert_eq!(db.shutdown(), report);
    assert_eq!(submit(&db, 2..=2), Err(TxnError::Fenced));
    assert!(!db.maintenance.running());
    assert!(!db.sequencer.is_accepting());
    drop(db);

    assert_eq!(
        latch.wait(Duration::from_millis(30)),
        Err(engine::kv_rocksdb::StillHeld)
    );
    assert!(WriterLock::acquire(&dir.0).is_err());
    snapshot.get(Keyspace::Meta, b"family").unwrap().unwrap();
    drop(snapshot);
    latch.wait(WAIT).unwrap();

    let recovered = reopen(&dir.0, ts).unwrap();
    assert_acknowledged(recovered.kv.as_ref(), &[ts]);
}

#[test]
fn shutdown_reports_commit_published_while_joining() {
    let dir = TestDir::new();
    let db = Arc::new(V2Db::create(&dir.0).unwrap());
    let hook = crate::CommitSyncHook::new();
    db.sequencer.set_sync_hook(Some(hook.clone()));

    let writer = {
        let db = db.clone();
        std::thread::spawn(move || submit(&db, 1..=1))
    };
    if !hook.wait_entered(WAIT) {
        hook.release(false);
        panic!("write did not reach Sync");
    }
    let shutdown = {
        let db = db.clone();
        std::thread::spawn(move || db.shutdown())
    };
    let deadline = std::time::Instant::now() + WAIT;
    while db.sequencer.is_accepting() && std::time::Instant::now() < deadline {
        std::thread::yield_now();
    }
    let stopped = !db.sequencer.is_accepting();
    hook.release(false);
    assert!(stopped, "shutdown did not stop sequencer submissions");
    assert_eq!(
        shutdown.join().unwrap().ack_hw,
        writer.join().unwrap().unwrap()
    );
}

#[test]
fn same_process_recovery_twenty_times() {
    let dir = TestDir::new();
    let mut db = V2Db::create(&dir.0).unwrap();
    let mut commits = Vec::new();
    for xid in 1..=20 {
        commits.push(submit(&db, xid..=xid).unwrap());
        let ack_hw = close(db);
        db = reopen(&dir.0, ack_hw).unwrap();
        assert_eq!(db.sequencer.visible_ts(), ack_hw);
        assert_acknowledged(db.kv.as_ref(), &commits);
    }
}

#[test]
fn recovery_open_preserves_the_write_fence_holder() {
    let dir = TestDir::new();
    let db = V2Db::create(&dir.0).unwrap();
    let token = b"recovery-test-holder";
    let epoch = db
        .sequencer
        .install_write_fence(token, "test fence")
        .unwrap();
    let ack_hw = close(db);

    let db = retry_writer_lock(|| V2Db::open_recovery(&dir.0, Some(token), RecoveryOpen { ack_hw }))
        .unwrap();
    db.sequencer.release_write_fence(token, epoch).unwrap();
}

fn child(name: &str, var: &str, path: &Path) -> Command {
    let mut command = Command::new(std::env::current_exe().unwrap());
    let (_, module) = module_path!().split_once("::").unwrap();
    command
        .args(["--exact", &format!("{module}::{name}"), "--nocapture"])
        .env(var, path);
    command
}

#[test]
fn crash_at_shutdown_and_during_native_recovery_flush() {
    const CHILD: &str = "GIGGLES_TEST_RECOVERY_CRASH";
    const STAGE: &str = "GIGGLES_TEST_RECOVERY_CRASH_STAGE";
    const LAST_XID: &str = "GIGGLES_TEST_RECOVERY_CRASH_LAST_XID";
    const ACKNOWLEDGED: &str = "acknowledged-commits";
    const NAME: &str = "crash_at_shutdown_and_during_native_recovery_flush";
    if let Some(path) = std::env::var_os(CHILD) {
        let path = Path::new(&path);
        let db = create_wal_only(path);
        let last_xid = std::env::var(LAST_XID).unwrap().parse().unwrap();
        let commits = [1..=1, 2..=2, 3..=last_xid].map(|xids| submit(&db, xids).unwrap());
        let ack_hw = close(db);
        assert_eq!(ack_hw, commits[2]);
        fs::write(
            path.join(ACKNOWLEDGED),
            commits.map(|ts| ts.0.to_be_bytes()).concat(),
        )
        .unwrap();
        match std::env::var(STAGE).unwrap().as_str() {
            "closed" => crate::storage::crash_now(),
            "before-native-open" => {
                RECOVERY_OPEN_CRASH.set(Some(RecoveryOpenCrash::BeforeNativeOpen))
            }
            "during-recovery-flush" => {
                #[cfg(target_os = "linux")]
                {
                    // SAFETY: the pointer addresses a live limit for this child.
                    let limit = libc::rlimit {
                        rlim_cur: 0,
                        rlim_max: 0,
                    };
                    assert_eq!(unsafe { libc::setrlimit(libc::RLIMIT_CORE, &limit) }, 0);
                }
                RECOVERY_OPEN_CRASH.set(Some(RecoveryOpenCrash::DuringRecoveryFlush));
                let _ = reopen(path, ack_hw).unwrap();
                std::process::exit(RECOVERY_FLUSH_MISSED);
            }
            "after-native-open" => {
                RECOVERY_OPEN_CRASH.set(Some(RecoveryOpenCrash::AfterNativeOpen))
            }
            _ => unreachable!(),
        }
        let _ = reopen(path, ack_hw);
        panic!("missed crash hook");
    }

    for stage in [
        "closed",
        "before-native-open",
        "during-recovery-flush",
        "after-native-open",
    ] {
        let during_flush = stage == "during-recovery-flush";
        let attempts: &[u64] = if during_flush {
            &[8192, 32768, 131072]
        } else {
            &[3]
        };
        let mut crashed = false;
        for &last_xid in attempts {
            let dir = TestDir::new();
            let status = child(NAME, CHILD, &dir.0)
                .env(STAGE, stage)
                .env(LAST_XID, last_xid.to_string())
                .status()
                .unwrap();
            if during_flush {
                if status.code() == Some(RECOVERY_FLUSH_MISSED) {
                    continue;
                }
                assert!(
                    !status.success() && !matches!(status.code(), Some(86 | 87 | 101)),
                    "{status}"
                );
                assert_eq!(
                    fs::read(dir.0.join(RECOVERY_FLUSH_MARKER)).unwrap(),
                    b"new SST during native open"
                );
            } else {
                assert_eq!(status.code(), Some(86), "{stage}");
            }
            drop(retry_writer_lock(|| WriterLock::acquire(&dir.0)).unwrap());
            let expected = fs::read(dir.0.join(ACKNOWLEDGED)).unwrap();
            assert_eq!(expected.len(), 24);
            let (timestamps, _) = expected.as_chunks::<8>();
            let commits: Vec<_> = timestamps
                .iter()
                .map(|raw| Ts(u64::from_be_bytes(*raw)))
                .collect();
            let db = retry_writer_lock(|| V2Db::open(&dir.0)).unwrap();
            assert_eq!(db.sequencer.visible_ts(), commits[2]);
            assert_acknowledged(db.kv.as_ref(), &commits);
            for xid in 4..=last_xid {
                assert_marker(db.kv.as_ref(), xid, Some(commits[2]));
            }
            assert_marker(db.kv.as_ref(), last_xid + 1, None);
            crashed = true;
            break;
        }
        assert!(
            crashed,
            "native open completed before the watcher fired in every attempt"
        );
    }
}

#[cfg(target_os = "linux")]
fn assert_child_passed(mut command: Command) {
    let output = command.output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success()
            && stdout.lines().any(|line| line == "running 1 test")
            && stdout.lines().any(|line| line.starts_with("test result: ok. 1 passed; 0 failed;")),
        "{}\n{stdout}\n{stderr}",
        output.status
    );
}

#[cfg(target_os = "linux")]
struct MaxFileSize(libc::rlimit);
#[cfg(target_os = "linux")]
impl MaxFileSize {
    #[allow(clippy::unnecessary_cast, reason = "rlim_t can be narrower on Linux")]
    fn new(bytes: u64) -> Self {
        // SAFETY: these calls use live libc rlimit structs. The child has no
        // concurrent limit changes, and restores its original soft limit.
        unsafe {
            let mut previous = libc::rlimit {
                rlim_cur: 0,
                rlim_max: 0,
            };
            assert_eq!(libc::getrlimit(libc::RLIMIT_FSIZE, &mut previous), 0);
            assert_ne!(libc::signal(libc::SIGXFSZ, libc::SIG_IGN), libc::SIG_ERR);
            let limit = libc::rlimit {
                rlim_cur: bytes as libc::rlim_t,
                ..previous
            };
            assert_eq!(libc::setrlimit(libc::RLIMIT_FSIZE, &limit), 0);
            Self(previous)
        }
    }
}
#[cfg(target_os = "linux")]
impl Drop for MaxFileSize {
    fn drop(&mut self) {
        // SAFETY: restore the saved rlimit through a pointer valid for this call.
        assert_eq!(unsafe { libc::setrlimit(libc::RLIMIT_FSIZE, &self.0) }, 0);
    }
}

#[cfg(target_os = "linux")]
#[test]
fn real_partial_wal_append_recovers_multi_cf_batch_atomically() {
    use engine::kv::{FenceClass, FenceSite, RocksStatus};
    const CHILD: &str = "GIGGLES_TEST_RECOVERY_REAL_WAL_FAULT";
    if let Some(path) = std::env::var_os(CHILD) {
        let path = Path::new(&path);
        let db = create_wal_only(path);
        let ts = submit(&db, 1..=1).unwrap();
        let before = wal_len(path);
        let limit = MaxFileSize::new(before + 16);
        assert_eq!(submit(&db, 100..=400), Err(TxnError::CommitAmbiguous));
        assert_eq!(
            wal_len(path),
            before + 16,
            "the OS permitted only a partial append"
        );
        drop(limit);
        assert!(db.kv.is_fenced());
        let cause = db.fence_cause().unwrap();
        assert_eq!(
            (cause.class, cause.site),
            (FenceClass::Recoverable, FenceSite::WalWrite),
            "{cause:?}"
        );
        assert_eq!(cause.status.unwrap().code, RocksStatus::IO_ERROR);
        assert_eq!(close(db), ts);

        let recovered = reopen(path, ts).unwrap();
        assert_acknowledged(recovered.kv.as_ref(), &[ts]);
        let replayed = recovered.sequencer.visible_ts();
        let failed_ts = (replayed > ts).then_some(replayed);
        for xid in 100..=400 {
            assert_marker(recovered.kv.as_ref(), xid, failed_ts);
        }
        let kernel_versions = recovered
            .kv
            .range(
                Keyspace::Kernel,
                std::ops::Bound::Unbounded,
                std::ops::Bound::Unbounded,
                engine::kv::Direction::Forward,
            )
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            kernel_versions.len(),
            if failed_ts.is_some() { 2 } else { 1 }
        );
        let next = submit(&recovered, 2..=2).unwrap();
        assert!(next > replayed);
        return;
    }
    let dir = TestDir::new();
    assert_child_passed(child(
        "real_partial_wal_append_recovers_multi_cf_batch_atomically",
        CHILD,
        &dir.0,
    ));
}

#[cfg(target_os = "linux")]
#[test]
fn real_background_flush_error_refuses_later_write() {
    use engine::kv::{FenceCause, FenceClass, FenceSite, RocksStatus};
    const CHILD: &str = "GIGGLES_TEST_RECOVERY_BACKGROUND_FLUSH_FAULT";
    if let Some(path) = std::env::var_os(CHILD) {
        let backend = Arc::new(RocksKv::open(Path::new(&path), 8 << 20).unwrap());
        let kv = Kv::new_single_writer(backend.clone());
        let mut payload = vec![0; 128 << 10];
        getrandom::fill(&mut payload).unwrap();
        let mut batch = WriteBatch::default();
        batch.put(Keyspace::Rows, b"seed".to_vec(), payload);
        kv.write(batch, Durability::Sync).unwrap();

        let limit = MaxFileSize::new(8 << 10);
        let flush = backend.flush_one_memtable(Keyspace::Rows).unwrap_err();
        let cause = FenceCause::for_kv(FenceSite::BackgroundError, &flush);
        assert_eq!(
            (cause.site, cause.class),
            (FenceSite::BackgroundError, FenceClass::NotRecoverable)
        );
        assert_eq!(cause.status.unwrap().code, RocksStatus::IO_ERROR);
        assert!(
            cause.error_text.contains(".sst") && cause.error_text.contains("File too large"),
            "{cause:?}"
        );

        let mut batch = WriteBatch::default();
        batch.put(
            Keyspace::Rows,
            b"refused".to_vec(),
            b"never applied".to_vec(),
        );
        let refused = kv.write(batch, Durability::Sync).unwrap_err();
        assert_eq!(refused, flush);
        let cause = kv.causes.cause().unwrap();
        assert_eq!(
            (cause.site, cause.class),
            (FenceSite::BackgroundError, FenceClass::NotRecoverable)
        );
        assert!(kv.is_fenced());
        drop(limit);
        assert!(backend.get(Keyspace::Rows, b"refused").unwrap().is_none());
        return;
    }
    let dir = TestDir::new();
    assert_child_passed(child(
        "real_background_flush_error_refuses_later_write",
        CHILD,
        &dir.0,
    ));
}

#[cfg(target_os = "linux")]
thread_local! {
    static FAILED_RECLAIM_SEQUENCER: std::cell::RefCell<Option<Arc<engine::commit::Sequencer>>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(target_os = "linux")]
#[test]
fn reclaim_wal_write_failure_refuses_recovery_and_closes() {
    use engine::kv::{FenceClass, FenceSite};
    const CHILD: &str = "GIGGLES_TEST_RECOVERY_RECLAIM_FAULT";
    if let Some(path) = std::env::var_os(CHILD) {
        let path = Path::new(&path);
        let db = create_wal_only(path);
        let ts = submit(&db, 1..=1).unwrap();
        seed_orphan(&db);
        close(db);
        TEST_OPEN_RECLAIM.set(Some(|db| {
            FAILED_RECLAIM_SEQUENCER.with(|seq| *seq.borrow_mut() = Some(db.sequencer.clone()));
            // DB::Open has completed, including native recovery flush. The new
            // WAL is empty. Reclaim's Sync delete hits the real OS file limit.
            let _limit = MaxFileSize::new(1);
            db.reclaim_background()
        }));
        let Err(error) = reopen(path, ts) else {
            panic!("recovery served a fenced engine")
        };
        assert_eq!(error.error, CoreError::Fenced);
        assert_eq!(
            (error.cause.class, error.cause.site),
            (FenceClass::Recoverable, FenceSite::WalWrite),
            "{error:?}"
        );
        let seq = FAILED_RECLAIM_SEQUENCER.with(|seq| seq.borrow_mut().take().unwrap());
        assert!(
            !seq.is_accepting(),
            "failed recovery must stop the sequencer even while held"
        );
        let latch = seq.kv().close_latch().unwrap();
        assert_eq!(
            latch.wait(Duration::ZERO),
            Err(engine::kv_rocksdb::StillHeld)
        );
        drop(seq);
        latch.wait(Duration::ZERO).unwrap();
        let db = reopen(path, ts).unwrap();
        assert_acknowledged(db.kv.as_ref(), &[ts]);
        return;
    }
    let dir = TestDir::new();
    assert_child_passed(child(
        "reclaim_wal_write_failure_refuses_recovery_and_closes",
        CHILD,
        &dir.0,
    ));
}
