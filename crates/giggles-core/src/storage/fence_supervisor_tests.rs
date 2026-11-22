use super::*;
use crate::{
    Admin, CoreError, Engine, FenceSite, Holder, Limits, SqlSession, StorageStatus,
    relational::{SqlResult, Value::Int},
    storage::engine::{
        kv::{KvError, RocksStatus},
        kv_rocksdb::{CloseLatch, WriterLock},
    },
};
use std::{
    collections::BTreeSet,
    path::PathBuf,
    sync::{Barrier, OnceLock, atomic::AtomicUsize, mpsc},
};

const WAIT: Duration = Duration::from_secs(15);

struct Directory(PathBuf);

impl Directory {
    fn new() -> Self {
        let mut id = [0; 16];
        getrandom::fill(&mut id).unwrap();
        Self(
            std::env::temp_dir().join(format!("fence-supervisor-{:032x}", u128::from_be_bytes(id))),
        )
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        if self.0.exists() {
            std::fs::remove_dir_all(&self.0).unwrap();
        }
    }
}

fn fixture() -> (Directory, Admin) {
    let dir = Directory::new();
    let admin = Admin::create_engine(&dir.0, Limits::default(), Engine::V2).unwrap();
    sql(
        &mut admin.sql_session(),
        "CREATE TABLE t(id BIGINT PRIMARY KEY)",
    )
    .unwrap();
    (dir, admin)
}

fn slot(admin: &Admin) -> &EngineSlot {
    let Backend::V2(slot) = &admin.inner.backend else {
        panic!("expected V2")
    };
    slot
}

fn settings() -> FenceRecovery {
    FenceRecovery {
        enabled: true,
        ..FenceRecovery::default()
    }
}

fn sql(session: &mut SqlSession, text: &str) -> Result<SqlResult, CoreError> {
    session.execute(text, &[], None)
}

fn ids(admin: &Admin) -> BTreeSet<i64> {
    ids_from(admin, "t")
}

fn ids_from(admin: &Admin, table: &str) -> BTreeSet<i64> {
    sql(&mut admin.sql_session(), &format!("SELECT id FROM {table}"))
        .unwrap()
        .rows
        .into_iter()
        .map(|row| {
            let [Int(id)] = row.values.as_slice() else {
                panic!("unexpected row: {row:?}")
            };
            *id
        })
        .collect()
}

fn wait_until(mut condition: impl FnMut() -> bool) {
    let start = Instant::now();
    while !condition() {
        assert!(start.elapsed() < WAIT, "timed out waiting for supervisor");
        thread::sleep(Duration::from_millis(1));
    }
}

fn wait_for(admin: &Admin, mut matches: impl FnMut(StorageStatus) -> bool) {
    wait_until(|| matches(admin.storage_status()));
}

fn serving(admin: &Admin, expected: u64) {
    wait_for(
        admin,
        |s| matches!(s, StorageStatus::Serving { generation, fenced: false, .. } if generation == expected),
    );
}

fn recovering(admin: &Admin) {
    wait_for(admin, |s| {
        matches!(s, StorageStatus::Recovering { generation: 1, .. })
    });
}

fn gave_up(admin: &Admin) -> (FenceCause, u32) {
    let mut result = None;
    wait_for(admin, |s| {
        if let StorageStatus::GaveUp {
            cause, attempts, ..
        } = s
        {
            result = Some((cause, attempts));
            true
        } else {
            false
        }
    });
    result.unwrap()
}

fn wal_error() -> KvError {
    KvError::RocksIo {
        message: "injected WAL error".into(),
        status: RocksStatus { code: RocksStatus::IO_ERROR, subcode: 0, severity: None },
        site: FenceSite::WalWrite,
    }
}

fn terminal_error(class: FenceClass) -> KvError {
    match class {
        FenceClass::Data => KvError::Corrupt("injected corruption".into()),
        FenceClass::NotRecoverable => KvError::Io("injected unclassified I/O error".into()),
        FenceClass::Recoverable => unreachable!("terminal error required"),
    }
}

fn fence(admin: &Admin, class: FenceClass) {
    let error = match class {
        FenceClass::Recoverable => wal_error(),
        terminal => terminal_error(terminal),
    };
    admin.test_v2().kv.test_report_write_error(error);
}

fn assert_fenced(admin: &Admin) {
    assert!(matches!(
        sql(&mut admin.sql_session(), "SELECT id FROM t"),
        Err(CoreError::Fenced)
    ));
}

fn ambiguous(error: &CoreError) -> bool {
    *error == CoreError::Storage("engine commit outcome unknown".into())
}

#[derive(Default)]
struct TestRuntime {
    elapsed: Arc<Mutex<Duration>>,
    origin: OnceLock<Instant>,
    wait: Option<Box<dyn Fn(Duration) -> bool + Send>>,
    idle: Option<mpsc::Sender<()>>,
    open: Option<Box<OpenHook>>,
    before_install: Option<Box<InstallHook>>,
    after_success_recorded: Option<mpsc::Sender<u64>>,
    jitter: f64,
}

type InstallHook = dyn Fn(&RecoveryCandidate) + Send;
type OpenHook = dyn Fn(&RecoveryOpenRequest) -> Result<RecoveryCandidate, OpenFailure> + Send;

impl Runtime for TestRuntime {
    fn now(&self) -> Instant {
        *self.origin.get_or_init(Instant::now) + *self.elapsed.lock().unwrap()
    }

    fn wait(&self, wake: &Wake, observed: u64, duration: Option<Duration>) {
        if duration.is_none()
            && let Some(idle) = &self.idle
        {
            let _ = idle.send(());
        }
        if let Some(duration) =
            duration.filter(|duration| self.wait.as_ref().is_none_or(|wait| wait(*duration)))
        {
            *self.elapsed.lock().unwrap() += duration;
        } else {
            SystemRuntime.wait(wake, observed, duration);
        }
    }

    fn jitter(&self) -> f64 {
        self.jitter
    }

    fn open(&self, request: &RecoveryOpenRequest) -> Result<RecoveryCandidate, OpenFailure> {
        match &self.open {
            Some(open) => open(request),
            None => request.open(),
        }
    }

    fn before_install(&self, candidate: &RecoveryCandidate) {
        if let Some(hook) = &self.before_install {
            hook(candidate);
        }
    }
    fn after_success_recorded(&self, generation: u64) {
        if let Some(recorded) = &self.after_success_recorded {
            recorded.send(generation).unwrap();
        }
    }

}

struct Worker(Arc<Wake>, Option<thread::JoinHandle<()>>);

impl Worker {
    fn start(admin: &Admin, settings: FenceRecovery, runtime: TestRuntime) -> Self {
        let mut worker = None;
        slot(admin)
            .start_recovery(settings, |wake| {
                let handle = launch(
                    Arc::downgrade(&admin.inner),
                    Arc::clone(&wake),
                    settings,
                    runtime,
                )?;
                worker = Some(Self(wake, Some(handle)));
                Ok(())
            })
            .unwrap();
        worker.unwrap()
    }

    fn join(mut self) {
        let handle = self.1.take().unwrap();
        wait_until(|| handle.is_finished());
        handle.join().unwrap();
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        if let Some(handle) = self.1.take() {
            self.0.stop();
            if !thread::panicking() {
                handle.join().unwrap();
            }
        }
    }
}

fn open_failure(kind: FenceClass) -> OpenFailure {
    match kind {
        FenceClass::Recoverable => KvError::RocksIo {
            message: "injected IOError".into(),
            status: RocksStatus {
                code: RocksStatus::IO_ERROR,
                subcode: 0,
                severity: None,
            },
            site: FenceSite::Open,
        }
        .into(),
        FenceClass::Data => KvError::Corrupt("injected WAL corruption".into()).into(),
        FenceClass::NotRecoverable => unreachable!(),
    }
}

#[test]
fn recoverable_fence_cannot_opt_in_after_stop() {
    let (_dir, admin) = fixture();
    let db = admin.test_v2();
    fence(&admin, FenceClass::Recoverable);
    admin.stop_submissions();
    assert!(!db.sequencer.is_accepting());
    assert_eq!(admin.set_fence_recovery(settings()), Err(CoreError::Fenced));
    assert_eq!(
        slot(&admin).start_recovery(settings(), |_| panic!("spawned on a stopped slot")),
        Err(CoreError::Fenced)
    );
    assert!(!slot(&admin).recovery_enabled());
    assert_eq!(db.fence_cause().unwrap().generation, 0);
    assert!(slot(&admin).admit_recovery(1, true).is_none());
    assert!(!db.sequencer.is_accepting());
    assert_fenced(&admin);
    assert!(matches!(admin.storage_status(), StorageStatus::Serving { generation: 1, .. }));
}

#[test]
fn enabled_stop_retains_engine_with_maintenance_parked() {
    stop_retains_engine_with_maintenance_parked(false);
}

#[test]
fn stop_during_recovery_retains_engine_with_maintenance_parked() {
    stop_retains_engine_with_maintenance_parked(true);
}

fn stop_retains_engine_with_maintenance_parked(during_recovery: bool) {
    let (_dir, admin) = fixture();
    let worker = Worker::start(
        &admin,
        settings(),
        TestRuntime {
            wait: Some(Box::new(|_| false)),
            ..TestRuntime::default()
        },
    );
    let (entered, parked) = mpsc::channel();
    let (release, resume) = mpsc::channel();
    let db = admin.test_v2();
    let latch = db.close_latch().unwrap();
    db.test_before_maintenance_tick(move || {
        entered.send(()).unwrap();
        resume.recv_timeout(WAIT).unwrap();
    });
    let lease = during_recovery.then_some(db);
    parked.recv_timeout(WAIT).unwrap();
    if during_recovery {
        fence(&admin, FenceClass::Recoverable);
        recovering(&admin);
    }

    let (stopped, returned) = mpsc::channel();
    let stop = thread::spawn({
        let admin = admin.clone();
        move || {
            admin.stop_submissions();
            let _ = stopped.send(());
        }
    });
    let returned_while_parked = returned.recv_timeout(Duration::from_secs(2));
    let start = Instant::now();
    let handle = worker.1.as_ref().unwrap();
    while !handle.is_finished() && start.elapsed() < Duration::from_secs(2) {
        thread::sleep(Duration::from_millis(1));
    }
    let worker_exited_while_parked = handle.is_finished();
    release.send(()).unwrap();
    stop.join().unwrap();
    worker.join();
    drop(lease);
    assert!(returned_while_parked.is_ok(), "stop joined the parked maintenance thread");
    assert!(worker_exited_while_parked, "supervisor dropped the last engine reference");
    assert_eq!(admin.storage_status(), StorageStatus::Stopping);
    assert_eq!(admin.set_fence_recovery(settings()), Err(CoreError::Fenced));
    assert_fenced(&admin);
    assert!(latch.wait(Duration::ZERO).is_err(), "generation closed before Admin drop");
    drop(admin);
    latch.wait(WAIT).unwrap();
}

#[cfg(any(target_os = "linux", windows))]
fn recovery_refuses_linked_writer_lock(symbolic: bool) {
    use std::fs;

    let dir = Directory::new();
    let path = dir.0.join("database");
    let sibling = dir.0.join("sibling");
    let bytes = b"unrelated sibling data\n";
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    fs::write(&sibling, bytes).unwrap();
    let lock = path.join("giggles.lock");
    let worker = Worker::start(
        &admin,
        settings(),
        TestRuntime {
            open: Some(Box::new(move |request| {
                fs::remove_file(&lock).unwrap();
                if symbolic {
                    #[cfg(target_os = "linux")]
                    std::os::unix::fs::symlink(&sibling, &lock).unwrap();
                    #[cfg(windows)]
                    std::os::windows::fs::symlink_file(&sibling, &lock).unwrap();
                } else {
                    fs::hard_link(&sibling, &lock).unwrap();
                }
                request.open()
            })),
            ..TestRuntime::default()
        },
    );
    fence(&admin, FenceClass::Recoverable);
    wait_for(&admin, |status| {
        matches!(
            status,
            StorageStatus::GaveUp { .. } | StorageStatus::Serving { generation: 2, .. }
        )
    });
    let status = admin.storage_status();
    if matches!(status, StorageStatus::GaveUp { .. }) {
        assert_fenced(&admin);
    }
    drop(worker);
    drop(admin);
    assert_eq!(fs::read(dir.0.join("sibling")).unwrap(), bytes);
    assert!(matches!(
        status,
        StorageStatus::GaveUp {
            attempts: 1,
            cause: FenceCause { site: FenceSite::Open, class: FenceClass::Data, .. },
            ..
        }
    ), "{status:?}");
}

#[cfg(any(target_os = "linux", windows))]
#[test]
fn recovery_refuses_symlinked_writer_lock() {
    #[cfg(windows)]
    {
        let probe = Directory::new();
        std::fs::create_dir(&probe.0).unwrap();
        let target = probe.0.join("target");
        std::fs::write(&target, b"").unwrap();
        if let Err(error) = std::os::windows::fs::symlink_file(target, probe.0.join("link")) {
            if error.raw_os_error() == Some(1314) {
                eprintln!("skipping Windows symlink test: {error}");
                return;
            }
            panic!("symlink probe: {error}");
        }
    }
    recovery_refuses_linked_writer_lock(true);
}

#[cfg(any(target_os = "linux", windows))]
#[test]
fn recovery_refuses_hard_linked_writer_lock() {
    recovery_refuses_linked_writer_lock(false);
}

#[cfg(any(target_os = "linux", windows))]
#[test]
fn recovery_skips_an_entry_removed_after_enumeration() {
    use crate::storage::directory_identity::TEST_BEFORE_RECOVERY_ENTRY_METADATA;

    const OBSOLETE: &str = "obsolete.sst";
    let (dir, admin) = fixture();
    let obsolete = dir.0.join(OBSOLETE);
    std::fs::write(&obsolete, b"obsolete").unwrap();
    let opens = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&opens);
    let _worker = Worker::start(
        &admin,
        settings(),
        TestRuntime {
            wait: Some(Box::new(|_| false)),
            open: Some(Box::new(move |request| {
                observed.fetch_add(1, Ordering::Relaxed);
                TEST_BEFORE_RECOVERY_ENTRY_METADATA.set(Some(|path| {
                    if path.file_name().is_some_and(|name| name == OBSOLETE) {
                        std::fs::remove_file(path)?;
                    }
                    Ok(())
                }));
                let opened = request.open();
                TEST_BEFORE_RECOVERY_ENTRY_METADATA.set(None);
                opened
            })),
            ..TestRuntime::default()
        },
    );
    fence(&admin, FenceClass::Recoverable);
    serving(&admin, 2);
    assert!(!obsolete.exists());
    assert_eq!(opens.load(Ordering::Relaxed), 1);
}

#[cfg(any(target_os = "linux", windows))]
#[test]
fn transient_link_scan_io_error_backs_off_and_retries() {
    use crate::storage::directory_identity::TEST_BEFORE_RECOVERY_ENTRY_METADATA;
    use std::io;

    let (_dir, admin) = fixture();
    let opens = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&opens);
    let (waiting, waited) = mpsc::channel();
    fence(&admin, FenceClass::Recoverable);
    let _worker = Worker::start(
        &admin,
        settings(),
        TestRuntime {
            wait: Some(Box::new(move |duration| {
                waiting.send(duration).unwrap();
                true
            })),
            open: Some(Box::new(move |request| {
                if observed.fetch_add(1, Ordering::Relaxed) != 0 {
                    return request.open();
                }
                TEST_BEFORE_RECOVERY_ENTRY_METADATA.set(Some(|_| {
                    Err(io::Error::new(io::ErrorKind::PermissionDenied, "injected scan error"))
                }));
                let failure = request.open().err().expect("the scan hook rejected the open");
                TEST_BEFORE_RECOVERY_ENTRY_METADATA.set(None);
                assert_eq!(failure.cause.site, FenceSite::Open);
                assert_eq!(failure.cause.class, FenceClass::Recoverable);
                assert!(failure.cause.error_text.contains("injected scan error"));
                Err(failure)
            })),
            ..TestRuntime::default()
        },
    );
    serving(&admin, 2);
    assert_eq!(opens.load(Ordering::Relaxed), 2);
    assert_eq!(waited.try_iter().collect::<Vec<_>>(), [settings().backoff_base * 2]);
}

#[test]
fn default_off_keeps_fenced_generation() {
    let (_dir, admin) = fixture();
    assert!(!slot(&admin).recovery_enabled());
    fence(&admin, FenceClass::Recoverable);
    assert_fenced(&admin);
    assert!(matches!(
        admin.storage_status(),
        StorageStatus::Serving {
            generation: 1,
            fenced: true,
            cause: Some(FenceCause { generation: 0, .. }),
            ..
        }
    ));
    assert!(!slot(&admin).recovery_enabled());
}

#[test]
fn recovers_acknowledged_commits_and_rejects_old_transaction() {
    let (_dir, admin) = fixture();
    admin.set_fence_recovery(settings()).unwrap();
    let old_generation = admin.test_v2().tag();
    let mut old = admin.sql_session();
    sql(&mut old, "INSERT INTO t VALUES (1), (2)").unwrap();
    sql(&mut old, "BEGIN").unwrap();
    sql(&mut old, "SELECT id FROM t").unwrap();

    let hook = crate::CommitSyncHook::new();
    admin.set_commit_sync_hook(Some(Arc::clone(&hook)));
    let mut client = admin.sql_session();
    let writer = thread::spawn(move || sql(&mut client, "INSERT INTO t VALUES (3), (4)").map(drop));
    let at_sync = hook.wait_entered(WAIT);
    hook.release_with_wal_error(true);
    assert!(at_sync);
    assert!(ambiguous(&writer.join().unwrap().unwrap_err()));
    recovering(&admin);
    assert!(matches!(
        sql(&mut old, "SELECT id FROM t"),
        Err(CoreError::Fenced)
    ));
    drop(old);
    admin.set_commit_sync_hook(None);
    serving(&admin, 2);
    assert!(matches!(
        old_generation.lease(Holder::Transaction),
        Err(CoreError::Fenced)
    ));
    let rows = ids(&admin);
    assert_eq!(rows, BTreeSet::from([1, 2, 3, 4]));
    sql(&mut admin.sql_session(), "INSERT INTO t VALUES (5)").unwrap();
    assert!(ids(&admin).contains(&5));
}

#[test]
fn repeated_io_failures_wait_exponentially_and_give_up_at_limit() {
    let (_dir, admin) = fixture();
    let delays = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&delays);
    // Drain and stall waits can precede the first open on a loaded machine;
    // only waits after an open attempt are backoff.
    let opened = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let opened_in_wait = Arc::clone(&opened);
    let _worker = Worker::start(
        &admin,
        settings(),
        TestRuntime {
            open: Some(Box::new(move |_| {
                opened.store(true, std::sync::atomic::Ordering::SeqCst);
                Err(open_failure(FenceClass::Recoverable))
            })),
            wait: Some(Box::new(move |delay| {
                if opened_in_wait.load(std::sync::atomic::Ordering::SeqCst) {
                    observed.lock().unwrap().push(delay);
                }
                true
            })),
            jitter: 1.0,
            ..TestRuntime::default()
        },
    );
    fence(&admin, FenceClass::Recoverable);
    let (cause, attempts) = gave_up(&admin);
    assert_eq!(attempts, 5);
    assert_eq!(cause.class, FenceClass::Recoverable);
    assert_eq!(cause.generation, 1);
    assert!(cause.error_text.contains("injected IOError"));
    assert_eq!(
        *delays.lock().unwrap(),
        [2400, 4800, 9600, 19200].map(Duration::from_millis)
    );
    assert_fenced(&admin);
}

#[test]
fn terminal_serving_fences_stay_serving_with_zero_opens() {
    for class in [FenceClass::Data, FenceClass::NotRecoverable] {
        let (_dir, admin) = fixture();
        let (idle, parked) = mpsc::channel();
        let opens = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&opens);
        let worker = Worker::start(
            &admin,
            settings(),
            TestRuntime {
                idle: Some(idle),
                open: Some(Box::new(move |_| {
                    observed.fetch_add(1, Ordering::Relaxed);
                    Err(open_failure(FenceClass::Data))
                })),
                ..TestRuntime::default()
            },
        );
        parked.recv_timeout(WAIT).unwrap();
        fence(&admin, class);
        parked.recv_timeout(WAIT).unwrap();
        assert!(matches!(admin.storage_status(), StorageStatus::Serving {
            generation: 1, fenced: true, cause: Some(FenceCause { class: recorded, .. }), ..
        } if recorded == class));
        assert_fenced(&admin);
        assert_eq!(opens.load(Ordering::Relaxed), 0);
        drop(worker);
    }
}

#[test]
fn read_io_error_keeps_serving_until_a_write_fences_the_same_generation() {
    read_io_error_keeps_serving_until_fenced(|admin| fence(admin, FenceClass::Recoverable));
}

#[test]
fn sequencer_fence_wakes_recovery_after_a_read_io_error() {
    read_io_error_keeps_serving_until_fenced(|admin| {
        let hook = crate::CommitSyncHook::new();
        hook.release(true);
        admin.set_commit_sync_hook(Some(hook));
        let error = sql(&mut admin.sql_session(), "INSERT INTO t VALUES (2)").unwrap_err();
        assert!(ambiguous(&error));
    });
}

fn read_io_error_keeps_serving_until_fenced(trigger: impl FnOnce(&Admin)) {
    let (_dir, admin) = fixture();
    let (idle, parked) = mpsc::channel();
    let opens = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&opens);
    let _worker = Worker::start(
        &admin,
        settings(),
        TestRuntime {
            idle: Some(idle),
            wait: Some(Box::new(|_| false)),
            open: Some(Box::new(move |request| {
                observed.fetch_add(1, Ordering::Relaxed);
                request.open()
            })),
            ..TestRuntime::default()
        },
    );
    parked.recv_timeout(WAIT).unwrap();
    admin.test_v2().kv.test_report_read_error(&wal_error());
    parked.recv_timeout(WAIT).unwrap();
    assert!(matches!(
        admin.storage_status(),
        StorageStatus::Serving {
            generation: 1,
            fenced: false,
            cause: Some(FenceCause { class: FenceClass::Recoverable, site: FenceSite::Read, .. }),
            ..
        }
    ));
    sql(&mut admin.sql_session(), "INSERT INTO t VALUES (1)").unwrap();
    assert_eq!(ids(&admin), BTreeSet::from([1]));
    assert_eq!(opens.load(Ordering::Relaxed), 0);

    trigger(&admin);
    serving(&admin, 2);
    admin.set_commit_sync_hook(None);
    assert_eq!(opens.load(Ordering::Relaxed), 1);
    assert_eq!(ids(&admin), BTreeSet::from([1]));
}

#[test]
fn supervisor_panic_gives_up_the_admitted_generation() {
    let (_dir, admin) = fixture();
    let worker = Worker::start(
        &admin,
        settings(),
        TestRuntime {
            wait: Some(Box::new(|_| false)),
            open: Some(Box::new(|_| panic!("injected reopen panic"))),
            ..TestRuntime::default()
        },
    );
    fence(&admin, FenceClass::Recoverable);
    worker.join();
    let (cause, attempts) = gave_up(&admin);
    assert_eq!(attempts, 1);
    assert_eq!(cause.generation, 1);
    assert_eq!(cause.class, FenceClass::NotRecoverable);
    assert_eq!(cause.error_text, "recovery supervisor panicked: injected reopen panic");
    assert_fenced(&admin);
}

#[test]
fn terminal_override_during_drain_gives_up_without_reopen() {
    for class in [FenceClass::Data, FenceClass::NotRecoverable] {
        let (_dir, admin) = fixture();
        let _worker = Worker::start(
            &admin,
            settings(),
            TestRuntime {
                wait: Some(Box::new(|_| false)),
                open: Some(Box::new(|_| panic!("terminal override must never open"))),
                ..TestRuntime::default()
            },
        );
        let hold = admin.test_v2();
        fence(&admin, FenceClass::Recoverable);
        recovering(&admin);
        hold.kv.test_report_read_error(&terminal_error(class));
        let (cause, attempts) = gave_up(&admin);
        assert_eq!((cause.class, attempts), (class, 0));
        assert_fenced(&admin);
    }
}

#[test]
fn data_open_failure_and_unhealthy_candidate_preserve_classification() {
    for (open, count, final_class) in [
        (
            Box::new(|_: &RecoveryOpenRequest| Err(open_failure(FenceClass::Data)))
                as Box<OpenHook>,
            1,
            FenceClass::Data,
        ),
        (
            Box::new(|request: &RecoveryOpenRequest| request.open()) as Box<OpenHook>,
            1,
            FenceClass::Data,
        ),
    ] {
        let (_dir, admin) = fixture();
        let _worker = Worker::start(
            &admin,
            settings(),
            TestRuntime {
                open: Some(open),
                before_install: Some(Box::new(|candidate| {
                    candidate.db().kv.test_report_read_error(&terminal_error(FenceClass::Data));
                })),
                ..TestRuntime::default()
            },
        );
        fence(&admin, FenceClass::Recoverable);
        let (cause, attempts) = gave_up(&admin);
        assert_eq!(
            (attempts, cause.class, cause.generation),
            (count, final_class, 1)
        );
        assert_fenced(&admin);
    }

    let (_dir, admin) = fixture();
    let opens = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&opens);
    let _worker = Worker::start(
        &admin,
        settings(),
        TestRuntime {
            before_install: Some(Box::new(move |candidate| {
                if observed.fetch_add(1, Ordering::Relaxed) == 0 {
                    candidate.db().kv.test_report_write_error(wal_error());
                }
            })),
            ..TestRuntime::default()
        },
    );
    fence(&admin, FenceClass::Recoverable);
    serving(&admin, 2);
    assert_eq!(opens.load(Ordering::Relaxed), 2);
}

#[test]
fn recorded_duplicate_and_stale_causes_do_not_reopen() {
    let (_dir, admin) = fixture();
    let opens = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&opens);
    let worker = Worker::start(
        &admin,
        settings(),
        TestRuntime {
            open: Some(Box::new(move |request| {
                observed.fetch_add(1, Ordering::Relaxed);
                request.open()
            })),
            wait: Some(Box::new(|_| false)),
            ..TestRuntime::default()
        },
    );
    let lease = admin.test_v2();
    let stale = Arc::clone(&lease.kv.causes);
    fence(&admin, FenceClass::Recoverable);
    stale.record(FenceCause::new(
        FenceSite::WalWrite,
        FenceClass::Recoverable,
        &"duplicate",
    ));
    worker.0.fenced(1);
    drop(lease);
    serving(&admin, 2);
    stale.record(FenceCause::new(FenceSite::Read, FenceClass::Data, &"stale"));
    fence(&admin, FenceClass::Recoverable);
    serving(&admin, 3);
    assert_eq!(opens.load(Ordering::Relaxed), 2);
}

#[test]
fn stalled_drain_reports_holder_and_resumes_only_after_release() {
    let (_dir, admin) = fixture();
    let (waited, wait_seen) = mpsc::channel();
    let _worker = Worker::start(
        &admin,
        FenceRecovery {
            drain_timeout: Duration::ZERO,
            ..settings()
        },
        TestRuntime {
            wait: Some(Box::new(move |duration| {
                waited.send(duration).unwrap();
                false
            })),
            ..TestRuntime::default()
        },
    );
    let hold = slot(&admin).fresh(Holder::Transaction).unwrap();
    let latch = hold.close_latch().unwrap();
    fence(&admin, FenceClass::Recoverable);
    assert_eq!(
        wait_seen.recv_timeout(WAIT).unwrap(),
        settings().stall_log_interval
    );
    assert!(
        matches!(admin.storage_status(), StorageStatus::Recovering { holders, .. }
        if holders == vec![(Holder::Transaction, 1)])
    );
    assert!(latch.wait(Duration::ZERO).is_err());
    assert_fenced(&admin);
    drop(hold);
    latch.wait(WAIT).unwrap();
    serving(&admin, 2);
}

#[test]
fn stall_limit_gives_up_without_closing_beneath_live_holder() {
    let (_dir, admin) = fixture();
    let _worker = Worker::start(
        &admin,
        FenceRecovery {
            stall_limit: Duration::from_secs(12),
            ..settings()
        },
        TestRuntime::default(),
    );
    let hold = slot(&admin).fresh(Holder::Transaction).unwrap();
    let latch = hold.close_latch().unwrap();
    fence(&admin, FenceClass::Recoverable);
    let (cause, attempts) = gave_up(&admin);
    assert_eq!(attempts, 0);
    assert!(cause.error_text.contains("stalled drain at leases"));
    assert!(
        matches!(admin.storage_status(), StorageStatus::GaveUp { holders, .. }
        if holders == vec![(Holder::Transaction, 1)])
    );
    assert!(latch.wait(Duration::ZERO).is_err());
    drop(hold);
    latch.wait(WAIT).unwrap();
}

#[test]
fn close_latch_waits_for_unregistered_native_snapshot() {
    let (_dir, admin) = fixture();
    let lease = admin.test_v2();
    let snapshot = lease.kv.snapshot().unwrap();
    let latch = lease.close_latch().unwrap();
    drop(lease);
    let (wait_tx, wait_rx) = mpsc::channel();
    let _worker = Worker::start(
        &admin,
        FenceRecovery {
            close_timeout: Duration::ZERO,
            ..settings()
        },
        TestRuntime {
            wait: Some(Box::new(move |duration| {
                wait_tx.send(duration).unwrap();
                false
            })),
            ..TestRuntime::default()
        },
    );
    fence(&admin, FenceClass::Recoverable);
    assert_eq!(
        wait_rx.recv_timeout(WAIT).unwrap(),
        settings().stall_log_interval
    );
    assert!(latch.wait(Duration::ZERO).is_err());
    assert_fenced(&admin);
    drop(snapshot);
    serving(&admin, 2);
}

#[test]
fn readers_and_autocommit_writers_cross_recovery_without_wrong_results() {
    let (_dir, admin) = fixture();
    admin.set_fence_recovery(settings()).unwrap();
    sql(&mut admin.sql_session(), "INSERT INTO t VALUES (0)").unwrap();
    let hold = admin.test_v2();
    let start = Arc::new(Barrier::new(5));
    let (tx, rx) = mpsc::channel();
    let mut children = Vec::new();
    for worker in 0..4 {
        let admin = admin.clone();
        let (start, tx) = (Arc::clone(&start), tx.clone());
        children.push(thread::spawn(move || {
            let mut acknowledged = Vec::new();
            let mut uncertain = Vec::new();
            let mut session = admin.sql_session();
            for phase in 0..3 {
                start.wait();
                for sample in 0..10 {
                    let id = 1000 * worker + 100 * phase + 2 * sample + 1;
                    match sql(
                        &mut session,
                        &format!("INSERT INTO t VALUES ({id}), ({})", id + 1),
                    ) {
                        Ok(_) => acknowledged.extend([id, id + 1]),
                        Err(CoreError::Fenced) => {}
                        Err(error) if ambiguous(&error) => uncertain.push(id),
                        Err(error) => panic!("unexpected write result: {error:?}"),
                    }
                    match sql(&mut session, "SELECT id FROM t WHERE id = 0") {
                        Ok(result) => assert!(
                            matches!(result.rows.as_slice(), [row] if row.values == [Int(0)])
                        ),
                        Err(CoreError::Fenced) => {}
                        Err(error) => panic!("unexpected read result: {error:?}"),
                    }
                }
                tx.send(phase).unwrap();
            }
            (acknowledged, uncertain)
        }));
    }
    start.wait();
    fence(&admin, FenceClass::Recoverable);
    recovering(&admin);
    for _ in 0..4 {
        assert_eq!(rx.recv_timeout(WAIT).unwrap(), 0);
    }
    start.wait();
    for _ in 0..4 {
        assert_eq!(rx.recv_timeout(WAIT).unwrap(), 1);
    }
    drop(hold);
    serving(&admin, 2);
    start.wait();
    let results: Vec<_> = children
        .into_iter()
        .map(|child| child.join().unwrap())
        .collect();
    let rows = ids(&admin);
    for (acknowledged, uncertain) in results {
        assert!(acknowledged.into_iter().all(|id| rows.contains(&id)));
        for id in uncertain {
            assert_eq!(rows.contains(&id), rows.contains(&(id + 1)));
        }
    }
}

#[test]
fn parked_commit_can_sync_after_recovery_admission() {
    let (_dir, admin) = fixture();
    admin.set_fence_recovery(settings()).unwrap();
    let latch = admin.test_v2().close_latch().unwrap();
    let hook = crate::CommitSyncHook::new();
    admin.set_commit_sync_hook(Some(Arc::clone(&hook)));
    let mut session = admin.sql_session();
    let (released, release_seen) = mpsc::channel();
    let client = thread::spawn(move || {
        let cancel = crate::Cancellation::default();
        let _registration = cancel.register_durability_wait(move |_, release_admission| {
            release_admission();
            released.send(()).unwrap();
            Some(Box::new(()))
        });
        cancel.scope(|| sql(&mut session, "INSERT INTO t VALUES (1), (2)").map(drop))
    });
    let entered = hook.wait_entered(WAIT);
    let admission_released = release_seen.recv_timeout(WAIT);
    let live_operations = admin.inner.sql_operations.load(Ordering::Acquire);
    // The read cause is recoverable. The native commit fence then stops new
    // statements while the already accepted commit can still sync through Kv.
    let old = admin.test_v2();
    old.kv.test_report_read_error(&wal_error());
    old.fence_native_commit(&admin.inner.fenced);
    drop(old);
    let start = Instant::now();
    while !matches!(admin.storage_status(), StorageStatus::Recovering { .. })
        && start.elapsed() < WAIT
    {
        thread::sleep(Duration::from_millis(1));
    }
    let still_held = latch.wait(Duration::ZERO).is_err();
    let status = admin.storage_status();
    hook.release(false);
    client.join().unwrap().unwrap();
    assert!(entered);
    admission_released.unwrap();
    assert_eq!(live_operations, 0);
    assert!(matches!(status, StorageStatus::Recovering { generation: 1, .. }));
    assert!(still_held);
    admin.set_commit_sync_hook(None);
    serving(&admin, 2);
    assert_eq!(ids(&admin), BTreeSet::from([1, 2]));
}

#[test]
fn real_writer_lock_contention_retries_within_and_beyond_attempt_bound() {
    for hold_past_bound in [false, true] {
        let (dir, admin) = fixture();
        let path = dir.0.clone();
        let holder = Arc::new(Mutex::new(None));
        let lock_for_open = Arc::clone(&holder);
        let delays = Arc::new(Mutex::new(Vec::new()));
        let delays_for_wait = Arc::clone(&delays);
        let opens = Arc::new(AtomicUsize::new(0));
        let opens_for_thread = Arc::clone(&opens);
        let opens_for_wait = Arc::clone(&opens);
        let _worker = Worker::start(
            &admin,
            settings(),
            TestRuntime {
                open: Some(Box::new(move |request| {
                    if opens_for_thread.fetch_add(1, Ordering::Relaxed) == 0 {
                        *lock_for_open.lock().unwrap() = Some(WriterLock::acquire(&path).unwrap());
                    }
                    request.open()
                })),
                wait: Some(Box::new(move |delay| {
                    // Drain and close run before the first reopen and use the
                    // real wake. On a slow box the drain can wait, and that
                    // delay (up to drain_timeout) is not a lock retry delay.
                    if opens_for_wait.load(Ordering::Relaxed) == 0 {
                        return false;
                    }
                    delays_for_wait.lock().unwrap().push(delay);
                    if !hold_past_bound || delay > Duration::from_secs(1) {
                        drop(holder.lock().unwrap().take());
                    }
                    true
                })),
                ..TestRuntime::default()
            },
        );
        fence(&admin, FenceClass::Recoverable);
        serving(&admin, 2);
        let delays = delays.lock().unwrap();
        assert_eq!(delays[0], Duration::from_millis(30));
        if hold_past_bound {
            assert_eq!(*delays.last().unwrap(), Duration::from_secs(2));
            assert_eq!(delays.iter().sum::<Duration>(), Duration::from_secs(12));
        } else {
            assert_eq!(delays.len(), 1);
            assert_eq!(opens.load(Ordering::Relaxed), 2);
        }
    }
}

#[test]
fn flapping_window_and_stable_backoff_reset() {
    let (_dir, admin) = fixture();
    let elapsed = Arc::new(Mutex::new(Duration::ZERO));
    let (recorded, success) = mpsc::channel();
    let delays = Arc::new(Mutex::new(Vec::new()));
    let seen_delays = Arc::clone(&delays);
    let calls = AtomicUsize::new(0);
    let config = FenceRecovery {
        max_recoveries: 3,
        backoff_cap: Duration::from_secs(8),
        ..settings()
    };
    let _worker = Worker::start(
        &admin,
        config,
        TestRuntime {
            elapsed: Arc::clone(&elapsed),
            after_success_recorded: Some(recorded),
            wait: Some(Box::new(move |delay| {
                // Drain and close use the real wake. Only advance the fake
                // clock for the bounded retry delays.
                if delay > config.backoff_cap {
                    return false;
                }
                seen_delays.lock().unwrap().push(delay);
                true
            })),
            open: Some(Box::new(move |request| {
                if calls.fetch_add(1, Ordering::Relaxed).is_multiple_of(2) {
                    Err(open_failure(FenceClass::Recoverable))
                } else {
                    request.open()
                }
            })),
            ..TestRuntime::default()
        },
    );
    for generation in 2..=4 {
        fence(&admin, FenceClass::Recoverable);
        serving(&admin, generation);
        assert_eq!(success.recv_timeout(WAIT).unwrap(), generation);
        if generation == 3 {
            *elapsed.lock().unwrap() += settings().stable_after;
        }
    }
    assert_eq!(*delays.lock().unwrap(), [2, 4, 2].map(Duration::from_secs));
    fence(&admin, FenceClass::Recoverable);
    let (cause, attempts) = gave_up(&admin);
    assert_eq!(attempts, 0);
    assert!(cause.error_text.contains("flapping limit"));
}

#[test]
fn shutdown_and_owner_drop_interrupt_drain_backoff_and_close_latch() {
    for drop_owner in [false, true] {
        let settings = settings();
        for (phase, expected_wait) in [
            ("drain", settings.drain_timeout),
            ("backoff", settings.backoff_base * 2),
            ("close", settings.close_timeout),
        ] {
            let (_dir, admin) = fixture();
            let (entered, entry_seen) = mpsc::channel();
            let lease = admin.test_v2();
            let latch = lease.close_latch().unwrap();
            let snapshot = (phase == "close").then(|| lease.kv.snapshot().unwrap());
            let lease = (phase == "drain").then_some(lease);
            fence(&admin, FenceClass::Recoverable);
            let worker = Worker::start(
                &admin,
                settings,
                TestRuntime {
                    wait: Some(Box::new(move |duration| {
                        let _ = entered.send(duration);
                        false
                    })),
                    open: Some(Box::new(|_| Err(open_failure(FenceClass::Recoverable)))),
                    ..TestRuntime::default()
                },
            );
            assert_eq!(entry_seen.recv_timeout(WAIT).unwrap(), expected_wait, "{phase}");
            let owner = if drop_owner {
                drop(admin);
                None
            } else {
                admin.stop_submissions();
                assert_eq!(admin.storage_status(), StorageStatus::Stopping);
                assert_fenced(&admin);
                Some(admin)
            };
            worker.join();
            drop((lease, snapshot, owner));
            latch.wait(WAIT).unwrap();
        }
    }
}

#[test]
fn shutdown_and_owner_drop_discard_reopened_candidate() {
    for drop_owner in [false, true] {
        let (_dir, admin) = fixture();
        let (opened, open_seen) = mpsc::channel::<CloseLatch>();
        let (release, paused) = mpsc::channel();
        let worker = Worker::start(
            &admin,
            settings(),
            TestRuntime {
                before_install: Some(Box::new(move |candidate| {
                    opened.send(candidate.db().close_latch().unwrap()).unwrap();
                    paused.recv_timeout(WAIT).unwrap();
                })),
                ..TestRuntime::default()
            },
        );
        fence(&admin, FenceClass::Recoverable);
        let latch = open_seen.recv_timeout(WAIT).unwrap();
        if drop_owner {
            drop(admin);
        } else {
            slot(&admin).begin_stopping();
            assert_eq!(admin.storage_status(), StorageStatus::Stopping);
        }
        release.send(()).unwrap();
        worker.join();
        latch.wait(WAIT).unwrap();
    }
}

#[cfg(any(target_os = "linux", windows))]
mod io_failures {
    include!("fence_supervisor_io_tests.rs");
}
