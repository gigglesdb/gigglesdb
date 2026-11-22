use super::*;
#[cfg(target_os = "linux")]
use crate::storage::v2::recovery_tests::{MaxFileSize, wal_len};
use crate::{CommitSyncHook, storage::engine::kv_rocksdb::WAL_ONLY_SHUTDOWN};
use std::{
    io::Read,
    path::Path,
    process::{Command, Stdio},
};

const ACKNOWLEDGED: [i64; 4] = [0, 1, 2, 3];
const FAILED: i64 = 10;
const ATTEMPTS: u32 = 3;

#[derive(Clone, Copy)]
enum Injection {
    #[cfg(target_os = "linux")]
    FileSize,
    SyncHook {
        after_apply: bool,
    },
}

enum Fault {
    #[cfg(target_os = "linux")]
    FileSize {
        limit: MaxFileSize,
        partial_len: Option<u64>,
    },
    SyncHook {
        after_apply: bool,
    },
}

impl Injection {
    fn arm(self, _path: &Path, _persistent: bool) -> Fault {
        match self {
            #[cfg(target_os = "linux")]
            Self::FileSize => {
                let before = wal_len(_path);
                let partial_len = (!_persistent).then_some(before + 16);
                // Leave room for the writer-lock identity while forcing
                // recovery's native open to fail writing a RocksDB file.
                let limit = partial_len.unwrap_or(128);
                assert!(!_persistent || before > limit);
                Fault::FileSize {
                    limit: MaxFileSize::new(limit),
                    partial_len,
                }
            }
            Self::SyncHook { after_apply } => Fault::SyncHook { after_apply },
        }
    }
}

impl Fault {
    fn restore(self) {
        #[cfg(target_os = "linux")]
        if let Self::FileSize { limit, .. } = self {
            drop(limit);
        }
    }

    fn release_sync(&self, hook: &CommitSyncHook) {
        match self {
            #[cfg(target_os = "linux")]
            Self::FileSize { .. } => hook.release(false),
            Self::SyncHook { after_apply } => hook.release_with_wal_error(*after_apply),
        }
    }

    fn assert_cause(&self, _path: &Path, cause: &FenceCause) {
        assert_eq!(cause.class, FenceClass::Recoverable, "{cause:?}");
        assert_eq!(cause.generation, 1);
        assert_eq!(cause.site, FenceSite::WalWrite, "{cause:?}");
        assert_eq!(cause.status.unwrap().code, RocksStatus::IO_ERROR);
        match self {
            #[cfg(target_os = "linux")]
            Self::FileSize { partial_len, .. } => {
                assert!(cause.error_text.contains("File too large"), "{cause:?}");
                if let Some(expected) = partial_len {
                    assert_eq!(wal_len(_path), *expected, "expected a partial WAL append");
                }
            }
            Self::SyncHook { .. } => {
                assert!(cause.error_text.contains("injected commit WAL error"))
            }
        }
    }
}

fn run_child(name: &str, scenario: impl FnOnce(&Path)) {
    const CHILD: &str = "GIGGLES_TEST_FENCE_SUPERVISOR_IO_CHILD";
    if let Some(path) = std::env::var_os(CHILD) {
        scenario(Path::new(&path));
        return;
    }

    let dir = Directory::new();
    let (_, module) = module_path!().split_once("::").unwrap();
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", &format!("{module}::{name}"), "--nocapture"])
        .env(CHILD, &dir.0)
        .env("LC_ALL", "C")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let capture = |mut pipe: Box<dyn Read + Send>| {
        thread::spawn(move || {
            let mut bytes = Vec::new();
            pipe.read_to_end(&mut bytes).unwrap();
            String::from_utf8_lossy(&bytes).into_owned()
        })
    };
    let stdout = capture(Box::new(child.stdout.take().unwrap()));
    let stderr = capture(Box::new(child.stderr.take().unwrap()));
    let start = Instant::now();
    let (status, timed_out) = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break (status, false);
        }
        if start.elapsed() >= WAIT * 4 {
            // These tests spawn only threads inside the re-executed test.
            let _ = child.kill();
            break (child.wait().unwrap(), true);
        }
        thread::sleep(Duration::from_millis(10));
    };
    let stdout = stdout.join().unwrap();
    let stderr = stderr.join().unwrap();
    assert!(
        !timed_out
            && status.success()
            && stdout.lines().any(|line| line == "running 1 test")
            && stdout
                .lines()
                .any(|line| line.starts_with("test result: ok. 1 passed; 0 failed;")),
        "{name}: {status}, timed out={timed_out}\n{stdout}\n{stderr}"
    );
}

fn short_settings() -> FenceRecovery {
    FenceRecovery {
        drain_timeout: Duration::from_millis(25),
        close_timeout: Duration::from_millis(25),
        backoff_base: Duration::from_millis(20),
        backoff_cap: Duration::from_millis(100),
        backoff_jitter_percent: 0,
        max_attempts: ATTEMPTS,
        stall_limit: WAIT,
        ..settings()
    }
}

fn seeded(path: &Path) -> Admin {
    // Keep acknowledged data in the WAL so closing the fenced generation
    // cannot hide a replay failure by flushing the old memtables first.
    WAL_ONLY_SHUTDOWN.set(true);
    let admin = Admin::create_engine(path, Limits::default(), Engine::V2);
    WAL_ONLY_SHUTDOWN.set(false);
    let admin = admin.unwrap();
    let mut session = admin.sql_session();
    sql(&mut session, "CREATE TABLE t(id BIGINT PRIMARY KEY)").unwrap();
    for id in ACKNOWLEDGED {
        sql(&mut session, &format!("INSERT INTO t VALUES ({id})")).unwrap();
    }
    admin
}

fn insert_pair(session: &mut SqlSession, first: i64) -> Result<(), CoreError> {
    sql(
        session,
        &format!("INSERT INTO t VALUES ({first}), ({})", first + 1),
    )
    .map(drop)
}

fn paused_writer(
    admin: &Admin,
    first: i64,
) -> (
    Arc<CommitSyncHook>,
    thread::JoinHandle<Result<(), CoreError>>,
) {
    let mut session = admin.sql_session();
    paused_commit(admin, move || insert_pair(&mut session, first))
}

fn paused_commit(
    admin: &Admin,
    commit: impl FnOnce() -> Result<(), CoreError> + Send + 'static,
) -> (
    Arc<CommitSyncHook>,
    thread::JoinHandle<Result<(), CoreError>>,
) {
    let hook = CommitSyncHook::new();
    admin.set_commit_sync_hook(Some(Arc::clone(&hook)));
    let writer = thread::spawn(commit);
    assert!(hook.wait_entered(WAIT), "writer did not reach Sync");
    (hook, writer)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Reply {
    Ack,
    Ambiguous,
    Fenced,
}

fn reply(result: Result<(), CoreError>) -> Reply {
    match result {
        Ok(()) => Reply::Ack,
        Err(error) => {
            assert_eq!(crate::sqlstate(&error), "08007", "{error:?}");
            match error {
                CoreError::Fenced => Reply::Fenced,
                _ if ambiguous(&error) => Reply::Ambiguous,
                _ => panic!("unexpected write error: {error:?}"),
            }
        }
    }
}

fn fenced_statements(admin: &Admin) {
    let mut session = admin.sql_session();
    let error = sql(&mut session, "SELECT id FROM t").unwrap_err();
    assert_eq!(error, CoreError::Fenced);
    assert_eq!(crate::sqlstate(&error), "08007");
    assert_eq!(reply(insert_pair(&mut session, 90)), Reply::Fenced);
}

fn recovery_draining(admin: &Admin, path: &Path, fault: &Fault) -> Vec<(Holder, usize)> {
    wait_for(admin, |status| {
        matches!(status, StorageStatus::Recovering { .. })
    });
    let StorageStatus::Recovering {
        generation: 1,
        attempt: 0,
        cause: Some(cause),
        holders,
        ..
    } = admin.storage_status()
    else {
        panic!(
            "expected generation 1 draining, saw {:?}",
            admin.storage_status()
        );
    };
    fault.assert_cause(path, &cause);
    holders
}

fn verify_rows(admin: &Admin, pairs: &[(i64, Reply)]) -> BTreeSet<i64> {
    let actual = ids(admin);
    let mut expected = BTreeSet::from(ACKNOWLEDGED);
    for &(first, reply) in pairs {
        let present = actual.contains(&first);
        assert_eq!(
            present,
            actual.contains(&(first + 1)),
            "torn pair at {first}"
        );
        match reply {
            Reply::Ack => assert!(present, "missing acknowledged pair at {first}"),
            Reply::Fenced => assert!(!present, "fenced pair was written at {first}"),
            Reply::Ambiguous => {}
        }
        if present {
            expected.extend([first, first + 1]);
        }
    }
    assert_eq!(actual, expected);
    actual
}

fn verify_normal_open(path: &Path, expected: &BTreeSet<i64>) -> Admin {
    let normal = Admin::open(path, Limits::default()).unwrap();
    serving(&normal, 1);
    assert!(!slot(&normal).recovery_enabled());
    assert_eq!(&ids(&normal), expected);
    normal
}

fn close_and_verify(admin: Admin, path: &Path, pairs: &[(i64, Reply)]) -> Admin {
    let expected = verify_rows(&admin, pairs);
    let latch = admin.test_v2().close_latch().unwrap();
    drop(admin);
    latch.wait(WAIT).unwrap();
    verify_normal_open(path, &expected)
}

fn acknowledged(path: &Path, injection: Injection, enabled: bool) {
    let admin = seeded(path);
    let mut session = admin.sql_session();
    sql(&mut session, "CREATE TABLE mirror(id BIGINT PRIMARY KEY)").unwrap();
    for id in ACKNOWLEDGED {
        sql(&mut session, &format!("INSERT INTO mirror VALUES ({id})")).unwrap();
    }
    admin
        .set_fence_recovery(FenceRecovery {
            enabled,
            ..short_settings()
        })
        .unwrap();
    let hold = admin.test_v2();
    sql(&mut session, "BEGIN").unwrap();
    insert_pair(&mut session, FAILED).unwrap();
    sql(
        &mut session,
        &format!("INSERT INTO mirror VALUES ({FAILED}), ({})", FAILED + 1),
    )
    .unwrap();
    let (hook, writer) = paused_commit(&admin, move || sql(&mut session, "COMMIT").map(drop));
    let fault = injection.arm(path, false);
    fault.release_sync(&hook);
    assert_eq!(reply(writer.join().unwrap()), Reply::Ambiguous);
    if enabled {
        recovery_draining(&admin, path, &fault);
        fenced_statements(&admin);
    } else {
        let status = admin.storage_status();
        assert!(matches!(
            status,
            StorageStatus::Serving {
                generation: 1,
                fenced: true,
                ..
            }
        ));
        fenced_statements(&admin);
        assert_eq!(admin.storage_status(), status);
        assert!(!slot(&admin).recovery_enabled());
    }
    admin.set_commit_sync_hook(None);
    fault.restore();
    let latch = hold.close_latch().unwrap();
    drop(hold);
    if enabled {
        serving(&admin, 2);
        let ambiguous_rows = verify_rows(&admin, &[(FAILED, Reply::Ambiguous)]);
        assert_eq!(
            ids_from(&admin, "mirror"),
            ambiguous_rows,
            "torn cross-table batch"
        );
        if matches!(injection, Injection::SyncHook { after_apply: true }) {
            assert!(
                ambiguous_rows.contains(&FAILED),
                "the synced ambiguous batch was lost"
            );
        }
        insert_pair(&mut admin.sql_session(), 20).unwrap();
        let reopened =
            close_and_verify(admin, path, &[(FAILED, Reply::Ambiguous), (20, Reply::Ack)]);
        assert_eq!(ids_from(&reopened, "mirror"), ambiguous_rows);
    } else {
        drop(admin);
        latch.wait(WAIT).unwrap();
        let reopened = verify_normal_open(path, &BTreeSet::from(ACKNOWLEDGED));
        assert_eq!(ids_from(&reopened, "mirror"), BTreeSet::from(ACKNOWLEDGED));
    }
}

fn persistent_failure(path: &Path, injection: Injection) {
    let admin = seeded(path);
    let hold = admin.test_v2();
    let latch = hold.close_latch().unwrap();
    let opens = Arc::new(AtomicUsize::new(0));
    let worker = if matches!(injection, Injection::SyncHook { .. }) {
        let opens = Arc::clone(&opens);
        let closed = latch.clone();
        Some(Worker::start(
            &admin,
            short_settings(),
            TestRuntime {
                open: Some(Box::new(move |_| {
                    opens.fetch_add(1, Ordering::Relaxed);
                    Err(open_failure(FenceClass::Recoverable))
                })),
                // Advance the simulated clock only after the real lease and
                // native handle drain, so the backoff runs without sleeping.
                wait: Some(Box::new(move |_| closed.wait(Duration::ZERO).is_ok())),
                ..TestRuntime::default()
            },
        ))
    } else {
        admin.set_fence_recovery(short_settings()).unwrap();
        None
    };
    let (hook, writer) = paused_writer(&admin, FAILED);
    let fault = injection.arm(path, true);
    fault.release_sync(&hook);
    assert_eq!(reply(writer.join().unwrap()), Reply::Ambiguous);
    recovery_draining(&admin, path, &fault);
    admin.set_commit_sync_hook(None);
    drop(hold);
    let (cause, attempts) = gave_up(&admin);
    assert_eq!(attempts, ATTEMPTS, "{cause:?}");
    assert_eq!(cause.generation, 1);
    assert_eq!(
        (cause.site, cause.class),
        (FenceSite::Open, FenceClass::Recoverable)
    );
    assert_eq!(cause.status.unwrap().code, RocksStatus::IO_ERROR);
    match fault {
        #[cfg(target_os = "linux")]
        Fault::FileSize { .. } => assert!(cause.error_text.contains("File too large"), "{cause:?}"),
        Fault::SyncHook { .. } => {
            assert_eq!(opens.load(Ordering::Relaxed), ATTEMPTS as usize);
            assert!(cause.error_text.contains("injected IOError"), "{cause:?}");
        }
    }
    fenced_statements(&admin);
    assert!(matches!(
        admin.storage_status(),
        StorageStatus::GaveUp {
            generation: 1,
            attempts: ATTEMPTS,
            ..
        }
    ));
    latch.wait(WAIT).unwrap();
    fault.restore();
    drop(worker);
    drop(admin);
    verify_normal_open(path, &BTreeSet::from(ACKNOWLEDGED));
}

fn concurrent(path: &Path, injection: Injection) {
    const CLIENTS: usize = 4;
    let admin = seeded(path);
    admin.set_fence_recovery(short_settings()).unwrap();
    let hold = admin.test_v2();
    let (hook, writer) = paused_writer(&admin, FAILED);
    let fault = injection.arm(path, false);
    let (done, phases) = mpsc::channel();
    let start = Arc::new(Barrier::new(CLIENTS + 1));
    let clients: Vec<_> = (0..CLIENTS)
        .map(|client| {
            let mut session = admin.sql_session();
            let (start, done) = (Arc::clone(&start), done.clone());
            thread::spawn(move || {
                let mut pairs = Vec::new();
                for phase in 0..3 {
                    start.wait();
                    for sample in 0..3 {
                        let read =
                            sql(&mut session, "SELECT id FROM t WHERE id = 0").map(|result| {
                                let [row] = result.rows.as_slice() else {
                                    panic!(
                                        "unexpected rows for acknowledged id 0: {:?}",
                                        result.rows
                                    );
                                };
                                assert_eq!(row.values, [Int(0)]);
                            });
                        let first = (100 + 100 * client + 10 * phase + 2 * sample) as i64;
                        let write = reply(insert_pair(&mut session, first));
                        for outcome in [reply(read), write] {
                            match phase {
                                1 => assert_eq!(outcome, Reply::Fenced),
                                2 => assert_eq!(outcome, Reply::Ack),
                                _ => {}
                            }
                        }
                        pairs.push((first, write));
                    }
                    done.send(phase).unwrap();
                }
                pairs
            })
        })
        .collect();
    drop(done);
    let finish_phase = |expected| {
        for _ in 0..CLIENTS {
            assert_eq!(phases.recv_timeout(WAIT).unwrap(), expected);
        }
    };

    start.wait();
    fault.release_sync(&hook);
    assert_eq!(reply(writer.join().unwrap()), Reply::Ambiguous);
    recovery_draining(&admin, path, &fault);
    finish_phase(0);

    start.wait();
    finish_phase(1);
    admin.set_commit_sync_hook(None);
    fault.restore();
    drop(hold);
    serving(&admin, 2);

    start.wait();
    finish_phase(2);
    let mut pairs = vec![(FAILED, Reply::Ambiguous)];
    for client in clients {
        pairs.extend(client.join().unwrap());
    }
    close_and_verify(admin, path, &pairs);
}

struct ParkedReply {
    synced: mpsc::Sender<()>,
    resume: mpsc::Receiver<()>,
}

impl Drop for ParkedReply {
    fn drop(&mut self) {
        if thread::panicking() {
            return;
        }
        self.synced.send(()).unwrap();
        self.resume.recv_timeout(WAIT).expect("resume parked reply");
    }
}

fn parked(path: &Path, injection: Injection) {
    for fail_parked in [false, true] {
        let path = path.join(if fail_parked {
            "ambiguous"
        } else {
            "acknowledged"
        });
        let admin = seeded(&path);
        admin.set_fence_recovery(short_settings()).unwrap();
        let latch = admin.test_v2().close_latch().unwrap();
        let clock = crate::relational::StatementClock {
            transaction_us: 100,
            statement_us: 100,
            unavailable: None,
            zone: None,
            wall: None,
        };
        let mut session = admin.sql_session();
        session.control_block_at(false, &clock).unwrap();
        insert_pair(&mut session, FAILED).unwrap();
        let hook = CommitSyncHook::new();
        admin.set_commit_sync_hook(Some(Arc::clone(&hook)));
        let (entered_wait, in_wait) = mpsc::channel();
        let (synced, sync_finished) = mpsc::channel();
        let (resume, release) = mpsc::channel();
        let writer = thread::spawn(move || {
            let cancellation = crate::Cancellation::default();
            let guard = Mutex::new(Some(ParkedReply {
                synced,
                resume: release,
            }));
            let _registration =
                cancellation.register_durability_wait(move |_, release_admission| {
                    release_admission();
                    entered_wait.send(()).unwrap();
                    Some(Box::new(guard.lock().unwrap().take().unwrap()))
                });
            cancellation.scope(|| session.control_block_at(true, &clock))
        });
        in_wait.recv_timeout(WAIT).unwrap();
        assert!(hook.wait_entered(WAIT), "parked writer did not reach Sync");
        assert_eq!(admin.inner.sql_operations.load(Ordering::Acquire), 0);

        let other = 20;
        let fault = if fail_parked {
            let fault = injection.arm(&path, false);
            fault.release_sync(&hook);
            sync_finished.recv_timeout(WAIT).unwrap();
            fault
        } else {
            // The sequencer has synced the first pair. Keep its P2 reply in
            // the durability wait while a second SQL commit hits the WAL fault.
            hook.release(false);
            sync_finished.recv_timeout(WAIT).unwrap();
            let (hook, writer) = paused_writer(&admin, other);
            let fault = injection.arm(&path, false);
            fault.release_sync(&hook);
            assert_eq!(reply(writer.join().unwrap()), Reply::Ambiguous);
            fault
        };

        let holders = recovery_draining(&admin, &path, &fault);
        assert_eq!(holders, [(Holder::Transaction, 1)]);
        assert!(
            latch.wait(Duration::ZERO).is_err(),
            "closed below parked commit"
        );
        assert!(
            !writer.is_finished(),
            "parked commit returned before release"
        );
        fenced_statements(&admin);
        admin.set_commit_sync_hook(None);
        fault.restore();
        resume.send(()).unwrap();
        let parked_outcome = reply(writer.join().unwrap());
        assert_eq!(
            parked_outcome,
            if fail_parked {
                Reply::Ambiguous
            } else {
                Reply::Ack
            }
        );
        latch.wait(WAIT).unwrap();
        serving(&admin, 2);
        let mut pairs = vec![(FAILED, parked_outcome)];
        if !fail_parked {
            pairs.push((other, Reply::Ambiguous));
        }
        close_and_verify(admin, &path, &pairs);
    }
}

#[cfg(target_os = "linux")]
#[test]
fn real_wal_failure_preserves_acknowledged_commits() {
    run_child("real_wal_failure_preserves_acknowledged_commits", |path| {
        acknowledged(path, Injection::FileSize, true)
    });
}

#[cfg(target_os = "linux")]
#[test]
fn real_wal_failure_gives_up_at_attempt_limit() {
    run_child("real_wal_failure_gives_up_at_attempt_limit", |path| {
        persistent_failure(path, Injection::FileSize)
    });
}

#[cfg(target_os = "linux")]
#[test]
fn real_wal_failure_during_concurrent_statements() {
    run_child("real_wal_failure_during_concurrent_statements", |path| {
        concurrent(path, Injection::FileSize)
    });
}

#[cfg(target_os = "linux")]
#[test]
fn real_wal_failure_with_parked_commits() {
    run_child("real_wal_failure_with_parked_commits", |path| {
        parked(path, Injection::FileSize)
    });
}

#[test]
fn sync_hook_preserves_acknowledged_commits() {
    run_child("sync_hook_preserves_acknowledged_commits", |path| {
        acknowledged(path, Injection::SyncHook { after_apply: false }, true)
    });
}

#[test]
fn sync_hook_recovers_a_durable_ambiguous_batch() {
    run_child("sync_hook_recovers_a_durable_ambiguous_batch", |path| {
        acknowledged(path, Injection::SyncHook { after_apply: true }, true)
    });
}

#[test]
fn sync_hook_with_open_failures_gives_up_at_attempt_limit() {
    run_child(
        "sync_hook_with_open_failures_gives_up_at_attempt_limit",
        |path| persistent_failure(path, Injection::SyncHook { after_apply: false }),
    );
}

#[test]
fn sync_hook_fault_during_concurrent_statements() {
    run_child("sync_hook_fault_during_concurrent_statements", |path| {
        concurrent(path, Injection::SyncHook { after_apply: false })
    });
}

#[test]
fn sync_hook_fault_with_parked_commits() {
    run_child("sync_hook_fault_with_parked_commits", |path| {
        parked(path, Injection::SyncHook { after_apply: false })
    });
}

#[test]
fn disabled_recovery_keeps_ambiguous_commit_fenced() {
    run_child("disabled_recovery_keeps_ambiguous_commit_fenced", |path| {
        acknowledged(path, Injection::SyncHook { after_apply: false }, false)
    });
}
