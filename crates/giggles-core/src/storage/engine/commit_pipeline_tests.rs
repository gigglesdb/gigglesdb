use super::*;
use crate::{
    CommitSyncHook,
    relational::{DataType, catalog_ext::SequenceDef},
    storage::{
        engine::{
            catalog_codec,
            kv::{FenceClass, FenceSite, KvBackend, RocksStatus},
            kv_mem::MemKv,
            kv_rocksdb::{RocksKv, mark_commit_groups},
            meta, mvcc,
            once_guard::GuardRejection,
        },
        range_api::{RangeError, Timestamp},
        replicated::{BatchSink, ReplicateError, ReplicatedKv},
        v2::{EngineOptions, V2Db},
    },
};
use std::{
    path::{Path, PathBuf},
    process::Command,
};

const WAIT: Duration = Duration::from_secs(10);
const CACHE_BYTES: usize = 8 << 20;

struct Store(PathBuf);

impl Store {
    fn new(marked: bool) -> Self {
        let mut nonce = [0; 8];
        getrandom::fill(&mut nonce).unwrap();
        let path = std::env::temp_dir().join(format!(
            "commit-pipeline-{:016x}",
            u64::from_be_bytes(nonce)
        ));
        let store = Self(path);
        let rocks = RocksKv::open(&store.0, CACHE_BYTES).unwrap();
        meta::write_initial_meta(&rocks, [1; 16], 0).unwrap();
        drop(rocks);
        meta::write_marker(&store.0).unwrap();
        if marked {
            mark_commit_groups(&store.0).unwrap();
        }
        store
    }

    fn open(&self, group_max: usize) -> Arc<Sequencer> {
        open(
            &self.0,
            SequencerConfig {
                group_max,
                commit_pipeline: true,
                ..Default::default()
            },
        )
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

fn open(path: &Path, config: SequencerConfig) -> Arc<Sequencer> {
    let backend = Arc::new(RocksKv::open(path, CACHE_BYTES).unwrap());
    Sequencer::open(
        Arc::new(Kv::new_single_writer(backend)),
        Arc::new(ManualClock::new(10)),
        config,
    )
    .unwrap()
}

struct Pause(Option<Arc<CommitSyncHook>>);

impl Pause {
    fn install(seq: &Sequencer, hook: Arc<CommitSyncHook>) -> Self {
        seq.set_sync_hook(Some(hook.clone()));
        Self(Some(hook))
    }

    fn sync(seq: &Sequencer) -> Self {
        Self::install(seq, CommitSyncHook::new())
    }

    fn entered(&self) {
        assert!(self.0.as_ref().unwrap().wait_entered(WAIT));
    }

    fn release(mut self) {
        self.0.take().unwrap().release(false);
    }
    fn fail(mut self) {
        self.0.take().unwrap().release_with_wal_error(false);
    }
}

impl Drop for Pause {
    fn drop(&mut self) {
        if let Some(hook) = self.0.take() {
            hook.release(false);
        }
    }
}

fn request() -> CommitRequest {
    super::super::guard_tests::request(0)
}

fn row_with_bytes(id: u64, bytes: usize) -> CommitRequest {
    let mut r = request();
    r.writes.insert(
        (Keyspace::Rows, id.to_be_bytes().to_vec()),
        Some(vec![1; bytes]),
    );
    r
}

fn row(id: u64) -> CommitRequest {
    row_with_bytes(id, 1)
}

fn send(
    seq: &Sequencer,
    request: CommitRequest,
    guard: Option<CommitGuard>,
) -> mpsc::Receiver<Result<Ts, TxnError>> {
    let (reply, receiver) = mpsc::channel();
    seq.sender
        .send(Message::Commit(Pending {
            request,
            guard,
            reply,
        }))
        .unwrap();
    receiver
}

fn recv<T>(receiver: &mpsc::Receiver<T>) -> T {
    receiver.recv_timeout(WAIT).unwrap()
}

fn pending<T>(receiver: &mpsc::Receiver<T>) {
    assert!(matches!(
        receiver.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));
}

fn raw(read: &dyn KvRead, id: u64) -> Option<Ts> {
    mvcc::latest_version(read, Keyspace::Rows, &id.to_be_bytes())
        .unwrap()
        .map(|(ts, _)| ts)
}

fn eventually(mut ready: impl FnMut() -> bool) {
    let deadline = Instant::now() + WAIT;
    while !ready() {
        assert!(Instant::now() < deadline, "condition timed out");
        thread::sleep(Duration::from_millis(1));
    }
}

fn validation_signal(
    mut r: CommitRequest,
    done: mpsc::Sender<()>,
    result: Result<(), TxnError>,
) -> CommitRequest {
    r.validation = Some(Box::new(move |_, _, _, _| {
        let _ = done.send(());
        result.clone()
    }));
    r
}

#[test]
fn overlaps_and_coalesces_only_groups_already_enqueued() {
    let store = Store::new(true);
    let seq = store.open(0);
    assert!(seq.commit_pipeline);
    let first_sync = Pause::sync(&seq);
    let first = send(&seq, row(1), None);
    first_sync.entered();
    let second = send(&seq, row(2), None);
    let third = send(&seq, row(3), None);
    let (at_empty, empty_started) = mpsc::channel();
    let empty = send(&seq, validation_signal(request(), at_empty, Ok(())), None);
    recv(&empty_started); // Stage A has enqueued both later appends.
    let first_ts = raw(seq.kv(), 1).unwrap();
    let second_ts = raw(seq.kv(), 2).unwrap();
    let third_ts = raw(seq.kv(), 3).unwrap();
    assert!(first_ts < second_ts && second_ts < third_ts);
    assert_eq!(read_last_commit_ts(seq.kv()).unwrap(), third_ts);
    assert_eq!(seq.visible_ts(), Ts(0));
    pending(&first);
    pending(&second);
    pending(&third);
    pending(&empty);
    let second_sync = Pause::sync(&seq);
    first_sync.release();
    second_sync.entered();
    assert_eq!(recv(&first), Ok(first_ts));
    assert_eq!(seq.visible_ts(), first_ts);
    pending(&second);
    pending(&third);
    pending(&empty);
    seq.set_sync_hook(None);
    second_sync.release();
    assert_eq!(recv(&second), Ok(second_ts));
    assert_eq!(recv(&third), Ok(third_ts));
    assert_eq!(recv(&empty), Ok(Ts(0)));
    assert_eq!(seq.shared.pipeline_barriers.load(Ordering::Relaxed), 2);
    drop(seq);
    let seq = store.open(0);
    assert_eq!(seq.visible_ts(), third_ts);
    assert!(seq.submit(row(4)).unwrap() > third_ts);
}

#[test]
fn append_success_and_predispatch_refusal_during_failed_barrier() {
    for append_applied in [true, false] {
        let store = Store::new(true);
        let seq = store.open(0);
        let sync = Pause::sync(&seq);
        let first = send(&seq, row(1), None);
        sync.entered();
        let append = Pause::install(
            &seq,
            if append_applied {
                CommitSyncHook::after_append_before_enqueue()
            } else {
                CommitSyncHook::before_append()
            },
        );
        let second = send(&seq, row(2), None);
        append.entered();
        assert_eq!(raw(seq.kv(), 2).is_some(), append_applied);
        let third = send(&seq, row(3), None);
        sync.fail();
        assert_eq!(recv(&first), Err(TxnError::CommitAmbiguous));
        pending(&second);
        pending(&third);
        append.release();
        let expected = if append_applied {
            TxnError::CommitAmbiguous
        } else {
            TxnError::Fenced
        };
        assert_eq!(recv(&second), Err(expected));
        assert_eq!(recv(&third), Err(TxnError::Fenced));
        assert_eq!(seq.visible_ts(), Ts(0));
        assert!(seq.kv.is_fenced() && seq.is_fenced());
        assert_eq!(seq.kv.causes.cause().unwrap().site, FenceSite::CommitSync);
        assert_eq!(
            seq.kv.causes.cause().unwrap().class,
            FenceClass::NotRecoverable
        );
        seq.shutdown();
    }
}

#[test]
fn successful_sync_stays_successful_if_later_append_fails_before_publish() {
    let store = Store::new(true);
    let seq = store.open(0);
    let publish = Pause::install(&seq, CommitSyncHook::before_publish());
    let first = send(&seq, row(1), None);
    publish.entered();
    let first_ts = raw(seq.kv(), 1).unwrap();
    let append = Pause::install(&seq, CommitSyncHook::before_append());
    let second = send(&seq, row(2), None);
    append.entered();
    append.fail();
    eventually(|| seq.is_fenced());
    pending(&first);
    pending(&second);
    publish.release();
    assert_eq!(recv(&first), Ok(first_ts));
    assert_eq!(recv(&second), Err(TxnError::CommitAmbiguous));
    assert_eq!(seq.visible_ts(), first_ts);
    assert_eq!(seq.kv.causes.cause().unwrap().site, FenceSite::CommitAppend);
}

#[test]
fn fence_guard_snapshot_and_shutdown_drain_in_fifo_order() {
    let store = Store::new(true);
    let seq = store.open(0);
    let sync = Pause::sync(&seq);
    let first = send(&seq, row(1), None);
    sync.entered();
    let first_ts = raw(seq.kv(), 1).unwrap();
    let (reply, status) = mpsc::channel();
    seq.sender
        .send(Message::Fence {
            action: FenceAction::Status,
            reply,
        })
        .unwrap();
    let guard = CommitGuard::capture(first_ts, Instant::now() + WAIT);
    let guarded = send(&seq, row(2), Some(guard));
    let (reply, snapshot) = mpsc::channel();
    seq.sender
        .send(Message::DurableSnapshot {
            pin: seq.snapshot(),
            reply,
        })
        .unwrap();
    let third = send(&seq, row(3), None);
    pending(&status);
    pending(&guarded);
    pending(&snapshot);
    assert!(raw(seq.kv(), 2).is_none());
    seq.set_sync_hook(None);
    sync.release();
    assert_eq!(recv(&first), Ok(first_ts));
    assert!(recv(&status).unwrap().is_none());
    let guarded_ts = recv(&guarded).unwrap();
    let (pin, read) = recv(&snapshot).unwrap();
    assert_eq!(pin.ts(), Ts(0));
    assert_eq!(raw(read.as_ref(), 2), Some(guarded_ts));
    assert!(recv(&third).unwrap() > guarded_ts);
    assert!(raw(read.as_ref(), 3).is_none());
    drop((pin, read));

    let last = Pause::sync(&seq);
    let fourth = send(&seq, row(4), None);
    last.entered();
    let later = send(&seq, row(5), None);
    eventually(|| raw(seq.kv(), 5).is_some());
    let (reply, done) = mpsc::channel();
    let shutdown = thread::spawn({
        let seq = seq.clone();
        move || {
            seq.shutdown();
            reply.send(()).unwrap();
        }
    });
    eventually(|| seq.stopped());
    pending(&done);
    last.release();
    assert!(recv(&fourth).is_ok());
    assert!(recv(&later).is_ok());
    recv(&done);
    shutdown.join().unwrap();
    assert!(seq.test_worker_joined());
}

#[test]
fn reply_only_items_take_slots_and_terminal_wakes_admission() {
    let store = Store::new(true);
    let seq = store.open(0);
    let sync = Pause::sync(&seq);
    let first = send(&seq, row(1), None);
    sync.entered();
    let (entered, starts) = mpsc::channel();
    let replies = (0..=MAX_GROUPS)
        .map(|_| {
            send(
                &seq,
                validation_signal(request(), entered.clone(), Err(TxnError::Misuse)),
                None,
            )
        })
        .collect::<Vec<_>>();
    // Seven empty items fit alongside the detached appender. The eighth can
    // validate but cannot reserve until the in-flight cohort sends its reply.
    for _ in 0..MAX_GROUPS {
        recv(&starts);
    }
    assert_eq!(
        starts.recv_timeout(Duration::from_millis(100)),
        Err(mpsc::RecvTimeoutError::Timeout)
    );
    for reply in &replies {
        pending(reply);
    }
    sync.fail();
    assert_eq!(recv(&first), Err(TxnError::CommitAmbiguous));
    for reply in &replies {
        assert_eq!(recv(reply), Err(TxnError::Fenced));
    }
}

#[test]
fn byte_reservation_blocks_append_and_oversized_batch_uses_serial_sync() {
    let store = Store::new(true);
    let seq = store.open(0);
    let sync = Pause::sync(&seq);
    let first = send(&seq, row_with_bytes(1, 3 * 1024 * 1024), None);
    sync.entered();
    let (validating, started) = mpsc::channel();
    let second = send(
        &seq,
        validation_signal(row_with_bytes(2, 2 * 1024 * 1024), validating, Ok(())),
        None,
    );
    recv(&started);
    let (validating, started) = mpsc::channel();
    let third = send(&seq, validation_signal(request(), validating, Ok(())), None);
    assert_eq!(
        started.recv_timeout(Duration::from_millis(100)),
        Err(mpsc::RecvTimeoutError::Timeout)
    );
    assert_eq!(read_group_number(seq.kv()).unwrap(), 1);
    assert!(raw(seq.kv(), 2).is_none());
    seq.set_sync_hook(None);
    sync.release();
    recv(&first).unwrap();
    recv(&second).unwrap();
    recv(&third).unwrap();
    let before_append = Pause::install(&seq, CommitSyncHook::before_append());
    let oversized = send(&seq, row_with_bytes(3, MAX_BYTES), None);
    recv(&oversized).unwrap();
    before_append.release();
    assert_eq!(read_group_number(seq.kv()).unwrap(), 3);
    assert_eq!(seq.shared.pipeline_barriers.load(Ordering::Relaxed), 2);
}

fn sequence(incarnation: u128) -> CommitRequest {
    let mut request = request();
    request.features_or = 64;
    request.writes.insert(
        (Keyspace::Catalog, catalog_codec::sequence_key(1)),
        Some(
            catalog_codec::encode_sequence(&SequenceDef {
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
            })
            .unwrap(),
        ),
    );
    if incarnation > 1 {
        request.raw.push(RawOp::DeleteCounter {
            key: catalog_codec::counter_key(1, incarnation - 1),
        });
    }
    request
}

#[test]
fn restart_rejection_is_fenced_in_the_same_or_later_group_if_sync_fails() {
    for same_group in [true, false] {
        let store = Store::new(true);
        // Queue both requests before a parked guard releases the sequencer.
        let seq = store.open(usize::from(same_group));
        seq.submit(sequence(1)).unwrap();
        let value = crate::storage::encode(&(1i64, false, 1i64, 1u64)).unwrap();
        let key = catalog_codec::counter_key(1, 1);
        seq.write_counter(key.clone(), value.clone(), 0).unwrap();
        let (entered, parked) = mpsc::channel();
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let guard =
            CommitGuard::capture(seq.visible_ts(), Instant::now() + WAIT).with_hook(Arc::new({
                let gate = gate.clone();
                move |point| {
                    if point == crate::DecisionPoint::BeforeDecision {
                        entered.send(()).unwrap();
                        drop(
                            gate.1
                                .wait_while(gate.0.lock().unwrap(), |ready| !*ready)
                                .unwrap(),
                        );
                    }
                }
            }));
        let previous = send(&seq, request(), Some(guard));
        recv(&parked);
        let sync = Pause::sync(&seq);
        let restarted = send(&seq, sequence(2), None);
        let old_counter = seq.send_counter(key, value, 0).unwrap();
        *gate.0.lock().unwrap() = true;
        gate.1.notify_all();
        recv(&previous).unwrap();
        sync.entered();
        pending(&restarted);
        pending(&old_counter.0);
        sync.fail();
        assert_eq!(recv(&restarted), Err(TxnError::CommitAmbiguous));
        assert_eq!(old_counter.wait(), Err(TxnError::Fenced));
    }
}

#[test]
fn maintenance_fence_is_not_recoverable_and_append_is_ambiguous() {
    let store = Store::new(true);
    let seq = store.open(0);
    let sync = Pause::sync(&seq);
    let first = send(&seq, row(1), None);
    sync.entered();
    seq.kv.test_report_write_error(KvError::RocksIo {
        message: "maintenance WAL error".into(),
        status: RocksStatus {
            code: RocksStatus::IO_ERROR,
            subcode: 0,
            severity: None,
        },
        site: FenceSite::WalWrite,
    });
    assert_eq!(
        seq.kv.causes.cause().unwrap().class,
        FenceClass::NotRecoverable
    );
    sync.release();
    assert_eq!(recv(&first), Err(TxnError::CommitAmbiguous));
    assert_eq!(seq.visible_ts(), Ts(0));
}

#[test]
fn fence_uses_physical_floor_without_moving_guard_visible_time() {
    let store = Store::new(true);
    let seq = store.open(0);
    let sync = Pause::sync(&seq);
    let first = send(&seq, row(1), None);
    sync.entered();
    let digest = holder_digest(b"token");
    let (reply, install) = mpsc::channel();
    seq.sender
        .send(Message::Fence {
            action: FenceAction::Install {
                digest,
                label: "pipeline".into(),
            },
            reply,
        })
        .unwrap();
    pending(&install);
    sync.release();
    let first = recv(&first).unwrap();
    let epoch = recv(&install).unwrap().unwrap().epoch;
    assert_eq!(read_group_number(seq.kv()).unwrap(), 2);
    let sync = Pause::sync(&seq);
    let mut key = vec![3];
    key.extend_from_slice(&1u64.to_be_bytes());
    let highwater = seq
        .write_highwater_async(key, 7u64.to_be_bytes().to_vec())
        .unwrap();
    sync.entered();
    let (reply, release) = mpsc::channel();
    seq.sender
        .send(Message::Fence {
            action: FenceAction::Release { digest, epoch },
            reply,
        })
        .unwrap();
    pending(&release);
    sync.release();
    recv(&highwater).unwrap();
    recv(&release).unwrap();
    assert_eq!(read_group_number(seq.kv()).unwrap(), 4);
    let floor = read_last_commit_ts(seq.kv()).unwrap();
    assert!(floor > first);
    assert_eq!(seq.visible_ts(), first);
    let guard = CommitGuard::capture(first, Instant::now() + WAIT);
    assert_eq!(seq.submit(request()), Ok(first));
    let mut key = vec![3];
    key.extend_from_slice(&1u64.to_be_bytes());
    recv(
        &seq.write_highwater_async(key, 8u64.to_be_bytes().to_vec())
            .unwrap(),
    )
    .unwrap();
    assert_eq!(read_group_number(seq.kv()).unwrap(), 5);
    assert_eq!(read_last_commit_ts(seq.kv()).unwrap(), floor);
    assert_eq!(seq.visible_ts(), first);
    assert!(seq.submit_guarded(row(2), guard).unwrap() > floor);
    assert_eq!(read_group_number(seq.kv()).unwrap(), 6);
}

#[test]
fn eight_writers_and_two_readers_see_only_the_published_durable_prefix() {
    let store = Store::new(true);
    let seq = store.open(0);
    let sync = Pause::sync(&seq);
    let mut writers = Vec::new();
    for id in 1u64..=8 {
        let seq = seq.clone();
        writers.push(thread::spawn(move || (id, seq.submit(row(id)).unwrap())));
    }
    sync.entered();
    eventually(|| read_group_number(seq.kv()).unwrap() == 8);
    let readers = (0..2)
        .map(|_| {
            let seq = seq.clone();
            thread::spawn(move || {
                for _ in 0..100 {
                    let snap = seq.snapshot();
                    for id in 1u64..=8 {
                        assert!(raw(seq.kv(), id).is_some());
                        assert_eq!(
                            mvcc::read_at(seq.kv(), Keyspace::Rows, &id.to_be_bytes(), snap.ts())
                                .unwrap(),
                            None
                        );
                    }
                }
            })
        })
        .collect::<Vec<_>>();
    for reader in readers {
        reader.join().unwrap();
    }
    assert!(writers.iter().all(|writer| !writer.is_finished()));
    sync.release();
    for writer in writers {
        let (id, ts) = writer.join().unwrap();
        let pin = seq.snapshot();
        assert!(pin.ts() >= ts);
        assert_eq!(
            mvcc::read_at(seq.kv(), Keyspace::Rows, &id.to_be_bytes(), pin.ts()).unwrap(),
            Some(vec![1])
        );
    }
}

#[test]
fn engine_options_enable_only_a_marked_local_hlc_store() {
    struct Oracle;
    impl TsOracle for Oracle {
        fn next(&self, _: u32) -> Result<Timestamp, RangeError> {
            panic!("no oracle requests expected")
        }
    }
    struct Sink;
    impl BatchSink for Sink {
        fn replicate(&self, _: Vec<u8>) -> Result<(), ReplicateError> {
            panic!("no writes expected")
        }
    }
    let marked = Store::new(true);
    let engine = V2Db::open_with_options(
        &marked.0,
        None,
        EngineOptions {
            commit_pipeline: true,
        },
    )
    .unwrap();
    assert!(engine.sequencer.commit_pipeline);
    drop(engine);
    assert!(!open(&marked.0, SequencerConfig::default()).commit_pipeline);
    assert!(
        !open(
            &marked.0,
            SequencerConfig {
                commit_pipeline: true,
                ts_source: TsSource::Oracle(Arc::new(Oracle)),
                ..Default::default()
            }
        )
        .commit_pipeline
    );
    let unmarked = Store::new(false);
    assert!(!unmarked.open(0).commit_pipeline);
    let mem = Arc::new(MemKv::new());
    for backend in [
        mem.clone() as Arc<dyn KvBackend>,
        Arc::new(ReplicatedKv::new(mem, Arc::new(Sink))),
    ] {
        let seq = Sequencer::open(
            Arc::new(Kv::new_single_writer(backend)),
            Arc::new(ManualClock::new(10)),
            SequencerConfig {
                commit_pipeline: true,
                ..Default::default()
            },
        )
        .unwrap();
        assert!(!seq.commit_pipeline);
    }
}

const CHILD_MODE: &str = "GIGGLES_PIPELINE_CHILD_MODE";
const CHILD_PATH: &str = "GIGGLES_PIPELINE_CHILD_PATH";

fn reexec(test: &str, path: &Path, mode: &str) -> std::process::Output {
    Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            &format!("{}::{test}", module_path!().split_once("::").unwrap().1),
            "--nocapture",
        ])
        .env(CHILD_MODE, mode)
        .env(CHILD_PATH, path)
        .output()
        .unwrap()
}

#[test]
fn process_fatal_before_unwind_in_either_stage() {
    if let Some(mode) = std::env::var_os(CHILD_MODE) {
        let path = PathBuf::from(std::env::var_os(CHILD_PATH).unwrap());
        let seq = open(
            &path,
            SequencerConfig {
                commit_pipeline: true,
                ..Default::default()
            },
        );
        let hook = if mode == "append" {
            CommitSyncHook::after_append_before_enqueue()
        } else {
            CommitSyncHook::before_publish()
        };
        seq.set_sync_hook(Some(hook.clone()));
        let mut r = row(1);
        r.writes.extend(row(2).writes);
        let reply = send(&seq, r, None);
        assert!(hook.wait_entered(WAIT));
        pending(&reply);
        hook.release_with_panic();
        eprintln!("commit channel returned {:?}", reply.recv_timeout(WAIT));
        std::process::exit(91);
    }
    for mode in ["append", "publish"] {
        let store = Store::new(true);
        let result = reexec(
            "process_fatal_before_unwind_in_either_stage",
            &store.0,
            mode,
        );
        let stderr = String::from_utf8_lossy(&result.stderr);
        assert!(
            !result.status.success() && result.status.code() != Some(91),
            "{result:?}"
        );
        assert!(
            stderr.contains("injected commit worker panic"),
            "{result:?}"
        );
        assert!(!stderr.contains("commit channel returned"), "{result:?}");
        let reopened = store.open(0);
        assert_eq!(raw(reopened.kv(), 1), raw(reopened.kv(), 2));
        if mode == "publish" {
            assert!(raw(reopened.kv(), 1).is_some());
        }
    }
}

#[test]
fn refused_and_empty_groups_do_not_leave_a_wal_chain_gap() {
    if std::env::var_os(CHILD_MODE).is_some() {
        let path = PathBuf::from(std::env::var_os(CHILD_PATH).unwrap());
        let seq = open(
            &path,
            SequencerConfig {
                group_max: 0,
                commit_pipeline: true,
                ..Default::default()
            },
        );
        let first = seq.submit(row(1)).unwrap();
        assert_eq!(seq.submit(request()), Ok(first));
        assert_eq!(
            seq.submit_guarded(row(2), CommitGuard::capture(first, Instant::now())),
            Err(TxnError::Guard(GuardRejection::LeaseExpired)),
        );
        let third = seq.submit(row(3)).unwrap();
        assert!(third > first);
        assert_eq!(read_group_number(seq.kv()).unwrap(), 2);
        crate::storage::crash_now(); // Leave the live WAL for recovery.
    }
    let store = Store::new(true);
    let result = reexec(
        "refused_and_empty_groups_do_not_leave_a_wal_chain_gap",
        &store.0,
        "number",
    );
    assert_eq!(result.status.code(), Some(86), "{result:?}");
    let seq = store.open(0);
    let recovered = raw(seq.kv(), 3).unwrap();
    assert!(raw(seq.kv(), 1).is_some() && raw(seq.kv(), 2).is_none());
    assert_eq!(read_group_number(seq.kv()).unwrap(), 2);
    assert!(seq.submit(row(4)).unwrap() > recovered);
    assert_eq!(read_group_number(seq.kv()).unwrap(), 3);
}
