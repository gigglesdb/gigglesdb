use super::*;
use crate::storage::engine::{
    kv::{Direction, KvBackend, KvError, KvIter},
    kv_mem::MemKv,
    mvcc,
};
use std::ops::Bound;

fn request(n: u64) -> CommitRequest {
    CommitRequest {
        snapshot: Ts(0),
        writes: BTreeMap::from([((Keyspace::Rows, n.to_be_bytes().to_vec()), Some(vec![1]))]),
        features_or: 0,
        minimum_subversion: 0,
        raw: Vec::new(),
        validation: None,
    }
}

fn pending(n: u64) -> (Pending, mpsc::Receiver<Result<Ts, TxnError>>) {
    let (reply, receive) = mpsc::channel();
    (
        Pending {
            request: request(n),
            guard: None,
            reply,
        },
        receive,
    )
}

#[test]
fn sequential_mem_commits_do_not_wait_for_company() {
    assert_eq!(SequencerConfig::default().group_wait, Duration::ZERO);
    for wait in [
        Duration::ZERO,
        Duration::from_micros(200),
        Duration::from_micros(999),
    ] {
        let seq = Sequencer::open(
            Arc::new(Kv::new(Arc::new(MemKv::new()))),
            Arc::new(ManualClock::new(10)),
            SequencerConfig {
                group_wait: wait,
                ..SequencerConfig::default()
            },
        )
        .unwrap();
        let started = Instant::now();
        for n in 0..50 {
            seq.submit(request(n)).unwrap();
        }
        let elapsed = started.elapsed();
        // MemKv has no disk latency. 200 ms leaves scheduler headroom while
        // rejecting the roughly 750 ms caused by 50 Windows timer ticks.
        assert!(
            elapsed < Duration::from_millis(200),
            "wait={wait:?}: {elapsed:?}"
        );
        eprintln!("50 sequential MemKv commits, wait={wait:?}: {elapsed:?}");
    }
}

#[test]
fn zero_wait_drains_queued_commits_up_to_the_additional_request_limit() {
    for group_max in [0, 1, 3, 64] {
        let (send, receive) = mpsc::channel();
        let (first, _reply) = pending(0);
        for n in 1..=4 {
            send.send(Message::Commit(pending(n).0)).unwrap();
        }
        let (group, deferred, shutdown) = gather(
            first,
            &receive,
            &SequencerConfig {
                group_max,
                ..SequencerConfig::default()
            },
        );
        assert_eq!(group.len(), 1 + group_max.min(4));
        assert!(deferred.is_none());
        assert!(!shutdown);
        assert_eq!(receive.try_iter().count(), 4 - group_max.min(4));
    }
}

#[test]
fn gather_includes_counters_and_preserves_shutdown() {
    for shutdown in [false, true] {
        let (send, receive) = mpsc::channel();
        let (first, _reply) = pending(0);
        send.send(Message::Commit(pending(1).0)).unwrap();
        let (reply, _counter_reply) = mpsc::channel();
        send.send(if shutdown {
            Message::Shutdown
        } else {
            Message::Counter {
                key: vec![],
                value: vec![],
                features_or: 0,
                reply,
            }
        })
        .unwrap();
        send.send(Message::Commit(pending(2).0)).unwrap();
        let (group, deferred, stopping) = gather(first, &receive, &SequencerConfig::default());
        assert_eq!(group.len(), if shutdown { 2 } else { 4 });
        assert_eq!(stopping, shutdown);
        assert!(deferred.is_none());
        if shutdown { assert!(matches!(receive.try_recv(), Ok(Message::Commit(_)))); }
        else { assert!(matches!(group[2], Message::Counter { .. })); }
    }
}

#[test]
fn millisecond_wait_can_gather_a_later_arrival() {
    let (send, receive) = mpsc::channel();
    let (first, _reply) = pending(0);
    let worker = thread::spawn(move || {
        thread::sleep(Duration::from_millis(10));
        send.send(Message::Commit(pending(1).0)).unwrap();
    });
    let (group, deferred, shutdown) = gather(
        first,
        &receive,
        &SequencerConfig {
            group_max: 1,
            group_wait: Duration::from_secs(5),
            ..SequencerConfig::default()
        },
    );
    worker.join().unwrap();
    assert_eq!(group.len(), 2);
    assert!(deferred.is_none());
    assert!(!shutdown);
}

type SyncGate = (mpsc::Sender<()>, mpsc::Receiver<()>);

/// Hold the first real Sync so concurrent senders deterministically queue
/// behind it. Observe batch sizes here, without production instrumentation.
struct ObservedSync {
    backend: Arc<dyn KvBackend>,
    first_sync: Mutex<Option<SyncGate>>,
    groups: Mutex<Vec<usize>>,
}

impl KvRead for ObservedSync {
    fn get(&self, space: Keyspace, key: &[u8]) -> Result<Option<Vec<u8>>, KvError> {
        self.backend.get(space, key)
    }

    fn range(
        &self,
        space: Keyspace,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        direction: Direction,
    ) -> Result<KvIter<'_>, KvError> {
        self.backend.range(space, start, end, direction)
    }
}

impl KvBackend for ObservedSync {
    fn write(&self, batch: WriteBatch, durability: Durability) -> Result<(), KvError> {
        assert_eq!(durability, Durability::Sync);
        // Each request writes one row; the group adds one last_commit_ts.
        self.groups.lock().unwrap().push(batch.len() - 1);
        if let Some((entered, release)) = self.first_sync.lock().unwrap().take() {
            entered.send(()).unwrap();
            release.recv_timeout(Duration::from_secs(10)).unwrap();
        }
        self.backend.write(batch, durability)
    }

    fn flush_barrier(&self) -> Result<(), KvError> {
        self.backend.flush_barrier()
    }

    fn snapshot(&self) -> Result<Arc<dyn KvRead>, KvError> {
        self.backend.snapshot()
    }

    fn engine_name(&self) -> &'static str {
        self.backend.engine_name()
    }
}

fn concurrent_groups(backend: Arc<dyn KvBackend>) {
    let (entered, syncing) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let observed = Arc::new(ObservedSync {
        backend,
        first_sync: Mutex::new(Some((entered, released))),
        groups: Mutex::new(Vec::new()),
    });
    let seq = Sequencer::open(
        Arc::new(Kv::new(observed.clone())),
        Arc::new(ManualClock::new(10)),
        SequencerConfig {
            group_max: 7,
            ..SequencerConfig::default()
        },
    )
    .unwrap();
    let (first, first_reply) = pending(0);
    seq.sender.send(Message::Commit(first)).unwrap();
    syncing.recv_timeout(Duration::from_secs(10)).unwrap();
    let (queued, arrivals) = mpsc::channel();
    let workers: Vec<_> = (1..=32)
        .map(|n| {
            let sender = seq.sender.clone();
            let queued = queued.clone();
            thread::spawn(move || {
                let (pending, reply) = pending(n);
                sender.send(Message::Commit(pending)).unwrap();
                queued.send(()).unwrap();
                reply
                    .recv_timeout(Duration::from_secs(10))
                    .unwrap()
                    .unwrap()
            })
        })
        .collect();
    for _ in 1..=32 {
        arrivals.recv_timeout(Duration::from_secs(10)).unwrap();
    }
    assert_eq!(seq.visible_ts(), Ts(0));
    assert!(matches!(
        first_reply.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));
    release.send(()).unwrap();
    let mut timestamps = vec![
        first_reply
            .recv_timeout(Duration::from_secs(10))
            .unwrap()
            .unwrap(),
    ];
    timestamps.extend(workers.into_iter().map(|worker| worker.join().unwrap()));
    timestamps.sort();
    assert!(timestamps.windows(2).all(|pair| pair[0] < pair[1]));
    assert_eq!(seq.visible_ts(), *timestamps.last().unwrap());
    assert_eq!(*observed.groups.lock().unwrap(), vec![1, 8, 8, 8, 8]);
    for n in 0u64..=32 {
        assert_eq!(
            mvcc::read_at(
                observed.as_ref(),
                Keyspace::Rows,
                &n.to_be_bytes(),
                seq.visible_ts()
            )
            .unwrap(),
            Some(vec![1])
        );
    }
}

#[test]
fn concurrent_mem_commits_drain_arrivals_during_sync() {
    let mem = Arc::new(MemKv::new());
    concurrent_groups(mem.clone());
    assert_durable_rows(&mem.crash());
}

fn assert_durable_rows(backend: &dyn KvRead) {
    let timestamp = backend
        .get(Keyspace::Meta, b"last_commit_ts")
        .unwrap()
        .unwrap();
    let timestamp = Ts(u64::from_be_bytes(timestamp.try_into().unwrap()));
    for n in 0u64..=32 {
        assert_eq!(
            mvcc::read_at(backend, Keyspace::Rows, &n.to_be_bytes(), timestamp).unwrap(),
            Some(vec![1])
        );
    }
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn concurrent_rocks_commits_drain_arrivals_during_sync() {
    let mut nonce = [0; 16];
    getrandom::fill(&mut nonce).unwrap();
    let path =
        std::env::temp_dir().join(format!("commit-batch-{:032x}", u128::from_be_bytes(nonce)));
    concurrent_groups(Arc::new(
        crate::storage::engine::kv_rocksdb::RocksKv::open(&path, 1024 * 1024).unwrap(),
    ));
    // Drop the sequencer/backend before reopening or removing Windows files.
    let recovered = crate::storage::engine::kv_rocksdb::RocksKv::open(&path, 1024 * 1024).unwrap();
    assert_durable_rows(&recovered);
    drop(recovered);
    std::fs::remove_dir_all(path).unwrap();
}


fn counter_fixture() -> (Arc<Sequencer>, Vec<u8>, Vec<u8>) {
    use crate::relational::{DataType, catalog_ext::SequenceDef};
    let seq = Sequencer::open(
        Arc::new(Kv::new(Arc::new(MemKv::new()))),
        Arc::new(ManualClock::new(10)),
        SequencerConfig { group_wait: Duration::from_millis(10), ..Default::default() },
    ).unwrap();
    let definition = SequenceDef {
        name: "mixed_sequence".into(), data_type: DataType::BigInt,
        start: 1, increment: 1, min: 1, max: 99, cache: 4, cycle: false,
        owned_by: None, identity: false, incarnation: 7,
    };
    let mut create = request(900);
    create.features_or = 64;
    create.writes.insert((Keyspace::Catalog, super::super::catalog_codec::sequence_key(1)),
        Some(super::super::catalog_codec::encode_sequence(&definition).unwrap()));
    seq.submit(create).unwrap();
    let key = super::super::catalog_codec::counter_key(1, 7);
    let value = super::super::catalog_codec::encode_counter(
        &crate::storage::sequences::SeqCounter {
            incarnation: 7, last_value: 8, is_called: true, log_hw: 8, stamp: 3,
        }, &definition).unwrap();
    (seq, key, value)
}

fn counter_message(key: Vec<u8>, value: Vec<u8>) -> (Message, mpsc::Receiver<Result<(), TxnError>>) {
    let (reply, receive) = mpsc::channel();
    (Message::Counter { key, value, features_or: 0, reply }, receive)
}

// Predetermined mixed groups exercise the exact worker publication path. The
// worker remains idle after fixture creation and is never submitted more work.
fn process_mixed_group_for_test(seq: &Sequencer, group: Vec<Message>) {
    let mut hlc = HlcClock::new(seq.visible_ts());
    let mut log = RecentLog::new(seq.config.recent_log_bytes);
    let mut features = load_features(seq.kv.as_ref()).unwrap();
    let mut subversion = load_subversion(seq.kv.as_ref()).unwrap();
    assert!(!process_group(group, &seq.kv, &ManualClock::new(20), &seq.shared,
        &mut hlc, &mut log, &mut features, &mut subversion, &seq.config.ts_source, &seq.served));
}

fn drop_sequence(key: &[u8]) -> (Pending, mpsc::Receiver<Result<Ts, TxnError>>) {
    let (mut drop, receive) = pending(901);
    drop.request.writes.insert((Keyspace::Catalog, super::super::catalog_codec::sequence_key(1)), None);
    drop.request.raw.push(RawOp::DeleteCounter { key: key.to_vec() });
    (drop, receive)
}

#[test]
fn mixed_group_uses_one_sync() {
    let (seq, key, value) = counter_fixture();
    let before = seq.shared.sync_writes.load(std::sync::atomic::Ordering::Relaxed);
    let (a, ar) = pending(1);
    let (counter, cr) = counter_message(key.clone(), value.clone());
    let (b, br) = pending(2);
    process_mixed_group_for_test(&seq, vec![Message::Commit(a), counter, Message::Commit(b)]);
    assert!(ar.recv().unwrap().is_ok());
    assert_eq!(cr.recv().unwrap(), Ok(()));
    assert!(br.recv().unwrap().is_ok());
    assert_eq!(seq.kv.get(Keyspace::Seq, &key).unwrap(), Some(value));
    assert_eq!(seq.shared.sync_writes.load(std::sync::atomic::Ordering::Relaxed) - before, 1);
}

#[test]
fn drop_before_counter_rejects_only_counter() {
    let (seq, key, value) = counter_fixture();
    let (drop, dr) = drop_sequence(&key);
    let (counter, cr) = counter_message(key.clone(), value);
    process_mixed_group_for_test(&seq, vec![Message::Commit(drop), counter]);
    assert!(dr.recv().unwrap().is_ok());
    assert_eq!(cr.recv().unwrap(), Err(TxnError::SequenceGone));
    assert_eq!(seq.kv.get(Keyspace::Seq, &key).unwrap(), None);
}

#[test]
fn counter_before_drop_is_deleted() {
    let (seq, key, value) = counter_fixture();
    let (drop, dr) = drop_sequence(&key);
    let (counter, cr) = counter_message(key.clone(), value);
    process_mixed_group_for_test(&seq, vec![counter, Message::Commit(drop)]);
    assert_eq!(cr.recv().unwrap(), Ok(()));
    assert!(dr.recv().unwrap().is_ok());
    assert_eq!(seq.kv.get(Keyspace::Seq, &key).unwrap(), None);
}

#[test]
fn bad_counter_incarnation_does_not_fail_commits() {
    let (seq, _, value) = counter_fixture();
    let key = super::super::catalog_codec::counter_key(1, 8);
    let (a, ar) = pending(1);
    let (counter, cr) = counter_message(key.clone(), value);
    let (b, br) = pending(2);
    process_mixed_group_for_test(&seq, vec![Message::Commit(a), counter, Message::Commit(b)]);
    assert!(ar.recv().unwrap().is_ok());
    assert_eq!(cr.recv().unwrap(), Err(TxnError::SequenceGone));
    assert!(br.recv().unwrap().is_ok());
    assert_eq!(seq.kv.get(Keyspace::Seq, &key).unwrap(), None);
}

#[test]
fn concurrent_commits_and_counters_share_groups() {
    let (seq, key, value) = counter_fixture();
    let barrier = Arc::new(std::sync::Barrier::new(8));
    let workers: Vec<_> = (0..8).map(|n| {
        let (seq, key, value, barrier) = (seq.clone(), key.clone(), value.clone(), barrier.clone());
        thread::spawn(move || {
            for round in 0..8 {
                barrier.wait();
                if (n + round) % 2 == 0 {
                    seq.submit(request(1000 + n * 8 + round)).unwrap();
                    seq.write_counter(key.clone(), value.clone(), 0).unwrap();
                } else {
                    seq.write_counter(key.clone(), value.clone(), 0).unwrap();
                    seq.submit(request(1000 + n * 8 + round)).unwrap();
                }
            }
        })
    }).collect();
    for worker in workers { worker.join().unwrap(); }
    assert!(seq.max_group_size() >= 2);
    assert!(seq.shared.max_mixed_group.load(std::sync::atomic::Ordering::Relaxed) >= 2);
    assert_eq!(seq.kv.get(Keyspace::Seq, &key).unwrap(), Some(value));
}

#[test]
fn counter_bytes_and_count_obey_gather_limits() {
    for (cap, maximum, expected) in [(Some(20), 9, 1), (Some(21), 9, 2), (None, 0, 1)] {
        let (send, receive) = mpsc::channel();
        let (first, _) = counter_message(vec![0; 5], vec![0; 7]);
        send.send(Message::Commit(pending(1).0)).unwrap(); // nine bytes
        let (group, deferred, shutdown) = gather(first, &receive,
            &SequencerConfig { group_max_bytes: cap, group_max: maximum, ..Default::default() });
        assert_eq!(group.len(), expected);
        assert!(!shutdown);
        if expected == 1 && maximum > 0 { assert!(matches!(deferred, Some(Message::Commit(_)))); }
    }
}

#[test]
fn row_version_highwaters_validate_and_share_commit_sync() {
    let kv = Arc::new(Kv::new(Arc::new(MemKv::new())));
    let seq = Sequencer::open(kv.clone(), Arc::new(ManualClock::new(10)), SequencerConfig::default()).unwrap();
    let key = vec![3, 0, 0, 0, 0, 0, 0, 0, 0];
    for (key, value) in [(vec![3; 8], 2_u64.to_be_bytes().to_vec()),
        (vec![2; 9], 2_u64.to_be_bytes().to_vec()),
        (key.clone(), vec![0; 7]), (key.clone(), 0_u64.to_be_bytes().to_vec())] {
        assert!(matches!(seq.write_highwater_async(key, value), Err(TxnError::Misuse)));
    }
    assert_eq!(seq.write_highwater_async(key.clone(), 1_u64.to_be_bytes().to_vec()).unwrap().recv().unwrap(), Err(TxnError::Misuse));
    let (reply, receive) = mpsc::channel();
    let (commit, committed) = pending(42);
    let group = vec![Message::Highwater { key: key.clone(), value: 100_u64.to_be_bytes().to_vec(), reply }, Message::Commit(commit)];
    let mut hlc = HlcClock::new(Ts(0));
    let mut log = RecentLog::new(1_000_000);
    let mut features = 0;
    let mut subversion = load_subversion(kv.as_ref()).unwrap();
    assert!(!process_group(group, &kv, &ManualClock::new(10), &seq.shared, &mut hlc,
        &mut log, &mut features, &mut subversion, &seq.config.ts_source, &seq.served));
    receive.recv().unwrap().unwrap();
    committed.recv().unwrap().unwrap();
    assert_eq!(seq.shared.sync_writes.load(std::sync::atomic::Ordering::Relaxed), 1);
    assert_eq!(features, 0);
    assert_eq!(kv.get(Keyspace::Seq, &key).unwrap(), Some(100_u64.to_be_bytes().to_vec()));
    for end in [99_u64, 100] {
        assert_eq!(validate_highwater(&kv, &key, &end.to_be_bytes(), &BTreeMap::new()), Err(TxnError::Misuse));
    }
}
