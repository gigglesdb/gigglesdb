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
        raw: Vec::new(),
        validation: None,
    }
}

fn pending(n: u64) -> (Pending, mpsc::Receiver<Result<Ts, TxnError>>) {
    let (reply, receive) = mpsc::channel();
    (
        Pending {
            request: request(n),
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
fn gather_defers_counters_and_preserves_shutdown() {
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
        assert_eq!(group.len(), 2);
        assert_eq!(stopping, shutdown);
        assert_eq!(matches!(deferred, Some(Message::Counter { .. })), !shutdown);
        assert!(matches!(receive.try_recv(), Ok(Message::Commit(_))));
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
