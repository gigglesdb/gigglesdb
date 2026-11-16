use super::super::{
    commit::{ManualClock, RecentLog, SequencerConfig},
    kv::{Durability, Kv, KvBackend, WriteBatch},
    kv_mem::MemKv,
    locks::LockConfig,
};
use super::*;
use std::{
    sync::{Barrier, mpsc},
    thread,
    time::Duration,
};
fn setup(config: SequencerConfig) -> (Arc<MemKv>, Arc<Sequencer>, Arc<LockManager>) {
    let mem = Arc::new(MemKv::new());
    let seq = Sequencer::open(
        Arc::new(Kv::new(mem.clone())),
        Arc::new(ManualClock::new(10)),
        config,
    )
    .unwrap();
    (mem, seq, Arc::new(LockManager::new(LockConfig::default())))
}
fn key(n: u64) -> Vec<u8> {
    let mut k = 1u64.to_be_bytes().to_vec();
    k.extend(n.to_be_bytes());
    k
}
fn txn(seq: &Arc<Sequencer>, locks: &Arc<LockManager>, id: u64, i: Isolation) -> Transaction {
    Transaction::begin(seq, locks, TxnId(id), i)
}
fn store(seq: &Arc<Sequencer>, locks: &Arc<LockManager>, id: u64, k: &[u8], v: &[u8]) -> Ts {
    let mut t = txn(seq, locks, id, Isolation::ReadCommitted);
    t.lock_for_write(Keyspace::Rows, k, None, &|| false)
        .unwrap();
    t.put(Keyspace::Rows, k.to_vec(), v.to_vec()).unwrap();
    t.commit().unwrap()
}
#[test]
fn isolation_snapshots_and_own_overlay() {
    let (_, seq, locks) = setup(SequencerConfig::default());
    let k = key(1);
    let k2 = key(2);
    let mut rr = txn(&seq, &locks, 1, Isolation::RepeatableRead);
    assert_eq!(seq.oldest_active(), None);
    store(&seq, &locks, 2, &k, b"a");
    assert_eq!(rr.get(Keyspace::Rows, &k).unwrap(), Some(b"a".to_vec()));
    let first = seq.oldest_active().unwrap();
    store(&seq, &locks, 3, &k, b"b");
    assert_eq!(rr.get(Keyspace::Rows, &k).unwrap(), Some(b"a".to_vec()));
    assert_eq!(seq.oldest_active(), Some(first));
    rr.rollback();
    let mut rc = txn(&seq, &locks, 4, Isolation::ReadCommitted);
    store(&seq, &locks, 5, &k, b"c");
    assert_eq!(rc.get(Keyspace::Rows, &k).unwrap(), Some(b"b".to_vec()));
    rc.begin_statement();
    assert_eq!(rc.get(Keyspace::Rows, &k).unwrap(), Some(b"c".to_vec()));
    rc.lock_for_write(Keyspace::Rows, &k, None, &|| false)
        .unwrap();
    rc.delete(Keyspace::Rows, k.clone()).unwrap();
    rc.lock_for_write(Keyspace::Rows, &k2, None, &|| false)
        .unwrap();
    rc.put(Keyspace::Rows, k2.clone(), b"own".to_vec()).unwrap();
    assert_eq!(rc.get(Keyspace::Rows, &k).unwrap(), None);
    for dir in [Direction::Forward, Direction::Reverse] {
        assert_eq!(
            rc.scan(
                Keyspace::Rows,
                Bound::Unbounded,
                Bound::Unbounded,
                dir,
                None
            )
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
            vec![(k2.clone(), b"own".to_vec())]
        );
    }
    let mut other = txn(&seq, &locks, 6, Isolation::ReadCommitted);
    assert_eq!(other.get(Keyspace::Rows, &k2).unwrap(), None);
    other.rollback();
    rc.rollback();
    assert!(locks.held(TxnId(4)).is_empty());
    assert_eq!(seq.oldest_active(), None);
}
#[test]
fn first_updater_and_deferred_write_snapshot() {
    for i in [Isolation::RepeatableRead, Isolation::Serializable] {
        let (_, seq, locks) = setup(SequencerConfig::default());
        let k = key(1);
        store(&seq, &locks, 1, &k, b"a");
        let mut stale = txn(&seq, &locks, 2, i);
        stale.get(Keyspace::Rows, &k).unwrap();
        store(&seq, &locks, 3, &k, b"b");
        assert!(matches!(
            stale.lock_for_write(Keyspace::Rows, &k, None, &|| false),
            Err(TxnError::Serialization(SerializationReason::WriteConflict))
        ));
        stale.rollback();
        assert!(locks.held(TxnId(2)).is_empty());
        let mut fresh = txn(&seq, &locks, 4, i);
        store(&seq, &locks, 5, &k, b"c");
        assert_eq!(
            fresh
                .lock_for_write(Keyspace::Rows, &k, None, &|| false)
                .unwrap()
                .current,
            Some(b"c".to_vec())
        );
        fresh.put(Keyspace::Rows, k.clone(), b"d".to_vec()).unwrap();
        assert_eq!(
            fresh
                .lock_for_write(Keyspace::Rows, &k, None, &|| false)
                .unwrap()
                .current,
            Some(b"d".to_vec())
        );
        fresh.rollback();
        let mut catalog = txn(&seq, &locks, 6, i);
        assert_eq!(seq.oldest_active(), None);
        catalog.put(Keyspace::Catalog, key(9), vec![1]).unwrap();
        assert!(seq.oldest_active().is_some());
        catalog.rollback();
    }
}
#[test]
fn blocked_read_committed_returns_current_value() {
    let (_, seq, locks) = setup(SequencerConfig::default());
    let k = key(1);
    store(&seq, &locks, 1, &k, b"old");
    let mut owner = txn(&seq, &locks, 2, Isolation::ReadCommitted);
    owner
        .lock_for_write(Keyspace::Rows, &k, None, &|| false)
        .unwrap();
    owner
        .put(Keyspace::Rows, k.clone(), b"new".to_vec())
        .unwrap();
    let (send, recv) = mpsc::channel();
    let s = seq.clone();
    let l = locks.clone();
    let read_key = k.clone();
    let waiter = thread::spawn(move || {
        let mut t = txn(&s, &l, 3, Isolation::ReadCommitted);
        send.send(()).unwrap();
        let v = t
            .lock_for_write(
                Keyspace::Rows,
                &read_key,
                Some(Instant::now() + Duration::from_secs(10)),
                &|| false,
            )
            .unwrap();
        assert!(v.changed_since_snapshot);
        assert_eq!(v.current, Some(b"new".to_vec()));
        t.rollback();
    });
    recv.recv().unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while locks.waiting_count() == 0 {
        assert!(Instant::now() < deadline);
        thread::yield_now();
    }
    owner.commit().unwrap();
    waiter.join().unwrap();
    assert!(locks.held(TxnId(3)).is_empty());
}
#[test]
fn serializable_scan_records_actual_coverage() {
    let (_, seq, locks) = setup(SequencerConfig::default());
    for n in 1..=4 {
        store(&seq, &locks, n, &key(n), &[n as u8]);
    }
    for dir in [Direction::Forward, Direction::Reverse] {
        let mut t = txn(&seq, &locks, 100, Isolation::Serializable);
        assert_eq!(
            t.scan(
                Keyspace::Rows,
                Bound::Unbounded,
                Bound::Unbounded,
                dir,
                Some(0)
            )
            .unwrap()
            .next(),
            None
        );
        assert!(t.read_set().ranges.is_empty());
        {
            let mut scan = t
                .scan(
                    Keyspace::Rows,
                    Bound::Unbounded,
                    Bound::Unbounded,
                    dir,
                    None,
                )
                .unwrap();
            assert!(scan.next().unwrap().is_ok());
        }
        let r = &t.read_set().ranges[0];
        match dir {
            Direction::Forward => assert_eq!(r.end, Bound::Included(key(1))),
            Direction::Reverse => assert_eq!(r.start, Bound::Included(key(4))),
        }
        t.scan(
            Keyspace::Rows,
            Bound::Excluded(&key(1)),
            Bound::Excluded(&key(4)),
            dir,
            None,
        )
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
        let r = t.read_set().ranges.last().unwrap();
        assert_eq!(r.start, Bound::Excluded(key(1)));
        assert_eq!(r.end, Bound::Excluded(key(4)));
        t.get(Keyspace::Rows, &key(99)).unwrap();
        assert!(t.read_set().points.contains(&(Keyspace::Rows, key(99))));
    }
}
#[test]
fn write_budget_replacements_and_lock_ownership() {
    let (_, seq, locks) = setup(SequencerConfig {
        write_set_bytes: 20,
        ..SequencerConfig::default()
    });
    let k = key(1);
    let mut t = txn(&seq, &locks, 1, Isolation::ReadCommitted);
    assert_eq!(
        t.put(Keyspace::Rows, k.clone(), vec![1]),
        Err(TxnError::Misuse)
    );
    t.lock_for_write(Keyspace::Rows, &k, None, &|| false)
        .unwrap();
    t.put(Keyspace::Rows, k.clone(), vec![1; 4]).unwrap();
    t.put(Keyspace::Rows, k.clone(), vec![2; 4]).unwrap();
    assert_eq!(
        t.put(Keyspace::Rows, k.clone(), vec![3; 5]),
        Err(TxnError::WriteSetLimit)
    );
    assert_eq!(t.get(Keyspace::Rows, &k).unwrap(), Some(vec![2; 4]));
    t.delete(Keyspace::Rows, k).unwrap();
    t.commit().unwrap();
    assert!(locks.held(TxnId(1)).is_empty());
}
#[test]
fn concurrent_increment_isolation() {
    for isolation in [Isolation::ReadCommitted, Isolation::RepeatableRead] {
        let (_, seq, locks) = setup(SequencerConfig::default());
        let k = key(1);
        store(&seq, &locks, 1, &k, &0u64.to_be_bytes());
        let start = Arc::new(Barrier::new(8));
        let threads: Vec<_> = (0..8)
            .map(|n| {
                let seq = seq.clone();
                let locks = locks.clone();
                let start = start.clone();
                let k = k.clone();
                thread::spawn(move || {
                    let mut t = txn(&seq, &locks, n + 2, isolation);
                    t.get(Keyspace::Rows, &k).unwrap();
                    start.wait();
                    match t.lock_for_write(Keyspace::Rows, &k, None, &|| false) {
                        Ok(v) => {
                            let n = u64::from_be_bytes(v.current.unwrap().try_into().unwrap());
                            t.put(Keyspace::Rows, k, (n + 1).to_be_bytes().to_vec())
                                .unwrap();
                            t.commit().unwrap();
                            true
                        }
                        Err(TxnError::Serialization(SerializationReason::WriteConflict)) => false,
                        Err(e) => panic!("{e:?}"),
                    }
                })
            })
            .collect();
        let success = threads
            .into_iter()
            .map(|t| t.join().unwrap())
            .filter(|ok| *ok)
            .count();
        let expected = if isolation == Isolation::ReadCommitted {
            8
        } else {
            1
        };
        assert_eq!(success, expected);
        let mut t = txn(&seq, &locks, 99, Isolation::ReadCommitted);
        assert_eq!(
            t.get(Keyspace::Rows, &k).unwrap(),
            Some((expected as u64).to_be_bytes().to_vec())
        );
    }
}
#[test]
fn parallel_unique_commits_publish_max_and_values() {
    for group_max in [1, 64] {
        let (_, seq, locks) = setup(SequencerConfig {
            group_max,
            ..SequencerConfig::default()
        });
        let start = Arc::new(Barrier::new(32));
        let workers: Vec<_> = (0..32)
            .map(|n| {
                let seq = seq.clone();
                let locks = locks.clone();
                let start = start.clone();
                thread::spawn(move || {
                    start.wait();
                    let mut ts = vec![];
                    for turn in 0..20 {
                        ts.push(store(
                            &seq,
                            &locks,
                            100 + n,
                            &key(n),
                            &(turn as u64).to_be_bytes(),
                        ));
                    }
                    assert!(ts.windows(2).all(|t| t[0] < t[1]));
                    ts
                })
            })
            .collect();
        let mut timestamps: Vec<_> = workers
            .into_iter()
            .flat_map(|w| w.join().unwrap())
            .collect();
        timestamps.sort();
        assert_eq!(timestamps.len(), 640);
        assert!(timestamps.windows(2).all(|w| w[0] < w[1]));
        assert_eq!(seq.visible_ts(), *timestamps.last().unwrap());
        let mut t = txn(&seq, &locks, 999, Isolation::ReadCommitted);
        for n in 0..32 {
            assert_eq!(
                t.get(Keyspace::Rows, &key(n)).unwrap(),
                Some(19u64.to_be_bytes().to_vec())
            );
        }
    }
}
#[test]
fn crash_seed_ambiguous_and_release() {
    for after in [false, true] {
        let (mem, seq, locks) = setup(SequencerConfig::default());
        let k = key(1);
        let first = store(&seq, &locks, 1, &k, b"first");
        let mut t = txn(&seq, &locks, 2, Isolation::ReadCommitted);
        t.lock_for_write(Keyspace::Rows, &k, None, &|| false)
            .unwrap();
        t.put(Keyspace::Rows, k.clone(), b"second".to_vec())
            .unwrap();
        if after {
            mem.fail_next_write_after_apply();
        } else {
            mem.fail_next_write(KvError::Io("before".into()));
        }
        assert_eq!(t.commit(), Err(TxnError::CommitAmbiguous));
        assert!(locks.held(TxnId(2)).is_empty());
        assert_eq!(seq.oldest_active(), None);
        assert_eq!(seq.visible_ts(), first);
        assert_eq!(
            txn(&seq, &locks, 3, Isolation::ReadCommitted).commit(),
            Ok(first)
        );
        let reopened = Sequencer::open(
            Arc::new(Kv::new(Arc::new(mem.crash()))),
            Arc::new(ManualClock::new(0)),
            SequencerConfig::default(),
        )
        .unwrap();
        let mut r = txn(&reopened, &locks, 4, Isolation::ReadCommitted);
        assert_eq!(
            r.get(Keyspace::Rows, &k).unwrap(),
            Some(if after {
                b"second".to_vec()
            } else {
                b"first".to_vec()
            })
        );
        r.rollback();
        let recovered = reopened.visible_ts();
        assert_eq!(recovered, if after { Ts(first.0 + 1) } else { first });
        let next = store(&reopened, &locks, 5, &k, b"third");
        assert!(next > recovered);
    }
}
#[test]
fn snapshots_log_horizon_and_validation_order() {
    let (_, seq, locks) = setup(SequencerConfig {
        recent_log_bytes: 1,
        ..SequencerConfig::default()
    });
    let held = seq.snapshot();
    assert_eq!(held.ts(), Ts(0));
    store(&seq, &locks, 1, &key(1), b"a");
    assert_eq!(seq.oldest_active(), Some(Ts(0)));
    let before = seq.visible_ts();
    let (s, r) = mpsc::channel();
    let result = seq.submit(CommitRequest {
        features_or: 0,
        minimum_subversion: 0,
        raw: Vec::new(),
        snapshot: Ts(0),
        writes: BTreeMap::new(),
        validation: Some(Box::new(move |log, _, snapshot, assigned| {
            assert!(log.writes_after(snapshot, |_, _, _| {}).is_err());
            s.send(assigned).unwrap();
            Err(TxnError::SnapshotTooOld)
        })),
    });
    assert_eq!(result, Err(TxnError::SnapshotTooOld));
    r.recv().unwrap();
    assert_eq!(seq.visible_ts(), before);
    drop(held);
    assert_eq!(seq.oldest_active(), None);
    let (s, r) = mpsc::channel();
    let assigned = seq
        .submit(CommitRequest {
            features_or: 0,
            minimum_subversion: 0,
            raw: Vec::new(),
            snapshot: before,
            writes: BTreeMap::from([((Keyspace::Rows, key(8)), Some(vec![8]))]),
            validation: Some(Box::new(move |_, _, _, assigned| {
                s.send(assigned).unwrap();
                Ok(())
            })),
        })
        .unwrap();
    assert_eq!(r.recv().unwrap(), assigned);
    assert_eq!(assigned.0, before.0 + 1);
    let mut log = RecentLog::new(10);
    log.append(Ts(1), Arc::from([(Keyspace::Rows, vec![1])]));
    log.append(Ts(2), Arc::from([(Keyspace::Rows, vec![2])]));
    log.trim(Some(Ts(0)));
    assert_eq!(log.horizon(), Ts(1));
    assert!(log.writes_after(Ts(0), |_, _, _| {}).is_err());
    let mut seen = vec![];
    log.writes_after(Ts(1), |ts, _, _| seen.push(ts)).unwrap();
    assert_eq!(seen, vec![Ts(2)]);
}
#[test]
fn sqlstate_and_core_routes() {
    for (e, s) in [
        (
            TxnError::Serialization(SerializationReason::ReadWriteConflict),
            "40001",
        ),
        (
            TxnError::Serialization(SerializationReason::WriteConflict),
            "40001",
        ),
        (
            TxnError::Serialization(SerializationReason::ValidationBudget),
            "40001",
        ),
        (
            TxnError::Serialization(SerializationReason::SnapshotTooOld),
            "40001",
        ),
        (TxnError::SnapshotTooOld, "40001"),
        (TxnError::Deadlock, "40P01"),
        (TxnError::LockTimeout, "55P03"),
        (TxnError::Cancelled, "57014"),
        (TxnError::LockLimit, "53200"),
        (TxnError::WriteSetLimit, "53200"),
        (TxnError::CommitAmbiguous, "08007"),
        (TxnError::Fenced, "XX000"),
        (TxnError::Misuse, "XX000"),
    ] {
        assert_eq!(e.sqlstate(), s);
        let _: crate::CoreError = e.into();
    }
    assert!(matches!(
        crate::CoreError::from(TxnError::CommitAmbiguous),
        crate::CoreError::Storage(_)
    ));
}

#[test]
fn sequencer_read_only_clock_overflow_and_malformed_metadata() {
    let mem = Arc::new(MemKv::new());
    let clock = Arc::new(ManualClock::new(1u64 << 48));
    let seq = Sequencer::open(
        Arc::new(Kv::new(mem.clone())),
        clock.clone(),
        SequencerConfig::default(),
    )
    .unwrap();
    let writes = || BTreeMap::from([((Keyspace::Rows, key(1)), Some(vec![1]))]);
    assert_eq!(
        seq.submit(CommitRequest {
            features_or: 0,
            minimum_subversion: 0,
            raw: Vec::new(),
            snapshot: Ts(0),
            writes: writes(),
            validation: None
        }),
        Err(TxnError::Codec(CodecError::Clock))
    );
    assert_eq!(seq.visible_ts(), Ts(0));
    assert_eq!(
        seq.kv().get(Keyspace::Meta, b"last_commit_ts").unwrap(),
        None
    );
    clock.set(1);
    let first = seq
        .submit(CommitRequest {
            features_or: 0,
            minimum_subversion: 0,
            raw: Vec::new(),
            snapshot: Ts(0),
            writes: writes(),
            validation: None,
        })
        .unwrap();
    mem.fail_next_write(KvError::Io("armed".into()));
    assert_eq!(
        seq.submit(CommitRequest {
            features_or: 0,
            minimum_subversion: 0,
            raw: Vec::new(),
            snapshot: first,
            writes: BTreeMap::new(),
            validation: None
        })
        .unwrap(),
        first
    );
    assert_eq!(
        seq.submit(CommitRequest {
            features_or: 0,
            minimum_subversion: 0,
            raw: Vec::new(),
            snapshot: first,
            writes: writes(),
            validation: None
        }),
        Err(TxnError::CommitAmbiguous)
    );
    let mem = Arc::new(MemKv::new());
    let mut batch = WriteBatch::default();
    batch.put(Keyspace::Meta, b"last_commit_ts".to_vec(), vec![0; 7]);
    mem.write(batch, Durability::Sync).unwrap();
    assert!(matches!(
        Sequencer::open(
            Arc::new(Kv::new(mem)),
            Arc::new(ManualClock::new(0)),
            SequencerConfig::default()
        ),
        Err(TxnError::Kv(KvError::Corrupt(_)))
    ));
}
#[test]
fn serializable_deleted_gaps_limit_and_full_exhaustion() {
    let (_, seq, locks) = setup(SequencerConfig::default());
    for n in 1..=5 {
        store(&seq, &locks, n, &key(n), &[n as u8]);
    }
    let mut t = txn(&seq, &locks, 9, Isolation::ReadCommitted);
    for n in [1, 3, 5] {
        t.lock_for_write(Keyspace::Rows, &key(n), None, &|| false)
            .unwrap();
        t.delete(Keyspace::Rows, key(n)).unwrap();
    }
    t.commit().unwrap();
    for dir in [Direction::Forward, Direction::Reverse] {
        let mut t = txn(&seq, &locks, 100, Isolation::Serializable);
        let found = t
            .scan(
                Keyspace::Rows,
                Bound::Included(&key(0)),
                Bound::Included(&key(6)),
                dir,
                Some(1),
            )
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(found.len(), 1);
        let r = &t.read_set().ranges[0];
        match dir {
            Direction::Forward => {
                assert_eq!(r.start, Bound::Included(key(0)));
                assert_eq!(r.end, Bound::Included(key(2)));
            }
            Direction::Reverse => {
                assert_eq!(r.start, Bound::Included(key(4)));
                assert_eq!(r.end, Bound::Included(key(6)));
            }
        }
        t.scan(
            Keyspace::Rows,
            Bound::Included(&key(0)),
            Bound::Included(&key(6)),
            dir,
            None,
        )
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
        let r = t.read_set().ranges.last().unwrap();
        assert_eq!(r.start, Bound::Included(key(0)));
        assert_eq!(r.end, Bound::Included(key(6)));
    }
}
#[test]
fn concurrent_validation_observes_accepted_predecessors_only() {
    let (_, seq, _) = setup(SequencerConfig {
        group_max: 64,
        ..SequencerConfig::default()
    });
    let held = seq.snapshot();
    let gate = Arc::new(Barrier::new(16));
    let events = Arc::new(std::sync::Mutex::new(Vec::<(Ts, u64)>::new()));
    let workers: Vec<_> = (0..16)
        .map(|n| {
            let seq = seq.clone();
            let gate = gate.clone();
            let events = events.clone();
            thread::spawn(move || {
                gate.wait();
                let reject = n % 3 == 0;
                let result = seq.submit(CommitRequest {
                    features_or: 0,
                    minimum_subversion: 0,
                    raw: Vec::new(),
                    snapshot: Ts(0),
                    writes: BTreeMap::from([((Keyspace::Rows, key(n)), Some(vec![n as u8]))]),
                    validation: Some(Box::new(move |log, _, snapshot, assigned| {
                        let mut expected = events.lock().unwrap();
                        let mut actual = vec![];
                        log.writes_after(snapshot, |ts, _, key| {
                            actual.push((ts, u64::from_be_bytes(key[8..].try_into().unwrap())));
                        })
                        .unwrap();
                        assert_eq!(actual, *expected);
                        if reject {
                            return Err(TxnError::Serialization(
                                SerializationReason::ReadWriteConflict,
                            ));
                        }
                        if let Some((prior, _)) = expected.last() {
                            assert_eq!(assigned.0, prior.0 + 1);
                        }
                        expected.push((assigned, n));
                        Ok(())
                    })),
                });
                if reject {
                    assert_eq!(
                        result,
                        Err(TxnError::Serialization(
                            SerializationReason::ReadWriteConflict
                        ))
                    );
                    None
                } else {
                    Some(result.unwrap())
                }
            })
        })
        .collect();
    let mut accepted: Vec<_> = workers
        .into_iter()
        .filter_map(|w| w.join().unwrap())
        .collect();
    accepted.sort();
    let events = events.lock().unwrap();
    assert_eq!(
        accepted,
        events.iter().map(|(ts, _)| *ts).collect::<Vec<_>>()
    );
    assert_eq!(seq.visible_ts(), *accepted.last().unwrap());
    for n in 0..16 {
        assert_eq!(
            mvcc::read_at(seq.kv(), Keyspace::Rows, &key(n), seq.visible_ts()).unwrap(),
            if n % 3 == 0 {
                None
            } else {
                Some(vec![n as u8])
            }
        );
    }
    drop(held);
}
struct CountWrites {
    gate: Option<(mpsc::Sender<()>, std::sync::Mutex<mpsc::Receiver<()>>)>,
    mem: Arc<MemKv>,
    writes: std::sync::atomic::AtomicUsize,
}
impl super::super::kv::KvRead for CountWrites {
    fn get(&self, ks: Keyspace, key: &[u8]) -> Result<Option<Vec<u8>>, KvError> {
        super::super::kv::KvRead::get(&*self.mem, ks, key)
    }
    fn range(
        &self,
        ks: Keyspace,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        dir: Direction,
    ) -> Result<super::super::kv::KvIter<'_>, KvError> {
        super::super::kv::KvRead::range(&*self.mem, ks, start, end, dir)
    }
}
impl KvBackend for CountWrites {
    fn write(&self, batch: WriteBatch, d: Durability) -> Result<(), KvError> {
        assert_eq!(d, Durability::Sync);
        if let Some((entered, release)) = &self.gate {
            entered.send(()).unwrap();
            release.lock().unwrap().recv().unwrap();
        }
        self.writes
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.mem.write(batch, d)
    }
    fn flush_barrier(&self) -> Result<(), KvError> {
        self.mem.flush_barrier()
    }
    fn snapshot(&self) -> Result<Arc<dyn super::super::kv::KvRead>, KvError> {
        self.mem.snapshot()
    }
    fn engine_name(&self) -> &'static str {
        "counting"
    }
}
#[test]
fn deterministic_group_hook_order_single_sync_and_ambiguous_all() {
    for fail in [false, true] {
        let mem = Arc::new(MemKv::new());
        let backend = Arc::new(CountWrites {
            gate: None,
            mem: mem.clone(),
            writes: std::sync::atomic::AtomicUsize::new(0),
        });
        let seq = Sequencer::open(
            Arc::new(Kv::new(backend.clone())),
            Arc::new(ManualClock::new(10)),
            SequencerConfig::default(),
        )
        .unwrap();
        let held = seq.snapshot();
        let args = Arc::new(std::sync::Mutex::new(vec![]));
        let requests = (1..=4)
            .map(|n| {
                let args = args.clone();
                CommitRequest {
                    features_or: 0,
                    minimum_subversion: 0,
                    raw: Vec::new(),
                    snapshot: Ts(0),
                    writes: if n == 4 {
                        BTreeMap::new()
                    } else {
                        BTreeMap::from([((Keyspace::Rows, key(n)), Some(vec![n as u8]))])
                    },
                    validation: Some(Box::new(move |log, _, snapshot, assigned| {
                        let mut actual = vec![];
                        log.writes_after(snapshot, |_, _, k| actual.push(k.to_vec()))
                            .unwrap();
                        assert_eq!(
                            actual,
                            match n {
                                1 => vec![],
                                2 | 3 => vec![key(1)],
                                4 => vec![key(1), key(3)],
                                _ => unreachable!(),
                            }
                        );
                        args.lock().unwrap().push(assigned);
                        if n == 2 {
                            Err(TxnError::Serialization(
                                SerializationReason::ReadWriteConflict,
                            ))
                        } else {
                            Ok(())
                        }
                    })),
                }
            })
            .collect();
        if fail {
            mem.fail_next_write_after_apply();
        }
        let mut log = RecentLog::new(256 * 1024 * 1024);
        let results = seq.process_group_for_test(requests, &ManualClock::new(10), &mut log);
        assert_eq!(backend.writes.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(
            results[1],
            Err(TxnError::Serialization(
                SerializationReason::ReadWriteConflict
            ))
        );
        let args = args.lock().unwrap();
        assert_eq!(args[1], args[2]);
        assert_eq!(args[2].0, args[0].0 + 1);
        if fail {
            for n in [0, 2, 3] {
                assert_eq!(results[n], Err(TxnError::CommitAmbiguous));
            }
            assert_eq!(seq.visible_ts(), Ts(0));
            assert_eq!(
                seq.process_group_for_test(
                    vec![CommitRequest {
                        features_or: 0,
                        minimum_subversion: 0,
                        raw: Vec::new(),
                        snapshot: Ts(0),
                        writes: BTreeMap::new(),
                        validation: None
                    }],
                    &ManualClock::new(10),
                    &mut log
                ),
                vec![Err(TxnError::Fenced)]
            );
            let durable = mem.crash();
            let recovered = Sequencer::open(
                Arc::new(Kv::new(Arc::new(durable))),
                Arc::new(ManualClock::new(0)),
                SequencerConfig::default(),
            )
            .unwrap();
            assert_eq!(recovered.visible_ts(), args[2]);
            for n in [1, 3] {
                assert_eq!(
                    mvcc::read_at(
                        recovered.kv(),
                        Keyspace::Rows,
                        &key(n),
                        recovered.visible_ts()
                    )
                    .unwrap(),
                    Some(vec![n as u8])
                );
            }
        } else {
            for n in [0, 2, 3] {
                assert_eq!(results[n], Ok(args[n]));
            }
            assert_eq!(seq.visible_ts(), args[2]);
            assert_eq!(
                mvcc::read_at(seq.kv(), Keyspace::Rows, &key(2), seq.visible_ts()).unwrap(),
                None
            );
        }
        drop(held);
    }
}

#[test]
fn mvcc_seeded_model_all_bounds_and_timestamp_extremes() {
    use super::super::{codec, mvcc};
    use std::collections::BTreeMap;
    let mem = MemKv::new();
    let mut model = BTreeMap::<Vec<u8>, BTreeMap<Ts, Option<Vec<u8>>>>::new();
    let mut seed = 0x5eeda11ceu64;
    let mut batch = WriteBatch::default();
    for n in 1..=12u64 {
        for version in 0..40u64 {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            let ts = Ts(match version {
                0 => 0,
                1 => u64::MAX,
                _ => seed % 1000,
            });
            let value = (!seed.is_multiple_of(4)).then(|| seed.to_be_bytes().to_vec());
            batch.put(
                Keyspace::Rows,
                codec::user_key_seek_at(&key(n), ts),
                value
                    .as_deref()
                    .map(mvcc::encode_live)
                    .unwrap_or_else(mvcc::encode_tombstone),
            );
            model.entry(key(n)).or_default().insert(ts, value);
        }
    }
    mem.write(batch, Durability::Sync).unwrap();
    let mut bounds = vec![Bound::Unbounded];
    for n in [0, 1, 6, 12, 13] {
        bounds.push(Bound::Included(key(n)));
        bounds.push(Bound::Excluded(key(n)));
    }
    for snapshot in [Ts(0), Ts(1), Ts(250), Ts(500), Ts(999), Ts(u64::MAX)] {
        for n in 0..=13 {
            let expected = model
                .get(&key(n))
                .and_then(|versions| versions.range(..=snapshot).next_back())
                .and_then(|(_, value)| value.clone());
            assert_eq!(
                mvcc::read_at(&mem, Keyspace::Rows, &key(n), snapshot).unwrap(),
                expected
            );
            let latest = model
                .get(&key(n))
                .and_then(|versions| versions.last_key_value())
                .map(|(ts, value)| (*ts, value.clone()));
            assert_eq!(
                mvcc::latest_version(&mem, Keyspace::Rows, &key(n)).unwrap(),
                latest
            );
        }
        for start in &bounds {
            for end in &bounds {
                let expected: Vec<_> = model
                    .iter()
                    .filter(|(k, _)| {
                        let lower = match start {
                            Bound::Unbounded => true,
                            Bound::Included(a) => *k >= a,
                            Bound::Excluded(a) => *k > a,
                        };
                        let upper = match end {
                            Bound::Unbounded => true,
                            Bound::Included(a) => *k <= a,
                            Bound::Excluded(a) => *k < a,
                        };
                        lower && upper
                    })
                    .filter_map(|(k, versions)| {
                        versions
                            .range(..=snapshot)
                            .next_back()
                            .and_then(|(_, v)| v.clone())
                            .map(|v| (k.clone(), v))
                    })
                    .collect();
                for dir in [Direction::Forward, Direction::Reverse] {
                    let mut expected = expected.clone();
                    if dir == Direction::Reverse {
                        expected.reverse();
                    }
                    let got = mvcc::VersionedRange::new(
                        &mem,
                        Keyspace::Rows,
                        start.as_ref().map(Vec::as_slice),
                        end.as_ref().map(Vec::as_slice),
                        dir,
                        snapshot,
                    )
                    .unwrap()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap();
                    assert_eq!(
                        got, expected,
                        "snapshot={snapshot:?}, start={start:?}, end={end:?}, dir={dir:?}"
                    );
                }
            }
        }
    }
}

#[test]
fn mvcc_lazy_cursors_and_strict_envelopes() {
    use super::super::{
        codec,
        kv::{KvError, KvIter, KvRead},
        mvcc,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Counting {
        mem: MemKv,
        count: Arc<AtomicUsize>,
    }
    impl KvRead for Counting {
        fn get(&self, ks: Keyspace, k: &[u8]) -> Result<Option<Vec<u8>>, KvError> {
            self.mem.get(ks, k)
        }
        fn range(
            &self,
            ks: Keyspace,
            start: Bound<&[u8]>,
            end: Bound<&[u8]>,
            dir: Direction,
        ) -> Result<KvIter<'_>, KvError> {
            let count = self.count.clone();
            Ok(Box::new(self.mem.range(ks, start, end, dir)?.inspect(
                move |_| {
                    count.fetch_add(1, Ordering::SeqCst);
                },
            )))
        }
    }
    assert_eq!(
        mvcc::decode(&mvcc::encode_live(&[])).unwrap(),
        Some(&[][..])
    );
    assert_eq!(mvcc::encode_tombstone(), vec![1]);
    assert_eq!(mvcc::decode(&[1]).unwrap(), None);
    for invalid in [&[][..], &[2][..], &[1, 0][..], &[255, 1][..]] {
        assert!(matches!(mvcc::decode(invalid), Err(KvError::Corrupt(_))));
    }
    let backend = Counting {
        mem: MemKv::new(),
        count: Arc::new(AtomicUsize::new(0)),
    };
    let mut batch = WriteBatch::default();
    for n in 1..=20 {
        for ts in 0..100 {
            batch.put(
                Keyspace::Rows,
                codec::user_key_seek_at(&key(n), Ts(ts)),
                mvcc::encode_live(&ts.to_be_bytes()),
            );
        }
    }
    backend.mem.write(batch, Durability::Sync).unwrap();
    for dir in [Direction::Forward, Direction::Reverse] {
        backend.count.store(0, Ordering::SeqCst);
        let mut cursor = mvcc::VersionedRange::new(
            &backend,
            Keyspace::Rows,
            Bound::Unbounded,
            Bound::Unbounded,
            dir,
            Ts(u64::MAX),
        )
        .unwrap();
        assert_eq!(backend.count.load(Ordering::SeqCst), 0);
        assert_eq!(
            cursor.next().unwrap().unwrap().0,
            key(if dir == Direction::Forward { 1 } else { 20 })
        );
        assert_eq!(
            backend.count.load(Ordering::SeqCst),
            if dir == Direction::Forward { 1 } else { 101 }
        );
    }
}
#[test]
fn group_ack_waits_for_sync_and_empty_scan_has_no_predicate() {
    let mem = Arc::new(MemKv::new());
    let (entered_s, entered_r) = mpsc::channel();
    let (release_s, release_r) = mpsc::channel();
    let backend = Arc::new(CountWrites {
        gate: Some((entered_s, std::sync::Mutex::new(release_r))),
        mem: mem.clone(),
        writes: std::sync::atomic::AtomicUsize::new(0),
    });
    let seq = Sequencer::open(
        Arc::new(Kv::new(backend)),
        Arc::new(ManualClock::new(1)),
        SequencerConfig::default(),
    )
    .unwrap();
    let worker_seq = seq.clone();
    let (done_s, done_r) = mpsc::channel();
    let worker = thread::spawn(move || {
        let result = worker_seq.submit(CommitRequest {
            features_or: 0,
            minimum_subversion: 0,
            raw: Vec::new(),
            snapshot: Ts(0),
            writes: BTreeMap::from([((Keyspace::Rows, key(1)), Some(vec![1]))]),
            validation: None,
        });
        done_s.send(result).unwrap();
    });
    let entered = entered_r.recv_timeout(Duration::from_secs(10));
    let reader_seq = seq.clone();
    let (read_s, read_r) = mpsc::channel();
    let reader = thread::spawn(move || {
        let snapshot = reader_seq.snapshot();
        let visible = reader_seq.visible_ts();
        let ts = snapshot.ts();
        drop(snapshot);
        let second = reader_seq.snapshot();
        drop(second);
        read_s.send((ts, visible)).unwrap();
    });
    let read = read_r.recv_timeout(Duration::from_secs(1));
    let premature = done_r.try_recv();
    // Always release the backend before asserting, so failures cannot strand the worker.
    let released = release_s.send(());
    let result = done_r.recv_timeout(Duration::from_secs(10));
    worker.join().unwrap();
    reader.join().unwrap();
    entered.unwrap();
    released.unwrap();
    assert_eq!(read.unwrap(), (Ts(0), Ts(0)));
    assert!(matches!(premature, Err(mpsc::TryRecvError::Empty)));
    let ts = result.unwrap().unwrap();
    assert_eq!(seq.visible_ts(), ts);
    let published = seq.snapshot();
    assert_eq!(published.ts(), ts);
    drop(published);
    let recovered = Sequencer::open(
        Arc::new(Kv::new(Arc::new(mem.crash()))),
        Arc::new(ManualClock::new(0)),
        SequencerConfig::default(),
    )
    .unwrap();
    assert_eq!(recovered.visible_ts(), ts);
    let locks = Arc::new(LockManager::new(LockConfig::default()));
    let mut t = txn(&seq, &locks, 1, Isolation::Serializable);
    for dir in [Direction::Forward, Direction::Reverse] {
        for (start, end) in [
            (Bound::Included(key(2)), Bound::Included(key(1))),
            (Bound::Excluded(key(1)), Bound::Included(key(1))),
            (Bound::Included(key(1)), Bound::Excluded(key(1))),
        ] {
            assert!(
                t.scan(
                    Keyspace::Rows,
                    start.as_ref().map(Vec::as_slice),
                    end.as_ref().map(Vec::as_slice),
                    dir,
                    None
                )
                .unwrap()
                .next()
                .is_none()
            );
        }
    }
    assert!(t.read_set().ranges.is_empty());
}

#[test]
fn two_thousand_commits_retain_exact_active_history() {
    let mem = Arc::new(MemKv::new());
    let seq = Sequencer::open(
        Arc::new(Kv::new(mem)),
        Arc::new(ManualClock::new(0)),
        SequencerConfig {
            group_wait: Duration::ZERO,
            ..SequencerConfig::default()
        },
    )
    .unwrap();
    let old = seq.snapshot();
    assert_eq!(old.ts(), Ts(0));
    for n in 1..=2000u64 {
        let ts = seq
            .submit(CommitRequest {
                features_or: 0,
                minimum_subversion: 0,
                raw: Vec::new(),
                snapshot: old.ts(),
                writes: BTreeMap::from([(
                    (Keyspace::Rows, key(1)),
                    Some(n.to_be_bytes().to_vec()),
                )]),
                validation: None,
            })
            .unwrap();
        assert_eq!(ts, Ts(n));
    }
    let last = seq
        .submit(CommitRequest {
            features_or: 0,
            minimum_subversion: 0,
            raw: Vec::new(),
            snapshot: old.ts(),
            writes: BTreeMap::new(),
            validation: Some(Box::new(|log, _, snapshot, assigned| {
                assert_eq!(log.len(), 2000);
                let mut expected = 1;
                log.writes_after(snapshot, |ts, ks, k| {
                    assert_eq!(ts, Ts(expected));
                    assert_eq!(ks, Keyspace::Rows);
                    assert_eq!(k, key(1));
                    expected += 1;
                })
                .unwrap();
                assert_eq!(expected, 2001);
                assert_eq!(assigned, Ts(2000));
                Ok(())
            })),
        })
        .unwrap();
    assert_eq!(last, Ts(2000));
    drop(old);
}
#[test]
fn concurrent_snapshot_registration_retains_every_needed_timestamp() {
    let mem = Arc::new(MemKv::new());
    let seq = Sequencer::open(
        Arc::new(Kv::new(mem)),
        Arc::new(ManualClock::new(0)),
        SequencerConfig {
            group_wait: Duration::ZERO,
            recent_log_bytes: 64 * 1024 * 1024,
            ..SequencerConfig::default()
        },
    )
    .unwrap();
    let start = Arc::new(Barrier::new(8));
    let workers: Vec<_> = (0..8)
        .map(|_| {
            let seq = seq.clone();
            let start = start.clone();
            thread::spawn(move || {
                start.wait();
                let deadline = Instant::now() + Duration::from_secs(2);
                let mut commits = 0;
                while Instant::now() < deadline {
                    let handle = seq.snapshot();
                    let snapshot = handle.ts();
                    let ts = seq
                        .submit(CommitRequest {
                            features_or: 0,
                            minimum_subversion: 0,
                            raw: Vec::new(),
                            snapshot,
                            writes: BTreeMap::from([((Keyspace::Rows, key(1)), Some(vec![1]))]),
                            validation: Some(Box::new(move |log, _, snapshot, assigned| {
                                let mut expected = snapshot.0 + 1;
                                log.writes_after(snapshot, |ts, ks, k| {
                                    assert_eq!(ts, Ts(expected));
                                    assert_eq!(ks, Keyspace::Rows);
                                    assert_eq!(k, key(1));
                                    expected += 1;
                                })
                                .expect("active snapshot must retain complete history");
                                assert_eq!(expected, assigned.0);
                                Ok(())
                            })),
                        })
                        .unwrap();
                    assert!(ts > snapshot);
                    drop(handle);
                    commits += 1;
                }
                assert!(commits > 0);
                commits
            })
        })
        .collect();
    let total: u64 = workers.into_iter().map(|w| w.join().unwrap()).sum();
    assert_eq!(seq.visible_ts(), Ts(total));
    assert_eq!(seq.oldest_active(), None);
}

fn rw_conflict() -> TxnError {
    TxnError::Serialization(SerializationReason::ReadWriteConflict)
}
fn write_owned(t: &mut Transaction, k: &[u8], value: &[u8]) {
    t.lock_for_write(Keyspace::Rows, k, None, &|| false)
        .unwrap();
    t.put(Keyspace::Rows, k.to_vec(), value.to_vec()).unwrap();
}
#[test]
fn doctors_serializable_prevents_write_skew_repeatable_read_allows_it() {
    for isolation in [Isolation::Serializable, Isolation::RepeatableRead] {
        let (_, seq, locks) = setup(SequencerConfig::default());
        store(&seq, &locks, 1, &key(1), b"on");
        store(&seq, &locks, 2, &key(2), b"on");
        let mut a = txn(&seq, &locks, 3, isolation);
        let mut b = txn(&seq, &locks, 4, isolation);
        for t in [&mut a, &mut b] {
            assert_eq!(
                t.get(Keyspace::Rows, &key(1)).unwrap(),
                Some(b"on".to_vec())
            );
            assert_eq!(
                t.get(Keyspace::Rows, &key(2)).unwrap(),
                Some(b"on".to_vec())
            );
        }
        write_owned(&mut a, &key(1), b"off");
        write_owned(&mut b, &key(2), b"off");
        assert!(a.commit().is_ok());
        let result = b.commit();
        if isolation == Isolation::Serializable {
            assert_eq!(result, Err(rw_conflict()));
        } else {
            assert!(result.is_ok());
        }
    }
}
#[test]
fn serializable_missing_points_and_empty_predicate_edges() {
    for fallback in [false, true] {
        for dir in [Direction::Forward, Direction::Reverse] {
            for include_start in [false, true] {
                for include_end in [false, true] {
                    for inserted in [9, 10, 15, 20, 25] {
                        let (_, seq, locks) = setup(SequencerConfig {
                            recent_log_bytes: if fallback { 0 } else { 256 * 1024 * 1024 },
                            ..SequencerConfig::default()
                        });
                        let mut t = txn(&seq, &locks, 1, Isolation::Serializable);
                        let lo = key(10);
                        let hi = key(20);
                        let start = if include_start {
                            Bound::Included(lo.as_slice())
                        } else {
                            Bound::Excluded(lo.as_slice())
                        };
                        let end = if include_end {
                            Bound::Included(hi.as_slice())
                        } else {
                            Bound::Excluded(hi.as_slice())
                        };
                        assert_eq!(
                            t.scan(Keyspace::Rows, start, end, dir, None)
                                .unwrap()
                                .count(),
                            0
                        );
                        store(&seq, &locks, 2, &key(inserted), b"insert");
                        write_owned(&mut t, &key(1000), b"elsewhere");
                        let result = if fallback {
                            let (result, validator) = commit_with_observed_validator(
                                t,
                                super::super::ssi_validate::ValidationConfig::default(),
                            );
                            assert_eq!(validator.fallback_count(), 1);
                            result
                        } else {
                            t.commit()
                        };
                        let inside = inserted > 10 && inserted < 20
                            || inserted == 10 && include_start
                            || inserted == 20 && include_end;
                        if inside {
                            assert_eq!(result, Err(rw_conflict()));
                        } else {
                            assert!(result.is_ok());
                        }
                    }
                }
            }
        }
        let (_, seq, locks) = setup(SequencerConfig {
            recent_log_bytes: if fallback { 0 } else { 256 * 1024 * 1024 },
            ..SequencerConfig::default()
        });
        let mut t = txn(&seq, &locks, 1, Isolation::Serializable);
        assert_eq!(t.get(Keyspace::Rows, &key(99)).unwrap(), None);
        store(&seq, &locks, 2, &key(99), b"new");
        let result = if fallback {
            let (result, validator) = commit_with_observed_validator(
                t,
                super::super::ssi_validate::ValidationConfig::default(),
            );
            assert_eq!(validator.fallback_count(), 1);
            result
        } else {
            t.commit()
        };
        assert_eq!(result, Err(rw_conflict()));
    }
}
#[test]
fn serializable_limit_tracks_only_consumed_prefix() {
    for inserted in [15, 50] {
        let (_, seq, locks) = setup(SequencerConfig::default());
        for n in [0, 10, 20, 30] {
            store(&seq, &locks, n + 1, &key(n), b"old");
        }
        let mut t = txn(&seq, &locks, 100, Isolation::Serializable);
        let rows = t
            .scan(
                Keyspace::Rows,
                Bound::Included(&key(0)),
                Bound::Excluded(&key(990)),
                Direction::Forward,
                Some(3),
            )
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            rows.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>(),
            vec![key(0), key(10), key(20)]
        );
        store(&seq, &locks, 101, &key(inserted), b"new");
        if inserted == 15 {
            assert_eq!(t.commit(), Err(rw_conflict()));
        } else {
            assert!(t.commit().is_ok());
        }
    }
}
#[test]
fn fekete_read_only_snapshot_anomaly_has_no_serializable_completion() {
    // RR batch reads old checking and plans savings. RR deposit changes checking.
    // Observer sees new checking and old savings. Batch then publishes savings:
    // batch -> deposit -> observer -> batch. SER observer must reject that view.
    for isolation in [Isolation::Serializable, Isolation::RepeatableRead] {
        let (_, seq, locks) = setup(SequencerConfig::default());
        let checking = key(1);
        let savings = key(2);
        store(&seq, &locks, 1, &checking, b"100");
        store(&seq, &locks, 2, &savings, b"100");
        let mut batch = txn(&seq, &locks, 3, Isolation::RepeatableRead);
        assert_eq!(
            batch.get(Keyspace::Rows, &checking).unwrap(),
            Some(b"100".to_vec())
        );
        let mut deposit = txn(&seq, &locks, 4, Isolation::RepeatableRead);
        write_owned(&mut deposit, &checking, b"200");
        deposit.commit().unwrap();
        let mut observer = txn(&seq, &locks, 5, isolation);
        assert_eq!(
            observer.get(Keyspace::Rows, &checking).unwrap(),
            Some(b"200".to_vec())
        );
        assert_eq!(
            observer.get(Keyspace::Rows, &savings).unwrap(),
            Some(b"100".to_vec())
        );
        write_owned(&mut batch, &savings, b"200");
        batch.commit().unwrap();
        if isolation == Isolation::Serializable {
            assert_eq!(observer.commit(), Err(rw_conflict()));
        } else {
            assert!(observer.commit().is_ok());
        }
    }
}
#[test]
fn serializable_disjoint_writers_stress_and_read_only_paths() {
    let (_, seq, locks) = setup(SequencerConfig {
        group_wait: Duration::ZERO,
        ..SequencerConfig::default()
    });
    let barrier = Arc::new(Barrier::new(8));
    let threads: Vec<_> = (0..8)
        .map(|n| {
            let seq = seq.clone();
            let locks = locks.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                for turn in 0..200 {
                    let mut t = txn(&seq, &locks, n + 1, Isolation::Serializable);
                    let _ = t.get(Keyspace::Rows, &key(n)).unwrap();
                    write_owned(&mut t, &key(n), &(turn as u64).to_be_bytes());
                    t.commit().unwrap();
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    let mut reader = txn(&seq, &locks, 99, Isolation::Serializable);
    assert_eq!(
        reader.get(Keyspace::Rows, &key(0)).unwrap(),
        Some(199u64.to_be_bytes().to_vec())
    );
    reader.commit().unwrap();
    let (mem, seq, locks) = setup(SequencerConfig::default());
    mem.fail_next_write(KvError::Io("fence".into()));
    assert_eq!(store_failed(&seq), Err(TxnError::CommitAmbiguous));
    for i in [Isolation::ReadCommitted, Isolation::RepeatableRead] {
        assert_eq!(txn(&seq, &locks, 1, i).commit(), Ok(Ts(0)));
    }
    assert_eq!(
        txn(&seq, &locks, 2, Isolation::Serializable).commit(),
        Err(TxnError::Fenced)
    );
}
fn store_failed(seq: &Arc<Sequencer>) -> Result<Ts, TxnError> {
    seq.submit(CommitRequest {
        features_or: 0,
        minimum_subversion: 0,
        raw: Vec::new(),
        snapshot: Ts(0),
        writes: BTreeMap::from([((Keyspace::Rows, key(1)), Some(vec![1]))]),
        validation: None,
    })
}

fn commit_with_observed_validator(
    mut t: Transaction,
    config: super::super::ssi_validate::ValidationConfig,
) -> (
    Result<Ts, TxnError>,
    Arc<super::super::ssi_validate::Validator>,
) {
    let snapshot = t.snapshot_ts();
    let validator = Arc::new(super::super::ssi_validate::Validator::new(
        t.read_set().clone(),
        config,
    ));
    let check = validator.clone();
    let seq = t.local_seq().clone();
    let result = seq.submit(CommitRequest {
        features_or: 0,
        minimum_subversion: 0,
        raw: Vec::new(),
        snapshot,
        writes: std::mem::take(&mut t.writes),
        validation: Some(Box::new(move |log, kv, snapshot, _| {
            check.validate(log, kv, snapshot)
        })),
    });
    drop(t);
    (result, validator)
}
#[test]
fn serializable_doctors_forced_fallback_and_unrelated_success() {
    for budget in [0, 1] {
        let (_, seq, locks) = setup(SequencerConfig {
            recent_log_bytes: budget,
            ..SequencerConfig::default()
        });
        store(&seq, &locks, 1, &key(1), b"on");
        store(&seq, &locks, 2, &key(2), b"on");
        let mut a = txn(&seq, &locks, 3, Isolation::Serializable);
        let mut b = txn(&seq, &locks, 4, Isolation::Serializable);
        for t in [&mut a, &mut b] {
            t.get(Keyspace::Rows, &key(1)).unwrap();
            t.get(Keyspace::Rows, &key(2)).unwrap();
        }
        write_owned(&mut a, &key(1), b"off");
        write_owned(&mut b, &key(2), b"off");
        a.commit().unwrap();
        let (result, validator) = commit_with_observed_validator(
            b,
            super::super::ssi_validate::ValidationConfig::default(),
        );
        assert_eq!(result, Err(rw_conflict()));
        assert_eq!(validator.fallback_count(), 1);
        let mut unrelated = txn(&seq, &locks, 5, Isolation::Serializable);
        assert_eq!(unrelated.get(Keyspace::Rows, &key(100)).unwrap(), None);
        store(&seq, &locks, 6, &key(200), b"outside");
        let (result, validator) = commit_with_observed_validator(
            unrelated,
            super::super::ssi_validate::ValidationConfig::default(),
        );
        assert!(result.is_ok());
        assert_eq!(validator.fallback_count(), 1);
    }
}
#[test]
fn grouped_serializable_fallback_sees_unpublished_accepted_write() {
    for fallback in [false, true] {
        let (_, seq, locks) = setup(SequencerConfig::default());
        let seed = store(&seq, &locks, 1, &key(99), b"seed");
        let mut log = RecentLog::new(if fallback { 0 } else { 1024 });
        if fallback {
            log.append(seed, Arc::from([(Keyspace::Rows, key(99))]));
            log.trim(Some(Ts(0)));
            assert!(log.writes_after(Ts(0), |_, _, _| {}).is_err());
        }
        let first = Arc::new(super::super::ssi_validate::Validator::new(
            ReadSet::default(),
            super::super::ssi_validate::ValidationConfig::default(),
        ));
        let second = Arc::new(super::super::ssi_validate::Validator::new(
            ReadSet {
                points: BTreeSet::from([(Keyspace::Rows, key(1))]),
                ranges: vec![],
            },
            super::super::ssi_validate::ValidationConfig::default(),
        ));
        let checks = [first.clone(), second.clone()];
        let requests = checks
            .into_iter()
            .enumerate()
            .map(|(n, validator)| CommitRequest {
                features_or: 0,
                minimum_subversion: 0,
                raw: Vec::new(),
                snapshot: Ts(0),
                writes: if n == 0 {
                    BTreeMap::from([((Keyspace::Rows, key(1)), Some(b"staged".to_vec()))])
                } else {
                    BTreeMap::new()
                },
                validation: Some(Box::new(move |log, kv, snapshot, _| {
                    validator.validate(log, kv, snapshot)
                })),
            })
            .collect();
        let results = seq.process_group_for_test(requests, &ManualClock::new(10), &mut log);
        assert!(results[0].is_ok());
        assert_eq!(results[1], Err(rw_conflict()));
        if fallback {
            assert!(second.fallback_count() > 0);
        } else {
            assert_eq!(second.fallback_count(), 0);
        }
    }
}
#[test]
fn serializable_fallback_budget_counts_old_raw_versions() {
    let (_, seq, locks) = setup(SequencerConfig {
        recent_log_bytes: 0,
        group_wait: Duration::ZERO,
        ..SequencerConfig::default()
    });
    for n in 0..100 {
        seq.submit(CommitRequest {
            features_or: 0,
            minimum_subversion: 0,
            raw: Vec::new(),
            snapshot: seq.visible_ts(),
            writes: BTreeMap::from([((Keyspace::Rows, key(1)), Some(vec![n]))]),
            validation: None,
        })
        .unwrap();
    }
    let mut t = txn(&seq, &locks, 1, Isolation::Serializable);
    t.set_validation_config(super::super::ssi_validate::ValidationConfig { scan_budget: 10 });
    let rows = t
        .scan(
            Keyspace::Rows,
            Bound::Included(&key(1)),
            Bound::Excluded(&key(100)),
            Direction::Forward,
            None,
        )
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(rows.len(), 1);
    store(&seq, &locks, 2, &key(1000), b"outside");
    assert_eq!(
        t.commit(),
        Err(TxnError::Serialization(
            SerializationReason::ValidationBudget
        ))
    );
}

#[test]
fn transaction_forwards_features_and_raw_and_discards_on_rollback_or_drop() {
    let (mem, seq, locks) = setup(SequencerConfig::default());
    let counter = super::super::catalog_codec::counter_key(3, 7);
    for rollback in [false, true] {
        let mut batch = WriteBatch::default();
        batch.put(Keyspace::Seq, counter.clone(), vec![42]);
        mem.write(batch, Durability::Sync).unwrap();
        let mut t = txn(&seq, &locks, 1, Isolation::ReadCommitted);
        t.require_features(64);
        t.require_features(128);
        t.delete_counter(counter.clone()).unwrap();
        if rollback {
            t.rollback();
        } else {
            drop(t);
        }
        assert_eq!(seq.kv().get(Keyspace::Meta, b"features").unwrap(), None);
        assert_eq!(
            seq.kv().get(Keyspace::Seq, &counter).unwrap(),
            Some(vec![42])
        );
    }
    let mut t = txn(&seq, &locks, 2, Isolation::ReadCommitted);
    t.require_features(64);
    t.require_features(128);
    t.delete_counter(counter.clone()).unwrap();
    assert_eq!(t.commit().unwrap(), Ts(0));
    assert_eq!(
        seq.kv().get(Keyspace::Meta, b"features").unwrap(),
        Some(192u64.to_be_bytes().to_vec())
    );
    assert_eq!(seq.kv().get(Keyspace::Seq, &counter).unwrap(), None);
    let mut batch = WriteBatch::default();
    batch.put(Keyspace::Seq, counter.clone(), vec![42]);
    mem.write(batch, Durability::Sync).unwrap();
    let mut t = txn(&seq, &locks, 3, Isolation::ReadCommitted);
    t.delete_counter(counter.clone()).unwrap();
    t.commit_then(|_| {
        assert_eq!(seq.kv().get(Keyspace::Seq, &counter)?, None);
        Ok(())
    })
    .unwrap();
    assert_eq!(
        seq.kv().get(Keyspace::Meta, b"last_commit_ts").unwrap(),
        None
    );
}

#[test]
fn transaction_rejects_invalid_counter_and_restores_auxiliary_savepoint_state() {
    let (_, seq, locks) = setup(SequencerConfig::default());
    let mut t = txn(&seq, &locks, 1, Isolation::ReadCommitted);
    assert_eq!(t.delete_counter(vec![2; 24]), Err(TxnError::Misuse));
    let mark = t.savepoint();
    t.require_features(64);
    t.delete_counter(super::super::catalog_codec::counter_key(3, 7))
        .unwrap();
    t.rollback_to(&mark).unwrap();
    t.commit().unwrap();
    assert_eq!(seq.kv().get(Keyspace::Meta, b"features").unwrap(), None);
}

mod f8a_commit_tests {
    use crate::relational::{DataType, catalog_ext::SequenceDef};
    use crate::storage::engine::commit::*;
    use crate::storage::engine::hlc::Ts;
    use crate::storage::engine::kv::{Durability, Keyspace, Kv, KvRead, WriteBatch};
    use crate::storage::engine::txn::TxnError;
    use crate::storage::engine::{
        catalog_codec,
        kv::{KvBackend, KvError},
        kv_mem::MemKv,
        meta,
    };
    use std::{collections::BTreeMap, sync::Arc};

    fn fixture() -> (Arc<MemKv>, Arc<Sequencer>) {
        let mem = Arc::new(MemKv::new());
        let mut batch = WriteBatch::default();
        for (key, value) in [
            (b"family".as_slice(), 2u32.to_be_bytes().to_vec()),
            (b"subversion", 1u32.to_be_bytes().to_vec()),
            (b"features", 0u64.to_be_bytes().to_vec()),
            (b"domain", vec![0; 16]),
            (b"engine", b"rocksdb".to_vec()),
        ] {
            batch.put(Keyspace::Meta, key.to_vec(), value);
        }
        mem.write(batch, Durability::Sync).unwrap();
        let seq = reopen(mem.clone());
        (mem, seq)
    }
    fn reopen(mem: Arc<MemKv>) -> Arc<Sequencer> {
        Sequencer::open(
            Arc::new(Kv::new(mem)),
            Arc::new(ManualClock::new(10)),
            SequencerConfig::default(),
        )
        .unwrap()
    }
    fn request(bits: u64) -> CommitRequest {
        CommitRequest {
            snapshot: Ts(0),
            writes: BTreeMap::new(),
            features_or: bits,
            minimum_subversion: 0,
            raw: Vec::new(),
            validation: None,
        }
    }
    fn sequence() -> SequenceDef {
        SequenceDef {
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
            incarnation: 7,
        }
    }
    fn sequence_request() -> CommitRequest {
        let mut request = request(64);
        request.writes.insert(
            (Keyspace::Catalog, catalog_codec::sequence_key(1)),
            Some(catalog_codec::encode_sequence(&sequence()).unwrap()),
        );
        request
    }
    fn counter_value() -> Vec<u8> {
        crate::storage::encode(&(8i64, true, 8i64, 1u64)).unwrap()
    }

    #[test]
    fn features_and_catalog_commit_together_across_crash() {
        for fail in [false, true] {
            let (mem, seq) = fixture();
            if fail {
                mem.fail_next_write(KvError::Io("injected".into()));
            }
            let result = seq.submit(sequence_request());
            assert_eq!(result.is_ok(), !fail);
            let durable = Arc::new(mem.crash());
            let info = meta::open_checks(durable.as_ref()).unwrap();
            assert_eq!(info.features, if fail { 0 } else { 64 });
            let recovered = reopen(durable);
            assert_eq!(
                crate::storage::engine::mvcc::latest_version(
                    recovered.kv(),
                    Keyspace::Catalog,
                    &catalog_codec::sequence_key(1)
                )
                .unwrap()
                .is_some(),
                !fail
            );
        }
    }

    #[test]
    fn invalid_features_and_raw_requests_do_not_poison_group() {
        let (_, seq) = fixture();
        let mut invalid_raw = request(64);
        invalid_raw
            .raw
            .push(RawOp::DeleteCounter { key: vec![2; 24] });
        let results = seq.process_group_for_test(
            vec![request(256), invalid_raw, request(320)],
            &ManualClock::new(10),
            &mut RecentLog::new(1024),
        );
        assert_eq!(
            results,
            vec![Err(TxnError::Misuse), Err(TxnError::Misuse), Ok(Ts(0))]
        );
        assert_eq!(load_features(seq.kv()).unwrap(), 320);
        assert_eq!(
            seq.kv().get(Keyspace::Meta, b"last_commit_ts").unwrap(),
            None
        );
    }

    #[test]
    fn counter_deletion_and_catalog_tombstone_share_atomic_batch() {
        for fail in [false, true] {
            let (mem, seq) = fixture();
            seq.submit(sequence_request()).unwrap();
            let key = catalog_codec::counter_key(1, 7);
            seq.write_counter(key.clone(), counter_value(), 0).unwrap();
            let mut drop = request(0);
            drop.writes
                .insert((Keyspace::Catalog, catalog_codec::sequence_key(1)), None);
            drop.raw.push(RawOp::DeleteCounter { key: key.clone() });
            if fail {
                mem.fail_next_write(KvError::Io("injected".into()));
            }
            assert_eq!(seq.submit(drop).is_ok(), !fail);
            let durable = mem.crash();
            assert_eq!(durable.get(Keyspace::Seq, &key).unwrap().is_some(), fail);
            meta::open_checks(&durable).unwrap();
            let entry = crate::storage::engine::mvcc::latest_version(
                &durable,
                Keyspace::Catalog,
                &catalog_codec::sequence_key(1),
            )
            .unwrap()
            .unwrap()
            .1;
            assert_eq!(entry.is_some(), fail);
        }
    }

    #[test]
    fn dropped_sequence_rejects_late_counter_write_across_crash() {
        let (mem, seq) = fixture();
        seq.submit(sequence_request()).unwrap();
        assert_eq!(load_features(seq.kv()).unwrap(), 64);
        let key = catalog_codec::counter_key(1, 7);
        seq.write_counter(key.clone(), counter_value(), 0).unwrap();
        let mut drop_request = request(0);
        drop_request
            .writes
            .insert((Keyspace::Catalog, catalog_codec::sequence_key(1)), None);
        drop_request
            .raw
            .push(RawOp::DeleteCounter { key: key.clone() });
        seq.submit(drop_request).unwrap();
        let error = seq.write_counter(key.clone(), counter_value(), 0).unwrap_err();
        assert_eq!(error, TxnError::SequenceGone);
        assert_eq!(error.sqlstate(), "42P01");
        assert_eq!(seq.kv().get(Keyspace::Seq, &key).unwrap(), None);
        drop(seq);
        let durable = Arc::new(mem.crash());
        assert_eq!(meta::open_checks(durable.as_ref()).unwrap().features, 320);
        let recovered = reopen(durable);
        assert_eq!(recovered.kv().get(Keyspace::Seq, &key).unwrap(), None);
        assert_eq!(
            crate::storage::engine::mvcc::latest_version(
                recovered.kv(),
                Keyspace::Catalog,
                &catalog_codec::sequence_key(1),
            )
            .unwrap()
            .unwrap()
            .1,
            None
        );
        meta::open_checks(recovered.kv()).unwrap();
    }

    #[test]
    fn counter_write_rejects_missing_sequence_and_stale_incarnation() {
        let (_, seq) = fixture();
        let missing = catalog_codec::counter_key(1, 7);
        assert_eq!(
            seq.write_counter(missing.clone(), counter_value(), 64),
            Err(TxnError::SequenceGone)
        );
        assert_eq!(seq.kv().get(Keyspace::Seq, &missing).unwrap(), None);
        assert_eq!(load_features(seq.kv()).unwrap(), 0);
        seq.submit(sequence_request()).unwrap();
        let stale = catalog_codec::counter_key(1, 6);
        assert_eq!(
            seq.write_counter(stale.clone(), counter_value(), 0),
            Err(TxnError::SequenceGone)
        );
        assert_eq!(seq.kv().get(Keyspace::Seq, &stale).unwrap(), None);
        assert_eq!(load_features(seq.kv()).unwrap(), 64);
        meta::open_checks(seq.kv()).unwrap();
    }

    #[test]
    fn counter_write_rejects_inconsistent_reservation_without_writing() {
        let (_, seq) = fixture();
        seq.submit(sequence_request()).unwrap();
        let key = catalog_codec::counter_key(1, 7);
        let invalid = crate::storage::encode(&(8i64, true, 9i64, 1u64)).unwrap();
        assert_eq!(
            seq.write_counter(key.clone(), invalid, 0),
            Err(TxnError::Misuse)
        );
        assert_eq!(seq.kv().get(Keyspace::Seq, &key).unwrap(), None);
        assert_eq!(load_features(seq.kv()).unwrap(), 64);
        meta::open_checks(seq.kv()).unwrap();
    }

    #[test]
    fn counter_write_adds_bit_atomically_without_timestamp() {
        for fail in [false, true] {
            let (mem, seq) = fixture();
            let committed = seq.submit(sequence_request()).unwrap();
            let timestamp = seq.kv().get(Keyspace::Meta, b"last_commit_ts").unwrap();
            let key = catalog_codec::counter_key(1, 7);
            if fail {
                mem.fail_next_write(KvError::Io("injected".into()));
            }
            assert_eq!(
                seq.write_counter(key.clone(), counter_value(), 64).is_ok(),
                !fail
            );
            assert_eq!(seq.visible_ts(), committed);
            let durable = mem.crash();
            assert_eq!(load_features(&durable).unwrap(), if fail { 64 } else { 320 });
            assert_eq!(durable.get(Keyspace::Seq, &key).unwrap().is_some(), !fail);
            assert_eq!(
                durable.get(Keyspace::Meta, b"last_commit_ts").unwrap(),
                timestamp
            );
        }
    }

    #[test]
    fn counter_requires_valid_key_and_catalog_ext() {
        let (_, seq) = fixture();
        for key in [vec![], vec![2; 24], vec![1; 25], vec![2; 26]] {
            assert_eq!(
                seq.write_counter(key, counter_value(), 320),
                Err(TxnError::Misuse)
            );
        }
        assert_eq!(
            seq.write_counter(catalog_codec::counter_key(1, 7), counter_value(), 0),
            Err(TxnError::Misuse)
        );
        assert_eq!(load_features(seq.kv()).unwrap(), 0);
    }

    #[test]
    fn reopened_worker_preserves_existing_feature_mask() {
        let (mem, seq) = fixture();
        seq.submit(sequence_request()).unwrap();
        seq.submit(request(192 | 0xC00)).unwrap();
        drop(seq);
        let seq = reopen(Arc::new(mem.crash()));
        seq.write_counter(catalog_codec::counter_key(1, 7), counter_value(), 0)
            .unwrap();
        assert_eq!(load_features(seq.kv()).unwrap(), 448 | 0xC00);
    }

    #[test]
    fn sequencer_rejects_malformed_or_invalid_persisted_features() {
        for value in [
            vec![0; 7],
            256u64.to_be_bytes().to_vec(),
            8192u64.to_be_bytes().to_vec(),
        ] {
            let (mem, seq) = fixture();
            drop(seq);
            let mut batch = WriteBatch::default();
            batch.put(Keyspace::Meta, b"features".to_vec(), value);
            mem.write(batch, Durability::Sync).unwrap();
            assert!(matches!(
                Sequencer::open(
                    Arc::new(Kv::new(mem)),
                    Arc::new(ManualClock::new(10)),
                    SequencerConfig::default()
                ),
                Err(TxnError::Kv(KvError::Corrupt(_)))
            ));
        }
    }

    fn load_features(kv: &dyn KvRead) -> Result<u64, TxnError> {
        Ok(u64::from_be_bytes(
            kv.get(Keyspace::Meta, b"features")?
                .unwrap()
                .try_into()
                .unwrap(),
        ))
    }
}
