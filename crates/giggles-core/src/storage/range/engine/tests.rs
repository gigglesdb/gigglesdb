use super::*;
use crate::storage::engine::locks::{LockKey, LockMode};
use crate::storage::range::store::{
    RangeApplyOutcome,
    tests::{Dir, SPLIT_TABLE, two_ranges},
};
use crate::storage::range_api::SystemService;
use crate::storage::range_api::{
    LockTarget, RangeLockMode, RangeSpace, ReadDirection, ReplicaDesc, ReplicaRole,
};
use crate::storage::replicated::{AppliedPosition, ReplicateError};
use std::ops::Bound;
use std::sync::atomic::AtomicU64;

/// Applies proposals to one range in log order, as its Raft group would.
struct Sink {
    store: Arc<RangeStore>,
    range: RangeId,
    fail: AtomicBool,
}
impl BatchSink for Sink {
    fn replicate(&self, encoded_batch: Vec<u8>) -> Result<(), ReplicateError> {
        if self.fail.load(Ordering::SeqCst) {
            return Err(ReplicateError::NotLeader);
        }
        let index = self
            .store
            .applied(self.range)
            .unwrap()
            .map_or(1, |p| p.index + 1);
        let outcome = self
            .store
            .apply(
                self.range,
                &encoded_batch,
                AppliedPosition { index, term: 1 },
            )
            .map_err(|e| ReplicateError::Unavailable(format!("{e:?}")))?;
        assert_eq!(outcome, RangeApplyOutcome::Applied);
        Ok(())
    }
}

struct Oracle(AtomicU64);
impl TsOracle for Oracle {
    fn next(&self, n: u32) -> Result<Timestamp, RangeError> {
        Ok(Timestamp(self.0.fetch_add(u64::from(n), Ordering::SeqCst)))
    }
}

struct Fixture {
    _dir: Dir,
    store: Arc<RangeStore>,
    oracle: Arc<Oracle>,
    sink: Arc<Sink>,
}
const B: RangeId = RangeId(2);
const T: u64 = SPLIT_TABLE;

fn fixture() -> Fixture {
    let dir = Dir::new("range-engine");
    let store = Arc::new(two_ranges(&dir));
    let sink = Arc::new(Sink {
        store: store.clone(),
        range: B,
        fail: AtomicBool::new(false),
    });
    Fixture {
        _dir: dir,
        store,
        oracle: Arc::new(Oracle(AtomicU64::new(1 << 20))),
        sink,
    }
}
fn config() -> EngineConfig {
    EngineConfig {
        lease: Duration::from_millis(300),
        read_wait: Duration::from_millis(400),
        sweep: Duration::from_millis(50),
    }
}
impl Fixture {
    fn open(&self) -> Arc<RangeEngine> {
        let descriptor = self.store.descriptor(B).unwrap().unwrap();
        RangeEngine::open(
            &self.store,
            descriptor,
            self.sink.clone(),
            self.oracle.clone(),
            config(),
        )
        .unwrap()
    }
}
fn epoch(e: &RangeEngine) -> Epoch {
    e.descriptor.epoch
}
fn txn(n: u64) -> ClusterTxnId {
    ClusterTxnId::new(1, n)
}
fn key(n: u64) -> Vec<u8> {
    let mut key = T.to_be_bytes().to_vec();
    key.extend_from_slice(&n.to_be_bytes());
    key
}
fn lock(
    e: &RangeEngine,
    t: ClusterTxnId,
    n: u64,
    timeout_ms: Option<u64>,
) -> Result<LockGrant, RangeError> {
    e.lock(LockRequest {
        range: B,
        epoch: epoch(e),
        txn: t,
        priority: Timestamp(0),
        target: LockTarget::Key {
            space: RangeSpace::Rows,
            key: key(n),
        },
        mode: RangeLockMode::Exclusive,
        timeout_ms,
    })
}
fn write(n: u64, value: &[u8]) -> RangeWrite {
    RangeWrite {
        space: RangeSpace::Rows,
        key: key(n),
        value: Some(value.to_vec()),
    }
}
fn commit(e: &RangeEngine, t: ClusterTxnId, n: u64, value: &[u8]) -> Timestamp {
    lock(e, t, n, Some(1000)).unwrap();
    e.commit(epoch(e), t, vec![write(n, value)], None).unwrap()
}
fn read_all(
    e: &RangeEngine,
    at: Timestamp,
    dir: ReadDirection,
    page_bytes: usize,
) -> Vec<(u64, Option<Vec<u8>>)> {
    let mut out = Vec::new();
    let (mut start, mut end) = (Bound::Unbounded, Bound::Unbounded);
    loop {
        let page = e
            .read(ReadRequest {
                range: B,
                epoch: epoch(e),
                space: RangeSpace::Rows,
                start: start.clone(),
                end: end.clone(),
                dir,
                ts: at,
                page_bytes,
            })
            .unwrap();
        for (raw, value) in page.pairs {
            let (user, _) = codec::split_versioned(&raw).unwrap();
            let n = u64::from_be_bytes(user[8..].try_into().unwrap());
            out.push((n, mvcc::decode(&value).unwrap().map(<[u8]>::to_vec)));
        }
        match (page.resume, dir) {
            (None, _) => return out,
            (Some(next), ReadDirection::Forward) => start = next,
            (Some(next), ReadDirection::Reverse) => end = next,
        }
    }
}
use crate::storage::engine::{codec, kv::KvRead, mvcc};

#[test]
fn commits_land_in_their_range_and_reads_page_at_a_timestamp() {
    let f = fixture();
    let e = f.open();
    let mut stamps = Vec::new();
    for n in 1..=20 {
        stamps.push(commit(&e, txn(n), n, format!("v{n}").as_bytes()));
    }
    let first = commit(&e, txn(100), 1, b"newer");
    assert!(stamps.windows(2).all(|w| w[0] < w[1]) && first > stamps[19]);
    assert_eq!(
        f.store.applied(RangeId(1)).unwrap(),
        None,
        "range 1 untouched"
    );
    let now = Timestamp(first.0 + 1);
    for page in [1, 40, 1 << 20] {
        let forward = read_all(&e, now, ReadDirection::Forward, page);
        assert_eq!(forward.len(), 20);
        assert_eq!(forward[0], (1, Some(b"newer".to_vec())));
        let mut reverse = read_all(&e, now, ReadDirection::Reverse, page);
        reverse.reverse();
        assert_eq!(reverse, forward, "page {page}");
    }
    // At an older timestamp only the earlier versions exist.
    let old = read_all(&e, stamps[4], ReadDirection::Forward, 64);
    assert_eq!(old.len(), 5);
    assert_eq!(old[0], (1, Some(b"v1".to_vec())));
    // A write outside the range, or without its lock, writes nothing.
    lock(&e, txn(200), 30, None).unwrap();
    assert!(matches!(
        e.commit(epoch(&e), txn(200), vec![write(31, b"x")], None),
        Err(RangeError::Aborted(_))
    ));
    let outside = RangeWrite {
        space: RangeSpace::Rows,
        key: (T - 1).to_be_bytes().to_vec(),
        value: None,
    };
    lock(&e, txn(201), 32, None).unwrap();
    assert!(matches!(
        e.commit(epoch(&e), txn(201), vec![outside], None),
        Err(RangeError::StaleEpoch { .. })
    ));
    assert!(matches!(
        e.commit(Epoch(99), txn(201), vec![], None),
        Err(RangeError::StaleEpoch { .. })
    ));
}

#[test]
fn a_new_term_aborts_old_locks_and_keeps_prepared_ones() {
    let f = fixture();
    let e = f.open();
    lock(&e, txn(1), 1, None).unwrap();
    lock(&e, txn(2), 2, None).unwrap();
    let p = e
        .prepare(epoch(&e), txn(2), B, vec![write(2, b"prepared")])
        .unwrap();
    assert!(matches!(
        lock(&e, txn(2), 3, None),
        Err(RangeError::Aborted(_))
    ));
    e.close();
    assert!(matches!(
        lock(&e, txn(3), 4, None),
        Err(RangeError::NotLeader { .. })
    ));
    let e = f.open();
    // Unprepared locks died with the old engine; the prepared record's lock is back.
    assert!(matches!(
        e.commit(epoch(&e), txn(1), vec![write(1, b"late")], None),
        Err(RangeError::Aborted(_))
    ));
    lock(&e, txn(3), 1, Some(100)).unwrap();
    assert_eq!(lock(&e, txn(4), 2, Some(100)), Err(RangeError::LockTimeout));
    assert_eq!(e.prepare(epoch(&e), txn(2), B, vec![]).unwrap(), p);
    // A reader at T >= p waits for the outcome; a reader below p does not.
    assert!(read_all(&e, Timestamp(p.0 - 1), ReadDirection::Forward, 1 << 20).is_empty());
    let reader = {
        let e = e.clone();
        thread::spawn(move || read_all(&e, Timestamp(p.0 + 10), ReadDirection::Forward, 1 << 20))
    };
    thread::sleep(Duration::from_millis(50));
    e.finalize(txn(2), Outcome::Commit(Timestamp(p.0 + 5)))
        .unwrap();
    assert_eq!(
        reader.join().unwrap(),
        vec![(2, Some(b"prepared".to_vec()))]
    );
    // Finalize is idempotent and released the lock.
    e.finalize(txn(2), Outcome::Commit(Timestamp(p.0 + 5)))
        .unwrap();
    lock(&e, txn(4), 2, Some(100)).unwrap();
}

#[test]
fn abort_releases_every_lock_with_or_without_a_prepared_record() {
    let f = fixture();
    let e = f.open();
    lock(&e, txn(1), 1, None).unwrap();
    e.finalize(txn(1), Outcome::Abort).unwrap();
    lock(&e, txn(2), 1, Some(100)).unwrap();
    lock(&e, txn(3), 5, None).unwrap();
    let p = e
        .prepare(epoch(&e), txn(3), B, vec![write(5, b"x")])
        .unwrap();
    // release() of a prepared transaction keeps its locks.
    e.release(txn(3)).unwrap();
    assert_eq!(lock(&e, txn(4), 5, Some(50)), Err(RangeError::LockTimeout));
    e.finalize(txn(3), Outcome::Abort).unwrap();
    lock(&e, txn(4), 5, Some(100)).unwrap();
    assert!(read_all(&e, Timestamp(p.0 + 100), ReadDirection::Forward, 64).is_empty());
    assert!(
        f.store
            .kv()
            .get(
                Keyspace::Meta,
                &crate::storage::engine::meta::range_local_key(
                    B,
                    &RangeLocalKey::Prepared(txn(3).0)
                )
            )
            .unwrap()
            .is_none()
    );
}

#[test]
fn an_expired_lease_releases_unprepared_locks() {
    let f = fixture();
    let e = f.open();
    lock(&e, txn(1), 1, None).unwrap();
    lock(&e, txn(2), 2, None).unwrap();
    let start = Instant::now();
    while start.elapsed() < Duration::from_millis(600) {
        e.heartbeat(txn(2)).unwrap();
        thread::sleep(Duration::from_millis(50));
    }
    lock(&e, txn(3), 1, Some(100)).unwrap();
    assert_eq!(lock(&e, txn(3), 2, Some(50)), Err(RangeError::LockTimeout));
    assert!(matches!(e.heartbeat(txn(1)), Err(RangeError::Aborted(_))));
}

#[test]
fn blockers_and_cancel_wait_serve_the_deadlock_detector() {
    let f = fixture();
    let e = f.open();
    lock(&e, txn(1), 1, None).unwrap();
    let waiter = {
        let e = e.clone();
        thread::spawn(move || lock(&e, txn(2), 1, None))
    };
    let start = Instant::now();
    while e.blockers(txn(2)).unwrap().is_empty() {
        assert!(start.elapsed() < Duration::from_secs(2));
        thread::yield_now();
    }
    assert_eq!(e.blockers(txn(2)).unwrap(), vec![txn(1)]);
    e.heartbeat(txn(2)).unwrap();
    e.cancel_wait(txn(2)).unwrap();
    assert_eq!(waiter.join().unwrap(), Err(RangeError::Deadlock));
}

#[test]
fn table_sentinels_live_on_ranges_with_the_tables_rows() {
    let f = fixture();
    let e = f.open();
    let sentinel = |table| LockRequest {
        range: B,
        epoch: epoch(&e),
        txn: txn(1),
        priority: Timestamp(0),
        target: LockTarget::TableSentinel { table },
        mode: RangeLockMode::Shared,
        timeout_ms: None,
    };
    assert_eq!(e.lock(sentinel(T)).unwrap(), LockGrant { latest: None });
    assert!(matches!(
        e.lock(sentinel(T - 1)),
        Err(RangeError::StaleEpoch { .. })
    ));
    let _ = (ReplicaDesc {
        node: 1,
        role: ReplicaRole::Voter,
    },);
}

#[test]
fn a_lost_sink_reports_an_unknown_outcome() {
    let f = fixture();
    let e = f.open();
    lock(&e, txn(1), 1, None).unwrap();
    f.sink.fail.store(true, Ordering::SeqCst);
    assert!(matches!(
        e.commit(epoch(&e), txn(1), vec![write(1, b"x")], None),
        Err(RangeError::Unknown(_))
    ));
    // The fenced engine serves nothing more.
    assert!(matches!(
        lock(&e, txn(2), 2, None),
        Err(RangeError::NotLeader { .. })
    ));
}

#[test]
fn the_opener_replaces_and_fences_engines() {
    let f = fixture();
    let engines = RangeEngines::new(f.store.clone(), f.oracle.clone(), config());
    let descriptor = f.store.descriptor(B).unwrap().unwrap();
    let first = engines.open(descriptor.clone(), f.sink.clone()).unwrap();
    let txn1 = txn(1);
    let lock_req = |svc: &Arc<dyn RangeService>, t| LockRequest {
        range: B,
        epoch: svc.descriptor().epoch,
        txn: t,
        priority: Timestamp(0),
        target: LockTarget::Key {
            space: RangeSpace::Rows,
            key: key(1),
        },
        mode: RangeLockMode::Exclusive,
        timeout_ms: Some(100),
    };
    first.lock(lock_req(&first, txn1)).unwrap();
    // Reopening (a new term) fences the old engine; its unprepared locks are gone.
    let second = engines.open(descriptor, f.sink.clone()).unwrap();
    assert!(matches!(
        first.lock(lock_req(&first, txn(2))),
        Err(RangeError::NotLeader { .. })
    ));
    second.lock(lock_req(&second, txn(2))).unwrap();
    engines.close(B);
    assert!(matches!(
        second.heartbeat(txn(2)),
        Err(RangeError::NotLeader { .. })
    ));
    // A stale descriptor is refused.
    let mut stale = f.store.descriptor(B).unwrap().unwrap();
    stale.epoch = Epoch(stale.epoch.0 + 1);
    assert!(matches!(
        engines.open(stale, f.sink.clone()),
        Err(RangeError::StaleEpoch { .. })
    ));
}

#[test]
fn the_system_range_opens_its_oracle_and_leases_with_its_engine() {
    let f = fixture();
    let a = RangeId(1);
    let sink = Arc::new(Sink {
        store: f.store.clone(),
        range: a,
        fail: AtomicBool::new(false),
    });
    let engines = Arc::new(RangeEngines::new(
        f.store.clone(),
        f.oracle.clone(),
        config(),
    ));
    assert!(engines.system().is_none());
    let descriptor = f.store.descriptor(a).unwrap().unwrap();
    engines.open(descriptor.clone(), sink.clone()).unwrap();
    let system = engines.system().unwrap();
    let first = system.next(1).unwrap();
    assert!(system.next(1).unwrap() > first);
    assert_eq!(
        system
            .lease(crate::storage::range_api::LeaseKind::ObjectId, 0, 3)
            .unwrap(),
        1..4
    );
    engines.close(a);
    assert!(engines.system().is_none());
    assert!(matches!(system.next(1), Err(RangeError::NotLeader { .. })));
    // A new term starts above every timestamp of the old one.
    engines.open(descriptor, sink).unwrap();
    assert!(engines.system().unwrap().next(1).unwrap() > first);
}

#[test]
fn duplicate_prepares_get_one_timestamp_and_expired_leases_stay_aborted() {
    let f = fixture();
    let e = f.open();
    lock(&e, txn(1), 1, None).unwrap();
    let racers: Vec<_> = (0..4)
        .map(|_| {
            let e = e.clone();
            thread::spawn(move || e.prepare(epoch(&e), txn(1), B, vec![write(1, b"p")]))
        })
        .collect();
    let stamps: Vec<_> = racers
        .into_iter()
        .map(|r| r.join().unwrap().unwrap())
        .collect();
    assert!(stamps.windows(2).all(|w| w[0] == w[1]), "{stamps:?}");
    let c = Timestamp(stamps[0].0 + 1);
    e.finalize(txn(1), Outcome::Commit(c)).unwrap();
    assert_eq!(
        read_all(&e, c, ReadDirection::Forward, 64),
        vec![(1, Some(b"p".to_vec()))]
    );
    // A lease that expired stays expired: no silent fresh lease, commit or prepare.
    lock(&e, txn(2), 2, None).unwrap();
    thread::sleep(Duration::from_millis(500));
    assert!(matches!(
        lock(&e, txn(2), 3, None),
        Err(RangeError::Aborted(_))
    ));
    assert!(matches!(
        e.prepare(epoch(&e), txn(2), B, vec![write(2, b"x")]),
        Err(RangeError::Aborted(_))
    ));
    // The gateway's release forgets it.
    e.release(txn(2)).unwrap();
    lock(&e, txn(2), 3, None).unwrap();
}

#[test]
fn reads_far_ahead_of_the_clock_are_refused() {
    let f = fixture();
    let e = f.open();
    let req = ReadRequest {
        range: B,
        epoch: epoch(&e),
        space: RangeSpace::Rows,
        start: Bound::Unbounded,
        end: Bound::Unbounded,
        dir: ReadDirection::Forward,
        ts: Timestamp(u64::MAX),
        page_bytes: 64,
    };
    assert!(matches!(e.read(req), Err(RangeError::Corrupt(_))));
    // Commits keep working afterwards.
    commit(&e, txn(1), 1, b"v");
}

#[test]
fn prepared_records_round_trip_and_reject_damage() {
    let mut writes = prepared::Writes::new();
    writes.insert((Keyspace::Rows, key(1)), Some(b"v".to_vec()));
    writes.insert((Keyspace::Index, key(2)), None);
    let record = prepared::Prepared {
        home: B,
        at: Ts(9),
        writes,
        locks: vec![
            (
                LockKey::Unique {
                    index: 4,
                    tuple: vec![1],
                },
                LockMode::Exclusive,
            ),
            (
                LockKey::Row {
                    table: T,
                    pk: vec![2],
                },
                LockMode::Shared,
            ),
        ],
    };
    let bytes = record.encode().unwrap();
    let back = prepared::Prepared::decode(&bytes).unwrap();
    assert_eq!(back.encode().unwrap(), bytes, "canonical");
    assert_eq!(
        (back.home, back.at, &back.writes),
        (record.home, record.at, &record.writes)
    );
    for cut in [0, 1, 17, bytes.len() - 1] {
        assert!(
            prepared::Prepared::decode(&bytes[..cut]).is_err(),
            "cut {cut}"
        );
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(prepared::Prepared::decode(&trailing).is_err());
    let mut zero = bytes.clone();
    zero[9..17].fill(0);
    assert!(prepared::Prepared::decode(&zero).is_err());
    let mut seq = record.clone();
    seq.writes.insert((Keyspace::Seq, vec![1]), None);
    assert!(seq.encode().is_err());
}

#[test]
fn home_records_persist_across_terms() {
    use crate::storage::range_api::{HomeOp, HomeReply, HomeStatus};
    let f = fixture();
    let e = f.open();
    let t = txn(9);
    let incarnation = match e.home(epoch(&e), HomeOp::Open { txn: t }).unwrap() {
        HomeReply::Opened { incarnation } => incarnation,
        other => panic!("{other:?}"),
    };
    e.home(
        epoch(&e),
        HomeOp::Seal {
            txn: t,
            incarnation,
            participants: vec![B],
        },
    )
    .unwrap();
    e.home(
        epoch(&e),
        HomeOp::Decide {
            txn: t,
            incarnation,
            outcome: Outcome::Abort,
        },
    )
    .unwrap();
    assert!(matches!(
        e.home(Epoch(99), HomeOp::Status { txn: t }),
        Err(RangeError::StaleEpoch { .. })
    ));
    e.close();
    assert!(matches!(
        e.home(epoch(&e), HomeOp::Status { txn: t }),
        Err(RangeError::NotLeader { .. })
    ));
    let e = f.open();
    assert_eq!(
        e.home(epoch(&e), HomeOp::Status { txn: t }).unwrap(),
        HomeReply::Status(HomeStatus::Decided(Outcome::Abort))
    );
    // A fresh Open after Forget gets a larger incarnation.
    e.home(
        epoch(&e),
        HomeOp::Forget {
            txn: t,
            incarnation,
        },
    )
    .unwrap();
    match e.home(epoch(&e), HomeOp::Open { txn: t }).unwrap() {
        HomeReply::Opened { incarnation: next } => assert!(next > incarnation),
        other => panic!("{other:?}"),
    }
}
