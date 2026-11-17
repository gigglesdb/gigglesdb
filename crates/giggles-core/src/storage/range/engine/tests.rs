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
pub(super) struct Sink {
    pub(super) store: Arc<RangeStore>,
    pub(super) range: RangeId,
    pub(super) fail: AtomicBool,
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

pub(super) struct Oracle(pub(super) AtomicU64);
impl TsOracle for Oracle {
    fn next(&self, n: u32) -> Result<Timestamp, RangeError> {
        Ok(Timestamp(self.0.fetch_add(u64::from(n), Ordering::SeqCst)))
    }
}

pub(super) struct Fixture {
    _dir: Dir,
    pub(super) store: Arc<RangeStore>,
    pub(super) oracle: Arc<Oracle>,
    pub(super) sink: Arc<Sink>,
}
pub(super) const B: RangeId = RangeId(2);
const T: u64 = SPLIT_TABLE;

pub(super) fn fixture() -> Fixture {
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
pub(super) fn config() -> EngineConfig {
    EngineConfig {
        lease: Duration::from_millis(300),
        read_wait: Duration::from_millis(400),
        sweep: Duration::from_millis(50),
        resolve_after: Duration::from_millis(50),
        recovery: RecoveryConfig::default(),
        recovery_tick: Duration::from_millis(20),
    }
}
impl Fixture {
    pub(super) fn open(&self) -> Arc<RangeEngine> {
        self.open_at(1)
    }
    /// Opens an engine of B with lock generation `generation`.
    pub(super) fn open_at(&self, generation: u64) -> Arc<RangeEngine> {
        let descriptor = self.store.descriptor(B).unwrap().unwrap();
        RangeEngine::open(
            &self.store,
            descriptor,
            self.sink.clone(),
            self.oracle.clone(),
            config(),
            generation,
        )
        .unwrap()
    }
}
pub(super) fn epoch(e: &RangeEngine) -> Epoch {
    e.descriptor.epoch
}
pub(super) fn txn(n: u64) -> ClusterTxnId {
    ClusterTxnId::new(1, n)
}
pub(super) fn key(n: u64) -> Vec<u8> {
    let mut key = T.to_be_bytes().to_vec();
    key.extend_from_slice(&n.to_be_bytes());
    key
}
pub(super) fn lock(
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
pub(super) fn write(n: u64, value: &[u8]) -> RangeWrite {
    RangeWrite {
        space: RangeSpace::Rows,
        key: key(n),
        value: Some(value.to_vec()),
    }
}
fn commit(e: &RangeEngine, t: ClusterTxnId, n: u64, value: &[u8]) -> Timestamp {
    let grant = lock(e, t, n, Some(1000)).unwrap();
    e.commit(
        epoch(e),
        t,
        vec![write(n, value)],
        None,
        Some(grant.generation),
    )
    .unwrap()
}
pub(super) fn read_all(
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
        e.commit(epoch(&e), txn(200), vec![write(31, b"x")], None, None),
        Err(RangeError::Aborted(_))
    ));
    let outside = RangeWrite {
        space: RangeSpace::Rows,
        key: (T - 1).to_be_bytes().to_vec(),
        value: None,
    };
    lock(&e, txn(201), 32, None).unwrap();
    assert!(matches!(
        e.commit(epoch(&e), txn(201), vec![outside], None, None),
        Err(RangeError::StaleEpoch { .. })
    ));
    assert!(matches!(
        e.commit(Epoch(0), txn(201), vec![], None, None),
        Err(RangeError::StaleEpoch { .. })
    ));
    // Review N4: an epoch this node has not applied yet is retried, not named stale.
    assert!(matches!(
        e.commit(Epoch(99), txn(201), vec![], None, None),
        Err(RangeError::Unavailable(_))
    ));
}

#[test]
fn a_new_term_aborts_old_locks_and_keeps_prepared_ones() {
    let f = fixture();
    let e = f.open();
    lock(&e, txn(1), 1, None).unwrap();
    lock(&e, txn(2), 2, None).unwrap();
    let p = e
        .prepare(epoch(&e), txn(2), B, vec![write(2, b"prepared")], None)
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
        e.commit(epoch(&e), txn(1), vec![write(1, b"late")], None, None),
        Err(RangeError::Aborted(_))
    ));
    lock(&e, txn(3), 1, Some(100)).unwrap();
    assert_eq!(lock(&e, txn(4), 2, Some(100)), Err(RangeError::LockTimeout));
    assert_eq!(e.prepare(epoch(&e), txn(2), B, vec![], None).unwrap(), p);
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
fn commit_and_prepare_refuse_locks_of_another_generation() {
    let f = fixture();
    let e1 = f.open_at(1);
    assert_eq!(lock(&e1, txn(1), 1, None).unwrap().generation, 1);
    assert_eq!(lock(&e1, txn(2), 2, None).unwrap().generation, 1);
    assert_eq!(lock(&e1, txn(3), 3, None).unwrap().generation, 1);
    let p = e1
        .prepare(epoch(&e1), txn(3), B, vec![write(3, b"prepared")], Some(1))
        .unwrap();
    e1.close();
    // Leader change: the new engine's lock table has generation 2.
    let e2 = f.open_at(2);
    let applied = f.store.applied(B).unwrap();
    let lost = |r: Result<Timestamp, RangeError>| match r {
        Err(RangeError::Aborted(m)) => m.starts_with("locks lost"),
        _ => false,
    };
    assert!(lost(e2.commit(
        epoch(&e2),
        txn(1),
        vec![write(1, b"lost")],
        None,
        Some(1)
    )));
    assert!(lost(e2.prepare(
        epoch(&e2),
        txn(2),
        B,
        vec![write(2, b"lost")],
        Some(1)
    )));
    // Refused before anything changed: nothing replicated, no lease taken.
    assert_eq!(f.store.applied(B).unwrap(), applied);
    assert!(matches!(e2.heartbeat(txn(1)), Err(RangeError::Aborted(_))));
    assert!(matches!(e2.heartbeat(txn(2)), Err(RangeError::Aborted(_))));
    // Re-locked under generation 2, the commit goes through.
    let grant = lock(&e2, txn(1), 1, Some(1000)).unwrap();
    assert_eq!(grant.generation, 2);
    e2.commit(
        epoch(&e2),
        txn(1),
        vec![write(1, b"relocked")],
        None,
        Some(2),
    )
    .unwrap();
    // A retried prepare of the transaction already prepared under generation 1 returns its
    // p (Track D rule (a)): it was accepted under a valid generation.
    assert_eq!(
        e2.prepare(epoch(&e2), txn(3), B, vec![write(3, b"prepared")], Some(1)),
        Ok(p)
    );
    // The record prepared under generation 1 has its lock back, and finalize applies it.
    assert_eq!(lock(&e2, txn(4), 3, Some(50)), Err(RangeError::LockTimeout));
    let c = f.oracle.next(1).unwrap();
    assert!(c > p);
    e2.finalize(txn(3), Outcome::Commit(c)).unwrap();
    let at = f.oracle.next(1).unwrap();
    assert_eq!(
        read_all(&e2, at, ReadDirection::Forward, 1 << 20),
        vec![
            (1, Some(b"relocked".to_vec())),
            (3, Some(b"prepared".to_vec()))
        ]
    );
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
        .prepare(epoch(&e), txn(3), B, vec![write(5, b"x")], None)
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
    assert_eq!(
        e.lock(sentinel(T)).unwrap(),
        LockGrant {
            latest: None,
            generation: 1
        }
    );
    assert!(matches!(
        e.lock(sentinel(T - 1)),
        Err(RangeError::StaleEpoch { .. })
    ));
    let _ = (ReplicaDesc {
        node: 1,
        role: ReplicaRole::Voter,
    },);
}

/// Fails every batch with its error once one is set; else applies through `inner`.
struct Switch {
    inner: Arc<Sink>,
    error: Mutex<Option<ReplicateError>>,
}
impl BatchSink for Switch {
    fn replicate(&self, encoded_batch: Vec<u8>) -> Result<(), ReplicateError> {
        match self.error.lock().unwrap().clone() {
            Some(error) => Err(error),
            None => self.inner.replicate(encoded_batch),
        }
    }
}

#[test]
fn a_refused_write_is_definite_and_a_lost_one_unknown() {
    // R5-81: NotLeader from the sink = never applied: commit Aborted (the sequencer's
    // "nothing written in this term"), prepare and finalize NotLeader; the engine is fenced.
    for op in ["commit", "prepare", "finalize"] {
        let f = fixture();
        let e = f.open();
        let generation = lock(&e, txn(1), 1, None).unwrap().generation;
        if op == "finalize" {
            e.prepare(epoch(&e), txn(1), B, vec![write(1, b"x")], Some(generation))
                .unwrap();
        }
        f.sink.fail.store(true, Ordering::SeqCst);
        let result = match op {
            "commit" => e
                .commit(
                    epoch(&e),
                    txn(1),
                    vec![write(1, b"x")],
                    None,
                    Some(generation),
                )
                .map(drop),
            "prepare" => e
                .prepare(epoch(&e), txn(1), B, vec![write(1, b"x")], Some(generation))
                .map(drop),
            _ => e.finalize(txn(1), Outcome::Abort),
        };
        let definite = match op {
            "commit" => {
                matches!(&result, Err(RangeError::Aborted(m)) if m == "range engine fenced")
            }
            _ => matches!(result, Err(RangeError::NotLeader { hint: None })),
        };
        assert!(definite, "{op}: {result:?}");
        assert!(matches!(
            lock(&e, txn(2), 2, None),
            Err(RangeError::NotLeader { .. })
        ));
    }
    // Unavailable (the proposal may still commit) stays an unknown outcome.
    let f = fixture();
    let sink = Arc::new(Switch {
        inner: f.sink.clone(),
        error: Mutex::new(None),
    });
    let descriptor = f.store.descriptor(B).unwrap().unwrap();
    let e = RangeEngine::open(
        &f.store,
        descriptor,
        sink.clone(),
        f.oracle.clone(),
        config(),
        1,
    )
    .unwrap();
    let generation = lock(&e, txn(1), 1, None).unwrap().generation;
    *sink.error.lock().unwrap() = Some(ReplicateError::Unavailable("lost".into()));
    let result = e.commit(
        epoch(&e),
        txn(1),
        vec![write(1, b"x")],
        None,
        Some(generation),
    );
    assert!(matches!(result, Err(RangeError::Unknown(_))), "{result:?}");
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
    let first = engines.open(descriptor.clone(), f.sink.clone(), 1).unwrap();
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
    assert_eq!(first.lock(lock_req(&first, txn1)).unwrap().generation, 1);
    // Reopening (a new term) fences the old engine; its unprepared locks are gone.
    let second = engines.open(descriptor, f.sink.clone(), 2).unwrap();
    assert!(matches!(
        first.lock(lock_req(&first, txn(2))),
        Err(RangeError::NotLeader { .. })
    ));
    assert_eq!(
        second.lock(lock_req(&second, txn(2))).unwrap().generation,
        2
    );
    engines.close(B);
    assert!(matches!(
        second.heartbeat(txn(2)),
        Err(RangeError::NotLeader { .. })
    ));
    // A stale descriptor is refused.
    let mut stale = f.store.descriptor(B).unwrap().unwrap();
    stale.epoch = Epoch(stale.epoch.0 + 1);
    assert!(matches!(
        engines.open(stale, f.sink.clone(), 3),
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
    engines.open(descriptor.clone(), sink.clone(), 1).unwrap();
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
    engines.open(descriptor, sink, 2).unwrap();
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
            thread::spawn(move || e.prepare(epoch(&e), txn(1), B, vec![write(1, b"p")], None))
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
        e.prepare(epoch(&e), txn(2), B, vec![write(2, b"x")], None),
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
        children: Vec::new(),
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
        e.home(Epoch(0), HomeOp::Status { txn: t }),
        Err(RangeError::StaleEpoch { .. })
    ));
    assert!(matches!(
        e.home(Epoch(99), HomeOp::Status { txn: t }),
        Err(RangeError::Unavailable(_))
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

/// Fences `engine` from inside the sequencer's timestamp reservation of a commit group: after
/// the worker's own fence check and before the group's first read (P9-5b).
struct FencingOracle {
    inner: Oracle,
    engine: Mutex<Option<Weak<RangeEngine>>>,
}
impl TsOracle for FencingOracle {
    fn next(&self, n: u32) -> Result<Timestamp, RangeError> {
        let armed = self.engine.lock().unwrap().take();
        if let Some(engine) = armed.and_then(|engine| engine.upgrade()) {
            engine.close();
        }
        self.inner.next(n)
    }
}

#[test]
fn a_commit_fenced_as_its_group_starts_is_aborted_not_corrupt() {
    let f = fixture();
    let oracle = Arc::new(FencingOracle {
        inner: Oracle(AtomicU64::new(1 << 20)),
        engine: Mutex::new(None),
    });
    let descriptor = f.store.descriptor(B).unwrap().unwrap();
    let e = RangeEngine::open(
        &f.store,
        descriptor,
        f.sink.clone(),
        oracle.clone(),
        config(),
        1,
    )
    .unwrap();
    let grant = lock(&e, txn(1), 1, Some(1000)).unwrap();
    let applied = f.store.applied(B).unwrap();
    *oracle.engine.lock().unwrap() = Some(Arc::downgrade(&e));
    let result = e.commit(
        epoch(&e),
        txn(1),
        vec![write(1, b"x")],
        None,
        Some(grant.generation),
    );
    // Was Corrupt("Misuse"): the group's native-content read failed on the fenced KV.
    assert_eq!(
        result,
        Err(RangeError::Aborted("range engine fenced".into()))
    );
    assert_eq!(f.store.applied(B).unwrap(), applied, "nothing proposed");
}

/// A Shared gap lock over Rows keys `from..=to` of the fixture table.
fn gap(
    e: &RangeEngine,
    t: ClusterTxnId,
    from: u64,
    to: u64,
    timeout_ms: Option<u64>,
) -> Result<LockGrant, RangeError> {
    e.lock(LockRequest {
        range: B,
        epoch: epoch(e),
        txn: t,
        priority: Timestamp(0),
        target: LockTarget::Interval {
            space: RangeSpace::Rows,
            start: Bound::Included(key(from)),
            end: Bound::Included(key(to)),
        },
        mode: RangeLockMode::Shared,
        timeout_ms,
    })
}

/// GD-0075 R5-72/R5-73, porting the prototype's
/// predicate_gap_locks_prevent_phantoms_cas_revalidates_complete_before_images (transactions.rs
/// 369) to range engines: a gap lock blocks an insert into the gap but not outside it, and a
/// gap request waits for a writer inside it. Prepared with its transaction, the gap lock is
/// re-taken by the next leader (here an engine reopened under a new lock generation), so the
/// phantom stays blocked across the leader change until the outcome; afterwards the writer
/// locks, prepares and commits.
#[test]
fn gap_locks_block_phantoms_and_survive_a_leader_change() {
    let f = fixture();
    let e = f.open_at(1);
    // Keys 1..=10 hold no rows: the reader's gap lock covers the empty interval.
    let grant = gap(&e, txn(1), 1, 10, Some(0)).unwrap();
    assert_eq!(grant.latest, None);
    assert_eq!(lock(&e, txn(2), 5, Some(50)), Err(RangeError::LockTimeout));
    lock(&e, txn(2), 20, Some(0)).unwrap();
    // A gap request over a key another transaction holds Exclusive waits for it.
    assert_eq!(
        gap(&e, txn(3), 15, 25, Some(50)),
        Err(RangeError::LockTimeout)
    );
    gap(&e, txn(3), 11, 19, Some(0)).unwrap();
    // Only Shared gap locks exist.
    let exclusive = e.lock(LockRequest {
        range: B,
        epoch: epoch(&e),
        txn: txn(4),
        priority: Timestamp(0),
        target: LockTarget::Interval {
            space: RangeSpace::Rows,
            start: Bound::Included(key(1)),
            end: Bound::Unbounded,
        },
        mode: RangeLockMode::Exclusive,
        timeout_ms: Some(0),
    });
    assert!(exclusive.is_err(), "{exclusive:?}");
    // The reader prepares with no writes: its gap lock goes into the prepared record.
    let p = e.prepare(epoch(&e), txn(1), B, vec![], Some(1)).unwrap();
    e.close();
    let e = f.open_at(2);
    // New leader: unprepared locks are gone (txn 2's key 20), the prepared gap lock is back.
    lock(&e, txn(5), 20, Some(0)).unwrap();
    assert_eq!(lock(&e, txn(6), 5, Some(50)), Err(RangeError::LockTimeout));
    assert_eq!(e.prepare(epoch(&e), txn(1), B, vec![], None).unwrap(), p);
    // Abort releases it: the writer goes ahead.
    e.finalize(txn(1), Outcome::Abort).unwrap();
    let grant = lock(&e, txn(6), 5, Some(1000)).unwrap();
    let c = e
        .commit(
            epoch(&e),
            txn(6),
            vec![write(5, b"phantom")],
            None,
            Some(grant.generation),
        )
        .unwrap();
    assert!(c.0 > p.0);
    let rows = read_all(&e, Timestamp(c.0 + 1), ReadDirection::Forward, 1 << 20);
    assert_eq!(rows, vec![(5, Some(b"phantom".to_vec()))]);
}

/// R5-73: a prepared record holding a gap lock round-trips; Shared plus Exclusive on one key is
/// stored once, as Exclusive; a gap lock in Exclusive mode is refused both ways.
#[test]
fn prepared_records_carry_gap_locks() {
    use crate::storage::engine::locks::LockSpace;
    let gap = LockKey::Interval {
        space: LockSpace::Unique,
        start: Bound::Excluded(vec![0, 0, 0, 0, 0, 0, 0, 9, 1]),
        end: Bound::Unbounded,
    };
    let row = LockKey::Row {
        table: T,
        pk: vec![1],
    };
    let record = prepared::Prepared {
        home: B,
        at: Ts(7),
        writes: prepared::Writes::new(),
        locks: vec![
            (gap.clone(), LockMode::Shared),
            (row.clone(), LockMode::Shared),
            (row.clone(), LockMode::Exclusive),
        ],
    };
    let back = prepared::Prepared::decode(&record.encode().unwrap()).unwrap();
    let mut locks = back.locks.clone();
    locks.sort_by_key(|(key, _)| format!("{key:?}"));
    let mut want = vec![(gap.clone(), LockMode::Shared), (row, LockMode::Exclusive)];
    want.sort_by_key(|(key, _)| format!("{key:?}"));
    assert_eq!(locks, want);
    let bad = prepared::Prepared {
        locks: vec![(gap, LockMode::Exclusive)],
        ..back
    };
    assert!(bad.encode().is_err());
}
