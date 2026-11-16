//! The engine with a `RangeClient` (GD-0075 Track C, c-engine): readers and lockers resolve
//! prepared records through their homes (R5-53), the recovery worker (R5-52), and catalog-only
//! commits without a lease.
use super::tests::{B, Fixture, Sink, config, epoch, fixture, lock, read_all, txn, write};
use super::*;
use crate::storage::engine::codec;
use crate::storage::range_api::{
    HomeStatus, LeaseKind, LockTarget, RangeLockMode, RangeSpace, ReadDirection,
};
use std::ops::Bound;
use std::sync::atomic::AtomicUsize;

const HOME: RangeId = RangeId(7);

/// A home range that answers Status with a scripted reply; every other call is unavailable.
struct Homes {
    status: Mutex<HomeStatus>,
    asked: AtomicUsize,
    oracle: super::tests::Oracle,
}
impl Homes {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            status: Mutex::new(HomeStatus::Open {
                sealed: None,
                votes: Vec::new(),
            }),
            asked: AtomicUsize::new(0),
            oracle: super::tests::Oracle(std::sync::atomic::AtomicU64::new(1)),
        })
    }
    fn set(&self, status: HomeStatus) {
        *self.status.lock().unwrap() = status;
    }
    fn asked(&self) -> usize {
        self.asked.load(Ordering::SeqCst)
    }
}
fn down<T>() -> Result<T, RangeError> {
    Err(RangeError::Unavailable("test double".into()))
}
impl RangeClient for Homes {
    fn descriptor_for(&self, _: &[u8]) -> Result<RangeDescriptor, RangeError> {
        down()
    }
    fn invalidate(&self, _: &RangeDescriptor) {}
    fn read(&self, _: ReadRequest) -> Result<ReadPage, RangeError> {
        down()
    }
    fn lock(&self, _: LockRequest) -> Result<LockGrant, RangeError> {
        down()
    }
    fn blockers(&self, _: RangeId, _: ClusterTxnId) -> Result<Vec<ClusterTxnId>, RangeError> {
        down()
    }
    fn cancel_wait(&self, _: RangeId, _: ClusterTxnId) -> Result<(), RangeError> {
        down()
    }
    fn release(&self, _: RangeId, _: ClusterTxnId) -> Result<(), RangeError> {
        down()
    }
    fn heartbeat(&self, _: RangeId, _: ClusterTxnId) -> Result<(), RangeError> {
        down()
    }
    fn commit(
        &self,
        _: RangeId,
        _: Epoch,
        _: ClusterTxnId,
        _: Vec<RangeWrite>,
        _: Option<u64>,
        _: Option<u64>,
    ) -> Result<Timestamp, RangeError> {
        down()
    }
    fn prepare(
        &self,
        _: RangeId,
        _: Epoch,
        _: ClusterTxnId,
        _: RangeId,
        _: Vec<RangeWrite>,
        _: Option<u64>,
    ) -> Result<Timestamp, RangeError> {
        down()
    }
    fn finalize(&self, _: RangeId, _: ClusterTxnId, _: Outcome) -> Result<(), RangeError> {
        down()
    }
    fn home(&self, range: RangeId, _: Epoch, op: HomeOp) -> Result<HomeReply, RangeError> {
        if range != HOME || !matches!(op, HomeOp::Status { .. }) {
            return down();
        }
        self.asked.fetch_add(1, Ordering::SeqCst);
        Ok(HomeReply::Status(self.status.lock().unwrap().clone()))
    }
    fn oracle(&self) -> &dyn TsOracle {
        &self.oracle
    }
    fn lease(&self, _: LeaseKind, _: u64, _: u64) -> Result<std::ops::Range<u64>, RangeError> {
        down()
    }
}

/// Range B's engine with `homes` as its client (keep the returned client alive).
fn open_with(f: &Fixture, homes: &Arc<Homes>) -> (Arc<RangeEngine>, Arc<dyn RangeClient>) {
    let e = f.open();
    let client: Arc<dyn RangeClient> = homes.clone();
    e.set_client(Arc::downgrade(&client));
    (e, client)
}
fn prepare(e: &RangeEngine, t: ClusterTxnId, n: u64, value: &[u8]) -> Timestamp {
    let grant = lock(e, t, n, None).unwrap();
    e.prepare(
        epoch(e),
        t,
        HOME,
        vec![write(n, value)],
        Some(grant.generation),
    )
    .unwrap()
}

#[test]
fn a_blocked_reader_resolves_the_prepared_record_through_its_home() {
    let f = fixture();
    let homes = Homes::new();
    let (e, _client) = open_with(&f, &homes);
    // Decided Commit: the reader finalizes the record here and sees the write.
    let p = prepare(&e, txn(1), 1, b"committed");
    homes.set(HomeStatus::Decided(Outcome::Commit(Timestamp(p.0 + 5))));
    let start = Instant::now();
    let rows = read_all(&e, Timestamp(p.0 + 10), ReadDirection::Forward, 1 << 20);
    assert!(
        start.elapsed() < Duration::from_millis(300),
        "read_wait is 400ms"
    );
    assert_eq!(rows, vec![(1, Some(b"committed".to_vec()))]);
    assert!(e.state().prepared.is_empty());
    // Unknown: presumed abort; the record and its lock are gone.
    let q = prepare(&e, txn(2), 2, b"orphan");
    homes.set(HomeStatus::Unknown);
    let start = Instant::now();
    let rows = read_all(&e, Timestamp(q.0 + 10), ReadDirection::Forward, 1 << 20);
    assert!(start.elapsed() < Duration::from_millis(300));
    assert_eq!(rows, vec![(1, Some(b"committed".to_vec()))]);
    assert!(e.state().prepared.is_empty());
    lock(&e, txn(3), 2, Some(0)).unwrap();
}

#[test]
fn a_pending_home_keeps_the_reader_waiting_until_its_deadline() {
    let f = fixture();
    let homes = Homes::new();
    let (e, _client) = open_with(&f, &homes);
    let p = prepare(&e, txn(1), 1, b"x");
    let start = Instant::now();
    let result = e.read(ReadRequest {
        range: B,
        epoch: epoch(&e),
        space: RangeSpace::Rows,
        start: Bound::Unbounded,
        end: Bound::Unbounded,
        dir: ReadDirection::Forward,
        ts: Timestamp(p.0 + 10),
        page_bytes: 64,
    });
    assert!(
        matches!(result, Err(RangeError::Unavailable(_))),
        "{result:?}"
    );
    assert!(start.elapsed() >= Duration::from_millis(400));
    // Asked after resolve_after (50ms), then at most once per resolve_after.
    assert!((2..=8).contains(&homes.asked()), "{}", homes.asked());
    assert_eq!(e.state().prepared.len(), 1);
}

#[test]
fn a_lock_request_resolves_a_prepared_holder_but_not_an_unprepared_one() {
    let f = fixture();
    let homes = Homes::new();
    let (e, _client) = open_with(&f, &homes);
    let p = prepare(&e, txn(1), 1, b"x");
    homes.set(HomeStatus::Decided(Outcome::Abort));
    let start = Instant::now();
    lock(&e, txn(2), 1, Some(2000)).unwrap();
    assert!(start.elapsed() < Duration::from_millis(1000));
    assert!(e.state().prepared.is_empty());
    assert!(read_all(&e, Timestamp(p.0 + 10), ReadDirection::Forward, 64).is_empty());
    // An unprepared holder is never resolved: the request times out at its own deadline.
    let asked = homes.asked();
    lock(&e, txn(3), 5, None).unwrap();
    assert_eq!(lock(&e, txn(4), 5, Some(200)), Err(RangeError::LockTimeout));
    assert_eq!(homes.asked(), asked);
}

#[test]
fn the_recovery_worker_aborts_an_idle_home_record_and_stops_with_its_engines() {
    let f = fixture();
    let homes = Homes::new();
    let client: Arc<dyn RangeClient> = homes.clone();
    let mut cfg = config();
    cfg.recovery = RecoveryConfig {
        recover_after: Duration::from_millis(100),
        finish_after: Duration::from_millis(50),
    };
    let engines = RangeEngines::new(f.store.clone(), f.oracle.clone(), cfg);
    engines.set_client(Arc::downgrade(&client));
    let svc = engines
        .open(f.store.descriptor(B).unwrap().unwrap(), f.sink.clone(), 1)
        .unwrap();
    assert!(engines.open.lock().unwrap()[&B].client().is_some());
    let at = svc.descriptor().epoch;
    let t = txn(9);
    let HomeReply::Opened { incarnation } = svc.home(at, HomeOp::Open { txn: t }).unwrap() else {
        panic!("open")
    };
    let seal = HomeOp::Seal {
        txn: t,
        incarnation,
        participants: vec![HOME],
    };
    svc.home(at, seal).unwrap();
    // The participant is down, so the decided record stays for a later pass.
    let decided = HomeReply::Status(HomeStatus::Decided(Outcome::Abort));
    let start = Instant::now();
    while svc.home(at, HomeOp::Status { txn: t }).unwrap() != decided {
        assert!(start.elapsed() < Duration::from_secs(5), "never recovered");
        thread::sleep(Duration::from_millis(10));
    }
    // Dropping the engines stops the worker: nothing keeps a reference to the client.
    drop(svc);
    drop(engines);
    let start = Instant::now();
    while Arc::weak_count(&client) > 0 {
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "worker still running"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn catalog_only_commits_need_no_lock_but_rows_and_expired_leases_still_do() {
    let f = fixture();
    let sink = Arc::new(Sink {
        store: f.store.clone(),
        range: SYSTEM_RANGE,
        fail: AtomicBool::new(false),
    });
    let descriptor = f.store.descriptor(SYSTEM_RANGE).unwrap().unwrap();
    let e = RangeEngine::open(&f.store, descriptor, sink, f.oracle.clone(), config(), 1).unwrap();
    let stamp = RangeWrite {
        space: RangeSpace::Kernel,
        key: vec![255],
        value: Some(Vec::new()),
    };
    let at = e
        .commit(epoch(&e), txn(1), vec![stamp.clone()], None, None)
        .unwrap();
    let page = e
        .read(ReadRequest {
            range: SYSTEM_RANGE,
            epoch: epoch(&e),
            space: RangeSpace::Kernel,
            start: Bound::Unbounded,
            end: Bound::Unbounded,
            dir: ReadDirection::Forward,
            ts: at,
            page_bytes: 1 << 20,
        })
        .unwrap();
    assert!(
        page.pairs
            .iter()
            .any(|(raw, _)| codec::split_versioned(raw).unwrap() == (&[255u8][..], Ts(at.0)))
    );
    // Table 5's rows live on the system range: a row write still needs its lock.
    let row_key = [5u64.to_be_bytes(), 1u64.to_be_bytes()].concat();
    let row = RangeWrite {
        space: RangeSpace::Rows,
        key: row_key.clone(),
        value: Some(b"r".to_vec()),
    };
    for writes in [vec![row.clone()], vec![stamp.clone(), row]] {
        assert!(matches!(
            e.commit(epoch(&e), txn(2), writes, None, None),
            Err(RangeError::Aborted(_))
        ));
    }
    // A transaction whose lease expired here stays refused, catalog-only or not.
    e.lock(LockRequest {
        range: SYSTEM_RANGE,
        epoch: epoch(&e),
        txn: txn(3),
        priority: Timestamp(0),
        target: LockTarget::Key {
            space: RangeSpace::Rows,
            key: row_key,
        },
        mode: RangeLockMode::Exclusive,
        timeout_ms: None,
    })
    .unwrap();
    thread::sleep(Duration::from_millis(500));
    assert!(matches!(
        e.commit(epoch(&e), txn(3), vec![stamp], None, None),
        Err(RangeError::Aborted(_))
    ));
}
