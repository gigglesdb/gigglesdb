//! R5-82: a prepared transaction divided by a split is finalized on both halves before its
//! home record can be forgotten (real engines over one RangeStore, direct-apply sinks).
use super::tests::{B, Fixture, Oracle, Sink, config, epoch, fixture, read_all, txn};
use super::*;
use crate::storage::range::store::SplitOutcome;
use crate::storage::range::store::tests::SPLIT_TABLE;
use crate::storage::range_api::{
    HomeStatus, LeaseKind, LockTarget, RangeLockMode, RangeSpace, ReadDirection, range_key,
};
use crate::storage::replicated::AppliedPosition;
use std::collections::HashSet;
use std::ops::Bound;

const R: RangeId = RangeId(5);
const LEFT_T: u64 = SPLIT_TABLE;
const RIGHT_T: u64 = SPLIT_TABLE + 2;

fn row(table: u64, n: u64) -> Vec<u8> {
    [table.to_be_bytes(), n.to_be_bytes()].concat()
}

/// Routes finalize and home calls to the node's engines; ranges in `down` are unreachable.
/// Like the routed client (review B1), a call by range id reaches only a range in `routes`
/// (known from the start, or learned); otherwise "no route".
struct Cluster {
    engines: Mutex<HashMap<RangeId, Arc<RangeEngine>>>,
    down: Mutex<HashSet<RangeId>>,
    routes: Mutex<HashSet<RangeId>>,
    learned: Mutex<Vec<RangeDescriptor>>,
    /// Learn is recorded but adds no route.
    deaf: AtomicBool,
    oracle: Arc<Oracle>,
}
impl Cluster {
    fn engine(&self, range: RangeId) -> Result<Arc<RangeEngine>, RangeError> {
        if !self.routes.lock().unwrap().contains(&range) {
            return Err(RangeError::Unavailable(format!(
                "no route to range {}",
                range.0
            )));
        }
        if self.down.lock().unwrap().contains(&range) {
            return down();
        }
        self.engines
            .lock()
            .unwrap()
            .get(&range)
            .cloned()
            .map_or_else(down, Ok)
    }
}
fn down<T>() -> Result<T, RangeError> {
    Err(RangeError::Unavailable("test double".into()))
}
impl RangeClient for Cluster {
    fn descriptor_for(&self, _: &[u8]) -> Result<RangeDescriptor, RangeError> {
        down()
    }
    fn invalidate(&self, _: &RangeDescriptor) {}
    fn learn(&self, descriptor: &RangeDescriptor) {
        self.learned.lock().unwrap().push(descriptor.clone());
        if !self.deaf.load(Ordering::SeqCst) {
            self.routes.lock().unwrap().insert(descriptor.id);
        }
    }
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
    fn finalize(
        &self,
        range: RangeId,
        txn: ClusterTxnId,
        outcome: Outcome,
    ) -> Result<(), RangeError> {
        self.engine(range)?.finalize(txn, outcome)
    }
    fn home(&self, range: RangeId, epoch: Epoch, op: HomeOp) -> Result<HomeReply, RangeError> {
        self.engine(range)?.home(epoch, op)
    }
    fn oracle(&self) -> &dyn TsOracle {
        self.oracle.as_ref()
    }
    fn lease(&self, _: LeaseKind, _: u64, _: u64) -> Result<std::ops::Range<u64>, RangeError> {
        down()
    }
}

struct Split {
    f: Fixture,
    left: Arc<RangeEngine>,
    right: Arc<RangeEngine>,
    cluster: Arc<Cluster>,
    _client: Arc<dyn RangeClient>,
    incarnation: u64,
}
/// `txn(1)`.
const T: ClusterTxnId = ClusterTxnId((1 << 48) | 1);

fn lock(
    e: &RangeEngine,
    t: ClusterTxnId,
    key: Vec<u8>,
    timeout_ms: Option<u64>,
) -> Result<LockGrant, RangeError> {
    e.lock(LockRequest {
        range: e.descriptor.id,
        epoch: epoch(e),
        txn: t,
        priority: Timestamp(0),
        target: LockTarget::Key {
            space: RangeSpace::Rows,
            key,
        },
        mode: RangeLockMode::Exclusive,
        timeout_ms,
    })
}
fn home(s: &Split, op: HomeOp) -> HomeReply {
    s.left.home(epoch(&s.left), op).unwrap()
}

/// T (homed on B) prepares writes on both sides of the split key on B; then B splits at
/// Rows table 11 into B (left) and R (right), and both halves open with a shared client.
fn prepared_then_split() -> Split {
    prepared_then_split_with_gap(false)
}

fn prepared_then_split_with_gap(gap: bool) -> Split {
    let f = fixture();
    let e = f.open();
    let HomeReply::Opened { incarnation } = e.home(epoch(&e), HomeOp::Open { txn: T }).unwrap()
    else {
        panic!("open")
    };
    let seal = HomeOp::Seal {
        txn: T,
        incarnation,
        participants: vec![B],
    };
    e.home(epoch(&e), seal).unwrap();
    let mut generation = None;
    for key in [row(LEFT_T, 1), row(RIGHT_T, 2)] {
        generation = Some(lock(&e, T, key, Some(0)).unwrap().generation);
    }
    if gap {
        generation = Some(
            e.lock(LockRequest {
                range: B,
                epoch: epoch(&e),
                txn: T,
                priority: Timestamp(0),
                target: LockTarget::Interval {
                    space: RangeSpace::Rows,
                    start: Bound::Included(row(LEFT_T, 0)),
                    end: Bound::Included(row(RIGHT_T, 9)),
                },
                mode: RangeLockMode::Shared,
                timeout_ms: Some(0),
            })
            .unwrap()
            .generation,
        );
    }
    let writes = [(LEFT_T, 1), (RIGHT_T, 2)]
        .map(|(table, n)| RangeWrite {
            space: RangeSpace::Rows,
            key: row(table, n),
            value: Some(format!("t{table}").into_bytes()),
        })
        .to_vec();
    let p = e.prepare(epoch(&e), T, B, writes, generation).unwrap();
    let vote = HomeOp::Vote {
        txn: T,
        incarnation,
        participant: B,
        prepared_at: p,
    };
    e.home(epoch(&e), vote).unwrap();
    let at_key = range_key(RangeSpace::Rows, &(SPLIT_TABLE + 1).to_be_bytes());
    let next = f.store.applied(B).unwrap().unwrap().index + 1;
    let outcome = f
        .store
        .split(
            B,
            epoch(&e),
            &at_key,
            R,
            AppliedPosition {
                index: next,
                term: 1,
            },
        )
        .unwrap();
    assert!(
        matches!(outcome, SplitOutcome::Applied { .. }),
        "{outcome:?}"
    );
    e.close();
    let left = f.open();
    let sink = Arc::new(Sink {
        store: f.store.clone(),
        range: R,
        fail: AtomicBool::new(false),
    });
    let descriptor = f.store.descriptor(R).unwrap().unwrap();
    let right =
        RangeEngine::open(&f.store, descriptor, sink, f.oracle.clone(), config(), 1).unwrap();
    let cluster = Arc::new(Cluster {
        engines: Mutex::new(HashMap::from([(B, left.clone()), (R, right.clone())])),
        down: Mutex::new(HashSet::new()),
        // Only the parent is routed: nothing has published the child yet.
        routes: Mutex::new(HashSet::from([B])),
        learned: Mutex::new(Vec::new()),
        deaf: AtomicBool::new(false),
        oracle: f.oracle.clone(),
    });
    let client: Arc<dyn RangeClient> = cluster.clone();
    for e in [&left, &right] {
        e.set_client(Arc::downgrade(&client));
    }
    // Reopened: the parent keeps its share and names R; R holds the rest and its locks.
    let kept = left.state().prepared[&T].clone();
    assert_eq!((kept.children.as_slice(), kept.at), (&[R][..], ts(p)));
    assert_eq!(
        kept.writes
            .keys()
            .map(|(_, k)| k.clone())
            .collect::<Vec<_>>(),
        [row(LEFT_T, 1)]
    );
    let moved = right.state().prepared[&T].clone();
    assert_eq!((moved.home, moved.at, moved.children.len()), (B, ts(p), 0));
    assert_eq!(
        moved
            .writes
            .keys()
            .map(|(_, k)| k.clone())
            .collect::<Vec<_>>(),
        [row(RIGHT_T, 2)]
    );
    let s = Split {
        f,
        left,
        right,
        cluster,
        _client: client,
        incarnation,
    };
    // A reader of a right key at T >= p waits (the home is undecided), and so does a locker.
    let read = s.right.read(ReadRequest {
        range: R,
        epoch: epoch(&s.right),
        space: RangeSpace::Rows,
        start: Bound::Unbounded,
        end: Bound::Unbounded,
        dir: ReadDirection::Forward,
        ts: Timestamp(p.0 + 10),
        page_bytes: 1 << 20,
    });
    assert!(matches!(read, Err(RangeError::Unavailable(_))), "{read:?}");
    assert_eq!(
        lock(&s.right, txn(2), row(RIGHT_T, 2), Some(100)).map(drop),
        Err(RangeError::LockTimeout)
    );
    s
}

fn decide(s: &Split, outcome: Outcome) {
    let decide = HomeOp::Decide {
        txn: T,
        incarnation: s.incarnation,
        outcome,
    };
    assert_eq!(home(s, decide), HomeReply::Ok);
}
fn records(s: &Split) -> [bool; 2] {
    [B, R].map(|range| {
        let key =
            crate::storage::engine::meta::range_local_key(range, &RangeLocalKey::Prepared(T.0));
        s.f.store.kv().get(Keyspace::Meta, &key).unwrap().is_some()
    })
}
fn rows(e: &RangeEngine, at: Timestamp) -> Vec<(u64, Option<Vec<u8>>)> {
    read_all(e, at, ReadDirection::Forward, 1 << 20)
}
/// Both halves show T's writes at c and not below; Forget then leaves no record anywhere.
fn committed_then_forgotten(s: &Split, c: Timestamp) {
    assert_eq!(records(s), [false, false]);
    for (e, n, value) in [(&s.left, 1, "t10"), (&s.right, 2, "t12")] {
        assert_eq!(rows(e, c), vec![(n, Some(value.as_bytes().to_vec()))]);
        assert_eq!(rows(e, Timestamp(c.0 - 1)), vec![]);
        assert!(e.state().prepared.is_empty());
    }
    let forget = HomeOp::Forget {
        txn: T,
        incarnation: s.incarnation,
    };
    assert_eq!(home(s, forget), HomeReply::Ok);
    assert_eq!(
        home(s, HomeOp::Status { txn: T }),
        HomeReply::Status(HomeStatus::Unknown)
    );
}

#[test]
fn a_slow_parent_finalizes_its_child_before_its_own_share() {
    let s = prepared_then_split();
    let c = s.f.oracle.next(1).unwrap();
    decide(&s, Outcome::Commit(c));
    // Review B1: without the child's descriptor the parent's forward has no route to R.
    s.cluster.deaf.store(true, Ordering::SeqCst);
    let result = s.left.finalize(T, Outcome::Commit(c));
    assert_eq!(
        result,
        Err(RangeError::Unavailable("no route to range 5".into()))
    );
    assert_eq!(records(&s), [true, true]);
    s.cluster.deaf.store(false, Ordering::SeqCst);
    s.cluster.finalize(B, T, Outcome::Commit(c)).unwrap();
    // The parent taught the client R's stored descriptor before forwarding.
    let stored = s.f.store.descriptor(R).unwrap().unwrap();
    assert_eq!(s.cluster.learned.lock().unwrap().last(), Some(&stored));
    committed_then_forgotten(&s, c);
    // A repeat after Forget changes nothing.
    s.cluster.finalize(B, T, Outcome::Commit(c)).unwrap();
    assert_eq!(records(&s), [false, false]);
}

#[test]
fn a_fast_child_resolved_through_the_home_makes_the_forward_a_no_op() {
    let s = prepared_then_split();
    let c = s.f.oracle.next(1).unwrap();
    decide(&s, Outcome::Commit(c));
    // R5-53: the child's reader asks the home (on the parent) and finalizes R alone.
    assert_eq!(rows(&s.right, c), vec![(2, Some(b"t12".to_vec()))]);
    assert_eq!(records(&s), [true, false]);
    s.cluster.finalize(B, T, Outcome::Commit(c)).unwrap();
    committed_then_forgotten(&s, c);
}

#[test]
fn abort_releases_both_halves_and_writes_nothing() {
    let s = prepared_then_split();
    decide(&s, Outcome::Abort);
    s.cluster.finalize(B, T, Outcome::Abort).unwrap();
    assert_eq!(records(&s), [false, false]);
    let late = s.f.oracle.next(1).unwrap();
    for (e, key) in [(&s.left, row(LEFT_T, 1)), (&s.right, row(RIGHT_T, 2))] {
        assert_eq!(rows(e, late), vec![]);
        assert!(e.state().prepared.is_empty());
        lock(e, txn(3), key, Some(0)).unwrap();
    }
}

#[test]
fn an_unreachable_child_fails_the_parent_finalize_until_it_returns() {
    let s = prepared_then_split();
    let c = s.f.oracle.next(1).unwrap();
    decide(&s, Outcome::Commit(c));
    s.cluster.down.lock().unwrap().insert(R);
    for _ in 0..2 {
        let result = s.left.finalize(T, Outcome::Commit(c));
        assert!(
            matches!(result, Err(RangeError::Unavailable(_))),
            "{result:?}"
        );
        assert_eq!(records(&s), [true, true]);
        assert_eq!(s.left.state().prepared[&T].children, vec![R]);
    }
    // An engine without a client cannot forward either.
    let alone = s.f.open();
    let result = alone.finalize(T, Outcome::Commit(c));
    assert!(
        matches!(result, Err(RangeError::Unavailable(_))),
        "{result:?}"
    );
    alone.close();
    // The child is back: the coordinator's retry goes through.
    s.cluster.down.lock().unwrap().clear();
    s.cluster.finalize(B, T, Outcome::Commit(c)).unwrap();
    committed_then_forgotten(&s, c);
}

#[test]
fn prepared_gap_protects_the_reopened_child_after_split() {
    let s = prepared_then_split_with_gap(true);
    // These keys have no point locks: only the split prepared gap protects them.
    for (engine, key) in [(&s.left, row(LEFT_T, 3)), (&s.right, row(RIGHT_T, 3))] {
        assert_eq!(
            lock(engine, txn(2), key.clone(), Some(0)).map(drop),
            Err(RangeError::LockTimeout)
        );
    }
    decide(&s, Outcome::Abort);
    s.cluster.finalize(B, T, Outcome::Abort).unwrap();
    lock(&s.right, txn(2), row(RIGHT_T, 3), Some(0)).unwrap();
}
