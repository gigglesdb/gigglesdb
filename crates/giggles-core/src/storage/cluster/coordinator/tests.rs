use super::*;
use crate::range_api::{
    LeaseKind, LockGrant, LockRequest, RangeDescriptor, RangeSpace, ReadPage, ReadRequest, TsOracle,
};
use crate::storage::range::home::tests::MemLog;
use crate::storage::range::home::{HomeRecords, RecoveryConfig};
use crate::storage::range::txn_record::{Resolution, resolve_prepared};
use std::collections::{HashMap, HashSet};
use std::ops::Range;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

struct Oracle(AtomicU64);
impl TsOracle for Oracle {
    fn next(&self, n: u32) -> Result<Timestamp, RangeError> {
        Ok(Timestamp(
            self.0.fetch_add(u64::from(n), Ordering::SeqCst) + 1,
        ))
    }
}

#[derive(Default)]
struct RangeState {
    prepared: HashMap<ClusterTxnId, (Timestamp, Vec<RangeWrite>)>,
    versions: Vec<(Vec<u8>, Timestamp)>,
    locks: HashSet<ClusterTxnId>,
    prepared_at: Vec<Timestamp>,
    /// The range's lock generation (0 until a test simulates a leader change).
    generation: u64,
}

/// In-memory cluster: participants follow the prepare/finalize rules of design.md 3-5, and
/// each home range keeps its records in the real [`HomeRecords`] over a [`MemLog`].
struct FakeCluster {
    oracle: Oracle,
    ranges: Mutex<HashMap<RangeId, RangeState>>,
    homes: Mutex<HashMap<RangeId, Arc<HomeRecords>>>,
    logs: Mutex<HashMap<RangeId, Arc<MemLog>>>,
    fail_prepare: Option<RangeId>,
    home_down: AtomicBool,
    home_ready_at: Option<Instant>,
    prepare_fault: Mutex<Option<(RangeId, usize, RangeError)>>,
    prepare_calls: AtomicUsize,
    /// When non-zero, home ops with another epoch are refused as stale.
    home_epoch: AtomicU64,
    lose_decide_reply: AtomicBool,
    /// Returned (without reaching the home) by the home op after a lost Decide reply.
    after_lost_reply: Mutex<Option<RangeError>>,
    /// Armed by a test: becomes `after_lost_reply` when the Decide reply is lost.
    next_error: Mutex<Option<RangeError>>,
    decide_calls: AtomicUsize,
}

impl FakeCluster {
    fn new(locked: &[RangeId], txn: ClusterTxnId) -> Self {
        let mut ranges = HashMap::<RangeId, RangeState>::new();
        for &r in locked {
            ranges.entry(r).or_default().locks.insert(txn);
        }
        Self {
            oracle: Oracle(AtomicU64::new(100)),
            ranges: Mutex::new(ranges),
            homes: Mutex::new(HashMap::new()),
            logs: Mutex::new(HashMap::new()),
            fail_prepare: None,
            home_down: AtomicBool::new(false),
            home_ready_at: None,
            prepare_fault: Mutex::new(None),
            prepare_calls: AtomicUsize::new(0),
            home_epoch: AtomicU64::new(0),
            lose_decide_reply: AtomicBool::new(false),
            after_lost_reply: Mutex::new(None),
            next_error: Mutex::new(None),
            decide_calls: AtomicUsize::new(0),
        }
    }

    fn home_records(&self, range: RangeId) -> Arc<HomeRecords> {
        let mut homes = self.homes.lock().unwrap();
        homes
            .entry(range)
            .or_insert_with(|| {
                let log = Arc::new(MemLog::default());
                self.logs.lock().unwrap().insert(range, log.clone());
                Arc::new(HomeRecords::new(log))
            })
            .clone()
    }

    fn home_record_count(&self) -> usize {
        let logs = self.logs.lock().unwrap();
        logs.values()
            .map(|l| {
                crate::storage::range::home::HomeLog::list(l.as_ref())
                    .unwrap()
                    .len()
            })
            .sum()
    }

    /// Runs recovery on every home range as if `after` had passed since the last operation.
    fn recover(&self, after: Duration) {
        let cfg = RecoveryConfig {
            // Coordinator failover retries can now consume five real seconds. Keep the
            // synthetic one-second recovery below the timeout; sixty seconds still expires it.
            recover_after: Duration::from_secs(60),
            finish_after: Duration::from_secs(1),
        };
        let homes: Vec<_> = self.homes.lock().unwrap().values().cloned().collect();
        for h in homes {
            h.recover_once(&Link::up(self), cfg, Instant::now() + after)
                .unwrap();
        }
    }
}

/// The coordinator's view of the cluster. With a budget, it dies after that many calls:
/// every later call fails without reaching the cluster.
struct Link<'a> {
    cluster: &'a FakeCluster,
    budget: Option<AtomicUsize>,
}

impl<'a> Link<'a> {
    fn up(cluster: &'a FakeCluster) -> Self {
        Self {
            cluster,
            budget: None,
        }
    }
    fn dies_after(cluster: &'a FakeCluster, calls: usize) -> Self {
        Self {
            cluster,
            budget: Some(AtomicUsize::new(calls)),
        }
    }
    fn call(&self) -> Result<(), RangeError> {
        let Some(budget) = &self.budget else {
            return Ok(());
        };
        budget
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |b| b.checked_sub(1))
            .map(|_| ())
            .map_err(|_| RangeError::Unavailable("coordinator dead".into()))
    }
}

impl RangeClient for Link<'_> {
    fn descriptor_for(&self, _: &[u8]) -> Result<RangeDescriptor, RangeError> {
        unimplemented!()
    }
    fn invalidate(&self, _: &RangeDescriptor) {}
    fn read(&self, _: ReadRequest) -> Result<ReadPage, RangeError> {
        unimplemented!()
    }
    fn lock(&self, _: LockRequest) -> Result<LockGrant, RangeError> {
        unimplemented!()
    }
    fn blockers(&self, _: RangeId, _: ClusterTxnId) -> Result<Vec<ClusterTxnId>, RangeError> {
        Ok(Vec::new())
    }
    fn cancel_wait(&self, _: RangeId, _: ClusterTxnId) -> Result<(), RangeError> {
        Ok(())
    }
    fn release(&self, range: RangeId, txn: ClusterTxnId) -> Result<(), RangeError> {
        self.call()?;
        let mut ranges = self.cluster.ranges.lock().unwrap();
        let rs = ranges.entry(range).or_default();
        // Release never touches prepared state.
        if !rs.prepared.contains_key(&txn) {
            rs.locks.remove(&txn);
        }
        Ok(())
    }
    fn heartbeat(&self, _: RangeId, _: ClusterTxnId) -> Result<(), RangeError> {
        Ok(())
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
        unimplemented!()
    }
    fn prepare(
        &self,
        range: RangeId,
        _: Epoch,
        txn: ClusterTxnId,
        _: RangeId,
        writes: Vec<RangeWrite>,
        lock_generation: Option<u64>,
    ) -> Result<Timestamp, RangeError> {
        self.call()?;
        self.cluster.prepare_calls.fetch_add(1, Ordering::SeqCst);
        if let Some((target, remaining, error)) =
            self.cluster.prepare_fault.lock().unwrap().as_mut()
            && *target == range
            && *remaining > 0
        {
            *remaining -= 1;
            return Err(error.clone());
        }
        if self.cluster.fail_prepare == Some(range) {
            return Err(RangeError::Aborted("injected prepare failure".into()));
        }
        let mut ranges = self.cluster.ranges.lock().unwrap();
        let rs = ranges.entry(range).or_default();
        if lock_generation.is_some_and(|g| g != rs.generation) {
            return Err(RangeError::Aborted("locks lost: another generation".into()));
        }
        if let Some((p, _)) = rs.prepared.get(&txn) {
            return Ok(*p);
        }
        if !rs.locks.contains(&txn) {
            return Err(RangeError::Aborted("locks lost".into()));
        }
        let p = self.cluster.oracle.next(1)?;
        rs.prepared.insert(txn, (p, writes));
        rs.prepared_at.push(p);
        Ok(p)
    }
    fn finalize(
        &self,
        range: RangeId,
        txn: ClusterTxnId,
        outcome: Outcome,
    ) -> Result<(), RangeError> {
        self.call()?;
        let mut ranges = self.cluster.ranges.lock().unwrap();
        let rs = ranges.entry(range).or_default();
        if let (Some((_, writes)), Outcome::Commit(c)) = (rs.prepared.remove(&txn), &outcome) {
            rs.versions.extend(writes.into_iter().map(|w| (w.key, *c)));
        }
        // finalize releases every lock of txn on the range (design.md Changes (d)).
        rs.locks.remove(&txn);
        Ok(())
    }
    fn home(&self, range: RangeId, epoch: Epoch, op: HomeOp) -> Result<HomeReply, RangeError> {
        self.call()?;
        if self
            .cluster
            .home_ready_at
            .is_some_and(|at| Instant::now() < at)
        {
            return Err(not_leader());
        }
        if self.cluster.home_down.load(Ordering::SeqCst) {
            return Err(RangeError::Unavailable("home down".into()));
        }
        if let Some(e) = self.cluster.after_lost_reply.lock().unwrap().take() {
            return Err(e);
        }
        let current = self.cluster.home_epoch.load(Ordering::SeqCst);
        if current != 0 && epoch.0 != current {
            let descriptor = RangeDescriptor {
                id: range,
                epoch: Epoch(current),
                start: Vec::new(),
                end: None,
                replicas: Vec::new(),
            };
            return Err(RangeError::StaleEpoch { descriptor });
        }
        let is_decide = matches!(op, HomeOp::Decide { .. });
        let reply = self.cluster.home_records(range).apply(op);
        if is_decide {
            self.cluster.decide_calls.fetch_add(1, Ordering::SeqCst);
            if self.cluster.lose_decide_reply.swap(false, Ordering::SeqCst) {
                let next = self.cluster.next_error.lock().unwrap().take();
                *self.cluster.after_lost_reply.lock().unwrap() = next;
                return Err(RangeError::Unavailable("reply lost".into()));
            }
        }
        reply
    }
    fn oracle(&self) -> &dyn TsOracle {
        &self.cluster.oracle
    }
    fn lease(&self, _: LeaseKind, _: u64, _: u64) -> Result<Range<u64>, RangeError> {
        unimplemented!()
    }
}

const TXN: ClusterTxnId = ClusterTxnId(7);

fn plan(ranges: &[u64], lock_only: &[u64]) -> CommitPlan {
    CommitPlan {
        txn: TXN,
        participants: ranges
            .iter()
            .map(|&r| Participant {
                range: RangeId(r),
                epoch: Epoch(1),
                writes: vec![RangeWrite {
                    space: RangeSpace::Rows,
                    key: format!("k{r}").into_bytes(),
                    value: Some(b"v".to_vec()),
                }],
                lock_generation: Some(0),
            })
            .collect(),
        lock_only: lock_only.iter().map(|&r| RangeId(r)).collect(),
    }
}

fn ids(r: &[u64]) -> Vec<RangeId> {
    r.iter().map(|&r| RangeId(r)).collect()
}

fn assert_clean(fake: &FakeCluster) {
    let ranges = fake.ranges.lock().unwrap();
    for (r, rs) in ranges.iter() {
        assert!(rs.prepared.is_empty(), "prepared record left on {r:?}");
        assert!(rs.locks.is_empty(), "lock left on {r:?}");
    }
    drop(ranges);
    assert_eq!(fake.home_record_count(), 0, "home record not forgotten");
}

/// The commit timestamp every participant applied, or None if none did; panics if only some
/// did or they disagree (atomicity).
fn applied(fake: &FakeCluster, ranges: &[u64]) -> Option<Timestamp> {
    let st = fake.ranges.lock().unwrap();
    let per: Vec<Option<Timestamp>> = ranges
        .iter()
        .map(|r| {
            st.get(&RangeId(*r))
                .and_then(|rs| rs.versions.first().map(|(_, c)| *c))
        })
        .collect();
    assert!(
        per.iter().all(|c| *c == per[0]),
        "participants disagree: {per:?}"
    );
    per[0]
}

#[test]
fn three_ranges_commit_atomically() {
    let fake = FakeCluster::new(&ids(&[1, 2, 3, 4]), TXN);
    let c = commit_cross_range(&Link::up(&fake), plan(&[1, 2, 3], &[4])).unwrap();
    assert_clean(&fake);
    assert_eq!(applied(&fake, &[1, 2, 3]), Some(c));
    let st = fake.ranges.lock().unwrap();
    for r in 1..=3 {
        let rs = &st[&RangeId(r)];
        assert_eq!(rs.versions, vec![(format!("k{r}").into_bytes(), c)]);
        assert!(rs.prepared_at.iter().all(|&p| p < c), "c > every p");
    }
    assert!(st[&RangeId(4)].versions.is_empty());
}

#[test]
fn refused_prepare_aborts_everywhere() {
    let mut fake = FakeCluster::new(&ids(&[1, 2, 3, 4]), TXN);
    fake.fail_prepare = Some(RangeId(2));
    let err = commit_cross_range(&Link::up(&fake), plan(&[1, 2, 3], &[4])).unwrap_err();
    assert!(matches!(err, CommitError::Aborted(_)), "{err:?}");
    assert_clean(&fake);
    assert_eq!(applied(&fake, &[1, 2, 3]), None);
}

#[test]
fn locks_of_an_older_generation_abort_everywhere() {
    // Range 2 changed leader after granting the locks (its lock generation moved on).
    let fake = FakeCluster::new(&ids(&[1, 2, 3]), TXN);
    fake.ranges
        .lock()
        .unwrap()
        .get_mut(&RangeId(2))
        .unwrap()
        .generation = 1;
    let err = commit_cross_range(&Link::up(&fake), plan(&[1, 2, 3], &[])).unwrap_err();
    assert!(
        matches!(&err, CommitError::Aborted(RangeError::Aborted(m)) if m.starts_with("locks lost")),
        "{err:?}"
    );
    assert_clean(&fake);
    assert_eq!(applied(&fake, &[1, 2, 3]), None);
}

#[test]
fn prepare_without_locks_aborts() {
    // Range 3 never granted a lock, so its prepare is refused.
    let fake = FakeCluster::new(&ids(&[1, 2]), TXN);
    let err = commit_cross_range(&Link::up(&fake), plan(&[1, 2, 3], &[])).unwrap_err();
    assert!(
        matches!(err, CommitError::Aborted(RangeError::Aborted(_))),
        "{err:?}"
    );
    assert_clean(&fake);
    assert_eq!(applied(&fake, &[1, 2, 3]), None);
}

#[test]
fn lost_decide_reply_retries_same_commit_ts() {
    let fake = FakeCluster::new(&ids(&[1, 2, 3]), TXN);
    fake.lose_decide_reply.store(true, Ordering::SeqCst);
    let c = commit_cross_range(&Link::up(&fake), plan(&[1, 2, 3], &[])).unwrap();
    assert_eq!(fake.decide_calls.load(Ordering::SeqCst), 2);
    assert_clean(&fake);
    assert_eq!(applied(&fake, &[1, 2, 3]), Some(c));
}

/// Independent review finding 1: a Commit applied at home whose reply was lost, followed by a
/// non-retryable error on the retry, must not be reported as an abort.
#[test]
fn applied_commit_with_a_lost_reply_then_an_error_is_unknown_never_aborted() {
    let fake = FakeCluster::new(&ids(&[1, 2]), TXN);
    fake.lose_decide_reply.store(true, Ordering::SeqCst);
    *fake.next_error.lock().unwrap() = Some(RangeError::Corrupt("injected".into()));
    let err = commit_cross_range(&Link::up(&fake), plan(&[1, 2], &[])).unwrap_err();
    assert!(matches!(err, CommitError::Unknown(_)), "{err:?}");
    // It did commit: recovery finishes it.
    fake.recover(Duration::from_secs(60));
    assert!(applied(&fake, &[1, 2]).is_some());
    assert_clean(&fake);
}

#[test]
fn a_home_epoch_change_is_followed_not_turned_into_an_abort() {
    let fake = FakeCluster::new(&ids(&[1, 2]), TXN);
    fake.lose_decide_reply.store(true, Ordering::SeqCst);
    // The home splits while the Decide reply is lost: the retry is refused as stale once.
    let descriptor = RangeDescriptor {
        id: RangeId(1),
        epoch: Epoch(2),
        start: Vec::new(),
        end: None,
        replicas: Vec::new(),
    };
    *fake.next_error.lock().unwrap() = Some(RangeError::StaleEpoch { descriptor });
    let c = commit_cross_range(&Link::up(&fake), plan(&[1, 2], &[])).unwrap();
    assert_eq!(applied(&fake, &[1, 2]), Some(c));
    assert_clean(&fake);
}

#[test]
fn single_participant_commits_and_forgets_home() {
    let fake = FakeCluster::new(&ids(&[5]), TXN);
    let c = commit_cross_range(&Link::up(&fake), plan(&[5], &[])).unwrap();
    assert_clean(&fake);
    assert_eq!(applied(&fake, &[5]), Some(c));
}

#[test]
fn home_unreachable_before_open_is_a_definite_abort_and_releases_locks() {
    let fake = FakeCluster::new(&ids(&[1, 2, 3]), TXN);
    fake.home_down.store(true, Ordering::SeqCst);
    let start = Instant::now();
    let err = commit_cross_range(&Link::up(&fake), plan(&[1, 2], &[3])).unwrap_err();
    let elapsed = start.elapsed();
    assert!(
        elapsed >= FAILOVER_WINDOW,
        "returned too early: {elapsed:?}"
    );
    assert!(
        elapsed < FAILOVER_WINDOW + Duration::from_secs(1),
        "{elapsed:?}"
    );
    assert!(matches!(err, CommitError::Aborted(_)), "{err:?}");
    assert_clean(&fake);
}

#[test]
fn home_lost_after_the_votes_is_unknown_and_recovery_aborts_it() {
    let fake = FakeCluster::new(&ids(&[1, 2, 3]), TXN);
    // Open, Seal, two prepares and two votes reach the cluster; then the coordinator is cut
    // off before Decide.
    let link = Link::dies_after(&fake, 6);
    let err = commit_cross_range(&link, plan(&[1, 2], &[3])).unwrap_err();
    assert!(matches!(err, CommitError::Unknown(_)), "{err:?}");
    assert_eq!(fake.home_record_count(), 1);
    fake.recover(Duration::from_secs(1));
    assert_eq!(fake.home_record_count(), 1, "not idle long enough yet");
    fake.recover(Duration::from_secs(60));
    assert_eq!(applied(&fake, &[1, 2]), None);
    assert_eq!(fake.home_record_count(), 0);
    let ranges = fake.ranges.lock().unwrap();
    for r in [1, 2] {
        assert!(ranges[&RangeId(r)].prepared.is_empty());
        assert!(ranges[&RangeId(r)].locks.is_empty());
    }
}

/// R5-52 acceptance (port of giggles-distributed's crash-at-every-boundary test, in process):
/// the coordinator dies after each possible number of calls; recovery then gives one
/// consistent outcome, agreeing with whatever the client was told.
#[test]
fn coordinator_killed_at_every_step_gives_one_consistent_outcome() {
    let participants = [1, 2, 3];
    // A full run makes 17 calls (Open, Seal, 3 prepares, 3 votes, Decide, 3 finalizes,
    // 4 releases, Forget); 24 covers every cut and a few complete runs.
    let mut outcomes = HashSet::new();
    for calls in 0..24 {
        let fake = FakeCluster::new(&ids(&[1, 2, 3, 4]), TXN);
        let link = Link::dies_after(&fake, calls);
        let told = commit_cross_range(&link, plan(&participants, &[4]));
        fake.recover(Duration::from_secs(60));
        fake.recover(Duration::from_secs(120));
        let result = applied(&fake, &participants);
        match &told {
            Ok(c) => assert_eq!(result, Some(*c), "cut {calls}: acked but not applied"),
            Err(CommitError::Aborted(_)) => assert_eq!(result, None, "cut {calls}"),
            Err(CommitError::Unknown(_)) => {}
        }
        let ranges = fake.ranges.lock().unwrap();
        for r in participants {
            let rs = ranges.get(&RangeId(r));
            assert!(
                rs.is_none_or(|rs| rs.prepared.is_empty()),
                "cut {calls}: prepared record left on {r}"
            );
        }
        drop(ranges);
        // Recovery finishes every record, sealed or not (an unsealed one has no prepared
        // participant and is forgotten without a finalize).
        assert_eq!(fake.home_record_count(), 0, "cut {calls}");
        outcomes.insert((
            calls,
            result.is_some(),
            matches!(told, Err(CommitError::Unknown(_))),
        ));
    }
    assert!(outcomes.iter().any(|(_, committed, _)| *committed));
    assert!(outcomes.iter().any(|(_, committed, _)| !*committed));
}

/// The reader on `range`: resolves TXN's prepared record there through home range 1.
fn reader(fake: &FakeCluster, range: u64) -> Resolution {
    let local = |o: Outcome| Link::up(fake).finalize(RangeId(range), TXN, o);
    resolve_prepared(&Link::up(fake), RangeId(1), TXN, &local).unwrap()
}

fn prepared_on(fake: &FakeCluster, range: u64) -> bool {
    fake.ranges.lock().unwrap()[&RangeId(range)]
        .prepared
        .contains_key(&TXN)
}

/// R5-53 acceptance: a reader is never blocked by a decided transaction whose coordinator died.
#[test]
fn reader_finalizes_a_decided_commit_whose_coordinator_died() {
    let fake = FakeCluster::new(&ids(&[1, 2]), TXN);
    fake.home_epoch.store(1, Ordering::SeqCst); // the reader must learn the home's epoch
    // Open, Seal, 2 prepares, 2 votes and Decide reach the cluster; then the coordinator dies.
    let c = commit_cross_range(&Link::dies_after(&fake, 7), plan(&[1, 2], &[])).unwrap();
    assert!(prepared_on(&fake, 1) && prepared_on(&fake, 2));
    assert_eq!(reader(&fake, 2), Resolution::Finalized(Outcome::Commit(c)));
    assert!(!prepared_on(&fake, 2));
    assert_eq!(
        fake.ranges.lock().unwrap()[&RangeId(2)].versions,
        vec![(b"k2".to_vec(), c)]
    );
    // Recovery finishes the other participant and forgets the record.
    fake.recover(Duration::from_secs(60));
    assert_eq!(applied(&fake, &[1, 2]), Some(c));
    assert_clean(&fake);
}

#[test]
fn reader_waits_on_an_undecided_record_then_follows_recovery_and_presumes_abort_once_forgotten() {
    let fake = FakeCluster::new(&ids(&[1, 2]), TXN);
    // Every vote recorded, no decision: the coordinator died before Decide.
    let err = commit_cross_range(&Link::dies_after(&fake, 6), plan(&[1, 2], &[])).unwrap_err();
    assert!(matches!(err, CommitError::Unknown(_)), "{err:?}");
    assert_eq!(reader(&fake, 2), Resolution::Pending);
    assert!(prepared_on(&fake, 2));
    // Recovery aborts it after recover_after, finalizes every participant and forgets it.
    fake.recover(Duration::from_secs(60));
    assert_eq!(applied(&fake, &[1, 2]), None);
    assert_clean(&fake);
    // A late duplicate prepared record (here on range 3) finds no home record: presumed abort.
    fake.ranges
        .lock()
        .unwrap()
        .entry(RangeId(3))
        .or_default()
        .prepared
        .insert(TXN, (Timestamp(1), Vec::new()));
    assert_eq!(reader(&fake, 3), Resolution::Finalized(Outcome::Abort));
    assert!(!prepared_on(&fake, 3));
}

/// R5-54 acceptance: home records stay bounded under sustained commits, including commits
/// whose coordinator dies after the decision (recovery forgets those).
#[test]
fn home_records_stay_bounded_under_sustained_commits() {
    let fake = FakeCluster::new(&[], TXN);
    for i in 0..200u64 {
        let txn = ClusterTxnId(1000 + i);
        {
            let mut ranges = fake.ranges.lock().unwrap();
            for r in 1..=3 {
                ranges.entry(RangeId(r)).or_default().locks.insert(txn);
            }
        }
        let mut p = plan(&[1, 2, 3], &[]);
        p.txn = txn;
        // Every tenth coordinator dies right after Decide (Open, Seal, 3 prepares, 3 votes).
        let link = if i % 10 == 0 {
            Link::dies_after(&fake, 9)
        } else {
            Link::up(&fake)
        };
        let _ = commit_cross_range(&link, p);
        if i % 25 == 24 {
            fake.recover(Duration::from_secs(60));
        }
        assert!(
            fake.home_record_count() <= 3,
            "at {i}: {}",
            fake.home_record_count()
        );
    }
    fake.recover(Duration::from_secs(60));
    assert_eq!(fake.home_record_count(), 0);
    let ranges = fake.ranges.lock().unwrap();
    assert!(
        ranges
            .values()
            .all(|rs| rs.prepared.is_empty() && rs.locks.is_empty())
    );
    assert!(ranges.values().all(|rs| rs.versions.len() == 200));
}

fn not_leader() -> RangeError {
    RangeError::NotLeader { hint: None }
}

#[test]
fn home_becomes_leader_after_300ms_and_commit_succeeds() {
    let mut fake = FakeCluster::new(&ids(&[1, 2]), TXN);
    let start = Instant::now();
    fake.home_ready_at = Some(start + Duration::from_millis(300));
    let c = commit_cross_range(&Link::up(&fake), plan(&[1, 2], &[])).unwrap();
    assert!(start.elapsed() >= Duration::from_millis(300));
    assert_eq!(applied(&fake, &[1, 2]), Some(c));
    assert_clean(&fake);
}

#[test]
fn participant_prepare_not_leader_twice_then_commits() {
    let fake = FakeCluster::new(&ids(&[1, 2]), TXN);
    *fake.prepare_fault.lock().unwrap() = Some((RangeId(2), 2, not_leader()));
    let c = commit_cross_range(&Link::up(&fake), plan(&[1, 2], &[])).unwrap();
    assert_eq!(fake.prepare_calls.load(Ordering::SeqCst), 4);
    assert_eq!(applied(&fake, &[1, 2]), Some(c));
    assert_clean(&fake);
}

#[test]
fn unknown_prepare_is_not_retried() {
    let fake = FakeCluster::new(&ids(&[1]), TXN);
    *fake.prepare_fault.lock().unwrap() = Some((
        RangeId(1),
        2,
        RangeError::Unknown("prepare reply lost".into()),
    ));
    let err = commit_cross_range(&Link::up(&fake), plan(&[1], &[])).unwrap_err();
    assert!(
        matches!(err, CommitError::Aborted(RangeError::Unknown(_))),
        "{err:?}"
    );
    assert_eq!(fake.prepare_calls.load(Ordering::SeqCst), 1);
    assert_eq!(applied(&fake, &[1]), None);
    assert_clean(&fake);
}
