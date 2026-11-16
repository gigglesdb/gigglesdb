use super::*;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// In-memory home log; incarnations come from a counter, as an applied index would.
#[derive(Default)]
pub(crate) struct MemLog {
    records: Mutex<BTreeMap<u64, Vec<u8>>>,
    next: AtomicU64,
}
impl HomeLog for MemLog {
    fn get(&self, txn: ClusterTxnId) -> Result<Option<Vec<u8>>, RangeError> {
        Ok(self.records.lock().unwrap().get(&txn.0).cloned())
    }
    fn put(&self, txn: ClusterTxnId, value: Option<Vec<u8>>) -> Result<(), RangeError> {
        let mut m = self.records.lock().unwrap();
        match value {
            Some(v) => m.insert(txn.0, v),
            None => m.remove(&txn.0),
        };
        Ok(())
    }
    fn list(&self) -> Result<Vec<ClusterTxnId>, RangeError> {
        Ok(self
            .records
            .lock()
            .unwrap()
            .keys()
            .map(|&k| ClusterTxnId(k))
            .collect())
    }
    fn fresh_incarnation(&self, _: ClusterTxnId) -> Result<u64, RangeError> {
        Ok(self.next.fetch_add(1, Ordering::SeqCst) + 1)
    }
}

const T: ClusterTxnId = ClusterTxnId(7);

fn homes() -> (Arc<MemLog>, HomeRecords) {
    let log = Arc::new(MemLog::default());
    (log.clone(), HomeRecords::new(log))
}
fn open(h: &HomeRecords) -> u64 {
    match h.apply(HomeOp::Open { txn: T }) {
        Ok(HomeReply::Opened { incarnation }) => incarnation,
        other => panic!("open: {other:?}"),
    }
}
fn seal(h: &HomeRecords, inc: u64, ranges: &[u64]) -> Result<HomeReply, RangeError> {
    h.apply(HomeOp::Seal {
        txn: T,
        incarnation: inc,
        participants: ranges.iter().map(|&r| RangeId(r)).collect(),
    })
}
fn vote(h: &HomeRecords, inc: u64, r: u64, ts: u64) -> Result<HomeReply, RangeError> {
    h.apply(HomeOp::Vote {
        txn: T,
        incarnation: inc,
        participant: RangeId(r),
        prepared_at: Timestamp(ts),
    })
}
fn decide(h: &HomeRecords, inc: u64, o: Outcome) -> Result<HomeReply, RangeError> {
    h.apply(HomeOp::Decide {
        txn: T,
        incarnation: inc,
        outcome: o,
    })
}
fn forget(h: &HomeRecords, inc: u64) -> Result<HomeReply, RangeError> {
    h.apply(HomeOp::Forget {
        txn: T,
        incarnation: inc,
    })
}
fn status(h: &HomeRecords) -> HomeStatus {
    match h.apply(HomeOp::Status { txn: T }).unwrap() {
        HomeReply::Status(s) => s,
        other => panic!("status reply {other:?}"),
    }
}

#[test]
fn decision_is_written_once_and_a_different_one_refused() {
    let (_, h) = homes();
    let inc = open(&h);
    assert_eq!(open(&h), inc, "a retried Open returns the same incarnation");
    seal(&h, inc, &[1, 2]).unwrap();
    vote(&h, inc, 1, 10).unwrap();
    assert!(matches!(
        decide(&h, inc, Outcome::Commit(Timestamp(20))),
        Err(RangeError::Conflict(_))
    ));
    vote(&h, inc, 2, 11).unwrap();
    decide(&h, inc, Outcome::Commit(Timestamp(20))).unwrap();
    decide(&h, inc, Outcome::Commit(Timestamp(20))).unwrap();
    assert!(matches!(
        decide(&h, inc, Outcome::Commit(Timestamp(21))),
        Err(RangeError::Conflict(_))
    ));
    assert!(matches!(
        decide(&h, inc, Outcome::Abort),
        Err(RangeError::Conflict(_))
    ));
    assert_eq!(
        status(&h),
        HomeStatus::Decided(Outcome::Commit(Timestamp(20)))
    );
}

#[test]
fn seal_once_votes_fixed_and_absent_records() {
    let (_, h) = homes();
    assert!(matches!(seal(&h, 1, &[1]), Err(RangeError::Aborted(_))));
    assert_eq!(decide(&h, 1, Outcome::Abort), Ok(HomeReply::Ok));
    // B1: a Commit on an absent record is unknown (it may have been forgotten after a commit).
    assert!(matches!(
        decide(&h, 1, Outcome::Commit(Timestamp(1))),
        Err(RangeError::Unknown(_))
    ));
    assert_eq!(status(&h), HomeStatus::Unknown);
    let inc = open(&h);
    seal(&h, inc, &[2, 1]).unwrap();
    seal(&h, inc, &[1, 2]).unwrap();
    assert!(matches!(
        seal(&h, inc, &[1, 3]),
        Err(RangeError::Conflict(_))
    ));
    assert!(matches!(
        seal(&h, inc, &[1, 1]),
        Err(RangeError::Conflict(_))
    ));
    vote(&h, inc, 1, 5).unwrap();
    vote(&h, inc, 1, 5).unwrap();
    assert!(matches!(vote(&h, inc, 1, 6), Err(RangeError::Conflict(_))));
    assert!(matches!(vote(&h, inc, 9, 6), Err(RangeError::Conflict(_))));
    assert_eq!(
        status(&h),
        HomeStatus::Open {
            sealed: Some(vec![RangeId(1), RangeId(2)]),
            votes: vec![(RangeId(1), Timestamp(5))]
        }
    );
    assert!(matches!(forget(&h, inc), Err(RangeError::Conflict(_))));
    decide(&h, inc, Outcome::Abort).unwrap();
    assert!(matches!(vote(&h, inc, 2, 6), Err(RangeError::Aborted(_))));
    assert_eq!(forget(&h, inc), Ok(HomeReply::Ok));
    assert_eq!(status(&h), HomeStatus::Unknown);
    assert_eq!(forget(&h, inc), Ok(HomeReply::Ok));
}

/// R5-50m B2: a delayed duplicate Open re-creates a forgotten record. It gets a new
/// incarnation, and every operation of the old coordinator sees no record.
#[test]
fn stale_open_after_forget_is_a_new_incarnation_the_old_coordinator_cannot_use() {
    let (log, h) = homes();
    let old = open(&h);
    seal(&h, old, &[1, 2]).unwrap();
    vote(&h, old, 1, 5).unwrap();
    // Recovery aborted it, finalized every participant and forgot it.
    decide(&h, old, Outcome::Abort).unwrap();
    forget(&h, old).unwrap();
    // The stale duplicate Open arrives.
    let new = open(&h);
    assert!(new > old);
    assert!(matches!(
        seal(&h, old, &[1, 2]),
        Err(RangeError::Aborted(_))
    ));
    assert!(matches!(vote(&h, old, 2, 6), Err(RangeError::Aborted(_))));
    assert!(matches!(
        decide(&h, old, Outcome::Commit(Timestamp(9))),
        Err(RangeError::Unknown(_))
    ));
    assert_eq!(decide(&h, old, Outcome::Abort), Ok(HomeReply::Ok));
    assert_eq!(forget(&h, old), Ok(HomeReply::Ok));
    // The new incarnation is untouched by all of that.
    assert_eq!(
        status(&h),
        HomeStatus::Open {
            sealed: None,
            votes: vec![]
        }
    );
    let (inc, record) = decode_record(&log.get(T).unwrap().unwrap()).unwrap();
    assert_eq!(inc, new);
    assert_eq!(record, Record::new());
}

#[test]
fn codec_round_trips_and_rejects_bad_bytes() {
    let mut r = Record::new();
    r.seal_set(
        [RangeId(1), RangeId(3)].into(),
        MAX_PARTICIPANTS,
        |_| Ok(()),
    )
    .unwrap();
    r.vote(RangeId(1), Timestamp(4), |_| true, |_| Ok(()))
        .unwrap();
    let bytes = encode_record(42, &r);
    assert_eq!(decode_record(&bytes).unwrap(), (42, r.clone()));
    r.vote(RangeId(3), Timestamp(5), |_| true, |_| Ok(()))
        .unwrap();
    r.decide(RuleOutcome::Commit, || Outcome::Commit(Timestamp(9)))
        .unwrap();
    let bytes = encode_record(u64::MAX, &r);
    assert_eq!(decode_record(&bytes).unwrap(), (u64::MAX, r));
    for cut in 0..bytes.len() {
        assert!(decode_record(&bytes[..cut]).is_err(), "truncated at {cut}");
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(decode_record(&trailing).is_err());
    let mut version = bytes.clone();
    version[0] = 2;
    assert!(decode_record(&version).is_err());
    let mut flags = bytes.clone();
    flags[9] |= 2;
    assert!(decode_record(&flags).is_err());
    // A commit without every vote is refused.
    let mut partial = Record::new();
    partial
        .seal_set(
            [RangeId(1), RangeId(3)].into(),
            MAX_PARTICIPANTS,
            |_| Ok(()),
        )
        .unwrap();
    let mut bad = encode_record(1, &partial);
    *bad.last_mut().unwrap() = COMMIT;
    bad.extend_from_slice(&9u64.to_be_bytes());
    assert!(decode_record(&bad).is_err());
}

/// The exact bytes of a version-1 record (persisted format; a change needs a new version).
#[test]
fn codec_layout_is_pinned() {
    let mut r = Record::new();
    r.seal_set(
        [RangeId(2), RangeId(5)].into(),
        MAX_PARTICIPANTS,
        |_| Ok(()),
    )
    .unwrap();
    r.vote(RangeId(2), Timestamp(0x0102), |_| true, |_| Ok(()))
        .unwrap();
    r.vote(RangeId(5), Timestamp(0x0304), |_| true, |_| Ok(()))
        .unwrap();
    r.decide(RuleOutcome::Commit, || Outcome::Commit(Timestamp(0x0506)))
        .unwrap();
    let mut want = vec![1u8];
    want.extend_from_slice(&0x0a0bu64.to_be_bytes());
    want.push(1);
    want.extend_from_slice(&2u32.to_be_bytes());
    want.extend_from_slice(&2u64.to_be_bytes());
    want.extend_from_slice(&5u64.to_be_bytes());
    want.extend_from_slice(&2u32.to_be_bytes());
    for (range, ts) in [(2u64, 0x0102u64), (5, 0x0304)] {
        want.extend_from_slice(&range.to_be_bytes());
        want.extend_from_slice(&ts.to_be_bytes());
    }
    want.push(1); // commit
    want.extend_from_slice(&0x0506u64.to_be_bytes());
    assert_eq!(encode_record(0x0a0b, &r), want);
    assert_eq!(decode_record(&want).unwrap(), (0x0a0b, r));
    let open: &[u8] = &[1, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
    assert_eq!(encode_record(3, &Record::new()), open);
    assert_eq!(decode_record(open).unwrap(), (3, Record::new()));
    // Aborted with a partial vote.
    let mut aborted = Record::new();
    aborted
        .seal_set(
            [RangeId(4), RangeId(9)].into(),
            MAX_PARTICIPANTS,
            |_| Ok(()),
        )
        .unwrap();
    aborted
        .vote(RangeId(9), Timestamp(0x77), |_| true, |_| Ok(()))
        .unwrap();
    aborted
        .decide(RuleOutcome::Abort, || Outcome::Abort)
        .unwrap();
    let golden: &[u8] = &[
        1, 0, 0, 0, 0, 0, 0, 0, 5, // version, incarnation 5
        1, 0, 0, 0, 2, // sealed, 2 participants
        0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 9, // ranges 4, 9
        0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 0, 0, 0, 0, 0x77, // vote 9 at 0x77
        2,    // abort
    ];
    assert_eq!(encode_record(5, &aborted), golden);
    assert_eq!(decode_record(golden).unwrap(), (5, aborted));
    // Decided Abort before any seal, incarnation 0.
    let mut early = Record::new();
    early.decide(RuleOutcome::Abort, || Outcome::Abort).unwrap();
    let golden: &[u8] = &[1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2];
    assert_eq!(encode_record(0, &early), golden);
    assert_eq!(decode_record(golden).unwrap(), (0, early));
}

/// Builds version-1 bytes field by field, for the rejection tests.
fn raw(sealed: bool, parts: &[u64], votes: &[(u64, u64)], decision: &[u8]) -> Vec<u8> {
    let mut b = vec![1u8];
    b.extend_from_slice(&1u64.to_be_bytes());
    b.push(u8::from(sealed));
    b.extend_from_slice(&(parts.len() as u32).to_be_bytes());
    for p in parts {
        b.extend_from_slice(&p.to_be_bytes());
    }
    b.extend_from_slice(&(votes.len() as u32).to_be_bytes());
    for (r, ts) in votes {
        b.extend_from_slice(&r.to_be_bytes());
        b.extend_from_slice(&ts.to_be_bytes());
    }
    b.extend_from_slice(decision);
    b
}

#[test]
fn codec_rejects_every_record_the_rules_cannot_produce() {
    let commit = |c: u64| {
        let mut d = vec![1u8];
        d.extend_from_slice(&c.to_be_bytes());
        d
    };
    assert!(decode_record(&raw(true, &[1, 2], &[(1, 5), (2, 6)], &commit(9))).is_ok());
    let bad = [
        ("decision tag 3", raw(true, &[1], &[], &[3])),
        ("unsorted participants", raw(true, &[2, 1], &[], &[0])),
        ("duplicate participants", raw(true, &[1, 1], &[], &[0])),
        (
            "unsorted votes",
            raw(true, &[1, 2], &[(2, 5), (1, 5)], &[0]),
        ),
        (
            "duplicate votes",
            raw(true, &[1, 2], &[(1, 5), (1, 5)], &[0]),
        ),
        (
            "vote from a non-participant",
            raw(true, &[1], &[(3, 5)], &[0]),
        ),
        ("votes without a seal", raw(false, &[], &[(1, 5)], &[0])),
        ("participants without a seal", raw(false, &[1], &[], &[0])),
        ("sealed with no participant", raw(true, &[], &[], &[0])),
        (
            "more votes than participants",
            raw(true, &[1], &[(1, 5), (2, 5)], &[0]),
        ),
        (
            "commit without every vote",
            raw(true, &[1, 2], &[(1, 5)], &commit(9)),
        ),
        (
            "commit ts below a vote",
            raw(true, &[1], &[(1, 9)], &commit(3)),
        ),
    ];
    for (what, bytes) in bad {
        assert!(
            matches!(decode_record(&bytes), Err(RangeError::Corrupt(_))),
            "{what} accepted"
        );
    }
    // Equal is valid version-1 bytes (a future c = max(p)); writers still require c > p.
    let equal = raw(true, &[1], &[(1, 9)], &commit(9));
    assert_eq!(encode_record_of(&equal), equal);
    let mut version0 = raw(true, &[1], &[], &[0]);
    version0[0] = 0;
    assert!(decode_record(&version0).is_err());
    let too_many: Vec<u64> = (1..=MAX_PARTICIPANTS as u64 + 1).collect();
    assert!(decode_record(&raw(true, &too_many, &[], &[0])).is_err());
    let max: Vec<u64> = (1..=MAX_PARTICIPANTS as u64).collect();
    let max = raw(true, &max, &[], &[0]);
    assert_eq!(encode_record_of(&max), max, "canonical");
}

/// Re-encodes decoded bytes (canonical form: one encoding per record).
fn encode_record_of(bytes: &[u8]) -> Vec<u8> {
    let (inc, record) = decode_record(bytes).unwrap();
    encode_record(inc, &record)
}

#[test]
fn seal_after_an_abort_and_a_commit_not_above_every_vote_are_refused() {
    let (_, h) = homes();
    let inc = open(&h);
    seal(&h, inc, &[1]).unwrap();
    vote(&h, inc, 1, 50).unwrap();
    assert!(matches!(
        decide(&h, inc, Outcome::Commit(Timestamp(50))),
        Err(RangeError::Conflict(_))
    ));
    decide(&h, inc, Outcome::Abort).unwrap();
    assert!(matches!(seal(&h, inc, &[1]), Err(RangeError::Aborted(_))));
}

#[test]
fn recovery_continues_past_a_corrupt_record() {
    let (log, _) = homes();
    let t0 = Instant::now();
    let h = HomeRecords::new_at(log.clone(), t0);
    log.put(ClusterTxnId(1), Some(vec![9, 9, 9])).unwrap();
    h.apply_at(HomeOp::Open { txn: T }, t0).unwrap();
    let report = h
        .recover_once(&Recorder::default(), cfg(), t0 + Duration::from_secs(60))
        .unwrap();
    assert_eq!(report.unfinished, vec![ClusterTxnId(1)]);
    assert_eq!(report.aborted, vec![T]);
    assert_eq!(report.forgotten, vec![T]);
}

fn cfg() -> RecoveryConfig {
    RecoveryConfig {
        recover_after: Duration::from_secs(5),
        finish_after: Duration::from_secs(1),
    }
}

#[test]
fn recovery_aborts_idle_undecided_and_never_commits() {
    let (log, _) = homes();
    let t0 = Instant::now();
    let h = HomeRecords::new_at(log.clone(), t0);
    h.apply_at(HomeOp::Open { txn: T }, t0).unwrap();
    let client = Recorder::default();
    // Unsealed: no participant prepared, so recovery needs no client call.
    let report = h
        .recover_once(&client, cfg(), t0 + Duration::from_secs(1))
        .unwrap();
    assert_eq!(report, RecoveryReport::default());
    let report = h
        .recover_once(&client, cfg(), t0 + Duration::from_secs(6))
        .unwrap();
    assert_eq!(report.aborted, vec![T]);
    assert_eq!(report.forgotten, vec![T]);
    assert!(client.finalized.lock().unwrap().is_empty());
    assert!(log.list().unwrap().is_empty());
}

#[test]
fn recovery_finalizes_a_sealed_record_on_every_participant_then_forgets_it() {
    let (log, _) = homes();
    let t0 = Instant::now();
    let h = HomeRecords::new_at(log.clone(), t0);
    let Ok(HomeReply::Opened { incarnation: inc }) = h.apply_at(HomeOp::Open { txn: T }, t0) else {
        panic!("open")
    };
    let seal = HomeOp::Seal {
        txn: T,
        incarnation: inc,
        participants: vec![RangeId(3), RangeId(4)],
    };
    h.apply_at(seal, t0).unwrap();
    let vote = HomeOp::Vote {
        txn: T,
        incarnation: inc,
        participant: RangeId(3),
        prepared_at: Timestamp(8),
    };
    h.apply_at(vote, t0 + Duration::from_secs(2)).unwrap();
    let client = Recorder::default();
    // B3: Status polling does not restart the idle clock.
    h.apply_at(HomeOp::Status { txn: T }, t0 + Duration::from_secs(6))
        .unwrap();
    let report = h
        .recover_once(&client, cfg(), t0 + Duration::from_secs(6))
        .unwrap();
    assert_eq!(report, RecoveryReport::default(), "idle 4s < 5s");
    let report = h
        .recover_once(&client, cfg(), t0 + Duration::from_secs(8))
        .unwrap();
    assert_eq!(report.aborted, vec![T]);
    assert_eq!(report.forgotten, vec![T]);
    assert_eq!(
        *client.finalized.lock().unwrap(),
        vec![(RangeId(3), Outcome::Abort), (RangeId(4), Outcome::Abort)]
    );
    assert!(log.list().unwrap().is_empty());
}

#[test]
fn recovery_finishes_a_commit_and_keeps_the_record_while_a_participant_is_down() {
    let (log, _) = homes();
    let t0 = Instant::now();
    let h = HomeRecords::new_at(log.clone(), t0);
    let Ok(HomeReply::Opened { incarnation: inc }) = h.apply_at(HomeOp::Open { txn: T }, t0) else {
        panic!("open")
    };
    for op in [
        HomeOp::Seal {
            txn: T,
            incarnation: inc,
            participants: vec![RangeId(3), RangeId(4)],
        },
        HomeOp::Vote {
            txn: T,
            incarnation: inc,
            participant: RangeId(3),
            prepared_at: Timestamp(8),
        },
        HomeOp::Vote {
            txn: T,
            incarnation: inc,
            participant: RangeId(4),
            prepared_at: Timestamp(9),
        },
        HomeOp::Decide {
            txn: T,
            incarnation: inc,
            outcome: Outcome::Commit(Timestamp(10)),
        },
    ] {
        h.apply_at(op, t0).unwrap();
    }
    let client = Recorder::default();
    client.down.lock().unwrap().push(RangeId(4));
    let later = t0 + Duration::from_secs(60);
    let report = h.recover_once(&client, cfg(), later).unwrap();
    assert_eq!(report.unfinished, vec![T]);
    assert!(report.aborted.is_empty());
    assert_eq!(
        status(&h),
        HomeStatus::Decided(Outcome::Commit(Timestamp(10)))
    );
    client.down.lock().unwrap().clear();
    let report = h.recover_once(&client, cfg(), later).unwrap();
    assert_eq!(report.forgotten, vec![T]);
    assert!(
        client
            .finalized
            .lock()
            .unwrap()
            .iter()
            .all(|(_, o)| *o == Outcome::Commit(Timestamp(10)))
    );
    assert!(log.list().unwrap().is_empty());
}

/// A RangeClient whose only allowed call is `finalize`, which it records (ranges in `down`
/// refuse it).
#[derive(Default)]
pub(crate) struct Recorder {
    pub(crate) finalized: Mutex<Vec<(RangeId, Outcome)>>,
    pub(crate) down: Mutex<Vec<RangeId>>,
}
impl RangeClient for Recorder {
    fn descriptor_for(&self, _: &[u8]) -> Result<crate::range_api::RangeDescriptor, RangeError> {
        unreachable!()
    }
    fn invalidate(&self, _: &crate::range_api::RangeDescriptor) {}
    fn read(
        &self,
        _: crate::range_api::ReadRequest,
    ) -> Result<crate::range_api::ReadPage, RangeError> {
        unreachable!()
    }
    fn lock(
        &self,
        _: crate::range_api::LockRequest,
    ) -> Result<crate::range_api::LockGrant, RangeError> {
        unreachable!()
    }
    fn blockers(&self, _: RangeId, _: ClusterTxnId) -> Result<Vec<ClusterTxnId>, RangeError> {
        unreachable!()
    }
    fn cancel_wait(&self, _: RangeId, _: ClusterTxnId) -> Result<(), RangeError> {
        unreachable!()
    }
    fn release(&self, _: RangeId, _: ClusterTxnId) -> Result<(), RangeError> {
        unreachable!()
    }
    fn heartbeat(&self, _: RangeId, _: ClusterTxnId) -> Result<(), RangeError> {
        unreachable!()
    }
    fn commit(
        &self,
        _: RangeId,
        _: crate::range_api::Epoch,
        _: ClusterTxnId,
        _: Vec<crate::range_api::RangeWrite>,
        _: Option<u64>,
    ) -> Result<Timestamp, RangeError> {
        unreachable!()
    }
    fn prepare(
        &self,
        _: RangeId,
        _: crate::range_api::Epoch,
        _: ClusterTxnId,
        _: RangeId,
        _: Vec<crate::range_api::RangeWrite>,
    ) -> Result<Timestamp, RangeError> {
        unreachable!()
    }
    fn finalize(
        &self,
        range: RangeId,
        _: ClusterTxnId,
        outcome: Outcome,
    ) -> Result<(), RangeError> {
        if self.down.lock().unwrap().contains(&range) {
            return Err(RangeError::Unavailable("participant down".into()));
        }
        self.finalized.lock().unwrap().push((range, outcome));
        Ok(())
    }
    fn home(
        &self,
        _: RangeId,
        _: crate::range_api::Epoch,
        _: HomeOp,
    ) -> Result<HomeReply, RangeError> {
        unreachable!()
    }
    fn oracle(&self) -> &dyn crate::range_api::TsOracle {
        unreachable!()
    }
    fn lease(
        &self,
        _: crate::range_api::LeaseKind,
        _: u64,
        _: u64,
    ) -> Result<std::ops::Range<u64>, RangeError> {
        unreachable!()
    }
}
