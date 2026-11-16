//! Home records of cross-range transactions (GD-0075 R5-50, R5-52, R5-54; Track D).
//!
//! The home range of a transaction (the first range it wrote) keeps one durable record per
//! transaction at `Meta/range/<home>/home/<txn BE8>`: the sealed participant set, one vote
//! (prepared timestamp) per participant and the decision. The decision rules are
//! `giggles_transactions::HomeRecord`'s (decide once, seal once, idempotent retries).
//!
//! Each record also stores its incarnation (R5-50m bug B2, design.md Changes 2026-11-16): the
//! `Open` that creates a record gives it an incarnation this range never gave that transaction
//! before, and `Seal`, `Vote`, `Decide` and `Forget` carry it. An operation for another
//! incarnation is treated as if no record exists, so a delayed duplicate `Open` that re-creates
//! a forgotten record cannot be sealed, voted, committed or forgotten by the old coordinator.
//!
//! The range engine (Track A) owns the key layout and the write path: it implements
//! [`HomeLog`] over its sequencer, so `put` returns only after the batch is durably applied,
//! and routes `RangeService::home` to [`HomeRecords::apply`]. The cross-range protocol these
//! records serve is in `storage/cluster/coordinator.rs`.
// Wired by the range engine (Track A) and the gateway later in R5; until then only tests use it.
#![cfg_attr(not(test), allow(dead_code))]

use crate::range_api::{
    ClusterTxnId, HomeOp, HomeReply, HomeStatus, Outcome, RangeClient, RangeError, RangeId,
    Timestamp,
};
use giggles_transactions::{Error as RuleError, HomeDecision, HomeRecord, Outcome as RuleOutcome};
use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

impl HomeDecision for Outcome {
    fn outcome(&self) -> RuleOutcome {
        match self {
            Outcome::Commit(_) => RuleOutcome::Commit,
            Outcome::Abort => RuleOutcome::Abort,
        }
    }
}

/// One transaction's home record: participants are ranges, a vote is a prepared timestamp.
pub(crate) type Record = HomeRecord<RangeId, Timestamp, (), Outcome>;

/// Most participants one transaction may seal (each is a range it wrote).
pub(crate) const MAX_PARTICIPANTS: usize = 4096;
/// Most participants a version-1 record may hold: part of the persisted format. Raising the
/// policy cap above it needs format version 2.
const V1_MAX_PARTICIPANTS: usize = 4096;
const _: () = assert!(MAX_PARTICIPANTS <= V1_MAX_PARTICIPANTS);
const VERSION: u8 = 1;
const SEALED: u8 = 1;
const NO_DECISION: u8 = 0;
const COMMIT: u8 = 1;
const ABORT: u8 = 2;

/// Encodes a home record and its incarnation (persisted format, version 1):
/// `[ver=1][incarnation BE8][flags u8: bit0 sealed][n BE4][range BE8]*n (sorted, unique)`
/// `[m BE4][(range BE8, prepared ts BE8)]*m (sorted by range)`
/// `[decision u8: 0 none, 1 commit, 2 abort][commit ts BE8, only for 1]`.
///
/// Version 1 is strict, like the range descriptor's: no extension block, and its acceptance
/// rules (n <= 4096, participants only when sealed, a commit ts at or above every vote) are
/// part of the format. Writers are stricter than the format (they require a commit ts above
/// every vote, rule O5), so a later `c = max(p)` stays readable by version-1 nodes. A new field or a looser rule is
/// version 2, written only after every node runs a build that decodes version 2.
pub(crate) fn encode_record(incarnation: u64, record: &Record) -> Vec<u8> {
    let n = record.participants().len();
    let m = record.votes().len();
    debug_assert!(n <= V1_MAX_PARTICIPANTS);
    let mut out = Vec::with_capacity(1 + 8 + 1 + 4 + 8 * n + 4 + 16 * m + 1 + 8);
    out.push(VERSION);
    out.extend_from_slice(&incarnation.to_be_bytes());
    out.push(if record.seal().is_some() { SEALED } else { 0 });
    out.extend_from_slice(&(n as u32).to_be_bytes());
    for range in record.participants() {
        out.extend_from_slice(&range.0.to_be_bytes());
    }
    out.extend_from_slice(&(m as u32).to_be_bytes());
    for (range, ts) in record.votes() {
        out.extend_from_slice(&range.0.to_be_bytes());
        out.extend_from_slice(&ts.0.to_be_bytes());
    }
    match record.decision() {
        None => out.push(NO_DECISION),
        Some(Outcome::Commit(ts)) => {
            out.push(COMMIT);
            out.extend_from_slice(&ts.0.to_be_bytes());
        }
        Some(Outcome::Abort) => out.push(ABORT),
    }
    debug_assert!(
        decode_record(&out).is_ok_and(|(i, r)| i == incarnation && r == *record),
        "home record does not decode to itself"
    );
    out
}

/// Decodes a version-1 home record into its incarnation and record. Rejects truncated or
/// trailing bytes, an unknown version, unknown flag bits or decision tags, unsorted or
/// duplicate ranges, participants without a seal, votes without a seal or from a
/// non-participant, a commit without every vote or below a vote, and more than 4096
/// participants.
pub(crate) fn decode_record(bytes: &[u8]) -> Result<(u64, Record), RangeError> {
    let corrupt = |what: &str| RangeError::Corrupt(format!("home record: {what}"));
    let mut r = Reader(bytes);
    if r.u8().ok_or_else(|| corrupt("truncated"))? != VERSION {
        return Err(corrupt("unknown version"));
    }
    let incarnation = r.u64().ok_or_else(|| corrupt("truncated"))?;
    let flags = r.u8().ok_or_else(|| corrupt("truncated"))?;
    if flags & !SEALED != 0 {
        return Err(corrupt("unknown flags"));
    }
    let n = r.u32().ok_or_else(|| corrupt("truncated"))? as usize;
    if n > V1_MAX_PARTICIPANTS {
        return Err(corrupt("too many participants"));
    }
    let mut participants = BTreeSet::new();
    let mut last = None;
    for _ in 0..n {
        let range = RangeId(r.u64().ok_or_else(|| corrupt("truncated"))?);
        if last.is_some_and(|l| l >= range) {
            return Err(corrupt("participants not sorted and unique"));
        }
        last = Some(range);
        participants.insert(range);
    }
    let m = r.u32().ok_or_else(|| corrupt("truncated"))? as usize;
    if m > n {
        return Err(corrupt("more votes than participants"));
    }
    let mut votes = Vec::with_capacity(m);
    let mut last = None;
    for _ in 0..m {
        let range = RangeId(r.u64().ok_or_else(|| corrupt("truncated"))?);
        let ts = Timestamp(r.u64().ok_or_else(|| corrupt("truncated"))?);
        if last.is_some_and(|l| l >= range) {
            return Err(corrupt("votes not sorted and unique"));
        }
        last = Some(range);
        votes.push((range, ts));
    }
    let decision = match r.u8().ok_or_else(|| corrupt("truncated"))? {
        NO_DECISION => None,
        COMMIT => Some(Outcome::Commit(Timestamp(
            r.u64().ok_or_else(|| corrupt("truncated"))?,
        ))),
        ABORT => Some(Outcome::Abort),
        _ => return Err(corrupt("unknown decision")),
    };
    if !r.0.is_empty() {
        return Err(corrupt("trailing bytes"));
    }
    // Rebuild through the rule methods, so a decoded record is one the rules can produce.
    // HomeRecords only ever seals a whole set: participants exist only once sealed.
    let mut record = Record::new();
    if flags & SEALED != 0 {
        record
            .seal_set(participants, V1_MAX_PARTICIPANTS, |_| Ok(()))
            .map_err(|_| corrupt("invalid seal"))?;
    } else if !participants.is_empty() {
        return Err(corrupt("participants without a seal"));
    }
    if let Some(Outcome::Commit(c)) = &decision
        && votes.iter().any(|(_, p)| p > c)
    {
        return Err(corrupt("commit ts below a vote"));
    }
    for (range, ts) in votes {
        record
            .vote(range, ts, |_| true, |_| Ok(()))
            .map_err(|_| corrupt("invalid vote"))?;
    }
    if let Some(d) = decision {
        let outcome = d.outcome();
        record
            .decide(outcome, || d)
            .map_err(|_| corrupt("invalid decision"))?;
    }
    Ok((incarnation, record))
}

struct Reader<'a>(&'a [u8]);
impl Reader<'_> {
    fn take<const N: usize>(&mut self) -> Option<[u8; N]> {
        let (head, rest) = self.0.split_first_chunk::<N>()?;
        self.0 = rest;
        Some(*head)
    }
    fn u8(&mut self) -> Option<u8> {
        self.take::<1>().map(|b| b[0])
    }
    fn u32(&mut self) -> Option<u32> {
        self.take().map(u32::from_be_bytes)
    }
    fn u64(&mut self) -> Option<u64> {
        self.take().map(u64::from_be_bytes)
    }
}

/// Durable home-record storage of one range, implemented by the range engine over
/// `Meta/range/<id>/home/<txn BE8>`.
pub(crate) trait HomeLog: Send + Sync {
    /// Linearizable: served only while this node is the range's leader (after its open read
    /// barrier, under a read index or leader lease), never from a deposed leader's state. A
    /// reader treats an absent record as presumed abort, so a stale `None` would be unsafe.
    fn get(&self, txn: ClusterTxnId) -> Result<Option<Vec<u8>>, RangeError>;
    /// Writes (`Some`) or deletes (`None`) the record; returns only once durably applied.
    /// Callers read with `get` and then `put` under one transaction's serialization, so `put`
    /// must fail (`NotLeader`) if this node's leadership changed since that `get`: it is
    /// proposed in the same term, never forwarded or re-proposed by a later leader.
    fn put(&self, txn: ClusterTxnId, value: Option<Vec<u8>>) -> Result<(), RangeError>;
    /// Every transaction with a record, for recovery.
    fn list(&self) -> Result<Vec<ClusterTxnId>, RangeError>;
    /// An incarnation for a new record of `txn`: strictly greater than the incarnation of every
    /// record of `txn` this range stored before, on any leader. Called under `txn`'s
    /// serialization, after the previous record was written or deleted. The range's applied
    /// log index satisfies this: a record's incarnation was read before its own write applied,
    /// at a higher index, and a new leader's applied index covers every applied write.
    fn fresh_incarnation(&self, txn: ClusterTxnId) -> Result<u64, RangeError>;
}

/// Bounds of the recovery worker (R5-52).
#[derive(Clone, Copy, Debug)]
pub(crate) struct RecoveryConfig {
    /// An undecided record idle this long is decided Abort.
    pub recover_after: Duration,
    /// A decided record idle this long is finalized on every participant and forgotten.
    pub finish_after: Duration,
}
impl Default for RecoveryConfig {
    fn default() -> Self {
        Self {
            recover_after: Duration::from_secs(30),
            finish_after: Duration::from_secs(10),
        }
    }
}

/// What one recovery pass did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct RecoveryReport {
    pub aborted: Vec<ClusterTxnId>,
    pub forgotten: Vec<ClusterTxnId>,
    pub unfinished: Vec<ClusterTxnId>,
}

const STRIPES: usize = 64;

/// The home records of one range on its current leader. Operations on one transaction are
/// serialized; different transactions proceed in parallel.
pub(crate) struct HomeRecords {
    log: Arc<dyn HomeLog>,
    stripes: Vec<Mutex<()>>,
    /// Last operation per transaction on this leader (in memory; recovery's idle clock).
    activity: Mutex<HashMap<ClusterTxnId, Instant>>,
    opened: Instant,
}

impl HomeRecords {
    /// Opens on a new leader, after its read barrier. Idle clocks restart here.
    pub(crate) fn new(log: Arc<dyn HomeLog>) -> Self {
        Self::new_at(log, Instant::now())
    }

    pub(crate) fn new_at(log: Arc<dyn HomeLog>, opened: Instant) -> Self {
        Self {
            log,
            stripes: (0..STRIPES).map(|_| Mutex::new(())).collect(),
            activity: Mutex::new(HashMap::new()),
            opened,
        }
    }

    fn stripe(&self, txn: ClusterTxnId) -> &Mutex<()> {
        let mixed = txn.0 ^ (txn.0 >> 29) ^ (txn.0 >> 48);
        &self.stripes[(mixed % STRIPES as u64) as usize]
    }

    fn load(&self, txn: ClusterTxnId) -> Result<Option<(u64, Record)>, RangeError> {
        self.log
            .get(txn)
            .and_then(|bytes| bytes.map(|b| decode_record(&b)).transpose())
    }

    fn activity(&self) -> MutexGuard<'_, HashMap<ClusterTxnId, Instant>> {
        // A poisoned map is still valid (inserts and removes do not panic midway); losing it
        // would make every record look idle since the leader opened.
        self.activity.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn touch(&self, txn: ClusterTxnId, now: Instant) {
        self.activity().insert(txn, now);
    }

    fn idle_since(&self, txn: ClusterTxnId) -> Instant {
        self.activity().get(&txn).copied().unwrap_or(self.opened)
    }

    /// Applies one home operation; a mutation is durable before this returns `Ok`.
    pub(crate) fn apply(&self, op: HomeOp) -> Result<HomeReply, RangeError> {
        self.apply_at(op, Instant::now())
    }

    pub(crate) fn apply_at(&self, op: HomeOp, now: Instant) -> Result<HomeReply, RangeError> {
        let (txn, incarnation) = match &op {
            HomeOp::Open { txn } | HomeOp::Status { txn } => (*txn, None),
            HomeOp::Seal {
                txn, incarnation, ..
            }
            | HomeOp::Vote {
                txn, incarnation, ..
            }
            | HomeOp::Decide {
                txn, incarnation, ..
            }
            | HomeOp::Forget { txn, incarnation } => (*txn, Some(*incarnation)),
        };
        // The stripe guards no data, so a poisoned one is still a valid lock.
        let _serial = self
            .stripe(txn)
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let stored = self.load(txn)?;
        if let HomeOp::Status { .. } = op {
            // Status never restarts the idle clock (R5-50m B3).
            return Ok(HomeReply::Status(status(stored.as_ref().map(|(_, r)| r))));
        }
        // An operation for another incarnation sees no record (F2).
        let current = match (stored, incarnation) {
            (Some((have, _)), Some(want)) if have != want => None,
            (stored, _) => stored,
        };
        let Some((inc, mut record)) = current else {
            return match op {
                HomeOp::Open { .. } => {
                    let incarnation = self.log.fresh_incarnation(txn)?;
                    self.log
                        .put(txn, Some(encode_record(incarnation, &Record::new())))?;
                    self.touch(txn, now);
                    Ok(HomeReply::Opened { incarnation })
                }
                // Never opened, forgotten after every participant finalized, or another
                // incarnation's record.
                HomeOp::Forget { .. }
                | HomeOp::Decide {
                    outcome: Outcome::Abort,
                    ..
                } => Ok(HomeReply::Ok),
                // B1: the record may have been forgotten after a Commit; never say Aborted.
                HomeOp::Decide {
                    outcome: Outcome::Commit(_),
                    ..
                } => Err(RangeError::Unknown(
                    "home record absent: never opened, already forgotten or another incarnation"
                        .into(),
                )),
                _ => Err(RangeError::Aborted("no home record".into())),
            };
        };
        if let HomeOp::Forget { .. } = op {
            if record.decision().is_none() {
                return Err(RangeError::Conflict(
                    "home record: forget before a decision".into(),
                ));
            }
            self.log.put(txn, None)?;
            self.activity().remove(&txn);
            return Ok(HomeReply::Ok);
        }
        self.touch(txn, now);
        match op {
            HomeOp::Open { .. } => return Ok(HomeReply::Opened { incarnation: inc }),
            HomeOp::Seal { participants, .. } => {
                // A coordinator that seals after recovery aborted is told so before it prepares.
                if let Some(Outcome::Abort) = record.decision() {
                    return Err(RangeError::Aborted("home record decided Abort".into()));
                }
                let set: BTreeSet<RangeId> = participants.iter().copied().collect();
                if set.len() != participants.len() {
                    return Err(RangeError::Conflict(
                        "home record: duplicate participant".into(),
                    ));
                }
                let sealed = record.seal().is_some();
                record
                    .seal_set(set, MAX_PARTICIPANTS, |_| Ok(()))
                    .map_err(rule_error)?;
                if sealed {
                    return Ok(HomeReply::Ok);
                }
            }
            HomeOp::Vote {
                participant,
                prepared_at,
                ..
            } => {
                if !record
                    .vote(participant, prepared_at, |_| true, |_| Ok(()))
                    .map_err(rule_error)?
                {
                    return Ok(HomeReply::Ok);
                }
            }
            HomeOp::Decide { outcome, .. } => {
                if let Some(d) = record.decision() {
                    return if *d == outcome {
                        Ok(HomeReply::Ok)
                    } else {
                        Err(RangeError::Conflict(format!(
                            "home record: already decided {d:?}"
                        )))
                    };
                }
                // A4: a commit ts is a fresh oracle ts taken after every vote.
                if let Outcome::Commit(c) = &outcome
                    && record.votes().values().any(|p| p >= c)
                {
                    return Err(RangeError::Conflict(
                        "home record: commit ts not above every vote".into(),
                    ));
                }
                let rule = outcome.outcome();
                record.decide(rule, || outcome).map_err(rule_error)?;
            }
            HomeOp::Status { .. } | HomeOp::Forget { .. } => unreachable!("handled above"),
        }
        self.log.put(txn, Some(encode_record(inc, &record)))?;
        Ok(HomeReply::Ok)
    }

    /// One recovery pass (R5-52, R5-54) on this range's leader: decide Abort for undecided
    /// records idle past `recover_after`; finalize decided records idle past `finish_after`
    /// (or just decided here) on every sealed participant, and forget them once all acked.
    /// Never decides Commit.
    pub(crate) fn recover_once(
        &self,
        client: &dyn RangeClient,
        cfg: RecoveryConfig,
        now: Instant,
    ) -> Result<RecoveryReport, RangeError> {
        let mut report = RecoveryReport::default();
        for txn in self.log.list()? {
            // One record that fails (corrupt bytes, a failed write) never stops the others.
            if self
                .recover_txn(txn, client, cfg, now, &mut report)
                .is_err()
            {
                report.unfinished.push(txn);
            }
        }
        Ok(report)
    }
}

impl HomeRecords {
    fn recover_txn(
        &self,
        txn: ClusterTxnId,
        client: &dyn RangeClient,
        cfg: RecoveryConfig,
        now: Instant,
        report: &mut RecoveryReport,
    ) -> Result<(), RangeError> {
        let Some((inc, record)) = self.load(txn)? else {
            return Ok(());
        };
        let idle = |since: Instant| now.saturating_duration_since(since);
        let (outcome, record) = match record.decision() {
            Some(d) if idle(self.idle_since(txn)) >= cfg.finish_after => (d.clone(), record),
            Some(_) => return Ok(()),
            None if idle(self.idle_since(txn)) >= cfg.recover_after => {
                match self.abort_if_idle(txn, inc, cfg.recover_after, now)? {
                    Some((outcome, record)) => {
                        report.aborted.push(txn);
                        (outcome, record)
                    }
                    None => return Ok(()),
                }
            }
            None => return Ok(()),
        };
        let participants: Vec<RangeId> = if record.seal().is_some() {
            record.participants().iter().copied().collect()
        } else {
            Vec::new()
        };
        let all_acked = participants
            .iter()
            .all(|&range| client.finalize(range, txn, outcome.clone()).is_ok());
        if all_acked {
            let forget = HomeOp::Forget {
                txn,
                incarnation: inc,
            };
            self.apply_at(forget, now)?;
            report.forgotten.push(txn);
        } else {
            report.unfinished.push(txn);
        }
        Ok(())
    }

    /// Under txn's serialization: if incarnation `inc` is still undecided and still idle past
    /// `recover_after`, decide Abort. Returns that decision and the record (sealed participants
    /// as now stored), or None if it is gone, replaced, decided meanwhile or no longer idle.
    fn abort_if_idle(
        &self,
        txn: ClusterTxnId,
        inc: u64,
        recover_after: Duration,
        now: Instant,
    ) -> Result<Option<(Outcome, Record)>, RangeError> {
        // The stripe guards no data, so a poisoned one is still a valid lock.
        let _serial = self
            .stripe(txn)
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let Some((i, mut record)) = self.load(txn)? else {
            return Ok(None);
        };
        if i != inc {
            return Ok(None);
        }
        // Decided meanwhile (the coordinator's Decide won the race): leave it to a later pass
        // after `finish_after`, so the coordinator's retries still find the record.
        if record.decision().is_some() {
            return Ok(None);
        }
        if now.saturating_duration_since(self.idle_since(txn)) < recover_after {
            return Ok(None);
        }
        record
            .decide(RuleOutcome::Abort, || Outcome::Abort)
            .map_err(rule_error)?;
        self.log.put(txn, Some(encode_record(inc, &record)))?;
        Ok(Some((Outcome::Abort, record)))
    }
}

fn status(record: Option<&Record>) -> HomeStatus {
    match record {
        None => HomeStatus::Unknown,
        Some(r) => match r.decision() {
            Some(d) => HomeStatus::Decided(d.clone()),
            None => HomeStatus::Open {
                sealed: r.seal().map(|_| r.participants().iter().copied().collect()),
                votes: r.votes().iter().map(|(&p, &ts)| (p, ts)).collect(),
            },
        },
    }
}

fn rule_error(e: RuleError) -> RangeError {
    match e {
        RuleError::WrongPhase => RangeError::Aborted("home record already decided".into()),
        RuleError::Capacity => RangeError::Conflict("home record: too many participants".into()),
        other => RangeError::Conflict(format!("home record: {other}")),
    }
}

#[cfg(test)]
pub(crate) mod tests;
