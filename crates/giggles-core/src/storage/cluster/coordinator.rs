//! Cross-range commit coordinator and distributed deadlock detection (GD-0075 Phase 6, Track D).
//!
//! [`commit_cross_range`] runs the protocol in `J:/gigglesdb-work/r5/trackD/protocol.md` on the
//! gateway for one committing transaction: Open the home record (the first range written) and
//! Seal the participant set, prepare every participant in parallel and record each vote at home,
//! take a fresh commit timestamp after every vote, decide at home, and only then acknowledge.
//! Every home operation after Open carries the incarnation Open returned (R5-50m bug B2).
pub(in crate::storage) mod deadlock; // Track C: the gateway runs it; cluster_node configures it.
#[cfg(test)]
mod tests;

use crate::range_api::{
    ClusterTxnId, Epoch, HomeOp, HomeReply, Outcome, RangeClient, RangeError, RangeId, RangeWrite,
    Timestamp,
};

use std::time::{Duration, Instant};

use super::txn::FAILOVER_WINDOW;

/// Retry transient failures long enough for a replacement leader to become ready.
fn backoff_retry<T>(
    mut op: impl FnMut() -> Result<T, RangeError>,
    retry: impl Fn(&RangeError) -> bool,
) -> Result<T, RangeError> {
    let deadline = Instant::now() + FAILOVER_WINDOW;
    let mut delay = Duration::from_millis(5);
    loop {
        match op() {
            Err(e) if retry(&e) => {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Err(e);
                }
                std::thread::sleep(delay.min(remaining));
                if Instant::now() >= deadline {
                    return Err(e);
                }
                delay = (delay * 2).min(Duration::from_millis(200));
            }
            result => return result,
        }
    }
}

/// One range the transaction wrote, with its writes there.
pub(crate) struct Participant {
    pub range: RangeId,
    pub epoch: Epoch,
    pub writes: Vec<RangeWrite>,
    /// Generation of the lock grants the transaction holds there, as its caller recorded
    /// them (None = no lock taken there); passed to prepare, which refuses a lost one.
    pub lock_generation: Option<u64>,
}

/// A cross-range commit; the first participant is the home (first range written).
pub(crate) struct CommitPlan {
    pub txn: ClusterTxnId,
    pub participants: Vec<Participant>,
    /// Ranges where the transaction only holds locks.
    pub lock_only: Vec<RangeId>,
}

/// Commit failure: definitely aborted, or outcome unknown (recovery finishes it).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CommitError {
    Aborted(RangeError),
    Unknown(RangeError),
}

fn retryable(e: &RangeError) -> bool {
    matches!(
        e,
        RangeError::Unavailable(_) | RangeError::Unknown(_) | RangeError::NotLeader { .. }
    )
}

/// Sends the same home op until it is answered or the retry budget is spent. A stale-epoch
/// refusal for the home range (it split or moved) is retried at the epoch it carries.
fn home_op(
    client: &dyn RangeClient,
    home: RangeId,
    epoch: Epoch,
    op: HomeOp,
) -> Result<HomeReply, RangeError> {
    let mut epoch = epoch;
    let updated_epoch = std::cell::Cell::new(false);
    backoff_retry(
        || {
            updated_epoch.set(false);
            match client.home(home, epoch, op.clone()) {
                Err(RangeError::StaleEpoch { descriptor })
                    if descriptor.id == home && descriptor.epoch != epoch =>
                {
                    updated_epoch.set(true);
                    epoch = descriptor.epoch;
                    Err(RangeError::StaleEpoch { descriptor })
                }
                result => result,
            }
        },
        |e| retryable(e) || updated_epoch.get(),
    )
}

fn finalize(
    client: &dyn RangeClient,
    range: RangeId,
    txn: ClusterTxnId,
    outcome: &Outcome,
) -> bool {
    backoff_retry(|| client.finalize(range, txn, outcome.clone()), retryable).is_ok()
}

/// Only retry prepare refusals that guarantee no prepared record was applied.
fn prepare_with_retry(
    client: &dyn RangeClient,
    range: RangeId,
    epoch: Epoch,
    txn: ClusterTxnId,
    home: RangeId,
    writes: Vec<RangeWrite>,
    lock_generation: Option<u64>,
) -> Result<Timestamp, RangeError> {
    backoff_retry(
        || client.prepare(range, epoch, txn, home, writes.clone(), lock_generation),
        |e| matches!(e, RangeError::NotLeader { .. } | RangeError::Unavailable(_)),
    )
}

/// Releases T's unprepared locks on every participant and lock-only range. Called on every
/// exit path, including "outcome unknown": release never touches prepared state, so it is
/// always safe, and a later duplicate prepare is then refused (design.md Changes 2026-11-16 (d)).
fn release_all(client: &dyn RangeClient, plan: &CommitPlan) {
    for p in &plan.participants {
        let _ = client.release(p.range, plan.txn);
    }
    for &r in &plan.lock_only {
        let _ = client.release(r, plan.txn);
    }
}

/// Protocol step 6 (and the tail of step 5): finalize every participant (which also releases
/// its locks), release the remaining locks, then Forget the home record once every finalize
/// acked. A failure leaves the record for recovery. Runs synchronously after the decision for
/// now; the gateway moves it off the client's path when it wires this in.
fn finish(client: &dyn RangeClient, plan: &CommitPlan, incarnation: u64, outcome: &Outcome) {
    let home = &plan.participants[0];
    let mut all_acked = true;
    for p in &plan.participants {
        all_acked &= finalize(client, p.range, plan.txn, outcome);
    }
    release_all(client, plan);
    if all_acked {
        let forget = HomeOp::Forget {
            txn: plan.txn,
            incarnation,
        };
        let _ = home_op(client, home.range, home.epoch, forget);
    }
}

/// Protocol step 5, entered only while a Commit decision cannot exist: before Decide(Commit)
/// was sent, or after the home refused it (Conflict). Recovery never decides Commit, so the
/// client is told Aborted. Decide(Abort) at home, then finalize(Abort) and release everywhere;
/// if the decision cannot be written, prepared records stay for recovery (or a reader) to
/// abort.
fn abort(
    client: &dyn RangeClient,
    plan: &CommitPlan,
    incarnation: Option<u64>,
    cause: RangeError,
) -> Result<Timestamp, CommitError> {
    let home = &plan.participants[0];
    let Some(incarnation) = incarnation else {
        // Not opened: nothing was prepared. A record whose Open reply was lost is unsealed,
        // and recovery aborts and forgets it.
        release_all(client, plan);
        return Err(CommitError::Aborted(cause));
    };
    let decide = HomeOp::Decide {
        txn: plan.txn,
        incarnation,
        outcome: Outcome::Abort,
    };
    match home_op(client, home.range, home.epoch, decide) {
        Ok(_) => finish(client, plan, incarnation, &Outcome::Abort),
        // Only a Commit decision refuses Decide(Abort); this coordinator never had one
        // accepted, so this is a broken invariant: do not claim either outcome.
        Err(e @ RangeError::Conflict(_)) => {
            release_all(client, plan);
            return Err(CommitError::Unknown(e));
        }
        Err(_) => release_all(client, plan),
    }
    Err(CommitError::Aborted(cause))
}

/// Process-level crash hook for recovery tests (GD-0075 P8-0): when the environment variable
/// `GIGGLES_CORE_FAILPOINT` names `point`, the process aborts there, as a gateway that dies.
fn crash_point(point: &str) {
    static ARMED: std::sync::OnceLock<Option<String>> = std::sync::OnceLock::new();
    let armed = ARMED.get_or_init(|| std::env::var("GIGGLES_CORE_FAILPOINT").ok());
    if armed.as_deref() == Some(point) {
        eprintln!("failpoint crash: {point}");
        std::process::abort();
    }
}

/// Commits `plan` atomically across its participant ranges (GD-0075 Track D protocol,
/// coordinator steps 1-6). Returns the commit timestamp only once Commit is durable at home.
pub(crate) fn commit_cross_range(
    client: &dyn RangeClient,
    plan: CommitPlan,
) -> Result<Timestamp, CommitError> {
    let Some(home) = plan.participants.first() else {
        return Err(CommitError::Aborted(RangeError::Aborted(
            "no participants".into(),
        )));
    };
    let (h, he, txn) = (home.range, home.epoch, plan.txn);
    // 1. Open; every later home op carries its incarnation.
    let incarnation = match home_op(client, h, he, HomeOp::Open { txn }) {
        Ok(HomeReply::Opened { incarnation }) => incarnation,
        Ok(other) => {
            let e = RangeError::Corrupt(format!("home Open replied {other:?}"));
            return abort(client, &plan, None, e);
        }
        Err(e) => return abort(client, &plan, None, e),
    };
    // 2. Seal the full participant set before any prepare.
    let set: Vec<RangeId> = plan.participants.iter().map(|p| p.range).collect();
    let seal = HomeOp::Seal {
        txn,
        incarnation,
        participants: set,
    };
    if let Err(e) = home_op(client, h, he, seal) {
        return abort(client, &plan, Some(incarnation), e);
    }
    // 3. Prepare in parallel, each followed by its Vote at home.
    let results: Vec<Result<(), RangeError>> = std::thread::scope(|s| {
        let handles: Vec<_> = plan
            .participants
            .iter()
            .map(|p| {
                s.spawn(move || {
                    let prepared_at = prepare_with_retry(
                        client,
                        p.range,
                        p.epoch,
                        txn,
                        h,
                        p.writes.clone(),
                        p.lock_generation,
                    )?;
                    let vote = HomeOp::Vote {
                        txn,
                        incarnation,
                        participant: p.range,
                        prepared_at,
                    };
                    home_op(client, h, he, vote).map(|_| ())
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|t| {
                t.join()
                    .unwrap_or_else(|_| Err(RangeError::Unknown("prepare worker panicked".into())))
            })
            .collect()
    });
    if let Some(e) = results.into_iter().find_map(Result::err) {
        return abort(client, &plan, Some(incarnation), e);
    }
    crash_point("gateway.commit.prepared");
    // 4. Fresh c after every vote, so c > every p_i.
    let c = match client.oracle().next(1) {
        Ok(c) => c,
        Err(e) => return abort(client, &plan, Some(incarnation), e),
    };
    let decide = HomeOp::Decide {
        txn,
        incarnation,
        outcome: Outcome::Commit(c),
    };
    match home_op(client, h, he, decide) {
        Ok(HomeReply::Ok) => {
            // The decision is durable: the ack value is fixed here, before finishing.
            let acked = Ok(c);
            finish(client, &plan, incarnation, &Outcome::Commit(c));
            acked
        }
        // Any other reply does not prove a durable Commit.
        Ok(other) => {
            release_all(client, &plan);
            Err(CommitError::Unknown(RangeError::Corrupt(format!(
                "home Decide replied {other:?}"
            ))))
        }
        // Refused: the home decided Abort for this incarnation (recovery), so Commit was
        // never applied (an identical retry of an applied Commit returns Ok).
        Err(e @ RangeError::Conflict(_)) => abort(client, &plan, Some(incarnation), e),
        // Any other failure after Decide(Commit) was sent, retryable or not (B1, a stale
        // epoch, a forgotten or replaced record): an earlier attempt may have been applied,
        // so the outcome is unknown, never an abort.
        Err(e) => {
            release_all(client, &plan);
            Err(CommitError::Unknown(e))
        }
    }
}
