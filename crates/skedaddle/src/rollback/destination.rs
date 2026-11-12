//! Destination-only rollback preparation. Never publishes ReverseApply intent.
//!
//! Call under the same exclusive authority guard as owner promotion. Native
//! records must be replayed in order into `Source.state`/`Source.drained` before
//! recovery (see `replay_native`). A failed call is retried with journal state,
//! never an optimistic in-memory checkpoint. Hooks must be idempotent by request.
use crate::{
    coordinator::{
        Materialized, State,
        rollback::{Record, Step},
    },
    cutover::{self, Authority, Key, Marker, SourceFence},
    hooks::{self, HookAudit, HookPhase, OperatorHook},
    pg::fence,
    security::Redactor,
};

/// Atomic, authenticated coordinator publication. Return only after durability
/// and local materialization. `native` records belong to this request alone.
pub trait Journal {
    fn state(&self) -> Materialized;
    fn publish(&mut self, record: Record) -> cutover::Result<()>;
    fn native(&mut self, record: &cutover::Record) -> Result<(), String>;
}

/// Adapter for reverse_workers: acknowledge ONLY the exact marker durably in
/// the reverse buffer, then join capture at that boundary. Never use WAL tip as
/// proof. Stop/restart must be idempotent, including an ambiguous lost reply.
pub trait Capture {
    fn captured(&mut self, marker: &Marker) -> cutover::Result<Option<Marker>>;
    fn stop_at(&mut self, marker: &Marker) -> cutover::Result<()>;
    fn restart(&mut self, key: &Key) -> cutover::Result<()>;
}

pub trait PreHook {
    fn run(&mut self, key: &Key) -> cutover::Result<()>;
}

pub struct Hooks<'a, A> {
    pub hooks: &'a [OperatorHook],
    pub redactor: &'a Redactor,
    pub audit: A,
}
impl<A: FnMut(&HookAudit)> PreHook for Hooks<'_, A> {
    fn run(&mut self, _: &Key) -> cutover::Result<()> {
        hooks::run_phase_then(
            HookPhase::PreRollback,
            self.hooks,
            self.redactor,
            &mut self.audit,
            || (),
        )
        .map_err(|e| cutover::Error::Component(e.to_string()))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    Progress,
    ReadyForReverseApply,
    Restored,
}

/// Injective, side- and request-scoped namespace for native fence and marker
/// receipts. Both endpoints may be the same database; forward rows stay intact.
/// The authority guard uses the ORIGINAL migration key, not this storage key.
pub fn destination_key(key: &Key, request: &str) -> Key {
    Key {
        migration: serde_json::json!(["rollback-destination-v1", key.migration, request])
            .to_string(),
        cutover: request.into(),
        ..key.clone()
    }
}

/// Rehydrate a swapped PG Source from this request's authenticated native log.
/// In particular Save is required even when the crash preceded its PG insert.
pub fn replay_native<C, H>(
    source: &mut cutover::pg::Source<C, H>,
    key: &Key,
    records: &[cutover::Record],
) -> cutover::Result<()> {
    for record in records {
        match record {
            cutover::Record::Source(
                fence::Intent::Step(state)
                | fence::Intent::Save(state)
                | fence::Intent::Unfence(state),
            ) => {
                if state.migration != key.migration || state.config.epoch > key.epoch {
                    return Err(cutover::Error::Invalid(
                        "destination fence identity differs",
                    ));
                }
                source.state = *state.clone();
            }
            cutover::Record::Drain(counts) => source.drained = counts.clone(),
            _ => {
                return Err(cutover::Error::Invalid(
                    "unexpected destination native record",
                ));
            }
        }
    }
    Ok(())
}

/// Execute at most one coordinator step. `now_ms` uses the admission clock.
/// Transient errors leave intents intact: callers retry with bounded backoff.
/// No deadline check can authorize restoration after durable ReverseApply intent.
pub fn execute_next<A: Authority, S: SourceFence, C: Capture, H: PreHook, J: Journal>(
    key: &Key,
    now_ms: u64,
    authority: &mut A,
    source: &mut S,
    capture: &mut C,
    hook: &mut H,
    journal: &mut J,
) -> cutover::Result<Outcome> {
    cutover::State::new(key.clone())?; // Validate identities before acquiring authority.
    authority.run(key, || {
        let state = journal.state();
        let cp = state
            .rollback
            .as_ref()
            .ok_or(cutover::Error::Invalid("rollback evidence missing"))?;
        if state.epoch != key.epoch as u64
            || state.owner != key.coordinator
            || state.rollback_eligibility_ended.is_some()
        {
            return Err(cutover::Error::LostOwnership);
        }
        if state.lifecycle.state == State::RollbackWindow && cp.next == Step::Aborted {
            return Ok(Outcome::Restored);
        }
        if state.lifecycle.state != State::RollingBack || cp.point_of_no_return {
            return Err(cutover::Error::Invalid(
                "destination preparation unavailable",
            ));
        }
        let step = if now_ms >= cp.deadline_at_ms && cp.next != Step::RestoreDestination {
            journal.publish(Record::DeadlineAbort)?;
            Step::RestoreDestination
        } else {
            cp.next
        };
        if step == Step::ReverseApply {
            return Ok(Outcome::ReadyForReverseApply);
        }
        if !matches!(
            step,
            Step::PreHook
                | Step::FenceDestination
                | Step::Drain
                | Step::FinalMarker
                | Step::RestoreDestination
        ) {
            return Err(cutover::Error::Invalid("unexpected destination step"));
        }
        let native_key = destination_key(key, &cp.request);
        journal.publish(Record::Intent { step })?;
        let mut marker_lsn = None;
        match step {
            Step::PreHook => hook.run(&native_key)?,
            Step::FenceDestination => {
                source.fence(&native_key, &mut |r| journal.native(r))?;
            }
            // Source.fence already drains application sessions. Emit the marker
            // in FinalMarker so its stable receipt also handles a lost reply.
            Step::Drain => {}
            Step::FinalMarker => {
                let marker = source.final_marker(&native_key)?;
                if marker.cutover != cp.request
                    || marker.epoch <= 0
                    || marker.epoch > key.epoch
                    || marker.lsn == 0
                {
                    return Err(cutover::Error::Invalid(
                        "destination marker identity differs",
                    ));
                }
                if capture.captured(&marker)?.as_ref() != Some(&marker) {
                    return Err(cutover::Error::Pending);
                }
                capture.stop_at(&marker)?;
                marker_lsn = Some(marker.lsn);
            }
            Step::RestoreDestination => {
                source.unfence(&native_key, &mut |r| journal.native(r))?;
                capture.restart(&native_key)?;
            }
            _ => unreachable!(),
        }
        journal.publish(Record::Checkpoint {
            step,
            final_marker: marker_lsn,
        })?;
        Ok(if step == Step::RestoreDestination {
            Outcome::Restored
        } else {
            Outcome::Progress
        })
    })
}

#[cfg(test)]
mod tests;
