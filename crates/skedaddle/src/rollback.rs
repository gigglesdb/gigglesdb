//! Fenced rollback using the cutover machine with its endpoints exchanged.
//!
//! `Components.source` is the current destination; `Components.destination`
//! is the old source. Use `cutover::pg::Source` to fence/drain the former and
//! `cutover::pg::Target` to restore the latter, initialized with
//! `pg::activate::DestinationFence::with_activation_state` and the ORIGINAL
//! source fence's saved state. SequenceSync and verification also run in reverse.
//! `post_activate` is the post-rollback hook. `Reverse::confirm` must confirm
//! reverse apply readiness (it must not install a new forward capture).
//!
//! Persist callbacks before acknowledging them. Replay Transfer intents and
//! checkpoints into `State.transfer`, and native Source/Destination/Drain records
//! into the swapped adapters, exactly as for cutover. Use a fresh transfer key,
//! retained across retries; the PG receipt namespace must not contain a different
//! transfer. Authority guards, hook/audit idempotency and breach detection have
//! the same contracts as cutover. A refused rollback stays fenced for operator
//! recovery; eligibility never reopens. After an ambiguous activation, resolve
//! its durable receipt before refusing: the old source may already be active.
//!
//! Finalize's manifest names ONLY this installation's objects. Its journal and
//! audit sink must be external to the removed `_skedaddle` schema. Keep the
//! authority guard and a stopped capture across retries; exclude concurrent
//! installers. Finalize never deletes journal or audit records.

use crate::{cutover, pg::reverse::Eligibility};
use serde::{Deserialize, Serialize};

pub mod apply;
pub mod destination;
pub mod driver;
pub mod finalize;
pub mod source;
pub use finalize::{Finalize, FinalizeRecord, FinalizeStep, Inventory, Object, ObjectKind, Side};

pub type Journal<'a> = dyn FnMut(&Record) -> Result<(), String> + 'a;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct State {
    pub transfer: cutover::State,
    pub refused: Option<crate::pg::reverse::Violation>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Record {
    Transfer(cutover::Record),
    Refused {
        state: State,
        reason: crate::pg::reverse::Violation,
    },
}

fn ensure(eligibility: &Eligibility) -> cutover::Result<()> {
    eligibility
        .ensure()
        .map_err(|e| cutover::Error::Component(e.to_string()))
}

impl State {
    pub fn new(key: cutover::Key) -> cutover::Result<Self> {
        Ok(Self {
            transfer: cutover::State::new(key)?,
            refused: None,
        })
    }

    /// Every transfer step uses cutover's intent-first callback, exact marker
    /// wait, frozen-boundary verifier, sequence synchronization and audit.
    pub fn execute_next<
        A: cutover::Authority,
        S: cutover::SourceFence,
        D: cutover::Destination,
        P: cutover::Progress,
        V: cutover::Verifier,
        Q: cutover::Sequences,
        R: cutover::Reverse,
        T: cutover::Status,
    >(
        &mut self,
        authority: &mut A,
        eligibility: &Eligibility,
        c: &mut cutover::Components<'_, S, D, P, V, Q, R, T>,
        journal: &mut Journal<'_>,
    ) -> cutover::Result<()> {
        let key = self.transfer.key.clone();
        // Use the same exclusive guard for eligibility, receipts and all effects.
        authority.run(&key, || {
            if self.transfer.next == cutover::Step::Complete {
                return Ok(());
            }
            let activated = c.destination.activated(&key)?.is_some();
            let eligible = if activated {
                Ok(())
            } else {
                ensure(eligibility)
            };
            let result = if !activated && self.refused.is_some() {
                Err(cutover::Error::Component(
                    "rollback eligibility ended".into(),
                ))
            } else if let Err(error) = eligible {
                Err(error)
            } else {
                let mut target = EligibleTarget {
                    inner: c.destination,
                    eligibility,
                };
                self.transfer.execute_next(
                    &mut Held,
                    &mut cutover::Components {
                        source: c.source,
                        destination: &mut target,
                        progress: c.progress,
                        verifier: c.verifier,
                        sequences: c.sequences,
                        reverse: c.reverse,
                        status: c.status,
                    },
                    &mut |r| journal(&Record::Transfer(r.clone())),
                )
            };
            // A lost restoration reply can make the initial probe stale.
            if c.destination.activated(&key)?.is_none()
                && let Some(reason) = self
                    .refused
                    .clone()
                    .or_else(|| eligibility.first_violation())
            {
                self.refused = Some(reason.clone());
                journal(&Record::Refused {
                    state: self.clone(),
                    reason,
                })
                .map_err(cutover::Error::Journal)?;
                return Err(cutover::Error::Component(
                    "rollback eligibility ended".into(),
                ));
            }
            result
        })
    }
}

// The outer call owns the real guard. Never acquire it recursively.
struct Held;
impl cutover::Authority for Held {
    fn run<T>(
        &mut self,
        _: &cutover::Key,
        run: impl FnOnce() -> cutover::Result<T>,
    ) -> cutover::Result<T> {
        run()
    }
}

struct EligibleTarget<'a, D> {
    inner: &'a mut D,
    eligibility: &'a Eligibility,
}
impl<D: cutover::Destination> cutover::Destination for EligibleTarget<'_, D> {
    fn activated(&mut self, key: &cutover::Key) -> cutover::Result<Option<u64>> {
        self.inner.activated(key)
    }
    fn prove_fenced(
        &mut self,
        key: &cutover::Key,
        journal: &mut cutover::Journal<'_>,
    ) -> cutover::Result<()> {
        self.inner.prove_fenced(key, journal)
    }
    fn readiness(
        &mut self,
        key: &cutover::Key,
        journal: &mut cutover::Journal<'_>,
    ) -> cutover::Result<()> {
        self.inner.readiness(key, journal)
    }
    fn activate(
        &mut self,
        key: &cutover::Key,
        marker: &cutover::Marker,
        journal: &mut cutover::Journal<'_>,
        authorize: &mut dyn FnMut() -> cutover::Result<()>,
    ) -> cutover::Result<u64> {
        self.inner.activate(key, marker, journal, &mut || {
            ensure(self.eligibility)?;
            authorize()
        })
    }
    fn post_activate(
        &mut self,
        key: &cutover::Key,
        journal: &mut cutover::Journal<'_>,
    ) -> cutover::Result<()> {
        self.inner.post_activate(key, journal)
    }
}
