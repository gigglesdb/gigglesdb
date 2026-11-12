//! Resumable fenced cutover. Persist every callback before returning success.
//!
//! Replay the latest `Checkpoint` (or `Intent.state`), plus the latest native
//! source/destination snapshots. Effects, hooks, audit sinks and reverse setup
//! must be idempotent under `Key`. An authority implementation must serialize
//! *all* effects for a migration across coordinators, including abort; a mere
//! ownership check before an effect is insufficient. P2 journal wiring is left
//! to the caller. Do not derive readiness from a cached UI/status report.

use serde::{Deserialize, Serialize};
use std::fmt;

pub mod pg;

pub type Result<T> = std::result::Result<T, Error>;
pub type Journal<'a> = dyn FnMut(&Record) -> std::result::Result<(), String> + 'a;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Key {
    pub migration: String,
    /// Stable cutover identity, retained across ownership/epoch changes.
    pub cutover: String,
    pub epoch: i64,
    pub coordinator: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Step {
    Readiness,
    Intent,
    Fence,
    FinalMarker,
    Boundary,
    Sequences,
    DestinationReadiness,
    Reverse,
    Activate,
    PostActivate,
    Audit,
    Complete,
    Abort,
    Aborted,
}
impl Step {
    fn next(self) -> Self {
        match self {
            Self::Readiness => Self::Intent,
            Self::Intent => Self::Fence,
            Self::Fence => Self::FinalMarker,
            Self::FinalMarker => Self::Boundary,
            Self::Boundary => Self::Sequences,
            Self::Sequences => Self::DestinationReadiness,
            Self::DestinationReadiness => Self::Reverse,
            Self::Reverse => Self::Activate,
            Self::Activate => Self::PostActivate,
            Self::PostActivate => Self::Audit,
            Self::Audit | Self::Complete => Self::Complete,
            Self::Abort | Self::Aborted => Self::Aborted,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Marker {
    pub cutover: String,
    pub epoch: i64,
    pub lsn: u64,
}
impl Marker {
    pub fn payload(&self) -> String {
        serde_json::json!({"final": self.cutover, "epoch": self.epoch}).to_string()
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DrainCounts {
    pub terminated: u64,
    /// A crash interrupted termination before its observed count was durable.
    pub termination_outcome_unknown: bool,
    pub remaining_sessions: u64,
    pub prepared_transactions: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Coverage {
    pub total: u64,
    pub verified: u64,
    pub busy: u64,
    pub mismatched: u64,
    pub excluded: u64,
}
impl From<crate::verify::Coverage> for Coverage {
    fn from(c: crate::verify::Coverage) -> Self {
        Self {
            total: c.total,
            verified: c.verified,
            busy: c.busy,
            mismatched: c.mismatched,
            excluded: c.excluded,
        }
    }
}
impl Coverage {
    fn complete(&self) -> bool {
        self.total == self.verified && self.busy == 0 && self.mismatched == 0 && self.excluded == 0
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Audit {
    pub fence_start_ms: u64,
    pub fence_end_ms: u64,
    pub drained: DrainCounts,
    pub final_lsn: u64,
    pub coverage: Coverage,
    pub activated_at_ms: u64,
    pub write_pause_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct State {
    pub key: Key,
    pub next: Step,
    pub fence_start_ms: Option<u64>,
    pub fence_end_ms: Option<u64>,
    pub drained: Option<DrainCounts>,
    pub marker: Option<Marker>,
    pub coverage: Option<Coverage>,
    pub activated_at_ms: Option<u64>,
    pub audit: Option<Audit>,
}
impl State {
    pub fn new(key: Key) -> Result<Self> {
        if key.epoch < 1
            || key.migration.is_empty()
            || key.cutover.is_empty()
            || key.coordinator.is_empty()
        {
            return Err(Error::Invalid(
                "positive epoch and nonempty identities required",
            ));
        }
        Ok(Self {
            key,
            next: Step::Readiness,
            fence_start_ms: None,
            fence_end_ms: None,
            drained: None,
            marker: None,
            coverage: None,
            activated_at_ms: None,
            audit: None,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Record {
    Intent { step: Step, state: State },
    Checkpoint(State),
    Source(crate::pg::fence::Intent),
    Destination(crate::pg::activate::Intent),
    Drain(DrainCounts),
}

#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    ReverseProof(crate::coordinator::reverse::ProofError),
    Invalid(&'static str),
    Component(String),
    Journal(String),
    LostOwnership,
    Pending,
    Verification,
    BreachAborted,
    FenceBreached(String),
    /// Activation may already have committed even when its reply was lost.
    UseRollback,
    /// Legacy receipt lacks authenticated owner/destination; operator action required.
    LegacyActivationReceipt,
    ActivationReceiptMismatch,
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReverseProof(error) => error.fmt(f),
            Self::LegacyActivationReceipt => f.write_str("legacy activation receipt: operator action required; inspect destination activation before recovery"),
            _ => write!(f, "cutover: {self:?}"),
        }
    }
}
impl std::error::Error for Error {}

/// Hold a non-expiring exclusive guard until `run` returns. Reject stale epochs
/// and different coordinators in the same epoch, even if their step is a retry.
/// Promotion must share this guard. Never implement this as check-then-release.
pub trait Authority {
    fn run<T>(&mut self, key: &Key, run: impl FnOnce() -> Result<T>) -> Result<T>;
}
pub trait Status {
    fn readiness(&mut self, key: &Key) -> Result<()>;
    /// Capture's breach detector must include committed changes after drain,
    /// including those observed while waiting. Errors must fail closed.
    fn breached(&mut self, key: &Key) -> Result<bool>;
    /// Persistent milliseconds from the SAME clock used by the destination
    /// activation receipt (for PG, query the destination clock). Reversal fails closed.
    fn now_ms(&mut self) -> Result<u64>;
    fn audit(&mut self, key: &Key, audit: &Audit) -> Result<()>;
}
pub trait SourceFence {
    fn fence(&mut self, key: &Key, journal: &mut Journal<'_>) -> Result<DrainCounts>;
    /// Emit transactionally and retain the original F under the stable key.
    fn final_marker(&mut self, key: &Key) -> Result<Marker>;
    fn unfence(&mut self, key: &Key, journal: &mut Journal<'_>) -> Result<()>;
}
pub trait Destination {
    /// Durable activation receipt, atomic with restoring application access.
    /// Must reject a different cutover, rather than returning "not activated".
    fn activated(&mut self, key: &Key) -> Result<Option<u64>>;
    /// Fresh proof of NOLOGIN, no effective write grants and drained sessions.
    fn prove_fenced(&mut self, key: &Key, journal: &mut Journal<'_>) -> Result<()>;
    fn readiness(&mut self, key: &Key, journal: &mut Journal<'_>) -> Result<()>;
    /// Invoke `authorize` immediately before the transaction restoring access.
    fn activate(
        &mut self,
        key: &Key,
        marker: &Marker,
        journal: &mut Journal<'_>,
        authorize: &mut dyn FnMut() -> Result<()>,
    ) -> Result<u64>;
    fn post_activate(&mut self, key: &Key, journal: &mut Journal<'_>) -> Result<()>;
}
pub trait Progress {
    /// Exact marker identity AND F, never a high-water LSN comparison.
    fn buffered(&mut self, marker: &Marker) -> Result<Option<Marker>>;
    /// Must attest that the marker itself was applied in stream order.
    fn applied(&mut self, marker: &Marker) -> Result<Option<Marker>>;
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifiedBoundary {
    pub marker: Marker,
    pub coverage: Coverage,
}
pub trait Verifier {
    /// Verify at this frozen boundary, returning its identity and row coverage.
    fn boundary(&mut self, marker: &Marker) -> Result<VerifiedBoundary>;
}
pub trait Sequences {
    fn sync(&mut self, marker: &Marker) -> Result<()>;
}
pub trait Reverse {
    fn confirm(&mut self, key: &Key, marker: &Marker) -> Result<()>;
}

pub struct Components<'a, S, D, P, V, Q, R, T> {
    pub source: &'a mut S,
    pub destination: &'a mut D,
    pub progress: &'a mut P,
    pub verifier: &'a mut V,
    pub sequences: &'a mut Q,
    pub reverse: &'a mut R,
    pub status: &'a mut T,
}
impl State {
    pub fn execute_next<
        A: Authority,
        S: SourceFence,
        D: Destination,
        P: Progress,
        V: Verifier,
        Q: Sequences,
        R: Reverse,
        T: Status,
    >(
        &mut self,
        authority: &mut A,
        components: &mut Components<'_, S, D, P, V, Q, R, T>,
        journal: &mut Journal<'_>,
    ) -> Result<()> {
        let key = self.key.clone();
        authority.run(&key, || match self.execute_locked(components, journal) {
            Err(Error::FenceBreached(_)) => {
                self.abort_locked(components.source, components.destination, journal)?;
                Err(Error::BreachAborted)
            }
            outcome => outcome,
        })
    }

    fn execute_locked<
        S: SourceFence,
        D: Destination,
        P: Progress,
        V: Verifier,
        Q: Sequences,
        R: Reverse,
        T: Status,
    >(
        &mut self,
        c: &mut Components<'_, S, D, P, V, Q, R, T>,
        journal: &mut Journal<'_>,
    ) -> Result<()> {
        if matches!(self.next, Step::Complete | Step::Aborted) {
            return Ok(());
        }
        if self.next == Step::Abort {
            return self.abort_locked(c.source, c.destination, journal);
        }
        // Detect ambiguous activation BEFORE checking breaches or considering abort.
        let activated = c.destination.activated(&self.key)?;
        if activated.is_some()
            && !matches!(self.next, Step::Activate | Step::PostActivate | Step::Audit)
        {
            return Err(Error::UseRollback);
        }
        if activated.is_none() && self.fence_start_ms.is_some() && c.status.breached(&self.key)? {
            self.abort_locked(c.source, c.destination, journal)?;
            return Err(Error::BreachAborted);
        }
        let mut updated = self.clone();
        // Persist the pause start BEFORE any fence statement, including retries.
        if self.next == Step::Fence && updated.fence_start_ms.is_none() {
            updated.fence_start_ms = Some(c.status.now_ms()?);
        }
        journal(&Record::Intent {
            step: self.next,
            state: updated.clone(),
        })
        .map_err(Error::Journal)?;
        *self = updated.clone();
        match self.next {
            Step::Readiness => c.status.readiness(&self.key)?,
            Step::Intent => {}
            Step::Fence => {
                updated.drained = Some(c.source.fence(&self.key, journal)?);
                updated.fence_end_ms = Some(c.status.now_ms()?);
            }
            Step::FinalMarker => {
                let marker = c.source.final_marker(&self.key)?;
                if marker.cutover != self.key.cutover
                    || marker.epoch < 1
                    || marker.epoch > self.key.epoch
                {
                    return Err(Error::Invalid("marker identity differs"));
                }
                updated.marker = Some(marker);
            }
            Step::Boundary => {
                let marker = self.marker()?;
                if c.progress.buffered(marker)?.as_ref() != Some(marker)
                    || c.progress.applied(marker)?.as_ref() != Some(marker)
                {
                    return Err(Error::Pending);
                }
                let verified = c.verifier.boundary(marker)?;
                if verified.marker != *marker || !verified.coverage.complete() {
                    return Err(Error::Verification);
                }
                updated.coverage = Some(verified.coverage);
            }
            Step::Sequences => c.sequences.sync(self.marker()?)?,
            Step::DestinationReadiness => c.destination.readiness(&self.key, journal)?,
            Step::Reverse => c.reverse.confirm(&self.key, self.marker()?)?,
            Step::Activate => {
                if self.coverage.as_ref().is_none_or(|c| !c.complete()) {
                    return Err(Error::Verification);
                }
                // A breach can arrive during any preceding component call.
                if activated.is_none() && c.status.breached(&self.key)? {
                    self.abort_locked(c.source, c.destination, journal)?;
                    return Err(Error::BreachAborted);
                }
                updated.activated_at_ms = Some(c.destination.activate(
                    &self.key,
                    self.marker()?,
                    journal,
                    &mut || {
                        if c.status.breached(&self.key)? {
                            Err(Error::FenceBreached(
                                "source changed before application access restoration".into(),
                            ))
                        } else {
                            Ok(())
                        }
                    },
                )?);
            }
            Step::PostActivate => {
                if activated.is_none() {
                    return Err(Error::Invalid("activation receipt missing"));
                }
                c.destination.post_activate(&self.key, journal)?;
            }
            Step::Audit => {
                let start = self
                    .fence_start_ms
                    .ok_or(Error::Invalid("fence start missing"))?;
                let end = self
                    .fence_end_ms
                    .ok_or(Error::Invalid("fence end missing"))?;
                let activation = self
                    .activated_at_ms
                    .ok_or(Error::Invalid("activation time missing"))?;
                if end < start || activation < end {
                    return Err(Error::Invalid("clock moved backwards"));
                }
                let audit = Audit {
                    fence_start_ms: start,
                    fence_end_ms: end,
                    drained: self
                        .drained
                        .clone()
                        .ok_or(Error::Invalid("drain counts missing"))?,
                    final_lsn: self.marker()?.lsn,
                    coverage: self.coverage.clone().ok_or(Error::Verification)?,
                    activated_at_ms: activation,
                    write_pause_ms: activation - start,
                };
                c.status.audit(&self.key, &audit)?;
                updated.audit = Some(audit);
            }
            Step::Complete | Step::Abort | Step::Aborted => unreachable!(),
        }
        updated.next = self.next.next();
        journal(&Record::Checkpoint(updated.clone())).map_err(Error::Journal)?;
        *self = updated;
        Ok(())
    }

    fn marker(&self) -> Result<&Marker> {
        self.marker.as_ref().ok_or(Error::Invalid("marker missing"))
    }

    pub fn abort<A: Authority, S: SourceFence, D: Destination>(
        &mut self,
        authority: &mut A,
        source: &mut S,
        destination: &mut D,
        journal: &mut Journal<'_>,
    ) -> Result<()> {
        let key = self.key.clone();
        authority.run(&key, || self.abort_locked(source, destination, journal))
    }
    fn abort_locked<S: SourceFence, D: Destination>(
        &mut self,
        source: &mut S,
        destination: &mut D,
        journal: &mut Journal<'_>,
    ) -> Result<()> {
        if destination.activated(&self.key)?.is_some()
            || self.activated_at_ms.is_some()
            || matches!(self.next, Step::PostActivate | Step::Audit | Step::Complete)
        {
            return Err(Error::UseRollback);
        }
        if self.next == Step::Aborted {
            return Ok(());
        }
        let mut aborting = self.clone();
        aborting.next = Step::Abort;
        journal(&Record::Intent {
            step: Step::Abort,
            state: aborting.clone(),
        })
        .map_err(Error::Journal)?;
        *self = aborting;
        destination.prove_fenced(&self.key, journal)?;
        source.unfence(&self.key, journal)?;
        let mut completed = self.clone();
        completed.next = Step::Aborted;
        journal(&Record::Checkpoint(completed.clone())).map_err(Error::Journal)?;
        *self = completed;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
