//! Journal-first migration coordination. The object-store journal is authoritative.

use std::{collections::BTreeMap, fmt};

use serde::{Deserialize, Serialize};

pub mod budget;
pub mod chunk;
pub mod epoch;
pub mod exclusion;
pub mod gc_authority;
pub mod gcs;
pub mod journal;
pub mod local;
pub mod readiness;
pub mod reverse;
pub mod rollback;
mod scratch;
pub mod verification;

pub use exclusion::{ExclusionGuard, MigrationExclusion};
pub use readiness::ReadinessFacts;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    BudgetExceeded(budget::Blocker),
    RecoveryRequired(String),
    UnsupportedFormatVersion {
        format: &'static str,
        found: Option<u16>,
        expected: u16,
    },
    Rejected(String),
    Lifecycle(TransitionRejection),
    ReverseProof(reverse::ProofError),
    LostOwnership,
    Observer,
    UnknownOutcome(String),
    TruncatedRead(String),
    Core(giggles_core::CoreError),
    Store(object_store::Error),
    Json(serde_json::Error),
    Io(std::io::Error),
    Postgres(tokio_postgres::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BudgetExceeded(b) => write!(
                f,
                "{}: {} exceeds {}; {}",
                b.id(),
                b.used,
                b.limit,
                b.remediation
            ),
            Self::RecoveryRequired(s) => write!(
                f,
                "recovery-required: {s}; preserve evidence and restore a verified journal before resuming"
            ),
            Self::UnsupportedFormatVersion {
                format,
                found,
                expected,
            } => write!(
                f,
                "unsupported {format} format version {found:?}, expected {expected}; preserve evidence and restore a supported journal before resuming"
            ),
            Self::Rejected(s) => write!(
                f,
                "transition rejected: {s}; inspect status and resolve the prerequisite"
            ),
            Self::Lifecycle(rejection) => rejection.fmt(f),
            Self::ReverseProof(error) => error.fmt(f),
            Self::LostOwnership => {
                f.write_str("ownership lost; stop effects and reload as an observer")
            }
            Self::Observer => {
                f.write_str("observer cannot write; explicitly acquire ownership first")
            }
            Self::UnknownOutcome(s) => write!(
                f,
                "put outcome remains unknown: {s}; stop effects and re-read the journal"
            ),
            Self::TruncatedRead(s) => write!(
                f,
                "truncated read: {s}; retry using a smaller explicit page"
            ),
            Self::Core(e) => write!(
                f,
                "local store: {e}; preserve the store and rebuild from the verified journal"
            ),
            Self::Store(e) => write!(f, "object store: {e}; restore access before resuming"),
            Self::Json(e) => write!(f, "invalid coordinator JSON: {e}"),
            Self::Io(e) => e.fmt(f),
            Self::Postgres(e) => e.fmt(f),
        }
    }
}
impl std::error::Error for Error {}
macro_rules! from_error {
    ($ty:ty, $variant:ident) => {
        impl From<$ty> for Error {
            fn from(e: $ty) -> Self {
                Self::$variant(e)
            }
        }
    };
}
from_error!(giggles_core::CoreError, Core);
from_error!(object_store::Error, Store);
from_error!(serde_json::Error, Json);
from_error!(std::io::Error, Io);
from_error!(tokio_postgres::Error, Postgres);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum State {
    Defined,
    Assessed,
    Copying,
    CatchingUp,
    VerifiedReady,
    SourceFenced,
    FinalBoundaryApplied,
    DestinationActive,
    RollbackWindow,
    RollingBack,
    Finalized,
    SourceActiveAfterRollback,
    AbortedToSource,
    Paused,
    Blocked,
    Failed,
    RecoveryRequired,
}

impl State {
    pub const ALL: [Self; 17] = [
        Self::Defined,
        Self::Assessed,
        Self::Copying,
        Self::CatchingUp,
        Self::VerifiedReady,
        Self::SourceFenced,
        Self::FinalBoundaryApplied,
        Self::DestinationActive,
        Self::RollbackWindow,
        Self::RollingBack,
        Self::Finalized,
        Self::SourceActiveAfterRollback,
        Self::AbortedToSource,
        Self::Paused,
        Self::Blocked,
        Self::Failed,
        Self::RecoveryRequired,
    ];

    /// Fixed edges from §3.11. Catching-up's ellipsis inherits copying's
    /// interruptions. Paused/blocked have a separate, saved-state-only resume edge.
    pub fn allowed_next(self) -> &'static [State] {
        use State::*;
        match self {
            Defined => &[Assessed, Blocked],
            Assessed => &[Copying],
            Copying => &[CatchingUp, Paused, Blocked, Failed, RecoveryRequired],
            CatchingUp => &[VerifiedReady, Paused, Blocked, Failed, RecoveryRequired],
            VerifiedReady => &[SourceFenced, CatchingUp],
            SourceFenced => &[FinalBoundaryApplied, AbortedToSource],
            FinalBoundaryApplied => &[DestinationActive, AbortedToSource],
            DestinationActive => &[RollbackWindow, Finalized],
            RollbackWindow => &[RollingBack, Finalized],
            RollingBack => &[SourceActiveAfterRollback],
            Finalized
            | SourceActiveAfterRollback
            | AbortedToSource
            | Paused
            | Blocked
            | Failed
            | RecoveryRequired => &[],
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Finalized | Self::SourceActiveAfterRollback | Self::AbortedToSource
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransitionRejectionCode {
    NotAllowed,
    ReadinessRequired,
    ReasonRequired,
    ResumeStateMissing,
    ResumeBoundary,
    BlockerRecheckRequired,
    BlockersStillHold,
    OperatorRecoveryRequired,
}

impl TransitionRejectionCode {
    pub fn remediation(self) -> &'static str {
        match self {
            Self::NotAllowed => {
                "Inspect the lifecycle and use an allowed next state with its required event."
            }
            Self::ReadinessRequired => {
                "Supply a fresh readiness facts snapshot; readiness transitions cannot be forced."
            }
            Self::ReasonRequired => "Record a non-empty interruption reason before retrying.",
            Self::ResumeStateMissing => {
                "Restore the saved pre-interruption state from the verified journal."
            }
            Self::ResumeBoundary => {
                "Resume only to the recorded pre-fence state; reconcile authority through explicit operator recovery."
            }
            Self::BlockerRecheckRequired => {
                "Recheck the blocker and supply ResumeAfterRecheck with the remaining blockers."
            }
            Self::BlockersStillHold => {
                "Resolve every remaining blocker, then recheck before resuming."
            }
            Self::OperatorRecoveryRequired => {
                "Preserve the journal and progress markers and perform explicit operator recovery; plain resume is forbidden."
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TransitionRejection {
    pub from: State,
    pub to: Option<State>,
    pub code: TransitionRejectionCode,
}

impl fmt::Display for TransitionRejection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "lifecycle {:?} -> {:?} rejected ({:?}); {}",
            self.from,
            self.to,
            self.code,
            self.code.remediation()
        )
    }
}

impl std::error::Error for TransitionRejection {}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Authority {
    Source,
    Destination,
    Neither,
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "kebab-case", deny_unknown_fields)]
pub enum Event {
    Advance {
        to: State,
    },
    Readiness {
        facts: ReadinessFacts,
    },
    Interrupt {
        to: State,
        reason: String,
    },
    Resume,
    /// Explicit recheck evidence is journaled, just like a readiness snapshot.
    /// Empty remaining blockers is required for a blocked migration to resume.
    ResumeAfterRecheck {
        remaining_blockers: Vec<String>,
        facts: Option<ReadinessFacts>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Lifecycle {
    pub state: State,
    pub since_seq: u64,
    pub authority: Authority,
    pub blockers: Vec<String>,
    pub resume_state: Option<State>,
}
impl Default for Lifecycle {
    fn default() -> Self {
        Self {
            state: State::Defined,
            since_seq: 0,
            authority: Authority::Source,
            blockers: vec![],
            resume_state: None,
        }
    }
}

impl Lifecycle {
    /// Resume never adds a forward edge or restores a fenced/ready state.
    pub fn allowed_next(&self) -> &[State] {
        if matches!(self.state, State::Paused | State::Blocked) {
            return match &self.resume_state {
                Some(prior @ (State::Defined | State::Copying | State::CatchingUp)) => {
                    // Defined can only have been interrupted by Blocked.
                    if self.state == State::Paused && *prior == State::Defined {
                        &[]
                    } else {
                        std::slice::from_ref(prior)
                    }
                }
                _ => &[],
            };
        }
        self.state.allowed_next()
    }
}

/// Total function: every state/event pair either returns a new value or rejects.
/// Same-state readiness observations are not lifecycle edges. No readiness
/// decision can be made through Advance or restored from a stored ready flag.
pub fn next_lifecycle(current: &Lifecycle, event: &Event, seq: u64) -> Result<Lifecycle> {
    use State::*;
    use TransitionRejectionCode as Code;
    let reject = |to, code| {
        Error::Lifecycle(TransitionRejection {
            from: current.state,
            to,
            code,
        })
    };
    let mut next = current.clone();
    let target = match event {
        Event::Advance { to } => {
            if !current.state.allowed_next().contains(to) {
                return Err(reject(Some(*to), Code::NotAllowed));
            }
            if matches!(to, Paused | Blocked | Failed | RecoveryRequired) {
                return Err(reject(Some(*to), Code::ReasonRequired));
            }
            if matches!(
                (current.state, *to),
                (CatchingUp, VerifiedReady) | (VerifiedReady, CatchingUp)
            ) {
                return Err(reject(Some(*to), Code::ReadinessRequired));
            }
            next.blockers.clear();
            *to
        }
        Event::Readiness { facts } => {
            if !matches!(current.state, CatchingUp | VerifiedReady) {
                return Err(reject(None, Code::NotAllowed));
            }
            next.blockers = facts.blockers();
            if next.blockers.is_empty() {
                VerifiedReady
            } else {
                CatchingUp
            }
        }
        Event::Interrupt { to, reason } => {
            if !matches!(to, Paused | Blocked | Failed | RecoveryRequired)
                || !current.state.allowed_next().contains(to)
            {
                return Err(reject(Some(*to), Code::NotAllowed));
            }
            if reason.trim().is_empty() {
                return Err(reject(Some(*to), Code::ReasonRequired));
            }
            next.resume_state = Some(current.state);
            next.blockers = vec![reason.clone()];
            *to
        }
        Event::Resume | Event::ResumeAfterRecheck { .. } => {
            if matches!(current.state, Failed | RecoveryRequired) {
                return Err(reject(current.resume_state, Code::OperatorRecoveryRequired));
            }
            if !matches!(current.state, Paused | Blocked) {
                return Err(reject(current.resume_state, Code::NotAllowed));
            }
            let prior = current
                .resume_state
                .ok_or_else(|| reject(None, Code::ResumeStateMissing))?;
            if !current.allowed_next().contains(&prior) {
                return Err(reject(Some(prior), Code::ResumeBoundary));
            }
            next.blockers.clear();
            match event {
                Event::ResumeAfterRecheck {
                    remaining_blockers,
                    facts,
                } => {
                    if !remaining_blockers.is_empty() {
                        return Err(reject(Some(prior), Code::BlockersStillHold));
                    }
                    if prior == CatchingUp {
                        let facts = facts
                            .as_ref()
                            .ok_or_else(|| reject(Some(prior), Code::ReadinessRequired))?;
                        // Re-evaluate but never jump forward on resume. A separate
                        // fresh Readiness event is required to enter verified-ready.
                        next.blockers = facts.blockers();
                    }
                }
                Event::Resume if current.state == Blocked => {
                    return Err(reject(Some(prior), Code::BlockerRecheckRequired));
                }
                _ => {}
            }
            next.resume_state = None;
            prior
        }
    };
    next.authority = match target {
        SourceFenced | FinalBoundaryApplied | RollingBack => Authority::Neither,
        DestinationActive | RollbackWindow | Finalized => Authority::Destination,
        SourceActiveAfterRollback | AbortedToSource => Authority::Source,
        _ => current.authority,
    };
    if next.state != target {
        next.since_seq = seq;
    }
    next.state = target;
    Ok(next)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum EffectKind {
    Fence,
    Unfence,
    FinalMarker,
    Activation,
    Rollback,
    BufferPublication,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Chunk {
    pub table: String,
    pub range_lo: String,
    pub range_hi: String,
    pub state: String,
    pub low_wm: Option<String>,
    pub high_wm: Option<String>,
}

/// Current verifier leaves only; split parents must be replaced, not retained.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerifierRange {
    pub table: String,
    pub range_lo: String,
    pub range_hi: String,
    pub source_lsn: String,
    pub source_digest: String,
    pub destination_digest: String,
    pub state: String,
    pub rows: u64,
    pub retries: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerifierException {
    pub range: String,
    pub key: String,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum Transition {
    Define {
        definition: Box<crate::MigrationDefinition>,
    },
    AcquireOwnership {
        epoch: u64,
    },
    Lifecycle {
        event: Event,
    },
    Progress {
        stream: String,
        marker: String,
        position: String,
    },
    Chunk {
        chunk: Chunk,
    },
    ChunkPlanned {
        plan: chunk::ChunkPlanRef,
    },
    ChunkStarted {
        chunk: crate::pg::chunk::ChunkRange,
        attempt: u64,
    },
    ChunkDone {
        chunk: crate::pg::chunk::ChunkRange,
        attempt: u64,
        high_lsn: u64,
        chunk_rows: chunk::ChunkRowsRef,
    },
    VerifierRange {
        id: String,
        range: Option<VerifierRange>,
    },
    VerifierException {
        id: String,
        exception: Option<VerifierException>,
    },
    VerificationCompleted {
        report: Box<verification::ReportRef>,
    },
    VerificationInvalidated {
        invalidation: verification::Invalidation,
    },
    RollbackEligibilityEnded {
        reason: reverse::EligibilityEnd,
    },
    Rollback {
        record: Box<rollback::Record>,
    },
    ReverseSetup {
        record: Box<reverse::SetupRecord>,
    },
    ReverseConsistentPoint {
        proof: Box<reverse::ReverseConsistentPoint>,
    },
    /// A later owner records the durable native activation receipt so it can
    /// publish DestinationActive without re-fencing (see reverse.rs).
    RecoveredActivation {
        receipt: Box<reverse::ActivationReceipt>,
    },
    FenceState {
        db: String,
        role: String,
        saved_privileges_json: String,
    },
    Intent {
        effect: EffectKind,
        payload: String,
    },
    /// A new owner adopts an older intent without changing its idempotency key.
    ResumeEffect {
        intent_seq: u64,
    },
    EffectCompleted {
        intent_seq: u64,
    },
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Materialized {
    pub definition: Option<crate::MigrationDefinition>,
    pub lifecycle: Lifecycle,
    pub epoch: u64,
    pub owner: String,
    pub applied_seq: u64,
    pub digest: String,
    pub progress: BTreeMap<String, (String, String)>,
    pub chunks: BTreeMap<String, Chunk>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub chunk_plans: BTreeMap<String, chunk::ChunkPlanRef>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub chunk_attempts: BTreeMap<String, chunk::ChunkAttempt>,
    pub fence_state: BTreeMap<String, (String, String, String)>,
    /// Active original/current intents and the latest replay anchor, never history.
    pub audit: BTreeMap<u64, journal::Entry>,
    pub intent_roots: BTreeMap<u64, u64>,
    #[serde(flatten)]
    pub metadata: Box<MaterializedMetadata>,
    pub verifier_ranges: BTreeMap<String, VerifierRange>,
    pub verifier_exceptions: BTreeMap<String, VerifierException>,
}

/// Cold metadata shares one allocation while retaining the checkpoint wire shape.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaterializedMetadata {
    #[serde(
        default,
        skip_serializing_if = "crate::pg::ddl::OperationalSchema::is_empty"
    )]
    pub operational_schema: crate::pg::ddl::OperationalSchema,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rollback: Option<rollback::Checkpoint>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rollback_eligibility_ended: Option<reverse::EligibilityEnd>,
    #[serde(
        default,
        skip_serializing_if = "verification::VerificationState::is_empty"
    )]
    pub verification: verification::VerificationState,
}

impl std::ops::Deref for Materialized {
    type Target = MaterializedMetadata;

    fn deref(&self) -> &Self::Target {
        &self.metadata
    }
}

impl std::ops::DerefMut for Materialized {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.metadata
    }
}

impl Materialized {
    /// Project a verified journal entry. Object prerequisites for typed chunk
    /// transitions are authenticated by Journal publication/recovery, not this
    /// synchronous, rebuildable cache operation.
    pub fn apply(&mut self, entry: &journal::Entry) -> Result<()> {
        *self = self.projected(entry)?;
        Ok(())
    }
    pub(crate) fn projected(&self, entry: &journal::Entry) -> Result<Self> {
        let mut next = self.clone();
        next.apply_inner(entry)?;
        budget::state(&next)?;
        local::projection(&next)?;
        Ok(next)
    }
    fn apply_inner(&mut self, entry: &journal::Entry) -> Result<()> {
        entry.validate()?;
        if entry.seq <= self.applied_seq {
            return if self.audit.get(&entry.seq) == Some(entry) {
                Ok(())
            } else {
                Err(Error::RecoveryRequired(
                    "replayed seq has different content".into(),
                ))
            };
        }
        if entry.seq != self.applied_seq + 1 || entry.prev_digest != self.digest {
            return Err(Error::RecoveryRequired(
                "local journal sequence/digest differs".into(),
            ));
        }
        match &entry.transition {
            Transition::AcquireOwnership { epoch } => {
                if *epoch != self.epoch + 1 || entry.epoch != *epoch {
                    return Err(Error::RecoveryRequired(
                        "ownership epoch is not last+1".into(),
                    ));
                }
                self.epoch = *epoch;
                self.owner = entry.coordinator_instance.clone();
            }
            transition => {
                if self.epoch == 0
                    || entry.epoch != self.epoch
                    || entry.coordinator_instance != self.owner
                {
                    return Err(Error::RecoveryRequired(
                        "transition is not from the journal owner".into(),
                    ));
                }
                match transition {
                    Transition::Define { definition } => {
                        let problems = definition.validate();
                        if !problems.is_empty() {
                            return Err(Error::Rejected(format!(
                                "definition has {} semantic errors",
                                problems.len()
                            )));
                        }
                        if self.definition.is_some() {
                            return Err(Error::Rejected("definition already committed".into()));
                        }
                        self.definition = Some((**definition).clone());
                        self.lifecycle.since_seq = entry.seq;
                    }
                    Transition::RollbackEligibilityEnded { reason } => {
                        reverse::end_eligibility(self, reason)?;
                    }
                    Transition::Rollback { record } => {
                        rollback::apply(self, record, entry.seq, entry.at)?;
                    }
                    Transition::ReverseSetup { record } => {
                        reverse::apply_setup(self, record, entry.seq)?;
                    }
                    Transition::ReverseConsistentPoint { proof } => {
                        reverse::apply_proof(self, proof, entry.seq)?;
                    }
                    Transition::RecoveredActivation { receipt } => {
                        reverse::apply_recovered(self, receipt)?;
                    }
                    Transition::Lifecycle { event } => {
                        if self.rollback.is_some() && self.lifecycle.state == State::RollingBack {
                            return Err(Error::Rejected(
                                "rollback lifecycle requires its typed journal checkpoint".into(),
                            ));
                        }
                        reverse::guard_lifecycle(self, event)?;
                        if matches!(
                            event,
                            Event::Advance {
                                to: State::DestinationActive
                            }
                        ) {
                            reverse::require_activation(self)?;
                        }
                        self.lifecycle = next_lifecycle(&self.lifecycle, event, entry.seq)?
                    }
                    Transition::Progress {
                        stream,
                        marker,
                        position,
                    } => {
                        if stream == crate::pg::ddl::BASELINE_STREAM {
                            let value = crate::pg::ddl::OperationalSchema {
                                baseline: Some(marker.clone()),
                                blocker: None,
                            };
                            // Validate a baseline without permitting replacement or clearing a blocker.
                            let mut validation = value.clone();
                            validation
                                .poll(
                                    crate::pg::ddl::PollPoint::Watermark(0),
                                    crate::pg::ddl::SchemaObservation {
                                        fingerprint: marker.clone(),
                                        hazards: vec![],
                                    },
                                )
                                .map_err(|e| Error::RecoveryRequired(e.to_string()))?;
                            if self
                                .operational_schema
                                .baseline
                                .as_ref()
                                .is_some_and(|b| b != marker)
                            {
                                return Err(Error::RecoveryRequired(
                                    "schema baseline replacement forbidden".into(),
                                ));
                            }
                            self.operational_schema.baseline = value.baseline;
                        }
                        if stream == crate::pg::ddl::BLOCKER_STREAM
                            && self.operational_schema.blocker.is_none()
                        {
                            self.operational_schema.blocker = Some(serde_json::from_str(marker)?);
                        }
                        self.progress
                            .insert(stream.clone(), (marker.clone(), position.clone()));
                    }
                    Transition::Chunk { chunk } => {
                        self.chunks.insert(
                            serde_json::to_string(&(&chunk.table, &chunk.range_lo))?,
                            chunk.clone(),
                        );
                    }
                    Transition::ChunkPlanned { .. }
                    | Transition::ChunkStarted { .. }
                    | Transition::ChunkDone { .. } => self.apply_chunk(transition)?,
                    Transition::VerifierRange { id, range } => {
                        if let Some(range) = range {
                            self.verifier_ranges.insert(id.clone(), range.clone());
                        } else {
                            self.verifier_ranges.remove(id);
                        }
                    }
                    Transition::VerifierException { id, exception } => {
                        if let Some(exception) = exception {
                            self.verifier_exceptions
                                .insert(id.clone(), exception.clone());
                        } else {
                            self.verifier_exceptions.remove(id);
                        }
                    }
                    Transition::VerificationCompleted { report } => {
                        report.validate()?;
                        let definition = self.definition.as_ref().ok_or_else(|| {
                            Error::Rejected("verification requires definition".into())
                        })?;
                        let selected = definition
                            .selected_objects
                            .iter()
                            .filter(|o| o.kind == crate::definition::ObjectKind::Table)
                            .map(|o| (o.schema.clone(), o.name.clone()))
                            .collect::<std::collections::BTreeSet<_>>();
                        let covered = report
                            .summary
                            .scope
                            .iter()
                            .map(|s| (s.schema.clone(), s.table.clone()))
                            .collect::<std::collections::BTreeSet<_>>();
                        if report.summary.generation != self.verification.generation
                            || report.summary.epoch != self.epoch
                            || report.summary.definition_digest != definition.canonical_digest()?
                            || selected.is_empty()
                            || covered != selected
                        {
                            return Err(Error::Rejected(
                                "verification epoch/definition/full scope differs".into(),
                            ));
                        }
                        if self.verification.runs.contains_key(&report.summary.run)
                            || self.verification.runs.len() >= 128
                        {
                            return Err(Error::Rejected(
                                "verification run already completed".into(),
                            ));
                        }
                        self.verification
                            .runs
                            .insert(report.summary.run.clone(), report.digest.clone());
                        self.verification.report = Some(report.clone());
                        self.verification.invalidated = false;
                    }
                    Transition::VerificationInvalidated { invalidation } => {
                        invalidation.validate()?;
                        if let verification::InvalidationScope::Tables(tables) = &invalidation.scope
                        {
                            let relevant = self.definition.as_ref().is_some_and(|d| {
                                d.selected_objects.iter().any(|o| {
                                    o.kind == crate::definition::ObjectKind::Table
                                        && tables.contains(&(o.schema.clone(), o.name.clone()))
                                })
                            });
                            if !relevant {
                                return Err(Error::Rejected(
                                    "invalidation outside migration scope".into(),
                                ));
                            }
                        }
                        self.verification.generation =
                            self.verification.generation.checked_add(1).ok_or_else(|| {
                                Error::Rejected("verification generation exhausted".into())
                            })?;
                        if self
                            .verification
                            .report
                            .as_ref()
                            .is_some_and(|r| invalidation.applies(r))
                        {
                            self.verification.invalidated = true;
                        }
                    }
                    Transition::FenceState {
                        db,
                        role,
                        saved_privileges_json,
                    } => {
                        self.fence_state.insert(
                            serde_json::to_string(&(db, role))?,
                            (db.clone(), role.clone(), saved_privileges_json.clone()),
                        );
                    }
                    Transition::ResumeEffect { intent_seq } => {
                        if !self.intent_roots.contains_key(intent_seq)
                            || !self.audit.get(intent_seq).is_some_and(|e| {
                                e.epoch < self.epoch
                                    && matches!(
                                        e.transition,
                                        Transition::Intent { .. } | Transition::ResumeEffect { .. }
                                    )
                            })
                        {
                            return Err(Error::Rejected(
                                "effect adoption requires an older intent".into(),
                            ));
                        }
                    }
                    Transition::EffectCompleted { intent_seq } => {
                        if !self.intent_roots.contains_key(intent_seq)
                            || !self.audit.get(intent_seq).is_some_and(|e| {
                                e.epoch == self.epoch
                                    && matches!(
                                        e.transition,
                                        Transition::Intent { .. } | Transition::ResumeEffect { .. }
                                    )
                            })
                        {
                            return Err(Error::Rejected(
                                "effect completion has no intent in this epoch".into(),
                            ));
                        }
                    }
                    Transition::Intent { .. } => {}
                    Transition::AcquireOwnership { .. } => unreachable!(),
                }
            }
        }
        self.applied_seq = entry.seq;
        self.digest = entry.digest.clone();
        match &entry.transition {
            Transition::Intent { .. } => {
                self.intent_roots.insert(entry.seq, entry.seq);
            }
            Transition::ResumeEffect { intent_seq } => {
                let root = self
                    .intent_roots
                    .remove(intent_seq)
                    .expect("validated active intent");
                self.intent_roots.insert(entry.seq, root);
            }
            Transition::EffectCompleted { intent_seq } => {
                self.intent_roots.remove(intent_seq);
            }
            _ => {}
        }
        self.audit.retain(|seq, _| {
            self.intent_roots.contains_key(seq)
                || self.intent_roots.values().any(|root| root == seq)
        });
        self.audit.insert(entry.seq, entry.clone());
        Ok(())
    }
}

#[cfg(feature = "test-crash")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CrashSeam {
    AfterReverseSetup,
    AfterVerificationReport,
    AfterJournalPut,
    AfterLocalApply,
    AfterEffect,
}

#[cfg(test)]
mod metadata_layout_tests {
    use super::*;

    #[test]
    fn boxed_metadata_preserves_checkpoint_fields_and_strict_decoding() {
        let state = Materialized::default();
        let legacy = serde_json::to_value(&state).unwrap();
        assert!(legacy.get("verification").is_none());
        assert!(legacy.get("rollback_eligibility_ended").is_none());
        assert_eq!(
            serde_json::from_value::<Materialized>(legacy.clone()).unwrap(),
            state
        );

        let mut wire = legacy;
        wire["rollback_eligibility_ended"] = serde_json::json!({"kind": "source-fence-breach"});
        wire["verification"] = serde_json::json!({
            "report": null, "invalidated": false, "generation": 7, "runs": {},
            "reverse": {"binding": null, "slot": null, "readiness": null,
                        "proof": null, "ready_seq": 0, "proof_seq": 0}
        });
        let decoded: Materialized = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(
            decoded.rollback_eligibility_ended,
            Some(reverse::EligibilityEnd::SourceFenceBreach)
        );
        assert_eq!(decoded.verification.generation, 7);
        assert_eq!(serde_json::to_value(&decoded).unwrap(), wire);
        wire["unexpected"] = serde_json::json!(true);
        assert!(serde_json::from_value::<Materialized>(wire).is_err());
    }
}
