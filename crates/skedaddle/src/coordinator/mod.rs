//! Journal-first migration coordination. The object-store journal is authoritative.

use std::{collections::BTreeMap, fmt};

use serde::{Deserialize, Serialize};

pub mod epoch;
pub mod gcs;
pub mod journal;
pub mod local;
mod scratch;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    RecoveryRequired(String),
    Rejected(String),
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
            Self::RecoveryRequired(s) => write!(
                f,
                "recovery-required: {s}; preserve evidence and restore a verified journal before resuming"
            ),
            Self::Rejected(s) => write!(
                f,
                "transition rejected: {s}; inspect status and resolve the prerequisite"
            ),
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Authority {
    Source,
    Destination,
    Neither,
    Unknown,
}

/// All ten readiness conditions are independently re-evaluated by callers.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadinessFacts {
    pub all_chunks_done: bool,
    pub capture_healthy: bool,
    pub lineage_matches: bool,
    pub apply_lag: u64,
    pub lag_budget: u64,
    pub blocked_ddl: bool,
    pub unknown_schema_job: bool,
    pub verified_rows: u64,
    pub in_scope_rows: u64,
    pub busy_rows_past_budget: u64,
    pub dead_letters: u64,
    pub indexes_constraints_valid: bool,
    pub sequences_syncable: bool,
    pub reverse_prerequisites: bool,
    pub fence_risks_acknowledged: bool,
    pub buffer_contract_matches: bool,
}

impl ReadinessFacts {
    pub fn blockers(&self) -> Vec<String> {
        let conditions = [
            (self.all_chunks_done, "chunks-incomplete"),
            (self.capture_healthy, "capture-unhealthy"),
            (self.lineage_matches, "lineage-mismatch"),
            (self.apply_lag <= self.lag_budget, "apply-lag-over-budget"),
            (
                !self.blocked_ddl && !self.unknown_schema_job,
                "schema-boundary-pending",
            ),
            (
                self.verified_rows == self.in_scope_rows,
                "verification-incomplete",
            ),
            (self.busy_rows_past_budget == 0, "busy-row-budget-exhausted"),
            (self.dead_letters == 0, "dead-letters-present"),
            (self.indexes_constraints_valid, "destination-schema-invalid"),
            (self.sequences_syncable, "sequences-not-syncable"),
            (self.reverse_prerequisites, "reverse-prerequisites-missing"),
            (self.fence_risks_acknowledged, "fence-risk-unacknowledged"),
            (self.buffer_contract_matches, "buffer-contract-mismatch"),
        ];
        conditions
            .into_iter()
            .filter(|(ok, _)| !ok)
            .map(|(_, why)| why.to_owned())
            .collect()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "kebab-case", deny_unknown_fields)]
pub enum Event {
    Advance { to: State },
    Readiness { facts: ReadinessFacts },
    Interrupt { to: State, reason: String },
    Resume,
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

/// Total function: every state/event pair either returns a new value or rejects.
/// The ellipsis in §3.11 catching-up inherits copying's interruptions. Resume
/// is limited to paused/blocked before fencing; failed/recovery require repair.
pub fn next_lifecycle(current: &Lifecycle, event: &Event, seq: u64) -> Result<Lifecycle> {
    use State::*;
    let mut next = current.clone();
    let target = match event {
        Event::Advance { to } => {
            let legal = matches!(
                (current.state, *to),
                (Defined, Assessed | Blocked)
                    | (Assessed, Copying)
                    | (Copying, CatchingUp)
                    | (VerifiedReady, SourceFenced)
                    | (SourceFenced, FinalBoundaryApplied | AbortedToSource)
                    | (FinalBoundaryApplied, DestinationActive | AbortedToSource)
                    | (DestinationActive, RollbackWindow)
                    | (RollbackWindow, RollingBack | Finalized)
                    | (RollingBack, SourceActiveAfterRollback)
            );
            if !legal {
                return Err(Error::Rejected(format!("{:?} -> {to:?}", current.state)));
            }
            if *to == Blocked {
                return Err(Error::Rejected(
                    "blocking requires a recorded reason".into(),
                ));
            }
            next.blockers.clear();
            *to
        }
        Event::Readiness { facts } => {
            if !matches!(current.state, CatchingUp | VerifiedReady) {
                return Err(Error::Rejected(
                    "readiness is evaluated in catching-up or verified-ready".into(),
                ));
            }
            next.blockers = facts.blockers();
            if next.blockers.is_empty() {
                VerifiedReady
            } else {
                CatchingUp
            }
        }
        Event::Interrupt { to, reason } => {
            let legal = matches!(
                (current.state, *to),
                (Defined, Blocked)
                    | (
                        Copying | CatchingUp,
                        Paused | Blocked | Failed | RecoveryRequired
                    )
            );
            if !legal || reason.trim().is_empty() {
                return Err(Error::Rejected(
                    "interruption needs a legal state and a reason".into(),
                ));
            }
            next.resume_state = Some(current.state);
            next.blockers = vec![reason.clone()];
            *to
        }
        Event::Resume => {
            if !matches!(current.state, Paused | Blocked) {
                return Err(Error::Rejected(
                    "only paused/blocked may resume after prerequisites are rechecked".into(),
                ));
            }
            let prior = current
                .resume_state
                .ok_or_else(|| Error::Rejected("missing resume state".into()))?;
            if !matches!(prior, Defined | Copying | CatchingUp) {
                return Err(Error::Rejected(
                    "resume crosses an authority boundary".into(),
                ));
            }
            next.blockers.clear();
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
    pub fence_state: BTreeMap<String, (String, String, String)>,
    pub audit: BTreeMap<u64, journal::Entry>,
}

impl Materialized {
    pub fn apply(&mut self, entry: &journal::Entry) -> Result<()> {
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
                    Transition::Lifecycle { event } => {
                        self.lifecycle = next_lifecycle(&self.lifecycle, event, entry.seq)?
                    }
                    Transition::Progress {
                        stream,
                        marker,
                        position,
                    } => {
                        self.progress
                            .insert(stream.clone(), (marker.clone(), position.clone()));
                    }
                    Transition::Chunk { chunk } => {
                        self.chunks.insert(
                            serde_json::to_string(&(&chunk.table, &chunk.range_lo))?,
                            chunk.clone(),
                        );
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
                        if !self.audit.get(intent_seq).is_some_and(|e| {
                            e.epoch < self.epoch
                                && matches!(
                                    e.transition,
                                    Transition::Intent { .. } | Transition::ResumeEffect { .. }
                                )
                        }) {
                            return Err(Error::Rejected(
                                "effect adoption requires an older intent".into(),
                            ));
                        }
                    }
                    Transition::EffectCompleted { intent_seq } => {
                        if !self.audit.get(intent_seq).is_some_and(|e| {
                            e.epoch == self.epoch
                                && matches!(
                                    e.transition,
                                    Transition::Intent { .. } | Transition::ResumeEffect { .. }
                                )
                        }) {
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
        self.audit.insert(entry.seq, entry.clone());
        Ok(())
    }
}

#[cfg(feature = "test-crash")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CrashSeam {
    AfterJournalPut,
    AfterLocalApply,
    AfterEffect,
}
