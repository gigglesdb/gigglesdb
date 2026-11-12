//! Fresh, pure evaluation of the ten readiness conditions in plan §3.12.
//!
//! Facts are supplied by the engine; neither the snapshot nor this evaluator
//! stores a readiness flag. Journaled snapshots explain past decisions only.

use serde::{Deserialize, Serialize};

/// One simultaneous snapshot. Producers are intentionally outside this module.
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Condition {
    Chunks,
    CaptureAndLineage,
    ApplyLag,
    SchemaJobs,
    Verification,
    DestinationSchema,
    Sequences,
    ReverseReplication,
    FencePlan,
    BufferContract,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum BlockerCode {
    ChunksIncomplete,
    CaptureUnhealthy,
    LineageMismatch,
    ApplyLagOverBudget,
    SchemaBoundaryPending,
    VerificationIncomplete,
    BusyRowBudgetExhausted,
    DeadLettersPresent,
    DestinationSchemaInvalid,
    SequencesNotSyncable,
    ReversePrerequisitesMissing,
    FenceRiskUnacknowledged,
    BufferContractMismatch,
}

impl BlockerCode {
    pub const ALL: [Self; 13] = [
        Self::ChunksIncomplete,
        Self::CaptureUnhealthy,
        Self::LineageMismatch,
        Self::ApplyLagOverBudget,
        Self::SchemaBoundaryPending,
        Self::VerificationIncomplete,
        Self::BusyRowBudgetExhausted,
        Self::DeadLettersPresent,
        Self::DestinationSchemaInvalid,
        Self::SequencesNotSyncable,
        Self::ReversePrerequisitesMissing,
        Self::FenceRiskUnacknowledged,
        Self::BufferContractMismatch,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::ChunksIncomplete => "chunks-incomplete",
            Self::CaptureUnhealthy => "capture-unhealthy",
            Self::LineageMismatch => "lineage-mismatch",
            Self::ApplyLagOverBudget => "apply-lag-over-budget",
            Self::SchemaBoundaryPending => "schema-boundary-pending",
            Self::VerificationIncomplete => "verification-incomplete",
            Self::BusyRowBudgetExhausted => "busy-row-budget-exhausted",
            Self::DeadLettersPresent => "dead-letters-present",
            Self::DestinationSchemaInvalid => "destination-schema-invalid",
            Self::SequencesNotSyncable => "sequences-not-syncable",
            Self::ReversePrerequisitesMissing => "reverse-prerequisites-missing",
            Self::FenceRiskUnacknowledged => "fence-risk-unacknowledged",
            Self::BufferContractMismatch => "buffer-contract-mismatch",
        }
    }

    pub fn condition(self) -> Condition {
        match self {
            Self::ChunksIncomplete => Condition::Chunks,
            Self::CaptureUnhealthy | Self::LineageMismatch => Condition::CaptureAndLineage,
            Self::ApplyLagOverBudget => Condition::ApplyLag,
            Self::SchemaBoundaryPending => Condition::SchemaJobs,
            Self::VerificationIncomplete
            | Self::BusyRowBudgetExhausted
            | Self::DeadLettersPresent => Condition::Verification,
            Self::DestinationSchemaInvalid => Condition::DestinationSchema,
            Self::SequencesNotSyncable => Condition::Sequences,
            Self::ReversePrerequisitesMissing => Condition::ReverseReplication,
            Self::FenceRiskUnacknowledged => Condition::FencePlan,
            Self::BufferContractMismatch => Condition::BufferContract,
        }
    }

    pub fn message(self) -> &'static str {
        match self {
            Self::ChunksIncomplete => {
                "Copy chunks remain unfinished; the destination is incomplete."
            }
            Self::CaptureUnhealthy => "Capture is unhealthy; source changes may not be durable.",
            Self::LineageMismatch => "Capture lineage differs; positions cannot be trusted.",
            Self::ApplyLagOverBudget => "Apply lag exceeds the cutover budget.",
            Self::SchemaBoundaryPending => {
                "Blocked DDL or an unknown schema job prevents safe apply."
            }
            Self::VerificationIncomplete => {
                "Verification does not cover exactly all in-scope rows."
            }
            Self::BusyRowBudgetExhausted => "Busy rows exceeded their verification retry budget.",
            Self::DeadLettersPresent => {
                "Dead-letter entries represent unresolved migration errors."
            }
            Self::DestinationSchemaInvalid => "Destination indexes or constraints are invalid.",
            Self::SequencesNotSyncable => "Sequences cannot be synchronized for safe activation.",
            Self::ReversePrerequisitesMissing => {
                "Reverse replication is not prepared before activation."
            }
            Self::FenceRiskUnacknowledged => {
                "The fence plan contains unacknowledged residual risk."
            }
            Self::BufferContractMismatch => {
                "The buffer backend or failure domain differs from the definition."
            }
        }
    }

    pub fn remediation(self) -> &'static str {
        match self {
            Self::ChunksIncomplete => {
                "Finish or retry every outstanding copy chunk, then re-evaluate readiness."
            }
            Self::CaptureUnhealthy => {
                "Restore capture health and verify durable capture progress before retrying."
            }
            Self::LineageMismatch => {
                "Reconcile the capture lineage with the migration definition before resuming."
            }
            Self::ApplyLagOverBudget => {
                "Let apply catch up until lag is within the configured budget."
            }
            Self::SchemaBoundaryPending => {
                "Resolve the blocked DDL boundary and identify or complete every pending schema job."
            }
            Self::VerificationIncomplete => {
                "Verify every in-scope row and reconcile the coverage denominator."
            }
            Self::BusyRowBudgetExhausted => {
                "Resolve the busy rows and complete their verification within the retry policy."
            }
            Self::DeadLettersPresent => {
                "Repair and replay every dead-letter entry, then verify the affected rows."
            }
            Self::DestinationSchemaInvalid => {
                "Build or repair destination indexes and validate every constraint."
            }
            Self::SequencesNotSyncable => {
                "Repair sequence mapping and privileges so final sequence synchronization can run."
            }
            Self::ReversePrerequisitesMissing => {
                "Prepare reverse publication, slot, DDL capture and old-source apply objects before activation."
            }
            Self::FenceRiskUnacknowledged => {
                "Review the fence plan and explicitly acknowledge or eliminate each residual risk."
            }
            Self::BufferContractMismatch => {
                "Use the backend and failure-domain contract specified by the migration definition."
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Reason {
    pub code: BlockerCode,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Readiness {
    Ready,
    /// Includes every failed component of each condition, without short circuiting.
    NotReady {
        reasons: Vec<Reason>,
    },
}

pub fn evaluate(facts: &ReadinessFacts) -> Readiness {
    use BlockerCode::*;
    let conditions = [
        (facts.all_chunks_done, ChunksIncomplete),
        (facts.capture_healthy, CaptureUnhealthy),
        (facts.lineage_matches, LineageMismatch),
        (facts.apply_lag <= facts.lag_budget, ApplyLagOverBudget),
        (
            !facts.blocked_ddl && !facts.unknown_schema_job,
            SchemaBoundaryPending,
        ),
        (
            facts.verified_rows == facts.in_scope_rows,
            VerificationIncomplete,
        ),
        (facts.busy_rows_past_budget == 0, BusyRowBudgetExhausted),
        (facts.dead_letters == 0, DeadLettersPresent),
        (facts.indexes_constraints_valid, DestinationSchemaInvalid),
        (facts.sequences_syncable, SequencesNotSyncable),
        (facts.reverse_prerequisites, ReversePrerequisitesMissing),
        (facts.fence_risks_acknowledged, FenceRiskUnacknowledged),
        (facts.buffer_contract_matches, BufferContractMismatch),
    ];
    let reasons: Vec<_> = conditions
        .into_iter()
        .filter(|(ok, _)| !ok)
        .map(|(_, code)| Reason { code })
        .collect();
    if reasons.is_empty() {
        Readiness::Ready
    } else {
        Readiness::NotReady { reasons }
    }
}

impl ReadinessFacts {
    pub fn evaluate(&self) -> Readiness {
        evaluate(self)
    }

    /// P2-compatible journal/status codes; messages and remediation are typed above.
    pub fn blockers(&self) -> Vec<String> {
        match self.evaluate() {
            Readiness::Ready => vec![],
            Readiness::NotReady { reasons } => reasons
                .into_iter()
                .map(|reason| reason.code.as_str().to_owned())
                .collect(),
        }
    }
}
