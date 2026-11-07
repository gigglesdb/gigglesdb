//! Admission limits for the rebuildable local profile, independent of history.
use serde::{Deserialize, Serialize};

use super::{Error, Materialized, Result};

pub const ENVELOPE_BYTES: usize = 12 * 1024 * 1024;
pub const ROW_BYTES: usize = 60 * 1024;
pub const CHUNKS: usize = 12_000;
pub const CHUNK_BYTES: usize = 4 * 1024 * 1024;
pub const VERIFIER_RANGES: usize = 12_000;
pub const VERIFIER_BYTES: usize = 6 * 1024 * 1024;
pub const EXCEPTIONS: usize = 1_024;
pub const EXCEPTION_BYTES: usize = 512 * 1024;
pub const DEFINITION_BYTES: usize = 512 * 1024;
pub const PRIVILEGE_BYTES: usize = 512 * 1024;
pub const INTENTS: usize = 64;
pub const INTENT_BYTES: usize = 512 * 1024;
pub const PROGRESS: usize = 32;
pub const PROGRESS_BYTES: usize = 64 * 1024;
pub const MEMORY_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Budget {
    Chunks,
    VerifierRanges,
    VerifierExceptions,
    Definition,
    Privileges,
    ActiveIntents,
    Progress,
    Row,
    Envelope,
    Materialized,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Blocker {
    pub budget: Budget,
    pub used: usize,
    pub limit: usize,
    pub remediation: String,
}
impl Blocker {
    pub fn id(&self) -> String {
        format!(
            "local-budget.{}",
            serde_json::to_value(self.budget)
                .expect("enum")
                .as_str()
                .expect("label")
        )
    }
}
pub(crate) fn check(budget: Budget, used: usize, limit: usize) -> Result<()> {
    if used <= limit {
        return Ok(());
    }
    Err(Error::BudgetExceeded(Blocker {
        budget, used, limit,
        remediation: match budget {
            Budget::Chunks => "Use larger copy chunks or split the migration into smaller scopes.",
            Budget::VerifierRanges => "Coalesce completed verifier ranges or split the migration; do not retain a bisection tree.",
            Budget::VerifierExceptions => "Resolve queued verifier exceptions before admitting more work.",
            Budget::Definition => "Split the selected objects into smaller migration definitions.",
            Budget::Privileges => "Reduce the fencing role/object scope or split the migration.",
            Budget::ActiveIntents => "Complete or reconcile outstanding effects before admitting more intents.",
            Budget::Progress => "Use the fixed buffered, applied and verified streams; coalesce obsolete marker streams.",
            Budget::Row => "Use a journal object reference for large effect payloads or shorten range metadata.",
            Budget::Envelope | Budget::Materialized => "Coalesce current ranges and split the migration into smaller scopes; history remains in the journal.",
        }.into(),
    }))
}

/// Offline check-time admission. Normalization removes the 64 KiB definition
/// row limit, but the complete current definition still has an explicit budget.
pub fn preflight(definition: &crate::MigrationDefinition) -> Result<usize> {
    let bytes = serde_json::to_vec(definition)?.len();
    check(Budget::Definition, bytes, DEFINITION_BYTES)?;
    Ok(bytes)
}

pub(crate) fn state(state: &Materialized) -> Result<()> {
    check(Budget::Chunks, state.chunks.len(), CHUNKS)?;
    check(
        Budget::VerifierRanges,
        state.verifier_ranges.len(),
        VERIFIER_RANGES,
    )?;
    check(
        Budget::VerifierExceptions,
        state.verifier_exceptions.len(),
        EXCEPTIONS,
    )?;
    check(Budget::Progress, state.progress.len(), PROGRESS)?;
    check(Budget::ActiveIntents, state.intent_roots.len(), INTENTS)?;
    if let Some(d) = &state.definition {
        preflight(d)?;
    }
    check(
        Budget::Privileges,
        serde_json::to_vec(&state.fence_state)?.len(),
        PRIVILEGE_BYTES,
    )?;
    let active_bytes = state
        .audit
        .iter()
        .filter(|(seq, _)| {
            **seq != state.applied_seq
                || state.intent_roots.contains_key(seq)
                || state.intent_roots.values().any(|root| root == *seq)
        })
        .map(|(_, e)| serde_json::to_vec(e).map(|v| v.len()))
        .collect::<std::result::Result<Vec<_>, _>>()?
        .into_iter()
        .sum();
    check(Budget::ActiveIntents, active_bytes, INTENT_BYTES)?;
    check(
        Budget::Materialized,
        serde_json::to_vec(state)?.len(),
        MEMORY_BYTES,
    )?;
    Ok(())
}
