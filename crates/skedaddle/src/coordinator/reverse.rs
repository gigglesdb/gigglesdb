//! P11a journal contract. Only replay of the authoritative journal mints the
//! capability consumed by cutover; a setup result alone cannot release a fence.
use super::{Error, Materialized, Result, State, Transition, epoch::Coordinator, journal::Journal};
use crate::pg::{activate, fence, reverse as pg};
use serde::{Deserialize, Serialize};
use std::{fmt, time::Duration};

pub const NEXT_STEP: &str = "run reverse setup (P11a) inside the fenced window";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProofError {
    Missing,
    Mismatch,
    NotPredating,
}
impl fmt::Display for ProofError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "reverse consistent point {:?}; {NEXT_STEP}", self)
    }
}
impl std::error::Error for ProofError {}
fn invalid(kind: ProofError) -> Error {
    Error::ReverseProof(kind)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DestinationIdentity {
    pub system_identifier: String,
    pub database: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FenceBinding {
    pub migration: String,
    pub epoch: u64,
    pub owner: String,
    pub destination: DestinationIdentity,
    /// Retained preactivation ownership and all successful observations.
    pub fence: Box<activate::DestinationFence>,
}
impl FenceBinding {
    fn validate(&self, state: &Materialized) -> Result<()> {
        if self.migration.is_empty()
            || self.epoch != state.epoch
            || self.owner != state.owner
            || self.epoch != self.fence.config.epoch as u64
            || self.destination.system_identifier.is_empty()
            || self.destination.database.is_empty()
            || self.fence.saved.is_none()
            || self.fence.evidence.is_empty()
            || self.fence.next != activate::Step::Fenced
        {
            return Err(invalid(ProofError::Mismatch));
        }
        Ok(())
    }
    fn extends(&self, prior: &Self) -> bool {
        self.migration == prior.migration
            && self.epoch == prior.epoch
            && self.owner == prior.owner
            && self.destination == prior.destination
            && same_fence(&self.fence, &prior.fence)
    }
}
fn same_fence(current: &activate::DestinationFence, retained: &activate::DestinationFence) -> bool {
    current.config == retained.config
        && current.saved == retained.saved
        && current.activation_state == retained.activation_state
        && !retained.evidence.is_empty()
        && current.evidence.starts_with(&retained.evidence)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SetupRecord {
    pub binding: FenceBinding,
    pub endpoint: pg::Endpoint,
    pub intent: pg::Intent,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReverseConsistentPoint {
    pub binding: FenceBinding,
    pub slot: pg::SlotEvidence,
    pub readiness: pg::Readiness,
    /// WAL barrier from this destination, after Ready and before activation.
    pub activation_lsn: u64,
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReverseState {
    pub binding: Option<FenceBinding>,
    pub slot: Option<pg::SlotEvidence>,
    pub readiness: Option<pg::Readiness>,
    pub proof: Option<Box<ReverseConsistentPoint>>,
    pub ready_seq: u64,
    pub proof_seq: u64,
    /// Durable native activation receipt recorded by a later owner after a
    /// crash between activation commit and DestinationActive publication.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovered: Option<ActivationReceipt>,
}

/// Native activation receipt (atomic with restoring application access),
/// bound to the retained pre-activation fence of the original owner.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActivationReceipt {
    pub migration: String,
    /// Stable cutover identity, retained across ownership/epoch changes.
    pub cutover: String,
    /// Fence epoch/owner the activation ran under (the retained binding).
    pub fence_epoch: u64,
    pub fence_owner: String,
    pub destination: DestinationIdentity,
    /// Destination-clock activation time from the receipt row.
    pub activated_at_ms: u64,
}

/// The retained proof, checked for internal consistency but bound to the
/// owner that set it up rather than to the current owner.
fn retained(state: &Materialized) -> Result<&ReverseConsistentPoint> {
    let reverse = state
        .verification
        .reverse
        .as_ref()
        .ok_or_else(|| invalid(ProofError::Missing))?;
    let proof = reverse
        .proof
        .as_deref()
        .ok_or_else(|| invalid(ProofError::Missing))?;
    let b = &proof.binding;
    if b.migration.is_empty()
        || b.epoch == 0
        || b.epoch >= state.epoch
        || b.epoch != b.fence.config.epoch as u64
        || b.owner.is_empty()
        || b.destination.system_identifier.is_empty()
        || b.destination.database.is_empty()
        || b.fence.saved.is_none()
        || b.fence.evidence.is_empty()
        || b.fence.next != activate::Step::Fenced
        || reverse.proof_seq == 0
        || reverse.proof_seq > state.applied_seq
        || reverse.ready_seq >= reverse.proof_seq
        || reverse.binding.as_ref() != Some(b)
        || reverse.slot.as_ref() != Some(&proof.slot)
        || reverse.readiness.as_ref() != Some(&proof.readiness)
    {
        return Err(invalid(ProofError::Mismatch));
    }
    if proof.slot.consistent_point >= proof.activation_lsn {
        return Err(invalid(ProofError::NotPredating));
    }
    Ok(proof)
}

fn receipt_matches(receipt: &ActivationReceipt, binding: &FenceBinding) -> bool {
    !receipt.cutover.is_empty()
        && receipt.activated_at_ms > 0
        && receipt.migration == binding.migration
        && receipt.fence_epoch == binding.epoch
        && receipt.fence_owner == binding.owner
        && receipt.destination == binding.destination
}

/// Record a receipt only for a later owner, while still FinalBoundaryApplied,
/// and only when it authenticates against the retained pre-activation proof.
pub(super) fn apply_recovered(state: &mut Materialized, receipt: &ActivationReceipt) -> Result<()> {
    if state.lifecycle.state != State::FinalBoundaryApplied {
        return Err(invalid(ProofError::Mismatch));
    }
    let proof = retained(state)?;
    if !receipt_matches(receipt, &proof.binding) {
        return Err(invalid(ProofError::Mismatch));
    }
    let reverse = state
        .verification
        .reverse
        .as_mut()
        .ok_or_else(|| invalid(ProofError::Missing))?;
    if reverse.recovered.is_some() {
        return Err(invalid(ProofError::Mismatch));
    }
    reverse.recovered = Some(receipt.clone());
    Ok(())
}

/// DestinationActive authority: the current owner's proof, or, after owner
/// promotion, the retained proof plus a matching recorded activation receipt.
pub(super) fn require_activation(state: &Materialized) -> Result<()> {
    let current = require(state).map(|_| ());
    if current.is_ok() {
        return current;
    }
    let Some(receipt) = state
        .verification
        .reverse
        .as_ref()
        .and_then(|r| r.recovered.as_ref())
    else {
        return current;
    };
    let proof = retained(state)?;
    if !receipt_matches(receipt, &proof.binding) {
        return Err(invalid(ProofError::Mismatch));
    }
    Ok(())
}

pub(super) fn apply_setup(state: &mut Materialized, record: &SetupRecord, seq: u64) -> Result<()> {
    record.binding.validate(state)?;
    if !matches!(
        state.lifecycle.state,
        State::SourceFenced | State::FinalBoundaryApplied
    ) {
        return Err(invalid(ProofError::Mismatch));
    }
    let reverse = state
        .verification
        .reverse
        .get_or_insert_with(Default::default);
    if let Some(prior) = &reverse.binding
        && prior.epoch == record.binding.epoch
        && !record.binding.extends(prior)
    {
        return Err(invalid(ProofError::Mismatch));
    }
    if reverse
        .binding
        .as_ref()
        .is_some_and(|b| b.epoch != record.binding.epoch)
    {
        **reverse = ReverseState::default();
    }
    if record.endpoint != pg::Endpoint::Destination
        && !matches!(record.intent, pg::Intent::ApplyObjects)
    {
        return Err(invalid(ProofError::Mismatch));
    }
    match &record.intent {
        pg::Intent::Publication => {
            reverse.readiness = None;
            reverse.proof = None;
            reverse.proof_seq = 0;
        }
        pg::Intent::SlotReady(slot) => {
            if slot.slot.is_empty() || slot.consistent_point == 0 {
                return Err(invalid(ProofError::Mismatch));
            }
            reverse.slot = Some(slot.clone());
        }
        pg::Intent::Ready(ready) => {
            if reverse.slot.as_ref() != Some(ready.slot()) {
                return Err(invalid(ProofError::Mismatch));
            }
            reverse.readiness = Some(ready.clone());
            reverse.ready_seq = seq;
        }
        _ => {}
    }
    reverse.binding = Some(record.binding.clone());
    Ok(())
}
pub(super) fn apply_proof(
    state: &mut Materialized,
    proof: &ReverseConsistentPoint,
    seq: u64,
) -> Result<()> {
    proof.binding.validate(state)?;
    let reverse = state
        .verification
        .reverse
        .as_mut()
        .ok_or_else(|| invalid(ProofError::Missing))?;
    if reverse.binding.as_ref() != Some(&proof.binding)
        || reverse.slot.as_ref() != Some(&proof.slot)
        || reverse.readiness.as_ref() != Some(&proof.readiness)
        || proof.readiness.slot() != &proof.slot
        || reverse.ready_seq == 0
        || reverse.ready_seq >= seq
    {
        return Err(invalid(ProofError::Mismatch));
    }
    if proof.slot.consistent_point == 0 || proof.slot.consistent_point >= proof.activation_lsn {
        return Err(invalid(ProofError::NotPredating));
    }
    reverse.proof = Some(Box::new(proof.clone()));
    reverse.proof_seq = seq;
    Ok(())
}
pub(super) fn require(state: &Materialized) -> Result<&ReverseConsistentPoint> {
    let reverse = state
        .verification
        .reverse
        .as_ref()
        .ok_or_else(|| invalid(ProofError::Missing))?;
    let proof = reverse
        .proof
        .as_deref()
        .ok_or_else(|| invalid(ProofError::Missing))?;
    proof.binding.validate(state)?;
    if reverse.proof_seq == 0
        || reverse.proof_seq > state.applied_seq
        || reverse.ready_seq >= reverse.proof_seq
        || reverse.binding.as_ref() != Some(&proof.binding)
        || reverse.slot.as_ref() != Some(&proof.slot)
        || reverse.readiness.as_ref() != Some(&proof.readiness)
    {
        return Err(invalid(ProofError::Mismatch));
    }
    if proof.slot.consistent_point >= proof.activation_lsn {
        return Err(invalid(ProofError::NotPredating));
    }
    Ok(proof)
}

/// Private fields, no Deserialize: callers cannot turn a setup response or a
/// local cache into activation authority. Acquire under the cutover owner guard.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Authenticated {
    proof: Box<ReverseConsistentPoint>,
}
impl Authenticated {
    pub fn check(
        &self,
        key: &crate::cutover::Key,
        current: &activate::DestinationFence,
        destination: &DestinationIdentity,
        activation_lsn: u64,
    ) -> std::result::Result<(), ProofError> {
        let binding = &self.proof.binding;
        if key.migration != binding.migration
            || key.epoch <= 0
            || key.epoch as u64 != binding.epoch
            || key.coordinator != binding.owner
            || destination != &binding.destination
            || !same_fence(current, &binding.fence)
            || !matches!(
                current.next,
                activate::Step::CheckActivation | activate::Step::Restore
            )
        {
            return Err(ProofError::Mismatch);
        }
        if self.proof.slot.consistent_point >= self.proof.activation_lsn
            || activation_lsn < self.proof.activation_lsn
            || self.proof.slot.consistent_point >= activation_lsn
        {
            return Err(ProofError::NotPredating);
        }
        Ok(())
    }
}
impl Journal {
    pub async fn authenticated_reverse(&self) -> Result<Authenticated> {
        // Raw replay authenticates the producer events, even after a restart.
        let state = self.load(false).await?;
        let proof = require(&state)?;
        if proof.binding.migration != self.migration_id() {
            return Err(invalid(ProofError::Mismatch));
        }
        Ok(Authenticated {
            proof: Box::new(proof.clone()),
        })
    }
    pub(super) fn validate_reverse(&self, transition: &Transition) -> Result<()> {
        let migration = match transition {
            Transition::ReverseSetup { record } => &record.binding.migration,
            Transition::ReverseConsistentPoint { proof } => &proof.binding.migration,
            Transition::RecoveredActivation { receipt } => &receipt.migration,
            _ => return Ok(()),
        };
        if migration != self.migration_id() {
            return Err(invalid(ProofError::Mismatch));
        }
        Ok(())
    }
}

pub fn destination_barrier(
    connection: &mut impl fence::Connection,
    timeout: Duration,
) -> Result<(DestinationIdentity, u64)> {
    let rows = connection.query("SELECT system_identifier::text,current_database(),pg_current_wal_insert_lsn()::text FROM pg_control_system()", timeout)
        .map_err(Error::Rejected)?;
    let row = rows
        .first()
        .filter(|r| r.len() == 3)
        .ok_or_else(|| invalid(ProofError::Mismatch))?;
    let lsn =
        crate::pg::replication::parse_lsn(&row[2]).map_err(|_| invalid(ProofError::Mismatch))?;
    Ok((
        DestinationIdentity {
            system_identifier: row[0].clone(),
            database: row[1].clone(),
        },
        lsn,
    ))
}

impl Coordinator {
    /// Synchronous PG transports must be called outside an entered runtime.
    /// The supplied runtime drives the journal before each database effect.
    pub fn setup_reverse(
        &mut self,
        runtime: &tokio::runtime::Runtime,
        config: &pg::Config,
        destination: &mut impl fence::Connection,
        old_source: &mut impl fence::Connection,
        destination_fence: &mut activate::DestinationFence,
    ) -> Result<Authenticated> {
        // Never embed a previous capability (which itself retains a fence) in
        // the next binding. Native serialization also omits this capability.
        destination_fence.clear_reverse_proof();
        let timeout = Duration::from_millis(destination_fence.config.timeout_ms);
        let previous = self
            .state()
            .verification
            .reverse
            .as_ref()
            .and_then(|r| r.slot.clone());
        let mut last = None;
        let readiness = config
            .setup_bound(
                destination,
                old_source,
                previous.as_ref(),
                timeout,
                |connection, endpoint, intent| {
                    let result = (|| -> Result<()> {
                        runtime.block_on(self.refresh())?;
                        if !self.is_owner() {
                            return Err(Error::LostOwnership);
                        }
                        destination_fence
                            .observe(connection, &mut |_| Ok(()))
                            .map_err(|e| Error::Rejected(e.to_string()))?;
                        let (identity, _) = destination_barrier(connection, timeout)?;
                        let binding = FenceBinding {
                            migration: self.journal.migration_id().into(),
                            epoch: self.state().epoch,
                            owner: self.instance_id().into(),
                            destination: identity,
                            fence: Box::new(destination_fence.clone()),
                        };
                        runtime.block_on(self.commit_inner(Transition::ReverseSetup {
                            record: Box::new(SetupRecord {
                                binding: binding.clone(),
                                endpoint,
                                intent: intent.clone(),
                            }),
                        }))?;
                        last = Some(binding);
                        Ok(())
                    })();
                    result.map_err(|e| pg::Error::Journal(e.to_string()))
                },
            )
            .map_err(|e| Error::Rejected(e.to_string()))?;
        #[cfg(feature = "test-crash")]
        self.crash_at(super::CrashSeam::AfterReverseSetup);
        runtime.block_on(self.refresh())?;
        if !self.is_owner() {
            return Err(Error::LostOwnership);
        }
        let binding = last.ok_or_else(|| invalid(ProofError::Missing))?;
        let (identity, activation_lsn) = destination_barrier(destination, timeout)?;
        if identity != binding.destination {
            return Err(invalid(ProofError::Mismatch));
        }
        readiness
            .check_activation(config, activation_lsn)
            .map_err(|e| Error::Rejected(e.to_string()))?;
        runtime.block_on(self.commit_inner(Transition::ReverseConsistentPoint {
            proof: Box::new(ReverseConsistentPoint {
                binding,
                slot: readiness.slot().clone(),
                readiness,
                activation_lsn,
            }),
        }))?;
        runtime.block_on(self.journal.authenticated_reverse())
    }
}

/// Permanent rollback disqualification. These reasons contain no row values.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum EligibilityEnd {
    /// Explicit operator finalization ends any remaining rollback opportunity.
    OperatorFinalized,
    Domain {
        violation: pg::Violation,
    },
    /// A source fence breach is also a critical operator alert.
    SourceFenceBreach,
}

pub(super) fn end_eligibility(state: &mut Materialized, reason: &EligibilityEnd) -> Result<()> {
    if !matches!(
        state.lifecycle.state,
        State::DestinationActive | State::RollbackWindow
    ) && !(state.lifecycle.state == State::RollingBack && state.rollback.is_some())
    {
        return Err(Error::Rejected("eligibility can end only after destination activation or during journaled rollback, before finalization".into()));
    }
    if state.rollback_eligibility_ended.is_some() {
        return Err(Error::Rejected("rollback eligibility already ended".into()));
    }
    state.rollback_eligibility_ended = Some(reason.clone());
    Ok(())
}

pub(super) fn guard_lifecycle(state: &Materialized, event: &super::Event) -> Result<()> {
    let super::Event::Advance { to } = event else {
        return Ok(());
    };
    if state.lifecycle.state == State::DestinationActive
        && *to == State::Finalized
        && state.rollback_eligibility_ended.is_none()
    {
        return Err(Error::Rejected(
            "destination-active finalization requires journaled rollback eligibility ending reason"
                .into(),
        ));
    }
    if matches!(to, State::RollbackWindow | State::RollingBack)
        && state.rollback_eligibility_ended.is_some()
    {
        return Err(Error::Rejected(
            "rollback eligibility permanently ended".into(),
        ));
    }
    Ok(())
}
