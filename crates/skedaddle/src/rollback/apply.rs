//! Post-PONR rollback, under the owner's exclusive migration authority guard.
//! Reconstruct all adapters after errors/cancellation; retry infrastructure faults
//! with bounded backoff. No operation here can release either endpoint's fence.
use crate::{
    buffer::{
        backends::BufferStore,
        format::Limits,
        reader::{BufferReader, ReaderCheckpoint, ReaderConfig},
    },
    coordinator::{
        Materialized, State,
        rollback::{Record, Step},
    },
    cutover::Key,
    engine::{CapturedControl, classify_captured_control},
    event::{AssembledTxn, PositionOrder},
    pg::apply::{
        self,
        executor::{self, Destination, ReaderLoop},
    },
    verify::{self, Projection, RangePlan, RowSource},
};
use std::{cell::Cell, fmt, sync::Arc};

pub type Result<T> = std::result::Result<T, Error>;
#[derive(Debug)]
pub enum Error {
    /// Rebuild from durable state and retry. Both fences remain held.
    Retry,
    /// Integration must map this to exit 5 and require operator intervention.
    OperatorAction(&'static str),
}
impl Error {
    pub fn exit_code(&self) -> u8 {
        5
    }
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Retry => f.write_str("rollback interrupted; retry with both sides fenced"),
            Self::OperatorAction(reason) => write!(
                f,
                "rollback requires operator action: {reason}; keep both sides fenced"
            ),
        }
    }
}
impl std::error::Error for Error {}

/// Atomic authenticated publication under the migration authority guard, returning
/// only after durability and local materialization. Progress belongs to this
/// rollback request, never the forward `applied` key. Encode the reader checkpoint
/// with engine::encode_reader_checkpoint; publish checkpoint + reached atomically.
/// The initial value is the retained reverse stream's durable start checkpoint.
pub trait Journal {
    fn state(&self) -> Materialized;
    fn publish(&mut self, record: Record) -> Result<()>;
    fn progress(&self, request: &str) -> Result<(ReaderCheckpoint, bool)>;
    fn save(&mut self, request: &str, checkpoint: ReaderCheckpoint, reached: bool) -> Result<()>;
}

#[allow(async_fn_in_trait)]
pub trait Verification {
    /// Both endpoints are fenced. Compare all migrated objects with destination
    /// as reference and source as target. Retry transport errors, never mismatch.
    async fn verify(&mut self) -> Result<()>;
}

/// Existing typed verifier, with endpoints swapped. Each entry must cover a whole
/// migrated table (unbounded range); projections come from qualified preflight.
/// RowSources can be PgRowSource, and must retain the same key ordering.
pub struct TableVerification<S, D> {
    pub destination: S,
    pub source: D,
    pub range: RangePlan,
    pub projection: Projection,
}
impl<S: RowSource, D: RowSource> Verification for Vec<TableVerification<S, D>> {
    async fn verify(&mut self) -> Result<()> {
        if self.is_empty() {
            return Err(Error::OperatorAction("verification inventory missing"));
        }
        for table in self {
            if table.range.lo.is_some() || table.range.hi.is_some() {
                return Err(Error::OperatorAction(
                    "verification inventory is not whole-table",
                ));
            }
            let comparison = verify::compare_sources(
                &mut table.destination,
                &mut table.source,
                &table.range,
                &table.projection,
            )
            .await
            .map_err(|e| match e {
                verify::VerifyError::Read(_) => Error::Retry,
                _ => Error::OperatorAction("verification evidence unavailable"),
            })?;
            if !comparison.differences.is_empty()
                || !comparison.busy.is_empty()
                || comparison.source_count != comparison.destination_count
                || comparison.source_digest != comparison.destination_digest
            {
                return Err(Error::OperatorAction("source differs from destination"));
            }
        }
        Ok(())
    }
}

pub struct Replay {
    pub store: Arc<dyn BufferStore>,
    pub order: Arc<dyn PositionOrder + Send + Sync>,
    /// Complete durable manifest high-water from the stopped reverse capture.
    pub required_next_mseq: u64,
    /// SOURCE catalog descriptors, with reverse event identities.
    pub tables: Vec<apply::Table>,
}
#[derive(Debug, PartialEq, Eq)]
pub enum Outcome {
    Applied,
    Verified,
    ReadyForSequences,
}

fn executor_error(error: executor::Error) -> Error {
    match error {
        executor::Error::Database | executor::Error::Checkpoint => Error::Retry,
        executor::Error::Buffer(crate::buffer::reader::Error::Store(_)) => Error::Retry,
        _ => Error::OperatorAction("reverse apply evidence unavailable"),
    }
}

/// Call only while holding the exclusive authority guard used by destination
/// preparation. Source must be the existing executor configured with stream
/// `<migration>_rev` and origin `skedaddle_<migration>_rev`, on the OLD SOURCE.
/// Reverse setup must already have installed its origin and progress tables.
/// One call applies the bounded stream, or reruns verification. Deadline expiry
/// after intent publication never permits destination restoration.
pub async fn execute_next(
    key: &Key,
    replay: &Replay,
    source: &mut dyn Destination,
    verification: &mut impl Verification,
    journal: &mut impl Journal,
) -> Result<Outcome> {
    let state = journal.state();
    let cp = state
        .rollback
        .as_ref()
        .ok_or(Error::OperatorAction("rollback checkpoint missing"))?;
    if key.epoch <= 0
        || state.epoch != key.epoch as u64
        || state.owner != key.coordinator
        || state.lifecycle.state != State::RollingBack
        || state.rollback_eligibility_ended.is_some()
    {
        return Err(Error::OperatorAction("rollback authority unavailable"));
    }
    let request = cp.request.clone();
    let marker = cp
        .final_marker
        .filter(|v| *v != 0)
        .ok_or(Error::OperatorAction("final marker missing"))?;
    let step = cp.next;
    if step == Step::Sequences {
        return Ok(Outcome::ReadyForSequences);
    }
    if !matches!(step, Step::ReverseApply | Step::Verify)
        || (step == Step::Verify && !cp.point_of_no_return)
    {
        return Err(Error::OperatorAction("unexpected rollback step"));
    }
    // This durable publication is the PONR, BEFORE any source-side effect.
    journal.publish(Record::Intent { step })?;
    if step == Step::Verify {
        verification.verify().await?;
        journal.publish(Record::Checkpoint {
            step,
            final_marker: None,
        })?;
        return Ok(Outcome::Verified);
    }
    let (checkpoint, reached) = journal.progress(&request)?;
    if !reached {
        let stream = format!("{}_rev", key.migration);
        let origin = format!("skedaddle_{}_rev", key.migration);
        let reader = BufferReader::new(
            replay.store.clone(),
            ReaderConfig {
                prefix: "reverse-runner-v1".into(),
                migration_id: key.migration.clone(),
                stream: stream.clone(),
                limits: Limits::default(),
                required_next_mseq: replay.required_next_mseq,
            },
            checkpoint,
            replay.order.clone(),
        )
        .map_err(|_| Error::OperatorAction("reverse checkpoint invalid"))?;
        let mut reader = ReaderLoop::new(reader, stream.clone());
        loop {
            let found = Cell::new(false);
            let save_failed = Cell::new(false);
            let applied = reader
                .step(
                    source,
                    |txn, mseq| {
                        found.set(is_final(txn, &request, marker, key.epoch)?);
                        apply::plan_transaction(
                            txn,
                            &replay.tables,
                            &apply::PlanContext {
                                stream: stream.clone(),
                                mseq,
                                origin: origin.clone(),
                                synthetic_commit_time: Some(0),
                            },
                            |_, _| Ok(apply::RangeState::Completed),
                        )
                    },
                    |checkpoint| {
                        let result = journal.save(&request, checkpoint, found.get());
                        save_failed.set(result.is_err());
                        std::future::ready(result.map_err(|_| executor::Error::Checkpoint))
                    },
                )
                .await;
            if save_failed.get() {
                return Err(Error::Retry);
            }
            if !applied.map_err(executor_error)? {
                return Err(Error::OperatorAction(
                    "reverse buffer ended before final marker",
                ));
            }
            if found.get() {
                break;
            }
        }
    }
    journal.publish(Record::Checkpoint {
        step,
        final_marker: None,
    })?;
    Ok(Outcome::Applied)
}

fn is_final(
    txn: &AssembledTxn,
    request: &str,
    lsn: u64,
    epoch: i64,
) -> std::result::Result<bool, apply::ApplyError> {
    let mut found = false;
    for event in &txn.events {
        if let CapturedControl::Final(marker) = classify_captured_control(event)
            .map_err(|_| apply::ApplyError::Invalid("rollback control"))?
        {
            if found
                || marker.cutover != request
                || marker.lsn != lsn
                || marker.epoch <= 0
                || marker.epoch > epoch
            {
                return Err(apply::ApplyError::Invalid("rollback marker identity"));
            }
            found = true;
        }
    }
    let commit = u64::from_be_bytes(
        txn.commit_position
            .bytes()
            .try_into()
            .map_err(|_| apply::ApplyError::Invalid("reverse position"))?,
    );
    if !found && commit >= lsn {
        return Err(apply::ApplyError::Invalid(
            "reverse stream passed final marker",
        ));
    }
    Ok(found)
}

#[cfg(test)]
mod tests;
