//! Durable snapshot plans and current attempts. Large samples never enter the
//! local projection: immutable plan objects are capped at 4 MiB, 12,000 ranges.
use super::{Error, Materialized, Result, Transition, budget, journal::Journal};
use crate::{
    buffer::{backends::BufferStore, format, reader},
    event::{ContentDigest, Position, PositionOrder},
    pg::chunk::{ChunkRange, Key, TableKey, plan_chunks},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::Arc;

pub const PLAN_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChunkPlan {
    pub table: TableKey,
    pub chunk_size: usize,
    /// Source order, never sorted or regenerated during recovery.
    pub samples: Vec<Key>,
    pub ranges: Vec<ChunkRange>,
}
impl ChunkPlan {
    pub fn new(table: TableKey, chunk_size: usize, samples: Vec<Key>) -> Result<Self> {
        let ranges = plan_chunks(&table, chunk_size, &samples).map_err(invalid)?;
        let plan = Self {
            table,
            chunk_size,
            samples,
            ranges,
        };
        plan.validate()?;
        Ok(plan)
    }
    fn validate(&self) -> Result<()> {
        budget::check(budget::Budget::Chunks, self.ranges.len(), budget::CHUNKS)?;
        if self.ranges
            != plan_chunks(&self.table, self.chunk_size, &self.samples).map_err(invalid)?
        {
            return Err(invalid("samples/plan mismatch"));
        }
        budget::check(
            budget::Budget::Chunks,
            serde_json::to_vec(self)?.len(),
            PLAN_BYTES,
        )
    }
    fn reference(&self, bytes: &[u8]) -> ChunkPlanRef {
        ChunkPlanRef {
            table: self.table.table.clone(),
            plan: self.ranges[0].id.plan.clone(),
            digest: format!("{:x}", Sha256::digest(bytes)),
            bytes: bytes.len(),
            chunks: self.ranges.len(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChunkPlanRef {
    pub table: String,
    pub plan: String,
    pub digest: String,
    pub bytes: usize,
    pub chunks: usize,
}
impl ChunkPlanRef {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.table.is_empty()
            || !hex_digest(&self.plan)
            || !hex_digest(&self.digest)
            || self.bytes == 0
            || self.bytes > PLAN_BYTES
            || self.chunks == 0
            || self.chunks > budget::CHUNKS
        {
            return Err(invalid("invalid plan reference"));
        }
        Ok(())
    }
}
fn hex_digest(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChunkRowsRef {
    pub mseq: u64,
    pub manifest_digest: ContentDigest,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChunkAttempt {
    pub chunk: ChunkRange,
    pub attempt: u64,
    pub done: Option<ChunkCompletion>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChunkCompletion {
    pub high_lsn: u64,
    pub chunk_rows: ChunkRowsRef,
}

pub(crate) fn invalid(error: impl std::fmt::Display) -> Error {
    Error::RecoveryRequired(format!("chunk: {error}"))
}
fn invalid_format(error: format::FormatError) -> Error {
    match error {
        format::FormatError::UnsupportedFormatVersion {
            format,
            found,
            expected,
        } => Error::UnsupportedFormatVersion {
            format,
            found,
            expected,
        },
        other => invalid(other),
    }
}
pub(crate) fn id(chunk: &ChunkRange) -> Result<String> {
    Ok(serde_json::to_string(&chunk.id)?)
}

impl Materialized {
    pub(crate) fn apply_chunk(&mut self, transition: &Transition) -> Result<()> {
        match transition {
            Transition::ChunkPlanned { plan } => {
                plan.validate()?;
                if self.chunk_plans.contains_key(&plan.table) {
                    return Err(invalid("table already planned; reuse the saved plan"));
                }
                self.chunk_plans.insert(plan.table.clone(), plan.clone());
            }
            Transition::ChunkStarted { chunk, attempt }
            | Transition::ChunkDone { chunk, attempt, .. } => {
                let plan = self
                    .chunk_plans
                    .get(&chunk.id.table)
                    .ok_or_else(|| invalid("unplanned table"))?;
                if chunk.id.plan != plan.plan
                    || chunk.id.ordinal >= plan.chunks as u64
                    || *attempt == 0
                {
                    return Err(invalid("plan identity/attempt mismatch"));
                }
                let key = id(chunk)?;
                let old = self.chunk_attempts.get(&key);
                if old.is_some_and(|old| old.chunk != *chunk) {
                    return Err(invalid("attempt bounds mismatch"));
                }
                match transition {
                    Transition::ChunkStarted { .. } => {
                        if old.is_some_and(|old| old.attempt >= *attempt) {
                            return Err(invalid("stale/reused attempt"));
                        }
                        self.chunk_attempts.insert(
                            key,
                            ChunkAttempt {
                                chunk: chunk.clone(),
                                attempt: *attempt,
                                done: None,
                            },
                        );
                    }
                    Transition::ChunkDone {
                        high_lsn,
                        chunk_rows,
                        ..
                    } => {
                        if *high_lsn == 0
                            || !old.is_some_and(|old| old.attempt == *attempt && old.done.is_none())
                        {
                            return Err(invalid("Done without matching unfinished Started"));
                        }
                        self.chunk_attempts
                            .get_mut(&key)
                            .expect("checked Started")
                            .done = Some(ChunkCompletion {
                            high_lsn: *high_lsn,
                            chunk_rows: chunk_rows.clone(),
                        });
                    }
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        }
        Ok(())
    }
    /// Includes never-started ranges and the latest Started without Done.
    pub fn unfinished_chunks(&self, plan: &ChunkPlan) -> Result<Vec<ChunkRange>> {
        plan.validate()?;
        let bytes = serde_json::to_vec(plan)?;
        if self.chunk_plans.get(&plan.table.table) != Some(&plan.reference(&bytes)) {
            return Err(invalid("resume requires the saved plan"));
        }
        plan.ranges
            .iter()
            .filter_map(|range| match id(range) {
                Err(e) => Some(Err(e)),
                Ok(key) => (!self
                    .chunk_attempts
                    .get(&key)
                    .is_some_and(|a| a.done.is_some()))
                .then(|| Ok(range.clone())),
            })
            .collect()
    }
}

#[derive(Clone)]
pub(crate) struct ChunkBuffer {
    store: Arc<dyn BufferStore>,
    config: reader::ReaderConfig,
    order: Arc<dyn PositionOrder + Send + Sync>,
}

impl Journal {
    /// Bind the authoritative buffer stream before publishing/replaying Done.
    pub fn with_chunk_buffer(
        mut self,
        store: Arc<dyn BufferStore>,
        config: reader::ReaderConfig,
        order: Arc<dyn PositionOrder + Send + Sync>,
    ) -> Self {
        self.chunk_buffer = Some(ChunkBuffer {
            store,
            config,
            order,
        });
        self
    }
    fn plan_key(&self, reference: &ChunkPlanRef) -> object_store::path::Path {
        object_store::path::Path::from(format!(
            "{}-chunk-plans/{}.json",
            self.namespace(),
            reference.digest
        ))
    }
    pub async fn publish_chunk_plan(&self, plan: &ChunkPlan) -> Result<ChunkPlanRef> {
        plan.validate()?;
        let bytes = serde_json::to_vec(plan)?;
        let reference = plan.reference(&bytes);
        self.create(&self.plan_key(&reference), bytes).await?;
        Ok(reference)
    }
    pub async fn chunk_plan(&self, reference: &ChunkPlanRef) -> Result<ChunkPlan> {
        reference.validate()?;
        let bytes = self
            .read_bounded(&self.plan_key(reference), PLAN_BYTES)
            .await?
            .ok_or_else(|| invalid("missing durable plan object"))?;
        let plan: ChunkPlan = serde_json::from_slice(&bytes)?;
        plan.validate()?;
        if plan.reference(&bytes) != *reference {
            return Err(invalid("plan object digest/identity mismatch"));
        }
        Ok(plan)
    }
    pub(crate) async fn validate_chunk_objects(
        &self,
        state: &Materialized,
        transition: &Transition,
    ) -> Result<()> {
        match transition {
            Transition::VerificationCompleted { report } => {
                self.verification_report(report).await?;
            }
            Transition::ChunkPlanned { plan } => {
                self.chunk_plan(plan).await?;
            }
            Transition::ChunkStarted { chunk, .. } | Transition::ChunkDone { chunk, .. } => {
                let reference = state
                    .chunk_plans
                    .get(&chunk.id.table)
                    .ok_or_else(|| invalid("missing saved plan"))?;
                let plan = self.chunk_plan(reference).await?;
                if plan
                    .ranges
                    .get(usize::try_from(chunk.id.ordinal).map_err(invalid)?)
                    != Some(chunk)
                {
                    return Err(invalid("bounds differ from durable plan"));
                }
                if let Transition::ChunkDone {
                    attempt,
                    high_lsn,
                    chunk_rows,
                    ..
                } = transition
                {
                    self.validate_chunk_rows(chunk, *attempt, *high_lsn, chunk_rows)
                        .await?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    async fn validate_chunk_rows(
        &self,
        chunk: &ChunkRange,
        attempt: u64,
        high: u64,
        reference: &ChunkRowsRef,
    ) -> Result<()> {
        let buffer = self
            .chunk_buffer
            .as_ref()
            .ok_or_else(|| invalid("Done requires an authoritative buffer binding"))?;
        let bytes = buffer
            .store
            .get(&buffer.config.manifest_key(reference.mseq))
            .await
            .map_err(invalid)?
            .ok_or_else(|| invalid("ChunkRows manifest is not durable"))?;
        let entry = format::ManifestEntry::from_canonical_json(&bytes).map_err(invalid_format)?;
        let segment_key = format::segment_key(
            &buffer.config.prefix,
            &buffer.config.migration_id,
            &buffer.config.stream,
            &entry.first_pos,
            &entry.last_pos,
            entry.epoch,
        );
        // An isolated reference can name either a normal segment or the one
        // equal-H slot. Both keys are derived, never taken from the reference.
        let equal_h_key = format::boundary_segment_key(
            segment_key.clone(),
            &entry.first_pos,
            Some(&entry.first_pos),
        );
        if entry.mseq != reference.mseq
            || entry.digest().map_err(invalid_format)? != reference.manifest_digest
            || (entry.segment_key != segment_key && entry.segment_key != equal_h_key)
        {
            return Err(invalid("ChunkRows manifest identity/digest"));
        }
        let (mut decoder, _) = reader::verify_segment(
            buffer.store.as_ref(),
            &buffer.config,
            &entry,
            None,
            buffer.order.as_ref(),
        )
        .await
        .map_err(|error| match error {
            reader::Error::Format(error) => invalid_format(error),
            other => invalid(other),
        })?;
        let mut identity = Sha256::new();
        identity.update(b"skedaddle-chunk-record-v1\0");
        identity.update(serde_json::to_vec(&chunk.id)?);
        identity.update(attempt.to_be_bytes());
        identity.update(high.to_be_bytes());
        let expected: Vec<u8> = identity.finalize().to_vec();
        let position = Position::new(buffer.order.domain(), high.to_be_bytes());
        let mut matches = 0;
        while let Some(record) = decoder.next_record().map_err(invalid_format)? {
            if let format::SegmentRecord::ChunkRows(txn) = record
                && txn.txn.0 == expected
                && txn.commit_position == position
                && txn
                    .events
                    .iter()
                    .all(|event| event.table.as_deref() == Some(chunk.id.table.as_str()))
            {
                matches += 1;
            }
        }
        if matches != 1 {
            return Err(invalid("manifest has no unique matching ChunkRows at H"));
        }
        Ok(())
    }
}
