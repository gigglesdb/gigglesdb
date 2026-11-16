//! Durable staging makes unordered partition replay safe: no batch is published
//! until its entire partition has an immutable ready record. Incomplete attempts
//! can be reread; ready attempts are retained until snapshot recovery is retired.
//! One owner must fence this store, journal and buffer writer together. A local
//! store survives process crashes; host loss requires restoring that same store.
use super::{Plan, PlannedPartition, canonical, timestamp, version};
use crate::{
    buffer::{backends::BufferStore, writer::BufferWriter},
    engine::{Capture, Runtime},
    event::{AssembledTxn, Event, EventKind, Position, Row, SourceIdentity, TxnId},
    spanner::{
        capture::{SpannerCapture, convert_value},
        client::{Client, Error},
        frontier::{DuplicateFilter, Frontier, SpannerPosition},
        records::ColumnType,
        stream::{Config, PartitionReader},
    },
};
use futures::{StreamExt, stream};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

type Result<T> = std::result::Result<T, Error>;
pub const PROGRESS_STREAM: &str = "spanner-snapshot-copy";
#[derive(Debug)]
pub enum CopyError {
    InvalidLimits,
    InvalidProgress,
    BindingMismatch,
    StagingMissing,
    StagingCorrupt,
    OversizedRow { bytes: usize, limit: usize },
}
impl std::fmt::Display for CopyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Spanner snapshot copy: {self:?}")
    }
}
impl std::error::Error for CopyError {}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Limits {
    pub concurrency: usize,
    pub rows: usize,
    /// Maximum canonical AssembledTxn bytes, including identity and framing.
    pub bytes: usize,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Progress {
    version: u32,
    plan: Value,
    limits: Limits,
    partition: usize,
    batch: u64,
    manifest: Option<(u64, [u8; 32])>,
}
impl Progress {
    pub fn encode(&self) -> Result<String> {
        Ok(String::from_utf8(canonical(self)?)?)
    }
    pub fn decode(marker: &str, plan: &Plan, limits: Limits) -> Result<Self> {
        let value: Value = serde_json::from_str(marker)?;
        version(&value, "copy progress")?;
        let p: Self = serde_json::from_value(value)?;
        if p.encode()? != marker
            || p.limits != limits
            || canonical(&p.plan)? != plan.encode()?
            || p.partition > plan.partitions().len()
            || (p.partition == plan.partitions().len() && p.batch != 0)
            || (p.batch > 0 && p.manifest.is_none())
        {
            return Err(CopyError::InvalidProgress.into());
        }
        Ok(p)
    }
    pub fn completed_partitions(&self) -> usize {
        self.partition
    }
    pub fn next_batch(&self) -> u64 {
        self.batch
    }
}
/// The journal equivalent of Runtime::marker also allows real Journal tests.
/// Implementations must return only after the progress entry is durable.
pub trait ProgressSink {
    fn save(&mut self, progress: &Progress) -> impl Future<Output = Result<()>>;
}
impl ProgressSink for Runtime {
    async fn save(&mut self, progress: &Progress) -> Result<()> {
        self.marker(
            PROGRESS_STREAM,
            progress.encode()?,
            format!("{}:{}", progress.partition, progress.batch),
        )
        .await?;
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Ready {
    version: u32,
    plan_digest: String,
    partition: String,
    attempt: String,
    batches: u64,
    chain: [u8; 32],
    limits: Limits,
}
fn identity(plan: &Plan, partition: usize, batch: u64) -> Vec<u8> {
    let mut id = b"\0skedaddle/snapshot/v1/".to_vec();
    id.extend_from_slice(plan.digest.as_bytes());
    id.extend_from_slice(&(partition as u64).to_be_bytes());
    id.extend_from_slice(&batch.to_be_bytes());
    id
}
fn chain(previous: [u8; 32], bytes: &[u8]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(previous);
    hash.update(Sha256::digest(bytes));
    hash.finalize().into()
}
fn check_row_bytes(bytes: usize, limit: usize) -> Result<()> {
    if bytes > limit {
        return Err(CopyError::OversizedRow { bytes, limit }.into());
    }
    Ok(())
}
fn sdk_type(ty: &google_cloud_spanner::value::Type) -> Result<serde_json::Map<String, Value>> {
    let ty: google_cloud_spanner::model::Type = ty.clone().into();
    let mut wire = serde_json::to_value(&ty)?
        .as_object()
        .ok_or(CopyError::StagingCorrupt)?
        .clone();
    // The SDK serializes enums numerically; change-stream conversion uses names.
    wire.insert(
        "code".into(),
        ty.code.name().ok_or(CopyError::StagingCorrupt)?.into(),
    );
    Ok(wire)
}

pub struct Copy {
    client: Arc<Client>,
    plan: Plan,
    staging: Arc<dyn BufferStore>,
    progress: Progress,
    executions: Vec<AtomicU64>,
}
impl Copy {
    /// saved must come from the authoritative journal, never a stage directory.
    /// Retain both staging and writer redelivery history throughout recovery.
    pub async fn open(
        client: Arc<Client>,
        plan: Plan,
        staging: Arc<dyn BufferStore>,
        limits: Limits,
        saved: Option<&str>,
    ) -> Result<Self> {
        if limits.concurrency == 0 || limits.rows == 0 || limits.bytes == 0 {
            return Err(CopyError::InvalidLimits.into());
        }
        let plan = Plan::restore(&client, &plan.encode()?, plan.binding()).await?;
        let progress = match saved {
            Some(marker) => Progress::decode(marker, &plan, limits)?,
            None => Progress {
                version: 1,
                plan: serde_json::to_value(&plan)?,
                limits,
                partition: 0,
                batch: 0,
                manifest: None,
            },
        };
        Ok(Self {
            executions: (0..plan.partitions().len())
                .map(|_| AtomicU64::new(0))
                .collect(),
            client,
            plan,
            staging,
            progress,
        })
    }
    pub fn progress(&self) -> &Progress {
        &self.progress
    }
    /// Actual saved-partition execution attempts in this driver instance.
    /// Staging reuse and completed partitions do not increment these counters.
    pub fn partition_executions(&self) -> Vec<u64> {
        self.executions
            .iter()
            .map(|n| n.load(Ordering::Relaxed))
            .collect()
    }
    /// Constructs both stream reader and capture from the verified plan's T.
    /// Consume stream steps only after run succeeds; reader creation starts no RPC.
    pub fn handoff(
        &self,
        writer: BufferWriter,
        config: Config,
        capacity: usize,
    ) -> Result<SpannerCapture> {
        self.plan.check_client(&self.client)?;
        let t = timestamp(self.plan.binding().timestamp());
        let reader = PartitionReader::new(
            self.client.clone(),
            t,
            Frontier::new(t),
            DuplicateFilter::restore(capacity, 1024, [])?,
            config,
        )?;
        SpannerCapture::new(
            self.client.snapshot_connection().1,
            t,
            reader,
            writer,
            capacity,
        )
    }
    fn source(&self) -> SourceIdentity {
        let e = self.client.snapshot_connection().1;
        SourceIdentity {
            engine: "spanner".into(),
            system_id: format!("{}/{}", e.project, e.instance),
            lineage: timestamp(self.plan.binding().timestamp()).to_string(),
            db_name: e.database.clone(),
            stream: e.change_stream.clone(),
        }
    }
    fn prefix(&self, p: &PlannedPartition) -> String {
        format!("snapshot/{}/{}", self.plan.digest, p.id())
    }
    fn transaction(&self, partition: usize, batch: u64, rows: Vec<Row>) -> Result<AssembledTxn> {
        let source = self.source();
        let id = TxnId(identity(&self.plan, partition, batch));
        let position = Position::new(
            self.plan.binding().domain(),
            SpannerPosition::Txn {
                commit_ts: timestamp(self.plan.binding().timestamp()),
                server_transaction_id: id.0.clone(),
            }
            .encode(),
        );
        let name = &self.plan.partitions()[partition].table;
        let table = self
            .plan
            .tables()
            .iter()
            .find(|t| &t.name == name)
            .ok_or(CopyError::BindingMismatch)?;
        let events = rows
            .into_iter()
            .enumerate()
            .map(|(ordinal, row)| Event {
                source: source.clone(),
                txn: id.clone(),
                ordinal_in_txn: ordinal as u64,
                position: position.clone(),
                schema_version: 0,
                kind: EventKind::ChunkRows,
                table: Some(if name.schema.is_empty() || name.schema == "public" {
                    name.name.clone()
                } else {
                    format!("{}.{}", name.schema, name.name)
                }),
                key: row
                    .iter()
                    .filter(|(name, _)| {
                        table
                            .columns
                            .iter()
                            .any(|c| &c.name == *name && c.key_ordinal.is_some())
                    })
                    .map(|(name, v)| (name.clone(), v.clone()))
                    .collect(),
                before: None,
                after: Some(row),
                unchanged_cols: Default::default(),
            })
            .collect();
        Ok(AssembledTxn::new(source, id, position, None, events)?)
    }
    async fn ready(&self, p: &PlannedPartition) -> Result<Option<Ready>> {
        let Some(bytes) = self
            .staging
            .get(&format!("{}/ready", self.prefix(p)))
            .await?
        else {
            return Ok(None);
        };
        let value: Value = serde_json::from_slice(&bytes)?;
        version(&value, "copy staging")?;
        let ready: Ready = serde_json::from_value(value)?;
        if canonical(&ready)? != bytes
            || ready.plan_digest != self.plan.digest
            || ready.partition != p.id
            || ready.limits != self.progress.limits
            || uuid::Uuid::parse_str(&ready.attempt).is_err()
        {
            return Err(CopyError::StagingCorrupt.into());
        }
        Ok(Some(ready))
    }
    async fn stage(&self, partition: usize) -> Result<Ready> {
        let p = &self.plan.partitions()[partition];
        if let Some(ready) = self.ready(p).await? {
            return Ok(ready);
        }
        if partition == self.progress.partition && self.progress.batch != 0 {
            return Err(CopyError::StagingMissing.into());
        }
        let mut ready = Ready {
            version: 1,
            plan_digest: self.plan.digest.clone(),
            partition: p.id.clone(),
            attempt: uuid::Uuid::new_v4().to_string(),
            batches: 0,
            chain: [0; 32],
            limits: self.progress.limits,
        };
        self.executions[partition].fetch_add(1, Ordering::Relaxed);
        let mut result = self.plan.execute(&self.client, p.id()).await?;
        let mut batch = Vec::new();
        while let Some(row) = result.next().await {
            let row = row?;
            let columns = &self
                .plan
                .tables()
                .iter()
                .find(|t| t.name == p.table)
                .ok_or(CopyError::BindingMismatch)?
                .columns;
            if row.raw_values().len() != columns.len()
                || result.0.metadata().column_types().len() != columns.len()
            {
                return Err(CopyError::StagingCorrupt.into());
            }
            let mut converted = Row::new();
            for (i, ((column, ty), value)) in columns
                .iter()
                .zip(result.0.metadata().column_types())
                .zip(row.raw_values())
                .enumerate()
            {
                let column = ColumnType {
                    name: column.name.clone(),
                    r#type: sdk_type(ty)?,
                    is_primary_key: column.key_ordinal.is_some(),
                    ordinal_position: i as u64 + 1,
                };
                // Let the shared conversion reject unsupported types, including NULLs.
                convert_value(&column, &Value::Null)?;
                let input = if value.is_null() {
                    Value::Null
                } else if let Some(s) = value.as_str() {
                    Value::String(s.into())
                } else if let Some(b) = value.as_bool() {
                    Value::Bool(b)
                } else if let Some(n) = value.as_f64() {
                    serde_json::Number::from_f64(n)
                        .map(Value::Number)
                        .unwrap_or_else(|| {
                            Value::String(
                                if n.is_nan() {
                                    "NaN"
                                } else if n > 0.0 {
                                    "Infinity"
                                } else {
                                    "-Infinity"
                                }
                                .into(),
                            )
                        })
                } else {
                    return Err(CopyError::StagingCorrupt.into());
                };
                converted.insert(column.name.clone(), convert_value(&column, &input)?);
            }
            let single = self
                .transaction(partition, ready.batches, vec![converted.clone()])?
                .canonical_bytes()?
                .len();
            check_row_bytes(single, ready.limits.bytes)?;
            let mut candidate = batch.clone();
            candidate.push(converted.clone());
            if !batch.is_empty()
                && (candidate.len() > ready.limits.rows
                    || self
                        .transaction(partition, ready.batches, candidate)?
                        .canonical_bytes()?
                        .len()
                        > ready.limits.bytes)
            {
                self.stage_batch(partition, &mut ready, std::mem::take(&mut batch))
                    .await?;
            }
            batch.push(converted);
        }
        if !batch.is_empty() {
            self.stage_batch(partition, &mut ready, batch).await?;
        }
        // Create-only ready is the commit point for the entire immutable attempt.
        // A cancelled PUT may finish later: the winner always determines replay.
        match self
            .staging
            .put_create_only(&format!("{}/ready", self.prefix(p)), &canonical(&ready)?)
            .await
        {
            Ok(_) | Err(crate::buffer::backends::Error::Conflict) => {}
            Err(e) => return Err(e.into()),
        }
        self.ready(p)
            .await?
            .ok_or_else(|| CopyError::StagingMissing.into())
    }
    async fn stage_batch(
        &self,
        partition: usize,
        ready: &mut Ready,
        batch: Vec<Row>,
    ) -> Result<()> {
        let bytes = self
            .transaction(partition, ready.batches, batch)?
            .canonical_bytes()?;
        self.staging
            .put_create_only(
                &format!(
                    "{}/{}/{:020}",
                    self.prefix(&self.plan.partitions()[partition]),
                    ready.attempt,
                    ready.batches
                ),
                &bytes,
            )
            .await?;
        ready.chain = chain(ready.chain, &bytes);
        ready.batches = ready
            .batches
            .checked_add(1)
            .ok_or(CopyError::StagingCorrupt)?;
        Ok(())
    }
    async fn batch(&self, partition: usize, ready: &Ready, batch: u64) -> Result<AssembledTxn> {
        let bytes = self
            .staging
            .get(&format!(
                "{}/{}/{batch:020}",
                self.prefix(&self.plan.partitions()[partition]),
                ready.attempt
            ))
            .await?
            .ok_or(CopyError::StagingMissing)?;
        if bytes.len() > ready.limits.bytes {
            return Err(CopyError::StagingCorrupt.into());
        }
        let txn = AssembledTxn::from_canonical_bytes(&bytes)?;
        let expected = self.transaction(
            partition,
            batch,
            txn.events
                .iter()
                .map(|e| e.after.clone().ok_or(CopyError::StagingCorrupt))
                .collect::<std::result::Result<_, _>>()?,
        )?;
        if txn != expected || txn.events.is_empty() || txn.events.len() > ready.limits.rows {
            return Err(CopyError::StagingCorrupt.into());
        }
        Ok(txn)
    }
    pub async fn run(
        &mut self,
        capture: &mut SpannerCapture,
        sink: &mut impl ProgressSink,
    ) -> Result<()> {
        if capture.source() != &self.source()
            || capture.position_order().domain() != self.plan.binding().domain()
        {
            return Err(CopyError::BindingMismatch.into());
        }
        if let Some((mseq, digest)) = self.progress.manifest {
            let checkpoint = capture
                .writer_checkpoint()?
                .ok_or(CopyError::InvalidProgress)?;
            if !checkpoint.redelivery.iter().any(|r| {
                r.source == self.source()
                    && r.chunk_rows
                    && r.manifest
                        .as_ref()
                        .is_some_and(|m| m.mseq == mseq && m.manifest_digest == digest)
            }) {
                return Err(CopyError::InvalidProgress.into());
            }
        }
        // Journal the exact plan, T and domain before executing any partition.
        sink.save(&self.progress).await?;
        while self.progress.partition < self.plan.partitions().len() {
            let end = self.plan.partitions().len().min(
                self.progress
                    .partition
                    .saturating_add(self.progress.limits.concurrency),
            );
            let prepared = stream::iter(self.progress.partition..end)
                .map(|i| self.stage(i))
                .buffered(self.progress.limits.concurrency)
                .collect::<Vec<_>>()
                .await;
            for ready in prepared {
                let ready = ready?;
                let partition = self.progress.partition;
                if self.progress.batch > ready.batches {
                    return Err(CopyError::InvalidProgress.into());
                }
                let mut hash = [0; 32];
                for n in 0..ready.batches {
                    let txn = self.batch(partition, &ready, n).await?;
                    hash = chain(hash, &txn.canonical_bytes()?);
                    if n < self.progress.batch {
                        continue;
                    }
                    let reference = capture.publish(txn.commit_position.clone(), txn).await?;
                    let mut next = self.progress.clone();
                    next.batch = n + 1;
                    next.manifest = Some((reference.mseq, reference.manifest_digest));
                    sink.save(&next).await?;
                    self.progress = next;
                }
                if hash != ready.chain {
                    return Err(CopyError::StagingCorrupt.into());
                }
                let mut next = self.progress.clone();
                next.partition += 1;
                next.batch = 0;
                sink.save(&next).await?;
                self.progress = next;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{event::PositionOrder, spanner::frontier::SpannerOrder};
    #[test]
    fn sdk_numeric_enums_use_capture_names_without_accepting_unsupported_null() {
        use google_cloud_spanner::types;
        let column = |ty: &google_cloud_spanner::value::Type| ColumnType {
            name: "v".into(),
            r#type: sdk_type(ty).unwrap(),
            is_primary_key: false,
            ordinal_position: 1,
        };
        for (ty, code, input) in [
            (types::int64(), "INT64", serde_json::json!("42")),
            (types::string(), "STRING", serde_json::json!("seed")),
            (types::bool(), "BOOL", serde_json::json!(true)),
            (
                types::timestamp(),
                "TIMESTAMP",
                serde_json::json!("2026-01-01T00:00:00Z"),
            ),
        ] {
            let model: google_cloud_spanner::model::Type = ty.clone().into();
            assert!(serde_json::to_value(model).unwrap()["code"].is_number());
            let col = column(&ty);
            assert_eq!(col.r#type["code"], code);
            convert_value(&col, &input).unwrap();
        }
        assert!(convert_value(&column(&types::array(types::string())), &Value::Null).is_err());
    }
    fn limits() -> Limits {
        Limits {
            concurrency: 2,
            rows: 10,
            bytes: 4096,
        }
    }
    #[test]
    fn batch_identity_is_stable_unique_and_strictly_ordered_at_t() {
        let plan = super::super::tests::fixture();
        let t = timestamp(plan.binding().timestamp());
        let order = SpannerOrder::new("p", "i", "d", "s", t).unwrap();
        let mut previous: Option<Vec<u8>> = None;
        for (partition, batch) in [(0, 0), (0, 1), (0, 255), (0, 256), (0, u64::MAX), (1, 0)] {
            let id = identity(&plan, partition, batch);
            assert_eq!(id, identity(&plan, partition, batch));
            let position = SpannerPosition::Txn {
                commit_ts: t,
                server_transaction_id: id,
            }
            .encode();
            if let Some(previous) = previous {
                assert!(order.compare(&previous, &position).unwrap().is_lt());
            }
            assert!(
                order
                    .compare(
                        &position,
                        &SpannerPosition::Txn {
                            commit_ts: t + 1,
                            server_transaction_id: vec![],
                        }
                        .encode()
                    )
                    .unwrap()
                    .is_lt()
            );
            previous = Some(position);
        }
    }
    #[test]
    fn oversized_row_is_typed_and_exact_byte_limit_is_allowed() {
        check_row_bytes(4096, 4096).unwrap();
        let error = check_row_bytes(4097, 4096).unwrap_err();
        assert!(matches!(
            error.downcast_ref::<CopyError>(),
            Some(CopyError::OversizedRow {
                bytes: 4097,
                limit: 4096
            })
        ));
    }
    #[test]
    fn progress_round_trip_and_reject_corrupt_or_foreign_binding() {
        let plan = super::super::tests::fixture();
        let p = Progress {
            version: 1,
            plan: serde_json::to_value(&plan).unwrap(),
            limits: limits(),
            partition: 0,
            batch: 3,
            manifest: Some((2, [7; 32])),
        };
        let marker = p.encode().unwrap();
        let decoded = Progress::decode(&marker, &plan, limits()).unwrap();
        assert_eq!(decoded.encode().unwrap(), marker);
        assert_eq!(decoded.next_batch(), 3);
        assert_eq!(decoded.completed_partitions(), 0);
        for field in ["domain", "timestamp"] {
            let mut changed = p.clone();
            changed.plan["plan"]["binding"][field] = "foreign".into();
            assert!(Progress::decode(&changed.encode().unwrap(), &plan, limits()).is_err());
        }
        for version in [None, Some(0), Some(2)] {
            let mut value = serde_json::to_value(&p).unwrap();
            value.as_object_mut().unwrap().remove("version");
            if let Some(v) = version {
                value["version"] = v.into();
            }
            assert!(
                Progress::decode(
                    &String::from_utf8(canonical(&value).unwrap()).unwrap(),
                    &plan,
                    limits()
                )
                .is_err()
            );
        }
        let mut bad = p.clone();
        bad.partition = plan.partitions().len() + 1;
        assert!(Progress::decode(&bad.encode().unwrap(), &plan, limits()).is_err());
        bad = p.clone();
        bad.manifest = None;
        assert!(Progress::decode(&bad.encode().unwrap(), &plan, limits()).is_err());
        assert!(Progress::decode(&(marker + " "), &plan, limits()).is_err());
        assert!(
            Progress::decode(
                &p.encode().unwrap(),
                &plan,
                Limits {
                    bytes: 4095,
                    ..limits()
                }
            )
            .is_err()
        );
    }
}
