//! Durable snapshot plans and current attempts. Large samples never enter the
//! local projection: immutable plan objects are capped at 4 MiB, 12,000 ranges.
use super::{Error, Materialized, Result, Transition, budget, journal::Journal};
use crate::{
    buffer::{backends::BufferStore, format, reader},
    event::{ContentDigest, Position, PositionOrder},
    mysql::chunk_key::MysqlTableKey,
    pg::chunk::{self, ChunkError, ChunkId, ChunkRange, Key, TableKey, Window, plan_chunks},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::Arc;

pub const PLAN_BYTES: usize = 4 * 1024 * 1024;
pub const PLAN_FORMAT_VERSION: u16 = 2;
const PLAN_FORMAT: &str = "skedaddle-chunk-plan";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum SourceTableKey {
    Postgres(TableKey),
    Mysql(MysqlTableKey),
}
impl From<TableKey> for SourceTableKey {
    fn from(table: TableKey) -> Self {
        Self::Postgres(table)
    }
}
impl From<MysqlTableKey> for SourceTableKey {
    fn from(table: MysqlTableKey) -> Self {
        Self::Mysql(table)
    }
}
impl SourceTableKey {
    pub fn event_table(&self) -> &str {
        match self {
            Self::Postgres(t) => &t.table,
            Self::Mysql(t) => &t.event_table,
        }
    }
    pub fn postgres(&self) -> chunk::Result<&TableKey> {
        match self {
            Self::Postgres(t) => Ok(t),
            Self::Mysql(_) => Err(ChunkError::UnsupportedSource(
                "MySQL plan not yet supported by PostgreSQL adapter",
            )),
        }
    }
    fn ranges(&self, size: usize, samples: &[Key]) -> chunk::Result<Vec<ChunkRange>> {
        let Self::Mysql(table) = self else {
            // Preserve the pre-v2 PostgreSQL logical hash byte for byte.
            return plan_chunks(self.postgres()?, size, samples);
        };
        table.validate().map_err(ChunkError::MysqlKey)?;
        if size == 0 || i64::try_from(size).is_err() {
            return Err(ChunkError::Invalid("chunk size"));
        }
        let mut seen = std::collections::BTreeSet::new();
        for key in samples {
            table.validate_key(key).map_err(ChunkError::MysqlKey)?;
            if !seen.insert(chunk::key_bytes(key)?) {
                return Err(ChunkError::Invalid("duplicate sample boundary"));
            }
        }
        let mut hash = Sha256::new();
        hash.update(b"skedaddle-chunk-plan-mysql-v1\0");
        hash.update(serde_json::to_vec(&(table, size, samples))?);
        let plan = format!("{:x}", hash.finalize());
        Ok((0..=samples.len())
            .map(|i| ChunkRange {
                id: ChunkId {
                    table: table.event_table.clone(),
                    plan: plan.clone(),
                    ordinal: i as u64,
                },
                lo: i.checked_sub(1).map(|j| samples[j].clone()),
                hi: samples.get(i).cloned(),
            })
            .collect())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PlanEnvelope<T> {
    format: String,
    version: u16,
    plan: T,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChunkPlan {
    pub table: SourceTableKey,
    pub chunk_size: usize,
    /// Source order, never sorted or regenerated during recovery.
    pub samples: Vec<Key>,
    pub ranges: Vec<ChunkRange>,
}
impl ChunkPlan {
    pub fn new(
        table: impl Into<SourceTableKey>,
        chunk_size: usize,
        samples: Vec<Key>,
    ) -> Result<Self> {
        let table = table.into();
        let ranges = table.ranges(chunk_size, &samples).map_err(invalid)?;
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
            != self
                .table
                .ranges(self.chunk_size, &self.samples)
                .map_err(invalid)?
        {
            return Err(invalid("samples/plan mismatch"));
        }
        budget::check(
            budget::Budget::Chunks,
            self.object_bytes()?.len(),
            PLAN_BYTES,
        )
    }
    fn object_bytes(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(&PlanEnvelope {
            format: PLAN_FORMAT.into(),
            version: PLAN_FORMAT_VERSION,
            plan: self,
        })?)
    }
    fn from_object_bytes(bytes: &[u8]) -> Result<Self> {
        // Decode only the version header before attempting the typed plan body.
        #[derive(Deserialize)]
        struct Header {
            version: Option<serde_json::Value>,
        }
        let header: Header = serde_json::from_slice(bytes)?;
        let found = header
            .version
            .and_then(|v| v.as_u64().and_then(|n| u16::try_from(n).ok()));
        if found != Some(PLAN_FORMAT_VERSION) {
            return Err(Error::UnsupportedFormatVersion {
                format: PLAN_FORMAT,
                found,
                expected: PLAN_FORMAT_VERSION,
            });
        }
        let envelope: PlanEnvelope<Self> = serde_json::from_slice(bytes)?;
        if envelope.format != PLAN_FORMAT {
            return Err(invalid("plan object format"));
        }
        envelope.plan.validate()?;
        Ok(envelope.plan)
    }
    /// The window removes touched mapped identities; it never classifies ranges.
    pub(crate) fn window(
        &self,
        range: ChunkRange,
        rows: Vec<crate::event::Row>,
        attempt: u64,
    ) -> chunk::Result<Window> {
        match &self.table {
            SourceTableKey::Postgres(table) => Window::new(table.clone(), range, rows, attempt),
            SourceTableKey::Mysql(table) => Window::with_keys(
                table.event_table.clone(),
                Box::new(table.clone()),
                range,
                rows,
                attempt,
            ),
        }
    }
    fn reference(&self, bytes: &[u8]) -> ChunkPlanRef {
        ChunkPlanRef {
            table: self.table.event_table().into(),
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

#[cfg(test)]
mod plan_tests {
    use super::*;
    use crate::{
        mapping::Value,
        mysql::{chunk_key::tests::table, schema::Collation},
    };

    #[test]
    fn plan_envelope_round_trip_and_versions_precede_body_decode() {
        let plan = ChunkPlan::new(table(), 10, vec![vec![Value::Int4(7)]]).unwrap();
        let bytes = plan.object_bytes().unwrap();
        assert_eq!(ChunkPlan::from_object_bytes(&bytes).unwrap(), plan);
        let object: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(object["format"], PLAN_FORMAT);
        assert_eq!(object["version"], 2);
        for (bytes, found) in [
            (br#"{"plan":false}"#.as_slice(), None),
            (br#"{"version":1,"plan":false}"#.as_slice(), Some(1)),
            (br#"{"version":3,"plan":false}"#.as_slice(), Some(3)),
        ] {
            assert!(matches!(ChunkPlan::from_object_bytes(bytes),
                Err(Error::UnsupportedFormatVersion { format: PLAN_FORMAT, found: v, expected: 2 }) if v == found));
        }
        assert!(matches!(
            ChunkPlan::from_object_bytes(&serde_json::to_vec(&plan).unwrap()),
            Err(Error::UnsupportedFormatVersion { found: None, .. })
        ));
        assert!(matches!(
            ChunkPlan::from_object_bytes(
                br#"{"format":"skedaddle-chunk-plan","version":2,"plan":false}"#
            ),
            Err(Error::Json(_))
        ));
        let mut foreign = object;
        foreign["format"] = "foreign".into();
        assert!(ChunkPlan::from_object_bytes(&serde_json::to_vec(&foreign).unwrap()).is_err());
    }

    #[test]
    fn mysql_plan_hash_has_own_domain_and_binds_every_key_field() {
        let mut t = table();
        t.columns[0].character_set = Some("utf8mb4".into());
        t.columns[0].collation = Some(Collation {
            name: "utf8mb4_bin".into(),
            id: 46,
        });
        let samples = vec![vec![Value::Int4(7)], vec![Value::Int4(9)]];
        let hash = |t: MysqlTableKey| {
            ChunkPlan::new(t, 10, samples.clone()).unwrap().ranges[0]
                .id
                .plan
                .clone()
        };
        let expected = hash(t.clone());
        let recipe = serde_json::to_vec(&(&t, 10usize, &samples)).unwrap();
        let digest = |domain: &[u8], body: &[u8]| {
            let mut h = Sha256::new();
            h.update(domain);
            h.update(body);
            format!("{:x}", h.finalize())
        };
        assert_eq!(
            expected,
            digest(b"skedaddle-chunk-plan-mysql-v1\0", &recipe)
        );
        assert_ne!(expected, digest(b"skedaddle-chunk-plan-v1\0", &recipe));
        // Each descriptor field, including collation's wire ID, is bound.
        for field in [
            "event_table",
            "schema",
            "table",
            "name",
            "data_type",
            "column_type",
            "character_set",
            "collation_name",
            "collation_id",
            "type_id",
        ] {
            let mut changed = t.clone();
            match field {
                "event_table" => changed.event_table.push('x'),
                "schema" => changed.schema.push('x'),
                "table" => changed.table.push('x'),
                "name" => changed.columns[0].name.push('x'),
                "data_type" => changed.columns[0].data_type = "bigint".into(),
                "column_type" => changed.columns[0].column_type = "bigint unsigned".into(),
                "character_set" => changed.columns[0].character_set = Some("utf8mb3".into()),
                "collation_name" => changed.columns[0]
                    .collation
                    .as_mut()
                    .unwrap()
                    .name
                    .push('x'),
                "collation_id" => changed.columns[0].collation.as_mut().unwrap().id += 1,
                "type_id" => changed.columns[0].type_id.push('x'),
                _ => unreachable!(),
            }
            assert_ne!(hash(changed), expected, "{field}");
        }
        let changed_direction = String::from_utf8(recipe.clone())
            .unwrap()
            .replace("\"Asc\"", "\"Desc\"");
        assert_ne!(
            expected,
            digest(
                b"skedaddle-chunk-plan-mysql-v1\0",
                changed_direction.as_bytes()
            )
        );
        assert_ne!(
            expected,
            ChunkPlan::new(t.clone(), 11, samples.clone())
                .unwrap()
                .ranges[0]
                .id
                .plan
        );
        let mut reversed = samples.clone();
        reversed.reverse();
        assert_ne!(
            expected,
            ChunkPlan::new(t.clone(), 10, reversed).unwrap().ranges[0]
                .id
                .plan
        );
        let mut changed = samples;
        changed[0][0] = Value::Int4(8);
        assert_ne!(
            expected,
            ChunkPlan::new(t.clone(), 10, changed).unwrap().ranges[0]
                .id
                .plan
        );
        let mut second = t.columns[0].clone();
        second.name.push('2');
        t.columns.push(second);
        let composite_samples = vec![vec![Value::Int4(1), Value::Int4(2)]];
        let original = ChunkPlan::new(t.clone(), 10, composite_samples.clone()).unwrap();
        t.columns.reverse();
        assert_ne!(
            original.ranges[0].id.plan,
            ChunkPlan::new(t, 10, composite_samples).unwrap().ranges[0]
                .id
                .plan
        );
    }

    #[test]
    fn mysql_plan_does_not_enter_postgres_adapters() {
        let plan = ChunkPlan::new(table(), 10, vec![]).unwrap();
        assert!(matches!(
            plan.table.postgres(),
            Err(ChunkError::UnsupportedSource(_))
        ));
    }
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
    #[serde(with = "position_wire")]
    pub high: Position,
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
                        high, chunk_rows, ..
                    } => {
                        if high.order_domain().is_empty()
                            || !old.is_some_and(|old| old.attempt == *attempt && old.done.is_none())
                        {
                            return Err(invalid("Done without matching unfinished Started"));
                        }
                        self.chunk_attempts
                            .get_mut(&key)
                            .expect("checked Started")
                            .done = Some(ChunkCompletion {
                            high: high.clone(),
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
        let bytes = plan.object_bytes()?;
        if self.chunk_plans.get(plan.table.event_table()) != Some(&plan.reference(&bytes)) {
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
        let bytes = plan.object_bytes()?;
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
        let plan = ChunkPlan::from_object_bytes(&bytes)?;
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
                    high,
                    chunk_rows,
                    ..
                } = transition
                {
                    self.validate_chunk_rows(chunk, *attempt, high, chunk_rows)
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
        high: &Position,
        reference: &ChunkRowsRef,
    ) -> Result<()> {
        let buffer = self
            .chunk_buffer
            .as_ref()
            .ok_or_else(|| invalid("Done requires an authoritative buffer binding"))?;
        high.compare(high, buffer.order.as_ref()).map_err(invalid)?;
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
        identity.update(high.bytes());
        let expected: Vec<u8> = identity.finalize().to_vec();
        let position = high;
        let mut matches = 0;
        while let Some(record) = decoder.next_record().map_err(invalid_format)? {
            if let format::SegmentRecord::ChunkRows(txn) = record
                && txn.txn.0 == expected
                && &txn.commit_position == position
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

/// Position's JSON wire is versioned by the enclosing journal generation.
pub(crate) mod position_wire {
    use crate::event::{Position, PositionWireV1};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    pub fn serialize<S: Serializer>(position: &Position, serializer: S) -> Result<S::Ok, S::Error> {
        PositionWireV1::from(position).serialize(serializer)
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Position, D::Error> {
        Position::try_from(PositionWireV1::deserialize(deserializer)?)
            .map_err(serde::de::Error::custom)
    }
}
