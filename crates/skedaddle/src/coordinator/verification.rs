//! V1 durable verification contract. Ordering is deliberately restricted to
//! non-null bool, int2/int4/int8, bytea and UUID, without collations.
use super::{Error, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{cmp::Ordering, collections::BTreeSet};

pub const VERSION: u32 = 1;
pub const REPORT_BYTES: usize = 256 * 1024;
pub const MAX_RANGES: usize = 1024;
pub fn range_identity(scope: &TableScope, lower: &Boundary, upper: &Boundary) -> Result<String> {
    digest(&(VERSION, scope, lower, upper))
}

fn invalid(message: &str) -> Error {
    Error::Rejected(format!("verification: {message}"))
}
fn required(value: &str) -> Result<()> {
    if value.is_empty() {
        Err(invalid("missing identity/evidence"))
    } else {
        Ok(())
    }
}
pub fn digest(value: &impl Serialize) -> Result<String> {
    Ok(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&serde_json::to_value(value)?)?)
    ))
}
fn hash(value: &str) -> Result<()> {
    if value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        Ok(())
    } else {
        Err(invalid("invalid digest"))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub enum KeyType {
    Bool,
    Int2,
    Int4,
    Int8,
    Bytea,
    Uuid,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "value",
    rename_all = "kebab-case",
    deny_unknown_fields
)]
pub enum Scalar {
    Bool(bool),
    Integer(i64),
    Bytea(Vec<u8>),
    Uuid([u8; 16]),
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KeyColumn {
    pub name: String,
    pub kind: KeyType,
    pub descending: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "bound",
    content = "key",
    rename_all = "kebab-case",
    deny_unknown_fields
)]
pub enum Boundary {
    NegativeInfinity,
    Key(Vec<Scalar>),
    PositiveInfinity,
}

impl KeyColumn {
    fn compare(&self, a: &Scalar, b: &Scalar) -> Result<Ordering> {
        let order = match (&self.kind, a, b) {
            (KeyType::Bool, Scalar::Bool(a), Scalar::Bool(b)) => a.cmp(b),
            (
                KeyType::Int2 | KeyType::Int4 | KeyType::Int8,
                Scalar::Integer(a),
                Scalar::Integer(b),
            ) => {
                let valid = |n: i64| match self.kind {
                    KeyType::Int2 => i16::try_from(n).is_ok(),
                    KeyType::Int4 => i32::try_from(n).is_ok(),
                    _ => true,
                };
                if !valid(*a) || !valid(*b) {
                    return Err(invalid("integer key width"));
                }
                a.cmp(b)
            }
            (KeyType::Bytea, Scalar::Bytea(a), Scalar::Bytea(b)) => a.cmp(b),
            (KeyType::Uuid, Scalar::Uuid(a), Scalar::Uuid(b)) => a.cmp(b),
            _ => return Err(invalid("key type mismatch")),
        };
        Ok(if self.descending {
            order.reverse()
        } else {
            order
        })
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableScope {
    pub schema: String,
    pub table: String,
    pub catalog_digest: String,
    pub plan_digest: String,
    pub key: Vec<KeyColumn>,
}
impl TableScope {
    fn validate(&self) -> Result<()> {
        required(&self.schema)?;
        required(&self.table)?;
        hash(&self.catalog_digest)?;
        hash(&self.plan_digest)?;
        let mut names = BTreeSet::new();
        if self.key.is_empty() {
            return Err(invalid("empty key"));
        }
        for column in &self.key {
            required(&column.name)?;
            if !names.insert(&column.name) {
                return Err(invalid("duplicate key column"));
            }
        }
        Ok(())
    }
    fn compare(&self, a: &Boundary, b: &Boundary) -> Result<Ordering> {
        let validate = |bound: &Boundary| -> Result<()> {
            if let Boundary::Key(key) = bound {
                if key.len() != self.key.len() {
                    return Err(invalid("key arity"));
                }
                for (column, scalar) in self.key.iter().zip(key) {
                    column.compare(scalar, scalar)?;
                }
            }
            Ok(())
        };
        validate(a)?;
        validate(b)?;
        Ok(match (a, b) {
            (Boundary::NegativeInfinity, Boundary::NegativeInfinity)
            | (Boundary::PositiveInfinity, Boundary::PositiveInfinity) => Ordering::Equal,
            (Boundary::NegativeInfinity, _) | (_, Boundary::PositiveInfinity) => Ordering::Less,
            (Boundary::PositiveInfinity, _) | (_, Boundary::NegativeInfinity) => Ordering::Greater,
            (Boundary::Key(a), Boundary::Key(b)) => {
                let mut order = Ordering::Equal;
                for ((column, a), b) in self.key.iter().zip(a).zip(b) {
                    order = column.compare(a, b)?;
                    if order != Ordering::Equal {
                        break;
                    }
                }
                order
            }
        })
    }
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Counts {
    pub total: u64,
    pub verified: u64,
    pub busy: u64,
    pub mismatched: u64,
    pub excluded: u64,
}
impl Counts {
    fn validate(&self) -> Result<()> {
        let total = [self.verified, self.busy, self.mismatched, self.excluded]
            .into_iter()
            .try_fold(0u64, |a, b| a.checked_add(b))
            .ok_or_else(|| invalid("count overflow"))?;
        if total != self.total {
            return Err(invalid("ambiguous category counts"));
        }
        Ok(())
    }
    fn add(&mut self, other: &Self) -> Result<()> {
        for (a, b) in [
            (&mut self.total, other.total),
            (&mut self.verified, other.verified),
            (&mut self.busy, other.busy),
            (&mut self.mismatched, other.mismatched),
            (&mut self.excluded, other.excluded),
        ] {
            *a = a.checked_add(b).ok_or_else(|| invalid("count overflow"))?;
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Lineage {
    pub system: String,
    pub database: String,
    pub timeline: u32,
}
impl Lineage {
    fn validate(&self) -> Result<()> {
        required(&self.system)?;
        required(&self.database)?;
        if self.timeline == 0 {
            return Err(invalid("zero timeline"));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Times are producer monotonic ticks within this run, not LSNs. Evidence
/// strings in this module contain inline collected proof, never object refs.
pub struct Interval {
    pub armed: u64,
    pub source_read: u64,
    pub destination_read: u64,
    pub complete_through_lsn: u64,
    pub evidence: String,
}
impl Interval {
    fn validate(&self) -> Result<()> {
        required(&self.evidence)?;
        if self.complete_through_lsn == 0
            || self.armed > self.source_read
            || self.source_read > self.destination_read
        {
            return Err(invalid("unordered evidence interval"));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Exclusion {
    pub key: Vec<Scalar>,
    pub reason: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RangeResult {
    pub id: String,
    pub lower: Boundary,
    pub upper: Boundary,
    pub counts: Counts,
    pub exclusions: Vec<Exclusion>,
    pub interval: Interval,
    pub applied_lsn: u64,
    pub applied_lineage: Lineage,
    pub applied_evidence: String,
    pub source_digest: String,
    pub destination_digest: String,
    pub retry_attempts: u32,
    pub exhausted_busy: u64,
    pub outstanding_keys: Vec<Vec<Scalar>>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableResult {
    pub scope: TableScope,
    pub counts: Counts,
    pub ranges: Vec<RangeResult>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetryPolicy {
    pub version: u32,
    pub policy: String,
    pub budget: u32,
    pub outstanding_ranges: Vec<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeadLetters {
    pub collection: String,
    pub completed: bool,
    pub count: u64,
    pub identities: Vec<String>,
    pub evidence: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Finding {
    pub object: String,
    pub detail: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Findings may contain several independent findings for one checked object.
pub struct Section {
    pub completed: bool,
    pub checked: u64,
    pub findings: Vec<Finding>,
    pub evidence: String,
}
impl Section {
    fn validate(&self) -> Result<()> {
        if !self.completed {
            return Err(invalid("incomplete section"));
        }
        required(&self.evidence)?;
        for finding in &self.findings {
            required(&finding.object)?;
            required(&finding.detail)?;
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Sections {
    pub schema: Section,
    pub sequences: Section,
    pub constraints: Section,
    pub indexes: Section,
    pub destination_foreign_keys: Section,
}
impl Sections {
    fn all(&self) -> [&Section; 5] {
        [
            &self.schema,
            &self.sequences,
            &self.constraints,
            &self.indexes,
            &self.destination_foreign_keys,
        ]
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Report {
    pub version: u32,
    pub coverage_version: u32,
    pub migration: String,
    pub definition_digest: String,
    pub run: String,
    pub epoch: u64,
    pub generation: u64,
    pub lineage: Lineage,
    pub completed: bool,
    pub interval: Interval,
    pub tables: Vec<TableResult>,
    pub retry: RetryPolicy,
    pub dead_letters: DeadLetters,
    pub sections: Sections,
    pub coverage_digest: String,
}
impl Report {
    pub fn computed_coverage_digest(&self) -> Result<String> {
        digest(&(self.coverage_version, &self.tables))
    }
    pub fn validate(&self) -> Result<()> {
        if self.version != VERSION
            || self.coverage_version != VERSION
            || self.retry.version != VERSION
        {
            return Err(invalid("unknown version"));
        }
        if !self.completed || self.epoch == 0 || self.tables.is_empty() {
            return Err(invalid("incomplete run/scope"));
        }
        required(&self.migration)?;
        required(&self.run)?;
        hash(&self.definition_digest)?;
        required(&self.retry.policy)?;
        self.lineage.validate()?;
        self.interval.validate()?;
        let mut tables = BTreeSet::new();
        let mut ranges = BTreeSet::new();
        let mut outstanding = BTreeSet::new();
        let mut range_count = 0usize;
        for table in &self.tables {
            table.scope.validate()?;
            table.counts.validate()?;
            if !tables.insert((&table.scope.schema, &table.scope.table)) || table.ranges.is_empty()
            {
                return Err(invalid("duplicate/empty table"));
            }
            if table.ranges.first().map(|r| &r.lower) != Some(&Boundary::NegativeInfinity)
                || table.ranges.last().map(|r| &r.upper) != Some(&Boundary::PositiveInfinity)
            {
                return Err(invalid("incomplete keyspace"));
            }
            let mut counts = Counts::default();
            for (index, range) in table.ranges.iter().enumerate() {
                range_count += 1;
                required(&range.id)?;
                if range.id != range_identity(&table.scope, &range.lower, &range.upper)? {
                    return Err(invalid("unstable range identity"));
                }
                if !ranges.insert(&range.id) {
                    return Err(invalid("duplicate range identity"));
                }
                if table.scope.compare(&range.lower, &range.upper)? != Ordering::Less
                    || (index > 0 && table.ranges[index - 1].upper != range.lower)
                {
                    return Err(invalid("gap/overlap/reversed range"));
                }
                range.counts.validate()?;
                counts.add(&range.counts)?;
                range.interval.validate()?;
                if range.interval.armed < self.interval.armed
                    || range.interval.destination_read > self.interval.destination_read
                    || range.interval.complete_through_lsn < self.interval.complete_through_lsn
                    || range.applied_lsn < range.interval.complete_through_lsn
                    || range.applied_lineage != self.lineage
                {
                    return Err(invalid("range evidence interval/applied lineage"));
                }
                required(&range.applied_evidence)?;
                hash(&range.source_digest)?;
                hash(&range.destination_digest)?;
                if range.counts.mismatched == 0
                    && range.counts.busy == 0
                    && range.source_digest != range.destination_digest
                {
                    return Err(invalid("unreported content mismatch"));
                }
                if range.retry_attempts > self.retry.budget
                    || range.exhausted_busy != range.counts.busy
                    || (range.exhausted_busy > 0 && range.retry_attempts != self.retry.budget)
                    || range.outstanding_keys.len() as u64 != range.counts.busy
                    || range.exclusions.len() as u64 != range.counts.excluded
                {
                    return Err(invalid("retry/exclusion counts"));
                }
                let mut keys = BTreeSet::new();
                for key in range
                    .outstanding_keys
                    .iter()
                    .chain(range.exclusions.iter().map(|e| &e.key))
                {
                    let bound = Boundary::Key(key.clone());
                    if table.scope.compare(&range.lower, &bound)? == Ordering::Greater
                        || table.scope.compare(&bound, &range.upper)? != Ordering::Less
                        || !keys.insert(serde_json::to_vec(key)?)
                    {
                        return Err(invalid("outstanding/excluded key identity"));
                    }
                }
                for exclusion in &range.exclusions {
                    required(&exclusion.reason)?;
                }
                if range.counts.busy > 0 {
                    outstanding.insert(range.id.clone());
                }
            }
            if counts != table.counts {
                return Err(invalid("table/range counts differ"));
            }
        }
        let listed = self
            .retry
            .outstanding_ranges
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        if listed.len() != self.retry.outstanding_ranges.len()
            || listed != outstanding
            || range_count > MAX_RANGES
        {
            return Err(invalid("outstanding range manifest/budget"));
        }
        let dead = &self.dead_letters;
        required(&dead.collection)?;
        required(&dead.evidence)?;
        if !dead.completed
            || dead.count != dead.identities.len() as u64
            || dead.identities.iter().collect::<BTreeSet<_>>().len() != dead.identities.len()
        {
            return Err(invalid("dead-letter completion/count"));
        }
        for id in &dead.identities {
            required(id)?;
        }
        for section in self.sections.all() {
            section.validate()?;
        }
        if self.coverage_digest != self.computed_coverage_digest()? {
            return Err(invalid("coverage digest mismatch"));
        }
        if serde_json::to_vec(self)?.len() > REPORT_BYTES {
            return Err(invalid("report size cap"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Summary {
    pub migration: String,
    pub definition_digest: String,
    pub run: String,
    pub epoch: u64,
    pub generation: u64,
    pub lineage: Lineage,
    pub scope: Vec<TableScope>,
    pub through_lsn: u64,
    pub ranges: usize,
    pub counts: Counts,
    pub exhausted_busy: u64,
    pub dead_letters: u64,
    pub sections_clean: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReportRef {
    pub version: u32,
    pub digest: String,
    pub bytes: usize,
    pub summary: Summary,
}
impl Report {
    fn reference(&self, bytes: &[u8]) -> Result<ReportRef> {
        let mut counts = Counts::default();
        let mut exhausted_busy = 0u64;
        let mut ranges = 0usize;
        for table in &self.tables {
            counts.add(&table.counts)?;
            ranges += table.ranges.len();
            for range in &table.ranges {
                exhausted_busy = exhausted_busy
                    .checked_add(range.exhausted_busy)
                    .ok_or_else(|| invalid("busy overflow"))?;
            }
        }
        Ok(ReportRef {
            version: VERSION,
            digest: format!("{:x}", Sha256::digest(bytes)),
            bytes: bytes.len(),
            summary: Summary {
                migration: self.migration.clone(),
                definition_digest: self.definition_digest.clone(),
                run: self.run.clone(),
                epoch: self.epoch,
                generation: self.generation,
                lineage: self.lineage.clone(),
                scope: self.tables.iter().map(|t| t.scope.clone()).collect(),
                through_lsn: self.interval.complete_through_lsn,
                ranges,
                counts,
                exhausted_busy,
                dead_letters: self.dead_letters.count,
                sections_clean: self.sections.all().iter().all(|s| s.findings.is_empty()),
            },
        })
    }
}
impl ReportRef {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.version != VERSION
            || self.bytes == 0
            || self.bytes > REPORT_BYTES
            || self.summary.scope.is_empty()
            || self.summary.ranges == 0
            || self.summary.ranges > MAX_RANGES
            || self.summary.epoch == 0
        {
            return Err(invalid("invalid report reference"));
        }
        hash(&self.digest)?;
        hash(&self.summary.definition_digest)?;
        required(&self.summary.migration)?;
        required(&self.summary.run)?;
        self.summary.lineage.validate()?;
        self.summary.counts.validate()?;
        if self.summary.exhausted_busy != self.summary.counts.busy {
            return Err(invalid("summary busy counts"));
        }
        let mut tables = BTreeSet::new();
        for scope in &self.summary.scope {
            scope.validate()?;
            if !tables.insert((&scope.schema, &scope.table)) {
                return Err(invalid("duplicate summary scope"));
            }
        }
        if serde_json::to_vec(self)?.len() > 32 * 1024 {
            return Err(invalid("summary size cap"));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub enum InvalidationReason {
    Repair,
    DefinitionKeyPlanChange,
    LineageDiscontinuity,
    EvidenceLoss,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "scope",
    content = "tables",
    rename_all = "kebab-case",
    deny_unknown_fields
)]
pub enum InvalidationScope {
    All,
    Tables(Vec<(String, String)>),
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Invalidation {
    pub version: u32,
    pub reason: InvalidationReason,
    pub scope: InvalidationScope,
}
impl Invalidation {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.version != VERSION {
            return Err(invalid("unknown invalidation version"));
        }
        if let InvalidationScope::Tables(tables) = &self.scope {
            if tables.is_empty() || tables.iter().collect::<BTreeSet<_>>().len() != tables.len() {
                return Err(invalid("empty/duplicate invalidation scope"));
            }
            for (schema, table) in tables {
                required(schema)?;
                required(table)?;
            }
        }
        Ok(())
    }
    pub(crate) fn applies(&self, report: &ReportRef) -> bool {
        match &self.scope {
            InvalidationScope::All => true,
            InvalidationScope::Tables(tables) => report
                .summary
                .scope
                .iter()
                .any(|s| tables.contains(&(s.schema.clone(), s.table.clone()))),
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CurrentFacts {
    pub migration: String,
    pub definition_digest: String,
    pub epoch: u64,
    pub generation: u64,
    pub lineage: Lineage,
    pub scope: Vec<TableScope>,
    pub capture_lsn: u64,
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct VerificationFacts {
    pub verification: bool,
    pub busy_rows_past_budget: Option<u64>,
    pub dead_letters: Option<u64>,
}
/// Pure projection over authenticated materialization. Callers must refresh
/// successfully and honor recovery errors before using this State; a retained
/// in-memory cache cannot detect subsequent object loss by itself. Missing/stale evidence
/// always returns unknown blocker counts, including an absent empty scope.
pub fn current_verification(
    state: &super::Materialized,
    current: &CurrentFacts,
) -> VerificationFacts {
    let unknown = VerificationFacts::default();
    let Some(reference) = &state.verification.report else {
        return unknown;
    };
    let summary = &reference.summary;
    if state.verification.invalidated
        || reference.validate().is_err()
        || current.scope.is_empty()
        || current.capture_lsn == 0
        || summary.migration != current.migration
        || summary.definition_digest != current.definition_digest
        || summary.generation != current.generation
        || summary.generation != state.verification.generation
        || summary.epoch != current.epoch
        || state.epoch != current.epoch
        || summary.lineage != current.lineage
        || summary.scope != current.scope
        || summary.through_lsn < current.capture_lsn
        || state
            .definition
            .as_ref()
            .and_then(|d| d.canonical_digest().ok())
            .as_ref()
            != Some(&current.definition_digest)
    {
        return unknown;
    }
    VerificationFacts {
        verification: summary.sections_clean
            && summary.counts.verified == summary.counts.total
            && summary.counts.busy == 0
            && summary.counts.mismatched == 0
            && summary.counts.excluded == 0
            && summary.dead_letters == 0,
        busy_rows_past_budget: Some(summary.exhausted_busy),
        dead_letters: Some(summary.dead_letters),
    }
}

impl super::journal::Journal {
    pub fn verification_key(&self, run: &str) -> object_store::path::Path {
        // Journal namespace is <prefix>/journal/<migration>. Use a distinct
        // top-level collection, never a suffix that aliases another migration.
        let parent = self.namespace().rsplit_once('/').expect("journal parent").0;
        let prefix = parent.strip_suffix("journal").expect("journal namespace");
        object_store::path::Path::from(format!(
            "{prefix}verification/{}/{:x}.json",
            self.migration_id(),
            Sha256::digest(run.as_bytes())
        ))
    }
    pub async fn publish_verification(&self, report: &Report) -> Result<ReportRef> {
        report.validate()?;
        if report.migration != self.migration_id() {
            return Err(invalid("migration namespace differs"));
        }
        let bytes = serde_json::to_vec(report)?;
        let reference = report.reference(&bytes)?;
        reference.validate()?;
        self.create(&self.verification_key(&report.run), bytes)
            .await?;
        Ok(reference)
    }
    /// Keep the new object-read state machine and large report output off legacy
    /// coordinator/recovery future stacks, including when no report is present.
    pub fn verification_report<'a>(
        &'a self,
        reference: &'a ReportRef,
    ) -> futures::future::BoxFuture<'a, Result<Box<Report>>> {
        Box::pin(async move {
            reference.validate()?;
            if reference.summary.migration != self.migration_id() {
                return Err(invalid("report namespace differs"));
            }
            let bytes = self
                .read_bounded(&self.verification_key(&reference.summary.run), REPORT_BYTES)
                .await?
                .ok_or_else(|| invalid("missing durable report"))?;
            let report: Report = serde_json::from_slice(&bytes)?;
            report.validate()?;
            if report.reference(&bytes)? != *reference {
                return Err(invalid("report digest/summary differs"));
            }
            Ok(Box::new(report))
        })
    }
}
impl super::epoch::Coordinator {
    /// Publication is create-only by run id. Exact retry is harmless; a changed
    /// payload with the same run id can never overwrite an earlier report.
    pub async fn complete_verification(&mut self, report: &Report) -> Result<()> {
        if !self.is_owner() {
            return Err(Error::Observer);
        }
        self.refresh().await?;
        if !self.is_owner() {
            return Err(Error::LostOwnership);
        }
        let reference = self.journal.publish_verification(report).await?;
        #[cfg(feature = "test-crash")]
        self.crash_at(super::CrashSeam::AfterVerificationReport);
        if reference.summary.epoch != self.state().epoch
            || reference.summary.generation != self.state().verification.generation
        {
            return Err(invalid("stale completion epoch/generation"));
        }
        if self.state().verification.report.as_deref() == Some(&reference)
            && !self.state().verification.invalidated
        {
            return Ok(());
        }
        self.commit(super::Transition::VerificationCompleted {
            report: Box::new(reference),
        })
        .await?;
        Ok(())
    }
}

/// One pointer in Materialized prevents nested coordinator futures from growing
/// with the durable verification metadata. Empty legacy state stays omitted.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerificationState {
    pub report: Option<Box<ReportRef>>,
    pub invalidated: bool,
    pub generation: u64,
    pub runs: std::collections::BTreeMap<String, String>,
    /// Shared heap metadata preserves Materialized's V1 layout and future caps.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reverse: Option<Box<super::reverse::ReverseState>>,
}
impl VerificationState {
    pub(crate) fn is_empty(&self) -> bool {
        self.report.is_none()
            && !self.invalidated
            && self.generation == 0
            && self.runs.is_empty()
            && self.reverse.is_none()
    }
}
