//! Capture-certified MySQL row pass and native v2 publication.
use super::{checkpoint::JournalMysqlCheckpoint, evidence::*, input::MysqlCaptureInput};
use crate::{
    coordinator::{
        journal::Journal,
        verification::{Counts, DeadLetters, RetryPolicy, Sections, native},
    },
    engine::Runtime,
    event::Position,
    mapping::Value,
    mysql::{MysqlOrder, chunk_key::MysqlTableKey},
    verify::{
        self, Applier, BoundarySource, ChangeEvidence, Key, KeyOrder, Projection, RangePlan, Row,
        RowSource, SourcePositionOrder, SourceRead, VerificationSession, VerifyError,
        sql::SqlChangeEvidence,
    },
};
use async_trait::async_trait;
use std::{cmp::Ordering, time::Duration};
pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
pub struct Publication {
    pub migration: String,
    pub run: String,
    pub catalog_digest: String,
    pub plan_digest: String,
    pub sections: Sections,
    pub dead_letters: DeadLetters,
    pub timeout: Duration,
    pub poll_interval: Duration,
}
struct CapturedSource<'a, S, I, O> {
    source: &'a mut S,
    evidence: &'a mut MysqlChangeEvidence<I, O>,
    complete: Option<Position>,
}
impl<S: KeyOrder, I, O> KeyOrder for CapturedSource<'_, S, I, O> {
    fn compare_keys(&self, a: &Key, b: &Key) -> verify::Result<Ordering> {
        self.source.compare_keys(a, b)
    }
}
#[async_trait]
impl<S: RowSource, I: Send, O: Send> RowSource for CapturedSource<'_, S, I, O> {
    async fn fetch(&mut self, r: &RangePlan, p: &Projection) -> verify::Result<Vec<Row>> {
        self.source.fetch(r, p).await
    }
}
#[async_trait]
impl<S: RowSource, I: MysqlEvidenceInput, O: KeyOrder + Send> BoundarySource
    for CapturedSource<'_, S, I, O>
{
    fn position_order(&self) -> SourcePositionOrder {
        self.evidence.position_order()
    }
    async fn read_source(&mut self, r: &RangePlan, p: &Projection) -> verify::Result<SourceRead> {
        self.complete = None;
        self.evidence.arm(r).await?;
        let position = self.evidence.armed_position().expect("armed").clone();
        Ok(SourceRead {
            rows: self.source.fetch(r, p).await?,
            position,
        })
    }
    async fn changed_since(
        &mut self,
        r: &RangePlan,
        p: Position,
    ) -> verify::Result<ChangeEvidence> {
        let evidence = self.evidence.changes(r, p).await?;
        self.complete = Some(evidence.through_position.clone());
        Ok(evidence)
    }
}
pub struct Pass<'a, S, D, O> {
    pub runtime: &'a Runtime,
    pub journal: &'a Journal,
    pub evidence: &'a mut MysqlChangeEvidence<MysqlCaptureInput, O>,
    pub source: &'a mut S,
    pub destination: &'a mut D,
    pub key: &'a MysqlTableKey,
    pub order: MysqlOrder,
    pub projection: Projection,
    pub publication: Publication,
}
impl<S: RowSource, D: RowSource, O: KeyOrder + Send> Pass<'_, S, D, O> {
    pub async fn run(self) -> Result<native::Report> {
        let checkpoint = JournalMysqlCheckpoint::new(
            self.journal.clone(),
            self.evidence.input().source.clone(),
            self.order.clone(),
        )?;
        self.with_checkpoint(checkpoint).await
    }
    pub async fn with_checkpoint(
        self,
        checkpoint: impl MysqlAppliedCheckpoint,
    ) -> Result<native::Report> {
        let binding = self.runtime.state().await.source_binding.clone();
        if binding
            .as_ref()
            .is_none_or(|b| b.stream != self.evidence.input().source.stream)
        {
            return Err(VerifyError::Invalid("capture stream binding mismatch").into());
        }
        let mut source = CapturedSource {
            source: self.source,
            evidence: self.evidence,
            complete: None,
        };
        let mut waiter = MysqlAppliedWaiter::new(
            checkpoint,
            self.order,
            self.publication.timeout,
            self.publication.poll_interval,
        )?;
        run_and_publish(
            self.runtime,
            self.journal,
            &mut source,
            self.destination,
            &mut waiter,
            self.key,
            self.projection,
            self.publication,
        )
        .await
    }
}
#[allow(clippy::too_many_arguments)]
async fn run_and_publish<S: RowSource, I: MysqlEvidenceInput, O: KeyOrder + Send>(
    runtime: &Runtime,
    journal: &Journal,
    source: &mut CapturedSource<'_, S, I, O>,
    destination: &mut impl RowSource,
    waiter: &mut MysqlAppliedWaiter<impl MysqlAppliedCheckpoint>,
    key: &MysqlTableKey,
    projection: Projection,
    publication: Publication,
) -> Result<native::Report> {
    let scope = native::TableScope {
        schema: key.schema.clone(),
        table: key.table.clone(),
        catalog_digest: publication.catalog_digest,
        plan_digest: publication.plan_digest,
        key: native::mysql_key(key)?,
    };
    let state = runtime.state().await;
    let binding = state
        .source_binding
        .clone()
        .ok_or(VerifyError::Invalid("missing MySQL binding"))?;
    if binding.stream.is_empty() || binding.order_domain != source.position_order().domain() {
        return Err(VerifyError::Invalid("MySQL verification binding mismatch").into());
    }
    let range = RangePlan {
        id: crate::pg::chunk::ChunkId {
            table: key.event_table.clone(),
            plan: scope.plan_digest.clone(),
            ordinal: 0,
        },
        lo: None,
        hi: None,
    };
    let mut session = VerificationSession::new(range, projection, 0)?;
    let verified = session.run(source, destination, waiter).await?;
    let marker = source
        .complete
        .as_ref()
        .ok_or(VerifyError::Invalid("missing captured marker"))?;
    let applied = waiter.wait_until(marker.clone()).await?;
    let coverage = &verified.comparison.coverage;
    let counts = Counts {
        total: coverage.total,
        verified: coverage.verified,
        busy: coverage.busy,
        mismatched: coverage.mismatched,
        excluded: coverage.excluded,
    };
    let interval = native::Interval {
        armed: 0,
        source_read: 1,
        destination_read: 2,
        complete_through: marker.into(),
        evidence: "capture-armed complete mapped images through exact verify marker".into(),
    };
    let lower = native::Boundary::NegativeInfinity;
    let upper = native::Boundary::PositiveInfinity;
    let id = native::range_identity(&scope, &lower, &upper)?;
    let busy = verified
        .comparison
        .busy
        .iter()
        .map(|k| scalar_key(k, &scope.key))
        .collect::<Result<Vec<_>>>()?;
    let hex = |digest: &[u8]| {
        digest
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
    };
    let mut report = native::Report {
        version: 2,
        coverage_version: 2,
        binding,
        migration: publication.migration,
        run: publication.run,
        definition_digest: state
            .definition
            .as_ref()
            .ok_or(VerifyError::Invalid("missing definition"))?
            .canonical_digest()?,
        epoch: state.epoch,
        generation: state.verification.generation,
        completed: true,
        interval: interval.clone(),
        tables: vec![native::TableResult {
            scope,
            counts: counts.clone(),
            ranges: vec![native::RangeResult {
                id: id.clone(),
                lower,
                upper,
                counts,
                exclusions: vec![],
                interval,
                applied: (&applied).into(),
                applied_evidence:
                    "destination applied source checkpoint at or beyond captured verify marker"
                        .into(),
                source_digest: hex(&verified.comparison.source_digest),
                destination_digest: hex(&verified.comparison.destination_digest),
                retry_attempts: 0,
                exhausted_busy: coverage.busy,
                outstanding_keys: busy,
            }],
        }],
        retry: RetryPolicy {
            version: 2,
            policy: "one-complete-captured-interval".into(),
            budget: 0,
            outstanding_ranges: if coverage.busy > 0 { vec![id] } else { vec![] },
        },
        sections: publication.sections,
        dead_letters: publication.dead_letters,
        coverage_digest: String::new(),
    };
    report.coverage_digest = report.computed_coverage_digest()?;
    crate::runner::verification::publish_native(
        runtime,
        journal,
        &report,
        std::slice::from_ref(key),
    )
    .await?;
    Ok(report)
}
fn scalar_key(key: &Key, columns: &[native::KeyColumn]) -> Result<Vec<native::Scalar>> {
    if key.len() != columns.len() {
        return Err(native::Error::KeyScope.into());
    }
    key.iter()
        .zip(columns)
        .map(|(v, c)| {
            let (negative, digits, scale) = match v {
                Value::Int2(n) => (*n < 0, n.unsigned_abs().to_string(), 0),
                Value::Int4(n) => (*n < 0, n.unsigned_abs().to_string(), 0),
                Value::Int8(n) => (*n < 0, n.unsigned_abs().to_string(), 0),
                Value::Numeric(n) => {
                    let (sign, digits, scale) = n.finite_parts().ok_or(native::Error::KeyScope)?;
                    (sign, digits.to_owned(), scale)
                }
                _ => return Err(native::Error::KeyScope.into()),
            };
            let text = format!("{}{}", if negative { "-" } else { "" }, digits);
            Ok(match c.kind {
                native::KeyType::Signed { .. } if scale == 0 => {
                    native::Scalar::Signed(text.parse()?)
                }
                native::KeyType::Unsigned { .. } if scale == 0 && !negative => {
                    native::Scalar::Unsigned(text.parse()?)
                }
                native::KeyType::Decimal {
                    scale: expected, ..
                } if scale == u32::from(expected) => native::Scalar::Decimal(text),
                _ => return Err(native::Error::KeyScope.into()),
            })
        })
        .collect()
}
#[cfg(test)]
mod tests;
