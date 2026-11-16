//! Owner-scoped online verification. Control files carry requests/results only;
//! authenticated coordinator objects remain the completion authority.
use super::*;
use crate::{
    coordinator::{self, Materialized, Transition, journal::Journal, verification as durable},
    engine::{self, ComponentSnapshot, ReadinessSource},
    mapping::{PgType, Value},
    pg::chunk::{ChunkRange, Direction},
    verify::{self, Applier, BoundarySource, KeyOrder, RowSource, sql},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value as Json, json};
use std::{
    fs::{File, OpenOptions},
    io::Write,
    path::Path,
    sync::{Arc, Mutex},
    time::Instant,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VerifyResponse {
    pub exit_code: u8,
    pub data: Json,
}
#[derive(Serialize, Deserialize)]
struct Request {
    generation: String,
    run: String,
    tables: Vec<String>,
}
#[derive(Serialize, Deserialize)]
struct Status {
    generation: String,
    epoch: u64,
    invalidation_generation: u64,
    capture_lsn: u64,
    capture_end: u64,
    report: Option<Box<durable::ReportRef>>,
    verification: bool,
}

#[derive(Serialize, Deserialize)]
struct ReadinessStatus {
    #[serde(default)]
    report: Option<Box<durable::ReportRef>>,
    #[serde(default)]
    captured_end: Option<u64>,
    #[serde(default)]
    terminal: bool,
    generation: String,
    epoch: u64,
    invalidation_generation: u64,
    definition_digest: String,
    observed_at_ms: u64,
    facts: super::readiness::Facts,
}
fn now_ms() -> Option<u64> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_millis()
        .try_into()
        .ok()
}
fn readiness_status(root: &Path, id: &str, state: &Materialized) -> super::readiness::Facts {
    let valid = (|| -> Option<super::readiness::Facts> {
        let generation = active(root).ok()??;
        let status: ReadinessStatus =
            serde_json::from_slice(&std::fs::read(root.join("readiness-status.json")).ok()?)
                .ok()?;
        let captured_end = state
            .progress
            .get("buffered")
            .map(|_| buffered_end(state, &format!("skedaddle-postgres:{id}")))
            .transpose()
            .ok()?;
        if status.report != state.verification.report
            || status.captured_end != captured_end
            || status.terminal
            || status.generation != generation
            || status.epoch != state.epoch
            || status.invalidation_generation != state.verification.generation
            || status.definition_digest != state.definition.as_ref()?.canonical_digest().ok()?
            || now_ms()?.checked_sub(status.observed_at_ms)? > 250
            || status
                .facts
                .destination_expires_at_ms
                .is_some_and(|deadline| now_ms().is_none_or(|now| now >= deadline))
        {
            return None;
        }
        Some(status.facts)
    })();
    valid.unwrap_or_else(|| {
        let mut facts = super::readiness::Facts::sample(state.definition.as_ref(), None);
        // Retain only an actual blocked-boundary diagnostic from the last
        // stopped owner. Every live fact remains unavailable/false. A new
        // active owner supersedes this historical observation immediately.
        let historical = (|| -> Option<crate::status::Blocker> {
            if active(root).ok()?.is_some() { return None; }
            let saved: ReadinessStatus = serde_json::from_slice(&std::fs::read(root.join("readiness-status.json")).ok()?).ok()?;
            if !saved.terminal || saved.epoch != state.epoch
                || saved.invalidation_generation != state.verification.generation
                || saved.definition_digest != state.definition.as_ref()?.canonical_digest().ok()? { return None; }
            saved.facts.schema_boundary
        })();
        if let Some(mut blocker) = historical {
            blocker.reason = format!("Last capture boundary (owner stopped; current schema state unknown): {}", blocker.reason);
            facts.schema_jobs.blocker = Some(blocker);
            facts.schema_jobs.source = "Historical blocked boundary bound to the last owner generation, epoch, definition and invalidation generation.".into();
            facts.schema_jobs.staleness = "Owner stopped: all live readiness authority is revoked; this reason is diagnostic only.".into();
        }
        facts
    })
}

/// A status file is only a live owner observation. The CLI first authenticates
/// the journal, then binds this observation to its exact current report/capture.
pub fn verification_status(root: &Path, id: &str, state: &Materialized) -> Json {
    let control = root.join(format!("{id}.control"));
    let valid = (|| -> Option<bool> {
        let generation = active(&control).ok()??;
        let status: Status =
            serde_json::from_slice(&std::fs::read(control.join("verification-status.json")).ok()?)
                .ok()?;
        if status.generation != generation
            || status.epoch != state.epoch
            || status.invalidation_generation != state.verification.generation
            || status.report != state.verification.report
            || status.capture_end
                != buffered_end(state, &format!("skedaddle-postgres:{id}")).ok()?
            || status.capture_lsn > status.capture_end
            || status.capture_lsn == 0
        {
            return None;
        }
        Some(status.verification)
    })();
    json!({"verification":valid.unwrap_or(false),"live_owner_observation":valid.is_some(),"completed_report":state.verification.report.as_ref().map(|r|&r.summary.run),"operational_schema":state.operational_schema,"schema_remediation":crate::pg::ddl::SCHEMA_REMEDIATION,"readiness":readiness_status(&control, id, state)})
}

fn control_file(root: &Path, name: &str) -> std::io::Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(root.join(name))
}
pub(super) fn active(root: &Path) -> std::io::Result<Option<String>> {
    if !root.is_dir() {
        return Ok(None);
    }
    let gate = control_file(root, "control.lock")?;
    gate.lock()?;
    let owner = control_file(root, "owner.lock")?;
    match owner.try_lock() {
        Ok(()) => Ok(None),
        Err(std::fs::TryLockError::WouldBlock) => {
            Ok(std::fs::read_to_string(root.join("owner-generation"))
                .ok()
                .and_then(|s| uuid::Uuid::parse_str(&s).ok())
                .map(|id| id.to_string()))
        }
        Err(std::fs::TryLockError::Error(error)) => Err(error),
    }
}
pub(super) fn write_json(path: &Path, value: &impl Serialize) -> coordinator::Result<()> {
    let staging = path.with_extension(format!("{}.staging", uuid::Uuid::new_v4()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&staging)?;
    file.write_all(&serde_json::to_vec(value)?)?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(staging, path)?;
    Ok(())
}
fn unavailable(what: &str) -> VerifyResponse {
    VerifyResponse {
        exit_code: 6,
        data: json!({"complete":false,"what":what}),
    }
}

/// The observer never acquires the migration mutation lock or publishes evidence.
/// Without an owner, live preparation still diagnoses genuine endpoint failures.
pub async fn request_verification(
    root: &Path,
    id: &str,
    definition: &MigrationDefinition,
    tables: &[String],
) -> Result<VerifyResponse, Failure> {
    let control = root.join(format!("{id}.control"));
    let generation = active(&control).map_err(|_| infrastructure())?;
    let Some(generation) = generation else {
        Box::pin(prepare(definition)).await?;
        return Ok(VerifyResponse {
            exit_code: 2,
            data: json!({"complete":false,"verification":false,"what":"No active capture owner; resume --until paused, then verify while the owner is running."}),
        });
    };
    let request = Request {
        generation: generation.clone(),
        run: uuid::Uuid::new_v4().to_string(),
        tables: tables.to_vec(),
    };
    let request_path = control.join(format!("verify-{generation}-{}.request", request.run));
    write_json(&request_path, &request).map_err(|_| infrastructure())?;
    let response_path = control.join(format!("verify-{generation}-{}.response", request.run));
    let started = Instant::now();
    loop {
        if let Ok(bytes) = std::fs::read(&response_path) {
            return serde_json::from_slice(&bytes).map_err(|_| infrastructure());
        }
        if active(&control).map_err(|_| infrastructure())? != Some(generation.clone()) {
            return Ok(VerifyResponse {
                exit_code: 2,
                data: json!({"complete":false,"verification":false,"what":"Owner stopped before completing verification; earlier completed evidence was preserved."}),
            });
        }
        if started.elapsed() > Duration::from_secs(120) {
            return Ok(unavailable(
                "Owner verification request timed out; inspect owner status.",
            ));
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[derive(Clone)]
struct Order(catalog::RangeClassifier);
impl KeyOrder for Order {
    fn compare_keys(&self, a: &verify::Key, b: &verify::Key) -> verify::Result<std::cmp::Ordering> {
        self.0
            .compare_keys(a, b)
            .map_err(|_| verify::VerifyError::Invalid("qualified key ordering failed"))
    }
}
#[derive(Default)]
struct Observed {
    armed: u64,
    source_read: u64,
    destination_read: u64,
    source_lsn: u64,
    through: u64,
    applied: u64,
}
fn tick(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX)
}
struct Source<O> {
    pg: sql::PgRowSource<O>,
    observed: Arc<Mutex<Observed>>,
    start: Instant,
}
impl<O: KeyOrder> KeyOrder for Source<O> {
    fn compare_keys(&self, a: &verify::Key, b: &verify::Key) -> verify::Result<std::cmp::Ordering> {
        self.pg.compare_keys(a, b)
    }
}
#[async_trait::async_trait]
impl<O: KeyOrder + Send> RowSource for Source<O> {
    async fn fetch(
        &mut self,
        range: &ChunkRange,
        projection: &verify::Projection,
    ) -> verify::Result<Vec<verify::Row>> {
        self.pg.fetch(range, projection).await
    }
}
#[async_trait::async_trait]
impl<O: KeyOrder + Send> BoundarySource for Source<O> {
    fn position_order(&self) -> verify::SourcePositionOrder {
        self.pg.position_order()
    }
    async fn read_source(
        &mut self,
        range: &ChunkRange,
        projection: &verify::Projection,
    ) -> verify::Result<verify::SourceRead> {
        // SqlChangeEvidence::arm happens inside this call, before its snapshot.
        self.observed.lock().expect("observation poisoned").armed = tick(self.start);
        let read = self.pg.read_source(range, projection).await?;
        let mut seen = self.observed.lock().expect("observation poisoned");
        seen.source_read = tick(self.start);
        seen.source_lsn = durable::pg_report_lsn(
            "postgres",
            &read.position,
            &sql::PgSourcePosition::new(self.pg.position_order().domain()),
        )?;
        Ok(read)
    }
    async fn changed_since(
        &mut self,
        range: &ChunkRange,
        lsn: crate::event::Position,
    ) -> verify::Result<verify::ChangeEvidence> {
        let changes = self.pg.changed_since(range, lsn).await?;
        self.observed.lock().expect("observation poisoned").through = durable::pg_report_lsn(
            "postgres",
            &changes.through_position,
            &sql::PgSourcePosition::new(self.pg.position_order().domain()),
        )?;
        Ok(changes)
    }
}
struct Destination<O> {
    pg: sql::PgRowSource<O>,
    observed: Arc<Mutex<Observed>>,
    start: Instant,
}
impl<O: KeyOrder> KeyOrder for Destination<O> {
    fn compare_keys(&self, a: &verify::Key, b: &verify::Key) -> verify::Result<std::cmp::Ordering> {
        self.pg.compare_keys(a, b)
    }
}
#[async_trait::async_trait]
impl<O: KeyOrder + Send> RowSource for Destination<O> {
    async fn fetch(
        &mut self,
        range: &ChunkRange,
        projection: &verify::Projection,
    ) -> verify::Result<Vec<verify::Row>> {
        let rows = self.pg.fetch(range, projection).await?;
        self.observed
            .lock()
            .expect("observation poisoned")
            .destination_read = tick(self.start);
        Ok(rows)
    }
}
fn invalid() -> coordinator::Error {
    coordinator::Error::Rejected("Runner verification scope or evidence is not qualified".into())
}
fn scalar(value: &Value) -> coordinator::Result<durable::Scalar> {
    Ok(match value {
        Value::Bool(v) => durable::Scalar::Bool(*v),
        Value::Int2(v) => durable::Scalar::Integer((*v).into()),
        Value::Int4(v) => durable::Scalar::Integer((*v).into()),
        Value::Int8(v) => durable::Scalar::Integer(*v),
        Value::Bytea(v) => durable::Scalar::Bytea(v.clone()),
        Value::Uuid(v) => durable::Scalar::Uuid(*v),
        _ => return Err(invalid()),
    })
}
fn boundary(key: &Option<verify::Key>, lower: bool) -> coordinator::Result<durable::Boundary> {
    match key {
        Some(key) => Ok(durable::Boundary::Key(
            key.iter().map(scalar).collect::<coordinator::Result<_>>()?,
        )),
        None if lower => Ok(durable::Boundary::NegativeInfinity),
        None => Ok(durable::Boundary::PositiveInfinity),
    }
}
fn counts(c: &verify::Coverage) -> durable::Counts {
    durable::Counts {
        total: c.total,
        verified: c.verified,
        busy: c.busy,
        mismatched: c.mismatched,
        excluded: c.excluded,
    }
}
fn scope(
    table: &catalog::TableCatalog,
    plan: &coordinator::chunk::ChunkPlanRef,
) -> coordinator::Result<durable::TableScope> {
    let key = table
        .key
        .columns
        .iter()
        .map(|column| {
            let kind = match column.pg_type {
                PgType::Bool => durable::KeyType::Bool,
                PgType::Int2 => durable::KeyType::Int2,
                PgType::Int4 => durable::KeyType::Int4,
                PgType::Int8 => durable::KeyType::Int8,
                PgType::Bytea => durable::KeyType::Bytea,
                PgType::Uuid => durable::KeyType::Uuid,
                _ => return Err(invalid()),
            };
            Ok(durable::KeyColumn {
                name: column.name.clone(),
                kind,
                descending: column.direction == Direction::Desc,
            })
        })
        .collect::<coordinator::Result<_>>()?;
    // Bind every qualified source/destination type, selected column, key/index
    // descriptor and destination apply mapping; no working-file/OID shortcut.
    let catalog_digest = durable::digest(&format!("{table:?}"))?;
    Ok(durable::TableScope {
        schema: table.selection.schema.clone(),
        table: table.selection.name.clone(),
        catalog_digest,
        plan_digest: plan.digest.clone(),
        key,
    })
}
fn lineage(state: &Materialized) -> coordinator::Result<durable::Lineage> {
    let marker = &state.progress.get("source-slot").ok_or_else(invalid)?.0;
    let checkpoint = engine::decode_source_checkpoint(marker)?;
    Ok(durable::Lineage {
        system: checkpoint.lineage.system_id.to_string(),
        database: checkpoint.lineage.database,
        timeline: checkpoint.lineage.timeline,
    })
}
fn buffered_end(state: &Materialized, domain: &str) -> coordinator::Result<u64> {
    let checkpoint = engine::decode_source_checkpoint(
        &state.progress.get("source-slot").ok_or_else(invalid)?.0,
    )?;
    let source = crate::event::SourceIdentity {
        engine: "postgres".into(),
        system_id: checkpoint.lineage.system_id.to_string(),
        lineage: checkpoint.lineage.timeline.to_string(),
        db_name: checkpoint.lineage.database,
        stream: checkpoint.slot,
    };
    let payload = &state.progress.get("buffered").ok_or_else(invalid)?.1;
    let position = engine::decode_buffered_position(payload, &source, domain)?;
    engine::pg_position_lsn(&position, domain)
}

#[derive(Clone)]
struct CapturePosition {
    source: crate::event::SourceIdentity,
    domain: String,
    commit: u64,
    end: u64,
}
pub(super) struct LiveCapture<C> {
    capture: C,
    source: crate::event::SourceIdentity,
    domain: String,
    observed: Arc<Mutex<Option<CapturePosition>>>,
}
impl<C> Drop for LiveCapture<C> {
    fn drop(&mut self) {
        *self.observed.lock().expect("capture observation poisoned") = None;
    }
}
impl<C: engine::Capture> engine::Capture for LiveCapture<C> {
    fn position_order(&self) -> Arc<dyn crate::event::PositionOrder + Send + Sync> {
        self.capture.position_order()
    }
    async fn prepare_schema(
        &mut self,
        journal: &coordinator::journal::Journal,
        runtime: &engine::Runtime,
    ) -> coordinator::Result<()> {
        self.capture.prepare_schema(journal, runtime).await
    }
    fn schema_state(&self) -> Option<engine::SchemaState> {
        self.capture.schema_state()
    }
    fn buffered_marker(&self, batch: &engine::Captured) -> coordinator::Result<String> {
        self.capture.buffered_marker(batch)
    }
    fn writer_checkpoint(
        &self,
    ) -> coordinator::Result<Option<crate::buffer::writer::WriterCheckpoint>> {
        self.capture.writer_checkpoint()
    }
    async fn step(&mut self) -> coordinator::Result<engine::Captured> {
        let batch = match self.capture.step().await {
            Ok(batch) => batch,
            Err(error) => {
                *self.observed.lock().expect("capture observation poisoned") = None;
                return Err(error);
            }
        };
        if let Some(position) = &batch.durable_position {
            let mut observed = self.observed.lock().expect("capture observation poisoned");
            let end = match engine::pg_position_lsn(position, &self.domain) {
                Ok(end) => end,
                Err(error) => {
                    *observed = None;
                    return Err(error);
                }
            };
            let next = batch
                .transaction
                .as_ref()
                .filter(|txn| {
                    txn.source == self.source && txn.commit_position.order_domain() == self.domain
                })
                .and_then(|txn| engine::pg_position_lsn(&txn.commit_position, &self.domain).ok())
                .filter(|commit| *commit > 0 && *commit <= end);
            match next {
                Some(commit)
                    if observed
                        .as_ref()
                        .is_none_or(|old| old.commit <= commit && old.end <= end) =>
                {
                    *observed = Some(CapturePosition {
                        source: self.source.clone(),
                        domain: self.domain.clone(),
                        commit,
                        end,
                    });
                }
                _ => {
                    *observed = None;
                    return Err(invalid());
                }
            }
        }
        if batch.stop {
            *self.observed.lock().expect("capture observation poisoned") = None;
        }
        Ok(batch)
    }
    async fn publish(
        &mut self,
        high: crate::event::Position,
        rows: crate::event::AssembledTxn,
    ) -> coordinator::Result<crate::buffer::writer::DurableManifestRef> {
        self.capture.publish(high, rows).await
    }
}
fn section(checked: usize, findings: Vec<durable::Finding>, evidence: &str) -> durable::Section {
    durable::Section {
        completed: true,
        checked: checked as u64,
        findings,
        evidence: evidence.into(),
    }
}
fn findings<T>(
    items: &[T],
    object: impl Fn(&T) -> String,
    detail: impl Fn(&T) -> String,
) -> Vec<durable::Finding> {
    items
        .iter()
        .map(|v| durable::Finding {
            object: object(v),
            detail: detail(v),
        })
        .collect()
}

/// No optimistic cache: a failed authenticated refresh clears this snapshot.
/// Each sample checks the current runtime epoch, scope, generation and capture
/// against the last authenticated report, so source progress makes it stale.
pub(super) struct Readiness {
    runtime: engine::Runtime,
    domain: String,
    captured: Arc<Mutex<Option<CapturePosition>>>,
    buffer: Option<Arc<dyn crate::buffer::backends::BufferStore>>,
    fence: Mutex<Option<super::fence_readiness::Observation>>,
    destination: Mutex<Option<super::destination_readiness::Observation>>,
    authenticated: Mutex<
        Option<(
            Materialized,
            durable::CurrentFacts,
            verify::VerificationReport,
        )>,
    >,
}
impl Readiness {
    pub(super) fn new(
        runtime: engine::Runtime,
        id: &str,
        buffer: Option<Arc<dyn crate::buffer::backends::BufferStore>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            runtime,
            domain: format!("skedaddle-postgres:{id}"),
            captured: Arc::new(Mutex::new(None)),
            buffer,
            fence: Mutex::new(None),
            destination: Mutex::new(None),
            authenticated: Mutex::new(None),
        })
    }
    fn facts(&self, state: &Materialized) -> super::readiness::Facts {
        let mut facts =
            super::readiness::Facts::sample(state.definition.as_ref(), self.buffer.as_deref());
        facts.compose_live(state.definition.as_ref(), &self.runtime.live_readiness());
        if let Err(error) = state.operational_schema.require_clear() {
            facts.schema_jobs.ready = false;
            facts.schema_jobs.blocker = Some(crate::status::Blocker {
                code: "scoped-schema-blocked".into(),
                reason: error.to_string(),
                remediation: crate::pg::ddl::SCHEMA_REMEDIATION.into(),
            });
        }
        if let Some(fact) = self.fence_fact(state) {
            facts.fence_risks = fact;
        }
        if let Some(checks) = self.destination_checks(state) {
            facts.destination_expires_at_ms = checks.expires_at_ms;
            facts.destination_validity = checks.validity;
            facts.sequences_syncable = checks.sequences;
            facts.reverse_prerequisites = checks.reverse;
        }
        let current_v1 = self.current_v1(state);
        if !current_v1 {
            facts.destination_validity.ready = false;
            let reason = "Authenticated current V1 schema and destination_foreign_keys integrity sections are unavailable, stale or have findings; next: run complete verification for the current generation/lineage/scope";
            if let Some(blocker) = &mut facts.destination_validity.blocker {
                blocker.reason.push_str("; ");
                blocker.reason.push_str(reason);
            } else {
                facts.destination_validity.blocker = Some(crate::status::Blocker {
                    code: "destination-invalid".into(), reason: reason.into(),
                    remediation: "Run full verification, restore live capture, and resample destination catalogs.".into(),
                });
            }
        }
        facts
    }
    fn destination_checks(
        &self,
        state: &Materialized,
    ) -> Option<super::destination_readiness::Checks> {
        let live = self.runtime.live_readiness();
        self.destination
            .lock()
            .expect("destination readiness poisoned")
            .as_ref()?
            .checks(state, live.owner_generation.as_deref(), live.running)
    }
    fn current_v1(&self, state: &Materialized) -> bool {
        let guard = self.authenticated.lock().expect("readiness poisoned");
        let Some((authenticated, current, report)) = guard.as_ref() else {
            return false;
        };
        if state.verification.report != authenticated.verification.report
            || lineage(state).ok().as_ref() != Some(&current.lineage)
            || current.scope.iter().any(|scope| {
                !state
                    .chunk_plans
                    .values()
                    .any(|plan| plan.digest == scope.plan_digest)
            })
        {
            return false;
        }
        let Ok(live) = self.captured(state) else {
            return false;
        };
        let mut current = current.clone();
        current.capture_lsn = live.commit;
        let clean = |s: &verify::ResultSlot<verify::SchemaFinding>| matches!(s,verify::ResultSlot::Collected {findings,..} if findings.is_empty());
        durable::current_verification(state, &current).verification
            && clean(&report.schema)
            && matches!(&report.foreign_keys,verify::ResultSlot::Collected {findings,..} if findings.is_empty())
    }
    fn fence_fact(&self, state: &Materialized) -> Option<super::readiness::Fact> {
        let guard = self.fence.lock().expect("fence inventory poisoned");
        let observed = guard.as_ref()?;
        let live = self.runtime.live_readiness();
        observed.fact(state, live.owner_generation.as_deref(), live.running)
    }
    pub(super) fn capture<C>(
        &self,
        capture: C,
        source: crate::event::SourceIdentity,
        domain: String,
    ) -> LiveCapture<C> {
        LiveCapture {
            capture,
            source,
            domain,
            observed: self.captured.clone(),
        }
    }
    fn captured(&self, state: &Materialized) -> coordinator::Result<CapturePosition> {
        let observed = self.captured.lock().expect("capture observation poisoned");
        let live = observed.as_ref().ok_or_else(invalid)?;
        let checkpoint = engine::decode_source_checkpoint(
            &state.progress.get("source-slot").ok_or_else(invalid)?.0,
        )?;
        if live.end != buffered_end(state, &self.domain)?
            || live.source.system_id != checkpoint.lineage.system_id.to_string()
            || live.source.lineage != checkpoint.lineage.timeline.to_string()
            || live.source.db_name != checkpoint.lineage.database
            || live.source.stream != checkpoint.slot
            || live.source.engine != "postgres"
            || live.domain != self.domain
        {
            return Err(invalid());
        }
        Ok(live.clone())
    }
    async fn refresh(
        &self,
        journal: &Journal,
        tables: &[catalog::TableCatalog],
    ) -> coordinator::Result<()> {
        *self.authenticated.lock().expect("readiness poisoned") = None;
        let state = journal.load(true).await?;
        let current = durable::CurrentFacts {
            migration: journal.migration_id().into(),
            definition_digest: state
                .definition
                .as_ref()
                .ok_or_else(invalid)?
                .canonical_digest()
                .map_err(|_| invalid())?,
            epoch: state.epoch,
            generation: state.verification.generation,
            lineage: lineage(&state)?,
            scope: tables
                .iter()
                .map(|t| scope(t, state.chunk_plans.get(&t.key.table).ok_or_else(invalid)?))
                .collect::<coordinator::Result<_>>()?,
            capture_lsn: self.captured(&state)?.commit,
        };
        let Some(reference) = &state.verification.report else {
            return Ok(());
        };
        let report = journal.verification_report(reference).await?;
        let mut live = verify::VerificationReport::default();
        for table in &report.tables {
            let catalog = tables
                .iter()
                .find(|t| {
                    t.selection.schema == table.scope.schema
                        && t.selection.name == table.scope.table
                })
                .ok_or_else(invalid)?;
            live.tables.push(verify::TableCoverage {
                table: catalog.key.table.clone(),
                rows: verify::Coverage {
                    total: table.counts.total,
                    verified: table.counts.verified,
                    busy: table.counts.busy,
                    mismatched: table.counts.mismatched,
                    excluded: table.counts.excluded,
                },
                exclusion_reason: None,
            });
        }
        // Only a clean typed section projects to a clean live section. Failure
        // remains uncollected, independently blocking the engine's readiness.
        macro_rules! clean {
            ($field:ident,$source:ident) => {
                if report.sections.$source.completed && report.sections.$source.findings.is_empty()
                {
                    live.$field = verify::ResultSlot::Collected {
                        checked: report.sections.$source.checked,
                        findings: vec![],
                        evidence: vec![],
                    };
                }
            };
        }
        clean!(schema, schema);
        clean!(sequences, sequences);
        clean!(constraints, constraints);
        clean!(indexes, indexes);
        clean!(foreign_keys, destination_foreign_keys);
        *self.authenticated.lock().expect("readiness poisoned") = Some((state, current, live));
        Ok(())
    }
}
impl ReadinessSource for Readiness {
    fn sample(&self) -> ComponentSnapshot {
        // These independent facts do not depend on having a completed
        // verification report. Missing producers remain None, never true.
        let state = futures::executor::block_on(self.runtime.state());
        let facts = self.facts(&state);
        let mut sample = ComponentSnapshot {
            buffer_contract_matches: Some(facts.buffer_contract.ready),
            sequences_syncable: Some(facts.sequences_syncable.ready),
            reverse_prerequisites: Some(facts.reverse_prerequisites.ready),
            fence_risks_acknowledged: self.fence_fact(&state).map(|fact| fact.ready),
            ..ComponentSnapshot::default()
        };
        let live = self.runtime.live_readiness();
        sample.capture = live.capture;
        sample.apply_lag = live.lag;
        if live.running && live.schema_fresh {
            sample.schema_jobs = live
                .schema
                .as_ref()
                .map(|schema| (schema.blocked_boundary.is_some(), schema.unknown_schema_job));
        }
        let guard = self.authenticated.lock().expect("readiness poisoned");
        if let Some((authenticated, current, report)) = guard.as_ref() {
            // Runtime::state is an immediate snapshot future; this does not
            // enter or block the Tokio scheduler or perform I/O.
            if state.verification.report != authenticated.verification.report {
                return sample;
            }
            let mut current = current.clone();
            // Preserve the successfully refreshed ownership facts. A lagging
            // runtime snapshot must not conceal a newer authenticated epoch.
            let Ok(current_lineage) = lineage(&state) else {
                return sample;
            };
            if current_lineage != current.lineage {
                return sample;
            }
            if current.scope.iter().any(|scope| {
                !state
                    .chunk_plans
                    .values()
                    .any(|plan| plan.digest == scope.plan_digest)
            }) {
                return sample;
            }
            let Ok(live) = self.captured(&state) else {
                return sample;
            };
            current.capture_lsn = live.commit;
            let verified = project(&state, &current, report);
            sample.verification = verified
                .verification
                .map(|report| gate_catalog_sections(report, facts.destination_validity.ready));
            sample.busy_rows_past_budget = verified.busy_rows_past_budget;
            sample.dead_letters = verified.dead_letters;
        }
        sample
    }
}
fn gate_catalog_sections(
    mut report: verify::VerificationReport,
    valid: bool,
) -> verify::VerificationReport {
    // A V1 index section predates indisready collection. Preserve row
    // evidence, but revoke condition6 without fresh destination catalogs.
    if !valid {
        report.indexes = verify::ResultSlot::NotCollected;
    }
    report
}
fn project(
    state: &Materialized,
    current: &durable::CurrentFacts,
    report: &verify::VerificationReport,
) -> ComponentSnapshot {
    let facts = durable::current_verification(state, current);
    ComponentSnapshot {
        busy_rows_past_budget: facts.busy_rows_past_budget,
        dead_letters: facts.dead_letters,
        verification: facts.verification.then(|| report.clone()),
        ..ComponentSnapshot::default()
    }
}

pub(super) struct Service {
    pub root: PathBuf,
    pub id: String,
    pub definition: MigrationDefinition,
    pub source_config: tls::Config,
    pub destination_config: tls::Config,
    pub tables: Vec<catalog::TableCatalog>,
    pub runtime: engine::Runtime,
    pub journal: Journal,
    pub readiness: Arc<Readiness>,
}
impl Service {
    pub(super) async fn publish_terminal_readiness(&self) -> coordinator::Result<()> {
        *self
            .readiness
            .destination
            .lock()
            .expect("destination readiness poisoned") = None;
        *self
            .readiness
            .fence
            .lock()
            .expect("fence inventory poisoned") = None;
        // A failed activation has no observations of its own. Never replace a
        // different active owner's status or erase an earlier boundary then.
        let live = self.runtime.live_readiness();
        if live.schema.is_none() {
            return Ok(());
        }
        let Some(generation) = live.owner_generation else {
            return Ok(());
        };
        let gate = control_file(&self.root, "control.lock")?;
        gate.lock()?;
        let owner = control_file(&self.root, "owner.lock")?;
        match owner.try_lock() {
            Ok(()) => {}
            Err(std::fs::TryLockError::WouldBlock) => return Ok(()),
            Err(std::fs::TryLockError::Error(error)) => return Err(error.into()),
        }
        if let Ok(saved) = std::fs::read(self.root.join("readiness-status.json"))
            && let Ok(saved) = serde_json::from_slice::<ReadinessStatus>(&saved)
            && saved.generation != generation
        {
            return Ok(());
        }
        let state = self.runtime.state().await;
        let (Some(definition), Some(observed_at_ms)) = (&state.definition, now_ms()) else {
            return Ok(());
        };
        write_json(
            &self.root.join("readiness-status.json"),
            &ReadinessStatus {
                report: state.verification.report.clone(),
                captured_end: buffered_end(&state, &self.readiness.domain).ok(),
                terminal: true,
                generation,
                epoch: state.epoch,
                invalidation_generation: state.verification.generation,
                definition_digest: definition.canonical_digest().map_err(|_| invalid())?,
                observed_at_ms,
                facts: self.readiness.facts(&state),
            },
        )
    }
    pub async fn serve(&self) -> coordinator::Result<()> {
        let mut handled = std::collections::BTreeSet::new();
        loop {
            // Supersede the previous generation before any network awaits.
            if let Some(generation) = active(&self.root)? {
                let state = self.runtime.state().await;
                if let (Some(definition), Some(observed_at_ms)) = (&state.definition, now_ms()) {
                    write_json(
                        &self.root.join("readiness-status.json"),
                        &ReadinessStatus {
                            report: state.verification.report.clone(),
                            captured_end: buffered_end(&state, &self.readiness.domain).ok(),
                            terminal: false,
                            generation,
                            epoch: state.epoch,
                            invalidation_generation: state.verification.generation,
                            definition_digest: definition
                                .canonical_digest()
                                .map_err(|_| invalid())?,
                            observed_at_ms,
                            facts: self.readiness.facts(&state),
                        },
                    )?;
                }
            }
            // Independent from verification completion. Connection or probe
            // failures revoke the old source observation immediately.
            let started = Instant::now();
            *self
                .readiness
                .destination
                .lock()
                .expect("destination readiness poisoned") = None;
            *self
                .readiness
                .fence
                .lock()
                .expect("fence inventory poisoned") = None;
            let probe = Box::pin(async {
                let replication = super::operational::replication(&self.source_config)?;
                let connected =
                    crate::pg::replication::ReplicationConnection::connect(&replication)
                        .await
                        .map_err(|_| invalid())?;
                let admin = Box::pin(session(&self.source_config, &self.definition.source))
                    .await
                    .map_err(|_| invalid())?;
                let state = self.runtime.state().await;
                let selected = self
                    .tables
                    .iter()
                    .map(|t| {
                        format!(
                            "\"{}\".\"{}\"",
                            t.selection.schema.replace('"', "\"\""),
                            t.selection.name.replace('"', "\"\"")
                        )
                    })
                    .collect::<Vec<_>>();
                let owner = active(&self.root)?.ok_or_else(invalid)?;
                super::fence_readiness::refresh(
                    &self.readiness.fence,
                    &admin.client,
                    super::fence_readiness::Request {
                        state: &state,
                        owner,
                        scope: selected,
                        started,
                    },
                )
                .await
                .map_err(|_| invalid())?;
                self.runtime
                    .probe_readiness(&admin.client, connected.lineage(), started)
                    .await?;
                Ok::<(), coordinator::Error>(())
            });
            if !matches!(
                tokio::time::timeout(Duration::from_secs(1), probe).await,
                Ok(Ok(()))
            ) {
                *self
                    .readiness
                    .fence
                    .lock()
                    .expect("fence inventory poisoned") = None;
                self.runtime.revoke_readiness_probe();
            }
            // These endpoint facts have a separate timeout and do not depend
            // on a successful source fence inventory or completed report.
            let started = Instant::now();
            let probe = async {
                let (source, destination) = tokio::join!(
                    session(&self.source_config, &self.definition.source),
                    session(&self.destination_config, &self.definition.destination)
                );
                let state = self.runtime.state().await;
                if state
                    .definition
                    .as_ref()
                    .and_then(|d| d.canonical_digest().ok())
                    != self.definition.canonical_digest().ok()
                {
                    return Err(invalid());
                }
                let owner = active(&self.root)?.ok_or_else(invalid)?;
                let scope = self
                    .tables
                    .iter()
                    .map(|t| {
                        format!(
                            "\"{}\".\"{}\"",
                            t.selection.schema.replace('"', "\"\""),
                            t.selection.name.replace('"', "\"\"")
                        )
                    })
                    .collect();
                super::destination_readiness::refresh(
                    &self.readiness.destination,
                    source.as_ref().ok().map(|s| &s.client),
                    destination.as_ref().ok().map(|s| &s.client),
                    &self.id,
                    super::fence_readiness::Request {
                        state: &state,
                        owner,
                        scope,
                        started,
                    },
                )
                .await;
                Ok::<(), coordinator::Error>(())
            };
            if !matches!(
                tokio::time::timeout(Duration::from_secs(1), probe).await,
                Ok(Ok(()))
            ) {
                *self
                    .readiness
                    .destination
                    .lock()
                    .expect("destination readiness poisoned") = None;
            }
            if let Some(generation) = active(&self.root)? {
                for entry in std::fs::read_dir(&self.root)? {
                    let path = entry?.path();
                    let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
                        continue;
                    };
                    if !name.starts_with(&format!("verify-{generation}-"))
                        || !name.ends_with(".request")
                        || handled.contains(name)
                    {
                        continue;
                    }
                    let bytes = std::fs::read(&path)?;
                    let Ok(request) = serde_json::from_slice::<Request>(&bytes) else {
                        continue;
                    };
                    if request.generation != generation {
                        continue;
                    }
                    handled.insert(name.to_owned());
                    let response = match Box::pin(self.execute(&request)).await {
                        Ok(response) => response,
                        Err(_) => unavailable(
                            "Independent verification failed; inspect endpoint, capture, apply and durable-state prerequisites.",
                        ),
                    };
                    write_json(&path.with_extension("response"), &response)?;
                }
            }
            // Refresh errors must revoke the cache. They do not manufacture an
            // engine report or terminate the copy owner from an observer task.
            let refreshed = self
                .readiness
                .refresh(&self.journal, &self.tables)
                .await
                .is_ok();
            if let Some(generation) = active(&self.root)? {
                let state = self.runtime.state().await;
                if let (Some(definition), Some(observed_at_ms)) = (&state.definition, now_ms()) {
                    write_json(
                        &self.root.join("readiness-status.json"),
                        &ReadinessStatus {
                            report: state.verification.report.clone(),
                            captured_end: buffered_end(&state, &self.readiness.domain).ok(),
                            terminal: false,
                            generation: generation.clone(),
                            epoch: state.epoch,
                            invalidation_generation: state.verification.generation,
                            definition_digest: definition
                                .canonical_digest()
                                .map_err(|_| invalid())?,
                            observed_at_ms,
                            facts: self.readiness.facts(&state),
                        },
                    )?;
                }
                if let Ok(live) = self.readiness.captured(&state) {
                    write_json(
                        &self.root.join("verification-status.json"),
                        &Status {
                            generation,
                            epoch: state.epoch,
                            invalidation_generation: state.verification.generation,
                            capture_lsn: live.commit,
                            capture_end: live.end,
                            report: state.verification.report.clone(),
                            verification: refreshed
                                && self.readiness.sample().verification.is_some(),
                        },
                    )?;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    async fn execute(&self, request: &Request) -> coordinator::Result<VerifyResponse> {
        let state = self.journal.load(true).await?;
        if !matches!(
            state.lifecycle.state,
            coordinator::State::CatchingUp | coordinator::State::VerifiedReady
        ) {
            return Ok(VerifyResponse {
                exit_code: 2,
                data: json!({"complete":false,"verification":false,"what":"Copy has not reached CatchingUp."}),
            });
        }
        let selected = self
            .tables
            .iter()
            .filter(|t| {
                request.tables.is_empty()
                    || request.tables.iter().any(|s| {
                        s == &t.key.table
                            || s == &format!("{}.{}", t.selection.schema, t.selection.name)
                    })
            })
            .collect::<Vec<_>>();
        if selected.is_empty()
            || request.tables.iter().any(|s| {
                !selected.iter().any(|t| {
                    s == &t.key.table
                        || s == &format!("{}.{}", t.selection.schema, t.selection.name)
                })
            })
        {
            return Ok(VerifyResponse {
                exit_code: 2,
                data: json!({"complete":false,"verification":false,"what":"Requested tables are outside the qualified selected scope."}),
            });
        }
        let start = Instant::now();
        let lineage = lineage(&state)?;
        let mut results = Vec::new();
        let mut details = Vec::new();
        let mut detail_budget = 100usize;
        let mut outstanding = Vec::new();
        let mut sections = durable::Sections {
            schema: section(0, vec![], "live source/destination catalog snapshots"),
            sequences: section(0, vec![], "live sequence snapshots"),
            constraints: section(0, vec![], "live constraint catalogs"),
            indexes: section(0, vec![], "live index catalogs"),
            destination_foreign_keys: section(0, vec![], "live paged destination FK anti-joins"),
        };
        for table in selected {
            let reference = state
                .chunk_plans
                .get(&table.key.table)
                .ok_or_else(invalid)?;
            let plan = self.journal.chunk_plan(reference).await?;
            let scope = scope(table, reference)?;
            let order = Order(
                table
                    .range_classifier(&plan, &format!("skedaddle-postgres:{}", self.id))
                    .map_err(|_| invalid())?,
            );
            let admin = Box::pin(session(&self.source_config, &self.definition.source))
                .await
                .map_err(|_| invalid())?;
            let _driver = admin.driver;
            let (evidence, mut waiter) = self
                .runtime
                .pg_verification(
                    admin.client,
                    table.key.clone(),
                    order.clone(),
                    engine::VerificationLimits {
                        max_keys: 100_000,
                        max_bytes: 16 * 1024 * 1024,
                        timeout: Duration::from_secs(30),
                    },
                )
                .map_err(|_| invalid())?;
            let columns = || {
                table
                    .columns
                    .iter()
                    .map(|c| sql::Column {
                        name: c.name.clone(),
                        pg_type: c.pg_type.clone(),
                    })
                    .collect()
            };
            let budget = sql::RangeBudget {
                max_rows: 100_000,
                max_bytes: 16 * 1024 * 1024,
            };
            let observed = Arc::new(Mutex::new(Observed::default()));
            let mut source = Source {
                pg: sql::PgRowSource::connect(
                    &self.source_config,
                    sql::Side::Source,
                    table.key.clone(),
                    columns(),
                    order.clone(),
                    budget,
                )
                .await
                .map_err(|_| invalid())?
                .with_change_evidence(evidence),
                observed: observed.clone(),
                start,
            };
            let mut destination = Destination {
                pg: sql::PgRowSource::connect(
                    &self.destination_config,
                    sql::Side::Destination,
                    table.key.clone(),
                    columns(),
                    order.clone(),
                    budget,
                )
                .await
                .map_err(|_| invalid())?,
                observed: observed.clone(),
                start,
            };
            let src = source.pg.collect_sections().await.map_err(|_| invalid())?;
            let dst = destination
                .pg
                .collect_sections()
                .await
                .map_err(|_| invalid())?;
            let differences = sql::diff_sections(&src, &dst);
            sections.schema.checked += src.columns.len() as u64;
            sections.schema.findings.extend(findings(
                &differences.schema,
                |v| v.object.clone(),
                |v| v.detail.clone(),
            ));
            sections.sequences.checked += src.sequences.len() as u64;
            sections.sequences.findings.extend(findings(
                &differences.sequences,
                |v| v.sequence.clone(),
                |v| v.detail.clone(),
            ));
            sections.constraints.checked += src.constraints.len() as u64;
            sections.constraints.findings.extend(findings(
                &differences.constraints,
                |v| v.constraint.clone(),
                |v| v.detail.clone(),
            ));
            sections.indexes.checked += src.indexes.len() as u64;
            sections.indexes.findings.extend(findings(
                &differences.indexes,
                |v| v.index.clone(),
                |v| v.detail.clone(),
            ));
            let fk = destination
                .pg
                .check_foreign_keys(1000)
                .await
                .map_err(|_| invalid())?;
            sections.destination_foreign_keys.checked +=
                dst.constraints.iter().filter(|c| c.kind == "f").count() as u64;
            sections.destination_foreign_keys.findings.extend(findings(
                &fk,
                |v| v.constraint.clone(),
                |v| format!("{} violating rows", v.violating_rows),
            ));
            let projection =
                verify::Projection::new(table.selection.columns.clone()).map_err(|_| invalid())?;
            let mut ranges = Vec::new();
            let mut verified = Vec::new();
            for range in &plan.ranges {
                let mut session =
                    verify::VerificationSession::new(range.clone(), projection.clone(), 2)
                        .map_err(|_| invalid())?;
                let result = session
                    .run(&mut source, &mut destination, &mut waiter)
                    .await
                    .map_err(|_| invalid())?
                    .clone();
                let through = observed.lock().expect("observation poisoned").through;
                // Let the already captured evidence marker apply naturally.
                // Calling an idle waiter too early emits a NEW source marker,
                // which would immediately make this completed interval stale.
                // This checks a committed source position, never bare mseq.
                tokio::time::timeout(Duration::from_secs(30), async {
                    loop {
                        let current = self.runtime.state().await;
                        let reached = current
                            .progress
                            .get("applied")
                            .and_then(|(marker, _)| engine::decode_reader_checkpoint(marker).ok())
                            .and_then(|checkpoint| checkpoint.last_position)
                            .is_some_and(|position| {
                                position.order_domain() == format!("skedaddle-postgres:{}", self.id)
                                    && position.bytes().len() == 8
                                    && u64::from_be_bytes(
                                        position.bytes().try_into().expect("checked LSN width"),
                                    ) >= through
                            });
                        if reached {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                })
                .await
                .map_err(|_| invalid())?;
                let pg_positions =
                    sql::PgSourcePosition::new(format!("skedaddle-postgres:{}", self.id));
                let applied = waiter
                    .wait_until(pg_positions.position(through))
                    .await
                    .map_err(|_| invalid())?;
                let applied = durable::pg_report_lsn("postgres", &applied, &pg_positions)
                    .map_err(|_| invalid())?;
                self.runtime.flush_progress().await.map_err(|_| invalid())?;
                let applied_state = self.runtime.state().await;
                observed.lock().expect("observation poisoned").applied = applied;
                let seen = observed.lock().expect("observation poisoned");
                let lower = boundary(&range.lo, true)?;
                let upper = boundary(&range.hi, false)?;
                let id = durable::range_identity(&scope, &lower, &upper)?;
                let c = counts(&result.comparison.coverage);
                if c.busy > 0 {
                    outstanding.push(id.clone());
                }
                let proof=json!({"producer":"runtime-pg-verification-v1","source_lsn":seen.source_lsn,"through_lsn":seen.through,"range":range.id,"changed_interval":"armed-before-source-snapshot; source-marker-after-destination-read; manifest-durable capture"}).to_string();
                ranges.push(durable::RangeResult {
                    id:id.clone(), lower, upper, counts:c, exclusions:vec![],
                    interval:durable::Interval {
                        armed:seen.armed, source_read:seen.source_read,
                        destination_read:seen.destination_read,
                        complete_through_lsn:seen.through, evidence:proof,
                    },
                    applied_lsn:seen.applied, applied_lineage:lineage.clone(),
                    applied_evidence:json!({"producer":"runtime-applied-source-waiter-v1","wait_target":seen.through,"returned_source_lsn":seen.applied,"authenticated_checkpoint":applied_state.progress.get("applied")}).to_string(),
                    source_digest:hex(&result.comparison.source_digest),
                    destination_digest:hex(&result.comparison.destination_digest),
                    retry_attempts:result.attempts-1,
                    exhausted_busy:result.comparison.coverage.busy,
                    outstanding_keys:result.comparison.busy.iter().map(|k|k.iter().map(scalar).collect()).collect::<coordinator::Result<_>>()?,
                });
                let differences=result.comparison.differences.iter().take(detail_budget).map(|difference| {
                    match difference {
                        verify::Difference::Missing { key }=>json!({"kind":"Missing","key":detail_key(key)}),
                        verify::Difference::Extra { key }=>json!({"kind":"Extra","key":detail_key(key)}),
                        verify::Difference::Changed { key,columns }=>json!({"kind":"Changed","key":detail_key(key),"columns":columns.iter().map(|c|&c.column).collect::<Vec<_>>() }),
                    }
                }).collect::<Vec<_>>();
                detail_budget -= differences.len();
                details.push(json!({"table":table.key.table,"range":id,"differences_total":result.comparison.differences.len(),"drilldown_truncated":differences.len()<result.comparison.differences.len(),"differences":differences,"busy_keys":result.comparison.busy}));
                verified.push(result);
            }
            let coverage =
                verify::TableCoverage::from_ranges(&order, &verified).map_err(|_| invalid())?;
            results.push(durable::TableResult {
                scope,
                counts: counts(&coverage.rows),
                ranges,
            });
        }
        let latest = self.journal.load(true).await?;
        if latest.epoch != state.epoch
            || latest.verification.generation != state.verification.generation
            || active(&self.root)? != Some(request.generation.clone())
        {
            return Err(invalid());
        }
        let first = results
            .iter()
            .flat_map(|t| &t.ranges)
            .map(|r| r.interval.armed)
            .min()
            .ok_or_else(invalid)?;
        let first_source = results
            .iter()
            .flat_map(|t| &t.ranges)
            .map(|r| r.interval.source_read)
            .min()
            .ok_or_else(invalid)?;
        let last = results
            .iter()
            .flat_map(|t| &t.ranges)
            .map(|r| r.interval.destination_read)
            .max()
            .ok_or_else(invalid)?;
        let through = results
            .iter()
            .flat_map(|t| &t.ranges)
            .map(|r| r.interval.complete_through_lsn)
            .min()
            .ok_or_else(invalid)?;
        let mut report=durable::Report {
            version:durable::VERSION, coverage_version:durable::VERSION,
            migration:self.id.clone(),
            definition_digest:self.definition.canonical_digest().map_err(|_|invalid())?,
            run:request.run.to_string(), epoch:state.epoch,
            generation:state.verification.generation, lineage, completed:true,
            interval:durable::Interval {
                armed:first, source_read:first_source, destination_read:last,
                complete_through_lsn:through,
                evidence:"All independently observed range intervals embedded inline; global through LSN is the minimum, never promoted.".into(),
            },
            tables:results,
            retry:durable::RetryPolicy {
                version:durable::VERSION,
                policy:"whole-range retries with complete changed-key evidence".into(),
                budget:2, outstanding_ranges:outstanding,
            },
            dead_letters:durable::DeadLetters {
                collection:"coordinator.verifier_exceptions".into(), completed:true,
                count:latest.verifier_exceptions.len() as u64,
                identities:latest.verifier_exceptions.keys().cloned().collect(),
                evidence:format!("authenticated journal refresh through sequence {}",latest.applied_seq),
            },
            sections, coverage_digest:String::new(),
        };
        report.coverage_digest = report.computed_coverage_digest()?;
        report.validate()?;
        let full_scope = report.tables.len() == self.tables.len();
        let clean = report.tables.iter().all(|t| {
            t.counts.total == t.counts.verified
                && t.counts.busy == 0
                && t.counts.mismatched == 0
                && t.counts.excluded == 0
        }) && report.dead_letters.count == 0
            && [
                &report.sections.schema,
                &report.sections.sequences,
                &report.sections.constraints,
                &report.sections.indexes,
                &report.sections.destination_foreign_keys,
            ]
            .iter()
            .all(|s| s.findings.is_empty());
        if full_scope {
            let reference = self.journal.publish_verification(&report).await?;
            self.runtime
                .commit(Transition::VerificationCompleted {
                    report: Box::new(reference),
                })
                .await?;
        }
        self.readiness.refresh(&self.journal, &self.tables).await?;
        let sample = self.readiness.sample();
        let current = full_scope && clean && sample.verification.is_some();
        Ok(VerifyResponse {
            exit_code: if current { 0 } else { 2 },
            data: json!({"complete":full_scope,"verification":current,"readiness_verification":sample.verification.is_some(),"report":report,"ranges":details,"partial_scope":!full_scope}),
        })
    }
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|v| format!("{v:02x}")).collect()
}
fn detail_key(key: &verify::Key) -> Json {
    match serde_json::to_vec(key) {
        Ok(bytes) if bytes.len() <= 2048 => {
            serde_json::from_slice(&bytes).expect("encoded key JSON")
        }
        _ => json!({"truncated":true,"digest":durable::digest(key).ok()}),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture() -> (
        Materialized,
        durable::CurrentFacts,
        verify::VerificationReport,
    ) {
        let def = MigrationDefinition::from_json(
            include_str!("../../README.md")
                .split("```json")
                .nth(1)
                .unwrap()
                .split("```")
                .next()
                .unwrap(),
        )
        .unwrap();
        let scope = durable::TableScope {
            schema: "public".into(),
            table: "accounts".into(),
            catalog_digest: "a".repeat(64),
            plan_digest: "b".repeat(64),
            key: vec![durable::KeyColumn {
                name: "id".into(),
                kind: durable::KeyType::Int8,
                descending: false,
            }],
        };
        let lineage = durable::Lineage {
            system: "qualified-source".into(),
            database: "db".into(),
            timeline: 1,
        };
        let current = durable::CurrentFacts {
            migration: "m".into(),
            definition_digest: def.canonical_digest().unwrap(),
            epoch: 1,
            generation: 0,
            lineage: lineage.clone(),
            scope: vec![scope.clone()],
            capture_lsn: 100,
        };
        let summary = durable::Summary {
            migration: "m".into(),
            definition_digest: current.definition_digest.clone(),
            run: "run".into(),
            epoch: 1,
            generation: 0,
            lineage,
            scope: vec![scope],
            through_lsn: 100,
            ranges: 1,
            counts: durable::Counts {
                total: 2,
                verified: 2,
                ..Default::default()
            },
            exhausted_busy: 0,
            dead_letters: 0,
            sections_clean: true,
        };
        let mut state = Materialized {
            definition: Some(def),
            epoch: 1,
            ..Default::default()
        };
        state.verification.report = Some(Box::new(durable::ReportRef {
            version: durable::VERSION,
            digest: "c".repeat(64),
            bytes: 100,
            summary,
        }));
        (state, current, verify::VerificationReport::default())
    }
    #[test]
    fn fresh_catalog_gate_preserves_rows_and_revokes_condition6_section() {
        let mut report = verify::VerificationReport {
            indexes: verify::ResultSlot::Collected {
                checked: 1,
                findings: vec![],
                evidence: vec![],
            },
            ..Default::default()
        };
        report.tables.push(verify::TableCoverage {
            table: "t".into(),
            rows: verify::Coverage {
                total: 2,
                verified: 2,
                ..Default::default()
            },
            exclusion_reason: None,
        });
        let positive = gate_catalog_sections(report.clone(), true);
        assert_eq!(positive, report);
        let blocked = gate_catalog_sections(report.clone(), false);
        assert!(matches!(blocked.indexes, verify::ResultSlot::NotCollected));
        assert_eq!(blocked.tables, report.tables);
        assert_eq!(blocked.foreign_keys, report.foreign_keys);
    }
    #[test]
    fn runner_verification_projects_only_current_exact_completed_scope() {
        let (mut state, mut current, report) = fixture();
        assert!(project(&state, &current, &report).verification.is_some());
        current.capture_lsn = 101;
        let stale = project(&state, &current, &report);
        assert!(stale.verification.is_none());
        assert_eq!(stale.dead_letters, None);
        assert_eq!(stale.busy_rows_past_budget, None);
        current.capture_lsn = 100;
        let exact = current.scope.clone();
        let mut extra = current.scope[0].clone();
        extra.table = "unreported_table".into();
        current.scope.push(extra);
        assert!(project(&state, &current, &report).verification.is_none());
        current.scope = exact;
        assert!(project(&state, &current, &report).verification.is_some());
        current.scope.clear();
        assert!(project(&state, &current, &report).verification.is_none());
        let (_, current, _) = fixture();
        state.verification.report = None;
        assert!(project(&state, &current, &report).verification.is_none());
    }
    #[tokio::test]
    async fn failed_real_journal_refresh_revokes_retained_readiness() {
        let root = PathBuf::from(
            std::env::var_os("SKEDADDLE_TEST_ROOT").expect("set SKEDADDLE_TEST_ROOT"),
        )
        .join(format!("runner-readiness-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let journal = Journal::new(
            Arc::new(coordinator::journal::local_store(&root).unwrap()),
            "runner",
            "m",
            Duration::from_secs(3),
        )
        .unwrap();
        let coordinator = coordinator::epoch::Coordinator::observer(
            journal.clone(),
            coordinator::local::Local::create(root.join("state.local")).unwrap(),
            "owner",
        )
        .unwrap();
        let engine = engine::Engine::new(coordinator, journal.clone());
        let source = Readiness::new(engine.runtime(), "m", None);
        *source.authenticated.lock().unwrap() = Some(fixture());
        assert!(source.refresh(&journal, &[]).await.is_err());
        assert!(source.authenticated.lock().unwrap().is_none());
        assert!(source.sample().verification.is_none());
    }
    #[tokio::test]
    async fn buffer_fact_survives_missing_verification_and_failed_refresh() {
        use crate::{
            buffer::backends::ObjectBufferStore,
            definition::{BufferBackend, LocalFsOptions},
        };
        let root = PathBuf::from(std::env::var_os("SKEDADDLE_TEST_ROOT").unwrap()).join(format!(
            "runner-independent-readiness-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let store = Arc::new(ObjectBufferStore::local(&root, Duration::from_secs(3)).unwrap());
        let journal = Journal::new(
            Arc::new(coordinator::journal::local_store(&root).unwrap()),
            "runner",
            "m",
            Duration::from_secs(3),
        )
        .unwrap();
        let mut coordinator = coordinator::epoch::Coordinator::observer(
            journal.clone(),
            coordinator::local::Local::create(root.join("state.local")).unwrap(),
            "owner",
        )
        .unwrap();
        coordinator.acquire().await.unwrap();
        let mut definition = fixture().0.definition.unwrap();
        definition.buffer.backend = BufferBackend::LocalFs(LocalFsOptions {
            path: root.to_str().unwrap().into(),
        });
        coordinator
            .commit(Transition::Define {
                definition: Box::new(definition),
            })
            .await
            .unwrap();
        let engine = engine::Engine::new(coordinator, journal.clone());
        let source = Readiness::new(engine.runtime(), "m", Some(store));
        let sample = source.sample();
        assert_eq!(sample.buffer_contract_matches, Some(true));
        assert!(sample.verification.is_none());
        assert_eq!(sample.sequences_syncable, Some(false));
        assert_eq!(sample.reverse_prerequisites, Some(false));
        let facts = source.facts(&engine.runtime().state().await);
        for fact in [
            &facts.destination_validity,
            &facts.sequences_syncable,
            &facts.reverse_prerequisites,
        ] {
            assert!(!fact.ready);
            assert!(
                fact.blocker
                    .as_ref()
                    .is_some_and(|b| !b.remediation.is_empty())
            );
        }
        assert!(sample.apply_lag.is_none());
        assert!(sample.schema_jobs.is_none());
        assert!(sample.fence_risks_acknowledged.is_none());
        assert!(source.refresh(&journal, &[]).await.is_err());
        assert_eq!(source.sample().buffer_contract_matches, Some(true));
    }
    #[test]
    fn published_readiness_expires_and_rejects_wrong_owner_definition_and_epoch() {
        use crate::{
            buffer::backends::ObjectBufferStore,
            definition::{BufferBackend, LocalFsOptions},
        };
        let root = PathBuf::from(std::env::var_os("SKEDADDLE_TEST_ROOT").unwrap()).join(format!(
            "runner-published-readiness-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let owner = control_file(&root, "owner.lock").unwrap();
        owner.lock().unwrap();
        let generation = uuid::Uuid::new_v4().to_string();
        std::fs::write(root.join("owner-generation"), &generation).unwrap();
        let store = ObjectBufferStore::local(&root, Duration::from_secs(3)).unwrap();
        let mut state = fixture().0;
        state.definition.as_mut().unwrap().buffer.backend =
            BufferBackend::LocalFs(LocalFsOptions {
                path: root.to_str().unwrap().into(),
            });
        let mut status = ReadinessStatus {
            report: state.verification.report.clone(),
            captured_end: buffered_end(&state, "skedaddle-postgres:m").ok(),
            terminal: false,
            generation: generation.clone(),
            epoch: state.epoch,
            invalidation_generation: state.verification.generation,
            definition_digest: state
                .definition
                .as_ref()
                .unwrap()
                .canonical_digest()
                .unwrap(),
            observed_at_ms: now_ms().unwrap(),
            facts: super::super::readiness::Facts::sample(state.definition.as_ref(), Some(&store)),
        };
        let path = root.join("readiness-status.json");
        write_json(&path, &status).unwrap();
        assert!(readiness_status(&root, "m", &state).buffer_contract.ready);
        state
            .progress
            .insert("buffered".into(), ("manifest-durable".into(), "123".into()));
        assert!(!readiness_status(&root, "m", &state).buffer_contract.ready);
        state.progress.remove("buffered");
        status.captured_end = Some(999);
        write_json(&path, &status).unwrap();
        assert!(!readiness_status(&root, "m", &state).buffer_contract.ready);
        status.captured_end = buffered_end(&state, "skedaddle-postgres:m").ok();
        status.report = None;
        write_json(&path, &status).unwrap();
        assert!(!readiness_status(&root, "m", &state).buffer_contract.ready);
        status.report = state.verification.report.clone();
        status.facts.destination_expires_at_ms = Some(now_ms().unwrap().saturating_sub(1));
        write_json(&path, &status).unwrap();
        assert!(!readiness_status(&root, "m", &state).buffer_contract.ready);
        status.facts.destination_expires_at_ms = None;
        status.observed_at_ms = now_ms().unwrap().saturating_sub(251);
        write_json(&path, &status).unwrap();
        assert!(!readiness_status(&root, "m", &state).buffer_contract.ready);
        status.observed_at_ms = now_ms().unwrap() + 60_000;
        write_json(&path, &status).unwrap();
        assert!(!readiness_status(&root, "m", &state).buffer_contract.ready);
        status.observed_at_ms = now_ms().unwrap();
        status.definition_digest = "0".repeat(64);
        write_json(&path, &status).unwrap();
        assert!(!readiness_status(&root, "m", &state).buffer_contract.ready);
        status.definition_digest = state
            .definition
            .as_ref()
            .unwrap()
            .canonical_digest()
            .unwrap();
        status.epoch += 1;
        write_json(&path, &status).unwrap();
        assert!(!readiness_status(&root, "m", &state).buffer_contract.ready);
        status.epoch = state.epoch;
        status.invalidation_generation += 1;
        write_json(&path, &status).unwrap();
        assert!(!readiness_status(&root, "m", &state).buffer_contract.ready);
        status.invalidation_generation = state.verification.generation;
        status.generation = uuid::Uuid::new_v4().to_string();
        write_json(&path, &status).unwrap();
        assert!(!readiness_status(&root, "m", &state).buffer_contract.ready);
        status.generation = generation;
        status.observed_at_ms = now_ms().unwrap();
        write_json(&path, &status).unwrap();
        assert!(readiness_status(&root, "m", &state).buffer_contract.ready);
        drop(owner);
        assert!(!readiness_status(&root, "m", &state).buffer_contract.ready);
    }

    #[tokio::test]
    async fn actual_ddl_exit_publishes_terminal_boundary_after_real_owner_retirement() {
        let root = PathBuf::from(std::env::var_os("SKEDADDLE_TEST_ROOT").unwrap())
            .join(format!("terminal-ddl-{}", uuid::Uuid::new_v4()));
        let control = root.join("m.control");
        let (runtime, journal) =
            engine::tests::live_readiness_tests::retired_ddl_runtime(control.clone()).await;
        let state = runtime.state().await;
        let definition = state.definition.clone().unwrap();
        let readiness = Readiness::new(runtime.clone(), "m", None);
        *readiness.fence.lock().unwrap() = super::super::fence_readiness::Observation::new(
            &state,
            "retired-owner".into(),
            vec!["s.t".into()],
            vec![],
            Instant::now(),
        );
        let config = tls::Config::new(tokio_postgres::Config::new(), PathBuf::new());
        let service = Service {
            root: control.clone(),
            id: "m".into(),
            definition,
            source_config: config.clone(),
            destination_config: config,
            tables: vec![],
            runtime,
            journal,
            readiness,
        };
        assert!(!control.join("owner-generation").exists());
        service.publish_terminal_readiness().await.unwrap();
        assert!(service.readiness.fence.lock().unwrap().is_none());
        let saved: ReadinessStatus =
            serde_json::from_slice(&std::fs::read(control.join("readiness-status.json")).unwrap())
                .unwrap();
        assert!(saved.terminal && !saved.facts.apply_lag.ready && !saved.facts.schema_jobs.ready);
        let public = verification_status(&root, "m", &state);
        assert_eq!(public["readiness"]["schema_jobs"]["ready"], false);
        assert!(
            public["readiness"]["schema_jobs"]["blocker"]["reason"]
                .as_str()
                .unwrap()
                .contains("Last capture boundary")
        );
        assert!(
            !public["readiness"]["schema_jobs"]["blocker"]["remediation"]
                .as_str()
                .unwrap()
                .is_empty()
        );
        // A newer generation's snapshot must survive an older final writer.
        let newer = uuid::Uuid::new_v4().to_string();
        let mut replacement = saved;
        replacement.generation = newer.clone();
        write_json(&control.join("readiness-status.json"), &replacement).unwrap();
        service.publish_terminal_readiness().await.unwrap();
        let saved: ReadinessStatus =
            serde_json::from_slice(&std::fs::read(control.join("readiness-status.json")).unwrap())
                .unwrap();
        assert_eq!(saved.generation, newer);
    }
}
