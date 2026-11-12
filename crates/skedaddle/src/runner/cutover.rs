//! Generation-scoped owner requests. Only the authenticated Runtime journal
//! stores cutover state; control files never authorize database effects.
use super::*;
use crate::{
    coordinator::{self, Event, Materialized, State as Lifecycle, Transition, readiness},
    cutover::{self as driver, Destination as _, Reverse as _, Status as _},
    engine::{self, Capture},
    event::{AssembledTxn, EventKind, SourceIdentity},
    pg::{activate, chunk, fence, reverse},
    verify::{self, RowSource, sql},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    fs::{File, OpenOptions, TryLockError},
    io::Write,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::sync::watch;

const STREAM: &str = "runner-cutover-v1";
const WRITES: &str = "runner-cutover-write-v1";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepTiming {
    pub step: driver::Step,
    pub elapsed_ms: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CutoverReason {
    pub code: readiness::BlockerCode,
    pub message: String,
    pub next_step: String,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CutoverErrorKind {
    InvalidRequest,
    NoOwner,
    StaleGeneration,
    OwnerStopped,
    Infrastructure,
    LostOwnership,
    Driver,
    ActivationUncertain,
    SourceBreach,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CutoverError {
    pub kind: CutoverErrorKind,
    pub message: String,
}
/// CLI envelope. Failed outcomes never contain raw endpoint/transport errors.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum CutoverOutcome {
    Completed {
        cutover_id: String,
        step_timings_ms: Vec<StepTiming>,
        write_pause_ms: u64,
    },
    NotReady {
        cutover_id: Option<String>,
        step_timings_ms: Vec<StepTiming>,
        write_pause_ms: Option<u64>,
        reasons: Vec<CutoverReason>,
        next_step: String,
    },
    DeadlineAborted {
        cutover_id: String,
        step_timings_ms: Vec<StepTiming>,
        write_pause_ms: Option<u64>,
        next_step: String,
    },
    Failed {
        cutover_id: Option<String>,
        step_timings_ms: Vec<StepTiming>,
        write_pause_ms: Option<u64>,
        error: CutoverError,
        next_step: String,
    },
}
fn failed(kind: CutoverErrorKind) -> CutoverOutcome {
    CutoverOutcome::Failed {
        cutover_id: None,
        step_timings_ms: vec![],
        write_pause_ms: None,
        error: CutoverError { kind, message: "Owner cutover request could not complete safely; endpoint details redacted.".into() },
        next_step: "Inspect the owner and authenticated migration state, restore endpoint access, and retry with the active owner.".into(),
    }
}
fn invalid() -> coordinator::Error {
    coordinator::Error::RecoveryRequired(
        "Owner cutover state or endpoint operation unavailable (details redacted)".into(),
    )
}
fn component<T>(_: T) -> driver::Error {
    driver::Error::Component("owner cutover adapter failed (details redacted)".into())
}
fn file(root: &Path, name: &str) -> std::io::Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(root.join(name))
}
fn active(root: &Path) -> std::io::Result<Option<String>> {
    if !root.is_dir() {
        return Ok(None);
    }
    let gate = file(root, "control.lock")?;
    gate.lock()?;
    let owner = file(root, "owner.lock")?;
    match owner.try_lock() {
        Ok(()) => Ok(None),
        Err(TryLockError::WouldBlock) => Ok(std::fs::read_to_string(root.join("owner-generation"))
            .ok()
            .and_then(|s| uuid::Uuid::parse_str(&s).ok())
            .map(|id| id.to_string())),
        Err(TryLockError::Error(error)) => Err(error),
    }
}
fn write_json(path: &Path, value: &impl Serialize) -> coordinator::Result<()> {
    let staging = path.with_extension(format!("{}.staging", uuid::Uuid::new_v4()));
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&staging)?;
    output.write_all(&serde_json::to_vec(value)?)?;
    output.sync_all()?;
    drop(output);
    std::fs::rename(staging, path)?;
    Ok(())
}
#[derive(Serialize, Deserialize)]
struct Request {
    generation: String,
    request: String,
    deadline_ms: u64,
}
/// Any process may request cutover. This API never creates/acquires a coordinator.
/// `deadline` is a duration; admission persists an absolute destination-clock
/// deadline once. Retrying an admitted cutover retains that original deadline.
pub async fn request_cutover(
    root: &Path,
    id: &str,
    deadline: Duration,
) -> Result<CutoverOutcome, Failure> {
    if id.is_empty()
        || !id
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
        || deadline.is_zero()
    {
        return Ok(failed(CutoverErrorKind::InvalidRequest));
    }
    if let Some(outcome) = terminal(root, id).await.ok().flatten() {
        return Ok(outcome);
    }
    let control = root.join(format!("{id}.control"));
    let Some(generation) = active(&control).map_err(|_| infrastructure())? else {
        return Ok(terminal(root, id)
            .await
            .ok()
            .flatten()
            .unwrap_or_else(|| failed(CutoverErrorKind::NoOwner)));
    };
    let request = Request {
        generation: generation.clone(),
        request: uuid::Uuid::new_v4().to_string(),
        deadline_ms: deadline
            .as_millis()
            .try_into()
            .map_err(|_| infrastructure())?,
    };
    let path = control.join(format!("cutover-{generation}-{}.request", request.request));
    write_json(&path, &request).map_err(|_| infrastructure())?;
    let response = path.with_extension("response");
    let mut retired = None;
    loop {
        if let Ok(bytes) = std::fs::read(&response) {
            return serde_json::from_slice(&bytes).map_err(|_| infrastructure());
        }
        if active(&control).map_err(|_| infrastructure())? != Some(generation.clone()) {
            if let Some(outcome) = terminal(root, id).await.ok().flatten() {
                return Ok(outcome);
            }
            // The engine retires its local generation before the guarded service
            // publishes terminal lifecycle/response. Allow that joined tail to finish.
            let tail = retired.get_or_insert_with(tokio::time::Instant::now);
            if tail.elapsed() >= Duration::from_secs(5) {
                return Ok(failed(CutoverErrorKind::OwnerStopped));
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Saved {
    state: driver::State,
    deadline_ms: u64,
    source: fence::Fence,
    destination: activate::DestinationFence,
    drained: driver::DrainCounts,
    timings: Vec<StepTiming>,
    aborted_at_ms: Option<u64>,
    #[serde(default)]
    deadline_abort: Option<bool>,
    last_clock_ms: u64,
    #[serde(default)]
    outcome: Option<CutoverOutcome>,
}
fn replay(state: &Materialized) -> coordinator::Result<Option<Saved>> {
    state
        .progress
        .get(STREAM)
        .map(|(bytes, _)| serde_json::from_str(bytes).map_err(|_| invalid()))
        .transpose()
}

/// Original authenticated snapshots; rollback must never snapshot fenced source grants.
pub(super) fn rollback_fences(
    state: &Materialized,
) -> coordinator::Result<(fence::Fence, activate::Config)> {
    let saved = replay(state)?.ok_or_else(invalid)?;
    Ok((saved.source, saved.destination.config))
}

pub(super) fn rollback_drain(state: &Materialized) -> coordinator::Result<u64> {
    replay(state)?
        .and_then(|s| s.source.drain_lsn)
        .filter(|lsn| *lsn != 0)
        .ok_or_else(invalid)
}

pub(super) fn terminal_outcome(
    state: &Materialized,
) -> coordinator::Result<Option<CutoverOutcome>> {
    if !matches!(
        state.lifecycle.state,
        Lifecycle::DestinationActive
            | Lifecycle::RollbackWindow
            | Lifecycle::Finalized
            | Lifecycle::AbortedToSource
    ) {
        return Ok(None);
    }
    let saved = replay(state)?.ok_or_else(invalid)?;
    let outcome = saved.outcome.ok_or_else(invalid)?;
    let valid = match (&outcome, state.lifecycle.state, saved.state.next) {
        (
            CutoverOutcome::Completed { cutover_id, .. },
            Lifecycle::DestinationActive | Lifecycle::RollbackWindow | Lifecycle::Finalized,
            driver::Step::Complete,
        )
        | (
            CutoverOutcome::DeadlineAborted { cutover_id, .. },
            Lifecycle::AbortedToSource,
            driver::Step::Aborted,
        ) => cutover_id == &saved.state.key.cutover,
        (
            CutoverOutcome::Failed {
                cutover_id: Some(cutover_id),
                ..
            },
            Lifecycle::AbortedToSource,
            driver::Step::Aborted,
        ) => cutover_id == &saved.state.key.cutover,
        _ => false,
    };
    if !valid {
        return Err(invalid());
    }
    Ok(Some(outcome))
}
async fn terminal(root: &Path, id: &str) -> coordinator::Result<Option<CutoverOutcome>> {
    let journal = super::operational::open_journal(root, id).await?;
    terminal_outcome(&journal.load(true).await?)
}

async fn admission(engine: &engine::Engine) -> coordinator::Result<Option<CutoverOutcome>> {
    let facts = engine.evaluate_readiness().await?;
    Ok(match readiness::evaluate(&facts) {
        readiness::Readiness::Ready => None,
        readiness::Readiness::NotReady { reasons } => Some(CutoverOutcome::NotReady {
            cutover_id: None,
            step_timings_ms: vec![],
            write_pause_ms: None,
            reasons: reasons
                .into_iter()
                .map(|r| CutoverReason {
                    code: r.code,
                    message: r.code.message().into(),
                    next_step: r.code.remediation().into(),
                })
                .collect(),
            next_step:
                "Resolve every blocking reason and refresh live readiness before retrying cutover."
                    .into(),
        }),
    })
}

/// Capture-side durable breach observation. Controls do not represent writes;
/// data/DDL, including reverted writes and key changes, do. It is persisted
/// before the engine can advance its ordinary capture checkpoint.
#[derive(Clone)]
pub(super) struct Monitor {
    written: Arc<AtomicU64>,
    captured: Arc<AtomicU64>,
}
impl Monitor {
    pub fn new(state: &Materialized) -> coordinator::Result<Self> {
        let written = state
            .progress
            .get(WRITES)
            .map(|(s, _)| s.parse::<u64>().map_err(|_| invalid()))
            .transpose()?
            .unwrap_or(0);
        Ok(Self {
            written: Arc::new(AtomicU64::new(written)),
            captured: Arc::new(AtomicU64::new(0)),
        })
    }
    pub fn capture<C>(
        &self,
        capture: C,
        runtime: engine::Runtime,
        installation: String,
        origin: String,
    ) -> Observed<C> {
        Observed {
            capture,
            runtime,
            monitor: self.clone(),
            installation,
            origin,
        }
    }
}
pub(super) struct Observed<C> {
    capture: C,
    runtime: engine::Runtime,
    monitor: Monitor,
    installation: String,
    origin: String,
}
impl<C: Capture> Capture for Observed<C> {
    async fn prepare_schema(
        &mut self,
        journal: &coordinator::journal::Journal,
        runtime: &engine::Runtime,
    ) -> coordinator::Result<()> {
        self.capture.prepare_schema(journal, runtime).await
    }
    fn schema_state(&self) -> Option<source::SchemaState> {
        self.capture.schema_state()
    }
    fn writer_checkpoint(
        &self,
    ) -> coordinator::Result<Option<crate::buffer::writer::WriterCheckpoint>> {
        self.capture.writer_checkpoint()
    }
    async fn step(&mut self) -> coordinator::Result<engine::Captured> {
        let batch = self.capture.step().await?;
        if let Some(txn) = &batch.transaction {
            let lsn = u64::from_be_bytes(
                txn.commit_position
                    .bytes()
                    .try_into()
                    .map_err(|_| invalid())?,
            );
            if application_write(txn, &self.installation, &self.origin) {
                self.runtime
                    .marker(WRITES, lsn.to_string(), "captured-data-or-ddl".into())
                    .await?;
                self.monitor.written.fetch_max(lsn, Ordering::SeqCst);
            }
            self.monitor.captured.fetch_max(lsn, Ordering::SeqCst);
        }
        if let Some(lsn) = batch.durable_lsn {
            self.runtime
                .marker(
                    "source-capture-durable-v1",
                    lsn.to_string(),
                    "breach-monitor-checked".into(),
                )
                .await?;
        }
        Ok(batch)
    }
    async fn publish(
        &mut self,
        high: u64,
        rows: AssembledTxn,
    ) -> coordinator::Result<crate::buffer::writer::DurableManifestRef> {
        self.capture.publish(high, rows).await
    }
}

// Capture has already checked cross-transaction schema continuity. Reuse the
// real apply preflight for canonical shape, XID, ordered versions and D8/D10's
// shared predicate. An empty mapping means ANY data in a mixed transaction
// remains a breach; malformed/foreign/application DDL fails closed too.
fn application_write(txn: &AssembledTxn, installation: &str, reverse_origin: &str) -> bool {
    if txn
        .source_origin
        .as_ref()
        .is_some_and(|origin| origin.name == reverse_origin)
    {
        return false;
    }
    if txn
        .events
        .iter()
        .all(|event| matches!(event.kind, EventKind::Watermark | EventKind::FinalMarker))
    {
        return false;
    }
    crate::pg::apply::plan_transaction_with_ddl(
        txn,
        &[],
        &crate::pg::apply::PlanContext {
            stream: "cutover-breach-monitor".into(),
            mseq: 0,
            origin: reverse_origin.into(),
            synthetic_commit_time: Some(0),
        },
        installation,
        |_, _| {
            Err(crate::pg::apply::ApplyError::Invalid(
                "unexpected mapped write",
            ))
        },
    )
    .is_err()
}

/// Owner-only service. `joined` lets the driver stop the forward workers at the
/// frozen boundary before activation changes the durable lifecycle.
pub(super) struct Service {
    pub engine: Arc<engine::Engine>,
    pub root: PathBuf,
    pub id: String,
    pub definition: MigrationDefinition,
    pub source_config: tls::Config,
    pub destination_config: tls::Config,
    pub tables: Vec<catalog::TableCatalog>,
    pub journal: coordinator::journal::Journal,
    pub source: SourceIdentity,
    pub monitor: Monitor,
    pub pause: watch::Sender<bool>,
    pub joined: watch::Receiver<bool>,
}
impl Service {
    pub async fn serve(&self) -> coordinator::Result<()> {
        let runtime = self.engine.runtime();
        let mut handled = BTreeSet::new();
        let mut retry_at = tokio::time::Instant::now();
        loop {
            if *self.joined.borrow() {
                return Ok(());
            }
            let live = runtime.live_readiness();
            if live.running
                && let Some(generation) = live.owner_generation
            {
                if active(&self.root)?.as_deref() != Some(&generation) {
                    return Err(invalid());
                }
                let state = runtime.state().await;
                if tokio::time::Instant::now() >= retry_at && replay(&state)?.is_some() {
                    let _ = self.execute(1).await?;
                    retry_at = tokio::time::Instant::now() + Duration::from_secs(1);
                    if *self.pause.borrow() {
                        return Ok(());
                    }
                }
                for entry in std::fs::read_dir(&self.root)? {
                    let path = entry?.path();
                    let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
                        continue;
                    };
                    if !name.starts_with("cutover-")
                        || !name.ends_with(".request")
                        || handled.contains(name)
                    {
                        continue;
                    }
                    let Ok(request) = serde_json::from_slice::<Request>(&std::fs::read(&path)?)
                    else {
                        continue;
                    };
                    handled.insert(name.to_string());
                    let response = self.response(&generation, &request).await;
                    write_json(&path.with_extension("response"), &response)?;
                    if *self.pause.borrow() {
                        return Ok(());
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }
    async fn response(&self, generation: &str, request: &Request) -> CutoverOutcome {
        if request.generation != generation {
            return failed(CutoverErrorKind::StaleGeneration);
        }
        if request.deadline_ms == 0 || uuid::Uuid::parse_str(&request.request).is_err() {
            return failed(CutoverErrorKind::InvalidRequest);
        }
        self.execute(request.deadline_ms)
            .await
            .unwrap_or_else(|_| failed(CutoverErrorKind::Infrastructure))
    }
    async fn execute(&self, deadline_ms: u64) -> coordinator::Result<CutoverOutcome> {
        let runtime = self.engine.runtime();
        runtime
            .state()
            .await
            .operational_schema
            .require_clear()
            .map_err(|e| coordinator::Error::RecoveryRequired(e.to_string()))?;
        let saved = replay(&runtime.state().await)?;
        // A fenced replay cannot meet pre-fence live readiness again. Its
        // authenticated admitted state/deadline, not a UI snapshot, resumes it.
        if saved.is_none()
            && let Some(outcome) = admission(&self.engine).await?
        {
            return Ok(outcome);
        }
        let state = runtime.state().await;
        let key = driver::Key {
            migration: self.id.clone(),
            cutover: saved.as_ref().map_or_else(
                || uuid::Uuid::new_v4().to_string(),
                |s| s.state.key.cutover.clone(),
            ),
            epoch: state.epoch.try_into().map_err(|_| invalid())?,
            coordinator: state.owner.clone(),
        };
        let progress = runtime
            .final_marker_progress(&key, &self.source)
            .map_err(|_| invalid())?;
        let task = Task {
            runtime,
            engine: self.engine.clone(),
            key,
            saved,
            deadline_ms,
            definition: self.definition.clone(),
            source_config: self.source_config.clone(),
            destination_config: self.destination_config.clone(),
            tables: self.tables.clone(),
            journal: self.journal.clone(),
            progress,
            monitor: self.monitor.clone(),
            pause: self.pause.clone(),
            joined: self.joined.clone(),
        };
        tokio::task::spawn_blocking(move || task.run())
            .await
            .map_err(|_| invalid())?
    }
}

/// Dedicated synchronous administrative transports; the blocking driver owns
/// their runtime. Reverse setup can move them to its joined bridge thread.
pub(super) struct Pg {
    rt: Arc<tokio::runtime::Runtime>,
    session: Session,
    config: tls::Config,
    connection: Connection,
    healthy: bool,
}
impl Pg {
    pub(super) fn open(
        rt: Arc<tokio::runtime::Runtime>,
        config: &tls::Config,
        connection: &Connection,
    ) -> driver::Result<Self> {
        let session = rt
            .block_on(session(config, connection))
            .map_err(component)?;
        rt.block_on(settings(&session.client)).map_err(component)?;
        Ok(Self {
            rt,
            session,
            config: config.clone(),
            connection: connection.clone(),
            healthy: true,
        })
    }
}
impl fence::Connection for Pg {
    fn query(&mut self, sql: &str, timeout: Duration) -> Result<fence::Rows, String> {
        if !self.healthy {
            self.session = self
                .rt
                .block_on(session(&self.config, &self.connection))
                .map_err(|_| {
                    "administrative PostgreSQL reconnect failed (details redacted)".to_string()
                })?;
            self.healthy = true;
        }
        let result = self.rt.block_on(async {
            tokio::time::timeout(timeout, self.session.client.simple_query(sql)).await
        });
        match result {
            Ok(Ok(rows)) => Ok(rows
                .into_iter()
                .filter_map(|r| match r {
                    tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                        (0..row.len())
                            .map(|i| row.get(i).unwrap_or("").into())
                            .collect(),
                    ),
                    _ => None,
                })
                .collect()),
            _ => {
                // Closing and joining this transport rolls back any open
                // transaction without an unverified cancellation socket or
                // an unbounded ROLLBACK queued behind a timed-out query.
                self.session.driver.0.abort();
                let _ = self.rt.block_on(&mut self.session.driver.0);
                self.healthy = false;
                Err("administrative PostgreSQL query failed (details redacted)".into())
            }
        }
    }
}
fn query(pg: &mut Pg, sql: &str) -> driver::Result<fence::Rows> {
    fence::Connection::query(pg, sql, Duration::from_secs(5)).map_err(component)
}
fn clock(pg: &mut Pg) -> driver::Result<u64> {
    query(
        pg,
        "SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint::text",
    )?
    .first()
    .and_then(|r| r.first())
    .ok_or(driver::Error::Invalid("destination clock unavailable"))?
    .parse()
    .map_err(component)
}
fn ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}
fn literal(value: &str) -> String {
    format!("E'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
}

struct Task {
    runtime: engine::Runtime,
    engine: Arc<engine::Engine>,
    key: driver::Key,
    saved: Option<Saved>,
    deadline_ms: u64,
    definition: MigrationDefinition,
    source_config: tls::Config,
    destination_config: tls::Config,
    tables: Vec<catalog::TableCatalog>,
    journal: coordinator::journal::Journal,
    progress: engine::FinalMarkerProgress,
    monitor: Monitor,
    pause: watch::Sender<bool>,
    joined: watch::Receiver<bool>,
}

#[derive(Clone)]
struct KeyOrder(catalog::RangeClassifier);
impl verify::KeyOrder for KeyOrder {
    fn compare_keys(&self, a: &verify::Key, b: &verify::Key) -> verify::Result<std::cmp::Ordering> {
        self.0
            .compare_keys(a, b)
            .map_err(|_| verify::VerifyError::Invalid("qualified frozen key ordering"))
    }
}
struct Frozen<'a> {
    task: &'a Task,
    rt: Arc<tokio::runtime::Runtime>,
}
impl driver::Verifier for Frozen<'_> {
    fn boundary(&mut self, marker: &driver::Marker) -> driver::Result<driver::VerifiedBoundary> {
        self.rt.block_on(async {
            let state = self.task.runtime.state().await;
            let mut total = 0u64;
            for table in &self.task.tables {
                let reference = state
                    .chunk_plans
                    .get(&table.key.table)
                    .ok_or(driver::Error::Verification)?;
                let plan = self
                    .task
                    .journal
                    .chunk_plan(reference)
                    .await
                    .map_err(component)?;
                let order = KeyOrder(
                    table
                        .range_classifier(
                            &plan,
                            &format!("skedaddle-postgres:{}", self.task.key.migration),
                        )
                        .map_err(component)?,
                );
                let columns = table
                    .columns
                    .iter()
                    .map(|c| sql::Column {
                        name: c.name.clone(),
                        pg_type: c.pg_type.clone(),
                    })
                    .collect::<Vec<_>>();
                let cap = sql::RangeBudget {
                    max_rows: 100_000,
                    max_bytes: self.task.definition.budgets.max_snapshot_bytes,
                };
                let mut source = sql::PgRowSource::connect(
                    &self.task.source_config,
                    sql::Side::Source,
                    table.key.clone(),
                    columns.clone(),
                    order.clone(),
                    cap,
                )
                .await
                .map_err(component)?;
                let mut destination = sql::PgRowSource::connect(
                    &self.task.destination_config,
                    sql::Side::Destination,
                    table.key.clone(),
                    columns,
                    order.clone(),
                    cap,
                )
                .await
                .map_err(component)?;
                let projection =
                    verify::Projection::new(table.selection.columns.clone()).map_err(component)?;
                for range in &plan.ranges {
                    let left = source.fetch(range, &projection).await.map_err(component)?;
                    let right = destination
                        .fetch(range, &projection)
                        .await
                        .map_err(component)?;
                    let compared = verify::compare_rows(&order, range, &projection, &left, &right)
                        .map_err(component)?;
                    if !compared.differences.is_empty() {
                        return Err(driver::Error::Verification);
                    }
                    total = total
                        .checked_add(left.len() as u64)
                        .ok_or(driver::Error::Verification)?;
                }
            }
            Ok(driver::VerifiedBoundary {
                marker: marker.clone(),
                coverage: driver::Coverage {
                    total,
                    verified: total,
                    busy: 0,
                    mismatched: 0,
                    excluded: 0,
                },
            })
        })
    }
}

struct Status<'a> {
    task: &'a Task,
    clock: Pg,
    source: Pg,
    drain: Arc<AtomicU64>,
    deadline: u64,
    last_now: u64,
    abort_reason: Arc<AtomicU64>,
}
impl driver::Status for Status<'_> {
    fn readiness(&mut self, _: &driver::Key) -> driver::Result<()> {
        let facts = self
            .clock
            .rt
            .block_on(self.task.engine.evaluate_readiness())
            .map_err(component)?;
        if matches!(readiness::evaluate(&facts), readiness::Readiness::Ready) {
            Ok(())
        } else {
            Err(driver::Error::Invalid(
                "fresh readiness changed before admission",
            ))
        }
    }
    fn breached(&mut self, _: &driver::Key) -> driver::Result<bool> {
        if self.now_ms()? >= self.deadline {
            self.abort_reason.store(1, Ordering::SeqCst);
            return Ok(true);
        }
        // A fresh transactional heartbeat closes the capture gap before testing
        // writes. Using a normal watermark preserves the existing strict codec.
        let token = uuid::Uuid::new_v4();
        let wm = chunk::Watermark {
            wm: chunk::WatermarkKind::High,
            chunk: chunk::ChunkId {
                table: format!("skedaddle:verification:{token}"),
                plan: "0".repeat(64),
                ordinal: 0,
            },
            attempt: 0,
        };
        let payload = serde_json::to_string(&wm).map_err(component)?;
        let rows = query(
            &mut self.source,
            &format!(
                "SELECT pg_logical_emit_message(true,{},{})::text",
                literal(chunk::MESSAGE_PREFIX),
                literal(&payload)
            ),
        )?;
        let tip = fence::parse_lsn(
            rows.first()
                .and_then(|r| r.first())
                .ok_or(driver::Error::Invalid("source heartbeat unavailable"))?,
        )
        .map_err(component)?;
        while self.task.monitor.captured.load(Ordering::SeqCst) < tip {
            if self.now_ms()? >= self.deadline {
                self.abort_reason.store(1, Ordering::SeqCst);
                return Ok(true);
            }
            if !self.task.runtime.live_readiness().running {
                return Err(driver::Error::Pending);
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        // Recheck after waiting: a long capture wait may cross the deadline.
        if self.now_ms()? >= self.deadline {
            self.abort_reason.store(1, Ordering::SeqCst);
            return Ok(true);
        }
        let drain = self.drain.load(Ordering::SeqCst);
        let breached = drain != 0 && self.task.monitor.written.load(Ordering::SeqCst) > drain;
        if breached {
            self.abort_reason.store(2, Ordering::SeqCst);
        }
        Ok(breached)
    }
    fn now_ms(&mut self) -> driver::Result<u64> {
        let now = clock(&mut self.clock)?;
        if now < self.last_now {
            return Err(driver::Error::Invalid("destination clock reversed"));
        }
        self.last_now = now;
        Ok(now)
    }
    fn audit(&mut self, _: &driver::Key, _: &driver::Audit) -> driver::Result<()> {
        Ok(())
    }
}

// RefCell sharing reflects the driver's sequential component calls. Reverse
// setup attaches the real journal capability to the same destination object.
struct Target<'a> {
    pg: &'a std::cell::RefCell<driver::pg::Target<Pg>>,
    task: &'a Task,
    rt: Arc<tokio::runtime::Runtime>,
}
impl driver::Destination for Target<'_> {
    fn activated(&mut self, k: &driver::Key) -> driver::Result<Option<u64>> {
        self.pg.borrow_mut().activated(k)
    }
    fn prove_fenced(&mut self, k: &driver::Key, j: &mut driver::Journal<'_>) -> driver::Result<()> {
        self.pg.borrow_mut().prove_fenced(k, j)
    }
    fn readiness(&mut self, k: &driver::Key, j: &mut driver::Journal<'_>) -> driver::Result<()> {
        self.pg.borrow_mut().readiness(k, j)
    }
    fn activate(
        &mut self,
        k: &driver::Key,
        m: &driver::Marker,
        j: &mut driver::Journal<'_>,
        a: &mut dyn FnMut() -> driver::Result<()>,
    ) -> driver::Result<u64> {
        self.pg.borrow_mut().activate(k, m, j, a)
    }
    fn post_activate(
        &mut self,
        k: &driver::Key,
        j: &mut driver::Journal<'_>,
    ) -> driver::Result<()> {
        self.pg.borrow_mut().post_activate(k, j)?;
        run_hooks(
            self.task,
            &self.rt,
            crate::definition::HookPhase::PostActivate,
        )
    }
}
fn run_hooks(
    task: &Task,
    rt: &tokio::runtime::Runtime,
    phase: crate::definition::HookPhase,
) -> driver::Result<()> {
    for (index, hook) in task
        .definition
        .hooks
        .iter()
        .enumerate()
        .filter(|(_, h)| h.phase == phase)
    {
        let stream = format!("runner-cutover-hook-{phase:?}-{index}");
        let state = rt.block_on(task.runtime.state());
        if state
            .progress
            .get(&stream)
            .is_some_and(|(value, id)| value == "succeeded" && id == &task.key.cutover)
        {
            continue;
        }
        let hook = crate::hooks::OperatorHook::try_from(hook).map_err(component)?;
        rt.block_on(
            task.runtime
                .marker(&stream, "intent".into(), task.key.cutover.clone()),
        )
        .map_err(component)?;
        let result =
            crate::hooks::run_phase_then(hook.phase, &[hook], &Redactor::default(), |_| {}, || ());
        rt.block_on(
            task.runtime.marker(
                &stream,
                if result.is_ok() {
                    "succeeded"
                } else {
                    "failed"
                }
                .into(),
                task.key.cutover.clone(),
            ),
        )
        .map_err(component)?;
        result.map_err(component)?;
    }
    Ok(())
}
struct Source<'a> {
    task: &'a Task,
    pg: driver::pg::Source<Pg, Box<dyn FnMut() -> Result<(), String> + Send + 'a>>,
    drain: Arc<AtomicU64>,
}
impl driver::SourceFence for Source<'_> {
    fn fence(
        &mut self,
        k: &driver::Key,
        j: &mut driver::Journal<'_>,
    ) -> driver::Result<driver::DrainCounts> {
        let result = self.pg.fence(k, j)?;
        self.drain.store(
            self.pg
                .state
                .drain_lsn
                .ok_or(driver::Error::Invalid("source drain missing"))?,
            Ordering::SeqCst,
        );
        Ok(result)
    }
    fn final_marker(&mut self, k: &driver::Key) -> driver::Result<driver::Marker> {
        let tables = self
            .pg
            .connection
            .rt
            .block_on(self.task.runtime.scoped_schema_tables())
            .map_err(component)?;
        self.pg
            .connection
            .rt
            .block_on(self.task.runtime.poll_schema(
                &self.pg.connection.session.client,
                &tables,
                crate::pg::ddl::PollPoint::FinalMarker(self.drain.load(Ordering::SeqCst)),
            ))
            .map_err(component)?;
        let marker = self.pg.final_marker(k)?;
        self.pg
            .connection
            .rt
            .block_on(self.task.runtime.poll_schema(
                &self.pg.connection.session.client,
                &tables,
                crate::pg::ddl::PollPoint::FinalMarker(marker.lsn),
            ))
            .map_err(component)?;
        Ok(marker)
    }
    fn unfence(&mut self, k: &driver::Key, j: &mut driver::Journal<'_>) -> driver::Result<()> {
        self.pg.unfence(k, j)
    }
}
struct Reverse<'a, 'b> {
    task: &'a Task,
    held: &'a engine::HeldCutover<'b>,
    target: &'a std::cell::RefCell<driver::pg::Target<Pg>>,
    destination: Pg,
    old_source: Pg,
    config: reverse::Config,
}
impl driver::Reverse for Reverse<'_, '_> {
    fn confirm(&mut self, _: &driver::Key, _: &driver::Marker) -> driver::Result<()> {
        let mut target = self.target.borrow_mut();
        let proof = self.task.runtime.setup_reverse(
            self.held,
            &self.config,
            &mut self.destination,
            &mut self.old_source,
            &mut target.state,
        )?;
        // Setup installs the reverse objects first; qualify and retain the exact
        // catalog pair while both application endpoints remain fenced.
        let rt = self.destination.rt.clone();
        rt.block_on(async {
            let objects = selection::catalog_objects(&self.task.definition.selected_objects);
            let source = preflight::collect(&mut self.old_source.session.client, &objects)
                .await
                .map_err(component)?;
            let destination = preflight::collect(&mut self.destination.session.client, &objects)
                .await
                .map_err(component)?;
            let selections: Vec<_> = self
                .task
                .tables
                .iter()
                .map(|t| t.selection.clone())
                .collect();
            catalog::build(&source, &destination, &selections, 0, true).map_err(component)?;
            super::activation_baseline::Baseline::new(&self.task.journal, source, destination)
                .persist(&self.task.journal, &self.task.runtime)
                .await
                .map_err(component)
        })?;
        target.state.attach_reverse_proof(proof);
        Ok(())
    }
}

impl Task {
    fn run(mut self) -> coordinator::Result<CutoverOutcome> {
        let rt = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()?,
        );
        let mut authority = self.runtime.cutover_authority();
        let key = self.key.clone();
        let result = authority.run_with(&key, |held| self.drive(rt.clone(), held));
        match result {
            Ok(outcome) => Ok(outcome),
            Err(error) => {
                let mut outcome = failed(match error {
                    driver::Error::LostOwnership => CutoverErrorKind::LostOwnership,
                    driver::Error::UseRollback => CutoverErrorKind::ActivationUncertain,
                    _ => CutoverErrorKind::Driver,
                });
                if matches!(error, driver::Error::LegacyActivationReceipt)
                    && let CutoverOutcome::Failed {
                        error: detail,
                        next_step,
                        ..
                    } = &mut outcome
                {
                    detail.message = error.to_string();
                    *next_step = "Operator action required: inspect the legacy destination activation receipt; automatic takeover recovery is disabled.".into();
                }
                if let CutoverOutcome::Failed {
                    cutover_id,
                    step_timings_ms,
                    write_pause_ms,
                    ..
                } = &mut outcome
                {
                    *cutover_id = Some(self.key.cutover.clone());
                    if let Some(saved) = replay(&rt.block_on(self.runtime.state()))? {
                        *step_timings_ms = saved.timings;
                        *write_pause_ms = saved.state.fence_start_ms.and_then(|start| {
                            saved
                                .state
                                .activated_at_ms
                                .or(saved.aborted_at_ms)
                                .and_then(|end| end.checked_sub(start))
                        });
                    }
                }
                Ok(outcome)
            }
        }
    }
    fn drive(
        &mut self,
        rt: Arc<tokio::runtime::Runtime>,
        held: &engine::HeldCutover<'_>,
    ) -> driver::Result<CutoverOutcome> {
        // Repeat admission after acquiring the cross-process exclusion and
        // refreshing ownership, before any administrative database effects.
        if self.saved.is_none()
            && let Some(outcome) = rt.block_on(admission(&self.engine)).map_err(component)?
        {
            return Ok(outcome);
        }
        let open_source = || Pg::open(rt.clone(), &self.source_config, &self.definition.source);
        let open_destination = || {
            Pg::open(
                rt.clone(),
                &self.destination_config,
                &self.definition.destination,
            )
        };
        let mut source_connection = open_source()?;
        let mut destination_connection = open_destination()?;
        let saved = match self.saved.take() {
            Some(mut saved) => {
                saved.state.key = self.key.clone();
                saved.source.config.epoch = self.key.epoch;
                saved.destination.config.epoch = self.key.epoch;
                saved
            }
            None => {
                let now = clock(&mut destination_connection)?;
                let mut source_tables = vec![];
                let mut destination_tables = vec![];
                for table in &self.tables {
                    let name = format!(
                        "{}.{}",
                        ident(&table.selection.schema),
                        ident(&table.selection.name)
                    );
                    for (connection, tables) in [
                        (&mut source_connection, &mut source_tables),
                        (&mut destination_connection, &mut destination_tables),
                    ] {
                        let owner = query(connection, &format!("SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid={}::regclass", literal(&name)))?.first().and_then(|r| r.first()).cloned().ok_or(driver::Error::Invalid("table owner missing"))?;
                        tables.push(fence::Table {
                            schema: table.selection.schema.clone(),
                            name: table.selection.name.clone(),
                            owner,
                        });
                    }
                }
                // Translate only exact, explicitly acknowledged inventory ids.
                let mut risks = rt
                    .block_on(fence::inventory::collect(
                        &source_connection.session.client,
                        &source_tables
                            .iter()
                            .map(|t| format!("{}.{}", ident(&t.schema), ident(&t.name)))
                            .collect::<Vec<_>>(),
                        &self.definition.fence_plan.application_roles,
                    ))
                    .map_err(component)?;
                if !fence::inventory::missing(
                    &risks,
                    &self.definition.fence_plan.acknowledged_residual_risks,
                )
                .is_empty()
                {
                    return Err(driver::Error::Invalid(
                        "source fence inventory changed since readiness",
                    ));
                }
                // The readiness inventory exempts its migration sessions; the
                // native fence deliberately does not. Translate any *explicit*
                // per-table acknowledgments of those principals too. Never
                // manufacture acknowledgment from a credential's privileges.
                let administrators =
                    query(&mut source_connection, "SELECT current_user,session_user")?;
                let mut exempt_rows = vec![];
                for role in administrators.iter().flatten() {
                    for table in &source_tables {
                        for kind in [
                            "superuser",
                            "table-owner",
                            "bypass-rls",
                            "out-of-scope-writer",
                        ] {
                            exempt_rows.push(vec![
                                kind.into(),
                                role.clone(),
                                format!("{}.{}", ident(&table.schema), ident(&table.name)),
                                "migration principal".into(),
                            ]);
                        }
                    }
                }
                risks.extend(
                    fence::inventory::from_rows(exempt_rows)
                        .map_err(component)?
                        .into_iter()
                        .filter(|risk| {
                            self.definition
                                .fence_plan
                                .acknowledged_residual_risks
                                .contains(&risk.id)
                        }),
                );
                let acknowledged = risks
                    .iter()
                    .filter_map(|r| {
                        let kind = match r.kind {
                            fence::inventory::Kind::Superuser => fence::ResidualKind::Superuser,
                            fence::inventory::Kind::TableOwner => fence::ResidualKind::TableOwner,
                            fence::inventory::Kind::BypassRls => fence::ResidualKind::BypassRls,
                            fence::inventory::Kind::OutOfScopeWriter => {
                                fence::ResidualKind::OutOfScopeWriter
                            }
                            _ => return None,
                        };
                        Some(fence::ResidualPrincipal {
                            role: r.principal.clone(),
                            kind,
                        })
                    })
                    .collect();
                Saved {
                    state: driver::State::new(self.key.clone())?,
                    deadline_ms: now
                        .checked_add(self.deadline_ms)
                        .ok_or(driver::Error::Invalid("deadline overflow"))?,
                    source: fence::Fence::new(fence::FenceConfig {
                        epoch: self.key.epoch,
                        applications: self
                            .definition
                            .fence_plan
                            .application_roles
                            .iter()
                            .cloned()
                            .collect(),
                        tables: source_tables,
                        acknowledged,
                        deadline_ms: self.deadline_ms.clamp(1, 30_000),
                    })
                    .map_err(component)?,
                    destination: activate::DestinationFence::new(activate::Config {
                        epoch: self.key.epoch,
                        applications: self
                            .definition
                            .fence_plan
                            .application_roles
                            .iter()
                            .cloned()
                            .collect(),
                        tables: destination_tables,
                        timeout_ms: 5000,
                        post_activate: None,
                    })
                    .map_err(component)?,
                    drained: Default::default(),
                    timings: vec![],
                    aborted_at_ms: None,
                    deadline_abort: None,
                    last_clock_ms: now,
                    outcome: None,
                }
            }
        };
        let saved = std::cell::RefCell::new(saved);
        let mut checkpoint_clock = open_destination()?;
        let persist = |saved: &Saved| -> driver::Result<()> {
            rt.block_on(self.runtime.marker(
                STREAM,
                serde_json::to_string(saved).map_err(component)?,
                self.key.cutover.clone(),
            ))
            .map_err(component)
        };
        persist(&saved.borrow())?;
        let abort_reason = Arc::new(AtomicU64::new(0));
        let mut journal = |record: &driver::Record| -> Result<(), String> {
            let mut saved = saved.borrow_mut();
            match record {
                driver::Record::Checkpoint(state) | driver::Record::Intent { state, .. } => {
                    saved.state = state.clone();
                    if state.next == driver::Step::Abort && saved.deadline_abort.is_none() {
                        saved.deadline_abort = Some(abort_reason.load(Ordering::SeqCst) == 1);
                    }
                    if state.next == driver::Step::Aborted && saved.aborted_at_ms.is_none() {
                        saved.aborted_at_ms = Some(
                            clock(&mut checkpoint_clock)
                                .map_err(|_| "destination clock unavailable")?,
                        );
                    }
                }
                driver::Record::Source(
                    fence::Intent::Step(state)
                    | fence::Intent::Save(state)
                    | fence::Intent::Unfence(state),
                ) => saved.source = *state.clone(),
                driver::Record::Destination(intent) => saved.destination = intent.state.clone(),
                driver::Record::Drain(counts) => saved.drained = counts.clone(),
            }
            persist(&saved).map_err(|_| "cutover journal append failed".into())
        };
        let mut state = saved.borrow().state.clone();
        let drain = Arc::new(AtomicU64::new(saved.borrow().source.drain_lsn.unwrap_or(0)));
        let hook_rt = rt.clone();
        let mut source = Source {
            task: self,
            pg: driver::pg::Source {
                connection: source_connection,
                state: saved.borrow().source.clone(),
                drained: saved.borrow().drained.clone(),
                pre_fence: Box::new(|| {
                    run_hooks(self, &hook_rt, crate::definition::HookPhase::PreFence)
                        .map_err(|_| "pre-fence hook failed (details redacted)".into())
                }),
            },
            drain: drain.clone(),
        };
        let target = std::cell::RefCell::new(driver::pg::Target {
            connection: destination_connection,
            state: saved.borrow().destination.clone(),
            acknowledged_triggers: BTreeSet::new(),
        });
        if saved.borrow().destination.next == activate::Step::Save {
            target.borrow_mut().initialize()?;
        }
        // The atomic receipt is authoritative before reproof or deadline logic.
        let initial_receipt = target.borrow_mut().activated(&state.key)?;
        if initial_receipt.is_none() && state.next == driver::Step::Activate {
            let mut target = target.borrow_mut();
            retain_fenced_for_retry(&mut target.state)?;
            let driver::pg::Target {
                connection, state, ..
            } = &mut *target;
            state
                .observe(connection, &mut |intent| {
                    journal(&driver::Record::Destination(intent.clone()))
                })
                .map_err(component)?;
        }
        while matches!(
            target.borrow().state.next,
            activate::Step::Save
                | activate::Step::Fence
                | activate::Step::Drain
                | activate::Step::VerifyFence
        ) {
            let mut target = target.borrow_mut();
            let driver::pg::Target {
                connection, state, ..
            } = &mut *target;
            state
                .execute_next(connection, &mut |i| {
                    journal(&driver::Record::Destination(i.clone()))
                })
                .map_err(component)?;
            journal(&driver::Record::Destination(activate::Intent {
                state: state.clone(),
            }))
            .map_err(driver::Error::Journal)?;
        }
        let version: u32 = query(
            &mut target.borrow_mut().connection,
            "SHOW server_version_num",
        )?
        .first()
        .and_then(|r| r.first())
        .ok_or(driver::Error::Invalid("destination version missing"))?
        .parse()
        .map_err(component)?;
        let config = reverse::Config::new(
            &self.key.migration,
            version,
            self.tables
                .iter()
                .map(|t| (t.selection.schema.clone(), t.selection.name.clone()))
                .collect(),
        )
        .map_err(component)?;
        let mut reverse = Reverse {
            task: self,
            held,
            target: &target,
            destination: open_destination()?,
            old_source: open_source()?,
            config,
        };
        let mut status = Status {
            task: self,
            clock: open_destination()?,
            source: open_source()?,
            drain,
            deadline: saved.borrow().deadline_ms,
            last_now: saved.borrow().last_clock_ms,
            abort_reason: abort_reason.clone(),
        };
        let mut verifier = Frozen {
            task: self,
            rt: rt.clone(),
        };
        let mut sequences = driver::pg::SequenceSync {
            source: open_source()?,
            destination: open_destination()?,
            scope: activate::SequenceScope {
                tables: source.pg.state.config.tables.clone(),
                sequences: vec![],
            },
            timeout_ms: 5000,
        };
        let mut progress = self.progress.clone();
        let mut destination = Target {
            pg: &target,
            task: self,
            rt: rt.clone(),
        };
        let mut nested = held;
        if initial_receipt.is_some()
            && matches!(state.next, driver::Step::PostActivate | driver::Step::Audit)
        {
            destination.activate(
                &state.key,
                state
                    .marker
                    .as_ref()
                    .ok_or(driver::Error::Invalid("marker missing"))?,
                &mut journal,
                &mut || Err(driver::Error::UseRollback),
            )?;
        }
        loop {
            if matches!(state.next, driver::Step::Complete | driver::Step::Aborted) {
                break;
            }
            let activated = destination.activated(&state.key)?;
            if activated.is_none() && status.now_ms()? >= saved.borrow().deadline_ms {
                abort_reason.store(1, Ordering::SeqCst);
                let start = status.now_ms()?;
                if rt.block_on(self.runtime.state()).lifecycle.state == Lifecycle::VerifiedReady {
                    advance(&self.runtime, &rt, Lifecycle::SourceFenced)?;
                }
                state.abort(&mut nested, &mut source, &mut destination, &mut journal)?;
                let end = status.now_ms()?;
                let mut saved = saved.borrow_mut();
                saved.last_clock_ms = end;
                saved.aborted_at_ms.get_or_insert(end);
                add_timing(&mut saved.timings, driver::Step::Abort, start, end)?;
                persist(&saved)?;
                break;
            }
            // Restore legal persisted lifecycle before native reverse setup.
            if matches!(
                state.next,
                driver::Step::Fence | driver::Step::FinalMarker | driver::Step::Boundary
            ) {
                let lifecycle = rt.block_on(self.runtime.state()).lifecycle.state;
                if lifecycle == Lifecycle::VerifiedReady {
                    advance(&self.runtime, &rt, Lifecycle::SourceFenced)?;
                }
            }
            if matches!(
                state.next,
                driver::Step::Sequences
                    | driver::Step::DestinationReadiness
                    | driver::Step::Reverse
                    | driver::Step::Activate
            ) && rt.block_on(self.runtime.state()).lifecycle.state == Lifecycle::SourceFenced
            {
                advance(&self.runtime, &rt, Lifecycle::FinalBoundaryApplied)?;
            }
            if state.next == driver::Step::Activate && activated.is_none() {
                {
                    let mut target = target.borrow_mut();
                    retain_fenced_for_retry(&mut target.state)?;
                    let driver::pg::Target {
                        connection, state, ..
                    } = &mut *target;
                    state
                        .observe(connection, &mut |intent| {
                            journal(&driver::Record::Destination(intent.clone()))
                        })
                        .map_err(component)?;
                }
                reverse.confirm(
                    &state.key,
                    state
                        .marker
                        .as_ref()
                        .ok_or(driver::Error::Invalid("marker missing"))?,
                )?;
            }
            let step = state.next;
            let start = status.now_ms()?;
            let result = state.execute_next(
                &mut nested,
                &mut driver::Components {
                    source: &mut source,
                    destination: &mut destination,
                    progress: &mut progress,
                    verifier: &mut verifier,
                    sequences: &mut sequences,
                    reverse: &mut reverse,
                    status: &mut status,
                },
                &mut journal,
            );
            let end = status.now_ms()?;
            {
                let mut saved = saved.borrow_mut();
                saved.last_clock_ms = end;
                add_timing(&mut saved.timings, step, start, end)?;
                persist(&saved)?;
            }
            match result {
                Ok(()) => {}
                Err(driver::Error::Pending) => std::thread::sleep(Duration::from_millis(10)),
                Err(driver::Error::BreachAborted) => {
                    saved.borrow_mut().aborted_at_ms.get_or_insert(end);
                    persist(&saved.borrow())?;
                    break;
                }
                Err(error) => {
                    // Even an interrupted native step must deadline-abort on
                    // this same owner, after resolving the atomic activation receipt.
                    if end >= saved.borrow().deadline_ms
                        && destination.activated(&state.key)?.is_none()
                    {
                        continue;
                    }
                    return Err(error);
                }
            }
        }
        // Join forward workers before publishing a terminal authority decision.
        self.pause.send_replace(true);
        rt.block_on(async {
            let mut joined = self.joined.clone();
            while !*joined.borrow_and_update() {
                joined.changed().await.map_err(component)?;
            }
            Ok::<(), driver::Error>(())
        })?;
        let mut saved = saved.borrow_mut();
        let outcome = saved_outcome(&saved)?;
        saved.outcome = Some(outcome.clone());
        persist(&saved)?;
        let lifecycle = rt.block_on(self.runtime.state()).lifecycle.state;
        if state.next == driver::Step::Aborted {
            if lifecycle == Lifecycle::VerifiedReady {
                advance(&self.runtime, &rt, Lifecycle::SourceFenced)?;
            }
            if matches!(
                lifecycle,
                Lifecycle::VerifiedReady
                    | Lifecycle::SourceFenced
                    | Lifecycle::FinalBoundaryApplied
            ) {
                advance(&self.runtime, &rt, Lifecycle::AbortedToSource)?;
            }
        } else if lifecycle == Lifecycle::FinalBoundaryApplied {
            let receipt = target
                .borrow_mut()
                .activation_receipt(&state.key)?
                .ok_or(driver::Error::Invalid("activation receipt missing"))?;
            let current = rt.block_on(self.runtime.state());
            if receipt.fence_epoch != current.epoch || receipt.fence_owner != current.owner {
                match current
                    .verification
                    .reverse
                    .as_ref()
                    .and_then(|r| r.recovered.as_ref())
                {
                    Some(recorded) if recorded == &receipt => {}
                    Some(_) => return Err(driver::Error::ActivationReceiptMismatch),
                    None => rt
                        .block_on(self.runtime.commit(Transition::RecoveredActivation {
                            receipt: Box::new(receipt),
                        }))
                        .map_err(component)?,
                }
            }
            advance(&self.runtime, &rt, Lifecycle::DestinationActive)?;
        }
        Ok(outcome)
    }
}
// Receipt absence was established under exclusion immediately before this.
// Retain the original saved grants and observations, then freshly prove Fenced.
fn retain_fenced_for_retry(state: &mut activate::DestinationFence) -> driver::Result<()> {
    if !matches!(
        state.next,
        activate::Step::Fenced | activate::Step::CheckActivation | activate::Step::Restore
    ) || state.saved.is_none()
        || state.evidence.is_empty()
    {
        return Err(driver::Error::UseRollback);
    }
    state.next = activate::Step::Fenced;
    state.final_boundary = None;
    state.clear_reverse_proof();
    Ok(())
}
fn saved_outcome(saved: &Saved) -> driver::Result<CutoverOutcome> {
    if let Some(outcome) = &saved.outcome {
        return Ok(outcome.clone());
    }
    let cutover_id = saved.state.key.cutover.clone();
    let step_timings_ms = saved.timings.clone();
    if saved.state.next == driver::Step::Complete {
        return Ok(CutoverOutcome::Completed {
            cutover_id,
            step_timings_ms,
            write_pause_ms: saved
                .state
                .audit
                .as_ref()
                .ok_or(driver::Error::Invalid("cutover audit missing"))?
                .write_pause_ms,
        });
    }
    if saved.state.next != driver::Step::Aborted {
        return Err(driver::Error::Invalid("cutover not terminal"));
    }
    let write_pause_ms = saved
        .state
        .fence_start_ms
        .and_then(|start| saved.aborted_at_ms.and_then(|end| end.checked_sub(start)));
    if !saved.deadline_abort.unwrap_or_else(|| {
        saved
            .aborted_at_ms
            .is_some_and(|end| end >= saved.deadline_ms)
    }) {
        return Ok(CutoverOutcome::Failed {
            cutover_id: Some(cutover_id),
            step_timings_ms,
            write_pause_ms,
            error: CutoverError {
                kind: CutoverErrorKind::SourceBreach,
                message: "Source changed after drain; cutover aborted and source writes restored."
                    .into(),
            },
            next_step:
                "Review the source writer inventory and fence plan before planning a new migration."
                    .into(),
        });
    }
    Ok(CutoverOutcome::DeadlineAborted { cutover_id, step_timings_ms, write_pause_ms,
        next_step: "Source writes are restored. Inspect the deadline and plan a new migration before retrying.".into() })
}
fn add_timing(
    timings: &mut Vec<StepTiming>,
    step: driver::Step,
    start: u64,
    end: u64,
) -> driver::Result<()> {
    let elapsed = end
        .checked_sub(start)
        .ok_or(driver::Error::Invalid("destination clock reversed"))?;
    if let Some(timing) = timings.iter_mut().find(|timing| timing.step == step) {
        timing.elapsed_ms = timing
            .elapsed_ms
            .checked_add(elapsed)
            .ok_or(driver::Error::Invalid("timing overflow"))?;
    } else {
        timings.push(StepTiming {
            step,
            elapsed_ms: elapsed,
        });
    }
    Ok(())
}
fn advance(
    runtime: &engine::Runtime,
    rt: &tokio::runtime::Runtime,
    to: Lifecycle,
) -> driver::Result<()> {
    rt.block_on(runtime.commit(Transition::Lifecycle {
        event: Event::Advance { to },
    }))
    .map_err(component)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinator::{
        epoch::Coordinator, exclusion::MigrationExclusion, journal::Journal, local::Local,
    };

    async fn service() -> Service {
        let root = PathBuf::from(std::env::var_os("SKEDADDLE_TEST_ROOT").expect("task test root"))
            .join(format!("runner-cutover-unit-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let journal = Journal::new(
            Arc::new(coordinator::journal::local_store(&root).unwrap()),
            "runner",
            "migration",
            Duration::from_secs(5),
        )
        .unwrap();
        let exclusion = MigrationExclusion::file(root.join("exclusion")).unwrap();
        let mut coordinator = Coordinator::observer(
            journal.clone(),
            Local::create(root.join("local")).unwrap(),
            "owner",
        )
        .unwrap()
        .with_exclusion(exclusion.clone());
        coordinator.acquire().await.unwrap();
        let engine = Arc::new(engine::Engine::new_with_exclusion(
            coordinator,
            journal.clone(),
            exclusion,
        ));
        let definition = MigrationDefinition::from_json(
            include_str!("../../README.md")
                .split("```json")
                .nth(1)
                .unwrap()
                .split("```")
                .next()
                .unwrap(),
        )
        .unwrap();
        let config = tls::Config::new(
            "host=127.0.0.1 port=1 user=not-used dbname=postgres"
                .parse()
                .unwrap(),
            "",
        );
        let (pause, _) = watch::channel(false);
        let (_, joined) = watch::channel(false);
        let control = root.join("migration.control");
        std::fs::create_dir_all(&control).unwrap();
        Service {
            monitor: Monitor::new(&engine.runtime().state().await).unwrap(),
            engine,
            root: control,
            id: "migration".into(),
            definition,
            source_config: config.clone(),
            destination_config: config,
            tables: vec![],
            journal,
            source: SourceIdentity {
                engine: "postgres".into(),
                system_id: "1".into(),
                lineage: "1".into(),
                db_name: "db".into(),
                slot: "slot".into(),
            },
            pause,
            joined,
        }
    }
    #[tokio::test]
    async fn non_owner_request_round_trip_is_served_by_owner_without_promotion() {
        let service = service().await;
        let generation = uuid::Uuid::new_v4().to_string();
        let owner = file(&service.root, "owner.lock").unwrap();
        owner.lock().unwrap();
        std::fs::write(service.root.join("owner-generation"), &generation).unwrap();
        let requester = request_cutover(
            service.root.parent().unwrap(),
            &service.id,
            Duration::from_secs(60),
        );
        let responder = async {
            loop {
                for entry in std::fs::read_dir(&service.root).unwrap() {
                    let path = entry.unwrap().path();
                    if path.extension().is_some_and(|s| s == "request") {
                        let request: Request =
                            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
                        assert_eq!(request.generation, generation);
                        let response = service.response(&generation, &request).await;
                        write_json(&path.with_extension("response"), &response).unwrap();
                        return;
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };
        let (response, ()) = tokio::time::timeout(Duration::from_secs(60), async {
            tokio::join!(requester, responder)
        })
        .await
        .unwrap();
        assert!(matches!(response.unwrap(), CutoverOutcome::NotReady { .. }));
        assert_eq!(service.journal.load(true).await.unwrap().epoch, 1);
        drop(owner);
        assert!(matches!(
            request_cutover(
                service.root.parent().unwrap(),
                &service.id,
                Duration::from_secs(60)
            )
            .await
            .unwrap(),
            CutoverOutcome::Failed {
                error: CutoverError {
                    kind: CutoverErrorKind::NoOwner,
                    ..
                },
                ..
            }
        ));
    }
    #[tokio::test]
    async fn fresh_engine_refusal_carries_all_reasons_and_no_cutover_effect() {
        let service = service().await;
        let request = Request {
            generation: "current".into(),
            request: uuid::Uuid::new_v4().to_string(),
            deadline_ms: 60_000,
        };
        let outcome = service.response("current", &request).await;
        let CutoverOutcome::NotReady {
            reasons,
            next_step,
            cutover_id,
            ..
        } = outcome
        else {
            panic!("expected real Engine readiness refusal")
        };
        assert!(cutover_id.is_none());
        assert!(
            reasons
                .iter()
                .any(|r| r.code == readiness::BlockerCode::ChunksIncomplete)
        );
        assert!(
            reasons
                .iter()
                .any(|r| r.code == readiness::BlockerCode::CaptureUnhealthy)
        );
        assert!(
            reasons
                .iter()
                .all(|r| !r.message.is_empty() && !r.next_step.is_empty())
        );
        assert!(!next_step.is_empty());
        assert!(
            replay(&service.journal.load(true).await.unwrap())
                .unwrap()
                .is_none()
        );
    }
    #[tokio::test]
    async fn stale_generation_is_rejected_before_readiness_or_database_access() {
        let service = service().await;
        let request = Request {
            generation: "old".into(),
            request: uuid::Uuid::new_v4().to_string(),
            deadline_ms: 60_000,
        };
        assert!(matches!(
            service.response("current", &request).await,
            CutoverOutcome::Failed {
                error: CutoverError {
                    kind: CutoverErrorKind::StaleGeneration,
                    ..
                },
                ..
            }
        ));
        assert!(
            replay(&service.journal.load(true).await.unwrap())
                .unwrap()
                .is_none()
        );
    }
    #[test]
    fn repeated_pending_timings_are_bounded_and_clock_reversal_is_rejected() {
        let mut timings = vec![];
        for _ in 0..1000 {
            add_timing(&mut timings, driver::Step::Boundary, 100, 105).unwrap();
        }
        assert_eq!(
            timings,
            vec![StepTiming {
                step: driver::Step::Boundary,
                elapsed_ms: 5000
            }]
        );
        assert!(add_timing(&mut timings, driver::Step::Fence, 100, 99).is_err());
    }
    fn admin_transaction(origin: Option<&str>, data: bool) -> AssembledTxn {
        use crate::event::{Event, OpaqueValue, Position, Row, SourceOrigin, TxnId};
        let source = SourceIdentity {
            engine: "postgres".into(),
            system_id: "1".into(),
            lineage: "1".into(),
            db_name: "db".into(),
            slot: "slot".into(),
        };
        let txn = TxnId(
            [
                42_u32.to_be_bytes().as_slice(),
                99_u64.to_be_bytes().as_slice(),
            ]
            .concat(),
        );
        let position = Position::new("pg/source", 99_u64.to_be_bytes());
        let record = crate::pg::ddl::DdlRecord {
            format: 1,
            installation: "test".into(),
            origin: origin.map(str::to_owned),
            server_major: 18,
            version: 1,
            tag: "REVOKE".into(),
            xid: "42".into(),
            query: "REVOKE INSERT ON public.items FROM app".into(),
            commands: vec![],
        };
        let mut event = Event {
            source: source.clone(),
            txn: txn.clone(),
            ordinal_in_txn: 0,
            position: position.clone(),
            schema_version: 1,
            kind: EventKind::DdlBoundary,
            table: None,
            key: Row::new(),
            before: None,
            after: Some(Row::from([(
                "control".into(),
                OpaqueValue {
                    type_id: "skedaddle.ddl.v1".into(),
                    bytes: Some(crate::pg::ddl::encode(&record).unwrap()),
                },
            )])),
            unchanged_cols: BTreeSet::new(),
        };
        if data {
            event.kind = EventKind::Insert;
            event.table = Some("public.items".into());
        }
        let source_origin = Some(SourceOrigin {
            name: "skedaddle_test".into(),
            lsn: 90,
        });
        AssembledTxn::new_with_origin(
            source,
            txn,
            position,
            Some(1_000_000_000_000_000),
            source_origin,
            vec![event],
        )
        .unwrap()
    }
    #[test]
    fn breach_monitor_accepts_only_validated_internal_ddl_and_exact_reverse_origin() {
        assert!(!application_write(
            &admin_transaction(Some("test"), false),
            "test",
            "skedaddle_test_rev"
        ));
        assert!(application_write(
            &admin_transaction(None, false),
            "test",
            "skedaddle_test_rev"
        ));
        assert!(application_write(
            &admin_transaction(Some("foreign"), false),
            "test",
            "skedaddle_test_rev"
        ));
        let data = admin_transaction(Some("test"), true);
        assert!(
            application_write(&data, "test", "skedaddle_test_rev"),
            "forward-origin data remains a breach"
        );
        let mut mixed = admin_transaction(Some("test"), false);
        let mut event = data.events[0].clone();
        event.ordinal_in_txn = 1;
        mixed.events.push(event);
        let mixed = AssembledTxn::new_with_origin(
            mixed.source,
            mixed.txn,
            mixed.commit_position,
            mixed.commit_time,
            mixed.source_origin,
            mixed.events,
        )
        .unwrap();
        assert!(application_write(&mixed, "test", "skedaddle_test_rev"));
        let reverse = AssembledTxn::new_with_origin(
            data.source,
            data.txn,
            data.commit_position,
            data.commit_time,
            Some(crate::event::SourceOrigin {
                name: "skedaddle_test_rev".into(),
                lsn: 90,
            }),
            data.events,
        )
        .unwrap();
        assert!(!application_write(&reverse, "test", "skedaddle_test_rev"));
    }
}
