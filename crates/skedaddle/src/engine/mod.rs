//! Copy/catch-up orchestration. Database effects use adapters; the journal and
//! buffer remain the existing authoritative implementations. Reconstruct an
//! interrupted run from its saved markers; never rewind the apply reader.

mod control;
mod cutover_authority;
mod final_marker;
mod live_readiness;
mod pg;
mod readiness;
mod retention;
mod verification;
pub use control::PauseControl;
pub use cutover_authority::{CutoverAuthority, HeldCutover};
pub use final_marker::{
    CapturedControl, FinalMarkerProgress, FinalMarkerReceipt, MarkerProgressError,
    classify_captured_control, decode_marker_payload,
};
pub use live_readiness::{
    ApplyLagObservation, LagMeasure, LiveReadinessObservation, ReadinessBlocked, ReadinessProbe,
    ReadinessProbeSample, RetentionMargin,
};
pub use pg::*;
pub use readiness::*;
pub use retention::{EnginePauseReason, RetentionStatus};
pub use verification::{
    AppliedSourceWaiter, NativeChangeEvidence, NativeVerificationMarker, PgChangeEvidence,
    VerificationEmitter, VerificationKey, VerificationLimits,
};

use std::{collections::VecDeque, future::Future, sync::Arc, time::Duration};
use tokio::sync::{Mutex, mpsc, oneshot, watch};

use crate::{
    budget::{self, ThroughputBudget},
    buffer::{format::SegmentRecord, writer::DurableManifestRef},
    coordinator::{
        self, EffectKind, Event, Materialized, State, Transition,
        chunk::{ChunkPlan, ChunkRowsRef},
        epoch::Coordinator,
        journal::Journal,
    },
    event::{AssembledTxn, Position, PositionOrder, Row, SourceIdentity},
    pg::{
        chunk::{ChunkRange, Watermark, WatermarkKind, Window},
        source,
    },
};

pub type Result<T> = coordinator::Result<T>;
fn recovery(reason: impl std::fmt::Display) -> coordinator::Error {
    coordinator::Error::RecoveryRequired(reason.to_string())
}

/// Typed rejection of an obsolete or rebound authoritative buffered position.
#[derive(Debug, PartialEq, Eq)]
pub enum BufferedProgressError {
    MissingVersion,
    UnsupportedVersion(u64),
    InvalidPayload,
    SourceMismatch,
    OrderDomainMismatch,
}
impl std::fmt::Display for BufferedProgressError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "buffered progress: {self:?}; recovery required")
    }
}
impl std::error::Error for BufferedProgressError {}
impl From<BufferedProgressError> for coordinator::Error {
    fn from(error: BufferedProgressError) -> Self {
        recovery(error)
    }
}
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct BufferedProgress {
    version: u64,
    source: [String; 5],
    position: crate::event::PositionWireV1,
}
fn progress_source(source: &SourceIdentity) -> [String; 5] {
    [
        source.engine.clone(),
        source.system_id.clone(),
        source.lineage.clone(),
        source.db_name.clone(),
        source.stream.clone(),
    ]
}
pub fn encode_buffered_position(source: &SourceIdentity, position: &Position) -> Result<String> {
    Ok(serde_json::to_string(&BufferedProgress {
        version: 1,
        source: progress_source(source),
        position: position.into(),
    })?)
}
pub fn decode_buffered_position(
    payload: &str,
    source: &SourceIdentity,
    domain: &str,
) -> std::result::Result<Position, BufferedProgressError> {
    use BufferedProgressError::*;
    let json: serde_json::Value = serde_json::from_str(payload).map_err(|_| InvalidPayload)?;
    let version = json
        .get("version")
        .ok_or(MissingVersion)?
        .as_u64()
        .ok_or(InvalidPayload)?;
    if version != 1 {
        return Err(UnsupportedVersion(version));
    }
    let value: BufferedProgress = serde_json::from_str(payload).map_err(|_| InvalidPayload)?;
    if value.source != progress_source(source) {
        return Err(SourceMismatch);
    }
    let position = Position::try_from(value.position).map_err(|_| InvalidPayload)?;
    if position.order_domain() != domain {
        return Err(OrderDomainMismatch);
    }
    Ok(position)
}

/// Disclosed installation effects, with stable journal payload names.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Installation {
    Publication,
    DdlTrigger,
    Slot,
    ProgressTables,
    ControlSchema,
    SourceStartup,
    DestinationSchema,
}
impl Installation {
    fn name(self) -> &'static str {
        match self {
            Self::Publication => "engine-install-publication-v1",
            Self::DdlTrigger => "engine-install-ddl-trigger-v1",
            Self::Slot => "engine-install-slot-v1",
            Self::ProgressTables => "engine-install-progress-tables-v2",
            Self::ControlSchema => "engine-install-control-schema-v1",
            Self::SourceStartup => "engine-install-source-startup-v1",
            Self::DestinationSchema => "engine-install-destination-schema-v1",
        }
    }
}
/// Invalid disclosed plans are rejected before any installation intent or effect.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InstallationPlanError {
    Empty,
    Duplicate(Installation),
    MissingProgressTables,
}
impl std::fmt::Display for InstallationPlanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid installation plan: {self:?}")
    }
}
impl std::error::Error for InstallationPlanError {}

/// Marker emitted after an installation effect has completed durably.
pub struct StartupMarker {
    pub stream: String,
    pub marker: String,
    pub label: String,
}

/// Each effect must be idempotent and verify an existing installation on restart.
/// The engine journals each intent before calling the database adapter. Defaults
/// preserve the PG installation protocol; other connectors override both hooks.
pub trait Installer {
    fn plan(&self) -> &'static [Installation] {
        &[
            Installation::Publication,
            Installation::DdlTrigger,
            Installation::Slot,
            Installation::ProgressTables,
        ]
    }
    fn markers_after(&self, step: Installation) -> Result<Vec<StartupMarker>> {
        let mut markers = Vec::new();
        if step == Installation::Slot {
            if let Some(marker) = self.schema_baseline() {
                markers.push(StartupMarker {
                    stream: crate::pg::ddl::BASELINE_STREAM.into(),
                    marker,
                    label: "slot-double-sample".into(),
                });
            }
            if let Some(marker) = self.slot_marker()? {
                markers.push(StartupMarker {
                    stream: "source-slot".into(),
                    marker,
                    label: "installed".into(),
                });
            }
        }
        Ok(markers)
    }
    fn schema_baseline(&self) -> Option<String> {
        None
    }
    /// `identity` is a stable, nonsecret description binding names and objects.
    fn identity(&self) -> String;
    fn install(&mut self, step: Installation) -> impl Future<Output = Result<()>>;
    fn slot_marker(&self) -> Result<Option<String>> {
        Ok(None)
    }
}

/// A capture step may return at most one manifest-durable transaction and its
/// committed markers. It must retain in-flight work across cancellation.
#[derive(Default)]
pub struct Captured {
    pub transaction: Option<AssembledTxn>,
    pub watermarks: Vec<CapturedMarker>,
    /// Connector-durable restart/ack boundary, which may follow the commit.
    pub durable_position: Option<Position>,
    pub stop: bool,
}
/// PG logical-message evidence stays PG-specific. Chunk-only connectors cannot
/// manufacture final-marker receipts through the neutral control route.
pub enum CapturedMarker {
    Pg(source::Watermark),
    /// Process-local connector verification evidence, isolated from copy windows.
    Verification {
        marker: NativeVerificationMarker,
        commit: Position,
        transaction: crate::event::TxnId,
        committed_events: Vec<crate::event::Event>,
    },
    Chunk {
        marker: Watermark,
        commit: Position,
        committed_events: Vec<crate::event::Event>,
    },
}
impl From<source::Watermark> for CapturedMarker {
    fn from(marker: source::Watermark) -> Self {
        Self::Pg(marker)
    }
}
/// Process-local schema evidence. `None` from a producer is unknown; an
/// explicit state with neither flag set establishes a known-clear classifier.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchemaState {
    pub blocked_boundary: Option<SchemaBoundary>,
    pub unknown_schema_job: bool,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SchemaBoundary {
    Postgres(crate::pg::ddl::SafeBoundaryStop),
    Connector(ConnectorSchemaStop),
}
/// Connector-native diagnostic evidence, never a persisted event/format.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConnectorSchemaStop {
    pub connector: String,
    pub kind: SchemaStopKind,
    /// Connector-qualified table name, when the classifier knows it.
    pub table: Option<String>,
    /// Original statement or classifier diagnostic.
    pub diagnostic: String,
    pub position: Position,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SchemaStopKind {
    Ddl,
    UnknownDdl,
    TableMapChanged,
}
pub trait Capture {
    fn position_order(&self) -> Arc<dyn PositionOrder + Send + Sync>;
    /// Restore schema state before any worker or marker is admitted. Stateless
    /// adapters may use this default; schema-aware wrappers must forward it.
    fn prepare_schema(
        &mut self,
        _journal: &Journal,
        _runtime: &Runtime,
    ) -> impl Future<Output = Result<()>> {
        std::future::ready(if self.schema_state().is_some() {
            Err(recovery("schema-aware capture must forward prepare_schema"))
        } else {
            Ok(())
        })
    }
    fn schema_state(&self) -> Option<SchemaState> {
        None
    }
    /// Schema-aware adapters must forward the writer-bound END receipt.
    fn buffered_marker(&self, _batch: &Captured) -> Result<String> {
        if self.schema_state().is_some() {
            return Err(recovery(
                "schema-aware capture must forward buffered_marker",
            ));
        }
        Ok("manifest-durable".into())
    }
    fn writer_checkpoint(&self) -> Result<Option<crate::buffer::writer::WriterCheckpoint>> {
        Ok(None)
    }
    fn step(&mut self) -> impl Future<Output = Result<Captured>>;
    fn publish(
        &mut self,
        high: Position,
        rows: AssembledTxn,
    ) -> impl Future<Output = Result<DurableManifestRef>>;
}
pub trait ChunkDatabase {
    fn emit(&mut self, marker: Watermark) -> impl Future<Output = Result<()>>;
    fn read(
        &mut self,
        plan: &ChunkPlan,
        chunk: &ChunkRange,
    ) -> impl Future<Output = Result<Vec<Row>>>;
}
pub trait Applier {
    /// Save the reader checkpoint through Runtime BEFORE receipt collection.
    /// Complete an in-flight step on shutdown; it must never be select-cancelled.
    fn step(&mut self, runtime: &Runtime) -> impl Future<Output = Result<bool>>;
    /// Adapters may stop read-only preparation here. Once destination effects
    /// begin they must drain through checkpoint persistence and receipt retention.
    fn controlled_step(
        &mut self,
        runtime: &Runtime,
        _stop: watch::Receiver<bool>,
    ) -> impl Future<Output = Result<bool>> {
        self.step(runtime)
    }
}

// Process-local only. Neither Materialized nor Transition changes its wire layout.
struct Persistence {
    pending: std::collections::BTreeMap<String, (String, String)>,
    updates: usize,
    last_flush: std::time::Instant,
    readiness: Option<(
        crate::coordinator::readiness::ReadinessFacts,
        std::time::Instant,
    )>,
}
impl Default for Persistence {
    fn default() -> Self {
        Self {
            pending: Default::default(),
            updates: 0,
            last_flush: std::time::Instant::now(),
            readiness: None,
        }
    }
}

struct HealthHub {
    healthy: std::sync::RwLock<bool>,
    cutover: Mutex<()>,
    persistence: std::sync::Mutex<Persistence>,
}
impl std::ops::Deref for HealthHub {
    type Target = std::sync::RwLock<bool>;
    fn deref(&self) -> &Self::Target {
        &self.healthy
    }
}

#[derive(Clone)]
pub struct Runtime(
    Arc<Mutex<Coordinator>>,
    Arc<std::sync::RwLock<Materialized>>,
    Arc<HealthHub>,
    Arc<Mutex<Option<retention::Retention>>>,
    Arc<std::sync::RwLock<Option<EnginePauseReason>>>,
    Arc<std::sync::RwLock<(String, u64, Option<u64>)>>,
    Arc<verification::Hub>,
    Arc<live_readiness::Hub>,
);
impl Runtime {
    /// Publish the complete applied boundary durably before receipt collection.
    pub(crate) async fn record_applied(
        &self,
        txn: &AssembledTxn,
        mseq: u64,
        checkpoint: &crate::buffer::reader::ReaderCheckpoint,
    ) -> Result<()> {
        self.applied_final_marker(txn, mseq, checkpoint).await?;
        self.progress_marker(
            "applied",
            encode_reader_checkpoint(checkpoint)?,
            checkpoint.next_mseq.to_string(),
        )
        .await?;
        self.flush_progress().await?;
        self.7.applied(&txn.commit_position, checkpoint);
        self.queue_applied(txn.commit_position.clone(), checkpoint.clone())
            .await;
        Ok(())
    }

    pub async fn state(&self) -> Materialized {
        self.snapshot()
    }
    fn snapshot(&self) -> Materialized {
        self.1.read().expect("engine snapshot poisoned").clone()
    }
    pub async fn commit(&self, transition: Transition) -> Result<()> {
        let mut coordinator = self.0.lock().await;
        self.flush_progress_locked(&mut coordinator).await?;
        let result = coordinator.commit(transition).await;
        self.observe_journal_error(&coordinator, &result);
        if !coordinator.is_owner() {
            self.6.markers.finish();
        }
        *self.1.write().expect("engine snapshot poisoned") = coordinator.state().clone();
        result?;
        Ok(())
    }
    pub(crate) async fn refresh_owner(&self) -> Result<()> {
        let mut coordinator = self.0.lock().await;
        self.flush_progress_locked(&mut coordinator).await?;
        coordinator.refresh().await?;
        *self.1.write().expect("engine snapshot poisoned") = coordinator.state().clone();
        if !coordinator.is_owner() {
            return Err(coordinator::Error::LostOwnership);
        }
        Ok(())
    }
    async fn progress_marker(&self, stream: &str, marker: String, position: String) -> Result<()> {
        if matches!(stream, "applied" | "buffered") {
            let mut coordinator = self.0.lock().await;
            // The old per-update commit refreshed authority even when progress
            // was diagnostic. Keep that fence check without appending an entry.
            let refreshed = coordinator.refresh().await;
            self.observe_journal_error(&coordinator, &refreshed);
            *self.1.write().expect("engine snapshot poisoned") = coordinator.state().clone();
            refreshed?;
            if !coordinator.is_owner() {
                return Err(coordinator::Error::LostOwnership);
            }
            let due = {
                let mut policy = self.2.persistence.lock().expect("persistence poisoned");
                policy.pending.insert(stream.into(), (marker, position));
                policy.updates += 1;
                policy.updates >= 64 || policy.last_flush.elapsed() >= Duration::from_millis(250)
            };
            if due {
                self.flush_progress_locked(&mut coordinator).await?;
            }
            return Ok(());
        }
        self.commit(Transition::Progress {
            stream: stream.into(),
            marker,
            position,
        })
        .await
    }
    pub async fn marker(&self, stream: &str, marker: String, position: String) -> Result<()> {
        self.commit(Transition::Progress {
            stream: stream.into(),
            marker,
            position,
        })
        .await
    }
    /// Publish a conservative restart boundary before any consumer relies on it.
    pub(crate) async fn flush_progress(&self) -> Result<()> {
        let mut coordinator = self.0.lock().await;
        self.flush_progress_locked(&mut coordinator).await
    }
    async fn flush_progress_locked(&self, coordinator: &mut Coordinator) -> Result<()> {
        let pending = self
            .2
            .persistence
            .lock()
            .expect("persistence poisoned")
            .pending
            .clone();
        for (stream, (marker, position)) in pending {
            let result = coordinator
                .commit(Transition::Progress {
                    stream: stream.clone(),
                    marker,
                    position,
                })
                .await;
            self.observe_journal_error(coordinator, &result);
            *self.1.write().expect("engine snapshot poisoned") = coordinator.state().clone();
            result?;
            if stream == "applied" {
                let checkpoint =
                    decode_reader_checkpoint(&coordinator.state().progress["applied"].0)?;
                if let Some(position) = &checkpoint.last_position {
                    self.6.applied(position, &checkpoint);
                }
            }
            self.2
                .persistence
                .lock()
                .expect("persistence poisoned")
                .pending
                .remove(&stream);
        }
        let mut policy = self.2.persistence.lock().expect("persistence poisoned");
        policy.updates = 0;
        policy.last_flush = std::time::Instant::now();
        Ok(())
    }
    async fn lifecycle(&self, event: Event) -> Result<()> {
        self.commit(Transition::Lifecycle { event }).await
    }
}

/// A boundary at which all borrowed workers must be joined before returning.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StopAt {
    CatchingUp,
    VerifiedReady,
    UntilPaused,
}
impl StopAt {
    fn matches(self, state: State) -> bool {
        match self {
            Self::CatchingUp => matches!(state, State::CatchingUp | State::VerifiedReady),
            Self::VerifiedReady => state == State::VerifiedReady,
            Self::UntilPaused => state == State::Paused,
        }
    }
}
/// The durable lifecycle after every worker has joined. A preserved VerifiedReady
/// is the qualified stop-boundary observation, not a claim that stopped capture
/// is live. `blockers` are freshly sampled AFTER joining and include stopped
/// capture health. A later evaluate_readiness() journals that live demotion.
#[derive(Debug)]
pub struct Reached {
    pub state: State,
    pub blockers: Vec<String>,
    pub target_reached: bool,
}

pub struct Engine {
    runtime: Runtime,
    journal: Journal,
    readiness_source: Option<Arc<dyn ReadinessSource>>,
    running: std::sync::atomic::AtomicBool,
    pause_control: Option<PauseControl>,
}
impl Engine {
    /// Configure promotion and cutover to share this coordinator's exclusion.
    /// Every competing coordinator must use the same backend and namespace.
    pub fn new_with_exclusion(
        coordinator: Coordinator,
        journal: Journal,
        exclusion: Arc<coordinator::MigrationExclusion>,
    ) -> Self {
        Self::new(coordinator.with_exclusion(exclusion), journal)
    }

    /// Supply the SAME authoritative, ChunkRows-bound journal as the coordinator.
    pub fn new(coordinator: Coordinator, journal: Journal) -> Self {
        let state = coordinator.state().clone();
        let authority = (state.owner.clone(), state.epoch, None);
        Self {
            runtime: Runtime(
                Arc::new(Mutex::new(coordinator)),
                Arc::new(std::sync::RwLock::new(state)),
                Arc::new(HealthHub {
                    healthy: std::sync::RwLock::new(false),
                    cutover: Mutex::new(()),
                    persistence: std::sync::Mutex::new(Persistence::default()),
                }),
                Arc::new(Mutex::new(None)),
                Arc::new(std::sync::RwLock::new(None)),
                Arc::new(std::sync::RwLock::new(authority)),
                Arc::new(verification::Hub::default()),
                Arc::new(live_readiness::Hub::default()),
            ),
            journal,
            readiness_source: None,
            running: std::sync::atomic::AtomicBool::new(false),
            pause_control: None,
        }
    }
    /// Share this state-root control with a separate CLI pause invocation.
    pub fn with_pause_control(mut self, control: PauseControl) -> Self {
        self.pause_control = Some(control);
        self
    }
    pub fn runtime(&self) -> Runtime {
        self.runtime.clone()
    }
    // A small returned handle prevents installer state from inflating callers.
    pub fn start(
        &self,
        installer: &mut impl Installer,
    ) -> impl std::future::Future<Output = Result<()>> {
        Box::pin(async move {
            self.require_coverage()?;
            if self.runtime.state().await.lifecycle.state != State::Assessed {
                return Err(coordinator::Error::Rejected(
                    "engine start requires assessed".into(),
                ));
            }
            let plan = installer.plan();
            if plan.is_empty() {
                return Err(coordinator::Error::InstallationPlan(
                    InstallationPlanError::Empty,
                ));
            }
            for (index, step) in plan.iter().enumerate() {
                if plan[..index].contains(step) {
                    return Err(coordinator::Error::InstallationPlan(
                        InstallationPlanError::Duplicate(*step),
                    ));
                }
            }
            if !plan.contains(&Installation::ProgressTables) {
                return Err(coordinator::Error::InstallationPlan(
                    InstallationPlanError::MissingProgressTables,
                ));
            }
            for &step in plan {
                let payload = format!("{}:{}", step.name(), installer.identity());
                let mut coordinator = self.runtime.0.lock().await;
                let outcome = coordinator
                    .effect(EffectKind::BufferPublication, payload, |_| {
                        installer.install(step)
                    })
                    .await;
                *self.runtime.1.write().expect("engine snapshot poisoned") =
                    coordinator.state().clone();
                drop(coordinator);
                outcome?;
                for marker in installer.markers_after(step)? {
                    self.runtime
                        .marker(&marker.stream, marker.marker, marker.label)
                        .await?;
                }
            }
            self.runtime
                .lifecycle(Event::Advance { to: State::Copying })
                .await
        })
    }
    pub async fn resume(&self) -> Result<()> {
        self.require_coverage()?;
        self.resume_after_journal_pause().await
    }
    /// Install a qualified plan once. Resumption reads its immutable saved object.
    pub async fn plan(&self, plan: &ChunkPlan) -> Result<()> {
        self.require_coverage()?;
        let mut coordinator = self.runtime.0.lock().await;
        if fenced(coordinator.state().lifecycle.state) {
            return Err(coordinator::Error::Rejected(
                "cannot plan copy work while source is fenced".into(),
            ));
        }
        let reference = self.journal.publish_chunk_plan(plan).await?;
        if coordinator.state().chunk_plans.get(&reference.table) == Some(&reference) {
            return Ok(());
        }
        let result = coordinator
            .commit(Transition::ChunkPlanned { plan: reference })
            .await;
        self.runtime.observe_journal_error(&coordinator, &result);
        *self.runtime.1.write().expect("engine snapshot poisoned") = coordinator.state().clone();
        result.map(|_| ())
    }
    /// Runs all three workers in one borrowed scope: no detached tasks survive
    /// pause or error. `pause=true` requests a graceful pause. On return the
    /// caller can retain capture, but must reconstruct unfinished copy windows.
    pub async fn run(
        &self,
        capture: &mut impl Capture,
        chunks: &mut impl ChunkDatabase,
        applier: &mut impl Applier,
        config: CopyConfig,
        budget: &mut ThroughputBudget,
        pause: watch::Receiver<bool>,
    ) -> Result<()> {
        self.run_until(
            capture,
            chunks,
            applier,
            config,
            budget,
            pause,
            StopAt::UntilPaused,
        )
        .await
        .map(|_| ())
    }
    /// Stop at a qualified boundary, complete accepted effects and join all
    /// workers. Capture retains pending protocol work; unfinished copy windows
    /// are retried from their durable Started marker. Continue CatchingUp or a
    /// stopped VerifiedReady directly with this method; Resume is for Paused.
    #[allow(clippy::too_many_arguments)]
    pub async fn run_until(
        &self,
        capture: &mut impl Capture,
        chunks: &mut impl ChunkDatabase,
        applier: &mut impl Applier,
        config: CopyConfig,
        budget: &mut ThroughputBudget,
        pause: watch::Receiver<bool>,
        stop_at: StopAt,
    ) -> Result<Reached> {
        Box::pin(self.run_admitted_until(
            capture, chunks, applier, config, budget, pause, stop_at, false,
        ))
        .await
    }
    /// Restart capture/apply for an authenticated owner whose journal is still
    /// SourceFenced or FinalBoundaryApplied. Saved marker receipts are rebound
    /// to this run. Pause/shutdown joins every worker and preserves the fence;
    /// only the cutover driver may advance or abort that lifecycle.
    #[allow(clippy::too_many_arguments)]
    pub async fn run_fenced_until(
        &self,
        capture: &mut impl Capture,
        chunks: &mut impl ChunkDatabase,
        applier: &mut impl Applier,
        config: CopyConfig,
        budget: &mut ThroughputBudget,
        pause: watch::Receiver<bool>,
        stop_at: StopAt,
    ) -> Result<Reached> {
        Box::pin(self.run_admitted_until(
            capture, chunks, applier, config, budget, pause, stop_at, true,
        ))
        .await
    }
    // The joined worker state was about 60 KiB inline on Windows. Keep its
    // construction and polling behind a heap boundary, including each worker.
    #[allow(clippy::too_many_arguments)]
    fn run_admitted_until(
        &self,
        capture: &mut impl Capture,
        chunks: &mut impl ChunkDatabase,
        applier: &mut impl Applier,
        config: CopyConfig,
        budget: &mut ThroughputBudget,
        mut pause: watch::Receiver<bool>,
        stop_at: StopAt,
        fenced_resume: bool,
    ) -> impl std::future::Future<Output = Result<Reached>> {
        Box::pin(async move {
            {
                // Authenticate lifecycle/receipts and detect promotion before any
                // schema restoration or worker admission on either run path.
                let mut coordinator = self.runtime.0.lock().await;
                let result = Box::pin(coordinator.refresh()).await;
                *self.runtime.1.write().expect("engine snapshot poisoned") =
                    coordinator.state().clone();
                result?;
                if !coordinator.is_owner() {
                    return Err(coordinator::Error::Observer);
                }
            }
            self.require_coverage()?;
            let state = self.runtime.snapshot().lifecycle.state;
            let admitted = if fenced_resume {
                fenced(state)
            } else {
                matches!(
                    state,
                    State::Copying | State::CatchingUp | State::VerifiedReady
                )
            };
            if !admitted {
                return Err(coordinator::Error::Rejected(
                    if fenced_resume {
                        "engine fenced run requires source-fenced/final-boundary-applied"
                    } else {
                        "engine run requires copying/catching-up/verified-ready"
                    }
                    .into(),
                ));
            }
            if self.running.swap(true, std::sync::atomic::Ordering::SeqCst) {
                return Err(recovery("engine is already running"));
            }
            let _running = RunGuard(&self.running);
            let mut owner = self
                .pause_control
                .as_ref()
                .map(PauseControl::activate)
                .transpose()?;
            *self.runtime.2.write().expect("engine health poisoned") = false;
            if !self.runtime.0.lock().await.is_owner() {
                return Err(coordinator::Error::Observer);
            }
            let order = capture.position_order();
            if order.domain() != config.order_domain {
                return Err(BufferedProgressError::OrderDomainMismatch.into());
            }
            if let Some((_, payload)) = self.runtime.snapshot().progress.get("buffered") {
                let position = decode_buffered_position(payload, &config.source, order.domain())?;
                position
                    .compare(&position, order.as_ref())
                    .map_err(recovery)?;
            }
            Box::pin(capture.prepare_schema(&self.journal, &self.runtime)).await?;
            self.runtime
                .6
                .markers
                .begin(
                    self.journal.migration_id(),
                    config.source.clone(),
                    config.order_domain.clone(),
                    &self.runtime.snapshot(),
                )
                .map_err(recovery)?;
            let _markers = final_marker::RunGuard(self.runtime.6.markers.clone());
            self.runtime.6.begin(config.source.clone(), order.clone());
            self.runtime.7.begin(
                config.source.clone(),
                order.clone(),
                capture.writer_checkpoint()?,
                self.runtime
                    .snapshot()
                    .progress
                    .get("applied")
                    .and_then(|(marker, _)| decode_reader_checkpoint(marker).ok()),
            );
            self.runtime
                .7
                .owner_generation(owner.as_ref().map(|owner| owner.generation().to_string()));
            let _live = LiveReadinessGuard(self.runtime.7.clone());
            let _verification = VerificationRunGuard(self.runtime.6.clone());
            let (cancel, stopped) = watch::channel(*pause.borrow());
            let (send, receive) = mpsc::channel(2);
            let capture_loop = cancel_on_exit(
                capture_loop(capture, receive, stopped.clone(), &self.runtime, &config),
                cancel.clone(),
            );
            let copy_loop = cancel_on_exit(
                copy_loop(
                    chunks,
                    send,
                    stopped.clone(),
                    &self.runtime,
                    &self.journal,
                    &config,
                    budget,
                ),
                cancel.clone(),
            );
            let apply_loop = cancel_on_exit(
                apply_loop(applier, stopped.clone(), &self.runtime),
                cancel.clone(),
            );
            let supervision_cancel = cancel.clone();
            let supervision = async {
                let result = self
                    .supervision_loop(stopped.clone(), cancel.clone(), stop_at)
                    .await;
                supervision_cancel.send_replace(true);
                result
            };
            let control = async {
                let mut stopped = stopped.clone();
                loop {
                    let requested = owner.as_ref().map(|o| o.requested()).transpose();
                    let requested = match requested {
                        Ok(requested) => requested.unwrap_or(false),
                        Err(error) => {
                            cancel.send_replace(true);
                            return Err(error);
                        }
                    };
                    if *pause.borrow_and_update()
                        || requested
                        || self.runtime.pause_reason().is_some()
                    {
                        cancel.send_replace(true);
                        break;
                    }
                    tokio::select! {
                        _ = cancelled(&mut stopped) => break,
                        changed = pause.changed() => if changed.is_err() {
                            cancel.send_replace(true); break;
                        },
                        _ = tokio::time::sleep(Duration::from_millis(50)) => {},
                    }
                }
                Ok(())
            };
            let (capture, copy, apply, control, supervision) =
                tokio::join!(capture_loop, copy_loop, apply_loop, control, supervision);
            *self.runtime.2.write().expect("engine health poisoned") = false;
            let retirement = owner.as_mut().map(|o| o.retire()).transpose();
            let pause_requested = *pause.borrow()
                || pause.has_changed().is_err()
                || retirement.as_ref().is_ok_and(|r| r == &Some(true));
            let (decision, target, supervision_fault) = match supervision {
                Ok(SupervisedStop::Wal(decision)) => (Some(decision), None, None),
                Ok(SupervisedStop::Target(wal)) => (None, Some(wal), None),
                Ok(SupervisedStop::Cancelled) => (None, None, None),
                Err(error) => (None, None, Some(error)),
            };
            let mut faults = [
                capture.err().map(|e| ("capture", e)),
                copy.err().map(|e| ("copy", e)),
                apply.err().map(|e| ("apply", e)),
                control.err().map(|e| ("control", e)),
                retirement.err().map(|e| ("control retirement", e)),
                supervision_fault.map(|e| ("supervision", e)),
            ];
            // Genuine integrity/authority failures outrank a separate outage. The
            // PgApplier checkpoint wrapper preserves the typed outage before here.
            let priority = faults
                .iter()
                .position(|f| matches!(f, Some((_, coordinator::Error::RecoveryRequired(_)))));
            let fault = priority
                .and_then(|i| faults[i].take())
                .or_else(|| faults.into_iter().flatten().next());
            // Retention can expose a local recovery state on failure. The journal
            // remains authoritative: never turn a durable fence into a local pause
            // or recovery lifecycle, including on the first run's mid-run fence.
            let durable = self.runtime.0.lock().await.state().clone();
            if fenced(durable.lifecycle.state) {
                *self.runtime.1.write().expect("engine snapshot poisoned") = durable;
                if let Some(decision) = &decision {
                    self.enforce_run_wal(decision).await?;
                }
                if let Some((component, error)) = fault {
                    if self.runtime.0.lock().await.is_owner() {
                        self.runtime
                            .marker(
                                "engine-stop-fault-v1",
                                format!("{component}: {error}"),
                                "fenced".into(),
                            )
                            .await?;
                    }
                    return Err(error);
                }
                if let Some(reason) = self.runtime.pause_reason() {
                    return Err(reason.error());
                }
                return self.reached(stop_at, false).await;
            }
            let aborting = decision
                .as_ref()
                .is_some_and(|d| d.level == WalLevel::Abort);
            if !aborting && let Some((_, coordinator::Error::RecoveryRequired(reason))) = &fault {
                if !self.runtime.0.lock().await.is_owner() {
                    return Err(self.runtime.local_recovery(reason));
                }
                *self.runtime.4.write().expect("engine stop reason poisoned") = None;
            }
            if self.runtime.pause_reason().is_some() && !aborting {
                return self.finish_journal_pause(stop_at).await;
            }
            if let Some(decision) = decision.as_ref().filter(|d| d.level == WalLevel::Abort) {
                if self.runtime.pause_reason().is_some()
                    && let Err(error) = self.reacquire_after_journal_pause().await
                {
                    // The journal may be unavailable, but observed coverage loss
                    // must still prevent this live engine from resuming locally.
                    let mut state = self.runtime.1.write().expect("engine snapshot poisoned");
                    state.progress.insert(
                        readiness::COVERAGE_LOSS.into(),
                        (
                            serde_json::to_string(decision)?,
                            "abort-with-coverage-loss".into(),
                        ),
                    );
                    state.lifecycle.state = State::Failed;
                    return Err(error);
                }
                // A simultaneous worker fault must not erase an already observed
                // coverage-loss boundary. Workers are joined; return its real error
                // after durably recording both the failure and the abort.
                if let Some((component, error)) = &fault {
                    self.runtime
                        .marker(
                            "engine-stop-fault-v1",
                            format!("{component}: {error}"),
                            "stopped".into(),
                        )
                        .await?;
                }
                self.evaluate_readiness().await?;
                self.enforce_run_wal(decision).await?;
                if let Some((_, error)) = fault {
                    return Err(error);
                }
                return self.reached(stop_at, false).await;
            }
            if let Some((component, error)) = fault {
                if !self.runtime.0.lock().await.is_owner() {
                    return Err(self.runtime.local_recovery(error));
                }
                if self.runtime.snapshot().lifecycle.state == State::VerifiedReady {
                    // Capture workers have joined and health is false, so the fresh
                    // observation demotes readiness before the failure interrupt.
                    self.evaluate_readiness().await?;
                }
                // Authority is retained by the C2 reducer and included in the reason.
                let authority = self.runtime.state().await.lifecycle.authority;
                self.interrupt_run(
                    State::RecoveryRequired,
                    format!("{component} failed; authority={authority:?}; {error}"),
                )
                .await?;
                return Err(error);
            }
            if let Some(mut wal) = target {
                // The lifecycle records a live pre-stop observation. Recheck all
                // external producers and authenticated journal state after draining;
                // intentional worker shutdown alone does not erase that boundary.
                let sample = self
                    .readiness_source
                    .as_ref()
                    .map_or_else(ComponentSnapshot::default, |s| s.sample());
                if let Some(decision) = wal.sample_components(&self.runtime, &sample)?
                    && matches!(decision.level, WalLevel::Blocker | WalLevel::Abort)
                {
                    self.evaluate_sample(&sample).await?;
                    self.enforce_run_wal(&decision).await?;
                    return self.reached(stop_at, false).await;
                }
                if !pause_requested {
                    let facts = readiness::assemble(&self.runtime, &self.journal, &sample).await?;
                    let boundary_holds = sample.capture.as_ref().is_some_and(|c| c.healthy)
                        && facts.blockers().iter().all(|b| b == "capture-unhealthy");
                    if self.runtime.snapshot().lifecycle.state != State::VerifiedReady
                        || !boundary_holds
                    {
                        self.evaluate_sample(&sample).await?;
                    }
                    let mut reached = self.reached_with_facts(stop_at, true, &facts);
                    if let Some(status) = self.runtime.retention_status().await
                        && let Some(reason) = status.blocked_reason
                    {
                        reached.blockers.push(reason);
                    }
                    return Ok(reached);
                }
            }
            self.evaluate_readiness().await?;
            if let Some(decision) = decision {
                self.enforce_run_wal(&decision).await?;
                return self.reached(stop_at, false).await;
            }
            self.interrupt_run(State::Paused, "engine pause requested".into())
                .await?;
            self.reached(stop_at, false).await
        })
    }
    async fn interrupt_run(&self, to: State, reason: String) -> Result<()> {
        let mut coordinator = self.runtime.0.lock().await;
        self.runtime.flush_progress_locked(&mut coordinator).await?;
        // A cutover may fence after the post-join snapshot was sampled. Check
        // under the same lock as the interrupt instead of attempting an illegal
        // Paused/RecoveryRequired edge from the newly fenced lifecycle.
        if fenced(coordinator.state().lifecycle.state) {
            *self.runtime.1.write().expect("engine snapshot poisoned") =
                coordinator.state().clone();
            return Ok(());
        }
        let result = coordinator
            .commit(Transition::Lifecycle {
                event: Event::Interrupt { to, reason },
            })
            .await;
        self.runtime.observe_journal_error(&coordinator, &result);
        *self.runtime.1.write().expect("engine snapshot poisoned") = coordinator.state().clone();
        result.map(|_| ())
    }
    async fn enforce_run_wal(&self, decision: &WalDecision) -> Result<()> {
        // Fenced states have no Blocked/Failed edges. Preserve coverage-loss
        // evidence (which still prevents resumption) without inventing an abort
        // or restoring source authority. Serialize with the cutover driver.
        let mut coordinator = self.runtime.0.lock().await;
        if !fenced(coordinator.state().lifecycle.state) {
            drop(coordinator);
            return self.enforce_wal(decision).await;
        }
        let result = coordinator
            .commit(Transition::Progress {
                stream: if decision.level == WalLevel::Abort {
                    readiness::COVERAGE_LOSS
                } else {
                    "engine-wal-risk-v1"
                }
                .into(),
                marker: serde_json::to_string(decision)?,
                position: if decision.level == WalLevel::Abort {
                    "abort-with-coverage-loss".into()
                } else {
                    format!("{:?}", decision.level)
                },
            })
            .await;
        self.runtime.observe_journal_error(&coordinator, &result);
        *self.runtime.1.write().expect("engine snapshot poisoned") = coordinator.state().clone();
        result.map(|_| ())
    }
    async fn reached(&self, stop_at: StopAt, target: bool) -> Result<Reached> {
        self.runtime.flush_progress().await?;
        let sample = self
            .readiness_source
            .as_ref()
            .map_or_else(ComponentSnapshot::default, |s| s.sample());
        let facts = readiness::assemble(&self.runtime, &self.journal, &sample).await?;
        let mut reached = self.reached_with_facts(stop_at, target, &facts);
        if let Some(status) = self.runtime.retention_status().await
            && let Some(reason) = status.blocked_reason
        {
            reached.blockers.push(reason);
        }
        Ok(reached)
    }
    fn reached_with_facts(
        &self,
        stop_at: StopAt,
        target: bool,
        facts: &coordinator::readiness::ReadinessFacts,
    ) -> Reached {
        let lifecycle = self.runtime.snapshot().lifecycle;
        let mut blockers = facts.blockers();
        for reason in lifecycle.blockers {
            if !blockers.contains(&reason) {
                blockers.push(reason);
            }
        }
        Reached {
            state: lifecycle.state,
            blockers,
            target_reached: (target || stop_at == StopAt::UntilPaused)
                && stop_at.matches(lifecycle.state),
        }
    }
}

fn fenced(state: State) -> bool {
    matches!(state, State::SourceFenced | State::FinalBoundaryApplied)
}

struct CaptureHealthGuard(Arc<HealthHub>);
impl Drop for CaptureHealthGuard {
    fn drop(&mut self) {
        *self.0.write().expect("engine health poisoned") = false;
    }
}
struct VerificationRunGuard(Arc<verification::Hub>);
impl Drop for VerificationRunGuard {
    fn drop(&mut self) {
        self.0
            .finish(crate::verify::ApplierTerminalCause::ApplyStopped);
    }
}
struct LiveReadinessGuard(Arc<live_readiness::Hub>);
impl Drop for LiveReadinessGuard {
    fn drop(&mut self) {
        self.0.finish();
    }
}
struct RunGuard<'a>(&'a std::sync::atomic::AtomicBool);
impl Drop for RunGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, std::sync::atomic::Ordering::SeqCst);
    }
}
pub struct CopyConfig {
    pub source: SourceIdentity,
    pub order_domain: String,
    pub schema_version: u64,
    /// Admission reservation and hard per-read result limits. Tune chunk size
    /// below these bounds; denied reads sleep only on the copy worker.
    pub read_rows: u64,
    pub read_bytes: u64,
    pub window_bytes: usize,
}
async fn cancelled(cancel: &mut watch::Receiver<bool>) {
    while !*cancel.borrow_and_update() {
        if cancel.changed().await.is_err() {
            break;
        }
    }
}
async fn cancel_on_exit<F: Future<Output = Result<()>>>(
    future: F,
    cancel: watch::Sender<bool>,
) -> Result<()> {
    let result = future.await;
    // A worker's normal shutdown also ends this borrowed scope. Channel closure
    // can reach a peer before its cancellation future observes the notification.
    cancel.send_replace(true);
    result
}

type Completion = (Position, DurableManifestRef);
enum Command {
    Begin {
        plan: Box<ChunkPlan>,
        chunk: ChunkRange,
        attempt: u64,
        complete: oneshot::Sender<Result<Completion>>,
        journaled: oneshot::Receiver<()>,
        ready: oneshot::Sender<()>,
    },
    Rows {
        rows: Vec<Row>,
        ready: oneshot::Sender<()>,
    },
}
struct Active {
    plan: Box<ChunkPlan>,
    chunk: ChunkRange,
    attempt: u64,
    window: Option<Window>,
    backlog: VecDeque<Captured>,
    bytes: usize,
    complete: oneshot::Sender<Result<Completion>>,
    journaled: oneshot::Receiver<()>,
}
impl Active {
    fn feed(&mut self, captured: &Captured, order: &dyn PositionOrder) -> Result<Option<Position>> {
        let window = self.window.as_mut().expect("installed window");
        if let Some(txn) = &captured.transaction {
            for event in &txn.events {
                window.event(event).map_err(recovery)?;
            }
        }
        let mut high = None;
        for watermark in &captured.watermarks {
            let (marker, commit) = match watermark {
                CapturedMarker::Verification { .. } => continue,
                CapturedMarker::Pg(w) => {
                    let CapturedControl::Chunk(marker) =
                        decode_marker_payload(&w.content, w.message_lsn).map_err(recovery)?
                    else {
                        continue;
                    };
                    window
                        .watermark(&w.message(), w.commit_lsn, order)
                        .map_err(recovery)?;
                    (
                        marker,
                        Position::new(order.domain(), w.commit_lsn.to_be_bytes()),
                    )
                }
                CapturedMarker::Chunk {
                    marker,
                    commit,
                    committed_events,
                } => {
                    if !captured.transaction.as_ref().is_some_and(|txn| {
                        txn.commit_position == *commit && txn.events == *committed_events
                    }) {
                        return Err(recovery("chunk marker transaction binding"));
                    }
                    window
                        .marker(marker, commit.clone(), order)
                        .map_err(recovery)?;
                    (marker.clone(), commit.clone())
                }
            };
            if marker.chunk == self.chunk.id
                && marker.attempt == self.attempt
                && marker.wm == WatermarkKind::High
            {
                high = Some(commit);
            }
        }
        Ok(high)
    }
}
fn capture_loop(
    capture: &mut impl Capture,
    mut commands: mpsc::Receiver<Command>,
    mut cancel: watch::Receiver<bool>,
    runtime: &Runtime,
    config: &CopyConfig,
) -> impl std::future::Future<Output = Result<()>> {
    Box::pin(async move {
        // Clear health before the worker's exit cancels peers, including when this
        // future is dropped while an uninterruptible apply step is still draining.
        let _health = CaptureHealthGuard(runtime.2.clone());
        let _verification = VerificationCaptureGuard(runtime.6.clone());
        let _live = LiveReadinessGuard(runtime.7.clone());
        let mut schema_tick = tokio::time::interval(Duration::from_millis(50));
        let order = capture.position_order();
        let mut active: Option<Active> = None;
        let mut last_buffered_position = runtime
            .snapshot()
            .progress
            .get("buffered")
            .map(|(_, payload)| {
                decode_buffered_position(payload, &config.source, &config.order_domain)
            })
            .transpose()?;
        loop {
            // Supervise the last completed capture boundary while the next step
            // owns capture. In particular, an unclassified DDL stays unknown until
            // its commit publication completes; a tick cannot clear that blocker.
            let schema = capture.schema_state();
            let batch = {
                // One boxed future survives every schema tick and copy command.
                // Only worker shutdown may drop an unfinished step.
                let mut step = Box::pin(capture.step());
                loop {
                    tokio::select! {
                        biased;
                        _ = cancelled(&mut cancel) => break None,
                        _ = schema_tick.tick() => runtime.7.schema(schema.clone()),
                        command = commands.recv() => match command {
                            Some(Command::Begin { plan, chunk, attempt, complete, journaled, ready }) => {
                                if active.is_some() { return Err(recovery("overlapping copy windows")); }
                                active = Some(Active { plan, chunk, attempt, window: None,
                                    backlog: VecDeque::new(), bytes: 0, complete, journaled });
                                let _ = ready.send(());
                            },
                            Some(Command::Rows { rows, ready }) => {
                                let current = active.as_mut().ok_or_else(|| recovery("rows without Started"))?;
                                current.window = Some(current.plan.window(current.chunk.clone(),
                                    rows, current.attempt).map_err(recovery)?);
                                while let Some(batch) = current.backlog.pop_front() { current.feed(&batch, order.as_ref())?; }
                                let _ = ready.send(());
                            },
                            None => break None,
                        },
                        batch = &mut step => break Some(batch),
                    }
                }
            };
            // Shutdown can race a durable publication consumed by another worker.
            // Once the step's borrow is released, retain its latest schema boundary
            // even when that step did not return to us before peer cancellation.
            runtime.7.schema(capture.schema_state());
            let Some(batch) = batch else {
                return Ok(());
            };
            let batch = match batch {
                Ok(batch) => {
                    *runtime.2.write().expect("engine health poisoned") = true;
                    batch
                }
                Err(error) => {
                    *runtime.2.write().expect("engine health poisoned") = false;
                    runtime.invalidate_final_marker_capture_gap();
                    return Err(error);
                }
            };
            runtime.6.observe(&batch);
            runtime.7.observe(&batch);
            let checkpoint = capture.writer_checkpoint()?;
            if let Some(position) = &batch.durable_position {
                let order = capture.position_order();
                if batch
                    .transaction
                    .as_ref()
                    .is_some_and(|txn| txn.source != config.source)
                {
                    return Err(BufferedProgressError::SourceMismatch.into());
                }
                position
                    .compare(position, order.as_ref())
                    .map_err(recovery)?;
                let ordering = last_buffered_position
                    .as_ref()
                    .map(|last| position.compare(last, order.as_ref()))
                    .transpose()
                    .map_err(recovery)?;
                if ordering == Some(std::cmp::Ordering::Less) {
                    return Err(recovery("buffered position regressed"));
                }
                if ordering != Some(std::cmp::Ordering::Equal) {
                    runtime
                        .progress_marker(
                            "buffered",
                            capture.buffered_marker(&batch)?,
                            encode_buffered_position(&config.source, position)?,
                        )
                        .await?;
                    // Include queued markers: batching must not let a later step
                    // replace a transaction's newer boundary before the flush.
                    last_buffered_position = Some(position.clone());
                }
            }
            // Final receipts flush the current END binding before notifying
            // receipt waiters; never publish a receipt ahead of restart proof.
            Box::pin(runtime.capture_final_marker(&batch, checkpoint.as_ref())).await?;
            runtime.queue_writer(checkpoint).await;
            if batch
                .transaction
                .as_ref()
                .is_some_and(|txn| final_marker::transaction_marker(txn).is_ok_and(|m| m.is_some()))
            {
                runtime.flush_progress().await?;
            }
            if batch.stop {
                return Err(recovery("capture reached unsupported DDL safe boundary"));
            }
            let Some(current) = active.as_mut() else {
                continue;
            };
            if current.window.is_none() {
                current.bytes = current
                    .bytes
                    .checked_add(
                        batch
                            .transaction
                            .as_ref()
                            .map(|t| t.canonical_bytes().map(|b| b.len()))
                            .transpose()
                            .map_err(recovery)?
                            .unwrap_or(0),
                    )
                    .and_then(|n| n.checked_add(64))
                    .ok_or_else(|| recovery("window size overflow"))?;
                if current.bytes > config.window_bytes {
                    return Err(recovery("copy window backlog limit"));
                }
                current.backlog.push_back(batch);
            } else if let Some(high) = current.feed(&batch, order.as_ref())? {
                let current = active.take().expect("active high");
                let SegmentRecord::ChunkRows(rows) = current
                    .window
                    .expect("window")
                    .finish(
                        config.source.clone(),
                        &config.order_domain,
                        config.schema_version,
                    )
                    .map_err(recovery)?
                else {
                    unreachable!()
                };
                // No next capture step can pass H until this durable hook returns.
                let reference = capture.publish(high.clone(), rows).await?;
                runtime.queue_writer(capture.writer_checkpoint()?).await;
                let _ = current.complete.send(Ok((high, reference)));
                // Do not publish post-H WAL until Done is visible to the
                // applier's synchronous, current-state range classifier.
                tokio::select! {
                    biased;
                    _ = cancelled(&mut cancel) => return Ok(()),
                    done = current.journaled => if done.is_err() { return Ok(()); },
                }
            }
        }
    })
}

struct VerificationCaptureGuard(Arc<verification::Hub>);
impl Drop for VerificationCaptureGuard {
    fn drop(&mut self) {
        self.0
            .finish(crate::verify::ApplierTerminalCause::CaptureStopped);
    }
}

fn copy_loop(
    database: &mut impl ChunkDatabase,
    commands: mpsc::Sender<Command>,
    mut cancel: watch::Receiver<bool>,
    runtime: &Runtime,
    journal: &Journal,
    config: &CopyConfig,
    budget: &mut ThroughputBudget,
) -> impl std::future::Future<Output = Result<()>> {
    Box::pin(async move {
        // Keep the command sender alive while capture/apply continue. Dropping it
        // would make capture exit and cancel the entire borrowed worker scope.
        if runtime.snapshot().lifecycle.state != State::Copying {
            cancelled(&mut cancel).await;
            return Ok(());
        }
        let references = runtime.state().await.chunk_plans;
        if references.is_empty() {
            return Err(recovery("no durable chunk plans"));
        }
        for reference in references.values() {
            let plan = journal.chunk_plan(reference).await?;
            for chunk in runtime.state().await.unfinished_chunks(&plan)? {
                if fenced(runtime.snapshot().lifecycle.state) {
                    cancelled(&mut cancel).await;
                    return Ok(());
                }
                loop {
                    if *cancel.borrow() {
                        return Ok(());
                    }
                    match budget.try_chunk_read(config.read_rows, config.read_bytes) {
                        Ok(()) => break,
                        Err(budget::Error::Throttled { retry_after }) => tokio::select! {
                            biased;
                            _ = cancelled(&mut cancel) => return Ok(()),
                            _ = tokio::time::sleep(retry_after) => {},
                        },
                        Err(error) => return Err(recovery(error)),
                    }
                }
                let key = serde_json::to_string(&chunk.id)?;
                let attempt =
                    runtime
                        .state()
                        .await
                        .chunk_attempts
                        .get(&key)
                        .map_or(Ok(1), |a| {
                            a.attempt
                                .checked_add(1)
                                .ok_or_else(|| recovery("chunk attempts exhausted"))
                        })?;
                let mut coordinator = runtime.0.lock().await;
                if fenced(coordinator.state().lifecycle.state) {
                    drop(coordinator);
                    cancelled(&mut cancel).await;
                    return Ok(());
                }
                let result = coordinator
                    .commit(Transition::ChunkStarted {
                        chunk: chunk.clone(),
                        attempt,
                    })
                    .await;
                runtime.observe_journal_error(&coordinator, &result);
                *runtime.1.write().expect("engine snapshot poisoned") = coordinator.state().clone();
                drop(coordinator);
                result?;
                let (complete, completion) = oneshot::channel();
                let (journaled, acknowledged) = oneshot::channel();
                let (ready, installed) = oneshot::channel();
                // Closed handshakes mean capture stopped: leave Started without
                // Done so resume retries with a new attempt. Engine::run retains
                // any actual capture error; shutdown ordering is not a copy fault.
                tokio::select! {
                    biased;
                    _ = cancelled(&mut cancel) => return Ok(()),
                    sent = commands.send(Command::Begin {
                        plan: Box::new(plan.clone()),
                        chunk: chunk.clone(),
                        attempt,
                        complete,
                        journaled: acknowledged,
                        ready,
                    }) => if sent.is_err() { return Ok(()); },
                }
                tokio::select! {
                    biased;
                    _ = cancelled(&mut cancel) => return Ok(()),
                    ready = installed => if ready.is_err() { return Ok(()); },
                }
                database
                    .emit(Watermark {
                        wm: WatermarkKind::Low,
                        chunk: chunk.id.clone(),
                        attempt,
                    })
                    .await?;
                let rows = tokio::select! {
                    biased;
                    _ = cancelled(&mut cancel) => return Ok(()),
                    rows = database.read(&plan, &chunk) => rows?,
                };
                let bytes: usize = rows
                    .iter()
                    .flat_map(|r| r.values())
                    .map(|v| v.bytes.as_ref().map_or(0, Vec::len) + v.type_id.len())
                    .sum();
                if rows.len() as u64 > config.read_rows || bytes as u64 > config.read_bytes {
                    return Err(recovery(
                        "chunk result exceeds its admitted read reservation",
                    ));
                }
                if *cancel.borrow() {
                    return Ok(());
                }
                let (ready, installed) = oneshot::channel();
                tokio::select! {
                    biased;
                    _ = cancelled(&mut cancel) => return Ok(()),
                    sent = commands.send(Command::Rows { rows, ready }) => if sent.is_err() { return Ok(()); },
                }
                tokio::select! {
                    biased;
                    _ = cancelled(&mut cancel) => return Ok(()),
                    ready = installed => if ready.is_err() { return Ok(()); },
                }
                database
                    .emit(Watermark {
                        wm: WatermarkKind::High,
                        chunk: chunk.id.clone(),
                        attempt,
                    })
                    .await?;
                let (high, reference) = tokio::select! {
                    biased;
                    _ = cancelled(&mut cancel) => return Ok(()),
                    done = completion => match done {
                        Ok(completion) => completion?,
                        Err(_) => return Ok(()),
                    },
                };
                runtime
                    .commit(Transition::ChunkDone {
                        chunk,
                        attempt,
                        high,
                        chunk_rows: ChunkRowsRef {
                            mseq: reference.mseq,
                            manifest_digest: reference.manifest_digest,
                        },
                    })
                    .await?;
                let _ = journaled.send(());
            }
        }
        if !*cancel.borrow() {
            let mut coordinator = runtime.0.lock().await;
            if coordinator.state().lifecycle.state == State::Copying {
                let result = coordinator
                    .commit(Transition::Lifecycle {
                        event: Event::Advance {
                            to: State::CatchingUp,
                        },
                    })
                    .await;
                runtime.observe_journal_error(&coordinator, &result);
                *runtime.1.write().expect("engine snapshot poisoned") = coordinator.state().clone();
                result?;
            }
        }
        cancelled(&mut cancel).await;
        Ok(())
    })
}
fn apply_loop(
    applier: &mut impl Applier,
    mut cancel: watch::Receiver<bool>,
    runtime: &Runtime,
) -> impl std::future::Future<Output = Result<()>> {
    Box::pin(async move {
        let _verification = VerificationRunGuard(runtime.6.clone());
        let _live = LiveReadinessGuard(runtime.7.clone());
        loop {
            if *cancel.borrow() {
                return Ok(());
            }
            runtime
                .snapshot()
                .operational_schema
                .require_clear()
                .map_err(recovery)?;
            if runtime
                .2
                .persistence
                .lock()
                .expect("persistence poisoned")
                .last_flush
                .elapsed()
                >= Duration::from_millis(250)
            {
                runtime.flush_progress().await?;
            }
            runtime.automatic_retention_tick().await?;
            if !applier.controlled_step(runtime, cancel.clone()).await? {
                tokio::select! {
                    biased;
                    _ = cancelled(&mut cancel) => return Ok(()),
                    _ = tokio::time::sleep(Duration::from_millis(10)) => {},
                }
            }
        }
    })
}

#[cfg(test)]
pub(crate) mod tests;

#[cfg(test)]
mod fenced_resume_tests;

#[cfg(test)]
mod schema_restore_tests;

#[cfg(test)]
mod installer_plan_tests {
    use super::*;
    use crate::coordinator::local::Local;

    pub(super) async fn assessed() -> (Engine, Journal, std::path::PathBuf) {
        let root = std::path::PathBuf::from(std::env::var_os("SKEDADDLE_TEST_ROOT").unwrap())
            .join(format!("installer-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(root.join("journal")).unwrap();
        let journal = Journal::new(
            Arc::new(coordinator::journal::local_store(root.join("journal")).unwrap()),
            "installer",
            "test",
            Duration::from_secs(3),
        )
        .unwrap();
        let mut coordinator = Coordinator::observer(
            journal.clone(),
            Local::create(root.join("local")).unwrap(),
            "installer-test",
        )
        .unwrap();
        coordinator.acquire().await.unwrap();
        coordinator
            .commit(Transition::Lifecycle {
                event: Event::Advance {
                    to: State::Assessed,
                },
            })
            .await
            .unwrap();
        (Engine::new(coordinator, journal.clone()), journal, root)
    }

    // Exercises the PG startup protocol, not database SQL. The live engine test
    // qualifies the actual PG installer against PostgreSQL.
    struct PgProtocol;
    impl Installer for PgProtocol {
        fn identity(&self) -> String {
            "pg-test".into()
        }
        fn schema_baseline(&self) -> Option<String> {
            Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into())
        }
        fn slot_marker(&self) -> Result<Option<String>> {
            Ok(Some("checkpoint".into()))
        }
        async fn install(&mut self, _: Installation) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn pg_start_journal_bytes_unchanged() {
        let (engine, journal, root) = assessed().await;
        engine.start(&mut PgProtocol).await.unwrap();
        let transitions: Vec<_> = journal
            .history_page(2, 128)
            .await
            .unwrap()
            .into_iter()
            .map(|e| e.transition)
            .collect();
        let bytes = serde_json::to_vec(&transitions).unwrap();
        assert_eq!(bytes, br##"[{"kind":"intent","effect":"buffer-publication","payload":"engine-install-publication-v1:pg-test"},{"kind":"effect-completed","intent_seq":3},{"kind":"intent","effect":"buffer-publication","payload":"engine-install-ddl-trigger-v1:pg-test"},{"kind":"effect-completed","intent_seq":5},{"kind":"intent","effect":"buffer-publication","payload":"engine-install-slot-v1:pg-test"},{"kind":"effect-completed","intent_seq":7},{"kind":"progress","stream":"scoped-schema-baseline-v1","marker":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","position":"slot-double-sample"},{"kind":"progress","stream":"source-slot","marker":"checkpoint","position":"installed"},{"kind":"intent","effect":"buffer-publication","payload":"engine-install-progress-tables-v2:pg-test"},{"kind":"effect-completed","intent_seq":11},{"kind":"lifecycle","event":{"event":"advance","to":"copying"}}]"##);
        assert_eq!(engine.runtime.state().await.lifecycle.state, State::Copying);
        drop(engine);
        std::fs::remove_dir_all(root).unwrap();
    }
}
#[cfg(test)]
mod connector_installer_tests {
    use super::installer_plan_tests::assessed;
    use super::*;

    const PLAN: &[Installation] = &[
        Installation::ControlSchema,
        Installation::SourceStartup,
        Installation::DestinationSchema,
        Installation::ProgressTables,
    ];
    struct ConnectorInstaller {
        journal: Journal,
        plan: &'static [Installation],
        called: Vec<Installation>,
        fail: Option<Installation>,
    }
    impl Installer for ConnectorInstaller {
        fn identity(&self) -> String {
            "connector-test".into()
        }
        fn plan(&self) -> &'static [Installation] {
            self.plan
        }
        fn markers_after(&self, step: Installation) -> Result<Vec<StartupMarker>> {
            Ok(if step == Installation::SourceStartup {
                vec![StartupMarker {
                    stream: "source-startup".into(),
                    marker: "native-position".into(),
                    label: "installed".into(),
                }]
            } else {
                vec![]
            })
        }
        async fn install(&mut self, step: Installation) -> Result<()> {
            // Read the real durable journal inside the adapter: its intent must
            // already exist and must be the last entry before the effect runs.
            let history = self.journal.history_page(2, 128).await?;
            assert_eq!(
                history.last().unwrap().transition,
                Transition::Intent {
                    effect: EffectKind::BufferPublication,
                    payload: format!("{}:connector-test", step.name()),
                }
            );
            let intents: Vec<_> = history
                .iter()
                .filter_map(|e| match &e.transition {
                    Transition::Intent { payload, .. } => Some(payload.clone()),
                    _ => None,
                })
                .collect();
            self.called.push(step);
            assert_eq!(
                intents,
                self.called
                    .iter()
                    .map(|step| format!("{}:connector-test", step.name()))
                    .collect::<Vec<_>>()
            );
            if self.fail == Some(step) {
                return Err(recovery("injected installer failure"));
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn connector_plan_orders_durable_intents_effects_and_markers() {
        let (engine, journal, root) = assessed().await;
        let mut installer = ConnectorInstaller {
            journal: journal.clone(),
            plan: PLAN,
            called: vec![],
            fail: None,
        };
        engine.start(&mut installer).await.unwrap();
        assert_eq!(installer.called, PLAN);
        let history = journal.history_page(2, 128).await.unwrap();
        assert_eq!(history.len(), 10);
        assert!(matches!(
            &history[3].transition,
            Transition::EffectCompleted { .. }
        ));
        assert_eq!(
            history[4].transition,
            Transition::Progress {
                stream: "source-startup".into(),
                marker: "native-position".into(),
                position: "installed".into(),
            }
        );
        assert!(
            matches!(&history[5].transition, Transition::Intent { payload, .. } if payload == "engine-install-destination-schema-v1:connector-test")
        );
        assert_eq!(engine.runtime.state().await.lifecycle.state, State::Copying);
        drop(installer);
        drop(engine);
        drop(journal);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn failing_step_has_no_completion_or_later_intent() {
        let (engine, journal, root) = assessed().await;
        let mut installer = ConnectorInstaller {
            journal: journal.clone(),
            plan: PLAN,
            called: vec![],
            fail: Some(Installation::SourceStartup),
        };
        assert!(engine.start(&mut installer).await.is_err());
        assert_eq!(installer.called, PLAN[..2]);
        let history = journal.history_page(2, 128).await.unwrap();
        assert_eq!(history.len(), 3);
        assert!(
            matches!(&history[2].transition, Transition::Intent { payload, .. } if payload == "engine-install-source-startup-v1:connector-test")
        );
        assert_eq!(
            engine.runtime.state().await.lifecycle.state,
            State::Assessed
        );
        drop(installer);
        drop(engine);
        drop(journal);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn invalid_plans_rejected_before_intents_or_effects() {
        for (plan, expected) in [
            (&[][..], InstallationPlanError::Empty),
            (
                &[
                    Installation::ControlSchema,
                    Installation::ControlSchema,
                    Installation::ProgressTables,
                ][..],
                InstallationPlanError::Duplicate(Installation::ControlSchema),
            ),
            (
                &[Installation::SourceStartup][..],
                InstallationPlanError::MissingProgressTables,
            ),
        ] {
            let (engine, journal, root) = assessed().await;
            let mut installer = ConnectorInstaller {
                journal: journal.clone(),
                plan,
                called: vec![],
                fail: None,
            };
            assert!(
                matches!(engine.start(&mut installer).await, Err(coordinator::Error::InstallationPlan(error)) if error == expected)
            );
            assert!(installer.called.is_empty());
            assert!(journal.history_page(2, 128).await.unwrap().is_empty());
            assert_eq!(
                engine.runtime.state().await.lifecycle.state,
                State::Assessed
            );
            drop(installer);
            drop(engine);
            drop(journal);
            std::fs::remove_dir_all(root).unwrap();
        }
    }
}
