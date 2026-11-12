//! Adapters take qualified descriptors and dedicated connected clients. Connection
//! policy stays in pg::tls / pg::source; the engine never opens plaintext clients.
use super::*;
use crate::{
    buffer::{
        format,
        reader::{Backpressure, BufferReader, Headroom, ReaderCheckpoint},
    },
    event::{ContentDigest, Position},
    pg::{
        self,
        apply::{
            self,
            executor::{self, Destination, ReaderLoop},
        },
        ddl::{SchemaVersions, TriggerSql},
        replication::{Checkpoint, ReplicationConnection},
    },
};
use serde::{Deserialize, Serialize};
use tokio_postgres::Client;

/// Dedicated setup sessions; create these through pg::tls::connect and
/// pg::source::connect. Caller retains the returned slot checkpoint with its
/// source lineage before constructing/reconnecting capture.
pub struct PgInstaller {
    pub source: Client,
    pub destination: Client,
    pub replication: ReplicationConnection,
    pub publication: String,
    pub slot: String,
    pub installation: String,
    pub tables: Vec<pg::chunk::QualifiedName>,
    pub checkpoint: Option<Checkpoint>,
    pub schema_baseline: Option<String>,
    /// Canonical fingerprint scope from the stored migration definition.
    pub schema_tables: Vec<pg::chunk::QualifiedName>,
}
fn identifier(s: &str) -> Result<String> {
    if s.is_empty() || s.contains('\0') {
        return Err(recovery("empty SQL identity"));
    }
    Ok(format!("\"{}\"", s.replace('"', "\"\"")))
}
impl Installer for PgInstaller {
    fn identity(&self) -> String {
        serde_json::json!({"publication":self.publication,"slot":self.slot,
            "installation":self.installation,"tables":self.tables})
        .to_string()
    }
    fn schema_baseline(&self) -> Option<String> {
        self.schema_baseline.clone()
    }
    fn slot_marker(&self) -> Result<Option<String>> {
        self.checkpoint
            .as_ref()
            .map(encode_source_checkpoint)
            .transpose()
    }
    async fn install(&mut self, step: Installation) -> Result<()> {
        match step {
            Installation::Publication => {
                if self.tables.is_empty() {
                    return Err(recovery("empty publication"));
                }
                let name = identifier(&self.publication)?;
                let relations = self
                    .tables
                    .iter()
                    .map(|t| t.sql().map_err(recovery))
                    .collect::<Result<Vec<_>>>()?
                    .join(", ");
                let exists = self
                    .source
                    .query_opt(
                        "SELECT 1 FROM pg_publication WHERE pubname=$1",
                        &[&self.publication],
                    )
                    .await?
                    .is_some();
                if !exists {
                    self.source
                        .batch_execute(&format!("CREATE PUBLICATION {name} FOR TABLE {relations}"))
                        .await?;
                }
                let options = self.source.query_one(
                    "SELECT NOT puballtables AND pubinsert AND pubupdate AND pubdelete AND pubtruncate AND NOT pubviaroot FROM pg_publication WHERE pubname=$1",
                    &[&self.publication],
                ).await?;
                if !options.get::<_, bool>(0) {
                    return Err(recovery("publication options differ"));
                }
                let rows = self
                    .source
                    .query(
                        "SELECT schemaname,tablename FROM pg_publication_tables WHERE pubname=$1",
                        &[&self.publication],
                    )
                    .await?;
                let actual: std::collections::BTreeSet<(String, String)> =
                    rows.iter().map(|r| (r.get(0), r.get(1))).collect();
                let expected = self
                    .tables
                    .iter()
                    .map(|t| (t.schema.clone(), t.name.clone()))
                    .collect();
                if actual != expected {
                    return Err(recovery("publication table set differs"));
                }
            }
            Installation::DdlTrigger => {
                let version: String = self
                    .source
                    .query_one("SHOW server_version_num", &[])
                    .await?
                    .get(0);
                let sql = TriggerSql::new(&self.installation, version.parse().map_err(recovery)?)
                    .map_err(recovery)?;
                // Avoid re-install DDL entering an already-created slot after
                // a crash between installation and the lifecycle transition.
                let trigger = format!("skedaddle_ddl_{}", self.installation);
                let exists = self
                    .source
                    .query_opt(
                        "SELECT 1 FROM pg_event_trigger WHERE evtname=$1",
                        &[&trigger],
                    )
                    .await?
                    .is_some();
                if !exists {
                    // Engine::start already persisted this installation intent.
                    self.source.batch_execute(&sql.install).await?;
                }
                if !self
                    .source
                    .query_one(&sql.verify, &[])
                    .await?
                    .get::<_, bool>(0)
                {
                    return Err(recovery("DDL trigger verification failed"));
                }
            }
            Installation::Slot => {
                let existing = self.source.query_opt("SELECT plugin,database,active,confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=$1", &[&self.slot]).await?;
                if let Some(row) = existing {
                    // A pre-existing slot is only adopted with its saved lineage.
                    if self.schema_baseline.is_none() {
                        return Err(recovery(
                            "existing slot has no durable scoped schema baseline; quiesce and restart from a fresh snapshot",
                        ));
                    }
                    let checkpoint = self.checkpoint.as_ref().ok_or_else(|| recovery("existing slot needs saved checkpoint; reconcile interrupted slot creation"))?;
                    checkpoint
                        .lineage
                        .check(self.replication.lineage())
                        .map_err(recovery)?;
                    if row.get::<_, String>(0) != "pgoutput"
                        || row.get::<_, String>(1) != checkpoint.lineage.database
                        || row.get::<_, bool>(2)
                        || checkpoint.slot != self.slot
                    {
                        return Err(recovery("slot identity/ownership differs"));
                    }
                } else {
                    if self.checkpoint.is_some() {
                        return Err(recovery("saved slot is missing"));
                    }
                    let before = pg::ddl::observe(&self.source, &self.schema_tables).await?;
                    if !before.hazards.is_empty() {
                        return Err(recovery(format!(
                            "split-transaction DDL hazards: {:?}. {}",
                            before.hazards,
                            pg::ddl::SCHEMA_REMEDIATION
                        )));
                    }
                    let created = self
                        .replication
                        .create_slot(&self.slot)
                        .await
                        .map_err(recovery)?;
                    let after = pg::ddl::observe(&self.source, &self.schema_tables).await?;
                    if before != after || !after.hazards.is_empty() {
                        return Err(recovery(format!(
                            "slot-time schema drift/hazard. {}",
                            pg::ddl::SCHEMA_REMEDIATION
                        )));
                    }
                    self.schema_baseline = Some(before.fingerprint);
                    self.checkpoint = Some(Checkpoint {
                        lineage: self.replication.lineage().clone(),
                        slot: self.slot.clone(),
                        durable_lsn: created.consistent_point,
                    });
                }
            }
            Installation::ProgressTables => {
                executor::install_progress(&mut self.destination, |_| async { Ok(()) })
                    .await
                    .map_err(recovery)?;
            }
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceMarker {
    system_id: u64,
    timeline: u32,
    database: String,
    slot: String,
    durable_lsn: u64,
}
pub fn encode_source_checkpoint(c: &Checkpoint) -> Result<String> {
    Ok(serde_json::to_string(&SourceMarker {
        system_id: c.lineage.system_id,
        timeline: c.lineage.timeline,
        database: c.lineage.database.clone(),
        slot: c.slot.clone(),
        durable_lsn: c.durable_lsn,
    })?)
}
pub fn decode_source_checkpoint(marker: &str) -> Result<Checkpoint> {
    let m: SourceMarker = serde_json::from_str(marker)?;
    Ok(Checkpoint {
        lineage: pg::replication::Lineage {
            system_id: m.system_id,
            timeline: m.timeline,
            xlogpos: m.durable_lsn,
            database: m.database,
        },
        slot: m.slot,
        durable_lsn: m.durable_lsn,
    })
}

#[derive(Default)]
struct Sink(Captured);
impl source::WatermarkSink for Sink {
    fn committed(&mut self, transaction: &AssembledTxn) -> source::Result<()> {
        self.0.transaction = Some(transaction.clone());
        Ok(())
    }
    fn watermark(&mut self, watermark: source::Watermark) -> source::Result<()> {
        self.0.watermarks.push(watermark);
        Ok(())
    }
}
pub struct PgCapture<R, T> {
    pub capture: source::Capture<R>,
    pub transport: T,
    /// This is exclusively buffer back-pressure, never the copy token bucket.
    pub buffer_paused: watch::Receiver<bool>,
    sink: Sink,
    headroom: Option<watch::Receiver<Headroom>>,
    schema_objects: Arc<dyn object_store::ObjectStore>,
    schema_stream: String,
    schema_owner: Option<(String, u64, String)>,
}
impl<R, T> PgCapture<R, T> {
    pub fn new(
        capture: source::Capture<R>,
        transport: T,
        buffer_paused: watch::Receiver<bool>,
        schema_objects: Arc<dyn object_store::ObjectStore>,
        schema_stream: String,
    ) -> Self {
        Self {
            capture,
            transport,
            buffer_paused,
            sink: Sink::default(),
            headroom: None,
            schema_objects,
            schema_stream,
            schema_owner: None,
        }
    }
}
impl<R, T> PgCapture<R, T> {
    /// Bind capture reads to the buffer's actual admission signal. The owner
    /// accounts durable records and releases charges after apply/journaled GC.
    /// Copy rate limits never participate in this gate.
    pub fn with_backpressure(mut self, capacity: &Backpressure) -> Self {
        self.headroom = Some(capacity.subscribe());
        self
    }
}
impl<R: source::TypeResolver, T: source::Transport> Capture for PgCapture<R, T> {
    async fn prepare_schema(&mut self, journal: &Journal, runtime: &Runtime) -> Result<()> {
        let state = runtime.snapshot();
        let owner = (journal.namespace().to_owned(), state.epoch, state.owner);
        if let Some(prepared) = &self.schema_owner {
            // A stopped run can retain in-flight protocol/publication state.
            // Restore only once; replacement ownership needs a recovered writer
            // and a fresh capture, never reuse of the old epoch's binding.
            return if prepared == &owner {
                Ok(())
            } else {
                Err(recovery(
                    "schema capture owner changed; reconstruct capture",
                ))
            };
        }
        Box::pin(self.capture.restore_schema(source::SchemaJournalBinding {
            journal: journal.clone(),
            runtime: runtime.clone(),
            objects: self.schema_objects.clone(),
            stream: self.schema_stream.clone(),
        }))
        .await
        .map_err(recovery)?;
        self.schema_owner = Some(owner);
        Ok(())
    }
    fn schema_state(&self) -> Option<source::SchemaState> {
        Some(self.capture.schema_state())
    }
    fn writer_checkpoint(&self) -> Result<Option<crate::buffer::writer::WriterCheckpoint>> {
        self.capture
            .writer()
            .checkpoint()
            .map(Some)
            .map_err(recovery)
    }
    async fn step(&mut self) -> Result<Captured> {
        if let Some(headroom) = &mut self.headroom {
            while headroom.borrow_and_update().paused {
                headroom
                    .changed()
                    .await
                    .map_err(|_| recovery("buffer admission closed"))?;
            }
        }
        let step = self
            .capture
            .step(&mut self.transport, &mut self.sink, &mut self.buffer_paused)
            .await
            .map_err(recovery)?;
        let mut captured = std::mem::take(&mut self.sink.0);
        if let source::Step::Durable { end_lsn, stop } = step {
            captured.durable_lsn = Some(end_lsn);
            captured.stop = stop.is_some();
        }
        Ok(captured)
    }
    async fn publish(&mut self, high: u64, rows: AssembledTxn) -> Result<DurableManifestRef> {
        self.capture
            .append_chunk_rows(high, rows)
            .await
            .map_err(recovery)
    }
}

/// Qualified row decoder uses the same mappings/type identities as capture and
/// apply. The existing chunk SQL performs the source's actual index ordering.
pub struct PgChunks {
    pub admin: Client,
    pub reader: Client,
    pub tables: Vec<pg::catalog::TableCatalog>,
}
impl PgChunks {
    fn catalog<'a>(
        tables: &'a [pg::catalog::TableCatalog],
        plan: &ChunkPlan,
    ) -> Result<&'a pg::catalog::TableCatalog> {
        tables
            .iter()
            .find(|table| table.key == plan.table)
            .ok_or_else(|| recovery("unqualified chunk table"))
    }
    /// Preview the exact qualified projection used by snapshot reads.
    pub fn chunk_query(
        tables: &[pg::catalog::TableCatalog],
        plan: &ChunkPlan,
        chunk: &ChunkRange,
    ) -> Result<pg::chunk::ChunkQuery> {
        Self::catalog(tables, plan)?
            .chunk_query(chunk)
            .map_err(recovery)
    }
}
impl ChunkDatabase for PgChunks {
    async fn emit(&mut self, marker: Watermark) -> Result<()> {
        marker.emit(&self.admin).await.map_err(recovery)
    }
    async fn read(&mut self, plan: &ChunkPlan, chunk: &ChunkRange) -> Result<Vec<Row>> {
        let query = Self::chunk_query(&self.tables, plan, chunk)?;
        let table = Self::catalog(&self.tables, plan)?;
        query
            .execute(&mut self.reader)
            .await
            .map_err(recovery)?
            .into_iter()
            .map(table.decoder())
            .collect::<pg::chunk::Result<Vec<_>>>()
            .map_err(recovery)
    }
}

/// Planning faults remain separate from source query/transport failures. Neither
/// permits a fallback classification. Infrastructure details are never exposed.
#[derive(Debug)]
pub enum PgPlannerError {
    Apply(apply::ApplyError),
    Infrastructure,
}
impl std::fmt::Display for PgPlannerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Apply(error) => error.fmt(f),
            Self::Infrastructure => f.write_str("apply planner infrastructure failure"),
        }
    }
}
impl std::error::Error for PgPlannerError {}
impl From<apply::ApplyError> for PgPlannerError {
    fn from(error: apply::ApplyError) -> Self {
        Self::Apply(error)
    }
}
impl From<std::io::Error> for PgPlannerError {
    fn from(_: std::io::Error) -> Self {
        Self::Infrastructure
    }
}
impl From<tokio_postgres::Error> for PgPlannerError {
    fn from(_: tokio_postgres::Error) -> Self {
        Self::Infrastructure
    }
}
pub type PgPlanFuture<'a> = std::pin::Pin<
    Box<
        dyn Future<Output = std::result::Result<apply::TransactionPlan, PgPlannerError>>
            + Send
            + 'a,
    >,
>;
/// A planner may borrow its dedicated source connection and one immutable
/// materialized snapshot across awaits. A cancelled planning future is dropped;
/// reconstruct this planner/applier from durable state before using it again.
pub trait PgTransactionPlanner: Send {
    fn plan<'a>(
        &'a mut self,
        transaction: &'a AssembledTxn,
        mseq: u64,
        state: &'a Materialized,
    ) -> PgPlanFuture<'a>;
}
/// Existing synchronous planners and constructors remain compatible.
impl<P> PgTransactionPlanner for P
where
    P: FnMut(
            &AssembledTxn,
            u64,
            &Materialized,
        ) -> std::result::Result<apply::TransactionPlan, apply::ApplyError>
        + Send,
{
    fn plan<'a>(
        &'a mut self,
        transaction: &'a AssembledTxn,
        mseq: u64,
        state: &'a Materialized,
    ) -> PgPlanFuture<'a> {
        Box::pin(std::future::ready(
            self(transaction, mseq, state).map_err(Into::into),
        ))
    }
}

/// The planner can use pg::apply::plan_transaction with the caller's
/// qualified range classifier. Classify against this transaction's position:
/// journal Done at H must NOT make an unapplied transaction before H Completed.
/// In particular never compare canonical key bytes as PostgreSQL sort keys.
pub struct PgApplier<D, P> {
    pub reader: ReaderLoop,
    pub destination: D,
    pub plan: P,
    stream: String,
}
impl<D, P> PgApplier<D, P> {
    pub fn new(reader: BufferReader, stream: String, destination: D, plan: P) -> Self {
        Self {
            reader: ReaderLoop::new(reader, stream.clone()),
            destination,
            plan,
            stream,
        }
    }
}
impl<D: Destination, P: PgTransactionPlanner> Applier for PgApplier<D, P> {
    async fn step(&mut self, runtime: &Runtime) -> Result<bool> {
        self.planned_step(runtime, None).await
    }
    async fn controlled_step(
        &mut self,
        runtime: &Runtime,
        stop: watch::Receiver<bool>,
    ) -> Result<bool> {
        self.planned_step(runtime, Some(stop)).await
    }
}
impl<D: Destination, P: PgTransactionPlanner> PgApplier<D, P> {
    async fn planned_step(
        &mut self,
        runtime: &Runtime,
        stop: Option<watch::Receiver<bool>>,
    ) -> Result<bool> {
        if stop.as_ref().is_some_and(|s| *s.borrow()) {
            return Ok(false);
        }
        runtime
            .snapshot()
            .operational_schema
            .require_clear()
            .map_err(recovery)?;
        let transaction = std::sync::Mutex::new(None);
        let stream = &self.stream;
        let applied = std::sync::Mutex::new(None);
        let receipt = &applied;
        let mut handoff = PlanningDestination {
            destination: &mut self.destination,
            planner: &mut self.plan,
            transaction: &transaction,
            runtime,
            stop,
            stopped: false,
        };
        let result = self
            .reader
            .step(
                &mut handoff,
                |txn, mseq| {
                    *receipt.lock().expect("apply position poisoned") =
                        Some((Box::new(txn.clone()), mseq));
                    *transaction.lock().expect("planning transaction poisoned") = Some(txn.clone());
                    // ReaderLoop authenticates this envelope and supplies its
                    // exact evidence/progress write. It is never sent to SQL.
                    let placeholder = apply::Statement {
                        sql: String::new(),
                        parameters: vec![],
                        row_count: apply::RowCount::Any,
                        if_previous_missing: false,
                    };
                    Ok(apply::TransactionPlan {
                        source_origin: txn.source_origin.clone(),
                        session_setup: placeholder.clone(),
                        statements: vec![placeholder],
                        progress: apply::Progress {
                            stream: stream.clone(),
                            mseq,
                            txn: txn.txn.clone(),
                            lsn: u64::from_be_bytes(
                                txn.commit_position.bytes().try_into().map_err(|_| {
                                    apply::ApplyError::Invalid(
                                        "PG commit position must be an eight-byte LSN",
                                    )
                                })?,
                            ),
                            digest: txn.digest,
                            evidence: None,
                        },
                    })
                },
                |checkpoint| async move {
                    // ReaderLoop invokes this only after the durable Applied receipt.
                    let (transaction, mseq) = receipt
                        .lock()
                        .expect("apply position poisoned")
                        .clone()
                        .expect("planned position");
                    Box::pin(runtime.applied_final_marker(&transaction, mseq, &checkpoint))
                        .await
                        .map_err(|_| executor::Error::Checkpoint)?;
                    let applied = transaction.commit_position.clone();
                    let position = checkpoint.next_mseq.to_string();
                    let marker = encode_reader_checkpoint(&checkpoint)
                        .map_err(|_| executor::Error::Checkpoint)?;
                    runtime
                        .marker("applied", marker, position)
                        .await
                        .map_err(|_| executor::Error::Checkpoint)?;
                    runtime.6.applied(&applied, &checkpoint);
                    runtime.7.applied(&applied, &checkpoint);
                    runtime.queue_applied(applied, checkpoint).await;
                    Ok(())
                },
            )
            .await;
        if handoff.stopped {
            // ReaderLoop is intentionally poisoned after consuming a record.
            // Restart from the last durable checkpoint, never its live cursor.
            return Ok(false);
        }
        if result.is_err() {
            runtime.invalidate_final_marker_capture_gap();
        }
        result.map_err(|error| match error {
            executor::Error::Checkpoint if runtime.pause_reason().is_some() => {
                EnginePauseReason::CoordinatorJournalUnavailable.error()
            }
            error => recovery(error),
        })
    }
}

/// Async preparation at ReaderLoop's final handoff keeps its private identity,
/// manifest evidence, equal-H and checkpoint-before-receipt logic authoritative.
struct PlanningDestination<'a, D, P> {
    destination: &'a mut D,
    planner: &'a mut P,
    transaction: &'a std::sync::Mutex<Option<AssembledTxn>>,
    runtime: &'a Runtime,
    stop: Option<watch::Receiver<bool>>,
    stopped: bool,
}
#[async_trait::async_trait]
impl<D: Destination, P: PgTransactionPlanner> Destination for PlanningDestination<'_, D, P> {
    async fn apply(
        &mut self,
        envelope: &apply::TransactionPlan,
    ) -> executor::Result<apply::CommitDecision> {
        let txn = self
            .transaction
            .lock()
            .expect("planning transaction poisoned")
            .take()
            .ok_or(apply::ApplyError::Invalid("missing planning transaction"))?;
        let snapshot = self.runtime.snapshot();
        if snapshot.operational_schema.require_clear().is_err() {
            return Err(executor::Error::Blocked);
        }
        super::final_marker::transaction_marker(&txn).map_err(|_| {
            executor::Error::Apply(apply::ApplyError::Invalid(
                "unknown captured control payload",
            ))
        })?;
        if self
            .stop
            .as_ref()
            .is_some_and(|s| *s.borrow() || s.has_changed().is_err())
        {
            self.stopped = true;
            return Err(executor::Error::Blocked);
        }
        let prepare = self.planner.plan(&txn, envelope.progress.mseq, &snapshot);
        let stopped = async {
            match &mut self.stop {
                Some(stop) => cancelled(stop).await,
                None => std::future::pending::<()>().await,
            }
        };
        let planned = tokio::select! {
            biased;
            _ = stopped => { self.stopped = true; return Err(executor::Error::Blocked); },
            planned = prepare => planned,
        };
        // A ready plan must not outrun a simultaneous stop notification.
        if self
            .stop
            .as_ref()
            .is_some_and(|s| *s.borrow() || s.has_changed().is_err())
        {
            self.stopped = true;
            return Err(executor::Error::Blocked);
        }
        let mut plan = planned.map_err(|error| match error {
            PgPlannerError::Apply(error) => executor::Error::Apply(error),
            PgPlannerError::Infrastructure => executor::Error::Database,
        })?;
        let p = &plan.progress;
        let e = &envelope.progress;
        if p.stream != e.stream
            || p.mseq != e.mseq
            || p.txn != e.txn
            || p.digest != e.digest
            || p.lsn != e.lsn
        {
            return Err(apply::ApplyError::Invalid("reader plan identity").into());
        }
        *plan
            .statements
            .last_mut()
            .ok_or(apply::ApplyError::Invalid("missing progress write"))? = envelope
            .statements
            .last()
            .expect("reader progress write")
            .clone();
        plan.progress = envelope.progress.clone();
        // From this point onward cancellation must drain, including persistence
        // of the reader checkpoint and receipt retention in ReaderLoop::step.
        if self
            .runtime
            .snapshot()
            .operational_schema
            .require_clear()
            .is_err()
        {
            return Err(executor::Error::Blocked);
        }
        self.destination.apply(&plan).await
    }
    async fn retain_after_checkpoint(
        &mut self,
        checkpoint: &ReaderCheckpoint,
    ) -> executor::Result<()> {
        self.destination.retain_after_checkpoint(checkpoint).await
    }
}

// ReaderCheckpoint deliberately has no serde contract. This engine-owned v2
// marker keeps its entire predecessor boundary (including equal-H consumption),
// and BufferReader::new performs the existing semantic validation on recovery.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReaderMarker {
    version: u16,
    next_mseq: u64,
    prev_digest: ContentDigest,
    last_epoch: u64,
    position: Option<(String, Vec<u8>)>,
    source: Option<(String, String, String, String, String, bool)>,
}
pub fn encode_reader_checkpoint(c: &ReaderCheckpoint) -> Result<String> {
    Ok(serde_json::to_string(&ReaderMarker {
        version: c.version,
        next_mseq: c.next_mseq,
        prev_digest: c.prev_digest,
        last_epoch: c.last_epoch,
        position: c
            .last_position
            .as_ref()
            .map(|p| (p.order_domain().into(), p.bytes().to_vec())),
        source: c.last_record.as_ref().map(|r| {
            (
                r.source.engine.clone(),
                r.source.system_id.clone(),
                r.source.lineage.clone(),
                r.source.db_name.clone(),
                r.source.slot.clone(),
                r.chunk_rows,
            )
        }),
    })?)
}
pub fn decode_reader_checkpoint(marker: &str) -> Result<ReaderCheckpoint> {
    let m: ReaderMarker = serde_json::from_str(marker)?;
    if m.version != format::CHECKPOINT_VERSION {
        return Err(recovery("obsolete reader marker"));
    }
    Ok(ReaderCheckpoint {
        version: m.version,
        next_mseq: m.next_mseq,
        prev_digest: m.prev_digest,
        last_epoch: m.last_epoch,
        last_position: m.position.map(|(d, b)| Position::new(d, b)),
        last_record: m
            .source
            .map(|(engine, system_id, lineage, db_name, slot, chunk_rows)| {
                format::RecordBoundary {
                    source: SourceIdentity {
                        engine,
                        system_id,
                        lineage,
                        db_name,
                        slot,
                    },
                    chunk_rows,
                }
            }),
    })
}

/// Make initial capture from a qualified writer/resolver after Engine::start.
#[allow(clippy::too_many_arguments)]
pub fn capture<R: source::TypeResolver>(
    identity: SourceIdentity,
    domain: String,
    durable_lsn: u64,
    writer: crate::buffer::writer::BufferWriter,
    resolver: R,
    installation: String,
    schemas: SchemaVersions,
) -> Result<source::Capture<R>> {
    source::Capture::new(
        identity,
        domain,
        durable_lsn,
        writer,
        resolver,
        installation,
        schemas,
        pg::chunk::MESSAGE_PREFIX.into(),
    )
    .map_err(recovery)
}
