//! MySQL engine adapter. No binlog acknowledgement is sent to the source.
//! Receipt-marked single-writer apply transactions are excluded before mapping.
use super::{
    assembly::{Assembler, AssemblerOutput},
    binlog::{MysqlEvent, PositionedEvent},
    client::{MysqlConfig, MysqlError, Result as MysqlResult},
    control,
    ddl::{DdlImpact, classify},
    position::{MysqlOrder, MysqlPosition},
    resume,
    schema::{TableDescriptor, TableMapChange, compare_table_map, load_descriptors},
};
use crate::{
    buffer::writer::{BufferWriter, DurableManifestRef, WriterCheckpoint},
    coordinator::{Error, Result},
    engine::{
        Capture, Captured, CapturedMarker, ConnectorSchemaStop, SchemaBoundary, SchemaState,
        SchemaStopKind,
    },
    event::{AssembledTxn, Position, PositionOrder, Row, SourceIdentity},
    mapping::mysql::{MysqlPgColumn, plan_column, to_pg_opaque},
};
use futures::{Stream, StreamExt};
use mysql_async::prelude::Queryable;
use std::{
    collections::{BTreeMap, VecDeque},
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};

fn recovery(e: impl std::fmt::Display) -> Error {
    Error::RecoveryRequired(e.to_string())
}

/// The stop details remain available after the boundary batch is delivered.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CaptureStopReason {
    Ddl {
        tables: Vec<(String, String)>,
        statement: String,
    },
    UnknownDdl {
        statement: String,
        reason: String,
    },
    TableMap {
        schema: String,
        table: String,
        change: TableMapChange,
    },
}
impl std::fmt::Display for CaptureStopReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for CaptureStopReason {}

#[derive(Clone, Debug)]
pub struct ReconnectPolicy {
    pub attempts: usize,
    pub initial_delay: Duration,
    pub max_delay: Duration,
    pub budget: Duration,
}
impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self {
            attempts: 5,
            initial_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(8),
            budget: Duration::from_secs(30),
        }
    }
}
struct RetryState {
    deadline: tokio::time::Instant,
    attempts: usize,
    delay: Duration,
}

pub struct MysqlCapture {
    evidence: Option<crate::verify::mysql::input::CaptureEvidenceGuard>,
    descriptors: Vec<TableDescriptor>,
    lower_case_table_names: u8,
    stop_reason: Option<CaptureStopReason>,
    stream: Pin<Box<dyn Stream<Item = MysqlResult<PositionedEvent>> + Send>>,
    assembler: Assembler,
    plans: BTreeMap<String, Vec<MysqlPgColumn>>,
    writer: BufferWriter,
    source: SourceIdentity,
    order: Arc<MysqlOrder>,
    outputs: VecDeque<AssemblerOutput>,
    pending: Option<Captured>,
    pending_chunk: Option<AssembledTxn>,
    boundary: Option<AssembledTxn>,
    safe: Position,
    // Only step's returned durable_position is eligible for reconnect. `safe`
    // can contain completed unselected progress not yet returned to the engine.
    delivered: Position,
    reconnect: Option<(MysqlConfig, u32)>,
    reconnect_policy: ReconnectPolicy,
    retry: Option<RetryState>,
    max_row_bytes: usize,
    last_progress: Instant,
    stopped: bool,
    failed: bool,
}
impl MysqlCapture {
    /// Register once, before driving capture. The input must be polled alongside
    /// capture; both share a bounded queue, never the engine's delivery channel.
    pub fn evidence_input(
        &mut self,
        config: MysqlConfig,
        max_bytes: usize,
        max_commits: usize,
    ) -> MysqlResult<crate::verify::mysql::input::MysqlCaptureInput> {
        if self.evidence.is_some()
            || self.pending.is_some()
            || self.failed
            || self.stopped
            || max_bytes == 0
            || max_commits == 0
        {
            return Err(MysqlError::Invalid("invalid evidence subscription"));
        }
        let keys = self
            .descriptors
            .iter()
            .map(|d| (format!("{}.{}", d.schema, d.table), d.primary_key.clone()))
            .collect();
        let capture = crate::verify::mysql::input::CaptureEvidence::new(
            self.safe.clone(),
            self.order.clone(),
            keys,
        );
        self.evidence = Some(crate::verify::mysql::input::CaptureEvidenceGuard(
            capture.clone(),
        ));
        Ok(crate::verify::mysql::input::MysqlCaptureInput {
            source: self.source.clone(),
            capture,
            config,
            id: uuid::Uuid::new_v4().to_string(),
            attempt: 0,
            max_bytes,
            max_commits,
        })
    }
    /// `source.engine` is mysql; system_id is the caller's stable cluster ID,
    /// lineage its replication incarnation, db_name the selected database/set,
    /// stream the configured capture name. Keep all five stable across failover;
    /// system_id must NOT be the current server_uuid (GTID supersets can move).
    /// Restore the journal-bound SourceIdentity before calling this constructor;
    /// the engine validates buffered progress identity before resuming capture.
    /// Descriptors and M7a plans are loaded before opening the dump connection.
    pub async fn new(
        config: &MysqlConfig,
        source: SourceIdentity,
        start: MysqlPosition,
        tables: &[(String, String)],
        writer: BufferWriter,
        server_id: u32,
    ) -> MysqlResult<Self> {
        validate_source(&source)?;
        let mut client = config.connect().await?;
        resume::check(&start, &client.status().await?)?;
        let lower_case_table_names: u8 = client
            .conn
            .query_first("SELECT @@lower_case_table_names")
            .await?
            .ok_or(MysqlError::Invalid("missing lower_case_table_names"))?;
        let descriptors = load_descriptors(&mut client.conn, tables).await?;
        let stream = client
            .open_stream(&start, server_id, Duration::from_secs(1))
            .await?;
        let mut capture = Self::from_stream_with_case_mode(
            source,
            start,
            descriptors,
            writer,
            stream,
            config.max_decompressed_bytes,
            lower_case_table_names,
        )?;
        capture.reconnect = Some((config.clone(), server_id));
        Ok(capture)
    }

    /// Replay entry point for recorded positioned binlogs, with the same writer
    /// and mapping path as live capture. Control descriptors are adapter-owned.
    pub fn from_stream(
        source: SourceIdentity,
        start: MysqlPosition,
        descriptors: Vec<TableDescriptor>,
        writer: BufferWriter,
        stream: impl Stream<Item = MysqlResult<PositionedEvent>> + Send + 'static,
        max_row_bytes: usize,
    ) -> MysqlResult<Self> {
        Self::from_stream_with_case_mode(
            source,
            start,
            descriptors,
            writer,
            stream,
            max_row_bytes,
            0,
        )
    }

    /// Recorded streams must supply the source's case mode when it is not zero.
    pub fn from_stream_with_case_mode(
        source: SourceIdentity,
        start: MysqlPosition,
        mut descriptors: Vec<TableDescriptor>,
        writer: BufferWriter,
        stream: impl Stream<Item = MysqlResult<PositionedEvent>> + Send + 'static,
        max_row_bytes: usize,
        lower_case_table_names: u8,
    ) -> MysqlResult<Self> {
        if lower_case_table_names > 2 {
            return Err(MysqlError::Invalid("lower_case_table_names"));
        }
        validate_source(&source)?;
        let gtid = matches!(start, MysqlPosition::Gtid { .. });
        let order = Arc::new(match &start {
            MysqlPosition::Gtid { .. } => MysqlOrder::gtid(&source.system_id)?,
            MysqlPosition::File { server_uuid, .. } => MysqlOrder::file(server_uuid)?,
        });
        let safe = Position::new(order.domain(), start.encode()?);
        let mut plans = BTreeMap::new();
        for d in &descriptors {
            if d.schema == "_skedaddle"
                || (lower_case_table_names != 0 && d.schema.eq_ignore_ascii_case("_skedaddle"))
            {
                return Err(MysqlError::Control("control selection is reserved"));
            }
            plans.insert(
                format!("{}.{}", d.schema, d.table),
                d.columns
                    .iter()
                    .map(plan_column)
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(MysqlError::Mapping)?,
            );
        }
        let selected = descriptors.clone();
        descriptors.push(control::descriptor());
        descriptors.extend(control::apply_descriptors());
        let assembler = Assembler::new(source.clone(), descriptors, start, gtid, max_row_bytes)?;
        Ok(Self {
            evidence: None,
            descriptors: selected,
            lower_case_table_names,
            stop_reason: None,
            stream: Box::pin(stream),
            assembler,
            plans,
            writer,
            source,
            order,
            outputs: VecDeque::new(),
            pending: None,
            pending_chunk: None,
            boundary: None,
            delivered: safe.clone(),
            safe,
            reconnect: None,
            reconnect_policy: ReconnectPolicy::default(),
            retry: None,
            max_row_bytes,
            last_progress: Instant::now(),
            stopped: false,
            failed: false,
        })
    }

    pub fn with_reconnect_policy(mut self, policy: ReconnectPolicy) -> MysqlResult<Self> {
        if policy.attempts == 0
            || policy.initial_delay.is_zero()
            || policy.max_delay < policy.initial_delay
            || policy.budget.is_zero()
        {
            return Err(MysqlError::Invalid("reconnect policy"));
        }
        self.reconnect_policy = policy;
        Ok(self)
    }

    async fn reconnect(&mut self, mut last: MysqlError) -> MysqlResult<()> {
        if let Some(e) = &self.evidence {
            e.invalidate(crate::verify::mysql::evidence::ContinuityEvent::Reconnect);
        }
        let Some((config, server_id)) = self.reconnect.clone() else {
            return Err(match last {
                MysqlError::StreamEnded => {
                    MysqlError::Invalid("binlog stream ended; reconnect required")
                }
                error => error,
            });
        };
        let start = MysqlPosition::decode(self.delivered.bytes())?;
        let mut descriptors = self.descriptors.clone();
        descriptors.push(control::descriptor());
        descriptors.extend(control::apply_descriptors());
        self.assembler = Assembler::new(
            self.source.clone(),
            descriptors,
            start.clone(),
            matches!(start, MysqlPosition::Gtid { .. }),
            self.max_row_bytes,
        )?;
        self.outputs.clear();
        self.safe = self.delivered.clone();
        self.boundary = None;
        self.stream = Box::pin(futures::stream::empty());
        // Preserve the budget across cancellation and connections that close
        // before producing any event. A successful event resets it below.
        self.retry.get_or_insert_with(|| RetryState {
            deadline: tokio::time::Instant::now() + self.reconnect_policy.budget,
            attempts: 0,
            delay: self.reconnect_policy.initial_delay,
        });
        while self.retry.as_ref().unwrap().attempts < self.reconnect_policy.attempts {
            let retry = self.retry.as_mut().unwrap();
            let delay = retry.delay;
            retry.attempts += 1;
            retry.delay = delay.saturating_mul(2).min(self.reconnect_policy.max_delay);
            let retry = async {
                tokio::time::sleep(delay).await;
                let client = config.connect().await?;
                // open_stream observes status and checks the durable boundary on
                // every attempt. It cannot fall back to a newer status offset.
                client
                    .open_stream(&start, server_id, Duration::from_secs(1))
                    .await
            };
            match tokio::time::timeout_at(self.retry.as_ref().unwrap().deadline, retry).await {
                Ok(Ok(stream)) => {
                    self.stream = Box::pin(stream);
                    return Ok(());
                }
                Ok(Err(e)) if e.is_transient() => last = e,
                Ok(Err(e)) => return Err(e),
                Err(_) => {
                    last = MysqlError::Io(std::io::ErrorKind::TimedOut.into());
                    break;
                }
            }
        }
        Err(MysqlError::ReconnectExhausted {
            attempts: self.retry.as_ref().unwrap().attempts,
            last: Box::new(last),
        })
    }

    pub fn stop_reason(&self) -> Option<&CaptureStopReason> {
        self.stop_reason.as_ref()
    }

    fn schema_snapshot(&self) -> SchemaState {
        let blocked_boundary = self.stop_reason.as_ref().map(|reason| {
            let (kind, table, diagnostic) = match reason {
                CaptureStopReason::Ddl { tables, statement } => (
                    SchemaStopKind::Ddl,
                    (tables.len() == 1).then(|| format!("{}.{}", tables[0].0, tables[0].1)),
                    statement.clone(),
                ),
                CaptureStopReason::UnknownDdl { statement, reason } => (
                    SchemaStopKind::UnknownDdl,
                    None,
                    format!("{statement}: {reason}"),
                ),
                CaptureStopReason::TableMap {
                    schema,
                    table,
                    change,
                } => (
                    SchemaStopKind::TableMapChanged,
                    Some(format!("{schema}.{table}")),
                    format!("{change:?}"),
                ),
            };
            SchemaBoundary::Connector(ConnectorSchemaStop {
                connector: "mysql".into(),
                kind,
                table,
                diagnostic,
                // DDL uses its completed query boundary; TABLE_MAP retains the
                // last safe commit, never an in-flight transaction coordinate.
                position: self.safe.clone(),
            })
        });
        SchemaState {
            blocked_boundary,
            unknown_schema_job: false,
        }
    }

    fn stop(&mut self, reason: CaptureStopReason) -> Captured {
        if let Some(e) = &self.evidence {
            e.invalidate(crate::verify::mysql::evidence::ContinuityEvent::CaptureStop);
        }
        self.stopped = true;
        self.stop_reason = Some(reason);
        self.boundary = None;
        Captured {
            durable_position: Some(self.safe.clone()),
            stop: true,
            ..Default::default()
        }
    }

    // Inspect compressed payloads too, before any changed rows can be decoded
    // against stale descriptors. Never promote an in-flight TABLE_MAP position.
    fn table_map_change(&self, event: &mut MysqlEvent) -> Option<CaptureStopReason> {
        match event {
            MysqlEvent::TransactionPayload(events) => {
                events.iter_mut().find_map(|e| self.table_map_change(e))
            }
            MysqlEvent::TableMap(map) => self.descriptors.iter().find_map(|d| {
                let equal = |a: &[u8], b: &str| {
                    if self.lower_case_table_names == 0 {
                        a == b.as_bytes()
                    } else {
                        String::from_utf8_lossy(a).to_lowercase() == b.to_lowercase()
                    }
                };
                if !equal(&map.schema, &d.schema) || !equal(&map.table, &d.table) {
                    return None;
                }
                if let Err(change) = compare_table_map(d, map) {
                    return Some(CaptureStopReason::TableMap {
                        schema: d.schema.clone(),
                        table: d.table.clone(),
                        change,
                    });
                }
                // Match the assembler's descriptor key under the source's case
                // rules, retaining the descriptor spelling in emitted events.
                map.schema = d.schema.as_bytes().to_vec();
                map.table = d.table.as_bytes().to_vec();
                None
            }),
            _ => None,
        }
    }

    fn mapped(&self, txn: AssembledTxn) -> MysqlResult<AssembledTxn> {
        let mut events = txn.events;
        for event in &mut events {
            let plans = event
                .table
                .as_ref()
                .and_then(|t| self.plans.get(t))
                .ok_or(MysqlError::Assembly("missing selected table plan"))?;
            let map = |row: &mut Row| -> MysqlResult<()> {
                for (name, value) in row.iter_mut() {
                    let plan = plans
                        .iter()
                        .find(|p| p.source.name == *name)
                        .ok_or(MysqlError::Assembly("missing selected column plan"))?;
                    *value = to_pg_opaque(plan, value).map_err(MysqlError::Mapping)?;
                }
                Ok(())
            };
            map(&mut event.key)?;
            for row in event.before.iter_mut().chain(event.after.iter_mut()) {
                map(row)?;
            }
        }
        Ok(AssembledTxn::new(
            txn.source,
            txn.txn,
            txn.commit_position,
            txn.commit_time,
            events,
        )?)
    }

    async fn durable(&mut self) -> Result<Captured> {
        let txn = self
            .pending
            .as_ref()
            .and_then(|p| p.transaction.as_ref())
            .expect("pending transaction");
        // Retain pending across await/cancellation. Repeated append has checked
        // redelivery semantics, and seal retains in-flight manifest publication.
        self.writer
            .append(txn.clone(), Instant::now())
            .await
            .map_err(recovery)?;
        self.writer.seal().await.map_err(recovery)?;
        let checkpoint = self.writer.checkpoint().map_err(recovery)?;
        if !checkpoint.redelivery.iter().any(|r| {
            r.source == txn.source
                && r.txn == txn.txn
                && r.digest == txn.digest
                && r.position == txn.commit_position
                && r.manifest.is_some()
        }) {
            return Err(recovery(
                "MySQL writer did not report transaction manifest-durable",
            ));
        }
        let batch = self.pending.take().expect("pending transaction");
        self.safe = txn_position(&batch)?.clone();
        self.boundary = batch.transaction.clone();
        self.last_progress = Instant::now();
        Ok(batch)
    }

    async fn next_step(&mut self) -> Result<Captured> {
        if self.failed || self.stopped {
            return Err(recovery(self.stop_reason.as_ref().map_or_else(
                || "MySQL capture stopped; reconstruct and replay".into(),
                |reason| reason.to_string(),
            )));
        }
        if self.pending_chunk.is_some() {
            return Err(recovery(
                "retry pending MySQL ChunkRows publication before step",
            ));
        }
        loop {
            if self.pending.is_some() {
                return self.durable().await;
            }
            if let Some(output) = self.outputs.pop_front() {
                match output {
                    AssemblerOutput::Txn { txn, control_rows } => {
                        let mut watermarks = Vec::new();
                        let mut verify_markers = Vec::new();
                        let mut own_apply = false;
                        let mut apply_control = false;
                        for row in &control_rows {
                            if let Some(marker) = control::decode_apply(row)? {
                                own_apply |= marker;
                                apply_control = true;
                            } else if control::is_verify_marker(row)? {
                                verify_markers.push(
                                    control::decode_verify_marker(row)?.expect("verified route"),
                                );
                            } else if let Some(marker) = control::decode_watermark(row)? {
                                watermarks.push(marker);
                            }
                        }
                        if let Some(e) = &self.evidence {
                            match self.mapped(txn.clone()) {
                                Ok(mapped) => e.commit(
                                    &mapped.commit_position,
                                    &mapped.events,
                                    &verify_markers,
                                ),
                                Err(_) => e.invalidate(
                                    crate::verify::mysql::evidence::ContinuityEvent::Gap,
                                ),
                            }
                        }
                        // Apply is single-writer: the receipt authenticates the entire
                        // InnoDB transaction, even if it includes additional tables.
                        // Validate all controls first, then skip mapping and chunk markers.
                        if own_apply
                            || (apply_control && txn.events.is_empty() && watermarks.is_empty())
                        {
                            self.safe = txn.commit_position;
                            self.boundary = None;
                            self.last_progress = Instant::now();
                            return Ok(Captured {
                                durable_position: Some(self.safe.clone()),
                                ..Default::default()
                            });
                        }
                        let txn = self.mapped(txn)?;
                        let watermarks = watermarks
                            .into_iter()
                            .map(|marker| CapturedMarker::Chunk {
                                marker,
                                commit: txn.commit_position.clone(),
                                committed_events: txn.events.clone(),
                            })
                            .collect();
                        self.pending = Some(Captured {
                            durable_position: Some(txn.commit_position.clone()),
                            transaction: Some(txn),
                            watermarks,
                            stop: false,
                        });
                    }
                    AssemblerOutput::Progress(p) => {
                        self.safe = Position::new(
                            self.order.domain(),
                            p.encode().map_err(MysqlError::from)?,
                        );
                        self.boundary = None;
                        if let Some(e) = &self.evidence {
                            e.commit(&self.safe, &[], &[]);
                        }
                        if self.last_progress.elapsed() >= Duration::from_secs(1) {
                            self.last_progress = Instant::now();
                            return Ok(Captured {
                                durable_position: Some(self.safe.clone()),
                                ..Default::default()
                            });
                        }
                    }
                    AssemblerOutput::Ddl {
                        schema,
                        sql,
                        position,
                    } => {
                        let selected = self
                            .descriptors
                            .iter()
                            .map(|d| (d.schema.clone(), d.table.clone()))
                            .collect::<Vec<_>>();
                        let statement = String::from_utf8_lossy(&sql).into_owned();
                        let impact = match (std::str::from_utf8(&schema), std::str::from_utf8(&sql))
                        {
                            (Ok(schema), Ok(sql)) => {
                                classify(schema, sql, &selected, self.lower_case_table_names)
                            }
                            _ => DdlImpact::Unknown {
                                reason: format!("non-UTF-8 DDL; selected tables {selected:?}"),
                            },
                        };
                        self.safe = Position::new(
                            self.order.domain(),
                            position.encode().map_err(MysqlError::from)?,
                        );
                        self.boundary = None;
                        let reason = match impact {
                            DdlImpact::Selected { tables, statement } => {
                                CaptureStopReason::Ddl { tables, statement }
                            }
                            DdlImpact::Unknown { reason } => {
                                CaptureStopReason::UnknownDdl { statement, reason }
                            }
                            DdlImpact::Unselected => {
                                if let Some(e) = &self.evidence {
                                    e.commit(&self.safe, &[], &[]);
                                }
                                self.last_progress = Instant::now();
                                return Ok(Captured {
                                    durable_position: Some(self.safe.clone()),
                                    ..Default::default()
                                });
                            }
                        };
                        return Ok(self.stop(reason));
                    }
                }
                continue;
            }
            let mut event = match self
                .stream
                .next()
                .await
                .unwrap_or(Err(MysqlError::StreamEnded))
            {
                Ok(event) => {
                    self.retry = None;
                    event
                }
                Err(error) if error.is_transient() => {
                    self.reconnect(error).await?;
                    continue;
                }
                Err(error) => return Err(error.into()),
            };
            if let Some(reason) = self.table_map_change(&mut event.event) {
                return Ok(self.stop(reason));
            }
            let heartbeat = matches!(event.event, MysqlEvent::Heartbeat);
            self.outputs.extend(self.assembler.push(event)?);
            if heartbeat {
                // Heartbeat coordinates may lie inside undelivered transactions.
                // Only a completed assembler boundary can become restart progress.
                self.last_progress = Instant::now();
                return Ok(Captured {
                    durable_position: Some(self.safe.clone()),
                    ..Default::default()
                });
            }
        }
    }
}
fn validate_source(source: &SourceIdentity) -> MysqlResult<()> {
    if source.engine != "mysql"
        || [
            &source.system_id,
            &source.lineage,
            &source.db_name,
            &source.stream,
        ]
        .iter()
        .any(|s| s.is_empty())
    {
        return Err(MysqlError::Invalid("stable MySQL source identity required"));
    }
    Ok(())
}
fn txn_position(batch: &Captured) -> Result<&Position> {
    batch
        .durable_position
        .as_ref()
        .ok_or_else(|| recovery("MySQL batch missing durable position"))
}
impl Capture for MysqlCapture {
    fn schema_state(&self) -> Option<SchemaState> {
        Some(self.schema_snapshot())
    }

    async fn prepare_schema(
        &mut self,
        _journal: &crate::coordinator::journal::Journal,
        _runtime: &crate::engine::Runtime,
    ) -> Result<()> {
        // MySQL descriptors are qualified before the stream is opened. There
        // is no persisted MySQL schema baseline to restore in this adapter.
        Ok(())
    }

    fn position_order(&self) -> Arc<dyn PositionOrder + Send + Sync> {
        self.order.clone()
    }
    /// mysql-buffered-v1 binds the native restart position to this writer's
    /// manifest prefix (next_mseq, prev_digest, epoch), stable source and optional
    /// delivered transaction digest. Progress-only boundaries carry no txn.
    fn buffered_marker(&self, batch: &Captured) -> Result<String> {
        let position = txn_position(batch)?;
        // Heartbeat batches can have no transaction at an unchanged boundary.
        if position != &self.safe
            || (batch.transaction.is_some() && batch.transaction != self.boundary)
        {
            return Err(recovery(
                "MySQL buffered marker is not the delivered boundary",
            ));
        }
        let cp = self.writer.checkpoint().map_err(recovery)?;
        Ok(serde_json::json!({ "format": "mysql-buffered-v1",
            "source": [&self.source.engine, &self.source.system_id, &self.source.lineage,
                       &self.source.db_name, &self.source.stream],
            "position": crate::engine::encode_buffered_position(&self.source, position)?,
            "next_mseq": cp.next_mseq, "prev_digest": cp.prev_digest, "epoch": cp.last_epoch,
            "transaction_digest": batch.transaction.as_ref().map(|t| t.digest) })
        .to_string())
    }
    fn writer_checkpoint(&self) -> Result<Option<WriterCheckpoint>> {
        self.writer.checkpoint().map(Some).map_err(recovery)
    }
    async fn step(&mut self) -> Result<Captured> {
        let result = self.next_step().await;
        match &result {
            Ok(batch) => {
                if let Some(position) = &batch.durable_position {
                    self.delivered = position.clone();
                }
            }
            Err(_) => {
                self.failed = true;
                if let Some(e) = &self.evidence {
                    e.invalidate(crate::verify::mysql::evidence::ContinuityEvent::Gap);
                }
            }
        }
        result
    }
    async fn publish(&mut self, high: Position, rows: AssembledTxn) -> Result<DurableManifestRef> {
        if self.failed
            || self.stopped
            || self.pending.is_some()
            || rows.commit_position != high
            || rows.source != self.source
            || self
                .boundary
                .as_ref()
                .is_none_or(|b| b.commit_position != high)
        {
            return Err(recovery(
                "MySQL ChunkRows publication requires current durable commit",
            ));
        }
        if let Some(pending) = &self.pending_chunk {
            if pending != &rows {
                return Err(recovery("MySQL ChunkRows publication conflict"));
            }
        } else {
            crate::buffer::format::validate_chunk(&rows).map_err(recovery)?;
            self.pending_chunk = Some(rows);
        }
        let (_, reference) = self
            .writer
            .append_chunk_rows(
                self.pending_chunk.as_ref().expect("pending chunk").clone(),
                Instant::now(),
            )
            .await
            .map_err(recovery)?;
        self.pending_chunk = None;
        Ok(reference)
    }
}
#[cfg(test)]
mod tests;
