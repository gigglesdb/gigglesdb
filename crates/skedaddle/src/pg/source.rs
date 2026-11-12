//! Bounded source capture, without snapshot/chunk scheduling.
//!
//! Each committed transaction is sealed before reading another message. This
//! intentionally favors a simple durability boundary over segment batching.
//! Cancellation or a retryable writer failure retains the complete transaction;
//! drive `step` again on the SAME capture, or reconnect at the last durable ack.
//!
//! Use [`connect`] and [`reconnect`] for mapping-safe sessions. Replication
//! startup applies the fixed SET commands and reads back SHOW values on the
//! same wire; these helpers verify the observations before START_REPLICATION.

mod schema_journal;
mod values;
pub use super::replication::{REQUIRED_SETTINGS_SQL, SessionSettings};
pub use schema_journal::{MAX_SCHEMA_SNAPSHOT_BYTES, SchemaJournalBinding};
pub use values::{ColumnMapping, RelationMapper, TypeResolver};

use std::{collections::BTreeSet, fmt, future::Future, time::Instant};

use tokio::sync::watch;

use super::{
    assembly::{self, CompletedTransaction, MappedEvent, TransactionAssembler},
    chunk,
    ddl::{self, Classification, DdlRecord, SafeBoundaryStop, SchemaVersions},
    pgoutput::{CopyData, Decoder, Message},
    replication::{Checkpoint, ConnectionConfig, Lineage, ReplicationConnection, ReplicationError},
};
use crate::{
    buffer::writer::{self, BufferWriter, DurableManifestRef},
    event::{AssembledTxn, Event, EventKind, OpaqueValue, Row, SourceIdentity},
};

#[derive(Debug)]
pub enum Error {
    Replication(ReplicationError),
    Decode(super::pgoutput::DecodeError),
    Assembly(assembly::AssembleError),
    Mapping(crate::mapping::MappingError),
    Ddl(ddl::DdlError),
    Buffer(writer::Error),
    SchemaJournal(crate::coordinator::Error),
    RecoveryRequired(&'static str),
    AdmissionClosed,
    ChunkBoundaryPassed { requested: u64, committed: u64 },
    ChunkBoundaryUnavailable,
    ChunkPublicationPending,
    ChunkPublicationConflict,
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "source capture: {self:?}")
    }
}
impl std::error::Error for Error {}
macro_rules! conversion {
    ($from:ty, $variant:ident) => {
        impl From<$from> for Error {
            fn from(error: $from) -> Self {
                Self::$variant(error)
            }
        }
    };
}
conversion!(ReplicationError, Replication);
conversion!(super::pgoutput::DecodeError, Decode);
conversion!(assembly::AssembleError, Assembly);
conversion!(crate::mapping::MappingError, Mapping);
conversion!(ddl::DdlError, Ddl);
conversion!(writer::Error, Buffer);
pub type Result<T> = std::result::Result<T, Error>;

/// Run on every new session, before START_REPLICATION, using observations from
/// that session after applying REQUIRED_SETTINGS_SQL. A mismatch fails closed.
pub fn verify_session(
    expected: &Lineage,
    actual: &Lineage,
    settings: &SessionSettings,
) -> Result<()> {
    expected.check(actual)?;
    if settings.date_style != "ISO, MDY"
        || settings.interval_style != "postgres"
        || settings.time_zone != "UTC"
        || settings.extra_float_digits < 1
        || settings.client_encoding != "UTF8"
        || !settings.standard_conforming_strings
    {
        return Err(Error::RecoveryRequired(
            "source session mapping settings changed",
        ));
    }
    Ok(())
}

/// Open a replication session, apply mapping setup, and verify its SHOW values.
/// The returned connection is ready for slot creation or START_REPLICATION.
pub async fn connect(config: &ConnectionConfig) -> Result<ReplicationConnection> {
    let connection = ReplicationConnection::connect(config).await?;
    verify_connected(&connection, connection.lineage())?;
    Ok(connection)
}

/// Reapply and verify mapping setup and checkpoint lineage on a fresh session
/// before starting at the exact durable position. Never reuse old observations.
pub async fn reconnect(
    config: &ConnectionConfig,
    checkpoint: &Checkpoint,
    publications: &[String],
) -> Result<ReplicationConnection> {
    let mut connection = ReplicationConnection::connect(config).await?;
    resume(&mut connection, checkpoint, publications).await?;
    Ok(connection)
}

pub(super) fn verify_connected<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(
    connection: &ReplicationConnection<S>,
    expected: &Lineage,
) -> Result<()> {
    verify_session(
        expected,
        connection.lineage(),
        connection.session_settings(),
    )
}

pub(super) async fn resume<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(
    connection: &mut ReplicationConnection<S>,
    checkpoint: &Checkpoint,
    publications: &[String],
) -> Result<()> {
    verify_connected(connection, &checkpoint.lineage)?;
    connection
        .start(&checkpoint.slot, checkpoint.durable_lsn, publications)
        .await?;
    Ok(())
}

/// A byte-only replication transport. Implementations must retain partial reads
/// and writes across cancellation, and acknowledge only the supplied durable LSN.
pub trait Transport {
    fn next(&mut self, durable_lsn: u64) -> impl Future<Output = Result<Vec<u8>>>;
}
impl Transport for ReplicationConnection {
    async fn next(&mut self, durable_lsn: u64) -> Result<Vec<u8>> {
        Ok(ReplicationConnection::next(self, durable_lsn)
            .await?
            .payload()
            .to_vec())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Watermark {
    pub transactional: bool,
    pub message_lsn: u64,
    pub content: Vec<u8>,
    /// COMMIT LSN, never the logical-message LSN or transaction end LSN.
    pub commit_lsn: u64,
    /// Ordered events of the manifest-durable transaction carrying this marker.
    pub committed_events: Vec<Event>,
}

impl Watermark {
    pub fn message(&self) -> Message<'_> {
        Message::LogicalMessage {
            transactional: true,
            lsn: self.message_lsn,
            prefix: chunk::MESSAGE_PREFIX,
            content: &self.content,
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct StoredWatermark {
    message_lsn: u64,
    content: Vec<u8>,
}

/// Called only after manifest durability, in committed transaction order.
/// `committed` receives EVERY transaction (including intervening row changes),
/// before its watermark callbacks. Feed its events to Window before its markers.
/// A callback error poisons capture and requires reconnect/replay.
pub trait WatermarkSink {
    fn committed(&mut self, _transaction: &AssembledTxn) -> Result<()> {
        Ok(())
    }
    fn watermark(&mut self, watermark: Watermark) -> Result<()>;
}
impl<F: FnMut(Watermark) -> Result<()>> WatermarkSink for F {
    fn watermark(&mut self, watermark: Watermark) -> Result<()> {
        self(watermark)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Step {
    Message,
    Durable {
        end_lsn: u64,
        stop: Option<SafeBoundaryStop>,
    },
}

struct Pending {
    completed: CompletedTransaction,
    stop: Option<SafeBoundaryStop>,
    schema_changed: bool,
}

pub struct Capture<R> {
    decoder: Decoder,
    assembler: TransactionAssembler,
    mapper: RelationMapper<R>,
    writer: BufferWriter,
    schemas: SchemaVersions,
    installation: String,
    watermark_prefix: String,
    ddl: Vec<DdlRecord>,
    pending: Option<Pending>,
    boundary: Option<AssembledTxn>,
    pending_chunk: Option<AssembledTxn>,
    durable_lsn: u64,
    failed: bool,
    source: SourceIdentity,
    order_domain: String,
    schema_journal: Option<schema_journal::SchemaJournal>,
    restored_unknown: bool,
}

/// Read-only state of the synchronous schema classifier. An unclassified DDL
/// transaction or a failed capture cannot establish that no schema job exists.
#[derive(Clone, Debug)]
pub struct SchemaState {
    pub blocked_boundary: Option<SafeBoundaryStop>,
    pub unknown_schema_job: bool,
}

impl<R: TypeResolver> Capture<R> {
    /// Low-level entry point for transports opened by `source::connect` or
    /// `source::reconnect`, with a persisted, lineage-bound starting checkpoint.
    /// The writer must belong exclusively to this capture and have no pending
    /// transactions. Recovered durable history is allowed. watermark_prefix
    /// must be chunk::MESSAGE_PREFIX; DDL retains its own exact prefix.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        source: SourceIdentity,
        order_domain: String,
        durable_lsn: u64,
        writer: BufferWriter,
        resolver: R,
        installation: String,
        schemas: SchemaVersions,
        watermark_prefix: String,
    ) -> Result<Self> {
        if writer.pending_transactions() != 0
            || writer.is_blocked()
            || watermark_prefix != chunk::MESSAGE_PREFIX
        {
            return Err(Error::RecoveryRequired(
                "capture configuration or writer ownership",
            ));
        }
        // Global DDL continuity includes administration; mapped versions do not.
        // Restore each known table from the retained descriptor at this boundary.
        let mut mapper = RelationMapper::new(resolver, 0);
        mapper.set_mapped_versions(schemas.mapped_versions());
        Ok(Self {
            decoder: Decoder::default(),
            assembler: TransactionAssembler::new(
                source.clone(),
                order_domain.clone(),
                assembly::MAX_TRANSACTION_BYTES,
                assembly::MAX_TRANSACTION_EVENTS,
            )?,
            mapper,
            writer,
            schemas,
            installation,
            watermark_prefix,
            ddl: Vec::new(),
            pending: None,
            boundary: None,
            pending_chunk: None,
            durable_lsn,
            failed: false,
            source,
            order_domain,
            schema_journal: None,
            restored_unknown: false,
        })
    }

    pub fn durable_lsn(&self) -> u64 {
        self.durable_lsn
    }
    pub fn writer(&self) -> &BufferWriter {
        &self.writer
    }

    pub fn schema_state(&self) -> SchemaState {
        SchemaState {
            blocked_boundary: self.schemas.blocker().cloned(),
            unknown_schema_job: self.restored_unknown
                || self.failed
                || !self.ddl.is_empty()
                || self.schema_journal.as_ref().is_some_and(|j| !j.ready),
        }
    }

    /// Engine admission hook for D11b: invoke on every newly constructed capture,
    /// after writer recovery and BEFORE reconnecting/reading WAL. On success the
    /// durable_lsn, retained descriptors, mapped versions and sticky flags match
    /// the latest schema-changing transaction at or below that writer boundary.
    /// Missing required snapshot evidence fails closed. Keep this Capture across
    /// cancellation and retry this hook; no WAL
    /// is admitted while restore is incomplete. The initial descriptor baseline
    /// supplied to new() remains the caller's consistent-snapshot responsibility.
    pub async fn restore_schema(&mut self, binding: SchemaJournalBinding) -> Result<()> {
        if self.pending.is_some()
            || self.pending_chunk.is_some()
            || !self.ddl.is_empty()
            || self.boundary.is_some()
            || self.failed
        {
            return Err(Error::RecoveryRequired(
                "schema restore after capture started",
            ));
        }
        if self.schema_journal.is_none() {
            // Fail closed even if binding construction is cancelled or rejected.
            self.restored_unknown = true;
            self.schema_journal = Some(
                schema_journal::SchemaJournal::new(
                    binding,
                    &self.source,
                    &self.order_domain,
                    &self.installation,
                )
                .await?,
            );
        }
        let journal = self
            .schema_journal
            .as_mut()
            .expect("schema journal attached");
        journal.ready = false;
        let (schemas, lsn, unknown) = journal
            .restore(&self.writer.checkpoint()?, &self.schemas, self.durable_lsn)
            .await?;
        self.schemas = schemas;
        self.mapper
            .set_mapped_versions(self.schemas.mapped_versions());
        self.durable_lsn = lsn;
        self.restored_unknown = unknown;
        journal.ready = true;
        Ok(())
    }

    /// Complete retained state for inspection, without exposing mutable capture
    /// classifier ownership. Its bytes require the journal/boundary authentication
    /// performed by restore_schema before use as restart evidence.
    pub fn schema_snapshot(&self) -> ddl::SchemaVersionsSnapshot {
        self.schemas.snapshot()
    }

    fn schema_ready(&self) -> Result<()> {
        if self.schema_journal.as_ref().is_some_and(|j| !j.ready)
            || (self.restored_unknown && self.schema_journal.is_none())
        {
            return Err(Error::RecoveryRequired("schema restore incomplete"));
        }
        Ok(())
    }

    /// Publish Window::finish's transaction through this capture's sole writer.
    /// Call after the Durable step carrying H and before a later commit. A
    /// cancelled/failed publication must be retried with the identical record;
    /// step refuses to read WAL until it completes. Returns durable evidence.
    pub async fn append_chunk_rows(
        &mut self,
        high_lsn: u64,
        transaction: AssembledTxn,
    ) -> Result<DurableManifestRef> {
        self.schema_ready()?;
        if self.failed {
            return Err(Error::RecoveryRequired(
                "capture poisoned; reconnect and replay",
            ));
        }
        let boundary = self
            .boundary
            .as_ref()
            .ok_or(Error::ChunkBoundaryUnavailable)?;
        let committed = commit_lsn(boundary)?;
        if committed > high_lsn {
            return Err(Error::ChunkBoundaryPassed {
                requested: high_lsn,
                committed,
            });
        }
        if committed != high_lsn
            || self.pending.is_some()
            || transaction.commit_position != boundary.commit_position
            || transaction.source != boundary.source
        {
            return Err(Error::ChunkBoundaryUnavailable);
        }
        if let Some(pending) = &self.pending_chunk {
            if pending != &transaction {
                return Err(Error::ChunkPublicationConflict);
            }
        } else {
            crate::buffer::format::validate_chunk(&transaction).map_err(writer::Error::from)?;
            self.pending_chunk = Some(transaction);
        }
        let (_, reference) = self
            .writer
            .append_chunk_rows(
                self.pending_chunk.as_ref().expect("pending chunk").clone(),
                Instant::now(),
            )
            .await?;
        self.pending_chunk = None;
        Ok(reference)
    }

    /// Drive until a typed failure or caller cancellation. Keep this Capture
    /// alive across cancellation to preserve an in-flight writer publication.
    pub async fn run(
        &mut self,
        stream: &mut impl Transport,
        sink: &mut impl WatermarkSink,
        paused: &mut watch::Receiver<bool>,
        mut progress: impl FnMut(Step) -> Result<()>,
    ) -> Result<()> {
        loop {
            progress(self.step(stream, sink, paused).await?)?;
        }
    }

    /// `paused` is caller-owned buffer admission, not an invitation to discard
    /// WAL. No transport reads occur while it is true. A closed gate fails
    /// explicitly. A blocked writer likewise leaves the source unread.
    pub async fn step(
        &mut self,
        stream: &mut impl Transport,
        sink: &mut impl WatermarkSink,
        paused: &mut watch::Receiver<bool>,
    ) -> Result<Step> {
        self.schema_ready()?;
        if self.failed {
            return Err(Error::RecoveryRequired(
                "capture poisoned; reconnect and replay",
            ));
        }
        if self.pending_chunk.is_some() {
            return Err(Error::ChunkPublicationPending);
        }
        if self.pending.is_some() {
            return self.publish(sink).await;
        }
        while *paused.borrow_and_update() {
            paused.changed().await.map_err(|_| Error::AdmissionClosed)?;
        }
        let payload = match stream.next(self.durable_lsn).await {
            Ok(payload) => payload,
            Err(error) => {
                self.failed = true;
                return Err(error);
            }
        };
        // All decoding/routing is synchronous once transport ownership passes
        // here. Errors poison the capture, so a caller cannot skip bad WAL.
        let result = self.consume(&payload);
        if result.is_err() {
            self.failed = true;
        }
        result?;
        if self.pending.is_some() {
            self.publish(sink).await
        } else {
            Ok(Step::Message)
        }
    }

    fn consume(&mut self, payload: &[u8]) -> Result<()> {
        let CopyData::XLogData { data, .. } = self.decoder.decode_copy_data(payload)? else {
            return Ok(());
        };
        let message = self.decoder.decode(data)?;
        if let Message::Relation(relation) = &message {
            self.mapper.relation(relation)?;
            return Ok(());
        }
        if matches!(message, Message::Type { .. }) {
            return Ok(());
        }
        if let Message::LogicalMessage {
            transactional,
            lsn,
            prefix,
            content,
        } = &message
        {
            if *prefix == self.watermark_prefix {
                if *transactional {
                    let bytes = serde_json::to_vec(&StoredWatermark {
                        message_lsn: *lsn,
                        content: content.to_vec(),
                    })
                    .map_err(|_| Error::RecoveryRequired("watermark encoding"))?;
                    let mapped = control(
                        EventKind::Watermark,
                        self.ddl
                            .last()
                            .map_or(self.schemas.last_version(), |record| record.version),
                        "skedaddle.watermark.v1",
                        bytes,
                    );
                    self.assembler.push(&message, |_| Ok(vec![mapped]))?;
                }
                // Nontransactional markers have no commit boundary and are
                // never reported or admitted as committed capture output.
                return Ok(());
            }
            if *prefix == ddl::MESSAGE_PREFIX {
                let record = ddl::decode(&message, &self.installation)?;
                let mapped = control(
                    EventKind::DdlBoundary,
                    record.version,
                    "skedaddle.ddl.v1",
                    ddl::encode(&record)?,
                );
                self.ddl.push(record);
                // Later rows in this transaction need its provisional descriptor
                // versions. Classify on a clone so a later rejection cannot
                // partially advance the durable schema state.
                let mut provisional = self.schemas.clone();
                if matches!(
                    provisional.classify_transaction(&self.ddl),
                    Classification::Supported { .. }
                ) {
                    self.mapper
                        .set_mapped_versions(provisional.mapped_versions());
                }
                self.assembler.push(&message, |_| Ok(vec![mapped]))?;
                return Ok(());
            }
            return Err(Error::RecoveryRequired(
                "unrecognized logical message prefix",
            ));
        }
        // Mapping errors are kept typed instead of collapsing them into the
        // assembler's static diagnostic. Any error poisons the whole capture.
        let mapped = match &message {
            Message::Begin(_) | Message::Commit(_) | Message::Origin { .. } => Vec::new(),
            _ => self.mapper.map(&message)?,
        };
        if let Some(mut completed) = self.assembler.push(&message, |_| Ok(mapped))? {
            // DML cannot mutate the classifier. Compare the complete retained
            // state for DDL, including a newly sticky stop with no version bump.
            let before = (!self.ddl.is_empty()).then(|| self.schemas.clone());
            let stop = if self.ddl.is_empty() {
                None
            } else {
                match self.schemas.classify_transaction(&self.ddl) {
                    Classification::Supported { .. } => None,
                    Classification::SafeBoundaryStop(stop) => Some(stop),
                }
            };
            self.ddl.clear();
            self.mapper
                .set_mapped_versions(self.schemas.mapped_versions());
            if let Some(stop) = &stop {
                append_stop(&mut completed.transaction, stop)?;
            }
            let schema_changed = before.is_some_and(|before| before != self.schemas);
            self.pending = Some(Pending {
                completed,
                stop,
                schema_changed,
            });
        }
        Ok(())
    }

    async fn publish(&mut self, sink: &mut impl WatermarkSink) -> Result<Step> {
        let pending = self.pending.as_ref().expect("pending transaction");
        if let Some(journal) = &mut self.schema_journal
            && pending.schema_changed
        {
            let previous = if journal.needs_checkpoint() {
                Some(self.writer.checkpoint()?)
            } else {
                None
            };
            journal
                .prepare(
                    &self.schemas,
                    self.restored_unknown,
                    &pending.completed.transaction,
                    pending.completed.end_lsn,
                    false,
                    previous,
                )
                .await?;
        }
        // append retains admitted data across cancellation; repeating it has
        // checked redelivery semantics. seal is also retry/cancellation safe.
        self.writer
            .append(pending.completed.transaction.clone(), Instant::now())
            .await?;
        self.writer.seal().await?;
        let checkpoint = self.writer.checkpoint()?;
        let txn = &pending.completed.transaction;
        let retained_durable = checkpoint.redelivery.iter().any(|r| {
            r.source == txn.source
                && r.txn == txn.txn
                && r.digest == txn.digest
                && r.position == txn.commit_position
        });
        if checkpoint.last_position.as_ref() != Some(&txn.commit_position) && !retained_durable {
            self.failed = true;
            return Err(Error::RecoveryRequired(
                "writer did not report this transaction durable",
            ));
        }
        // Promotion must precede source acknowledgement and all callbacks. A
        // journal outage retains pending and the old ack even after writer seal.
        if let Some(journal) = &mut self.schema_journal
            && pending.schema_changed
        {
            journal.promote(&checkpoint).await?;
        }
        // Only the writer's manifest-durable position authorizes its associated
        // end LSN. Neither XLogData.wal_end nor keepalives participate.
        self.durable_lsn = self.durable_lsn.max(pending.completed.end_lsn);
        let pending = self.pending.take().expect("pending transaction");
        self.boundary = Some(pending.completed.transaction);
        let transaction = self.boundary.as_ref().expect("durable boundary");
        let notified = notify_committed(transaction, sink);
        if notified.is_err() {
            self.failed = true;
        }
        notified?;
        Ok(Step::Durable {
            end_lsn: self.durable_lsn,
            stop: pending.stop,
        })
    }
}

fn commit_lsn(transaction: &AssembledTxn) -> Result<u64> {
    let bytes = transaction
        .commit_position
        .bytes()
        .try_into()
        .map_err(|_| Error::RecoveryRequired("invalid capture commit position"))?;
    Ok(u64::from_be_bytes(bytes))
}

fn notify_committed(transaction: &AssembledTxn, sink: &mut impl WatermarkSink) -> Result<()> {
    sink.committed(transaction)?;
    for event in &transaction.events {
        if event.kind != EventKind::Watermark {
            continue;
        }
        let bytes = event
            .after
            .as_ref()
            .and_then(|row| row.get("control"))
            .filter(|value| value.type_id == "skedaddle.watermark.v1")
            .and_then(|value| value.bytes.as_deref())
            .ok_or(Error::RecoveryRequired("durable watermark payload"))?;
        let stored: StoredWatermark = serde_json::from_slice(bytes)
            .map_err(|_| Error::RecoveryRequired("durable watermark encoding"))?;
        sink.watermark(Watermark {
            transactional: true,
            message_lsn: stored.message_lsn,
            content: stored.content,
            commit_lsn: commit_lsn(transaction)?,
            committed_events: transaction.events.clone(),
        })?;
    }
    Ok(())
}

fn control(kind: EventKind, schema_version: u64, type_id: &str, bytes: Vec<u8>) -> MappedEvent {
    MappedEvent {
        schema_version,
        kind,
        table: None,
        key: Row::new(),
        before: None,
        after: Some(Row::from([(
            "control".into(),
            OpaqueValue {
                type_id: type_id.into(),
                bytes: Some(bytes),
            },
        )])),
        unchanged_cols: BTreeSet::new(),
    }
}

fn append_stop(txn: &mut AssembledTxn, stop: &SafeBoundaryStop) -> Result<()> {
    // Versioned typed control payload, distinct from mapped SQL column values.
    let bytes = serde_json::to_vec(&serde_json::json!({ "kind": "SafeBoundaryStop", "xid": stop.xid, "schema_version": stop.schema_version, "reason": stop.reason, "remediation": stop.remediation, "capture_continues": stop.capture_continues, "stop_apply_before_transaction": stop.stop_apply_before_transaction, "readiness_blocked": stop.readiness_blocked }))
        .map_err(|_| Error::RecoveryRequired("stop encoding"))?;
    let data = control(
        EventKind::DdlBoundary,
        stop.schema_version,
        "skedaddle.stop.v1",
        bytes,
    );
    txn.events.push(Event {
        source: txn.source.clone(),
        txn: txn.txn.clone(),
        ordinal_in_txn: txn.events.len() as u64,
        position: txn.commit_position.clone(),
        schema_version: data.schema_version,
        kind: data.kind,
        table: data.table,
        key: data.key,
        before: data.before,
        after: data.after,
        unchanged_cols: data.unchanged_cols,
    });
    *txn = AssembledTxn::new_with_origin(
        txn.source.clone(),
        txn.txn.clone(),
        txn.commit_position.clone(),
        txn.commit_time,
        txn.source_origin.clone(),
        std::mem::take(&mut txn.events),
    )
    .map_err(assembly::AssembleError::from)?;
    Ok(())
}

#[cfg(test)]
mod tests;
