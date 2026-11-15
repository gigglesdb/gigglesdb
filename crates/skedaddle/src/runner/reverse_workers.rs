//! Joined, cancellation-safe post-activation capture. No reverse apply authority.
use super::operational::{Order, Resolver, buffer, replication};
use super::*;
use crate::{
    buffer::{
        backends::BufferStore,
        format,
        writer::{BufferWriter, WriterConfig},
    },
    coordinator::{self, Materialized, journal::Journal, reverse::EligibilityEnd},
    engine::{self, Runtime},
    event::{PositionOrder, SourceIdentity},
    pg::{
        ddl::SchemaVersions,
        pgoutput::{CopyData, Decoder, Message},
        replication::{Checkpoint, ReplicationConnection, ReplicationError},
        reverse::{self, Eligibility, Violation},
    },
};
use std::sync::Arc;
use tokio::sync::watch;

pub(super) const PREFIX: &str = "reverse-runner-v1";
const SLOT: &str = "reverse-slot-v1";

#[derive(Default, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Boundary {
    next: u64,
    digest: [u8; 32],
}

pub(super) enum Failure {
    Retry,
    End(EligibilityEnd),
}
fn invalid() -> Failure {
    Failure::End(EligibilityEnd::Domain {
        violation: Violation::ReverseCaptureUnverifiable,
    })
}
fn capture_error(error: source::Error) -> Failure {
    match error {
        source::Error::Replication(
            ReplicationError::Io(_) | ReplicationError::Timeout | ReplicationError::Tls(_),
        )
        | source::Error::SchemaJournal(_) => Failure::Retry,
        source::Error::Replication(ReplicationError::Server { code, .. }) => {
            if ["08", "40", "53", "55", "57", "58"]
                .iter()
                .any(|class| code.starts_with(class))
            {
                Failure::Retry
            } else {
                invalid()
            }
        }
        source::Error::Buffer(error) => buffer_error(error),
        _ => invalid(),
    }
}

fn buffer_error(error: crate::buffer::writer::Error) -> Failure {
    use crate::buffer::{backends::Error as StoreError, writer::Error};
    match error {
        Error::Io(_)
        | Error::Store(
            StoreError::Store
            | StoreError::UnknownPutOutcome
            | StoreError::UnknownPutOutcomeLocal(_)
            | StoreError::LocalIo
            | StoreError::LocalIoDetail(_),
        ) => Failure::Retry,
        _ => invalid(),
    }
}

pub(super) struct Workers {
    pub(super) tables: Vec<catalog::TableCatalog>,
    capture: reverse::Capture<Resolver>,
    destination: Option<ReplicationConnection>,
    destination_config: crate::pg::replication::ConnectionConfig,
    source: ReplicationConnection,
    eligibility: Eligibility,
    checkpoint: Checkpoint,
    monitor: BreachMonitor,
    source_ack: u64,
    tip: u64,
    paused: watch::Receiver<bool>,
    _pause: watch::Sender<bool>,
    runtime: Runtime,
    executor: tokio::runtime::Handle,
    publication: String,
}
#[derive(Default)]
struct BreachMonitor {
    drain: u64,
    written: bool,
    trusted: String,
    installation: String,
    origin: Option<String>,
}
impl BreachMonitor {
    fn observe(&mut self, message: &Message<'_>) -> bool {
        match message {
            Message::Begin(_) => {
                self.written = false;
                self.origin = None;
            }
            Message::Origin { name, .. } => self.origin = Some((*name).into()),
            Message::Insert { .. }
            | Message::Update { .. }
            | Message::Delete { .. }
            | Message::Truncate { .. } => self.written = true,
            Message::LogicalMessage { prefix, .. } if *prefix == crate::pg::ddl::MESSAGE_PREFIX => {
                self.written |= !crate::pg::ddl::decode(message, &self.installation)
                    .is_ok_and(|r| crate::pg::ddl::accepted_tool_internal(&r, &self.installation));
            }
            Message::Commit(c) => {
                return self.written
                    && c.commit_lsn > self.drain
                    && self.origin.as_deref() != Some(&self.trusted);
            }
            _ => {}
        }
        false
    }
}
struct TipTransport<'a> {
    inner: &'a mut ReplicationConnection,
    tip: &'a mut u64,
}
impl source::Transport for TipTransport<'_> {
    async fn next(&mut self, durable_lsn: u64) -> source::Result<Vec<u8>> {
        let frame = self.inner.next(durable_lsn).await?;
        let (CopyData::XLogData { wal_end, .. } | CopyData::PrimaryKeepalive { wal_end, .. }) =
            frame.decode()?;
        *self.tip = (*self.tip).max(wal_end);
        Ok(frame.payload().to_vec())
    }
}
impl Workers {
    pub(super) async fn open(
        runtime: &Runtime,
        journal: &Journal,
        definition: &MigrationDefinition,
        root: &std::path::Path,
        id: &str,
    ) -> Result<Box<Self>, Failure> {
        let state = runtime.state().await;
        if !matches!(
            state.lifecycle.state,
            coordinator::State::RollbackWindow | coordinator::State::RollingBack
        ) || state.rollback_eligibility_ended.is_some()
        {
            return Err(invalid());
        }
        let stopped = recovery_boundary(&state)?;
        let proof = state
            .verification
            .reverse
            .as_ref()
            .and_then(|r| r.proof.as_ref())
            .ok_or_else(invalid)?;
        let baseline = super::activation_baseline::load(journal)
            .await
            .map_err(|_| Failure::Retry)?
            .ok_or_else(invalid)?;
        let objects = selection::catalog_objects(&definition.selected_objects);
        let selections = definition
            .selected_objects
            .iter()
            .zip(&objects)
            .map(|(object, catalog_object)| {
                let columns = baseline
                    .destination
                    .observations
                    .iter()
                    .find(|o| {
                        o.check == preflight::Check::ColumnTypes
                            && o.object.as_ref() == Some(catalog_object)
                    })
                    .map(|o| {
                        o.rows
                            .iter()
                            .filter_map(|r| r["column"].as_str().map(str::to_owned))
                            .collect()
                    })
                    .unwrap_or_default();
                selection::from_observed(object, columns, &Redactor::default())
                    .map_err(|_| invalid())
            })
            .collect::<Result<Vec<_>, _>>()?;
        // Qualify in the reverse direction; never reuse source OIDs on destination.
        let tables = catalog::build(
            &baseline.destination,
            &baseline.source,
            &selections,
            0,
            true,
        )
        .map_err(|_| invalid())?;
        let mut redactor = Redactor::default();
        let destination =
            config(&definition.destination, &mut redactor).map_err(|_| Failure::Retry)?;
        let destination_config = replication(&destination).map_err(|_| Failure::Retry)?;
        let mut connection = source::connect(&destination_config)
            .await
            .map_err(capture_error)?;
        let reverse = reverse::Config::new(
            id,
            match connection.version() {
                crate::pg::replication::PostgresVersion::V16 => 160000,
                crate::pg::replication::PostgresVersion::V17 => 170000,
                crate::pg::replication::PostgresVersion::V18 => 180000,
            },
            objects
                .iter()
                .map(|o| (o.schema.clone(), o.name.clone()))
                .collect(),
        )
        .map_err(|_| invalid())?;
        if connection.lineage().system_id.to_string() != proof.binding.destination.system_identifier
            || connection.lineage().database != proof.binding.destination.database
        {
            return Err(invalid());
        }
        let mut checkpoint = match state.progress.get(SLOT) {
            Some((saved, _)) => engine::decode_source_checkpoint(saved).map_err(|_| invalid())?,
            None => Checkpoint {
                lineage: connection.lineage().clone(),
                slot: proof.slot.slot.clone(),
                durable_lsn: proof.slot.consistent_point,
            },
        };
        checkpoint
            .lineage
            .check(connection.lineage())
            .map_err(|_| invalid())?;
        if checkpoint.slot != reverse.slot() {
            return Err(invalid());
        }
        // Persist lineage before any WAL is acknowledged. Manifests recover even
        // a publication completed just before the checkpoint marker was saved.
        let boundary = match state.progress.get(SLOT) {
            Some((_, saved)) => serde_json::from_str::<Boundary>(saved).map_err(|_| invalid())?,
            None => {
                runtime
                    .marker(
                        SLOT,
                        engine::encode_source_checkpoint(&checkpoint).map_err(|_| invalid())?,
                        serde_json::to_string(&Boundary::default()).map_err(|_| invalid())?,
                    )
                    .await
                    .map_err(|_| Failure::Retry)?;
                Boundary::default()
            }
        };
        let store = buffer(definition, true).map_err(|_| Failure::Retry)?;
        let order: Arc<dyn PositionOrder + Send + Sync> = Arc::new(Order(reverse.stream()));
        let (feedback, _receive) = std::sync::mpsc::channel();
        let scratch = root.join(format!("{id}.reverse-scratch"));
        std::fs::create_dir_all(&scratch).map_err(|_| Failure::Retry)?;
        let max = definition
            .budgets
            .max_snapshot_bytes
            .clamp(1, 16 * 1024 * 1024);
        let writer = BufferWriter::open_from_checkpoint(
            store.clone(),
            WriterConfig {
                prefix: PREFIX.into(),
                migration_id: id.into(),
                stream: reverse.stream(),
                epoch: state.epoch,
                segment_bytes: max,
                segment_transactions: 100,
                segment_age: Duration::from_secs(1),
                max_transaction_bytes: max,
            },
            Default::default(),
            order,
            feedback,
            scratch,
        )
        .await
        .map_err(buffer_error)?;
        let recovered = writer.checkpoint().map_err(|_| invalid())?;
        if recovered.next_mseq < boundary.next {
            return Err(invalid());
        }
        if boundary.next > 0 {
            let key = format::manifest_key(PREFIX, id, &reverse.stream(), boundary.next - 1);
            let bytes = store
                .get(&key)
                .await
                .map_err(|_| Failure::Retry)?
                .ok_or_else(invalid)?;
            let manifest =
                format::ManifestEntry::from_canonical_json(&bytes).map_err(|_| invalid())?;
            if manifest.digest().map_err(|_| invalid())? != boundary.digest {
                return Err(invalid());
            }
        }
        if let Some(boundary) = stopped {
            boundary.validate(&recovered).map_err(capture_error)?;
            if checkpoint.durable_lsn > boundary.durable_lsn {
                return Err(invalid());
            }
            checkpoint.durable_lsn = boundary.durable_lsn;
        }
        if let Some(last) = recovered.last_position {
            checkpoint.durable_lsn = checkpoint.durable_lsn.max(u64::from_be_bytes(
                last.bytes().try_into().map_err(|_| invalid())?,
            ));
        }
        let identity = SourceIdentity {
            engine: "postgres".into(),
            system_id: checkpoint.lineage.system_id.to_string(),
            lineage: checkpoint.lineage.timeline.to_string(),
            db_name: checkpoint.lineage.database.clone(),
            stream: checkpoint.slot.clone(),
        };
        let eligibility = Eligibility::new(|_| Ok(()));
        let mut capture = reverse::Capture::new(
            &reverse,
            identity,
            checkpoint.durable_lsn,
            writer,
            Resolver(tables.clone()),
            SchemaVersions::new(id.into(), 0, []).map_err(|_| invalid())?,
            eligibility.clone(),
        )
        .map_err(capture_error)?;
        capture
            .restore_schema(source::SchemaJournalBinding {
                journal: journal.clone(),
                runtime: runtime.clone(),
                objects: Arc::new(
                    coordinator::journal::local_store(root).map_err(|_| Failure::Retry)?,
                ),
                stream: serde_json::json!([PREFIX, id, reverse.stream()]).to_string(),
            })
            .await
            .map_err(capture_error)?;
        checkpoint.durable_lsn = capture.durable_lsn();
        if let Some(boundary) = stopped {
            capture.restore_stop(boundary).map_err(capture_error)?;
        } else {
            connection
                .start(
                    &checkpoint.slot,
                    checkpoint.durable_lsn,
                    &[reverse.publication()],
                )
                .await
                .map_err(|e| capture_error(e.into()))?;
        }
        let source_config =
            config(&definition.source, &mut redactor).map_err(|_| Failure::Retry)?;
        let mut source_checkpoint = state
            .progress
            .get("source-slot")
            .map(|(s, _)| engine::decode_source_checkpoint(s))
            .transpose()
            .map_err(|_| invalid())?
            .ok_or_else(invalid)?;
        let drain = super::cutover::rollback_drain(&state).map_err(|_| invalid())?;
        // Never advance the source slot while monitoring: restart replays the
        // entire fence interval, so an unjournaled breach cannot be lost.
        if state
            .progress
            .get("runner-cutover-write-v1")
            .and_then(|(p, _)| p.parse::<u64>().ok())
            .is_some_and(|lsn| lsn > drain)
        {
            return Err(Failure::End(EligibilityEnd::SourceFenceBreach));
        }
        source_checkpoint.durable_lsn = state
            .progress
            .get("source-capture-durable-v1")
            .and_then(|(p, _)| p.parse::<u64>().ok())
            .filter(|p| *p >= drain)
            .ok_or_else(invalid)?;
        let source = source::reconnect(
            &replication(&source_config).map_err(|_| Failure::Retry)?,
            &source_checkpoint,
            &[format!("skedaddle_{id}")],
        )
        .await
        .map_err(capture_error)?;
        let (send, paused) = watch::channel(false);
        Ok(Box::new(Self {
            tables,
            capture,
            destination: Some(connection),
            destination_config,
            source,
            eligibility,
            checkpoint,
            monitor: BreachMonitor {
                drain,
                trusted: reverse.origin(),
                installation: id.into(),
                ..Default::default()
            },
            source_ack: source_checkpoint.durable_lsn,
            tip: 0,
            paused,
            _pause: send,
            runtime: runtime.clone(),
            executor: tokio::runtime::Handle::current(),
            publication: reverse.publication(),
        }))
    }
    pub(super) async fn step(&mut self) -> Result<bool, Failure> {
        let mut sink = |_: source::Watermark| Ok(());
        let mut destination = TipTransport {
            inner: self.destination.as_mut().ok_or(Failure::Retry)?,
            tip: &mut self.tip,
        };
        tokio::select! {
            result = self.capture.step(&mut destination, &mut sink, &mut self.paused) => {
                if let Some(violation) = self.eligibility.first_violation() { return Err(Failure::End(EligibilityEnd::Domain { violation })); }
                if matches!(result.map_err(capture_error)?, source::Step::Durable { .. }) {
                    return Ok(true);
                }
            }
            result = self.source.next(self.source_ack) => {
                let frame = result.map_err(|e| capture_error(e.into()))?;
                if let CopyData::XLogData { data, .. } = frame.decode().map_err(|_| invalid())?
                    && self.monitor.observe(&Decoder::default().decode(data).map_err(|_| invalid())?) {
                    return Err(Failure::End(EligibilityEnd::SourceFenceBreach));
                }
            }
        }
        Ok(false)
    }
    pub(super) async fn status(&mut self, runtime: &Runtime) -> coordinator::Result<()> {
        if let Some(boundary) = self
            .capture
            .stop_boundary()
            .map_err(|_| coordinator::Error::Rejected("reverse stop boundary unavailable".into()))?
        {
            let state = runtime.state().await;
            match state
                .rollback
                .as_ref()
                .and_then(|cp| cp.capture_boundary.as_ref())
            {
                Some(saved) if saved == &boundary => {}
                Some(_) => {
                    return Err(coordinator::Error::Rejected(
                        "reverse stop receipt differs".into(),
                    ));
                }
                None => {
                    runtime
                        .commit(coordinator::Transition::Rollback {
                            record: Box::new(coordinator::rollback::Record::CaptureStopped {
                                boundary,
                            }),
                        })
                        .await?
                }
            }
        }
        self.checkpoint.durable_lsn = self.capture.durable_lsn();
        let writer = self.capture.writer().checkpoint().map_err(|_| {
            coordinator::Error::Rejected("reverse writer checkpoint unavailable".into())
        })?;
        let boundary = serde_json::to_string(&Boundary {
            next: writer.next_mseq,
            digest: writer.prev_digest,
        })?;
        runtime
            .marker(
                SLOT,
                engine::encode_source_checkpoint(&self.checkpoint)?,
                boundary,
            )
            .await?;
        runtime
            .marker(
                "reverse-position",
                self.checkpoint.durable_lsn.to_string(),
                self.tip.to_string(),
            )
            .await?;
        runtime
            .marker(
                "reverse-capture-health",
                "healthy".into(),
                if self.capture.stopped_at().is_some() {
                    "stopped-at-marker"
                } else {
                    "streaming"
                }
                .into(),
            )
            .await
    }
}

/// A receipt suppresses destination replication on takeover. Before the stop,
/// recovery may continue capture; after FinalMarker it must never guess a tip.
fn recovery_boundary(state: &Materialized) -> Result<Option<&reverse::StopBoundary>, Failure> {
    use coordinator::rollback::Step;
    if state.lifecycle.state != coordinator::State::RollingBack {
        return Ok(None);
    }
    let cp = state.rollback.as_ref().ok_or_else(invalid)?;
    if let Some(boundary) = &cp.capture_boundary {
        return Ok(Some(boundary));
    }
    if matches!(
        cp.next,
        Step::PreHook
            | Step::FenceDestination
            | Step::Drain
            | Step::FinalMarker
            | Step::RestoreDestination
    ) && !cp.point_of_no_return
    {
        Ok(None)
    } else {
        Err(invalid())
    }
}

fn driver_failure(failure: Failure) -> crate::cutover::Error {
    match failure {
        Failure::Retry => crate::cutover::Error::Component("reverse capture unavailable".into()),
        Failure::End(_) => crate::cutover::Error::Invalid("reverse capture evidence differs"),
    }
}
fn driver_journal(error: coordinator::Error) -> crate::cutover::Error {
    match error {
        coordinator::Error::LostOwnership | coordinator::Error::Observer => {
            crate::cutover::Error::LostOwnership
        }
        coordinator::Error::Rejected(_) | coordinator::Error::RecoveryRequired(_) => {
            crate::cutover::Error::Invalid("reverse capture journal differs")
        }
        _ => crate::cutover::Error::Component("reverse capture journal unavailable".into()),
    }
}

/// The synchronous driver calls this adapter on its blocking thread, retaining
/// exclusive authority throughout. Each poll is bounded so the owner can serve
/// status and check its deadline between driver slices.
impl crate::rollback::destination::Capture for Workers {
    fn captured(
        &mut self,
        marker: &crate::cutover::Marker,
    ) -> crate::cutover::Result<Option<crate::cutover::Marker>> {
        let executor = self.executor.clone();
        executor.block_on(async {
            self.runtime.refresh_owner().await.map_err(driver_journal)?;
            self.capture
                .stop_at(marker)
                .map_err(capture_error)
                .map_err(driver_failure)?;
            if self.capture.stopped_at().is_none()
                && let Ok(Err(failure)) =
                    tokio::time::timeout(Duration::from_millis(25), self.step()).await
            {
                if let Failure::End(reason) = &failure {
                    self.runtime
                        .commit(coordinator::Transition::RollbackEligibilityEnded {
                            reason: reason.clone(),
                        })
                        .await
                        .map_err(driver_journal)?;
                }
                return Err(driver_failure(failure));
            }
            let runtime = self.runtime.clone();
            // Publication is outside the cancellable read: never detach or
            // cancel an ambiguous durable receipt.
            self.status(&runtime).await.map_err(driver_journal)?;
            Ok(self.capture.stopped_at().cloned())
        })
    }
    fn stop_at(&mut self, marker: &crate::cutover::Marker) -> crate::cutover::Result<()> {
        if self.capture.stopped_at() != Some(marker) {
            return Err(crate::cutover::Error::Pending);
        }
        let executor = self.executor.clone();
        executor.block_on(async {
            self.runtime.refresh_owner().await.map_err(driver_journal)?;
            let runtime = self.runtime.clone();
            self.status(&runtime).await.map_err(driver_journal)?;
            let state = runtime.state().await;
            if state
                .rollback
                .as_ref()
                .and_then(|cp| cp.capture_boundary.as_ref())
                .map(|b| &b.marker)
                != Some(marker)
            {
                return Err(crate::cutover::Error::Invalid(
                    "durable capture receipt missing",
                ));
            }
            Ok(())
        })
    }
    fn restart(&mut self, key: &crate::cutover::Key) -> crate::cutover::Result<()> {
        let executor = self.executor.clone();
        executor.block_on(async {
            self.runtime.refresh_owner().await.map_err(driver_journal)?;
            let state = self.runtime.state().await;
            if key.epoch <= 0 || state.epoch != key.epoch as u64 || state.owner != key.coordinator {
                return Err(crate::cutover::Error::LostOwnership);
            }
            let cp = state
                .rollback
                .as_ref()
                .ok_or(crate::cutover::Error::Invalid("rollback admission missing"))?;
            if cp.request != key.cutover
                || cp.point_of_no_return
                || cp.next != coordinator::rollback::Step::RestoreDestination
                || state.rollback_eligibility_ended.is_some()
            {
                return Err(crate::cutover::Error::Invalid(
                    "capture restart not authorized",
                ));
            }
            if self.capture.stopped_at().is_some() {
                // Drop CopyBoth before reconnecting: the slot permits one reader.
                self.checkpoint.durable_lsn = self.capture.durable_lsn();
                self.destination = None;
                self.destination = Some(
                    source::reconnect(
                        &self.destination_config,
                        &self.checkpoint,
                        std::slice::from_ref(&self.publication),
                    )
                    .await
                    .map_err(|e| driver_failure(capture_error(e)))?,
                );
            }
            // Clear an armed marker too: the deadline may expire before its
            // transaction arrives, and normal capture must not stop later.
            self.capture.restart();
            let runtime = self.runtime.clone();
            self.status(&runtime).await.map_err(driver_journal)
        })
    }
}

pub fn status(state: &Materialized) -> serde_json::Value {
    let position = state
        .progress
        .get("reverse-position")
        .and_then(|(p, _)| p.parse::<u64>().ok());
    let tip = state
        .progress
        .get("reverse-position")
        .and_then(|(_, p)| p.parse::<u64>().ok())
        .filter(|p| *p != 0);
    let alert = matches!(
        state.rollback_eligibility_ended,
        Some(EligibilityEnd::SourceFenceBreach)
    )
    .then_some("source-fence-breach");
    serde_json::json!({"capture_healthy":state.rollback_eligibility_ended.is_none() && state.progress.get("reverse-capture-health").is_some_and(|(h,mode)| h == "healthy" && mode == "streaming"), "capture_status":state.progress.get("reverse-capture-health").map(|(h,_)| h), "last_durable_lsn":position, "lag_bytes":position.zip(tip).map(|(p,t)| t.saturating_sub(p)), "tip_lsn":tip, "eligible":state.rollback_eligibility_ended.is_none(), "eligibility_ended":state.rollback_eligibility_ended, "critical_alert":alert})
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        buffer::{
            backends::{BufferStore, ObjectBufferStore},
            format,
            reader::{
                BufferReader, ManifestRange, OrphanFence, ReaderCheckpoint, ReaderConfig,
                list_orphan_segments,
            },
        },
        event::{AssembledTxn, Position, TxnId},
        pg::pgoutput::{Begin, Commit},
    };
    #[test]
    fn recovery_requires_a_receipt_after_final_marker_but_not_before() {
        use coordinator::{
            State,
            rollback::{Checkpoint, Step},
        };
        let mut state = Materialized::default();
        state.lifecycle.state = State::RollingBack;
        state.rollback = Some(Checkpoint {
            request: "request".into(),
            deadline_at_ms: 100,
            next: Step::PreHook,
            intent: None,
            final_marker: None,
            capture_boundary: None,
            point_of_no_return: false,
        });
        for step in [
            Step::PreHook,
            Step::FenceDestination,
            Step::Drain,
            Step::FinalMarker,
            Step::RestoreDestination,
        ] {
            state.rollback.as_mut().unwrap().next = step;
            assert!(matches!(recovery_boundary(&state), Ok(None)), "{step:?}");
        }
        for step in [
            Step::ReverseApply,
            Step::Verify,
            Step::Sequences,
            Step::RestoreSource,
            Step::PostHook,
        ] {
            state.rollback.as_mut().unwrap().next = step;
            assert!(recovery_boundary(&state).is_err(), "{step:?}");
        }
        let boundary = reverse::StopBoundary {
            marker: crate::cutover::Marker {
                cutover: "request".into(),
                epoch: 1,
                lsn: 10,
            },
            durable_lsn: 11,
            next_mseq: 1,
            digest: [7; 32],
        };
        state.rollback.as_mut().unwrap().capture_boundary = Some(boundary.clone());
        assert_eq!(recovery_boundary(&state).ok().flatten(), Some(&boundary));
        state.rollback = None;
        assert!(recovery_boundary(&state).is_err());
    }

    #[test]
    fn capture_adapter_preserves_lost_ownership_and_retries_infrastructure() {
        use crate::cutover::Error;
        assert!(matches!(
            driver_journal(coordinator::Error::LostOwnership),
            Error::LostOwnership
        ));
        assert!(matches!(
            driver_journal(coordinator::Error::Observer),
            Error::LostOwnership
        ));
        assert!(matches!(
            driver_journal(coordinator::Error::UnknownOutcome("lost reply".into())),
            Error::Component(_)
        ));
        assert!(matches!(
            driver_failure(Failure::Retry),
            Error::Component(_)
        ));
        assert!(matches!(driver_failure(invalid()), Error::Invalid(_)));
    }

    fn root() -> PathBuf {
        let root = PathBuf::from(std::env::var_os("SKEDADDLE_TEST_ROOT").unwrap())
            .join(format!("reverse-worker-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        root
    }
    fn writer_config(prefix: &str) -> WriterConfig {
        WriterConfig {
            prefix: prefix.into(),
            migration_id: "m".into(),
            stream: "m_rev".into(),
            epoch: 1,
            segment_bytes: 100000,
            segment_transactions: 10,
            segment_age: Duration::from_secs(1),
            max_transaction_bytes: 100000,
        }
    }
    fn reader_config(prefix: &str) -> ReaderConfig {
        ReaderConfig {
            prefix: prefix.into(),
            migration_id: "m".into(),
            stream: "m_rev".into(),
            limits: format::Limits::default(),
            required_next_mseq: 0,
        }
    }
    fn identity() -> SourceIdentity {
        SourceIdentity {
            engine: "postgres".into(),
            system_id: "1".into(),
            lineage: "1".into(),
            db_name: "db".into(),
            stream: "skedaddle_m_rev".into(),
        }
    }
    async fn writer(
        root: &std::path::Path,
        store: Arc<ObjectBufferStore>,
        prefix: &str,
    ) -> BufferWriter {
        std::fs::create_dir_all(root.join("scratch")).unwrap();
        let (send, _) = std::sync::mpsc::channel();
        BufferWriter::open_from_checkpoint(
            store,
            writer_config(prefix),
            Default::default(),
            Arc::new(Order("m_rev".into())),
            send,
            root.join("scratch"),
        )
        .await
        .unwrap()
    }
    #[tokio::test]
    async fn reverse_manifests_resume_and_forward_gc_cannot_discover_them() {
        let root = root();
        std::fs::create_dir_all(root.join("store")).unwrap();
        let store =
            Arc::new(ObjectBufferStore::local(root.join("store"), Duration::from_secs(5)).unwrap());
        let mut reverse = writer(&root, store.clone(), PREFIX).await;
        let txn = AssembledTxn::new(
            identity(),
            TxnId(vec![1]),
            Position::new("m_rev", 100u64.to_be_bytes().to_vec()),
            None,
            vec![],
        )
        .unwrap();
        reverse
            .append(txn.clone(), std::time::Instant::now())
            .await
            .unwrap();
        reverse.seal().await.unwrap();
        let saved = reverse.checkpoint().unwrap();
        drop(reverse);
        // Simulate a crash after manifest publication, before a checkpoint save.
        let resumed = writer(&root, store.clone(), PREFIX).await;
        assert_eq!(resumed.checkpoint().unwrap(), saved);
        let forward = reader_config("runner");
        assert_ne!(
            forward.manifest_key(0),
            reader_config(PREFIX).manifest_key(0)
        );
        assert!(
            store
                .list_prefix(&forward.segment_prefix(), None, 100)
                .await
                .unwrap()
                .objects
                .is_empty()
        );
        let range = ManifestRange {
            start: ReaderCheckpoint::default(),
            end_mseq: 0,
            end_digest: [0; 32],
        };
        let orphans = list_orphan_segments(
            store.as_ref(),
            &forward,
            &range,
            OrphanFence { before_epoch: 2 },
            None,
            100,
            10000,
        )
        .await
        .unwrap();
        assert!(orphans.orphans.is_empty());
        let mut reader = BufferReader::new(
            store.clone(),
            reader_config(PREFIX),
            Default::default(),
            Arc::new(Order("m_rev".into())),
        )
        .unwrap();
        assert_eq!(reader.next_txn().await.unwrap(), Some(txn));
        assert!(reader.next_txn().await.unwrap().is_none());
        drop(reader);
        drop(resumed);
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn breach_checks_commit_boundary_and_exact_origin() {
        let mut monitor = BreachMonitor {
            drain: 100,
            trusted: "skedaddle_m_rev".into(),
            ..Default::default()
        };
        for (origin, lsn, expected) in [
            (None, 100, false),
            (None, 101, true),
            (Some("skedaddle_m_rev"), 101, false),
            (Some("skedaddle_other_rev"), 101, true),
        ] {
            monitor.observe(&Message::Begin(Begin {
                final_lsn: lsn,
                commit_time: 0,
                xid: 1,
            }));
            if let Some(name) = origin {
                monitor.observe(&Message::Origin {
                    commit_lsn: lsn,
                    name,
                });
            }
            monitor.observe(&Message::Truncate {
                cascade: false,
                restart_identity: false,
                relation_ids: vec![1],
            });
            assert_eq!(
                monitor.observe(&Message::Commit(Commit {
                    commit_lsn: lsn,
                    end_lsn: lsn + 1,
                    commit_time: 0
                })),
                expected
            );
        }
        monitor.observe(&Message::Begin(Begin {
            final_lsn: 105,
            commit_time: 0,
            xid: 2,
        }));
        assert!(!monitor.observe(&Message::Commit(Commit {
            commit_lsn: 105,
            end_lsn: 106,
            commit_time: 0
        })));
    }
    #[test]
    fn infrastructure_errors_retry_and_protocol_loss_ends_eligibility() {
        for error in [
            ReplicationError::Io(std::io::Error::from(std::io::ErrorKind::TimedOut)),
            ReplicationError::Timeout,
            ReplicationError::Server {
                code: "08006".into(),
                message: "disconnected".into(),
            },
        ] {
            assert!(matches!(capture_error(error.into()), Failure::Retry));
        }
        assert!(matches!(
            capture_error(source::Error::RecoveryRequired("slot lost")),
            Failure::End(_)
        ));
        assert!(matches!(
            capture_error(
                ReplicationError::Server {
                    code: "42501".into(),
                    message: "denied".into()
                }
                .into()
            ),
            Failure::End(_)
        ));
    }
    struct Failing {
        calls: usize,
        transient: bool,
    }
    impl source::Transport for Failing {
        async fn next(&mut self, _: u64) -> source::Result<Vec<u8>> {
            self.calls += 1;
            if self.transient {
                Err(ReplicationError::Timeout.into())
            } else {
                Err(source::Error::Mapping(
                    crate::mapping::MappingError::OutOfDomain("test".into()),
                ))
            }
        }
    }
    #[tokio::test]
    async fn transient_retries_but_first_violation_stops_capture_exactly_once() {
        let root = root();
        std::fs::create_dir_all(root.join("store")).unwrap();
        let store =
            Arc::new(ObjectBufferStore::local(root.join("store"), Duration::from_secs(5)).unwrap());
        let count = std::rc::Rc::new(std::cell::Cell::new(0));
        let recorded = count.clone();
        let eligibility = Eligibility::new(move |_| {
            recorded.set(recorded.get() + 1);
            Ok(())
        });
        let reverse =
            reverse::Config::new("m", 180000, vec![("public".into(), "rows".into())]).unwrap();
        let mut capture = reverse::Capture::new(
            &reverse,
            identity(),
            1,
            writer(&root, store.clone(), PREFIX).await,
            Resolver(vec![]),
            SchemaVersions::new("m".into(), 0, []).unwrap(),
            eligibility.clone(),
        )
        .unwrap();
        let (_pause, mut paused) = watch::channel(false);
        let mut transport = Failing {
            calls: 0,
            transient: true,
        };
        let mut sink = |_: source::Watermark| Ok(());
        for _ in 0..1 {
            assert!(
                capture
                    .step(&mut transport, &mut sink, &mut paused)
                    .await
                    .is_err()
            );
        }
        assert!(eligibility.eligible());
        assert_eq!(count.get(), 0);
        // A wire failure poisons the source session. The owner reconnects by
        // rebuilding capture at its durable boundary, retaining eligibility.
        drop(capture);
        let mut capture = reverse::Capture::new(
            &reverse,
            identity(),
            1,
            writer(&root, store.clone(), PREFIX).await,
            Resolver(vec![]),
            SchemaVersions::new("m".into(), 0, []).unwrap(),
            eligibility.clone(),
        )
        .unwrap();
        transport.transient = false;
        for _ in 0..3 {
            assert!(
                capture
                    .step(&mut transport, &mut sink, &mut paused)
                    .await
                    .is_err()
            );
        }
        assert_eq!(transport.calls, 2);
        assert_eq!(count.get(), 1);
        assert!(!eligibility.eligible());
        drop(capture);
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }
    struct Frames(std::collections::VecDeque<Vec<u8>>);
    impl source::Transport for Frames {
        async fn next(&mut self, _: u64) -> source::Result<Vec<u8>> {
            Ok(self
                .0
                .pop_front()
                .expect("capture read beyond stop boundary"))
        }
    }
    fn marker_frames(marker: &crate::cutover::Marker) -> Vec<Vec<u8>> {
        let mut begin = vec![b'B'];
        begin.extend((marker.lsn + 10).to_be_bytes());
        begin.extend(0i64.to_be_bytes());
        begin.extend(1u32.to_be_bytes());
        let mut message = vec![b'M', 1];
        message.extend(marker.lsn.to_be_bytes());
        message.extend(crate::pg::chunk::MESSAGE_PREFIX.as_bytes());
        message.push(0);
        let payload = marker.payload();
        message.extend((payload.len() as u32).to_be_bytes());
        message.extend(payload.as_bytes());
        let mut commit = vec![b'C', 0];
        commit.extend((marker.lsn + 10).to_be_bytes());
        commit.extend((marker.lsn + 11).to_be_bytes());
        commit.extend(0i64.to_be_bytes());
        [begin, message, commit]
            .into_iter()
            .map(|message| {
                let mut frame = vec![b'w'];
                frame.extend([0; 24]);
                frame.extend(message);
                frame
            })
            .collect()
    }
    #[tokio::test]
    async fn deadline_restart_clears_an_armed_stop_before_marker_arrives() {
        let root = root();
        std::fs::create_dir_all(root.join("store")).unwrap();
        let store =
            Arc::new(ObjectBufferStore::local(root.join("store"), Duration::from_secs(5)).unwrap());
        let config =
            reverse::Config::new("m", 180000, vec![("public".into(), "rows".into())]).unwrap();
        let mut capture = reverse::Capture::new(
            &config,
            identity(),
            1,
            writer(&root, store.clone(), PREFIX).await,
            Resolver(vec![]),
            SchemaVersions::new("m".into(), 0, []).unwrap(),
            Eligibility::new(|_| Ok(())),
        )
        .unwrap();
        let marker = crate::cutover::Marker {
            cutover: "request".into(),
            epoch: 1,
            lsn: 100,
        };
        let later = crate::cutover::Marker {
            lsn: 200,
            ..marker.clone()
        };
        capture.stop_at(&marker).unwrap();
        // Deadline compensation before the final transaction is observed.
        capture.restart();
        let mut frames = Frames(
            marker_frames(&marker)
                .into_iter()
                .chain(marker_frames(&later))
                .collect(),
        );
        let (_send, mut paused) = watch::channel(false);
        for _ in 0..6 {
            capture
                .step(&mut frames, &mut |_: source::Watermark| Ok(()), &mut paused)
                .await
                .unwrap();
        }
        assert!(frames.0.is_empty());
        assert!(capture.stopped_at().is_none());
        assert!(capture.stop_boundary().unwrap().is_none());
        drop(capture);
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn exact_marker_stop_is_durable_idempotent_and_restart_keeps_following_wal() {
        let root = root();
        std::fs::create_dir_all(root.join("store")).unwrap();
        let store =
            Arc::new(ObjectBufferStore::local(root.join("store"), Duration::from_secs(5)).unwrap());
        let config =
            reverse::Config::new("m", 180000, vec![("public".into(), "rows".into())]).unwrap();
        let mut capture = reverse::Capture::new(
            &config,
            identity(),
            1,
            writer(&root, store.clone(), PREFIX).await,
            Resolver(vec![]),
            SchemaVersions::new("m".into(), 0, []).unwrap(),
            Eligibility::new(|_| Ok(())),
        )
        .unwrap();
        let marker = crate::cutover::Marker {
            cutover: "request".into(),
            epoch: 1,
            lsn: 100,
        };
        let later = crate::cutover::Marker {
            lsn: 200,
            ..marker.clone()
        };
        let mut frames = Frames(
            marker_frames(&marker)
                .into_iter()
                .chain(marker_frames(&later))
                .collect(),
        );
        let (_send, mut paused) = watch::channel(false);
        let mut seen = Vec::new();
        capture.stop_at(&marker).unwrap();
        for _ in 0..2 {
            capture
                .step(
                    &mut frames,
                    &mut |w| {
                        seen.push(w);
                        Ok(())
                    },
                    &mut paused,
                )
                .await
                .unwrap();
            assert!(capture.stopped_at().is_none());
            assert_eq!(capture.writer().checkpoint().unwrap().next_mseq, 0);
        }
        capture
            .step(
                &mut frames,
                &mut |w| {
                    seen.push(w);
                    Ok(())
                },
                &mut paused,
            )
            .await
            .unwrap();
        assert_eq!(capture.stopped_at(), Some(&marker));
        assert_eq!(seen.len(), 1);
        assert_eq!(capture.writer().checkpoint().unwrap().next_mseq, 1);
        let saved = capture.writer().checkpoint().unwrap();
        for _ in 0..3 {
            capture.stop_at(&marker).unwrap();
            capture
                .step(
                    &mut frames,
                    &mut |_: source::Watermark| panic!("duplicate callback"),
                    &mut paused,
                )
                .await
                .unwrap();
        }
        assert_eq!(frames.0.len(), 3);
        assert!(capture.stop_at(&later).is_err());
        let boundary = capture.stop_boundary().unwrap().unwrap();
        let durable_lsn = capture.durable_lsn();
        drop(capture);
        let mut capture = reverse::Capture::new(
            &config,
            identity(),
            durable_lsn,
            writer(&root, store.clone(), PREFIX).await,
            Resolver(vec![]),
            SchemaVersions::new("m".into(), 0, []).unwrap(),
            Eligibility::new(|_| Ok(())),
        )
        .unwrap();
        let boundary: reverse::StopBoundary =
            serde_json::from_slice(&serde_json::to_vec(&boundary).unwrap()).unwrap();
        for bad in [
            reverse::StopBoundary {
                next_mseq: boundary.next_mseq + 1,
                ..boundary.clone()
            },
            reverse::StopBoundary {
                digest: [7; 32],
                ..boundary.clone()
            },
            reverse::StopBoundary {
                durable_lsn: durable_lsn + 1,
                ..boundary.clone()
            },
        ] {
            assert!(capture.restore_stop(&bad).is_err());
            assert!(capture.stopped_at().is_none());
        }
        capture.restore_stop(&boundary).unwrap();
        capture.restore_stop(&boundary).unwrap();
        assert_eq!(capture.stopped_at(), Some(&marker));
        capture
            .step(
                &mut frames,
                &mut |_: source::Watermark| panic!("recovered stop read"),
                &mut paused,
            )
            .await
            .unwrap();
        assert_eq!(frames.0.len(), 3);
        capture.restart();
        assert_eq!(capture.writer().checkpoint().unwrap(), saved);
        assert!(capture.stop_at(&marker).is_err()); // passed LSN is not proof
        capture.stop_at(&later).unwrap();
        for _ in 0..3 {
            capture
                .step(&mut frames, &mut |_: source::Watermark| Ok(()), &mut paused)
                .await
                .unwrap();
        }
        assert_eq!(capture.stopped_at(), Some(&later));
        assert_eq!(capture.writer().checkpoint().unwrap().next_mseq, 2);
        assert!(
            boundary
                .validate(&capture.writer().checkpoint().unwrap())
                .is_err()
        );
        drop(capture);
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn marker_identity_mismatch_never_authorizes_stop() {
        for (epoch, lsn) in [(2, 100), (1, 101)] {
            let root = root();
            std::fs::create_dir_all(root.join("store")).unwrap();
            let store = Arc::new(
                ObjectBufferStore::local(root.join("store"), Duration::from_secs(5)).unwrap(),
            );
            let config =
                reverse::Config::new("m", 180000, vec![("public".into(), "rows".into())]).unwrap();
            let mut capture = reverse::Capture::new(
                &config,
                identity(),
                1,
                writer(&root, store.clone(), PREFIX).await,
                Resolver(vec![]),
                SchemaVersions::new("m".into(), 0, []).unwrap(),
                Eligibility::new(|_| Ok(())),
            )
            .unwrap();
            let marker = crate::cutover::Marker {
                cutover: "request".into(),
                epoch: 1,
                lsn: 100,
            };
            capture.stop_at(&marker).unwrap();
            let mut frames = Frames(
                marker_frames(&crate::cutover::Marker {
                    epoch,
                    lsn,
                    ..marker
                })
                .into(),
            );
            let (_send, mut paused) = watch::channel(false);
            let mut sink = |_: source::Watermark| Ok(());
            for _ in 0..2 {
                capture
                    .step(&mut frames, &mut sink, &mut paused)
                    .await
                    .unwrap();
            }
            assert!(
                capture
                    .step(&mut frames, &mut sink, &mut paused)
                    .await
                    .is_err()
            );
            assert!(capture.stopped_at().is_none());
            assert!(
                capture
                    .step(&mut frames, &mut sink, &mut paused)
                    .await
                    .is_err()
            );
            drop(capture);
            drop(store);
            std::fs::remove_dir_all(root).unwrap();
        }
    }
    #[test]
    fn breach_reason_replays_as_critical_alert() {
        let state = Materialized {
            metadata: Box::new(coordinator::MaterializedMetadata {
                operational_schema: Default::default(),
                rollback_eligibility_ended: Some(EligibilityEnd::SourceFenceBreach),
                ..Default::default()
            }),
            ..Default::default()
        };
        let status = status(&state);
        assert_eq!(status["critical_alert"], "source-fence-breach");
        assert_eq!(status["eligible"], false);
        assert_eq!(status["capture_healthy"], false);
        assert_eq!(status["eligibility_ended"]["kind"], "source-fence-breach");
    }
}
