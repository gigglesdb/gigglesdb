use super::*;
use crate::{
    budget::{Limits, RateLimit, ReadLimits, SystemClock, ThroughputBudget},
    buffer::{
        backends::ObjectBufferStore,
        format,
        gc::RetentionPolicy,
        reader::{BufferReader, ReaderCheckpoint, ReaderConfig},
        writer::{BufferWriter, WriterConfig},
    },
    coordinator::{
        self, EffectKind, Materialized, State, epoch::Coordinator,
        gc_authority::CoordinatorGcJournal, journal::Journal, local::Local,
    },
    definition::BufferBackend,
    engine::{
        self, Capture as _, CopyConfig, PauseControl, PgApplier, PgCapture, PgChunks, PgInstaller,
        Reached, StopAt,
    },
    event::{AssembledTxn, EventError, PositionOrder, Row, SourceIdentity},
    gigglesdb::{
        apply::GigglesdbExecutor,
        engine::{DestinationInstaller, RunnerDestination},
    },
    mapping::Value,
    pg::{
        apply::{
            self,
            executor::{self, Connector, Executor},
        },
        chunk::{ChunkRange, Watermark},
        ddl::SchemaVersions,
        pgoutput::{Relation, RelationColumn},
        replication::ConnectionConfig,
    },
};
use std::{cmp::Ordering, collections::BTreeSet, path::Path, sync::Arc};
use tokio::sync::{Mutex, watch};

const PREFIX: &str = "runner";
const READ_ROWS: u64 = 1000;

enum ExclusionBackend<'a> {
    Source(&'a tls::Config, &'a crate::security::TlsPolicy),
    #[cfg(test)]
    File(&'a Path),
}
fn migration_exclusion(
    binding: &SourceBinding,
    backend: ExclusionBackend<'_>,
) -> coordinator::Result<Arc<coordinator::exclusion::MigrationExclusion>> {
    use coordinator::exclusion::MigrationExclusion;
    if binding.connector != SourceConnector::Postgres {
        return Err(coordinator::Error::SourceBinding(
            SourceBindingError::ExclusionUnsupported(binding.connector),
        ));
    }
    match backend {
        ExclusionBackend::Source(config, policy) => {
            Ok(MigrationExclusion::postgres(config.clone(), *policy))
        }
        #[cfg(test)]
        ExclusionBackend::File(path) => MigrationExclusion::file(path).map_err(recovery),
    }
}
fn operational_coordinator(
    journal: Journal,
    local: Local,
    exclusion: Arc<coordinator::exclusion::MigrationExclusion>,
) -> coordinator::Result<Coordinator> {
    Ok(
        Coordinator::observer(journal, local, format!("runner-{}", std::process::id()))?
            .with_exclusion(exclusion),
    )
}

pub enum RunError {
    Preparation(Failure),
    Engine(coordinator::Error),
}
impl From<coordinator::Error> for RunError {
    fn from(error: coordinator::Error) -> Self {
        Self::Engine(error)
    }
}
impl From<std::io::Error> for RunError {
    fn from(error: std::io::Error) -> Self {
        Self::Engine(coordinator::Error::Io(error))
    }
}
fn recovery(_: impl std::fmt::Display) -> coordinator::Error {
    coordinator::Error::RecoveryRequired(
        "Runner adapter operation failed; preserve state and inspect endpoint prerequisites".into(),
    )
}
pub(super) struct Order(pub String);
impl PositionOrder for Order {
    fn domain(&self) -> &str {
        &self.0
    }
    fn compare(&self, a: &[u8], b: &[u8]) -> Result<Ordering, EventError> {
        if a.len() != 8 || b.len() != 8 {
            return Err(EventError::InvalidEncoding("PostgreSQL LSN width"));
        }
        Ok(a.cmp(b))
    }
}
impl SourceBinding {
    /// Until connector startup supplies an observed native boundary, non-PG
    /// selections are journal-only and stop before constructing any adapter.
    pub fn for_definition(id: &str, definition: &MigrationDefinition) -> coordinator::Result<Self> {
        let connector = SourceConnector::from_engine(definition.source.engine)?;
        let (order_domain, stream) =
            match connector {
                SourceConnector::Postgres => (
                    format!("skedaddle-postgres:{id}"),
                    format!("skedaddle_{id}"),
                ),
                SourceConnector::Mysql => (
                    crate::mysql::position::MysqlOrder::gtid(id)
                        .map_err(recovery)?
                        .domain()
                        .into(),
                    format!("skedaddle-{id}-binlog"),
                ),
                SourceConnector::Spanner => {
                    let endpoint = definition.source.spanner.as_ref().ok_or(
                        coordinator::Error::SourceBinding(SourceBindingError::Invalid),
                    )?;
                    let order = crate::spanner::frontier::SpannerOrder::new(
                        &endpoint.project,
                        &endpoint.instance,
                        &endpoint.database,
                        &endpoint.change_stream,
                        0,
                    )
                    .map_err(recovery)?;
                    (order.domain().into(), endpoint.change_stream.clone())
                }
            };
        let binding = Self {
            version: 1,
            connector,
            order_domain,
            stream,
        };
        binding.validate()?;
        Ok(binding)
    }
    fn reader_config(&self, id: &str) -> ReaderConfig {
        ReaderConfig {
            prefix: PREFIX.into(),
            migration_id: id.into(),
            stream: if self.connector == SourceConnector::Postgres {
                format!("skedaddle-{id}-wal")
            } else {
                self.stream.clone()
            },
            limits: format::Limits::default(),
            required_next_mseq: 0,
        }
    }
    fn position_order(
        &self,
        id: &str,
        definition: &MigrationDefinition,
    ) -> coordinator::Result<Arc<dyn PositionOrder + Send + Sync>> {
        self.require_same(&Self::for_definition(id, definition)?)?;
        Ok(match self.connector {
            SourceConnector::Postgres => Arc::new(Order(self.order_domain.clone())),
            SourceConnector::Mysql => {
                Arc::new(crate::mysql::position::MysqlOrder::gtid(id).map_err(recovery)?)
            }
            SourceConnector::Spanner => {
                let e =
                    definition
                        .source
                        .spanner
                        .as_ref()
                        .ok_or(coordinator::Error::SourceBinding(
                            SourceBindingError::Invalid,
                        ))?;
                Arc::new(
                    crate::spanner::frontier::SpannerOrder::new(
                        &e.project,
                        &e.instance,
                        &e.database,
                        &e.change_stream,
                        0,
                    )
                    .map_err(recovery)?,
                )
            }
        })
    }
    fn validate_state(&self, id: &str, state: &Materialized) -> coordinator::Result<()> {
        self.validate()?;
        if let Some(definition) = &state.definition {
            self.require_same(&Self::for_definition(id, definition)?)?;
        }
        if let Some(saved) = &state.source_binding {
            self.require_same(saved)?;
        } else if self.connector != SourceConnector::Postgres
            && (!state.progress.is_empty()
                || !state.intent_roots.is_empty()
                || !state.chunk_plans.is_empty())
        {
            return Err(coordinator::Error::SourceBinding(
                SourceBindingError::Missing,
            ));
        }
        Ok(())
    }
    fn postgres_checkpoint(
        &self,
        state: &Materialized,
    ) -> coordinator::Result<Option<crate::pg::replication::Checkpoint>> {
        if self.connector != SourceConnector::Postgres {
            return Err(coordinator::Error::SourceBinding(
                SourceBindingError::NotWired(self.connector),
            ));
        }
        state
            .progress
            .get("source-slot")
            .map(|(marker, _)| engine::decode_source_checkpoint(marker))
            .transpose()
    }
}
pub(super) fn buffer(
    definition: &MigrationDefinition,
    create: bool,
) -> coordinator::Result<Arc<ObjectBufferStore>> {
    let BufferBackend::LocalFs(options) = &definition.buffer.backend else {
        return Err(coordinator::Error::Rejected("Runner cloud buffer credentials_ref payload contract is unavailable; qualify a credential adapter before using S3/GCS".into()));
    };
    if create {
        std::fs::create_dir_all(&options.path)?;
    }
    Ok(Arc::new(
        ObjectBufferStore::local(&options.path, Duration::from_secs(30)).map_err(recovery)?,
    ))
}

/// Observers retry with the committed buffer binding when replay reaches Done.
/// Reading the immutable first entries discovers the definition without trusting
/// the local projection or bypassing the subsequent authenticated full replay.
pub async fn open_journal(root: &Path, id: &str) -> coordinator::Result<Journal> {
    let raw = Journal::new(
        Arc::new(
            object_store::local::LocalFileSystem::new_with_prefix(root)
                .map_err(coordinator::Error::Store)?
                .with_fsync(true),
        ),
        "skedaddle",
        id,
        Duration::from_secs(30),
    )?;
    match raw.load(true).await {
        Ok(state) => {
            if let Some(definition) = &state.definition {
                SourceBinding::for_definition(id, definition)?.validate_state(id, &state)?;
            }
            return Ok(raw);
        }
        Err(error @ coordinator::Error::SourceBinding(_)) => return Err(error),
        Err(_) => {}
    }
    let entries = raw.history_page(0, 128).await?;
    if let Some(definition) = entries.iter().find_map(|e| match &e.transition {
        coordinator::Transition::Define { definition } => Some(definition),
        _ => None,
    }) {
        let binding = SourceBinding::for_definition(id, definition)?;
        let store = buffer(definition, false)?;
        let journal = raw.with_chunk_buffer(
            store,
            binding.reader_config(id),
            binding.position_order(id, definition)?,
        );
        binding.validate_state(id, &journal.load(true).await?)?;
        return Ok(journal);
    }
    // Retain the journal's original integrity diagnostic on the caller's load.
    Ok(raw)
}

/// Journal metadata uses create-only ownership CAS; no source exclusion or
/// endpoint is acquired here. A connector authority is mandatory before effects.
async fn record_unwired_binding(
    root: &Path,
    id: &str,
    binding: &SourceBinding,
) -> coordinator::Result<()> {
    let journal = open_journal(root, id).await?;
    let state = journal.load(true).await?;
    binding.validate_state(id, &state)?;
    if state.definition.is_none() {
        return Err(coordinator::Error::SourceBinding(
            SourceBindingError::Missing,
        ));
    }
    if state.source_binding.is_none() {
        let path = root.join(format!("{id}.local"));
        let local = if path.exists() {
            Local::open_journal(&path, &journal).await?
        } else {
            let mut local = Local::create(&path)?;
            local.replay_journal(&journal).await?;
            local
        };
        let mut coordinator =
            Coordinator::observer(journal, local, format!("runner-{}", std::process::id()))?;
        coordinator.acquire().await?;
        binding.validate_state(id, coordinator.state())?;
        if coordinator.state().source_binding.is_none() {
            coordinator
                .commit(coordinator::Transition::SourceBindingV1 {
                    binding: binding.clone(),
                })
                .await?;
        }
    }
    Err(coordinator::Error::SourceBinding(
        SourceBindingError::NotWired(binding.connector),
    ))
}

pub(super) struct Resolver(pub Vec<catalog::TableCatalog>);
impl Resolver {
    fn table(&self, relation: &Relation<'_>) -> source::Result<&catalog::TableCatalog> {
        self.0
            .iter()
            .find(|t| t.selection.schema == relation.namespace && t.selection.name == relation.name)
            .ok_or(source::Error::RecoveryRequired("unqualified relation"))
    }
}
impl source::TypeResolver for Resolver {
    fn included_columns(
        &mut self,
        relation: &Relation<'_>,
    ) -> source::Result<Option<BTreeSet<String>>> {
        self.table(relation)?.resolver().included_columns(relation)
    }
    fn resolve(
        &mut self,
        relation: &Relation<'_>,
        column: &RelationColumn<'_>,
    ) -> source::Result<source::ColumnMapping> {
        self.table(relation)?.resolver().resolve(relation, column)
    }
}
struct Chunks {
    pg: PgChunks,
    _drivers: (Driver, Driver),
}
impl engine::ChunkDatabase for Chunks {
    async fn emit(&mut self, marker: Watermark) -> coordinator::Result<()> {
        self.pg.emit(marker).await
    }
    async fn read(
        &mut self,
        plan: &ChunkPlan,
        chunk: &ChunkRange,
    ) -> coordinator::Result<Vec<Row>> {
        self.pg.read(plan, chunk).await
    }
}
pub(super) struct Connect {
    pub(super) config: tls::Config,
    pub(super) connection: Connection,
    pub(super) drivers: Vec<Driver>,
}
#[async_trait::async_trait]
impl Connector for Connect {
    async fn connect(&mut self) -> executor::Result<Client> {
        self.drivers.clear();
        let session = Box::pin(session(&self.config, &self.connection))
            .await
            .map_err(|_| executor::Error::Database)?;
        settings(&session.client)
            .await
            .map_err(|_| executor::Error::Database)?;
        self.drivers.push(session.driver);
        Ok(session.client)
    }
}
pub(super) fn replication(config: &tls::Config) -> coordinator::Result<ConnectionConfig> {
    let pg = &config.postgres;
    let [tokio_postgres::config::Host::Tcp(host)] = pg.get_hosts() else {
        return Err(recovery("replication needs exactly one TCP host"));
    };
    let address = match pg.get_hostaddrs() {
        [] => host.clone(),
        [address] => address.to_string(),
        _ => return Err(recovery("replication needs at most one hostaddr")),
    };
    let user = pg
        .get_user()
        .ok_or_else(|| recovery("explicit replication user required"))?;
    let password = std::str::from_utf8(
        pg.get_password()
            .ok_or_else(|| recovery("explicit replication password required"))?,
    )
    .map_err(recovery)?;
    let mut roots = rustls::RootCertStore::empty();
    let mut pem = std::io::BufReader::new(std::fs::File::open(&config.ca_pem)?);
    for cert in rustls_pemfile::certs(&mut pem) {
        roots.add(cert.map_err(recovery)?).map_err(recovery)?;
    }
    if roots.is_empty() {
        return Err(recovery("explicit replication TLS roots required"));
    }
    Ok(ConnectionConfig {
        host: address,
        server_name: host.clone(),
        port: pg.get_ports().first().copied().unwrap_or(5432),
        user: user.into(),
        password: password.into(),
        database: pg.get_dbname().unwrap_or(user).into(),
        roots,
        connect_timeout: Duration::from_secs(5),
        status_interval: Duration::from_secs(1),
        limits: Default::default(),
    })
}
async fn plan(
    table: &catalog::TableCatalog,
    client: &mut Client,
) -> coordinator::Result<ChunkPlan> {
    let mut query = table
        .key
        .sampling_query(READ_ROWS as usize)
        .map_err(recovery)?;
    // Preserve PostgreSQL's typed input ORDER BY. Distinct output names keep
    // ORDER BY bound to the original key rather than its text representation.
    let (_, tail) = query
        .sql
        .split_once(" FROM ")
        .ok_or_else(|| recovery("sampling query shape"))?;
    let projection = (0..table.key.columns.len())
        .map(|i| format!("\"__k{i}\"::text AS \"__text{i}\""))
        .collect::<Vec<_>>()
        .join(",");
    query.sql = format!("SELECT {projection} FROM {tail}");
    let rows = query.execute(client).await.map_err(recovery)?;
    let samples = rows
        .into_iter()
        .map(|row| {
            table
                .key
                .columns
                .iter()
                .enumerate()
                .map(|(i, c)| {
                    let text: String = row.try_get(i).map_err(recovery)?;
                    Value::parse_pg_text(&c.pg_type, Some(text.as_bytes())).map_err(recovery)
                })
                .collect::<coordinator::Result<Vec<_>>>()
        })
        .collect::<coordinator::Result<Vec<_>>>()?;
    ChunkPlan::new(table.key.clone(), READ_ROWS as usize, samples)
}

/// The caller holds its CLI migration lock throughout this joined operation.
/// A watch request and PauseControl both stop through Engine's safe boundary.
// Allocate the operational state machine before returning to the CLI.
pub fn run(
    root: &Path,
    id: &str,
    definition: &MigrationDefinition,
    resume: bool,
    stop: StopAt,
    mut pause: watch::Receiver<bool>,
) -> impl std::future::Future<Output = Result<Reached, RunError>> {
    Box::pin(async move {
        let binding = SourceBinding::for_definition(id, definition)?;
        let initial = open_journal(root, id).await?.load(true).await?;
        binding.validate_state(id, &initial)?;
        if binding.connector != SourceConnector::Postgres {
            record_unwired_binding(root, id, &binding).await?;
        }
        // Terminal replay is authenticated before connections or copy workers start.
        if resume {
            let terminal = open_journal(root, id).await?.load(true).await?;
            binding.validate_state(id, &terminal)?;
            if terminal.lifecycle.state.is_terminal() {
                return Ok(Reached {
                    state: terminal.lifecycle.state,
                    blockers: vec![],
                    target_reached: false,
                });
            }
        }
        if resume {
            let state = open_journal(root, id).await?.load(true).await?;
            if super::post_activation::owns(state.lifecycle.state) {
                return Box::pin(super::post_activation::resume(root, id, definition, pause)).await;
            }
        }
        let prepared = Box::pin(prepare(definition))
            .await
            .map_err(RunError::Preparation)?;
        if matches!(stop, StopAt::VerifiedReady) {
            return Err(RunError::Preparation(failure(
                FailureKind::Blocked,
                "Verified-ready runner evidence producers are unavailable",
                "Wire live lineage, WAL, verification and schema-readiness observations before requesting --until verified-ready; --until catching-up remains a copy lifecycle boundary.",
            )));
        }
        if definition.source.tls != crate::security::TlsPolicy::Required {
            return Err(RunError::Preparation(failure(
                FailureKind::Blocked,
                "Operational replication requires verified TLS and an explicit CA reference",
                "Use source tls=require with tls_ca_ref; the replication transport has no plaintext local-test adapter.",
            )));
        }
        if !matches!(definition.buffer.backend, BufferBackend::LocalFs(_)) {
            return Err(RunError::Preparation(failure(
                FailureKind::Blocked,
                "Cloud buffer credential payload contract is not qualified",
                "Qualify the credentials_ref adapter for S3/GCS; select a dedicated local-fs buffer for the current runner.",
            )));
        }
        let repl = replication(&prepared.source_config)?;
        let connection = Box::pin(source::connect(&repl)).await.map_err(recovery)?;
        let store = buffer(definition, true)?;
        let config = binding.reader_config(id);
        let order = binding.position_order(id, definition)?;
        let journal_objects = Arc::new(coordinator::journal::local_store(root)?);
        let raw = Journal::new(
            journal_objects.clone(),
            "skedaddle",
            id,
            Duration::from_secs(30),
        )?;
        let journal = raw.with_chunk_buffer(store.clone(), config.clone(), order.clone());
        let path = root.join(format!("{id}.local"));
        let local = if path.exists() {
            Local::open_journal(&path, &journal).await?
        } else {
            let mut local = Local::create(&path)?;
            local.replay_journal(&journal).await?;
            local
        };
        let exclusion = migration_exclusion(
            &binding,
            ExclusionBackend::Source(&prepared.source_config, &definition.source.tls),
        )?;
        let mut coordinator = operational_coordinator(journal.clone(), local, exclusion.clone())?;
        coordinator.acquire().await?;
        let holder = Arc::new(Mutex::new(coordinator));
        let authority =
            CoordinatorGcJournal::new(holder.clone(), store.clone(), config.clone(), order.clone())
                .await?;
        let writer_checkpoint = authority.writer_checkpoint().await?;
        let reader_checkpoint = authority.reader_checkpoint().await?;
        drop(authority);
        let mut coordinator = Arc::try_unwrap(holder)
            .map_err(|_| recovery("coordinator retained"))?
            .into_inner();
        let state = coordinator.state().clone();
        let reader_checkpoint = restart_reader_checkpoint(&state, reader_checkpoint)?;
        binding.validate_state(id, &state)?;
        let checkpoint = binding.postgres_checkpoint(&state)?;
        if resume && checkpoint.is_none() {
            return Err(recovery("resume requires saved slot checkpoint").into());
        }
        let origin = format!("skedaddle_{id}");
        let gigglesdb = definition.destination.engine == Engine::Gigglesdb;
        let Session {
            client: source_client,
            driver: source_driver,
        } = prepared.source;
        let Session {
            client: destination_client,
            driver: destination_driver,
        } = prepared.destination;
        let _drivers = (source_driver, destination_driver);
        if !gigglesdb {
            coordinator
                .effect(
                    EffectKind::BufferPublication,
                    format!("runner-origin:{origin}"),
                    |_| async {
                        if destination_client
                            .query_opt(
                                "SELECT 1 FROM pg_replication_origin WHERE roname=$1",
                                &[&origin],
                            )
                            .await?
                            .is_none()
                        {
                            destination_client
                                .query_one("SELECT pg_replication_origin_create($1)", &[&origin])
                                .await?;
                        }
                        Ok(())
                    },
                )
                .await?;
        }
        let engine = engine::Engine::new_with_exclusion(coordinator, journal.clone(), exclusion)
            .with_pause_control(PauseControl::new(root.join(format!("{id}.control")))?);
        let readiness =
            super::verification::Readiness::new(engine.runtime(), id, Some(store.clone()));
        let engine = Arc::new(engine.with_readiness_source(readiness.clone()));
        let verification = super::verification::Service {
            root: root.join(format!("{id}.control")),
            id: id.into(),
            definition: definition.clone(),
            source_config: prepared.source_config.clone(),
            destination_config: prepared.destination_config.clone(),
            tables: prepared.tables.clone(),
            runtime: engine.runtime(),
            journal: journal.clone(),
            readiness: readiness.clone(),
        };
        engine
            .enable_retention(
                store.clone(),
                config.clone(),
                order.clone(),
                RetentionPolicy {
                    retained_for_ms: definition.buffer_retention.min_seconds.saturating_mul(1000),
                    orphan_grace_ms: definition.buffer_retention.max_seconds.saturating_mul(1000),
                },
                Duration::from_secs(2),
            )
            .await?;
        let publication = format!("skedaddle_{id}");
        let slot = binding.stream.clone();
        let installer = PgInstaller {
            schema_tables: engine.runtime().scoped_schema_tables().await?,
            schema_baseline: engine
                .runtime()
                .state()
                .await
                .operational_schema
                .baseline
                .clone(),
            source: source_client,
            destination: destination_client,
            replication: connection,
            publication: publication.clone(),
            slot: slot.clone(),
            installation: id.into(),
            tables: prepared
                .tables
                .iter()
                .map(|t| t.key.relation.clone())
                .collect(),
            checkpoint,
        };
        let mut installer = DestinationInstaller {
            pg: installer,
            gigglesdb,
        };
        if !resume {
            Box::pin(engine.start(&mut installer)).await?;
        } else if installer.pg.schema_baseline.is_none() {
            return Err(coordinator::Error::RecoveryRequired("Missing durable scoped schema baseline; quiesce schema changes and restart from a fresh snapshot".into()).into());
        }
        engine
            .runtime()
            .state()
            .await
            .operational_schema
            .require_clear()
            .map_err(|e| coordinator::Error::RecoveryRequired(e.to_string()))?;
        let mut checkpoint = installer
            .pg
            .checkpoint
            .take()
            .ok_or_else(|| recovery("slot checkpoint missing"))?;
        if let Some(saved) = &writer_checkpoint {
            // Re-open the retained writer before choosing a source restart point.
            // Its manifest scan can recover publications newer than the checkpoint.
            if saved
                .last_position
                .as_ref()
                .is_some_and(|p| p.order_domain() != order.domain())
            {
                return Err(recovery("writer domain changed").into());
            }
        }
        let identity = SourceIdentity {
            engine: "postgres".into(),
            system_id: checkpoint.lineage.system_id.to_string(),
            lineage: checkpoint.lineage.timeline.to_string(),
            db_name: checkpoint.lineage.database.clone(),
            stream: slot,
        };
        let scratch = root.join(format!("{id}.scratch"));
        std::fs::create_dir_all(&scratch)?;
        let (feedback, _receive) = std::sync::mpsc::channel();
        let max_bytes = definition
            .budgets
            .max_snapshot_bytes
            .clamp(1, 16 * 1024 * 1024);
        let writer = Box::pin(BufferWriter::open_from_checkpoint(
            store.clone(),
            WriterConfig {
                prefix: PREFIX.into(),
                migration_id: id.into(),
                stream: config.stream.clone(),
                epoch: state.epoch,
                segment_bytes: max_bytes,
                segment_transactions: 100,
                segment_age: Duration::from_secs(1),
                max_transaction_bytes: max_bytes,
            },
            writer_checkpoint.unwrap_or_default(),
            order.clone(),
            feedback,
            scratch,
        ))
        .await
        .map_err(recovery)?;
        let recovered = writer.checkpoint().map_err(recovery)?;
        let mut classifiers = Vec::new();
        for table in &prepared.tables {
            let saved = if let Some(reference) = state.chunk_plans.get(&table.key.table) {
                journal.chunk_plan(reference).await?
            } else {
                if resume {
                    return Err(recovery("resume table plan missing").into());
                }
                let saved = plan(table, &mut installer.pg.source).await?;
                engine.plan(&saved).await?;
                saved
            };
            classifiers.push((
                table.key.table.clone(),
                table
                    .range_classifier(&saved, order.domain())
                    .map_err(recovery)?,
            ));
        }
        let mut capture = engine::capture(
            identity.clone(),
            order.domain().into(),
            checkpoint.durable_lsn,
            writer,
            Resolver(prepared.tables.clone()),
            id.into(),
            SchemaVersions::new(id.into(), 0, []).map_err(recovery)?,
        )?;
        // The recovered writer selects the exact retained classifier and end LSN.
        // An empty baseline is valid only at genesis; restore fails closed if a
        // non-genesis boundary has no matching authenticated schema snapshot.
        let schema_stream = serde_json::json!([PREFIX, id, config.stream]).to_string();
        Box::pin(capture.restore_schema(source::SchemaJournalBinding {
            journal: journal.clone(),
            runtime: engine.runtime(),
            objects: journal_objects.clone(),
            stream: schema_stream.clone(),
        }))
        .await
        .map_err(recovery)?;
        checkpoint.durable_lsn = capture.durable_lsn();
        // Release setup replication ownership before reconnecting the live slot.
        drop(installer);
        let transport = Box::pin(source::reconnect(&repl, &checkpoint, &[publication]))
            .await
            .map_err(recovery)?;
        let (_admission, admitted) = watch::channel(false);
        let capture = PgCapture::new(
            capture,
            transport,
            admitted,
            journal_objects,
            schema_stream,
            order.clone(),
            identity.clone(),
        );
        let order = capture.position_order();
        let monitor = super::cutover::Monitor::new(&engine.runtime().state().await)?;
        let capture = monitor.capture(
            capture,
            engine.runtime(),
            id.into(),
            format!("skedaddle_{id}_rev"),
        );
        let mut capture = readiness.capture(capture, identity.clone(), order.domain().into());
        let admin = Box::pin(session(&prepared.source_config, &definition.source))
            .await
            .map_err(|_| recovery("chunk session"))?;
        let reader = Box::pin(session(&prepared.source_config, &definition.source))
            .await
            .map_err(|_| recovery("chunk session"))?;
        settings(&admin.client)
            .await
            .map_err(|_| recovery("chunk settings"))?;
        settings(&reader.client)
            .await
            .map_err(|_| recovery("chunk settings"))?;
        if prepared.tables.is_empty() {
            return Err(recovery("no qualified tables").into());
        }
        let mut chunks = Chunks {
            pg: PgChunks {
                admin: admin.client,
                reader: reader.client,
                tables: prepared.tables.clone(),
            },
            _drivers: (admin.driver, reader.driver),
        };
        let tables = prepared
            .tables
            .iter()
            .map(|t| t.destination.clone())
            .collect::<Vec<_>>();
        let plan_origin = origin.clone();
        // Destination receipt keys contain stream without a migration column.
        // Keep the exact migration-qualified identity across all buffer bindings.
        let stream = config.stream.clone();
        let plan_stream = stream.clone();
        let positions =
            apply::SourcePositions::new(order.clone(), apply::OriginPolicy::NativePgLsn);
        let plan_positions = positions.clone();
        let planner = move |txn: &AssembledTxn, mseq, state: &Materialized| {
            if gigglesdb {
                crate::gigglesdb::engine::check_values(txn, &tables)?;
            }
            let plan = apply::plan_transaction_with_ddl(
                txn,
                &tables,
                &apply::PlanContext {
                    positions: plan_positions.clone(),
                    stream: plan_stream.clone(),
                    mseq,
                    origin: plan_origin.clone(),
                    synthetic_commit_time: Some(0),
                },
                id,
                |table, row| {
                    classifiers
                        .iter()
                        .find(|(name, _)| name == &table.event_table)
                        .ok_or(apply::ApplyError::Invalid("unqualified apply table"))?
                        .1
                        .classify(row, &txn.commit_position, state)
                        .map(|s| s.apply_state())
                },
            )?;
            if gigglesdb {
                crate::gigglesdb::apply::plan_for_gigglesdb(plan)
            } else {
                Ok(plan)
            }
        };
        let connector = Connect {
            config: prepared.destination_config.clone(),
            connection: definition.destination.clone(),
            drivers: vec![],
        };
        let executor = if gigglesdb {
            RunnerDestination::Gigglesdb(
                GigglesdbExecutor::new(connector, stream.clone(), 1000, positions).map_err(recovery)?,
            )
        } else {
            RunnerDestination::Postgresql(
                Executor::new(connector, stream.clone(), origin, 1000, positions).map_err(recovery)?,
            )
        };
        let mut reader_config = config;
        reader_config.required_next_mseq = recovered.next_mseq;
        let mut applier = PgApplier::new(
            BufferReader::new(
                store,
                reader_config,
                reader_checkpoint.unwrap_or_default(),
                order.clone(),
            )
            .map_err(recovery)?,
            stream,
            executor,
            planner,
        );
        let rate = RateLimit {
            per_second: max_bytes,
            burst: max_bytes,
        };
        let reads = ReadLimits {
            rows: RateLimit {
                per_second: READ_ROWS,
                burst: READ_ROWS,
            },
            bytes: rate,
        };
        let mut budget = ThroughputBudget::new(
            Limits {
                chunk_reads: reads,
                verification_reads: reads,
                applier_transactions: rate,
                connections: Default::default(),
                in_memory_segments: 2,
            },
            Arc::new(SystemClock::default()),
        )
        .map_err(recovery)?;
        if resume && engine.runtime().state().await.lifecycle.state == State::Paused {
            engine.resume().await?;
        }
        let (joined_pause, joined_requested) = watch::channel(*pause.borrow());
        let relay_pause = async {
            while pause.changed().await.is_ok() {
                if *pause.borrow_and_update() {
                    joined_pause.send_replace(true);
                }
            }
            std::future::pending::<()>().await;
        };
        let (joined, workers_joined) = watch::channel(false);
        let cutover = super::cutover::Service {
            engine: engine.clone(),
            root: root.join(format!("{id}.control")),
            id: id.into(),
            definition: definition.clone(),
            source_config: prepared.source_config.clone(),
            destination_config: prepared.destination_config.clone(),
            tables: prepared.tables.clone(),
            journal: journal.clone(),
            source: identity.clone(),
            monitor,
            pause: joined_pause.clone(),
            joined: workers_joined.clone(),
        };
        let copy_config = CopyConfig {
            source: identity,
            order_domain: order.domain().into(),
            schema_version: 0,
            read_rows: READ_ROWS,
            read_bytes: max_bytes,
            window_bytes: max_bytes as usize,
        };
        let fenced = matches!(
            engine.runtime().state().await.lifecycle.state,
            State::SourceFenced | State::FinalBoundaryApplied
        );
        let operation = async {
            let result = if fenced {
                engine
                    .run_fenced_until(
                        &mut capture,
                        &mut chunks,
                        &mut applier,
                        copy_config,
                        &mut budget,
                        joined_requested,
                        StopAt::UntilPaused,
                    )
                    .await
            } else {
                engine
                    .run_until(
                        &mut capture,
                        &mut chunks,
                        &mut applier,
                        copy_config,
                        &mut budget,
                        joined_requested,
                        stop,
                    )
                    .await
            };
            joined.send_replace(true);
            result
        };
        // Never drop a running blocking cutover task: the service finishes its
        // guarded operation even when the forward workers stop independently.
        let services = async {
            let gc = super::gc::serve(
                engine.runtime(),
                root.join(format!("{id}.control")),
                workers_joined,
            );
            tokio::pin!(gc);
            let mut cutover_service = Box::pin(cutover.serve());
            let result = tokio::select! {
                result = &mut cutover_service => result,
                result = &mut gc => {
                    joined_pause.send_replace(true);
                    let cutover_result = cutover_service.await;
                    result.and(cutover_result)
                },
                result = Box::pin(verification.serve()) => {
                    joined_pause.send_replace(true);
                    // A live cutover must finish its guarded operation; dropping
                    // spawn_blocking's JoinHandle would detach database effects.
                    let cutover_result = cutover_service.await;
                    result.and(cutover_result)
                },
            };
            joined_pause.send_replace(true);
            result
        };
        let operation_and_services = async {
            let (result, service) = tokio::join!(operation, services);
            match (result, service) {
                (Ok(_), Err(error)) => Err(error),
                (result, _) => result,
            }
        };
        let result = tokio::select! {
            result = Box::pin(operation_and_services) => result,
            () = relay_pause => unreachable!("pause relay remains pending after its input closes"),
        };
        verification.publish_terminal_readiness().await?;
        applier.destination.close().await.map_err(recovery)?;
        let final_state = engine.runtime().state().await.lifecycle.state;
        if result.is_ok() && super::post_activation::owns(final_state) {
            return Box::pin(super::post_activation::resume(root, id, definition, pause)).await;
        }
        result
            .map(|mut reached| {
                reached.state = final_state;
                reached
                    .blockers
                    .push("buffer-budget-accounting-unavailable".into());
                reached
            })
            .map_err(RunError::Engine)
    })
}

// Receipt collection follows ordinary durable apply progress, not GC cadence.
// Restarting behind it could require receipts that were correctly collected.
fn restart_reader_checkpoint(
    state: &Materialized,
    retained: Option<ReaderCheckpoint>,
) -> engine::Result<Option<ReaderCheckpoint>> {
    let applied = state
        .progress
        .get("applied")
        .map(|(marker, _)| engine::decode_reader_checkpoint(marker))
        .transpose()?;
    match (applied, retained) {
        (Some(applied), Some(retained)) => {
            if applied.next_mseq == retained.next_mseq && applied != retained {
                return Err(recovery("conflicting durable reader checkpoints"));
            }
            Ok(Some(if applied.next_mseq >= retained.next_mseq {
                applied
            } else {
                retained
            }))
        }
        (Some(applied), None) => Ok(Some(applied)),
        (None, retained) => Ok(retained),
    }
}

#[cfg(test)]
mod exclusion_tests {
    use super::*;

    #[test]
    fn restart_uses_durable_apply_progress_when_gc_is_absent_or_behind() {
        let checkpoint = |n: u64| ReaderCheckpoint {
            next_mseq: n,
            prev_digest: [n as u8; 32],
            last_epoch: 1,
            last_position: Some(crate::event::Position::new("pg-test", n.to_be_bytes())),
            last_record: Some(format::RecordBoundary {
                source: SourceIdentity {
                    engine: "postgres".into(),
                    system_id: "1".into(),
                    lineage: "1".into(),
                    db_name: "db".into(),
                    stream: "slot".into(),
                },
                chunk_rows: false,
            }),
            ..ReaderCheckpoint::default()
        };
        let applied = checkpoint(4);
        let mut state = Materialized::default();
        state.progress.insert(
            "applied".into(),
            (
                engine::encode_reader_checkpoint(&applied).unwrap(),
                "4".into(),
            ),
        );
        for retained in [None, Some(checkpoint(2)), Some(applied.clone())] {
            assert_eq!(
                restart_reader_checkpoint(&state, retained).unwrap(),
                Some(applied.clone())
            );
        }
        assert_eq!(
            restart_reader_checkpoint(&state, Some(checkpoint(5))).unwrap(),
            Some(checkpoint(5))
        );
        let mut conflicting = applied.clone();
        conflicting.prev_digest = [99; 32];
        assert!(restart_reader_checkpoint(&state, Some(conflicting)).is_err());
        state.progress.get_mut("applied").unwrap().0 = "corrupt".into();
        assert!(restart_reader_checkpoint(&state, Some(checkpoint(2))).is_err());
        state.progress.clear();
        assert_eq!(
            restart_reader_checkpoint(&state, Some(checkpoint(2))).unwrap(),
            Some(checkpoint(2))
        );
    }

    #[tokio::test]
    async fn operational_coordinator_configures_exclusion_before_acquire() {
        let root = PathBuf::from(std::env::var_os("SKEDADDLE_TEST_ROOT").expect("task test root"))
            .join(format!("runner-exclusion-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let binding = SourceBinding {
            version: 1,
            connector: SourceConnector::Postgres,
            order_domain: "pg-test".into(),
            stream: "slot".into(),
        };
        let exclusion =
            migration_exclusion(&binding, ExclusionBackend::File(&root.join("locks"))).unwrap();
        let journal = Journal::new(
            Arc::new(coordinator::journal::local_store(&root).unwrap()),
            "runner",
            "migration",
            Duration::from_secs(5),
        )
        .unwrap();
        let mut coordinator = operational_coordinator(
            journal.clone(),
            Local::create(root.join("local")).unwrap(),
            exclusion.clone(),
        )
        .unwrap();
        let guard = exclusion.acquire("migration").await.unwrap();
        {
            let acquisition = coordinator.acquire();
            tokio::pin!(acquisition);
            // Observe that acquire cannot append while an actual OS lock is held.
            assert!(
                tokio::time::timeout(Duration::from_millis(100), &mut acquisition)
                    .await
                    .is_err()
            );
            assert_eq!(journal.load(true).await.unwrap().epoch, 0);
            drop(guard);
            acquisition.await.unwrap();
        }
        let guard = coordinator.exclusive().await.unwrap();
        assert_eq!(guard.owner().unwrap().1, 1);
    }

    fn binding_definition() -> MigrationDefinition {
        let readme = include_str!("../../README.md");
        serde_json::from_str(
            readme
                .split("```json")
                .nth(1)
                .unwrap()
                .split("```")
                .next()
                .unwrap(),
        )
        .unwrap()
    }
    fn binding_root() -> PathBuf {
        let root = PathBuf::from(std::env::var_os("SKEDADDLE_TEST_ROOT").expect("task test root"))
            .join(format!("runtime-binding-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        root
    }
    async fn defined_journal(
        root: &Path,
        id: &str,
        definition: MigrationDefinition,
    ) -> Coordinator {
        let journal = Journal::new(
            Arc::new(coordinator::journal::local_store(root).unwrap()),
            "skedaddle",
            id,
            Duration::from_secs(5),
        )
        .unwrap();
        let mut owner = Coordinator::observer(
            journal,
            Local::create(root.join("seed-local")).unwrap(),
            "binding-test",
        )
        .unwrap();
        owner.acquire().await.unwrap();
        owner
            .commit(coordinator::Transition::Define {
                definition: Box::new(definition),
            })
            .await
            .unwrap();
        owner
    }
    fn assert_binding_error<T>(result: coordinator::Result<T>, expected: SourceBindingError) {
        match result {
            Err(coordinator::Error::SourceBinding(error)) => assert_eq!(error, expected),
            Err(error) => panic!("unexpected error: {error}"),
            Ok(_) => panic!("expected binding rejection"),
        }
    }
    #[test]
    fn postgres_binding_preserves_startup_and_journal_golden_bytes() {
        let definition = binding_definition();
        let binding = SourceBinding::for_definition("golden", &definition).unwrap();
        assert_eq!(binding.connector, SourceConnector::Postgres);
        assert_eq!(binding.stream, "skedaddle_golden");
        assert_eq!(
            binding.reader_config("golden").stream,
            "skedaddle-golden-wal"
        );
        let order = binding.position_order("golden", &definition).unwrap();
        assert_eq!(order.domain(), "skedaddle-postgres:golden");
        assert_eq!(
            order
                .compare(&1u64.to_be_bytes(), &256u64.to_be_bytes())
                .unwrap(),
            Ordering::Less
        );
        assert!(order.compare(b"not an LSN", b"bad").is_err());
        let checkpoint = crate::pg::replication::Checkpoint {
            lineage: crate::pg::replication::Lineage {
                system_id: 42,
                timeline: 3,
                xlogpos: 256,
                database: "db".into(),
            },
            slot: binding.stream.clone(),
            durable_lsn: 256,
        };
        let marker = engine::encode_source_checkpoint(&checkpoint).unwrap();
        assert_eq!(
            marker,
            r#"{"system_id":42,"timeline":3,"database":"db","slot":"skedaddle_golden","durable_lsn":256}"#
        );
        let transition = coordinator::Transition::Progress {
            stream: "source-slot".into(),
            marker: marker.clone(),
            position: "installed".into(),
        };
        let golden = r#"{"version":3,"seq":7,"epoch":2,"prev_digest":"abc","transition":{"kind":"progress","stream":"source-slot","marker":"{\"system_id\":42,\"timeline\":3,\"database\":\"db\",\"slot\":\"skedaddle_golden\",\"durable_lsn\":256}","position":"installed"},"at":1234,"coordinator_instance":"runner-golden","digest":"07c70bd2c6f60c799e4458db3f769ead19eef7e3270202d412232911a40e64e3"}"#;
        let entry: coordinator::journal::Entry = serde_json::from_str(golden).unwrap();
        entry.validate().unwrap();
        assert_eq!(entry.transition, transition);
        assert_eq!(serde_json::to_string(&entry).unwrap(), golden);
        let mut state = Materialized::default();
        state
            .progress
            .insert("source-slot".into(), (marker, "installed".into()));
        assert_eq!(
            binding.postgres_checkpoint(&state).unwrap().unwrap(),
            checkpoint
        );
        assert!(
            !serde_json::to_string(&state)
                .unwrap()
                .contains("source_binding")
        );
    }
    #[tokio::test]
    async fn mysql_run_journals_binding_before_not_wired_and_reopens_without_effects() {
        let root = binding_root();
        let mut definition = binding_definition();
        definition.source = serde_json::from_value(serde_json::json!({
            "engine": "mysql", "version": "8.4", "tls_ca_ref": "env:MYSQL_CA",
            "mysql": { "host": "localhost", "port": 3306, "user": "env:MYSQL_USER",
                "password": "env:MYSQL_PASSWORD", "gtid_mode": "ON" }
        }))
        .unwrap();
        let binding = SourceBinding::for_definition("mysql", &definition).unwrap();
        let owner = defined_journal(&root, "mysql", definition.clone()).await;
        drop(owner);
        for resume in [false, true] {
            let (_send, pause) = watch::channel(false);
            let result = run(
                &root,
                "mysql",
                &definition,
                resume,
                StopAt::CatchingUp,
                pause,
            )
            .await;
            assert!(matches!(
                result,
                Err(RunError::Engine(coordinator::Error::SourceBinding(
                    SourceBindingError::NotWired(SourceConnector::Mysql)
                )))
            ));
        }
        let journal = open_journal(&root, "mysql").await.unwrap();
        let state = journal.load(false).await.unwrap();
        assert_eq!(state.source_binding.as_ref(), Some(&binding));
        assert_eq!(state.lifecycle.state, State::Defined);
        assert!(state.progress.is_empty());
        assert!(state.intent_roots.is_empty());
        let entries = journal.history_page(0, 128).await.unwrap();
        assert_eq!(
            entries
                .iter()
                .filter(|e| matches!(
                    e.transition,
                    coordinator::Transition::SourceBindingV1 { .. }
                ))
                .count(),
            1
        );
        assert!(
            !entries
                .iter()
                .any(|e| matches!(e.transition, coordinator::Transition::Intent { .. }))
        );
        let record = serde_json::to_value(&entries.last().unwrap().transition).unwrap();
        assert_eq!(record["kind"], "skedaddle-source-binding-v1");
        journal.checkpoint(&state).await.unwrap();
        assert_eq!(
            open_journal(&root, "mysql")
                .await
                .unwrap()
                .load(true)
                .await
                .unwrap(),
            state
        );
        let order = binding.position_order("mysql", &definition).unwrap();
        assert_eq!(order.domain(), "mysql:mysql/gtid");
        // Even a valid legacy PG marker must never select a PG decoder for MySQL.
        assert_binding_error(
            binding.postgres_checkpoint(&state),
            SourceBindingError::NotWired(SourceConnector::Mysql),
        );
        definition.source.engine = Engine::Postgresql;
        for resume in [false, true] {
            let (_send, pause) = watch::channel(false);
            assert!(matches!(
                run(
                    &root,
                    "mysql",
                    &definition,
                    resume,
                    StopAt::CatchingUp,
                    pause
                )
                .await,
                Err(RunError::Engine(coordinator::Error::SourceBinding(
                    SourceBindingError::Connector
                )))
            ));
        }
    }
    #[tokio::test]
    async fn reopen_rejects_changed_order_domain_or_stream_in_startup_record() {
        for (field, expected) in [
            ("domain", SourceBindingError::OrderDomain),
            ("stream", SourceBindingError::Stream),
        ] {
            let root = binding_root();
            let mut definition = binding_definition();
            definition.source = serde_json::from_value(serde_json::json!({
                "engine": "mysql", "version": "8.4", "tls_ca_ref": "env:MYSQL_CA",
                "mysql": { "host": "localhost", "port": 3306, "user": "env:MYSQL_USER",
                    "password": "env:MYSQL_PASSWORD", "gtid_mode": "ON" }
            }))
            .unwrap();
            let mut binding = SourceBinding::for_definition("mysql", &definition).unwrap();
            if field == "domain" {
                binding.order_domain.push_str("-different");
            } else {
                binding.stream.push_str("-different");
            }
            let mut owner = defined_journal(&root, "mysql", definition).await;
            owner
                .commit(coordinator::Transition::SourceBindingV1 { binding })
                .await
                .unwrap();
            assert_binding_error(open_journal(&root, "mysql").await, expected);
        }
    }
    #[tokio::test]
    async fn binding_record_rejects_wrong_connector_unknown_version_replacement_and_late_startup() {
        let root = binding_root();
        let mut definition = binding_definition();
        definition.source = serde_json::from_value(serde_json::json!({
            "engine": "mysql", "version": "8.4", "tls_ca_ref": "env:MYSQL_CA",
            "mysql": { "host": "localhost", "port": 3306, "user": "env:MYSQL_USER",
                "password": "env:MYSQL_PASSWORD", "gtid_mode": "ON" }
        }))
        .unwrap();
        let binding = SourceBinding::for_definition("mysql", &definition).unwrap();
        let mut owner = defined_journal(&root, "mysql", definition).await;
        assert_binding_error(
            owner
                .effect(
                    EffectKind::BufferPublication,
                    "must-not-run".into(),
                    |_| async { Ok(()) },
                )
                .await,
            SourceBindingError::Missing,
        );
        for (mut changed, expected) in [
            (binding.clone(), SourceBindingError::Connector),
            (binding.clone(), SourceBindingError::Version(2)),
        ] {
            match expected {
                SourceBindingError::Connector => changed.connector = SourceConnector::Spanner,
                _ => changed.version = 2,
            }
            assert_binding_error(
                owner
                    .commit(coordinator::Transition::SourceBindingV1 { binding: changed })
                    .await,
                expected,
            );
        }
        owner
            .commit(coordinator::Transition::SourceBindingV1 {
                binding: binding.clone(),
            })
            .await
            .unwrap();
        let mut changed = binding.clone();
        changed.stream.push_str("-other");
        assert_binding_error(
            owner
                .commit(coordinator::Transition::SourceBindingV1 { binding: changed })
                .await,
            SourceBindingError::Stream,
        );
        assert_binding_error(
            owner
                .commit(coordinator::Transition::SourceBindingV1 { binding })
                .await,
            SourceBindingError::Invalid,
        );
        let root = binding_root();
        let mut definition = binding_definition();
        definition.source = serde_json::from_value(serde_json::json!({
            "engine": "mysql", "version": "8.4", "tls_ca_ref": "env:MYSQL_CA",
            "mysql": { "host": "localhost", "port": 3306, "user": "env:MYSQL_USER",
                "password": "env:MYSQL_PASSWORD", "gtid_mode": "ON" }
        }))
        .unwrap();
        let binding = SourceBinding::for_definition("mysql", &definition).unwrap();
        let mut owner = defined_journal(&root, "mysql", definition).await;
        owner
            .commit(coordinator::Transition::Progress {
                stream: "source-slot".into(),
                marker: "legacy-PG".into(),
                position: "installed".into(),
            })
            .await
            .unwrap();
        assert_binding_error(
            owner
                .commit(coordinator::Transition::SourceBindingV1 { binding })
                .await,
            SourceBindingError::Missing,
        );
        assert_binding_error(
            open_journal(&root, "mysql").await,
            SourceBindingError::Missing,
        );
    }
    #[test]
    fn non_postgres_authority_is_explicitly_unsupported_and_spanner_has_its_native_order() {
        let root = binding_root();
        let mut definition = binding_definition();
        definition.source = serde_json::from_value(serde_json::json!({
            "engine": "mysql", "version": "8.4", "tls_ca_ref": "env:MYSQL_CA",
            "mysql": { "host": "localhost", "port": 3306, "user": "env:MYSQL_USER",
                "password": "env:MYSQL_PASSWORD", "gtid_mode": "ON" }
        }))
        .unwrap();
        let binding = SourceBinding::for_definition("mysql", &definition).unwrap();
        assert_binding_error(
            migration_exclusion(&binding, ExclusionBackend::File(&root.join("locks"))),
            SourceBindingError::ExclusionUnsupported(SourceConnector::Mysql),
        );
        definition.source.engine = Engine::Spanner;
        definition.source.spanner = Some(crate::definition::SpannerEndpoint {
            project: "project".into(),
            instance: "instance".into(),
            database: "database".into(),
            dialect: crate::definition::SpannerDialect::GoogleSql,
            change_stream: "changes".into(),
            credentials: crate::definition::SecretRef::parse("env:SPANNER_BINDING_TEST").unwrap(),
            emulator: true,
        });
        let binding = SourceBinding::for_definition("spanner", &definition).unwrap();
        assert_eq!(binding.stream, "changes");
        assert_eq!(
            binding
                .position_order("spanner", &definition)
                .unwrap()
                .domain(),
            "spanner:project/instance/database/changes/0"
        );
        assert_binding_error(
            migration_exclusion(&binding, ExclusionBackend::File(&root.join("locks"))),
            SourceBindingError::ExclusionUnsupported(SourceConnector::Spanner),
        );
    }
    #[tokio::test]
    async fn reopening_invalid_persisted_connector_or_version_keeps_the_typed_error() {
        for (expected, checkpoint) in [
            SourceBindingError::Connector,
            SourceBindingError::Version(9),
        ]
        .into_iter()
        .flat_map(|error| [false, true].map(move |checkpoint| (error.clone(), checkpoint)))
        {
            let root = binding_root();
            let mut definition = binding_definition();
            definition.source = serde_json::from_value(serde_json::json!({
                "engine": "mysql", "version": "8.4", "tls_ca_ref": "env:MYSQL_CA",
                "mysql": { "host": "localhost", "port": 3306, "user": "env:MYSQL_USER",
                    "password": "env:MYSQL_PASSWORD", "gtid_mode": "ON" }
            }))
            .unwrap();
            let mut binding = SourceBinding::for_definition("mysql", &definition).unwrap();
            let owner = defined_journal(&root, "mysql", definition).await;
            match expected {
                SourceBindingError::Connector => binding.connector = SourceConnector::Postgres,
                _ => binding.version = 9,
            }
            let entry = coordinator::journal::Entry::new(
                owner.state(),
                coordinator::Transition::SourceBindingV1 { binding },
                owner.instance_id(),
            )
            .unwrap();
            let journal = Journal::new(
                Arc::new(coordinator::journal::local_store(&root).unwrap()),
                "skedaddle",
                "mysql",
                Duration::from_secs(5),
            )
            .unwrap();
            assert_binding_error(journal.put(&entry).await, expected.clone());
            if checkpoint {
                journal.checkpoint(owner.state()).await.unwrap();
            }
            // Real corrupt journal evidence: simulate an otherwise correctly hashed
            // entry from an incompatible writer, bypassing publication admission.
            std::fs::write(
                root.join(journal.entry_key(entry.seq).as_ref()),
                serde_json::to_vec(&entry).unwrap(),
            )
            .unwrap();
            assert_binding_error(open_journal(&root, "mysql").await, expected);
        }
    }
}
