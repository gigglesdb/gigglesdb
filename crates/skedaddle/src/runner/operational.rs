use super::*;
use crate::{
    budget::{Limits, RateLimit, ReadLimits, SystemClock, ThroughputBudget},
    buffer::{
        backends::ObjectBufferStore,
        format,
        gc::RetentionPolicy,
        reader::{BufferReader, ReaderConfig},
        writer::{BufferWriter, WriterConfig},
    },
    coordinator::{
        self, EffectKind, Materialized, State, epoch::Coordinator,
        gc_authority::CoordinatorGcJournal, journal::Journal, local::Local,
    },
    definition::BufferBackend,
    engine::{
        self, CopyConfig, PauseControl, PgApplier, PgCapture, PgChunks, PgInstaller, Reached,
        StopAt,
    },
    event::{AssembledTxn, EventError, PositionOrder, Row, SourceIdentity},
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
    backend: ExclusionBackend<'_>,
) -> coordinator::Result<Arc<coordinator::exclusion::MigrationExclusion>> {
    use coordinator::exclusion::MigrationExclusion;
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
struct Order(String);
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
fn reader_config(id: &str) -> ReaderConfig {
    ReaderConfig {
        prefix: PREFIX.into(),
        migration_id: id.into(),
        stream: format!("skedaddle-{id}-wal"),
        limits: format::Limits::default(),
        required_next_mseq: 0,
    }
}
fn order(id: &str) -> Arc<dyn PositionOrder + Send + Sync> {
    Arc::new(Order(format!("skedaddle-postgres:{id}")))
}
fn buffer(
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
    if raw.load(true).await.is_ok() {
        return Ok(raw);
    }
    let entries = raw.history_page(0, 128).await?;
    if let Some(definition) = entries.iter().find_map(|e| match &e.transition {
        coordinator::Transition::Define { definition } => Some(definition),
        _ => None,
    }) {
        let store = buffer(definition, false)?;
        return Ok(raw.with_chunk_buffer(store, reader_config(id), order(id)));
    }
    // Retain the journal's original integrity diagnostic on the caller's load.
    Ok(raw)
}

struct Resolver(Vec<catalog::TableCatalog>);
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
struct Connect {
    config: tls::Config,
    connection: Connection,
    drivers: Vec<Driver>,
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
pub async fn run(
    root: &Path,
    id: &str,
    definition: &MigrationDefinition,
    resume: bool,
    stop: StopAt,
    mut pause: watch::Receiver<bool>,
) -> Result<Reached, RunError> {
    // Terminal replay is authenticated before connections or copy workers start.
    if resume {
        let terminal = open_journal(root, id).await?.load(true).await?;
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
            return super::post_activation::resume(root, id, definition, pause).await;
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
    let config = reader_config(id);
    let order = order(id);
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
    let exclusion = migration_exclusion(ExclusionBackend::Source(
        &prepared.source_config,
        &definition.source.tls,
    ))?;
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
    let checkpoint = state
        .progress
        .get("source-slot")
        .map(|(m, _)| engine::decode_source_checkpoint(m))
        .transpose()?;
    if resume && checkpoint.is_none() {
        return Err(recovery("resume requires saved slot checkpoint").into());
    }
    let origin = format!("skedaddle_{id}");
    let Session {
        client: source_client,
        driver: source_driver,
    } = prepared.source;
    let Session {
        client: destination_client,
        driver: destination_driver,
    } = prepared.destination;
    let _drivers = (source_driver, destination_driver);
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
    let engine = engine::Engine::new_with_exclusion(coordinator, journal.clone(), exclusion)
        .with_pause_control(PauseControl::new(root.join(format!("{id}.control")))?);
    let readiness = super::verification::Readiness::new(engine.runtime(), id, Some(store.clone()));
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
            Duration::from_millis(50),
        )
        .await?;
    let publication = format!("skedaddle_{id}");
    let slot = format!("skedaddle_{id}");
    let mut installer = PgInstaller {
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
    if !resume {
        Box::pin(engine.start(&mut installer)).await?;
    }
    let mut checkpoint = installer
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
        slot,
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
            let saved = plan(table, &mut installer.source).await?;
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
    let capture = PgCapture::new(capture, transport, admitted, journal_objects, schema_stream);
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
    let planner = move |txn: &AssembledTxn, mseq, state: &Materialized| {
        apply::plan_transaction_with_ddl(
            txn,
            &tables,
            &apply::PlanContext {
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
        )
    };
    let executor = Executor::new(
        Connect {
            config: prepared.destination_config.clone(),
            connection: definition.destination.clone(),
            drivers: vec![],
        },
        stream.clone(),
        origin,
        1000,
    )
    .map_err(recovery)?;
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
        return super::post_activation::resume(root, id, definition, pause).await;
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
}

#[cfg(test)]
mod exclusion_tests {
    use super::*;

    #[tokio::test]
    async fn operational_coordinator_configures_exclusion_before_acquire() {
        let root = PathBuf::from(std::env::var_os("SKEDADDLE_TEST_ROOT").expect("task test root"))
            .join(format!("runner-exclusion-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let exclusion = migration_exclusion(ExclusionBackend::File(&root.join("locks"))).unwrap();
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
}
