//! Preparation and joined copy/capture/apply for the MySQL forward runner.
use super::*;
use crate::{
    capability::ItemStatus,
    definition::MysqlGtidMode,
    mysql::{
        capture::StartupInputs,
        client::{MysqlConfig, MysqlVersion},
        dest_schema::{self, MappedSchema},
        preflight::{self, MysqlCheck, Options},
        schema::{TableDescriptor, load_descriptors},
        startup::{GtidMode, SelectedTable},
    },
};

pub const MYSQL_MAPPING_VERSION: &str = "mysql.v1";
pub const VALUE_MAPPING_VERSION: &str = "skva.v1";

/// Owns the destination session so its driver remains alive for the next stage.
/// Intentionally has no Debug implementation: connection configs hold secrets.
pub struct MysqlPrepared {
    pub mysql_config: MysqlConfig,
    pub destination_config: tls::Config,
    pub descriptors: Vec<TableDescriptor>,
    pub mapped_schema: MappedSchema,
    pub startup_inputs: StartupInputs,
    pub gtid_mode: GtidMode,
    pub version: MysqlVersion,
    destination: Session,
}
impl MysqlPrepared {
    pub fn destination(&self) -> &Client {
        &self.destination.client
    }
}
fn mysql_infrastructure() -> Failure {
    failure(
        FailureKind::Infrastructure,
        "MySQL preparation connection or query failed (details redacted)",
        "Check endpoint reachability, TLS trust/hostname, credentials and catalog permissions.",
    )
}

/// Apply the same observed-column and operational-key rules at assess and start.
pub(super) fn project_descriptor(
    object: &crate::definition::ObjectSelector,
    descriptor: &mut TableDescriptor,
    redactor: &Redactor,
) -> Result<(), Failure> {
    let projection = selection::from_observed(
        object,
        descriptor.columns.iter().map(|c| c.name.clone()).collect(),
        redactor,
    )?;
    if let Some(key) = descriptor
        .primary_key
        .iter()
        .find(|k| !projection.columns.contains(k))
    {
        return Err(Failure {
            kind: FailureKind::Blocked,
            what: "MySQL primary-key column cannot be excluded".into(),
            next_step: "Retain every primary-key column.".into(),
            table: Some(redactor.redact(&format!("{}.{}", object.schema, object.name))),
            column: Some(redactor.redact(key)),
        });
    }
    descriptor
        .columns
        .retain(|c| projection.columns.contains(&c.name));
    Ok(())
}

/// Estimates and decoder limits must be supplied explicitly by the caller.
/// No installer DDL, capture stream or apply work is started here.
pub async fn prepare(
    definition: &MigrationDefinition,
    mut options: Options,
) -> Result<MysqlPrepared, Failure> {
    if definition.source.engine != Engine::Mysql
        || definition.destination.engine != Engine::Postgresql
    {
        return Err(failure(
            FailureKind::Blocked,
            "MySQL preparation requires a PostgreSQL destination",
            "Select the qualified MySQL to PostgreSQL connector pair.",
        ));
    }
    let mut redactor = Redactor::default();
    let mysql_config =
        super::mysql_config(definition, options.max_decompressed_bytes, &mut redactor)?;
    let destination_config = config(&definition.destination, &mut redactor)?;
    let operation = async {
        let mut client = mysql_config
            .connect()
            .await
            .map_err(|_| mysql_infrastructure())?;
        let destination = Box::pin(session(&destination_config, &definition.destination)).await?;
        settings(&destination.client).await?;
        if options.estimated_copy_seconds == 0
            || options.planned_catchup_seconds == 0
            || options
                .estimated_copy_seconds
                .checked_add(options.planned_catchup_seconds)
                .is_none()
            || options.max_decompressed_bytes == 0
        {
            return Err(failure(
                FailureKind::InvalidInput,
                "Explicit nonzero copy/catch-up estimates and decoder bound required",
                "Supply measured copy and catch-up planning estimates and a bounded decoder size.",
            ));
        }
        if definition
            .selected_objects
            .iter()
            .any(|o| o.kind != ObjectKind::Table)
        {
            return Err(failure(
                FailureKind::Blocked,
                "MySQL preparation requires selected tables",
                "Remove non-table objects.",
            ));
        }
        options.selected_tables = definition.selected_objects.clone();
        let snapshot = preflight::collect(&mut client, &options).await;
        if let Some(item) = preflight::classify(&snapshot, &options)
            .into_iter()
            .find(|i| {
                !matches!(
                    i.status,
                    ItemStatus::Supported | ItemStatus::SupportedWithTransformation
                )
            })
        {
            return Err(failure(
                FailureKind::Blocked,
                &format!("MySQL preflight blocked: {}", item.id),
                &redactor.redact(&item.remediation),
            ));
        }
        let version = client.version;
        let family = match version {
            MysqlVersion::V80 => "8.0",
            MysqlVersion::V84 => "8.4",
        };
        if family != definition.source.version {
            return Err(failure(
                FailureKind::Blocked,
                "MySQL declared version differs from observed version family",
                "Correct the declared version family.",
            ));
        }
        let mode = snapshot
            .observations
            .iter()
            .find(|o| o.check == MysqlCheck::GtidMode)
            .and_then(|o| o.rows.first())
            .and_then(|r| r["value"].as_str());
        let gtid_mode = match mode {
            Some("ON") => GtidMode::ON,
            Some("OFF") => GtidMode::OFF,
            _ => {
                return Err(failure(
                    FailureKind::Blocked,
                    "MySQL GTID mode was not observed",
                    "Recollect source preflight.",
                ));
            }
        };
        if matches!(
            (
                definition.source.mysql.as_ref().and_then(|e| e.gtid_mode),
                &gtid_mode
            ),
            (Some(MysqlGtidMode::On), GtidMode::OFF) | (Some(MysqlGtidMode::Off), GtidMode::ON)
        ) {
            return Err(failure(
                FailureKind::Blocked,
                "MySQL declared GTID mode differs from observed mode",
                "Correct the declared GTID mode.",
            ));
        }
        let tables = definition
            .selected_objects
            .iter()
            .map(|o| (o.schema.clone(), o.name.clone()))
            .collect::<Vec<_>>();
        let mut descriptors = load_descriptors(&mut client.conn, &tables)
            .await
            .map_err(|_| {
                failure(
                    FailureKind::Blocked,
                    "MySQL selected-table descriptors unavailable",
                    "Restore catalog visibility and retain a primary key on every table.",
                )
            })?;
        for (object, descriptor) in definition.selected_objects.iter().zip(&mut descriptors) {
            project_descriptor(object, descriptor, &redactor)?;
        }
        let mapped_schema = dest_schema::build(&descriptors).map_err(|_| failure(FailureKind::Blocked, "MySQL projected destination schema mapping blocked", "Convert unsupported columns or explicitly exclude non-key columns; retain generated-column dependencies."))?;
        let startup_inputs = StartupInputs {
            selected: descriptors
                .iter()
                .map(|t| SelectedTable {
                    schema: t.schema.clone(),
                    table: t.table.clone(),
                    columns: t.columns.iter().map(|c| c.name.clone()).collect(),
                    primary_key: t.primary_key.clone(),
                })
                .collect(),
            mapping_versions: vec![MYSQL_MAPPING_VERSION.into(), VALUE_MAPPING_VERSION.into()],
            gtid_mode: gtid_mode.clone(),
            version,
        };
        let _ = client.conn.disconnect().await;
        Ok((
            destination,
            descriptors,
            mapped_schema,
            startup_inputs,
            gtid_mode,
            version,
        ))
    };
    let (destination, descriptors, mapped_schema, startup_inputs, gtid_mode, version) =
        tokio::time::timeout(Duration::from_secs(30), operation)
            .await
            .map_err(|_| mysql_infrastructure())??;
    Ok(MysqlPrepared {
        mysql_config,
        destination_config,
        descriptors,
        mapped_schema,
        startup_inputs,
        gtid_mode,
        version,
        destination,
    })
}

/// Durable assess inputs: never substitute zero estimates at startup.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Assessment {
    version: AssessmentVersion,
    estimated_copy_seconds: u64,
    planned_catchup_seconds: u64,
    max_decompressed_bytes: usize,
}
#[derive(serde::Serialize, serde::Deserialize)]
enum AssessmentVersion {
    #[serde(rename = "skedaddle-mysql-assessment-v1")]
    V1,
}
fn assessment_error() -> Failure {
    failure(
        FailureKind::InvalidInput,
        "Missing or invalid MySQL assessment; re-run assess",
        "Re-run assess with explicit nonzero copy/catch-up estimates and decoder bound.",
    )
}
impl Assessment {
    pub fn encode(options: &Options) -> Result<String, Failure> {
        let record = Self {
            version: AssessmentVersion::V1,
            estimated_copy_seconds: options.estimated_copy_seconds,
            planned_catchup_seconds: options.planned_catchup_seconds,
            max_decompressed_bytes: options.max_decompressed_bytes,
        };
        record.options()?;
        serde_json::to_string(&record).map_err(|_| assessment_error())
    }
    fn options(&self) -> Result<Options, Failure> {
        if self.estimated_copy_seconds == 0
            || self.planned_catchup_seconds == 0
            || self.max_decompressed_bytes == 0
            || self
                .estimated_copy_seconds
                .checked_add(self.planned_catchup_seconds)
                .is_none()
        {
            return Err(assessment_error());
        }
        Ok(Options {
            estimated_copy_seconds: self.estimated_copy_seconds,
            planned_catchup_seconds: self.planned_catchup_seconds,
            max_decompressed_bytes: self.max_decompressed_bytes,
            selected_tables: vec![],
        })
    }
    pub fn from_state(state: &crate::coordinator::Materialized) -> Result<Options, Failure> {
        let (marker, _) = state
            .progress
            .get("mysql-assessment")
            .ok_or_else(assessment_error)?;
        serde_json::from_str::<Self>(marker)
            .map_err(|_| assessment_error())?
            .options()
    }
}

/// Caller retains the destination exclusion capability for this entire operation.
#[allow(clippy::too_many_arguments)]
pub(super) async fn run(
    root: &std::path::Path,
    id: &str,
    definition: &MigrationDefinition,
    prepared: MysqlPrepared,
    exclusion: std::sync::Arc<crate::coordinator::exclusion::MigrationExclusion>,
    guard: &crate::coordinator::exclusion::ExclusionGuard,
    order: std::sync::Arc<dyn crate::event::PositionOrder + Send + Sync>,
    resume: bool,
    stop: crate::engine::StopAt,
    pause: tokio::sync::watch::Receiver<bool>,
) -> Result<crate::engine::Reached, RunError> {
    use crate::{
        coordinator::chunk::SourceTableKey,
        coordinator::{self, State, epoch::Coordinator, journal::Journal, local::Local},
        engine,
        event::SourceIdentity,
        mysql::{chunk, chunk_key::*, installer::MysqlInstaller, startup::MysqlStartupRecord},
    };
    use std::sync::Arc;
    let adapter_error = |_: &dyn std::fmt::Display| {
        coordinator::Error::RecoveryRequired(
            "MySQL startup or chunk planning failed; preserve state and inspect prerequisites"
                .into(),
        )
    };
    let binding = SourceBinding::for_definition(id, definition)?;
    let store = super::operational::buffer(definition, true)?;
    let config = crate::buffer::reader::ReaderConfig {
        prefix: "runner".into(),
        migration_id: id.into(),
        stream: binding.stream.clone(),
        limits: Default::default(),
        required_next_mseq: 0,
    };
    let journal = Journal::new(
        Arc::new(coordinator::journal::local_store(root)?),
        "skedaddle",
        id,
        Duration::from_secs(30),
    )?
    .with_chunk_buffer(store.clone(), config.clone(), order.clone());
    let path = root.join(format!("{id}.local"));
    let local = if path.exists() {
        Local::open_journal(&path, &journal).await?
    } else {
        let mut local = Local::create(&path)?;
        local.replay_journal(&journal).await?;
        local
    };
    let mut owner = Coordinator::observer(
        journal.clone(),
        local,
        format!("mysql-runner-{}", std::process::id()),
    )?
    .with_exclusion(exclusion.clone());
    owner.acquire_with_guard(guard).await?;
    let state = owner.state().clone();
    if !matches!(
        state.lifecycle.state,
        State::Assessed | State::Copying | State::CatchingUp | State::Paused
    ) {
        return Err(coordinator::Error::Rejected(
            "MySQL startup requires Assessed or Copying".into(),
        )
        .into());
    }
    let identity = SourceIdentity {
        engine: "mysql".into(),
        system_id: id.into(),
        lineage: MYSQL_MAPPING_VERSION.into(),
        db_name: id.into(),
        stream: binding.stream.clone(),
    };
    let mut installer = MysqlInstaller::new(
        prepared.mysql_config.clone(),
        prepared.destination_config.clone(),
        identity.clone(),
        prepared.descriptors.clone(),
        prepared.startup_inputs.selected.clone(),
    )
    .map_err(|e| adapter_error(&e))?;
    installer.startup_record = state
        .progress
        .get("source-startup")
        .map(|(marker, _)| MysqlStartupRecord::decode(marker).map_err(|e| adapter_error(&e)))
        .transpose()?;
    if state.lifecycle.state == State::Copying && installer.startup_record.is_none() {
        return Err(coordinator::Error::RecoveryRequired(
            "MySQL Copying requires source-startup record".into(),
        )
        .into());
    }
    let holder = Arc::new(tokio::sync::Mutex::new(owner));
    let authority = crate::coordinator::gc_authority::CoordinatorGcJournal::new(
        holder.clone(),
        store.clone(),
        config.clone(),
        order.clone(),
    )
    .await?;
    let writer_checkpoint = authority.writer_checkpoint().await?;
    let reader_checkpoint = super::operational::restart_reader_checkpoint(
        &state,
        authority.reader_checkpoint().await?,
    )?;
    drop(authority);
    let mut owner = Arc::try_unwrap(holder)
        .map_err(|_| coordinator::Error::RecoveryRequired("MySQL coordinator retained".into()))?
        .into_inner();
    // Opaque apply still uses a PG origin session as an exclusion identity;
    // only native PG positions may use replication_origin_xact_setup.
    let origin = format!("skedaddle_{id}");
    owner
        .effect(
            coordinator::EffectKind::BufferPublication,
            format!("runner-origin:{origin}"),
            |_| async {
                if prepared
                    .destination
                    .client
                    .query_opt(
                        "SELECT 1 FROM pg_replication_origin WHERE roname=$1",
                        &[&origin],
                    )
                    .await?
                    .is_none()
                {
                    prepared
                        .destination
                        .client
                        .query_one("SELECT pg_replication_origin_create($1)", &[&origin])
                        .await?;
                }
                Ok(())
            },
        )
        .await?;
    let engine = engine::Engine::new_with_exclusion(owner, journal.clone(), exclusion)
        .with_pause_control(engine::PauseControl::new(
            root.join(format!("{id}.control")),
        )?);
    if state.lifecycle.state == State::Assessed {
        engine.start(&mut installer).await?;
    }
    let mut plans = Vec::new();
    let mut connection = None;
    for table in &prepared.descriptors {
        let mut columns = Vec::new();
        for name in &table.primary_key {
            let c = table
                .columns
                .iter()
                .find(|c| &c.name == name)
                .ok_or_else(|| {
                    coordinator::Error::Rejected("MySQL primary-key descriptor missing".into())
                })?;
            let mapping = crate::mapping::mysql::plan_column(c).map_err(|e| adapter_error(&e))?;
            columns.push(MysqlKeyColumn {
                name: c.name.clone(),
                data_type: c.data_type.clone(),
                column_type: c.column_type.clone(),
                character_set: c.character_set.clone(),
                collation: c.collation.clone(),
                type_id: mapping.type_id,
                direction: MysqlKeyDirection::Asc,
            });
        }
        let key = MysqlTableKey {
            event_table: format!("{}.{}", table.schema, table.table),
            schema: table.schema.clone(),
            table: table.table.clone(),
            columns,
        };
        key.validate().map_err(|e| adapter_error(&e))?;
        if let Some(reference) = state.chunk_plans.get(&key.event_table) {
            let saved = journal.chunk_plan(reference).await?;
            if saved.table != SourceTableKey::Mysql(key) {
                return Err(coordinator::Error::RecoveryRequired(
                    "MySQL saved chunk key drift".into(),
                )
                .into());
            }
            plans.push(saved);
        } else {
            if connection.is_none() {
                connection = Some(
                    prepared
                        .mysql_config
                        .connect()
                        .await
                        .map_err(|e| adapter_error(&e))?,
                );
            }
            // Leave headroom for concurrent inserts and key moves. The engine
            // still enforces its 1,000-row reservation; no overfull read is admitted.
            let plan = chunk::sample_plan(&mut connection.as_mut().unwrap().conn, &key, 500)
                .await
                .map_err(|e| adapter_error(&e))?;
            engine.plan(&plan).await?;
            plans.push(plan);
        }
    }
    // Setup sessions are released before admitting the live worker set.
    if let Some(connection) = connection {
        connection
            .conn
            .disconnect()
            .await
            .map_err(|e| adapter_error(&e))?;
    }
    drop(installer);
    drive(
        root,
        id,
        definition,
        prepared,
        engine,
        store,
        config,
        order,
        identity,
        plans,
        writer_checkpoint,
        reader_checkpoint,
        resume,
        stop,
        pause,
    )
    .await
}

// Deliberately local: PG's runner also owns fencing/cutover and verification
// services. MySQL admits only the common Engine copy/capture/apply workers.
#[allow(clippy::too_many_arguments)]
async fn drive(
    root: &std::path::Path,
    id: &str,
    definition: &MigrationDefinition,
    prepared: MysqlPrepared,
    engine: crate::engine::Engine,
    store: std::sync::Arc<crate::buffer::backends::ObjectBufferStore>,
    config: crate::buffer::reader::ReaderConfig,
    order: std::sync::Arc<dyn crate::event::PositionOrder + Send + Sync>,
    identity: crate::event::SourceIdentity,
    plans: Vec<crate::coordinator::chunk::ChunkPlan>,
    writer_checkpoint: Option<crate::buffer::writer::WriterCheckpoint>,
    reader_checkpoint: Option<crate::buffer::reader::ReaderCheckpoint>,
    resume: bool,
    stop: crate::engine::StopAt,
    pause: tokio::sync::watch::Receiver<bool>,
) -> Result<crate::engine::Reached, RunError> {
    use crate::{
        budget::{Limits, RateLimit, ReadLimits, SystemClock, ThroughputBudget},
        buffer::{
            gc::RetentionPolicy,
            reader::BufferReader,
            writer::{BufferWriter, WriterConfig},
        },
        coordinator::{self, State},
        engine::{CopyConfig, PgApplier},
        event::PositionOrder,
        mysql::{
            capture::MysqlCapture,
            chunk::MysqlChunks,
            planner::{MysqlPlanner, MysqlPlannerTable},
            position::{MysqlOrder, MysqlPosition},
            startup::MysqlStartupRecord,
        },
        pg::apply::{self, executor::Executor},
    };
    use std::sync::Arc;
    fn recovery(_: impl std::fmt::Display) -> coordinator::Error {
        coordinator::Error::RecoveryRequired(
            "MySQL forward worker failed; preserve state and inspect prerequisites".into(),
        )
    }
    let state = engine.runtime().state().await;
    let startup = MysqlStartupRecord::decode(
        &state
            .progress
            .get("source-startup")
            .ok_or_else(|| recovery("missing startup"))?
            .0,
    )
    .map_err(recovery)?;
    let start = startup.start_position().map_err(recovery)?;
    let native_order = match &start {
        MysqlPosition::Gtid { .. } => MysqlOrder::gtid(id),
        MysqlPosition::File { server_uuid, .. } => MysqlOrder::file(server_uuid),
    }
    .map_err(recovery)?;
    if native_order.domain() != order.domain() {
        return Err(coordinator::Error::RecoveryRequired("MySQL startup/binding order domain mismatch; file-position binding must be qualified before GTID OFF can run".into()).into());
    }
    let max_bytes = definition
        .budgets
        .max_snapshot_bytes
        .clamp(1, 16 * 1024 * 1024);
    let scratch = root.join(format!("{id}.scratch"));
    std::fs::create_dir_all(&scratch)?;
    let (feedback, _receive) = std::sync::mpsc::channel();
    let writer = Box::pin(BufferWriter::open_from_checkpoint(
        store.clone(),
        WriterConfig {
            prefix: "runner".into(),
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
    let restart = recovered
        .last_position
        .as_ref()
        .map(|p| MysqlPosition::decode(p.bytes()).map_err(recovery))
        .transpose()?
        .unwrap_or(start);
    let tables = prepared
        .descriptors
        .iter()
        .map(|t| (t.schema.clone(), t.table.clone()))
        .collect::<Vec<_>>();
    // Concurrent migrations on one server must not share a dump server ID.
    use sha2::{Digest, Sha256};
    let server_hash = Sha256::digest(id.as_bytes());
    let server_id = u32::from_be_bytes(server_hash[..4].try_into().expect("hash width")).max(1);
    let mut capture = Box::pin(MysqlCapture::new(
        &prepared.mysql_config,
        identity.clone(),
        restart,
        &tables,
        writer,
        server_id,
    ))
    .await
    .map_err(recovery)?
    .with_startup_inputs(prepared.startup_inputs);
    let mut planner_tables = Vec::new();
    for (descriptor, plan) in prepared.descriptors.iter().zip(plans) {
        let quote = |s: &str| format!("\"{}\"", s.replace('"', "\"\""));
        let statement = prepared
            .destination
            .client
            .prepare(&format!(
                "SELECT {} FROM {}.{} LIMIT 0",
                descriptor
                    .columns
                    .iter()
                    .map(|c| quote(&c.name))
                    .collect::<Vec<_>>()
                    .join(","),
                quote(&descriptor.schema),
                quote(&descriptor.table),
            ))
            .await
            .map_err(recovery)?;
        let columns = descriptor
            .columns
            .iter()
            .zip(statement.columns())
            .map(|(c, wire)| {
                let mapped = crate::mapping::mysql::plan_column(c).map_err(recovery)?;
                Ok(apply::Column {
                    name: c.name.clone(),
                    type_id: mapped.type_id,
                    pg_type: mapped.pg_type,
                    wire_type: wire.type_().clone(),
                })
            })
            .collect::<Result<Vec<_>, coordinator::Error>>()?;
        planner_tables.push(
            MysqlPlannerTable::new(
                plan,
                apply::Table {
                    event_table: format!("{}.{}", descriptor.schema, descriptor.table),
                    schema: descriptor.schema.clone(),
                    name: descriptor.table.clone(),
                    schema_version: 1,
                    columns,
                    key: descriptor.primary_key.clone(),
                },
            )
            .map_err(recovery)?,
        );
    }
    let stream = config.stream.clone();
    let origin = format!("skedaddle_{id}");
    let planner = MysqlPlanner::connect(
        &prepared.mysql_config,
        planner_tables,
        native_order,
        stream.clone(),
        origin.clone(),
        1000,
    )
    .await
    .map_err(recovery)?;
    let mut chunks = MysqlChunks::connect(&prepared.mysql_config, prepared.descriptors)
        .await
        .map_err(recovery)?;
    let connector = super::operational::Connect {
        config: prepared.destination_config,
        connection: definition.destination.clone(),
        drivers: vec![],
    };
    let positions = apply::SourcePositions::new(order.clone(), apply::OriginPolicy::Opaque);
    let executor =
        Executor::new(connector, stream.clone(), origin, 1000, positions).map_err(recovery)?;
    let mut reader_config = config.clone();
    reader_config.required_next_mseq = recovered.next_mseq;
    let mut applier = PgApplier::new(
        BufferReader::new(
            store.clone(),
            reader_config,
            reader_checkpoint.unwrap_or_default(),
            order.clone(),
        )
        .map_err(recovery)?,
        stream,
        executor,
        planner,
    );
    engine
        .enable_retention(
            store,
            config,
            order.clone(),
            RetentionPolicy {
                retained_for_ms: definition.buffer_retention.min_seconds.saturating_mul(1000),
                orphan_grace_ms: definition.buffer_retention.max_seconds.saturating_mul(1000),
            },
            Duration::from_secs(2),
        )
        .await?;
    let rate = RateLimit {
        per_second: max_bytes,
        burst: max_bytes,
    };
    let reads = ReadLimits {
        rows: RateLimit {
            per_second: 1000,
            burst: 1000,
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
    if resume && state.lifecycle.state == State::Paused {
        engine.resume().await?;
    }
    let result = engine
        .run_until(
            &mut capture,
            &mut chunks,
            &mut applier,
            CopyConfig {
                source: identity,
                order_domain: order.domain().into(),
                schema_version: 1,
                read_rows: 1000,
                read_bytes: max_bytes,
                window_bytes: max_bytes as usize,
            },
            &mut budget,
            pause,
            stop,
        )
        .await;
    let close = applier.destination.close().await.map_err(recovery);
    let disconnect = chunks.disconnect().await.map_err(recovery);
    result
        .and_then(|mut reached| {
            close?;
            disconnect?;
            reached
                .blockers
                .push("buffer-budget-accounting-unavailable".into());
            Ok(reached)
        })
        .map_err(RunError::Engine)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn assessment_golden_and_strict_decode() {
        let json = Assessment::encode(&Options {
            estimated_copy_seconds: 10,
            planned_catchup_seconds: 20,
            max_decompressed_bytes: 1024,
            selected_tables: vec![],
        })
        .unwrap();
        assert_eq!(
            json,
            r#"{"version":"skedaddle-mysql-assessment-v1","estimated_copy_seconds":10,"planned_catchup_seconds":20,"max_decompressed_bytes":1024}"#
        );
        assert_eq!(
            serde_json::from_str::<Assessment>(&json)
                .unwrap()
                .options()
                .unwrap()
                .estimated_copy_seconds,
            10
        );
        for bad in [
            json.replace("-v1", "-v2"),
            json.replace("1024}", "1024,\"extra\":1}"),
            json.replace("10,", "0,"),
            json.replace("1024}", "0}"),
        ] {
            let result = serde_json::from_str::<Assessment>(&bad)
                .map_err(|_| assessment_error())
                .and_then(|record| record.options());
            assert!(matches!(
                result,
                Err(Failure {
                    kind: FailureKind::InvalidInput,
                    ..
                })
            ));
        }
    }
    #[test]
    fn missing_assessment_refuses_before_effects() {
        let state = crate::coordinator::Materialized::default();
        assert!(matches!(
            Assessment::from_state(&state),
            Err(Failure {
                kind: FailureKind::InvalidInput,
                ..
            })
        ));
        assert!(state.intent_roots.is_empty());
    }
}
