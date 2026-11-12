//! H1-gated end-to-end engine test. No service startup here. Database operations
//! are real; local buffer, journal, capture, chunk SQL and apply executor are real.
use async_trait::async_trait;
use futures::FutureExt;
use sha2::{Digest, Sha256};
use skedaddle::{
    budget::{Limits, RateLimit, ReadLimits, SystemClock, ThroughputBudget},
    buffer::{
        backends::ObjectBufferStore,
        format,
        reader::{BufferReader, ReaderCheckpoint, ReaderConfig},
        writer::{BufferWriter, WriterConfig},
    },
    coordinator::{
        Event, Materialized, State, Transition, chunk::ChunkPlan, epoch::Coordinator,
        journal::Journal, local::Local,
    },
    cutover::{self, SourceFence as _},
    engine::{self, CopyConfig, Engine, PgApplier, PgCapture, PgChunks, PgInstaller},
    event::{AssembledTxn, EventError, PositionOrder, SourceIdentity},
    mapping::{PgType, Value},
    pg::{
        self,
        apply::{
            self,
            executor::{self, Connector, Executor},
        },
        chunk::{Direction, KeyColumn, QualifiedName, TableKey},
        ddl::{SchemaVersions, TriggerSql},
        replication::ConnectionConfig,
        source,
    },
    security::TlsPolicy,
    verify::{
        self, Applier as _, KeyOrder, RowSource as _,
        sql::{Column, PgRowSource, RangeBudget, Side, SqlChangeEvidence},
    },
};
use std::{
    cmp::Ordering,
    error::Error,
    io::{BufReader, Write},
    panic::AssertUnwindSafe,
    path::PathBuf,
    sync::Arc,
    time::Duration,
};
use tokio_postgres::{Client, types::Type};
type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

// Exercise the async handoff alongside the real database qualification path.
struct AwaitPlanner<P>(P);

// A scheduling gate around the REAL destination executor. It adds no database
// behavior; the live final-marker test withholds effects to prove ordering.
struct GatedDestination<D> {
    destination: D,
    gate: tokio::sync::watch::Receiver<bool>,
}
#[async_trait]
impl<D: executor::Destination> executor::Destination for GatedDestination<D> {
    async fn apply(
        &mut self,
        plan: &apply::TransactionPlan,
    ) -> executor::Result<apply::CommitDecision> {
        while *self.gate.borrow_and_update() {
            self.gate
                .changed()
                .await
                .map_err(|_| executor::Error::Blocked)?;
        }
        self.destination.apply(plan).await
    }
    async fn retain_after_checkpoint(
        &mut self,
        checkpoint: &ReaderCheckpoint,
    ) -> executor::Result<()> {
        self.destination.retain_after_checkpoint(checkpoint).await
    }
}
// Withhold preparation only. Once accepted, real destination effects and
// checkpoint persistence drain normally before this adapter acknowledges hold.
struct HoldApplier<A> {
    inner: A,
    hold: tokio::sync::watch::Receiver<bool>,
    held: Arc<tokio::sync::Notify>,
}
impl<A: engine::Applier> engine::Applier for HoldApplier<A> {
    async fn step(&mut self, runtime: &engine::Runtime) -> skedaddle::coordinator::Result<bool> {
        let (_keep, stop) = tokio::sync::watch::channel(false);
        self.controlled_step(runtime, stop).await
    }
    async fn controlled_step(
        &mut self,
        runtime: &engine::Runtime,
        stop: tokio::sync::watch::Receiver<bool>,
    ) -> skedaddle::coordinator::Result<bool> {
        if *self.hold.borrow() {
            self.held.notify_one();
            return Ok(false);
        }
        self.inner.controlled_step(runtime, stop).await
    }
}
impl<P: engine::PgTransactionPlanner> engine::PgTransactionPlanner for AwaitPlanner<P> {
    fn plan<'a>(
        &'a mut self,
        txn: &'a AssembledTxn,
        mseq: u64,
        state: &'a Materialized,
    ) -> engine::PgPlanFuture<'a> {
        Box::pin(async move {
            tokio::task::yield_now().await;
            self.0.plan(txn, mseq, state).await
        })
    }
}

struct Order(String);
impl PositionOrder for Order {
    fn domain(&self) -> &str {
        &self.0
    }
    fn compare(&self, a: &[u8], b: &[u8]) -> Result<Ordering, EventError> {
        Ok(a.cmp(b))
    }
}
async fn connect(dsn: &str, ca: &str) -> TestResult<Client> {
    let (client, driver, evidence) = pg::tls::connect(
        &pg::tls::Config::new(dsn.parse()?, ca),
        &TlsPolicy::Required,
    )
    .await?;
    assert!(evidence.encrypted());
    tokio::spawn(driver);
    client.batch_execute(source::REQUIRED_SETTINGS_SQL).await?;
    Ok(client)
}
struct Connect {
    dsn: String,
    ca: String,
}

// The synchronous cutover adapter uses the same real TLS transport, on a
// blocking worker so the engine's async capture/apply loop keeps running.
struct MarkerConnection {
    client: Client,
    runtime: tokio::runtime::Handle,
}
impl pg::fence::Connection for MarkerConnection {
    fn query(&mut self, sql: &str, timeout: Duration) -> Result<pg::fence::Rows, String> {
        self.runtime.block_on(async {
            match tokio::time::timeout(timeout, self.client.simple_query(sql)).await {
                Ok(Ok(messages)) => Ok(messages
                    .into_iter()
                    .filter_map(|message| match message {
                        tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                            (0..row.len())
                                .map(|i| row.get(i).unwrap_or("").to_owned())
                                .collect(),
                        ),
                        _ => None,
                    })
                    .collect()),
                failure => {
                    let _ =
                        tokio::time::timeout(timeout, self.client.batch_execute("ROLLBACK")).await;
                    Err(format!("native marker query failed: {failure:?}"))
                }
            }
        })
    }
}

async fn native_final_marker(
    dsn: &str,
    destination_dsn: &str,
    ca: &str,
    key: cutover::Key,
    application: String,
) -> TestResult<cutover::Marker> {
    let client = connect(dsn, ca).await?;
    let owner: String = client.query_one(
        "SELECT pg_get_userbyid(c.relowner) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relname='rows'", &[&key.migration],
    ).await?.get(0);
    let table = pg::fence::Table {
        schema: key.migration.clone(),
        name: "rows".into(),
        owner,
    };
    // Acknowledge the real fixture's owner/superuser residuals, while the
    // dedicated application login itself must lose write access and drain.
    let roles = client
        .query(
            "SELECT rolname,rolcanlogin,rolsuper,rolbypassrls FROM pg_roles",
            &[],
        )
        .await?
        .iter()
        .map(|r| pg::fence::Role {
            name: r.get(0),
            login: r.get(1),
            superuser: r.get(2),
            bypass_rls: r.get(3),
        })
        .collect();
    let memberships = client
        .query(
            "SELECT pg_get_userbyid(member),pg_get_userbyid(roleid) FROM pg_auth_members",
            &[],
        )
        .await?
        .iter()
        .map(|r| pg::fence::Membership {
            member: r.get(0),
            role: r.get(1),
        })
        .collect();
    let applications = [application.clone()].into();
    let acknowledged = pg::fence::classify_residuals(
        &pg::fence::Catalog {
            roles,
            memberships,
            tables: vec![table.clone()],
            grants: vec![],
        },
        &applications,
        &Default::default(),
    )
    .into_iter()
    .map(|r| r.principal)
    .collect();
    let destination_client = connect(destination_dsn, ca).await?;
    let version: String = destination_client
        .query_one("SHOW server_version_num", &[])
        .await?
        .get(0);
    let reverse = pg::reverse::Config::new(
        &key.migration,
        version.parse()?,
        vec![(key.migration.clone(), "rows".into())],
    )?;
    // This fixture exclusively owns this NOLOGIN destination application role.
    let destination_fenced: bool = destination_client
        .query_one(
            "SELECT NOT rolcanlogin FROM pg_roles WHERE rolname=$1",
            &[&format!("{}_rev_app", key.migration)],
        )
        .await?
        .get(0);
    let mut destination = MarkerConnection {
        client: destination_client,
        runtime: tokio::runtime::Handle::current(),
    };
    let connection = MarkerConnection {
        client,
        runtime: tokio::runtime::Handle::current(),
    };
    tokio::task::spawn_blocking(move || {
        let state = pg::fence::Fence::new(pg::fence::FenceConfig {
            epoch: key.epoch,
            applications,
            tables: vec![table],
            acknowledged,
            deadline_ms: 10_000,
        })?;
        let mut adapter = cutover::pg::Source {
            connection,
            state,
            drained: Default::default(),
            pre_fence: || Ok(()),
        };
        adapter.fence(&key, &mut |_| Ok(()))?;
        assert_eq!(adapter.state.next, pg::fence::Step::Complete);
        assert!(adapter.state.saved.is_some() && adapter.state.drain_lsn.is_some());
        let fenced = pg::fence::Connection::query(&mut adapter.connection,
            &format!("SELECT (NOT rolcanlogin AND NOT has_table_privilege(rolname,'{}.rows','UPDATE'))::text FROM pg_roles WHERE rolname='{}'", key.migration, application),
            Duration::from_secs(5))?;
        assert_eq!(fenced[0][0], "true");
        // Live forward capture is running. Exercise the real reverse installer,
        // including source progress CREATE/ALTER and REVOKE, then emit a marker.
        let reverse_ready = reverse.setup(
            &mut destination, &mut adapter.connection, None,
            Duration::from_secs(30), || Ok(destination_fenced), |_, _| Ok(()),
        )?;
        assert!(reverse_ready.slot().consistent_point > 0);
        let marker = adapter.final_marker(&key)?;
        adapter.state.config.epoch += 1;
        let retry = adapter.final_marker(&cutover::Key {
            epoch: key.epoch + 1,
            ..key
        })?;
        assert_eq!(
            retry, marker,
            "retry must return the original durable receipt"
        );
        TestResult::Ok(marker)
    })
    .await?
}
#[async_trait]
impl Connector for Connect {
    async fn connect(&mut self) -> executor::Result<Client> {
        connect(&self.dsn, &self.ca)
            .await
            .map_err(|_| executor::Error::Database)
    }
}
fn type_id(ty: &PgType) -> String {
    format!(
        "skva.v1:{:x}",
        Sha256::digest(serde_json::to_vec(&(ty, -1_i32)).unwrap())
    )
}
fn text() -> PgType {
    PgType::Text {
        encoding: "UTF8".into(),
    }
}
fn table(schema: &str) -> TableKey {
    TableKey {
        table: format!("\"{schema}\".\"rows\""),
        relation: QualifiedName {
            schema: schema.into(),
            name: "rows".into(),
        },
        columns: vec![KeyColumn {
            name: "id".into(),
            pg_type: PgType::Int4,
            sql_type: QualifiedName {
                schema: "pg_catalog".into(),
                name: "int4".into(),
            },
            collation: None,
            direction: Direction::Asc,
        }],
    }
}
fn apply_table(schema: &str) -> apply::Table {
    apply::Table {
        event_table: table(schema).table,
        schema: schema.into(),
        name: "rows".into(),
        schema_version: 0,
        columns: vec![
            apply::Column {
                name: "id".into(),
                type_id: type_id(&PgType::Int4),
                pg_type: PgType::Int4,
                wire_type: Type::INT4,
            },
            apply::Column {
                name: "body".into(),
                type_id: type_id(&text()),
                pg_type: text(),
                wire_type: Type::TEXT,
            },
        ],
        key: vec!["id".into()],
    }
}
fn catalog(schema: &str) -> pg::catalog::TableCatalog {
    let destination = apply_table(schema);
    pg::catalog::TableCatalog {
        selection: pg::catalog::TableSelection {
            schema: schema.into(),
            name: "rows".into(),
            columns: destination.columns.iter().map(|c| c.name.clone()).collect(),
        },
        columns: destination
            .columns
            .iter()
            .map(|c| pg::catalog::QualifiedColumn {
                name: c.name.clone(),
                pg_type: c.pg_type.clone(),
                source_wire: c.wire_type.clone(),
                destination_wire: c.wire_type.clone(),
                typmod: -1,
                source_type_id: c.type_id.clone(),
            })
            .collect(),
        index_columns: vec![],
        key: table(schema),
        destination,
    }
}

#[test]
fn chunk_adapter_query_projects_only_qualified_columns() -> TestResult {
    let plan = ChunkPlan::new(table("projection"), 1000, vec![])?;
    let query = PgChunks::chunk_query(&[catalog("projection")], &plan, &plan.ranges[0])?;
    assert!(query.sql.starts_with("SELECT \"id\", \"body\" FROM "));
    assert!(!query.sql.contains('*'));
    assert!(!query.sql.contains("excluded"));
    assert!(query.sql.contains("ORDER BY"));
    assert!(PgChunks::chunk_query(&[catalog("other")], &plan, &plan.ranges[0]).is_err());
    Ok(())
}

const EXCLUDED_SENTINEL: &str = "E4D_SECRET_843e98a61b294cfd";

fn assert_no_sentinel(path: &std::path::Path) -> TestResult<u64> {
    let mut bytes_scanned = 0;
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            bytes_scanned += assert_no_sentinel(&entry.path())?;
        } else {
            let bytes = std::fs::read(entry.path())?;
            bytes_scanned += bytes.len() as u64;
            assert!(
                !bytes
                    .windows(EXCLUDED_SENTINEL.len())
                    .any(|bytes| bytes == EXCLUDED_SENTINEL.as_bytes()),
                "excluded value entered test buffer/journal files"
            );
        }
    }
    Ok(bytes_scanned)
}
fn replication(config: &serde_json::Value) -> TestResult<ConnectionConfig> {
    let r = &config["replication"];
    let field = |name: &str| -> TestResult<String> {
        Ok(r[name]
            .as_str()
            .ok_or("missing H1 replication field")?
            .into())
    };
    let mut roots = rustls::RootCertStore::empty();
    for certificate in
        rustls_pemfile::certs(&mut BufReader::new(std::fs::File::open(field("ca_pem")?)?))
    {
        roots.add(certificate?)?;
    }
    Ok(ConnectionConfig {
        host: field("host")?,
        port: r["port"].as_u64().ok_or("replication port")?.try_into()?,
        server_name: field("server_name")?,
        user: field("user")?,
        password: field("password")?,
        database: field("database")?,
        roots,
        connect_timeout: Duration::from_secs(10),
        status_interval: Duration::from_millis(20),
        limits: pg::pgoutput::Limits::default(),
    })
}
async fn rows(client: &Client, schema: &str) -> TestResult<Vec<(i32, String)>> {
    Ok(client
        .query(
            &format!("SELECT id,body FROM {schema}.rows ORDER BY id"),
            &[],
        )
        .await?
        .into_iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect())
}
fn skip(reason: &str) {
    let _ = writeln!(
        std::io::stderr().lock(),
        "SKIP engine: {reason}; real PostgreSQL engine qualification NOT RUN (use harness/run.ps1 -Phase pg-matrix)"
    );
}

#[tokio::test]
async fn copies_one_table_with_concurrent_writes_and_reaches_catching_up() -> TestResult {
    copy_table(false, false, false, None).await
}

#[tokio::test]
async fn live_verification_tracks_boundary_writes_and_fails_closed() -> TestResult {
    copy_table(false, true, false, None).await
}

#[tokio::test]
async fn live_readiness_light_load_burst_and_ddl_boundary_fail_closed() -> TestResult {
    copy_table(false, false, true, None).await
}

#[tokio::test]
async fn partial_chunk_copy_excludes_sentinel_from_destination_buffer_and_logs() -> TestResult {
    if std::env::var("E4D_PARTIAL_LOG_CHILD").as_deref() == Ok("1") {
        return copy_table(true, false, false, None).await;
    }
    let Some(path) = std::env::var_os("SKEDADDLE_TEST_CONFIG") else {
        skip("SKEDADDLE_TEST_CONFIG absent");
        return Ok(());
    };
    let cfg: serde_json::Value = serde_json::from_slice(&std::fs::read(path)?)?;
    if cfg["engine"]["source"].as_str().is_none() {
        skip("H1 config lacks engine.source");
        return Ok(());
    }
    // Capture the real process diagnostics in a separate test process so
    // test-threads=8 cannot mix another test's output into this qualification.
    let output = std::process::Command::new(std::env::current_exe()?)
        .args([
            "--exact",
            "partial_chunk_copy_excludes_sentinel_from_destination_buffer_and_logs",
            "--nocapture",
            "--test-threads=1",
        ])
        .env("E4D_PARTIAL_LOG_CHILD", "1")
        .output()?;
    let log =
        PathBuf::from(std::env::var_os("SKEDADDLE_TEST_ROOT").ok_or("task test root absent")?)
            .join(format!("e4d-partial-{}.log", uuid::Uuid::new_v4()));
    let mut diagnostics = output.stdout;
    diagnostics.extend(output.stderr);
    std::fs::write(&log, &diagnostics)?;
    assert!(
        !diagnostics
            .windows(EXCLUDED_SENTINEL.len())
            .any(|bytes| bytes == EXCLUDED_SENTINEL.as_bytes()),
        "excluded value entered actual runtime diagnostics"
    );
    assert!(
        output.status.success(),
        "partial copy child failed: {log:?}"
    );
    Ok(())
}

#[tokio::test]
async fn final_marker_capture_and_apply_receipts_are_exact_live() -> TestResult {
    copy_table(false, false, false, Some(MarkerEmitter::Manual)).await
}

#[tokio::test]
async fn native_final_marker_capture_and_apply_receipts_are_exact_live() -> TestResult {
    copy_table(false, false, false, Some(MarkerEmitter::Native)).await
}

#[tokio::test]
async fn native_fence_owner_restart_restores_schema_then_applies_final_marker_live() -> TestResult {
    copy_table(false, false, false, Some(MarkerEmitter::NativeRestart)).await
}

#[derive(Clone, Copy)]
enum MarkerEmitter {
    Manual,
    Native,
    NativeRestart,
}

async fn copy_table(
    partial: bool,
    verification: bool,
    readiness: bool,
    final_marker: Option<MarkerEmitter>,
) -> TestResult {
    let Some(path) = std::env::var_os("SKEDADDLE_TEST_CONFIG") else {
        skip("SKEDADDLE_TEST_CONFIG absent");
        return Ok(());
    };
    let cfg: serde_json::Value = serde_json::from_slice(&std::fs::read(path)?)?;
    let Some(source_dsn) = cfg["engine"]["source"].as_str() else {
        skip("H1 config lacks engine.source");
        return Ok(());
    };
    let destination_dsn = cfg["engine"]["destination"]
        .as_str()
        .ok_or("H1 engine.destination absent")?;
    let ca = cfg["ca_pem"].as_str().ok_or("H1 ca_pem absent")?;
    // Operational engine DSNs are fixture owners. Event-trigger installation
    // needs H1's explicit pg_ddl admin and progress/origin setup uses pg_apply.
    let source_admin = cfg["pg_ddl"].as_str().ok_or("H1 pg_ddl setup DSN absent")?;
    let destination_admin = cfg["pg_apply"]
        .as_str()
        .ok_or("H1 pg_apply setup DSN absent")?;
    let source = connect(source_dsn, ca).await?;
    let destination = connect(destination_dsn, ca).await?;
    let cleanup_source = connect(source_admin, ca).await?;
    let cleanup_destination = connect(destination_admin, ca).await?;
    let id = format!("e{}", &uuid::Uuid::new_v4().simple().to_string()[..20]);
    let publication = format!("{id}_pub");
    let slot = format!("{id}_slot");
    let origin = format!("{id}_origin");
    let application = format!("{id}_app");
    source.batch_execute(&format!("CREATE SCHEMA {id};CREATE TABLE {id}.rows(id integer PRIMARY KEY,body text NOT NULL);INSERT INTO {id}.rows SELECT i,'before-'||i FROM generate_series(1,100) i")).await?;
    destination
        .batch_execute(&format!(
            "CREATE SCHEMA {id};CREATE TABLE {id}.rows(id integer PRIMARY KEY,body text NOT NULL)"
        ))
        .await?;
    if partial {
        source
            .batch_execute(&format!(
                "ALTER TABLE {id}.rows ADD COLUMN excluded text NOT NULL DEFAULT '{EXCLUDED_SENTINEL}'"
            ))
            .await?;
        destination
            .batch_execute(&format!(
                "ALTER TABLE {id}.rows ADD COLUMN excluded text NOT NULL DEFAULT 'destination-default'"
            ))
            .await?;
    }
    if matches!(
        final_marker,
        Some(MarkerEmitter::Native | MarkerEmitter::NativeRestart)
    ) {
        cleanup_source.batch_execute(&format!("CREATE ROLE {application} LOGIN; GRANT USAGE ON SCHEMA {id} TO {application}; GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE ON {id}.rows TO {application}")).await?;
        cleanup_destination.batch_execute(&format!("CREATE ROLE {id}_rev_app NOLOGIN; GRANT USAGE ON SCHEMA {id} TO {id}_rev_app; GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE ON {id}.rows TO {id}_rev_app")).await?;
    }
    cleanup_destination
        .query_one("SELECT pg_replication_origin_create($1)", &[&origin])
        .await?;
    let repl = replication(&cfg)?;
    let result = AssertUnwindSafe(async {
        let root =
            PathBuf::from(std::env::var_os("SKEDADDLE_TEST_ROOT").ok_or("task test root absent")?)
                .join(&id);
        for name in ["journal", "buffer", "scratch"] {
            std::fs::create_dir_all(root.join(name))?;
        }
        let connection = source::connect(&repl).await?;
        let lineage = connection.lineage().clone();
        let domain = format!("pg:{}:{}:{slot}", lineage.system_id, lineage.timeline);
        let identity = SourceIdentity {
            engine: "postgres".into(),
            system_id: lineage.system_id.to_string(),
            lineage: lineage.timeline.to_string(),
            db_name: lineage.database.clone(),
            slot: slot.clone(),
        };
        let store = Arc::new(ObjectBufferStore::local(
            root.join("buffer"),
            Duration::from_secs(5),
        )?);
        let reader_config = ReaderConfig {
            prefix: "engine".into(),
            migration_id: id.clone(),
            stream: "wal".into(),
            limits: format::Limits::default(),
            required_next_mseq: 0,
        };
        let order = Arc::new(Order(domain.clone()));
        let journal_objects = Arc::new(skedaddle::coordinator::journal::local_store(
            root.join("journal"),
        )?);
        let journal = Journal::new(
            journal_objects.clone(),
            "engine",
            &id,
            Duration::from_secs(5),
        )?
        .with_chunk_buffer(store.clone(), reader_config.clone(), order.clone());
        let mut coordinator = Coordinator::observer(
            journal.clone(),
            Local::create(root.join("local"))?,
            "live-engine",
        )?;
        coordinator.acquire().await?;
        coordinator
            .commit(Transition::Lifecycle {
                event: Event::Advance {
                    to: State::Assessed,
                },
            })
            .await?;
        let engine = Engine::new(coordinator, journal.clone());
        // Verification must publish applied proof independently of optional GC.
        if !verification {
            engine
                .enable_retention(
                    store.clone(),
                    reader_config.clone(),
                    order.clone(),
                    skedaddle::buffer::gc::RetentionPolicy {
                        retained_for_ms: if partial { 120_000 } else { 0 },
                        orphan_grace_ms: 0,
                    },
                    Duration::from_millis(1),
                )
                .await?;
        }
        let mut installer = PgInstaller {
            source: connect(source_admin, ca).await?,
            destination: connect(destination_admin, ca).await?,
            replication: connection,
            publication: publication.clone(),
            slot: slot.clone(),
            installation: id.clone(),
            tables: vec![table(&id).relation],
            checkpoint: None,
        };
        engine.start(&mut installer).await?;
        let checkpoint = installer
            .checkpoint
            .take()
            .ok_or("slot checkpoint absent")?;
        assert!(
            engine
                .runtime()
                .state()
                .await
                .progress
                .contains_key("source-slot")
        );
        drop(installer);
        let transport =
            source::reconnect(&repl, &checkpoint, std::slice::from_ref(&publication)).await?;
        let (feedback, _) = std::sync::mpsc::channel();
        let writer = BufferWriter::open_with_scratch(
            store.clone(),
            WriterConfig {
                prefix: "engine".into(),
                migration_id: id.clone(),
                stream: "wal".into(),
                epoch: 1,
                segment_bytes: 1_000_000,
                segment_transactions: 100,
                segment_age: Duration::from_secs(1),
                max_transaction_bytes: 1_000_000,
            },
            order.clone(),
            feedback,
            root.join("scratch"),
        )
        .await?;
        let capture = engine::capture(
            identity.clone(),
            domain.clone(),
            checkpoint.durable_lsn,
            writer,
            catalog(&id).resolver(),
            id.clone(),
            SchemaVersions::new(id.clone(), 0, [])?,
        )?;
        let (_gate, admission) = tokio::sync::watch::channel(false);
        let mut capture = PgCapture::new(
            capture,
            transport,
            admission,
            journal_objects.clone(),
            serde_json::json!(["engine", id, "wal"]).to_string(),
        );
        let plan = ChunkPlan::new(table(&id), 1000, vec![])?;
        engine.plan(&plan).await?;
        let mut chunks = PgChunks {
            admin: connect(source_dsn, ca).await?,
            reader: connect(source_dsn, ca).await?,
            tables: vec![catalog(&id)],
        };
        let tables = vec![apply_table(&id)];
        let plan_origin = origin.clone();
        let plan_installation = id.clone();
        let internal_plans = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let planned_internal = internal_plans.clone();
        let planner = move |txn: &AssembledTxn, mseq: u64, state: &Materialized| {
            // This qualification has one unbounded range, so no Rust key-order
            // approximation is needed. Pre-H records remain Copying on replay.
            let lsn = u64::from_be_bytes(
                txn.commit_position
                    .bytes()
                    .try_into()
                    .map_err(|_| apply::ApplyError::Invalid("LSN"))?,
            );
            let completed = state
                .chunk_attempts
                .values()
                .any(|a| a.done.as_ref().is_some_and(|d| lsn > d.high_lsn));
            let plan = apply::plan_transaction_with_ddl(
                txn,
                &tables,
                &apply::PlanContext {
                    stream: "wal".into(),
                    mseq,
                    origin: plan_origin.clone(),
                    synthetic_commit_time: Some(0),
                },
                &plan_installation,
                |_, _| {
                    Ok(if completed {
                        apply::RangeState::Completed
                    } else {
                        apply::RangeState::Copying
                    })
                },
            )?;
            assert_eq!(plan.progress.digest, txn.digest);
            assert_eq!(plan.progress.txn, txn.txn);
            if txn.events.iter().any(|e| e.kind == skedaddle::event::EventKind::DdlBoundary) {
                planned_internal.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            Ok(plan)
        };
        let executor = Executor::new(
            Connect {
                dsn: destination_admin.into(),
                ca: ca.into(),
            },
            "wal".into(),
            origin.clone(),
            1000,
        )?;
        let (apply_gate, gated) = tokio::sync::watch::channel(false);
        let applier = PgApplier::new(
            BufferReader::new(store.clone(), reader_config.clone(), ReaderCheckpoint::default(), order.clone())?,
            "wal".into(),
            GatedDestination {
                destination: executor,
                gate: gated,
            },
            AwaitPlanner(planner),
        );
        let (hold_apply, hold) = tokio::sync::watch::channel(false);
        let held = Arc::new(tokio::sync::Notify::new());
        let mut applier = HoldApplier { inner: applier, hold, held: held.clone() };
        let rate = RateLimit {
            per_second: 1_000_000,
            burst: 1_000_000,
        };
        let read = ReadLimits {
            rows: rate,
            bytes: rate,
        };
        let mut budget = ThroughputBudget::new(
            Limits {
                chunk_reads: read,
                verification_reads: read,
                applier_transactions: rate,
                connections: Default::default(),
                in_memory_segments: 2,
            },
            Arc::new(SystemClock::default()),
        )?;
        let copy = CopyConfig {
            source: identity.clone(),
            order_domain: domain.clone(),
            schema_version: 0,
            read_rows: 1000,
            read_bytes: 1_000_000,
            window_bytes: 1_000_000,
        };
        let (pause, paused) = tokio::sync::watch::channel(false);
        let runtime = engine.runtime();
        let final_observed = std::sync::Mutex::new(None);
        let workload = async {
            // Start writes only after the engine journals Started. They continue
            // while snapshot/capture/apply are all running under the same scope.
            loop {
                if !runtime.state().await.chunk_attempts.is_empty() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            for i in 0..30_i32 {
                source
                    .execute(
                        &format!("UPDATE {id}.rows SET body=$1 WHERE id=7"),
                        &[&format!("changed-{i}")],
                    )
                    .await?;
                source
                    .execute(
                        &format!("INSERT INTO {id}.rows(id,body) VALUES($1,$2)"),
                        &[&(101 + i), &format!("new-{i}")],
                    )
                    .await?;
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            source
                .execute(&format!("DELETE FROM {id}.rows WHERE id=9"), &[])
                .await?;
            let expected = rows(&source, &id).await?;
            tokio::time::timeout(Duration::from_secs(30), async {
                loop {
                    if runtime.state().await.lifecycle.state == State::CatchingUp
                        && rows(&destination, &id).await? == expected
                    {
                        return TestResult::Ok(());
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await??;
            if let Some(emitter) = final_marker {
                let restart = matches!(emitter, MarkerEmitter::NativeRestart);
                if restart {
                    hold_apply.send_replace(true);
                    held.notified().await;
                } else {
                    apply_gate.send_replace(true);
                }
                // Real transactional source message: keep its message LSN,
                // which differs from the transaction COMMIT and WAL high-water.
                let mut marker = skedaddle::cutover::Marker {
                    cutover: id.clone(),
                    epoch: 1,
                    lsn: 0,
                };
                let key = skedaddle::cutover::Key {
                    migration: id.clone(),
                    cutover: id.clone(),
                    epoch: marker.epoch,
                    coordinator: runtime.state().await.owner,
                };
                let progress = runtime.final_marker_progress(&key, &identity)?;
                match emitter {
                    MarkerEmitter::Manual => {
                        source.batch_execute("BEGIN").await?;
                        let row = source
                            .query_one(
                                "SELECT pg_logical_emit_message(true,$1::text,$2::text)::text",
                                &[&pg::chunk::MESSAGE_PREFIX, &marker.payload()],
                            )
                            .await?;
                        marker.lsn = pg::fence::parse_lsn(row.get::<_, &str>(0))?;
                        assert!(
                            progress.receipt(&marker, false)?.is_none(),
                            "uncommitted source marker cannot be buffered"
                        );
                        source.batch_execute("COMMIT").await?;
                    }
                    MarkerEmitter::Native | MarkerEmitter::NativeRestart => {
                        marker = native_final_marker(
                            source_admin,
                            destination_admin,
                            ca,
                            key,
                            application.clone(),
                        )
                        .await?;
                    }
                }
                let buffered = progress
                    .wait(&marker, false, Duration::from_secs(30))
                    .await?;
                assert!(
                    progress.receipt(&marker, true)?.is_none(),
                    "withheld destination effects cannot have applied evidence"
                );
                if restart {
                    // Fixture lifecycle admission is explicit; this does not
                    // claim that the live readiness producers are qualified.
                    runtime.commit(Transition::Lifecycle { event: Event::Readiness {
                        facts: skedaddle::coordinator::readiness::ReadinessFacts {
                            all_chunks_done: true, capture_healthy: true, lineage_matches: true,
                            indexes_constraints_valid: true, sequences_syncable: true,
                            reverse_prerequisites: true, fence_risks_acknowledged: true,
                            buffer_contract_matches: true, ..Default::default()
                        }
                    }}).await?;
                    runtime.commit(Transition::Lifecycle { event: Event::Advance { to: State::SourceFenced } }).await?;
                    *final_observed.lock().unwrap() = Some((progress, marker));
                    pause.send_replace(true);
                    return TestResult::Ok(());
                }
                apply_gate.send_replace(false);
                let applied = progress
                    .wait(&marker, true, Duration::from_secs(30))
                    .await?;
                assert_eq!(buffered.marker(), &marker);
                assert_eq!(applied.marker(), &marker);
                assert_eq!(buffered.manifest(), applied.manifest());
                if matches!(emitter, MarkerEmitter::Native | MarkerEmitter::NativeRestart) {
                    assert!(internal_plans.load(std::sync::atomic::Ordering::SeqCst) > 0,
                        "the real fence's administrative DDL must pass apply before the exact marker receipt");
                }
                *final_observed.lock().unwrap() = Some((progress.clone(), marker.clone()));
                assert!(
                    progress
                        .receipt(
                            &skedaddle::cutover::Marker {
                                lsn: marker.lsn + 1,
                                ..marker
                            },
                            true
                        )
                        .is_err()
                );
            }
            if matches!(final_marker, Some(MarkerEmitter::Native | MarkerEmitter::NativeRestart)) {
                assert!(
                    runtime
                        .live_readiness()
                        .schema
                        .as_ref()
                        .is_some_and(|s| s.blocked_boundary.is_none())
                );
                source
                    .batch_execute(&format!("CREATE TABLE {id}.application_ddl(id integer)"))
                    .await?;
                tokio::time::timeout(Duration::from_secs(10), async {
                    loop {
                        if let Some(boundary) = runtime
                            .live_readiness()
                            .schema
                            .and_then(|s| s.blocked_boundary)
                        {
                            assert!(boundary.reason.contains("CREATE TABLE"));
                            assert!(
                                boundary.stop_apply_before_transaction
                                    && boundary.readiness_blocked
                            );
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                })
                .await?;
                return TestResult::Ok(());
            }
            if verification {
                Box::pin(qualify_live_verification(
                    &runtime,
                    source_dsn,
                    destination_dsn,
                    ca,
                    &id,
                    &source,
                ))
                .await?;
            }
            if readiness {
                Box::pin(qualify_live_readiness(
                    &runtime,
                    &source,
                    &destination,
                    &repl,
                    &id,
                ))
                .await?;
                return TestResult::Ok(());
            }
            pause.send_replace(true);
            TestResult::Ok(())
        };
        tokio::time::timeout(Duration::from_secs(60), async {
            let (run, writes) = tokio::join!(
                engine.run(
                    &mut capture,
                    &mut chunks,
                    &mut applier,
                    copy,
                    &mut budget,
                    paused
                ),
                workload
            );
            writes?;
            if readiness || matches!(final_marker, Some(MarkerEmitter::Native)) {
                assert!(run.is_err(), "application DDL must stop the run");
            } else {
                run?;
            }
            TestResult::Ok(())
        })
        .await??;
        if matches!(final_marker, Some(MarkerEmitter::NativeRestart)) {
            let (old_progress, marker) = final_observed.into_inner().unwrap().ok_or("restart marker absent")?;
            assert_eq!(old_progress.receipt(&marker, true), Err(engine::MarkerProgressError::RunExited));
            let writer_checkpoint = capture.capture.writer().checkpoint()?;
            let saved_schema = serde_json::to_value(capture.capture.schema_snapshot())?;
            assert!(saved_schema["last_version"].as_u64().is_some_and(|v| v > 0), "native REVOKE must be captured before owner restart");
            assert_eq!(runtime.state().await.lifecycle.state, State::SourceFenced);
            let epoch = runtime.state().await.epoch;
            drop(capture);
            drop(runtime);
            drop(engine);
            let mut owner = Coordinator::observer(journal.clone(), Local::create(root.join("replacement"))?, "live-engine-replacement")?;
            assert_eq!(owner.acquire().await?, epoch + 1);
            let engine = Engine::new(owner, journal.clone());
            let state = engine.runtime().state().await;
            let (feedback, _) = std::sync::mpsc::channel();
            let writer = Box::pin(BufferWriter::open_from_checkpoint(store.clone(), WriterConfig {
                prefix: "engine".into(), migration_id: id.clone(), stream: "wal".into(), epoch: state.epoch,
                segment_bytes: 1_000_000, segment_transactions: 100, segment_age: Duration::from_secs(1),
                max_transaction_bytes: 1_000_000,
            }, writer_checkpoint, order.clone(), feedback, root.join("scratch"))).await?;
            let mut raw = engine::capture(identity.clone(), domain.clone(), checkpoint.durable_lsn, writer, catalog(&id).resolver(), id.clone(), SchemaVersions::new(id.clone(), 0, [])?)?;
            Box::pin(raw.restore_schema(source::SchemaJournalBinding {
                journal: journal.clone(), runtime: engine.runtime(), objects: journal_objects.clone(),
                stream: serde_json::json!(["engine", id, "wal"]).to_string(),
            })).await?;
            assert_eq!(serde_json::to_value(raw.schema_snapshot())?, saved_schema);
            let mut checkpoint = checkpoint.clone();
            checkpoint.durable_lsn = raw.durable_lsn();
            let transport = Box::pin(source::reconnect(&repl, &checkpoint, std::slice::from_ref(&publication))).await?;
            let (_gate, admission) = tokio::sync::watch::channel(false);
            let mut capture = PgCapture::new(raw, transport, admission, journal_objects, serde_json::json!(["engine", id, "wal"]).to_string());
            let reader_checkpoint = state.progress.get("applied").map(|(m, _)| engine::decode_reader_checkpoint(m)).transpose()?.unwrap_or_default();
            let inner = applier.inner;
            let mut applier = PgApplier::new(BufferReader::new(store, reader_config, reader_checkpoint, order)?, "wal".into(), inner.destination, inner.plan);
            let runtime = engine.runtime();
            let key = cutover::Key { migration: id.clone(), cutover: id.clone(), epoch: state.epoch.try_into()?, coordinator: state.owner };
            let (pause, paused) = tokio::sync::watch::channel(false);
            let wait = async {
                while !runtime.live_readiness().running {
                    tokio::task::yield_now().await;
                }
                let progress = runtime.final_marker_progress(&key, &identity)?;
                let receipt = progress.wait(&marker, true, Duration::from_secs(30)).await?;
                assert_eq!(receipt.marker(), &marker);
                assert!(internal_plans.load(std::sync::atomic::Ordering::SeqCst) > 0);
                pause.send_replace(true);
                TestResult::Ok(())
            };
            let copy = CopyConfig { source: identity.clone(), order_domain: domain, schema_version: 0, read_rows: 1000, read_bytes: 1_000_000, window_bytes: 1_000_000 };
            let (run, wait) = tokio::time::timeout(Duration::from_secs(45), async {
                tokio::join!(engine.run_fenced_until(&mut capture, &mut chunks, &mut applier, copy, &mut budget, paused, engine::StopAt::UntilPaused), wait)
            }).await?;
            run?; wait?;
            assert_eq!(runtime.state().await.lifecycle.state, State::SourceFenced);
            assert_eq!(rows(&source, &id).await?, rows(&destination, &id).await?);
            applier.destination.destination.close().await?;
            return TestResult::Ok(());
        }
        if let Some((progress, marker)) = final_observed.into_inner().unwrap() {
            assert_eq!(
                progress.receipt(&marker, true),
                Err(engine::MarkerProgressError::RunExited)
            );
        }
        if matches!(final_marker, Some(MarkerEmitter::Native | MarkerEmitter::NativeRestart)) {
            assert!(
                runtime
                    .live_readiness()
                    .schema
                    .unwrap()
                    .blocked_boundary
                    .is_some()
            );
            assert_eq!(rows(&source, &id).await?, rows(&destination, &id).await?);
            applier.inner.destination.destination.close().await?;
            return TestResult::Ok(());
        }
        if readiness {
            let live = runtime.live_readiness();
            assert!(!live.running && live.lag.is_none() && live.capture.is_none());
            assert!(live.schema.unwrap().blocked_boundary.is_some());
            applier.inner.destination.destination.close().await?;
            return TestResult::Ok(());
        }
        let state = journal.load(false).await?;
        assert_eq!(state.lifecycle.resume_state, Some(State::CatchingUp));
        assert_eq!(rows(&source, &id).await?, rows(&destination, &id).await?);
        assert!(state.chunk_attempts.values().all(|a| a.done.is_some()));
        if partial {
            assert!(
                destination
                    .query_one(
                        &format!("SELECT bool_and(excluded='destination-default') FROM {id}.rows"),
                        &[],
                    )
                    .await?
                    .get::<_, bool>(0)
            );
            // UUID-scoped directories remain independent at test-threads=8.
            // Scan the actual raw buffer and coordinator journal files.
            assert!(assert_no_sentinel(&root.join("buffer"))? > 0);
            assert_no_sentinel(&root)?;
        }
        let readiness = engine.evaluate_readiness().await?;
        assert!(readiness.all_chunks_done);
        assert!(!readiness.blockers().is_empty());
        assert!(
            !readiness.lineage_matches,
            "connected lineage producer is not wired"
        );
        assert!(!readiness.indexes_constraints_valid);
        if !verification {
            let retention = engine
                .runtime()
                .retention_status()
                .await
                .ok_or("retention status absent")?;
            assert!(retention.blocked_reason.is_none());
            if !partial {
                assert!(
                    retention.deleted_bytes > 0,
                    "real durable destination receipts did not release buffer bytes"
                );
            }
        } else {
            assert!(engine.runtime().retention_status().await.is_none());
        }
        applier.inner.destination.destination.close().await?;
        TestResult::Ok(())
    })
    .catch_unwind()
    .await;
    // Remove the trigger before other source DDL, after dropping capture's slot
    // connection. Only this UUID's resources are removed, including on failure.
    let sql = TriggerSql::new(
        &id,
        cfg["pg_version"].as_u64().ok_or("H1 pg_version")? as u32 * 10000,
    )?;
    let cleanup=async {
        cleanup_source.batch_execute(&sql.remove).await?;
        if matches!(final_marker, Some(MarkerEmitter::Native | MarkerEmitter::NativeRestart)) {
            cleanup_destination.batch_execute(&sql.remove).await?;
            cleanup_destination.query("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name=$1", &[&format!("skedaddle_{id}_rev")]).await?;
            cleanup_destination.batch_execute(&format!("DROP PUBLICATION IF EXISTS skedaddle_{id}_rev")).await?;
            cleanup_source.query("SELECT pg_replication_origin_drop(roname) FROM pg_replication_origin WHERE roname=$1", &[&format!("skedaddle_{id}_rev")]).await?;
        }
        if matches!(final_marker, Some(MarkerEmitter::Native | MarkerEmitter::NativeRestart)) {
            cleanup_source.execute("DELETE FROM _skedaddle.cutover_marker WHERE migration=$1", &[&id]).await?;
            cleanup_source.execute("DELETE FROM _skedaddle.fence_state WHERE state::jsonb->>'migration'=$1", &[&id]).await?;
            cleanup_source.execute("DELETE FROM _skedaddle.authority WHERE migration=$1", &[&id]).await?;
        }
        cleanup_source.query_one("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name=$1",&[&slot]).await.ok();
        cleanup_source.batch_execute(&format!("DROP PUBLICATION IF EXISTS {publication};DROP SCHEMA IF EXISTS {id} CASCADE")).await?;
        if matches!(final_marker, Some(MarkerEmitter::Native | MarkerEmitter::NativeRestart)) {
            cleanup_source.batch_execute(&format!("DROP ROLE IF EXISTS {application}")).await?;
        }
        cleanup_destination.query_one("SELECT pg_replication_origin_drop($1)",&[&origin]).await?;
        cleanup_destination.batch_execute(&format!("DROP SCHEMA IF EXISTS {id} CASCADE")).await?;
        if matches!(final_marker, Some(MarkerEmitter::Native | MarkerEmitter::NativeRestart)) {
            cleanup_destination.batch_execute(&format!("DROP ROLE IF EXISTS {id}_rev_app")).await?;
        }
        TestResult::Ok(())
    }.await;
    match result {
        Ok(r) => {
            r?;
            cleanup
        }
        Err(p) => std::panic::resume_unwind(p),
    }
}

async fn qualify_live_readiness(
    runtime: &engine::Runtime,
    source: &Client,
    destination: &Client,
    replication: &ConnectionConfig,
    schema: &str,
) -> TestResult {
    let mut definition = skedaddle::MigrationDefinition::from_json(
        include_str!("../README.md")
            .split("```json")
            .nth(1)
            .unwrap()
            .split("```")
            .next()
            .unwrap(),
    )?;
    definition.readiness_policy.max_lag_seconds = 1;
    definition.budgets.max_replication_lag_seconds = 1;
    async fn probe(
        runtime: &engine::Runtime,
        source: &Client,
        config: &ConnectionConfig,
    ) -> TestResult {
        let started = std::time::Instant::now();
        let connected = source::connect(config).await?;
        runtime
            .probe_readiness(source, connected.lineage(), started)
            .await?;
        Ok(())
    }
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            probe(runtime, source, replication).await?;
            tokio::time::sleep(Duration::from_millis(25)).await;
            let live = runtime.live_readiness();
            if live
                .lag
                .as_ref()
                .is_some_and(|lag| lag.within_budget(&definition))
                && live.running
                && live.schema_fresh
                && live
                    .schema
                    .as_ref()
                    .is_some_and(|s| s.blocked_boundary.is_none() && !s.unknown_schema_job)
            {
                break;
            }
        }
        TestResult::Ok(())
    })
    .await??;
    // A real row lock holds destination apply while real source transactions
    // accumulate beyond the definition's one-second policy while source
    // probes remain fresh. Age, independently of byte distance, must fail.
    destination
        .batch_execute(&format!(
            "BEGIN;UPDATE {schema}.rows SET body=body WHERE id=7"
        ))
        .await?;
    let burst = async {
        for i in 0..20 {
            source
                .execute(
                    &format!("UPDATE {schema}.rows SET body=$1 WHERE id=7"),
                    &[&format!("e5-burst-{i}")],
                )
                .await?;
        }
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                probe(runtime, source, replication).await?;
                tokio::time::sleep(Duration::from_millis(25)).await;
                if let Some(lag) = runtime.live_readiness().lag
                    && lag.oldest_unapplied_age_ms > 1000
                    && lag.lag_bytes > 0
                    && !lag.within_budget(&definition)
                {
                    break;
                }
            }
            TestResult::Ok(())
        })
        .await??;
        TestResult::Ok(())
    }
    .await;
    destination.batch_execute("ROLLBACK").await?;
    burst?;
    source
        .batch_execute(&format!(
            "ALTER TABLE {schema}.rows ADD COLUMN e5_policy_boundary integer"
        ))
        .await?;
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(boundary) = runtime
                .live_readiness()
                .schema
                .as_ref()
                .and_then(|s| s.blocked_boundary.as_ref())
            {
                assert!(boundary.readiness_blocked);
                assert!(!boundary.reason.is_empty() && !boundary.remediation.is_empty());
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await?;
    Ok(())
}

struct Int4KeyOrder;
impl KeyOrder for Int4KeyOrder {
    fn compare_keys(&self, left: &verify::Key, right: &verify::Key) -> verify::Result<Ordering> {
        match (left.as_slice(), right.as_slice()) {
            ([Value::Int4(a)], [Value::Int4(b)]) => Ok(a.cmp(b)),
            _ => Err(verify::VerifyError::Invalid("qualified int4 key required")),
        }
    }
}

async fn verification_rows(
    dsn: &str,
    ca: &str,
    id: &str,
    side: Side,
) -> TestResult<PgRowSource<Int4KeyOrder>> {
    Ok(PgRowSource::connect(
        &pg::tls::Config::new(dsn.parse()?, ca),
        side,
        table(id),
        vec![
            Column {
                name: "id".into(),
                pg_type: PgType::Int4,
            },
            Column {
                name: "body".into(),
                pg_type: text(),
            },
        ],
        Int4KeyOrder,
        RangeBudget {
            max_rows: 1000,
            max_bytes: 1_000_000,
        },
    )
    .await?)
}

// This wrapper only schedules real SQL after arm and before snapshot creation;
// capture, decode, durable buffer, applied receipts and evidence stay production.
struct ArmingWrites<E> {
    inner: E,
    writer: Client,
    sql: String,
}
#[async_trait]
impl<E: SqlChangeEvidence> SqlChangeEvidence for ArmingWrites<E> {
    async fn arm(&mut self, range: &verify::RangePlan) -> verify::Result<()> {
        self.inner.arm(range).await?;
        self.writer
            .batch_execute(&self.sql)
            .await
            .map_err(|_| verify::VerifyError::Read("live verification arm workload failed".into()))
    }
    async fn changes(
        &mut self,
        range: &verify::RangePlan,
        lsn: u64,
    ) -> verify::Result<verify::ChangeEvidence> {
        self.inner.changes(range, lsn).await
    }
}

async fn qualify_live_verification(
    runtime: &engine::Runtime,
    source_dsn: &str,
    destination_dsn: &str,
    ca: &str,
    id: &str,
    source: &Client,
) -> TestResult {
    let limits = engine::VerificationLimits {
        max_keys: 1000,
        max_bytes: 1_000_000,
        timeout: Duration::from_secs(15),
    };
    let range = ChunkPlan::new(table(id), 1000, vec![])?.ranges.remove(0);
    let projection = verify::Projection::new(vec!["body".into()])?;
    let (mut evidence, mut waiter) = runtime.pg_verification(
        connect(source_dsn, ca).await?,
        table(id),
        Int4KeyOrder,
        limits,
    )?;
    let mut source_rows = verification_rows(source_dsn, ca, id, Side::Source).await?;
    let mut destination_rows =
        verification_rows(destination_dsn, ca, id, Side::Destination).await?;
    assert_eq!(
        std::mem::size_of_val(&evidence.arm(&range)),
        2 * std::mem::size_of::<usize>()
    );
    assert_eq!(
        std::mem::size_of_val(&evidence.changes(&range, 0)),
        2 * std::mem::size_of::<usize>()
    );
    assert_eq!(
        std::mem::size_of_val(&waiter.wait_until(0)),
        2 * std::mem::size_of::<usize>()
    );
    // This transaction writes BEFORE arm and commits AFTER the source snapshot.
    // No sampled LSN filtering may discard its row changes.
    let inflight = connect(source_dsn, ca).await?;
    inflight
        .batch_execute(&format!(
            "BEGIN;UPDATE {id}.rows SET body='inflight' WHERE id=7"
        ))
        .await?;
    evidence.arm(&range).await?;
    source
        .batch_execute(&format!("INSERT INTO {id}.rows VALUES(203,'during-arm')"))
        .await?;
    let snapshot_sql = format!("INSERT INTO {id}.rows VALUES(204,'during-snapshot')");
    let during_snapshot = source.batch_execute(&snapshot_sql);
    let (snapshot, snapshot_write) = tokio::join!(
        source_rows.read_with_lsn(&range, &projection),
        during_snapshot
    );
    let snapshot = snapshot?;
    snapshot_write?;
    inflight.batch_execute("COMMIT").await?;
    // Include inserts, deletes, old/new identities, transaction-local reverted
    // writes, and a rolled-back write with no externally visible effect.
    source.batch_execute(&format!(
        "INSERT INTO {id}.rows VALUES(201,'insert');DELETE FROM {id}.rows WHERE id=10;UPDATE {id}.rows SET id=208 WHERE id=8;BEGIN;UPDATE {id}.rows SET body='temporary' WHERE id=7;UPDATE {id}.rows SET body='inflight' WHERE id=7;INSERT INTO {id}.rows VALUES(299,'reverted');DELETE FROM {id}.rows WHERE id=299;COMMIT;BEGIN;INSERT INTO {id}.rows VALUES(298,'aborted');ROLLBACK"
    )).await?;
    // Snapshot WAL-end can lie beyond the last row commit. The waiter must get
    // an authenticated applied SOURCE marker rather than use destination WAL.
    let wait_sql = format!("INSERT INTO {id}.rows VALUES(205,'during-wait')");
    let during_wait = source.batch_execute(&wait_sql);
    let (waited, wait_write) = tokio::join!(waiter.wait_until(snapshot.lsn), during_wait);
    assert!(waited? >= snapshot.lsn);
    wait_write?;
    let during_destination = async {
        source.batch_execute(&format!("UPDATE {id}.rows SET body='during-destination' WHERE id=11;INSERT INTO {id}.rows VALUES(202,'during-destination')")).await
    };
    let (destination_read, writes) = tokio::join!(
        destination_rows.fetch(&range, &projection),
        during_destination
    );
    destination_read?;
    writes?;
    let changes = evidence.changes(&range, snapshot.lsn).await?;
    for id in [7, 8, 10, 11, 201, 202, 203, 204, 205, 208, 299] {
        assert!(
            changes.keys.contains(&vec![Value::Int4(id)]),
            "missing real captured key {id}"
        );
    }
    assert!(
        !changes.keys.contains(&vec![Value::Int4(298)]),
        "aborted transaction became committed evidence"
    );
    assert!(
        changes
            .keys
            .windows(2)
            .all(|w| Int4KeyOrder.compare_keys(&w[0], &w[1]).unwrap() == Ordering::Less)
    );
    assert!(
        waiter.wait_until(changes.through_lsn).await? >= changes.through_lsn,
        "capture-only marker cannot satisfy applied proof"
    );

    // An empty armed interval still needs a fresh real committed barrier.
    evidence.arm(&range).await?;
    let empty = evidence.changes(&range, changes.through_lsn).await?;
    assert!(empty.keys.is_empty());
    assert!(waiter.wait_until(empty.through_lsn).await? >= empty.through_lsn);
    evidence.arm(&range).await?;
    evidence.disarm();
    evidence.disarm();
    assert!(matches!(
        evidence.changes(&range, empty.through_lsn).await,
        Err(verify::VerifyError::EvidenceInvalidated {
            reason: verify::EvidenceInvalidation::CaptureGap
        })
    ));
    evidence.arm(&range).await?;
    let rearmed = evidence.changes(&range, empty.through_lsn).await?;
    assert!(rearmed.keys.is_empty());
    waiter.wait_until(rearmed.through_lsn).await?;

    // Overflow propagates all the way through the real SQL boundary session.
    // It must not emit a successful/mismatched comparison, even for empty reads.
    for (max_keys, max_bytes, start_id, expected_limit) in [
        (1, 1_000_000, 301, verify::EvidenceLimit::Keys(1)),
        (1000, 1, 303, verify::EvidenceLimit::EncodedBytes(1)),
    ] {
        let (bounded, mut apply) = runtime.pg_verification(
            connect(source_dsn, ca).await?,
            table(id),
            Int4KeyOrder,
            engine::VerificationLimits {
                max_keys,
                max_bytes,
                ..limits
            },
        )?;
        let provider = ArmingWrites {
            inner: bounded,
            writer: connect(source_dsn, ca).await?,
            sql: format!(
                "INSERT INTO {id}.rows VALUES({start_id},'overflow'),({},'overflow')",
                start_id + 1
            ),
        };
        let mut read = verification_rows(source_dsn, ca, id, Side::Source)
            .await?
            .with_change_evidence(provider);
        let mut session = verify::VerificationSession::new(range.clone(), projection.clone(), 0)?;
        let error = session
            .run(&mut read, &mut destination_rows, &mut apply)
            .await
            .unwrap_err();
        assert!(
            matches!(error, verify::VerifyError::EvidenceOverflow { range: affected, limit } if affected == range.id && limit == expected_limit)
        );
        assert!(matches!(
            session.state(),
            verify::BoundaryState::NotVerifiable { .. }
        ));
        assert!(session.result().is_none());
    }

    // A caller can cancel marker emission/observation; its observer is removed,
    // and a later interval requires a distinct fresh marker.
    evidence.arm(&range).await?;
    let mut cancelled = Box::pin(evidence.changes(&range, empty.through_lsn));
    assert!(matches!(
        futures::poll!(cancelled.as_mut()),
        std::task::Poll::Pending
    ));
    drop(cancelled);
    evidence.arm(&range).await?;
    let after_cancel = evidence.changes(&range, empty.through_lsn).await?;
    waiter.wait_until(after_cancel.through_lsn).await?;

    let (_, mut timeout_waiter) = runtime.pg_verification(
        connect(source_dsn, ca).await?,
        table(id),
        Int4KeyOrder,
        engine::VerificationLimits {
            timeout: Duration::from_millis(30),
            ..limits
        },
    )?;
    assert!(matches!(
        timeout_waiter.wait_until(u64::MAX).await,
        Err(verify::VerifyError::ApplierTimeout {
            waited_lsn: u64::MAX,
            ..
        })
    ));
    // The ONLY injected fault: real stream capture cannot deliberately omit a
    // committed interval. It fails closed alongside the real path exercised above.
    evidence.arm(&range).await?;
    runtime.invalidate_verification_capture_gap();
    assert!(matches!(
        evidence.changes(&range, after_cancel.through_lsn).await,
        Err(verify::VerifyError::EvidenceInvalidated {
            reason: verify::EvidenceInvalidation::CaptureGap
        })
    ));
    assert!(matches!(
        waiter.wait_until(0).await,
        Err(verify::VerifyError::EvidenceInvalidated {
            reason: verify::EvidenceInvalidation::CaptureGap
        })
    ));
    Ok(())
}

// A real operational runner needs PG for descriptor qualification and reconnect.
// The local-only Engine tests above cannot exercise this connection ordering.
#[tokio::test]
async fn operational_runner_restart_restores_schema_before_reconnect_live() -> TestResult {
    let Some(path) = std::env::var_os("SKEDADDLE_TEST_CONFIG") else {
        skip(
            "runner schema restart: SKEDADDLE_TEST_CONFIG absent; PostgreSQL restart/reconnect qualification NOT RUN",
        );
        return Ok(());
    };
    let cfg: serde_json::Value = serde_json::from_slice(&std::fs::read(path)?)?;
    let source_dsn = cfg["pg_ddl"].as_str().ok_or("runner pg_ddl DSN absent")?;
    let destination_dsn = cfg["pg_apply"]
        .as_str()
        .ok_or("runner pg_apply DSN absent")?;
    let ca = cfg["ca_pem"].as_str().ok_or("runner CA absent")?;
    let source = connect(source_dsn, ca).await?;
    let destination = connect(destination_dsn, ca).await?;
    let schema = format!("rs{}", uuid::Uuid::new_v4().simple());
    let root = PathBuf::from(std::env::var_os("SKEDADDLE_TEST_ROOT").ok_or("task root absent")?)
        .join(&schema);
    std::fs::create_dir_all(&root)?;
    let ddl = format!(
        "CREATE SCHEMA {schema};CREATE TABLE {schema}.rows(id integer PRIMARY KEY,body text)"
    );
    source.batch_execute(&ddl).await?;
    destination.batch_execute(&ddl).await?;
    source
        .batch_execute(&format!("INSERT INTO {schema}.rows VALUES(1,'seed')"))
        .await?;
    let example = include_str!("../README.md")
        .split("```json")
        .nth(1)
        .ok_or("definition example absent")?
        .split("```")
        .next()
        .ok_or("definition example end absent")?;
    let mut value: serde_json::Value = serde_json::from_str(example)?;
    value["buffer"]["backend"] =
        serde_json::json!({"type":"local-fs","options":{"path":root.join("buffer")}});
    value["selected_objects"] = serde_json::json!([{"schema":schema,"name":"rows","kind":"table"}]);
    for (side, dsn) in [("source", source_dsn), ("destination", destination_dsn)] {
        let connection = root.join(format!("{side}.secret"));
        let trust = root.join(format!("{side}-ca.secret"));
        std::fs::write(&connection, dsn)?;
        std::fs::write(&trust, ca)?;
        value[side]["connection_ref"] = serde_json::json!(format!("file:{}", connection.display()));
        value[side]["tls_ca_ref"] = serde_json::json!(format!("file:{}", trust.display()));
        value[side]["tls"] = serde_json::json!("require");
    }
    let definition = skedaddle::MigrationDefinition::from_json(&value.to_string())?;
    let id = definition.canonical_digest()?[..24].to_owned();
    let result = AssertUnwindSafe(async {
        let journal = Journal::new(Arc::new(skedaddle::coordinator::journal::local_store(&root)?), "skedaddle", &id, Duration::from_secs(5))?;
        let mut owner = Coordinator::observer(journal, Local::create(root.join(format!("{id}.local")))?, "runner-schema-fixture")?;
        owner.acquire().await?;
        owner.commit(Transition::Define { definition: Box::new(definition.clone()) }).await?;
        owner.commit(Transition::Lifecycle { event: Event::Advance { to: State::Assessed } }).await?;
        drop(owner);
        let (_pause, paused) = tokio::sync::watch::channel(false);
        let first = Box::pin(skedaddle::runner::run(&root, &id, &definition, false, engine::StopAt::CatchingUp, paused)).await.map_err(runner_error)?;
        assert_eq!(first.state, State::CatchingUp);
        let mut journal = skedaddle::runner::open_journal(&root, &id).await?;
        let initial = runner_schema(&root, &id, &journal).await?;
        let initial_version = initial["schemas"]["last_version"].as_u64().ok_or("runner schema version absent")?;
        let mut epoch = journal.load(true).await?.epoch;
        for next in [initial_version + 1, initial_version + 2] {
            let (pause, paused) = tokio::sync::watch::channel(false);
            let operation = Box::pin(skedaddle::runner::run(&root, &id, &definition, true, engine::StopAt::UntilPaused, paused));
            let drive = async {
                loop {
                    let state = journal.load(true).await?;
                    if state.epoch > epoch && root.join(format!("{id}.control/owner-generation")).exists() { break; }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                source.batch_execute(&format!("BEGIN;SET LOCAL skedaddle.admin_origin='{id}';REVOKE INSERT ON {schema}.rows FROM PUBLIC;COMMIT")).await?;
                loop {
                    let snapshot = runner_schema(&root, &id, &journal).await?;
                    if snapshot["schemas"]["last_version"].as_u64() == Some(next) {
                        assert!(snapshot["schemas"]["blocker"].is_null());
                        pause.send_replace(true);
                        return TestResult::Ok(());
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            };
            let (run, drive) = tokio::time::timeout(Duration::from_secs(60), async { tokio::join!(operation, drive) }).await?;
            drive?;
            let reached = run.map_err(runner_error)?;
            assert_eq!(reached.state, State::Paused);
            journal = skedaddle::runner::open_journal(&root, &id).await?;
            let state = journal.load(true).await?;
            assert!(state.epoch > epoch);
            epoch = state.epoch;
            assert_eq!(runner_schema(&root, &id, &journal).await?["schemas"]["last_version"].as_u64(), Some(next));
        }
        assert_eq!(rows(&source, &schema).await?, rows(&destination, &schema).await?);
        TestResult::Ok(())
    }).catch_unwind().await;
    let cleanup = async {
        let trigger = TriggerSql::new(&id, cfg["pg_version"].as_u64().ok_or("PG version absent")? as u32 * 10000)?;
        source.batch_execute(&trigger.remove).await?;
        let slot = format!("skedaddle_{id}");
        source.query("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name=$1", &[&slot]).await?;
        source.batch_execute(&format!("DROP PUBLICATION IF EXISTS {slot};DROP SCHEMA {schema} CASCADE")).await?;
        destination.query("SELECT pg_replication_origin_drop(roname) FROM pg_replication_origin WHERE roname=$1", &[&slot]).await?;
        destination.batch_execute(&format!("DROP SCHEMA {schema} CASCADE")).await?;
        TestResult::Ok(())
    }.await;
    match result {
        Ok(result) => {
            result?;
            cleanup
        }
        Err(panic) => std::panic::resume_unwind(panic),
    }
}
fn runner_error(error: skedaddle::runner::RunError) -> Box<dyn Error + Send + Sync> {
    match error {
        skedaddle::runner::RunError::Engine(error) => Box::new(error),
        skedaddle::runner::RunError::Preparation(_) => "runner preparation failed".into(),
    }
}
async fn runner_schema(
    root: &std::path::Path,
    id: &str,
    journal: &Journal,
) -> TestResult<serde_json::Value> {
    let stream = serde_json::json!(["runner", id, format!("skedaddle-{id}-wal")]).to_string();
    let state = journal.load(true).await?;
    let (head, _) = state
        .progress
        .get(&format!("capture-schema:{stream}"))
        .ok_or("runner schema head absent")?;
    let head: serde_json::Value = serde_json::from_str(head)?;
    let digest = head["current"]["digest"]
        .as_str()
        .ok_or("runner current schema absent")?;
    let path = root.join(format!(
        "capture-schema/{:x}/{:x}/{digest}.json",
        Sha256::digest(journal.namespace().as_bytes()),
        Sha256::digest(stream.as_bytes())
    ));
    Ok(serde_json::from_slice(&std::fs::read(path)?)?)
}
