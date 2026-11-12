//! Only the database side is doubled: setup SQL, snapshot reads, pgoutput
//! transport and destination execution. This permits deterministic crash/error
//! injection; engine, capture, codecs, local-FS buffer, reader and journal are
//! real. tests/engine.rs qualifies the same adapters against PostgreSQL.
use super::*;
use crate::{
    budget::{Limits, RateLimit, ReadLimits, SystemClock},
    buffer::{
        backends::{BufferStore, ObjectBufferStore},
        format,
        reader::{BufferReader, ReaderCheckpoint, ReaderConfig},
        writer::{BufferWriter, WriterConfig},
    },
    coordinator::{Authority, local::Local},
    event::{EventError, OpaqueValue, PositionOrder},
    mapping::{PgType, Value},
    pg::{
        apply::{self, CommitDecision, executor::Destination},
        chunk::{Direction, KeyColumn, QualifiedName, TableKey},
        ddl::SchemaVersions,
        pgoutput::{Relation, RelationColumn},
        source::{ColumnMapping, TypeResolver},
    },
};
use async_trait::async_trait;
use std::{
    cmp::Ordering,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering as AtomicOrdering},
};

struct Order;
impl PositionOrder for Order {
    fn domain(&self) -> &str {
        "engine-test"
    }
    fn compare(&self, a: &[u8], b: &[u8]) -> std::result::Result<Ordering, EventError> {
        Ok(a.cmp(b))
    }
}
struct Resolver;
impl TypeResolver for Resolver {
    fn resolve(
        &mut self,
        _: &Relation<'_>,
        _: &RelationColumn<'_>,
    ) -> source::Result<ColumnMapping> {
        Ok(ColumnMapping::identity(PgType::Int4))
    }
}
/// DB byte transport is doubled to inject a precise disconnect and marker order.
struct Transport(mpsc::Receiver<source::Result<Vec<u8>>>);
impl source::Transport for Transport {
    async fn next(&mut self, _: u64) -> source::Result<Vec<u8>> {
        self.0
            .recv()
            .await
            .unwrap_or_else(|| Err(source::Error::RecoveryRequired("injected disconnect")))
    }
}
struct Setup(Arc<AtomicU64>);
impl Installer for Setup {
    fn identity(&self) -> String {
        "test-slot".into()
    }
    async fn install(&mut self, step: Installation) -> Result<()> {
        if step == Installation::Slot {
            self.0.store(1, AtomicOrdering::SeqCst);
        }
        Ok(())
    }
}
fn wal(data: Vec<u8>) -> Vec<u8> {
    let mut p = vec![b'w'];
    p.extend(1u64.to_be_bytes());
    p.extend(10000u64.to_be_bytes());
    p.extend(0i64.to_be_bytes());
    p.extend(data);
    p
}
async fn marker(tx: &mpsc::Sender<source::Result<Vec<u8>>>, marker: Watermark, lsn: u64) {
    let mut p = vec![b'B'];
    p.extend(lsn.to_be_bytes());
    p.extend(0i64.to_be_bytes());
    p.extend((lsn as u32).to_be_bytes());
    tx.send(Ok(wal(p))).await.unwrap();
    let content = marker.encode().unwrap();
    let mut p = vec![b'M', 1];
    p.extend((lsn - 1).to_be_bytes());
    p.extend(crate::pg::chunk::MESSAGE_PREFIX.as_bytes());
    p.push(0);
    p.extend((content.len() as u32).to_be_bytes());
    p.extend(content);
    tx.send(Ok(wal(p))).await.unwrap();
    let mut p = vec![b'C', 0];
    p.extend(lsn.to_be_bytes());
    p.extend((lsn + 1).to_be_bytes());
    p.extend(0i64.to_be_bytes());
    tx.send(Ok(wal(p))).await.unwrap();
}
/// Snapshot DB is doubled so a pause/crash can land between Started and H.
struct Database {
    tx: mpsc::Sender<source::Result<Vec<u8>>>,
    slot: Arc<AtomicU64>,
    lsn: u64,
    reads: Arc<AtomicU64>,
    pause_on_read: Option<watch::Sender<bool>>,
    stall_read: bool,
}
impl ChunkDatabase for Database {
    async fn emit(&mut self, wm: Watermark) -> Result<()> {
        assert_eq!(
            self.slot.load(AtomicOrdering::SeqCst),
            1,
            "first low preceded slot creation"
        );
        self.lsn += 10;
        marker(&self.tx, wm, self.lsn).await;
        Ok(())
    }
    async fn read(&mut self, _: &ChunkPlan, _: &ChunkRange) -> Result<Vec<Row>> {
        self.reads.fetch_add(1, AtomicOrdering::SeqCst);
        if self.stall_read {
            std::future::pending::<()>().await;
        }
        if let Some(pause) = self.pause_on_read.take() {
            pause.send_replace(true);
        }
        Ok(vec![
            [(
                "id".into(),
                OpaqueValue {
                    type_id: "int4".into(),
                    bytes: Some(Value::Int4(7).canonical_bytes().unwrap()),
                },
            )]
            .into(),
        ])
    }
}
/// Destination SQL is doubled only to count atomic commits. ReaderLoop and
/// checkpoint persistence run unchanged against the actual on-disk buffer.
struct DestinationDb(Arc<AtomicU64>);
#[async_trait]
impl Destination for DestinationDb {
    async fn apply(
        &mut self,
        _: &apply::TransactionPlan,
    ) -> crate::pg::apply::executor::Result<CommitDecision> {
        self.0.fetch_add(1, AtomicOrdering::SeqCst);
        Ok(CommitDecision::Applied)
    }
    async fn retain_after_checkpoint(
        &mut self,
        _: &ReaderCheckpoint,
    ) -> crate::pg::apply::executor::Result<()> {
        Ok(())
    }
}
type Planner = fn(
    &AssembledTxn,
    u64,
    &Materialized,
) -> std::result::Result<apply::TransactionPlan, apply::ApplyError>;
fn planner(
    txn: &AssembledTxn,
    mseq: u64,
    _: &Materialized,
) -> std::result::Result<apply::TransactionPlan, apply::ApplyError> {
    apply::plan_transaction(
        txn,
        &[apply::Table {
            event_table: "rows".into(),
            schema: "public".into(),
            name: "rows".into(),
            schema_version: 1,
            columns: vec![apply::Column {
                name: "id".into(),
                type_id: "int4".into(),
                pg_type: PgType::Int4,
                wire_type: tokio_postgres::types::Type::INT4,
            }],
            key: vec!["id".into()],
        }],
        &apply::PlanContext {
            stream: "wal".into(),
            mseq,
            origin: "test".into(),
            synthetic_commit_time: Some(0),
        },
        |_, _| Ok(apply::RangeState::Copying),
    )
}
struct Fixture {
    engine: Engine,
    journal: Journal,
    journal_objects: Arc<dyn object_store::ObjectStore>,
    store: Arc<ObjectBufferStore>,
    root: PathBuf,
    slot: Arc<AtomicU64>,
    config: ReaderConfig,
}
fn source() -> SourceIdentity {
    SourceIdentity {
        engine: "postgres".into(),
        system_id: "1".into(),
        lineage: "1".into(),
        db_name: "db".into(),
        slot: "slot".into(),
    }
}
fn plan() -> ChunkPlan {
    ChunkPlan::new(
        TableKey {
            table: "rows".into(),
            relation: QualifiedName {
                schema: "public".into(),
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
        },
        100,
        vec![],
    )
    .unwrap()
}
fn config() -> CopyConfig {
    CopyConfig {
        source: source(),
        order_domain: "engine-test".into(),
        schema_version: 1,
        read_rows: 100,
        read_bytes: 10000,
        window_bytes: 1_000_000,
    }
}
fn budget() -> ThroughputBudget {
    budget_with_clock(Arc::new(SystemClock::default()))
}
fn budget_with_clock(clock: Arc<dyn budget::Clock>) -> ThroughputBudget {
    let rate = RateLimit {
        per_second: 1_000_000,
        burst: 1_000_000,
    };
    ThroughputBudget::new(
        Limits {
            chunk_reads: ReadLimits {
                rows: rate,
                bytes: rate,
            },
            verification_reads: ReadLimits {
                rows: rate,
                bytes: rate,
            },
            applier_transactions: rate,
            connections: Default::default(),
            in_memory_segments: 2,
        },
        clock,
    )
    .unwrap()
}
impl Fixture {
    async fn new() -> Self {
        Self::with_definition(None).await
    }
    async fn with_definition(definition: Option<crate::MigrationDefinition>) -> Self {
        let root =
            PathBuf::from(std::env::var_os("SKEDADDLE_TEST_ROOT").expect("set task test root"))
                .join(format!("engine-{}", uuid::Uuid::new_v4()));
        for p in ["journal", "buffer", "scratch"] {
            std::fs::create_dir_all(root.join(p)).unwrap();
        }
        let store = Arc::new(
            ObjectBufferStore::local(root.join("buffer"), Duration::from_secs(3)).unwrap(),
        );
        let config = ReaderConfig {
            prefix: "engine".into(),
            migration_id: "test".into(),
            stream: "wal".into(),
            limits: format::Limits::default(),
            required_next_mseq: 0,
        };
        let journal_objects =
            Arc::new(coordinator::journal::local_store(root.join("journal")).unwrap());
        let journal = Journal::new(
            journal_objects.clone(),
            "engine",
            "test",
            Duration::from_secs(3),
        )
        .unwrap()
        .with_chunk_buffer(store.clone(), config.clone(), Arc::new(Order));
        let mut coordinator = Coordinator::observer(
            journal.clone(),
            Local::create(root.join("local")).unwrap(),
            "engine-test",
        )
        .unwrap();
        coordinator.acquire().await.unwrap();
        if let Some(definition) = definition {
            coordinator
                .commit(Transition::Define {
                    definition: Box::new(definition),
                })
                .await
                .unwrap();
        }
        coordinator
            .commit(Transition::Lifecycle {
                event: Event::Advance {
                    to: State::Assessed,
                },
            })
            .await
            .unwrap();
        let engine = Engine::new(coordinator, journal.clone());
        let slot = Arc::new(AtomicU64::new(0));
        engine.start(&mut Setup(slot.clone())).await.unwrap();
        engine.plan(&plan()).await.unwrap();
        Self {
            engine,
            journal,
            journal_objects,
            store,
            root,
            slot,
            config,
        }
    }
    async fn capture(
        &self,
        rx: mpsc::Receiver<source::Result<Vec<u8>>>,
        lsn: u64,
    ) -> PgCapture<Resolver, Transport> {
        let (tx, _) = std::sync::mpsc::channel();
        let writer = BufferWriter::open_with_scratch(
            self.store.clone(),
            WriterConfig {
                prefix: "engine".into(),
                migration_id: "test".into(),
                stream: "wal".into(),
                epoch: 1,
                segment_bytes: 1_000_000,
                segment_transactions: 100,
                segment_age: Duration::from_secs(1),
                max_transaction_bytes: 1_000_000,
            },
            Arc::new(Order),
            tx,
            self.root.join("scratch"),
        )
        .await
        .unwrap();
        let capture = capture(
            source(),
            "engine-test".into(),
            lsn,
            writer,
            Resolver,
            "test".into(),
            SchemaVersions::new("test".into(), 0, []).unwrap(),
        )
        .unwrap();
        let (gate, paused) = watch::channel(false);
        // Keep a live admission sender for the run without leaking it.
        drop(gate);
        PgCapture::new(
            capture,
            Transport(rx),
            paused,
            self.journal_objects.clone(),
            serde_json::json!(["engine", "test", "wal"]).to_string(),
        )
    }
    fn applier(
        &self,
        count: Arc<AtomicU64>,
        checkpoint: ReaderCheckpoint,
    ) -> PgApplier<DestinationDb, Planner> {
        PgApplier::new(
            BufferReader::new(
                self.store.clone(),
                self.config.clone(),
                checkpoint,
                Arc::new(Order),
            )
            .unwrap(),
            "wal".into(),
            DestinationDb(count),
            planner as Planner,
        )
    }
    fn database(
        &self,
        tx: mpsc::Sender<source::Result<Vec<u8>>>,
        reads: Arc<AtomicU64>,
    ) -> Database {
        Database {
            tx,
            slot: self.slot.clone(),
            lsn: 10,
            reads,
            pause_on_read: None,
            stall_read: false,
        }
    }
}
// Admission is false, so source::step never waits on changed(). A dropped sender
// is legal in that state; no global state or leaked channel is required.

#[tokio::test]
async fn delayed_manifest_acknowledgement_completes_without_tick_cancellation() {
    use crate::buffer::backends as backend;
    struct DelayedAck {
        real: Arc<ObjectBufferStore>,
        manifest_puts: Arc<AtomicU64>,
        segment_puts: Arc<AtomicU64>,
    }
    #[async_trait]
    impl BufferStore for DelayedAck {
        fn report(&self) -> backend::BackendReport {
            self.real.report()
        }
        async fn put_create_only(
            &self,
            key: &str,
            bytes: &[u8],
        ) -> backend::Result<backend::PutOutcome> {
            self.manifest_puts.fetch_add(1, AtomicOrdering::SeqCst);
            let result = self.real.put_create_only(key, bytes).await;
            // Fault injection: actual local publication is complete, but the
            // caller sees a response later than the engine's schema tick.
            tokio::time::sleep(Duration::from_millis(75)).await;
            result
        }
        async fn put_file_create_only(
            &self,
            key: &str,
            file: std::fs::File,
        ) -> backend::Result<backend::PutOutcome> {
            self.segment_puts.fetch_add(1, AtomicOrdering::SeqCst);
            self.real.put_file_create_only(key, file).await
        }
        async fn get(&self, key: &str) -> backend::Result<Option<Vec<u8>>> {
            self.real.get(key).await
        }
        async fn get_into(
            &self,
            key: &str,
            output: &mut (dyn std::io::Write + Send),
        ) -> backend::Result<Option<backend::ObjectDigest>> {
            self.real.get_into(key, output).await
        }
        async fn list_prefix(
            &self,
            prefix: &str,
            token: Option<&str>,
            limit: usize,
        ) -> backend::Result<backend::ListPage> {
            self.real.list_prefix(prefix, token, limit).await
        }
        async fn delete(&self, key: &str) -> backend::Result<()> {
            self.real.delete(key).await
        }
    }
    let f = Fixture::new().await;
    let manifest_puts = Arc::new(AtomicU64::new(0));
    let segment_puts = Arc::new(AtomicU64::new(0));
    let store = Arc::new(DelayedAck {
        real: f.store.clone(),
        manifest_puts: manifest_puts.clone(),
        segment_puts: segment_puts.clone(),
    });
    let (ack, _) = std::sync::mpsc::channel();
    let writer = BufferWriter::open_with_scratch(
        store,
        WriterConfig {
            prefix: "engine".into(),
            migration_id: "test".into(),
            stream: "wal".into(),
            epoch: 1,
            segment_bytes: 1_000_000,
            segment_transactions: 100,
            segment_age: Duration::from_secs(1),
            max_transaction_bytes: 1_000_000,
        },
        Arc::new(Order),
        ack,
        f.root.join("scratch"),
    )
    .await
    .unwrap();
    let raw_capture = capture(
        source(),
        "engine-test".into(),
        10,
        writer,
        Resolver,
        "test".into(),
        SchemaVersions::new("test".into(), 0, []).unwrap(),
    )
    .unwrap();
    let (_admit, admitted) = watch::channel(false);
    let (tx, rx) = mpsc::channel(32);
    let capture = PgCapture::new(
        raw_capture,
        Transport(rx),
        admitted,
        f.journal_objects.clone(),
        serde_json::json!(["engine", "test", "wal"]).to_string(),
    );
    // Count dropped steps independently of the writer's retained PUT handles.
    // Shutdown may cancel the final idle transport read; schema ticks may not.
    struct StepGuard(Arc<AtomicU64>, bool);
    impl Drop for StepGuard {
        fn drop(&mut self) {
            if !self.1 {
                self.0.fetch_add(1, AtomicOrdering::SeqCst);
            }
        }
    }
    struct CountedCapture(PgCapture<Resolver, Transport>, Arc<AtomicU64>);
    impl Capture for CountedCapture {
        async fn prepare_schema(&mut self, journal: &Journal, runtime: &Runtime) -> Result<()> {
            self.0.prepare_schema(journal, runtime).await
        }
        fn schema_state(&self) -> Option<source::SchemaState> {
            self.0.schema_state()
        }
        fn writer_checkpoint(&self) -> Result<Option<crate::buffer::writer::WriterCheckpoint>> {
            self.0.writer_checkpoint()
        }
        async fn step(&mut self) -> Result<Captured> {
            let mut guard = StepGuard(self.1.clone(), false);
            let result = self.0.step().await;
            guard.1 = true;
            result
        }
        async fn publish(&mut self, high: u64, rows: AssembledTxn) -> Result<DurableManifestRef> {
            self.0.publish(high, rows).await
        }
    }
    let cancelled_steps = Arc::new(AtomicU64::new(0));
    let mut capture = CountedCapture(capture, cancelled_steps.clone());
    let count = Arc::new(AtomicU64::new(0));
    let mut applier = f.applier(count.clone(), ReaderCheckpoint::default());
    let mut database = f.database(tx, Arc::new(AtomicU64::new(0)));
    let (pause, paused) = watch::channel(false);
    let mut budget = budget();
    let (result, ()) = tokio::join!(
        f.engine.run(
            &mut capture,
            &mut database,
            &mut applier,
            config(),
            &mut budget,
            paused
        ),
        wait_state(f.engine.runtime(), State::CatchingUp, pause, count.clone())
    );
    result.unwrap();
    let state = f.journal.load(false).await.unwrap();
    assert_eq!(state.lifecycle.state, State::Paused);
    assert_eq!(
        state
            .chunk_attempts
            .values()
            .filter(|a| a.done.is_some())
            .count(),
        1
    );
    assert!(state.progress.contains_key("buffered"));
    assert_eq!(count.load(AtomicOrdering::SeqCst), 3);
    assert_eq!(manifest_puts.load(AtomicOrdering::SeqCst), 3);
    assert_eq!(segment_puts.load(AtomicOrdering::SeqCst), 3);
    assert_eq!(capture.writer_checkpoint().unwrap().unwrap().next_mseq, 3);
    assert!(
        cancelled_steps.load(AtomicOrdering::SeqCst) <= 1,
        "only shutdown may cancel a capture step"
    );
}

async fn wait_state(
    runtime: Runtime,
    target: State,
    pause: watch::Sender<bool>,
    applied: Arc<AtomicU64>,
) {
    // This bounds a hang; state and durable apply progress synchronize the test.
    // Concurrent CPU/IO load can delay the final journal writes well past 10s.
    let completed = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            if runtime.state().await.lifecycle.state == target
                && applied.load(AtomicOrdering::SeqCst) >= 3
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    let state = runtime.state().await;
    assert!(
        completed.is_ok(),
        "completion timed out: state={:?}, blockers={:?}, applied={}",
        state.lifecycle.state,
        state.lifecycle.blockers,
        applied.load(AtomicOrdering::SeqCst)
    );
    pause.send_replace(true);
}

#[tokio::test]
async fn chunk_done_follows_durable_rows_and_slot_precedes_first_chunk() {
    // DB doubles make creation/order deterministic; storage is real local FS.
    let f = Fixture::new().await;
    let (tx, rx) = mpsc::channel(32);
    let mut capture = f.capture(rx, 10).await;
    let count = Arc::new(AtomicU64::new(0));
    let mut applier = f.applier(count.clone(), ReaderCheckpoint::default());
    let mut database = f.database(tx, Arc::new(AtomicU64::new(0)));
    let (pause, paused) = watch::channel(false);
    let mut budget = budget();
    let (result, ()) = tokio::join!(
        f.engine.run(
            &mut capture,
            &mut database,
            &mut applier,
            config(),
            &mut budget,
            paused
        ),
        wait_state(f.engine.runtime(), State::CatchingUp, pause, count)
    );
    result.unwrap();
    let state = f.journal.load(false).await.unwrap();
    assert_eq!(state.lifecycle.state, State::Paused);
    assert_eq!(
        state
            .chunk_attempts
            .values()
            .filter(|a| a.done.is_some())
            .count(),
        1
    );
    // Journal::load authenticates the referenced manifest and typed ChunkRows.
    let done = state
        .chunk_attempts
        .values()
        .next()
        .unwrap()
        .done
        .as_ref()
        .unwrap();
    let mut reader = BufferReader::new(
        f.store.clone(),
        f.config.clone(),
        ReaderCheckpoint::default(),
        Arc::new(Order),
    )
    .unwrap();
    let mut txns = vec![];
    while let Some(txn) = reader.next_txn().await.unwrap() {
        txns.push(txn);
    }
    assert_eq!(txns.len(), 3);
    assert_eq!(done.high_lsn.to_be_bytes(), txns[2].commit_position.bytes());
    assert!(matches!(
        txns[2].events[0].kind,
        crate::event::EventKind::ChunkRows
    ));
    let markers = state.progress.clone();
    f.engine.resume().await.unwrap();
    let resumed = f.journal.load(false).await.unwrap();
    assert_eq!(resumed.lifecycle.state, State::CatchingUp);
    assert_eq!(resumed.progress, markers);
    assert_eq!(resumed.chunk_attempts, state.chunk_attempts);
    // Exercise retention with the real ChunkDone journal dependency. A later
    // durable WAL record permits earlier records to retire, while replay must
    // still authenticate the completed ChunkRows reference.
    marker(
        &database.tx,
        Watermark {
            wm: WatermarkKind::Low,
            chunk: plan().ranges[0].id.clone(),
            attempt: 1,
        },
        40,
    )
    .await;
    for _ in 0..3 {
        capture.step().await.unwrap();
    }
    f.engine
        .enable_retention(
            f.store.clone(),
            f.config.clone(),
            Arc::new(Order),
            crate::buffer::gc::RetentionPolicy {
                retained_for_ms: 0,
                orphan_grace_ms: 0,
            },
            Duration::from_nanos(1),
        )
        .await
        .unwrap();
    let runtime = f.engine.runtime();
    runtime
        .queue_writer(capture.writer_checkpoint().unwrap())
        .await;
    // Reuse the existing ReaderLoop/destination path so the new receipt enters
    // through PgApplier's post-apply checkpoint callback.
    while applier.step(&runtime).await.unwrap() {}
    for _ in 0..4 {
        runtime.retention_tick().await.unwrap();
    }
    assert!(
        f.store
            .get(&f.config.manifest_key(0))
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        f.store
            .get(&f.config.manifest_key(done.chunk_rows.mseq))
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(
        f.journal.load(false).await.unwrap().chunk_attempts,
        state.chunk_attempts
    );
}

#[tokio::test]
async fn chunk_done_without_durable_rows_is_rejected() {
    // No DB operation is needed: this tests authoritative Journal validation
    // against the real, empty local-FS buffer, rather than a permissive fake.
    let f = Fixture::new().await;
    let chunk = plan().ranges.remove(0);
    f.engine
        .runtime()
        .commit(Transition::ChunkStarted {
            chunk: chunk.clone(),
            attempt: 1,
        })
        .await
        .unwrap();
    assert!(
        f.engine
            .runtime()
            .commit(Transition::ChunkDone {
                chunk,
                attempt: 1,
                high_lsn: 20,
                chunk_rows: ChunkRowsRef {
                    mseq: 0,
                    manifest_digest: [0; 32]
                }
            })
            .await
            .is_err()
    );
    let state = f.journal.load(false).await.unwrap();
    assert!(state.chunk_attempts.values().all(|a| a.done.is_none()));
}

#[tokio::test]
async fn pause_resume_keeps_markers_and_restarts_an_unfinished_chunk() {
    // Snapshot DB requests a pause mid-read; this is the deterministic crash seam.
    let mut f = Fixture::new().await;
    let (tx, rx) = mpsc::channel(32);
    let mut capture = f.capture(rx, 10).await;
    let count = Arc::new(AtomicU64::new(0));
    let mut applier = f.applier(count.clone(), ReaderCheckpoint::default());
    let reads = Arc::new(AtomicU64::new(0));
    let mut database = f.database(tx.clone(), reads.clone());
    let (pause, paused) = watch::channel(false);
    database.pause_on_read = Some(pause);
    let mut budget = budget();
    f.engine
        .run(
            &mut capture,
            &mut database,
            &mut applier,
            config(),
            &mut budget,
            paused,
        )
        .await
        .unwrap();
    let old = f.journal.load(false).await.unwrap();
    assert!(old.chunk_attempts.values().all(|a| a.done.is_none()));
    let markers = old.progress.clone();
    drop(capture);
    drop(applier);
    // Discard every in-memory component and rebuild from the actual journal and
    // buffer, as after process death, then redo the same durable plan.
    let mut coordinator = Coordinator::observer(
        f.journal.clone(),
        Local::create(f.root.join("recovered-local")).unwrap(),
        "restarted",
    )
    .unwrap();
    coordinator.acquire().await.unwrap();
    f.engine = Engine::new(coordinator, f.journal.clone());
    f.engine.resume().await.unwrap();
    assert_eq!(markers, f.engine.runtime().state().await.progress);
    let (tx2, rx2) = mpsc::channel(32);
    database.tx = tx2;
    database.lsn = 100;
    let mut capture = f
        .capture(
            rx2,
            old.progress
                .get("buffered")
                .map_or(10, |(_, p)| p.parse().unwrap()),
        )
        .await;
    let checkpoint = old
        .progress
        .get("applied")
        .map(|(m, _)| decode_reader_checkpoint(m).unwrap())
        .unwrap_or_default();
    let mut applier = f.applier(count.clone(), checkpoint);
    let (pause, paused) = watch::channel(false);
    let (result, ()) = tokio::join!(
        f.engine.run(
            &mut capture,
            &mut database,
            &mut applier,
            config(),
            &mut budget,
            paused
        ),
        wait_state(f.engine.runtime(), State::CatchingUp, pause, count)
    );
    result.unwrap();
    assert_eq!(reads.load(AtomicOrdering::SeqCst), 2);
    let state = f.journal.load(false).await.unwrap();
    let attempt = state.chunk_attempts.values().next().unwrap();
    assert_eq!(attempt.attempt, 2);
    assert!(attempt.done.is_some());
}

#[tokio::test]
async fn capture_shutdown_before_high_abandons_window_and_resume_retries() {
    let f = Fixture::new().await;
    let (tx, _rx) = mpsc::channel(32);
    let reads = Arc::new(AtomicU64::new(0));
    let mut database = f.database(tx, reads.clone());
    let (commands, mut receive) = mpsc::channel(2);
    let (_cancel, cancelled) = watch::channel(false);
    let mut budget = budget();
    let config = config();
    // Fault only the capture-worker handshake: stop after L and the snapshot
    // read, while copy awaits Rows acknowledgement, before it can emit H.
    // Withhold the cancellation notification to force channel closure to be
    // observed first, independently of runtime scheduling or CPU load.
    let shutdown = async {
        let Some(Command::Begin {
            ready, complete, ..
        }) = receive.recv().await
        else {
            panic!("expected Begin");
        };
        ready.send(()).unwrap();
        let Some(Command::Rows { ready, .. }) = receive.recv().await else {
            panic!("expected Rows");
        };
        assert_eq!(reads.load(AtomicOrdering::SeqCst), 1);
        drop(ready);
        drop(complete);
        drop(receive);
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(
            copy_loop(
                &mut database,
                commands,
                cancelled,
                &f.engine.runtime,
                &f.journal,
                &config,
                &mut budget,
            ),
            shutdown
        )
    })
    .await
    .unwrap();
    result.unwrap();
    assert_eq!(database.lsn, 20, "only L was emitted");
    let old = f.journal.load(false).await.unwrap();
    let attempt = old.chunk_attempts.values().next().unwrap();
    assert_eq!(attempt.attempt, 1);
    assert!(attempt.done.is_none());
    assert_eq!(old.lifecycle.state, State::Copying);
    f.engine
        .runtime
        .lifecycle(Event::Interrupt {
            to: State::Paused,
            reason: "capture stopped during an open window".into(),
        })
        .await
        .unwrap();
    f.engine.resume().await.unwrap();
    let (tx, rx) = mpsc::channel(32);
    database.tx = tx;
    database.lsn = 100;
    let mut capture = f.capture(rx, 10).await;
    let count = Arc::new(AtomicU64::new(0));
    let mut applier = f.applier(count.clone(), ReaderCheckpoint::default());
    let (pause, paused) = watch::channel(false);
    let (result, ()) = tokio::join!(
        f.engine.run(
            &mut capture,
            &mut database,
            &mut applier,
            config,
            &mut budget,
            paused
        ),
        wait_state(f.engine.runtime(), State::CatchingUp, pause, count)
    );
    result.unwrap();
    let state = f.journal.load(false).await.unwrap();
    assert_eq!(state.lifecycle.state, State::Paused);
    let attempt = state.chunk_attempts.values().next().unwrap();
    assert_eq!(attempt.attempt, 2);
    assert!(attempt.done.is_some());
    assert_eq!(reads.load(AtomicOrdering::SeqCst), 2);
}

#[tokio::test]
async fn copy_throttle_never_stalls_capture() {
    // Fake source DB supplies committed markers while copy's tokens are denied.
    // Keep the reservation denied until pause, regardless of CPU/IO scheduling.
    struct FrozenClock;
    impl budget::Clock for FrozenClock {
        fn now(&self) -> Duration {
            Duration::ZERO
        }
    }
    struct ProgressCapture<C> {
        inner: C,
        buffered: Option<oneshot::Sender<()>>,
        durable: bool,
    }
    impl<C: Capture> Capture for ProgressCapture<C> {
        async fn prepare_schema(&mut self, journal: &Journal, runtime: &Runtime) -> Result<()> {
            self.inner.prepare_schema(journal, runtime).await
        }
        fn schema_state(&self) -> Option<source::SchemaState> {
            self.inner.schema_state()
        }
        fn writer_checkpoint(&self) -> Result<Option<crate::buffer::writer::WriterCheckpoint>> {
            self.inner.writer_checkpoint()
        }
        async fn step(&mut self) -> Result<Captured> {
            // Entering the next step proves capture_loop finished recording
            // the preceding durable batch, including its buffered marker.
            if self.durable
                && let Some(signal) = self.buffered.take()
            {
                signal.send(()).expect("buffered observer alive");
            }
            let batch = self.inner.step().await?;
            self.durable |= batch.durable_lsn.is_some();
            Ok(batch)
        }
        async fn publish(&mut self, high: u64, rows: AssembledTxn) -> Result<DurableManifestRef> {
            self.inner.publish(high, rows).await
        }
    }
    let f = Fixture::new().await;
    let (tx, rx) = mpsc::channel(32);
    let (buffered, buffer_progress) = oneshot::channel();
    let mut capture = ProgressCapture {
        inner: f.capture(rx, 10).await,
        buffered: Some(buffered),
        durable: false,
    };
    let count = Arc::new(AtomicU64::new(0));
    // Signal after the real reader has applied and persisted its checkpoint.
    // No scheduling deadline or polling interval defines successful progress.
    struct ProgressApplier<A> {
        inner: A,
        progressed: Option<oneshot::Sender<()>>,
    }
    impl<A: Applier> Applier for ProgressApplier<A> {
        async fn step(&mut self, runtime: &Runtime) -> Result<bool> {
            let (_keep_alive, stop) = watch::channel(false);
            self.controlled_step(runtime, stop).await
        }
        async fn controlled_step(
            &mut self,
            runtime: &Runtime,
            stop: watch::Receiver<bool>,
        ) -> Result<bool> {
            let applied = self.inner.controlled_step(runtime, stop).await?;
            if applied && let Some(signal) = self.progressed.take() {
                signal.send(()).expect("progress observer alive");
            }
            Ok(applied)
        }
    }
    let (progressed, progress) = oneshot::channel();
    let mut applier = ProgressApplier {
        inner: f.applier(count.clone(), ReaderCheckpoint::default()),
        progressed: Some(progressed),
    };
    let reads = Arc::new(AtomicU64::new(0));
    let mut database = f.database(tx.clone(), reads.clone());
    let mut budget = budget_with_clock(Arc::new(FrozenClock));
    budget.try_chunk_read(1_000_000, 1_000_000).unwrap();
    let (pause, paused) = watch::channel(false);
    let mut config = config();
    config.read_rows = 1_000_000;
    config.read_bytes = 1_000_000;
    let observer = async {
        marker(
            &tx,
            Watermark {
                wm: WatermarkKind::Low,
                chunk: plan().ranges[0].id.clone(),
                attempt: 99,
            },
            20,
        )
        .await;
        let observed = tokio::time::timeout(Duration::from_secs(60), async {
            tokio::try_join!(progress, buffer_progress)
        })
        .await;
        let state = f.engine.runtime().state().await;
        assert!(
            matches!(observed, Ok(Ok(((), ())))),
            "capture/apply progress stalled: signal={observed:?}, state={:?}, blockers={:?}, progress={:?}, applied={}, copy_reads={}",
            state.lifecycle.state,
            state.lifecycle.blockers,
            state.progress,
            count.load(AtomicOrdering::SeqCst),
            reads.load(AtomicOrdering::SeqCst),
        );
        assert!(count.load(AtomicOrdering::SeqCst) > 0);
        assert!(state.progress.contains_key("buffered"));
        assert_eq!(reads.load(AtomicOrdering::SeqCst), 0);
        pause.send_replace(true);
    };
    let (result, ()) = tokio::join!(
        f.engine.run(
            &mut capture,
            &mut database,
            &mut applier,
            config,
            &mut budget,
            paused
        ),
        observer
    );
    result.unwrap();
    assert!(
        f.journal
            .load(false)
            .await
            .unwrap()
            .progress
            .contains_key("buffered")
    );
}

#[tokio::test]
async fn fatal_capture_records_recovery_required_and_source_authority() {
    // Only DB transport is faulted; recovery authority is journaled on real FS.
    let f = Fixture::new().await;
    let (tx, rx) = mpsc::channel(32);
    let mut capture = f.capture(rx, 10).await;
    tx.send(Err(source::Error::RecoveryRequired("injected lost slot")))
        .await
        .unwrap();
    let mut database = f.database(tx, Arc::new(AtomicU64::new(0)));
    let mut applier = f.applier(Arc::new(AtomicU64::new(0)), ReaderCheckpoint::default());
    let (_pause, paused) = watch::channel(false);
    let mut budget = budget();
    assert!(
        f.engine
            .run(
                &mut capture,
                &mut database,
                &mut applier,
                config(),
                &mut budget,
                paused
            )
            .await
            .is_err()
    );
    let state = f.journal.load(false).await.unwrap();
    assert_eq!(state.lifecycle.state, State::RecoveryRequired);
    assert_eq!(state.lifecycle.authority, Authority::Source);
    assert!(state.lifecycle.blockers[0].contains("capture failed; authority=Source"));
}

#[tokio::test]
async fn abrupt_crash_mid_chunk_replays_started_and_redoes_the_chunk() {
    // The database snapshot read stalls at an injected crash seam. Dropping the
    // entire run (without a pause transition) models losing all volatile state.
    let mut f = Fixture::new().await;
    let (tx, rx) = mpsc::channel(32);
    let mut capture = f.capture(rx, 10).await;
    let count = Arc::new(AtomicU64::new(0));
    let mut applier = f.applier(count.clone(), ReaderCheckpoint::default());
    let reads = Arc::new(AtomicU64::new(0));
    let mut database = f.database(tx, reads.clone());
    database.stall_read = true;
    let (_pause, paused) = watch::channel(false);
    let mut budget = budget();
    {
        let run = f.engine.run(
            &mut capture,
            &mut database,
            &mut applier,
            config(),
            &mut budget,
            paused,
        );
        tokio::pin!(run);
        tokio::select! {
            result = &mut run => panic!("crash seam unexpectedly returned: {result:?}"),
            _ = async { while reads.load(AtomicOrdering::SeqCst) == 0 { tokio::task::yield_now().await; } } => {},
        }
    }
    drop(capture);
    drop(applier);
    let old = f.journal.load(false).await.unwrap();
    assert_eq!(old.lifecycle.state, State::Copying);
    assert!(old.chunk_attempts.values().all(|a| a.done.is_none()));
    let mut coordinator = Coordinator::observer(
        f.journal.clone(),
        Local::create(f.root.join("crash-local")).unwrap(),
        "crash-resume",
    )
    .unwrap();
    coordinator.acquire().await.unwrap();
    f.engine = Engine::new(coordinator, f.journal.clone());
    let (tx, rx) = mpsc::channel(32);
    database.tx = tx;
    database.lsn = 100;
    database.stall_read = false;
    let mut capture = f
        .capture(
            rx,
            old.progress
                .get("buffered")
                .map_or(10, |(_, p)| p.parse().unwrap()),
        )
        .await;
    let checkpoint = old
        .progress
        .get("applied")
        .map(|(m, _)| decode_reader_checkpoint(m).unwrap())
        .unwrap_or_default();
    let mut applier = f.applier(count.clone(), checkpoint);
    let (pause, paused) = watch::channel(false);
    let (result, ()) = tokio::join!(
        f.engine.run(
            &mut capture,
            &mut database,
            &mut applier,
            config(),
            &mut budget,
            paused
        ),
        wait_state(f.engine.runtime(), State::CatchingUp, pause, count)
    );
    result.unwrap();
    assert_eq!(reads.load(AtomicOrdering::SeqCst), 2);
    let state = f.journal.load(false).await.unwrap();
    let attempt = state.chunk_attempts.values().next().unwrap();
    assert_eq!(attempt.attempt, 2);
    assert!(attempt.done.is_some());
}

#[cfg(test)]
#[path = "backpressure_tests.rs"]
mod backpressure_tests;
#[tokio::test]
async fn pause_cancels_an_inflight_snapshot_read_without_recovery_required() {
    let f = Fixture::new().await;
    let (tx, rx) = mpsc::channel(32);
    let mut capture = f.capture(rx, 10).await;
    let reads = Arc::new(AtomicU64::new(0));
    let mut database = f.database(tx, reads.clone());
    database.stall_read = true;
    let mut applier = f.applier(Arc::new(AtomicU64::new(0)), ReaderCheckpoint::default());
    let mut budget = budget();
    let (pause, paused) = watch::channel(false);
    let observer = async {
        while reads.load(AtomicOrdering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        pause.send_replace(true);
    };
    tokio::time::timeout(Duration::from_secs(5), async {
        let (result, ()) = tokio::join!(
            f.engine.run(
                &mut capture,
                &mut database,
                &mut applier,
                config(),
                &mut budget,
                paused
            ),
            observer
        );
        result.unwrap();
    })
    .await
    .unwrap();
    let state = f.journal.load(false).await.unwrap();
    assert_eq!(state.lifecycle.state, State::Paused);
    assert_eq!(state.lifecycle.authority, Authority::Source);
    assert!(state.chunk_attempts.values().all(|a| a.done.is_none()));
}

#[path = "readiness_tests.rs"]
mod readiness_tests;

#[path = "retention_tests.rs"]
mod retention_tests;

#[path = "async_planner_tests.rs"]
mod async_planner_tests;

#[path = "live_readiness_tests.rs"]
pub(crate) mod live_readiness_tests;
