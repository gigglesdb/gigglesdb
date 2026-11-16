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
            positions: apply::SourcePositions::native_pg("engine-test"),
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
        stream: "slot".into(),
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
            Arc::new(Order),
            source(),
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

mod opaque_capture {
    use super::*;
    use std::time::Instant;

    struct FrontierOrder;
    impl PositionOrder for FrontierOrder {
        fn domain(&self) -> &str {
            "test-frontier"
        }
        fn compare(&self, a: &[u8], b: &[u8]) -> std::result::Result<Ordering, EventError> {
            // The zero branch models a valid frontier incomparable with the
            // other branch, while remaining comparable with itself.
            if a != b && (a == [0] || b == [0]) {
                return Err(EventError::InvalidEncoding("incomparable test frontiers"));
            }
            Ok(a.len().cmp(&b.len()).then_with(|| a.cmp(b)))
        }
    }

    fn identity() -> SourceIdentity {
        SourceIdentity {
            engine: "test-connector".into(),
            system_id: "source".into(),
            lineage: "generation".into(),
            db_name: "database".into(),
            stream: "frontier".into(),
        }
    }
    fn transaction(bytes: &[u8]) -> AssembledTxn {
        AssembledTxn::new(
            identity(),
            crate::event::TxnId(bytes.to_vec()),
            Position::new(FrontierOrder.domain(), bytes),
            None,
            vec![],
        )
        .unwrap()
    }
    struct FrontierCapture {
        writer: BufferWriter,
        steps: VecDeque<Captured>,
        incoming: Option<mpsc::Receiver<Captured>>,
        drained: Option<oneshot::Sender<()>>,
        buffered_markers: AtomicU64,
        // Inject shutdown precisely after admission, before durable publication.
        // All buffer I/O still goes through the real local object store.
        interrupt: Option<(Position, oneshot::Sender<()>)>,
    }
    impl Capture for FrontierCapture {
        fn position_order(&self) -> Arc<dyn PositionOrder + Send + Sync> {
            Arc::new(FrontierOrder)
        }
        fn writer_checkpoint(&self) -> Result<Option<crate::buffer::writer::WriterCheckpoint>> {
            self.writer.checkpoint().map(Some).map_err(recovery)
        }
        fn buffered_marker(&self, _: &Captured) -> Result<String> {
            self.buffered_markers.fetch_add(1, AtomicOrdering::SeqCst);
            let checkpoint = self.writer.checkpoint().map_err(recovery)?;
            Ok(format!("manifest-durable:{}", checkpoint.next_mseq))
        }
        async fn step(&mut self) -> Result<Captured> {
            let next = match self.steps.pop_front() {
                Some(batch) => Some(batch),
                None => match self.incoming.as_mut() {
                    Some(incoming) => incoming.recv().await,
                    None => None,
                },
            };
            let Some(batch) = next else {
                if let Some(drained) = self.drained.take() {
                    let _ = drained.send(());
                }
                return std::future::pending().await;
            };
            if let Some(txn) = &batch.transaction {
                self.publish(txn.commit_position.clone(), txn.clone())
                    .await?;
            }
            Ok(batch)
        }
        async fn publish(
            &mut self,
            high: Position,
            rows: AssembledTxn,
        ) -> Result<DurableManifestRef> {
            high.compare(&rows.commit_position, self.position_order().as_ref())
                .map_err(recovery)?;
            assert_eq!(high, rows.commit_position);
            if rows
                .events
                .iter()
                .any(|e| matches!(e.kind, crate::event::EventKind::ChunkRows))
            {
                return self
                    .writer
                    .append_chunk_rows(rows, Instant::now())
                    .await
                    .map(|(_, reference)| reference)
                    .map_err(recovery);
            }
            self.writer
                .append(rows, Instant::now())
                .await
                .map_err(recovery)?;
            if self
                .interrupt
                .as_ref()
                .is_some_and(|(position, _)| position == &high)
            {
                self.interrupt.take().unwrap().1.send(()).unwrap();
                std::future::pending::<()>().await;
            }
            self.writer.seal().await.map_err(recovery)?;
            let checkpoint = self.writer.checkpoint().map_err(recovery)?;
            Ok(DurableManifestRef {
                mseq: checkpoint.next_mseq - 1,
                manifest_digest: checkpoint.prev_digest,
            })
        }
    }
    struct Seam {
        root: PathBuf,
        store: Arc<ObjectBufferStore>,
        journal: Journal,
        engine: Engine,
    }
    impl Seam {
        async fn new() -> Self {
            let root = PathBuf::from(std::env::var_os("SKEDADDLE_TEST_ROOT").unwrap())
                .join(format!("opaque-capture-{}", uuid::Uuid::new_v4()));
            for directory in ["buffer", "journal", "scratch"] {
                std::fs::create_dir_all(root.join(directory)).unwrap();
            }
            let store = Arc::new(
                ObjectBufferStore::local(root.join("buffer"), Duration::from_secs(3)).unwrap(),
            );
            let journal = Journal::new(
                Arc::new(coordinator::journal::local_store(root.join("journal")).unwrap()),
                "seam",
                "test",
                Duration::from_secs(3),
            )
            .unwrap()
            .with_chunk_buffer(
                store.clone(),
                ReaderConfig {
                    prefix: "seam".into(),
                    migration_id: "test".into(),
                    stream: "frontier".into(),
                    limits: format::Limits::default(),
                    required_next_mseq: 0,
                },
                Arc::new(FrontierOrder),
            );
            let mut coordinator = Coordinator::observer(
                journal.clone(),
                Local::create(root.join("local")).unwrap(),
                "seam",
            )
            .unwrap();
            coordinator.acquire().await.unwrap();
            let engine = Engine::new(coordinator, journal.clone());
            Self {
                root,
                store,
                journal,
                engine,
            }
        }
        async fn capture(&self, transactions: VecDeque<AssembledTxn>) -> FrontierCapture {
            let (progress, _) = std::sync::mpsc::channel();
            let writer = BufferWriter::open_with_scratch(
                self.store.clone(),
                WriterConfig {
                    prefix: "seam".into(),
                    migration_id: "test".into(),
                    stream: "frontier".into(),
                    epoch: 1,
                    segment_bytes: 1_000_000,
                    segment_transactions: 100,
                    segment_age: Duration::from_secs(60),
                    max_transaction_bytes: 1_000_000,
                },
                Arc::new(FrontierOrder),
                progress,
                self.root.join("scratch"),
            )
            .await
            .unwrap();
            FrontierCapture {
                writer,
                steps: transactions
                    .into_iter()
                    .map(|txn| Captured {
                        durable_position: Some(txn.commit_position.clone()),
                        transaction: Some(txn),
                        ..Captured::default()
                    })
                    .collect(),
                incoming: None,
                drained: None,
                buffered_markers: AtomicU64::new(0),
                interrupt: None,
            }
        }
        async fn manifest(&self, mseq: u64) -> format::ManifestEntry {
            let key = format::manifest_key("seam", "test", "frontier", mseq);
            format::ManifestEntry::from_canonical_json(
                &self.store.get(&key).await.unwrap().unwrap(),
            )
            .unwrap()
        }
    }

    fn progress(bytes: &[u8]) -> Captured {
        Captured {
            durable_position: Some(Position::new(FrontierOrder.domain(), bytes)),
            ..Captured::default()
        }
    }

    async fn run_steps(capture: &mut FrontierCapture, runtime: &Runtime) -> Result<()> {
        let (drained, exhausted) = oneshot::channel();
        capture.drained = Some(drained);
        let (_commands, receive) = mpsc::channel(1);
        let (stop, stopped) = watch::channel(false);
        let config = CopyConfig {
            source: identity(),
            order_domain: FrontierOrder.domain().into(),
            schema_version: 1,
            read_rows: 1,
            read_bytes: 1024,
            window_bytes: 1024,
        };
        tokio::time::timeout(Duration::from_secs(10), async {
            let run = capture_loop(capture, receive, stopped, runtime, &config);
            tokio::pin!(run);
            tokio::select! {
                result = &mut run => result,
                drained = exhausted => {
                    drained.unwrap();
                    stop.send_replace(true);
                    run.await
                }
            }
        })
        .await
        .unwrap()
    }

    async fn seed_progress(seam: &Seam, bytes: &[u8]) {
        seam.engine
            .runtime()
            .marker(
                "buffered",
                "manifest-durable:0".into(),
                encode_buffered_position(
                    &identity(),
                    &Position::new(FrontierOrder.domain(), bytes),
                )
                .unwrap(),
            )
            .await
            .unwrap();
    }

    struct FrontierDatabase {
        tx: mpsc::Sender<Captured>,
        boundary: u8,
        stall: bool,
        reads: Arc<AtomicU64>,
    }
    impl ChunkDatabase for FrontierDatabase {
        async fn emit(&mut self, marker: Watermark) -> Result<()> {
            self.boundary += 1;
            let bytes = vec![1; self.boundary as usize];
            let txn = transaction(&bytes);
            self.tx
                .send(Captured {
                    watermarks: vec![CapturedMarker::Chunk {
                        marker,
                        commit: txn.commit_position.clone(),
                        committed_events: txn.events.clone(),
                    }],
                    durable_position: Some(txn.commit_position.clone()),
                    transaction: Some(txn),
                    stop: false,
                })
                .await
                .map_err(recovery)
        }
        async fn read(&mut self, _: &ChunkPlan, _: &ChunkRange) -> Result<Vec<Row>> {
            self.reads.fetch_add(1, AtomicOrdering::SeqCst);
            if self.stall {
                return std::future::pending().await;
            }
            Ok(vec![
                ([(
                    "id".into(),
                    OpaqueValue {
                        type_id: "int4".into(),
                        bytes: Some(Value::Int4(7).canonical_bytes().unwrap()),
                    },
                )])
                .into(),
            ])
        }
    }
    async fn setup_copy(seam: &Seam) {
        seam.engine
            .runtime
            .lifecycle(Event::Advance {
                to: State::Assessed,
            })
            .await
            .unwrap();
        seam.engine
            .start(&mut Setup(Arc::new(AtomicU64::new(0))))
            .await
            .unwrap();
        seam.engine.plan(&plan()).await.unwrap();
    }
    async fn native_copy(
        seam: &Seam,
        capture: &mut FrontierCapture,
        database: &mut impl ChunkDatabase,
    ) {
        let runtime = seam.engine.runtime();
        let config = CopyConfig {
            source: identity(),
            order_domain: FrontierOrder.domain().into(),
            ..super::config()
        };
        let (commands, receive) = mpsc::channel(1);
        let (stop, stopped) = watch::channel(false);
        let mut budget = budget();
        tokio::time::timeout(Duration::from_secs(10), async {
            tokio::try_join!(
                capture_loop(capture, receive, stopped.clone(), &runtime, &config),
                copy_loop(
                    database,
                    commands,
                    stopped,
                    &runtime,
                    &seam.journal,
                    &config,
                    &mut budget
                ),
                async {
                    while runtime.snapshot().lifecycle.state != State::CatchingUp {
                        tokio::task::yield_now().await;
                    }
                    stop.send_replace(true);
                    Ok::<(), coordinator::Error>(())
                }
            )
        })
        .await
        .unwrap()
        .unwrap();
    }
    #[tokio::test]
    async fn native_chunk_completes_and_authenticates_done_after_restart() {
        let seam = Seam::new().await;
        setup_copy(&seam).await;
        let (tx, rx) = mpsc::channel(8);
        let mut capture = seam.capture(VecDeque::new()).await;
        capture.incoming = Some(rx);
        let mut db = FrontierDatabase {
            tx,
            boundary: 0,
            stall: false,
            reads: Arc::new(AtomicU64::new(0)),
        };
        native_copy(&seam, &mut capture, &mut db).await;
        let state = seam.journal.load(false).await.unwrap();
        let done = state
            .chunk_attempts
            .values()
            .next()
            .unwrap()
            .done
            .as_ref()
            .unwrap();
        assert_eq!(done.high, Position::new(FrontierOrder.domain(), [1, 1]));
        assert!(state.unfinished_chunks(&plan()).unwrap().is_empty());
        seam.journal.checkpoint(&state).await.unwrap();
        assert_eq!(
            seam.journal.load(true).await.unwrap().chunk_attempts,
            state.chunk_attempts
        );
        let mut restarted = Coordinator::observer(
            seam.journal.clone(),
            Local::create(seam.root.join("restart")).unwrap(),
            "restart",
        )
        .unwrap();
        restarted.acquire().await.unwrap();
        assert_eq!(restarted.state().chunk_attempts, state.chunk_attempts);
        // A changed native H cannot borrow the original manifest's identity.
        let attempt = state.chunk_attempts.values().next().unwrap();
        let altered = Transition::ChunkDone {
            chunk: attempt.chunk.clone(),
            attempt: attempt.attempt,
            high: Position::new(FrontierOrder.domain(), [1, 1, 1]),
            chunk_rows: done.chunk_rows.clone(),
        };
        assert!(
            seam.journal
                .validate_chunk_objects(&state, &altered)
                .await
                .is_err()
        );
        assert!(
            state
                .progress
                .keys()
                .all(|key| !key.starts_with("engine-final-marker"))
        );
        let root = seam.root.clone();
        drop(restarted);
        drop(capture);
        drop(seam);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn native_unfinished_copy_restarts_with_new_attempt() {
        let mut seam = Seam::new().await;
        setup_copy(&seam).await;
        let (tx, rx) = mpsc::channel(8);
        let mut capture = seam.capture(VecDeque::new()).await;
        capture.incoming = Some(rx);
        let reads = Arc::new(AtomicU64::new(0));
        let mut db = FrontierDatabase {
            tx,
            boundary: 0,
            stall: true,
            reads: reads.clone(),
        };
        {
            let run = native_copy(&seam, &mut capture, &mut db);
            tokio::pin!(run);
            tokio::select! { _ = &mut run => panic!("unfinished read returned"), _ = async { while reads.load(AtomicOrdering::SeqCst) == 0 { tokio::task::yield_now().await; } } => {} }
        }
        drop(capture);
        let old = seam.journal.load(false).await.unwrap();
        assert_eq!(old.chunk_attempts.values().next().unwrap().attempt, 1);
        assert!(old.chunk_attempts.values().all(|a| a.done.is_none()));
        let mut restarted = Coordinator::observer(
            seam.journal.clone(),
            Local::create(seam.root.join("retry")).unwrap(),
            "retry",
        )
        .unwrap();
        restarted.acquire().await.unwrap();
        seam.engine = Engine::new(restarted, seam.journal.clone());
        let (tx, rx) = mpsc::channel(8);
        db.tx = tx;
        db.stall = false;
        let mut capture = seam.capture(VecDeque::new()).await;
        capture.incoming = Some(rx);
        native_copy(&seam, &mut capture, &mut db).await;
        let state = seam.journal.load(false).await.unwrap();
        let attempt = state.chunk_attempts.values().next().unwrap();
        assert_eq!(attempt.attempt, 2);
        assert_eq!(
            attempt.done.as_ref().unwrap().high,
            Position::new(FrontierOrder.domain(), [1, 1, 1])
        );
        let root = seam.root.clone();
        drop(capture);
        drop(seam);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn mysql_plan_window_excludes_touched_mapped_keys() {
        let seam = Seam::new().await;
        seam.engine
            .runtime
            .lifecycle(Event::Advance {
                to: State::Assessed,
            })
            .await
            .unwrap();
        seam.engine
            .start(&mut Setup(Arc::new(AtomicU64::new(0))))
            .await
            .unwrap();
        let plan = ChunkPlan::new(crate::mysql::chunk_key::tests::table(), 10, vec![]).unwrap();
        seam.engine.plan(&plan).await.unwrap();
        let (tx, rx) = mpsc::channel(8);
        let mut capture = seam.capture(VecDeque::new()).await;
        capture.incoming = Some(rx);
        struct TouchedDatabase(FrontierDatabase);
        impl ChunkDatabase for TouchedDatabase {
            async fn emit(&mut self, marker: Watermark) -> Result<()> {
                if marker.wm == WatermarkKind::High {
                    let mut txn = transaction(&[1, 0]);
                    txn.events.push(crate::event::Event {
                        source: identity(),
                        txn: txn.txn.clone(),
                        ordinal_in_txn: 0,
                        position: txn.commit_position.clone(),
                        schema_version: 1,
                        kind: crate::event::EventKind::Delete,
                        table: Some("rows".into()),
                        key: [(
                            "id".into(),
                            OpaqueValue {
                                type_id: "int4".into(),
                                bytes: Some(Value::Int4(7).canonical_bytes().unwrap()),
                            },
                        )]
                        .into(),
                        before: None,
                        after: None,
                        unchanged_cols: Default::default(),
                    });
                    let txn = AssembledTxn::new(
                        txn.source,
                        txn.txn,
                        txn.commit_position,
                        None,
                        txn.events,
                    )
                    .unwrap();
                    self.0
                        .tx
                        .send(Captured {
                            durable_position: Some(txn.commit_position.clone()),
                            transaction: Some(txn),
                            ..Captured::default()
                        })
                        .await
                        .map_err(recovery)?;
                }
                self.0.emit(marker).await
            }
            async fn read(&mut self, plan: &ChunkPlan, range: &ChunkRange) -> Result<Vec<Row>> {
                let mut rows = self.0.read(plan, range).await?;
                rows.push(
                    [(
                        "id".into(),
                        OpaqueValue {
                            type_id: "int4".into(),
                            bytes: Some(Value::Int4(8).canonical_bytes().unwrap()),
                        },
                    )]
                    .into(),
                );
                Ok(rows)
            }
        }
        let mut db = TouchedDatabase(FrontierDatabase {
            tx,
            boundary: 0,
            stall: false,
            reads: Arc::new(AtomicU64::new(0)),
        });
        native_copy(&seam, &mut capture, &mut db).await;
        assert!(
            seam.engine
                .runtime()
                .snapshot()
                .unfinished_chunks(&plan)
                .unwrap()
                .is_empty()
        );
        let mut reader = BufferReader::new(
            seam.store.clone(),
            ReaderConfig {
                prefix: "seam".into(),
                migration_id: "test".into(),
                stream: "frontier".into(),
                limits: format::Limits::default(),
                required_next_mseq: 0,
            },
            ReaderCheckpoint::default(),
            Arc::new(FrontierOrder),
        )
        .unwrap();
        let mut records = Vec::new();
        while let Some(txn) = reader.next_txn().await.unwrap() {
            records.push(txn);
        }
        assert_eq!(records.len(), 4); // L, delete, H, authenticated ChunkRows.
        assert_eq!(records.iter().map(|t| t.events.len()).sum::<usize>(), 2);
        assert_eq!(records[1].events[0].kind, crate::event::EventKind::Delete);
        let snapshot = &records.last().unwrap().events;
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].kind, crate::event::EventKind::ChunkRows);
        assert_eq!(
            Value::from_canonical_bytes(snapshot[0].key["id"].bytes.as_deref().unwrap()).unwrap(),
            Value::Int4(8)
        );

        let root = seam.root.clone();
        drop(reader);
        drop(capture);
        drop(seam);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn neutral_chunk_marker_never_creates_pg_final_marker_evidence() {
        let seam = Seam::new().await;
        let batch = Captured {
            watermarks: vec![CapturedMarker::Chunk {
                marker: Watermark {
                    wm: WatermarkKind::High,
                    chunk: plan().ranges[0].id.clone(),
                    attempt: 1,
                },
                commit: Position::new(FrontierOrder.domain(), [1, 2, 3]),
                committed_events: vec![],
            }],
            ..Captured::default()
        };
        seam.engine
            .runtime
            .capture_final_marker(&batch, None)
            .await
            .unwrap();
        assert!(seam.journal.load(false).await.unwrap().progress.is_empty());
        let root = seam.root.clone();
        drop(seam);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn neutral_key_identity_handles_non_pg_values_and_empty_result() {
        struct RawKey;
        impl crate::pg::chunk::KeyIdentity for RawKey {
            fn key(&self, row: &Row) -> crate::pg::chunk::Result<(Vec<u8>, Row)> {
                Ok((row["native"].bytes.clone().unwrap(), row.clone()))
            }
        }
        let p = plan();
        let row: Row = [(
            "native".into(),
            OpaqueValue {
                type_id: "native-key".into(),
                bytes: Some(vec![9, 8, 7]),
            },
        )]
        .into();
        let mut w = Window::with_keys(
            "rows".into(),
            Box::new(RawKey),
            p.ranges[0].clone(),
            vec![row.clone()],
            1,
        )
        .unwrap();
        let wm = |wm| Watermark {
            wm,
            chunk: p.ranges[0].id.clone(),
            attempt: 1,
        };
        w.marker(
            &wm(WatermarkKind::Low),
            Position::new(FrontierOrder.domain(), [1]),
            &FrontierOrder,
        )
        .unwrap();
        let event = crate::event::Event {
            source: identity(),
            txn: crate::event::TxnId(vec![1]),
            ordinal_in_txn: 0,
            position: Position::new(FrontierOrder.domain(), [1, 0]),
            schema_version: 1,
            kind: crate::event::EventKind::Delete,
            table: Some("rows".into()),
            key: row,
            before: None,
            after: None,
            unchanged_cols: Default::default(),
        };
        w.event(&event).unwrap();
        let high = Position::new(FrontierOrder.domain(), [1, 1]);
        w.marker(&wm(WatermarkKind::High), high.clone(), &FrontierOrder)
            .unwrap();
        let SegmentRecord::ChunkRows(txn) =
            w.finish(identity(), FrontierOrder.domain(), 1).unwrap()
        else {
            panic!()
        };
        assert!(txn.events.is_empty());
        assert_eq!(txn.commit_position, high);
    }
    #[test]
    fn native_window_rejects_nonincreasing_wrong_domain_and_incomparable_boundaries() {
        use crate::pg::chunk::ChunkError;
        let p = plan();
        for high in [vec![1], vec![], vec![0]] {
            let mut window = Window::new(
                p.table.postgres().unwrap().clone(),
                p.ranges[0].clone(),
                vec![],
                1,
            )
            .unwrap();
            let marker = |wm| Watermark {
                wm,
                chunk: p.ranges[0].id.clone(),
                attempt: 1,
            };
            window
                .marker(
                    &marker(WatermarkKind::Low),
                    Position::new(FrontierOrder.domain(), [1]),
                    &FrontierOrder,
                )
                .unwrap();
            let error = window
                .marker(
                    &marker(WatermarkKind::High),
                    Position::new(FrontierOrder.domain(), high),
                    &FrontierOrder,
                )
                .unwrap_err();
            assert!(matches!(
                error,
                ChunkError::Invalid("watermark order/replay mismatch")
                    | ChunkError::Event(EventError::InvalidEncoding("incomparable test frontiers"))
            ));
            assert!(
                window
                    .finish(identity(), FrontierOrder.domain(), 1)
                    .is_err()
            );
        }
        let mut window = Window::new(
            p.table.postgres().unwrap().clone(),
            p.ranges[0].clone(),
            vec![],
            1,
        )
        .unwrap();
        let marker = Watermark {
            wm: WatermarkKind::Low,
            chunk: p.ranges[0].id.clone(),
            attempt: 1,
        };
        assert!(matches!(
            window.marker(&marker, Position::new("foreign", [1]), &FrontierOrder),
            Err(ChunkError::Event(EventError::PositionDomainMismatch))
        ));
    }

    #[tokio::test]
    async fn progress_only_advances_buffered_position_and_survives_restart() {
        let mut seam = Seam::new().await;
        let mut capture = seam.capture(VecDeque::new()).await;
        capture.steps = VecDeque::from([progress(&[255]), progress(&[1, 0, 2])]);
        run_steps(&mut capture, &seam.engine.runtime())
            .await
            .unwrap();
        seam.engine.runtime().flush_progress().await.unwrap();
        let saved = seam.journal.load(false).await.unwrap();
        let position = Position::new(FrontierOrder.domain(), [1, 0, 2]);
        assert_eq!(
            decode_buffered_position(
                &saved.progress["buffered"].1,
                &identity(),
                FrontierOrder.domain(),
            )
            .unwrap(),
            position
        );
        assert_eq!(saved.progress["buffered"].0, "manifest-durable:0");
        assert_eq!(capture.writer.checkpoint().unwrap().next_mseq, 0);
        drop(capture);
        let mut coordinator = Coordinator::observer(
            seam.journal.clone(),
            Local::create(seam.root.join("restart-local")).unwrap(),
            "restart",
        )
        .unwrap();
        coordinator.acquire().await.unwrap();
        seam.engine = Engine::new(coordinator, seam.journal.clone());
        assert_eq!(seam.engine.runtime().snapshot().progress, saved.progress);
        // The buffer has no transaction position; restart must use the journal.
        let mut resumed = seam.capture(VecDeque::new()).await;
        assert_eq!(resumed.writer.checkpoint().unwrap().last_position, None);
        resumed.steps.push_back(progress(position.bytes()));
        run_steps(&mut resumed, &seam.engine.runtime())
            .await
            .unwrap();
        assert_eq!(resumed.buffered_markers.load(AtomicOrdering::SeqCst), 0);
    }

    #[tokio::test]
    async fn regressing_progress_only_requires_recovery() {
        let seam = Seam::new().await;
        seed_progress(&seam, &[1, 0, 2]).await;
        let saved = seam.journal.load(false).await.unwrap();
        let mut capture = seam.capture(VecDeque::new()).await;
        capture.steps.push_back(progress(&[255]));
        assert!(matches!(
            run_steps(&mut capture, &seam.engine.runtime()).await,
            Err(coordinator::Error::RecoveryRequired(reason)) if reason.contains("regressed")
        ));
        seam.engine.runtime().flush_progress().await.unwrap();
        assert_eq!(seam.journal.load(false).await.unwrap(), saved);
        assert_eq!(capture.buffered_markers.load(AtomicOrdering::SeqCst), 0);
    }

    #[tokio::test]
    async fn incomparable_progress_only_requires_recovery() {
        let seam = Seam::new().await;
        seed_progress(&seam, &[255]).await;
        let saved = seam.journal.load(false).await.unwrap();
        let mut capture = seam.capture(VecDeque::new()).await;
        capture.steps.push_back(progress(&[0]));
        assert!(matches!(
            run_steps(&mut capture, &seam.engine.runtime()).await,
            Err(coordinator::Error::RecoveryRequired(reason)) if reason.contains("incomparable")
        ));
        seam.engine.runtime().flush_progress().await.unwrap();
        assert_eq!(seam.journal.load(false).await.unwrap(), saved);
        assert_eq!(capture.buffered_markers.load(AtomicOrdering::SeqCst), 0);
    }

    #[tokio::test]
    async fn equal_progress_only_writes_no_marker() {
        let seam = Seam::new().await;
        seed_progress(&seam, &[255]).await;
        let saved = seam.journal.load(false).await.unwrap();
        let mut capture = seam.capture(VecDeque::new()).await;
        capture.steps = VecDeque::from([progress(&[255]), progress(&[255])]);
        run_steps(&mut capture, &seam.engine.runtime())
            .await
            .unwrap();
        seam.engine.runtime().flush_progress().await.unwrap();
        assert_eq!(seam.journal.load(false).await.unwrap(), saved);
        assert_eq!(capture.buffered_markers.load(AtomicOrdering::SeqCst), 0);
    }

    #[tokio::test]
    async fn progress_only_cannot_regress_pending_transaction_progress() {
        let seam = Seam::new().await;
        let txn = transaction(&[1, 0, 2]);
        let mut capture = seam.capture(VecDeque::from([txn.clone()])).await;
        capture.steps.push_back(progress(&[255]));
        let runtime = seam.engine.runtime();
        // Keep the transaction marker queued to exercise coalescing explicitly.
        runtime.2.persistence.lock().unwrap().last_flush = Instant::now() + Duration::from_secs(60);
        assert!(matches!(
            run_steps(&mut capture, &runtime).await,
            Err(coordinator::Error::RecoveryRequired(reason)) if reason.contains("regressed")
        ));
        assert!(
            !seam
                .journal
                .load(false)
                .await
                .unwrap()
                .progress
                .contains_key("buffered")
        );
        runtime.flush_progress().await.unwrap();
        let saved = seam.journal.load(false).await.unwrap();
        assert_eq!(
            decode_buffered_position(
                &saved.progress["buffered"].1,
                &identity(),
                FrontierOrder.domain(),
            )
            .unwrap(),
            txn.commit_position
        );
        assert_eq!(saved.progress["buffered"].0, "manifest-durable:1");
        assert_eq!(seam.manifest(0).await.last_pos, txn.commit_position);
        assert_eq!(capture.buffered_markers.load(AtomicOrdering::SeqCst), 1);
    }

    #[tokio::test]
    async fn variable_length_positions_publish_and_round_trip_through_durable_progress() {
        let seam = Seam::new().await;
        let first = transaction(&[255]);
        let last = transaction(&[1, 0, 2]);
        assert_eq!(
            first
                .commit_position
                .compare(&last.commit_position, &FrontierOrder)
                .unwrap(),
            Ordering::Less
        );
        let mut capture = seam.capture(VecDeque::new()).await;
        // Retain two distinct opaque positions in the same segment to exercise
        // both manifest endpoints, with an order different from byte lexicography.
        capture
            .writer
            .append(first.clone(), Instant::now())
            .await
            .unwrap();
        let reference = capture
            .publish(last.commit_position.clone(), last.clone())
            .await
            .unwrap();
        let manifest = seam.manifest(reference.mseq).await;
        assert_eq!(manifest.first_pos, first.commit_position);
        assert_eq!(manifest.last_pos, last.commit_position);
        assert_eq!(manifest.txn_count, 2);
        assert_eq!(manifest.digest().unwrap(), reference.manifest_digest);
        seam.engine
            .runtime()
            .progress_marker(
                "buffered",
                "manifest-durable".into(),
                encode_buffered_position(&identity(), &last.commit_position).unwrap(),
            )
            .await
            .unwrap();
        seam.engine.runtime().flush_progress().await.unwrap();
        let saved = seam.journal.load(false).await.unwrap();
        assert_eq!(
            decode_buffered_position(
                &saved.progress["buffered"].1,
                &identity(),
                FrontierOrder.domain()
            )
            .unwrap(),
            last.commit_position
        );
        drop(capture);
        let resumed = seam.capture(VecDeque::new()).await;
        assert_eq!(
            resumed.writer.checkpoint().unwrap().last_position,
            Some(last.commit_position)
        );
    }

    #[tokio::test]
    async fn shutdown_drops_mid_publish_step_and_restart_uses_only_durable_boundary() {
        let seam = Seam::new().await;
        let durable = transaction(&[255]);
        let interrupted = transaction(&[1, 0, 2]);
        let mut capture = seam
            .capture(VecDeque::from([durable.clone(), interrupted.clone()]))
            .await;
        let (entered, publishing) = oneshot::channel();
        capture.interrupt = Some((interrupted.commit_position.clone(), entered));
        let (_commands, receive) = mpsc::channel(1);
        let (stop, stopped) = watch::channel(false);
        let config = CopyConfig {
            source: identity(),
            order_domain: FrontierOrder.domain().into(),
            schema_version: 1,
            read_rows: 1,
            read_bytes: 1024,
            window_bytes: 1024,
        };
        let runtime = seam.engine.runtime();
        let (result, ()) = tokio::time::timeout(Duration::from_secs(10), async {
            tokio::join!(
                capture_loop(&mut capture, receive, stopped, &runtime, &config),
                async {
                    publishing.await.unwrap();
                    stop.send_replace(true);
                },
            )
        })
        .await
        .unwrap();
        result.unwrap();
        assert!(
            capture.interrupt.is_none(),
            "shutdown reached the in-flight publish"
        );
        assert_eq!(capture.writer.pending_transactions(), 1);
        runtime.flush_progress().await.unwrap();
        let saved = seam.journal.load(false).await.unwrap();
        assert_eq!(
            decode_buffered_position(
                &saved.progress["buffered"].1,
                &identity(),
                FrontierOrder.domain()
            )
            .unwrap(),
            durable.commit_position
        );
        drop(capture);
        // Discard admitted-but-unpublished state; startup discovers only the
        // manifest-durable transaction, then accepts redelivery of the suffix.
        let mut resumed = seam.capture(VecDeque::new()).await;
        let checkpoint = resumed.writer.checkpoint().unwrap();
        assert_eq!(checkpoint.next_mseq, 1);
        assert_eq!(checkpoint.last_position, Some(durable.commit_position));
        let reference = resumed
            .publish(interrupted.commit_position.clone(), interrupted.clone())
            .await
            .unwrap();
        assert_eq!(reference.mseq, 1);
        let manifest = seam.manifest(reference.mseq).await;
        assert_eq!(manifest.first_pos, interrupted.commit_position);
        assert_eq!(manifest.last_pos, interrupted.commit_position);
    }
}

#[tokio::test]
async fn publish_rejects_capture_order_different_from_writer() {
    struct ForeignOrder;
    impl PositionOrder for ForeignOrder {
        fn domain(&self) -> &str {
            "foreign"
        }
        fn compare(&self, a: &[u8], b: &[u8]) -> std::result::Result<Ordering, EventError> {
            Ok(a.cmp(b))
        }
    }
    let f = Fixture::new().await;
    let (tx, rx) = mpsc::channel(8);
    let mut capture = f.capture(rx, 10).await;
    capture
        .prepare_schema(&f.journal, &f.engine.runtime())
        .await
        .unwrap();
    marker(
        &tx,
        Watermark {
            wm: WatermarkKind::High,
            chunk: plan().ranges[0].id.clone(),
            attempt: 1,
        },
        20,
    )
    .await;
    let batch = loop {
        let batch = capture.step().await.unwrap();
        if batch.durable_position.is_some() {
            break batch;
        }
    };
    assert_eq!(
        batch.durable_position,
        Some(Position::new("engine-test", 21u64.to_be_bytes()))
    );
    let checkpoint = capture.writer_checkpoint().unwrap();
    let mut capture = PgCapture::new(
        capture.capture,
        capture.transport,
        capture.buffer_paused,
        f.journal_objects.clone(),
        serde_json::json!(["engine", "test", "wal"]).to_string(),
        Arc::new(ForeignOrder),
        source(),
    );
    let rows = batch.transaction.unwrap();
    let high = rows.commit_position.clone();
    let error = capture.publish(high, rows).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("PostgreSQL position order domain mismatch"),
        "{error}"
    );
    assert_eq!(capture.writer_checkpoint().unwrap(), checkpoint);
}

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
        Arc::new(Order),
        source(),
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
        fn position_order(&self) -> Arc<dyn PositionOrder + Send + Sync> {
            self.0.position_order()
        }
        async fn prepare_schema(&mut self, journal: &Journal, runtime: &Runtime) -> Result<()> {
            self.0.prepare_schema(journal, runtime).await
        }
        fn schema_state(&self) -> Option<source::SchemaState> {
            self.0.schema_state()
        }
        fn buffered_marker(&self, batch: &Captured) -> Result<String> {
            self.0.buffered_marker(batch)
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
        async fn publish(
            &mut self,
            high: Position,
            rows: AssembledTxn,
        ) -> Result<DurableManifestRef> {
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
    assert_eq!(done.high.bytes(), txns[2].commit_position.bytes());
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
                high: Position::new("engine-test", 20u64.to_be_bytes()),
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
            old.progress.get("buffered").map_or(10, |(_, p)| {
                pg_position_lsn(
                    &decode_buffered_position(p, &source(), Order.domain()).unwrap(),
                    Order.domain(),
                )
                .unwrap()
            }),
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
        fn position_order(&self) -> Arc<dyn PositionOrder + Send + Sync> {
            self.inner.position_order()
        }
        async fn prepare_schema(&mut self, journal: &Journal, runtime: &Runtime) -> Result<()> {
            self.inner.prepare_schema(journal, runtime).await
        }
        fn schema_state(&self) -> Option<source::SchemaState> {
            self.inner.schema_state()
        }
        fn buffered_marker(&self, batch: &Captured) -> Result<String> {
            self.inner.buffered_marker(batch)
        }
        fn writer_checkpoint(&self) -> Result<Option<crate::buffer::writer::WriterCheckpoint>> {
            self.inner.writer_checkpoint()
        }
        async fn step(&mut self) -> Result<Captured> {
            // Entering the next step proves capture_loop finished recording
            // the preceding durable batch and queued its buffered marker.
            if self.durable
                && let Some(signal) = self.buffered.take()
            {
                signal.send(()).expect("buffered observer alive");
            }
            let batch = self.inner.step().await?;
            self.durable |= batch.durable_position.is_some();
            Ok(batch)
        }
        async fn publish(
            &mut self,
            high: Position,
            rows: AssembledTxn,
        ) -> Result<DurableManifestRef> {
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
        f.engine.runtime().flush_progress().await.unwrap();
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
            old.progress.get("buffered").map_or(10, |(_, p)| {
                pg_position_lsn(
                    &decode_buffered_position(p, &source(), Order.domain()).unwrap(),
                    Order.domain(),
                )
                .unwrap()
            }),
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

#[tokio::test]
async fn engine_futures_stay_within_stack_budget() {
    // Check the real PG installation type without opening a live connection.
    fn check_install<F: Future>(_: impl FnOnce(&'static Engine, &'static mut PgInstaller) -> F) {
        assert!(std::mem::size_of::<F>() <= 1024);
    }
    check_install(|engine, installer| engine.start(installer));
    let f = Fixture::new().await;
    let (tx, rx) = mpsc::channel(32);
    let mut capture = f.capture(rx, 10).await;
    let mut applier = f.applier(Arc::new(AtomicU64::new(0)), ReaderCheckpoint::default());
    let mut database = f.database(tx, Arc::new(AtomicU64::new(0)));
    let (_pause, paused) = watch::channel(false);
    let mut budget = budget();
    {
        let future = f.engine.run(
            &mut capture,
            &mut database,
            &mut applier,
            config(),
            &mut budget,
            paused.clone(),
        );
        assert!(std::mem::size_of_val(&future) <= 1024);
    }
    let future = f.engine.run_admitted_until(
        &mut capture,
        &mut database,
        &mut applier,
        config(),
        &mut budget,
        paused,
        StopAt::UntilPaused,
        false,
    );
    assert!(std::mem::size_of_val(&future) <= 1024);
    drop(future);
    drop(capture);
    drop(applier);
    let root = f.root.clone();
    drop(database);
    drop(f);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn progress_commit_count_and_durable_boundaries() {
    let f = Fixture::new().await;
    let runtime = f.engine.runtime();
    let before = f.journal.load(false).await.unwrap().applied_seq;
    for n in 1..=256 {
        runtime.2.persistence.lock().unwrap().last_flush = std::time::Instant::now();
        runtime
            .progress_marker("buffered", "manifest-durable".into(), n.to_string())
            .await
            .unwrap();
    }
    let state = f.journal.load(false).await.unwrap();
    assert_eq!(state.applied_seq - before, 4);
    assert_eq!(state.progress["buffered"].1, "256");
    runtime
        .progress_marker("buffered", "manifest-durable".into(), "257".into())
        .await
        .unwrap();
    assert_eq!(
        f.journal.load(false).await.unwrap().progress["buffered"].1,
        "256"
    );
    // Any ordinary transition (including lifecycle/fence/checkpoint-producing
    // transitions) first publishes pending restart progress.
    runtime
        .marker("boundary", "checkpoint".into(), "1".into())
        .await
        .unwrap();
    assert_eq!(
        f.journal.load(false).await.unwrap().progress["buffered"].1,
        "257"
    );
    runtime
        .progress_marker("buffered", "manifest-durable".into(), "258".into())
        .await
        .unwrap();
    runtime
        .lifecycle(Event::Interrupt {
            to: State::Paused,
            reason: "stop".into(),
        })
        .await
        .unwrap();
    assert_eq!(
        f.journal.load(false).await.unwrap().progress["buffered"].1,
        "258"
    );
}

#[tokio::test]
async fn coalesced_progress_still_authenticates_ownership_on_every_update() {
    let f = Fixture::new().await;
    let runtime = f.engine.runtime();
    runtime
        .progress_marker("buffered", "manifest-durable".into(), "1".into())
        .await
        .unwrap();
    let mut replacement = Coordinator::observer(
        f.journal.clone(),
        Local::create(f.root.join("batch-replacement")).unwrap(),
        "replacement",
    )
    .unwrap();
    replacement.acquire().await.unwrap();
    let before = f.journal.load(false).await.unwrap().applied_seq;
    assert!(
        runtime
            .progress_marker("buffered", "manifest-durable".into(), "2".into())
            .await
            .is_err()
    );
    assert_eq!(f.journal.load(false).await.unwrap().applied_seq, before);
}

#[tokio::test]
async fn source_feedback_waits_for_durable_coalesced_buffered_progress() {
    struct Probe(Arc<std::sync::Mutex<Vec<u64>>>);
    impl source::Transport for Probe {
        async fn next(&mut self, durable: u64) -> source::Result<Vec<u8>> {
            self.0.lock().unwrap().push(durable);
            Ok(vec![])
        }
    }
    let f = Fixture::new().await;
    let runtime = f.engine.runtime();
    let baseline = crate::pg::replication::Checkpoint {
        lineage: crate::pg::replication::Lineage {
            system_id: 1,
            timeline: 1,
            database: "db".into(),
            xlogpos: 10,
        },
        slot: "slot".into(),
        durable_lsn: 10,
    };
    assert_eq!(
        super::pg::durable_feedback_lsn(&runtime, 30, 0, &source(), Order.domain()).unwrap(),
        0
    );
    runtime
        .marker(
            "source-slot",
            encode_source_checkpoint(&baseline).unwrap(),
            "installed".into(),
        )
        .await
        .unwrap();
    let identity = source();
    let seen = Arc::new(std::sync::Mutex::new(vec![]));
    let mut probe = Probe(seen.clone());
    let mut transport = super::pg::DurableFeedbackTransport::new(
        &mut probe,
        Some(&runtime),
        0,
        &identity,
        Order.domain(),
    );
    source::Transport::next(&mut transport, 30).await.unwrap();
    runtime.2.persistence.lock().unwrap().last_flush =
        std::time::Instant::now() + Duration::from_secs(60);
    runtime
        .progress_marker(
            "buffered",
            "manifest-durable".into(),
            encode_buffered_position(
                &identity,
                &Position::new(Order.domain(), 20u64.to_be_bytes()),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    source::Transport::next(&mut transport, 30).await.unwrap();
    assert_eq!(
        *seen.lock().unwrap(),
        vec![10, 10],
        "pending marker cannot release source WAL"
    );
    runtime.flush_progress().await.unwrap();
    source::Transport::next(&mut transport, 30).await.unwrap();
    source::Transport::next(&mut transport, 15).await.unwrap();
    assert_eq!(
        *seen.lock().unwrap(),
        vec![10, 10, 20, 15],
        "never acknowledge beyond this capture's durable buffer"
    );
    // A recovered prefix is independently durable even when the last batched
    // marker lags it; never send feedback behind this connection's start.
    let mut resumed_probe = Probe(seen.clone());
    let mut resumed = super::pg::DurableFeedbackTransport::new(
        &mut resumed_probe,
        Some(&runtime),
        25,
        &identity,
        Order.domain(),
    );
    source::Transport::next(&mut resumed, 30).await.unwrap();
    assert_eq!(*seen.lock().unwrap(), vec![10, 10, 20, 15, 25]);
    for payload in [
        "invalid".to_string(),
        "20".into(),
        "{}".into(),
        r#"{"version":2}"#.into(),
        encode_buffered_position(&identity, &Position::new("foreign", [0; 8])).unwrap(),
    ] {
        runtime
            .marker("buffered", "manifest-durable".into(), payload)
            .await
            .unwrap();
        assert!(matches!(
            source::Transport::next(&mut transport, 30).await,
            Err(source::Error::SchemaJournal(
                coordinator::Error::RecoveryRequired(_)
            ))
        ));
    }
    assert_eq!(
        seen.lock().unwrap().len(),
        5,
        "invalid evidence sends no feedback"
    );
}

#[test]
fn buffered_progress_round_trip_preserves_opaque_bytes_and_rejects_rebinding() {
    let position = Position::new(Order.domain(), [1, 2, 3]);
    let payload = encode_buffered_position(&source(), &position).unwrap();
    assert_eq!(
        decode_buffered_position(&payload, &source(), Order.domain()).unwrap(),
        position
    );
    assert_eq!(
        decode_buffered_position(&payload, &source(), "foreign"),
        Err(BufferedProgressError::OrderDomainMismatch)
    );
    let mut foreign = source();
    foreign.stream.push('x');
    assert_eq!(
        decode_buffered_position(&payload, &foreign, Order.domain()),
        Err(BufferedProgressError::SourceMismatch)
    );
}

#[test]
fn obsolete_buffered_progress_is_typed_and_requires_recovery() {
    for (payload, expected) in [
        ("123", BufferedProgressError::MissingVersion),
        ("{}", BufferedProgressError::MissingVersion),
        (
            r#"{"version":2}"#,
            BufferedProgressError::UnsupportedVersion(2),
        ),
    ] {
        let error = decode_buffered_position(payload, &source(), Order.domain()).unwrap_err();
        assert_eq!(error, expected);
        assert!(matches!(
            coordinator::Error::from(error),
            coordinator::Error::RecoveryRequired(_)
        ));
    }
}

#[tokio::test]
async fn obsolete_buffered_progress_blocks_worker_admission() {
    let f = Fixture::new().await;
    let (tx, rx) = mpsc::channel(32);
    let mut capture = f.capture(rx, 10).await;
    let mut applier = f.applier(Arc::new(AtomicU64::new(0)), ReaderCheckpoint::default());
    let mut database = f.database(tx, Arc::new(AtomicU64::new(0)));
    let (_pause, paused) = watch::channel(false);
    let mut budget = budget();
    for payload in ["123", "{}", r#"{"version":2}"#] {
        f.engine
            .runtime()
            .marker("buffered", "manifest-durable".into(), payload.into())
            .await
            .unwrap();
        let result = f
            .engine
            .run(
                &mut capture,
                &mut database,
                &mut applier,
                config(),
                &mut budget,
                paused.clone(),
            )
            .await;
        assert!(matches!(
            result,
            Err(coordinator::Error::RecoveryRequired(_))
        ));
        assert_eq!(capture.capture.writer().checkpoint().unwrap().next_mseq, 0);
    }
}
