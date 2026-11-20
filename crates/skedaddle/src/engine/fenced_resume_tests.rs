//! Control-only replay uses the real journal, writer, reader and marker hooks.
//! No database behavior is simulated: final-marker transactions have no rows.
use super::*;
use crate::{
    budget::{Limits, RateLimit, ReadLimits, SystemClock},
    buffer::{
        backends::ObjectBufferStore,
        format,
        reader::{BufferReader, ReaderCheckpoint, ReaderConfig},
        writer::{BufferWriter, WriterCheckpoint, WriterConfig},
    },
    coordinator::{Authority, journal, local::Local, readiness::ReadinessFacts},
    cutover::{Key, Marker},
    event::{
        Event as RowEvent, EventError, EventKind, OpaqueValue, Position, PositionOrder, TxnId,
    },
    pg::chunk::{Direction, KeyColumn, QualifiedName, TableKey},
};
use std::{
    cmp::Ordering,
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering as AtomicOrdering},
    time::Instant,
};

struct Order;
impl PositionOrder for Order {
    fn domain(&self) -> &str {
        "pg:1:1:slot"
    }
    fn compare(&self, a: &[u8], b: &[u8]) -> std::result::Result<Ordering, EventError> {
        Ok(a.cmp(b))
    }
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
fn marker() -> Marker {
    Marker {
        cutover: "cutover".into(),
        epoch: 1,
        lsn: 15,
    }
}
fn transaction() -> AssembledTxn {
    let position = Position::new(Order.domain(), 20u64.to_be_bytes());
    let event = RowEvent {
        source: source(), txn: TxnId(vec![1]), ordinal_in_txn: 0,
        position: position.clone(), schema_version: 0, kind: EventKind::Watermark,
        table: None, key: Row::new(), before: None,
        after: Some(Row::from([("control".into(), OpaqueValue {
            type_id: "skedaddle.watermark.v1".into(),
            bytes: Some(serde_json::to_vec(&serde_json::json!({"message_lsn":15,"content":marker().payload().as_bytes()})).unwrap()),
        })])), unchanged_cols: Default::default(),
    };
    AssembledTxn::new(source(), TxnId(vec![1]), position, Some(1), vec![event]).unwrap()
}
fn batch() -> Captured {
    Captured {
        transaction: Some(transaction()),
        durable_position: Some(Position::new(Order.domain(), 21u64.to_be_bytes())),
        ..Default::default()
    }
}
fn ready() -> ReadinessFacts {
    ReadinessFacts {
        all_chunks_done: true,
        capture_healthy: true,
        lineage_matches: true,
        indexes_constraints_valid: true,
        sequences_syncable: true,
        reverse_prerequisites: true,
        fence_risks_acknowledged: true,
        buffer_contract_matches: true,
        ..Default::default()
    }
}
fn config() -> CopyConfig {
    CopyConfig {
        source: source(),
        order_domain: Order.domain().into(),
        schema_version: 0,
        read_rows: 100,
        read_bytes: 10000,
        window_bytes: 1000000,
    }
}
fn budget() -> ThroughputBudget {
    let rate = RateLimit {
        per_second: 1000000,
        burst: 1000000,
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
        Arc::new(SystemClock::default()),
    )
    .unwrap()
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
                pg_type: crate::mapping::PgType::Int4,
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
struct NoCopy;
impl ChunkDatabase for NoCopy {
    async fn emit(&mut self, _: Watermark) -> Result<()> {
        panic!("copy marker while fenced/catching up")
    }
    async fn read(&mut self, _: &ChunkPlan, _: &ChunkRange) -> Result<Vec<Row>> {
        panic!("copy read while fenced/catching up")
    }
}
struct CaptureInput {
    checkpoint: WriterCheckpoint,
    input: mpsc::Receiver<Result<Captured>>,
    calls: Arc<AtomicUsize>,
}
impl Capture for CaptureInput {
    fn position_order(&self) -> Arc<dyn PositionOrder + Send + Sync> {
        Arc::new(Order)
    }
    fn writer_checkpoint(&self) -> Result<Option<WriterCheckpoint>> {
        Ok(Some(self.checkpoint.clone()))
    }
    async fn step(&mut self) -> Result<Captured> {
        self.calls.fetch_add(1, AtomicOrdering::SeqCst);
        self.input
            .recv()
            .await
            .unwrap_or_else(|| Err(recovery("injected capture disconnect")))
    }
    async fn publish(&mut self, _: Position, _: AssembledTxn) -> Result<DurableManifestRef> {
        panic!("copy publication while fenced")
    }
}
struct Replay {
    reader: BufferReader,
    enabled: watch::Receiver<bool>,
    calls: Arc<AtomicUsize>,
}
impl Applier for Replay {
    async fn step(&mut self, runtime: &Runtime) -> Result<bool> {
        self.calls.fetch_add(1, AtomicOrdering::SeqCst);
        if !*self.enabled.borrow() {
            return Ok(false);
        }
        let Some(txn) = self.reader.next_txn().await.map_err(recovery)? else {
            return Ok(false);
        };
        assert_eq!(txn, transaction());
        let checkpoint = self.reader.checkpoint().clone();
        Box::pin(runtime.applied_final_marker(&txn, checkpoint.next_mseq - 1, &checkpoint)).await?;
        runtime
            .marker(
                "applied",
                encode_reader_checkpoint(&checkpoint)?,
                checkpoint.next_mseq.to_string(),
            )
            .await?;
        Ok(true)
    }
}
struct Fixture {
    engine: Engine,
    journal: Journal,
    root: PathBuf,
    writer: BufferWriter,
    store: Arc<ObjectBufferStore>,
}
impl Fixture {
    async fn new(state: State) -> Self {
        let root = crate::test_root::scratch_root()
            .join(format!("fenced-resume-{}", uuid::Uuid::new_v4()));
        for name in ["journal", "buffer", "scratch"] {
            std::fs::create_dir_all(root.join(name)).unwrap();
        }
        let journal = Journal::new(
            Arc::new(journal::local_store(root.join("journal")).unwrap()),
            "engine",
            "migration",
            Duration::from_secs(5),
        )
        .unwrap();
        let mut owner = Coordinator::observer(
            journal.clone(),
            Local::create(root.join("owner")).unwrap(),
            "owner",
        )
        .unwrap();
        owner.acquire().await.unwrap();
        for to in [State::Assessed, State::Copying, State::CatchingUp] {
            if owner.state().lifecycle.state == state {
                break;
            }
            owner
                .commit(Transition::Lifecycle {
                    event: Event::Advance { to },
                })
                .await
                .unwrap();
        }
        if matches!(
            state,
            State::Paused | State::Blocked | State::Failed | State::RecoveryRequired
        ) {
            owner
                .commit(Transition::Lifecycle {
                    event: Event::Interrupt {
                        to: state,
                        reason: "fixture boundary".into(),
                    },
                })
                .await
                .unwrap();
        }
        if matches!(
            state,
            State::VerifiedReady | State::SourceFenced | State::FinalBoundaryApplied
        ) {
            owner
                .commit(Transition::Lifecycle {
                    event: Event::Readiness { facts: ready() },
                })
                .await
                .unwrap();
            if state != State::VerifiedReady {
                owner
                    .commit(Transition::Lifecycle {
                        event: Event::Advance {
                            to: State::SourceFenced,
                        },
                    })
                    .await
                    .unwrap();
                if state == State::FinalBoundaryApplied {
                    owner
                        .commit(Transition::Lifecycle {
                            event: Event::Advance { to: state },
                        })
                        .await
                        .unwrap();
                }
            }
        }
        let engine = Engine::new(owner, journal.clone());
        let store = Arc::new(
            ObjectBufferStore::local(root.join("buffer"), Duration::from_secs(5)).unwrap(),
        );
        let (feedback, _) = std::sync::mpsc::channel();
        let mut writer = BufferWriter::open_with_scratch(
            store.clone(),
            WriterConfig {
                prefix: "engine".into(),
                migration_id: "migration".into(),
                stream: "wal".into(),
                epoch: 1,
                segment_bytes: 1000000,
                segment_transactions: 100,
                segment_age: Duration::from_secs(1),
                max_transaction_bytes: 1000000,
            },
            Arc::new(Order),
            feedback,
            root.join("scratch"),
        )
        .await
        .unwrap();
        writer.append(transaction(), Instant::now()).await.unwrap();
        writer.seal().await.unwrap();
        Self {
            engine,
            journal,
            root,
            writer,
            store,
        }
    }
    fn workers(
        &self,
    ) -> (
        CaptureInput,
        mpsc::Sender<Result<Captured>>,
        Replay,
        watch::Sender<bool>,
    ) {
        let (tx, input) = mpsc::channel(4);
        let (enabled, gate) = watch::channel(false);
        let capture = CaptureInput {
            checkpoint: self.writer.checkpoint().unwrap(),
            input,
            calls: Arc::new(AtomicUsize::new(0)),
        };
        let replay = Replay {
            reader: BufferReader::new(
                self.store.clone(),
                ReaderConfig {
                    prefix: "engine".into(),
                    migration_id: "migration".into(),
                    stream: "wal".into(),
                    limits: format::Limits::default(),
                    required_next_mseq: 1,
                },
                ReaderCheckpoint::default(),
                Arc::new(Order),
            )
            .unwrap(),
            enabled: gate,
            calls: Arc::new(AtomicUsize::new(0)),
        };
        (capture, tx, replay, enabled)
    }
    fn progress(&self) -> FinalMarkerProgress {
        let snapshot = self.engine.runtime.snapshot();
        self.engine
            .runtime
            .final_marker_progress(
                &Key {
                    migration: "migration".into(),
                    cutover: "cutover".into(),
                    epoch: snapshot.epoch.try_into().unwrap(),
                    coordinator: snapshot.owner,
                },
                &source(),
            )
            .unwrap()
    }
    async fn assert_fenced(&self, state: State) {
        for snapshot in [
            self.engine.runtime.state().await,
            self.journal.load(false).await.unwrap(),
        ] {
            assert_eq!(snapshot.lifecycle.state, state);
            assert_eq!(snapshot.lifecycle.authority, Authority::Neither);
            assert!(snapshot.chunk_attempts.is_empty());
        }
    }
    async fn wait_started(calls: &AtomicUsize) {
        while calls.load(AtomicOrdering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    }
}
async fn restarted(state: State) {
    let mut f = Fixture::new(state).await;
    f.engine
        .runtime
        .6
        .markers
        .begin(
            "migration",
            source(),
            Order.domain().into(),
            &f.engine.runtime.snapshot(),
        )
        .unwrap();
    f.engine
        .runtime
        .capture_final_marker(&batch(), Some(&f.writer.checkpoint().unwrap()))
        .await
        .unwrap();
    let buffered = f.progress().receipt(&marker(), false).unwrap().unwrap();
    assert!(f.progress().receipt(&marker(), true).unwrap().is_none());
    let mut replacement = Coordinator::observer(
        f.journal.clone(),
        Local::create(f.root.join("replacement")).unwrap(),
        "replacement",
    )
    .unwrap();
    assert_eq!(replacement.acquire().await.unwrap(), 2);
    f.engine = Engine::new(replacement, f.journal.clone());
    let (mut capture, tx, mut replay, enabled) = f.workers();
    let calls = capture.calls.clone();
    let (pause, stopped) = watch::channel(false);
    let mut budget = budget();
    let mut chunks = NoCopy;
    let driver = async {
        Fixture::wait_started(&calls).await;
        let progress = f.progress();
        assert_eq!(progress.receipt(&marker(), false).unwrap(), Some(buffered));
        assert!(progress.receipt(&marker(), true).unwrap().is_none());
        tx.send(Ok(Captured::default())).await.unwrap();
        enabled.send_replace(true);
        let receipt = progress
            .wait(&marker(), true, Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(receipt.marker(), &marker());
        pause.send_replace(true);
        progress
    };
    let (result, progress) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(
            f.engine.run_fenced_until(
                &mut capture,
                &mut chunks,
                &mut replay,
                config(),
                &mut budget,
                stopped,
                StopAt::UntilPaused
            ),
            driver
        )
    })
    .await
    .unwrap();
    let reached = result.unwrap();
    assert_eq!(reached.state, state);
    assert!(!reached.target_reached);
    assert_eq!(
        progress.receipt(&marker(), true),
        Err(MarkerProgressError::RunExited)
    );
    f.assert_fenced(state).await;
    assert!(
        f.journal
            .load(false)
            .await
            .unwrap()
            .progress
            .contains_key("applied")
    );
}
#[tokio::test]
async fn source_fenced_replacement_rebinds_receipts_and_applies() {
    Box::pin(restarted(State::SourceFenced)).await;
}
#[tokio::test]
async fn final_boundary_replacement_rebinds_receipts_and_applies() {
    Box::pin(restarted(State::FinalBoundaryApplied)).await;
}

#[tokio::test]
async fn ordinary_run_rejects_both_fenced_states() {
    for state in [State::SourceFenced, State::FinalBoundaryApplied] {
        let f = Fixture::new(state).await;
        let (mut capture, _tx, mut replay, _enabled) = f.workers();
        let (_pause, stopped) = watch::channel(false);
        assert!(matches!(
            Box::pin(f.engine.run_until(
                &mut capture,
                &mut NoCopy,
                &mut replay,
                config(),
                &mut budget(),
                stopped,
                StopAt::UntilPaused
            ))
            .await,
            Err(coordinator::Error::Rejected(_))
        ));
        assert_eq!(capture.calls.load(AtomicOrdering::SeqCst), 0);
        f.assert_fenced(state).await;
    }
}
#[tokio::test]
async fn fenced_entry_rejects_non_fenced_states_and_forged_local_state() {
    for state in [
        State::Defined,
        State::Assessed,
        State::Copying,
        State::CatchingUp,
        State::VerifiedReady,
        State::Paused,
        State::Blocked,
        State::Failed,
        State::RecoveryRequired,
    ] {
        let f = Fixture::new(state).await;
        assert_eq!(f.journal.load(false).await.unwrap().lifecycle.state, state);
        // Admission must authenticate the journal rather than a forged cache.
        f.engine.runtime.1.write().unwrap().lifecycle.state = State::SourceFenced;
        let (mut capture, _tx, mut replay, _enabled) = f.workers();
        let (_pause, stopped) = watch::channel(false);
        assert!(matches!(
            Box::pin(f.engine.run_fenced_until(
                &mut capture,
                &mut NoCopy,
                &mut replay,
                config(),
                &mut budget(),
                stopped,
                StopAt::UntilPaused
            ))
            .await,
            Err(coordinator::Error::Rejected(_))
        ));
        assert_eq!(capture.calls.load(AtomicOrdering::SeqCst), 0);
        assert_eq!(f.engine.runtime.snapshot().lifecycle.state, state);
    }
}
#[tokio::test]
async fn fenced_entry_rejects_observer_and_promoted_old_owner() {
    for observer in [true, false] {
        let mut f = Fixture::new(State::SourceFenced).await;
        let mut other = Coordinator::observer(
            f.journal.clone(),
            Local::create(f.root.join("other")).unwrap(),
            "other",
        )
        .unwrap();
        if observer {
            other.refresh().await.unwrap();
            f.engine = Engine::new(other, f.journal.clone());
        } else {
            other.acquire().await.unwrap();
        }
        let (mut capture, _tx, mut replay, _enabled) = f.workers();
        let (_pause, stopped) = watch::channel(false);
        assert!(matches!(
            Box::pin(f.engine.run_fenced_until(
                &mut capture,
                &mut NoCopy,
                &mut replay,
                config(),
                &mut budget(),
                stopped,
                StopAt::UntilPaused
            ))
            .await,
            Err(coordinator::Error::Observer)
        ));
        assert_eq!(capture.calls.load(AtomicOrdering::SeqCst), 0);
        f.assert_fenced(State::SourceFenced).await;
    }
}
async fn mid_run(initial: State) {
    let f = Fixture::new(initial).await;
    let (mut capture, tx, mut replay, enabled) = f.workers();
    let calls = capture.calls.clone();
    let applies = replay.calls.clone();
    let (pause, stopped) = watch::channel(false);
    let mut budget = budget();
    let mut chunks = NoCopy;
    let driver = async {
        Fixture::wait_started(&calls).await;
        let mut owner = f.engine.runtime.0.lock().await;
        owner
            .commit(Transition::Lifecycle {
                event: Event::Readiness { facts: ready() },
            })
            .await
            .unwrap();
        owner
            .commit(Transition::Lifecycle {
                event: Event::Advance {
                    to: State::SourceFenced,
                },
            })
            .await
            .unwrap();
        *f.engine.runtime.1.write().unwrap() = owner.state().clone();
        drop(owner);
        tx.send(Ok(batch())).await.unwrap();
        let progress = f.progress();
        progress
            .wait(&marker(), false, Duration::from_secs(5))
            .await
            .unwrap();
        enabled.send_replace(true);
        progress
            .wait(&marker(), true, Duration::from_secs(5))
            .await
            .unwrap();
        assert!(applies.load(AtomicOrdering::SeqCst) > 0);
        pause.send_replace(true);
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(
            f.engine.run_until(
                &mut capture,
                &mut chunks,
                &mut replay,
                config(),
                &mut budget,
                stopped,
                StopAt::UntilPaused
            ),
            driver
        )
    })
    .await
    .unwrap();
    assert_eq!(result.unwrap().state, State::SourceFenced);
    f.assert_fenced(State::SourceFenced).await;
}
#[tokio::test]
async fn catching_up_run_keeps_capture_and_apply_after_fencing() {
    Box::pin(mid_run(State::CatchingUp)).await;
}
#[tokio::test]
async fn verified_ready_run_keeps_capture_and_apply_after_fencing() {
    Box::pin(mid_run(State::VerifiedReady)).await;
}

#[tokio::test]
async fn pause_and_sender_shutdown_preserve_both_fenced_lifecycles() {
    for state in [State::SourceFenced, State::FinalBoundaryApplied] {
        for shutdown in [false, true] {
            let f = Fixture::new(state).await;
            let (mut capture, _tx, mut replay, _enabled) = f.workers();
            let calls = capture.calls.clone();
            let (pause, stopped) = watch::channel(false);
            let mut budget = budget();
            let mut chunks = NoCopy;
            let driver = async {
                Fixture::wait_started(&calls).await;
                if shutdown {
                    drop(pause);
                } else {
                    pause.send_replace(true);
                }
            };
            let (result, ()) = tokio::time::timeout(Duration::from_secs(10), async {
                tokio::join!(
                    f.engine.run_fenced_until(
                        &mut capture,
                        &mut chunks,
                        &mut replay,
                        config(),
                        &mut budget,
                        stopped,
                        StopAt::UntilPaused
                    ),
                    driver
                )
            })
            .await
            .unwrap();
            assert_eq!(result.unwrap().state, state);
            f.assert_fenced(state).await;
            assert!(!f.engine.running.load(AtomicOrdering::SeqCst));
        }
    }
}
#[tokio::test]
async fn fenced_capture_error_is_reported_without_illegal_recovery_edge() {
    for state in [State::SourceFenced, State::FinalBoundaryApplied] {
        let f = Fixture::new(state).await;
        let (mut capture, tx, mut replay, _enabled) = f.workers();
        tx.send(Err(recovery("injected capture disconnect")))
            .await
            .unwrap();
        let (_pause, stopped) = watch::channel(false);
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            Box::pin(f.engine.run_fenced_until(
                &mut capture,
                &mut NoCopy,
                &mut replay,
                config(),
                &mut budget(),
                stopped,
                StopAt::UntilPaused,
            )),
        )
        .await
        .unwrap();
        assert!(
            matches!(result, Err(coordinator::Error::RecoveryRequired(ref reason)) if reason.contains("injected capture disconnect"))
        );
        f.assert_fenced(state).await;
        assert!(
            f.journal
                .load(false)
                .await
                .unwrap()
                .progress
                .contains_key("engine-stop-fault-v1")
        );
    }
}
#[tokio::test]
async fn fenced_copy_planning_is_rejected_without_publication() {
    for state in [State::SourceFenced, State::FinalBoundaryApplied] {
        let f = Fixture::new(state).await;
        assert!(matches!(
            f.engine.plan(&plan()).await,
            Err(coordinator::Error::Rejected(_))
        ));
        assert!(f.journal.load(false).await.unwrap().chunk_plans.is_empty());
        f.assert_fenced(state).await;
    }
}
#[tokio::test]
async fn fenced_wal_levels_record_evidence_without_lifecycle_edges() {
    for state in [State::SourceFenced, State::FinalBoundaryApplied] {
        let f = Fixture::new(state).await;
        for level in [WalLevel::Warning, WalLevel::Blocker, WalLevel::Abort] {
            f.engine
                .enforce_run_wal(&WalDecision {
                    level,
                    wal_bytes_per_second: Some(1.0),
                    seconds_to_exhaustion: Some(1.0),
                    slot_headroom_bytes: Some(1),
                    disk_free_bytes: 1,
                    coverage_loss: level == WalLevel::Abort,
                    resumable: level != WalLevel::Abort,
                    source_untouched: true,
                })
                .await
                .unwrap();
            f.assert_fenced(state).await;
        }
        assert!(!f.engine.resumable());
        assert!(f.engine.require_coverage().is_err());
    }
}

struct LostSlot;
impl ReadinessSource for LostSlot {
    fn sample(&self) -> ComponentSnapshot {
        ComponentSnapshot {
            wal: Some(WalObservation {
                current_lsn: 21,
                slot: crate::status::SlotStatus {
                    restart_lsn: None,
                    confirmed_flush_lsn: None,
                    valid: false,
                    invalidation_reason: Some("injected lost slot".into()),
                    retained_wal_bytes: 0,
                    max_slot_wal_keep_size_bytes: None,
                    disk_free_bytes: 1000000,
                },
            }),
            ..Default::default()
        }
    }
}
#[tokio::test]
async fn fenced_supervision_records_coverage_loss_and_joins() {
    for state in [State::SourceFenced, State::FinalBoundaryApplied] {
        let mut f = Fixture::new(state).await;
        f.engine = f.engine.with_readiness_source(Arc::new(LostSlot));
        let (mut capture, _tx, mut replay, _enabled) = f.workers();
        let (_pause, stopped) = watch::channel(false);
        let reached = tokio::time::timeout(
            Duration::from_secs(10),
            Box::pin(f.engine.run_fenced_until(
                &mut capture,
                &mut NoCopy,
                &mut replay,
                config(),
                &mut budget(),
                stopped,
                StopAt::UntilPaused,
            )),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(reached.state, state);
        assert!(!f.engine.resumable());
        assert!(
            f.journal
                .load(false)
                .await
                .unwrap()
                .progress
                .contains_key(readiness::COVERAGE_LOSS)
        );
        assert!(!f.engine.running.load(AtomicOrdering::SeqCst));
        f.assert_fenced(state).await;
    }
}
struct DrainingApply {
    replay: Replay,
    entered: Option<oneshot::Sender<()>>,
    release: Option<oneshot::Receiver<()>>,
    finished: Arc<AtomicUsize>,
}
impl Applier for DrainingApply {
    async fn step(&mut self, runtime: &Runtime) -> Result<bool> {
        if let Some(entered) = self.entered.take() {
            entered.send(()).unwrap();
            self.release.take().unwrap().await.unwrap();
            let applied = self.replay.step(runtime).await?;
            self.finished.store(1, AtomicOrdering::SeqCst);
            return Ok(applied);
        }
        Ok(false)
    }
}
#[tokio::test]
async fn fenced_pause_drains_inflight_apply_before_returning() {
    let f = Fixture::new(State::SourceFenced).await;
    let (mut capture, _tx, replay, enabled) = f.workers();
    enabled.send_replace(true);
    let (entered, entry) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let finished = Arc::new(AtomicUsize::new(0));
    let mut apply = DrainingApply {
        replay,
        entered: Some(entered),
        release: Some(released),
        finished: finished.clone(),
    };
    let (pause, stopped) = watch::channel(false);
    let mut budget = budget();
    let mut chunks = NoCopy;
    let driver = async {
        entry.await.unwrap();
        pause.send_replace(true);
        tokio::task::yield_now().await;
        assert_eq!(finished.load(AtomicOrdering::SeqCst), 0);
        assert!(f.engine.running.load(AtomicOrdering::SeqCst));
        release.send(()).unwrap();
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(
            f.engine.run_fenced_until(
                &mut capture,
                &mut chunks,
                &mut apply,
                config(),
                &mut budget,
                stopped,
                StopAt::UntilPaused
            ),
            driver
        )
    })
    .await
    .unwrap();
    assert_eq!(result.unwrap().state, State::SourceFenced);
    assert_eq!(finished.load(AtomicOrdering::SeqCst), 1);
    f.assert_fenced(State::SourceFenced).await;
}
struct JournalPause;
impl Applier for JournalPause {
    async fn step(&mut self, runtime: &Runtime) -> Result<bool> {
        // Inject only the otherwise unavailable journal-outage notification.
        *runtime.4.write().unwrap() = Some(EnginePauseReason::CoordinatorJournalUnavailable);
        Err(EnginePauseReason::CoordinatorJournalUnavailable.error())
    }
}
#[tokio::test]
async fn fenced_journal_pause_keeps_durable_and_local_fence() {
    for state in [State::SourceFenced, State::FinalBoundaryApplied] {
        let f = Fixture::new(state).await;
        let (mut capture, _tx, _replay, _enabled) = f.workers();
        let (_pause, stopped) = watch::channel(false);
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            Box::pin(f.engine.run_fenced_until(
                &mut capture,
                &mut NoCopy,
                &mut JournalPause,
                config(),
                &mut budget(),
                stopped,
                StopAt::UntilPaused,
            )),
        )
        .await
        .unwrap();
        assert!(matches!(result, Err(coordinator::Error::Io(_))));
        assert_eq!(f.engine.runtime.snapshot().epoch, 1);
        f.assert_fenced(state).await;
    }
}
