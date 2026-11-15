//! Only planner timing/errors are injected; use the existing real buffer/journal
//! fixtures. tests/engine.rs also drives an awaiting planner with real PostgreSQL.
use super::*;
use std::sync::atomic::AtomicBool;

async fn populated() -> (
    Fixture,
    PgCapture<Resolver, Transport>,
    mpsc::Sender<source::Result<Vec<u8>>>,
) {
    let f = Fixture::new().await;
    let (tx, rx) = mpsc::channel(32);
    let mut capture = f.capture(rx, 10).await;
    capture
        .prepare_schema(&f.journal, &f.engine.runtime())
        .await
        .unwrap();
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
    for _ in 0..3 {
        capture.step().await.unwrap();
    }
    (f, capture, tx)
}
fn applier<P>(f: &Fixture, count: Arc<AtomicU64>, plan: P) -> PgApplier<DestinationDb, P> {
    PgApplier::new(
        BufferReader::new(
            f.store.clone(),
            f.config.clone(),
            ReaderCheckpoint::default(),
            Arc::new(Order),
        )
        .unwrap(),
        "wal".into(),
        DestinationDb(count),
        plan,
    )
}
struct DropFlag(Arc<AtomicBool>);
impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.store(true, AtomicOrdering::SeqCst);
    }
}
struct PendingPlanner {
    entered: Arc<tokio::sync::Notify>,
    dropped: Arc<AtomicBool>,
}
impl PgTransactionPlanner for PendingPlanner {
    fn plan<'a>(
        &'a mut self,
        _: &'a AssembledTxn,
        _: u64,
        _: &'a Materialized,
    ) -> PgPlanFuture<'a> {
        Box::pin(async move {
            let _guard = DropFlag(self.dropped.clone());
            self.entered.notify_one();
            std::future::pending().await
        })
    }
}

#[tokio::test]
async fn pause_during_planning_joins_drops_future_and_restart_retries_record() {
    let (f, mut capture, tx) = populated().await;
    let count = Arc::new(AtomicU64::new(0));
    let entered = Arc::new(tokio::sync::Notify::new());
    let dropped = Arc::new(AtomicBool::new(false));
    let mut applier = applier(
        &f,
        count.clone(),
        PendingPlanner {
            entered: entered.clone(),
            dropped: dropped.clone(),
        },
    );
    let mut database = f.database(tx, Arc::new(AtomicU64::new(0)));
    database.stall_read = true;
    let (pause, paused) = watch::channel(false);
    let mut budget = budget();
    let request = async {
        entered.notified().await;
        pause.send_replace(true);
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(
            f.engine.run(
                &mut capture,
                &mut database,
                &mut applier,
                config(),
                &mut budget,
                paused
            ),
            request
        )
    })
    .await
    .unwrap();
    result.unwrap();
    assert!(dropped.load(AtomicOrdering::SeqCst));
    assert_eq!(count.load(AtomicOrdering::SeqCst), 0);
    let state = f.journal.load(false).await.unwrap();
    assert_eq!(state.lifecycle.state, State::Paused);
    assert!(!state.progress.contains_key("applied"));
    assert!(matches!(
        applier.step(&f.engine.runtime()).await,
        Err(coordinator::Error::RecoveryRequired(_))
    ));
    f.engine.resume().await.unwrap();
    let mut restarted = f.applier(count.clone(), ReaderCheckpoint::default());
    assert!(restarted.step(&f.engine.runtime()).await.unwrap());
    assert_eq!(count.load(AtomicOrdering::SeqCst), 1);
    f.engine.runtime().flush_progress().await.unwrap();
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
async fn cancelled_planning_handoff_is_poisoned_without_checkpoint() {
    let (f, _capture, _tx) = populated().await;
    let count = Arc::new(AtomicU64::new(0));
    let entered = Arc::new(tokio::sync::Notify::new());
    let dropped = Arc::new(AtomicBool::new(false));
    let mut applier = applier(
        &f,
        count.clone(),
        PendingPlanner {
            entered: entered.clone(),
            dropped: dropped.clone(),
        },
    );
    let (stop, stopped) = watch::channel(false);
    let runtime = f.engine.runtime();
    let (result, ()) = tokio::join!(applier.controlled_step(&runtime, stopped), async {
        entered.notified().await;
        stop.send_replace(true);
    });
    assert!(!result.unwrap());
    assert!(dropped.load(AtomicOrdering::SeqCst));
    assert_eq!(count.load(AtomicOrdering::SeqCst), 0);
    assert!(
        !f.journal
            .load(false)
            .await
            .unwrap()
            .progress
            .contains_key("applied")
    );
    assert!(applier.step(&runtime).await.is_err());
}

struct YieldingPlanner {
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
    seen_seq: Option<u64>,
    stop_on_ready: Option<watch::Sender<bool>>,
}
impl PgTransactionPlanner for YieldingPlanner {
    fn plan<'a>(
        &'a mut self,
        txn: &'a AssembledTxn,
        mseq: u64,
        state: &'a Materialized,
    ) -> PgPlanFuture<'a> {
        Box::pin(async move {
            self.seen_seq = Some(state.applied_seq);
            self.entered.notify_one();
            self.release.notified().await;
            tokio::task::yield_now().await;
            assert_eq!(self.seen_seq, Some(state.applied_seq));
            let plan = planner(txn, mseq, state)?;
            if let Some(stop) = &self.stop_on_ready {
                stop.send_replace(true);
            }
            Ok(plan)
        })
    }
}
#[tokio::test]
async fn awaiting_planner_borrows_self_transaction_and_one_snapshot() {
    let (f, _capture, _tx) = populated().await;
    let count = Arc::new(AtomicU64::new(0));
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let mut applier = applier(
        &f,
        count.clone(),
        YieldingPlanner {
            entered: entered.clone(),
            release: release.clone(),
            seen_seq: None,
            stop_on_ready: None,
        },
    );
    let runtime = f.engine.runtime();
    let before = runtime.snapshot().applied_seq;
    let update = async {
        entered.notified().await;
        runtime
            .marker("concurrent", "during-plan".into(), "1".into())
            .await
            .unwrap();
        release.notify_one();
    };
    let (result, ()) = tokio::join!(applier.step(&runtime), update);
    assert!(result.unwrap());
    assert_eq!(applier.plan.seen_seq, Some(before));
    assert_eq!(count.load(AtomicOrdering::SeqCst), 1);
    assert!(runtime.snapshot().applied_seq > before);
}

#[tokio::test]
async fn ready_plan_cannot_outrun_simultaneous_stop() {
    let (f, _capture, _tx) = populated().await;
    let count = Arc::new(AtomicU64::new(0));
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    release.notify_one();
    let (stop, stopped) = watch::channel(false);
    let mut applier = applier(
        &f,
        count.clone(),
        YieldingPlanner {
            entered,
            release,
            seen_seq: None,
            stop_on_ready: Some(stop),
        },
    );
    assert!(
        !applier
            .controlled_step(&f.engine.runtime(), stopped)
            .await
            .unwrap()
    );
    assert_eq!(count.load(AtomicOrdering::SeqCst), 0);
    assert!(
        !f.journal
            .load(false)
            .await
            .unwrap()
            .progress
            .contains_key("applied")
    );
}

enum Fault {
    Apply,
    Io,
    Identity,
    MissingProgress,
}
impl PgTransactionPlanner for Fault {
    fn plan<'a>(
        &'a mut self,
        txn: &'a AssembledTxn,
        mseq: u64,
        state: &'a Materialized,
    ) -> PgPlanFuture<'a> {
        Box::pin(async move {
            tokio::task::yield_now().await;
            match self {
                Self::Apply => Err(apply::ApplyError::Invalid("injected planner fault").into()),
                Self::Io => Err(std::io::Error::other("secret source query details").into()),
                Self::Identity => {
                    let mut p = planner(txn, mseq, state)?;
                    p.progress.lsn += 1;
                    Ok(p)
                }
                Self::MissingProgress => {
                    let mut p = planner(txn, mseq, state)?;
                    p.statements.clear();
                    Ok(p)
                }
            }
        })
    }
}

struct ClassifyingPlanner {
    completed: bool,
    calls: u64,
    row_sql: String,
}
impl PgTransactionPlanner for ClassifyingPlanner {
    fn plan<'a>(
        &'a mut self,
        txn: &'a AssembledTxn,
        mseq: u64,
        state: &'a Materialized,
    ) -> PgPlanFuture<'a> {
        Box::pin(async move {
            tokio::task::yield_now().await;
            let completed = self.completed;
            assert!(!state.chunk_plans.is_empty());
            let plan = apply::plan_transaction(
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
                    synthetic_commit_time: None,
                },
                |_, key| {
                    self.calls += 1;
                    assert_eq!(
                        key["id"].bytes,
                        Some(Value::Int4(7).canonical_bytes().unwrap())
                    );
                    Ok(if completed {
                        apply::RangeState::Completed
                    } else {
                        apply::RangeState::Copying
                    })
                },
            )?;
            tokio::task::yield_now().await;
            self.row_sql = plan.statements[2].sql.clone();
            Ok(plan)
        })
    }
}

#[tokio::test]
async fn awaited_row_classification_selects_copying_upsert_or_completed_insert() {
    let f = Fixture::new().await;
    let (tx, _) = std::sync::mpsc::channel();
    let mut writer = BufferWriter::open_with_scratch(
        f.store.clone(),
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
        f.root.join("scratch"),
    )
    .await
    .unwrap();
    let txn_id = crate::event::TxnId(vec![1]);
    let position = crate::event::Position::new("engine-test", 20u64.to_be_bytes());
    let row: Row = [(
        "id".into(),
        OpaqueValue {
            type_id: "int4".into(),
            bytes: Some(Value::Int4(7).canonical_bytes().unwrap()),
        },
    )]
    .into();
    let txn = AssembledTxn::new(
        source(),
        txn_id.clone(),
        position.clone(),
        Some(0),
        vec![crate::event::Event {
            source: source(),
            txn: txn_id,
            ordinal_in_txn: 0,
            position,
            schema_version: 1,
            kind: crate::event::EventKind::Insert,
            table: Some("rows".into()),
            key: row.clone(),
            before: None,
            after: Some(row),
            unchanged_cols: Default::default(),
        }],
    )
    .unwrap();
    writer.append(txn, std::time::Instant::now()).await.unwrap();
    writer.seal().await.unwrap();
    for completed in [false, true] {
        let count = Arc::new(AtomicU64::new(0));
        let mut applier = applier(
            &f,
            count.clone(),
            ClassifyingPlanner {
                completed,
                calls: 0,
                row_sql: String::new(),
            },
        );
        assert!(applier.step(&f.engine.runtime()).await.unwrap());
        assert_eq!(applier.plan.calls, 1);
        assert_eq!(applier.plan.row_sql.contains("ON CONFLICT"), !completed);
        assert_eq!(count.load(AtomicOrdering::SeqCst), 1);
    }
}
#[tokio::test]
async fn planner_errors_and_invalid_plans_fail_closed_and_require_recovery() {
    assert!(matches!(
        PgPlannerError::from(std::io::Error::other("secret")),
        PgPlannerError::Infrastructure
    ));
    for (fault, reason) in [
        (Fault::Apply, "injected planner fault"),
        (Fault::Io, "Database"),
        (Fault::Identity, "reader plan identity"),
        (Fault::MissingProgress, "missing progress write"),
    ] {
        let (f, _capture, _tx) = populated().await;
        let count = Arc::new(AtomicU64::new(0));
        let mut applier = applier(&f, count.clone(), fault);
        let error = applier.step(&f.engine.runtime()).await.unwrap_err();
        assert!(matches!(error, coordinator::Error::RecoveryRequired(_)));
        assert!(error.to_string().contains(reason), "{error}");
        assert!(!error.to_string().contains("secret"));
        assert_eq!(count.load(AtomicOrdering::SeqCst), 0);
        assert!(
            !f.journal
                .load(false)
                .await
                .unwrap()
                .progress
                .contains_key("applied")
        );
        assert!(applier.step(&f.engine.runtime()).await.is_err());
    }
}

#[tokio::test]
async fn durable_schema_blocker_refuses_apply_readiness_and_cutover_admission() {
    use crate::pg::ddl::*;
    let (f, _capture, _tx) = populated().await;
    let runtime = f.engine.runtime();
    runtime
        .marker(BASELINE_STREAM, "a".repeat(32), "slot-double-sample".into())
        .await
        .unwrap();
    let mut state = runtime.state().await.operational_schema.clone();
    state
        .poll(
            PollPoint::Watermark(42),
            SchemaObservation {
                fingerprint: "b".repeat(32),
                hazards: vec![],
            },
        )
        .unwrap_err();
    runtime
        .marker(
            BLOCKER_STREAM,
            serde_json::to_string(state.blocker.as_ref().unwrap()).unwrap(),
            "sticky".into(),
        )
        .await
        .unwrap();
    f.journal.checkpoint(&runtime.state().await).await.unwrap();
    let replay = f.journal.load(true).await.unwrap();
    assert_eq!(replay.operational_schema, state);
    assert_eq!(
        f.journal.load(false).await.unwrap().operational_schema,
        state
    );
    assert!(f.engine.evaluate_readiness().await.is_err());
    // Both ordinary and fenced cutover worker admission use require_coverage.
    assert!(f.engine.require_coverage().is_err());
    let count = Arc::new(AtomicU64::new(0));
    let mut apply = applier(
        &f,
        count.clone(),
        |_txn: &AssembledTxn,
         _mseq: u64,
         _state: &Materialized|
         -> std::result::Result<apply::TransactionPlan, apply::ApplyError> {
            panic!("blocked apply must not plan")
        },
    );
    assert!(apply.step(&runtime).await.is_err());
    assert_eq!(count.load(AtomicOrdering::SeqCst), 0);
    runtime
        .marker(
            BASELINE_STREAM,
            "a".repeat(32),
            "repeat cannot clear blocker".into(),
        )
        .await
        .unwrap();
    assert!(
        runtime
            .state()
            .await
            .operational_schema
            .require_clear()
            .is_err()
    );
}
