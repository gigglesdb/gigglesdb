//! Pure producer reports are tested with the actual engine, journal and buffer.
//! The database fault-injection transport is shared with the qualified engine
//! path in tests/engine.rs, not a substitute object store or journal.
use super::*;
use crate::{
    status::{RetentionPolicy, SlotStatus},
    verify::{Coverage, ResultSlot, TableCoverage, VerificationReport},
};

#[derive(Default)]
struct Clock(AtomicU64);
impl budget::Clock for Clock {
    fn now(&self) -> Duration {
        Duration::from_secs(self.0.load(AtomicOrdering::SeqCst))
    }
}
fn slot(lsn: u64, retained: u64, free: u64, maximum: Option<u64>) -> WalObservation {
    WalObservation {
        slot: SlotStatus {
            restart_lsn: Some("0/1".into()),
            confirmed_flush_lsn: Some("0/1".into()),
            valid: true,
            invalidation_reason: None,
            retained_wal_bytes: retained,
            max_slot_wal_keep_size_bytes: maximum,
            disk_free_bytes: free,
        },
        current_lsn: lsn,
    }
}
fn collected<T>() -> ResultSlot<T> {
    ResultSlot::Collected {
        checked: 0,
        findings: vec![],
        evidence: vec![],
    }
}
fn snapshot() -> ComponentSnapshot {
    ComponentSnapshot {
        capture: Some(CaptureObservation {
            healthy: true,
            checkpoint: checkpoint(),
        }),
        apply_lag: Some(ApplyLagObservation {
            source: source(),
            order_domain: "engine-test".into(),
            source_tip_lsn: 10,
            applied_source_lsn: 10,
            lag_bytes: 0,
            oldest_unapplied_age_ms: 0,
            observed_at: std::time::Instant::now(),
        }),
        schema_jobs: Some((false, false)),
        verification: Some(VerificationReport {
            tables: vec![TableCoverage {
                table: "rows".into(),
                rows: Coverage {
                    total: 1,
                    verified: 1,
                    ..Coverage::default()
                },
                exclusion_reason: None,
            }],
            schema: collected(),
            sequences: collected(),
            constraints: collected(),
            indexes: collected(),
            foreign_keys: collected(),
        }),
        busy_rows_past_budget: Some(0),
        dead_letters: Some(0),
        sequences_syncable: Some(true),
        reverse_prerequisites: Some(true),
        fence_risks_acknowledged: Some(true),
        buffer_contract_matches: Some(true),
        wal: None,
    }
}
fn checkpoint() -> crate::pg::replication::Checkpoint {
    crate::pg::replication::Checkpoint {
        lineage: crate::pg::replication::Lineage {
            system_id: 1,
            timeline: 1,
            database: "db".into(),
            xlogpos: 10,
        },
        slot: "slot".into(),
        durable_lsn: 10,
    }
}

#[tokio::test]
async fn lag_uses_milliseconds_and_missing_stale_or_wrong_domain_fails_at_maximum_budget() {
    let f = completed().await;
    let mut definition = crate::MigrationDefinition::from_json(
        include_str!("../../README.md")
            .split("```json")
            .nth(1)
            .unwrap()
            .split("```")
            .next()
            .unwrap(),
    )
    .unwrap();
    definition.readiness_policy.max_lag_seconds = 1;
    definition.budgets.max_replication_lag_seconds = 1;
    f.engine.runtime.1.write().unwrap().definition = Some(definition.clone());
    let mut sample = snapshot();
    sample.apply_lag.as_mut().unwrap().oldest_unapplied_age_ms = 1000;
    let facts = super::super::readiness::assemble(&f.engine.runtime, &f.journal, &sample)
        .await
        .unwrap();
    assert_eq!((facts.apply_lag, facts.lag_budget), (1000, 1000));
    sample.apply_lag.as_mut().unwrap().oldest_unapplied_age_ms = 1001;
    assert!(
        super::super::readiness::assemble(&f.engine.runtime, &f.journal, &sample)
            .await
            .unwrap()
            .blockers()
            .contains(&"apply-lag-over-budget".into())
    );
    definition.readiness_policy.max_lag_seconds = 0;
    f.engine.runtime.1.write().unwrap().definition = Some(definition.clone());
    sample.apply_lag.as_mut().unwrap().oldest_unapplied_age_ms = 1;
    assert!(
        super::super::readiness::assemble(&f.engine.runtime, &f.journal, &sample)
            .await
            .unwrap()
            .blockers()
            .contains(&"apply-lag-over-budget".into())
    );
    definition.readiness_policy.max_lag_seconds = u64::MAX;
    definition.budgets.max_replication_lag_seconds = u64::MAX;
    f.engine.runtime.1.write().unwrap().definition = Some(definition);
    sample.apply_lag = None;
    for stale_or_wrong in 0..3 {
        if stale_or_wrong == 1 {
            sample = snapshot();
            sample.apply_lag.as_mut().unwrap().observed_at -= Duration::from_secs(2);
        } else if stale_or_wrong == 2 {
            sample = snapshot();
            sample.apply_lag.as_mut().unwrap().order_domain = "wrong".into();
        }
        let facts = super::super::readiness::assemble(&f.engine.runtime, &f.journal, &sample)
            .await
            .unwrap();
        assert_eq!((facts.apply_lag, facts.lag_budget), (1, 0));
    }
}
async fn completed() -> Fixture {
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
    f.engine.resume().await.unwrap();
    f.engine
        .runtime
        .marker(
            "source-slot",
            encode_source_checkpoint(&checkpoint()).unwrap(),
            "installed".into(),
        )
        .await
        .unwrap();
    // Inject connected health only after the real path has completed and paused.
    *f.engine.runtime.2.write().unwrap() = true;
    f
}
#[tokio::test]
async fn every_component_regression_recomputes_and_journals_its_reason() {
    let f = completed().await;
    type Regression = (fn(&mut ComponentSnapshot), &'static str);
    let regressions: [Regression; 13] = [
        (
            |s| s.capture.as_mut().unwrap().healthy = false,
            "capture-unhealthy",
        ),
        (
            |s| s.capture.as_mut().unwrap().checkpoint.lineage.timeline += 1,
            "lineage-mismatch",
        ),
        (
            |s| s.apply_lag.as_mut().unwrap().oldest_unapplied_age_ms = u64::MAX,
            "apply-lag-over-budget",
        ),
        (
            |s| s.schema_jobs = Some((true, false)),
            "schema-boundary-pending",
        ),
        (
            |s| s.schema_jobs = Some((false, true)),
            "schema-boundary-pending",
        ),
        (
            |s| s.verification.as_mut().unwrap().tables[0].rows.verified = 0,
            "verification-incomplete",
        ),
        (
            |s| s.busy_rows_past_budget = Some(1),
            "busy-row-budget-exhausted",
        ),
        (|s| s.dead_letters = Some(1), "dead-letters-present"),
        (
            |s| s.verification.as_mut().unwrap().indexes = ResultSlot::NotCollected,
            "destination-schema-invalid",
        ),
        (
            |s| s.verification.as_mut().unwrap().sequences = ResultSlot::NotCollected,
            "sequences-not-syncable",
        ),
        (|s| s.sequences_syncable = None, "sequences-not-syncable"),
        (
            |s| s.reverse_prerequisites = Some(false),
            "reverse-prerequisites-missing",
        ),
        (
            |s| s.fence_risks_acknowledged = Some(false),
            "fence-risk-unacknowledged",
        ),
    ];
    for (regress, expected) in regressions {
        let mut sample = snapshot();
        f.engine.evaluate_sample(&sample).await.unwrap();
        assert_eq!(
            f.engine.runtime.snapshot().lifecycle.state,
            State::VerifiedReady
        );
        regress(&mut sample);
        let facts = f.engine.evaluate_sample(&sample).await.unwrap();
        let expected = if expected == "lineage-mismatch" {
            vec![expected.to_string(), "apply-lag-over-budget".into()]
        } else {
            vec![expected.to_string()]
        };
        assert_eq!(facts.blockers(), expected);
        let state = f.journal.load(false).await.unwrap();
        assert_eq!(state.lifecycle.state, State::CatchingUp);
        assert_eq!(state.lifecycle.blockers, expected);
    }
    let mut sample = snapshot();
    f.engine.evaluate_sample(&sample).await.unwrap();
    sample.buffer_contract_matches = Some(false);
    f.engine.evaluate_sample(&sample).await.unwrap();
    let state = f.journal.load(false).await.unwrap();
    assert_eq!(state.lifecycle.state, State::CatchingUp);
    assert_eq!(state.lifecycle.blockers, vec!["buffer-contract-mismatch"]);
}
#[tokio::test]
async fn missing_producers_and_zero_denominator_fail_closed() {
    let f = completed().await;
    let facts = f
        .engine
        .evaluate_sample(&ComponentSnapshot::default())
        .await
        .unwrap();
    assert_eq!(facts.blockers().len(), 12); // chunks are authenticated and complete.
    let mut sample = snapshot();
    sample.verification.as_mut().unwrap().tables.clear();
    assert!(
        f.engine
            .evaluate_sample(&sample)
            .await
            .unwrap()
            .blockers()
            .contains(&"verification-incomplete".into())
    );
    let mut sample = snapshot();
    let rows = &mut sample.verification.as_mut().unwrap().tables[0].rows;
    *rows = Coverage {
        busy: 1,
        ..Coverage::default()
    };
    assert!(
        f.engine
            .evaluate_sample(&sample)
            .await
            .unwrap()
            .blockers()
            .contains(&"verification-incomplete".into())
    );
    sample.verification.as_mut().unwrap().tables[0].rows = Coverage::default();
    assert!(
        f.engine
            .evaluate_sample(&sample)
            .await
            .unwrap()
            .blockers()
            .is_empty()
    );
    // A fresh chunk plan regression must override otherwise holding reports.
    let mut table = plan().table;
    table.table = "other".into();
    let different = ChunkPlan::new(table, 100, vec![]).unwrap();
    f.engine.plan(&different).await.unwrap();
    let mut sample = snapshot();
    sample
        .verification
        .as_mut()
        .unwrap()
        .tables
        .push(TableCoverage {
            table: "other".into(),
            rows: Coverage::default(),
            exclusion_reason: None,
        });
    let facts = f.engine.evaluate_sample(&sample).await.unwrap();
    assert_eq!(facts.blockers(), vec!["chunks-incomplete"]);
    let state = f.journal.load(false).await.unwrap();
    assert_eq!(state.lifecycle.state, State::CatchingUp);
    assert_eq!(state.lifecycle.blockers, vec!["chunks-incomplete"]);
}
#[tokio::test]
async fn wal_warning_blocker_abort_is_durable_and_invalidates_reconstructed_resume() {
    let f = completed().await;
    let clock = Arc::new(Clock::default());
    let mut monitor = WalMonitor::new(clock.clone(), RetentionPolicy::default()).unwrap();
    assert_eq!(
        f.engine
            .monitor_wal(&mut monitor, &slot(100, 0, 100_000, Some(100_000)))
            .await
            .unwrap()
            .level,
        WalLevel::Healthy
    );
    clock.0.store(10, AtomicOrdering::SeqCst);
    let warning = f
        .engine
        .monitor_wal(&mut monitor, &slot(200, 0, 20_000, Some(100_000)))
        .await
        .unwrap();
    assert_eq!(warning.level, WalLevel::Warning);
    clock.0.store(20, AtomicOrdering::SeqCst);
    let blocker = f
        .engine
        .monitor_wal(&mut monitor, &slot(300, 0, 5_000, Some(100_000)))
        .await
        .unwrap();
    assert_eq!(blocker.level, WalLevel::Blocker);
    assert_eq!(f.engine.runtime.snapshot().lifecycle.state, State::Blocked);
    clock.0.store(30, AtomicOrdering::SeqCst);
    let abort = f
        .engine
        .monitor_wal(&mut monitor, &slot(400, 0, 1_000, None))
        .await
        .unwrap();
    assert_eq!(abort.level, WalLevel::Abort);
    assert!(abort.coverage_loss && !abort.resumable && abort.source_untouched);
    assert!(!f.engine.resumable());
    let report: WalDecision =
        serde_json::from_str(&f.engine.coverage_loss_report().unwrap()).unwrap();
    assert!(report.coverage_loss);
    // Even a coordinator-only recheck/pause cannot erase the engine boundary.
    f.engine
        .runtime
        .lifecycle(Event::ResumeAfterRecheck {
            remaining_blockers: vec![],
            facts: Some(coordinator::ReadinessFacts::default()),
        })
        .await
        .unwrap();
    f.engine
        .runtime
        .lifecycle(Event::Interrupt {
            to: State::Paused,
            reason: "injected coordinator-only state manipulation".into(),
        })
        .await
        .unwrap();
    assert!(
        f.engine
            .runtime
            .snapshot()
            .lifecycle
            .allowed_next()
            .contains(&State::CatchingUp)
    );
    assert!(
        f.engine
            .resume()
            .await
            .unwrap_err()
            .to_string()
            .contains("coverage-loss")
    );
    let mut coordinator = Coordinator::observer(
        f.journal.clone(),
        Local::create(f.root.join("reopened-local")).unwrap(),
        "new-owner",
    )
    .unwrap();
    coordinator.acquire().await.unwrap();
    let restored = Engine::new(coordinator, f.journal.clone());
    assert!(!restored.resumable());
    assert!(
        restored
            .resume()
            .await
            .unwrap_err()
            .to_string()
            .contains("coverage-loss")
    );
    assert_eq!(
        restored.runtime.snapshot().lifecycle.authority,
        Authority::Source
    );
    assert_eq!(
        restored.runtime.snapshot().chunk_attempts,
        f.engine.runtime.snapshot().chunk_attempts
    );
}
#[test]
fn wal_uses_source_growth_even_when_retained_wal_decreases_and_checks_slot_limit() {
    let clock = Arc::new(Clock::default());
    let mut monitor = WalMonitor::new(clock.clone(), RetentionPolicy::default()).unwrap();
    monitor
        .sample(&slot(100, 1000, 1_000_000, Some(100_000)))
        .unwrap();
    clock.0.store(10, AtomicOrdering::SeqCst);
    let decision = monitor
        .sample(&slot(200, 0, 1_000_000, Some(1_000)))
        .unwrap();
    assert_eq!(decision.wal_bytes_per_second, Some(10.0));
    assert_eq!(decision.level, WalLevel::Abort);
    assert_eq!(decision.slot_headroom_bytes, Some(1000));
    assert!(monitor.sample(&slot(199, 0, 1000, None)).is_err());
}
struct AbortWhenApplying {
    entered: Arc<std::sync::atomic::AtomicBool>,
    sampled: Arc<std::sync::atomic::AtomicBool>,
}
impl ReadinessSource for AbortWhenApplying {
    fn sample(&self) -> ComponentSnapshot {
        let mut sample = ComponentSnapshot::default();
        if self.entered.load(AtomicOrdering::SeqCst) {
            sample.wal = Some(slot(100, 100, 1000, Some(100)));
            self.sampled.store(true, AtomicOrdering::SeqCst);
        }
        sample
    }
}
/// Fault-inject an uninterruptible apply step around the real buffer reader.
struct SlowApply {
    real: PgApplier<DestinationDb, Planner>,
    entered: Arc<std::sync::atomic::AtomicBool>,
    drained: Arc<std::sync::atomic::AtomicBool>,
    release: Arc<tokio::sync::Notify>,
}
impl Applier for SlowApply {
    async fn step(&mut self, runtime: &Runtime) -> Result<bool> {
        if !self.entered.swap(true, AtomicOrdering::SeqCst) {
            self.release.notified().await;
        }
        let result = self.real.step(runtime).await;
        self.drained.store(true, AtomicOrdering::SeqCst);
        result
    }
}
#[tokio::test]
async fn active_abort_joins_destination_operation_before_invalidating_coverage() {
    let mut f = completed().await;
    let mut reader = BufferReader::new(
        f.store.clone(),
        f.config.clone(),
        ReaderCheckpoint::default(),
        Arc::new(Order),
    )
    .unwrap();
    let mut before = vec![];
    while let Some(txn) = reader.next_txn().await.unwrap() {
        before.push(txn.canonical_bytes().unwrap());
    }
    assert!(!before.is_empty());
    let entered = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let sampled = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let drained = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let release = Arc::new(tokio::sync::Notify::new());
    f.engine.readiness_source = Some(Arc::new(AbortWhenApplying {
        entered: entered.clone(),
        sampled: sampled.clone(),
    }));
    let (tx, rx) = mpsc::channel(32);
    let mut capture = f.capture(rx, 10).await;
    let mut database = f.database(tx, Arc::new(AtomicU64::new(0)));
    let applied =
        decode_reader_checkpoint(&f.engine.runtime.snapshot().progress["applied"].0).unwrap();
    let mut applier = SlowApply {
        real: f.applier(Arc::new(AtomicU64::new(0)), applied),
        entered,
        drained: drained.clone(),
        release: release.clone(),
    };
    let (_pause, paused) = watch::channel(false);
    let mut budget = budget();
    let observer = async {
        while !sampled.load(AtomicOrdering::SeqCst) {
            tokio::task::yield_now().await;
        }
        assert!(
            f.engine.coverage_loss_report().is_none(),
            "in-flight apply must drain first"
        );
        let mut external =
            WalMonitor::new(Arc::new(Clock::default()), RetentionPolicy::default()).unwrap();
        assert!(
            f.engine
                .monitor_wal(&mut external, &slot(100, 100, 1000, Some(100)))
                .await
                .unwrap_err()
                .to_string()
                .contains("active engine")
        );
        release.notify_one();
    };
    tokio::time::timeout(Duration::from_secs(60), async {
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
    assert!(drained.load(AtomicOrdering::SeqCst));
    let recovered = f.journal.load(false).await.unwrap();
    assert_eq!(recovered.lifecycle.state, State::Failed);
    assert_eq!(recovered.lifecycle.authority, Authority::Source);
    assert!(!f.engine.resumable());
    assert!(f.engine.coverage_loss_report().is_some());
    let mut reader = BufferReader::new(
        f.store.clone(),
        f.config.clone(),
        ReaderCheckpoint::default(),
        Arc::new(Order),
    )
    .unwrap();
    let mut after = vec![];
    while let Some(txn) = reader.next_txn().await.unwrap() {
        after.push(txn.canonical_bytes().unwrap());
    }
    assert_eq!(
        before, after,
        "abort must preserve every durable buffered change"
    );
}
#[tokio::test]
async fn journaled_dead_letters_override_an_optimistic_collector() {
    let f = completed().await;
    f.engine.evaluate_sample(&snapshot()).await.unwrap();
    f.engine
        .runtime
        .commit(Transition::VerifierException {
            id: "dead-letter".into(),
            exception: Some(coordinator::VerifierException {
                range: "rows".into(),
                key: "7".into(),
                reason: "injected verification failure".into(),
            }),
        })
        .await
        .unwrap();
    let facts = f.engine.evaluate_sample(&snapshot()).await.unwrap();
    assert_eq!(facts.blockers(), vec!["dead-letters-present"]);
    assert_eq!(
        f.journal.load(false).await.unwrap().lifecycle.state,
        State::CatchingUp
    );
}

#[path = "stop_tests.rs"]
mod stop_tests;

#[tokio::test]
async fn identical_readiness_is_coalesced_change_and_heartbeat_are_immediate() {
    let f = completed().await;
    f.engine
        .runtime
        .progress_marker("buffered", "manifest-durable".into(), "123".into())
        .await
        .unwrap();
    let mut sample = snapshot();
    f.engine.evaluate_sample(&sample).await.unwrap();
    let before = f.journal.load(false).await.unwrap().applied_seq;
    assert_eq!(
        f.journal.load(false).await.unwrap().progress["buffered"].1,
        "123"
    );
    // Hold the heartbeat deadline constant while exercising repeated evaluation.
    f.engine
        .runtime
        .2
        .persistence
        .lock()
        .unwrap()
        .readiness
        .as_mut()
        .unwrap()
        .1 = std::time::Instant::now() + Duration::from_secs(60);
    for _ in 0..32 {
        f.engine.evaluate_sample(&sample).await.unwrap();
    }
    assert_eq!(f.journal.load(false).await.unwrap().applied_seq, before);
    sample.dead_letters = Some(1);
    f.engine.evaluate_sample(&sample).await.unwrap();
    let changed = f.journal.load(false).await.unwrap();
    assert_eq!(changed.applied_seq, before + 1);
    assert_eq!(changed.lifecycle.state, State::CatchingUp);
    assert!(
        changed
            .lifecycle
            .blockers
            .contains(&"dead-letters-present".into())
    );
    f.engine
        .runtime
        .2
        .persistence
        .lock()
        .unwrap()
        .readiness
        .as_mut()
        .unwrap()
        .1 = std::time::Instant::now() - Duration::from_secs(2);
    f.engine.evaluate_sample(&sample).await.unwrap();
    assert_eq!(f.journal.load(false).await.unwrap().applied_seq, before + 2);
}

#[tokio::test]
async fn identical_readiness_still_authenticates_owner_before_suppressing_write() {
    let f = completed().await;
    let sample = ComponentSnapshot::default();
    f.engine.evaluate_sample(&sample).await.unwrap();
    f.engine
        .runtime
        .2
        .persistence
        .lock()
        .unwrap()
        .readiness
        .as_mut()
        .unwrap()
        .1 = std::time::Instant::now() + Duration::from_secs(60);
    let mut replacement = Coordinator::observer(
        f.journal.clone(),
        Local::create(f.root.join("readiness-replacement")).unwrap(),
        "replacement",
    )
    .unwrap();
    replacement.acquire().await.unwrap();
    let before = f.journal.load(false).await.unwrap().applied_seq;
    assert!(f.engine.evaluate_sample(&sample).await.is_err());
    assert_eq!(f.journal.load(false).await.unwrap().applied_seq, before);
}
