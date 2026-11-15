use super::*;

fn bounded<F: Future>(future: F) -> impl Future<Output = F::Output> {
    // Keep multi-run test scenarios off the Windows test-thread stack.
    let future = Box::pin(future);
    async move {
        tokio::time::timeout(Duration::from_secs(60), future)
            .await
            .unwrap()
    }
}

#[tokio::test]
async fn catching_up_target_joins_apply_and_continues_with_new_transactions() {
    let f = Fixture::new().await;
    let (tx, rx) = mpsc::channel(32);
    let mut capture = f.capture(rx, 10).await;
    let count = Arc::new(AtomicU64::new(0));
    let entered = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let drained = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let release = Arc::new(tokio::sync::Notify::new());
    let mut applier = SlowApply {
        real: f.applier(count.clone(), ReaderCheckpoint::default()),
        entered: entered.clone(),
        drained: drained.clone(),
        release: release.clone(),
    };
    let reads = Arc::new(AtomicU64::new(0));
    let mut database = f.database(tx.clone(), reads.clone());
    let (_pause, paused) = watch::channel(false);
    let mut budget = budget();
    let observer = async {
        while f.engine.runtime.snapshot().lifecycle.state != State::CatchingUp
            || *f.engine.runtime.2.read().unwrap()
        {
            tokio::task::yield_now().await;
        }
        assert!(entered.load(AtomicOrdering::SeqCst));
        assert!(!drained.load(AtomicOrdering::SeqCst));
        assert!(f.engine.running.load(AtomicOrdering::SeqCst));
        release.notify_one();
    };
    let (result, ()) = bounded(async {
        tokio::join!(
            f.engine.run_until(
                &mut capture,
                &mut database,
                &mut applier,
                config(),
                &mut budget,
                paused,
                StopAt::CatchingUp
            ),
            observer
        )
    })
    .await;
    let reached = result.unwrap();
    assert_eq!(reached.state, State::CatchingUp);
    assert!(reached.target_reached);
    assert!(drained.load(AtomicOrdering::SeqCst));
    assert!(!f.engine.running.load(AtomicOrdering::SeqCst));
    assert!(!*f.engine.runtime.2.read().unwrap());
    assert_eq!(
        f.journal.load(false).await.unwrap().lifecycle.state,
        State::CatchingUp
    );
    let (pause, paused) = watch::channel(false);
    let writes = async {
        marker(
            &tx,
            Watermark {
                wm: WatermarkKind::Low,
                chunk: plan().ranges[0].id.clone(),
                attempt: 99,
            },
            100,
        )
        .await;
        while count.load(AtomicOrdering::SeqCst) < 4 {
            tokio::task::yield_now().await;
        }
        pause.send_replace(true);
    };
    let (result, ()) = bounded(async {
        tokio::join!(
            f.engine.run_until(
                &mut capture,
                &mut database,
                &mut applier,
                config(),
                &mut budget,
                paused,
                StopAt::UntilPaused
            ),
            writes
        )
    })
    .await;
    assert_eq!(result.unwrap().state, State::Paused);
    assert_eq!(reads.load(AtomicOrdering::SeqCst), 1);
    assert_eq!(count.load(AtomicOrdering::SeqCst), 4);
}

#[tokio::test]
async fn external_pause_retries_interrupted_copy_and_idle_pause_is_harmless() {
    let mut f = Fixture::new().await;
    let root = f.root.join("control");
    f.engine = f
        .engine
        .with_pause_control(PauseControl::new(&root).unwrap());
    let control = PauseControl::new(&root).unwrap();
    assert!(!control.request_pause().unwrap());
    assert_eq!(
        f.journal.load(false).await.unwrap().lifecycle.state,
        State::Copying
    );
    let (tx, rx) = mpsc::channel(32);
    let mut capture = f.capture(rx, 10).await;
    let count = Arc::new(AtomicU64::new(0));
    let mut applier = f.applier(count.clone(), ReaderCheckpoint::default());
    let reads = Arc::new(AtomicU64::new(0));
    let mut database = f.database(tx, reads.clone());
    database.stall_read = true;
    let (_pause, paused) = watch::channel(false);
    let mut budget = budget();
    let request = async {
        while reads.load(AtomicOrdering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        assert!(control.request_pause().unwrap());
    };
    let (result, ()) = bounded(async {
        tokio::join!(
            f.engine.run_until(
                &mut capture,
                &mut database,
                &mut applier,
                config(),
                &mut budget,
                paused,
                StopAt::UntilPaused
            ),
            request
        )
    })
    .await;
    assert_eq!(result.unwrap().state, State::Paused);
    let old = f.journal.load(false).await.unwrap();
    assert!(
        old.chunk_attempts
            .values()
            .all(|a| a.attempt == 1 && a.done.is_none())
    );
    assert!(!control.request_pause().unwrap());
    drop(capture);
    drop(applier);
    f.engine.resume().await.unwrap();
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
    let mut applier = f.applier(count, checkpoint);
    let (_pause, paused) = watch::channel(false);
    let reached = bounded(f.engine.run_until(
        &mut capture,
        &mut database,
        &mut applier,
        config(),
        &mut budget,
        paused,
        StopAt::CatchingUp,
    ))
    .await
    .unwrap();
    assert_eq!(reached.state, State::CatchingUp);
    assert!(reached.target_reached);
    let state = f.journal.load(false).await.unwrap();
    assert!(
        state
            .chunk_attempts
            .values()
            .all(|a| a.attempt == 2 && a.done.is_some())
    );
    assert_eq!(reads.load(AtomicOrdering::SeqCst), 2);
    assert!(!control.request_pause().unwrap());
    assert_eq!(
        f.journal.load(false).await.unwrap().lifecycle.state,
        State::CatchingUp
    );
}

#[tokio::test]
async fn verified_target_never_qualifies_missing_producers() {
    let f = Fixture::new().await;
    let (tx, rx) = mpsc::channel(32);
    let mut capture = f.capture(rx, 10).await;
    let count = Arc::new(AtomicU64::new(0));
    let mut applier = f.applier(count.clone(), ReaderCheckpoint::default());
    let mut database = f.database(tx, Arc::new(AtomicU64::new(0)));
    let (pause, paused) = watch::channel(false);
    let mut budget = budget();
    let (result, ()) = bounded(async {
        tokio::join!(
            f.engine.run_until(
                &mut capture,
                &mut database,
                &mut applier,
                config(),
                &mut budget,
                paused,
                StopAt::VerifiedReady
            ),
            wait_state(f.engine.runtime(), State::CatchingUp, pause, count)
        )
    })
    .await;
    let reached = result.unwrap();
    assert_eq!(reached.state, State::Paused);
    assert!(!reached.target_reached);
    assert!(!reached.blockers.is_empty());
}

struct Qualified(std::sync::atomic::AtomicBool);
impl ReadinessSource for Qualified {
    fn sample(&self) -> ComponentSnapshot {
        let mut sample = snapshot();
        if self.0.load(AtomicOrdering::SeqCst) {
            sample.apply_lag.as_mut().unwrap().oldest_unapplied_age_ms = u64::MAX;
        }
        sample
    }
}

#[tokio::test]
async fn verified_boundary_is_preserved_but_live_readiness_demotes_after_join() {
    let mut f = Fixture::new().await;
    f.engine.readiness_source = Some(Arc::new(Qualified(std::sync::atomic::AtomicBool::new(
        false,
    ))));
    f.engine
        .runtime
        .marker(
            "source-slot",
            encode_source_checkpoint(&checkpoint()).unwrap(),
            "installed".into(),
        )
        .await
        .unwrap();
    let (tx, rx) = mpsc::channel(32);
    let mut capture = f.capture(rx, 10).await;
    let mut database = f.database(tx, Arc::new(AtomicU64::new(0)));
    let mut applier = f.applier(Arc::new(AtomicU64::new(0)), ReaderCheckpoint::default());
    let (_pause, paused) = watch::channel(false);
    let mut budget = budget();
    let reached = bounded(f.engine.run_until(
        &mut capture,
        &mut database,
        &mut applier,
        config(),
        &mut budget,
        paused,
        StopAt::VerifiedReady,
    ))
    .await
    .unwrap();
    assert_eq!(reached.state, State::VerifiedReady);
    assert!(reached.target_reached);
    assert_eq!(reached.blockers, vec!["capture-unhealthy"]);
    assert_eq!(
        f.journal.load(false).await.unwrap().lifecycle.state,
        State::VerifiedReady
    );
    assert!(!*f.engine.runtime.2.read().unwrap());
    // Direct continuation accepts the saved boundary but re-evaluates health;
    // an immediate pause must demote before the Paused interrupt.
    let (_pause, paused) = watch::channel(true);
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
    assert_eq!(
        f.journal.load(false).await.unwrap().lifecycle.resume_state,
        Some(State::CatchingUp)
    );
    f.engine.resume().await.unwrap();
    let facts = f.engine.evaluate_readiness().await.unwrap();
    assert_eq!(facts.blockers(), vec!["capture-unhealthy"]);
    assert_eq!(
        f.journal.load(false).await.unwrap().lifecycle.state,
        State::CatchingUp
    );
}

#[tokio::test]
async fn verified_target_rechecks_producer_regression_during_apply_drain() {
    let mut f = Fixture::new().await;
    let source = Arc::new(Qualified(std::sync::atomic::AtomicBool::new(false)));
    f.engine.readiness_source = Some(source.clone());
    f.engine
        .runtime
        .marker(
            "source-slot",
            encode_source_checkpoint(&checkpoint()).unwrap(),
            "installed".into(),
        )
        .await
        .unwrap();
    let (tx, rx) = mpsc::channel(32);
    let mut capture = f.capture(rx, 10).await;
    let mut database = f.database(tx, Arc::new(AtomicU64::new(0)));
    let release = Arc::new(tokio::sync::Notify::new());
    let mut applier = FirstThenSlow {
        first_applied: false,
        slow: SlowApply {
            real: f.applier(Arc::new(AtomicU64::new(0)), ReaderCheckpoint::default()),
            entered: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            drained: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            release: release.clone(),
        },
    };
    let (_pause, paused) = watch::channel(false);
    let mut budget = budget();
    let regression = async {
        while f.engine.runtime.snapshot().lifecycle.state != State::VerifiedReady
            || *f.engine.runtime.2.read().unwrap()
        {
            tokio::task::yield_now().await;
        }
        source.0.store(true, AtomicOrdering::SeqCst);
        release.notify_one();
    };
    let (result, ()) = bounded(async {
        tokio::join!(
            f.engine.run_until(
                &mut capture,
                &mut database,
                &mut applier,
                config(),
                &mut budget,
                paused,
                StopAt::VerifiedReady
            ),
            regression
        )
    })
    .await;
    let reached = result.unwrap();
    assert_eq!(reached.state, State::CatchingUp);
    assert!(!reached.target_reached);
    assert!(reached.blockers.contains(&"apply-lag-over-budget".into()));
    assert_eq!(
        f.journal.load(false).await.unwrap().lifecycle.state,
        State::CatchingUp
    );
}

// A typed lag report needs a real journaled Applied receipt before it can
// qualify. Hold a later operation so the stop-drain regression retains that
// prerequisite while still exercising an in-flight destination operation.
struct FirstThenSlow {
    first_applied: bool,
    slow: SlowApply,
}
impl Applier for FirstThenSlow {
    async fn step(&mut self, runtime: &Runtime) -> Result<bool> {
        if !self.first_applied {
            let applied = self.slow.real.step(runtime).await?;
            self.first_applied = applied;
            Ok(applied)
        } else {
            self.slow.step(runtime).await
        }
    }
}

#[test]
fn control_contention_and_stale_generations_never_pause_a_new_owner() {
    let root = PathBuf::from(std::env::var_os("SKEDADDLE_TEST_ROOT").unwrap())
        .join(format!("control-{}", uuid::Uuid::new_v4()));
    let control = PauseControl::new(&root).unwrap();
    let owner = control.activate().unwrap();
    assert!(control.activate().is_err());
    assert!(control.request_pause().unwrap());
    assert!(owner.requested().unwrap());
    let token = std::fs::read_to_string(root.join("owner-generation")).unwrap();
    drop(owner);
    assert!(!control.request_pause().unwrap());
    let next = control.activate().unwrap();
    std::fs::File::create(root.join(format!("pause-{token}"))).unwrap();
    assert!(!next.requested().unwrap());
    assert!(control.request_pause().unwrap());
    assert!(next.requested().unwrap());
    drop(next);
    assert!(!control.request_pause().unwrap());
}

#[tokio::test]
async fn accepted_external_pause_during_target_drain_takes_priority() {
    let mut f = Fixture::new().await;
    let control = PauseControl::new(f.root.join("control")).unwrap();
    f.engine = f.engine.with_pause_control(control.clone());
    let (tx, rx) = mpsc::channel(32);
    let mut capture = f.capture(rx, 10).await;
    let mut database = f.database(tx, Arc::new(AtomicU64::new(0)));
    let release = Arc::new(tokio::sync::Notify::new());
    let drained = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut applier = SlowApply {
        real: f.applier(Arc::new(AtomicU64::new(0)), ReaderCheckpoint::default()),
        entered: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        drained: drained.clone(),
        release: release.clone(),
    };
    let (_pause, paused) = watch::channel(false);
    let mut budget = budget();
    let request = async {
        while f.engine.runtime.snapshot().lifecycle.state != State::CatchingUp
            || *f.engine.runtime.2.read().unwrap()
        {
            tokio::task::yield_now().await;
        }
        assert!(!drained.load(AtomicOrdering::SeqCst));
        assert!(control.request_pause().unwrap());
        release.notify_one();
    };
    let (result, ()) = bounded(async {
        tokio::join!(
            f.engine.run_until(
                &mut capture,
                &mut database,
                &mut applier,
                config(),
                &mut budget,
                paused,
                StopAt::CatchingUp
            ),
            request
        )
    })
    .await;
    let reached = result.unwrap();
    assert_eq!(reached.state, State::Paused);
    assert!(!reached.target_reached);
    assert!(reached.blockers.contains(&"engine pause requested".into()));
    assert!(!control.request_pause().unwrap());
    assert_eq!(
        f.journal.load(false).await.unwrap().lifecycle.state,
        State::Paused
    );
}

#[cfg(windows)]
#[tokio::test]
async fn journal_outage_joins_inflight_apply_before_durable_typed_pause() {
    use std::os::windows::fs::OpenOptionsExt;
    let f = completed().await;
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
    let (tx, rx) = mpsc::channel(32);
    let mut capture = f.capture(rx, 100).await;
    let entered = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let drained = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let release = Arc::new(tokio::sync::Notify::new());
    let mut applier = SlowApply {
        real: f.applier(
            Arc::new(AtomicU64::new(0)),
            decode_reader_checkpoint(&f.engine.runtime.snapshot().progress["applied"].0).unwrap(),
        ),
        entered: entered.clone(),
        drained: drained.clone(),
        release: release.clone(),
    };
    let mut database = f.database(tx, Arc::new(AtomicU64::new(0)));
    let (_pause, paused) = watch::channel(false);
    let mut budget = budget();
    let inject = async {
        while !entered.load(AtomicOrdering::SeqCst) {
            tokio::task::yield_now().await;
        }
        let seq = f.engine.runtime.snapshot().applied_seq;
        let held = std::fs::OpenOptions::new()
            .read(true)
            .share_mode(0)
            .open(
                f.root
                    .join("journal")
                    .join(f.journal.entry_key(seq).as_ref()),
            )
            .unwrap();
        assert!(f.engine.runtime.retention_tick().await.is_err());
        assert_eq!(
            f.engine.runtime.pause_reason(),
            Some(EnginePauseReason::CoordinatorJournalUnavailable)
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!drained.load(AtomicOrdering::SeqCst));
        assert!(f.engine.running.load(AtomicOrdering::SeqCst));
        drop(held);
        release.notify_one();
    };
    let (result, ()) = bounded(async {
        tokio::join!(
            f.engine.run_until(
                &mut capture,
                &mut database,
                &mut applier,
                config(),
                &mut budget,
                paused,
                StopAt::UntilPaused
            ),
            inject
        )
    })
    .await;
    let reached = result.unwrap();
    assert!(drained.load(AtomicOrdering::SeqCst));
    assert_eq!(reached.state, State::Paused);
    assert_eq!(
        reached.blockers,
        vec![EnginePauseReason::CoordinatorJournalUnavailable.to_string()]
    );
    assert_eq!(
        f.journal.load(false).await.unwrap().lifecycle.state,
        State::Paused
    );
}
