//! Actual capture codecs, local buffer, ReaderLoop and durable journal receipts.
//! Tip/clock metadata are explicit inputs to the pure observation contract;
//! the config-gated engine test qualifies their PostgreSQL producers.
use super::*;
use crate::pg::{ddl, replication::Lineage};
use std::time::Instant;

const PG_EPOCH_US: i64 = 946_684_800_000_000;
fn lineage() -> Lineage {
    Lineage {
        system_id: 1,
        timeline: 1,
        database: "db".into(),
        xlogpos: 10,
    }
}
fn heartbeat() -> Watermark {
    super::super::verification::verification_marker(uuid::Uuid::new_v4())
}
async fn durable(capture: &mut PgCapture<Resolver, Transport>) -> Captured {
    loop {
        let batch = capture.step().await.unwrap();
        if batch.durable_lsn.is_some() {
            return batch;
        }
    }
}
async fn ready() -> (
    Fixture,
    PgCapture<Resolver, Transport>,
    mpsc::Sender<source::Result<Vec<u8>>>,
    PgApplier<DestinationDb, Planner>,
) {
    let f = Fixture::new().await;
    let (tx, rx) = mpsc::channel(32);
    let mut capture = f.capture(rx, 10).await;
    let runtime = f.engine.runtime();
    runtime.7.begin(
        source(),
        "engine-test".into(),
        capture.writer_checkpoint().unwrap(),
        None,
    );
    runtime.7.schema(capture.schema_state());
    marker(&tx, heartbeat(), 20).await;
    let first = durable(&mut capture).await;
    runtime.7.observe(&first);
    let mut apply = f.applier(Arc::new(AtomicU64::new(0)), ReaderCheckpoint::default());
    assert!(apply.step(&runtime).await.unwrap());
    (f, capture, tx, apply)
}
async fn probe(
    f: &Fixture,
    capture: &mut PgCapture<Resolver, Transport>,
    tx: &mpsc::Sender<source::Result<Vec<u8>>>,
    age_ms: i64,
    started: Instant,
) {
    let mark = heartbeat();
    f.engine
        .runtime
        .7
        .source_tip(
            &lineage(),
            25,
            PG_EPOCH_US + age_ms * 1000,
            started,
            mark.clone(),
        )
        .unwrap();
    // An uncaptured source tip can never look like zero-age readiness.
    assert!(f.engine.runtime.live_readiness().lag.is_none());
    marker(tx, mark, 30).await;
    f.engine.runtime.7.observe(&durable(capture).await);
}

#[tokio::test]
async fn paired_lag_reports_bytes_and_source_commit_age_then_apply_clears_age() {
    let (f, mut capture, tx, mut apply) = ready().await;
    let started = Instant::now();
    probe(&f, &mut capture, &tx, 500, started).await;
    let live = f.engine.runtime.live_readiness();
    let lag = live.lag.unwrap();
    assert_eq!(
        (lag.source_tip_lsn, lag.applied_source_lsn, lag.lag_bytes),
        (25, 20, 5)
    );
    assert!(
        lag.oldest_unapplied_age_ms >= 500
            && lag.oldest_unapplied_age_ms < 500 + started.elapsed().as_millis() as u64 + 50
    );
    assert!(live.capture.unwrap().healthy);
    assert!(apply.step(&f.engine.runtime()).await.unwrap());
    let lag = f.engine.runtime.live_readiness().lag.unwrap();
    // Applied can advance past a sampled tip; its diagnostic distance is zero.
    assert!(lag.applied_source_lsn > lag.source_tip_lsn);
    assert_eq!((lag.oldest_unapplied_age_ms, lag.lag_bytes), (0, 0));
}

#[tokio::test]
async fn stale_tip_wrong_lineage_and_worker_exit_revoke_live_facts() {
    let (f, mut capture, tx, _) = ready().await;
    probe(
        &f,
        &mut capture,
        &tx,
        1500,
        Instant::now() - Duration::from_secs(2),
    )
    .await;
    assert!(f.engine.runtime.live_readiness().lag.is_none());
    assert!(f.engine.runtime.live_readiness().capture.is_none());
    let mut wrong = lineage();
    wrong.timeline += 1;
    assert!(
        f.engine
            .runtime
            .7
            .source_tip(&wrong, 25, PG_EPOCH_US, Instant::now(), heartbeat())
            .is_err()
    );
    f.engine.runtime.7.finish();
    let live = f.engine.runtime.live_readiness();
    assert!(!live.running && live.lag.is_none() && live.capture.is_none());
    assert!(
        live.schema.is_some(),
        "retain last schema diagnostic while revoked"
    );
}

#[tokio::test]
async fn recovered_buffer_baseline_is_unknown_until_its_real_applied_receipt() {
    let (f, mut capture, tx, _) = ready().await;
    marker(&tx, heartbeat(), 30).await;
    durable(&mut capture).await;
    let runtime = f.engine.runtime();
    let checkpoint = decode_reader_checkpoint(&runtime.snapshot().progress["applied"].0).unwrap();
    runtime.7.begin(
        source(),
        "engine-test".into(),
        capture.writer_checkpoint().unwrap(),
        Some(checkpoint.clone()),
    );
    let mark = heartbeat();
    runtime
        .7
        .source_tip(
            &lineage(),
            35,
            PG_EPOCH_US + 500_000,
            Instant::now(),
            mark.clone(),
        )
        .unwrap();
    marker(&tx, mark, 40).await;
    runtime.7.observe(&durable(&mut capture).await);
    assert!(
        runtime.live_readiness().lag.is_none(),
        "pre-existing LSN30 backlog was not observed this run"
    );
    let mut apply = f.applier(Arc::new(AtomicU64::new(0)), checkpoint);
    assert!(apply.step(&runtime).await.unwrap());
    assert!(
        runtime.live_readiness().lag.is_some(),
        "authenticated receipt covers the unobserved baseline"
    );
}

#[tokio::test]
async fn ddl_accessor_blocks_unclassified_and_sticky_boundary_then_new_baseline_clears() {
    let (f, mut capture, tx, _) = ready().await;
    let record = ddl::DdlRecord {
        origin: None,
        format: 1,
        installation: "test".into(),
        server_major: 18,
        version: 1,
        tag: "ALTER TABLE".into(),
        xid: "40".into(),
        query: "ALTER TABLE unknown ADD COLUMN x int".into(),
        commands: vec![],
    };
    let mut begin = vec![b'B'];
    begin.extend(40u64.to_be_bytes());
    begin.extend(0i64.to_be_bytes());
    begin.extend(40u32.to_be_bytes());
    tx.send(Ok(wal(begin))).await.unwrap();
    capture.step().await.unwrap();
    let content = serde_json::to_vec(&record).unwrap();
    let mut message = vec![b'M', 1];
    message.extend(39u64.to_be_bytes());
    message.extend(ddl::MESSAGE_PREFIX.as_bytes());
    message.push(0);
    message.extend((content.len() as u32).to_be_bytes());
    message.extend(content);
    tx.send(Ok(wal(message))).await.unwrap();
    capture.step().await.unwrap();
    assert!(capture.schema_state().unwrap().unknown_schema_job);
    let mut commit = vec![b'C', 0];
    commit.extend(40u64.to_be_bytes());
    commit.extend(41u64.to_be_bytes());
    commit.extend(0i64.to_be_bytes());
    tx.send(Ok(wal(commit))).await.unwrap();
    let batch = capture.step().await.unwrap();
    assert!(batch.stop);
    let schema = capture.schema_state().unwrap();
    assert!(schema.blocked_boundary.is_some() && !schema.unknown_schema_job);
    f.engine.runtime.7.schema(Some(schema));
    f.engine.runtime.7.finish();
    let live = f.engine.runtime.live_readiness();
    assert!(!live.running && live.schema.unwrap().blocked_boundary.is_some());
    // Resolving a sticky boundary requires a newly qualified capture baseline;
    // no mutation/clear API is added to SchemaVersions.
    let new = Fixture::new().await;
    let (_tx, rx) = mpsc::channel(1);
    let clean = new.capture(rx, 10).await.schema_state().unwrap();
    assert!(clean.blocked_boundary.is_none() && !clean.unknown_schema_job);
    new.engine.runtime.7.begin(
        source(),
        "engine-test".into(),
        Some(crate::buffer::writer::WriterCheckpoint::default()),
        None,
    );
    new.engine.runtime.7.schema(Some(clean));
    let live = new.engine.runtime.live_readiness();
    assert!(live.running && live.schema_fresh);
    let schema = live.schema.unwrap();
    let sample = ComponentSnapshot {
        schema_jobs: Some((schema.blocked_boundary.is_some(), schema.unknown_schema_job)),
        ..ComponentSnapshot::default()
    };
    let facts = super::super::readiness::assemble(&new.engine.runtime, &new.journal, &sample)
        .await
        .unwrap();
    assert!(!facts.blocked_ddl && !facts.unknown_schema_job);
}

struct FailingApply {
    real: PgApplier<DestinationDb, Planner>,
    fail: Arc<std::sync::atomic::AtomicBool>,
}
impl Applier for FailingApply {
    async fn step(&mut self, runtime: &Runtime) -> Result<bool> {
        if self.fail.load(AtomicOrdering::SeqCst) {
            return Err(recovery("injected apply-only exit"));
        }
        self.real.step(runtime).await
    }
}
#[tokio::test]
async fn actual_capture_and_apply_only_failures_revoke_previously_positive_observations() {
    for apply_failure in [false, true] {
        let f = Fixture::new().await;
        let (tx, rx) = mpsc::channel(32);
        let mut capture = f.capture(rx, 10).await;
        let mut database = f.database(tx.clone(), Arc::new(AtomicU64::new(0)));
        let fail = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut apply = FailingApply {
            real: f.applier(Arc::new(AtomicU64::new(0)), ReaderCheckpoint::default()),
            fail: fail.clone(),
        };
        let (_pause, paused) = watch::channel(false);
        let mut budget = budget();
        let runtime = f.engine.runtime();
        let observer = async {
            while runtime.state().await.lifecycle.state != State::CatchingUp {
                tokio::task::yield_now().await;
            }
            let mut next_lsn = 40;
            'probe: loop {
                let mark = heartbeat();
                runtime
                    .7
                    .source_tip(
                        &lineage(),
                        next_lsn - 5,
                        PG_EPOCH_US + 1_000_000,
                        Instant::now(),
                        mark.clone(),
                    )
                    .unwrap();
                marker(&tx, mark, next_lsn).await;
                next_lsn += 10;
                // Whole-library I/O can outlast a single probe. Keep producing
                // fresh, real heartbeat transactions rather than waiting on
                // an observation that has correctly expired.
                let retry = Instant::now();
                while retry.elapsed() < Duration::from_millis(500) {
                    let live = runtime.live_readiness();
                    if live.lag.is_some() && live.schema_fresh && live.schema.is_some() {
                        break 'probe;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            }
            if apply_failure {
                fail.store(true, AtomicOrdering::SeqCst);
            } else {
                tx.send(Err(source::Error::RecoveryRequired(
                    "injected capture exit after positive sample",
                )))
                .await
                .unwrap();
            }
        };
        let (run, ()) = tokio::time::timeout(Duration::from_secs(60), async {
            tokio::join!(
                f.engine.run(
                    &mut capture,
                    &mut database,
                    &mut apply,
                    config(),
                    &mut budget,
                    paused
                ),
                observer
            )
        })
        .await
        .unwrap();
        assert!(run.is_err());
        let live = runtime.live_readiness();
        assert!(!live.running && live.lag.is_none() && live.capture.is_none());
        assert!(live.schema.is_some());
    }
}

/// Narrow reuse by the terminal-status regression: real engine ownership,
/// capture codec, local durable buffer and a policy-hitting DDL exit.
pub(crate) async fn retired_ddl_runtime(control: PathBuf) -> (Runtime, Journal) {
    let definition = crate::MigrationDefinition::from_json(
        include_str!("../../README.md")
            .split("```json")
            .nth(1)
            .unwrap()
            .split("```")
            .next()
            .unwrap(),
    )
    .unwrap();
    let mut f = Fixture::with_definition(Some(definition)).await;
    f.engine = f
        .engine
        .with_pause_control(PauseControl::new(&control).unwrap());
    let (tx, rx) = mpsc::channel(32);
    let mut capture = f.capture(rx, 10).await;
    let mut database = f.database(tx.clone(), Arc::new(AtomicU64::new(0)));
    let mut apply = f.applier(Arc::new(AtomicU64::new(0)), ReaderCheckpoint::default());
    let (_pause, paused) = watch::channel(false);
    let mut budget = budget();
    let runtime = f.engine.runtime();
    let ddl = async {
        while runtime.state().await.lifecycle.state != State::CatchingUp {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            runtime.live_readiness().owner_generation.as_deref(),
            Some(
                std::fs::read_to_string(control.join("owner-generation"))
                    .unwrap()
                    .as_str()
            )
        );
        let record = ddl::DdlRecord {
            origin: None,
            format: 1,
            installation: "test".into(),
            server_major: 18,
            version: 1,
            tag: "ALTER TABLE".into(),
            xid: "40".into(),
            query: "ALTER TABLE unknown ADD COLUMN x int".into(),
            commands: vec![],
        };
        let mut begin = vec![b'B'];
        begin.extend(40u64.to_be_bytes());
        begin.extend(0i64.to_be_bytes());
        begin.extend(40u32.to_be_bytes());
        tx.send(Ok(wal(begin))).await.unwrap();
        let content = serde_json::to_vec(&record).unwrap();
        let mut message = vec![b'M', 1];
        message.extend(39u64.to_be_bytes());
        message.extend(ddl::MESSAGE_PREFIX.as_bytes());
        message.push(0);
        message.extend((content.len() as u32).to_be_bytes());
        message.extend(content);
        tx.send(Ok(wal(message))).await.unwrap();
        let mut commit = vec![b'C', 0];
        commit.extend(40u64.to_be_bytes());
        commit.extend(41u64.to_be_bytes());
        commit.extend(0i64.to_be_bytes());
        tx.send(Ok(wal(commit))).await.unwrap();
    };
    let (run, ()) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(
            f.engine.run(
                &mut capture,
                &mut database,
                &mut apply,
                config(),
                &mut budget,
                paused
            ),
            ddl
        )
    })
    .await
    .unwrap();
    assert!(run.is_err());
    assert!(
        !control.join("owner-generation").exists(),
        "real Owner::retire removes the token"
    );
    let live = runtime.live_readiness();
    assert!(!live.running && live.owner_generation.is_some());
    assert!(live.schema.unwrap().blocked_boundary.is_some());
    (runtime, f.journal)
}
