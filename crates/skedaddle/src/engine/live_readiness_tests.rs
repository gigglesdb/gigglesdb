//! Actual capture codecs, local buffer, ReaderLoop and durable journal receipts.
//! Tip/clock metadata are explicit inputs to the pure observation contract;
//! the config-gated engine test qualifies their PostgreSQL producers.
use super::*;
use crate::engine::live_readiness::MAX_OBSERVATION_AGE;
use crate::pg::{ddl, replication::Lineage};
use std::time::Instant;

const PG_EPOCH_US: i64 = 946_684_800_000_000;

#[test]
fn pg_probe_distance_age_and_budget_match_old_numbers_golden() {
    let order = Arc::new(crate::pg::apply::NativePgOrder("engine-test".into()));
    for tip in [1, 9, 10, 255, u32::MAX as u64, u64::MAX] {
        for applied in [1, 9, 10, 256, u32::MAX as u64, u64::MAX] {
            let probe =
                PgReadinessProbe::new(source(), order.domain().into(), tip, 0, Instant::now());
            let sample = probe
                .sample(&Position::new(order.domain(), applied.to_be_bytes()), None)
                .unwrap();
            let mut lag = ApplyLagObservation {
                source: source(),
                order: order.clone(),
                source_tip: probe.tip().clone(),
                applied_source: Position::new(order.domain(), applied.to_be_bytes()),
                lag: sample.lag.unwrap(),
                oldest_unapplied_age_ms: 0,
                observed_at: sample.observed_at,
            };
            let capture = CaptureObservation {
                healthy: true,
                source: source(),
                position: probe.tip().clone(),
            };
            assert_eq!(
                lag.lag,
                LagMeasure {
                    value: tip.saturating_sub(applied),
                    unit: "bytes"
                }
            );
            assert!(lag.matches(&capture));
            for age in [0, 1, 999, 1000, 1001, u64::MAX] {
                lag.oldest_unapplied_age_ms = age;
                for budget in [0u64, 1, u64::MAX] {
                    assert_eq!(
                        lag.compare_budget(budget),
                        (age, budget.saturating_mul(1000))
                    );
                }
            }
            lag.lag.value ^= 1;
            assert!(!lag.matches(&capture), "tampered byte distance");
            lag.lag.value ^= 1;
            lag.order = Arc::new(crate::pg::apply::NativePgOrder(String::new()));
            lag.source_tip = Position::new("", tip.to_be_bytes());
            lag.applied_source = Position::new("", applied.to_be_bytes());
            let capture = CaptureObservation {
                position: lag.source_tip.clone(),
                ..capture
            };
            assert!(!lag.matches(&capture), "empty ordering domain");
        }
    }
    // Exact old checked arithmetic, including rounding, future clocks and overflow.
    for (clock, elapsed_us, committed, expected) in [
        (1000, 0, Some(Some(1000)), Some(0)),
        (1000, 0, Some(Some(999)), Some(1)),
        (1000, 0, Some(Some(0)), Some(1)),
        (1000, 1, Some(Some(0)), Some(2)),
        (1000, 1000, Some(Some(0)), Some(2)),
        (0, 0, Some(Some(1)), None),
        (i64::MAX, 1, None, None),
        (i64::MAX, 0, Some(Some(i64::MIN)), None),
        (
            i64::MAX,
            0,
            Some(Some(0)),
            Some((i64::MAX as u64).div_ceil(1000)),
        ),
        (0, 0, None, Some(0)),
        (0, 0, Some(None), None),
    ] {
        assert_eq!(
            PgReadinessProbe::commit_age(clock, Duration::from_micros(elapsed_us), committed),
            expected
        );
    }
    let probe = PgReadinessProbe::new(source(), order.domain().into(), 1, 0, Instant::now());
    assert_eq!(
        probe
            .sample(&Position::new("wrong", 1u64.to_be_bytes()), None)
            .err(),
        Some(crate::event::EventError::PositionDomainMismatch)
    );
    for applied in [
        Position::new(order.domain(), vec![1]),
        Position::new(order.domain(), 0u64.to_be_bytes()),
    ] {
        assert!(probe.sample(&applied, None).is_err());
    }
    assert!(
        PgReadinessProbe::new(source(), order.domain().into(), 0, 0, Instant::now())
            .sample(&Position::new(order.domain(), 1u64.to_be_bytes()), None)
            .is_err()
    );
}

struct DecimalReadinessOrder;
fn decimal(bytes: &[u8]) -> std::result::Result<u64, crate::event::EventError> {
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or(crate::event::EventError::InvalidEncoding(
            "readiness decimal",
        ))
}
impl PositionOrder for DecimalReadinessOrder {
    fn domain(&self) -> &str {
        "readiness-decimal"
    }
    fn compare(
        &self,
        left: &[u8],
        right: &[u8],
    ) -> std::result::Result<std::cmp::Ordering, crate::event::EventError> {
        Ok(decimal(left)?.cmp(&decimal(right)?))
    }
}
fn native_position(value: &str) -> Position {
    Position::new("readiness-decimal", value.as_bytes())
}
fn native_source() -> SourceIdentity {
    SourceIdentity {
        engine: "test-native".into(),
        ..source()
    }
}
fn native_checkpoint(value: &str) -> ReaderCheckpoint {
    ReaderCheckpoint {
        next_mseq: 1,
        last_position: Some(native_position(value)),
        last_record: Some(crate::buffer::format::RecordBoundary {
            source: native_source(),
            chunk_rows: false,
        }),
        ..ReaderCheckpoint::default()
    }
}
#[derive(Clone)]
struct NativeProbe {
    source: SourceIdentity,
    tip: Position,
    started: Instant,
    age: Option<u64>,
    lag_known: bool,
    retention: Option<RetentionMargin>,
    schema_unknown: bool,
    schema_boundary: Option<SchemaBoundary>,
}
impl NativeProbe {
    fn new() -> Self {
        Self {
            source: native_source(),
            tip: native_position("10"),
            started: Instant::now(),
            age: Some(1500),
            lag_known: true,
            schema_unknown: false,
            schema_boundary: None,
            retention: Some(RetentionMargin {
                position: native_position("9"),
                remaining: LagMeasure {
                    value: 500,
                    unit: "transactions",
                },
                history_available: true,
                time_to_exhaustion: Some(Duration::from_secs(7200)),
            }),
        }
    }
}
impl ReadinessProbe for NativeProbe {
    fn source(&self) -> &SourceIdentity {
        &self.source
    }
    fn tip(&self) -> &Position {
        &self.tip
    }
    fn observed_at(&self) -> Instant {
        self.started
    }
    fn sample(
        &self,
        applied: &Position,
        _: Option<Option<i64>>,
    ) -> std::result::Result<ReadinessProbeSample, crate::event::EventError> {
        self.tip.compare(applied, &DecimalReadinessOrder)?;
        Ok(ReadinessProbeSample {
            lag: self.lag_known.then(|| LagMeasure {
                value: decimal(self.tip.bytes())
                    .unwrap()
                    .saturating_sub(decimal(applied.bytes()).unwrap()),
                unit: "transactions",
            }),
            oldest_unapplied_age_ms: self.age,
            retention: self.retention.clone(),
            schema: Some(SchemaState {
                blocked_boundary: self.schema_boundary.clone(),
                unknown_schema_job: self.schema_unknown,
            }),
            observed_at: self.started,
        })
    }
}
fn begin_native(runtime: &Runtime, baseline: Option<&str>) {
    let writer = baseline.map_or_else(crate::buffer::writer::WriterCheckpoint::default, |value| {
        crate::buffer::writer::WriterCheckpoint {
            next_mseq: 1,
            last_position: Some(native_position(value)),
            last_record: native_checkpoint(value).last_record,
            ..crate::buffer::writer::WriterCheckpoint::default()
        }
    });
    runtime.7.begin(
        native_source(),
        Arc::new(DecimalReadinessOrder),
        Some(writer),
        Some(native_checkpoint("9")),
    );
}
fn observe_native(runtime: &Runtime, mark: Watermark, value: &str) {
    let position = native_position(value);
    runtime.7.observe(&Captured {
        transaction: Some(
            AssembledTxn::new(
                native_source(),
                crate::event::TxnId(vec![1]),
                position.clone(),
                None,
                vec![],
            )
            .unwrap(),
        ),
        durable_position: Some(position.clone()),
        watermarks: vec![super::super::CapturedMarker::Chunk {
            marker: mark,
            commit: position,
            committed_events: vec![],
        }],
        ..Captured::default()
    });
}
fn admit_native(runtime: &Runtime, probe: NativeProbe) {
    let mark = heartbeat();
    let previous = runtime.live_readiness().lag.map(|lag| lag.observed_at);
    runtime
        .observe_readiness_probe(Arc::new(probe), mark.clone())
        .unwrap();
    assert_eq!(
        runtime.live_readiness().lag.map(|lag| lag.observed_at),
        previous,
        "uncaptured tip cannot replace or refresh previous durable evidence"
    );
    observe_native(runtime, mark, "10");
}

#[tokio::test]
async fn connector_schema_stop_retains_native_evidence_across_clear_probe_and_exit() {
    let f = Fixture::new().await;
    let runtime = f.engine.runtime();
    for kind in [
        SchemaStopKind::Ddl,
        SchemaStopKind::UnknownDdl,
        SchemaStopKind::TableMapChanged,
    ] {
        begin_native(&runtime, None);
        let boundary = SchemaBoundary::Connector(ConnectorSchemaStop {
            connector: "test-native".into(),
            kind,
            table: Some("source.widgets".into()),
            diagnostic: "ALTER TABLE source.widgets ADD COLUMN value int".into(),
            position: native_position("10"),
        });
        runtime.7.schema(Some(SchemaState {
            blocked_boundary: Some(boundary.clone()),
            unknown_schema_job: false,
        }));
        // A capture stop is already a blocker before lag/retention is known.
        assert_eq!(
            runtime.live_readiness().blocked,
            Some(ReadinessBlocked::Schema(boundary.clone()))
        );
        tokio::time::sleep(MAX_OBSERVATION_AGE + Duration::from_millis(10)).await;
        admit_native(&runtime, NativeProbe::new());
        let live = runtime.live_readiness();
        assert!(live.lag.is_some() && live.retention.is_some());
        assert!(
            !live.schema_fresh,
            "fresh clear probe cannot refresh capture"
        );
        assert_eq!(
            live.schema.unwrap().blocked_boundary,
            Some(boundary.clone())
        );
        assert_eq!(
            live.blocked,
            Some(ReadinessBlocked::Schema(boundary.clone()))
        );
        let mut lost_retention = NativeProbe::new();
        lost_retention.retention.as_mut().unwrap().history_available = false;
        admit_native(&runtime, lost_retention);
        let live = runtime.live_readiness();
        assert!(!live.retention.unwrap().history_available);
        assert_eq!(
            live.blocked,
            Some(ReadinessBlocked::Schema(boundary.clone()))
        );
        runtime.7.finish();
        let live = runtime.live_readiness();
        assert!(!live.running && live.lag.is_none());
        assert_eq!(live.blocked, Some(ReadinessBlocked::Schema(boundary)));
    }
}

#[tokio::test]
async fn unknown_schema_job_blocks_from_probe_and_capture_with_clear_probe() {
    let f = Fixture::new().await;
    let runtime = f.engine.runtime();
    begin_native(&runtime, None);
    let mut probe = NativeProbe::new();
    probe.schema_unknown = true;
    admit_native(&runtime, probe);
    assert_eq!(
        runtime.live_readiness().blocked,
        Some(ReadinessBlocked::UnknownSchemaJob)
    );
    runtime.7.schema(Some(SchemaState {
        blocked_boundary: None,
        unknown_schema_job: true,
    }));
    admit_native(&runtime, NativeProbe::new());
    assert_eq!(
        runtime.live_readiness().blocked,
        Some(ReadinessBlocked::UnknownSchemaJob)
    );
    // A fresh owner with an explicitly clear classifier can establish readiness.
    begin_native(&runtime, None);
    admit_native(&runtime, NativeProbe::new());
    assert_eq!(runtime.live_readiness().blocked, None);
}

#[tokio::test]
async fn connector_probe_schema_stop_blocks_even_without_lag_or_retention() {
    let f = Fixture::new().await;
    let runtime = f.engine.runtime();
    begin_native(&runtime, None);
    let mut probe = NativeProbe::new();
    let boundary = SchemaBoundary::Connector(ConnectorSchemaStop {
        connector: "test-native".into(),
        kind: SchemaStopKind::UnknownDdl,
        table: None,
        diagnostic: "unclassified statement".into(),
        position: native_position("10"),
    });
    probe.schema_boundary = Some(boundary.clone());
    probe.retention = None;
    probe.lag_known = false;
    admit_native(&runtime, probe);
    let live = runtime.live_readiness();
    assert!(live.schema_fresh && live.lag.is_none());
    assert_eq!(live.blocked, Some(ReadinessBlocked::Schema(boundary)));
}

#[tokio::test]
async fn native_probe_uses_order_age_freshness_baseline_and_independent_schema() {
    let f = Fixture::new().await;
    let runtime = f.engine.runtime();
    begin_native(&runtime, Some("10"));
    admit_native(&runtime, NativeProbe::new());
    assert_eq!(
        runtime.live_readiness().blocked,
        Some(ReadinessBlocked::UnknownLag)
    );
    runtime
        .7
        .applied(&native_position("10"), &native_checkpoint("10"));
    let live = runtime.live_readiness();
    let lag = live.lag.unwrap();
    assert!(lag.matches(live.capture.as_ref().unwrap()));
    assert_eq!(lag.compare_budget(1), (1500, 1000));
    assert_eq!(lag.compare_budget(2), (1500, 2000));
    assert_eq!(
        lag.lag,
        LagMeasure {
            value: 0,
            unit: "transactions"
        }
    );
    assert_eq!(lag.applied_source, native_position("10"));
    // Lexical bytes put 10 below 9; the source order must accept 9 -> 10.
    begin_native(&runtime, None);
    let mut probe = NativeProbe::new();
    probe.schema_unknown = true;
    admit_native(&runtime, probe);
    let live = runtime.live_readiness();
    assert_eq!(live.lag.unwrap().lag.value, 1);
    assert!(live.schema_fresh && live.schema.unwrap().unknown_schema_job);
    runtime.7.schema(Some(SchemaState {
        blocked_boundary: None,
        unknown_schema_job: true,
    }));
    admit_native(&runtime, NativeProbe::new());
    assert!(
        runtime.live_readiness().schema.unwrap().unknown_schema_job,
        "probe cannot clear capture blocker"
    );
    let mut stale = NativeProbe::new();
    stale.started -= Duration::from_secs(2);
    admit_native(&runtime, stale);
    let live = runtime.live_readiness();
    assert_eq!(live.blocked, Some(ReadinessBlocked::Stale));
    assert!(live.lag.is_none() && live.capture.is_none());
    runtime.7.finish();
    assert_eq!(
        runtime.live_readiness().blocked,
        Some(ReadinessBlocked::Unavailable)
    );
}

#[tokio::test]
async fn native_unknown_missing_and_retention_block_at_maximum_budget() {
    for case in 0..8 {
        let f = Fixture::new().await;
        let runtime = f.engine.runtime();
        {
            let mut state = runtime.1.write().unwrap();
            state.definition = Some(
                crate::MigrationDefinition::from_json(
                    include_str!("../../README.md")
                        .split("```json")
                        .nth(1)
                        .unwrap()
                        .split("```")
                        .next()
                        .unwrap(),
                )
                .unwrap(),
            );
            let definition = state.definition.as_mut().unwrap();
            definition.readiness_policy.max_lag_seconds = u64::MAX;
            definition.budgets.max_replication_lag_seconds = u64::MAX;
        }
        begin_native(&runtime, None);
        let mut probe = NativeProbe::new();
        let expected = match case {
            0 => ReadinessBlocked::MissingProbe,
            1 => {
                probe.lag_known = false;
                ReadinessBlocked::UnknownLag
            }
            2 => {
                probe.age = None;
                ReadinessBlocked::UnknownLag
            }
            3 => {
                probe.retention = None;
                ReadinessBlocked::UnknownRetention
            }
            4 => {
                probe.retention.as_mut().unwrap().history_available = false;
                ReadinessBlocked::RetentionLost
            }
            5 => {
                probe.retention.as_mut().unwrap().time_to_exhaustion = None;
                ReadinessBlocked::UnknownRetention
            }
            6 => {
                probe.retention.as_mut().unwrap().time_to_exhaustion =
                    Some(Duration::from_secs(900));
                ReadinessBlocked::RetentionRisk(WalLevel::Blocker)
            }
            _ => {
                probe.retention.as_mut().unwrap().time_to_exhaustion =
                    Some(Duration::from_secs(120));
                ReadinessBlocked::RetentionRisk(WalLevel::Abort)
            }
        };
        if case != 0 {
            admit_native(&runtime, probe);
        }
        let live = runtime.live_readiness();
        assert_eq!(live.blocked, Some(expected), "case {case}");
        assert!(live.lag.is_none());
        let sample = ComponentSnapshot {
            capture: live.capture,
            apply_lag: live.lag,
            ..ComponentSnapshot::default()
        };
        let facts = super::super::readiness::assemble(&runtime, &f.journal, &sample)
            .await
            .unwrap();
        assert_eq!((facts.apply_lag, facts.lag_budget), (1, 0));
        assert_eq!(
            facts.lineage_matches,
            case != 0,
            "lag/retention blockers do not change source identity"
        );
        assert!(facts.blockers().contains(&"apply-lag-over-budget".into()));
        if matches!(case, 4 | 6 | 7) {
            let mut monitor = WalMonitor::new(
                Arc::new(budget::SystemClock::default()),
                crate::status::RetentionPolicy::default(),
            )
            .unwrap();
            let decision = monitor
                .sample_components(&runtime, &sample)
                .unwrap()
                .unwrap();
            assert_eq!(
                decision.level,
                if case == 6 {
                    WalLevel::Blocker
                } else {
                    WalLevel::Abort
                }
            );
            f.engine.enforce_wal(&decision).await.unwrap();
            assert_eq!(f.engine.resumable(), case == 6);
        }
    }
}

#[tokio::test]
async fn native_wrong_domain_and_incomparable_positions_keep_typed_errors() {
    let f = Fixture::new().await;
    let runtime = f.engine.runtime();
    for (position, expected) in [
        (
            Position::new("wrong", b"10"),
            crate::event::EventError::PositionDomainMismatch,
        ),
        (
            native_position("incomparable"),
            crate::event::EventError::InvalidEncoding("readiness decimal"),
        ),
    ] {
        begin_native(&runtime, None);
        let mut probe = NativeProbe::new();
        probe.tip = position.clone();
        assert_eq!(
            runtime.observe_readiness_probe(Arc::new(probe), heartbeat()),
            Err(expected.clone())
        );
        begin_native(&runtime, None);
        let mut probe = NativeProbe::new();
        probe.retention.as_mut().unwrap().position = position.clone();
        admit_native(&runtime, probe);
        assert_eq!(
            runtime.live_readiness().blocked,
            Some(ReadinessBlocked::Position(expected.clone()))
        );
        begin_native(&runtime, None);
        let mut checkpoint = native_checkpoint("10");
        checkpoint.last_position = Some(position.clone());
        runtime.7.applied(&position, &checkpoint);
        assert_eq!(
            runtime.live_readiness().blocked,
            Some(ReadinessBlocked::Position(expected.clone()))
        );
        begin_native(&runtime, None);
        runtime.7.observe(&Captured {
            transaction: Some(
                AssembledTxn::new(
                    native_source(),
                    crate::event::TxnId(vec![1]),
                    position.clone(),
                    None,
                    vec![],
                )
                .unwrap(),
            ),
            durable_position: Some(position),
            ..Captured::default()
        });
        assert_eq!(
            runtime.live_readiness().blocked,
            Some(ReadinessBlocked::Position(expected))
        );
    }
}
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
        if batch.durable_position.is_some() {
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
        capture.position_order(),
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
async fn pg_probe_heartbeat_admission_matches_old_decisions_golden() {
    let (f, mut capture, tx, _) = ready().await;
    let runtime = f.engine.runtime();
    let expected = heartbeat();
    runtime
        .7
        .source_tip(
            &lineage(),
            25,
            PG_EPOCH_US,
            Instant::now(),
            expected.clone(),
        )
        .unwrap();
    assert!(runtime.live_readiness().lag.is_none());
    marker(&tx, heartbeat(), 30).await;
    runtime.7.observe(&durable(&mut capture).await);
    assert!(
        runtime.live_readiness().lag.is_none(),
        "unrelated marker cannot admit sampled tip"
    );
    marker(&tx, expected, 40).await;
    runtime.7.observe(&durable(&mut capture).await);
    assert_eq!(runtime.live_readiness().lag.unwrap().lag.value, 5);
    let previous = runtime.live_readiness().lag.unwrap().observed_at;
    let next = heartbeat();
    runtime
        .7
        .source_tip(&lineage(), 45, PG_EPOCH_US, Instant::now(), next.clone())
        .unwrap();
    let old = runtime.live_readiness().lag.unwrap();
    assert_eq!(
        old.observed_at, previous,
        "pending heartbeat cannot refresh old evidence"
    );
    assert_eq!(pg_position_lsn(&old.source_tip, "engine-test").unwrap(), 25);
    marker(&tx, next, 50).await;
    runtime.7.observe(&durable(&mut capture).await);
    assert_eq!(
        pg_position_lsn(
            &runtime.live_readiness().lag.unwrap().source_tip,
            "engine-test"
        )
        .unwrap(),
        45
    );
    for (tip, started) in [
        (0, Instant::now()),
        (25, Instant::now() + Duration::from_secs(60)),
    ] {
        assert!(
            runtime
                .7
                .source_tip(&lineage(), tip, PG_EPOCH_US, started, heartbeat())
                .is_err()
        );
        let live = runtime.live_readiness();
        assert!(
            live.capture.is_none() && live.lag.is_none(),
            "invalid probe revokes prior tip"
        );
    }
}

#[tokio::test]
async fn paired_lag_reports_bytes_and_source_commit_age_then_apply_clears_age() {
    let (f, mut capture, tx, mut apply) = ready().await;
    let started = Instant::now();
    probe(&f, &mut capture, &tx, 500, started).await;
    let live = f.engine.runtime.live_readiness();
    let lag = live.lag.unwrap();
    assert_eq!(
        (
            pg_position_lsn(&lag.source_tip, "engine-test").unwrap(),
            pg_position_lsn(&lag.applied_source, "engine-test").unwrap(),
            lag.lag.value
        ),
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
    assert!(
        lag.applied_source
            .compare(&lag.source_tip, lag.order.as_ref())
            .unwrap()
            .is_gt()
    );
    assert_eq!((lag.oldest_unapplied_age_ms, lag.lag.value), (0, 0));
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
    runtime.flush_progress().await.unwrap();
    let checkpoint = decode_reader_checkpoint(&runtime.snapshot().progress["applied"].0).unwrap();
    runtime.7.begin(
        source(),
        capture.position_order(),
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
    let Some(SchemaBoundary::Postgres(boundary)) = schema.blocked_boundary.as_ref() else {
        panic!("PG producer must retain its typed boundary");
    };
    assert_eq!((&*boundary.xid, boundary.schema_version), ("40", 1));
    assert!(boundary.stop_apply_before_transaction && boundary.readiness_blocked);
    let expected = schema.blocked_boundary.clone().unwrap();
    f.engine.runtime.7.schema(Some(schema));
    f.engine.runtime.7.finish();
    let live = f.engine.runtime.live_readiness();
    assert_eq!(live.blocked, Some(ReadinessBlocked::Schema(expected)));
    assert!(!live.running && live.schema.unwrap().blocked_boundary.is_some());
    // Resolving a sticky boundary requires a newly qualified capture baseline;
    // no mutation/clear API is added to SchemaVersions.
    let new = Fixture::new().await;
    let (_tx, rx) = mpsc::channel(1);
    let clean = new.capture(rx, 10).await.schema_state().unwrap();
    assert!(clean.blocked_boundary.is_none() && !clean.unknown_schema_job);
    new.engine.runtime.7.begin(
        source(),
        Arc::new(crate::pg::apply::NativePgOrder("engine-test".into())),
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

#[tokio::test]
async fn installer_readiness_and_cutover_share_stored_definition_scope() {
    use crate::definition::{ExcludedObject, ObjectKind, ObjectSelector};
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
    let object = |schema: &str, name: &str, kind| ObjectSelector {
        schema: schema.into(),
        name: name.into(),
        kind,
        exclude_columns: vec![],
    };
    definition.selected_objects = vec![
        object("z", "parent", ObjectKind::Table),
        object("a", "child", ObjectKind::Table),
        object("a", "other", ObjectKind::Table),
        object("a", "child", ObjectKind::Table),
        object("a", "view", ObjectKind::View),
        object("a", "seq", ObjectKind::Sequence),
    ];
    definition.excluded_objects = vec![ExcludedObject {
        object: object("a", "excluded", ObjectKind::Table),
        reason: "out of scope".into(),
    }];
    let deduplicated = crate::pg::ddl::scoped_schema_tables(&definition);
    assert_eq!(deduplicated.len(), 3);
    // Coordinator definitions reject duplicate selectors; exercise normalization
    // above, then store the equivalent valid definition for all three phases.
    definition.selected_objects.sort();
    definition.selected_objects.dedup();
    let fixture = Fixture::with_definition(Some(definition)).await;
    let runtime = fixture.engine.runtime();
    // These phases all use this runtime accessor, including installer schema_tables.
    let installer_scope = runtime.scoped_schema_tables().await.unwrap();
    let readiness_scope = runtime.scoped_schema_tables().await.unwrap();
    let cutover_scope = runtime.scoped_schema_tables().await.unwrap();
    assert_eq!(installer_scope, deduplicated);
    assert_eq!(installer_scope, readiness_scope);
    assert_eq!(installer_scope, cutover_scope);
    assert_eq!(
        installer_scope
            .iter()
            .map(|t| (t.schema.as_str(), t.name.as_str()))
            .collect::<Vec<_>>(),
        vec![("a", "child"), ("a", "other"), ("z", "parent")]
    );
}
