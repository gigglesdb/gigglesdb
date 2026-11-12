use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use serde_json::{Value, json};
use skedaddle::{
    budget::Clock,
    coordinator::{Authority, ReadinessFacts, State},
    status::{
        AbortPolicy, BufferStatus, CatchUp, ChunkProgress, ChunkState, Counters, Counts,
        LatencyProbe, Measurement, Observations, Rate, RateMeter, RetentionLimit, RetentionPolicy,
        RiskLevel, SlotStatus, SourceImpact, Status, TableCoverage, VerificationCoverage,
    },
};

#[derive(Default)]
struct ManualClock(AtomicU64);
impl Clock for ManualClock {
    fn now(&self) -> Duration {
        Duration::from_millis(self.0.load(Ordering::SeqCst))
    }
}

fn rate(transactions: f64, bytes: f64) -> Rate {
    Rate {
        rows_per_second: transactions * 2.0,
        bytes_per_second: bytes,
        transactions_per_second: transactions,
    }
}

fn observations() -> Observations {
    Observations {
        state: State::VerifiedReady,
        epoch: 7,
        authority: Authority::Source,
        chunks: BTreeMap::from([(
            "public.t".into(),
            vec![ChunkProgress {
                range_lo: "0".into(),
                range_hi: "100".into(),
                state: ChunkState::Done,
                copied_rows: 100,
                copied_bytes: 1000,
                estimated_total_rows: Some(100),
            }],
        )]),
        slot: SlotStatus {
            restart_lsn: Some("0/10".into()),
            confirmed_flush_lsn: Some("0/20".into()),
            valid: true,
            invalidation_reason: None,
            retained_wal_bytes: 1000,
            max_slot_wal_keep_size_bytes: Some(1_000_000),
            disk_free_bytes: 2_000_000,
        },
        rates: Measurement {
            window_ms: 10_000,
            capture: rate(10.0, 100.0),
            buffer: rate(10.0, 100.0),
            apply: rate(20.0, 200.0),
            wal_bytes_per_second: 100.0,
        },
        buffer: BufferStatus {
            pending_bytes: 1000,
            pending_transactions: 100,
            oldest_unapplied_transaction_age_ms: 2000,
            capture_paused_by_backpressure: false,
            resident_segments: 1,
            max_resident_segments: 2,
            backend: "local-fs".into(),
            encryption: "volume-encryption".into(),
            failure_domain_contract: "process-crash; host-loss-requires-resnapshot".into(),
        },
        verification: VerificationCoverage {
            tables: BTreeMap::from([(
                "public.t".into(),
                TableCoverage {
                    total_rows: 100,
                    verified_rows: 100,
                    ..Default::default()
                },
            )]),
            schema_verified: true,
            sequences_verified: true,
            constraints_verified: true,
            indexes_verified: true,
            foreign_keys_verified: true,
        },
        blockers: vec![],
        source_impact: SourceImpact {
            replication_connection_lag_ms: Some(50),
            latency_probe: LatencyProbe {
                query: "SELECT 1".into(),
                window_ms: 10_000,
                samples: 100,
                p50_ms: Some(1.0),
                p99_ms: Some(3.0),
                error: None,
            },
        },
        readiness: ReadinessFacts {
            all_chunks_done: true,
            capture_healthy: true,
            lineage_matches: true,
            apply_lag: 2,
            lag_budget: 3,
            blocked_ddl: false,
            unknown_schema_job: false,
            verified_rows: 100,
            in_scope_rows: 100,
            busy_rows_past_budget: 0,
            dead_letters: 0,
            indexes_constraints_valid: true,
            sequences_syncable: true,
            reverse_prerequisites: true,
            fence_risks_acknowledged: true,
            buffer_contract_matches: true,
        },
    }
}

fn status(input: Observations) -> Status {
    Status::evaluate(input, RetentionPolicy::default()).unwrap()
}

#[test]
fn catch_up_not_achievable_when_apply_slower() {
    for apply in [0.0, 9.0, 10.0] {
        let mut input = observations();
        input.rates.apply = rate(apply, apply * 10.0);
        let output = status(input);
        assert_eq!(output.eta.catch_up, CatchUp::NotCurrentlyAchievable);
        let json: Value = serde_json::from_str(&output.to_json().unwrap()).unwrap();
        assert_eq!(json["eta"]["catch_up"], "not-currently-achievable");
        assert!(json["eta"].get("seconds").is_none());
        assert_eq!(json["eta"]["window_ms"], 10_000);
        assert!(!output.eta.assumptions.is_empty());
    }
}

#[test]
fn catch_up_accounts_for_byte_bottleneck_and_declares_assumptions() {
    let mut input = observations();
    input.rates.apply.bytes_per_second = 150.0;
    let output = status(input);
    assert_eq!(output.eta.catch_up, CatchUp::Estimated { seconds: 20.0 });
    assert_eq!(output.eta.window_ms, 10_000);
    assert_eq!(output.eta.assumptions.len(), 3);
    let mut input = observations();
    input.rates.apply.bytes_per_second = 100.0;
    assert_eq!(status(input).eta.catch_up, CatchUp::NotCurrentlyAchievable);
}

#[test]
fn rate_meter_uses_injected_clock_and_committed_counter_deltas() {
    let clock = Arc::new(ManualClock::default());
    let mut meter = RateMeter::new(clock.clone(), Counters::default()).unwrap();
    let counters = Counters {
        capture: Counts {
            rows: 20,
            bytes: 100,
            transactions: 10,
        },
        buffer: Counts {
            rows: 16,
            bytes: 80,
            transactions: 8,
        },
        apply: Counts {
            rows: 10,
            bytes: 50,
            transactions: 5,
        },
        wal_bytes: 1000,
    };
    assert!(meter.sample(counters).is_err());
    clock.0.store(2000, Ordering::SeqCst);
    let measured = meter.sample(counters).unwrap();
    assert_eq!(measured.window_ms, 2000);
    assert_eq!(measured.capture, rate(5.0, 50.0));
    assert_eq!(measured.buffer, rate(4.0, 40.0));
    assert_eq!(measured.apply, rate(2.5, 25.0));
    assert_eq!(measured.wal_bytes_per_second, 500.0);
    clock.0.store(3000, Ordering::SeqCst);
    assert!(meter.sample(Counters::default()).is_err());
    clock.0.store(4000, Ordering::SeqCst);
    let idle = meter.sample(counters).unwrap();
    assert_eq!(idle.window_ms, 2000); // failed reset did not replace baseline
    assert_eq!(idle.apply, Rate::default());
}

#[test]
fn stalled_apply_reports_retention_risk() {
    let clock = Arc::new(ManualClock::default());
    let mut meter = RateMeter::new(clock.clone(), Counters::default()).unwrap();
    clock.0.store(10_000, Ordering::SeqCst);
    let measured = meter
        .sample(Counters {
            capture: Counts {
                rows: 200,
                bytes: 1000,
                transactions: 100,
            },
            buffer: Counts {
                rows: 200,
                bytes: 1000,
                transactions: 100,
            },
            apply: Counts::default(),
            wal_bytes: 1000,
        })
        .unwrap();
    for (headroom, level) in [
        (400_000, RiskLevel::Healthy),
        (360_000, RiskLevel::Warning),
        (90_000, RiskLevel::Blocker),
        (12_000, RiskLevel::Abort),
    ] {
        let mut input = observations();
        input.rates = measured.clone();
        input.slot.max_slot_wal_keep_size_bytes = Some(input.slot.retained_wal_bytes + headroom);
        input.buffer.capture_paused_by_backpressure = true;
        let output = status(input);
        assert_eq!(output.retention_risk.level, level);
        assert_eq!(
            output.retention_risk.seconds_to_slot_limit,
            Some(headroom as f64 / 100.0)
        );
        assert_eq!(
            output.retention_risk.limiting_resource,
            Some(RetentionLimit::SlotWal)
        );
        assert_eq!(output.eta.catch_up, CatchUp::NotCurrentlyAchievable);
        if matches!(level, RiskLevel::Blocker | RiskLevel::Abort) {
            assert!(!output.ready_for_cutover);
            assert!(
                output
                    .blockers
                    .iter()
                    .any(|b| b.code == "retention-headroom")
            );
        }
        if level == RiskLevel::Abort {
            let directive = output.retention_risk.abort.unwrap();
            assert_eq!(directive.policy, AbortPolicy::AbortWithCoverageLoss);
            assert!(
                directive.leave_source_untouched
                    && directive.invalidate_resumability
                    && directive.declare_coverage_loss
            );
            assert_eq!(output.authority, Authority::Source);
        }
    }
}

#[test]
fn disk_exhaustion_can_limit_unlimited_slot_retention() {
    let mut input = observations();
    input.slot.max_slot_wal_keep_size_bytes = None;
    input.slot.disk_free_bytes = 10_000;
    let output = status(input);
    assert_eq!(output.retention_risk.slot_headroom_bytes, None);
    assert_eq!(output.retention_risk.seconds_to_slot_limit, None);
    assert_eq!(
        output.retention_risk.seconds_to_disk_exhaustion,
        Some(100.0)
    );
    assert_eq!(
        output.retention_risk.limiting_resource,
        Some(RetentionLimit::Disk)
    );
    assert_eq!(output.retention_risk.level, RiskLevel::Abort);
}

#[test]
fn exhausted_headroom_aborts_even_when_measured_wal_is_zero() {
    let mut input = observations();
    input.slot.max_slot_wal_keep_size_bytes = Some(999);
    input.rates.wal_bytes_per_second = 0.0;
    let output = status(input);
    assert_eq!(output.retention_risk.seconds_to_slot_limit, Some(0.0));
    assert_eq!(output.retention_risk.level, RiskLevel::Abort);
    let mut input = observations();
    input.rates.wal_bytes_per_second = 0.0;
    let output = status(input);
    assert_eq!(output.retention_risk.level, RiskLevel::Healthy);
    assert_eq!(output.retention_risk.seconds_to_slot_limit, None);
    assert_eq!(output.retention_risk.seconds_to_disk_exhaustion, None);
}

#[test]
fn slot_invalidation_blocks_success_even_if_throughput_healthy() {
    for (valid, reason) in [(false, None), (true, Some("wal_removed".into()))] {
        let mut input = observations();
        input.slot.valid = valid;
        input.slot.invalidation_reason = reason;
        let output = status(input);
        assert_eq!(output.eta.catch_up, CatchUp::Estimated { seconds: 10.0 });
        assert!(!output.ready_for_cutover);
        assert_eq!(output.state, State::CatchingUp);
        assert!(output.blockers.iter().any(|b| b.code == "slot-invalidated"));
        assert!(output.blockers.iter().all(|b| !b.remediation.is_empty()));
    }
}

#[test]
fn readiness_cannot_be_promoted_by_partial_coverage_or_busy_rows() {
    let mut input = observations();
    input.verification.tables.clear();
    let output = status(input);
    assert!(!output.ready_for_cutover);
    assert!(
        output
            .blockers
            .iter()
            .any(|b| b.code == "verification-evidence-mismatch")
    );
    let mut input = observations();
    let coverage = input.verification.tables.get_mut("public.t").unwrap();
    coverage.verified_rows = 99;
    coverage.busy_rows = 1;
    input.readiness.verified_rows = 99;
    let output = status(input);
    assert!(!output.ready_for_cutover);
    assert!(
        output
            .blockers
            .iter()
            .any(|b| b.code == "verification-incomplete")
    );
}

#[test]
fn every_independent_readiness_regression_blocks_cutover() {
    let regressions: [fn(&mut ReadinessFacts); 10] = [
        |r| r.all_chunks_done = false,
        |r| r.capture_healthy = false,
        |r| r.lineage_matches = false,
        |r| r.apply_lag = 4,
        |r| r.blocked_ddl = true,
        |r| r.dead_letters = 1,
        |r| r.indexes_constraints_valid = false,
        |r| r.sequences_syncable = false,
        |r| r.reverse_prerequisites = false,
        |r| r.buffer_contract_matches = false,
    ];
    for regress in regressions {
        let mut input = observations();
        regress(&mut input.readiness);
        assert!(!status(input).ready_for_cutover);
    }
}

#[test]
fn invalid_measurements_coverage_and_thresholds_fail_closed() {
    for invalid in [f64::NAN, f64::INFINITY, -1.0] {
        let mut input = observations();
        input.rates.apply.bytes_per_second = invalid;
        assert!(Status::evaluate(input, RetentionPolicy::default()).is_err());
    }
    let mut input = observations();
    input.rates.window_ms = 0;
    assert!(Status::evaluate(input, RetentionPolicy::default()).is_err());
    let mut input = observations();
    input
        .verification
        .tables
        .get_mut("public.t")
        .unwrap()
        .excluded_rows = 1;
    assert!(Status::evaluate(input, RetentionPolicy::default()).is_err());
    assert!(
        Status::evaluate(
            observations(),
            RetentionPolicy {
                abort_seconds: 1000,
                ..Default::default()
            }
        )
        .is_err()
    );
}

#[test]
fn configured_retention_thresholds_and_coverage_exclusions_are_respected() {
    let mut input = observations();
    input.slot.disk_free_bytes = 10_000;
    let output = Status::evaluate(
        input,
        RetentionPolicy {
            warning_seconds: 200,
            blocker_seconds: 90,
            abort_seconds: 50,
            abort_policy: AbortPolicy::AbortWithCoverageLoss,
        },
    )
    .unwrap();
    assert_eq!(output.retention_risk.level, RiskLevel::Warning);
    assert!(output.retention_risk.remediation.is_some());
    assert!(output.ready_for_cutover);

    let mut input = observations();
    let coverage = input.verification.tables.get_mut("public.t").unwrap();
    coverage.verified_rows = 90;
    coverage.excluded_rows = 10;
    input.readiness.in_scope_rows = 90;
    input.readiness.verified_rows = 90;
    assert!(status(input).ready_for_cutover);
}

#[test]
fn range_and_blocker_order_is_stable_and_duplicate_ranges_are_rejected() {
    let mut input = observations();
    input.chunks.get_mut("public.t").unwrap()[0].range_lo = "a".into();
    input.chunks.get_mut("public.t").unwrap()[0].range_hi = "b".into();
    let mut range = input.chunks["public.t"][0].clone();
    range.range_lo = "b".into();
    range.range_hi = "c".into();
    input
        .chunks
        .get_mut("public.t")
        .unwrap()
        .insert(0, range.clone());
    let output = status(input);
    assert_eq!(output.chunks["public.t"][0].range_lo, "a");
    assert_eq!(output.chunks["public.t"][1].range_lo, "b");

    let mut input = observations();
    let duplicate = input.chunks["public.t"][0].clone();
    input.chunks.get_mut("public.t").unwrap().push(duplicate);
    assert!(Status::evaluate(input, RetentionPolicy::default()).is_err());
    let mut input = observations();
    input.slot.valid = false;
    input.buffer.capture_paused_by_backpressure = true;
    let output = status(input);
    assert!(output.blockers.windows(2).all(|b| b[0].code <= b[1].code));
}

#[test]
fn status_json_golden() {
    let output = status(observations());
    assert!(output.ready_for_cutover);
    let expected = json!({
        "schema_version":1,"state":"verified-ready","epoch":7,"authority":"source",
        "chunks":{"public.t":[{"range_lo":"0","range_hi":"100","state":"done","copied_rows":100,"copied_bytes":1000,"estimated_total_rows":100}]},
        "slot":{"restart_lsn":"0/10","confirmed_flush_lsn":"0/20","valid":true,"invalidation_reason":null,
            "retained_wal_bytes":1000,"max_slot_wal_keep_size_bytes":1000000,"disk_free_bytes":2000000},
        "rates":{"window_ms":10000,"capture":{"rows_per_second":20.0,"bytes_per_second":100.0,"transactions_per_second":10.0},
            "buffer":{"rows_per_second":20.0,"bytes_per_second":100.0,"transactions_per_second":10.0},
            "apply":{"rows_per_second":40.0,"bytes_per_second":200.0,"transactions_per_second":20.0},"wal_bytes_per_second":100.0},
        "buffer":{"pending_bytes":1000,"pending_transactions":100,"oldest_unapplied_transaction_age_ms":2000,
            "capture_paused_by_backpressure":false,"resident_segments":1,"max_resident_segments":2,
            "backend":"local-fs","encryption":"volume-encryption","failure_domain_contract":"process-crash; host-loss-requires-resnapshot"},
        "verification":{"tables":{"public.t":{"total_rows":100,"verified_rows":100,"busy_rows":0,"mismatched_rows":0,"excluded_rows":0}},
            "schema_verified":true,"sequences_verified":true,"constraints_verified":true,"indexes_verified":true,"foreign_keys_verified":true},
        "blockers":[],"source_impact":{"replication_connection_lag_ms":50,"latency_probe":{"query":"SELECT 1","window_ms":10000,"samples":100,"p50_ms":1.0,"p99_ms":3.0,"error":null}},
        "ready_for_cutover":true,
        "eta":{"catch_up":"estimated","seconds":10.0,"window_ms":10000,"assumptions":[
            "Capture and committed apply rates remain equal to the measured window averages.",
            "Pending work includes all captured work; no remaining copy work, pauses, retries or schema changes are added.",
            "The slower of transaction and byte backlog drainage bounds catch-up; this is not a cutover ETA."]},
        "retention_risk":{"level":"healthy","slot_headroom_bytes":999000,"seconds_to_slot_limit":9990.0,"seconds_to_disk_exhaustion":20000.0,
            "limiting_resource":"slot-wal","policy":{"warning_seconds":3600,"blocker_seconds":900,"abort_seconds":120,"abort_policy":"abort-with-coverage-loss"},
            "window_ms":10000,"assumptions":[
                "Source WAL generation remains equal to the measured window average.",
                "No retained WAL is released; all new WAL consumes disk headroom; other disk consumers are excluded.",
                "max_slot_wal_keep_size is a conservative planning boundary, not an exact slot invalidation deadline."],"remediation":null}
    });
    let encoded = output.to_json().unwrap();
    assert_eq!(serde_json::from_str::<Value>(&encoded).unwrap(), expected);
    assert_eq!(encoded, status(observations()).to_json().unwrap());
}
