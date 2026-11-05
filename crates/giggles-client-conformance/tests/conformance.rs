use giggles_client_conformance::{Report, Scenario, run};

fn assert_unknown(r: &Report, reason: Option<&str>) {
    r.validate().unwrap();
    assert_eq!(r.observations.len(), 1);
    let o = &r.observations[0];
    assert_eq!(o.class, "unknown", "{r:?}");
    assert_eq!(o.operation_id, Some(r.operation_id));
    if let Some(reason) = reason {
        assert_eq!(o.reason.as_deref(), Some(reason), "{r:?}");
    }
    assert_eq!(r.counters.request_starts, 1, "{r:?}");
    assert_eq!(r.counters.connections, 1, "{r:?}");
}

macro_rules! unknown_case {
    ($name:ident, $scenario:ident, $reason:expr) => {
        #[tokio::test]
        async fn $name() {
            let r = run(Scenario::$scenario, 42).await.unwrap();
            assert_unknown(&r, $reason);
            r.retain().unwrap();
        }
    };
}
unknown_case!(prefix_one_consumed_no_replay, Prefix1, None);
unknown_case!(prefix_two_consumed_no_replay, Prefix2, None);
unknown_case!(prefix_three_consumed_no_replay, Prefix3, None);
unknown_case!(partial_body_consumed_no_replay, BodyClose, None);
unknown_case!(complete_request_lost_reply_no_replay, CompleteClose, None);
unknown_case!(reply_prefix_one_truncated, ReplyPrefix1, None);
unknown_case!(reply_prefix_two_truncated, ReplyPrefix2, None);
unknown_case!(reply_prefix_three_truncated, ReplyPrefix3, None);
unknown_case!(reply_body_truncated, ReplyBody, None);
unknown_case!(outer_id_mismatch, OuterId, Some("Protocol"));
unknown_case!(outer_version_mismatch, OuterVersion, Some("Protocol"));
unknown_case!(reply_version_mismatch, ReplyVersion, Some("Protocol"));
unknown_case!(inner_operation_mismatch, InnerId, Some("Protocol"));
unknown_case!(inner_version_mismatch, InnerVersion, Some("Protocol"));
unknown_case!(zero_response_length, ZeroLength, Some("Protocol"));
unknown_case!(oversize_response_length, OversizeLength, Some("Protocol"));
unknown_case!(invalid_response_json, InvalidJson, Some("Protocol"));
unknown_case!(both_reply_and_error, Both, Some("Protocol"));
unknown_case!(neither_reply_nor_error, Neither, Some("Protocol"));
unknown_case!(
    availability_string_does_not_authorize_replay,
    Unavailable,
    Some("ServerIndeterminate")
);
unknown_case!(
    indeterminate_does_not_authorize_replay,
    Indeterminate,
    Some("ServerIndeterminate")
);
unknown_case!(
    reply_deadline_preserves_identity,
    ReadDeadline,
    Some("Timeout")
);

#[tokio::test]
async fn valid_committed_baseline() {
    let r = run(Scenario::Baseline, 0).await.unwrap();
    assert_eq!(r.observations[0].class, "committed");
    assert_eq!(r.counters.complete_execute[&r.operation_id], 1);
    assert_eq!(r.counters.authenticated, 1);
    r.retain().unwrap();
}

macro_rules! tls_case {
    ($name:ident, $scenario:ident) => {
        #[tokio::test]
        async fn $name() {
            let r = run(Scenario::$scenario, 1).await.unwrap();
            assert_eq!(r.observations[0].class, "tls-rejected", "{r:?}");
            assert_eq!(r.counters.request_starts, 0);
            assert!(r.counters.complete_execute.is_empty());
            r.retain().unwrap();
        }
    };
}
tls_case!(wrong_hostname_rejects_before_application, WrongName);
tls_case!(wrong_trust_rejects_before_application, Untrusted);
tls_case!(mismatched_alpn_rejects_before_application, WrongAlpn);
tls_case!(missing_alpn_rejects_before_application, NoAlpn);

#[tokio::test]
async fn tls_deadline_before_application() {
    let r = run(Scenario::TlsDeadline, 2).await.unwrap();
    assert_eq!(r.observations[0].class, "timeout-before-send", "{r:?}");
    assert_eq!(r.counters.request_starts, 0);
    r.retain().unwrap();
}

#[tokio::test]
async fn admission_deadline_never_dispatches_waiter() {
    let r = run(Scenario::AdmissionDeadline, 3).await.unwrap();
    assert_eq!(r.observations[0].class, "timeout-before-send", "{r:?}");
    assert_eq!(r.observations[1].class, "committed", "{r:?}");
    assert_eq!(r.counters.complete_execute[&r.operation_id], 1);
    assert_eq!(r.counters.complete_status[&r.operation_id], 1);
    r.retain().unwrap();
}

#[tokio::test]
async fn cancellation_active_reclaims_shared_permit() {
    let r = run(Scenario::CancelActive, 4).await.unwrap();
    assert_eq!(r.observations[0].class, "committed", "{r:?}");
    assert_eq!(r.counters.complete_execute[&r.operation_id], 1);
    assert_eq!(r.counters.complete_status[&r.operation_id], 1);
    r.retain().unwrap();
}

#[tokio::test]
async fn cancellation_admission_never_dispatches_waiter() {
    let r = run(Scenario::CancelAdmission, 5).await.unwrap();
    assert_eq!(r.observations[0].class, "committed", "{r:?}");
    assert_eq!(r.counters.complete_status[&r.operation_id], 1);
    r.retain().unwrap();
}

#[tokio::test]
async fn explicit_prewrite_seed_failover_dispatches_once() {
    let r = run(Scenario::PrewriteFailover, 6).await.unwrap();
    assert_eq!(r.observations[0].class, "committed", "{r:?}");
    assert_eq!(r.counters.connections, 1);
    assert_eq!(r.counters.complete_execute[&r.operation_id], 1);
    r.retain().unwrap();
}

#[tokio::test]
async fn status_absent_then_committed_is_never_rollback() {
    let r = run(Scenario::AbsentThenCommitted, 7).await.unwrap();
    assert_eq!(r.observations[0].class, "absent", "{r:?}");
    assert_eq!(r.observations[1].class, "committed", "{r:?}");
    assert!(r.observations.iter().all(|o| !o.status_is_rollback));
    assert_eq!(r.counters.complete_status[&r.operation_id], 2);
    assert!(r.counters.complete_execute.is_empty());
    r.retain().unwrap();
}

#[tokio::test]
async fn status_protocol_negative_controls() {
    for scenario in [Scenario::StatusInnerId, Scenario::StatusInnerVersion] {
        let r = run(scenario, 71).await.unwrap();
        r.validate().unwrap();
        assert_eq!(r.observations[0].class, "protocol");
        assert_eq!(r.counters.complete_status[&r.operation_id], 1);
        assert!(r.counters.complete_execute.is_empty());
        r.retain().unwrap();
    }
}

#[tokio::test]
async fn lost_reply_resolves_with_original_id_without_execute_replay() {
    let r = run(Scenario::LostReplyStatus, 72).await.unwrap();
    r.validate().unwrap();
    assert_eq!(r.observations[0].class, "unknown");
    assert_eq!(r.observations[1].class, "committed");
    assert!(
        r.observations
            .iter()
            .all(|o| o.operation_id == Some(r.operation_id))
    );
    assert_eq!(r.counters.complete_execute[&r.operation_id], 1);
    assert_eq!(r.counters.complete_status[&r.operation_id], 1);
    r.retain().unwrap();
}

#[tokio::test]
async fn write_pressure_reports_observation_without_inventing_flush_stage() {
    let r = run(Scenario::WritePressure, 8).await.unwrap();
    assert_eq!(r.observations[0].class, "unknown", "{r:?}");
    assert_eq!(r.observations[0].reason.as_deref(), Some("Timeout"));
    assert_eq!(r.counters.request_starts, 0);
    assert!(r.counters.events.iter().any(|e| e == "no_application_read"));
    assert!(
        r.limits
            .iter()
            .any(|s| s.contains("no deterministic flush"))
    );
    r.retain().unwrap();
}

#[tokio::test]
async fn seeded_fault_offsets_and_extreme_ids_preserve_identity() {
    for seed in [0, 1, 11, 19, u64::MAX] {
        let r = run(Scenario::BodyClose, seed).await.unwrap();
        assert_unknown(&r, None);
        assert!(
            r.counters
                .events
                .iter()
                .any(|e| e.starts_with(&format!("request_body_consumed_{}_", 1 + seed % 11)))
        );
        r.retain().unwrap();
    }
}

#[tokio::test]
async fn redaction_and_exclusive_report_retention() {
    let r = run(Scenario::InnerId, 9).await.unwrap();
    let json = serde_json::to_string(&r).unwrap();
    for forbidden in [
        "BEGIN PRIVATE",
        "BEGIN CERTIFICATE",
        "selection",
        "after",
        "client.key",
        "client.pem",
    ] {
        assert!(!json.contains(forbidden));
    }
    let path = r.retain().unwrap();
    let parsed: Report = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    parsed.validate().unwrap();
    assert!(r.retain().is_err());
}

#[tokio::test]
async fn every_supported_scenario_roundtrips_a_falsifiable_retained_report() {
    for scenario in Scenario::ALL {
        let report = run(*scenario, 314159).await.unwrap();
        let path = report.retain().unwrap();
        let parsed: Report = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        assert_eq!(parsed.scenario, *scenario);
        assert_eq!(parsed.seed, 314159);
        parsed
            .validate()
            .unwrap_or_else(|e| panic!("{}: {e}; {parsed:?}", scenario.name()));
        let mut wrong_reason = parsed.clone();
        wrong_reason.observations[0].reason = Some("unrelated-failure".into());
        assert!(
            wrong_reason.validate().is_err(),
            "{} reason",
            scenario.name()
        );
        if !matches!(
            scenario,
            Scenario::WrongName | Scenario::Untrusted | Scenario::WrongAlpn | Scenario::NoAlpn
        ) {
            let mut missing_boundary = parsed;
            missing_boundary.counters.events.clear();
            assert!(
                missing_boundary.validate().is_err(),
                "{} boundary",
                scenario.name()
            );
        }
    }
}
