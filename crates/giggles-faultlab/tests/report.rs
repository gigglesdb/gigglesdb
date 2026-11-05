use giggles_client::Operation;
use giggles_core::*;
use giggles_faultlab::{
    proxy::{Direction, Event, Fault, Trace},
    report::*,
};
use serde_json::json;
use sha2::{Digest, Sha256};
fn traces() -> std::collections::BTreeMap<String, Trace> {
    let names = (1..=3)
        .flat_map(|from| {
            (1..=3)
                .filter(move |to| *to != from)
                .map(move |to| format!("peer-{from}-{to}"))
        })
        .chain((1..=3).map(|id| format!("sdk-{id}")));
    names
        .map(|name| {
            let mut events = vec![
                Event {
                    sequence: 0,
                    micros: 0,
                    connection: 1,
                    direction: None,
                    kind: "accepted".into(),
                    bytes: 0,
                    fault: None,
                },
                Event {
                    sequence: 1,
                    micros: 1,
                    connection: 1,
                    direction: None,
                    kind: "closed".into(),
                    bytes: 0,
                    fault: None,
                },
            ];
            for d in [Direction::Upstream, Direction::Downstream] {
                if name.starts_with("peer-") || name == "sdk-3" {
                    events.push(Event {
                        sequence: 0,
                        micros: 2,
                        connection: 0,
                        direction: Some(d),
                        kind: "control".into(),
                        bytes: 0,
                        fault: Some(Fault::Disconnect),
                    });
                }
                events.push(Event {
                    sequence: 0,
                    micros: 2,
                    connection: 0,
                    direction: Some(d),
                    kind: "control".into(),
                    bytes: 0,
                    fault: Some(Fault::Delay { millis: 15 }),
                });
                events.push(Event {
                    sequence: 0,
                    micros: 2,
                    connection: 0,
                    direction: Some(d),
                    kind: "control".into(),
                    bytes: 0,
                    fault: Some(Fault::Pass),
                });
            }
            events.push(Event {
                sequence: 0,
                micros: 3,
                connection: 0,
                direction: None,
                kind: "stopped".into(),
                bytes: 0,
                fault: None,
            });
            for (n, e) in events.iter_mut().enumerate() {
                e.sequence = n as u64;
            }
            (
                name,
                Trace {
                    events,
                    overflow: false,
                },
            )
        })
        .collect()
}
fn fixture() -> Report {
    let mut invocations = Vec::new();
    let mut correlation = 1;
    let mut receipts = Vec::new();
    for (n, phase) in [
        Phase::Healthy,
        Phase::DomainLoss,
        Phase::NoQuorum,
        Phase::Restored,
    ]
    .into_iter()
    .enumerate()
    {
        let business = n as u64 + 1;
        let request = Request {
            operation: OperationId(business),
            selection: Selection::Keys(vec![Key {
                tenant: TenantId(42),
                row: RowId(business),
            }]),
            column: Column::Status,
            after: Status::Queued,
        };
        let before = Row {
            status: Status::Failed,
            failed_at: 103,
            attempt_count: 0,
            version: 0,
        };
        let after = Row {
            status: Status::Queued,
            version: 1,
            ..before.clone()
        };
        let effects = vec![Effect {
            key: Key {
                tenant: TenantId(42),
                row: RowId(business),
            },
            before,
            after,
        }];
        let digest = Sha256::digest(serde_json::to_vec(&effects).unwrap())
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        let receipt = Receipt {
            domain: 42,
            task: TaskId(1),
            operation: OperationId(business),
            payload: request.canonical(),
            issuer: Principal(1),
            executor: Principal(2),
            approver: None,
            schema: 0,
            decided_at: 1000,
            effects,
            charged_transitions: 1,
            lifetime_transitions: business,
            lifetime_distinct: business,
        };
        receipts.push(json!([[1,business],{"decision":{"Ok":receipt}}]));
        let op = Operation::execute(correlation, request, None, 1500).unwrap();
        let response = |id| Observation::Recorded {
            sdk_validated_correlation_id: id,
            reply: json!({"version":1,"code":0,"data":{"version":1,"code":"committed","operation":business,"count":1,"digest":digest,"sql":null}}),
        };
        invocations.push(Invocation {
            phase,
            action: Action::Execute,
            correlation_id: correlation,
            business_id: business,
            start_micros: correlation * 100,
            end_micros: correlation * 100 + 10,
            request: serde_json::from_slice(op.wire_bytes()).unwrap(),
            outcome: if phase == Phase::NoQuorum {
                Observation::Unknown {
                    business_id: business,
                    reason: "Timeout".into(),
                }
            } else {
                response(correlation)
            },
        });
        correlation += 1;
        let status = Operation::status(correlation, OperationId(business), 1500).unwrap();
        invocations.push(Invocation {
            phase: Phase::Restored,
            action: Action::Status,
            correlation_id: correlation,
            business_id: business,
            start_micros: 10000 + correlation * 100,
            end_micros: 10000 + correlation * 100 + 10,
            request: serde_json::from_slice(status.wire_bytes()).unwrap(),
            outcome: response(correlation),
        });
        correlation += 1;
    }
    Report {
        version: 1,
        scope: LOCAL_SCOPE.into(),
        node_binary_sha256: "a".repeat(64),
        host_os: "windows".into(),
        host_arch: "x86_64".into(),
        samples_per_phase: 1,
        domains: (1..=3)
            .map(|i| (i, format!("LOCAL-provider-{i}")))
            .collect(),
        isolated_node: 3,
        initial_leader: 1,
        phase_nodes: [
            (Phase::Healthy, 1),
            (Phase::DomainLoss, 1),
            (Phase::NoQuorum, 1),
            (Phase::Restored, 1),
        ]
        .into_iter()
        .collect(),
        summary: summarize(&invocations),
        invocations,
        traces: traces(),
        final_inspection: json!({"reply":{"data":{"tasks":[[1,{"transitions":4}]],"operations":receipts}}}),
        catch_up: (1..=3)
            .map(|node| CatchUp {
                node,
                applied: 10,
                diagnostics: json!({}),
            })
            .collect(),
        barrier: 10,
    }
}
#[test]
fn healthy_summary() {
    fixture().validate().unwrap();
}
#[test]
fn forged_success_rejected() {
    let mut r = fixture();
    r.summary.committed_responses += 1;
    assert!(r.validate().is_err());
}
#[test]
fn unknown_not_counted_as_failure() {
    let r = fixture();
    assert_eq!(r.summary.unknown_responses, 1);
    assert_eq!(r.summary.other_responses, 0);
    r.validate().unwrap();
}
#[test]
fn missing_status_rejected() {
    let mut r = fixture();
    r.invocations
        .retain(|i| !(i.action == Action::Status && i.business_id == 3));
    r.summary = summarize(&r.invocations);
    assert!(r.validate().is_err());
}
#[test]
fn wrong_business_id_rejected() {
    let mut r = fixture();
    r.invocations[0].business_id = 99;
    r.summary = summarize(&r.invocations);
    assert!(r.validate().is_err());
}
#[test]
fn duplicate_correlation_rejected() {
    let mut r = fixture();
    r.invocations[2].correlation_id = r.invocations[0].correlation_id;
    assert!(r.validate().is_err());
}
#[test]
fn altered_effect_count_rejected() {
    let mut r = fixture();
    r.final_inspection["reply"]["data"]["tasks"][0][1]["transitions"] = json!(5);
    assert!(r.validate().is_err());
}
#[test]
fn noquorum_commit_rejected() {
    let mut r = fixture();
    let i = r
        .invocations
        .iter_mut()
        .find(|i| i.phase == Phase::NoQuorum)
        .unwrap();
    i.outcome = Observation::Recorded {
        sdk_validated_correlation_id: i.correlation_id,
        reply: json!({"id":i.correlation_id,"code":0,"data":{"version":1,"code":"committed","operation":i.business_id,"count":1,"digest":"a".repeat(64),"sql":null}}),
    };
    r.summary = summarize(&r.invocations);
    assert!(r.validate().is_err());
}
#[test]
fn truncated_trace_rejected() {
    let mut r = fixture();
    r.traces.values_mut().next().unwrap().overflow = true;
    assert!(r.validate().is_err());
}
#[test]
fn percentile_boundaries() {
    assert!(distribution(&[]).is_none());
    assert_eq!(distribution(&[7]).unwrap().p99, 7);
    let d = distribution(&(1..=100).collect::<Vec<_>>()).unwrap();
    assert_eq!((d.p50, d.p95, d.p99), (50, 95, 99));
}
#[test]
fn distributions_include_every_response() {
    for n in 1..=256 {
        let samples = (0..n).map(|i| (i * 7919 % 997) as u64).collect::<Vec<_>>();
        let d = distribution(&samples).unwrap();
        assert_eq!(d.samples, n);
        assert!(d.min <= d.p50 && d.p50 <= d.p95 && d.p95 <= d.p99 && d.p99 <= d.max);
    }
}
#[test]
fn forged_fields_property() {
    for field in ["version", "operation", "count", "digest"] {
        let mut r = fixture();
        if let Observation::Recorded { reply, .. } = &mut r.invocations[0].outcome {
            reply["data"][field] = json!(999);
        }
        r.summary = summarize(&r.invocations);
        assert!(r.validate().is_err(), "accepted forged {field}");
    }
}
#[test]
fn changed_status_digest_rejected() {
    let mut r = fixture();
    if let Observation::Recorded { reply, .. } = &mut r.invocations[1].outcome {
        reply["data"]["digest"] = json!("b".repeat(64));
    }
    assert!(r.validate().is_err());
}
#[test]
fn unresolved_unknown_rejected() {
    let mut r = fixture();
    let status = r
        .invocations
        .iter_mut()
        .find(|i| i.business_id == 3 && i.action == Action::Status)
        .unwrap();
    status.outcome = Observation::Unavailable;
    r.summary = summarize(&r.invocations);
    assert!(r.validate().is_err());
}
#[test]
fn catchup_missing_or_stale_rejected() {
    for mode in 0..3 {
        let mut r = fixture();
        match mode {
            0 => {
                r.catch_up.pop();
            }
            1 => r.catch_up[0].applied = 9,
            _ => r.catch_up[0].node = 2,
        };
        assert!(r.validate().is_err());
    }
}
#[test]
fn local_scope_required() {
    let mut r = fixture();
    r.scope = "cloud qualified".into();
    assert!(r.validate().is_err());
}
#[test]
fn backwards_timing_rejected() {
    let mut r = fixture();
    r.invocations[0].end_micros = 0;
    assert!(r.validate().is_err());
}
#[test]
fn missing_phase_rejected() {
    let mut r = fixture();
    r.invocations.retain(|i| i.phase != Phase::DomainLoss);
    r.summary = summarize(&r.invocations);
    assert!(r.validate().is_err());
}
#[test]
fn unknown_id_mismatch_rejected() {
    let mut r = fixture();
    r.invocations
        .iter_mut()
        .find(|i| i.phase == Phase::NoQuorum)
        .unwrap()
        .outcome = Observation::Unknown {
        business_id: 999,
        reason: "x".into(),
    };
    assert!(r.validate().is_err());
}
#[test]
fn trace_sequence_and_time_rejected() {
    for mode in 0..2 {
        let mut r = fixture();
        let t = r.traces.values_mut().next().unwrap();
        if mode == 0 {
            t.events[0].sequence = 1;
        } else {
            let mut e = t.events[0].clone();
            t.events[0].micros = 5;
            e.sequence = 1;
            e.micros = 0;
            t.events.push(e);
        }
        assert!(r.validate().is_err());
    }
}
#[test]
fn serde_roundtrip_retains_raw_evidence() {
    let r = fixture();
    let bytes = serde_json::to_vec(&r).unwrap();
    let copy: Report = serde_json::from_slice(&bytes).unwrap();
    copy.validate().unwrap();
    assert_eq!(copy.summary, r.summary);
}
#[test]
fn response_id_mismatch_rejected() {
    let mut r = fixture();
    if let Observation::Recorded {
        sdk_validated_correlation_id,
        ..
    } = &mut r.invocations[0].outcome
    {
        *sdk_validated_correlation_id = 999;
    }
    assert!(r.validate().is_err());
}

#[test]
fn altered_receipt_digest_rejected() {
    let mut r = fixture();
    r.final_inspection["reply"]["data"]["operations"][0][1]["decision"]["Ok"]["effects"][0]["after"]
        ["attempt_count"] = json!(99);
    assert!(r.validate().is_err());
}
#[test]
fn missing_receipt_rejected() {
    let mut r = fixture();
    r.final_inspection["reply"]["data"]["operations"]
        .as_array_mut()
        .unwrap()
        .pop();
    assert!(r.validate().is_err());
}
#[test]
fn receipt_payload_mismatch_rejected() {
    let mut r = fixture();
    r.final_inspection["reply"]["data"]["operations"][0][1]["decision"]["Ok"]["payload"] =
        json!([]);
    assert!(r.validate().is_err());
}
#[test]
fn receipt_version_overflow_returns_error() {
    let mut r = fixture();
    r.final_inspection["reply"]["data"]["operations"][0][1]["decision"]["Ok"]["effects"][0]["before"]
        ["version"] = json!(u64::MAX);
    assert!(r.validate().is_err());
}
#[test]
fn missing_fault_records_rejected() {
    let mut r = fixture();
    for t in r.traces.values_mut() {
        t.events.retain(|e| e.fault != Some(Fault::Disconnect));
        for (n, e) in t.events.iter_mut().enumerate() {
            e.sequence = n as u64;
        }
    }
    assert!(r.validate().is_err());
}
#[test]
fn incorrect_phase_routing_rejected() {
    let mut r = fixture();
    r.phase_nodes.insert(Phase::DomainLoss, r.isolated_node);
    assert!(r.validate().is_err());
}
#[test]
fn unfinished_proxy_trace_rejected() {
    let mut r = fixture();
    r.traces.values_mut().next().unwrap().events.pop();
    assert!(r.validate().is_err());
}
#[test]
fn orphan_proxy_terminal_rejected() {
    let mut r = fixture();
    r.traces.values_mut().next().unwrap().events[1].connection = 999;
    assert!(r.validate().is_err());
}
#[test]
fn forged_proxy_forward_count_rejected() {
    let mut r = fixture();
    let e = &mut r.traces.values_mut().next().unwrap().events[1];
    e.kind = "forwarded".into();
    e.direction = Some(Direction::Upstream);
    e.bytes = 1;
    assert!(r.validate().is_err());
}
