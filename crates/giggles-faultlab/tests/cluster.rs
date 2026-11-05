use giggles_faultlab::{lab, report::*};
use std::path::PathBuf;
fn node() -> PathBuf {
    std::env::var_os("GIGGLES_NODE_EXE")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../target-faultlab/debug/giggles-node.exe")
        })
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_three_process_faults_and_reconciliation() {
    let path = lab::run(&node(), 2).await.unwrap();
    eprintln!("cluster report: {}", path.display());
    let report = lab::check(&path).unwrap();
    assert_eq!(report.initial_leader, report.isolated_node);
    assert_ne!(
        report.phase_nodes[&Phase::DomainLoss],
        report.initial_leader
    );
    assert_eq!(
        report
            .invocations
            .iter()
            .filter(|i| i.action == Action::Execute)
            .count(),
        8
    );
    assert!(
        report
            .invocations
            .iter()
            .filter(|i| i.phase == Phase::NoQuorum)
            .all(|i| !i.outcome.committed())
    );
    assert_eq!(
        report.final_inspection["reply"]["data"]["tasks"][0][1]["transitions"],
        8
    );
    // Independent-review regressions against genuine shared-clock evidence.
    for mutation in 0..5 {
        let mut forged = report.clone();
        match mutation {
            0 => {
                forged.final_inspection["reply"]["data"]["rows"][0][1]["status"] =
                    serde_json::json!("Failed")
            }
            1 => {
                forged.catch_up[0].diagnostics["diagnostics"] = serde_json::json!("id=1 applied=0")
            }
            2 => {
                for trace in forged.traces.values_mut() {
                    let end = trace.events.last().unwrap().micros + 1;
                    for event in &mut trace.events {
                        if event.kind == "control" {
                            event.micros = end;
                        }
                        if event.kind == "stopped" {
                            event.micros = end + 1;
                        }
                    }
                    trace.events.sort_by_key(|e| e.micros);
                    for (n, e) in trace.events.iter_mut().enumerate() {
                        e.sequence = n as u64;
                    }
                }
            }
            3 => {
                for call in &mut forged.invocations {
                    if let Observation::Recorded { reply, .. } = &mut call.outcome {
                        reply["version"] = serde_json::json!(999);
                    }
                }
            }
            _ => {
                forged.invocations.retain(|i| i.action != Action::Replay);
                forged.summary = summarize(&forged.invocations);
            }
        }
        assert!(
            forged.validate().is_err(),
            "accepted review mutation {mutation}"
        );
    }
    // Negative controls on the actual report, preserving its raw observations.
    let mut forged = report.clone();
    forged.summary.committed_responses += 1;
    assert!(forged.validate().is_err());
    let mut forged = report.clone();
    forged.final_inspection["reply"]["data"]["tasks"][0][1]["transitions"] = serde_json::json!(9);
    assert!(forged.validate().is_err());
    let mut forged = report.clone();
    forged.invocations.retain(|i| i.action != Action::Status);
    forged.summary = summarize(&forged.invocations);
    assert!(forged.validate().is_err());
    let mut forged = report;
    forged.traces.values_mut().next().unwrap().overflow = true;
    assert!(forged.validate().is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follower_domain_loss_preserves_serving_leader() {
    let path = lab::run_with_loss(&node(), 1, false).await.unwrap();
    let report = lab::check(&path).unwrap();
    assert_ne!(report.initial_leader, report.isolated_node);
    assert_eq!(
        report.phase_nodes[&Phase::DomainLoss],
        report.initial_leader
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn maximum_samples_preserve_complete_trace_and_receipts() {
    let path = lab::run(&node(), 8).await.unwrap();
    eprintln!("maximum report: {}", path.display());
    let report = lab::check(&path).unwrap();
    assert_eq!(report.invocations.len(), 96);
    assert_eq!(
        report.final_inspection["reply"]["data"]["tasks"][0][1]["transitions"],
        32
    );
    assert!(
        report
            .traces
            .values()
            .all(|t| !t.overflow && t.events.len() <= 16384)
    );
    assert!(
        report.traces.values().any(|t| t.events.len() > 4096),
        "regression must exercise the old insufficient event cap"
    );
}
