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
