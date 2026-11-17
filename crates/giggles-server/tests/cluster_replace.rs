#![cfg(feature = "engine-rocksdb")]
//! Replacement with a genuinely new process, using only public operator commands.
use crate::cluster_harness::ClusterProcs;
use std::{process::Output, time::Duration};
use tokio::process::Command;

async fn cli(cluster: &ClusterProcs, args: &[&str]) -> Output {
    cli_node(cluster, 0, args).await
}
async fn cli_node(cluster: &ClusterProcs, node: usize, args: &[&str]) -> Output {
    let mut c = Command::new(env!("CARGO_BIN_EXE_giggles-server"));
    c.args(&args[..2])
        .arg(cluster.config_path(node))
        .args(&args[2..])
        .kill_on_drop(true);
    tokio::time::timeout(Duration::from_secs(120), c.output())
        .await
        .expect("operator timed out")
        .unwrap()
}
fn succeeded(out: &Output) {
    assert!(
        out.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_node_replaced_by_enrolled_spare() {
    tokio::time::timeout(Duration::from_secs(175), replacement())
        .await
        .expect("replacement exceeded 175 seconds");
}

async fn replacement() {
    let mut cluster = ClusterProcs::with_spare().await;
    let client = cluster.client(0).await;
    client
        .batch_execute("CREATE TABLE t (id BIGINT PRIMARY KEY); INSERT INTO t VALUES (1), (2), (3)")
        .await
        .unwrap();
    cluster.start(3);
    let (address, fingerprint) = cluster.spare_identity();
    succeeded(&cli(&cluster, &["cluster", "enrol", "4", &address, &fingerprint]).await);
    cluster.ready(3).await;
    let before = cli_node(&cluster, 3, &["range", "status"]).await;
    succeeded(&before);
    assert!(!String::from_utf8_lossy(&before.stdout).contains(",4"));
    cluster.kill(2);
    let replaced = cli(
        &cluster,
        &["range", "replace-node", "3", "4", "--timeout", "60"],
    )
    .await;
    succeeded(&replaced);
    let status = cli(&cluster, &["range", "status"]).await;
    succeeded(&status);
    let text = String::from_utf8(status.stdout).unwrap();
    let lines: Vec<_> = text.lines().collect();
    assert!(lines.len() > 2, "expected pre-split ranges: {text}");
    for line in lines {
        assert!(line.contains("voters=[1,2,4]"), "{line}");
        assert!(line.contains("learners=[]"), "{line}");
        assert!(!line.contains("leader=none"), "{line}");
    }
    for (i, id) in [(0, 4_i64), (3, 5)] {
        let gateway = cluster.client(i).await;
        let rows = gateway
            .query("SELECT id FROM t ORDER BY id", &[])
            .await
            .unwrap();
        assert_eq!(
            rows.iter().map(|r| r.get::<_, i64>(0)).collect::<Vec<_>>(),
            (1..id).collect::<Vec<_>>()
        );
        gateway
            .execute("INSERT INTO t VALUES ($1)", &[&id])
            .await
            .unwrap();
    }
    assert_eq!(
        client
            .query("SELECT id FROM t ORDER BY id", &[])
            .await
            .unwrap()
            .len(),
        5
    );
}
