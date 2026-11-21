#![cfg(feature = "engine-rocksdb")]
//! Live size splits in real server processes, with SQL routed through multiple gateways.

use crate::cluster_harness::ClusterProcs;
use std::{path::Path, process::Command, time::Duration};
use tokio_postgres::{Client, Row, types::ToSql};

const BATCH_ROWS: i64 = 16;

fn range_count(config: &Path) -> usize {
    let output = Command::new(env!("CARGO_BIN_EXE_giggles-server"))
        .args(["cluster", "status"])
        .arg(config)
        .arg("--json")
        .output()
        .unwrap();
    assert!(output.status.success(), "cluster status: {output:?}");
    let status: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    status["ranges"].as_array().unwrap().len()
}

async fn query(client: &Client, sql: &str) -> Row {
    loop {
        match client.query_one(sql, &[]).await {
            Ok(row) => return row,
            Err(e) if transient(&e) => tokio::time::sleep(Duration::from_millis(50)).await,
            Err(e) => panic!("{sql}: {e:?}"),
        }
    }
}

fn transient(error: &tokio_postgres::Error) -> bool {
    error
        .code()
        .is_some_and(|code| matches!(code.code(), "08007" | "40001"))
}

async fn insert_batch(client: &Client, observer: &Client, committed: &mut i64) {
    // One statement is one atomic batch. Resolve an uncertain commit through node 3 before
    // advancing the count; retries use the same primary keys and cannot duplicate a batch.
    // The size API includes unflushed writes. Use varied 48 KiB text to exercise size splits,
    // staying below the 64 KiB row and 1 MiB batch budgets.
    let payloads: Vec<String> = (*committed..*committed + BATCH_ROWS)
        .map(|id| {
            let mut state = id as u64 + 1;
            (0..48 * 1024)
                .map(|_| {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    char::from(b'a' + (state % 26) as u8)
                })
                .collect()
        })
        .collect();
    let params: Vec<&(dyn ToSql + Sync)> = payloads
        .iter()
        .map(|payload| payload as &(dyn ToSql + Sync))
        .collect();
    let values = (*committed..*committed + BATCH_ROWS)
        .enumerate()
        .map(|(i, id)| format!("({id}, ${})", i + 1))
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!("INSERT INTO split_load VALUES {values}");
    let count = format!(
        "SELECT count(*) FROM split_load WHERE id >= {} AND id < {}",
        *committed,
        *committed + BATCH_ROWS
    );
    let mut retrying = false;
    loop {
        match client.execute(&sql, &params).await {
            Ok(inserted) => {
                assert_eq!(inserted, BATCH_ROWS as u64);
                break;
            }
            Err(e)
                if transient(&e) || (retrying && e.code().is_some_and(|c| c.code() == "23505")) =>
            {
                eprintln!("resolving batch at {} after {e:?}", *committed);
                retrying = true;
                let count: i64 = query(observer, &count).await.get(0);
                assert!(count == 0 || count == BATCH_ROWS, "partial batch: {count}");
                if count == BATCH_ROWS {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(e) => panic!("insert batch at {}: {e:?}", *committed),
        }
    }
    *committed += BATCH_ROWS;
}

async fn assert_counts(client: &Client, committed: i64) {
    let row = query(
        client,
        "SELECT count(*), count(DISTINCT id) FROM split_load",
    )
    .await;
    assert_eq!(row.get::<_, i64>(0), committed);
    assert_eq!(row.get::<_, i64>(1), committed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_splits_preserve_rows_and_accept_more_inserts() {
    let started = std::time::Instant::now();
    let mut cluster = ClusterProcs::new(3).await;
    // The harness already exposes config paths. Reuse it unchanged, restarting all nodes
    // with the policy before the workload rather than adding a configuration hook.
    for i in 0..3 {
        cluster.kill(i);
        let path = cluster.config_path(i);
        let mut config: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        config["cluster"]["split_size_bytes"] = 65536.into();
        config["cluster"]["split_interval_ms"] = 200.into();
        config["cluster"]["split_cooldown_ms"] = 1000.into();
        std::fs::write(path, serde_json::to_vec(&config).unwrap()).unwrap();
    }
    for i in 0..3 {
        cluster.start(i);
    }
    for i in 0..3 {
        cluster.ready(i).await;
    }
    let before = range_count(&cluster.config_path(0));
    let c1 = cluster.client(0).await;
    let c2 = cluster.client(1).await;
    let c3 = cluster.client(2).await;
    let mut committed = 0;
    let mut after = before;
    tokio::time::timeout(Duration::from_secs(120), async {
        c1.batch_execute("CREATE TABLE split_load (id BIGINT PRIMARY KEY, payload TEXT NOT NULL)")
            .await
            .unwrap();
        loop {
            insert_batch(&c1, &c3, &mut committed).await;
            insert_batch(&c2, &c3, &mut committed).await;
            after = range_count(&cluster.config_path(0));
            if after >= before + 3 {
                break;
            }
        }
        assert_counts(&c3, committed).await;
        insert_batch(&c1, &c3, &mut committed).await;
        insert_batch(&c2, &c3, &mut committed).await;
        assert_counts(&c3, committed).await;
    })
    .await
    .unwrap_or_else(|_| {
        panic!("split workload timed out: {before} -> {after} ranges, {committed} rows")
    });
    eprintln!(
        "live size splits: {before} -> {after} ranges, {committed} rows, {:?}",
        started.elapsed()
    );
}
