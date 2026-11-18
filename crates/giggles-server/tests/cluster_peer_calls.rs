#![cfg(feature = "engine-rocksdb")]
//! Real-process peer-call isolation and failover (GD-0075 HD-1b).
use crate::cluster_harness::ClusterProcs;
use std::{
    collections::BTreeSet,
    time::{Duration, Instant},
};
use tokio_postgres::{Client, Error};

const WAIT: Duration = Duration::from_secs(45);
const POLL: Duration = Duration::from_millis(200);

async fn batch(c: &Client, sql: &str) {
    tokio::time::timeout(WAIT, c.batch_execute(sql))
        .await
        .unwrap()
        .unwrap();
}

fn code(e: &Error) -> String {
    e.code()
        .map_or_else(|| format!("transport: {e}"), |c| c.code().to_owned())
}

async fn status(cluster: &ClusterProcs, via: usize) -> serde_json::Value {
    let config = cluster.config_path(via);
    tokio::task::spawn_blocking(move || {
        let output = std::process::Command::new(env!("CARGO_BIN_EXE_giggles-server"))
            .args(["cluster", "status"])
            .arg(config)
            .arg("--json")
            .output()
            .unwrap();
        assert!(output.status.success(), "{:?}", output);
        serde_json::from_slice(&output.stdout).unwrap()
    })
    .await
    .unwrap()
}

async fn row_leader(cluster: &ClusterProcs, via: usize, table: i64, avoid: u64) -> u64 {
    let key = format!("80{table:016x}");
    let until = Instant::now() + WAIT;
    loop {
        let s = status(cluster, via).await;
        let leader = s["ranges"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| {
                r["start"].as_str().is_some_and(|v| v <= key.as_str())
                    && r["end"].as_str().is_none_or(|v| key.as_str() < v)
            })
            .and_then(|r| r["leader"].as_u64());
        if let Some(n) = leader.filter(|n| *n != avoid) {
            return n;
        }
        assert!(Instant::now() < until, "no live leader for {key}: {s}");
        tokio::time::sleep(POLL).await;
    }
}

/// Unrelated reads and writes keep succeeding on the same gateway while a read
/// waits on a prepared record, using the default peer-call deadline.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_slow_peer_call_does_not_fail_the_others() {
    let started = Instant::now();
    let mut cluster = ClusterProcs::new(3).await;
    let c = cluster.client(0).await;
    batch(&c, "CREATE TABLE u (id BIGINT PRIMARY KEY, v BIGINT)").await;
    batch(&c, "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)").await;
    batch(&c, "INSERT INTO u VALUES (1, 10), (15, 150)").await;
    let oid: i64 = c
        .query_one("SELECT oid::bigint FROM pg_class WHERE relname = 'u'", &[])
        .await
        .unwrap()
        .get(0);
    let table = (oid - 65536) / 4;
    let leader = row_leader(&cluster, 0, table, 0).await;
    let g = (1..3).find(|n| *n as u64 + 1 != leader).unwrap();
    cluster.kill(g);
    cluster.set_env(g, "GIGGLES_CORE_FAILPOINT", "gateway.commit.prepared");
    cluster.start(g);
    cluster.ready(g).await;
    let crash = cluster.client(g).await;
    batch(&crash, "BEGIN").await;
    batch(&crash, "UPDATE u SET v = -1 WHERE id = 15").await;
    batch(&crash, "INSERT INTO t VALUES (1, 1)").await;
    assert!(!matches!(
        tokio::time::timeout(WAIT, crash.batch_execute("COMMIT")).await,
        Ok(Ok(()))
    ));
    cluster.exited(g, WAIT).await;
    assert!(
        cluster
            .stderr(g)
            .contains("failpoint crash: gateway.commit.prepared")
    );
    let gateway = (0..3).find(|n| *n != g && *n as u64 + 1 != leader).unwrap();
    // Separate SQL sessions on the SAME gateway, sharing its single peer connection.
    let fast = cluster.client(gateway).await;
    let slow = cluster.client(gateway).await;
    let held = tokio::spawn(async move {
        let at = Instant::now();
        let result = slow.query("SELECT v FROM u WHERE id = 15", &[]).await;
        (
            at.elapsed(),
            result
                .map(|r| r[0].get::<_, i64>(0))
                .map_err(|e| (code(&e), format!("{e:?}"))),
        )
    });
    let until = Instant::now() + Duration::from_secs(18);
    let mut max = Duration::ZERO;
    let mut statements = 0_u64;
    while Instant::now() < until || !held.is_finished() {
        assert!(
            started.elapsed() < Duration::from_secs(90),
            "slow read never finished"
        );
        let at = Instant::now();
        let row = tokio::time::timeout(
            Duration::from_secs(2),
            fast.query_one("SELECT v FROM u WHERE id = 1", &[]),
        )
        .await
        .expect("unrelated read stalled")
        .expect("unrelated read failed");
        assert_eq!(row.get::<_, i64>(0), 10);
        max = max.max(at.elapsed());
        statements += 1;
        if statements.is_multiple_of(5) {
            let at = Instant::now();
            let sql = format!("INSERT INTO u VALUES ({}, 1)", 1000 + statements);
            tokio::time::timeout(Duration::from_secs(2), fast.batch_execute(&sql))
                .await
                .expect("unrelated write stalled")
                .expect("unrelated write failed");
            max = max.max(at.elapsed());
        }
        tokio::time::sleep(POLL).await;
    }
    let (waited, outcome) = held.await.unwrap();
    assert!(
        waited >= Duration::from_secs(2),
        "prepared row was not blocked: {outcome:?}"
    );
    if let Err((ref error, _)) = outcome {
        assert!(
            ["40001", "55P03", "57014", "53300"].contains(&error.as_str()),
            "{outcome:?}"
        );
    }
    eprintln!(
        "peer isolation: gateway {}, leader {leader}, {statements} reads + writes, max {max:?}, slow {waited:?} {outcome:?}, total {:?}",
        gateway + 1,
        started.elapsed()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dead_node_fails_over_by_liveness() {
    let started = Instant::now();
    let mut cluster = ClusterProcs::new(3).await;
    let c = cluster.client(0).await;
    // Each table's row range is separate in the harness's presplit layout.
    for t in 1..=8 {
        batch(
            &c,
            &format!("CREATE TABLE p{t} (id BIGINT PRIMARY KEY, v BIGINT)"),
        )
        .await;
        batch(&c, &format!("INSERT INTO p{t} VALUES (1, 10)")).await;
    }
    // Startup order can leave node 3 with no leaders. Make the bootstrap leader
    // step down first, so the kill below actually exercises leader failover.
    cluster.kill(0);
    let until = Instant::now() + WAIT;
    loop {
        let s = status(&cluster, 1).await;
        let leads_rows = s["ranges"].as_array().unwrap().iter().any(|r| {
            r["leader"] == 3
                && r["start"]
                    .as_str()
                    .is_some_and(|v| v.starts_with("80") && v.len() == 18)
        });
        if leads_rows {
            break;
        }
        assert!(Instant::now() < until, "node 3 never led a row range: {s}");
        tokio::time::sleep(POLL).await;
    }
    cluster.restart(0).await;
    // Reconnect after the setup restart; this session stays open across node 3's kill.
    let c = cluster.client(0).await;
    let mut codes = BTreeSet::new();
    let mut longest = Duration::ZERO;
    let mut last_success = Instant::now();
    let mut killed = None;
    let mut resumed = None;
    let mut successes = 0;
    // Include each table's setup row. A failed insert with 08007 may have committed.
    let mut acked = [1_i64; 8];
    let mut unknown = [0_i64; 8];
    let mut failed_inserts = 0;
    for round in 0..30 {
        if round == 3 {
            cluster.kill(2);
            let now = Instant::now();
            killed = Some(now);
            last_success = now;
        }
        for t in 1..=8 {
            for (is_insert, sql) in [
                (false, format!("SELECT v FROM p{t} WHERE id = 1")),
                (
                    true,
                    format!("INSERT INTO p{t} VALUES ({}, 1)", round + 100),
                ),
            ] {
                let result = tokio::time::timeout(Duration::from_secs(10), c.simple_query(&sql))
                    .await
                    .expect("statement did not fail over within 10 s");
                match result {
                    Ok(_) => {
                        if killed.is_some() {
                            longest = longest.max(last_success.elapsed());
                        }
                        if is_insert {
                            acked[t - 1] += 1;
                        }
                        last_success = Instant::now();
                        successes += 1;
                        if let Some(kill) = killed {
                            assert!(resumed.is_some() || kill.elapsed() < Duration::from_secs(10));
                            resumed.get_or_insert_with(Instant::now);
                        }
                    }
                    Err(e) => {
                        let state = code(&e);
                        assert!(
                            ["08007", "40001", "57P01", "57P02", "57P03"].contains(&state.as_str()),
                            "unexpected failure: {e:?}"
                        );
                        if is_insert {
                            failed_inserts += 1;
                            if state == "08007" {
                                unknown[t - 1] += 1;
                            }
                        }
                        codes.insert(state);
                        assert!(
                            last_success.elapsed() <= Duration::from_secs(10),
                            "no successful statement within 10 s"
                        );
                    }
                }
            }
        }
        tokio::time::sleep(POLL).await;
    }
    longest = longest.max(last_success.elapsed());
    assert!(resumed.is_some());
    assert!(
        longest <= Duration::from_secs(10),
        "longest gap {longest:?}"
    );
    cluster.restart(2).await;
    let back = cluster.client(2).await;
    for t in 1..=8 {
        let sql = format!("SELECT count(*) FROM p{t}");
        let row = tokio::time::timeout(WAIT, back.query_one(&sql, &[]))
            .await
            .unwrap()
            .unwrap();
        let count = row.get::<_, i64>(0);
        assert!(
            (acked[t - 1]..=acked[t - 1] + unknown[t - 1]).contains(&count),
            "p{t}: count {count}, acknowledged {}, unknown {}",
            acked[t - 1],
            unknown[t - 1]
        );
    }
    eprintln!(
        "dead failover: {successes} successful statements, acknowledged rows {acked:?}, unknown inserts {unknown:?}, failed inserts {failed_inserts}, longest gap {longest:?}, codes {codes:?}, total {:?}",
        started.elapsed()
    );
}
