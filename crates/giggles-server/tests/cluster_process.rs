#![cfg(feature = "engine-rocksdb")]
//! Real cluster processes (GD-0075 R5-63): every node of a pre-split cluster is a SQL gateway.
//! LONG: each test starts its own 3-node cluster of server processes (tens of seconds).

use crate::{
    cluster_harness::{ClusterProcs, connect},
    replication_harness::Relay,
};
use std::{
    collections::BTreeSet,
    time::{Duration, Instant},
};
use tokio_postgres::{Client, types::ToSql};

/// One statement must answer within this, including a leader change behind it. Well above
/// the cluster runtime's 5 s range call timeout, so a call black-holed by a partition fails
/// over inside the statement.
const STATEMENT_WAIT: Duration = Duration::from_secs(45);
/// How long a healed node may take to answer with the rows written while it was cut off.
const CONVERGE_WAIT: Duration = Duration::from_secs(60);
/// How long another gateway may answer 'in progress' for a transaction whose gateway died.
const STATUS_WAIT: Duration = Duration::from_secs(60);

async fn exec(c: &Client, sql: &str, params: &[&(dyn ToSql + Sync)]) -> u64 {
    tokio::time::timeout(STATEMENT_WAIT, c.execute(sql, params))
        .await
        .unwrap_or_else(|_| panic!("{sql}: no answer within {STATEMENT_WAIT:?}"))
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
}

async fn batch(c: &Client, sql: &str) {
    tokio::time::timeout(STATEMENT_WAIT, c.batch_execute(sql))
        .await
        .unwrap_or_else(|_| panic!("{sql}: no answer within {STATEMENT_WAIT:?}"))
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
}

/// Every row of a query whose columns are all bigint.
async fn rows(c: &Client, sql: &str) -> Vec<Vec<i64>> {
    let rows = tokio::time::timeout(STATEMENT_WAIT, c.query(sql, &[]))
        .await
        .unwrap_or_else(|_| panic!("{sql}: no answer within {STATEMENT_WAIT:?}"))
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    rows.iter()
        .map(|r| (0..r.len()).map(|i| r.get::<_, i64>(i)).collect())
        .collect()
}

async fn ids(c: &Client) -> Vec<i64> {
    rows(c, "SELECT id FROM t ORDER BY id")
        .await
        .into_iter()
        .map(|r| r[0])
        .collect()
}

/// Two tables whose rows sit in different ranges; t takes ids from a sequence.
async fn create_tables(c: &Client) {
    batch(
        c,
        "CREATE TABLE t (id BIGSERIAL PRIMARY KEY, node BIGINT NOT NULL)",
    )
    .await;
    batch(
        c,
        "CREATE TABLE u (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)",
    )
    .await;
}

async fn insert_t(c: &Client, node: i64) -> i64 {
    let row = tokio::time::timeout(
        STATEMENT_WAIT,
        c.query_one("INSERT INTO t (node) VALUES ($1) RETURNING id", &[&node]),
    )
    .await
    .unwrap_or_else(|_| panic!("INSERT INTO t on node {node}: no answer"))
    .unwrap_or_else(|e| panic!("INSERT INTO t on node {node}: {e:?}"));
    row.get(0)
}

async fn insert_u(c: &Client, id: i64, v: i64) {
    assert_eq!(
        exec(c, "INSERT INTO u (id, v) VALUES ($1, $2)", &[&id, &v]).await,
        1
    );
}

/// The plan's acceptance: DDL on one node, writes on every node, every node reads every row;
/// a transaction spanning both tables commits or rolls back as one; sequence values are unique.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pre_split_cluster_answers_on_every_node() {
    let started = Instant::now();
    let cluster = ClusterProcs::new(3).await;
    let clients = [
        cluster.client(0).await,
        cluster.client(1).await,
        cluster.client(2).await,
    ];
    create_tables(&clients[0]).await;
    let mut t_rows = Vec::new();
    let mut u_rows = Vec::new();
    for (n, c) in (1_i64..).zip(&clients) {
        for k in 0..3 {
            t_rows.push(vec![insert_t(c, n).await, n]);
            insert_u(c, 10 * n + k, n).await;
            u_rows.push(vec![10 * n + k, n]);
        }
    }
    t_rows.sort();
    u_rows.sort();
    for c in &clients {
        assert_eq!(rows(c, "SELECT id, node FROM t ORDER BY id").await, t_rows);
        assert_eq!(rows(c, "SELECT id, v FROM u ORDER BY id").await, u_rows);
    }

    // One transaction on node 2 writes both tables; node 3 sees both writes once it commits.
    batch(&clients[1], "BEGIN").await;
    let committed = insert_t(&clients[1], 2).await;
    insert_u(&clients[1], 100, 2).await;
    batch(&clients[1], "COMMIT").await;
    let probe = format!("SELECT id FROM t WHERE id = {committed}");
    assert_eq!(rows(&clients[2], &probe).await, [[committed]]);
    assert_eq!(
        rows(&clients[2], "SELECT id FROM u WHERE id = 100").await,
        [[100]]
    );

    // The same shape rolled back leaves nothing on either node.
    batch(&clients[1], "BEGIN").await;
    let rolled = insert_t(&clients[1], 2).await;
    insert_u(&clients[1], 200, 2).await;
    batch(&clients[1], "ROLLBACK").await;
    let probe = format!("SELECT id FROM t WHERE id = {rolled}");
    for c in &clients[1..] {
        assert!(rows(c, &probe).await.is_empty());
        assert!(rows(c, "SELECT id FROM u WHERE id = 200").await.is_empty());
    }

    // SERIAL values and nextval through every gateway never repeat.
    let mut seen: BTreeSet<i64> = t_rows.iter().map(|r| r[0]).collect();
    assert!(seen.insert(committed) && seen.insert(rolled));
    for _ in 0..3 {
        for c in &clients {
            let v = rows(c, "SELECT nextval('t_id_seq')").await[0][0];
            assert!(seen.insert(v), "sequence value {v} handed out twice");
        }
    }
    cluster.assert_proxies_unsaturated();
    eprintln!(
        "pre_split_cluster_answers_on_every_node: {:?}",
        started.elapsed()
    );
}

/// A killed non-bootstrap node leaves every range with a majority: the other two gateways keep
/// reading and writing; restarted, the node answers again and sees what was written meanwhile.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_killed_node_leaves_the_others_answering() {
    let started = Instant::now();
    let mut cluster = ClusterProcs::new(3).await;
    let c1 = cluster.client(0).await;
    create_tables(&c1).await;
    let c2 = cluster.client(1).await;
    let c3 = cluster.client(2).await;
    let mut expected = Vec::new();
    for (n, c) in [(1, &c1), (2, &c2), (3, &c3)] {
        expected.push(insert_t(c, n).await);
    }
    drop(c3);

    cluster.kill(2);
    for round in 0..3 {
        for (n, c) in [(1, &c1), (2, &c2)] {
            expected.push(insert_t(c, n).await);
            insert_u(c, 10 * n + round, n).await;
        }
    }
    expected.sort();
    assert_eq!(ids(&c1).await, expected);
    assert_eq!(ids(&c2).await, expected);

    cluster.restart(2).await;
    let c3 = cluster.client(2).await;
    assert_eq!(ids(&c3).await, expected);
    assert_eq!(
        rows(&c3, "SELECT count(*) FROM u").await,
        [[6]],
        "node 3 misses writes made while it was down"
    );
    let id = insert_t(&c3, 3).await;
    assert_eq!(
        rows(&c1, &format!("SELECT node FROM t WHERE id = {id}")).await,
        [[3]]
    );
    cluster.assert_proxies_unsaturated();
    eprintln!(
        "a_killed_node_leaves_the_others_answering: {:?}",
        started.elapsed()
    );
}

/// A node cut off the network while clients write elsewhere answers with the new rows once the
/// links heal, and takes writes again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_isolated_node_rejoins() {
    let started = Instant::now();
    let cluster = ClusterProcs::new(3).await;
    let c1 = cluster.client(0).await;
    create_tables(&c1).await;
    let c2 = cluster.client(1).await;
    let c3 = cluster.client(2).await;
    let mut expected = vec![insert_t(&c3, 3).await];

    cluster.isolate(2);
    for _ in 0..5 {
        expected.push(insert_t(&c1, 1).await);
        expected.push(insert_t(&c2, 2).await);
    }
    expected.sort();
    assert_eq!(ids(&c2).await, expected);

    cluster.heal();
    let deadline = Instant::now() + CONVERGE_WAIT;
    loop {
        let read = tokio::time::timeout(
            STATEMENT_WAIT,
            c3.query("SELECT id FROM t ORDER BY id", &[]),
        )
        .await;
        let last = match read {
            Ok(Ok(r)) => {
                let got: Vec<i64> = r.iter().map(|row| row.get(0)).collect();
                if got == expected {
                    break;
                }
                format!("{got:?}")
            }
            Ok(Err(e)) => {
                assert!(!e.is_closed(), "node 3 session closed: {e:?}");
                format!("{e:?}")
            }
            Err(_) => "read timed out".into(),
        };
        assert!(
            Instant::now() < deadline,
            "node 3 did not converge within {CONVERGE_WAIT:?}: {last}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let id = insert_t(&c3, 3).await;
    assert_eq!(
        rows(&c1, &format!("SELECT node FROM t WHERE id = {id}")).await,
        [[3]]
    );
    cluster.assert_proxies_unsaturated();
    eprintln!("an_isolated_node_rejoins: {:?}", started.elapsed());
}

/// One text column of every row.
async fn texts(c: &Client, sql: &str) -> Vec<String> {
    let rows = tokio::time::timeout(STATEMENT_WAIT, c.query(sql, &[]))
        .await
        .unwrap_or_else(|_| panic!("{sql}: no answer within {STATEMENT_WAIT:?}"))
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    rows.iter().map(|r| r.get(0)).collect()
}

/// Asks `c` for the status of transaction `xid` until it is definite or `until` passes.
async fn xact_status(c: &Client, xid: u64, until: Instant) -> String {
    loop {
        let status = texts(c, &format!("SELECT pg_xact_status({xid})")).await;
        let status = status.into_iter().next().expect("one row");
        if status != "in progress" || Instant::now() >= until {
            return status;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

#[derive(Clone, Copy, Debug)]
enum Kill {
    /// After the COMMIT bytes reached node 2, before its reply.
    AfterCommitSent,
    /// While the transaction is open, COMMIT never sent.
    BeforeCommit,
    /// After the client received COMMIT's reply: the outcome is known to be committed.
    AfterCommitReply,
}

/// R5-62: the gateway owning a transaction dies around COMMIT. Another gateway reports a
/// definite status for the transaction id (after the owner restarts at the latest), and that
/// status matches the table: the row is present exactly when the status is 'committed'.
async fn commit_outcome(kill: Kill) {
    let started = Instant::now();
    let mut cluster = ClusterProcs::new(3).await;
    let c1 = cluster.client(0).await;
    create_tables(&c1).await;
    // The relay takes one session: let the owner role reach node 2 before using it.
    drop(cluster.client(1).await);
    let relay = Relay::start(cluster.pg_addr(1));
    let c2 = std::sync::Arc::new(connect(relay.addr()).await);
    batch(&c2, "BEGIN").await;
    let xid: u64 = texts(&c2, "SELECT pg_current_xact_id()::text").await[0]
        .parse()
        .unwrap();
    insert_u(&c2, 1, 2).await;
    insert_t(&c2, 2).await;
    match kill {
        Kill::AfterCommitSent => {
            let commit = {
                let c2 = c2.clone();
                tokio::spawn(async move { c2.batch_execute("COMMIT").await })
            };
            relay.commit_forwarded().await;
            cluster.kill(1);
            // The session ends without a definite answer; its result is not evidence.
            let _ = tokio::time::timeout(STATEMENT_WAIT, commit).await;
        }
        Kill::BeforeCommit => cluster.kill(1),
        Kill::AfterCommitReply => {
            batch(&c2, "COMMIT").await;
            cluster.kill(1);
        }
    }

    // 'in progress' is allowed while the owner is down; a restarted owner answers not-live.
    let mut status = xact_status(&c1, xid, Instant::now() + Duration::from_secs(10)).await;
    if matches!(kill, Kill::AfterCommitReply) {
        // The owner is still down: the committed marker alone decides.
        assert_eq!(status, "committed", "acknowledged commit, owner down");
    }
    if status == "in progress" {
        cluster.restart(1).await;
        status = xact_status(&c1, xid, Instant::now() + STATUS_WAIT).await;
    }
    let present = rows(&c1, "SELECT count(*) FROM u WHERE id = 1").await[0][0];
    let t_rows = rows(&c1, "SELECT count(*) FROM t").await[0][0];
    assert_eq!(
        present, t_rows,
        "{kill:?}: the transaction committed only in part"
    );
    match status.as_str() {
        "committed" => assert_eq!(present, 1, "{kill:?}: committed, row missing"),
        "aborted" => assert_eq!(present, 0, "{kill:?}: aborted, row present"),
        other => panic!("{kill:?}: status of {xid} still {other:?}"),
    }
    match kill {
        Kill::BeforeCommit => assert_eq!(status, "aborted", "COMMIT was never sent"),
        Kill::AfterCommitReply => assert_eq!(status, "committed", "COMMIT was acknowledged"),
        Kill::AfterCommitSent => {}
    }
    // Every gateway agrees once the owner is back.
    let c3 = cluster.client(2).await;
    assert_eq!(xact_status(&c3, xid, Instant::now()).await, status);
    cluster.assert_proxies_unsaturated();
    eprintln!(
        "commit_outcome_survives_the_gateway {kill:?}: {status} in {:?}",
        started.elapsed()
    );
}

/// R5-62 (LONG): the gateway dies right after COMMIT is sent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_outcome_survives_the_gateway() {
    commit_outcome(Kill::AfterCommitSent).await;
}

/// R5-62 variant: the gateway dies after acknowledging COMMIT; another node reports
/// 'committed' while the owner is still down (the commit marker decides, not the owner).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_outcome_survives_the_gateway_after_the_reply() {
    commit_outcome(Kill::AfterCommitReply).await;
}

/// R5-62 variant: the gateway dies before COMMIT is sent; the transaction is aborted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_outcome_survives_the_gateway_before_commit() {
    commit_outcome(Kill::BeforeCommit).await;
}

/// Runs `giggles-server cluster status <config> --json` against the cluster.
fn cluster_status(config: &std::path::Path) -> (i32, serde_json::Value) {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_giggles-server"))
        .args(["cluster", "status"])
        .arg(config)
        .arg("--json")
        .output()
        .unwrap();
    let json = serde_json::from_slice(&output.stdout).unwrap_or_else(|e| {
        panic!(
            "cluster status: {e}; stdout {:?} stderr {:?}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    });
    (output.status.code().unwrap_or(-1), json)
}

/// R5-64: `giggles-server cluster status` on a running cluster names a leader for every range,
/// counts load on the nodes that served SQL, and reports a killed node as unreachable while the
/// other nodes keep every range led.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_status_reports_a_running_cluster() {
    let started = Instant::now();
    let mut cluster = ClusterProcs::new(3).await;
    let c1 = cluster.client(0).await;
    create_tables(&c1).await;
    for node in 0..3 {
        let c = cluster.client(node).await;
        insert_t(&c, node as i64).await;
    }
    let (code, status) = cluster_status(&cluster.config_path(0));
    assert_eq!(code, 0, "{status}");
    let ranges = status["ranges"].as_array().unwrap();
    assert_eq!(
        ranges.len(),
        crate::cluster_harness::split_keys().len() + 2,
        "the system range, the range below the first split key, one range per split key"
    );
    for range in ranges {
        assert!(range["leader"].is_u64(), "range without a leader: {range}");
    }
    let nodes = status["nodes"].as_array().unwrap();
    assert_eq!(nodes.len(), 3);
    assert!(nodes.iter().all(|n| n["reachable"] == true), "{status}");
    let served: u64 = nodes.iter().map(|n| n["calls_total"].as_u64().unwrap_or(0)).sum();
    assert!(served > 0, "no load counted: {status}");

    // Node 3 down: it is reported unreachable; ranges are led by the other two.
    cluster.kill(2);
    let deadline = Instant::now() + STATUS_WAIT;
    loop {
        let (code, status) = cluster_status(&cluster.config_path(0));
        assert_eq!(code, 0, "{status}");
        let nodes = status["nodes"].as_array().unwrap();
        let down = nodes.iter().find(|n| n["node"] == 3).unwrap();
        let led = status["ranges"]
            .as_array()
            .unwrap()
            .iter()
            .all(|r| r["leader"].as_u64().is_some_and(|l| l != 3));
        if down["reachable"] == false && led {
            break;
        }
        assert!(Instant::now() < deadline, "after killing node 3: {status}");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    eprintln!("cluster_status_reports_a_running_cluster: {:?}", started.elapsed());
}
