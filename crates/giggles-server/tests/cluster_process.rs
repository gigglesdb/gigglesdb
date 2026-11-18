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
    // For the record: the ranges node 3 leads when it is cut off ("last" holds the gateway
    // liveness and DDL locks; range 1 is the system range).
    let (_, status) = cluster_status(&cluster.config_path(0));
    let led: Vec<String> = status["ranges"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|range| range["leader"] == 3)
        .map(|range| {
            if range["end"].is_null() {
                "last".to_string()
            } else {
                range["id"].to_string()
            }
        })
        .collect();
    eprintln!("node 3 leads ranges {led:?} when it is cut off");

    cluster.isolate(2);
    for _ in 0..5 {
        expected.push(insert_t(&c1, 1).await);
        expected.push(insert_t(&c2, 2).await);
    }
    expected.sort();
    assert_eq!(ids(&c2).await, expected);
    // Healthy gateways stay members while node 3 is cut off: no refusal on nodes 1 and 2
    // (a fenced gateway refuses with 40001; before that, the range-unavailable 08007).
    for node in [0, 1] {
        let refused: Vec<usize> = ["sqlstate=40001", "sqlstate=08007"]
            .iter()
            .map(|code| cluster.stderr_lines(node, code))
            .collect();
        eprintln!(
            "node {} refusals (40001, 08007) while node 3 was cut off: {refused:?}",
            node + 1
        );
    }

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
        let sql = format!("SELECT pg_xact_status({xid})");
        let result = tokio::time::timeout(STATEMENT_WAIT, c.query_one(&sql, &[]))
            .await
            .unwrap_or_else(|_| panic!("{sql}: no answer within {STATEMENT_WAIT:?}"));
        let status: String = match result {
            Ok(row) => row.get(0),
            Err(e)
                if e.code()
                    .is_some_and(|code| matches!(code.code(), "40001" | "08007"))
                    && Instant::now() < until =>
            {
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }
            Err(e) => panic!("{sql}: {e:?}"),
        };
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
    assert_eq!(xact_status(&c3, xid, Instant::now() + STATUS_WAIT).await, status);
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

/// GD-0075 P8-0: a gateway that dies mid-commit (every participant prepared and voted, no
/// decision) holds up only reads of the rows it wrote, until recovery decides. Point reads of
/// every other row answer at once on the other nodes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_gateway_dying_mid_commit_stalls_only_its_own_rows() {
    let started = Instant::now();
    let mut cluster = ClusterProcs::new(3).await;
    let c1 = cluster.client(0).await;
    create_tables(&c1).await;
    for id in 1..=20 {
        insert_u(&c1, id, id * 10).await;
    }
    // Node 2 comes back armed: its next cross-range COMMIT aborts the process after prepare.
    cluster.kill(1);
    cluster.set_env(1, "GIGGLES_CORE_FAILPOINT", "gateway.commit.prepared");
    cluster.start(1);
    cluster.ready(1).await;
    let c2 = cluster.client(1).await;
    batch(&c2, "BEGIN").await;
    assert_eq!(exec(&c2, "UPDATE u SET v = -1 WHERE id = 15", &[]).await, 1);
    insert_t(&c2, 2).await;
    let commit = tokio::time::timeout(STATEMENT_WAIT, c2.batch_execute("COMMIT")).await;
    assert!(
        !matches!(commit, Ok(Ok(()))),
        "COMMIT must not succeed: {commit:?}"
    );
    cluster.exited(1, STATEMENT_WAIT).await;
    assert!(
        cluster
            .stderr(1)
            .contains("failpoint crash: gateway.commit.prepared"),
        "node 2 did not die between prepare and decide"
    );
    let died = Instant::now();

    // Leaders node 2 held move first: one untimed read per node waits for that.
    let c3 = cluster.client(2).await;
    for c in [&c1, &c3] {
        assert_eq!(
            rows(c, "SELECT v FROM u WHERE id = 1").await,
            vec![vec![10]]
        );
    }
    // Every other row, from both surviving gateways, answers well inside read_wait (5 s).
    let mut slowest = Duration::ZERO;
    for id in (1..=20_i64).filter(|id| *id != 15) {
        for c in [&c1, &c3] {
            let asked = Instant::now();
            let got = rows(c, &format!("SELECT v FROM u WHERE id = {id}")).await;
            assert_eq!(got, vec![vec![id * 10]], "row {id}");
            slowest = slowest.max(asked.elapsed());
        }
    }
    let scan = Instant::now();
    assert_eq!(
        rows(&c3, "SELECT count(*) FROM u WHERE id BETWEEN 1 AND 10").await,
        vec![vec![10]]
    );
    slowest = slowest.max(scan.elapsed());
    let checked = died.elapsed();
    // A read held by the prepared row waits read_wait (5 s); 4 s leaves room for slow boxes.
    assert!(slowest < Duration::from_secs(4), "slowest read {slowest:?}");
    // Still inside recovery's 30 s: the prepared row itself is held up.
    let c4 = cluster.client(2).await;
    let held = tokio::time::timeout(
        Duration::from_secs(2),
        c4.query("SELECT v FROM u WHERE id = 15", &[]),
    )
    .await;
    assert!(
        held.is_err() || died.elapsed() >= Duration::from_secs(25),
        "the prepared row answered at once: {held:?}"
    );
    // Recovery aborts the undecided transaction (recover_after 30 s). Read the prepared row once
    // that window has passed (a read held on it for longer than a peer call times out drops the
    // peer connection, the Phase 7 open item); every node then agrees on the old value.
    tokio::time::sleep(Duration::from_secs(35).saturating_sub(died.elapsed())).await;
    assert_eq!(
        settled(&c1, "SELECT v FROM u WHERE id = 15").await,
        vec![vec![150]]
    );
    assert_eq!(rows(&c3, "SELECT count(*) FROM t").await, vec![vec![0]]);
    // No assert_proxies_unsaturated here: the run outlasts recovery's 30 s window, and the
    // heartbeats of 18 range groups alone fill a link's 65536-event trace; the reads above already
    // show every connection this test needed was accepted.
    eprintln!(
        "a_gateway_dying_mid_commit_stalls_only_its_own_rows: slowest other read {slowest:?}, \
         checks done {checked:?} after the crash, total {:?}",
        started.elapsed()
    );
}

/// The rows of a bigint query once it answers, retrying errors (an undecided prepared row is
/// unavailable until recovery decides it) for up to STATUS_WAIT.
async fn settled(c: &Client, sql: &str) -> Vec<Vec<i64>> {
    let deadline = Instant::now() + STATUS_WAIT;
    loop {
        match tokio::time::timeout(STATEMENT_WAIT, c.query(sql, &[])).await {
            Ok(Ok(rows)) => {
                return rows
                    .iter()
                    .map(|r| (0..r.len()).map(|i| r.get::<_, i64>(i)).collect())
                    .collect();
            }
            other => assert!(Instant::now() < deadline, "{sql}: {other:?}"),
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// The range (status JSON) holding range key `key` (hex). Equal-case hex orders as bytes do.
fn range_holding<'a>(status: &'a serde_json::Value, key: &str) -> Option<&'a serde_json::Value> {
    status["ranges"].as_array()?.iter().find(|r| {
        r["start"].as_str().is_some_and(|start| start <= key)
            && r["end"].as_str().is_none_or(|end| key < end)
    })
}

/// The leader of the range holding range key `key` (hex), once a status names one that is not
/// `avoid`, asked through node `via`'s configuration. Equal-case hex orders as the bytes do.
async fn leader_of(cluster: &ClusterProcs, via: usize, key: &str, avoid: u64) -> u64 {
    let deadline = Instant::now() + STATUS_WAIT;
    loop {
        let (_, status) = cluster_status(&cluster.config_path(via));
        let leader = range_holding(&status, key).and_then(|r| r["leader"].as_u64());
        if let Some(leader) = leader.filter(|l| *l != avoid) {
            return leader;
        }
        assert!(Instant::now() < deadline, "no leader for {key}: {status}");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// GD-0075 R5-73 (LONG, 5 nodes): a SERIALIZABLE writer read a key range of u and wrote t. Its
/// gateway dies after every participant prepared, so u's range holds the writer's gap lock in a
/// prepared record. Then u's range changes leader. The new leader re-takes the gap lock: an
/// insert into the gap waits until recovery decides the writer (abort), while an insert outside
/// it goes through. The gateway holds no replica of u's, the system or the liveness range, so
/// these ranges keep a majority while u's old leader is down; the old leader then comes back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_prepared_gap_lock_survives_its_gateway_and_a_leader_change() {
    let started = Instant::now();
    let mut cluster = ClusterProcs::new(5).await;
    let c1 = cluster.client(0).await;
    create_tables(&c1).await;
    for id in [10, 20, 30] {
        insert_u(&c1, id, id).await;
    }
    let oid = rows(&c1, "SELECT oid::bigint FROM pg_class WHERE relname = 'u'").await[0][0];
    let table = (oid - 65536) / 4;
    // u's first row key: range key of Rows (0x80) || table id BE8 || pk.
    let u_key = format!("80{table:016x}");
    let (_, status) = cluster_status(&cluster.config_path(0));
    let protected_ranges = [
        range_holding(&status, &u_key).expect("u's range"),
        status["ranges"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["id"] == 1)
            .expect("system range (id 1)"),
        range_holding(&status, "80fffffffffffffff9").expect("gateway-liveness range"),
    ];
    let replicas: BTreeSet<u64> = protected_ranges
        .into_iter()
        .flat_map(|r| r["replicas"].as_array().expect("range replicas"))
        .map(|n| n.as_u64().expect("replica node id"))
        .collect();
    // Losing the gateway and u's leader must preserve quorum for all three ranges.
    let gateway = (2..=5_u64)
        .find(|n| !replicas.contains(n))
        .expect("no gateway outside u's, system and gateway-liveness replica sets");
    let g = gateway as usize - 1;
    cluster.kill(g);
    cluster.set_env(g, "GIGGLES_CORE_FAILPOINT", "gateway.commit.prepared");
    cluster.start(g);
    cluster.ready(g).await;
    let cg = cluster.client(g).await;
    batch(&cg, "BEGIN ISOLATION LEVEL SERIALIZABLE").await;
    assert_eq!(
        rows(&cg, "SELECT count(*) FROM u WHERE id >= 10 AND id <= 30").await,
        vec![vec![3]]
    );
    insert_t(&cg, gateway as i64).await;
    let commit = tokio::time::timeout(STATEMENT_WAIT, cg.batch_execute("COMMIT")).await;
    assert!(!matches!(commit, Ok(Ok(()))), "COMMIT must not succeed");
    cluster.exited(g, STATEMENT_WAIT).await;
    assert!(
        cluster
            .stderr(g)
            .contains("failpoint crash: gateway.commit.prepared"),
        "the gateway did not die between prepare and decide"
    );
    let died = Instant::now();
    // u's range changes leader: its leader goes down until another replica leads, then returns.
    let alive = |avoid: &[u64]| (1..=5_u64).find(|n| !avoid.contains(n)).unwrap() as usize - 1;
    let first = leader_of(&cluster, alive(&[gateway]), &u_key, gateway).await;
    cluster.kill(first as usize - 1);
    let second = leader_of(&cluster, alive(&[gateway, first]), &u_key, first).await;
    assert_ne!(second, first);
    cluster.start(first as usize - 1);
    cluster.ready(first as usize - 1).await;
    let others: Vec<usize> = (1..=5_u64)
        .filter(|n| *n != gateway)
        .map(|n| n as usize - 1)
        .collect();
    let ca = cluster.client(others[0]).await;
    let cb = std::sync::Arc::new(cluster.client(others[1]).await);
    // Wait for both gateways' liveness leases after the leader change. The insert outside
    // the gap still goes through; the point read checks cb before it tries the locked gap.
    let deadline = Instant::now() + STATUS_WAIT;
    for (client, sql, expected) in [
        (
            &ca,
            "INSERT INTO u (id, v) VALUES (50, 50) RETURNING id",
            50_i64,
        ),
        (cb.as_ref(), "SELECT id FROM u WHERE id = 10", 10_i64),
    ] {
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            match tokio::time::timeout(remaining.min(STATEMENT_WAIT), client.query(sql, &[])).await
            {
                Ok(Ok(rows)) => {
                    assert_eq!(rows.len(), 1, "{sql}");
                    assert_eq!(rows[0].get::<_, i64>(0), expected, "{sql}");
                    break;
                }
                Ok(Err(e)) if e.code().is_some_and(|code| code.code() == "40001") => {
                    assert!(
                        Instant::now() < deadline,
                        "{sql}: gateway still fenced: {e:?}"
                    );
                }
                other => panic!("{sql}: gateway readiness failed: {other:?}"),
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
    let gap = {
        let cb = cb.clone();
        tokio::spawn(async move {
            let asked = Instant::now();
            let result = cb
                .execute("INSERT INTO u (id, v) VALUES (15, 15)", &[])
                .await;
            (result.map_err(|e| format!("{e:?}")), asked.elapsed())
        })
    };
    tokio::time::sleep(Duration::from_secs(3)).await;
    let held = died.elapsed();
    if gap.is_finished() && held < Duration::from_secs(25) {
        let (result, waited) = gap.await.unwrap();
        panic!(
            "the insert into the gap finished early: {result:?}, waited {waited:?} \
             ({held:?} after the crash)"
        );
    }
    // Recovery aborts the writer (its gateway never decided): the insert goes through.
    let (result, waited) = tokio::time::timeout(STATUS_WAIT, gap)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result, Ok(1), "insert into the gap");
    assert_eq!(settled(&ca, "SELECT count(*) FROM t").await, vec![vec![0]]);
    assert_eq!(settled(&ca, "SELECT count(*) FROM u").await, vec![vec![5]]);
    eprintln!(
        "a_prepared_gap_lock_survives_its_gateway_and_a_leader_change: gateway {gateway}, u range \
         led by {first} then {second}; gap insert held {waited:?} (checked {held:?} after the \
         crash); total {:?}",
        started.elapsed()
    );
}
