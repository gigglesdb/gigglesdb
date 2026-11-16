#![cfg(feature = "engine-rocksdb")]

use super::replication_harness::{Cluster, Relay, connect};
use std::time::{Duration, Instant};
use tokio_postgres::{Client, Config, SimpleQueryMessage, config::SslMode};

const WAIT: Duration = Duration::from_secs(20);

async fn scalar(client: &Client, sql: &str) -> Result<String, tokio_postgres::Error> {
    let rows = tokio::time::timeout(WAIT, client.simple_query(sql))
        .await
        .expect("query timed out")?;
    for row in rows {
        if let SimpleQueryMessage::Row(row) = row {
            return Ok(row.get(0).expect("unexpected NULL").to_owned());
        }
    }
    panic!("no scalar row for {sql}");
}
async fn relay_client(cluster: &Cluster, leader: usize) -> (Relay, Client) {
    let relay = Relay::start(cluster.pg_addr(leader));
    let mut config = Config::new();
    config
        .host("127.0.0.1")
        .port(relay.addr().port())
        .user("owner")
        .dbname("postgres")
        .ssl_mode(SslMode::Disable);
    let client = connect(&config).await;
    (relay, client)
}
async fn begin(client: &Client, id: i64) -> i64 {
    client.batch_execute("BEGIN").await.unwrap();
    client
        .batch_execute(&format!("INSERT INTO lr VALUES ({id})"))
        .await
        .unwrap();
    scalar(client, "SELECT pg_current_xact_id()")
        .await
        .unwrap()
        .parse()
        .unwrap()
}
async fn outcome(client: &Client, xid: i64, id: i64, name: &str) -> (String, bool) {
    let present = scalar(client, &format!("SELECT count(*) FROM lr WHERE id = {id}"))
        .await
        .unwrap()
        == "1";
    let status = match scalar(client, &format!("SELECT pg_xact_status({xid})")).await {
        Ok(status) => status,
        Err(error) => {
            let detail = format!("ERROR SQLSTATE={:?}: {error:?}", error.code());
            println!("R4-41 {name}: xid={xid} {detail}, row present={present}");
            panic!("R4-41 finding: {detail}");
        }
    };
    println!("R4-41 {name}: {status}, row present={present} (xid={xid})");
    (status, present)
}

#[tokio::test]
async fn committed_reply_lost_then_leader_changes() {
    let mut cluster = Cluster::new();
    let (leader, direct) = cluster.leader_client().await;
    direct
        .batch_execute("CREATE TABLE lr(id bigint PRIMARY KEY)")
        .await
        .unwrap();
    let (relay, client) = relay_client(&cluster, leader).await;
    let xid = begin(&client, 1).await;
    relay.drop_replies();
    assert!(
        tokio::time::timeout(Duration::from_secs(2), client.batch_execute("COMMIT"))
            .await
            .is_err()
    );
    relay.commit_forwarded().await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    cluster.kill(leader);
    let (replacement, client) = cluster.leader_client().await;
    assert_ne!(replacement, leader);
    let (status, present) =
        outcome(&client, xid, 1, "committed_reply_lost_then_leader_changes").await;
    assert_eq!(status, "committed", "R4-41 finding");
    assert!(present, "R4-41 finding: committed row missing");
}

#[tokio::test]
async fn uncommitted_when_quorum_lost_then_leader_changes() {
    let mut cluster = Cluster::new();
    let (leader, client) = cluster.leader_client().await;
    client
        .batch_execute("CREATE TABLE lr(id bigint PRIMARY KEY)")
        .await
        .unwrap();
    let xid = begin(&client, 2).await;
    let followers: Vec<_> = (0..3).filter(|&i| i != leader).collect();
    for &i in &followers {
        cluster.kill(i);
    }
    match tokio::time::timeout(Duration::from_secs(5), client.batch_execute("COMMIT")).await {
        Err(_) => println!("R4-41 quorum-loss COMMIT: 5 s timeout"),
        Ok(Err(error)) => println!(
            "R4-41 quorum-loss COMMIT: error SQLSTATE={:?}: {error:?}",
            error.code()
        ),
        Ok(Ok(())) => panic!("R4-41 finding: COMMIT succeeded without quorum"),
    }
    cluster.kill(leader);
    for i in followers {
        cluster.start(i);
    }
    let (replacement, client) = cluster.leader_client().await;
    assert_ne!(replacement, leader);
    let (status, present) = outcome(
        &client,
        xid,
        2,
        "uncommitted_when_quorum_lost_then_leader_changes",
    )
    .await;
    assert_eq!(status, "aborted", "R4-41 finding");
    assert!(!present, "R4-41 finding: uncommitted row present");
}

#[tokio::test]
async fn status_matches_data_after_commit_racing_leader_kill() {
    let mut cluster = Cluster::new();
    let (_, client) = cluster.leader_client().await;
    client
        .batch_execute("CREATE TABLE lr(id bigint PRIMARY KEY)")
        .await
        .unwrap();
    let (mut committed, mut aborted) = (0, 0);
    for id in 100..110 {
        let leader = cluster.leader().await;
        let (relay, client) = relay_client(&cluster, leader).await;
        let xid = begin(&client, id).await;
        relay.drop_replies();
        let commit = tokio::spawn(async move { client.batch_execute("COMMIT").await });
        relay.commit_forwarded().await;
        cluster.kill(leader);
        commit.abort();
        let replacement = cluster.leader().await;
        assert_ne!(replacement, leader);
        cluster.start(leader);
        cluster.follower_until(leader, Instant::now() + WAIT).await;
        // Connect only after the old leader has rejoined as a follower, so a
        // session opened during the rejoin cannot be closed by a later
        // leadership change (seen under a loaded local gate).
        let (_, client) = cluster.leader_client().await;
        let name = format!(
            "status_matches_data_after_commit_racing_leader_kill round {}",
            id - 99
        );
        let (status, present) = outcome(&client, xid, id, &name).await;
        assert_eq!(
            status,
            if present { "committed" } else { "aborted" },
            "R4-41 finding"
        );
        if present {
            committed += 1;
        } else {
            aborted += 1;
        }
    }
    println!(
        "R4-41 status_matches_data_after_commit_racing_leader_kill: committed={committed}, aborted={aborted}"
    );
}
