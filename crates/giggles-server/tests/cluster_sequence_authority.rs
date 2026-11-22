#![cfg(feature = "engine-rocksdb")]

use crate::cluster_harness::{ClusterProcs, connect_as};
use std::time::Duration;
use tokio_postgres::{Client, error::SqlState};

const STATEMENT_WAIT: Duration = Duration::from_secs(45);

async fn batch(client: &Client, sql: &str) {
    tokio::time::timeout(STATEMENT_WAIT, client.batch_execute(sql))
        .await
        .unwrap_or_else(|_| panic!("{sql}: timed out"))
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
}

async fn value(client: &Client, sql: &str) -> i64 {
    tokio::time::timeout(STATEMENT_WAIT, client.query_one(sql, &[]))
        .await
        .unwrap_or_else(|_| panic!("{sql}: timed out"))
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"))
        .get(0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seqauth2_every_gateway_checks_sequence_privilege() {
    let cluster = ClusterProcs::new(3).await;
    let root = cluster.client(0).await;
    for sql in [
        "CREATE ROLE alice LOGIN",
        "CREATE ROLE bob LOGIN",
        "GRANT CREATE ON SCHEMA public TO alice, bob",
    ] {
        batch(&root, sql).await;
    }
    let alice = connect_as(cluster.pg_addr(0), "alice").await;
    let bob = connect_as(cluster.pg_addr(0), "bob").await;
    batch(&alice, "CREATE TABLE gateway_serial (id BIGSERIAL PRIMARY KEY, marker BIGINT)").await;
    batch(&alice, "GRANT SELECT, INSERT ON gateway_serial TO bob").await;
    // Give bob some sequence scope and demonstrate that the table grant works.
    batch(&bob, "CREATE SEQUENCE bob_seq").await;
    batch(&bob, "INSERT INTO gateway_serial VALUES (1, 0)").await;

    // The system range leader owns the counter. Testing every gateway includes
    // the leader and the gateways that would have to forward the sequence call.
    const NEXT: &str = "SELECT nextval('gateway_' || 'serial_id_seq')";
    const INSERT: &str = "INSERT INTO gateway_serial (marker) VALUES (1) RETURNING id";
    const SET: &str = "SELECT setval('gateway_' || 'serial_id_seq', 2000, false)";
    const COUNT: &str = "SELECT count(*) FROM gateway_serial WHERE marker = 1";

    let mut expected = 100;
    for gateway in 0..cluster.len() {
        let bob = connect_as(cluster.pg_addr(gateway), "bob").await;
        let alice = connect_as(cluster.pg_addr(gateway), "alice").await;
        let set = format!("SELECT setval('gateway_serial_id_seq', {expected}, false)");
        assert_eq!(value(&alice, &set).await, expected, "gateway {gateway}: owner setval");
        for sql in [NEXT, SET, INSERT] {
            let result = tokio::time::timeout(STATEMENT_WAIT, bob.query_one(sql, &[]))
                .await
                .unwrap_or_else(|_| panic!("gateway {gateway}: {sql}: timed out"));
            assert_eq!(
                result.as_ref().err().and_then(tokio_postgres::Error::code),
                Some(&SqlState::INSUFFICIENT_PRIVILEGE),
                "gateway {gateway}: {sql}: {result:?}",
            );
        }
        assert_eq!(value(&alice, COUNT).await, gateway as i64, "gateway {gateway}: refused row");
        assert_eq!(value(&alice, NEXT).await, expected, "gateway {gateway}: counter changed");
        assert_eq!(value(&alice, INSERT).await, expected + 1, "gateway {gateway}: owner insert");
        expected += 2;
    }
    assert_eq!(value(&alice, COUNT).await, cluster.len() as i64);
    cluster.assert_proxies_unsaturated();
}
