#![cfg(feature = "engine-rocksdb")]

use crate::{cluster_harness::ClusterProcs, replication_harness::Cluster};
use std::{io::Write, process::Child, time::Duration};
use tokio_postgres::Client;

const EXIT_WAIT: Duration = Duration::from_secs(15);

/// Keep ownership of the pipe through try_wait: EOF must not enable the exit.
async fn shutdown(child: &mut Child) {
    let mut stdin = child.stdin.take().expect("piped stdin");
    let started = tokio::time::Instant::now();
    stdin.write_all(b"shutdown\n").unwrap();
    stdin.flush().unwrap();
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert_eq!(status.code(), Some(0), "shutdown failed: {status}");
            eprintln!(
                "stdin shutdown pid={} elapsed={:?}",
                child.id(),
                started.elapsed()
            );
            // Explicitly retain the pipe until after the child has exited.
            drop(stdin);
            return;
        }
        assert!(
            started.elapsed() < EXIT_WAIT,
            "pid={} did not exit within {EXIT_WAIT:?} with stdin open",
            child.id()
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn assert_sql(client: &Client) {
    let row = tokio::time::timeout(
        EXIT_WAIT,
        client.query_one("SELECT id FROM shutdown_probe", &[]),
    )
    .await
    .expect("survivor SQL timed out")
    .expect("survivor SQL failed");
    assert_eq!(row.get::<_, i64>(0), 42);
}

async fn setup(client: &Client) {
    tokio::time::timeout(EXIT_WAIT, client.batch_execute(
        "CREATE TABLE shutdown_probe (id BIGINT PRIMARY KEY); INSERT INTO shutdown_probe VALUES (42)",
    ))
    .await
    .expect("setup timed out")
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replicated_shutdown_exits_with_stdin_open() {
    let mut cluster = Cluster::new();
    let (leader, client) = cluster.leader_client().await;
    setup(&client).await;
    let followers: Vec<_> = (0..3).filter(|&i| i != leader).collect();
    shutdown(cluster.child_mut(followers[0])).await;
    // Replicated-mode followers reject SQL; the surviving leader must still
    // serve a real table read with the other follower forming its quorum.
    assert_sql(&client).await;
    drop(client);
    shutdown(cluster.child_mut(followers[1])).await;
    shutdown(cluster.child_mut(leader)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "GD-0075 hardening open item: on main 9508e463 a cluster node shut down after its peers takes 10 s or more to exit (peer connect timeout); see the work log"]
async fn cluster_shutdown_exits_with_stdin_open() {
    let mut cluster = ClusterProcs::new(3).await;
    let first = cluster.client(0).await;
    setup(&first).await;
    drop(first);
    let second = cluster.client(1).await;
    let third = cluster.client(2).await;
    shutdown(cluster.child_mut(0)).await;
    assert_sql(&second).await;
    assert_sql(&third).await;
    drop((second, third));
    shutdown(cluster.child_mut(1)).await;
    shutdown(cluster.child_mut(2)).await;
}
