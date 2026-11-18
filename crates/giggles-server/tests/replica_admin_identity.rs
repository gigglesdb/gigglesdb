#![cfg(feature = "engine-rocksdb")]
use super::replication_harness::Cluster;
use giggles_replication::admin::{self, AdminRequest, AdminResponse};
use giggles_server::replication::{cli, config::ReplicationConfig, runtime::transport_config};
use std::{
    path::Path,
    process::{Command, Stdio},
    time::{Duration, Instant},
};

fn transport(c: &Cluster, i: usize) -> giggles_replication::transport::TransportConfig {
    let value: serde_json::Value =
        serde_json::from_slice(&std::fs::read(c.config_path(i)).unwrap()).unwrap();
    let config: ReplicationConfig = serde_json::from_value(value["replication"].clone()).unwrap();
    transport_config(
        &config
            .validate(
                Path::new(value["database"].as_str().unwrap()),
                Path::new(value["certificate"].as_str().unwrap()),
                Path::new(value["private_key"].as_str().unwrap()),
                Path::new(value["client_ca"].as_str().unwrap()),
            )
            .unwrap(),
    )
    .unwrap()
}

async fn status_cli(
    c: &Cluster,
    node: usize,
    operator: bool,
    deadline: Instant,
) -> std::process::Output {
    let config = c.config_path(node);
    let identity = operator.then(|| c.operator_identity());
    tokio::task::spawn_blocking(move || {
        let mut command = Command::new(env!("CARGO_BIN_EXE_giggles-server"));
        command.args(["replica", "status"]).arg(config);
        if let Some((cert, key)) = identity {
            command.arg("--identity").arg(cert).arg(key);
        }
        let mut child = command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        loop {
            if child.try_wait().unwrap().is_some() {
                return child.wait_with_output().unwrap();
            }
            if Instant::now() >= deadline {
                child.kill().unwrap();
                let output = child.wait_with_output().unwrap();
                panic!("identity CLI deadline: {output:?}");
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    })
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn own_peer_and_operator_admin_identities() {
    let mut c = Cluster::with_operators(true);
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let leader = tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), c.leader())
            .await
            .expect("identity leader deadline");
        let own = status_cli(&c, 0, false, deadline).await;
        let own_matches = if leader == 0 {
            own.status.success()
        } else {
            own.status.code() == Some(1)
                && String::from_utf8_lossy(&own.stderr).contains("pass an operator identity")
        };
        let local = status_cli(&c, leader, false, deadline).await;
        let follower = (leader + 1) % 3;
        let operator = status_cli(&c, follower, true, deadline).await;
        if own_matches
            && local.status.success()
            && operator.status.success()
            && String::from_utf8_lossy(&operator.stdout).contains("voters: [1, 2, 3]")
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "identity CLI deadline: leader={leader}, own={own:?}, local={local:?}, operator={operator:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // A peer's certificate is never an admin credential, even for status.
    let peer = transport(&c, 1);
    let response = tokio::task::spawn_blocking(move || {
        admin::call(
            &peer,
            1,
            &AdminRequest::Status,
            Duration::from_secs(10),
            None,
        )
        .unwrap()
    })
    .await
    .unwrap();
    assert!(
        matches!(&response, AdminResponse::Refused(_)),
        "{response:?}"
    );
    let (code, text) = cli::output(response);
    assert_eq!(code, 1);
    assert!(
        text.contains("refused: replica admin needs an operator identity"),
        "{text}"
    );
}
