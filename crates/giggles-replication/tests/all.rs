//! Single integration test binary (`autotests = false`).
#[path = "barrier.rs"]
mod barrier;
#[path = "entry.rs"]
mod entry;
#[path = "failpoint.rs"]
mod failpoint;
#[path = "log_store.rs"]
mod log_store;
#[path = "state.rs"]
mod state;

#[test]
fn every_test_file_is_registered() {
    let all = include_str!("all.rs");
    let manifest = include_str!("../Cargo.toml");
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
    let mut missing = Vec::new();
    for entry in std::fs::read_dir(&dir).expect("read tests dir") {
        let path = entry.expect("dir entry").path();
        if path.extension().and_then(|e| e.to_str()) != Some("rs") {
            continue;
        }
        let stem = path
            .file_stem()
            .and_then(|s| s.to_str())
            .expect("utf-8 name");
        if stem == "all" || stem == "common" {
            continue;
        }
        let as_module = all.contains(&format!("mod {stem};"));
        let as_target = manifest.contains(&format!("path = \"tests/{stem}.rs\""));
        if !as_module && !as_target {
            missing.push(stem.to_owned());
        }
    }
    assert!(
        missing.is_empty(),
        "tests/*.rs files not registered in tests/all.rs or Cargo.toml [[test]]: {missing:?}"
    );
}
#[path = "chaos.rs"]
mod chaos;
#[path = "cluster.rs"]
mod cluster;
mod common;
#[path = "core_store.rs"]
mod core_store;
#[path = "failover.rs"]
mod failover;
#[path = "node.rs"]
mod node;
#[path = "role.rs"]
mod role;
#[path = "sink.rs"]
mod sink;
#[path = "snapshot.rs"]
mod snapshot;
#[path = "store_slot.rs"]
mod store_slot;

#[path = "snapshot_node.rs"]
mod snapshot_node;

#[path = "snapshot_policy.rs"]
mod snapshot_policy;
#[path = "transport_frame.rs"]
mod transport_frame;
#[path = "transport_runtime.rs"]
mod transport_runtime;
#[path = "transport_snapshot.rs"]
mod transport_snapshot;

#[path = "cluster_net.rs"]
mod cluster_net;

#[path = "membership.rs"]
mod membership;
#[path = "transport_peers.rs"]
mod transport_peers;

#[path = "membership_node.rs"]
mod membership_node;

#[path = "membership_crash.rs"]
mod membership_crash;
#[path = "replace_net.rs"]
mod replace_net;

#[path = "admin_net.rs"]
mod admin_net;

#[path = "measure.rs"]
mod measure;
#[path = "recovery_matrix.rs"]
mod recovery_matrix;
#[path = "reopen_inflight.rs"]
mod reopen_inflight;
#[path = "sql_history.rs"]
mod sql_history;

#[path = "once_net.rs"]
mod once_net;

#[path = "multi_group.rs"]
mod multi_group;

#[path = "host.rs"]
mod host;

#[path = "split.rs"]
mod split;
#[path = "split_lifecycle.rs"]
mod split_lifecycle;
#[path = "split_driver.rs"]
mod split_driver;
#[path = "split_size.rs"]
mod split_size;
#[path = "split_prepared.rs"]
mod split_prepared;
#[path = "split_snapshot.rs"]
mod split_snapshot;
#[path = "range_state.rs"]
mod range_state;
#[path = "range_host.rs"]
mod range_host;

#[path = "routing.rs"]
mod routing;

#[path = "group.rs"]
mod group;

#[path = "idle.rs"]
mod idle;

#[path = "call_codec.rs"]
mod call_codec;

#[path = "quiesce.rs"]
mod quiesce;

#[path = "bootstrap.rs"]
mod bootstrap;

#[path = "liveness.rs"]
mod liveness;

#[path = "transport_groups.rs"]
mod transport_groups;

#[path = "idle_hosts.rs"]
mod idle_hosts;

#[path = "range_calls.rs"]
mod range_calls;

#[path = "presplit.rs"]
mod presplit;

#[path = "group_snapshot.rs"]
mod group_snapshot;

#[path = "leader_reads.rs"]
mod leader_reads;

#[path = "gateway_cluster.rs"]
mod gateway_cluster;

#[path = "split_scan.rs"]
mod split_scan;

#[path = "registry.rs"]
mod registry;

#[path = "registry_join.rs"]
mod registry_join;
#[path = "range_conf.rs"]
mod range_conf;

#[path = "range_move.rs"]
mod range_move;

#[path = "range_move_load.rs"]
mod range_move_load;
#[path = "admin_identity.rs"]
mod admin_identity;
