//! Single integration test binary (`autotests = false`).
#[path = "barrier.rs"]
mod barrier;
#[path = "entry.rs"]
mod entry;
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
