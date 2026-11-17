//! Registry enrolment and retirement through a real three-node system range.
use super::common::certs::{config_for, make_pki};
use giggles_core::cluster_node::EngineSettings;
use giggles_replication::{
    bootstrap::presplit,
    cluster_node::{ClusterNode, ClusterNodeConfig},
    registry::{self, NodeRecord, NodeState},
};
use std::{
    collections::BTreeMap,
    net::TcpListener,
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const NODES: [u64; 3] = [1, 2, 3];
const DEADLINE: Duration = Duration::from_secs(60);
struct Root(PathBuf);
impl Drop for Root {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn start() -> (Root, Vec<ClusterNode>, Vec<NodeRecord>) {
    let root = Root(
        std::env::temp_dir().join(format!(
            "registry-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        )));
    let layout = presplit(&[], &NODES, 3).unwrap();
    let pki = make_pki(&root.0.join("pki"), &NODES);
    let listeners: Vec<_> = NODES
        .iter()
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let addresses: BTreeMap<_, _> = NODES
        .iter()
        .zip(&listeners)
        .map(|(id, l)| (*id, l.local_addr().unwrap().to_string()))
        .collect();
    drop(listeners);
    let records = NODES
        .iter()
        .map(|&id| NodeRecord {
            id,
            state: NodeState::Active,
            address: addresses[&id].clone(),
            fingerprint: pki.nodes[&id].2.clone(),
        })
        .collect();
    let nodes: Vec<_> = NODES
        .iter()
        .map(|&node| {
            ClusterNode::start(ClusterNodeConfig {
                transport: config_for(&pki, node, [90; 16], &addresses),
                data_dir: root.0.join(format!("n{node}/data")),
                log_dir: root.0.join(format!("n{node}/log")),
                snap_dir: root.0.join(format!("n{node}/snap")),
                tick: Duration::from_millis(20),
                layout: layout.clone(),
                engine: EngineSettings {
                    lease: Duration::from_secs(3),
                    ..EngineSettings::default()
                },
                liveness: None,
                policy: None,
                call_limits: None,
            })
            .unwrap()
        })
        .collect();
    assert!(nodes[0].bootstrap_directory(DEADLINE).unwrap());
    for node in &nodes {
        node.wait_ready(DEADLINE).unwrap();
    }
    (root, nodes, records)
}

#[test]
fn enrol_read_and_retire() {
    let (_root, nodes, mut records) = start();
    let client = &**nodes[0].client();
    assert!(registry::read_all(client).unwrap().is_empty());
    for record in &records {
        assert!(registry::enrol(client, 1, record.clone()).unwrap());
    }
    // A large address also forces read_all to follow its page resume.
    let fourth = NodeRecord {
        id: 4,
        state: NodeState::Active,
        address: format!("127.0.0.1:4444{}", " ".repeat(4200)),
        fingerprint: "fourth-fingerprint".into(),
    };
    records.push(fourth.clone());
    assert!(registry::enrol(client, 1, fourth.clone()).unwrap());
    assert_eq!(registry::read_all(&**nodes[1].client()).unwrap(), records);
    assert!(!registry::enrol(client, 1, fourth.clone()).unwrap());
    let mut changed = fourth.clone();
    changed.fingerprint = "different".into();
    assert!(
        registry::enrol(client, 1, changed)
            .unwrap_err()
            .to_string()
            .contains("different address or fingerprint")
    );
    let mut changed = fourth.clone();
    changed.address = "different".into();
    assert!(registry::enrol(client, 1, changed).is_err());
    let mut reused = fourth.clone();
    reused.id = 5;
    assert!(
        registry::enrol(client, 1, reused.clone())
            .unwrap_err()
            .to_string()
            .contains("another active")
    );
    for id in NODES {
        assert!(
            registry::retire(client, 1, id)
                .unwrap_err()
                .to_string()
                .contains("still a replica")
        );
    }
    assert!(registry::retire(client, 1, 4).unwrap());
    assert!(!registry::retire(client, 1, 4).unwrap());
    assert!(
        registry::retire(client, 1, 99)
            .unwrap_err()
            .to_string()
            .contains("unknown node id")
    );
    assert!(
        registry::enrol(client, 1, fourth)
            .unwrap_err()
            .to_string()
            .contains("retired")
    );
    assert!(
        registry::enrol(client, 1, reused)
            .unwrap_err()
            .to_string()
            .contains("retired")
    );
    records[3].state = NodeState::Retired;
    assert_eq!(registry::read_all(&**nodes[2].client()).unwrap(), records);
    for node in nodes {
        node.stop();
    }
}
