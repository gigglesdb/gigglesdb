//! A gateway with no hosted ranges joins and leaves the live TLS peer table.
use super::common::certs::{config_for, make_pki};
use giggles_core::cluster_node::EngineSettings;
use giggles_replication::{
    bootstrap::presplit,
    call_codec::{NodeCall, PeerCall, PeerReply},
    cluster_node::{ClusterNode, ClusterNodeConfig},
    registry::{self, NodeRecord, NodeState},
    transport::CallLimits,
};
use std::{
    collections::BTreeMap,
    net::TcpListener,
    path::PathBuf,
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const MEMBERS: [u64; 3] = [1, 2, 3];
const NODES: [u64; 4] = [1, 2, 3, 4];
const READY: Duration = Duration::from_secs(60);
const CALL: Duration = Duration::from_secs(1);

struct Root(PathBuf);
impl Drop for Root {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn eventually(mut ready: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if ready() {
            return;
        }
        assert!(Instant::now() < deadline, "registry propagation timed out");
        thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn unhosted_node_becomes_reachable_then_is_retired() {
    let root = Root(std::env::temp_dir().join(format!(
        "registry-join-{}-{}",
        std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
    )));
    let pki = make_pki(&root.0.join("pki"), &NODES);
    let listeners: Vec<_> = NODES
        .iter()
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let addresses: BTreeMap<_, _> = NODES
        .iter()
        .zip(&listeners)
        .map(|(&id, l)| (id, l.local_addr().unwrap().to_string()))
        .collect();
    drop(listeners);
    let layout = presplit(&[], &MEMBERS, 3).unwrap();
    let nodes: Vec<_> = NODES
        .iter()
        .map(|&node| {
            let mut transport = config_for(&pki, node, [91; 16], &addresses);
            if node != 4 {
                transport.peers.retain(|id, _| *id != 4);
            }
            ClusterNode::start(ClusterNodeConfig {
                transport,
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
                call_limits: Some(CallLimits {
                    timeout: CALL,
                    ..CallLimits::default()
                }),
            })
            .unwrap()
        })
        .collect();
    assert!(nodes[3].host().groups().is_empty());
    nodes[0].bootstrap_directory(READY).unwrap();
    for node in &nodes[..3] {
        node.wait_ready(READY).unwrap();
    }
    assert_eq!(nodes[0].enrol_members(READY).unwrap(), 3);
    assert_eq!(nodes[0].enrol_members(READY).unwrap(), 0);
    // A self-enrolled wildcard must never replace a statically configured dial address.
    {
        use giggles_core::range_api::{
            ClusterTxnId, LeaseKind, RangeClient, RangeSpace, range_key,
        };
        let client = &**nodes[0].client();
        let system = client
            .descriptor_for(&range_key(RangeSpace::Catalog, &[registry::REGISTRY_TAG]))
            .unwrap();
        let counter = client
            .lease(LeaseKind::Xid, (1 << 32) | 1, 1)
            .unwrap()
            .start;
        client
            .commit(
                system.id,
                system.epoch,
                ClusterTxnId::try_new(1, counter).unwrap(),
                vec![registry::record_write(&NodeRecord {
                    id: 1,
                    state: NodeState::Active,
                    address: "0.0.0.0:1".into(),
                    fingerprint: pki.nodes[&1].2.clone(),
                })],
                None,
                None,
            )
            .unwrap();
    }
    let mut watchers: Vec<_> = nodes[..3]
        .iter()
        .map(|node| node.start_registry_watcher(Duration::from_millis(100)))
        .collect();
    // Every possible system-range leader rejects node 4's still unknown certificate.
    assert!(registry::read_all(&**nodes[3].client()).is_err());
    let status = || {
        nodes[0]
            .transport()
            .calls()
            .call(4, PeerCall::Node(NodeCall::Status), CALL)
    };
    assert!(status().is_err());
    assert!(
        registry::enrol(
            &**nodes[0].client(),
            1,
            NodeRecord {
                id: 4,
                state: NodeState::Active,
                address: addresses[&4].clone(),
                fingerprint: pki.nodes[&4].2.clone(),
            }
        )
        .unwrap()
    );
    eventually(|| registry::read_all(&**nodes[3].client()).is_ok());
    // Test every watcher's outbound table, and every member's inbound authorization.
    for node in &nodes[..3] {
        eventually(|| {
            matches!(node.transport().calls().call(4, PeerCall::Node(NodeCall::Status), CALL),
            Ok(PeerReply::Node(report)) if report.node == 4 && report.ranges.is_empty())
        });
        eventually(|| {
            nodes[3]
                .transport()
                .calls()
                .call(node.node_id(), PeerCall::Node(NodeCall::Status), CALL)
                .is_ok()
        });
    }
    // Registry propagation above forces each watcher through a successful pass.
    for node in &nodes[1..3] {
        assert!(
            matches!(node.transport().calls().call(1, PeerCall::Node(NodeCall::Status), CALL),
            Ok(PeerReply::Node(report)) if report.node == 1)
        );
    }
    assert!(registry::retire(&**nodes[0].client(), 1, 4).unwrap());
    // Existing inbound call connections must also lose authorization on retirement.
    for node in &nodes[..3] {
        eventually(|| {
            nodes[3]
                .transport()
                .calls()
                .call(node.node_id(), PeerCall::Node(NodeCall::Status), CALL)
                .is_err()
        });
    }
    eventually(|| registry::read_all(&**nodes[3].client()).is_err());
    for watcher in &mut watchers {
        watcher.stop();
        watcher.stop();
    }
    drop(watchers);
    for node in nodes {
        node.stop();
    }
}
