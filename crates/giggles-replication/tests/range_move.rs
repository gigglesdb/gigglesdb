//! Four-node range move with TLS, snapshot catch-up and directory publication.
use super::common::certs::{config_for, make_pki};
use giggles_core::{
    cluster_node::EngineSettings,
    range_api::{
        ClusterTxnId, LeaseKind, LockRequest, LockTarget, RangeClient, RangeId, RangeLockMode,
        RangeSpace, RangeWrite, ReadDirection, ReadRequest, ReplicaRole, SYSTEM_RANGE, range_key,
    },
};
use giggles_replication::{
    bootstrap::presplit,
    call_codec::{NodeCall, PeerCall},
    client::{LocalRanges, RangeTransport, RoutedRangeClient},
    cluster_node::{ClusterNode, ClusterNodeConfig, RegistryWatcher, query_status, request_move},
    move_range::{MoveOutcome, move_replica},
    registry::{self, NodeRecord, NodeState},
    transport::CallLimits,
};
use std::{
    collections::BTreeMap,
    net::TcpListener,
    ops::Bound,
    path::PathBuf,
    sync::Arc,
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

fn start() -> (
    Root,
    Vec<ClusterNode>,
    Vec<RegistryWatcher>,
    Vec<ClusterNodeConfig>,
) {
    let root = Root(std::env::temp_dir().join(format!(
        "range-move-{}-{}",
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
    let mut configs = Vec::new();
    let nodes: Vec<_> = NODES
        .iter()
        .map(|&node| {
            let mut transport = config_for(&pki, node, [91; 16], &addresses);
            if node != 4 {
                transport.peers.retain(|id, _| *id != 4);
            }
            let config = ClusterNodeConfig {
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
            };
            configs.push(config.clone());
            ClusterNode::start(config).unwrap()
        })
        .collect();
    assert!(nodes[3].host().groups().is_empty());
    nodes[0].bootstrap_directory(READY).unwrap();
    for node in &nodes[..3] {
        node.wait_ready(READY).unwrap();
    }
    assert_eq!(nodes[0].enrol_members(READY).unwrap(), 3);
    assert_eq!(nodes[0].enrol_members(READY).unwrap(), 0);
    let mut watchers: Vec<_> = nodes[..3]
        .iter()
        .map(|node| node.start_registry_watcher(Duration::from_millis(100)))
        .collect();
    registry::enrol(
        &**nodes[0].client(),
        1,
        NodeRecord {
            id: 4,
            state: NodeState::Active,
            address: addresses[&4].clone(),
            fingerprint: pki.nodes[&4].2.clone(),
        },
    )
    .unwrap();
    watchers.push(nodes[3].start_registry_watcher(Duration::from_millis(100)));
    eventually(|| {
        nodes[..3].iter().all(|node| {
            node.transport()
                .calls()
                .call(4, PeerCall::Node(NodeCall::Status), CALL)
                .is_ok()
        })
    });
    (root, nodes, watchers, configs)
}

fn leader(nodes: &[ClusterNode]) -> &ClusterNode {
    let mut id = None;
    eventually(|| {
        id = nodes
            .iter()
            .position(|n| n.host().service(RangeId(2)).is_some());
        id.is_some()
    });
    &nodes[id.unwrap()]
}

fn exercise(move_leader: bool) {
    let (_root, nodes, mut watchers, _) = start();
    let range = RangeId(2);
    if !move_leader && leader(&nodes).node_id() == 3 {
        leader(&nodes)
            .host()
            .group(range)
            .unwrap()
            .transfer_leader(1);
        eventually(|| leader(&nodes).node_id() != 3);
    }
    let from = if move_leader {
        leader(&nodes).node_id()
    } else {
        3
    };
    let client = nodes[0].client();
    let mut key = 9_u64.to_be_bytes().to_vec();
    key.push(1);
    let desc = client
        .descriptor_for(&range_key(RangeSpace::Rows, &key))
        .unwrap();
    let xid = client.lease(LeaseKind::Xid, 1, 1).unwrap().start;
    let txn = ClusterTxnId::new(1, xid);
    let grant = client
        .lock(LockRequest {
            range,
            epoch: desc.epoch,
            txn,
            priority: client.oracle().next(1).unwrap(),
            target: LockTarget::Key {
                space: RangeSpace::Rows,
                key: key.clone(),
            },
            mode: RangeLockMode::Exclusive,
            timeout_ms: Some(5000),
        })
        .unwrap();
    client
        .commit(
            range,
            desc.epoch,
            txn,
            vec![RangeWrite {
                space: RangeSpace::Rows,
                key: key.clone(),
                value: Some(b"moved".to_vec()),
            }],
            None,
            Some(grant.generation),
        )
        .unwrap();
    let deadline = Instant::now() + READY;
    let mut redirects = 0;
    let moved = loop {
        let n = leader(&nodes);
        match move_replica(n, range, from, 4, READY).unwrap() {
            MoveOutcome::Done(desc) => break desc,
            MoveOutcome::NotLeader { .. } => {
                redirects += 1;
                thread::sleep(Duration::from_millis(50));
            }
        }
        assert!(Instant::now() < deadline, "move did not finish");
    };
    if move_leader {
        assert!(redirects > 0);
    }
    assert!(moved.epoch > desc.epoch);
    let mut want = vec![1, 2, 3, 4];
    want.retain(|id| *id != from);
    let mut got: Vec<_> = moved
        .replicas
        .iter()
        .map(|r| {
            assert_eq!(r.role, ReplicaRole::Voter);
            r.node
        })
        .collect();
    got.sort();
    assert_eq!(got, want);
    assert_eq!(
        leader(&nodes)
            .host()
            .store()
            .unwrap()
            .descriptor(range)
            .unwrap(),
        Some(moved.clone())
    );
    assert_eq!(
        move_replica(leader(&nodes), range, from, 4, READY).unwrap(),
        MoveOutcome::Done(moved.clone())
    );
    assert_eq!(
        nodes[3].host().store().unwrap().descriptor(range).unwrap(),
        Some(moved.clone())
    );
    leader(&nodes)
        .host()
        .group(range)
        .unwrap()
        .transfer_leader(4);
    eventually(|| nodes[3].host().service(range).is_some());
    let fresh = RoutedRangeClient::new(
        nodes[3].node_id(),
        nodes[3]
            .layout()
            .iter()
            .find(|d| d.id == SYSTEM_RANGE)
            .unwrap()
            .clone(),
        nodes[3].transport().calls().clone() as Arc<dyn RangeTransport>,
        nodes[3].host().clone() as Arc<dyn LocalRanges>,
    );
    assert_eq!(
        fresh
            .descriptor_for(&range_key(RangeSpace::Rows, &key))
            .unwrap(),
        moved
    );
    let page = fresh
        .read(ReadRequest {
            range,
            epoch: moved.epoch,
            space: RangeSpace::Rows,
            start: Bound::Included(key.clone()),
            end: Bound::Unbounded,
            dir: ReadDirection::Forward,
            ts: fresh.oracle().next(1).unwrap(),
            page_bytes: 65536,
        })
        .unwrap();
    assert!(
        page.pairs
            .iter()
            .any(|(k, v)| k.starts_with(&key) && v.ends_with(b"moved"))
    );
    for watcher in &mut watchers {
        watcher.stop();
    }
    for node in nodes {
        node.stop();
    }
}

#[test]
fn moves_follower_to_unhosted_node() {
    exercise(false);
}

#[test]
fn transfers_leadership_before_moving_leader() {
    exercise(true);
}

#[test]
fn admin_move_redirects_and_resumes_after_leader_restart() {
    let (_root, mut nodes, mut watchers, configs) = start();
    let range = RangeId(2);
    let leader_id = leader(&nodes).node_id();
    let follower = MEMBERS.iter().copied().find(|id| *id != leader_id).unwrap();
    let mut admin = configs[(follower - 1) as usize].transport.clone();
    admin.peers.extend(configs[3].transport.peers.clone());
    let from = follower;
    // An own-certificate connection to this follower must redirect to the leader.
    let reply = request_move(&admin, &[follower], range, from, 4, READY).unwrap();
    assert_eq!(reply.leader, Some(leader_id));
    assert!(reply.step.is_some());
    assert!(!reply.done);
    // Wait for a durable replacement step, not just the background job's Starting
    // reply. The elected successor must resume this membership state.
    let handle = leader(&nodes).host().group(range).unwrap();
    let deadline = Instant::now() + READY;
    while giggles_replication::move_range::move_status(&handle).is_none() {
        assert!(Instant::now() < deadline, "no persisted move step observed");
        thread::sleep(Duration::from_millis(1));
    }
    drop(handle);
    let index = nodes.iter().position(|n| n.node_id() == leader_id).unwrap();
    watchers[index].stop();
    nodes.remove(index).stop();
    let replacement_leader = leader(&nodes).node_id();
    assert_ne!(replacement_leader, leader_id);
    let restarted = ClusterNode::start(configs[(leader_id - 1) as usize].clone()).unwrap();
    let watcher = restarted.start_registry_watcher(Duration::from_millis(100));
    nodes.push(restarted);
    watchers.push(watcher);
    eventually(|| {
        let handle = leader(&nodes).host().group(range).unwrap();
        giggles_replication::replace::status(&handle).is_ok_and(|s| {
            s.reachable.contains(&leader_id)
                && s.reachable.iter().filter(|&&id| id != from).count() >= 2
        })
    });
    let deadline = Instant::now() + READY;
    loop {
        if let Ok(reply) = request_move(&admin, &[replacement_leader], range, from, 4, CALL * 3) {
            assert!(reply.error.is_none(), "{reply:?}");
            if reply.done {
                break;
            }
        }
        assert!(Instant::now() < deadline, "admin move did not resume");
        thread::sleep(Duration::from_millis(100));
    }
    let reports = query_status(&admin, &MEMBERS, CALL).unwrap();
    assert!(
        reports
            .iter()
            .filter_map(|(_, r)| r.as_ref().ok())
            .flat_map(|r| &r.ranges)
            .any(|r| r.descriptor.id == range && r.descriptor.replicas.iter().any(|r| r.node == 4))
    );
    for watcher in &mut watchers {
        watcher.stop();
    }
    for node in nodes {
        node.stop();
    }
}

#[test]
fn registry_read_retries_after_system_range_move() {
    let (_root, nodes, mut watchers, _) = start();
    let old = nodes[0]
        .client()
        .descriptor_for(&range_key(RangeSpace::Catalog, &[registry::REGISTRY_TAG]))
        .unwrap();
    let stale = RoutedRangeClient::new(
        nodes[0].node_id(),
        old.clone(),
        nodes[0].transport().calls().clone() as Arc<dyn RangeTransport>,
        nodes[0].host().clone() as Arc<dyn LocalRanges>,
    );
    stale.learn(&old);
    let expected = registry::read_all(&**nodes[0].client()).unwrap();
    let mut system_leader = None;
    eventually(|| {
        system_leader = nodes
            .iter()
            .find(|n| n.host().service(SYSTEM_RANGE).is_some())
            .map(ClusterNode::node_id);
        system_leader.is_some()
    });
    // Keep the replacement identity stable even if leadership changes mid-move.
    let from = MEMBERS
        .iter()
        .copied()
        .find(|id| Some(*id) != system_leader)
        .unwrap();
    let deadline = Instant::now() + READY;
    let moved = loop {
        let leader = nodes
            .iter()
            .find(|n| n.host().service(SYSTEM_RANGE).is_some());
        if let Some(leader) = leader {
            match move_replica(leader, SYSTEM_RANGE, from, 4, READY).unwrap() {
                MoveOutcome::Done(desc) => break desc,
                MoveOutcome::NotLeader { .. } => {}
            }
        }
        assert!(Instant::now() < deadline, "system move did not finish");
        thread::sleep(Duration::from_millis(50));
    };
    let key = range_key(RangeSpace::Catalog, &[registry::REGISTRY_TAG]);
    assert!(moved.epoch > old.epoch);
    assert_eq!(stale.descriptor_for(&key).unwrap().epoch, old.epoch);
    assert_eq!(registry::read_all(&stale).unwrap(), expected);
    assert_eq!(stale.descriptor_for(&key).unwrap().epoch, moved.epoch);
    // Retirement also scans the directory after the system range changes.
    assert!(
        registry::retire(&stale, 1, 4)
            .unwrap_err()
            .to_string()
            .contains("still a replica")
    );
    for watcher in &mut watchers {
        watcher.stop();
    }
    for node in nodes {
        node.stop();
    }
}
