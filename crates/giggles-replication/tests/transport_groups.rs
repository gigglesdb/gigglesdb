//! Many Raft groups over one TLS transport (GD-0075 R5-11).
use super::common::certs::{config_for, make_pki};
use giggles_core::range_api::RangeId;
use giggles_replication::{
    host::{RangeHost, RangeHostConfig, StateFactory},
    sink::{ReplicaHandle, ReplicateError},
    state::{MemStateMachine, StateMachine},
    transport::{GroupRouter, Transport, TransportNetwork},
};
use raft::{
    StateRole,
    eraftpb::{Message, MessageType},
};
use std::{
    collections::{BTreeMap, HashMap},
    net::TcpListener,
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

type States = Arc<Mutex<HashMap<(u64, RangeId), Arc<MemStateMachine>>>>;

const CLUSTER: [u8; 16] = [76; 16];
const RANGES: [RangeId; 2] = [RangeId(11), RangeId(12)];
const VOTERS: [u64; 3] = [1, 2, 3];

fn until(what: &str, mut ready: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !ready() {
        assert!(Instant::now() < deadline, "deadline: {what}");
        thread::sleep(Duration::from_millis(10));
    }
}

fn leader(hosts: &[RangeHost], range: RangeId) -> ReplicaHandle {
    let mut found = None;
    until(&format!("leader of range {}", range.0), || {
        found = hosts
            .iter()
            .filter_map(|h| h.group(range))
            .find(|g| g.role().state == StateRole::Leader);
        found.is_some()
    });
    found.unwrap()
}

fn replicate(hosts: &[RangeHost], range: RangeId, payload: &[u8]) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match leader(hosts, range).replicate(payload.to_vec()) {
            Ok(()) => return,
            Err(ReplicateError::NotLeader) if Instant::now() < deadline => {}
            Err(e) => panic!("replicate on range {}: {e}", range.0),
        }
    }
}

/// Every node's state for `range` holds exactly `batches`.
fn wait_applied(states: &States, range: RangeId, batches: &[&[u8]]) {
    until(&format!("range {} applied {batches:?}", range.0), || {
        VOTERS.iter().all(|node| {
            let s = states.lock().unwrap().get(&(*node, range)).cloned();
            s.is_some_and(|s| {
                let applied: Vec<Vec<u8>> =
                    s.applied_batches().into_iter().map(|(_, b)| b).collect();
                applied == batches.iter().map(|b| b.to_vec()).collect::<Vec<_>>()
            })
        })
    });
}

#[test]
fn two_groups_share_one_connection_and_unknown_groups_are_dropped() {
    let root = std::env::temp_dir().join(format!(
        "transport-groups-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let pki = make_pki(&root.join("pki"), &VOTERS);
    let listeners: Vec<_> = VOTERS
        .iter()
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let addresses: BTreeMap<_, _> = VOTERS
        .iter()
        .zip(&listeners)
        .map(|(id, l)| (*id, l.local_addr().unwrap().to_string()))
        .collect();
    drop(listeners);
    let states: States = Arc::default();
    let mut transports = Vec::new();
    let hosts: Vec<RangeHost> = VOTERS
        .iter()
        .map(|&node| {
            let transport = Arc::new(
                Transport::start_groups(
                    config_for(&pki, node, CLUSTER, &addresses),
                    Arc::new(GroupRouter::new()),
                )
                .unwrap(),
            );
            transports.push(transport.clone());
            let recorded = states.clone();
            let factory: StateFactory = Arc::new(move |range| {
                let s = Arc::new(MemStateMachine::new());
                recorded.lock().unwrap().insert((node, range), s.clone());
                s as Arc<dyn StateMachine>
            });
            let config = RangeHostConfig {
                node_id: node,
                cluster: CLUSTER,
                log_dir: root.join(format!("n{node}/log")),
                snap_dir: root.join(format!("n{node}/snap")),
                tick: Duration::from_millis(20),
                policy: None,
                liveness: None,
            };
            let listed = RANGES.iter().map(|r| (*r, VOTERS.to_vec())).collect();
            let network = Arc::new(TransportNetwork::new(transport));
            RangeHost::start(config, network, factory, listed).unwrap()
        })
        .collect();
    for t in &transports {
        assert_eq!(t.router().groups(), vec![11, 12]);
    }

    // Each group elects and replicates its own batch; no other group sees it.
    for range in RANGES {
        replicate(&hosts, range, &[range.0 as u8; 4]);
    }
    for range in RANGES {
        wait_applied(&states, range, &[&[range.0 as u8; 4]]);
    }
    // Frames that arrived while a host was still registering its groups at start
    // were dropped and counted; count from here.
    let base: Vec<u64> = transports
        .iter()
        .map(|t| t.router().dropped_unknown_group())
        .collect();

    // A frame for a group the receiver does not host is dropped and counted.
    transports[0].group_outbox(99).send(Message {
        from: 1,
        to: 2,
        msg_type: MessageType::MsgHeartbeat,
        ..Default::default()
    });
    until("group 99 frame counted", || {
        transports[1].router().dropped_unknown_group() == base[1] + 1
    });

    // Node 3 stops routing group 12: its peers' group 12 traffic is dropped
    // and counted, while group 11 keeps committing over the same connections.
    transports[2].router().unregister(12);
    until("group 12 frames counted on node 3", || {
        transports[2].router().dropped_unknown_group() > base[2]
    });
    replicate(&hosts, RANGES[0], b"second");
    wait_applied(&states, RANGES[0], &[&[11; 4], b"second"]);
    wait_applied(&states, RANGES[1], &[&[12; 4]]);
    assert_eq!(transports[1].router().dropped_unknown_group(), base[1] + 1);

    drop(hosts);
    drop(transports);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn single_group_start_is_group_zero() {
    let root = std::env::temp_dir().join(format!(
        "transport-groups-zero-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let pki = make_pki(&root.join("pki"), &[1]);
    let addresses = BTreeMap::from([(1, "127.0.0.1:0".to_string())]);
    let sink = Arc::new(giggles_replication::transport::LateSink::<ReplicaHandle>::new());
    let transport = Transport::start(config_for(&pki, 1, CLUSTER, &addresses), sink, None).unwrap();
    assert_eq!(transport.router().groups(), vec![0]);
    drop(transport);
    let _ = std::fs::remove_dir_all(&root);
}
