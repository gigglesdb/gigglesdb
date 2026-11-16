//! Per-group snapshots over the shared transport (GD-0075 R5-14): one node's
//! copy of one group is wiped after the leaders compacted their logs; on
//! restart that group catches up by snapshot while another group keeps
//! committing through the same connections.
use super::common::certs::{config_for, make_pki};
use giggles_core::range_api::RangeId;
use giggles_replication::{
    host::{RangeHost, RangeHostConfig, StateFactory},
    sink::{ReplicateError, SnapshotPolicy},
    state::{MemStateMachine, StateMachine},
    transport::{GroupRouter, Transport, TransportNetwork},
};
use raft::StateRole;
use std::{
    collections::{BTreeMap, HashMap},
    net::TcpListener,
    path::Path,
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const CLUSTER: [u8; 16] = [79; 16];
const VOTERS: [u64; 3] = [1, 2, 3];
const WIPED: RangeId = RangeId(21);
const BUSY: RangeId = RangeId(22);

type States = Arc<Mutex<HashMap<(u64, RangeId), Arc<MemStateMachine>>>>;

fn until(what: &str, mut ready: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(60);
    while !ready() {
        assert!(Instant::now() < deadline, "deadline: {what}");
        thread::sleep(Duration::from_millis(10));
    }
}

fn config(root: &Path, node: u64) -> RangeHostConfig {
    RangeHostConfig {
        node_id: node,
        cluster: CLUSTER,
        log_dir: root.join(format!("n{node}/log")),
        snap_dir: root.join(format!("n{node}/snap")),
        tick: Duration::from_millis(20),
        policy: Some(SnapshotPolicy {
            every_entries: 8,
            margin: 2,
        }),
        liveness: None,
    }
}

fn host(root: &Path, node: u64, transport: &Arc<Transport>, states: &States) -> RangeHost {
    // Kept across restarts like a durable store, unless the test removed it.
    let recorded = states.clone();
    let factory: StateFactory = Arc::new(move |range| {
        let mut states = recorded.lock().unwrap();
        let s = states
            .entry((node, range))
            .or_insert_with(|| Arc::new(MemStateMachine::new()))
            .clone();
        s as Arc<dyn StateMachine>
    });
    let listed = [WIPED, BUSY]
        .iter()
        .map(|r| (*r, VOTERS.to_vec()))
        .collect();
    RangeHost::start(
        config(root, node),
        Arc::new(TransportNetwork::new(transport.clone())),
        factory,
        listed,
    )
    .unwrap()
}

fn write(hosts: &[Option<RangeHost>], range: RangeId, payload: Vec<u8>) {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let leader = hosts
            .iter()
            .flatten()
            .filter_map(|h| h.group(range))
            .find(|g| g.role().state == StateRole::Leader);
        if let Some(leader) = leader {
            match leader.replicate(payload.clone()) {
                Ok(()) => return,
                Err(ReplicateError::NotLeader) => {}
                Err(e) => panic!("replicate {range:?}: {e}"),
            }
        }
        assert!(Instant::now() < deadline, "no leader for {range:?}");
        thread::sleep(Duration::from_millis(10));
    }
}

fn batches(states: &States, node: u64, range: RangeId) -> Vec<Vec<u8>> {
    let s = states.lock().unwrap().get(&(node, range)).cloned();
    s.map(|s| s.applied_batches().into_iter().map(|(_, b)| b).collect())
        .unwrap_or_default()
}

#[test]
fn a_wiped_group_catches_up_by_snapshot_while_another_keeps_serving() {
    let root = std::env::temp_dir().join(format!(
        "group-snapshot-{}-{}",
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
    let transports: Vec<Arc<Transport>> = VOTERS
        .iter()
        .map(|&node| {
            Arc::new(
                Transport::start_groups(
                    config_for(&pki, node, CLUSTER, &addresses),
                    Arc::new(GroupRouter::new()),
                )
                .unwrap(),
            )
        })
        .collect();
    let mut hosts: Vec<Option<RangeHost>> = VOTERS
        .iter()
        .zip(&transports)
        .map(|(&node, t)| Some(host(&root, node, t, &states)))
        .collect();

    for i in 0..30u8 {
        write(&hosts, WIPED, vec![1, i]);
        write(&hosts, BUSY, vec![2, i]);
    }
    let wiped_node = 3;
    // Replicas snapshot every 8 entries and keep 2: the early entries are gone.
    let expected_wiped: Vec<Vec<u8>> = batches(&states, 1, WIPED);
    until("node 3 caught up before the wipe", || {
        batches(&states, wiped_node, WIPED) == batches(&states, 1, WIPED)
    });

    // Wipe node 3's copy of WIPED (log, snapshots, state) and restart node 3.
    hosts[2].take().unwrap().stop();
    states.lock().unwrap().remove(&(wiped_node, WIPED));
    std::fs::remove_dir_all(root.join(format!("n{wiped_node}/log/{}", WIPED.0))).unwrap();
    std::fs::remove_dir_all(root.join(format!("n{wiped_node}/snap/{}", WIPED.0))).unwrap();
    hosts[2] = Some(host(&root, wiped_node, &transports[2], &states));

    // As in R4, the leader learns node 3 lost its log on the next append to the
    // group (heartbeats carry no entries): write to WIPED, then BUSY keeps
    // committing while WIPED catches up by snapshot.
    for i in 30..33u8 {
        write(&hosts, WIPED, vec![1, i]);
    }
    for i in 0..20u8 {
        write(&hosts, BUSY, vec![3, i]);
    }
    until("node 3 restored WIPED by snapshot", || {
        let mine = batches(&states, wiped_node, WIPED);
        !mine.is_empty() && mine == batches(&states, 1, WIPED)
    });
    assert!(batches(&states, 1, WIPED).len() >= expected_wiped.len());
    // The first entries were compacted away on every replica, so the restored
    // copy can only have come from a snapshot transfer.
    until("BUSY equal on all nodes", || {
        let first = batches(&states, 1, BUSY);
        VOTERS.iter().all(|&n| batches(&states, n, BUSY) == first)
    });
    drop(hosts);
    drop(transports);
    let _ = std::fs::remove_dir_all(root);
}
