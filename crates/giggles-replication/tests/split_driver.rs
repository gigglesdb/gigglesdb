//! Split driver end to end (GD-0075 R5-81): three ClusterNodes with real engines and
//! transport; `Splitter` splits a range on its leader and publishes both halves to the
//! directory; a crash between split and publish still routes through StaleEpoch and is
//! repaired by `reconcile`; a routed write racing the split fence has one complete outcome.
use super::common::certs::{config_for, make_pki};
use giggles_core::{
    cluster_node::EngineSettings,
    range_api::{
        ClusterTxnId, Epoch, LeaseKind, LockRequest, LockTarget, RangeClient, RangeDescriptor,
        RangeError, RangeId, RangeLockMode, RangeSpace, RangeWrite, ReadDirection, ReadRequest,
        SYSTEM_RANGE, Timestamp, range_key,
    },
};
use giggles_replication::{
    bootstrap::presplit,
    client::{LocalRanges, MAX_ATTEMPTS, RangeTransport, RoutedRangeClient},
    cluster_node::{ClusterNode, ClusterNodeConfig},
    split::{RANGE_ID_ALLOCATOR, RANGE_ID_BASE, Splitter},
};
use std::{
    collections::BTreeMap,
    net::TcpListener,
    ops::Bound,
    path::PathBuf,
    sync::{
        Arc, Barrier,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const CLUSTER: [u8; 16] = [81; 16];
const NODES: [u64; 3] = [1, 2, 3];
const PARENT: RangeId = RangeId(2);
const DEADLINE: Duration = Duration::from_secs(60);

/// Tests of this file run in parallel in one process: one root each.
static ROOTS: AtomicU64 = AtomicU64::new(0);

struct Root(PathBuf);
impl Drop for Root {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Three nodes over TLS; range 1 = system, range 2 = every user key.
fn start() -> (Root, Vec<ClusterNode>) {
    let root = Root(std::env::temp_dir().join(format!(
        "split-driver-{}-{}-{}",
        std::process::id(),
        ROOTS.fetch_add(1, Ordering::Relaxed),
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
    let nodes: Vec<ClusterNode> = NODES
        .iter()
        .map(|&node| {
            ClusterNode::start(ClusterNodeConfig {
                transport: config_for(&pki, node, CLUSTER, &addresses),
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
    (root, nodes)
}

/// The range key of table `id` (a valid split key).
fn table(id: u64) -> Vec<u8> {
    range_key(RangeSpace::Rows, &id.to_be_bytes())
}
/// Rows user key of row `pk` of table `id`.
fn row(id: u64, pk: u8) -> Vec<u8> {
    let mut key = id.to_be_bytes().to_vec();
    key.push(pk);
    key
}

/// The node whose open leader engine serves `range`.
fn leader(nodes: &[ClusterNode], range: RangeId) -> &ClusterNode {
    let deadline = Instant::now() + DEADLINE;
    loop {
        if let Some(node) = nodes.iter().find(|n| n.host().service(range).is_some()) {
            return node;
        }
        assert!(Instant::now() < deadline, "range {} has no leader", range.0);
        thread::sleep(Duration::from_millis(20));
    }
}

fn splitter(node: &ClusterNode) -> Splitter {
    Splitter::new(
        node.host().clone(),
        node.client().clone() as Arc<dyn RangeClient>,
    )
}

/// A routed client on `node` with an empty route cache.
fn fresh_client(node: &ClusterNode) -> RoutedRangeClient {
    let system = node
        .layout()
        .iter()
        .find(|d| d.id == SYSTEM_RANGE)
        .unwrap()
        .clone();
    RoutedRangeClient::new(
        node.node_id(),
        system,
        node.transport().calls().clone() as Arc<dyn RangeTransport>,
        node.host().clone() as Arc<dyn LocalRanges>,
    )
}

fn rows_read(desc: &RangeDescriptor, user_key: &[u8], ts: Timestamp) -> ReadRequest {
    ReadRequest {
        range: desc.id,
        epoch: desc.epoch,
        space: RangeSpace::Rows,
        start: Bound::Included(user_key.to_vec()),
        end: Bound::Unbounded,
        dir: ReadDirection::Forward,
        ts,
        page_bytes: 1 << 16,
    }
}

/// Routes `user_key` (Rows) through `client` until a read at a fresh timestamp is served.
/// Returns the serving descriptor, the StaleEpoch replies on the way and whether the row
/// exists (its newest version at that timestamp is live).
fn route(client: &dyn RangeClient, user_key: &[u8]) -> (RangeDescriptor, usize, bool) {
    let key = range_key(RangeSpace::Rows, user_key);
    let deadline = Instant::now() + DEADLINE;
    let mut stale = 0;
    loop {
        let attempt = client.descriptor_for(&key).and_then(|desc| {
            let ts = client.oracle().next(1)?;
            let page = client.read(rows_read(&desc, user_key, ts))?;
            Ok((desc, page))
        });
        match attempt {
            Ok((desc, page)) => {
                let found = page.pairs.iter().any(|(k, v)| {
                    k.len() == user_key.len() + 8
                        && k.starts_with(user_key)
                        && v.first() == Some(&0)
                });
                return (desc, stale, found);
            }
            Err(RangeError::StaleEpoch { .. }) => stale += 1,
            Err(RangeError::NotLeader { .. } | RangeError::Unavailable(_)) => {
                thread::sleep(Duration::from_millis(20));
            }
            Err(e) => panic!("routing {user_key:02x?}: {e}"),
        }
        assert!(
            Instant::now() < deadline,
            "routing {user_key:02x?} timed out"
        );
    }
}

/// Retries `call` while the range has no serving leader (elections, engine reopens).
fn retry<T>(mut call: impl FnMut() -> Result<T, RangeError>) -> T {
    let deadline = Instant::now() + DEADLINE;
    loop {
        match call() {
            Ok(value) => return value,
            Err(RangeError::NotLeader { .. } | RangeError::Unavailable(_))
                if Instant::now() < deadline =>
            {
                thread::sleep(Duration::from_millis(20));
            }
            Err(e) => panic!("{e}"),
        }
    }
}

fn txn(client: &dyn RangeClient, node: u64) -> ClusterTxnId {
    let counter = retry(|| client.lease(LeaseKind::Xid, (1 << 32) | node, 1)).start;
    ClusterTxnId::new(node as u16, counter)
}

fn put(user_key: &[u8]) -> RangeWrite {
    RangeWrite {
        space: RangeSpace::Rows,
        key: user_key.to_vec(),
        value: Some(b"v".to_vec()),
    }
}

#[test]
fn split_publishes_both_halves_to_the_directory() {
    let (_root, nodes) = start();
    let l = leader(&nodes, PARENT);
    let (left, right) = splitter(l).split(PARENT, &table(50)).unwrap();
    assert_eq!(
        (left.id, left.epoch, left.end.clone()),
        (PARENT, Epoch(2), Some(table(50)))
    );
    assert!(right.id.0 > RANGE_ID_BASE, "{right:?}");
    assert_eq!(
        (right.epoch, right.start.clone(), right.end.clone()),
        (Epoch(2), table(50), Some(table(u64::MAX - 6)))
    );
    // A client on another node with an empty cache routes both halves through the directory.
    let other = nodes.iter().find(|n| n.node_id() != l.node_id()).unwrap();
    let client = fresh_client(other);
    assert_eq!(
        client
            .descriptor_for(&range_key(RangeSpace::Rows, &row(40, 1)))
            .unwrap(),
        left
    );
    assert_eq!(
        client
            .descriptor_for(&range_key(RangeSpace::Rows, &row(60, 1)))
            .unwrap(),
        right
    );
    for (key, want) in [(row(40, 1), &left), (row(60, 1), &right)] {
        let (desc, stale, _) = route(&client, &key);
        assert_eq!((&desc, stale), (want, 0));
    }
    // Both records are current: nothing left to reconcile.
    assert_eq!(splitter(leader(&nodes, PARENT)).reconcile(None).unwrap(), 0);
    for node in nodes {
        node.stop();
    }
}

#[test]
fn crash_between_split_and_publish_routes_by_stale_epoch_until_reconcile() {
    let (_root, nodes) = start();
    let l = leader(&nodes, PARENT);
    let other = nodes.iter().find(|n| n.node_id() != l.node_id()).unwrap();
    // A client that cached the parent before the split.
    let cached = fresh_client(other);
    let parent = cached.descriptor_for(&table(60)).unwrap();
    assert_eq!((parent.id, parent.epoch), (PARENT, Epoch(1)));
    // The split without its publish.
    let n = retry(|| l.client().lease(LeaseKind::Xid, RANGE_ID_ALLOCATOR, 1)).start;
    let (left, right) = l
        .host()
        .split(PARENT, &table(50), RangeId(RANGE_ID_BASE + n))
        .unwrap();
    let (desc, stale, _) = route(&cached, &row(60, 1));
    assert_eq!(desc, right);
    assert!(
        (1..=MAX_ATTEMPTS).contains(&stale),
        "{stale} StaleEpoch replies"
    );
    // A fresh client still finds the parent's record, and gets to the child the same way.
    let (desc, _, _) = route(&fresh_client(other), &row(60, 1));
    assert_eq!(desc, right);
    // The parent's leader publishes both records; a second pass has nothing to do.
    let s = splitter(leader(&nodes, PARENT));
    assert_eq!(s.reconcile(None).unwrap(), 2);
    assert_eq!(s.reconcile(None).unwrap(), 0);
    let client = fresh_client(other);
    assert_eq!(
        client
            .descriptor_for(&range_key(RangeSpace::Rows, &row(40, 1)))
            .unwrap(),
        left
    );
    assert_eq!(
        client
            .descriptor_for(&range_key(RangeSpace::Rows, &row(60, 1)))
            .unwrap(),
        right
    );
    for node in nodes {
        node.stop();
    }
}

/// Port of giggles-distributed process.rs `concurrent_routed_write_and_split_fence_has_one_
/// complete_outcome`, repeated: the write commits and is in the right child, or fails and is
/// nowhere; afterwards the parent refuses its pre-split epoch with the right child.
#[test]
fn routed_write_racing_the_split_fence_has_one_complete_outcome() {
    let (_root, nodes) = start();
    let mut parent = PARENT;
    let (mut committed, mut failed) = (0, 0);
    for i in 0..20_u64 {
        let at = 10 + 2 * i;
        let key = row(at + 1, i as u8);
        let writer = &nodes[i as usize % nodes.len()];
        let client = writer.client().clone();
        // Route the write to the current parent and take its lock before the race.
        let (route_desc, _, _) = route(&*client, &key);
        assert_eq!(route_desc.id, parent);
        let txn_id = txn(&*client, writer.node_id());
        let grant = client
            .lock(LockRequest {
                range: route_desc.id,
                epoch: route_desc.epoch,
                txn: txn_id,
                priority: client.oracle().next(1).unwrap(),
                target: LockTarget::Key {
                    space: RangeSpace::Rows,
                    key: key.clone(),
                },
                mode: RangeLockMode::Exclusive,
                timeout_ms: Some(5_000),
            })
            .unwrap();
        let s = splitter(leader(&nodes, parent));
        let barrier = Barrier::new(2);
        let (commit, split) = thread::scope(|scope| {
            let write = scope.spawn(|| {
                // The split reconciles and leases before its entry: spread the commit over
                // that time so some commits land before the fence and some after.
                barrier.wait();
                thread::sleep(Duration::from_millis(3 * i));
                client.commit(
                    route_desc.id,
                    route_desc.epoch,
                    txn_id,
                    vec![put(&key)],
                    None,
                    Some(grant.generation),
                )
            });
            let split = scope.spawn(|| {
                barrier.wait();
                let started = Instant::now();
                let split = s.split(parent, &table(at));
                (split, started.elapsed())
            });
            (write.join().unwrap(), split.join().unwrap())
        });
        let ((left, right), took) = (split.0.unwrap(), split.1);
        eprintln!(
            "iteration {i}: commit after {} ms: {:?}; split took {took:?}",
            3 * i,
            commit.as_ref().map(|_| ())
        );
        // Through the directory, at a fresh timestamp, from a client with an empty cache.
        let fresh = fresh_client(&nodes[(i as usize + 1) % nodes.len()]);
        let (desc, _, in_right) = route(&fresh, &key);
        assert_eq!(desc, right);
        // Only there: the left half never shows it.
        let page = retry(|| {
            // The client routes by id only to ranges it has cached.
            assert_eq!(fresh.descriptor_for(&left.start)?, left);
            let ts = fresh.oracle().next(1)?;
            fresh.read(rows_read(&left, &left.start[1..], ts))
        });
        assert!(!page.pairs.iter().any(|(k, _)| k.starts_with(&key)));
        match commit {
            Ok(_) => {
                assert!(in_right, "iteration {i}: committed write missing");
                committed += 1;
            }
            Err(e) => {
                // A proposal at the old epoch refused at apply is a definite sink NotLeader:
                // Aborted (40001) since stack A; Unknown is still accepted (a lost proposal),
                // and no leader changes here, so either way it must not have applied.
                assert!(
                    matches!(
                        e,
                        RangeError::StaleEpoch { .. }
                            | RangeError::Aborted(_)
                            | RangeError::NotLeader { .. }
                            | RangeError::Unavailable(_)
                            | RangeError::Unknown(_)
                    ),
                    "iteration {i}: {e}"
                );
                assert!(!in_right, "iteration {i}: failed write ({e}) is visible");
                failed += 1;
                let _ = client.release(route_desc.id, txn_id);
            }
        }
        // The parent refuses its pre-split epoch and names the right child.
        let deadline = Instant::now() + DEADLINE;
        loop {
            let attempt = client.commit(
                parent,
                route_desc.epoch,
                txn(&*client, writer.node_id()),
                vec![put(&row(at + 1, 200))],
                None,
                None,
            );
            match attempt {
                Err(RangeError::StaleEpoch { descriptor }) => {
                    assert_eq!(descriptor, right, "iteration {i}");
                    break;
                }
                Err(RangeError::NotLeader { .. } | RangeError::Unavailable(_)) => {
                    assert!(Instant::now() < deadline, "iteration {i}: no parent leader");
                    thread::sleep(Duration::from_millis(20));
                }
                other => panic!("iteration {i}: pre-split commit gave {other:?}"),
            }
        }
        parent = right.id;
    }
    eprintln!("split race: {committed} committed, {failed} failed");
    for node in nodes {
        node.stop();
    }
}

#[test]
fn split_refuses_an_enrolled_replacement() {
    use giggles_replication::membership::{AdminCommand, PeerRecord};
    let (_root, nodes) = start();
    let node = leader(&nodes, PARENT);
    let handle = node.host().group(PARENT).unwrap();
    let lost = NODES.into_iter().find(|&id| id != node.node_id()).unwrap();
    handle
        .propose_admin(AdminCommand::Enrol {
            lost,
            peer: PeerRecord {
                id: 4,
                address: "127.0.0.1:4444".into(),
                fingerprint: "04".repeat(32),
            },
        })
        .unwrap();
    assert!(
        handle
            .membership_status()
            .unwrap()
            .membership
            .active
            .is_some()
    );
    assert!(
        splitter(node)
            .split(PARENT, &table(50))
            .unwrap_err()
            .to_string()
            .contains("membership change")
    );
    for node in nodes {
        node.stop();
    }
}

#[test]
fn successive_splits_spread_user_range_leaders() {
    let (_root, nodes) = start();
    let mut parent = PARENT;
    let mut ranges = vec![PARENT];
    for i in 0..6_u64 {
        let (_, right) = splitter(leader(&nodes, parent))
            .split(parent, &table(10 + 2 * i))
            .unwrap();
        parent = right.id;
        ranges.push(parent);
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let mut counts = BTreeMap::<u64, usize>::new();
        for &range in &ranges {
            // Count actual leaders, rather than possibly stale follower observations.
            if let Some(node) = nodes.iter().find(|node| {
                node.host()
                    .group(range)
                    .is_some_and(|h| h.role().state == raft::StateRole::Leader)
            }) {
                *counts.entry(node.node_id()).or_default() += 1;
            }
        }
        if counts.values().sum::<usize>() == ranges.len()
            && counts.len() >= 2
            && counts.values().all(|&n| n < ranges.len())
        {
            break;
        }
        assert!(Instant::now() < deadline, "user range leaders: {counts:?}");
        thread::sleep(Duration::from_millis(20));
    }
    for node in nodes {
        node.stop();
    }
}
