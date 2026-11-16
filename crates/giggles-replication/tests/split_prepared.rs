//! Prepared work across a range split, end to end (GD-0075 R5-82, review B1): three
//! ClusterNodes with real engines and transport. A transaction is prepared as Track D's
//! coordinator does it (giggles-core storage/cluster/coordinator.rs: lock, Open, Seal, prepare
//! and Vote per participant) with writes on both sides of a key K of range B and one on range A,
//! its home. B splits at K (`Splitter::split`) with its right child led by another node than
//! B; the decision and B's finalize then go through a routed client that has never seen the
//! child, so B's leader must route its forward to the child from the lineage record.
use super::common::certs::{config_for, make_pki};
use giggles_core::{
    cluster_node::EngineSettings,
    range_api::{
        ClusterTxnId, HomeOp, HomeReply, HomeStatus, LeaseKind, LockRequest, LockTarget, Outcome,
        RangeClient, RangeDescriptor, RangeError, RangeId, RangeLockMode, RangeSpace, RangeWrite,
        ReadDirection, ReadPage, ReadRequest, SYSTEM_RANGE, Timestamp, range_key,
    },
};
use giggles_replication::{
    bootstrap::presplit,
    client::{LocalRanges, RangeTransport, RoutedRangeClient},
    cluster_node::{ClusterNode, ClusterNodeConfig},
    role::Serving,
    split::Splitter,
};
use std::{
    collections::BTreeMap,
    net::TcpListener,
    ops::Bound,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const CLUSTER: [u8; 16] = [86; 16];
const NODES: [u64; 3] = [1, 2, 3];
/// Range 2 = B (user keys below table HOME_TABLE), range 3 = A (the home, from HOME_TABLE).
const B: RangeId = RangeId(2);
const HOME_TABLE: u64 = 1000;
const DEADLINE: Duration = Duration::from_secs(60);
/// A read or lock blocked by a prepared record asks the home only after this long, so a lock
/// granted sooner proves no prepared record held the key.
const RESOLVE_AFTER: Duration = Duration::from_secs(3);
/// B splits at this table; the transaction writes rows of the tables on either side.
const SPLIT_TABLE: u64 = 50;

/// Tests of this file run in parallel in one process: one root each.
static ROOTS: AtomicU64 = AtomicU64::new(0);

struct Root(PathBuf);
impl Drop for Root {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn start() -> (Root, Vec<ClusterNode>) {
    let root = Root(std::env::temp_dir().join(format!(
        "split-prepared-{}-{}-{}",
        std::process::id(),
        ROOTS.fetch_add(1, Ordering::Relaxed),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )));
    let layout = presplit(&[table(HOME_TABLE)], &NODES, 3).unwrap();
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
                // Home recovery never decides or finishes for the test's coordinator.
                engine: EngineSettings {
                    lease: Duration::from_secs(5),
                    read_wait: Duration::from_secs(20),
                    resolve_after: RESOLVE_AFTER,
                    recover_after: Duration::from_secs(600),
                    finish_after: Duration::from_secs(600),
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

fn table(id: u64) -> Vec<u8> {
    range_key(RangeSpace::Rows, &id.to_be_bytes())
}
/// Rows user key of row `pk` of table `id`.
fn row(id: u64, pk: u8) -> Vec<u8> {
    let mut key = id.to_be_bytes().to_vec();
    key.push(pk);
    key
}
fn rows_key(user_key: &[u8]) -> Vec<u8> {
    range_key(RangeSpace::Rows, user_key)
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

/// Retries `call` while a range has no serving leader or its route moved.
fn retry<T>(mut call: impl FnMut() -> Result<T, RangeError>) -> T {
    let deadline = Instant::now() + DEADLINE;
    loop {
        match call() {
            Ok(value) => return value,
            Err(
                RangeError::NotLeader { .. }
                | RangeError::Unavailable(_)
                | RangeError::StaleEpoch { .. },
            ) if Instant::now() < deadline => thread::sleep(Duration::from_millis(20)),
            Err(e) => panic!("{e}"),
        }
    }
}

/// A transaction id as gateways pack it.
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

/// Commit timestamp of the newest live version of `user_key` in `page` (`user || !ts`).
fn version(page: &ReadPage, user_key: &[u8]) -> Option<Timestamp> {
    let (key, value) = page
        .pairs
        .iter()
        .find(|(k, _)| k.len() == user_key.len() + 8 && k.starts_with(user_key))?;
    let inverted = u64::from_be_bytes(key[user_key.len()..].try_into().unwrap());
    (value.first() == Some(&0)).then_some(Timestamp(!inverted))
}

fn read_at(desc: &RangeDescriptor, user_key: &[u8], ts: Timestamp) -> ReadRequest {
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

/// The range serving `user_key` and its newest live version at a fresh timestamp.
fn newest(client: &RoutedRangeClient, user_key: &[u8]) -> (RangeDescriptor, Option<Timestamp>) {
    retry(|| {
        let desc = client.descriptor_for(&rows_key(user_key))?;
        let ts = client.oracle().next(1)?;
        let page = client.read(read_at(&desc, user_key, ts))?;
        Ok((desc, version(&page, user_key)))
    })
}

/// An exclusive lock on `user_key` for a new transaction: granted well before a blocked
/// request would ask the home, so no prepared record holds the key. Returns the grant's newest
/// committed version.
fn lock_free(client: &RoutedRangeClient, node: u64, user_key: &[u8]) -> Option<Timestamp> {
    let other = txn(client, node);
    let started = Instant::now();
    let (desc, grant) = retry(|| {
        let desc = client.descriptor_for(&rows_key(user_key))?;
        let grant = client.lock(LockRequest {
            range: desc.id,
            epoch: desc.epoch,
            txn: other,
            priority: client.oracle().next(1)?,
            target: LockTarget::Key {
                space: RangeSpace::Rows,
                key: user_key.to_vec(),
            },
            mode: RangeLockMode::Exclusive,
            timeout_ms: Some(1_000),
        })?;
        Ok((desc, grant))
    });
    assert!(
        started.elapsed() < RESOLVE_AFTER,
        "lock waited on a prepared record"
    );
    retry(|| client.release(desc.id, other));
    grant.latest.map(|(ts, _)| ts)
}

fn home(client: &dyn RangeClient, home: &RangeDescriptor, op: HomeOp) -> HomeReply {
    retry(|| client.home(home.id, home.epoch, op.clone()))
}

/// A transaction prepared on A (its home) and on B with writes on both sides of table(at),
/// then B split at table(at).
struct Scenario {
    txn: ClusterTxnId,
    incarnation: u64,
    home: RangeDescriptor,
    /// Row keys: left and right of the split (in B), and the home's.
    keys: [Vec<u8>; 3],
    left: RangeDescriptor,
    right: RangeDescriptor,
    /// Index in `nodes` of B's leader, the right child's leader and the third node.
    at: [usize; 3],
}

/// Track D's coordinator up to the votes (coordinator.rs `commit_cross_range` steps 1-3),
/// with raw RangeClient calls: lock every key, Open and Seal at home, prepare and Vote.
fn prepare(
    client: &RoutedRangeClient,
    node: u64,
    at: u64,
) -> (ClusterTxnId, u64, RangeDescriptor, [Vec<u8>; 3]) {
    let txn = txn(client, node);
    let keys = [row(at - 1, 1), row(at + 1, 1), row(HOME_TABLE + at, 1)];
    let priority = retry(|| client.oracle().next(1));
    let mut participants: Vec<(RangeDescriptor, Vec<RangeWrite>, u64)> = Vec::new();
    for key in &keys {
        let desc = retry(|| client.descriptor_for(&rows_key(key)));
        let grant = retry(|| {
            client.lock(LockRequest {
                range: desc.id,
                epoch: desc.epoch,
                txn,
                priority,
                target: LockTarget::Key {
                    space: RangeSpace::Rows,
                    key: key.clone(),
                },
                mode: RangeLockMode::Exclusive,
                timeout_ms: Some(5_000),
            })
        });
        match participants.iter_mut().find(|(d, _, _)| d.id == desc.id) {
            Some((_, writes, generation)) => {
                assert_eq!(*generation, grant.generation);
                writes.push(put(key));
            }
            None => participants.push((desc, vec![put(key)], grant.generation)),
        }
    }
    // The home is the range of the last key; B (both sides of the split) the other one.
    participants.reverse();
    assert_eq!(participants.len(), 2);
    assert_eq!(participants[1].1.len(), 2);
    let a = participants[0].0.clone();
    let HomeReply::Opened { incarnation } = home(client, &a, HomeOp::Open { txn }) else {
        panic!("Open did not open");
    };
    let sealed = participants.iter().map(|(d, _, _)| d.id).collect();
    home(
        client,
        &a,
        HomeOp::Seal {
            txn,
            incarnation,
            participants: sealed,
        },
    );
    for (desc, writes, generation) in participants {
        let prepared_at = client
            .prepare(desc.id, desc.epoch, txn, a.id, writes, Some(generation))
            .unwrap();
        let vote = HomeOp::Vote {
            txn,
            incarnation,
            participant: desc.id,
            prepared_at,
        };
        home(client, &a, vote);
    }
    (txn, incarnation, a, keys)
}

/// Decide `outcome` at home, finalize B (by id) and A, then
/// Forget; the home then knows nothing of the transaction.
fn finish(client: &RoutedRangeClient, s: &Scenario, outcome: Outcome) {
    let decide = HomeOp::Decide {
        txn: s.txn,
        incarnation: s.incarnation,
        outcome: outcome.clone(),
    };
    // By-id calls need the range in the route cache: the directory's records of A and of
    // the left half (the child is never looked up here).
    assert_eq!(retry(|| client.descriptor_for(&s.home.start)), s.home);
    assert_eq!(home(client, &s.home, decide), HomeReply::Ok);
    assert_eq!(retry(|| client.descriptor_for(&s.left.start)), s.left);
    retry(|| client.finalize(s.left.id, s.txn, outcome.clone()));
    retry(|| client.finalize(s.home.id, s.txn, outcome.clone()));
    let forget = HomeOp::Forget {
        txn: s.txn,
        incarnation: s.incarnation,
    };
    assert_eq!(home(client, &s.home, forget), HomeReply::Ok);
    let status = home(client, &s.home, HomeOp::Status { txn: s.txn });
    assert_eq!(status, HomeReply::Status(HomeStatus::Unknown));
}

/// Prepares a transaction, splits B at table(SPLIT_TABLE) on B's leader and, when the right
/// child elected the same node (B's leader applies the split first, so it often wins), hands
/// the child's leadership to another voter: B's and the child's leaders differ
/// (`RangeHost::serving`). B's leader splits through a client of its own, so its node's routed
/// client (the one its engines forward with) has never seen the child.
fn prepared_split(nodes: &[ClusterNode]) -> Scenario {
    let coordinator = &nodes[0];
    let client = fresh_client(coordinator);
    let (txn, incarnation, home, keys) = prepare(&client, coordinator.node_id(), SPLIT_TABLE);
    let x = leader(nodes, B);
    let splitter = Splitter::new(x.host().clone(), Arc::new(fresh_client(x)));
    let (left, right) = splitter.split(B, &table(SPLIT_TABLE)).unwrap();
    assert_eq!((left.id, right.start.clone()), (B, table(SPLIT_TABLE)));
    let index = |node: &ClusterNode| nodes.iter().position(|n| n.node_id() == node.node_id());
    let xi = index(leader(nodes, B)).unwrap();
    let deadline = Instant::now() + DEADLINE;
    let yi = loop {
        let yi = index(leader(nodes, right.id)).unwrap();
        if yi != xi {
            break yi;
        }
        assert!(Instant::now() < deadline, "the child stays on B's leader");
        let to = nodes[(xi + 1) % nodes.len()].node_id();
        nodes[yi]
            .host()
            .group(right.id)
            .unwrap()
            .transfer_leader(to);
        thread::sleep(Duration::from_millis(200));
    };
    let leads =
        |i: usize, range| matches!(nodes[i].host().serving(range), Some(Serving::Leader { .. }));
    assert!(leads(xi, B) && leads(yi, right.id));
    eprintln!(
        "B led by node {}, child {} by node {}",
        nodes[xi].node_id(),
        right.id.0,
        nodes[yi].node_id()
    );
    Scenario {
        txn,
        incarnation,
        home,
        keys,
        left,
        right,
        at: [xi, yi, (0..3).find(|&i| i != xi && i != yi).unwrap()],
    }
}

#[test]
fn finalize_reaches_the_split_child_on_another_node() {
    let (_root, nodes) = start();
    let s = prepared_split(&nodes);
    // The coordinator's client is on the third node and has never seen the child.
    let client = fresh_client(&nodes[s.at[2]]);
    let c = retry(|| client.oracle().next(1));
    finish(&client, &s, Outcome::Commit(c));
    // Both halves (and the home) show the writes at c through a fresh client, after Forget:
    // a share still prepared would be presumed aborted here.
    let fresh = fresh_client(&nodes[s.at[0]]);
    for (key, range) in s.keys.iter().zip([&s.left, &s.right, &s.home]) {
        let (desc, found) = newest(&fresh, key);
        assert_eq!((&desc, found), (range, Some(c)), "{key:02x?}");
        assert_eq!(lock_free(&fresh, nodes[s.at[0]].node_id(), key), Some(c));
    }
    for node in nodes {
        node.stop();
    }
}

#[test]
fn abort_across_the_split_leaves_no_writes_and_frees_the_right_half() {
    let (_root, nodes) = start();
    let s = prepared_split(&nodes);
    let client = fresh_client(&nodes[s.at[2]]);
    finish(&client, &s, Outcome::Abort);
    let fresh = fresh_client(&nodes[s.at[1]]);
    for key in &s.keys {
        assert_eq!(newest(&fresh, key).1, None, "{key:02x?}");
    }
    // Another transaction's lock on a right-half key is granted at once.
    let (desc, _) = newest(&fresh, &s.keys[1]);
    assert_eq!(desc, s.right);
    assert_eq!(
        lock_free(&fresh, nodes[s.at[1]].node_id(), &s.keys[1]),
        None
    );
    for node in nodes {
        node.stop();
    }
}

#[test]
fn reader_of_the_right_half_waits_for_the_decision_and_sees_the_commit() {
    let (_root, nodes) = start();
    let s = prepared_split(&nodes);
    let client = fresh_client(&nodes[s.at[2]]);
    // c is taken after every vote (coordinator step 4); the reader's T after it, so T >= c > p.
    let c = retry(|| client.oracle().next(1));
    let reader_client = fresh_client(&nodes[s.at[0]]);
    let right_key = s.keys[1].clone();
    let t = retry(|| reader_client.oracle().next(1));
    let desc = retry(|| reader_client.descriptor_for(&rows_key(&right_key)));
    assert_eq!(desc, s.right);
    thread::scope(|scope| {
        let reader = scope.spawn(|| {
            let page = retry(|| reader_client.read(read_at(&desc, &right_key, t)));
            (Instant::now(), version(&page, &right_key))
        });
        thread::sleep(Duration::from_millis(500));
        assert!(
            !reader.is_finished(),
            "the reader did not wait for the prepared share"
        );
        let decided = Instant::now();
        finish(&client, &s, Outcome::Commit(c));
        let (returned, found) = reader.join().unwrap();
        assert!(returned >= decided);
        assert_eq!(found, Some(c));
    });
    for node in nodes {
        node.stop();
    }
}
