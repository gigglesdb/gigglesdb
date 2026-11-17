//! A replica move preserves acknowledged writes and in-flight prepared transactions.
use super::common::certs::{config_for, make_pki};
use giggles_core::{
    cluster_node::EngineSettings,
    range_api::{
        ClusterTxnId, HomeOp, HomeReply, LeaseKind, LockRequest, LockTarget, Outcome, RangeClient,
        RangeError, RangeId, RangeLockMode, RangeSpace, RangeWrite, ReadDirection, ReadRequest,
        ReplicaRole, SYSTEM_RANGE, range_key,
    },
};
use giggles_replication::{
    bootstrap::presplit,
    call_codec::{NodeCall, PeerCall},
    client::{LocalRanges, RangeTransport, RoutedRangeClient},
    cluster_node::{ClusterNode, ClusterNodeConfig, RegistryWatcher},
    error::Error,
    move_range::{MoveOutcome, move_replica},
    registry::{self, NodeRecord, NodeState},
    transport::CallLimits,
};
use std::{
    collections::BTreeMap,
    net::TcpListener,
    ops::Bound,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const MEMBERS: [u64; 3] = [1, 2, 3];
const NODES: [u64; 4] = [1, 2, 3, 4];
const READY: Duration = Duration::from_secs(30);
const CALL: Duration = Duration::from_secs(1);

struct Root(PathBuf);
impl Drop for Root {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn eventually(mut ready: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if ready() {
            return;
        }
        assert!(Instant::now() < deadline, "registry propagation timed out");
        thread::sleep(Duration::from_millis(50));
    }
}

// Retry routing/election failures without hiding ambiguous transaction outcomes.
fn retry<T>(mut call: impl FnMut() -> Result<T, RangeError>) -> Result<T, RangeError> {
    let deadline = Instant::now() + READY;
    loop {
        match call() {
            Err(
                RangeError::NotLeader { .. }
                | RangeError::Unavailable(_)
                | RangeError::StaleEpoch { .. },
            ) if Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(5));
            }
            result => return result,
        }
    }
}

fn start() -> (Root, Vec<ClusterNode>, Vec<RegistryWatcher>) {
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
    let layout = presplit(
        &[range_key(RangeSpace::Rows, &2_u64.to_be_bytes())],
        &MEMBERS,
        3,
    )
    .unwrap();
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
        nodes[0]
            .transport()
            .calls()
            .call(4, PeerCall::Node(NodeCall::Status), CALL)
            .is_ok()
    });
    (root, nodes, watchers)
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

// Ensure scoped writers exit even when the scenario panics.
struct StopWriters<'a>(&'a AtomicBool);
impl Drop for StopWriters<'_> {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

const A: RangeId = RangeId(2);
const B: RangeId = RangeId(3);

fn key(table: u64, writer: u8, seq: u64) -> Vec<u8> {
    [
        table.to_be_bytes().as_slice(),
        &[writer],
        &seq.to_be_bytes(),
    ]
    .concat()
}

fn lock(
    client: &dyn RangeClient,
    txn: ClusterTxnId,
    key: &[u8],
) -> Result<(giggles_core::range_api::RangeDescriptor, u64), RangeError> {
    let desc = retry(|| client.descriptor_for(&range_key(RangeSpace::Rows, key)))?;
    let grant = client.lock(LockRequest {
        range: desc.id,
        epoch: desc.epoch,
        txn,
        priority: retry(|| client.oracle().next(1))?,
        target: LockTarget::Key {
            space: RangeSpace::Rows,
            key: key.to_vec(),
        },
        mode: RangeLockMode::Exclusive,
        timeout_ms: Some(5000),
    })?;
    Ok((desc, grant.generation))
}

fn write(key: Vec<u8>, value: Vec<u8>) -> Vec<RangeWrite> {
    vec![RangeWrite {
        space: RangeSpace::Rows,
        key,
        value: Some(value),
    }]
}

fn retryable(error: &RangeError) -> bool {
    matches!(
        error,
        RangeError::StaleEpoch { .. }
            | RangeError::NotLeader { .. }
            | RangeError::Unavailable(_)
            | RangeError::Aborted(_)
    )
}

// Read all pages at one fresh timestamp. Raw MVCC keys have a version suffix.
fn rows(client: &dyn RangeClient) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let desc = retry(|| client.descriptor_for(&range_key(RangeSpace::Rows, &1_u64.to_be_bytes())))
        .unwrap();
    let mut req = ReadRequest {
        range: A,
        epoch: desc.epoch,
        space: RangeSpace::Rows,
        start: Bound::Included(1_u64.to_be_bytes().to_vec()),
        end: Bound::Excluded(2_u64.to_be_bytes().to_vec()),
        dir: ReadDirection::Forward,
        ts: retry(|| client.oracle().next(1)).unwrap(),
        page_bytes: 65536,
    };
    let mut rows = BTreeMap::new();
    loop {
        let page = retry(|| {
            req.epoch = client
                .descriptor_for(&range_key(RangeSpace::Rows, &1_u64.to_be_bytes()))?
                .epoch;
            client.read(req.clone())
        })
        .unwrap();
        for (key, value) in page.pairs {
            rows.insert(key[..17].to_vec(), value);
        }
        match page.resume {
            Some(bound) => req.start = bound,
            None => return rows,
        }
    }
}

#[test]
fn preserves_writes_and_prepared_transactions_during_move() {
    let (_root, nodes, mut watchers) = start();
    let client = nodes[0].client();
    let home_key = key(2, 0, 0);
    let txn = ClusterTxnId::new(
        1,
        retry(|| client.lease(LeaseKind::Xid, 1, 1)).unwrap().start,
    );
    let (home, generation) = retry(|| lock(&**client, txn, &home_key)).unwrap();
    assert_eq!(home.id, B);
    retry(|| {
        client.commit(
            B,
            home.epoch,
            txn,
            write(home_key.clone(), b"home".to_vec()),
            None,
            Some(generation),
        )
    })
    .unwrap();
    let stop = AtomicBool::new(false);
    let progress = [AtomicUsize::new(0), AtomicUsize::new(0)];
    let (acked, maybe) = thread::scope(|scope| {
        let writers: Vec<_> = (0..2)
            .map(|id| {
                let stop = &stop;
                let progress = &progress[id];
                let client = client.clone();
                scope.spawn(move || {
                    let mut acked = BTreeMap::new();
                    let mut maybe = Vec::new();
                    let mut seq = 0;
                    while !stop.load(Ordering::SeqCst) {
                        let key = key(1, id as u8, seq);
                        let value = format!("writer-{id}-{seq}").into_bytes();
                        let deadline = Instant::now() + READY;
                        loop {
                            // Every attempt has a fresh transaction id and descriptor.
                            let txn = ClusterTxnId::new(
                                1,
                                retry(|| client.lease(LeaseKind::Xid, 1, 1)).unwrap().start,
                            );
                            let result =
                                lock(&*client, txn, &key).and_then(|(desc, generation)| {
                                    assert_eq!(desc.id, A);
                                    client.commit(
                                        A,
                                        desc.epoch,
                                        txn,
                                        write(key.clone(), value.clone()),
                                        None,
                                        Some(generation),
                                    )
                                });
                            match result {
                                Ok(_) => {
                                    acked.insert(key.clone(), value.clone());
                                    progress.fetch_add(1, Ordering::SeqCst);
                                    break;
                                }
                                Err(RangeError::Unknown(_)) => {
                                    maybe.push((key.clone(), value.clone()));
                                    break;
                                }
                                Err(e) if retryable(&e) => {
                                    let _ = client.release(A, txn);
                                    assert!(
                                        Instant::now() < deadline,
                                        "writer {id} stuck at {key:02x?}: {e}"
                                    );
                                    thread::sleep(Duration::from_millis(5));
                                }
                                Err(e) => panic!("writer {id}, key {key:02x?}: {e}"),
                            }
                        }
                        seq += 1;
                        thread::sleep(Duration::from_millis(5));
                    }
                    (acked, maybe)
                })
            })
            .collect();
        let _stop_on_unwind = StopWriters(&stop);
        let mut prepared = Vec::new();
        for seq in 0..3 {
            let txn = ClusterTxnId::new(
                1,
                retry(|| client.lease(LeaseKind::Xid, 1, 1)).unwrap().start,
            );
            let incarnation =
                match retry(|| client.home(B, home.epoch, HomeOp::Open { txn })).unwrap() {
                    HomeReply::Opened { incarnation } => incarnation,
                    reply => panic!("unexpected Open: {reply:?}"),
                };
            retry(|| {
                client.home(
                    B,
                    home.epoch,
                    HomeOp::Seal {
                        txn,
                        incarnation,
                        participants: vec![A],
                    },
                )
            })
            .unwrap();
            let key = key(1, 2, seq);
            let value = format!("prepared-{seq}").into_bytes();
            let (desc, generation) = retry(|| lock(&**client, txn, &key)).unwrap();
            let prepared_at = retry(|| {
                client.prepare(
                    A,
                    desc.epoch,
                    txn,
                    B,
                    write(key.clone(), value.clone()),
                    Some(generation),
                )
            })
            .unwrap();
            retry(|| {
                client.home(
                    B,
                    home.epoch,
                    HomeOp::Vote {
                        txn,
                        incarnation,
                        participant: A,
                        prepared_at,
                    },
                )
            })
            .unwrap();
            prepared.push((txn, incarnation, key, value));
        }
        eventually(|| progress.iter().all(|n| n.load(Ordering::SeqCst) >= 2));
        let before: Vec<_> = progress.iter().map(|n| n.load(Ordering::SeqCst)).collect();
        eprintln!("prepared before move: {prepared:02x?}");
        let deadline = Instant::now() + READY;
        let moved = loop {
            match move_replica(
                leader(&nodes),
                A,
                3,
                4,
                deadline.saturating_duration_since(Instant::now()),
            ) {
                Ok(MoveOutcome::Done(desc)) => break desc,
                Ok(MoveOutcome::NotLeader { leader: id }) => {
                    eprintln!("move redirected to {id}");
                }
                Err(Error::NotLeader) => {}
                // The move driver wraps transport errors in Apply and system-range
                // errors in Core; retry only explicitly transient failures.
                Err(Error::Apply(reason) | Error::Core(reason))
                    if reason.starts_with("unavailable:")
                        || reason.starts_with("not leader")
                        || reason.starts_with("stale epoch") =>
                {
                    eprintln!("move retry: {reason}");
                }
                Err(error) => panic!("move failed: {error}"),
            }
            assert!(Instant::now() < deadline, "move stuck");
            thread::sleep(Duration::from_millis(20));
        };
        let mut voters: Vec<_> = moved
            .replicas
            .iter()
            .map(|r| {
                assert_eq!(r.role, ReplicaRole::Voter);
                r.node
            })
            .collect();
        voters.sort_unstable();
        assert_eq!(voters, [1, 2, 4]);
        assert_eq!(
            leader(&nodes)
                .host()
                .store()
                .unwrap()
                .descriptor(A)
                .unwrap(),
            Some(moved)
        );
        for (seq, (txn, incarnation, _, _)) in prepared.iter().enumerate() {
            let outcome = if seq < 2 {
                Outcome::Commit(retry(|| client.oracle().next(1)).unwrap())
            } else {
                Outcome::Abort
            };
            retry(|| {
                client.home(
                    B,
                    home.epoch,
                    HomeOp::Decide {
                        txn: *txn,
                        incarnation: *incarnation,
                        outcome: outcome.clone(),
                    },
                )
            })
            .unwrap();
            retry(|| client.finalize(A, *txn, outcome.clone())).unwrap();
        }
        eventually(|| {
            progress
                .iter()
                .zip(&before)
                .all(|(n, old)| n.load(Ordering::SeqCst) > *old)
        });
        stop.store(true, Ordering::SeqCst);
        let mut acked = BTreeMap::new();
        let mut maybe = Vec::new();
        for writer in writers {
            let (a, m) = writer.join().unwrap();
            acked.extend(a);
            maybe.extend(m);
        }
        for (seq, (_, _, key, value)) in prepared.into_iter().enumerate() {
            if seq < 2 {
                acked.insert(key, value);
            } else {
                assert!(!rows(&**client).contains_key(&key), "aborted row present");
            }
        }
        (acked, maybe)
    });
    let verify = |client: &dyn RangeClient| {
        let rows = rows(client);
        for (key, value) in &acked {
            assert!(
                rows.get(key).is_some_and(|v| v.ends_with(value)),
                "lost acknowledged/prepared row {key:02x?}"
            );
        }
        assert!(!rows.contains_key(&key(1, 2, 2)), "aborted row present");
    };
    verify(&**client);
    leader(&nodes).host().group(A).unwrap().transfer_leader(4);
    eventually(|| nodes[3].host().service(A).is_some());
    let fresh = RoutedRangeClient::new(
        4,
        nodes[3]
            .layout()
            .iter()
            .find(|d| d.id == SYSTEM_RANGE)
            .unwrap()
            .clone(),
        nodes[3].transport().calls().clone() as Arc<dyn RangeTransport>,
        nodes[3].host().clone() as Arc<dyn LocalRanges>,
    );
    verify(&fresh);
    eprintln!(
        "verified {} acknowledged/prepared rows; {} maybe writes",
        acked.len(),
        maybe.len()
    );
    for watcher in &mut watchers {
        watcher.stop();
    }
    for node in nodes {
        node.stop();
    }
}
