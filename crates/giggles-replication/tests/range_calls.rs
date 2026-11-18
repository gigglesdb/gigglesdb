//! Range and gateway calls over the TLS peer transport (GD-0075 R5-21).
use super::common::certs::{config_for, make_pki};
use giggles_core::range_api::*;
use giggles_replication::{
    call_codec::{GatewayCall, PeerCall},
    client::{LocalRanges, RangeCall, RangeReply, RangeTransport},
    transport::{CallLimits, GroupRouter, Transport},
};
use std::{
    collections::{BTreeMap, HashMap},
    net::TcpListener,
    ops::Bound,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const CLUSTER: [u8; 16] = [77; 16];
/// Served normally.
const ECHO: RangeId = RangeId(5);
/// Not led here; the fake knows the leader is node 3.
const MOVED: RangeId = RangeId(6);
/// Every call blocks on the test's gate.
const SLOW: RangeId = RangeId(7);

/// A handler announces itself on `entered`, then waits for one message on `release`.
struct Gate {
    entered: Mutex<mpsc::Sender<()>>,
    release: Mutex<mpsc::Receiver<()>>,
}

struct FakeRange {
    desc: RangeDescriptor,
    hint: Option<u64>,
    gate: Option<Gate>,
}

impl FakeRange {
    fn serve(&self) -> Result<(), RangeError> {
        if let Some(gate) = &self.gate {
            gate.entered.lock().unwrap().send(()).unwrap();
            gate.release.lock().unwrap().recv().unwrap();
        }
        if self.desc.id == MOVED {
            return Err(RangeError::NotLeader { hint: self.hint });
        }
        Ok(())
    }
}

impl RangeService for FakeRange {
    fn descriptor(&self) -> RangeDescriptor {
        self.desc.clone()
    }
    fn read(&self, req: ReadRequest) -> Result<ReadPage, RangeError> {
        self.serve()?;
        Ok(ReadPage {
            pairs: vec![(b"k".to_vec(), req.ts.0.to_be_bytes().to_vec())],
            resume: None,
            range_end: self.desc.end.clone(),
        })
    }
    fn lock(&self, _: LockRequest) -> Result<LockGrant, RangeError> {
        self.serve().map(|()| LockGrant {
            latest: None,
            generation: 0,
        })
    }
    fn blockers(&self, _: ClusterTxnId) -> Result<Vec<ClusterTxnId>, RangeError> {
        self.serve().map(|()| vec![])
    }
    fn cancel_wait(&self, _: ClusterTxnId) -> Result<(), RangeError> {
        self.serve()
    }
    fn release(&self, _: ClusterTxnId) -> Result<(), RangeError> {
        self.serve()
    }
    fn heartbeat(&self, _: ClusterTxnId) -> Result<(), RangeError> {
        self.serve()
    }
    fn commit(
        &self,
        epoch: Epoch,
        _: ClusterTxnId,
        writes: Vec<RangeWrite>,
        _: Option<u64>,
        _: Option<u64>,
    ) -> Result<Timestamp, RangeError> {
        self.serve()
            .map(|()| Timestamp(epoch.0 * 100 + writes.len() as u64))
    }
    fn prepare(
        &self,
        _: Epoch,
        _: ClusterTxnId,
        _: RangeId,
        _: Vec<RangeWrite>,
        _: Option<u64>,
    ) -> Result<Timestamp, RangeError> {
        self.serve().map(|()| Timestamp(8))
    }
    fn finalize(&self, _: ClusterTxnId, _: Outcome) -> Result<(), RangeError> {
        self.serve()
    }
    fn home(&self, _: Epoch, _: HomeOp) -> Result<HomeReply, RangeError> {
        self.serve().map(|()| HomeReply::Ok)
    }
}

struct FakeNode(HashMap<RangeId, Arc<FakeRange>>);

impl LocalRanges for FakeNode {
    fn service(&self, range: RangeId) -> Option<Arc<dyn RangeService>> {
        let service = self.0.get(&range).cloned()?;
        Some(service)
    }
    fn system(&self) -> Option<Arc<dyn SystemService>> {
        None
    }
}

#[derive(Default)]
struct FakeGateway(Mutex<Vec<String>>);

impl GatewayService for FakeGateway {
    fn probe(&self, probe: DeadlockProbe) {
        let line = format!("probe {} -> {}", probe.initiator.0, probe.target.0);
        self.0.lock().unwrap().push(line);
    }
    fn abort(&self, victim: ClusterTxnId) {
        self.0.lock().unwrap().push(format!("abort {}", victim.0));
    }
    fn blockers(&self, txn: ClusterTxnId) -> Vec<ClusterTxnId> {
        vec![ClusterTxnId(txn.0 + 1), ClusterTxnId(txn.0 + 2)]
    }
    fn cancel_wait(&self, txn: ClusterTxnId) {
        self.0
            .lock()
            .unwrap()
            .push(format!("cancel_wait {}", txn.0));
    }
}

fn desc(range: RangeId) -> RangeDescriptor {
    RangeDescriptor {
        id: range,
        epoch: Epoch(1),
        start: vec![range.0 as u8],
        end: Some(vec![range.0 as u8 + 1]),
        replicas: vec![ReplicaDesc {
            node: 2,
            role: ReplicaRole::Voter,
        }],
    }
}

/// Node 2's leaders: ECHO, MOVED (`NotLeader { hint }`) and SLOW (gated). Returns the
/// gate's `entered` receiver and `release` sender.
fn node_two(hint: Option<u64>) -> (Arc<FakeNode>, mpsc::Receiver<()>, mpsc::Sender<()>) {
    let (entered_tx, entered) = mpsc::channel();
    let (release, release_rx) = mpsc::channel();
    let gate = Gate {
        entered: Mutex::new(entered_tx),
        release: Mutex::new(release_rx),
    };
    let range = |id, hint, gate| {
        let fake = FakeRange {
            desc: desc(id),
            hint,
            gate,
        };
        (id, Arc::new(fake))
    };
    let ranges = [range(ECHO, None, None), range(MOVED, hint, None)];
    let mut ranges: HashMap<_, _> = ranges.into_iter().collect();
    let (id, slow) = range(SLOW, None, Some(gate));
    ranges.insert(id, slow);
    (Arc::new(FakeNode(ranges)), entered, release)
}

/// Counts actual TCP connections without changing the TLS peer protocol.
struct Connects {
    count: Arc<AtomicUsize>,
    address: String,
    _runtime: tokio::runtime::Runtime,
}

impl Connects {
    fn new(upstream: String) -> Self {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        listener.set_nonblocking(true).unwrap();
        let count = Arc::new(AtomicUsize::new(0));
        let accepted = count.clone();
        let listener = {
            let _entered = runtime.enter();
            tokio::net::TcpListener::from_std(listener).unwrap()
        };
        runtime.spawn(async move {
            while let Ok((mut client, _)) = listener.accept().await {
                accepted.fetch_add(1, Ordering::SeqCst);
                let upstream = upstream.clone();
                tokio::spawn(async move {
                    let mut server = tokio::net::TcpStream::connect(upstream).await.unwrap();
                    let _ = tokio::io::copy_bidirectional(&mut client, &mut server).await;
                });
            }
        });
        Self {
            count,
            address,
            _runtime: runtime,
        }
    }

    fn count(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }
}

/// Starts transports for `run` out of `nodes`; every node is in every peer table.
fn start(name: &str, nodes: &[u64], run: &[u64]) -> (PathBuf, BTreeMap<u64, Transport>) {
    let (root, transports, _) = start_counted(name, nodes, run, false);
    (root, transports)
}

fn start_counted(
    name: &str,
    nodes: &[u64],
    run: &[u64],
    counted: bool,
) -> (PathBuf, BTreeMap<u64, Transport>, Option<Connects>) {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root =
        std::env::temp_dir().join(format!("range-calls-{name}-{}-{nanos}", std::process::id()));
    let pki = make_pki(&root.join("pki"), nodes);
    let listeners: Vec<_> = nodes
        .iter()
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let addresses: BTreeMap<_, _> = nodes
        .iter()
        .zip(&listeners)
        .map(|(id, l)| (*id, l.local_addr().unwrap().to_string()))
        .collect();
    drop(listeners);
    let connects = counted.then(|| Connects::new(addresses[&2].clone()));
    let transports = run
        .iter()
        .map(|&node| {
            let mut config = config_for(&pki, node, CLUSTER, &addresses);
            if node == 1
                && let Some(connects) = &connects
            {
                config.peers.get_mut(&2).unwrap().address = connects.address.clone();
            }
            let router = Arc::new(GroupRouter::new());
            (node, Transport::start_groups(config, router).unwrap())
        })
        .collect();
    (root, transports, connects)
}

fn read(range: RangeId) -> RangeCall {
    RangeCall::Read(ReadRequest {
        range,
        epoch: Epoch(1),
        space: RangeSpace::Rows,
        start: Bound::Unbounded,
        end: Bound::Unbounded,
        dir: ReadDirection::Forward,
        ts: Timestamp(42),
        page_bytes: 1024,
    })
}

fn commit(range: RangeId) -> RangeCall {
    RangeCall::Commit {
        range,
        epoch: Epoch(3),
        txn: ClusterTxnId::new(1, 9),
        writes: vec![RangeWrite {
            space: RangeSpace::Rows,
            key: b"k".to_vec(),
            value: Some(b"v".to_vec()),
        }],
        marker_xid: None,
        lock_generation: Some(3 << 20),
    }
}

fn unavailable(result: Result<RangeReply, RangeError>) -> String {
    match result {
        Err(RangeError::Unavailable(why)) => why,
        other => panic!("expected Unavailable, got {other:?}"),
    }
}

#[test]
fn calls_round_trip_and_errors_map_to_their_kinds() {
    let (root, transports) = start("echo", &[1, 2, 3], &[1, 2]);
    let (local, entered, release) = node_two(Some(3));
    let gateway = Arc::new(FakeGateway::default());
    transports[&2].set_call_handlers(Some(local), Some(gateway.clone()));
    let calls = transports[&1].calls().clone();
    let client: Arc<dyn RangeTransport> = calls.clone();

    // Echo.
    assert_eq!(
        client.call(2, RangeCall::Descriptor { range: ECHO }),
        Ok(RangeReply::Descriptor(desc(ECHO)))
    );
    let Ok(RangeReply::Read(page)) = client.call(2, read(ECHO)) else {
        panic!("read reply")
    };
    assert_eq!(
        page.pairs,
        vec![(b"k".to_vec(), 42_u64.to_be_bytes().to_vec())]
    );
    assert_eq!(page.range_end, desc(ECHO).end);
    assert_eq!(
        client.call(2, commit(ECHO)),
        Ok(RangeReply::Commit(Timestamp(301)))
    );

    // NotLeader: the fake's hint passes through; a range not led there has none.
    assert_eq!(
        client.call(2, read(MOVED)),
        Err(RangeError::NotLeader { hint: Some(3) })
    );
    assert_eq!(
        client.call(2, RangeCall::Descriptor { range: RangeId(99) }),
        Err(RangeError::NotLeader { hint: None })
    );

    // Gateway calls reach node 2's GatewayService.
    let peer: Arc<dyn GatewayPeer> = calls.clone();
    let probe = DeadlockProbe {
        initiator: ClusterTxnId(10),
        path: vec![(ClusterTxnId(10), Timestamp(4))],
        target: ClusterTxnId(11),
    };
    peer.probe(2, probe).unwrap();
    peer.abort(2, ClusterTxnId(12)).unwrap();
    assert_eq!(
        peer.blockers(2, ClusterTxnId(20)).unwrap(),
        vec![ClusterTxnId(21), ClusterTxnId(22)]
    );
    peer.cancel_wait(2, ClusterTxnId(13)).unwrap();
    // The probe is fire-and-forget: wait for it to land.
    let deadline = Instant::now() + Duration::from_secs(30);
    let seen = loop {
        let mut seen = gateway.0.lock().unwrap().clone();
        seen.sort();
        if seen.len() == 3 || Instant::now() > deadline {
            break seen;
        }
        thread::sleep(Duration::from_millis(10));
    };
    assert_eq!(seen, ["abort 12", "cancel_wait 13", "probe 10 -> 11"]);

    // A timed-out commit's outcome is unknown; a timed-out read is safe to retry.
    let short = Duration::from_millis(200);
    let result = calls.call(2, PeerCall::Range(commit(SLOW)), short);
    assert!(matches!(result, Err(RangeError::Unknown(_))), "{result:?}");
    entered.recv().unwrap();
    release.send(()).unwrap();
    let result = calls.call(2, PeerCall::Range(read(SLOW)), short);
    assert!(
        matches!(result, Err(RangeError::Unavailable(_))),
        "{result:?}"
    );
    entered.recv().unwrap();
    release.send(()).unwrap();

    // A node without handlers, and an unreachable node (never sent, so even a commit is
    // safe to retry), answer Unavailable.
    let result = transports[&2]
        .calls()
        .call(1, PeerCall::Range(commit(ECHO)), short);
    let handler = |why: &str| why.contains("no range handler");
    assert!(
        matches!(&result, Err(RangeError::Unavailable(why)) if handler(why)),
        "{result:?}"
    );
    let gateway_call = PeerCall::Gateway(GatewayCall::Abort(ClusterTxnId(1)));
    let result = transports[&2].calls().call(1, gateway_call, short);
    assert!(
        matches!(result, Err(RangeError::Unavailable(_))),
        "{result:?}"
    );
    let why = unavailable(client.call(3, commit(ECHO)));
    assert!(why.contains("no call connection to node 3"), "{why}");

    drop(transports);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn full_queue_and_exhausted_pool_answer_unavailable_promptly() {
    let (root, transports) = start("overload", &[1, 2, 3], &[1, 2, 3]);
    let (local, entered, release) = node_two(None);
    transports[&2].set_call_handlers(Some(local), None);
    let limits = CallLimits::default();
    transports[&1].set_call_limits(CallLimits { queue: 1, ..limits });
    transports[&2].set_call_limits(CallLimits {
        workers: 1,
        ..limits
    });
    let one: Arc<dyn RangeTransport> = transports[&1].calls().clone();
    let three: Arc<dyn RangeTransport> = transports[&3].calls().clone();
    let txn = ClusterTxnId::new(1, 1);

    // Node 1's only queue slot and node 2's only worker are taken by a blocked call.
    let blocked = {
        let one = one.clone();
        thread::spawn(move || one.call(2, RangeCall::Heartbeat { range: SLOW, txn }))
    };
    entered.recv_timeout(Duration::from_secs(30)).unwrap();

    let started = Instant::now();
    let why = unavailable(one.call(2, RangeCall::Descriptor { range: ECHO }));
    assert_eq!(why, "call queue full");
    let why = unavailable(three.call(2, RangeCall::Descriptor { range: ECHO }));
    assert_eq!(why, "overloaded");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "{:?}",
        started.elapsed()
    );

    release.send(()).unwrap();
    assert_eq!(blocked.join().unwrap(), Ok(RangeReply::Heartbeat));
    assert_eq!(
        three.call(2, RangeCall::Descriptor { range: ECHO }),
        Ok(RangeReply::Descriptor(desc(ECHO)))
    );
    assert_eq!(
        one.call(2, RangeCall::Descriptor { range: ECHO }),
        Ok(RangeReply::Descriptor(desc(ECHO)))
    );

    drop(transports);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn call_frames_round_trip_strictly() {
    use giggles_replication::transport::{Body, Header, decode, encode};
    let h = Header {
        cluster: CLUSTER,
        from: 1,
        to: 2,
    };
    for body in [
        Body::CallRequest {
            id: 7,
            body: vec![1, 2, 3],
        },
        Body::CallResponse {
            id: u64::MAX,
            body: vec![1],
        },
    ] {
        let bytes = encode(&h, &body).unwrap();
        assert_eq!(decode(&h, &bytes).unwrap(), body);
    }
    // Empty bodies and short payloads are rejected both ways.
    assert!(
        encode(
            &h,
            &Body::CallRequest {
                id: 1,
                body: vec![]
            }
        )
        .is_err()
    );
    let mut bytes = encode(
        &h,
        &Body::CallResponse {
            id: 1,
            body: vec![9],
        },
    )
    .unwrap();
    bytes.pop();
    assert!(decode(&h, &bytes).is_err());
    bytes.truncate(45);
    assert!(decode(&h, &bytes).is_err());
}

#[test]
fn waiting_locks_do_not_keep_out_the_calls_that_end_them() {
    let (root, transports) = start("lock-waits", &[1, 2, 3], &[1, 2, 3]);
    let (local, entered, release) = node_two(None);
    transports[&2].set_call_handlers(Some(local), None);
    let limits = CallLimits::default();
    transports[&2].set_call_limits(CallLimits {
        workers: 1,
        lock_waits: 1,
        ..limits
    });
    let one: Arc<dyn RangeTransport> = transports[&1].calls().clone();
    let three: Arc<dyn RangeTransport> = transports[&3].calls().clone();
    let txn = ClusterTxnId::new(1, 1);
    let lock = |txn| {
        RangeCall::Lock(LockRequest {
            range: SLOW,
            epoch: Epoch(1),
            txn,
            priority: Timestamp(1),
            target: LockTarget::TableSentinel { table: 9 },
            mode: RangeLockMode::Exclusive,
            timeout_ms: None,
        })
    };

    // Node 2's only lock-wait slot holds a waiting lock request.
    let waiting = {
        let one = one.clone();
        thread::spawn(move || one.call(2, lock(txn)))
    };
    entered.recv_timeout(Duration::from_secs(30)).unwrap();

    // Calls that end lock waits still get a worker; another lock wait does not.
    let started = Instant::now();
    assert!(three.call(2, commit(ECHO)).is_ok());
    assert_eq!(
        three.call(2, RangeCall::Descriptor { range: ECHO }),
        Ok(RangeReply::Descriptor(desc(ECHO)))
    );
    let why = unavailable(three.call(2, lock(ClusterTxnId::new(3, 1))));
    assert_eq!(why, "overloaded");
    assert!(started.elapsed() < Duration::from_secs(5));

    release.send(()).unwrap();
    assert!(waiting.join().unwrap().is_ok());
    drop(transports);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn timed_out_call_fails_alone() {
    let (root, transports) = start("timeout-alone", &[1, 2], &[1, 2]);
    let (local, entered, release) = node_two(None);
    transports[&2].set_call_handlers(Some(local), None);
    transports[&1].set_call_limits(CallLimits {
        timeout: Duration::from_secs(1),
        ..CallLimits::default()
    });
    let client: Arc<dyn RangeTransport> = transports[&1].calls().clone();
    let blocked = {
        let client = client.clone();
        thread::spawn(move || client.call(2, read(SLOW)))
    };
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    // A second waiting call must survive A's shorter deadline on the same peer.
    let survivor = {
        let calls = transports[&1].calls().clone();
        thread::spawn(move || calls.call(2, PeerCall::Range(read(SLOW)), Duration::from_secs(5)))
    };
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    // B succeeds while A is still blocked; C succeeds after A expires.
    assert!(client.call(2, read(ECHO)).is_ok());
    assert!(unavailable(blocked.join().unwrap()).contains("timed out"));
    assert!(client.call(2, read(ECHO)).is_ok());
    release.send(()).unwrap();
    release.send(()).unwrap();
    assert!(survivor.join().unwrap().is_ok());
    // D and subsequent replies must not be confused with A's late reply.
    assert!(client.call(2, read(ECHO)).is_ok());
    let blocked = {
        let client = client.clone();
        thread::spawn(move || client.call(2, commit(SLOW)))
    };
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    let result = blocked.join().unwrap();
    assert!(matches!(result, Err(RangeError::Unknown(why)) if why.contains("timed out")));
    release.send(()).unwrap();
    assert!(client.call(2, read(ECHO)).is_ok());
    drop(client);
    drop(transports);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn dead_node_fails_waiting_calls_and_new_calls_fast() {
    let (root, transports, connects) = start_counted("dead-node", &[1, 2], &[1, 2], true);
    let connects = connects.unwrap();
    let (local, entered, release) = node_two(None);
    transports[&2].set_call_handlers(Some(local), None);
    transports[&1].set_call_limits(CallLimits {
        timeout: Duration::from_secs(30),
        ..CallLimits::default()
    });
    let calls = transports[&1].calls().clone();
    let client: Arc<dyn RangeTransport> = calls.clone();
    assert!(calls.is_live(2)); // No liveness report yet.
    for call in [read(SLOW), commit(SLOW)] {
        let write = matches!(call, RangeCall::Commit { .. });
        let (done, result) = mpsc::channel();
        let blocked = {
            let client = client.clone();
            thread::spawn(move || done.send(client.call(2, call)).unwrap())
        };
        entered.recv_timeout(Duration::from_secs(5)).unwrap();
        // The transport also opens a Raft stream; count relative to the live socket.
        let connected = connects.count();
        let started = Instant::now();
        calls.set_alive(2, false);
        assert_eq!(
            unavailable(client.call(2, read(ECHO))),
            "node 2 is not live"
        );
        assert!(started.elapsed() < Duration::from_millis(500));
        if write {
            assert!(matches!(
                result.recv_timeout(Duration::from_millis(2300)),
                Err(mpsc::RecvTimeoutError::Timeout)
            ));
            release.send(()).unwrap();
            assert!(result.recv_timeout(Duration::from_secs(5)).unwrap().is_ok());
        } else {
            let result = result.recv_timeout(Duration::from_secs(5)).unwrap();
            assert!(
                (Duration::from_millis(1500)..Duration::from_secs(5)).contains(&started.elapsed())
            );
            assert_eq!(unavailable(result), "node 2 is not live");
            release.send(()).unwrap();
        }
        blocked.join().unwrap();
        assert!(!calls.is_live(2));
        let started = Instant::now();
        assert_eq!(
            unavailable(client.call(2, commit(ECHO))),
            "node 2 is not live"
        );
        let probe = PeerCall::Gateway(GatewayCall::Probe(DeadlockProbe {
            initiator: ClusterTxnId(10),
            path: vec![],
            target: ClusterTxnId(11),
        }));
        assert_eq!(
            calls.send(2, probe),
            Err(RangeError::Unavailable("node 2 is not live".into()))
        );
        assert!(started.elapsed() < Duration::from_secs(2));
        calls.set_alive(2, true);
        assert!(calls.is_live(2));
        assert!(client.call(2, read(ECHO)).is_ok());
        assert_eq!(
            connects.count(),
            connected + 1,
            "expired connection was reused"
        );
    }
    drop(client);
    drop(calls);
    drop(transports);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn alive_report_inside_the_grace_keeps_in_flight_calls() {
    let (root, transports, connects) = start_counted("alive-grace", &[1, 2], &[1, 2], true);
    let connects = connects.unwrap();
    let (local, entered, release) = node_two(None);
    transports[&2].set_call_handlers(Some(local), None);
    let calls = transports[&1].calls().clone();
    let (done, result) = mpsc::channel();
    let blocked = {
        let calls = calls.clone();
        thread::spawn(move || {
            done.send(calls.call(2, PeerCall::Range(commit(SLOW)), Duration::from_secs(10)))
                .unwrap();
        })
    };
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    let connected = connects.count();
    calls.set_alive(2, false);
    calls.set_alive(2, true);
    // Wait past the canceled timer, then prove the original write still gets its reply.
    assert!(matches!(
        result.recv_timeout(Duration::from_millis(2300)),
        Err(mpsc::RecvTimeoutError::Timeout)
    ));
    assert!(calls.is_live(2));
    release.send(()).unwrap();
    assert!(result.recv_timeout(Duration::from_secs(5)).unwrap().is_ok());
    blocked.join().unwrap();
    assert!(
        calls
            .call(2, PeerCall::Range(read(ECHO)), Duration::from_secs(5))
            .is_ok()
    );
    assert_eq!(
        connects.count(),
        connected,
        "alive inside grace replaced the socket"
    );
    drop(calls);
    drop(transports);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn three_timeouts_without_a_reply_reconnect() {
    let (root, transports) = start("three-timeouts", &[1, 2], &[1, 2]);
    let (local, entered, release) = node_two(None);
    transports[&2].set_call_handlers(Some(local), None);
    let calls = transports[&1].calls().clone();
    let survivor = {
        let calls = calls.clone();
        thread::spawn(move || calls.call(2, PeerCall::Range(commit(SLOW)), Duration::from_secs(30)))
    };
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    for _ in 0..3 {
        let waiting = {
            let calls = calls.clone();
            thread::spawn(move || {
                calls.call(2, PeerCall::Range(read(SLOW)), Duration::from_secs(1))
            })
        };
        entered.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(
            matches!(waiting.join().unwrap(), Err(RangeError::Unavailable(why)) if why.contains("timed out"))
        );
    }
    assert!(
        matches!(survivor.join().unwrap(), Err(RangeError::Unknown(why)) if why.contains("connection") && why.contains("lost"))
    );
    // None of the old requests has replied. The recycled connection must serve a new call.
    assert!(
        calls
            .call(2, PeerCall::Range(read(ECHO)), Duration::from_secs(5))
            .is_ok()
    );
    for _ in 0..4 {
        release.send(()).unwrap();
    }
    drop(calls);
    drop(transports);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn never_heard_peer_is_not_refused_by_liveness() {
    use giggles_replication::{
        liveness::{Liveness, LivenessConfig},
        sink::Outbox,
    };
    struct Heartbeats(mpsc::Sender<raft::eraftpb::Message>);
    impl Outbox for Heartbeats {
        fn send(&mut self, m: raft::eraftpb::Message) {
            let _ = self.0.send(m);
        }
    }
    let (root, transports) = start("never-heard", &[1, 2], &[1, 2]);
    let (local, _, _) = node_two(None);
    transports[&2].set_call_handlers(Some(local), None);
    let (tx, rx) = mpsc::channel();
    let liveness = Liveness::start(
        1,
        LivenessConfig {
            interval: Duration::from_millis(20),
            timeout: Duration::from_millis(100),
        },
        Box::new(Heartbeats(tx)),
    );
    let calls = transports[&1].calls().clone();
    let tracker = Arc::downgrade(&liveness);
    let listener_calls = calls.clone();
    let (dead, reported) = mpsc::channel();
    liveness.listen(Arc::new(move |node, alive| {
        if alive || tracker.upgrade().is_some_and(|l| l.ever_heard(node)) {
            listener_calls.set_alive(node, alive);
        }
        if !alive {
            let _ = dead.send(node);
        }
    }));
    liveness.add_peers([2]);
    assert!(!liveness.ever_heard(2));
    assert_eq!(reported.recv_timeout(Duration::from_secs(5)).unwrap(), 2);
    assert!(calls.is_live(2));
    assert!(
        calls
            .call(2, PeerCall::Range(read(ECHO)), Duration::from_secs(5))
            .is_ok()
    );
    // An untracked sender is remembered and gets heartbeats back.
    assert!(!liveness.ever_heard(3));
    liveness.heard(3);
    assert!(liveness.ever_heard(3));
    loop {
        if rx.recv_timeout(Duration::from_secs(5)).unwrap().to == 3 {
            break;
        }
    }
    liveness.stop();
    drop(liveness);
    drop(calls);
    drop(transports);
    let _ = std::fs::remove_dir_all(root);
}
