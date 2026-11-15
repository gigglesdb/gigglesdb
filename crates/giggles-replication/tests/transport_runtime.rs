use super::common::certs::{TestPki, config_for, make_pki};
use giggles_replication::transport::*;
use raft::eraftpb::{Message, MessageType, Snapshot};
use std::{
    collections::BTreeMap,
    net::TcpListener,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

#[derive(Default)]
struct Collect {
    messages: Mutex<Vec<(u64, Message)>>,
    snapshots: Mutex<Vec<(u64, bool)>>,
}
impl PeerSink for Collect {
    fn deliver(&self, from: u64, m: Message) {
        self.messages.lock().unwrap().push((from, m));
    }
    fn snapshot_status(&self, to: u64, ok: bool) {
        self.snapshots.lock().unwrap().push((to, ok));
    }
}

struct Fixture {
    pki: TestPki,
    addresses: BTreeMap<u64, String>,
}
impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "transport-runtime-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let pki = make_pki(&dir, &[1, 2, 3]);
        // Keep reservations alive together so every peer gets a distinct port.
        let listeners: Vec<_> = (0..3)
            .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        let addresses = listeners
            .iter()
            .enumerate()
            .map(|(i, l)| (i as u64 + 1, l.local_addr().unwrap().to_string()))
            .collect();
        Self { pki, addresses }
    }
    fn config(&self, id: u64) -> TransportConfig {
        config_for(&self.pki, id, [74; 16], &self.addresses)
    }
    fn start(&self, id: u64, sink: Arc<Collect>) -> Transport {
        Transport::start(self.config(id), sink, None).unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.pki.dir).unwrap();
    }
}
fn heartbeat(from: u64, to: u64) -> Message {
    Message {
        from,
        to,
        msg_type: MessageType::MsgHeartbeat,
        ..Default::default()
    }
}
fn wait(mut ready: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !ready() {
        assert!(Instant::now() < deadline, "transport deadline exceeded");
        std::thread::sleep(Duration::from_millis(10));
    }
}
fn remains_empty(sink: &Collect) {
    let end = Instant::now() + Duration::from_millis(400);
    wait(|| {
        assert!(sink.messages.lock().unwrap().is_empty());
        Instant::now() >= end
    });
}

#[test]
fn three_nodes_deliver_once() {
    let f = Fixture::new();
    let sinks: Vec<_> = (0..3).map(|_| Arc::new(Collect::default())).collect();
    let nodes: Vec<_> = (1..=3)
        .map(|id| f.start(id, sinks[id as usize - 1].clone()))
        .collect();
    assert_eq!(nodes[0].local_addr().to_string(), f.addresses[&1]);
    nodes[0].outbox().send(heartbeat(1, 2));
    nodes[0].outbox().send(heartbeat(1, 3));
    nodes[2].outbox().send(heartbeat(3, 1));
    wait(|| sinks.iter().all(|s| !s.messages.lock().unwrap().is_empty()));
    let end = Instant::now() + Duration::from_millis(200);
    wait(|| {
        for (i, from) in [3, 1, 1].into_iter().enumerate() {
            assert_eq!(
                *sinks[i].messages.lock().unwrap(),
                vec![(from, heartbeat(from, i as u64 + 1))]
            );
        }
        Instant::now() >= end
    });
    for node in nodes {
        node.shutdown();
    }
}

#[test]
fn reconnect_preserves_queued_message() {
    let f = Fixture::new();
    let sink = Arc::new(Collect::default());
    let one = f.start(1, Arc::new(Collect::default()));
    let two = f.start(2, sink.clone());
    one.outbox().send(heartbeat(1, 2));
    wait(|| sink.messages.lock().unwrap().len() == 1);
    two.shutdown();
    let mut queued = heartbeat(1, 2);
    queued.term = 42;
    one.outbox().send(queued.clone());
    // Keep the receiver offline while the message waits in the sender queue.
    let end = Instant::now() + Duration::from_millis(200);
    wait(|| Instant::now() >= end);
    let two = f.start(2, sink.clone());
    wait(|| sink.messages.lock().unwrap().len() == 2);
    assert_eq!(sink.messages.lock().unwrap()[1], (1, queued));
    one.shutdown();
    two.shutdown();
}

#[test]
fn wrong_fingerprint_rejects_outbound() {
    let f = Fixture::new();
    let sink = Arc::new(Collect::default());
    let two = f.start(2, sink.clone());
    let mut config = f.config(1);
    config.peers.get_mut(&2).unwrap().fingerprint = "00".repeat(32);
    let one = Transport::start(config, Arc::new(Collect::default()), None).unwrap();
    one.outbox().send(heartbeat(1, 2));
    remains_empty(&sink);
    one.shutdown();
    two.shutdown();
}

#[test]
fn wrong_cluster_closes_connection() {
    let f = Fixture::new();
    let sink = Arc::new(Collect::default());
    let two = f.start(2, sink.clone());
    let mut config = f.config(1);
    config.cluster = [99; 16];
    let (_, connector) = tls(&config).unwrap();
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        tokio::time::timeout(Duration::from_secs(10), async {
            let mut stream = connect(&config, &connector, 2).await.unwrap();
            let bytes = encode(
                &Header {
                    cluster: config.cluster,
                    from: 1,
                    to: 2,
                },
                &Body::Raft(heartbeat(1, 2)),
            )
            .unwrap();
            write_frame(&mut stream, &bytes).await.unwrap();
            use tokio::io::AsyncReadExt;
            let mut b = [0];
            match stream.read(&mut b).await {
                Ok(0) | Err(_) => {}
                other => panic!("server did not close rejected connection: {other:?}"),
            }
        })
        .await
        .expect("raw client deadline");
    });
    assert!(sink.messages.lock().unwrap().is_empty());
    two.shutdown();
}

#[test]
fn snapshots_report_failure_and_late_sink_sets_once() {
    let f = Fixture::new();
    let sink = Arc::new(Collect::default());
    let late = Arc::new(LateSink::<ArcSink>::new());
    late.deliver(1, heartbeat(1, 2));
    late.snapshot_status(2, false);
    late.set(ArcSink(sink.clone()));
    late.set(ArcSink(Arc::new(Collect::default())));
    let one = Transport::start(f.config(1), late, None).unwrap();
    let mut snapshot = Message {
        from: 1,
        to: 2,
        msg_type: MessageType::MsgSnapshot,
        ..Default::default()
    };
    snapshot.set_snapshot(Snapshot::default());
    one.outbox().send(snapshot);
    wait(|| !sink.snapshots.lock().unwrap().is_empty());
    assert_eq!(*sink.snapshots.lock().unwrap(), vec![(2, false)]);
    assert!(sink.messages.lock().unwrap().is_empty());
    // Unknown and self destinations are harmless drops.
    one.outbox().send(heartbeat(1, 1));
    one.outbox().send(heartbeat(1, 99));
    one.shutdown();
}
struct ArcSink(Arc<Collect>);
impl PeerSink for ArcSink {
    fn deliver(&self, from: u64, m: Message) {
        self.0.deliver(from, m);
    }
    fn snapshot_status(&self, to: u64, ok: bool) {
        self.0.snapshot_status(to, ok);
    }
}

#[test]
fn drop_inside_async_runtime() {
    let f = Fixture::new();
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let transport = f.start(1, Arc::new(Collect::default()));
            drop(transport);
        });
}
