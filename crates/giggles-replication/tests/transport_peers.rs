use super::common::certs::{config_for, make_pki};
use giggles_replication::transport::*;
use raft::eraftpb::{Message, MessageType};
use std::{
    collections::BTreeMap,
    net::TcpListener,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

#[derive(Default)]
struct Collect(Mutex<Vec<Message>>, Mutex<Vec<(u64, bool)>>);
impl PeerSink for Collect {
    fn deliver(&self, _: u64, m: Message) {
        self.0.lock().unwrap().push(m);
    }
    fn snapshot_status(&self, to: u64, ok: bool) {
        self.1.lock().unwrap().push((to, ok));
    }
}
fn message(from: u64, to: u64, term: u64) -> Message {
    Message {
        from,
        to,
        term,
        msg_type: MessageType::MsgHeartbeat,
        ..Default::default()
    }
}
fn wait(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(6);
    while !condition() {
        assert!(Instant::now() < deadline, "peer update deadline");
        std::thread::sleep(Duration::from_millis(10));
    }
}
fn absent(sinks: &[Arc<Collect>], term: u64) {
    let deadline = Instant::now() + Duration::from_millis(350);
    wait(|| {
        assert!(
            sinks
                .iter()
                .all(|s| !s.0.lock().unwrap().iter().any(|m| m.term == term))
        );
        Instant::now() >= deadline
    });
}

#[test]
fn live_updates_and_invalid_maps() {
    let dir = std::env::temp_dir().join(format!("transport-peers-{}", std::process::id()));
    let pki = make_pki(&dir, &[1, 2, 3]);
    let ports: Vec<_> = (0..3)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let addresses: BTreeMap<_, _> = ports
        .iter()
        .enumerate()
        .map(|(i, l)| (i as u64 + 1, l.local_addr().unwrap().to_string()))
        .collect();
    let configs: Vec<_> = (1..=3)
        .map(|id| config_for(&pki, id, [74; 16], &addresses))
        .collect();
    let all = configs[0].peers.clone();
    let mut initial = configs[0].clone();
    initial.peers.remove(&3);
    let reduced = initial.peers.clone();
    drop(ports);
    let sinks: Vec<_> = (0..3).map(|_| Arc::new(Collect::default())).collect();
    let nodes = [
        Transport::start(initial, sinks[0].clone(), None).unwrap(),
        Transport::start(configs[1].clone(), sinks[1].clone(), None).unwrap(),
        Transport::start(configs[2].clone(), sinks[2].clone(), None).unwrap(),
    ];
    let mut out = nodes[0].outbox(); // Must observe changes through other handles.
    out.send(message(1, 3, 10));
    nodes[2].outbox().send(message(3, 1, 10));
    absent(&sinks, 10);
    let exchange = |from: usize, to: usize, term| {
        nodes[from]
            .outbox()
            .send(message(from as u64 + 1, to as u64 + 1, term));
        wait(|| sinks[to].0.lock().unwrap().iter().any(|m| m.term == term));
    };
    exchange(0, 1, 11);
    exchange(1, 0, 12);
    nodes[0].set_peers(all.clone()).unwrap();
    out.send(message(1, 3, 20));
    wait(|| sinks[2].0.lock().unwrap().iter().any(|m| m.term == 20));
    exchange(2, 0, 21); // Leaves an inbound connection open for removal below.
    exchange(0, 1, 22);
    exchange(1, 0, 23);

    let mut invalid = all.clone();
    invalid.remove(&1);
    assert!(nodes[0].set_peers(invalid).is_err());
    let mut invalid = all.clone();
    invalid.get_mut(&1).unwrap().fingerprint = "00".repeat(32);
    assert!(nodes[0].set_peers(invalid).is_err());
    let mut invalid = all.clone();
    invalid.get_mut(&3).unwrap().fingerprint = all[&2].fingerprint.clone();
    assert!(nodes[0].set_peers(invalid).is_err());
    for fp in ["G".repeat(64), "A".repeat(64), "a".repeat(63)] {
        let mut invalid = all.clone();
        invalid.get_mut(&3).unwrap().fingerprint = fp;
        assert!(nodes[0].set_peers(invalid).is_err());
    }
    exchange(2, 0, 24); // Failed updates left the table intact.
    let mut changed = all.clone();
    changed.get_mut(&3).unwrap().fingerprint = "00".repeat(32);
    nodes[0].set_peers(changed).unwrap();
    out.send(message(1, 3, 25));
    nodes[2].outbox().send(message(3, 1, 25));
    absent(&sinks, 25);
    out.set_peers(&all);
    exchange(0, 2, 26);
    exchange(2, 0, 27);
    let mut changed = all.clone();
    changed.get_mut(&3).unwrap().address = addresses[&2].clone();
    nodes[0].set_peers(changed).unwrap();
    out.send(message(1, 3, 28));
    absent(&sinks, 28);
    nodes[0].set_peers(all).unwrap();
    exchange(0, 2, 29);
    nodes[0].outbox().set_peers(&reduced);
    out.send(message(1, 3, 30));
    nodes[2].outbox().send(message(3, 1, 30));
    absent(&sinks, 30);
    let mut snapshot = message(1, 3, 31);
    snapshot.msg_type = MessageType::MsgSnapshot;
    out.send(snapshot);
    assert_eq!(*sinks[0].1.lock().unwrap(), vec![(3, false)]);
    exchange(0, 1, 32);
    exchange(1, 0, 33);
    drop(nodes);
    std::fs::remove_dir_all(dir).unwrap();
}
