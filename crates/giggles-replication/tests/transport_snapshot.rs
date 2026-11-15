use super::common::certs::{config_for, make_pki};
use giggles_replication::{snapshot::SnapshotStore, transport::*};
use raft::eraftpb::{Message, MessageType};
use std::{
    collections::BTreeMap,
    fs,
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
    statuses: Mutex<Vec<(u64, bool)>>,
}
impl PeerSink for Collect {
    fn deliver(&self, from: u64, m: Message) {
        self.messages.lock().unwrap().push((from, m));
    }
    fn snapshot_status(&self, to: u64, ok: bool) {
        self.statuses.lock().unwrap().push((to, ok));
    }
}

fn wait(mut ready: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(15);
    while !ready() {
        assert!(
            Instant::now() < deadline,
            "snapshot transport deadline exceeded"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn transfer(resume: bool, corrupt: bool) {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let root = std::env::temp_dir().join(format!(
        "transport-snapshot-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let pki = make_pki(&root.join("pki"), &[1, 2]);
    let listeners: Vec<_> = (0..2)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let addresses: BTreeMap<_, _> = listeners
        .iter()
        .enumerate()
        .map(|(i, l)| (i as u64 + 1, l.local_addr().unwrap().to_string()))
        .collect();
    drop(listeners);
    let cluster = [74; 16];
    let one_store = Arc::new(SnapshotStore::open(&root.join("one"), cluster).unwrap());
    let two_store = Arc::new(SnapshotStore::open(&root.join("two"), cluster).unwrap());
    let mut state = 123456789u32;
    let big: Vec<u8> = (0..(1536 << 10))
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            state as u8
        })
        .collect();
    let files = [
        ("a-small", b"small checkpoint file".to_vec()),
        ("b-big", big),
        ("c-last", b"checkpoint tail".to_vec()),
    ];
    let manifest = one_store
        .build(42, 7, &[1, 2], |dir| {
            fs::create_dir_all(dir)?;
            for (name, data) in &files {
                fs::write(dir.join(name), data)?;
            }
            Ok(())
        })
        .unwrap();
    if resume {
        let mut writer = two_store.incoming(&manifest).unwrap();
        writer.write_chunk(0, 0, &files[0].1).unwrap();
        let half = files[1].1.len() / 2;
        writer.write_chunk(1, 0, &files[1].1[..half]).unwrap();
        assert_eq!(writer.resume_point(), (1, half as u64));
    }
    if corrupt {
        let mut changed = files[1].1.clone();
        changed[12345] ^= 1;
        fs::write(one_store.built_data_dir(&manifest).join("b-big"), changed).unwrap();
    }
    let one_sink = Arc::new(Collect::default());
    let two_sink = Arc::new(Collect::default());
    let one = Transport::start(
        config_for(&pki, 1, cluster, &addresses),
        one_sink.clone(),
        Some(one_store),
    )
    .unwrap();
    let two = Transport::start(
        config_for(&pki, 2, cluster, &addresses),
        two_sink.clone(),
        Some(two_store.clone()),
    )
    .unwrap();
    let mut message = Message {
        from: 1,
        to: 2,
        term: 7,
        msg_type: MessageType::MsgSnapshot,
        ..Default::default()
    };
    message.set_snapshot(manifest.to_raft());
    one.outbox().send(message.clone());
    wait(|| !one_sink.statuses.lock().unwrap().is_empty());
    assert_eq!(*one_sink.statuses.lock().unwrap(), vec![(2, !corrupt)]);
    if corrupt {
        assert!(two_sink.messages.lock().unwrap().is_empty());
        assert!(two_store.take_incoming(42, 7).is_none());
    } else {
        wait(|| !two_sink.messages.lock().unwrap().is_empty());
        assert_eq!(*two_sink.messages.lock().unwrap(), vec![(1, message)]);
        let incoming = two_store.take_incoming(42, 7).unwrap();
        for (name, expected) in &files {
            assert_eq!(&fs::read(incoming.join(name)).unwrap(), expected);
        }
    }
    one.shutdown();
    two.shutdown();
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn transfers_multiple_chunks_and_files() {
    transfer(false, false);
}
#[test]
fn resumes_partial_snapshot() {
    transfer(true, false);
}
#[test]
fn corrupt_file_rejected_before_raft_delivery() {
    transfer(false, true);
}
