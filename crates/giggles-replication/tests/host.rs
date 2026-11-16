use giggles_core::range_api::RangeId;
use giggles_replication::{
    host::{GroupNetwork, RangeHost, RangeHostConfig, StateFactory},
    sink::{Outbox, ReplicaHandle, ReplicateError},
    state::{MemStateMachine, StateMachine},
    transport::PeerSink,
};
use raft::{StateRole, eraftpb::Message};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant},
};

type Sinks = Arc<Mutex<HashMap<(u64, RangeId), Arc<dyn PeerSink>>>>;
type States = Arc<Mutex<HashMap<(u64, RangeId), Arc<MemStateMachine>>>>;

struct MemNet {
    node: u64,
    sinks: Sinks,
}
struct MemOutbox {
    group: RangeId,
    sinks: Sinks,
}
impl Outbox for MemOutbox {
    fn send(&mut self, m: Message) {
        let sink = self.sinks.lock().unwrap().get(&(m.to, self.group)).cloned();
        if let Some(sink) = sink {
            sink.deliver(m.from, m);
        }
    }
}
impl GroupNetwork for MemNet {
    fn outbox(&self, group: RangeId) -> Box<dyn Outbox> {
        Box::new(MemOutbox {
            group,
            sinks: self.sinks.clone(),
        })
    }
    fn register(&self, group: RangeId, sink: Arc<dyn PeerSink>) {
        self.sinks.lock().unwrap().insert((self.node, group), sink);
    }
    fn unregister(&self, group: RangeId) {
        self.sinks.lock().unwrap().remove(&(self.node, group));
    }
}

const RANGES: [RangeId; 4] = [RangeId(11), RangeId(12), RangeId(13), RangeId(14)];
const VOTERS: [u64; 3] = [1, 2, 3];

fn start_hosts(dir: &Path, sinks: &Sinks, states: &States, existing: bool) -> Vec<RangeHost> {
    (1..=3u64)
        .map(|node| {
            let recorded = states.clone();
            let factory: StateFactory = Arc::new(move |range| {
                let s = Arc::new(MemStateMachine::new());
                recorded.lock().unwrap().insert((node, range), s.clone());
                s as Arc<dyn StateMachine>
            });
            let config = RangeHostConfig {
                node_id: node,
                cluster: [7; 16],
                log_dir: dir.join(format!("n{node}/log")),
                snap_dir: dir.join(format!("n{node}/snap")),
                tick: Duration::from_millis(10),
                policy: None,
                liveness: None,
            };
            let listed = if existing {
                RANGES.iter().map(|r| (*r, VOTERS.to_vec())).collect()
            } else {
                Vec::new()
            };
            let network = Arc::new(MemNet {
                node,
                sinks: sinks.clone(),
            });
            RangeHost::start(config, network, factory, listed).unwrap()
        })
        .collect()
}

fn leader(hosts: &[RangeHost], range: RangeId) -> ReplicaHandle {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        for host in hosts {
            let handle = host.group(range).expect("group hosted");
            if handle.role().state == StateRole::Leader {
                return handle;
            }
        }
        assert!(Instant::now() < deadline, "no leader for range {}", range.0);
        thread::sleep(Duration::from_millis(10));
    }
}

fn wait_applied(states: &States, range: RangeId, payload: &[u8]) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let done = VOTERS.iter().all(|node| {
            let s = states.lock().unwrap().get(&(*node, range)).cloned();
            s.is_some_and(|s| {
                let batches: Vec<Vec<u8>> =
                    s.applied_batches().into_iter().map(|(_, b)| b).collect();
                batches == vec![payload.to_vec()]
            })
        });
        if done {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "range {} batch not applied on all hosts",
            range.0
        );
        thread::sleep(Duration::from_millis(10));
    }
}

fn temp_dir() -> PathBuf {
    std::env::temp_dir().join(format!(
        "range-host-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

#[test]
fn three_hosts_four_groups_replicate_and_reopen() {
    let dir = temp_dir();
    let sinks: Sinks = Arc::default();
    let states: States = Arc::default();
    let hosts = start_hosts(&dir, &sinks, &states, false);
    for host in &hosts {
        for range in RANGES {
            host.create_group(range, &VOTERS).unwrap();
        }
        assert!(host.create_group(RANGES[0], &VOTERS).is_err());
        assert_eq!(host.groups(), RANGES.to_vec());
    }
    for range in RANGES {
        let payload = vec![range.0 as u8; 4];
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            match leader(&hosts, range).replicate(payload.clone()) {
                Ok(()) => break,
                Err(ReplicateError::NotLeader) if Instant::now() < deadline => {}
                Err(e) => panic!("replicate on range {}: {e}", range.0),
            }
        }
    }
    for range in RANGES {
        wait_applied(&states, range, &[range.0 as u8; 4]);
    }
    for host in &hosts {
        host.stop();
        assert!(host.groups().is_empty());
    }
    drop(hosts);
    assert!(sinks.lock().unwrap().is_empty());

    // Restart: every listed group reopens from its own log directory and the
    // fresh state machines replay exactly that group's committed batch.
    states.lock().unwrap().clear();
    let hosts = start_hosts(&dir, &sinks, &states, true);
    for host in &hosts {
        assert_eq!(host.groups(), RANGES.to_vec());
    }
    for range in RANGES {
        leader(&hosts, range);
        wait_applied(&states, range, &[range.0 as u8; 4]);
    }
    drop(hosts);
    let _ = std::fs::remove_dir_all(&dir);
}
