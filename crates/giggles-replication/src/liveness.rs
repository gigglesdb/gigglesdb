//! Coalesced node-to-node liveness for quiesced groups (GD-0075 R5-17b).
//!
//! One heartbeat per peer node per interval replaces the per-group heartbeats of
//! every quiesced group. It travels as a bare Raft heartbeat on the reserved group
//! `LIVENESS_GROUP` (never a range id). A peer not heard from for `timeout` is
//! declared dead and every listener (each hosted group's replica) is told; hearing
//! from it again declares it alive. Quiesced followers whose leader's node dies
//! wake and campaign; quiesced leaders wake on any peer change (node.rs).
use crate::sink::Outbox;
use raft::eraftpb::{Message, MessageType};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Condvar, Mutex},
    thread,
    time::{Duration, Instant},
};

/// Group id reserved for liveness heartbeats.
pub const LIVENESS_GROUP: u64 = u64::MAX;

#[derive(Clone, Copy, Debug)]
pub struct LivenessConfig {
    /// How often this node heartbeats every peer node.
    pub interval: Duration,
    /// Silence after which a peer node is declared dead.
    pub timeout: Duration,
}
impl Default for LivenessConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_millis(250),
            timeout: Duration::from_secs(1),
        }
    }
}

/// Receives liveness changes: `(node, alive)`.
pub type Listener = Arc<dyn Fn(u64, bool) + Send + Sync>;

struct Peer {
    heard: Instant,
    alive: bool,
}
struct State {
    peers: BTreeMap<u64, Peer>,
    listeners: Vec<Listener>,
    stop: bool,
}

/// Node liveness tracker and heartbeat thread for one node.
pub struct Liveness {
    node: u64,
    config: LivenessConfig,
    state: Arc<(Mutex<State>, Condvar)>,
    join: Mutex<Option<thread::JoinHandle<()>>>,
}

impl Liveness {
    /// Starts heartbeating through `outbox` every `config.interval`.
    pub fn start(node: u64, config: LivenessConfig, mut outbox: Box<dyn Outbox>) -> Arc<Self> {
        assert!(!config.interval.is_zero() && config.timeout > config.interval);
        let state = Arc::new((
            Mutex::new(State {
                peers: BTreeMap::new(),
                listeners: Vec::new(),
                stop: false,
            }),
            Condvar::new(),
        ));
        let worker = state.clone();
        let join = thread::spawn(move || {
            let (lock, wake) = &*worker;
            loop {
                let (peers, changes, listeners) = {
                    let mut s = lock.lock().expect("liveness mutex poisoned");
                    if s.stop {
                        return;
                    }
                    let now = Instant::now();
                    let mut changes = Vec::new();
                    for (&id, peer) in &mut s.peers {
                        let alive = now.duration_since(peer.heard) < config.timeout;
                        if alive != peer.alive {
                            peer.alive = alive;
                            changes.push((id, alive));
                        }
                    }
                    let peers: Vec<u64> = s.peers.keys().copied().collect();
                    (peers, changes, s.listeners.clone())
                };
                for to in peers {
                    let mut m = Message::default();
                    m.set_msg_type(MessageType::MsgHeartbeat);
                    m.from = node;
                    m.to = to;
                    outbox.send(m);
                }
                for (id, alive) in changes {
                    for listener in &listeners {
                        listener(id, alive);
                    }
                }
                let s = lock.lock().expect("liveness mutex poisoned");
                let (s, _) = wake
                    .wait_timeout_while(s, config.interval, |s| !s.stop)
                    .expect("liveness mutex poisoned");
                if s.stop {
                    return;
                }
            }
        });
        Arc::new(Self {
            node,
            config,
            state,
            join: Mutex::new(Some(join)),
        })
    }

    pub fn node(&self) -> u64 {
        self.node
    }

    pub fn config(&self) -> LivenessConfig {
        self.config
    }

    /// Tracks these peer nodes from now on (self is ignored).
    pub fn add_peers(&self, nodes: impl IntoIterator<Item = u64>) {
        let mut s = self.state.0.lock().expect("liveness mutex poisoned");
        for id in nodes {
            if id != self.node {
                // A new peer starts alive; its silence counts from now.
                s.peers.entry(id).or_insert(Peer {
                    heard: Instant::now(),
                    alive: true,
                });
            }
        }
    }

    /// Registers a listener for liveness changes.
    pub fn listen(&self, listener: Listener) {
        self.state
            .0
            .lock()
            .expect("liveness mutex poisoned")
            .listeners
            .push(listener);
    }

    /// Records a heartbeat from `from` (the receiving side of `LIVENESS_GROUP`).
    pub fn heard(&self, from: u64) {
        let mut s = self.state.0.lock().expect("liveness mutex poisoned");
        if let Some(peer) = s.peers.get_mut(&from) {
            peer.heard = Instant::now();
        }
    }

    /// Peers currently considered alive.
    pub fn alive(&self) -> BTreeSet<u64> {
        let s = self.state.0.lock().expect("liveness mutex poisoned");
        s.peers
            .iter()
            .filter(|(_, p)| p.alive)
            .map(|(&id, _)| id)
            .collect()
    }

    /// Stops the heartbeat thread. Idempotent.
    pub fn stop(&self) {
        {
            let (lock, wake) = &*self.state;
            lock.lock().expect("liveness mutex poisoned").stop = true;
            wake.notify_all();
        }
        if let Some(join) = self.join.lock().expect("liveness mutex poisoned").take() {
            let _ = join.join();
        }
    }
}

impl Drop for Liveness {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Inbound side: a `PeerSink` for `LIVENESS_GROUP` that records heartbeats.
pub struct LivenessSink(pub Arc<Liveness>);
impl crate::transport::PeerSink for LivenessSink {
    fn deliver(&self, from: u64, _m: Message) {
        self.0.heard(from);
    }
    fn snapshot_status(&self, _to: u64, _ok: bool) {}
}
