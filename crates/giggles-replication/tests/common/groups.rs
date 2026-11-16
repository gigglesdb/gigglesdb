//! In-process harness for many Raft groups per node (GD-0075 R5-15).
//!
//! Every node hosts one replica of every group. Messages travel on one shared
//! queue tagged with their group, so drop, duplicate and reorder faults and
//! node isolation, crash and restart act on all of a node's groups at once,
//! as they would on one process with one transport. Each group binds its log,
//! entries and snapshots to its own group cluster id (R5-10), so a message
//! delivered to the wrong group fails to decode.
use giggles_replication::{
    error::Result,
    log_store::LogStore,
    node::Replica,
    snapshot::{Manifest, SnapshotStore},
    state::{AppliedPosition, MemStateMachine},
};
use raft::{StateRole, eraftpb::Message};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::Read,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

static NEXT: AtomicU64 = AtomicU64::new(0);

/// Database cluster id; group `g` binds to `group::group_cluster(CLUSTER, g)`.
pub const CLUSTER: [u8; 16] = [75; 16];

/// Cluster id bound into group `g`'s log, entries and snapshots.
pub fn group_cluster(group: u64) -> [u8; 16] {
    giggles_replication::group::group_cluster(CLUSTER, group)
}

pub struct Groups {
    /// (group, node) -> replica, for live nodes only.
    pub replicas: BTreeMap<(u64, u64), Replica>,
    /// (group, node) -> state; survives crashes like a durable store.
    pub states: BTreeMap<(u64, u64), Arc<MemStateMachine>>,
    pub root: PathBuf,
    groups: Vec<u64>,
    voters: Vec<u64>,
    live: BTreeSet<u64>,
    applied: BTreeMap<u64, u64>,
    failed: BTreeSet<u64>,
    pending: BTreeMap<u64, (u64, u64)>,
    queue: Vec<(u64, Message)>,
    isolated: BTreeSet<u64>,
    rng: u64,
    serial: u64,
    faults: (u64, u64, bool),
    delivered: BTreeMap<u64, u64>,
}

impl Groups {
    pub fn new(name: &str, nodes: u64, groups: u64, seed: u64) -> Self {
        assert!((1..10).contains(&nodes), "timeouts must be in [10, 20)");
        let root = std::env::temp_dir().join(format!(
            "{name}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let mut g = Self {
            replicas: BTreeMap::new(),
            states: BTreeMap::new(),
            root,
            groups: (1..=groups).collect(),
            voters: (1..=nodes).collect(),
            live: BTreeSet::new(),
            applied: BTreeMap::new(),
            failed: BTreeSet::new(),
            pending: BTreeMap::new(),
            queue: Vec::new(),
            isolated: BTreeSet::new(),
            rng: seed,
            serial: 0,
            faults: (0, 0, false),
            delivered: BTreeMap::new(),
        };
        for node in 1..=nodes {
            for group in 1..=groups {
                g.states
                    .insert((group, node), Arc::new(MemStateMachine::new()));
            }
            g.restart(node);
        }
        g
    }
    pub fn groups(&self) -> Vec<u64> {
        self.groups.clone()
    }
    fn rand(&mut self) -> u64 {
        self.rng = self.rng.wrapping_add(0x9e3779b97f4a7c15);
        let mut z = self.rng;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
        z ^ (z >> 31)
    }
    pub fn set_faults(&mut self, drop_pct: u64, dup_pct: u64, reorder: bool) {
        assert!(drop_pct <= 100 && dup_pct <= 100);
        self.faults = (drop_pct, dup_pct, reorder);
    }
    /// Cut a node off from every peer, for every group it hosts.
    pub fn isolate(&mut self, node: u64) {
        self.isolated.insert(node);
    }
    pub fn heal(&mut self) {
        self.isolated.clear();
        self.set_faults(0, 0, false);
        self.tick_all(2);
    }
    /// Messages delivered to each group so far (duplicates counted once per copy).
    pub fn delivered(&self, group: u64) -> u64 {
        self.delivered.get(&group).copied().unwrap_or(0)
    }
    fn drive_all(&mut self) {
        for (&(group, _), replica) in &mut self.replicas {
            let out = replica.drive().expect("drive");
            self.queue
                .extend(out.messages.into_iter().map(|m| (group, m)));
            for (waiter, index) in out.applied {
                self.applied.insert(waiter, index);
                self.pending.remove(&waiter);
            }
            for waiter in out.failed.into_iter().chain(out.refused.into_iter().map(|r| r.0)) {
                self.failed.insert(waiter);
                self.pending.remove(&waiter);
            }
        }
    }
    pub fn pump(&mut self) {
        for _ in 0..400_000 {
            self.drive_all();
            if self.queue.is_empty() {
                return;
            }
            let i = if self.faults.2 {
                self.rand() as usize % self.queue.len()
            } else {
                0
            };
            let (group, m) = self.queue.remove(i);
            if self.isolated.contains(&m.from)
                || self.isolated.contains(&m.to)
                || !self.live.contains(&m.to)
                || !self.live.contains(&m.from)
                || self.rand() % 100 < self.faults.0
            {
                continue;
            }
            // Deliver duplicates directly: never recursively duplicate a queued duplicate.
            let copies = 1 + u64::from(self.rand() % 100 < self.faults.1);
            let snapshot = !m.get_snapshot().is_empty();
            if snapshot {
                self.transfer_snapshot(group, &m);
            }
            let replica = self
                .replicas
                .get_mut(&(group, m.to))
                .expect("live replica");
            for _ in 0..copies {
                replica.step(m.from, m.clone()).expect("step");
            }
            *self.delivered.entry(group).or_default() += copies;
            if snapshot {
                self.replicas
                    .get_mut(&(group, m.from))
                    .expect("live sender")
                    .report_snapshot(m.to, true);
            }
        }
        panic!("message schedule did not quiesce");
    }
    fn transfer_snapshot(&self, group: u64, m: &Message) {
        let manifest = Manifest::from_raft(group_cluster(group), m.get_snapshot()).unwrap();
        let sender = self.replicas[&(group, m.from)].log().snapshots().unwrap();
        let dir = sender.built_data_dir(&manifest);
        let receiver = self.replicas[&(group, m.to)].log().snapshots().unwrap();
        // A duplicate message may refer to data already transferred.
        if receiver
            .take_incoming(manifest.index, manifest.term)
            .is_some()
        {
            return;
        }
        let mut writer = receiver.incoming(&manifest).unwrap();
        for (i, f) in manifest.files.iter().enumerate() {
            let mut file = std::fs::File::open(dir.join(&f.name)).unwrap();
            let mut buf = [0; 65536];
            let mut offset = 0;
            loop {
                let n = file.read(&mut buf).unwrap();
                if n == 0 {
                    break;
                }
                writer.write_chunk(i as u32, offset, &buf[..n]).unwrap();
                offset += n as u64;
            }
        }
        writer.finish().unwrap();
    }
    pub fn tick_all(&mut self, n: usize) {
        for _ in 0..n {
            for (&(_, node), replica) in &mut self.replicas {
                replica.set_election_timeout(10 + node as usize);
                replica.tick();
            }
            self.pump();
        }
    }
    /// Make `node` the leader of `group`.
    pub fn elect(&mut self, group: u64, node: u64) {
        for attempt in 0..100 {
            if self.role(group, node) == Some(StateRole::Leader) {
                return;
            }
            if attempt % 20 == 0 {
                self.replicas
                    .get_mut(&(group, node))
                    .expect("live replica")
                    .campaign()
                    .unwrap();
            }
            self.pump();
            if self.role(group, node) == Some(StateRole::Leader) {
                return;
            }
            self.tick_all(1);
        }
        panic!("node {node} did not become leader of group {group}");
    }
    pub fn role(&self, group: u64, node: u64) -> Option<StateRole> {
        self.replicas.get(&(group, node)).map(|r| r.role().state)
    }
    pub fn leader(&self, group: u64) -> Option<u64> {
        self.replicas
            .iter()
            .find_map(|(&(g, node), r)| (g == group && r.role().state == StateRole::Leader).then_some(node))
    }
    pub fn propose(&mut self, group: u64, node: u64, payload: &[u8]) -> Result<u64> {
        self.serial += 1;
        self.replicas
            .get_mut(&(group, node))
            .expect("live replica")
            .propose(payload, self.serial)?;
        self.pending.insert(self.serial, (group, node));
        self.pump();
        Ok(self.serial)
    }
    pub fn committed(&self, waiter: u64) -> bool {
        self.applied.contains_key(&waiter)
    }
    pub fn failed(&self, waiter: u64) -> bool {
        self.failed.contains(&waiter)
    }
    /// Stop every replica on a node; their logs and states stay on "disk".
    pub fn crash(&mut self, node: u64) {
        self.live.remove(&node);
        self.replicas.retain(|&(_, n), _| n != node);
        // Process-local waiters cannot survive a crash, even if their entries do.
        self.pending.retain(|waiter, &mut (_, owner)| {
            if owner != node {
                return true;
            }
            self.failed.insert(*waiter);
            false
        });
        self.queue.retain(|(_, m)| m.from != node && m.to != node);
    }
    /// Reopen every group's replica on a node from its log directory.
    pub fn restart(&mut self, node: u64) {
        assert!(!self.live.contains(&node), "node {node} is running");
        for &group in &self.groups {
            let dir = self.root.join(node.to_string());
            let mut log =
                LogStore::open(&dir.join(group.to_string()), group_cluster(group), node).unwrap();
            log.attach_snapshots(Arc::new(
                SnapshotStore::open(&dir.join(format!("{group}-snap")), group_cluster(group))
                    .unwrap(),
            ));
            let mut replica =
                Replica::new(log, self.states[&(group, node)].clone(), &self.voters).unwrap();
            replica.set_election_timeout(10 + node as usize);
            self.replicas.insert((group, node), replica);
        }
        self.live.insert(node);
    }
    pub fn batches(&self, group: u64, node: u64) -> Vec<(AppliedPosition, Vec<u8>)> {
        self.states[&(group, node)].applied_batches()
    }
    /// Every live replica of `group` applied the same batches at the same positions.
    pub fn assert_equal(&mut self, group: u64) {
        self.pump();
        let nodes: Vec<u64> = self.live.iter().copied().collect();
        let first = self.batches(group, nodes[0]);
        for node in nodes {
            assert_eq!(self.batches(group, node), first, "group {group} node {node}");
        }
    }
}

impl Drop for Groups {
    fn drop(&mut self) {
        self.replicas.clear();
        let _ = std::fs::remove_dir_all(&self.root);
    }
}
