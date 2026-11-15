pub mod certs;
use giggles_replication::{
    error::Result,
    log_store::LogStore,
    membership::AdminCommand,
    node::Replica,
    snapshot::{Manifest, SnapshotStore},
    state::MemStateMachine,
};
use raft::{
    StateRole,
    eraftpb::{ConfChangeV2, Message},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::Read,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

pub const CLUSTER: [u8; 16] = [74; 16];
static NEXT: AtomicU64 = AtomicU64::new(0);
pub struct Cluster {
    pub nodes: BTreeMap<u64, Replica>,
    pub states: BTreeMap<u64, Arc<MemStateMachine>>,
    pub root: PathBuf,
    pub applied: BTreeMap<u64, u64>,
    pub reads: BTreeMap<u64, Vec<(Vec<u8>, u64)>>,
    failed: BTreeSet<u64>,
    refused: BTreeMap<u64, String>,
    joiners: BTreeSet<u64>,
    pending: BTreeMap<u64, u64>,
    voters: Vec<u64>,
    queue: Vec<Message>,
    isolated: BTreeSet<u64>,
    rng: u64,
    serial: u64,
    faults: (u64, u64, bool),
}
impl Cluster {
    pub fn new(name: &str, voters: u64, seed: u64) -> Self {
        assert!((1..10).contains(&voters), "timeouts must be in [10, 20)");
        let root = std::env::temp_dir().join(format!(
            "{name}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let mut c = Self {
            nodes: BTreeMap::new(),
            states: BTreeMap::new(),
            root,
            applied: BTreeMap::new(),
            reads: BTreeMap::new(),
            failed: BTreeSet::new(),
            refused: BTreeMap::new(),
            joiners: BTreeSet::new(),
            pending: BTreeMap::new(),
            voters: (1..=voters).collect(),
            queue: Vec::new(),
            isolated: BTreeSet::new(),
            rng: seed,
            serial: 0,
            faults: (0, 0, false),
        };
        for id in 1..=voters {
            c.states.insert(id, Arc::new(MemStateMachine::new()));
            c.restart(id);
        }
        c
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
    pub fn isolate(&mut self, id: u64) {
        self.isolated.insert(id);
    }
    pub fn heal(&mut self) {
        self.isolated.clear();
        self.set_faults(0, 0, false);
        self.tick_all(2);
    }
    pub fn pump(&mut self) {
        for _ in 0..100_000 {
            for (&id, node) in &mut self.nodes {
                let out = node.drive().expect("drive");
                self.queue.extend(out.messages);
                for (waiter, index) in out.applied {
                    self.applied.insert(waiter, index);
                    self.pending.remove(&waiter);
                }
                for waiter in out.failed {
                    self.failed.insert(waiter);
                    self.pending.remove(&waiter);
                }
                for (waiter, reason) in out.refused {
                    self.refused.insert(waiter, reason);
                    self.pending.remove(&waiter);
                }
                self.reads.entry(id).or_default().extend(out.read_states);
            }
            if self.queue.is_empty() {
                return;
            }
            let i = if self.faults.2 {
                self.rand() as usize % self.queue.len()
            } else {
                0
            };
            let m = self.queue.remove(i);
            if self.isolated.contains(&m.from)
                || self.isolated.contains(&m.to)
                || !self.nodes.contains_key(&m.to)
                || !self.nodes.contains_key(&m.from)
                || self.rand() % 100 < self.faults.0
            {
                continue;
            }
            // Deliver duplicates directly: never recursively duplicate a queued duplicate.
            let copies = 1 + usize::from(self.rand() % 100 < self.faults.1);
            let snapshot = !m.get_snapshot().is_empty();
            if snapshot {
                let manifest = Manifest::from_raft(CLUSTER, m.get_snapshot()).unwrap();
                let sender = self.nodes[&m.from].log().snapshots().unwrap();
                let dir = sender.built_data_dir(&manifest);
                let receiver = self.nodes[&m.to].log().snapshots().unwrap();
                // A duplicate message may refer to data already transferred.
                if receiver
                    .take_incoming(manifest.index, manifest.term)
                    .is_none()
                {
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
            }
            let node = self.nodes.get_mut(&m.to).unwrap();
            for _ in 0..copies {
                node.step(m.from, m.clone()).expect("step");
            }
            if snapshot {
                self.nodes
                    .get_mut(&m.from)
                    .unwrap()
                    .report_snapshot(m.to, true);
            }
        }
        panic!("message schedule did not quiesce");
    }
    pub fn tick_all(&mut self, n: usize) {
        for _ in 0..n {
            for (&id, node) in &mut self.nodes {
                node.set_election_timeout(10 + id as usize);
                node.tick();
            }
            self.pump();
        }
    }
    pub fn elect(&mut self, id: u64) {
        for attempt in 0..100 {
            if self.nodes[&id].role().state == StateRole::Leader {
                return;
            }
            if attempt % 20 == 0 {
                self.nodes.get_mut(&id).unwrap().campaign().unwrap();
            }
            self.pump();
            if self.nodes[&id].role().state == StateRole::Leader {
                return;
            }
            self.tick_all(1);
        }
        panic!("node {id} did not become leader");
    }
    pub fn leader(&self) -> Option<u64> {
        self.nodes
            .iter()
            .find_map(|(&id, n)| (n.role().state == StateRole::Leader).then_some(id))
    }
    pub fn propose(&mut self, id: u64, payload: &[u8]) -> Result<u64> {
        self.serial += 1;
        self.nodes
            .get_mut(&id)
            .expect("live node")
            .propose(payload, self.serial)?;
        self.pending.insert(self.serial, id);
        self.pump();
        Ok(self.serial)
    }
    #[allow(dead_code)]
    pub fn propose_admin(&mut self, id: u64, cmd: &AdminCommand) -> Result<u64> {
        self.serial += 1;
        self.nodes
            .get_mut(&id)
            .unwrap()
            .propose_admin(cmd, self.serial)?;
        self.pending.insert(self.serial, id);
        self.pump();
        Ok(self.serial)
    }
    #[allow(dead_code)]
    pub fn propose_conf_change(&mut self, id: u64, cc: ConfChangeV2) -> Result<u64> {
        self.serial += 1;
        self.nodes
            .get_mut(&id)
            .unwrap()
            .propose_conf_change(cc, self.serial)?;
        self.pending.insert(self.serial, id);
        self.pump();
        Ok(self.serial)
    }
    #[allow(dead_code)]
    pub fn refused(&self, waiter: u64) -> Option<String> {
        self.refused.get(&waiter).cloned()
    }
    #[allow(dead_code)]
    pub fn join(&mut self, id: u64) {
        assert!(!self.states.contains_key(&id));
        self.joiners.insert(id);
        self.states.insert(id, Arc::new(MemStateMachine::new()));
        self.restart(id);
    }
    pub fn committed(&self, waiter: u64) -> bool {
        self.applied.contains_key(&waiter)
    }
    pub fn failed(&self, waiter: u64) -> bool {
        self.failed.contains(&waiter)
    }
    pub fn crash(&mut self, id: u64) {
        drop(self.nodes.remove(&id));
        // The process-local waiter cannot survive a crash, even if its log entry does.
        self.pending.retain(|waiter, owner| {
            if *owner != id {
                return true;
            }
            self.failed.insert(*waiter);
            false
        });
        self.queue.retain(|m| m.from != id && m.to != id);
    }
    pub fn restart(&mut self, id: u64) {
        assert!(!self.nodes.contains_key(&id));
        let mut log = LogStore::open(&self.root.join(id.to_string()), CLUSTER, id).unwrap();
        log.attach_snapshots(Arc::new(
            SnapshotStore::open(&self.root.join(format!("{id}-snap")), CLUSTER).unwrap(),
        ));
        let mut node = Replica::new(
            log,
            self.states[&id].clone(),
            if self.joiners.contains(&id) {
                &[]
            } else {
                &self.voters
            },
        )
        .unwrap();
        node.set_election_timeout(10 + id as usize);
        self.nodes.insert(id, node);
    }
    #[allow(dead_code)]
    pub fn snapshot(&mut self, id: u64) -> Manifest {
        self.nodes
            .get_mut(&id)
            .unwrap()
            .build_snapshot()
            .unwrap()
            .unwrap()
    }
    #[allow(dead_code)]
    pub fn compact(&mut self, id: u64, margin: u64) -> Option<u64> {
        self.nodes
            .get_mut(&id)
            .unwrap()
            .compact_log(margin)
            .unwrap()
    }
    #[allow(dead_code)]
    pub fn wipe(&mut self, id: u64) {
        self.crash(id);
        std::fs::remove_dir_all(self.root.join(id.to_string())).unwrap();
        std::fs::remove_dir_all(self.root.join(format!("{id}-snap"))).unwrap();
        self.states.insert(id, Arc::new(MemStateMachine::new()));
    }
    pub fn assert_equal(&mut self) {
        self.pump();
        let first = self.states[self.nodes.keys().next().expect("live node")].applied_batches();
        for id in self.nodes.keys() {
            assert_eq!(self.states[id].applied_batches(), first, "node {id}");
        }
    }
}
impl Drop for Cluster {
    fn drop(&mut self) {
        self.nodes.clear();
        std::fs::remove_dir_all(&self.root).expect("remove cluster dirs");
    }
}
