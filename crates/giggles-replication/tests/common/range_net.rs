//! In-process `GroupNetwork` for store-backed RangeHosts, the Rows write batch
//! encoding their range proposals carry (range_host.rs, split.rs), and a
//! three-node store-backed cluster (split.rs, split_lifecycle.rs).
use giggles_core::{
    range_api::{Epoch, RangeEngineOpener, RangeId, RangeSpace, range_key},
    replicated::RangeStore,
};
use giggles_replication::{
    bootstrap::presplit,
    host::{GroupNetwork, RangeHost, RangeHostConfig},
    range_state::encode_proposal,
    sink::{Outbox, ReplicateError},
    transport::PeerSink,
};
use raft::{StateRole, eraftpb::Message};
use std::{
    collections::{BTreeSet, HashMap},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

pub type Sinks = Arc<Mutex<HashMap<(u64, RangeId), Arc<dyn PeerSink>>>>;
/// Nodes whose messages are dropped both ways.
pub type Blocked = Arc<Mutex<BTreeSet<u64>>>;

pub struct MemNet {
    pub node: u64,
    pub sinks: Sinks,
    pub blocked: Blocked,
}
struct MemOutbox {
    group: RangeId,
    sinks: Sinks,
    blocked: Blocked,
}
impl Outbox for MemOutbox {
    fn send(&mut self, m: Message) {
        {
            let blocked = self.blocked.lock().unwrap();
            if blocked.contains(&m.from) || blocked.contains(&m.to) {
                return;
            }
        }
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
            blocked: self.blocked.clone(),
        })
    }
    fn register(&self, group: RangeId, sink: Arc<dyn PeerSink>) {
        self.sinks.lock().unwrap().insert((self.node, group), sink);
    }
    fn unregister(&self, group: RangeId) {
        self.sinks.lock().unwrap().remove(&(self.node, group));
    }
}

/// WriteBatch wire format (kv.rs `WriteBatch::encode`): version 1, op count BE4,
/// per op `[tag][keyspace index in Keyspace::ALL]` + `len BE4 + bytes` fields,
/// then crc32c BE4 of everything before it. Put = tag 1, Rows = index 2.
pub fn rows_batch(puts: &[(Vec<u8>, Vec<u8>)]) -> Vec<u8> {
    let mut out = vec![1];
    out.extend_from_slice(&(puts.len() as u32).to_be_bytes());
    for (key, value) in puts {
        out.extend_from_slice(&[1, 2]);
        for field in [key, value] {
            out.extend_from_slice(&(field.len() as u32).to_be_bytes());
            out.extend_from_slice(field);
        }
    }
    let crc = crc32c(&out);
    out.extend_from_slice(&crc.to_be_bytes());
    out
}
fn crc32c(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ if crc & 1 != 0 { 0x82f6_3b78 } else { 0 };
        }
    }
    !crc
}

pub const NODES: [u64; 3] = [1, 2, 3];

/// The range key of table `id` (a valid split key).
pub fn table(id: u64) -> Vec<u8> {
    range_key(RangeSpace::Rows, &id.to_be_bytes())
}
/// A raw Rows key: table `id`, row `n`, 8-byte version suffix.
pub fn row(id: u64, n: u8) -> Vec<u8> {
    let mut key = id.to_be_bytes().to_vec();
    key.push(n);
    key.extend_from_slice(&[0xff; 8]);
    key
}

pub struct Cluster {
    pub hosts: Vec<RangeHost>,
    pub stores: Vec<Arc<RangeStore>>,
    pub dir: PathBuf,
    pub cluster: [u8; 16],
    /// Nodes cut off from every other node (`isolate`).
    pub blocked: Blocked,
}

/// Three store-backed hosts (no engine opener) over [system 1, range 2 = Rows..].
pub fn start(dir: &Path, cluster: [u8; 16], fresh: bool) -> Cluster {
    start_with(dir, cluster, fresh, &|_| None)
}

/// As `start`, each node's engine opener from `opener(node)`.
pub fn start_with(
    dir: &Path,
    cluster: [u8; 16],
    fresh: bool,
    opener: &dyn Fn(u64) -> Option<Arc<dyn RangeEngineOpener>>,
) -> Cluster {
    let sinks: Sinks = Arc::default();
    let blocked: Blocked = Arc::default();
    let (mut hosts, mut stores) = (Vec::new(), Vec::new());
    for node in NODES {
        let data = dir.join(format!("n{node}/data"));
        let store = Arc::new(if fresh {
            RangeStore::create(&data, cluster).unwrap()
        } else {
            RangeStore::open(&data).unwrap()
        });
        let config = RangeHostConfig {
            node_id: node,
            cluster,
            log_dir: dir.join(format!("n{node}/log")),
            snap_dir: dir.join(format!("n{node}/snap")),
            tick: Duration::from_millis(10),
            policy: None,
            liveness: None,
        };
        let network = Arc::new(MemNet {
            node,
            sinks: sinks.clone(),
            blocked: blocked.clone(),
        });
        let host = RangeHost::open(config, network, store.clone(), opener(node)).unwrap();
        if fresh {
            for descriptor in presplit(&[], &NODES, 3).unwrap() {
                host.create_range(&descriptor).unwrap();
            }
        }
        hosts.push(host);
        stores.push(store);
    }
    Cluster {
        hosts,
        stores,
        dir: dir.to_owned(),
        cluster,
        blocked,
    }
}

pub fn wait(what: &str, mut done: impl FnMut() -> bool) {
    // Generous: gates run these under heavy parallel load.
    let deadline = Instant::now() + Duration::from_secs(120);
    while !done() {
        assert!(Instant::now() < deadline, "timed out: {what}");
        thread::sleep(Duration::from_millis(10));
    }
}

impl Cluster {
    /// Drops every message to or from `node` until `heal`.
    pub fn isolate(&self, node: u64) {
        self.blocked.lock().unwrap().insert(node);
    }
    pub fn heal(&self) {
        self.blocked.lock().unwrap().clear();
    }
    pub fn host(&self, node: u64) -> &RangeHost {
        &self.hosts[node as usize - 1]
    }
    pub fn store(&self, node: u64) -> &RangeStore {
        &self.stores[node as usize - 1]
    }
    pub fn applied(&self, node: u64, range: RangeId) -> u64 {
        let applied = self.store(node).applied(range).unwrap();
        applied.map_or(0, |p| p.index)
    }
    pub fn leader(&self, range: RangeId) -> u64 {
        let mut leader = None;
        wait("a leader", || {
            leader = NODES.into_iter().find(|&n| {
                let group = self.host(n).group(range);
                group.is_some_and(|g| g.role().state == StateRole::Leader)
            });
            leader.is_some()
        });
        leader.unwrap()
    }
    /// Proposes Rows puts to `range` at `epoch` on its leader and waits until that
    /// entry (applied or refused) applied on every node.
    pub fn write(
        &self,
        range: RangeId,
        epoch: Epoch,
        keys: &[Vec<u8>],
    ) -> Result<(), ReplicateError> {
        let puts: Vec<_> = keys.iter().map(|k| (k.clone(), b"v".to_vec())).collect();
        self.write_rows(range, epoch, &puts)
    }
    /// As `write`, with values.
    pub fn write_rows(
        &self,
        range: RangeId,
        epoch: Epoch,
        puts: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<(), ReplicateError> {
        let proposal = encode_proposal(epoch, &rows_batch(puts));
        for _ in 0..100 {
            let node = self.leader(range);
            let result = self
                .host(node)
                .group(range)
                .unwrap()
                .replicate(proposal.clone());
            if result != Err(ReplicateError::NotLeader) {
                self.wait_applied(range, self.applied(node, range));
                return result;
            }
        }
        panic!("no stable leader for range {}", range.0);
    }
    /// A write expected to be refused (stale epoch, key outside the range): a proposal lost to a
    /// leader change (Unknown) is proposed again until the group decides it.
    pub fn write_refused(
        &self,
        range: RangeId,
        epoch: Epoch,
        puts: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<(), ReplicateError> {
        for _ in 0..10 {
            match self.write_rows(range, epoch, puts) {
                Err(ReplicateError::Unknown(_)) => continue,
                other => return other,
            }
        }
        panic!("range {} never decided a write", range.0);
    }
    pub fn wait_applied(&self, range: RangeId, index: u64) {
        wait("applied on every node", || {
            NODES.iter().all(|&n| self.applied(n, range) >= index)
        });
    }
    /// Rows keys inside `range`'s bounds on `node` (its export manifest's Rows part).
    pub fn rows(&self, node: u64, range: RangeId) -> u64 {
        static EXPORTS: AtomicU64 = AtomicU64::new(0);
        let target = self.dir.join(format!(
            "export-{}",
            EXPORTS.fetch_add(1, Ordering::Relaxed)
        ));
        self.store(node).export(range, &target).unwrap();
        let manifest = std::fs::read(target.join("range.manifest")).unwrap();
        // [ver][id 8][domain 16][subversion 4][index 8][term 8], then descriptor and meta.
        let mut r = &manifest[45..];
        for _ in 0..2 {
            let len = u32::from_be_bytes(r[..4].try_into().unwrap()) as usize;
            r = &r[4 + len..];
        }
        let parts = u16::from_be_bytes(r[..2].try_into().unwrap());
        r = &r[2..];
        for _ in 0..parts {
            let (tag, keys) = (r[0], u64::from_be_bytes(r[1..9].try_into().unwrap()));
            let file = r[17] == 1;
            r = &r[18..];
            if file {
                r = &r[2 + usize::from(u16::from_be_bytes(r[..2].try_into().unwrap()))..];
            }
            if tag == RangeSpace::Rows.tag() {
                return keys;
            }
        }
        0
    }
    /// Stops every host, closes the stores and reopens all of them from their directories.
    pub fn restart(self) -> Cluster {
        for host in &self.hosts {
            host.stop();
        }
        let (dir, cluster) = (self.dir.clone(), self.cluster);
        drop(self);
        start(&dir, cluster, false)
    }
}

/// A fresh directory name; the counter keeps tests started together apart.
pub fn temp_dir() -> PathBuf {
    static DIRS: AtomicU64 = AtomicU64::new(0);
    std::env::temp_dir().join(format!(
        "range-split-{}-{}-{}",
        std::process::id(),
        DIRS.fetch_add(1, Ordering::Relaxed),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}
