//! Store-backed RangeHost with per-range role managers (GD-0075 R5-13, R5-16).
use super::common::range_net::{Blocked, MemNet, Sinks, rows_batch};
use giggles_core::{
    range_api::{
        ClusterTxnId, Epoch, HomeOp, HomeReply, LockGrant, LockRequest, Outcome, RangeDescriptor,
        RangeEngineOpener, RangeError, RangeId, RangeService, RangeSpace, RangeWrite, ReadPage,
        ReadRequest, ReplicaDesc, ReplicaRole, Timestamp, range_key,
    },
    replicated::{BatchSink, RangeStore, ReplicateError},
};
use giggles_replication::{
    client::LocalRanges,
    host::{RangeHost, RangeHostConfig},
};
use raft::StateRole;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant},
};

/// Opens and closes per (node, range), and every open's (node, range, lock generation).
#[derive(Default)]
struct Log(
    Mutex<BTreeMap<(u64, RangeId), (usize, usize)>>,
    Mutex<Vec<(u64, RangeId, u64)>>,
);
impl Log {
    fn counts(&self, node: u64, range: RangeId) -> (usize, usize) {
        self.0
            .lock()
            .unwrap()
            .get(&(node, range))
            .copied()
            .unwrap_or_default()
    }
    fn open_nodes(&self, range: RangeId) -> Vec<u64> {
        let log = self.0.lock().unwrap();
        log.iter()
            .filter(|((_, r), (opens, closes))| *r == range && opens > closes)
            .map(|((n, _), _)| *n)
            .collect()
    }
}

struct StubOpener {
    node: u64,
    log: Arc<Log>,
}
impl RangeEngineOpener for StubOpener {
    fn open(
        &self,
        descriptor: RangeDescriptor,
        sink: Arc<dyn BatchSink>,
        generation: u64,
    ) -> Result<Arc<dyn RangeService>, RangeError> {
        let mut log = self.log.0.lock().unwrap();
        let entry = log.entry((self.node, descriptor.id)).or_default();
        assert_eq!(entry.0, entry.1, "open twice without close");
        entry.0 += 1;
        self.log
            .1
            .lock()
            .unwrap()
            .push((self.node, descriptor.id, generation));
        Ok(Arc::new(StubService { descriptor, sink }))
    }
    fn close(&self, range: RangeId) {
        let mut log = self.log.0.lock().unwrap();
        let entry = log.entry((self.node, range)).or_default();
        assert_eq!(entry.0, entry.1 + 1, "close without open");
        entry.1 += 1;
    }
}

/// `commit` replicates one batch of Rows puts through the sink.
struct StubService {
    descriptor: RangeDescriptor,
    sink: Arc<dyn BatchSink>,
}
fn stub<T>() -> Result<T, RangeError> {
    Err(RangeError::Unavailable("stub".into()))
}
impl RangeService for StubService {
    fn descriptor(&self) -> RangeDescriptor {
        self.descriptor.clone()
    }
    fn read(&self, _: ReadRequest) -> Result<ReadPage, RangeError> {
        stub()
    }
    fn lock(&self, _: LockRequest) -> Result<LockGrant, RangeError> {
        stub()
    }
    fn blockers(&self, _: ClusterTxnId) -> Result<Vec<ClusterTxnId>, RangeError> {
        stub()
    }
    fn cancel_wait(&self, _: ClusterTxnId) -> Result<(), RangeError> {
        stub()
    }
    fn release(&self, _: ClusterTxnId) -> Result<(), RangeError> {
        stub()
    }
    fn heartbeat(&self, _: ClusterTxnId) -> Result<(), RangeError> {
        stub()
    }
    fn commit(
        &self,
        _: Epoch,
        _: ClusterTxnId,
        writes: Vec<RangeWrite>,
        _: Option<u64>,
        _: Option<u64>,
    ) -> Result<Timestamp, RangeError> {
        let puts: Vec<(Vec<u8>, Vec<u8>)> = writes
            .into_iter()
            .map(|w| {
                assert_eq!(w.space, RangeSpace::Rows);
                let mut raw = w.key;
                raw.extend_from_slice(&[0xff; 8]); // version suffix
                (raw, w.value.unwrap_or_default())
            })
            .collect();
        match self.sink.replicate(rows_batch(&puts)) {
            Ok(()) => Ok(Timestamp(1)),
            Err(ReplicateError::NotLeader) => Err(RangeError::NotLeader { hint: None }),
            Err(ReplicateError::Unavailable(reason)) => Err(RangeError::Unavailable(reason)),
        }
    }
    fn prepare(
        &self,
        _: Epoch,
        _: ClusterTxnId,
        _: RangeId,
        _: Vec<RangeWrite>,
        _: Option<u64>,
    ) -> Result<Timestamp, RangeError> {
        stub()
    }
    fn finalize(&self, _: ClusterTxnId, _: Outcome) -> Result<(), RangeError> {
        stub()
    }
    fn home(&self, _: Epoch, _: HomeOp) -> Result<HomeReply, RangeError> {
        stub()
    }
}

const NODES: [u64; 3] = [1, 2, 3];
const CLUSTER: [u8; 16] = [7; 16];

/// Eight ranges splitting Rows at user-key first bytes 32, 64, ..., 224.
fn descriptors() -> Vec<RangeDescriptor> {
    (0..8u8)
        .map(|i| RangeDescriptor {
            id: RangeId(101 + u64::from(i)),
            epoch: Epoch(1),
            start: range_key(RangeSpace::Rows, &[i * 32][..usize::from(i > 0)]),
            end: (i < 7).then(|| range_key(RangeSpace::Rows, &[(i + 1) * 32])),
            replicas: NODES
                .iter()
                .map(|&node| ReplicaDesc {
                    node,
                    role: ReplicaRole::Voter,
                })
                .collect(),
        })
        .collect()
}
fn ids() -> Vec<RangeId> {
    descriptors().iter().map(|d| d.id).collect()
}
/// A Rows user key inside range number `i` (0..8).
fn user_key(i: usize) -> Vec<u8> {
    vec![(i * 32 + 1) as u8, 0, 0, 0, 0, 0, 0, 7]
}

struct Cluster {
    hosts: Vec<RangeHost>,
    stores: Vec<Arc<RangeStore>>,
    log: Arc<Log>,
    blocked: Blocked,
    sinks: Sinks,
}

fn start(dir: &Path, fresh: bool) -> Cluster {
    let sinks: Sinks = Arc::default();
    let blocked: Blocked = Arc::default();
    let log: Arc<Log> = Arc::default();
    let mut hosts = Vec::new();
    let mut stores = Vec::new();
    for node in NODES {
        let data = dir.join(format!("n{node}/data"));
        let store = Arc::new(if fresh {
            RangeStore::create(&data, CLUSTER).unwrap()
        } else {
            RangeStore::open(&data).unwrap()
        });
        let config = RangeHostConfig {
            node_id: node,
            cluster: CLUSTER,
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
            snaps: Default::default(),
        });
        let opener = Arc::new(StubOpener {
            node,
            log: log.clone(),
        });
        let host = RangeHost::open(config, network, store.clone(), Some(opener)).unwrap();
        if fresh {
            assert!(host.groups().is_empty());
            for descriptor in descriptors() {
                host.create_range(&descriptor).unwrap();
            }
        }
        assert_eq!(host.groups(), ids());
        hosts.push(host);
        stores.push(store);
    }
    Cluster {
        hosts,
        stores,
        log,
        blocked,
        sinks,
    }
}

fn wait(what: &str, done: impl FnMut() -> bool) {
    wait_until(what, Instant::now() + Duration::from_secs(30), done);
}

fn wait_until(what: &str, deadline: Instant, mut done: impl FnMut() -> bool) {
    while !done() {
        assert!(Instant::now() < deadline, "timed out: {what}");
        thread::sleep(Duration::from_millis(10));
    }
}

impl Cluster {
    fn host(&self, node: u64) -> &RangeHost {
        &self.hosts[node as usize - 1]
    }
    fn store(&self, node: u64) -> &RangeStore {
        &self.stores[node as usize - 1]
    }
    fn applied(&self, node: u64, range: RangeId) -> u64 {
        self.store(node)
            .applied(range)
            .unwrap()
            .map_or(0, |p| p.index)
    }
    fn is_leader(&self, node: u64, range: RangeId) -> bool {
        self.host(node).group(range).unwrap().role().state == StateRole::Leader
    }
    /// The one node (among `among`) serving `range`, if it leads the group and
    /// is the only node with an open engine for it.
    fn steady(&self, range: RangeId, among: &[u64]) -> Option<u64> {
        let serving: Vec<u64> = among
            .iter()
            .copied()
            .filter(|&n| self.host(n).service(range).is_some())
            .collect();
        let open: Vec<u64> = self
            .log
            .open_nodes(range)
            .into_iter()
            .filter(|n| among.contains(n))
            .collect();
        match serving[..] {
            [node] if open == [node] && self.is_leader(node, range) => Some(node),
            _ => None,
        }
    }
    /// Waits until every range is steady among `among`; returns range -> leader.
    fn leaders(&self, among: &[u64]) -> BTreeMap<RangeId, u64> {
        self.leaders_until(among, Instant::now() + Duration::from_secs(30))
    }
    fn leaders_until(&self, among: &[u64], deadline: Instant) -> BTreeMap<RangeId, u64> {
        let mut leaders = BTreeMap::new();
        wait_until("one open engine per range on its leader", deadline, || {
            leaders.clear();
            for range in ids() {
                match self.steady(range, among) {
                    Some(node) => leaders.insert(range, node),
                    None => return false,
                };
            }
            true
        });
        leaders
    }
    fn wait_replicated(&self, range: RangeId, index: u64) {
        wait("applied on all three stores", || {
            NODES.iter().all(|&n| self.applied(n, range) >= index)
        });
    }
    fn isolate(&self, node: u64) {
        self.blocked.lock().unwrap().insert(node);
    }
    fn heal(&self) {
        self.blocked.lock().unwrap().clear();
    }
    /// Retrying these identical puts is safe even if an earlier outcome was unknown.
    fn commit_retry(&self, range: RangeId, key: Vec<u8>) -> u64 {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            assert!(Instant::now() < deadline, "write never settled: {range:?}");
            let node = self.leaders_until(&NODES, deadline)[&range];
            match self.commit(node, range, key.clone()) {
                Ok(_) => return node,
                Err(
                    RangeError::NotLeader { .. }
                    | RangeError::Unavailable(_)
                    | RangeError::Unknown(_),
                ) => {
                    thread::sleep(Duration::from_millis(10));
                }
                Err(error) => panic!("write failed: {error:?}"),
            }
        }
    }
    fn commit(&self, node: u64, range: RangeId, key: Vec<u8>) -> Result<Timestamp, RangeError> {
        let service = self
            .host(node)
            .service(range)
            .ok_or_else(|| RangeError::Unavailable("leader engine changed during lookup".into()))?;
        let epoch = service.descriptor().epoch;
        let write = RangeWrite {
            space: RangeSpace::Rows,
            key,
            value: Some(b"v".to_vec()),
        };
        service.commit(epoch, ClusterTxnId(1), vec![write], None, None)
    }
}

fn temp_dir() -> PathBuf {
    std::env::temp_dir().join(format!(
        "range-role-host-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

#[test]
fn store_backed_hosts_open_engines_on_leaders_fence_on_isolation_and_reopen() {
    let dir = temp_dir();
    let c = start(&dir, true);

    // A write through each leader lands on all stores. An outside-range write
    // must be refused at apply and fence the captured engine in the same term.
    for (i, range) in ids().into_iter().enumerate() {
        let node = c.commit_retry(range, user_key(i));
        c.wait_replicated(range, c.applied(node, range));
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            assert!(
                Instant::now() < deadline,
                "refusal never settled: {range:?}"
            );
            let node = c.leaders_until(&NODES, deadline)[&range];
            let group = c.host(node).group(range).unwrap();
            let term = group.role().term;
            let Some(service) = c.host(node).service(range) else {
                continue;
            };
            let written = c.applied(node, range);
            let opens = c.log.counts(node, range).0;
            let generation = c
                .log
                .1
                .lock()
                .unwrap()
                .iter()
                .rev()
                .find(|(n, r, _)| (*n, *r) == (node, range))
                .unwrap()
                .2;
            let refused = service.commit(
                Epoch(1),
                ClusterTxnId(1),
                vec![RangeWrite {
                    space: RangeSpace::Rows,
                    key: user_key((i + 1) % 8),
                    value: Some(b"v".to_vec()),
                }],
                None,
                None,
            );
            let refused_position = c.applied(node, range);
            let fenced = service.commit(Epoch(1), ClusterTxnId(2), Vec::new(), None, None);
            if group.role().term != term {
                continue;
            }
            // Refused at apply = definitely not applied, never an unknown outcome.
            assert!(
                matches!(&refused, Err(RangeError::NotLeader { .. })),
                "{refused:?}"
            );
            assert!(refused_position > written);
            assert_eq!(fenced, Err(RangeError::NotLeader { hint: None }));
            wait_until("refusal replicated and engine reopened", deadline, || {
                group.role().term != term
                    || (NODES.iter().all(|&n| c.applied(n, range) > written)
                        && c.log.counts(node, range).0 > opens
                        && c.steady(range, &NODES) == Some(node))
            });
            if group.role().term != term {
                continue;
            }
            let reopened = c
                .log
                .1
                .lock()
                .unwrap()
                .iter()
                .rev()
                .find(|(n, r, _)| (*n, *r) == (node, range))
                .unwrap()
                .2;
            if group.role().term != term {
                continue;
            }
            assert!(reopened > generation);
            assert_eq!(reopened >> 20, generation >> 20, "reopen changed term");
            break;
        }
    }
    let mut leaders = c.leaders(&NODES);

    // Each open of a range on a node got a larger lock generation than the one before;
    // the engines fenced above were reopened in the same term, with the next k.
    let gens = c.log.1.lock().unwrap().clone();
    let mut same_term = 0;
    for (i, &(node, range, generation)) in gens.iter().enumerate() {
        let previous = gens[..i]
            .iter()
            .rev()
            .find(|(n, r, _)| (*n, *r) == (node, range));
        if let Some(&(_, _, previous)) = previous {
            assert!(generation > previous, "generation went back: {gens:?}");
            same_term += usize::from(generation >> 20 == previous >> 20);
        }
    }
    assert!(same_term > 0, "no same-term reopen: {gens:?}");

    // Explicitly transfer one range if all ranges happen to share a leader.
    let deadline = Instant::now() + Duration::from_secs(30);
    while leaders.values().copied().collect::<BTreeSet<_>>().len() < 2 {
        assert!(
            Instant::now() < deadline,
            "leaders never spread: {leaders:?}"
        );
        let range = ids()[0];
        let from = leaders[&range];
        let to = *NODES.iter().find(|&&n| n != from).unwrap();
        c.host(from).group(range).unwrap().transfer_leader(to);
        thread::sleep(Duration::from_millis(20));
        leaders = c.leaders_until(&NODES, deadline);
    }
    let nodes: BTreeSet<u64> = leaders.values().copied().collect();
    assert!(nodes.len() > 1, "leaders never spread: {leaders:?}");

    // Isolation fences all victim engines. Unchanged terms must keep their engines.
    let victim = *NODES
        .iter()
        .max_by_key(|&&n| leaders.values().filter(|&&l| l == n).count())
        .unwrap();
    let kept: BTreeMap<RangeId, (u64, u64, (usize, usize))> = leaders
        .iter()
        .filter(|(_, n)| **n != victim)
        .map(|(r, n)| {
            (
                *r,
                (
                    *n,
                    c.host(*n).group(*r).unwrap().role().term,
                    c.log.counts(*n, *r),
                ),
            )
        })
        .collect();
    c.isolate(victim);
    let rest: Vec<u64> = NODES.iter().copied().filter(|&n| n != victim).collect();
    c.leaders(&rest);
    wait("isolated node's engines closed", || {
        ids()
            .into_iter()
            .all(|r| c.host(victim).service(r).is_none() && !c.log.open_nodes(r).contains(&victim))
    });
    let moved = c.leaders(&rest);
    for (range, (node, term, counts)) in &kept {
        assert_ne!(moved[range], victim);
        assert_eq!(c.log.open_nodes(*range), vec![moved[range]]);
        if c.host(*node).group(*range).unwrap().role().term != *term {
            continue;
        }
        assert_eq!(moved[range], *node, "range {} changed leader", range.0);
        assert_eq!(c.log.counts(*node, *range), *counts, "engine reopened");
        assert!(c.host(*node).service(*range).is_some());
    }
    c.heal();
    let healed = c.leaders(&NODES);
    for (range, (node, term, counts)) in &kept {
        assert_ne!(healed[range], victim);
        assert_eq!(c.log.open_nodes(*range), vec![healed[range]]);
        if c.host(*node).group(*range).unwrap().role().term == *term {
            assert_eq!(healed[range], *node);
            assert_eq!(c.log.counts(*node, *range), *counts, "engine reopened");
            assert!(c.host(*node).service(*range).is_some());
        }
    }
    for (i, range) in ids().into_iter().enumerate() {
        c.commit_retry(range, user_key(i));
    }
    let applied: BTreeMap<(u64, RangeId), u64> = NODES
        .iter()
        .flat_map(|&n| ids().into_iter().map(move |r| (n, r)))
        .map(|(n, r)| ((n, r), c.applied(n, r)))
        .collect();

    // Restart: stop closes every engine; reopening the stores reopens all groups.
    for host in &c.hosts {
        host.stop();
        assert!(host.groups().is_empty());
    }
    for range in ids() {
        assert!(c.log.open_nodes(range).is_empty());
    }
    assert!(c.sinks.lock().unwrap().is_empty());
    drop(c);
    let c = start(&dir, false);
    c.leaders(&NODES);
    for ((node, range), index) in applied {
        assert!(c.applied(node, range) >= index);
    }
    for (i, range) in ids().into_iter().enumerate() {
        c.commit_retry(range, user_key(i));
    }
    drop(c);
    let _ = std::fs::remove_dir_all(&dir);
}
