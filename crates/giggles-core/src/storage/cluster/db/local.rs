//! An in-process cluster for gateway tests (GD-0075 Track C).
//!
//! Real range engines (Track A) over one shared `RangeStore`, with one sink per range that
//! applies each batch directly (one replica, no Raft) and a fixed route table. Only the parts
//! Track B owns are stood in for: routing (the descriptors never change) and replication. Every
//! read, lock, commit, prepare, finalize, home record, oracle timestamp, lease and sequence is
//! served by the landed engine code.
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, Mutex, Weak},
    time::{Duration, Instant},
};

use crate::storage::range::RangeStore;
use crate::storage::range::engine::{EngineConfig, RangeEngines};
use crate::storage::range_api::{
    ClusterTxnId, DeadlockProbe, Epoch, GatewayPeer, GatewayService, HomeOp, HomeReply, LeaseKind,
    LockGrant, LockRequest, Outcome, RangeClient, RangeDescriptor, RangeEngineOpener, RangeError,
    RangeId, RangeService, RangeSpace, RangeWrite, ReadPage, ReadRequest, ReplicaDesc, ReplicaRole,
    SequenceOp, SequenceReply, SystemService, Timestamp, TsOracle, first_user_key, range_key,
};
use crate::storage::replicated::{AppliedPosition, BatchSink, ReplicateError};

/// A temporary directory removed on drop.
struct Dir(PathBuf);
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// One range's replication stand-in: applies batches in order, one at a time.
struct Sink {
    store: Arc<RangeStore>,
    range: RangeId,
    order: Mutex<()>,
}
impl BatchSink for Sink {
    fn replicate(&self, encoded_batch: Vec<u8>) -> Result<(), ReplicateError> {
        let _order = self.order.lock().unwrap_or_else(|p| p.into_inner());
        let index = self
            .store
            .applied(self.range)
            .map_err(|e| ReplicateError::Unavailable(format!("{e:?}")))?
            .map_or(1, |p| p.index + 1);
        self.store
            .apply(
                self.range,
                &encoded_batch,
                AppliedPosition { index, term: 1 },
            )
            .map_err(|e| ReplicateError::Unavailable(format!("{e:?}")))?;
        Ok(())
    }
}

/// The oracle every engine uses: the system range's, wherever it is open.
struct SystemOracle(Weak<RangeEngines>);
impl TsOracle for SystemOracle {
    fn next(&self, n: u32) -> Result<Timestamp, RangeError> {
        self.0
            .upgrade()
            .and_then(|engines| engines.system())
            .ok_or(RangeError::NotLeader { hint: None })?
            .next(n)
    }
}

/// Test engine settings: short leases and waits.
pub(crate) fn engine_config() -> EngineConfig {
    EngineConfig {
        lease: Duration::from_secs(2),
        read_wait: Duration::from_secs(2),
        sweep: Duration::from_millis(50),
        ..EngineConfig::default()
    }
}

pub(crate) struct LocalCluster {
    _dir: Dir,
    engines: Arc<RangeEngines>,
    oracle: Arc<SystemOracle>,
    descriptors: Vec<RangeDescriptor>,
    sinks: HashMap<RangeId, Arc<Sink>>,
    services: Mutex<HashMap<RangeId, Arc<dyn RangeService>>>,
    /// Lock-table generation of each range's current engine (incremented per open).
    generations: Mutex<HashMap<RangeId, u64>>,
    /// Gateway services (deadlock calls, `txn_live`) by node, for `GatewayPeer`.
    gateways: Mutex<HashMap<u64, Weak<dyn GatewayService>>>,
    member_route: Mutex<Option<MemberRoute>>,
}

/// A split child not yet published/elected, for membership retry regressions only.
struct MemberRoute {
    child: RangeDescriptor,
    parent: RangeDescriptor,
    until: Instant,
    learned: bool,
    left_holder: bool,
    stale: usize,
    unavailable: usize,
    granted: usize,
}

impl LocalCluster {
    /// A cluster whose user keyspace is split before each of `splits` (range keys, ascending,
    /// each above `first_user_key()`): the system range, then one range per interval.
    pub(crate) fn new(splits: &[Vec<u8>]) -> Arc<Self> {
        Self::with_config(splits, engine_config())
    }

    pub(crate) fn with_config(splits: &[Vec<u8>], config: EngineConfig) -> Arc<Self> {
        let mut nonce = [0; 16];
        getrandom::fill(&mut nonce).unwrap();
        let dir = Dir(std::env::temp_dir().join(format!(
            "gateway-cluster-{:032x}",
            u128::from_be_bytes(nonce)
        )));
        std::fs::create_dir_all(&dir.0).unwrap();
        let store = Arc::new(RangeStore::create(dir.0.join("db"), [9; 16]).unwrap());
        let replicas = vec![ReplicaDesc {
            node: 1,
            role: ReplicaRole::Voter,
        }];
        let mut bounds = vec![Vec::new(), first_user_key()];
        bounds.extend(splits.iter().cloned());
        let descriptors: Vec<RangeDescriptor> = bounds
            .iter()
            .enumerate()
            .map(|(i, start)| RangeDescriptor {
                id: RangeId(i as u64 + 1),
                epoch: Epoch(1),
                start: start.clone(),
                end: bounds.get(i + 1).cloned(),
                replicas: replicas.clone(),
            })
            .collect();
        for descriptor in &descriptors {
            store.create_range(descriptor).unwrap();
        }
        let engines = Arc::new_cyclic(|weak: &Weak<RangeEngines>| {
            RangeEngines::new(store.clone(), Arc::new(SystemOracle(weak.clone())), config)
        });
        let sinks = descriptors
            .iter()
            .map(|d| {
                (
                    d.id,
                    Arc::new(Sink {
                        store: store.clone(),
                        range: d.id,
                        order: Mutex::new(()),
                    }),
                )
            })
            .collect();
        let cluster = Arc::new(Self {
            _dir: dir,
            oracle: Arc::new(SystemOracle(Arc::downgrade(&engines))),
            engines,
            descriptors,
            sinks,
            services: Mutex::new(HashMap::new()),
            generations: Mutex::new(HashMap::new()),
            gateways: Mutex::new(HashMap::new()),
            member_route: Mutex::new(None),
        });
        // Engines resolve prepared records and run home recovery through this cluster.
        let client: Weak<dyn RangeClient> = Arc::downgrade(&cluster) as Weak<dyn RangeClient>;
        cluster.engines.set_client(client);
        // The system range opens first: every engine's open takes an oracle timestamp.
        for descriptor in &cluster.descriptors {
            cluster.restart(descriptor.id);
        }
        cluster
    }

    pub(crate) fn member_split_outage(&self, duration: Duration, left_holder: bool) {
        let key = super::super::txn::ddl::route(&super::super::txn::ddl::liveness_target(1));
        let parent = self.descriptor_for(&key).unwrap();
        let mut child = parent.clone();
        child.id = RangeId(u64::MAX);
        child.epoch.0 += 1;
        child.start = range_key(RangeSpace::Rows, &(u64::MAX - 6).to_be_bytes());
        *self.member_route.lock().unwrap() = Some(MemberRoute {
            child,
            parent,
            until: Instant::now() + duration,
            learned: false,
            left_holder,
            stale: 0,
            unavailable: 0,
            granted: 0,
        });
    }

    pub(crate) fn member_split_counts(&self) -> (usize, usize, usize) {
        let route = self.member_route.lock().unwrap();
        let route = route.as_ref().unwrap();
        (route.stale, route.unavailable, route.granted)
    }

    /// The range holding `key` of `space`.
    pub(crate) fn range_of(&self, space: RangeSpace, key: &[u8]) -> RangeId {
        self.descriptor_for(&range_key(space, key)).unwrap().id
    }

    /// A new leader term for `range`: its engine is closed and reopened, so locks of
    /// transactions that have not prepared are gone and prepared records are re-installed.
    pub(crate) fn restart(&self, range: RangeId) {
        let descriptor = self
            .descriptors
            .iter()
            .find(|d| d.id == range)
            .expect("known range")
            .clone();
        self.services.lock().unwrap().remove(&range);
        let generation = {
            let mut generations = self.generations.lock().unwrap();
            let generation = generations.entry(range).or_insert(0);
            *generation += 1;
            *generation
        };
        let service = self
            .engines
            .open(descriptor, self.sinks[&range].clone(), generation)
            .expect("open range engine");
        self.services.lock().unwrap().insert(range, service);
    }

    /// Takes `range`'s leader away: every call to it fails NotLeader until `restart`.
    pub(crate) fn stop(&self, range: RangeId) {
        self.services.lock().unwrap().remove(&range);
        self.engines.close(range);
    }

    /// Registers a gateway's service under its node id (stands in for B's wire).
    pub(crate) fn register_gateway(&self, node: u64, service: Arc<dyn GatewayService>) {
        // Weak: the gateway's services own the detector; a dropped gateway stops answering.
        self.gateways
            .lock()
            .unwrap()
            .insert(node, Arc::downgrade(&service));
    }

    /// Makes gateway `node` unreachable for every peer call (until registered again).
    pub(crate) fn unregister_gateway(&self, node: u64) {
        self.gateways.lock().unwrap().remove(&node);
    }

    fn service(&self, range: RangeId) -> Result<Arc<dyn RangeService>, RangeError> {
        self.services
            .lock()
            .unwrap()
            .get(&range)
            .cloned()
            .ok_or(RangeError::NotLeader { hint: None })
    }

    fn gateway(&self, node: u64) -> Result<Arc<dyn GatewayService>, RangeError> {
        self.gateways
            .lock()
            .unwrap()
            .get(&node)
            .and_then(Weak::upgrade)
            .ok_or_else(|| RangeError::Unavailable(format!("no gateway {node}")))
    }
}

impl RangeClient for LocalCluster {
    fn descriptor_for(&self, range_key: &[u8]) -> Result<RangeDescriptor, RangeError> {
        if let Some(route) = self.member_route.lock().unwrap().as_ref()
            && route.learned
            && route.child.contains(range_key)
        {
            return Ok(route.child.clone());
        }
        self.descriptors
            .iter()
            .find(|d| d.contains(range_key))
            .cloned()
            .ok_or_else(|| RangeError::Corrupt("no range holds the key".into()))
    }
    fn invalidate(&self, descriptor: &RangeDescriptor) {
        if let Some(route) = self.member_route.lock().unwrap().as_mut()
            && route.left_holder
            && *descriptor == route.parent
        {
            // Invalidating the old parent lets the next lookup discover the right child.
            route.learned = true;
        }
    }
    fn learn(&self, descriptor: &RangeDescriptor) {
        if let Some(route) = self.member_route.lock().unwrap().as_mut()
            && *descriptor == route.child
        {
            route.learned = true;
        }
    }
    fn read(&self, req: ReadRequest) -> Result<ReadPage, RangeError> {
        self.service(req.range)?.read(req)
    }
    fn lock(&self, mut req: LockRequest) -> Result<LockGrant, RangeError> {
        if let Some(route) = self.member_route.lock().unwrap().as_mut()
            && route
                .child
                .contains(&super::super::txn::ddl::route(&req.target))
        {
            if req.range != route.child.id {
                // Match a routed client's eager caching of the holder in a StaleEpoch reply.
                route.learned = !route.left_holder;
                route.stale += 1;
                let mut holder = route.child.clone();
                if route.left_holder {
                    holder = route.parent.clone();
                    holder.epoch = route.child.epoch;
                    holder.end = Some(route.child.start.clone());
                }
                return Err(RangeError::StaleEpoch { descriptor: holder });
            }
            if Instant::now() < route.until {
                route.unavailable += 1;
                return Err(RangeError::NotLeader { hint: None });
            }
            route.granted += 1;
            req.range = route.parent.id;
            req.epoch = route.parent.epoch;
        }
        self.service(req.range)?.lock(req)
    }
    fn blockers(&self, range: RangeId, txn: ClusterTxnId) -> Result<Vec<ClusterTxnId>, RangeError> {
        self.service(range)?.blockers(txn)
    }
    fn cancel_wait(&self, range: RangeId, txn: ClusterTxnId) -> Result<(), RangeError> {
        self.service(range)?.cancel_wait(txn)
    }
    fn release(&self, mut range: RangeId, txn: ClusterTxnId) -> Result<(), RangeError> {
        if let Some(route) = self.member_route.lock().unwrap().as_ref()
            && range == route.child.id
        {
            range = route.parent.id;
        }
        self.service(range)?.release(txn)
    }
    fn heartbeat(&self, range: RangeId, txn: ClusterTxnId) -> Result<(), RangeError> {
        self.service(range)?.heartbeat(txn)
    }
    fn commit(
        &self,
        range: RangeId,
        epoch: Epoch,
        txn: ClusterTxnId,
        writes: Vec<RangeWrite>,
        marker_xid: Option<u64>,
        lock_generation: Option<u64>,
    ) -> Result<Timestamp, RangeError> {
        self.service(range)?
            .commit(epoch, txn, writes, marker_xid, lock_generation)
    }
    fn prepare(
        &self,
        range: RangeId,
        epoch: Epoch,
        txn: ClusterTxnId,
        home: RangeId,
        writes: Vec<RangeWrite>,
        lock_generation: Option<u64>,
    ) -> Result<Timestamp, RangeError> {
        self.service(range)?
            .prepare(epoch, txn, home, writes, lock_generation)
    }
    fn finalize(
        &self,
        range: RangeId,
        txn: ClusterTxnId,
        outcome: Outcome,
    ) -> Result<(), RangeError> {
        self.service(range)?.finalize(txn, outcome)
    }
    fn home(&self, range: RangeId, epoch: Epoch, op: HomeOp) -> Result<HomeReply, RangeError> {
        self.service(range)?.home(epoch, op)
    }
    fn oracle(&self) -> &dyn TsOracle {
        self.oracle.as_ref()
    }
    fn lease(
        &self,
        kind: LeaseKind,
        id: u64,
        count: u64,
    ) -> Result<std::ops::Range<u64>, RangeError> {
        let system = self
            .engines
            .system()
            .ok_or(RangeError::NotLeader { hint: None })?;
        system.lease(kind, id, count)
    }
    fn sequence(
        &self,
        id: u64,
        incarnation: u128,
        op: SequenceOp,
    ) -> Result<SequenceReply, RangeError> {
        let system = self
            .engines
            .system()
            .ok_or(RangeError::NotLeader { hint: None })?;
        system.sequence(id, incarnation, op)
    }
}

impl GatewayPeer for LocalCluster {
    fn probe(&self, node: u64, probe: DeadlockProbe) -> Result<(), RangeError> {
        self.gateway(node)?.probe(probe);
        Ok(())
    }
    fn abort(&self, node: u64, victim: ClusterTxnId) -> Result<(), RangeError> {
        self.gateway(node)?.abort(victim);
        Ok(())
    }
    fn blockers(&self, node: u64, txn: ClusterTxnId) -> Result<Vec<ClusterTxnId>, RangeError> {
        Ok(self.gateway(node)?.blockers(txn))
    }
    fn cancel_wait(&self, node: u64, txn: ClusterTxnId) -> Result<(), RangeError> {
        self.gateway(node)?.cancel_wait(txn);
        Ok(())
    }
    fn txn_live(&self, node: u64, txn: ClusterTxnId) -> Result<bool, RangeError> {
        Ok(self.gateway(node)?.txn_live(txn))
    }
    fn ddl_close(
        &self,
        node: u64,
        txn: ClusterTxnId,
        priority: Timestamp,
        tables: &[u64],
    ) -> Result<bool, RangeError> {
        self.gateway(node)?.ddl_close(txn, priority, tables)
    }
    fn ddl_open(&self, node: u64, txn: ClusterTxnId) -> Result<(), RangeError> {
        self.gateway(node)?.ddl_open(txn);
        Ok(())
    }
    fn ddl_live(&self, node: u64, txn: ClusterTxnId) -> Result<bool, RangeError> {
        Ok(self.gateway(node)?.ddl_live(txn))
    }
}
