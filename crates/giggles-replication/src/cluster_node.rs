//! One node of a range cluster (GD-0075 Phase 7): the range store, its engines, the
//! multi-group host, the peer transport and a routed client, assembled once for tests and
//! for giggles-server. Every node is a gateway: `attach_gateway` gives it a SQL `Admin`.
//! Each node counts the calls its leader engines serve per range and answers node-status
//! calls (R5-64, `giggles-server cluster status`).
use crate::{
    bootstrap::{directory_records, hosted_by},
    call_codec::{
        self, CallCounts, MoveReply, NodeCall, NodeReport, PeerCall, PeerReply, RangeReport,
        RangeRole,
    },
    client::{LocalRanges, RangeTransport, RoutedRangeClient},
    directory,
    error::{Error, Result},
    host::{RangeHost, RangeHostConfig},
    liveness::LivenessConfig,
    registry::{self, NodeRecord, NodeState},
    sink::SnapshotPolicy,
    transport::{
        self, Body, CallLimits, GroupRouter, Header, NodeService, PeerConfig, Transport,
        TransportConfig, TransportNetwork,
    },
};
use giggles_core::{
    Limits,
    cluster_node::{ClusterGateway, EngineSettings, GatewaySettings, NodeEngines, attach_gateway},
    range_api::{
        ClusterTxnId, Epoch, GatewayPeer, HomeOp, HomeReply, LeaseKind, LockBatchRequest, LockGrant, LockRequest,
        Outcome, RangeClient, RangeDescriptor, RangeError, RangeId, RangeService, RangeSpace,
        RangeWrite, ReadDirection, ReadPage, ReadRequest, SYSTEM_RANGE, SequenceOp, SequenceReply,
        SystemService, Timestamp, TsOracle,
    },
    replicated::RangeStore,
};
use std::{
    collections::{BTreeMap, HashMap},
    io::Write,
    ops::{Bound, Range},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock, Weak, mpsc},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

/// What one node needs to join a cluster.
#[derive(Clone)]
pub struct ClusterNodeConfig {
    /// Node id, database cluster id, peer listen address, certificates and every peer.
    pub transport: TransportConfig,
    /// The range store (one RocksDB for every range this node replicates).
    pub data_dir: PathBuf,
    /// Raft logs, one subdirectory per range.
    pub log_dir: PathBuf,
    pub snap_dir: PathBuf,
    pub tick: Duration,
    /// The database's initial ranges (`bootstrap::presplit`); identical on every node. Used only
    /// when the store is created: a restarted node opens the ranges it stored.
    pub layout: Vec<RangeDescriptor>,
    pub engine: EngineSettings,
    pub liveness: Option<LivenessConfig>,
    pub policy: Option<SnapshotPolicy>,
    pub call_limits: Option<CallLimits>,
}

/// The host, bound after the client that engines and the host need is built.
#[derive(Default)]
struct LateHost(OnceLock<Weak<RangeHost>>);
impl LocalRanges for LateHost {
    fn service(&self, range: RangeId) -> Option<Arc<dyn RangeService>> {
        self.0.get()?.upgrade()?.service(range)
    }
    fn system(&self) -> Option<Arc<dyn SystemService>> {
        self.0.get()?.upgrade()?.system()
    }
}

/// Load windows: 12 slots of 5 s, so a rate covers the last 55 to 60 seconds.
const SLOTS: u64 = 12;
const SLOT_SECS: u64 = 5;

#[derive(Clone, Copy)]
enum Kind {
    Read,
    Lock,
    Write,
    Other,
}

#[derive(Clone, Default)]
struct RangeLoad {
    calls: CallCounts,
    slots: [u64; SLOTS as usize],
    /// Absolute index of the newest slot.
    slot: u64,
}

impl RangeLoad {
    fn advance(&mut self, slot: u64) {
        for s in (self.slot + 1..=slot).take(SLOTS as usize) {
            self.slots[(s % SLOTS) as usize] = 0;
        }
        self.slot = self.slot.max(slot);
    }
}

/// Calls served per range by this node's leader engines: totals since start and a recent window.
struct LoadCounters {
    start: Instant,
    ranges: Mutex<HashMap<RangeId, RangeLoad>>,
}

impl LoadCounters {
    fn new() -> Self {
        Self {
            start: Instant::now(),
            ranges: Mutex::default(),
        }
    }

    fn slot(&self) -> u64 {
        self.start.elapsed().as_secs() / SLOT_SECS
    }

    fn record(&self, range: RangeId, kind: Kind) {
        let slot = self.slot();
        let mut ranges = self.ranges.lock().unwrap_or_else(|p| p.into_inner());
        let load = ranges.entry(range).or_default();
        load.advance(slot);
        load.slots[(slot % SLOTS) as usize] += 1;
        let c = &mut load.calls;
        *match kind {
            Kind::Read => &mut c.reads,
            Kind::Lock => &mut c.locks,
            Kind::Write => &mut c.writes,
            Kind::Other => &mut c.other,
        } += 1;
    }

    /// (totals, calls in the window, window length in ms).
    fn snapshot(&self, range: RangeId) -> (CallCounts, u64, u64) {
        let slot = self.slot();
        let oldest = Duration::from_secs(slot.saturating_sub(SLOTS - 1) * SLOT_SECS);
        let window = self
            .start
            .elapsed()
            .saturating_sub(oldest)
            .as_millis()
            .max(1) as u64;
        let ranges = self.ranges.lock().unwrap_or_else(|p| p.into_inner());
        let mut load = ranges.get(&range).cloned().unwrap_or_default();
        load.advance(slot);
        (load.calls, load.slots.iter().sum(), window)
    }
}

/// A leader engine whose served calls are counted.
struct Counted {
    inner: Arc<dyn RangeService>,
    range: RangeId,
    load: Arc<LoadCounters>,
}

impl Counted {
    fn count(&self, kind: Kind) -> &dyn RangeService {
        self.load.record(self.range, kind);
        &*self.inner
    }
}

type Res<T> = std::result::Result<T, RangeError>;

impl RangeService for Counted {
    fn descriptor(&self) -> RangeDescriptor {
        self.count(Kind::Other).descriptor()
    }
    fn read(&self, req: ReadRequest) -> Res<ReadPage> {
        self.count(Kind::Read).read(req)
    }
    fn lock(&self, req: LockRequest) -> Res<LockGrant> {
        self.count(Kind::Lock).lock(req)
    }
    fn lock_batch(&self, req: LockBatchRequest) -> Res<Vec<LockGrant>> {
        // Count the batch once, as one served lock call.
        self.count(Kind::Lock).lock_batch(req)
    }
    fn blockers(&self, txn: ClusterTxnId) -> Res<Vec<ClusterTxnId>> {
        self.count(Kind::Other).blockers(txn)
    }
    fn cancel_wait(&self, txn: ClusterTxnId) -> Res<()> {
        self.count(Kind::Other).cancel_wait(txn)
    }
    fn release(&self, txn: ClusterTxnId) -> Res<()> {
        self.count(Kind::Other).release(txn)
    }
    fn heartbeat(&self, txn: ClusterTxnId) -> Res<()> {
        self.count(Kind::Other).heartbeat(txn)
    }
    fn commit(
        &self,
        epoch: Epoch,
        txn: ClusterTxnId,
        writes: Vec<RangeWrite>,
        marker_xid: Option<u64>,
        lock_generation: Option<u64>,
    ) -> Res<Timestamp> {
        self.count(Kind::Write)
            .commit(epoch, txn, writes, marker_xid, lock_generation)
    }
    fn prepare(
        &self,
        epoch: Epoch,
        txn: ClusterTxnId,
        home: RangeId,
        writes: Vec<RangeWrite>,
        lock_generation: Option<u64>,
    ) -> Res<Timestamp> {
        self.count(Kind::Write)
            .prepare(epoch, txn, home, writes, lock_generation)
    }
    fn finalize(&self, txn: ClusterTxnId, outcome: Outcome) -> Res<()> {
        self.count(Kind::Write).finalize(txn, outcome)
    }
    fn home(&self, epoch: Epoch, op: HomeOp) -> Res<HomeReply> {
        self.count(Kind::Other).home(epoch, op)
    }
}

/// The system service; its timestamp, lease and sequence calls count on the system range.
struct CountedSystem {
    inner: Arc<dyn SystemService>,
    load: Arc<LoadCounters>,
}

impl TsOracle for CountedSystem {
    fn next(&self, n: u32) -> Res<Timestamp> {
        self.load.record(SYSTEM_RANGE, Kind::Other);
        self.inner.oracle().next(n)
    }
}

impl SystemService for CountedSystem {
    fn oracle(&self) -> &dyn TsOracle {
        self
    }
    fn lease(&self, kind: LeaseKind, id: u64, count: u64) -> Res<Range<u64>> {
        self.load.record(SYSTEM_RANGE, Kind::Other);
        self.inner.lease(kind, id, count)
    }
    fn sequence(&self, id: u64, incarnation: u128, op: SequenceOp) -> Res<SequenceReply> {
        self.load.record(SYSTEM_RANGE, Kind::Other);
        self.inner.sequence(id, incarnation, op)
    }
}

/// The node's leaders as both the routed client and the peer call server see them; calls are
/// counted in the returned services' methods, not in lookups (routing looks up too).
struct CountedRanges {
    inner: Arc<dyn LocalRanges>,
    load: Arc<LoadCounters>,
}

impl LocalRanges for CountedRanges {
    fn service(&self, range: RangeId) -> Option<Arc<dyn RangeService>> {
        let inner = self.inner.service(range)?;
        let load = self.load.clone();
        Some(Arc::new(Counted { inner, range, load }))
    }
    fn system(&self) -> Option<Arc<dyn SystemService>> {
        let inner = self.inner.system()?;
        let load = self.load.clone();
        Some(Arc::new(CountedSystem { inner, load }))
    }
}

/// Approximate bytes the store holds for `range`.
fn range_size(store: &RangeStore, range: RangeId) -> Option<u64> {
    store.approximate_size(range).ok()
}

fn validate_enrolment(
    peers: &BTreeMap<u64, PeerConfig>,
    id: u64,
    address: &str,
    fingerprint: &str,
) -> std::result::Result<(), RangeError> {
    if id == 0
        || fingerprint.len() != 64
        || !fingerprint
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        || !address
            .parse::<std::net::SocketAddr>()
            .is_ok_and(|a| !a.ip().is_unspecified())
        || peers.get(&id).is_some_and(|p| p.fingerprint != fingerprint)
    {
        return Err(RangeError::Unavailable(
            "invalid node enrollment identity or address".into(),
        ));
    }
    Ok(())
}

/// Answers node-status calls; holds the host weakly (the transport's handlers outlive it).
struct StatusService {
    node_id: u64,
    host: Weak<RangeHost>,
    store: Arc<RangeStore>,
    load: Arc<LoadCounters>,
    client: Weak<RoutedRangeClient>,
    transport: Weak<Transport>,
    moves: Mutex<HashMap<RangeId, MoveJob>>,
    stopping: std::sync::atomic::AtomicBool,
    static_peers: BTreeMap<u64, PeerConfig>,
}

struct MoveJob {
    from: u64,
    to: u64,
    thread: thread::JoinHandle<()>,
    error: Arc<Mutex<Option<String>>>,
}

impl StatusService {
    fn report(&self) -> NodeReport {
        let mut ranges = Vec::new();
        let host = self.host.upgrade();
        for range in host.as_ref().map(|h| h.groups()).unwrap_or_default() {
            let Some(handle) = host.as_ref().and_then(|h| h.group(range)) else {
                continue;
            };
            let Ok(Some(descriptor)) = self.store.descriptor(range) else {
                continue;
            };
            let role = handle.role();
            let (calls, recent, window_ms) = self.load.snapshot(range);
            ranges.push(RangeReport {
                descriptor,
                role: match role.state {
                    raft::StateRole::Leader => RangeRole::Leader,
                    raft::StateRole::Follower => RangeRole::Follower,
                    _ => RangeRole::Candidate,
                },
                term: role.term,
                leader: role.leader,
                applied: handle.membership_status().map_or(0, |s| s.applied),
                size: range_size(&self.store, range),
                calls,
                recent,
                window_ms,
                move_step: (role.state == raft::StateRole::Leader)
                    .then(|| crate::move_range::move_status(&handle))
                    .flatten(),
            });
        }
        NodeReport {
            node: self.node_id,
            ranges,
        }
    }
}

impl NodeService for StatusService {
    fn registry_update(&self, call: NodeCall) -> std::result::Result<PeerReply, RangeError> {
        let client = self
            .client
            .upgrade()
            .ok_or_else(|| RangeError::Unavailable("node stopped".into()))?;
        let result = match call {
            NodeCall::RetireNode { id } => {
                registry::retire(&*client, self.node_id, id).map(PeerReply::Retired)
            }
            NodeCall::EnrolNode {
                id,
                address,
                fingerprint,
            } => {
                validate_enrolment(&self.static_peers, id, &address, &fingerprint)?;
                registry::enrol(
                    &*client,
                    self.node_id,
                    NodeRecord {
                        id,
                        address,
                        fingerprint,
                        state: NodeState::Active,
                    },
                )
                .map(PeerReply::Enrolled)
            }
            _ => return Err(RangeError::Corrupt("not a registry call".into())),
        };
        result.map_err(|e| RangeError::Unavailable(e.to_string()))
    }

    fn move_range(
        &self,
        range: RangeId,
        from: u64,
        to: u64,
        forwarded: bool,
    ) -> std::result::Result<MoveReply, RangeError> {
        let unavailable = |s: &str| RangeError::Unavailable(s.into());
        let host = self
            .host
            .upgrade()
            .ok_or_else(|| unavailable("host stopped"))?;
        let handle = host
            .group(range)
            .ok_or_else(|| unavailable("range not hosted"))?;
        let role = handle.role();
        let mut reply = MoveReply {
            leader: (role.leader != 0).then_some(role.leader),
            ..MoveReply::default()
        };
        if role.state != raft::StateRole::Leader {
            if !forwarded && let Some(leader) = reply.leader {
                let transport = self
                    .transport
                    .upgrade()
                    .ok_or_else(|| unavailable("transport stopped"))?;
                return match transport
                    .calls()
                    .call(
                        leader,
                        PeerCall::Node(NodeCall::MoveRange {
                            range,
                            from,
                            to,
                            forwarded: true,
                        }),
                        Duration::from_secs(5),
                    )
                    .map_err(|e| unavailable(&e.to_string()))?
                {
                    PeerReply::Moving(reply) => Ok(reply),
                    _ => Err(unavailable("mismatched forwarded move reply")),
                };
            }
            return Ok(reply);
        }
        reply.leader = Some(self.node_id);
        reply.step = crate::move_range::move_status(&handle);
        let mut jobs = self.moves.lock().unwrap();
        if self.stopping.load(std::sync::atomic::Ordering::Acquire) {
            return Err(unavailable("node stopping"));
        }
        if jobs.get(&range).is_some_and(|job| job.thread.is_finished()) {
            let job = jobs.remove(&range).unwrap();
            let panicked = job.thread.join().is_err();
            if job.from == from && job.to == to {
                reply.error = if panicked {
                    Some("move job panicked".into())
                } else {
                    job.error.lock().unwrap().clone()
                };
                if reply.error.is_some() {
                    return Ok(reply);
                }
            }
        }
        if let Some(job) = jobs.get(&range) {
            if job.from != from || job.to != to {
                reply.error = Some("another move is running for this range".into());
            }
            return Ok(reply);
        }
        let desc = self
            .store
            .descriptor(range)
            .map_err(|e| unavailable(&e.to_string()))?;
        reply.done = reply.step.is_none()
            && desc.is_some_and(|d| {
                d.replicas
                    .iter()
                    .any(|r| r.node == to && r.role == giggles_core::range_api::ReplicaRole::Voter)
                    && !d.replicas.iter().any(|r| r.node == from)
            });
        if reply.done {
            if let Some(client) = self.client.upgrade()
                && let Err(e) = crate::split::Splitter::new(host, client).reconcile(Some(range))
            {
                eprintln!("node {}: completed move reconciliation: {e}", self.node_id);
            }
            return Ok(reply);
        }
        let transport = self
            .transport
            .upgrade()
            .ok_or_else(|| unavailable("transport stopped"))?;
        let client = self
            .client
            .upgrade()
            .ok_or_else(|| unavailable("node stopped"))?;
        let node_id = self.node_id;
        let error = Arc::new(Mutex::new(None));
        let saved_error = error.clone();
        let thread = thread::spawn(move || {
            loop {
                if handle.is_stopped() || host.group(range).is_none() {
                    break;
                }
                match crate::move_range::run_move(
                    node_id,
                    &host,
                    &client,
                    &transport,
                    range,
                    from,
                    to,
                    Duration::from_secs(60),
                ) {
                    Ok(crate::move_range::MoveOutcome::Done(_)) => break,
                    Ok(crate::move_range::MoveOutcome::NotLeader { .. }) => {
                        if handle.role().state != raft::StateRole::Leader {
                            break;
                        }
                        thread::sleep(Duration::from_millis(50));
                    }
                    Err(e) => {
                        *saved_error.lock().unwrap() = Some(e.to_string());
                        break;
                    }
                }
            }
        });
        jobs.insert(
            range,
            MoveJob {
                from,
                to,
                thread,
                error,
            },
        );
        reply.step.get_or_insert_with(|| "Starting".into());
        Ok(reply)
    }

    fn host_range(&self, desc: RangeDescriptor) -> std::result::Result<bool, RangeError> {
        self.host
            .upgrade()
            .ok_or_else(|| RangeError::Unavailable("host stopped".into()))?
            .host_learner(&desc)
            .map_err(|e| RangeError::Unavailable(e.to_string()))
    }

    fn status(&self) -> NodeReport {
        self.report()
    }
}

/// A running node: its groups elect leaders and open engines in the background.
pub struct ClusterNode {
    node_id: u64,
    cluster: [u8; 16],
    layout: Vec<RangeDescriptor>,
    engines: Arc<NodeEngines>,
    host: Arc<RangeHost>,
    transport: Arc<Transport>,
    static_peers: BTreeMap<u64, PeerConfig>,
    client: Arc<RoutedRangeClient>,
    /// The counted leaders the transport serves peer calls from.
    ranges: Arc<dyn LocalRanges>,
    status: Arc<StatusService>,
    /// The system range's descriptor as this node last learned it: the layout's at start,
    /// then the one a StaleEpoch refusal from the system range carried (review S6).
    system: Mutex<RangeDescriptor>,
    /// Every member of the cluster (the transport's peers, this node included).
    members: Vec<u64>,
    /// The range engines' lock lease; a gateway's liveness lease must stay below it.
    lock_lease: Duration,
}

/// Registry polling thread. Dropping it stops polling and joins an in-flight pass.
pub struct RegistryWatcher {
    stop: Option<mpsc::Sender<()>>,
    thread: Option<thread::JoinHandle<()>>,
}

impl RegistryWatcher {
    /// Stops and joins the watcher. Safe to call repeatedly.
    pub fn stop(&mut self) {
        drop(self.stop.take());
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for RegistryWatcher {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Sibling of the store directory, written (and synced) once every hosted range of the layout
/// is in a newly created store (review S5).
fn layout_marker(data_dir: &Path) -> Result<PathBuf> {
    let mut name = data_dir
        .file_name()
        .ok_or_else(|| Error::Identity("data directory needs a file name".into()))?
        .to_os_string();
    name.push(".layout-created");
    Ok(data_dir.with_file_name(name))
}

fn write_marker(path: &Path) -> Result<()> {
    let mut file = std::fs::File::create(path)?;
    file.write_all(b"layout created\n")?;
    file.sync_all()?;
    Ok(())
}

fn core(e: impl std::fmt::Display) -> Error {
    Error::Core(e.to_string())
}

impl ClusterNode {
    /// Creates the store from `layout` on first start (the ranges listing this node), otherwise
    /// opens it; then starts the transport, the engines, every stored range's group and the
    /// routed client. A first start ends with a marker file next to the store; a store without
    /// it was cut off while its ranges were created, and the hosted ranges of the layout it
    /// lacks are created now.
    pub fn start(config: ClusterNodeConfig) -> Result<Self> {
        let node_id = config.transport.node_id;
        let cluster = config.transport.cluster;
        let members: Vec<u64> = config.transport.peers.keys().copied().collect();
        let lock_lease = config.engine.lease;
        let system = config
            .layout
            .iter()
            .find(|d| d.id == SYSTEM_RANGE)
            .cloned()
            .ok_or_else(|| Error::Identity("layout has no system range".into()))?;
        let fresh = match std::fs::read_dir(&config.data_dir) {
            Ok(mut entries) => entries.next().is_none(),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => true,
            Err(e) => return Err(e.into()),
        };
        let marker = layout_marker(&config.data_dir)?;
        let store = if fresh {
            RangeStore::create(&config.data_dir, cluster).map_err(core)?
        } else {
            let store = RangeStore::open(&config.data_dir).map_err(core)?;
            if store.domain() != cluster {
                return Err(Error::Identity(
                    "range store belongs to another cluster".into(),
                ));
            }
            store
        };
        if fresh || !marker.exists() {
            let stored = store.ranges().map_err(core)?;
            for descriptor in hosted_by(&config.layout, node_id) {
                if !stored.contains(&descriptor.id) {
                    store.create_range(&descriptor).map_err(core)?;
                }
            }
            write_marker(&marker)?;
        }
        let store = Arc::new(store);
        let transport = Arc::new(Transport::start_groups(
            config.transport.clone(),
            Arc::new(GroupRouter::new()),
        )?);
        if let Some(limits) = config.call_limits {
            transport.set_call_limits(limits);
        }
        // Client first: engines resolve prepared records and take timestamps through it.
        let late = Arc::new(LateHost::default());
        let load = Arc::new(LoadCounters::new());
        let ranges: Arc<dyn LocalRanges> = Arc::new(CountedRanges {
            inner: late.clone(),
            load: load.clone(),
        });
        let client = Arc::new(RoutedRangeClient::new(
            node_id,
            system.clone(),
            transport.calls().clone() as Arc<dyn RangeTransport>,
            ranges.clone(),
        ));
        let engines = NodeEngines::new(store.clone(), config.engine);
        let dyn_client: Arc<dyn RangeClient> = client.clone();
        engines.set_client(Arc::downgrade(&dyn_client));
        let host = Arc::new(RangeHost::open(
            RangeHostConfig {
                node_id,
                cluster,
                log_dir: config.log_dir,
                snap_dir: config.snap_dir,
                tick: config.tick,
                policy: config.policy,
                liveness: config.liveness,
            },
            Arc::new(TransportNetwork::new(transport.clone())),
            store.clone(),
            Some(engines.opener()),
        )?);
        host.set_system(Some(engines.system_service()));
        let _ = late.0.set(Arc::downgrade(&host));
        transport.set_call_handlers(Some(ranges.clone()), None);
        let status = Arc::new(StatusService {
            node_id,
            host: Arc::downgrade(&host),
            store,
            load,
            client: Arc::downgrade(&client),
            transport: Arc::downgrade(&transport),
            moves: Mutex::default(),
            stopping: std::sync::atomic::AtomicBool::new(false),
            static_peers: config.transport.peers.clone(),
        });
        transport.set_node_handler(Some(status.clone() as Arc<dyn NodeService>));
        Ok(Self {
            node_id,
            cluster,
            system: Mutex::new(system),
            layout: config.layout,
            engines,
            host,
            transport,
            static_peers: config.transport.peers,
            client,
            ranges,
            status,
            members,
            lock_lease,
        })
    }

    /// Enrols the configured members after directory bootstrap; existing records are idempotent.
    /// The timeout bounds retries between registry operations (an in-flight operation may finish later).
    pub fn enrol_members(&self, timeout: Duration) -> Result<usize> {
        let deadline = Instant::now() + timeout;
        let mut written = 0;
        for (&id, peer) in &self.static_peers {
            if id == self.node_id
                && peer
                    .address
                    .parse::<std::net::SocketAddr>()
                    .is_ok_and(|a| a.ip().is_unspecified())
            {
                eprintln!("node {id}: skipping enrollment of unspecified listen address");
                continue;
            }
            let record = NodeRecord {
                id,
                state: NodeState::Active,
                address: peer.address.clone(),
                fingerprint: peer.fingerprint.clone(),
            };
            loop {
                match registry::enrol(&*self.client, self.node_id, record.clone()) {
                    Ok(new) => {
                        written += usize::from(new);
                        break;
                    }
                    Err(Error::Core(_)) if Instant::now() < deadline => {
                        thread::sleep(Duration::from_millis(50));
                    }
                    Err(e) => return Err(e),
                }
            }
        }
        Ok(written)
    }

    /// Watches registry identities without changing the static bootstrap configuration.
    /// Stop or drop the watcher before stopping the node.
    pub fn start_registry_watcher(&self, interval: Duration) -> RegistryWatcher {
        let (stop, stopped) = mpsc::channel::<()>();
        let (node, client, transport, host) = (
            self.node_id,
            self.client.clone(),
            self.transport.clone(),
            self.host.clone(),
        );
        let static_peers = self.static_peers.clone();
        let thread = thread::spawn(move || {
            let mut applied = static_peers.clone();
            while let Err(mpsc::RecvTimeoutError::Timeout) = stopped.recv_timeout(interval) {
                let result = (|| -> Result<()> {
                    let records = registry::read_all(&*client).map_err(core)?;
                    let (active, retired) = registry::peers(&records, node);
                    let mut table = static_peers.clone();
                    for (id, (address, fingerprint)) in active {
                        table.entry(id).or_insert(PeerConfig {
                            address,
                            fingerprint,
                        });
                    }
                    table.retain(|id, _| *id == node || !retired.contains(id));
                    let same = table.len() == applied.len()
                        && table.iter().all(|(id, peer)| {
                            applied.get(id).is_some_and(|old| {
                                old.address == peer.address && old.fingerprint == peer.fingerprint
                            })
                        });
                    if !same {
                        transport.set_peers(table.clone())?;
                        let added: Vec<_> = table
                            .keys()
                            .copied()
                            .filter(|id| *id != node && !applied.contains_key(id))
                            .collect();
                        host.add_liveness_peers(&added);
                        applied = table;
                    }
                    Ok(())
                })();
                if let Err(e) = result {
                    eprintln!("node {node}: registry watch: {e}");
                }
            }
        });
        RegistryWatcher {
            stop: Some(stop),
            thread: Some(thread),
        }
    }

    /// Every range this node replicates: role, term, leader, applied index, size and the calls
    /// its leader engines served (what a `NodeCall::Status` peer call answers).
    pub fn status(&self) -> NodeReport {
        self.status.report()
    }

    pub fn node_id(&self) -> u64 {
        self.node_id
    }
    pub fn cluster(&self) -> [u8; 16] {
        self.cluster
    }
    pub fn layout(&self) -> &[RangeDescriptor] {
        &self.layout
    }
    pub fn client(&self) -> &Arc<RoutedRangeClient> {
        &self.client
    }
    pub fn host(&self) -> &Arc<RangeHost> {
        &self.host
    }
    pub fn engines(&self) -> &Arc<NodeEngines> {
        &self.engines
    }
    pub fn transport(&self) -> &Arc<Transport> {
        &self.transport
    }

    fn system(&self) -> RangeDescriptor {
        self.system
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    /// Keeps the system range's descriptor that a StaleEpoch refusal from it carries (after a
    /// split or membership change); returns whether it was new.
    fn learn_system(&self, error: &RangeError) -> bool {
        let RangeError::StaleEpoch { descriptor } = error else {
            return false;
        };
        let mut system = self.system.lock().unwrap_or_else(|p| p.into_inner());
        if descriptor.id != SYSTEM_RANGE || descriptor.epoch == system.epoch {
            return false;
        }
        *system = descriptor.clone();
        true
    }

    /// The system range's directory record for its own start key, at a fresh timestamp;
    /// `None` until the directory is written. A StaleEpoch refusal is retried with the
    /// descriptor it carries, which is used from then on.
    fn directory_entry(&self) -> std::result::Result<Option<RangeDescriptor>, RangeError> {
        let ts = self.client.oracle().next(1)?;
        let mut attempts = 0;
        loop {
            let system = self.system();
            let key = system.start.clone();
            let error = match self
                .client
                .read(directory::lookup_request(&key, &system, ts))
            {
                Ok(page) => return directory::parse_lookup(&page, &key),
                Err(e) => e,
            };
            attempts += 1;
            if attempts >= 8 || !self.learn_system(&error) {
                return Err(error);
            }
        }
    }

    /// Writes the layout's directory records to the system range unless they are there.
    /// Run on one node only (giggles-server: the smallest member id). Retries while the
    /// system range has no leader. Returns whether this call wrote them.
    pub fn bootstrap_directory(&self, timeout: Duration) -> Result<bool> {
        let deadline = Instant::now() + timeout;
        let writes = directory_records(&self.layout).map_err(core)?;
        // Transaction ids outside every gateway's space (gateways set the high 16 bits), a new
        // one per attempt: an engine refuses a transaction whose earlier attempt's lease expired.
        let mut txn = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(1, |d| d.as_nanos() as u64)
            & ((1 << 47) - 1);
        loop {
            txn += 1;
            let attempt = self.directory_entry().and_then(|found| match found {
                Some(_) => Ok(false),
                None => {
                    let system = self.system();
                    let result = self.client.commit(
                        system.id,
                        system.epoch,
                        ClusterTxnId(txn),
                        writes.clone(),
                        None,
                        None,
                    );
                    // The next attempt (under a new transaction id) uses the newer descriptor.
                    if let Err(e) = &result {
                        self.learn_system(e);
                    }
                    result.map(|_| true)
                }
            });
            match attempt {
                Ok(wrote) => return Ok(wrote),
                Err(e) if Instant::now() >= deadline => {
                    return Err(Error::Core(format!("directory bootstrap: {e}")));
                }
                Err(_) => thread::sleep(Duration::from_millis(50)),
            }
        }
    }

    /// Waits until every range of the layout is reachable through the directory and serves a
    /// read at a fresh timestamp (a leader engine is open somewhere).
    pub fn wait_ready(&self, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            let last = match self.ready() {
                Ok(()) => return Ok(()),
                Err(e) => e,
            };
            if Instant::now() >= deadline {
                return Err(Error::Core(format!("cluster not ready: {last}")));
            }
            thread::sleep(Duration::from_millis(50));
        }
    }

    fn ready(&self) -> std::result::Result<(), RangeError> {
        if self.directory_entry()?.is_none() {
            return Err(RangeError::Unavailable("no directory yet".into()));
        }
        for range in &self.layout {
            let descriptor = self.client.descriptor_for(&range.start)?;
            // Reads are clamped to the range, so an unbounded one-pair page of any keyspace
            // proves a leader engine serves it.
            self.client.read(ReadRequest {
                range: descriptor.id,
                epoch: descriptor.epoch,
                space: RangeSpace::Catalog,
                start: Bound::Unbounded,
                end: Bound::Unbounded,
                dir: ReadDirection::Forward,
                ts: self.client.oracle().next(1)?,
                page_bytes: 1,
            })?;
        }
        Ok(())
    }

    /// This node's SQL gateway; its deadlock service answers other gateways' probes. Every
    /// member of the cluster is a gateway (DDL across gateways, GD-0075 Phase 11): an empty
    /// `settings.gateways` becomes every member id, and the gateway joins before it returns.
    /// `settings.gateway_lease` must stay below the engines' lock lease.
    pub fn attach_gateway(
        &self,
        mut settings: GatewaySettings,
        limits: Limits,
    ) -> Result<ClusterGateway> {
        if settings.gateways.is_empty() {
            settings.gateways = self
                .members
                .iter()
                .map(|&id| u16::try_from(id))
                .collect::<std::result::Result<_, _>>()
                .map_err(|_| Error::Identity("a member id is above 65535".into()))?;
        }
        if !settings.gateways.contains(&settings.node) {
            return Err(Error::Identity(format!(
                "gateway {} is not among the gateways {:?}",
                settings.node, settings.gateways
            )));
        }
        if settings.gateway_lease.is_zero() || settings.gateway_lease >= self.lock_lease {
            return Err(Error::Core(format!(
                "gateway lease {:?} must be positive and below the lock lease {:?}",
                settings.gateway_lease, self.lock_lease
            )));
        }
        settings.lock_lease = self.lock_lease;
        let peer: Arc<dyn GatewayPeer> = self.transport.calls().clone();
        let static_members = settings.gateways.clone();
        let gateway = attach_gateway(
            self.client.clone() as Arc<dyn RangeClient>,
            settings,
            Some(peer),
            self.cluster,
            limits,
        )
        .map_err(core)?;
        let client = self.client.clone();
        gateway.set_member_source(Box::new(move || {
            let records = registry::read_all(&*client)?;
            if records.is_empty() {
                return Ok(static_members.clone());
            }
            records
                .into_iter()
                .filter(|record| record.state == NodeState::Active)
                .map(|record| {
                    u16::try_from(record.id).map_err(|_| {
                        RangeError::Unavailable("an active gateway id is above 65535".into())
                    })
                })
                .collect()
        }));
        self.transport
            .set_call_handlers(Some(self.ranges.clone()), gateway.gateway_service.clone());
        Ok(gateway)
    }

    /// Stops every group and the transport.
    pub fn stop(self) {
        let jobs = {
            let mut jobs = self.status.moves.lock().unwrap();
            self.status
                .stopping
                .store(true, std::sync::atomic::Ordering::Release);
            std::mem::take(&mut *jobs)
        };
        self.transport.set_node_handler(None);
        self.host.stop();
        for (_, job) in jobs {
            let _ = job.thread.join();
        }
    }
}

type Answer = std::result::Result<NodeReport, String>;

/// Asks each of `targets` for its `NodeReport` over a call connection made with `config`'s
/// node certificate (an operator CLI; no node need run from `config`). Each target gets
/// `timeout`; the answers come back in `targets` order.
pub fn query_status(
    config: &TransportConfig,
    targets: &[u64],
    timeout: Duration,
) -> Result<Vec<(u64, Answer)>> {
    let (_, connector) = transport::tls(config)?;
    let body = call_codec::encode_call(&PeerCall::Node(NodeCall::Status)).map_err(core)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let mut asks = tokio::task::JoinSet::new();
        for (i, &target) in targets.iter().enumerate() {
            let (config, connector, body) = (config.clone(), connector.clone(), body.clone());
            asks.spawn(async move {
                let ask = async {
                    let mut stream =
                        transport::tls::connect_admin(&config, &connector, target).await?;
                    let out = Header {
                        cluster: config.cluster,
                        from: config.node_id,
                        to: target,
                    };
                    let request = Body::CallRequest { id: 1, body };
                    transport::write_frame(&mut stream, &transport::encode(&out, &request)?)
                        .await?;
                    let back = Header {
                        from: target,
                        to: config.node_id,
                        ..out
                    };
                    match transport::decode(&back, &transport::read_frame(&mut stream).await?)? {
                        Body::CallResponse { id: 1, body } => Ok(call_codec::decode_reply(&body)),
                        _ => Err(Error::Corrupt("unexpected status reply frame".into())),
                    }
                };
                let answer: Answer = match tokio::time::timeout(timeout, ask).await {
                    Err(_) => Err("timed out".into()),
                    Ok(Err(e)) => Err(e.to_string()),
                    Ok(Ok(Err(e) | Ok(Err(e)))) => Err(e.to_string()),
                    Ok(Ok(Ok(Ok(PeerReply::Node(report))))) => Ok(report),
                    Ok(Ok(Ok(Ok(_)))) => Err("mismatched status reply".into()),
                };
                (i, target, answer)
            });
        }
        let mut answers = Vec::new();
        while let Some(joined) = asks.join_next().await {
            answers.extend(joined.ok());
        }
        answers.sort_by_key(|(i, _, _)| *i);
        Ok(answers.into_iter().map(|(_, t, a)| (t, a)).collect())
    })
}

/// Requests a move using the operator certificate, following the range leader hint.
/// The timeout bounds the complete request, including redirects and unavailable members.
pub fn request_move(
    config: &TransportConfig,
    members: &[u64],
    range: RangeId,
    from: u64,
    to: u64,
    timeout: Duration,
) -> Result<MoveReply> {
    match request_node(
        config,
        members,
        NodeCall::MoveRange {
            range,
            from,
            to,
            forwarded: false,
        },
        timeout,
    )? {
        PeerReply::Moving(reply) => Ok(reply),
        _ => Err(Error::Corrupt("mismatched move reply".into())),
    }
}

/// Send an operator call to a reachable member; move requests follow leader hints.
pub fn request_node(
    config: &TransportConfig,
    members: &[u64],
    call: NodeCall,
    timeout: Duration,
) -> Result<PeerReply> {
    let moving = matches!(call, NodeCall::MoveRange { .. });
    let (_, connector) = transport::tls(config)?;
    let body = call_codec::encode_call(&PeerCall::Node(call)).map_err(core)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let ask = async {
            let mut targets: std::collections::VecDeque<_> = members.iter().copied().collect();
            let mut visited = std::collections::BTreeSet::new();
            let mut last = "no reachable range leader".to_string();
            while let Some(target) = targets.pop_front() {
                if !visited.insert(target) {
                    continue;
                }
                let exchange = async {
                    let mut stream =
                        transport::tls::connect_admin(config, &connector, target).await?;
                    let out = Header {
                        cluster: config.cluster,
                        from: config.node_id,
                        to: target,
                    };
                    transport::write_frame(
                        &mut stream,
                        &transport::encode(
                            &out,
                            &Body::CallRequest {
                                id: 1,
                                body: body.clone(),
                            },
                        )?,
                    )
                    .await?;
                    let back = Header {
                        from: target,
                        to: config.node_id,
                        ..out
                    };
                    match transport::decode(&back, &transport::read_frame(&mut stream).await?)? {
                        Body::CallResponse { id: 1, body } => {
                            call_codec::decode_reply(&body).map_err(core)?.map_err(core)
                        }
                        _ => Err(Error::Corrupt("unexpected move reply frame".into())),
                    }
                };
                match tokio::time::timeout(
                    if moving {
                        Duration::from_secs(1)
                    } else {
                        timeout
                    },
                    exchange,
                )
                .await
                {
                    Ok(Ok(PeerReply::Moving(reply))) => {
                        if reply.leader == Some(target)
                            || reply.done
                            || reply.step.is_some()
                            || reply.error.is_some()
                        {
                            return Ok(PeerReply::Moving(reply));
                        }
                        if let Some(leader) = reply.leader {
                            targets.push_front(leader);
                        }
                    }
                    Ok(Ok(reply)) if !moving => return Ok(reply),
                    Ok(Ok(_)) => return Err(Error::Corrupt("mismatched move reply".into())),
                    Ok(Err(e)) => last = e.to_string(),
                    Err(_) => last = "move request timed out".into(),
                }
            }
            Err(Error::Apply(last))
        };
        tokio::time::timeout(timeout, ask)
            .await
            .map_err(|_| Error::Apply("move request timed out".into()))?
    })
}

/// Outcome of each sequential range replacement.
#[derive(Debug, PartialEq, Eq)]
pub enum ReplaceOutcome {
    Moved,
    Refused(String),
    Failed(String),
}
#[derive(Debug)]
pub struct ReplaceReport {
    pub ranges: Vec<(RangeId, ReplaceOutcome)>,
    pub retired: bool,
    pub retirement_error: Option<String>,
}

/// Replace every replica of a lost node, allowing each range its own timeout.
/// Enrolling the destination first is required. Failed ranges do not block attempts on others.
pub fn replace_node(
    config: &TransportConfig,
    members: &[u64],
    lost: u64,
    new: u64,
    timeout: Duration,
) -> Result<ReplaceReport> {
    if lost == new || lost == 0 || new == 0 {
        return Err(Error::Identity("distinct nonzero node ids required".into()));
    }
    let mut report = ReplaceReport {
        ranges: Vec::new(),
        retired: false,
        retirement_error: None,
    };
    for _ in 0..3 {
        let answers = query_status(config, members, timeout.min(Duration::from_secs(5)))?;
        if !answers.iter().any(|(_, a)| a.is_ok()) {
            return Err(Error::Apply("no reachable members".into()));
        }
        let mut newest = BTreeMap::<u64, RangeDescriptor>::new();
        for (_, answer) in answers {
            for r in answer.into_iter().flat_map(|n| n.ranges) {
                let d = r.descriptor;
                if newest.get(&d.id.0).is_none_or(|old| old.epoch < d.epoch) {
                    newest.insert(d.id.0, d);
                }
            }
        }
        // A timed-out request can still finish before the next discovery pass.
        for (range, outcome) in &mut report.ranges {
            if newest.get(&range.0).is_some_and(|d| {
                !d.replicas.iter().any(|r| r.node == lost)
                    && d.replicas.iter().any(|r| {
                        r.node == new && r.role == giggles_core::range_api::ReplicaRole::Voter
                    })
            }) {
                *outcome = ReplaceOutcome::Moved;
            }
        }
        for d in newest
            .values()
            .filter(|d| d.replicas.iter().any(|r| r.node == lost))
        {
            let deadline = Instant::now() + timeout;
            let mut delay = Duration::from_millis(100);
            let mut last_error = "move timed out".to_string();
            let outcome = loop {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    break ReplaceOutcome::Failed(last_error);
                }
                match request_move(
                    config,
                    members,
                    d.id,
                    lost,
                    new,
                    remaining.min(Duration::from_secs(5)),
                ) {
                    Ok(reply) => {
                        if let Some(error) = reply.error {
                            last_error = error;
                        }
                        if reply.done {
                            break ReplaceOutcome::Moved;
                        }
                    }
                    Err(error) => last_error = error.to_string(),
                }
                thread::sleep(delay.min(deadline.saturating_duration_since(Instant::now())));
                delay = (delay * 2).min(Duration::from_secs(2));
            };
            if let Some(entry) = report.ranges.iter_mut().find(|(id, _)| *id == d.id) {
                entry.1 = outcome;
            } else {
                report.ranges.push((d.id, outcome));
            }
        }
        if report
            .ranges
            .iter()
            .all(|(_, o)| *o == ReplaceOutcome::Moved)
        {
            match request_node(config, members, NodeCall::RetireNode { id: lost }, timeout) {
                Ok(PeerReply::Retired(_)) => {
                    report.retired = true;
                    report.retirement_error = None;
                }
                Ok(_) => report.retirement_error = Some("mismatched retirement reply".into()),
                Err(e) => report.retirement_error = Some(e.to_string()),
            }
        }
        if report.retired
            || !newest
                .values()
                .any(|d| d.replicas.iter().any(|r| r.node == lost))
        {
            break;
        }
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enrolment_requires_specified_address_and_pinned_identity() {
        let fp = "a".repeat(64);
        let peers = BTreeMap::from([(
            1,
            PeerConfig {
                address: "127.0.0.1:7000".into(),
                fingerprint: fp.clone(),
            },
        )]);
        for address in ["127.0.0.1:7000", "[::1]:7000"] {
            assert!(validate_enrolment(&peers, 1, address, &fp).is_ok());
            assert!(validate_enrolment(&peers, 2, address, &fp).is_ok());
        }
        for address in ["0.0.0.0:7000", "[::]:7000", "localhost:7000", "bad"] {
            assert!(validate_enrolment(&peers, 2, address, &fp).is_err());
        }
        for fingerprint in ["A".repeat(64), "g".repeat(64), "a".repeat(63)] {
            assert!(validate_enrolment(&peers, 2, "127.0.0.1:7000", &fingerprint).is_err());
        }
        assert!(validate_enrolment(&peers, 0, "127.0.0.1:7000", &fp).is_err());
        assert!(validate_enrolment(&peers, 1, "127.0.0.1:7000", &"b".repeat(64)).is_err());
    }
}
