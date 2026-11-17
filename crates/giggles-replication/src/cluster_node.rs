//! One node of a range cluster (GD-0075 Phase 7): the range store, its engines, the
//! multi-group host, the peer transport and a routed client, assembled once for tests and
//! for giggles-server. Every node is a gateway: `attach_gateway` gives it a SQL `Admin`.
//! Each node counts the calls its leader engines serve per range and answers node-status
//! calls (R5-64, `giggles-server cluster status`).
use crate::{
    bootstrap::{directory_records, hosted_by},
    call_codec::{
        self, CallCounts, NodeCall, NodeReport, PeerCall, PeerReply, RangeReport, RangeRole,
    },
    client::{LocalRanges, RangeTransport, RoutedRangeClient},
    directory,
    error::{Error, Result},
    host::{RangeHost, RangeHostConfig},
    liveness::LivenessConfig,
    sink::SnapshotPolicy,
    transport::{
        self, Body, CallLimits, GroupRouter, Header, NodeService, Transport, TransportConfig,
        TransportNetwork,
    },
};
use giggles_core::{
    Limits,
    cluster_node::{ClusterGateway, EngineSettings, GatewaySettings, NodeEngines, attach_gateway},
    range_api::{
        ClusterTxnId, Epoch, GatewayPeer, HomeOp, HomeReply, LeaseKind, LockGrant, LockRequest,
        Outcome, RangeClient, RangeDescriptor, RangeError, RangeId, RangeService, RangeSpace,
        RangeWrite, ReadDirection, ReadPage, ReadRequest, SYSTEM_RANGE, SequenceOp,
        SequenceReply, SystemService, Timestamp, TsOracle,
    },
    replicated::RangeStore,
};
use std::{
    collections::HashMap,
    ops::{Bound, Range},
    io::Write,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock, Weak},
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
        let window = self.start.elapsed().saturating_sub(oldest).as_millis().max(1) as u64;
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

/// Answers node-status calls; holds the host weakly (the transport's handlers outlive it).
struct StatusService {
    node_id: u64,
    host: Weak<RangeHost>,
    store: Arc<RangeStore>,
    load: Arc<LoadCounters>,
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
            });
        }
        NodeReport {
            node: self.node_id,
            ranges,
        }
    }
}

impl NodeService for StatusService {
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
    client: Arc<RoutedRangeClient>,
    /// The counted leaders the transport serves peer calls from.
    ranges: Arc<dyn LocalRanges>,
    status: Arc<StatusService>,
    /// The system range's descriptor as this node last learned it: the layout's at start,
    /// then the one a StaleEpoch refusal from the system range carried (review S6).
    system: Mutex<RangeDescriptor>,
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
                return Err(Error::Identity("range store belongs to another cluster".into()));
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
            client,
            ranges,
            status,
        })
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

    /// This node's SQL gateway; its deadlock service answers other gateways' probes.
    pub fn attach_gateway(
        &self,
        settings: GatewaySettings,
        limits: Limits,
    ) -> Result<ClusterGateway> {
        let peer: Arc<dyn GatewayPeer> = self.transport.calls().clone();
        let gateway = attach_gateway(
            self.client.clone() as Arc<dyn RangeClient>,
            settings,
            Some(peer),
            self.cluster,
            limits,
        )
        .map_err(core)?;
        self.transport
            .set_call_handlers(Some(self.ranges.clone()), gateway.gateway_service.clone());
        Ok(gateway)
    }

    /// Stops every group and the transport.
    pub fn stop(self) {
        self.host.stop();
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
