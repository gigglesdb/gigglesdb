use super::calls::{CallLimits, CallServer, PeerCalls};
use super::{
    Body, GroupRouter, Header, OPERATOR, PeerConfig, TransportConfig, fingerprint, frame, tls,
};
use crate::{
    client::LocalRanges,
    error::{Error, Result},
    group::{group_cluster, group_of},
    sink::Outbox,
    snapshot::{Manifest, SnapshotStore},
};
use giggles_core::range_api::GatewayService;
use raft::eraftpb::{Message, MessageType};
use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    sync::{Arc, OnceLock, RwLock},
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWrite},
    net::TcpListener,
    runtime::Runtime,
    sync::{Semaphore, mpsc, oneshot, watch},
    task::{JoinHandle, JoinSet},
};

pub trait PeerSink: Send + Sync {
    fn deliver(&self, from: u64, m: Message);
    fn admin(&self, _from: u64, _req: crate::admin::AdminRequest) -> crate::admin::AdminResponse {
        crate::admin::AdminResponse::Failed("admin not supported".into())
    }
    fn snapshot_status(&self, to: u64, ok: bool);
}

pub struct LateSink<S>(OnceLock<S>);

impl<S> Default for LateSink<S> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S> LateSink<S> {
    pub fn new() -> Self {
        Self(OnceLock::new())
    }
    pub fn set(&self, s: S) {
        let _ = self.0.set(s);
    }
}

impl<S: PeerSink> PeerSink for LateSink<S> {
    fn admin(&self, from: u64, req: crate::admin::AdminRequest) -> crate::admin::AdminResponse {
        self.0.get().map_or_else(
            || crate::admin::AdminResponse::Failed("admin not supported".into()),
            |s| s.admin(from, req),
        )
    }
    fn deliver(&self, from: u64, m: Message) {
        if let Some(s) = self.0.get() {
            s.deliver(from, m);
        }
    }
    fn snapshot_status(&self, to: u64, ok: bool) {
        if let Some(s) = self.0.get() {
            s.snapshot_status(to, ok);
        }
    }
}

/// One group's outbox; every group's outbox shares the per-peer queues.
#[derive(Clone)]
struct Sender {
    shared: Arc<Peers>,
    router: Arc<GroupRouter>,
    group: u64,
}

struct Peer {
    tx: mpsc::Sender<(u64, Message)>,
    task: JoinHandle<()>,
    permit: Arc<Semaphore>,
    cancel: watch::Sender<()>,
}
impl Drop for Peer {
    fn drop(&mut self) {
        self.task.abort();
    }
}
struct PeerTable {
    config: BTreeMap<u64, PeerConfig>,
    active: BTreeMap<u64, Peer>,
}
pub(crate) struct Peers {
    pub(super) config: Arc<TransportConfig>,
    table: RwLock<PeerTable>,
    skipped: RwLock<BTreeSet<(u64, String, &'static str)>>,
    connector: tokio_rustls::TlsConnector,
    pub(super) runtime: tokio::runtime::Handle,
}
impl Peers {
    fn set(self: &Arc<Self>, mut peers: BTreeMap<u64, PeerConfig>) -> Result<()> {
        peers.retain(|id, peer| {
            let reason = if *id == OPERATOR {
                "node id 0 is reserved for operators"
            } else if self.config.operators.contains(&peer.fingerprint) {
                "fingerprint belongs to an operator"
            } else {
                return true;
            };
            if self
                .skipped
                .write()
                .unwrap()
                .insert((*id, peer.fingerprint.clone(), reason))
            {
                eprintln!("replication skipping peers[{id}]: {reason}");
            }
            false
        });
        let mut seen = BTreeSet::new();
        if peers.get(&self.config.node_id).map(|p| &p.fingerprint)
            != self
                .config
                .peers
                .get(&self.config.node_id)
                .map(|p| &p.fingerprint)
        {
            return Err(Error::Identity(
                "own node missing or fingerprint changed".into(),
            ));
        }
        for (id, peer) in &peers {
            let fp = &peer.fingerprint;
            if fp.len() != 64
                || !fp
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                || !seen.insert(fp)
            {
                return Err(Error::Identity(format!(
                    "peers[{id}].fingerprint invalid or duplicate"
                )));
            }
        }
        let mut table = self.table.write().unwrap();
        let changed: Vec<_> = table
            .active
            .keys()
            .copied()
            .filter(|id| {
                peers.get(id).is_none_or(|p| {
                    table.config[id].address != p.address
                        || table.config[id].fingerprint != p.fingerprint
                })
            })
            .collect();
        for id in changed {
            table.active.remove(&id);
        }
        for &id in peers.keys() {
            if id == self.config.node_id || table.active.contains_key(&id) {
                continue;
            }
            let (tx, rx) = mpsc::channel(1024);
            let task = self.runtime.spawn(outbound(self.clone(), id, rx));
            let (cancel, _) = watch::channel(());
            table.active.insert(
                id,
                Peer {
                    tx,
                    task,
                    permit: Arc::new(Semaphore::new(1)),
                    cancel,
                },
            );
        }
        table.config = peers;
        Ok(())
    }
    pub(super) async fn connect(
        &self,
        id: u64,
    ) -> Result<tokio_rustls::client::TlsStream<tokio::net::TcpStream>> {
        let mut config = (*self.config).clone();
        config.peers = self.table.read().unwrap().config.clone();
        let stream = tls::connect(&config, &self.connector, id).await?;
        let table = self.table.read().unwrap();
        if table.config.get(&id).is_none_or(|p| {
            p.fingerprint != config.peers[&id].fingerprint || p.address != config.peers[&id].address
        }) {
            return Err(protocol_error());
        }
        Ok(stream)
    }
}

impl Outbox for Sender {
    fn apply_membership(&mut self, m: &crate::membership::Membership) {
        // The peer table is node-wide. Only an R4 single-group database (group 0)
        // derives it from its group's membership; a range host sets it itself.
        if self.group != 0 {
            return;
        }
        let mut peers = self.shared.config.peers.clone();
        for (id, peer) in &m.peers {
            peers.insert(
                *id,
                PeerConfig {
                    address: peer.address.clone(),
                    fingerprint: peer.fingerprint.clone(),
                },
            );
        }
        peers.retain(|id, _| !m.retired.contains(id));
        if let Err(error) = self.shared.set(peers) {
            eprintln!("replication apply_membership failed: {error}");
        }
    }

    fn set_peers(&mut self, peers: &BTreeMap<u64, PeerConfig>) {
        if self.group != 0 {
            return;
        }
        if let Err(error) = self.shared.set(peers.clone()) {
            eprintln!("replication set_peers failed: {error}");
        }
    }
    fn send(&mut self, m: Message) {
        if m.has_snapshot() || m.get_msg_type() == MessageType::MsgSnapshot {
            // A group unregistered here has no replica left to report to.
            let Some(route) = self.router.route(self.group) else {
                return;
            };
            let sink = route.sink;
            let Some(store) = route.snapshots else {
                sink.snapshot_status(m.to, false);
                return;
            };
            let shared = self.shared.clone();
            let cluster = group_cluster(shared.config.cluster, self.group);
            let table = self.shared.table.read().unwrap();
            let Some(peer) = table.active.get(&m.to) else {
                sink.snapshot_status(m.to, false);
                return;
            };
            let Ok(permit) = peer.permit.clone().try_acquire_owned() else {
                sink.snapshot_status(m.to, false);
                return;
            };
            let mut cancelled = peer.cancel.subscribe();
            let runtime = shared.runtime.clone();
            runtime.spawn(async move {
                let _permit = permit;
                let ok = tokio::select! {
                    biased;
                    _ = cancelled.changed() => false,
                    result = tokio::time::timeout(Duration::from_secs(300), send_snapshot(&shared, &store, cluster, &m)) => matches!(result, Ok(Ok(()))),
                };
                sink.snapshot_status(m.to, ok);
            });
        } else if let Some(peer) = self.shared.table.read().unwrap().active.get(&m.to) {
            let _ = peer.tx.try_send((self.group, m));
        }
    }
}

pub struct Transport {
    runtime: Option<Runtime>,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
    addr: SocketAddr,
    shared: Arc<Peers>,
    router: Arc<GroupRouter>,
    calls: Arc<PeerCalls>,
    call_server: Arc<CallServer>,
}

impl Transport {
    /// R4 single-group mode: a router holding only group 0.
    pub fn start(
        config: TransportConfig,
        sink: Arc<dyn PeerSink>,
        snapshots: Option<Arc<SnapshotStore>>,
    ) -> Result<Self> {
        let router = Arc::new(GroupRouter::new());
        router.register(0, sink, snapshots);
        Self::start_groups(config, router)
    }

    /// Many groups over one transport. `config.cluster` is the database
    /// cluster id; group `g`'s frames carry `group_cluster(config.cluster, g)`.
    pub fn start_groups(config: TransportConfig, router: Arc<GroupRouter>) -> Result<Self> {
        config.validate()?;
        let (acceptor, connector) = tls::tls(&config)?;
        let listener = std::net::TcpListener::bind(&config.listen)?;
        listener.set_nonblocking(true)?;
        let addr = listener.local_addr()?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()?;
        let listener = {
            let _enter = runtime.enter();
            TcpListener::from_std(listener)?
        };
        let config = Arc::new(config);
        let shared = Arc::new(Peers {
            config: config.clone(),
            table: RwLock::new(PeerTable {
                config: BTreeMap::new(),
                active: BTreeMap::new(),
            }),
            skipped: RwLock::default(),
            connector,
            runtime: runtime.handle().clone(),
        });
        shared.set(config.peers.clone())?;
        let accept_shared = shared.clone();
        let accept_router = router.clone();
        let call_server = Arc::new(CallServer::new());
        let accept_calls = call_server.clone();
        let (stop, mut stopped) = oneshot::channel();
        let task = runtime.spawn(async move {
            let mut tasks = JoinSet::new();
            // Each peer keeps two lasting inbound connections (Raft stream, calls),
            // plus short snapshot and admin exchanges.
            let permits = Arc::new(Semaphore::new(64));
            loop {
                tokio::select! {
                    _ = &mut stopped => break,
                    Some(_) = tasks.join_next(), if !tasks.is_empty() => {},
                    accepted = listener.accept() => {
                        let (tcp, _) = match accepted {
                            Ok(pair) => pair,
                            Err(error) => {
                                eprintln!("replication accept failed: {error}");
                                tokio::time::sleep(Duration::from_millis(50)).await;
                                continue;
                            }
                        };
                        // Peer traffic is small request/reply frames: without NODELAY, Nagle
                        // holds each reply for the peer's delayed ACK (40 ms on Linux).
                        if tcp.set_nodelay(true).is_err() { continue }
                        let Ok(permit) = permits.clone().try_acquire_owned() else { continue };
                        let config = config.clone();
                        let shared = accept_shared.clone();
                        let acceptor = acceptor.clone();
                        let router = accept_router.clone();
                        let calls = accept_calls.clone();
                        tasks.spawn(async move {
                            let _permit = permit;
                            let Ok(Ok(mut stream)) = tokio::time::timeout(
                                Duration::from_secs(10), acceptor.accept(tcp)
                            ).await else { return };
                            let Some(fp) = stream.get_ref().1.peer_certificates().and_then(|c| c.first()).map(|c| fingerprint(c)) else { return };
                            let peer = shared.table.read().unwrap().config.iter().find_map(|(&id, p)|
                                (p.fingerprint == fp).then_some(id));
                            let peer = match peer {
                                Some(peer) => peer,
                                None if config.operators.contains(&fp) => OPERATOR,
                                None => return,
                            };
                            let h = Header { cluster: config.cluster, from: peer, to: config.node_id };
                            let _ = inbound(&mut stream, h, &router, &calls, &shared, &fp).await;

                        });
                    }
                }
            }
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
        });
        Ok(Self {
            runtime: Some(runtime),
            stop: Some(stop),
            task: Some(task),
            addr,
            calls: Arc::new(PeerCalls::new(shared.clone())),
            shared,
            router,
            call_server,
        })
    }

    /// Client side of range and gateway calls to peers (R5-21).
    pub fn calls(&self) -> &Arc<PeerCalls> {
        &self.calls
    }

    /// Handlers of inbound peer calls; a call without its handler gets `Unavailable`.
    pub fn set_call_handlers(
        &self,
        local: Option<Arc<dyn LocalRanges>>,
        gateway: Option<Arc<dyn GatewayService>>,
    ) {
        self.call_server.set_handlers(local, gateway);
    }

    /// Handler of inbound node-status calls (R5-64); without it they get `Unavailable`.
    pub fn set_node_handler(&self, node: Option<Arc<dyn super::NodeService>>) {
        self.call_server.set_node_handler(node);
    }

    /// Queue, pool and timeout bounds of peer calls; open call connections are restarted.
    pub fn set_call_limits(&self, limits: CallLimits) {
        self.calls.set_limits(limits);
        self.call_server.set_limits(limits);
    }

    pub fn set_peers(&self, peers: BTreeMap<u64, PeerConfig>) -> Result<()> {
        self.shared.set(peers)
    }

    pub fn router(&self) -> &Arc<GroupRouter> {
        &self.router
    }

    /// Group `group`'s outbox; it shares each peer's connection with every other group.
    pub fn group_outbox(&self, group: u64) -> Box<dyn Outbox> {
        Box::new(Sender {
            shared: self.shared.clone(),
            router: self.router.clone(),
            group,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }
    pub fn outbox(&self) -> Box<dyn Outbox> {
        self.group_outbox(0)
    }
    pub fn shutdown(self) {
        drop(self);
    }
}

impl Drop for Transport {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(runtime) = self.runtime.take() {
            let task = self.task.take();
            let _ = std::thread::spawn(move || {
                let deadline = Instant::now() + Duration::from_secs(5);
                if let Some(task) = task {
                    runtime.block_on(async {
                        let _ = tokio::time::timeout(Duration::from_secs(5), task).await;
                    });
                }
                runtime.shutdown_timeout(deadline.saturating_duration_since(Instant::now()));
            })
            .join();
        }
    }
}

async fn outbound(peers: Arc<Peers>, id: u64, mut rx: mpsc::Receiver<(u64, Message)>) {
    let config = &peers.config;
    let mut backoff = Duration::from_millis(100);
    let mut pending = None;
    loop {
        let connected = peers.connect(id).await;
        if let Ok(mut stream) = connected {
            backoff = Duration::from_millis(100);
            let mut probe = [0];
            loop {
                let (group, m) = if let Some(item) = pending.take() {
                    item
                } else {
                    tokio::select! {
                        biased;
                        // Outbound Raft streams are one-way; EOF or unsolicited data
                        // invalidates this connection even when there is no queued work.
                        _ = stream.read(&mut probe) => break,
                        m = rx.recv() => match m { Some(m) => m, None => return },
                    }
                };
                let h = Header {
                    cluster: group_cluster(config.cluster, group),
                    from: config.node_id,
                    to: id,
                };
                let Ok(bytes) = frame::encode(&h, &Body::Raft(m.clone())) else {
                    continue;
                };
                if frame::write_frame(&mut stream, &bytes).await.is_err() {
                    pending = Some((group, m));
                    break;
                }
            }
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(2));
    }
}

fn protocol_error() -> Error {
    Error::Corrupt("unexpected snapshot transfer frame or resume point".into())
}

async fn write_body<S: AsyncWrite + Unpin>(s: &mut S, h: &Header, b: Body) -> Result<()> {
    frame::write_frame(s, &frame::encode(h, &b)?).await
}

async fn read_body<S: AsyncRead + Unpin>(s: &mut S, h: &Header) -> Result<Body> {
    frame::decode(h, &frame::read_frame(s).await?)
}

/// Sends one group's snapshot; `cluster` is that group's cluster id.
async fn send_snapshot(
    shared: &Peers,
    store: &SnapshotStore,
    cluster: [u8; 16],
    m: &Message,
) -> Result<()> {
    let manifest = Manifest::from_raft(cluster, m.get_snapshot())?;
    let dir = store.built_data_dir(&manifest);
    if !dir.is_dir() {
        return Err(protocol_error());
    }
    let mut stream = shared.connect(m.to).await?;
    let h = Header {
        cluster,
        from: shared.config.node_id,
        to: m.to,
    };
    let reply = Header {
        from: h.to,
        to: h.from,
        ..h
    };
    write_body(&mut stream, &h, Body::SnapOffer(manifest.encode())).await?;
    let Body::SnapResume { file, offset } = read_body(&mut stream, &reply).await? else {
        return Err(protocol_error());
    };
    if file as usize > manifest.files.len()
        || (file as usize == manifest.files.len() && offset != 0)
        || manifest
            .files
            .get(file as usize)
            .is_some_and(|f| offset > f.size)
    {
        return Err(protocol_error());
    }
    let mut buffer = vec![0; (256 << 10).min(frame::MAX_CHUNK)];
    for (i, entry) in manifest.files.iter().enumerate().skip(file as usize) {
        let mut offset = if i == file as usize { offset } else { 0 };
        let mut input = tokio::fs::File::open(dir.join(&entry.name)).await?;
        if input.metadata().await?.len() != entry.size {
            return Err(protocol_error());
        }
        input.seek(std::io::SeekFrom::Start(offset)).await?;
        while offset < entry.size {
            let n = buffer.len().min((entry.size - offset) as usize);
            input.read_exact(&mut buffer[..n]).await?;
            write_body(
                &mut stream,
                &h,
                Body::SnapChunk {
                    file: i as u32,
                    offset,
                    data: buffer[..n].to_vec(),
                },
            )
            .await?;
            offset += n as u64;
        }
    }
    write_body(&mut stream, &h, Body::SnapDone).await?;
    match read_body(&mut stream, &reply).await? {
        Body::SnapAck(Ok(())) => write_body(&mut stream, &h, Body::Raft(m.clone())).await,
        _ => Err(protocol_error()),
    }
}

/// Serves one inbound connection. `base` carries the database cluster id;
/// each frame names its group through its header cluster (`group::group_of`).
async fn inbound<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    base: Header,
    router: &GroupRouter,
    calls: &CallServer,
    peers: &Peers,
    fp: &str,
) -> Result<()> {
    loop {
        let (group, h, body) = read_group(stream, &base, peers, fp).await?;
        if h.from == OPERATOR && !matches!(body, Body::AdminRequest(_)) {
            return Err(protocol_error());
        }
        let reply = Header {
            from: h.to,
            to: h.from,
            ..h
        };
        if group != 0 && matches!(body, Body::AdminRequest(_)) {
            let response = crate::admin::AdminResponse::Refused(
                "replica admin is only for the replicated database group".into(),
            );
            write_body(stream, &reply, Body::AdminResponse(response.encode()?)).await?;
            return Ok(());
        }
        if let Body::CallRequest { id, body } = body {
            // A call connection carries only calls, all under the database cluster id. One
            // from this node's own certificate gets the node CLI calls allowed by CallServer.
            if group != 0 {
                return Err(protocol_error());
            }
            let check = || authorized(&peers.table.read().unwrap(), h.from, fp).is_ok();
            return calls.serve(stream, h, (id, body), &check).await;
        }
        let Some(route) = router.route(group) else {
            // Dropped and counted. A Raft stream stays up for the other groups;
            // an admin call or snapshot transfer is one exchange, so it closes.
            router.count_unknown();
            if matches!(body, Body::Raft(_)) {
                continue;
            }
            return Err(protocol_error());
        };
        let sink = route.sink;
        match body {
            Body::AdminRequest(bytes) => {
                let response = if h.from != OPERATOR && h.from != h.to {
                    crate::admin::AdminResponse::Refused(
                        "replica admin needs an operator identity".into(),
                    )
                } else {
                    let req = crate::admin::AdminRequest::decode(&bytes)?;
                    tokio::task::spawn_blocking(move || sink.admin(h.from, req))
                        .await
                        .map_err(|_| protocol_error())?
                };
                write_body(stream, &reply, Body::AdminResponse(response.encode()?)).await?;
                return Ok(());
            }
            Body::Raft(m) => {
                if h.from == h.to {
                    return Err(protocol_error());
                }
                let table = peers.table.read().unwrap();
                authorized(&table, h.from, fp)?;
                sink.deliver(h.from, m);
            }
            Body::SnapOffer(bytes) => {
                if h.from == h.to {
                    return Err(protocol_error());
                }
                let manifest = Manifest::decode(h.cluster, &bytes)?;
                let store = route.snapshots.as_deref();
                let mut writer = store.ok_or_else(protocol_error)?.incoming(&manifest)?;
                let (file, offset) = writer.resume_point();
                write_body(stream, &reply, Body::SnapResume { file, offset }).await?;
                loop {
                    match read_current(stream, &h, peers, fp).await? {
                        Body::SnapChunk { file, offset, data } => {
                            if let Err(error) = writer.write_chunk(file, offset, &data) {
                                write_body(stream, &reply, Body::SnapAck(Err(error.to_string())))
                                    .await?;
                                return Err(error);
                            }
                        }
                        Body::SnapDone => {
                            let result = writer.finish().map(|_| ()).map_err(|e| e.to_string());
                            let ok = result.is_ok();
                            write_body(stream, &reply, Body::SnapAck(result)).await?;
                            if !ok {
                                return Err(protocol_error());
                            }
                            break;
                        }
                        _ => return Err(protocol_error()),
                    }
                }
                match read_current(stream, &h, peers, fp).await? {
                    Body::Raft(m)
                        if m.get_msg_type() == MessageType::MsgSnapshot
                            && m.has_snapshot()
                            && m.get_snapshot().get_metadata().index == manifest.index
                            && m.get_snapshot().get_metadata().term == manifest.term =>
                    {
                        let table = peers.table.read().unwrap();
                        authorized(&table, h.from, fp)?;
                        sink.deliver(h.from, m);
                        return Ok(());
                    }
                    _ => return Err(protocol_error()),
                }
            }
            _ => return Err(protocol_error()),
        }
    }
}

fn authorized(table: &PeerTable, id: u64, fp: &str) -> Result<()> {
    if table.config.get(&id).is_some_and(|p| p.fingerprint == fp) {
        Ok(())
    } else {
        Err(protocol_error())
    }
}
fn authorize_identity(peers: &Peers, id: u64, fp: &str) -> Result<()> {
    if id == OPERATOR {
        if peers.config.operators.iter().any(|operator| operator == fp) {
            Ok(())
        } else {
            Err(protocol_error())
        }
    } else {
        authorized(&peers.table.read().unwrap(), id, fp)
    }
}
async fn read_current<S: AsyncRead + Unpin>(
    stream: &mut S,
    h: &Header,
    peers: &Peers,
    fp: &str,
) -> Result<Body> {
    let body = read_body(stream, h).await?;
    authorize_identity(peers, h.from, fp)?;
    Ok(body)
}

/// Reads one frame of any group derived from `base.cluster`. A cluster that
/// derives from no group is rejected exactly as R4 rejects another cluster.
async fn read_group<S: AsyncRead + Unpin>(
    stream: &mut S,
    base: &Header,
    peers: &Peers,
    fp: &str,
) -> Result<(u64, Header, Body)> {
    let bytes = frame::read_frame(stream).await?;
    let group = bytes
        .get(8..24)
        .and_then(|c| <[u8; 16]>::try_from(c).ok())
        .and_then(|c| Some((group_of(base.cluster, c)?, c)));
    let Some((group, cluster)) = group else {
        frame::decode(base, &bytes)?;
        return Err(protocol_error());
    };
    let h = Header { cluster, ..*base };
    let body = frame::decode(&h, &bytes)?;
    authorize_identity(peers, h.from, fp)?;
    Ok((group, h, body))
}
