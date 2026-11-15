use super::{Body, Header, TransportConfig, frame, tls};
use crate::{
    error::{Error, Result},
    sink::Outbox,
    snapshot::{Manifest, SnapshotStore},
};
use raft::eraftpb::{Message, MessageType};
use std::{
    collections::BTreeMap,
    net::SocketAddr,
    sync::{Arc, OnceLock},
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWrite},
    net::TcpListener,
    runtime::Runtime,
    sync::{Semaphore, mpsc, oneshot},
    task::{JoinHandle, JoinSet},
};

pub trait PeerSink: Send + Sync {
    fn deliver(&self, from: u64, m: Message);
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

#[derive(Clone)]
struct Sender {
    peers: BTreeMap<u64, mpsc::Sender<Message>>,
    sink: Arc<dyn PeerSink>,
    snapshots: Option<Arc<SnapshotSender>>,
}

struct SnapshotSender {
    config: Arc<TransportConfig>,
    connector: tokio_rustls::TlsConnector,
    store: Arc<SnapshotStore>,
    runtime: tokio::runtime::Handle,
    permits: BTreeMap<u64, Arc<Semaphore>>,
}

impl Outbox for Sender {
    fn send(&mut self, m: Message) {
        if m.has_snapshot() || m.get_msg_type() == MessageType::MsgSnapshot {
            let Some(snapshots) = self.snapshots.clone() else {
                self.sink.snapshot_status(m.to, false);
                return;
            };
            let permit = snapshots
                .permits
                .get(&m.to)
                .and_then(|p| p.clone().try_acquire_owned().ok());
            let Some(permit) = permit else {
                self.sink.snapshot_status(m.to, false);
                return;
            };
            let sink = self.sink.clone();
            let runtime = snapshots.runtime.clone();
            runtime.spawn(async move {
                let _permit = permit;
                let ok = matches!(
                    tokio::time::timeout(Duration::from_secs(300), send_snapshot(&snapshots, &m))
                        .await,
                    Ok(Ok(()))
                );
                sink.snapshot_status(m.to, ok);
            });
        } else if let Some(tx) = self.peers.get(&m.to) {
            let _ = tx.try_send(m);
        }
    }
}

pub struct Transport {
    runtime: Option<Runtime>,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
    addr: SocketAddr,
    sender: Sender,
}

impl Transport {
    pub fn start(
        config: TransportConfig,
        sink: Arc<dyn PeerSink>,
        snapshots: Option<Arc<SnapshotStore>>,
    ) -> Result<Self> {
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
        let mut peers = BTreeMap::new();
        let mut queues = Vec::new();
        for &id in config.peers.keys().filter(|&&id| id != config.node_id) {
            let (tx, rx) = mpsc::channel(1024);
            peers.insert(id, tx);
            queues.push((id, rx));
        }
        let snapshot_sender = snapshots.as_ref().map(|store| {
            Arc::new(SnapshotSender {
                config: config.clone(),
                connector: connector.clone(),
                store: store.clone(),
                runtime: runtime.handle().clone(),
                permits: peers
                    .keys()
                    .map(|&id| (id, Arc::new(Semaphore::new(1))))
                    .collect(),
            })
        });
        let sender = Sender {
            snapshots: snapshot_sender,
            peers,
            sink: sink.clone(),
        };
        let (stop, mut stopped) = oneshot::channel();
        let task = runtime.spawn(async move {
            let mut tasks = JoinSet::new();
            for (id, rx) in queues {
                tasks.spawn(outbound(config.clone(), connector.clone(), id, rx));
            }
            let permits = Arc::new(Semaphore::new(16));
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
                        let Ok(permit) = permits.clone().try_acquire_owned() else { continue };
                        let config = config.clone();
                        let acceptor = acceptor.clone();
                        let sink = sink.clone();
                        let snapshots = snapshots.clone();
                        tasks.spawn(async move {
                            let _permit = permit;
                            let Ok(Ok((peer, mut stream))) = tokio::time::timeout(
                                Duration::from_secs(10), tls::accept(&config, &acceptor, tcp)
                            ).await else { return };
                            let h = Header { cluster: config.cluster, from: peer, to: config.node_id };
                            let _ = inbound(&mut stream, h, sink.as_ref(), snapshots.as_deref()).await;
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
            sender,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }
    pub fn outbox(&self) -> Box<dyn Outbox> {
        Box::new(self.sender.clone())
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

async fn outbound(
    config: Arc<TransportConfig>,
    connector: tokio_rustls::TlsConnector,
    id: u64,
    mut rx: mpsc::Receiver<Message>,
) {
    let h = Header {
        cluster: config.cluster,
        from: config.node_id,
        to: id,
    };
    let mut backoff = Duration::from_millis(100);
    let mut pending = None;
    loop {
        let connected = tls::connect(&config, &connector, id).await;
        if let Ok(mut stream) = connected {
            backoff = Duration::from_millis(100);
            let mut probe = [0];
            loop {
                let m = if let Some(m) = pending.take() {
                    m
                } else {
                    tokio::select! {
                        biased;
                        // Outbound Raft streams are one-way; EOF or unsolicited data
                        // invalidates this connection even when there is no queued work.
                        _ = stream.read(&mut probe) => break,
                        m = rx.recv() => match m { Some(m) => m, None => return },
                    }
                };
                let Ok(bytes) = frame::encode(&h, &Body::Raft(m.clone())) else {
                    continue;
                };
                if frame::write_frame(&mut stream, &bytes).await.is_err() {
                    pending = Some(m);
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

async fn send_snapshot(sender: &SnapshotSender, m: &Message) -> Result<()> {
    let manifest = Manifest::from_raft(sender.config.cluster, m.get_snapshot())?;
    let dir = sender.store.built_data_dir(&manifest);
    if !dir.is_dir() {
        return Err(protocol_error());
    }
    let mut stream = tls::connect(&sender.config, &sender.connector, m.to).await?;
    let h = Header {
        cluster: sender.config.cluster,
        from: sender.config.node_id,
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

async fn inbound<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    h: Header,
    sink: &dyn PeerSink,
    store: Option<&SnapshotStore>,
) -> Result<()> {
    let reply = Header {
        from: h.to,
        to: h.from,
        ..h
    };
    loop {
        match read_body(stream, &h).await? {
            Body::Raft(m) => sink.deliver(h.from, m),
            Body::SnapOffer(bytes) => {
                let manifest = Manifest::decode(h.cluster, &bytes)?;
                let mut writer = store.ok_or_else(protocol_error)?.incoming(&manifest)?;
                let (file, offset) = writer.resume_point();
                write_body(stream, &reply, Body::SnapResume { file, offset }).await?;
                loop {
                    match read_body(stream, &h).await? {
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
                match read_body(stream, &h).await? {
                    Body::Raft(m)
                        if m.get_msg_type() == MessageType::MsgSnapshot
                            && m.has_snapshot()
                            && m.get_snapshot().get_metadata().index == manifest.index
                            && m.get_snapshot().get_metadata().term == manifest.term =>
                    {
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
