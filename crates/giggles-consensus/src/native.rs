//! Version 1 external native subset. Trusted commands never cross this boundary.
use crate::{
    Applied, Node, Result, TaskAction,
    authority::{ClockSource, LeaderClock, Session},
    node::HeldOutput,
};
use giggles_core::local::Reply;
use rustls::{
    ClientConfig, RootCertStore, ServerConfig,
    pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::BufReader,
    path::PathBuf,
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot},
    time::{Instant, timeout_at},
};
use tokio_rustls::{TlsAcceptor, TlsConnector};
pub const MAX_FRAME: usize = 64 * 1024;
pub const MAX_SESSIONS: usize = 16;
pub const MAX_PENDING: usize = 32;
pub const MAX_DEADLINE_MS: u64 = 5000;
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Configuration {
    pub listen: String,
    pub certificate: PathBuf,
    pub key: PathBuf,
    pub ca: PathBuf,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub version: u32,
    pub id: u64,
    pub deadline_ms: u64,
    pub action: TaskAction,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Response {
    pub version: u32,
    pub id: u64,
    pub error: Option<String>,
    pub reply: Option<Reply>,
}
impl Response {
    fn error(id: u64, code: &str) -> Self {
        Self {
            version: 1,
            id,
            error: Some(code.into()),
            reply: None,
        }
    }
}
pub fn parse(bytes: &[u8]) -> Result<Request> {
    let r: Request = serde_json::from_slice(bytes)?;
    if r.version != 1
        || r.deadline_ms == 0
        || r.deadline_ms > MAX_DEADLINE_MS
        || serde_json::to_value(&r)? != serde_json::from_slice::<serde_json::Value>(bytes)?
    {
        return Err("closed native grammar/version/deadline".into());
    }
    Ok(r)
}
pub async fn read_frame<R: AsyncRead + Unpin>(r: &mut R) -> Result<Vec<u8>> {
    let n = r.read_u32().await? as usize;
    if n == 0 || n > MAX_FRAME {
        return Err("native frame limit".into());
    }
    let mut b = vec![0; n];
    r.read_exact(&mut b).await?;
    Ok(b)
}
pub async fn write_frame<W: AsyncWrite + Unpin>(w: &mut W, b: &[u8]) -> Result<()> {
    if b.is_empty() || b.len() > MAX_FRAME {
        return Err("native frame limit".into());
    }
    w.write_u32(b.len() as u32).await?;
    w.write_all(b).await?;
    w.flush().await?;
    Ok(())
}
pub(crate) fn chain(p: &PathBuf) -> Result<Vec<CertificateDer<'static>>> {
    Ok(
        rustls_pemfile::certs(&mut BufReader::new(std::fs::File::open(p)?))
            .collect::<std::io::Result<_>>()?,
    )
}
pub(crate) fn key(p: &PathBuf) -> Result<PrivateKeyDer<'static>> {
    rustls_pemfile::private_key(&mut BufReader::new(std::fs::File::open(p)?))?
        .ok_or_else(|| "missing key".into())
}
pub(crate) fn roots(p: &PathBuf) -> Result<RootCertStore> {
    let mut r = RootCertStore::empty();
    for c in chain(p)? {
        r.add(c)?;
    }
    Ok(r)
}
/// Returned only inside this process; never deserialized from a client.
pub struct Ready {
    held: HeldOutput,
    permit: OwnedSemaphorePermit,
}
pub struct Released {
    pub reply: Reply,
    pub permit: OwnedSemaphorePermit,
}
#[derive(Default)]
pub struct Cancellation {
    cancelled: std::sync::atomic::AtomicBool,
    notify: tokio::sync::Notify,
}
impl Cancellation {
    pub fn cancel(&self) {
        self.cancelled
            .store(true, std::sync::atomic::Ordering::Release);
        self.notify.notify_waiters();
    }
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(std::sync::atomic::Ordering::Acquire)
    }
    pub async fn wait(&self) {
        loop {
            let n = self.notify.notified();
            tokio::pin!(n);
            n.as_mut().enable();
            if self.is_cancelled() {
                return;
            }
            n.await;
        }
    }
}
pub enum Event {
    Authenticate {
        fingerprint: String,
        deadline: Instant,
        reply: oneshot::Sender<std::result::Result<Session, &'static str>>,
    },
    Decide {
        session: Session,
        action: TaskAction,
        cancellation: Option<Arc<Cancellation>>,
        deadline: Instant,
        permit: OwnedSemaphorePermit,
        reply: oneshot::Sender<std::result::Result<Ready, &'static str>>,
    },
    Finish {
        ready: Ready,
        deadline: Instant,
        reply: oneshot::Sender<std::result::Result<Released, &'static str>>,
    },
}

enum Stage {
    Decision {
        reply: oneshot::Sender<std::result::Result<Ready, &'static str>>,
    },
    Release {
        held: HeldOutput,
        reply: oneshot::Sender<std::result::Result<Released, &'static str>>,
    },
}
impl Stage {
    fn closed(&self) -> bool {
        match self {
            Self::Decision { reply } => reply.is_closed(),
            Self::Release { reply, .. } => reply.is_closed(),
        }
    }
}
struct Pending {
    deadline: Instant,
    term: u64,
    permit: OwnedSemaphorePermit,
    stage: Stage,
}
pub struct Actor {
    prefix: Vec<u8>,
    next: u64,
    pending: BTreeMap<Vec<u8>, Pending>,
}
impl Actor {
    pub fn new(node: u64) -> Result<Self> {
        let mut nonce = [0; 16];
        rustls::crypto::ring::default_provider()
            .secure_random
            .fill(&mut nonce)
            .map_err(|_| "context random")?;
        let mut prefix = b"GDNATIVE1".to_vec();
        prefix.extend(node.to_be_bytes());
        prefix.extend(nonce);
        Ok(Self {
            prefix,
            next: 0,
            pending: BTreeMap::new(),
        })
    }
    fn context(&mut self) -> Result<Vec<u8>> {
        self.next = self.next.checked_add(1).ok_or("native context exhausted")?;
        let mut c = self.prefix.clone();
        c.extend(self.next.to_be_bytes());
        Ok(c)
    }
    pub fn reap(&mut self, node: &Node) {
        // Retain submitted permits even after disconnect/deadline until apply or
        // term loss. Abandoned contexts can never be assigned to another caller.
        self.pending
            .retain(|_, p| p.term == node.term() && node.is_leader());
    }
    pub fn handle<C: ClockSource>(
        &mut self,
        e: Event,
        node: &mut Node,
        clock: &mut LeaderClock<C>,
    ) {
        match e {
            Event::Authenticate {
                fingerprint,
                deadline,
                reply,
            } => {
                let result = if Instant::now() >= deadline || reply.is_closed() {
                    Err("deadline_before_submission")
                } else {
                    node.authenticate(&fingerprint)
                        .map_err(|_| "unauthenticated")
                };
                let _ = reply.send(result);
            }
            Event::Decide {
                session,
                action,
                cancellation,
                deadline,
                permit,
                reply,
            } => {
                let result = (|| -> Result<Vec<u8>> {
                    if Instant::now() >= deadline
                        || reply.is_closed()
                        || cancellation.as_ref().is_some_and(|c| c.is_cancelled())
                    {
                        return Err("deadline".into());
                    }
                    if self.pending.len() >= MAX_PENDING {
                        return Err("capacity".into());
                    }
                    let ctx = self.context()?;
                    node.decide_when(ctx.clone(), &session, action, clock, || {
                        Instant::now() < deadline
                            && !reply.is_closed()
                            && !cancellation.as_ref().is_some_and(|c| c.is_cancelled())
                    })?;
                    Ok(ctx)
                })();
                match result {
                    Ok(ctx) => {
                        self.pending.insert(
                            ctx,
                            Pending {
                                deadline,
                                term: node.term(),
                                permit,
                                stage: Stage::Decision { reply },
                            },
                        );
                    }
                    Err(_) => {
                        let _ = reply.send(Err(
                            if Instant::now() >= deadline
                                || cancellation.as_ref().is_some_and(|c| c.is_cancelled())
                            {
                                "deadline_before_submission"
                            } else {
                                "unavailable"
                            },
                        ));
                    }
                }
            }
            Event::Finish {
                ready,
                deadline,
                reply,
            } => {
                // The connection has reached write readiness. Acquire a NEW quorum
                // proof now, never before socket holding/backpressure/test gates.
                let result = (|| -> Result<Vec<u8>> {
                    if Instant::now() >= deadline
                        || reply.is_closed()
                        || self.pending.len() >= MAX_PENDING
                    {
                        return Err("deadline/capacity".into());
                    }
                    let ctx = self.context()?;
                    node.request_release_when(ctx.clone(), &ready.held, clock, || {
                        Instant::now() < deadline && !reply.is_closed()
                    })?;
                    Ok(ctx)
                })();
                match result {
                    Ok(ctx) => {
                        self.pending.insert(
                            ctx,
                            Pending {
                                deadline,
                                term: node.term(),
                                permit: ready.permit,
                                stage: Stage::Release {
                                    held: ready.held,
                                    reply,
                                },
                            },
                        );
                    }
                    Err(_) => {
                        let _ = reply.send(Err("indeterminate"));
                    }
                }
            }
        }
    }
    /// Exact local contexts only; no follower apply is a response. A fresh release
    /// apply is consumed into final authority checking without a socket-side wait.
    pub fn applied<C: ClockSource>(
        &mut self,
        a: Applied,
        node: &mut Node,
        clock: &mut LeaderClock<C>,
    ) -> Option<Applied> {
        let Some(p) = self.pending.remove(&a.context) else {
            return Some(a);
        };
        if Instant::now() >= p.deadline || p.stage.closed() {
            return None;
        }
        match p.stage {
            Stage::Decision { reply } => {
                let result = node
                    .hold(a)
                    .map(|held| Ready {
                        held,
                        permit: p.permit,
                    })
                    .map_err(|_| "indeterminate");
                let _ = reply.send(result);
            }
            Stage::Release { held, reply } => {
                let result = node
                    .release_proof(a)
                    .and_then(|proof| node.finish_release(held, proof, clock))
                    .map(|r| Released {
                        reply: r,
                        permit: p.permit,
                    })
                    .map_err(|_| "indeterminate");
                let _ = reply.send(result);
            }
        }
        None
    }
}
/// Separate TLS authority/configuration from peer transport. Session permits
/// cover handshake through final write. Request permits follow submitted work.
// Trusted local test controller only. Marker files provide observable race
// boundaries; no remote request can choose a path or enable a gate. Every gate
// is still covered by the original connection deadline.
async fn gate(stage: &str, id: u64) -> Result<()> {
    let Some(root) = std::env::var_os("GIGGLES_NATIVE_TEST_HOOK_ROOT") else {
        return Ok(());
    };
    let root = PathBuf::from(root);
    if !root.join(format!("pause-{stage}-{id}")).exists() {
        return Ok(());
    }
    tokio::fs::write(root.join(format!("reached-{stage}-{id}")), b"reached").await?;
    while !root.join(format!("resume-{stage}-{id}")).exists() {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    Ok(())
}
pub async fn bind(
    config: Configuration,
    peers: BTreeSet<String>,
    events: mpsc::Sender<Event>,
) -> Result<()> {
    bind_shared(config, peers, events, Limits::default()).await
}
/// Shared across external transports; no independently spendable admission pool.
#[derive(Clone)]
pub struct Limits {
    pub sessions: Arc<Semaphore>,
    pub requests: Arc<Semaphore>,
    rate: Arc<std::sync::Mutex<(Instant, u32)>>,
    work_rate: Arc<std::sync::Mutex<(Instant, u32)>>,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            sessions: Arc::new(Semaphore::new(MAX_SESSIONS)),
            requests: Arc::new(Semaphore::new(MAX_PENDING)),
            rate: Arc::new(std::sync::Mutex::new((Instant::now(), 0))),
            work_rate: Arc::new(std::sync::Mutex::new((Instant::now(), 0))),
        }
    }
}
impl Limits {
    pub fn work(&self) -> bool {
        let Ok(mut r) = self.work_rate.lock() else {
            return false;
        };
        if r.0.elapsed() >= Duration::from_secs(1) {
            *r = (Instant::now(), 0);
        }
        r.1 = r.1.saturating_add(1);
        r.1 <= 128
    }
    pub fn accept(&self) -> Option<OwnedSemaphorePermit> {
        let mut r = self.rate.lock().ok()?;
        if r.0.elapsed() >= Duration::from_secs(1) {
            *r = (Instant::now(), 0);
        }
        r.1 = r.1.saturating_add(1);
        if r.1 > 128 {
            return None;
        }
        self.sessions.clone().try_acquire_owned().ok()
    }
}
pub async fn bind_shared(
    config: Configuration,
    peers: BTreeSet<String>,
    events: mpsc::Sender<Event>,
    limits: Limits,
) -> Result<()> {
    let verifier =
        rustls::server::WebPkiClientVerifier::builder(Arc::new(roots(&config.ca)?)).build()?;
    let mut server = ServerConfig::builder()
        .with_client_cert_verifier(verifier.clone())
        .with_single_cert(chain(&config.certificate)?, key(&config.key)?)?;
    server.alpn_protocols = vec![b"giggles-native/1".to_vec()];
    let acceptor = TlsAcceptor::from(Arc::new(server));
    let listener = TcpListener::bind(&config.listen).await?;
    let requests = limits.requests.clone();
    let peers = Arc::new(peers);
    tokio::spawn(async move {
        while let Ok((tcp, _)) = listener.accept().await {
            let receipt = Instant::now();
            let Some(permit) = limits.accept() else {
                continue;
            };
            let acceptor = acceptor.clone();
            let verifier = verifier.clone();
            let events = events.clone();
            let requests = requests.clone();
            let peers = peers.clone();
            let limits = limits.clone();
            tokio::spawn(async move {
                let _session = permit;
                let ceiling = receipt + Duration::from_millis(MAX_DEADLINE_MS);
                let work = async {
                    let mut tls =
                        timeout_at(receipt + Duration::from_secs(2), acceptor.accept(tcp))
                            .await??;
                    let certs = tls
                        .get_ref()
                        .1
                        .peer_certificates()
                        .ok_or("missing client cert")?
                        .to_vec();
                    let leaf = certs.first().ok_or("missing leaf")?;
                    let fp = crate::transport::fingerprint(leaf);
                    if peers.contains(&fp) {
                        return Err::<(), Box<dyn std::error::Error + Send + Sync>>(
                            "peer role denied".into(),
                        );
                    }
                    let (tx, rx) = oneshot::channel();
                    events
                        .try_send(Event::Authenticate {
                            fingerprint: fp,
                            deadline: ceiling,
                            reply: tx,
                        })
                        .map_err(|_| "capacity")?;
                    let session = rx.await?.map_err(|_| "unauthenticated")?;
                    let bytes = read_frame(&mut tls).await?;
                    let request = match parse(&bytes) {
                        Ok(r) => r,
                        Err(_) => {
                            write_frame(
                                &mut tls,
                                &serde_json::to_vec(&Response::error(0, "protocol"))?,
                            )
                            .await?;
                            return Ok(());
                        }
                    };
                    let deadline = receipt + Duration::from_millis(request.deadline_ms);
                    if Instant::now() >= deadline {
                        return Ok(());
                    }
                    let operation = async {
                        gate("before", request.id).await?;
                        verifier
                            .verify_client_cert(leaf, &certs[1..], UnixTime::now())
                            .map_err(|_| "certificate no longer valid")?;
                        if !limits.work() {
                            return Ok(Response::error(request.id, "capacity"));
                        }
                        let permit = match requests.try_acquire_owned() {
                            Ok(p) => p,
                            Err(_) => {
                                return Ok::<Response, Box<dyn std::error::Error + Send + Sync>>(
                                    Response::error(request.id, "capacity"),
                                );
                            }
                        };
                        tokio::select! {biased; _=tls.read_u8()=>return Err("disconnected/protocol pipelining".into()), _=std::future::ready(())=>{}};
                        let (tx, rx) = oneshot::channel();
                        if events
                            .try_send(Event::Decide {
                                session,
                                action: request.action,
                                cancellation: None,
                                deadline,
                                permit,
                                reply: tx,
                            })
                            .is_err()
                        {
                            return Ok(Response::error(request.id, "capacity"));
                        }
                        // A disconnect before apply closes the receiver; actor never releases
                        // output. After submission no cancellation can promise rollback.
                        let ready = tokio::select! {v=rx=>match v?{Ok(v)=>v,Err(e)=>return Ok(Response::error(request.id,e))},_ = tls.read_u8()=>return Err("disconnected/protocol pipelining".into())};
                        gate("held", request.id).await?;
                        tls.get_ref().0.writable().await?;
                        verifier
                            .verify_client_cert(leaf, &certs[1..], UnixTime::now())
                            .map_err(|_| "certificate no longer valid")?;
                        let (tx, rx) = oneshot::channel();
                        events
                            .try_send(Event::Finish {
                                ready,
                                deadline,
                                reply: tx,
                            })
                            .map_err(|_| "capacity")?;
                        let released = match rx.await? {
                            Ok(v) => v,
                            Err(e) => return Ok(Response::error(request.id, e)),
                        };
                        gate("write", request.id).await?;
                        verifier
                            .verify_client_cert(leaf, &certs[1..], UnixTime::now())
                            .map_err(|_| "certificate no longer valid")?;
                        let response = Response {
                            version: 1,
                            id: request.id,
                            error: None,
                            reply: Some(released.reply),
                        };
                        // Exactly one authorized response, no output queue. Keep request
                        // permit until the bounded write has actually completed.
                        write_frame(&mut tls, &serde_json::to_vec(&response)?).await?;
                        drop(released.permit);
                        Ok(Response::error(request.id, "already_written"))
                    };
                    match timeout_at(deadline, operation).await {
                        Ok(Ok(r)) if r.error.as_deref() != Some("already_written") => {
                            let _ = timeout_at(
                                deadline,
                                write_frame(&mut tls, &serde_json::to_vec(&r)?),
                            )
                            .await;
                        }
                        Err(_) => { // The deadline is exhausted: close. Client must resolve indeterminate.
                        }
                        Ok(Err(_)) | Ok(Ok(_)) => {}
                    }
                    Ok(())
                };
                let _ = timeout_at(ceiling, work).await;
            });
        }
    });
    Ok(())
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientConfiguration {
    pub addresses: Vec<String>,
    pub certificate: PathBuf,
    pub key: PathBuf,
    pub ca: PathBuf,
    pub server_name: String,
}
/// Retries the identical operation only; business idempotency is in OperationId.
/// Lost/timeout responses remain indeterminate if every configured node fails.
pub async fn call(config: &ClientConfiguration, request: &Request) -> Result<Response> {
    if config.addresses.is_empty() || config.addresses.len() > 3 {
        return Err("one to three nodes required".into());
    }
    let bytes = serde_json::to_vec(request)?;
    parse(&bytes)?;
    let mut client = ClientConfig::builder()
        .with_root_certificates(roots(&config.ca)?)
        .with_client_auth_cert(chain(&config.certificate)?, key(&config.key)?)?;
    client.alpn_protocols = vec![b"giggles-native/1".to_vec()];
    let connector = TlsConnector::from(Arc::new(client));
    let deadline = Instant::now() + Duration::from_millis(request.deadline_ms);
    let mut last = None;
    let mut uncertain = false;
    let mut explicit_uncertain = false;
    for address in &config.addresses {
        let result = timeout_at(deadline, async {
            let tcp = TcpStream::connect(address).await?;
            let mut tls = connector
                .connect(ServerName::try_from(config.server_name.clone())?, tcp)
                .await?;
            write_frame(&mut tls, &bytes).await?;
            let response: Response = serde_json::from_slice(&read_frame(&mut tls).await?)?;
            if response.id != request.id
                || response.version != 1
                || response.reply.is_some() == response.error.is_some()
            {
                return Err::<Response, Box<dyn std::error::Error + Send + Sync>>(
                    "response correlation/shape".into(),
                );
            }
            Ok(response)
        })
        .await;
        match result {
            Ok(Ok(r)) => {
                if r.error.is_none() {
                    return Ok(r);
                }
                // A server can explicitly report a submitted-but-unresolved
                // attempt. A later node's pre-submission refusal cannot erase it.
                explicit_uncertain |= r.error.as_deref() == Some("indeterminate");
                last = Some(r);
            }
            _ => {
                uncertain = true;
            }
        }
    }
    if uncertain {
        return Err("indeterminate: resolve original OperationId against quorum".into());
    }
    if explicit_uncertain {
        return Ok(Response::error(request.id, "indeterminate"));
    }
    last.ok_or_else(|| "indeterminate: resolve original OperationId against quorum".into())
}
#[cfg(test)]
mod admission_tests {
    use super::*;
    #[test]
    fn transports_clone_one_session_work_and_rate_pool() {
        let native = Limits::default();
        let pg = native.clone();
        let sessions = (0..MAX_SESSIONS)
            .map(|_| native.accept().unwrap())
            .collect::<Vec<_>>();
        assert!(pg.accept().is_none());
        drop(sessions);
        assert!(pg.accept().is_some());
        let work = (0..MAX_PENDING)
            .map(|_| native.requests.clone().try_acquire_owned().unwrap())
            .collect::<Vec<_>>();
        assert!(pg.requests.clone().try_acquire_owned().is_err());
        drop(work);
        for i in 0..128 {
            assert!(if i % 2 == 0 { native.work() } else { pg.work() });
        }
        assert!(!native.work());
        assert!(!pg.work());
    }
}
