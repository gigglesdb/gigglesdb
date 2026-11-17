//! Range and gateway calls between nodes over the peer transport (GD-0075 R5-21).
//!
//! Each peer gets one call connection of its own (Raft traffic keeps its stream) carrying
//! `CallRequest` frames out and `CallResponse` frames back, matched by id. Call frames carry
//! the database cluster id (group 0's id): calls are node to node, not per group. A caller
//! holds one slot of the peer's bounded queue until its reply or timeout; a full queue or an
//! exhausted server pool answers `Unavailable` at once, never silence.
use super::{Body, Header, MAX_FRAME, frame, runtime::Peers};
use crate::call_codec::{
    self, GatewayCall, GatewayReply, NodeCall, NodeReport, PeerCall, PeerReply,
};
use crate::client::{self, LocalRanges, RangeCall, RangeReply, RangeTransport};
use crate::error::{Error, Result};
use giggles_core::range_api::{
    ClusterTxnId, DeadlockProbe, GatewayPeer, GatewayService, RangeError, RangeId,
};
use std::{
    collections::{BTreeMap, HashMap},
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Mutex, RwLock, mpsc as std_mpsc},
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{OwnedSemaphorePermit, Semaphore, mpsc},
    task::JoinHandle,
};

/// Frame header (41) plus call id (8).
const MAX_BODY: usize = MAX_FRAME - 49;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// A lock request without its own timeout waits at most `lock_wait_backstop` (design.md 3.2).
const LOCK_BACKSTOP: Duration = Duration::from_secs(300);

/// Bounds of the call path.
#[derive(Clone, Copy, Debug)]
pub struct CallLimits {
    /// Calls in flight per peer (client side); one more returns `Unavailable("call queue full")`.
    pub queue: usize,
    /// Inbound calls served at once (server side); one more returns `Unavailable("overloaded")`.
    pub workers: usize,
    /// Inbound lock requests waiting at once, in their own pool so that waiting locks never
    /// keep out the calls that end them (commit, release, heartbeat, deadlock detection).
    /// Keep `workers + lock_waits` under the runtime's blocking-thread limit (512).
    pub lock_waits: usize,
    /// Reply deadline of a call (a lock call adds its own wait).
    pub timeout: Duration,
}

impl Default for CallLimits {
    fn default() -> Self {
        Self {
            queue: 256,
            workers: 32,
            lock_waits: 256,
            timeout: Duration::from_secs(30),
        }
    }
}

type Reply = std::result::Result<PeerReply, RangeError>;

/// A write's outcome is unknown once sent; a read or lock is safe to retry.
fn lost(call: &PeerCall, what: String) -> RangeError {
    match call {
        PeerCall::Range(
            RangeCall::Commit { .. }
            | RangeCall::Prepare { .. }
            | RangeCall::Finalize { .. }
            | RangeCall::Home { .. },
        ) => RangeError::Unknown(what),
        _ => RangeError::Unavailable(what),
    }
}

struct Outgoing {
    body: Vec<u8>,
    reply: Option<(std_mpsc::SyncSender<Reply>, Instant)>,
}

struct Conn {
    tx: mpsc::Sender<Outgoing>,
    slots: Arc<Semaphore>,
    task: JoinHandle<()>,
}

impl Drop for Conn {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Client side of peer calls; implements `RangeTransport` and `GatewayPeer`.
pub struct PeerCalls {
    peers: Arc<Peers>,
    limits: RwLock<CallLimits>,
    conns: Mutex<BTreeMap<u64, Arc<Conn>>>,
}

impl PeerCalls {
    pub(crate) fn new(peers: Arc<Peers>) -> Self {
        Self {
            peers,
            limits: RwLock::new(CallLimits::default()),
            conns: Mutex::default(),
        }
    }

    /// New limits apply to peers first called (or reconnected) after this.
    pub(crate) fn set_limits(&self, limits: CallLimits) {
        *self.limits.write().unwrap() = limits;
        self.conns.lock().unwrap().clear();
    }

    /// Sends `call` to `node` and waits up to `timeout` for its reply.
    pub fn call(&self, node: u64, call: PeerCall, timeout: Duration) -> Reply {
        let (tx, rx) = std_mpsc::sync_channel(1);
        let deadline = Instant::now()
            .checked_add(timeout)
            .unwrap_or_else(|| Instant::now() + LOCK_BACKSTOP);
        let _slot = self.enqueue(node, &call, Some((tx, deadline)))?;
        match rx.recv_timeout(timeout) {
            // A reply that cannot be decoded or was replaced at the server says nothing
            // about whether a write ran: its outcome is unknown (design.md 2).
            Ok(Err(RangeError::Corrupt(why))) => Err(lost(&call, why)),
            Ok(reply) => reply,
            Err(std_mpsc::RecvTimeoutError::Timeout) => {
                // A black-holed peer would make every later call wait out its timeout
                // too: drop the connection so they fail fast (never sent = Unavailable).
                self.conns.lock().unwrap().remove(&node);
                Err(lost(&call, format!("call to node {node} timed out")))
            }
            Err(std_mpsc::RecvTimeoutError::Disconnected) => {
                Err(lost(&call, format!("call connection to node {node} lost")))
            }
        }
    }

    /// Idempotently asks a peer to start an empty learner group.
    pub fn host_range(
        &self,
        node: u64,
        desc: giggles_core::range_api::RangeDescriptor,
    ) -> std::result::Result<bool, RangeError> {
        let timeout = self.limits.read().unwrap().timeout;
        match self.call(node, PeerCall::Node(NodeCall::HostRange(desc)), timeout)? {
            PeerReply::Hosted(created) => Ok(created),
            _ => Err(RangeError::Corrupt("expected hosting reply".into())),
        }
    }

    /// Queues `call` without waiting for its reply (a deadlock probe).
    pub fn send(&self, node: u64, call: PeerCall) -> std::result::Result<(), RangeError> {
        self.enqueue(node, &call, None).map(drop)
    }

    fn enqueue(
        &self,
        node: u64,
        call: &PeerCall,
        reply: Option<(std_mpsc::SyncSender<Reply>, Instant)>,
    ) -> std::result::Result<OwnedSemaphorePermit, RangeError> {
        let body = call_codec::encode_call(call)?;
        if body.len() > MAX_BODY {
            return Err(RangeError::Corrupt("call exceeds the frame limit".into()));
        }
        let conn = self.conn(node)?;
        let full = || RangeError::Unavailable("call queue full".into());
        let slot = conn.slots.clone().try_acquire_owned().map_err(|_| full())?;
        match conn.tx.try_send(Outgoing { body, reply }) {
            Ok(()) => Ok(slot),
            Err(mpsc::error::TrySendError::Full(_)) => Err(full()),
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.conns.lock().unwrap().remove(&node);
                Err(RangeError::Unavailable(format!(
                    "call connection to node {node} closed"
                )))
            }
        }
    }

    fn conn(&self, node: u64) -> std::result::Result<Arc<Conn>, RangeError> {
        if node == self.peers.config.node_id {
            return Err(RangeError::Unavailable("peer call to own node".into()));
        }
        let mut conns = self.conns.lock().unwrap();
        if let Some(conn) = conns.get(&node).filter(|c| !c.tx.is_closed()) {
            return Ok(conn.clone());
        }
        let queue = self.limits.read().unwrap().queue.max(1);
        let (tx, rx) = mpsc::channel(queue);
        let task = self
            .peers
            .runtime
            .spawn(connection(self.peers.clone(), node, rx, queue));
        let slots = Arc::new(Semaphore::new(queue));
        let conn = Arc::new(Conn { tx, slots, task });
        conns.insert(node, conn.clone());
        Ok(conn)
    }

    fn timeout(&self, call: &RangeCall) -> Duration {
        let base = self.limits.read().unwrap().timeout;
        match call {
            RangeCall::Lock(req) => {
                base.saturating_add(req.timeout_ms.map_or(LOCK_BACKSTOP, Duration::from_millis))
            }
            _ => base,
        }
    }

    fn gateway(
        &self,
        node: u64,
        call: GatewayCall,
    ) -> std::result::Result<GatewayReply, RangeError> {
        let timeout = self.limits.read().unwrap().timeout;
        match PeerCalls::call(self, node, PeerCall::Gateway(call), timeout)? {
            PeerReply::Gateway(reply) => Ok(reply),
            _ => Err(mismatched()),
        }
    }
}

fn mismatched() -> RangeError {
    RangeError::Corrupt("mismatched peer reply".into())
}

impl RangeTransport for PeerCalls {
    fn call(&self, node: u64, call: RangeCall) -> std::result::Result<RangeReply, RangeError> {
        let timeout = self.timeout(&call);
        match PeerCalls::call(self, node, PeerCall::Range(call), timeout)? {
            PeerReply::Range(reply) => Ok(reply),
            _ => Err(mismatched()),
        }
    }
}

impl GatewayPeer for PeerCalls {
    fn probe(&self, node: u64, probe: DeadlockProbe) -> std::result::Result<(), RangeError> {
        self.send(node, PeerCall::Gateway(GatewayCall::Probe(probe)))
    }
    fn abort(&self, node: u64, victim: ClusterTxnId) -> std::result::Result<(), RangeError> {
        match self.gateway(node, GatewayCall::Abort(victim))? {
            GatewayReply::Done => Ok(()),
            _ => Err(mismatched()),
        }
    }
    fn blockers(
        &self,
        node: u64,
        txn: ClusterTxnId,
    ) -> std::result::Result<Vec<ClusterTxnId>, RangeError> {
        match self.gateway(node, GatewayCall::Blockers(txn))? {
            GatewayReply::Blockers(txns) => Ok(txns),
            _ => Err(mismatched()),
        }
    }
    fn cancel_wait(&self, node: u64, txn: ClusterTxnId) -> std::result::Result<(), RangeError> {
        match self.gateway(node, GatewayCall::CancelWait(txn))? {
            GatewayReply::Done => Ok(()),
            _ => Err(mismatched()),
        }
    }
    fn txn_live(&self, node: u64, txn: ClusterTxnId) -> std::result::Result<bool, RangeError> {
        match self.gateway(node, GatewayCall::TxnLive(txn))? {
            GatewayReply::Live(live) => Ok(live),
            _ => Err(mismatched()),
        }
    }
}

/// One peer's call connection: connects on demand; a failed connect fails the queued calls
/// with `Unavailable` (never sent); a lost connection drops the waiting callers' replies.
async fn connection(peers: Arc<Peers>, node: u64, mut rx: mpsc::Receiver<Outgoing>, queue: usize) {
    while let Some(first) = rx.recv().await {
        let stream = match tokio::time::timeout(CONNECT_TIMEOUT, peers.connect(node)).await {
            Ok(Ok(stream)) => stream,
            failed => {
                let why = match failed {
                    Ok(Err(error)) => error.to_string(),
                    _ => "connect timed out".into(),
                };
                let fail = |o: Outgoing| {
                    if let Some((reply, _)) = o.reply {
                        let _ = reply.try_send(Err(RangeError::Unavailable(format!(
                            "no call connection to node {node}: {why}"
                        ))));
                    }
                };
                fail(first);
                while let Ok(o) = rx.try_recv() {
                    fail(o);
                }
                continue;
            }
        };
        let me = peers.config.node_id;
        let cluster = peers.config.cluster;
        let out = Header {
            cluster,
            from: me,
            to: node,
        };
        let back = Header {
            cluster,
            from: node,
            to: me,
        };
        let (mut r, mut w) = tokio::io::split(stream);
        let pending = Mutex::new(HashMap::new());
        let mut first = Some(first);
        let mut next_id = 0_u64;
        let writer = async {
            loop {
                let o = match first.take() {
                    Some(o) => o,
                    None => match rx.recv().await {
                        Some(o) => o,
                        None => return,
                    },
                };
                next_id += 1;
                if let Some(reply) = o.reply {
                    let mut pending = pending.lock().unwrap();
                    // Live entries hold queue slots; anything past the cap timed out.
                    if pending.len() >= queue {
                        let now = Instant::now();
                        pending.retain(|_, entry: &mut (_, Instant)| entry.1 > now);
                    }
                    pending.insert(next_id, reply);
                }
                let request = Body::CallRequest {
                    id: next_id,
                    body: o.body,
                };
                let Ok(bytes) = frame::encode(&out, &request) else {
                    pending.lock().unwrap().remove(&next_id);
                    continue;
                };
                if frame::write_frame(&mut w, &bytes).await.is_err() {
                    return;
                }
            }
        };
        let reader = async {
            while let Ok(bytes) = frame::read_frame(&mut r).await {
                let Ok(Body::CallResponse { id, body }) = frame::decode(&back, &bytes) else {
                    return;
                };
                let Some((reply, _)) = pending.lock().unwrap().remove(&id) else {
                    continue;
                };
                let _ = reply.try_send(call_codec::decode_reply(&body).and_then(|r| r));
            }
        };
        tokio::select! {
            _ = writer => {},
            _ = reader => {},
        }
    }
}

/// Answers `NodeCall::Status` (R5-64): every range this node replicates.
pub trait NodeService: Send + Sync {
    fn status(&self) -> NodeReport;
    fn move_range(
        &self,
        _range: RangeId,
        _from: u64,
        _to: u64,
    ) -> std::result::Result<call_codec::MoveReply, RangeError> {
        Err(RangeError::Unavailable("range move unavailable".into()))
    }
    fn registry_update(&self, _call: NodeCall) -> std::result::Result<PeerReply, RangeError> {
        Err(RangeError::Unavailable(
            "registry updates unavailable".into(),
        ))
    }
    fn host_range(
        &self,
        _desc: giggles_core::range_api::RangeDescriptor,
    ) -> std::result::Result<bool, RangeError> {
        Err(RangeError::Unavailable("range hosting unavailable".into()))
    }
}

/// Server side: the registered handlers and the bounded worker pool.
pub(crate) struct CallServer {
    handlers: RwLock<Handlers>,
    pool: RwLock<Arc<Semaphore>>,
    waits: RwLock<Arc<Semaphore>>,
}

#[derive(Clone, Default)]
struct Handlers {
    local: Option<Arc<dyn LocalRanges>>,
    gateway: Option<Arc<dyn GatewayService>>,
    node: Option<Arc<dyn NodeService>>,
}

impl CallServer {
    pub(crate) fn new() -> Self {
        Self {
            handlers: RwLock::default(),
            pool: RwLock::new(Arc::new(Semaphore::new(CallLimits::default().workers))),
            waits: RwLock::new(Arc::new(Semaphore::new(CallLimits::default().lock_waits))),
        }
    }

    pub(crate) fn set_handlers(
        &self,
        local: Option<Arc<dyn LocalRanges>>,
        gateway: Option<Arc<dyn GatewayService>>,
    ) {
        let mut handlers = self.handlers.write().unwrap();
        handlers.local = local;
        handlers.gateway = gateway;
    }

    pub(crate) fn set_node_handler(&self, node: Option<Arc<dyn NodeService>>) {
        self.handlers.write().unwrap().node = node;
    }

    pub(crate) fn set_limits(&self, limits: CallLimits) {
        *self.pool.write().unwrap() = Arc::new(Semaphore::new(limits.workers));
        *self.waits.write().unwrap() = Arc::new(Semaphore::new(limits.lock_waits));
    }

    /// Serves one call connection from its first request on; `authorized` rechecks the
    /// caller's identity against the current peer table for every frame. A connection that
    /// presents this node's own certificate (an operator CLI) may ask for status or request a range move.
    pub(crate) async fn serve<S: AsyncRead + AsyncWrite + Unpin>(
        &self,
        stream: &mut S,
        base: Header,
        first: (u64, Vec<u8>),
        authorized: &(dyn Fn() -> bool + Send + Sync),
    ) -> Result<()> {
        let node_only = base.from == base.to;
        let reply_header = Header {
            from: base.to,
            to: base.from,
            ..base
        };
        let (mut r, mut w) = tokio::io::split(stream);
        let (tx, mut responses) = mpsc::channel::<(u64, Vec<u8>)>(1024);
        let reader = async {
            let mut next = Some(first);
            loop {
                let (id, body) = match next.take() {
                    Some(request) => request,
                    None => match frame::decode(&base, &frame::read_frame(&mut r).await?)? {
                        Body::CallRequest { id, body } if authorized() => (id, body),
                        _ => return Err(Error::Corrupt("unexpected peer call frame".into())),
                    },
                };
                self.start(id, &body, node_only, &tx).await;
            }
        };
        let writer = async {
            while let Some((id, body)) = responses.recv().await {
                let response = Body::CallResponse { id, body };
                frame::write_frame(&mut w, &frame::encode(&reply_header, &response)?).await?;
            }
            Ok(())
        };
        tokio::select! {
            result = reader => result,
            result = writer => result,
        }
    }

    /// Answers at once when the call is invalid or the pool is full; else runs it on a worker.
    async fn start(
        &self,
        id: u64,
        body: &[u8],
        node_only: bool,
        tx: &mpsc::Sender<(u64, Vec<u8>)>,
    ) {
        let call = match call_codec::decode_call(body) {
            Ok(call)
                if node_only
                    && !matches!(
                        call,
                        PeerCall::Node(
                            NodeCall::Status
                                | NodeCall::MoveRange { .. }
                                | NodeCall::RetireNode { .. }
                                | NodeCall::EnrolNode { .. }
                        )
                    ) =>
            {
                let refused = Err(RangeError::Unavailable("not a peer node".into()));
                let _ = tx.send((id, reply_body(&refused))).await;
                return;
            }
            Ok(call) => call,
            Err(error) => {
                let _ = tx.send((id, reply_body(&Err(error)))).await;
                return;
            }
        };
        let pool = if matches!(call, PeerCall::Range(RangeCall::Lock(_))) {
            self.waits.read().unwrap().clone()
        } else {
            self.pool.read().unwrap().clone()
        };
        let Ok(permit) = pool.try_acquire_owned() else {
            let overloaded = Err(RangeError::Unavailable("overloaded".into()));
            let _ = tx.send((id, reply_body(&overloaded))).await;
            return;
        };
        let handlers = self.handlers.read().unwrap().clone();
        let tx = tx.clone();
        tokio::task::spawn_blocking(move || {
            let failed = lost(&call, "peer call handler panicked".into());
            let reply =
                catch_unwind(AssertUnwindSafe(|| handle(&handlers, call))).unwrap_or(Err(failed));
            // The worker is free once its reply exists, before the caller can see it.
            drop(permit);
            let _ = tx.blocking_send((id, reply_body(&reply)));
        });
    }
}

fn handle(handlers: &Handlers, call: PeerCall) -> Reply {
    let missing = |what: &str| RangeError::Unavailable(format!("no {what} handler on this node"));
    match call {
        PeerCall::Range(call) => {
            let local = handlers.local.as_ref().ok_or_else(|| missing("range"))?;
            client::dispatch(&**local, call).map(PeerReply::Range)
        }
        PeerCall::Gateway(call) => {
            let gateway = handlers
                .gateway
                .as_ref()
                .ok_or_else(|| missing("gateway"))?;
            Ok(PeerReply::Gateway(match call {
                GatewayCall::Probe(probe) => {
                    gateway.probe(probe);
                    GatewayReply::Done
                }
                GatewayCall::Abort(victim) => {
                    gateway.abort(victim);
                    GatewayReply::Done
                }
                GatewayCall::Blockers(txn) => GatewayReply::Blockers(gateway.blockers(txn)),
                GatewayCall::CancelWait(txn) => {
                    gateway.cancel_wait(txn);
                    GatewayReply::Done
                }
                GatewayCall::TxnLive(txn) => GatewayReply::Live(gateway.txn_live(txn)),
            }))
        }
        PeerCall::Node(call @ (NodeCall::RetireNode { .. } | NodeCall::EnrolNode { .. })) => {
            handlers
                .node
                .as_ref()
                .ok_or_else(|| missing("node registry"))?
                .registry_update(call)
        }
        PeerCall::Node(NodeCall::MoveRange { range, from, to }) => handlers
            .node
            .as_ref()
            .ok_or_else(|| missing("node move"))?
            .move_range(range, from, to)
            .map(PeerReply::Moving),
        PeerCall::Node(NodeCall::HostRange(desc)) => handlers
            .node
            .as_ref()
            .ok_or_else(|| missing("node hosting"))?
            .host_range(desc)
            .map(PeerReply::Hosted),
        PeerCall::Node(NodeCall::Status) => {
            let node = handlers
                .node
                .as_ref()
                .ok_or_else(|| missing("node status"))?;
            Ok(PeerReply::Node(node.status()))
        }
    }
}

/// Encoded reply; one that cannot be encoded or framed becomes a `Corrupt` error reply.
fn reply_body(reply: &Reply) -> Vec<u8> {
    match call_codec::encode_reply(reply) {
        Ok(body) if body.len() <= MAX_BODY => body,
        _ => call_codec::encode_reply(&Err(RangeError::Corrupt(
            "peer call reply exceeds the frame limit".into(),
        )))
        .unwrap_or_default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn own_certificate_cannot_host_ranges() {
        let desc = giggles_core::range_api::RangeDescriptor {
            id: giggles_core::range_api::RangeId(2),
            epoch: giggles_core::range_api::Epoch(1),
            start: vec![],
            end: None,
            replicas: vec![giggles_core::range_api::ReplicaDesc {
                node: 4,
                role: giggles_core::range_api::ReplicaRole::Learner,
            }],
        };
        let server = CallServer::new();
        let (tx, mut rx) = mpsc::channel(1);
        let body = call_codec::encode_call(&PeerCall::Node(NodeCall::HostRange(desc))).unwrap();
        server.start(1, &body, true, &tx).await;
        let (_, reply) = rx.recv().await.unwrap();
        assert_eq!(
            call_codec::decode_reply(&reply).unwrap(),
            Err(RangeError::Unavailable("not a peer node".into()))
        );
        let body = call_codec::encode_call(&PeerCall::Node(NodeCall::MoveRange {
            range: RangeId(2),
            from: 3,
            to: 4,
        }))
        .unwrap();
        server.start(3, &body, true, &tx).await;
        let (_, reply) = rx.recv().await.unwrap();
        assert_eq!(
            call_codec::decode_reply(&reply).unwrap(),
            Err(RangeError::Unavailable(
                "no node move handler on this node".into()
            ))
        );
        let body = call_codec::encode_call(&PeerCall::Node(NodeCall::Status)).unwrap();
        server.start(2, &body, true, &tx).await;
        let (_, reply) = rx.recv().await.unwrap();
        assert_eq!(
            call_codec::decode_reply(&reply).unwrap(),
            Err(RangeError::Unavailable(
                "no node status handler on this node".into()
            ))
        );
    }
}
