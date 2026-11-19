//! Separate HTTP/1 peer listener with bounded bodies and fail-closed routing.
use super::*;
use crate::{
    policy::{DomainName, kube_leader::LeaderState},
    state::{
        authority::{Authority, Operation, TokenizeError, TokenizeLimits},
        model::SessionState,
    },
    token::SessionId,
};
use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::{Request, Response, body::Incoming, server::conn::http1, service::service_fn};
use hyper_util::rt::TokioIo;
use serde::{Serialize, de::DeserializeOwned};
use std::{
    collections::HashMap,
    convert::Infallible,
    sync::{Arc, Mutex},
};
/// Current policy limit resolver for leader reservations.
pub type LimitSource = Arc<dyn Fn(&str) -> Option<TokenizeLimits> + Send + Sync>;
struct Batch {
    session: SessionId,
    domain: DomainName,
    window: u64,
    granted: u64,
    expires: u64,
}
/// Leader state service with server-owned per-domain limits.
pub struct PeerServer {
    ready: Option<crate::state::leader_store::LeadershipSource>,
    bindings_store: Option<Arc<dyn crate::state::store::Store>>,
    authority: Arc<Authority>,
    leader: LeaderState,
    serving_gate: Option<Arc<crate::policy::serving::ServingGate>>,
    auth: Arc<PeerAuth>,
    limits: HashMap<String, TokenizeLimits>,
    limit_source: Option<LimitSource>,
    batches: Mutex<HashMap<String, Batch>>,
    pub(super) slots: Arc<tokio::sync::Semaphore>,
    #[cfg(test)]
    pub(super) slow_lookup: Mutex<Option<(SessionId, Arc<tokio::sync::Notify>)>>,
}
impl std::fmt::Debug for PeerServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PeerServer { [REDACTED] }")
    }
}
fn decode<T: DeserializeOwned>(body: &[u8]) -> Result<T, PeerError> {
    serde_json::from_slice(body).map_err(|_| PeerError::Protocol)
}
fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, PeerError> {
    serde_json::to_vec(value).map_err(|_| PeerError::Protocol)
}
struct PeerThread {
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}
impl Drop for PeerThread {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() { let _ = stop.send(()); }
        if let Some(thread) = self.thread.take() {
            // Cancellation runs on the caller's executor, while a synchronous
            // handler may still be waiting on storage or policy publication.
            let _ = std::thread::Builder::new().name("nunya-peer-reaper".into()).spawn(move || { let _ = thread.join(); });
        }
    }
}
impl PeerServer {
    /// Construct an unwired peer listener service.
    pub fn new(
        authority: Arc<Authority>,
        leader: LeaderState,
        auth: Arc<PeerAuth>,
        limits: HashMap<String, TokenizeLimits>,
    ) -> Arc<Self> {
        Arc::new(Self {
            ready: None,
            bindings_store: None,
            authority,
            leader,
            serving_gate: None,
            auth,
            limits,
            limit_source: None,
            batches: Mutex::default(),
            slots: Arc::new(tokio::sync::Semaphore::new(256)),
            #[cfg(test)]
            slow_lookup: Mutex::default(),
        })
    }
    /// Require initialized state in the term used to seal the reply.
    pub fn with_ready(mut self: Arc<Self>, ready: crate::state::leader_store::LeadershipSource) -> Result<Arc<Self>, PeerError> {
        Arc::get_mut(&mut self).ok_or(PeerError::Protocol)?.ready = Some(ready);
        Ok(self)
    }
    /// Supply the leader's durable bindings store.
    pub fn with_bindings_store(mut self: Arc<Self>, store: Arc<dyn crate::state::store::Store>) -> Result<Arc<Self>, PeerError> {
        Arc::get_mut(&mut self).ok_or(PeerError::Protocol)?.bindings_store = Some(store);
        Ok(self)
    }
    /// Refuse requests when the leader cannot currently serve its policy.
    pub fn with_serving_gate(mut self: Arc<Self>, gate: Arc<crate::policy::serving::ServingGate>) -> Result<Arc<Self>, PeerError> {
        Arc::get_mut(&mut self).ok_or(PeerError::Protocol)?.serving_gate = Some(gate);
        Ok(self)
    }
    /// Resolve reservation limits from the current served policy.
    pub fn with_limit_source(mut self: Arc<Self>, source: LimitSource) -> Result<Arc<Self>, PeerError> {
        Arc::get_mut(&mut self).ok_or(PeerError::Protocol)?.limit_source = Some(source);
        Ok(self)
    }
    /// Serve on a dedicated runtime until the caller cancels this future.
    pub async fn serve(
        self: Arc<Self>,
        listener: tokio::net::TcpListener,
    ) -> Result<(), PeerError> {
        if self.serving_gate.is_none() { return Err(PeerError::Protocol); }
        let listener = listener.into_std().map_err(|_| PeerError::Transport)?;
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let (done, finished) = tokio::sync::oneshot::channel();
        let thread = std::thread::Builder::new().name("nunya-peer".into()).spawn(move || {
            // Synchronous self-peer callers may occupy every gateway worker.
            // Both listener registration and request execution belong here.
            let result = (|| {
                let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().map_err(|_| PeerError::Transport)?;
                runtime.block_on(async move {
                    let listener = tokio::net::TcpListener::from_std(listener).map_err(|_| PeerError::Transport)?;
                    tokio::select! {
                        result = self.serve_connections(listener) => result,
                        _ = stopped => Ok(()),
                    }
                })
            })();
            let _ = done.send(result);
        }).map_err(|_| PeerError::Transport)?;
        let _thread = PeerThread { stop: Some(stop), thread: Some(thread) };
        finished.await.map_err(|_| PeerError::Transport)?
    }
    async fn serve_connections(self: Arc<Self>, listener: tokio::net::TcpListener) -> Result<(), PeerError> {
        let mut connections = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                accepted=listener.accept()=> { let (socket,_)=accepted.map_err(|_|PeerError::Transport)?; let Ok(permit) = self.slots.clone().try_acquire_owned() else { drop(socket); continue; }; let service=self.clone();
                    connections.spawn(async move { let _permit = permit; let _=tokio::time::timeout(std::time::Duration::from_secs(5), http1::Builder::new().serve_connection(TokioIo::new(socket),service_fn(move |req| {let service=service.clone();async move {Ok::<_,Infallible>(service.handle(req).await)}}))).await; });
                },
                _=connections.join_next(), if !connections.is_empty()=>{}
            }
        }
    }
    async fn handle(&self, req: Request<Incoming>) -> Response<Full<Bytes>> {
        let (parts, body) = req.into_parts();
        let body = match tokio::time::timeout(std::time::Duration::from_secs(5), Limited::new(body, 64 * 1024).collect()).await {
            Ok(Ok(b)) => b.to_bytes(),
            Ok(Err(_)) | Err(_) => return response(400, vec![]),
        };
        let nonce = match self.auth.verify(
            parts.method.as_str(),
            parts.uri.path(),
            &parts.headers,
            &body,
            auth::now(),
        ) {
            Ok(n) => n,
            Err(_) => return response(401, vec![]),
        };
        #[cfg(test)]
        if parts.uri.path() == "/peer/v1/session/lookup" {
            let slow = self.slow_lookup.lock().unwrap().clone();
            if let Some((id, entered)) = slow
                && decode::<Lookup>(&body).is_ok_and(|r| r.session_id == id) {
                entered.notify_one();
                tokio::time::sleep(std::time::Duration::from_millis(400)).await;
            }
        }
        let generation = self.leader.term();
        let result = self.reply(&parts.method, parts.uri.path(), parts.uri.query().is_some(), &body, generation);
        let (status, body) = result.unwrap_or_else(|_| (400, b"{\"error\":\"refused\"}".to_vec()));
        match self.auth.seal(&body, &nonce, status, generation) {
            Ok(sealed) => {
                let mut r = response(status, sealed);
                if let Ok(v) = generation.to_string().parse() {
                    r.headers_mut().insert("x-nunya-peer-generation", v);
                }
                r
            }
            Err(_) => response(500, vec![]),
        }
    }
    fn reply(&self, method: &http::Method, path: &str, has_query: bool, body: &[u8], generation: u64) -> Result<(u16, Vec<u8>), PeerError> {
        if self.leader.held_term() != Some(generation) || self.ready.as_ref().is_some_and(|ready| ready() != Some(generation)) || self.serving_gate.as_ref().is_some_and(|gate| !gate.allow()) {
            encode(&Failure {
                error: FailureCode::NotLeader,
                generation,
                leader: self.leader.leader_url(),
            })
            .map(|b| (409, b))
        } else if method != http::Method::POST || has_query {
            Err(PeerError::Protocol)
        } else {
            self.dispatch(path, body, generation)
        }
    }
    #[cfg(test)]
    pub(super) fn expire_batch(&self, id: &str) {
        self.batches.lock().unwrap().get_mut(id).unwrap().expires = 0;
    }
    fn dispatch(&self, path: &str, body: &[u8], generation: u64) -> Result<(u16, Vec<u8>), PeerError> {
        let reply = match path {
            "/peer/v1/bindings" => {
                let request: BindingsRequest = decode(body)?;
                let store = self.bindings_store.as_ref().ok_or(PeerError::Protocol)?;
                let snapshot = crate::policy::bindings::snapshot(store.as_ref()).map_err(|_| PeerError::Protocol)?;
                encode(&BindingsReply {
                    term: generation,
                    version: snapshot.version,
                    snapshot: if request.have_term == generation && request.have_version == snapshot.version { None } else { Some(snapshot) },
                })?
            }
            "/peer/v1/session/lookup" => {
                let r: Lookup = decode(body)?;
                let result = match self.authority.session(r.session_id) {
                    None => LookupResult::Unknown,
                    Some(session) => {
                        if session.state == SessionState::Active {
                            if let Some(credentials) = self.authority.peer_credentials(r.session_id)
                            {
                                let login = self
                                    .authority
                                    .session_login(r.session_id, &session.route)
                                    .ok_or(PeerError::Protocol)?;
                                LookupResult::Active {
                                    session: Box::new(session),
                                    credentials,
                                    verifiers: login.verifiers,
                                }
                            } else {
                                LookupResult::Ended {
                                    reason: session.revoke_reason,
                                }
                            }
                        } else {
                            LookupResult::Ended {
                                reason: session.revoke_reason,
                            }
                        }
                    }
                };
                encode(&result)?
            }
            "/peer/v1/scope/get" => {
                let r: ScopeGet = decode(body)?;
                encode(&ScopeResult {
                    scope: self.authority.scope(r.scope_id),
                })?
            }
            "/peer/v1/changes" => {
                let r: ChangesRequest = decode(body)?;
                if r.touched.len() > 1024 { return Err(PeerError::Protocol); }
                self.authority.peer_touch(&r.touched);
                self.authority.sweep().map_err(|_| PeerError::Protocol)?;
                encode(
                    &self
                        .authority
                        .peer_changes(r.since, self.leader.term()),
                )?
            }
            "/peer/v1/counter/reserve" => {
                let r: Reserve = decode(body)?;
                let limits = match &self.limit_source { Some(source) => source(&r.domain), None => self.limits.get(&r.domain).copied() }.ok_or(PeerError::Protocol)?;
                let mut batches = self.batches.lock().map_err(|_| PeerError::Protocol)?;
                let now = self.authority.peer_now();
                batches.retain(|_, b| b.expires > now);
                if batches.len() >= 65_536 {
                    return Err(PeerError::Protocol);
                }
                let id = auth::nonce()?;
                let domain = DomainName(r.domain);
                let (granted, window, window_end) =
                    match self
                        .authority
                        .peer_reserve(r.session_id, &domain, &limits, r.want)
                    {
                        Ok(r) => r,
                        Err(TokenizeError::Ended) => {
                            return Ok((
                                200,
                                encode(&Ended {
                                    state: EndedState::Ended,
                                })?,
                            ));
                        }
                        Err(TokenizeError::LimitReached | TokenizeError::Store) => {
                            return Err(PeerError::Protocol);
                        }
                    };
                let expires = now.saturating_add(30_000).min(window_end);
                if granted > 0 {
                    batches.insert(
                        id.clone(),
                        Batch {
                            session: r.session_id,
                            domain,
                            window,
                            granted,
                            expires,
                        },
                    );
                }
                encode(&Reservation {
                    granted,
                    batch_id: id,
                    expires_ms: expires,
                })?
            }
            "/peer/v1/counter/return" => {
                let r: Return = decode(body)?;
                let mut batches = self.batches.lock().map_err(|_| PeerError::Protocol)?;
                if let Some(batch) = batches.get(&r.batch_id) {
                    if r.unused > batch.granted {
                        return Err(PeerError::Protocol);
                    }
                    if self.authority.peer_now() < batch.expires {
                        self.authority
                            .peer_refund(batch.session, &batch.domain, batch.window, r.unused)
                            .map_err(|_| PeerError::Protocol)?;
                    }
                    batches.remove(&r.batch_id);
                }
                encode(&Ack { ok: true })?
            }
            "/peer/v1/session/report" => {
                let r: Report = decode(body)?;
                match r.event {
                    Event::Start | Event::End => {
                        self.authority
                            .admit(r.session_id, Operation::Read)
                            .map_err(|_| PeerError::Protocol)?;
                    }
                }
                encode(&Ack { ok: true })?
            }
            _ => return Err(PeerError::Protocol),
        };
        Ok((200, reply))
    }
}
fn response(status: u16, body: Vec<u8>) -> Response<Full<Bytes>> {
    let mut r = Response::new(Full::new(Bytes::from(body)));
    *r.status_mut() =
        http::StatusCode::from_u16(status).unwrap_or(http::StatusCode::INTERNAL_SERVER_ERROR);
    r
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn ready_term_gates_leader_replies() {
        use crate::state::{authority::SessionLimits, leader_store::LeaderStore};
        let proxy = Arc::new(LeaderStore::new(Arc::new(|| None), Arc::new(|| 0)));
        let authority = Authority::empty(proxy, SessionLimits::default());
        let leader = LeaderState::live_for_test();
        leader.term_for_test(7);
        for (ready, expected) in [(None, 409), (Some(6), 409), (Some(7), 200)] {
            let server = PeerServer::new(authority.clone(), leader.clone(), Arc::new(PeerAuth::new(&[7; 32]).unwrap()), HashMap::new())
                .with_ready(Arc::new(move || ready)).unwrap();
            let request = encode(&Lookup { session_id: SessionId([0; 16]) }).unwrap();
            let (status, body) = server.reply(&http::Method::POST, "/peer/v1/session/lookup", false, &request, 7).unwrap();
            assert_eq!(status, expected);
            if expected == 409 { assert!(String::from_utf8(body).unwrap().contains("not_leader")); }
        }
    }
}
