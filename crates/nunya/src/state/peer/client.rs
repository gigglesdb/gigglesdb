//! Per-call HTTP/1 client with authenticated generation fencing.
use super::*;
use crate::policy::kube_leader::LeaderState;
use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper_util::rt::TokioIo;
use serde::{Serialize, de::DeserializeOwned};
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
/// Peer client; no connection state or authentication material is logged.
pub struct PeerClient {
    leader: LeaderState,
    port: u16,
    pod: String,
    auth: Arc<PeerAuth>,
    highest: AtomicU64,
    base: Option<String>,
}
impl std::fmt::Debug for PeerClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PeerClient { [REDACTED] }")
    }
}
impl PeerClient {
    /// Map the elected management URL host to the configured peer port using HTTP.
    pub fn new(leader: LeaderState, port: u16, pod: String, auth: Arc<PeerAuth>) -> Self {
        Self {
            leader,
            port,
            pod,
            auth,
            highest: AtomicU64::new(0),
            base: None,
        }
    }
    /// Use an explicit peer base URL, for local listeners and callers with resolved discovery.
    pub fn with_url(url: String, pod: String, auth: Arc<PeerAuth>) -> Self {
        Self {
            base: Some(url),
            ..Self::new(LeaderState::default(), 6482, pod, auth)
        }
    }
    fn url(&self) -> Result<String, PeerError> {
        if let Some(base) = &self.base {
            return Ok(base.clone());
        }
        let url: http::Uri = self
            .leader
            .leader_url()
            .ok_or(PeerError::NotLeader)?
            .parse()
            .map_err(|_| PeerError::Protocol)?;
        let host = url.host().ok_or(PeerError::Protocol)?;
        Ok(format!("http://{host}:{}", self.port))
    }
    async fn call<Q: Serialize, R: DeserializeOwned>(
        &self,
        op: &str,
        input: &Q,
        timeout: Duration,
    ) -> Result<R, PeerError> {
        tokio::time::timeout(timeout, self.call_inner(op, input))
            .await
            .map_err(|_| PeerError::Timeout)?
    }
    async fn call_inner<Q: Serialize, R: DeserializeOwned>(
        &self,
        op: &str,
        input: &Q,
    ) -> Result<R, PeerError> {
        let base: http::Uri = self.url()?.parse().map_err(|_| PeerError::Protocol)?;
        if base.scheme_str() != Some("http") {
            return Err(PeerError::Protocol);
        }
        let host = base.host().ok_or(PeerError::Protocol)?;
        let port = base.port_u16().unwrap_or(80);
        let socket = tokio::net::TcpStream::connect((host.trim_matches(['[', ']']), port))
            .await
            .map_err(|_| PeerError::Transport)?;
        let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(socket))
            .await
            .map_err(|_| PeerError::Transport)?;
        // Poll the connection inside this call, so timeouts drop the socket too.
        let request = async {
            let path = format!("/peer/v1/{op}");
            let ts = auth::now().to_string();
            let nonce = auth::nonce()?;
            let body = serde_json::to_vec(input).map_err(|_| PeerError::Protocol)?;
            let mac = self.auth.sign(&path, &ts, &nonce, &self.pod, &body);
            let req = hyper::Request::builder()
                .method("POST")
                .uri(&path)
                .header(
                    "host",
                    base.authority().ok_or(PeerError::Protocol)?.as_str(),
                )
                .header("x-nunya-peer-ts", ts)
                .header("x-nunya-peer-nonce", &nonce)
                .header("x-nunya-peer-pod", &self.pod)
                .header("x-nunya-peer-mac", mac)
                .body(Full::new(Bytes::from(body)))
                .map_err(|_| PeerError::Protocol)?;
            let response = sender
                .send_request(req)
                .await
                .map_err(|_| PeerError::Transport)?;
            let status = response.status().as_u16();
            if status == 401 {
                return Err(PeerError::Unauthorized);
            }
            let generation = response
                .headers()
                .get("x-nunya-peer-generation")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<u64>().ok())
                .ok_or(PeerError::Protocol)?;
            let body = Limited::new(response.into_body(), 4 * 1024 * 1024)
                .collect()
                .await
                .map_err(|_| PeerError::Transport)?
                .to_bytes();
            let plain = self.auth.open(&body, &nonce, status, generation)?;
            let highest = self.highest.fetch_max(generation, Ordering::SeqCst);
            if generation < highest {
                return Err(PeerError::Stale);
            }
            match status {
                200 => serde_json::from_slice(&plain).map_err(|_| PeerError::Protocol),
                409 => {
                    let _: Failure =
                        serde_json::from_slice(&plain).map_err(|_| PeerError::Protocol)?;
                    Err(PeerError::NotLeader)
                }
                _ => Err(PeerError::Protocol),
            }
        };
        tokio::pin!(request);
        tokio::pin!(conn);
        tokio::select! {result=&mut request=>result, _=&mut conn=>Err(PeerError::Transport)}
    }
    /// Highest authenticated election term observed so far.
    pub fn term(&self) -> u64 { self.highest.load(Ordering::SeqCst) }
    /// Fetch the leader's bindings using authenticated generation fencing.
    pub async fn bindings(&self, req: BindingsRequest, timeout: Duration) -> Result<BindingsReply, PeerError> {
        self.call("bindings", &req, timeout).await
    }
    /// Fetch live session rows and stored authentication digests.
    pub async fn session_lookup(&self, r: Lookup, t: Duration) -> Result<LookupResult, PeerError> {
        self.call("session/lookup", &r, t).await
    }
    /// Fetch derivation metadata only.
    pub async fn scope_get(&self, r: ScopeGet, t: Duration) -> Result<ScopeResult, PeerError> {
        self.call("scope/get", &r, t).await
    }
    /// Fetch invalidations since a previous cursor.
    pub async fn changes(&self, r: ChangesRequest, t: Duration) -> Result<Changes, PeerError> {
        self.call("changes", &r, t).await
    }
    /// Reserve durable tokenization capacity.
    pub async fn counter_reserve(
        &self,
        r: Reserve,
        t: Duration,
    ) -> Result<ReserveResult, PeerError> {
        self.call("counter/reserve", &r, t).await
    }
    /// Return unused capacity at most once.
    pub async fn counter_return(&self, r: Return, t: Duration) -> Result<Ack, PeerError> {
        self.call("counter/return", &r, t).await
    }
    /// Report session connection activity.
    pub async fn session_report(&self, r: Report, t: Duration) -> Result<Ack, PeerError> {
        self.call("session/report", &r, t).await
    }
    #[cfg(test)]
    pub(super) fn observe_generation(&self, g: u64) {
        self.highest.store(g, Ordering::SeqCst);
    }
}
