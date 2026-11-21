//! Shared authenticated accept loop and bounded HTTP protocol driver.
use super::{
    body::ResponseBody, dispatch::ApiRuntime, error::ApiError, identity::ConnectionIdentity,
};
use crate::{Running, tls, transport::IO_TIMEOUT};
use http::{HeaderMap, Request, Response};
use hyper::{body::Incoming, service::service_fn};
use hyper_util::{
    rt::{TokioExecutor, TokioIo, TokioTimer},
    server::conn::auto,
};
use rustls::ServerConfig;
use std::{
    collections::BTreeMap,
    convert::Infallible,
    io,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Instant,
};
use tokio::{
    net::TcpListener,
    sync::{Notify, OwnedSemaphorePermit, Semaphore, oneshot, watch},
    time::timeout,
};
use tokio_rustls::TlsAcceptor;

pub(crate) const MAX_HEADERS: usize = 64;
pub(crate) const MAX_HEADER_BYTES: usize = 16 * 1024;

pub async fn serve_http(
    address: SocketAddr,
    runtime: Arc<ApiRuntime>,
    config: Arc<ServerConfig>,
    connections: usize,
) -> io::Result<Running> {
    let listener = TcpListener::bind(address).await?;
    let (_, gate) = watch::channel(true);
    serve_http_bound(listener, runtime, config, connections, gate).await
}

pub async fn serve_grpc(
    address: SocketAddr,
    runtime: Arc<ApiRuntime>,
    config: Arc<ServerConfig>,
    connections: usize,
) -> io::Result<Running> {
    let listener = TcpListener::bind(address).await?;
    let (_, gate) = watch::channel(true);
    serve_grpc_bound(listener, runtime, config, connections, gate).await
}

pub async fn serve_http_bound(
    listener: TcpListener,
    runtime: Arc<ApiRuntime>,
    config: Arc<ServerConfig>,
    connections: usize,
    start: watch::Receiver<bool>,
) -> io::Result<Running> {
    serve_bound(listener, runtime, config, connections, start, false).await
}

pub async fn serve_grpc_bound(
    listener: TcpListener,
    runtime: Arc<ApiRuntime>,
    config: Arc<ServerConfig>,
    connections: usize,
    start: watch::Receiver<bool>,
) -> io::Result<Running> {
    serve_bound(listener, runtime, config, connections, start, true).await
}

async fn serve_bound(
    listener: TcpListener,
    runtime: Arc<ApiRuntime>,
    config: Arc<ServerConfig>,
    connections: usize,
    mut start: watch::Receiver<bool>,
    grpc: bool,
) -> io::Result<Running> {
    if !(1..=128).contains(&connections) {
        return Err(io::Error::other("connection limit"));
    }
    let address = listener.local_addr()?;
    let mut config = (*config).clone();
    config.alpn_protocols = if grpc {
        vec![b"h2".to_vec()]
    } else {
        vec![b"h2".to_vec(), b"http/1.1".to_vec()]
    };
    let acceptor = TlsAcceptor::from(Arc::new(config));
    let (stop, mut stopped) = oneshot::channel();
    let task = tokio::spawn(async move {
        while !*start.borrow_and_update() {
            tokio::select! {
                _ = &mut stopped => return Ok(()),
                changed = start.changed() => if changed.is_err() { return Ok(()); },
            }
        }
        let cap = Arc::new(Semaphore::new(connections));
        let mut sockets = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                _ = &mut stopped => break,
                Some(_) = sockets.join_next(), if !sockets.is_empty() => {},
                incoming = listener.accept() => {
                    let (socket, _) = incoming?;
                    crate::transport::no_delay(&socket);
                    let Some(shared) = runtime.service().admission.connect() else { continue; };
                    let Ok(local) = cap.clone().try_acquire_owned() else { continue; };
                    let runtime = runtime.clone();
                    let acceptor = acceptor.clone();
                    sockets.spawn(async move {
                        let _local = local;
                        let Ok(Ok(stream)) = timeout(IO_TIMEOUT, acceptor.accept(socket)).await else { return; };
                        let h2 = stream.get_ref().1.alpn_protocol() == Some(b"h2");
                        if grpc && !h2 { return; }
                        let cert = stream.get_ref().1.peer_certificates().and_then(|chain| chain.first());
                        if cert.is_none() && runtime.kind() != super::dispatch::ListenerKind::Agent { return; }
                        let identity = cert.map_or(Err(ApiError::Unauthenticated), |cert| runtime.authenticate(&tls::fingerprint(cert.as_ref())));
                        let activity = Arc::new(ConnectionActivity::new());
                        let monitor = activity.clone();
                        let handler = service_fn(move |request| {
                            let headers_received = Instant::now();
                            handle(request, runtime.clone(), identity.clone(), shared.clone(), activity.clone(), grpc, headers_received)
                        });
                        let mut builder = auto::Builder::new(TokioExecutor::new());
                        builder.http1()
                            .timer(TokioTimer::new())
                            .header_read_timeout(IO_TIMEOUT)
                            .max_headers(MAX_HEADERS)
                            .max_buf_size(MAX_HEADER_BYTES);
                        builder.http2()
                            .timer(TokioTimer::new())
                            .max_concurrent_streams(16)
                            .max_header_list_size(MAX_HEADER_BYTES as u32)
                            .max_frame_size(16 * 1024)
                            .initial_stream_window_size(64 * 1024)
                            .initial_connection_window_size(1024 * 1024)
                            .adaptive_window(false)
                            .max_send_buf_size(64 * 1024)
                            .max_pending_accept_reset_streams(16)
                            .max_local_error_reset_streams(16);
                        let builder = if h2 { builder.http2_only() } else { builder.http1_only() };
                        tokio::select! {
                            _ = builder.serve_connection(TokioIo::new(stream), handler) => {},
                            _ = monitor.watchdog() => {},
                        }
                    });
                }
            }
        }
        sockets.abort_all();
        while sockets.join_next().await.is_some() {}
        Ok(())
    });
    Ok(Running::new(address, stop, task))
}

/// Transport facts for role sign-in; no authority is granted at accept time.
#[derive(Clone)]
pub(crate) struct RoleConnection {
    pub peer: SocketAddr,
    pub certificate: Option<String>,
    pub tls: bool,
}
trait RoleIo: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send {}
impl<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send> RoleIo for T {}

pub async fn serve_role_bound(
    listener: TcpListener,
    runtime: Arc<ApiRuntime>,
    settings: Arc<crate::auth::RoleApiListener>,
    mut start: watch::Receiver<bool>,
    grpc: bool,
) -> io::Result<Running> {
    let connections = settings.connections;
    if !(1..=128).contains(&connections) {
        return Err(io::Error::other("connection limit"));
    }
    let address = listener.local_addr()?;
    let acceptor = settings.tls_config.as_ref().map(|config| {
        let mut config = (**config).clone();
        config.alpn_protocols = if grpc {
            vec![b"h2".to_vec()]
        } else {
            vec![b"h2".to_vec(), b"http/1.1".to_vec()]
        };
        TlsAcceptor::from(Arc::new(config))
    });
    let (stop, mut stopped) = oneshot::channel();
    let task = tokio::spawn(async move {
        while !*start.borrow_and_update() {
            tokio::select! {
                _ = &mut stopped => return Ok(()),
                changed = start.changed() => if changed.is_err() { return Ok(()); },
            }
        }
        let cap = Arc::new(Semaphore::new(connections));
        let mut sockets = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                _ = &mut stopped => break,
                Some(_) = sockets.join_next(), if !sockets.is_empty() => {},
                incoming = listener.accept() => {
                    let (socket, peer) = incoming?;
                    let Some(shared) = runtime.service().admission.connect() else { continue; };
                    let Ok(local) = cap.clone().try_acquire_owned() else { continue; };
                    let runtime = runtime.clone();
                    let acceptor = acceptor.clone();
                    sockets.spawn(async move {
                        let _local = local;
                        let tls = acceptor.is_some();
                        let (stream, certificate): (Box<dyn RoleIo>, Option<String>) = if let Some(acceptor) = acceptor {
                            let Ok(Ok(stream)) = timeout(IO_TIMEOUT, acceptor.accept(socket)).await else { return; };
                            if grpc && stream.get_ref().1.alpn_protocol() != Some(b"h2") { return; }
                            let certificate = stream.get_ref().1.peer_certificates().and_then(|chain| chain.first()).map(|cert| tls::fingerprint(cert.as_ref()));
                            (Box::new(stream), certificate)
                        } else { (Box::new(socket), None) };
                        let identity = Ok(ConnectionIdentity::role_connection(RoleConnection { peer, certificate, tls }));
                        let activity = Arc::new(ConnectionActivity::new());
                        let monitor = activity.clone();
                        let handler = service_fn(move |request| {
                            let headers_received = Instant::now();
                            handle(request, runtime.clone(), identity.clone(), shared.clone(), activity.clone(), grpc, headers_received)
                        });
                        let mut builder = auto::Builder::new(TokioExecutor::new());
                        builder.http1()
                            .timer(TokioTimer::new())
                            .header_read_timeout(IO_TIMEOUT)
                            .max_headers(MAX_HEADERS)
                            .max_buf_size(MAX_HEADER_BYTES);
                        builder.http2()
                            .timer(TokioTimer::new())
                            .max_concurrent_streams(16)
                            .max_header_list_size(MAX_HEADER_BYTES as u32)
                            .max_frame_size(16 * 1024)
                            .initial_stream_window_size(64 * 1024)
                            .initial_connection_window_size(1024 * 1024)
                            .adaptive_window(false)
                            .max_send_buf_size(64 * 1024)
                            .max_pending_accept_reset_streams(16)
                            .max_local_error_reset_streams(16);
                        let builder = if grpc { builder.http2_only() } else { builder };
                        tokio::select! {
                            _ = builder.serve_connection(TokioIo::new(stream), handler) => {},
                            _ = monitor.watchdog() => {},
                        }
                    });
                }
            }
        }
        sockets.abort_all();
        while sockets.join_next().await.is_some() {}
        Ok(())
    });
    Ok(Running::new(address, stop, task))
}

async fn handle(
    request: Request<Incoming>,
    runtime: Arc<ApiRuntime>,
    identity: Result<ConnectionIdentity, ApiError>,
    socket: Arc<OwnedSemaphorePermit>,
    activity: Arc<ConnectionActivity>,
    grpc: bool,
    headers_received: Instant,
) -> Result<Response<ResponseBody>, Infallible> {
    let identity = if runtime.kind() == super::dispatch::ListenerKind::Agent && identity.is_err() {
        bearer_secret(request.headers())
            .and_then(|secret| ConnectionIdentity::verified_credential(runtime.service(), secret))
    } else {
        identity
    };
    let version = request.version();
    let release_binding = identity
        .as_ref()
        .ok()
        .map(|identity| (identity.clone(), runtime.service().clone()));
    let ticket = activity.begin();
    let admission = runtime.admit_call(socket);
    let mut response = match admission {
        Ok(ownership) => {
            if let Err(error) = validate_request_headers(&request).and_then(|()| {
                // Bare role connections pass here; token lookup/leases recheck owners.
                identity
                    .as_ref()
                    .map_err(|error| *error)?
                    .check(runtime.service())
            }) {
                if grpc {
                    super::grpc::error_response(error, Some(ownership), ticket)
                } else {
                    super::http::error_response(error, Some(ownership), ticket)
                }
            } else if grpc {
                super::grpc::handle(
                    request,
                    runtime,
                    identity.expect("validated identity"),
                    ownership,
                    ticket,
                    headers_received,
                )
                .await
            } else {
                super::http::handle(
                    request,
                    runtime,
                    identity.expect("validated identity"),
                    ownership,
                    ticket,
                    headers_received,
                )
                .await
            }
        }
        Err(error) => {
            if grpc {
                super::grpc::error_response(error, None, ticket)
            } else {
                super::http::error_response(error, None, ticket)
            }
        }
    };
    if version == http::Version::HTTP_2 {
        response.headers_mut().remove(http::header::CONNECTION);
    }
    if response.status().is_success()
        && let Some((identity, service)) = release_binding
        // Role opens carry their signed-in owner; execute replies carry a lease.
        && identity.connection.is_none()
    {
        response.body_mut().guard_binding(identity, service);
    }
    Ok(response)
}

fn bearer_secret(headers: &HeaderMap) -> Result<&str, ApiError> {
    let mut values = headers.get_all(http::header::AUTHORIZATION).iter();
    let value = values.next().ok_or(ApiError::Unauthenticated)?;
    if values.next().is_some() || value.as_bytes().len() > 256 {
        return Err(ApiError::Unauthenticated);
    }
    let value = value.to_str().map_err(|_| ApiError::Unauthenticated)?;
    let (scheme, token) = value.split_once(' ').ok_or(ApiError::Unauthenticated)?;
    if !scheme.eq_ignore_ascii_case("Bearer")
        || token.is_empty()
        || token.bytes().any(|b| b.is_ascii_whitespace())
    {
        return Err(ApiError::Unauthenticated);
    }
    Ok(token)
}

fn validate_request_headers<B>(request: &Request<B>) -> Result<(), ApiError> {
    // Hyper moves HTTP/2 pseudoheaders into the method and URI. Reserve all
    // four standard fields so the regular HeaderMap cannot bypass the count.
    let pseudo = if request.version() == http::Version::HTTP_2 {
        4
    } else {
        0
    };
    if request.headers().len() > MAX_HEADERS - pseudo {
        return Err(ApiError::InvalidArgument);
    }
    validate_headers(request.headers())
}

pub(crate) fn validate_headers(headers: &HeaderMap) -> Result<(), ApiError> {
    if headers.len() > MAX_HEADERS {
        return Err(ApiError::InvalidArgument);
    }
    let size = headers
        .iter()
        .try_fold(0usize, |n, (name, value)| {
            n.checked_add(name.as_str().len())?
                .checked_add(value.as_bytes().len())?
                .checked_add(32)
        })
        .ok_or(ApiError::InvalidArgument)?;
    if size > MAX_HEADER_BYTES {
        return Err(ApiError::InvalidArgument);
    }
    Ok(())
}

pub(crate) fn session_header(
    headers: &HeaderMap,
    required: bool,
) -> Result<Option<String>, ApiError> {
    let mut values = headers.get_all("x-giggles-session").iter();
    let first = values.next();
    if values.next().is_some() {
        return Err(ApiError::InvalidArgument);
    }
    match (first, required) {
        (None, false) => Ok(None),
        (Some(value), true) => {
            let value = value.to_str().map_err(|_| ApiError::InvalidArgument)?;
            if value.len() != 64
                || !value
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            {
                return Err(ApiError::InvalidArgument);
            }
            Ok(Some(value.to_owned()))
        }
        _ => Err(ApiError::InvalidArgument),
    }
}

pub(crate) fn declared_length(headers: &HeaderMap, limit: usize) -> Result<(), ApiError> {
    if let Some(value) = headers.get(http::header::CONTENT_LENGTH) {
        let length = value
            .to_str()
            .map_err(|_| ApiError::InvalidArgument)?
            .parse::<u64>()
            .map_err(|_| ApiError::InvalidArgument)?;
        if length > limit as u64 {
            return Err(ApiError::PayloadTooLarge);
        }
    }
    Ok(())
}

struct ActivityState {
    next: u64,
    last_application: Instant,
    calls: BTreeMap<u64, Instant>,
}

pub(crate) struct ConnectionActivity {
    state: Mutex<ActivityState>,
    changed: Notify,
}

impl ConnectionActivity {
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(ActivityState {
                next: 0,
                last_application: Instant::now(),
                calls: BTreeMap::new(),
            }),
            changed: Notify::new(),
        }
    }

    pub(crate) fn begin(self: &Arc<Self>) -> ActivityTicket {
        let mut state = self.state.lock().expect("connection activity lock");
        // A connection has a finite stream cap and ten-second idle timeout.
        // On counter exhaustion, use an already-expired deadline and terminate it.
        let id = state.next;
        state.next = state.next.checked_add(1).unwrap_or(state.next);
        state.last_application = Instant::now();
        let deadline = if id == u64::MAX {
            Instant::now()
        } else {
            Instant::now() + IO_TIMEOUT
        };
        state.calls.insert(id, deadline);
        drop(state);
        self.changed.notify_one();
        ActivityTicket {
            activity: self.clone(),
            id,
        }
    }

    fn deadline(&self) -> Instant {
        let state = self.state.lock().expect("connection activity lock");
        state
            .calls
            .values()
            .copied()
            .min()
            .unwrap_or(state.last_application + IO_TIMEOUT)
    }

    async fn watchdog(&self) {
        loop {
            let notified = self.changed.notified();
            let deadline = self.deadline();
            tokio::select! {
                _ = notified => {},
                _ = tokio::time::sleep_until(deadline.into()) => {
                    if Instant::now() >= self.deadline() { return; }
                }
            }
        }
    }
}

pub(crate) struct ActivityTicket {
    activity: Arc<ConnectionActivity>,
    id: u64,
}

impl ActivityTicket {
    pub(crate) fn stage(&self, deadline: Instant) {
        if let Some(value) = self
            .activity
            .state
            .lock()
            .expect("connection activity lock")
            .calls
            .get_mut(&self.id)
        {
            *value = deadline;
        }
        self.activity.changed.notify_one();
    }
    pub(crate) fn write_stage(&self) {
        self.stage(Instant::now() + IO_TIMEOUT);
    }
}

impl Drop for ActivityTicket {
    fn drop(&mut self) {
        let mut state = self
            .activity
            .state
            .lock()
            .expect("connection activity lock");
        state.calls.remove(&self.id);
        state.last_application = Instant::now();
        drop(state);
        self.activity.changed.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn http2_pseudoheaders_are_included_in_count_bound() {
        let mut request = Request::builder()
            .version(http::Version::HTTP_2)
            .body(())
            .unwrap();
        for _ in 0..60 {
            request
                .headers_mut()
                .append("x-field", "x".parse().unwrap());
        }
        assert!(validate_request_headers(&request).is_ok());
        request
            .headers_mut()
            .append("x-field", "x".parse().unwrap());
        assert_eq!(
            validate_request_headers(&request),
            Err(ApiError::InvalidArgument)
        );
        *request.version_mut() = http::Version::HTTP_11;
        assert!(validate_request_headers(&request).is_ok());
    }
    #[test]
    fn session_header_rejects_duplicate_missing_and_noncanonical() {
        let mut headers = HeaderMap::new();
        assert!(session_header(&headers, false).unwrap().is_none());
        assert!(session_header(&headers, true).is_err());
        headers.insert("x-giggles-session", "a".repeat(64).parse().unwrap());
        assert_eq!(
            session_header(&headers, true).unwrap().unwrap(),
            "a".repeat(64)
        );
        assert!(session_header(&headers, false).is_err());
        headers.append("x-giggles-session", "b".repeat(64).parse().unwrap());
        assert!(session_header(&headers, true).is_err());
    }
}
