//! Shared authenticated accept loop and bounded HTTP protocol driver.
use super::{
    body::TransportBody, dispatch::ApiRuntime, error::ApiError, identity::ConnectionIdentity,
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
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
    time::Instant,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
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
                        let stream = FlushIo { inner: stream, tracker: activity.flush.clone() };
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
                        let stream = FlushIo { inner: stream, tracker: activity.flush.clone() };
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
) -> Result<Response<TransportBody>, Infallible> {
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
    Ok(response.map(|body| {
        body.with_flush_tracker(activity.flush.clone())
            .into_transport()
    }))
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
    flush: Arc<FlushTracker>,
    state: Mutex<ActivityState>,
    changed: Notify,
}

impl ConnectionActivity {
    pub(crate) fn new() -> Self {
        Self {
            flush: Arc::new(FlushTracker::default()),
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

// Receipts are per response; the tracker belongs to exactly one connection.
// Registration means protocol consumption, never socket completion. Only a
// successful downstream flush acknowledges registered receipts. Hyper/h2 drain
// their codec buffers before calling this IO's poll_flush.
#[derive(Default)]
pub(crate) struct FlushTracker {
    pending: Mutex<Vec<Arc<FlushReceipt>>>,
}
#[derive(Default)]
pub(crate) struct FlushReceipt(Mutex<(bool, Option<Waker>)>);
impl FlushReceipt {
    pub(crate) fn poll_flushed(&self, cx: &mut Context<'_>) -> Poll<()> {
        let mut state = self.0.lock().expect("flush receipt");
        if state.0 {
            Poll::Ready(())
        } else {
            state.1 = Some(cx.waker().clone());
            Poll::Pending
        }
    }
    fn acknowledge(&self) {
        let wake = {
            let mut state = self.0.lock().expect("flush receipt");
            state.0 = true;
            state.1.take()
        };
        if let Some(wake) = wake {
            wake.wake();
        }
    }
}
impl FlushTracker {
    pub(crate) fn consumed(&self, receipt: Arc<FlushReceipt>) {
        self.pending.lock().expect("connection flush").push(receipt);
    }
}
struct FlushIo<T> {
    inner: T,
    tracker: Arc<FlushTracker>,
}
impl<T: AsyncRead + Unpin> AsyncRead for FlushIo<T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}
impl<T: AsyncWrite + Unpin> AsyncWrite for FlushIo<T> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write_vectored(cx, bufs)
    }
    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        // Consumption cannot occur concurrently: protocol buffers and IO are
        // driven by the same connection task. Do not hold locks while waking.
        std::task::ready!(Pin::new(&mut this.inner).poll_flush(cx))?;
        let receipts = std::mem::take(&mut *this.tracker.pending.lock().expect("connection flush"));
        for receipt in receipts {
            receipt.acknowledge();
        }
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod flush_tests {
    use super::super::body::ResponseBody;
    use super::*;
    use bytes::Bytes;
    use http_body_util::{BodyExt, Empty, StreamBody};

    #[test]
    fn receipt_requires_successful_flush_on_its_own_connection() {
        struct Gate(bool);
        impl AsyncWrite for Gate {
            fn poll_write(
                self: Pin<&mut Self>,
                _: &mut Context<'_>,
                bytes: &[u8],
            ) -> Poll<io::Result<usize>> {
                Poll::Ready(Ok(bytes.len()))
            }
            fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
                if self.0 {
                    Poll::Ready(Ok(()))
                } else {
                    Poll::Pending
                }
            }
            fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
                Poll::Ready(Ok(()))
            }
        }
        let tracker = Arc::new(FlushTracker::default());
        let receipt = Arc::new(FlushReceipt::default());
        let mut io = FlushIo {
            inner: Gate(false),
            tracker: tracker.clone(),
        };
        let mut other = FlushIo {
            inner: Gate(true),
            tracker: Arc::new(FlushTracker::default()),
        };
        let mut cx = Context::from_waker(Waker::noop());
        tracker.consumed(receipt.clone());
        assert!(Pin::new(&mut other).poll_flush(&mut cx).is_ready());
        assert!(receipt.poll_flushed(&mut cx).is_pending());
        assert!(Pin::new(&mut io).poll_flush(&mut cx).is_pending());
        assert!(receipt.poll_flushed(&mut cx).is_pending());
        io.inner.0 = true;
        assert!(Pin::new(&mut io).poll_flush(&mut cx).is_ready());
        assert!(receipt.poll_flushed(&mut cx).is_ready());
    }

    async fn diagnostic_stall(h2: bool) {
        // One-byte h2 window, and a small duplex transport for h1, make a
        // partially sent diagnostic unavoidable before we suspend consumption.
        let (server, client) = tokio::io::duplex(64);
        let diagnostic = serde_json::json!({"sql":{"streamError":{
            "code":"58030", "message":"x".repeat(4096)
        }}})
        .to_string();
        let expected = diagnostic.clone();
        let activity = Arc::new(ConnectionActivity::new());
        let tracker = activity.flush.clone();
        let service = service_fn(move |_: Request<Incoming>| {
            let frames = vec![
                Ok(http_body::Frame::data(Bytes::from(diagnostic.clone()))),
                Err(tonic::Status::internal("58030")),
            ];
            let inner = StreamBody::new(tonic::codegen::tokio_stream::iter(frames));
            let body =
                ResponseBody::new_http(tonic::body::Body::new(inner), None, None, activity.begin())
                    .with_flush_tracker(activity.flush.clone())
                    .into_transport();
            async move { Ok::<_, Infallible>(Response::new(body)) }
        });
        let server_task = tokio::spawn(async move {
            let mut builder = auto::Builder::new(TokioExecutor::new());
            if h2 {
                builder = builder.http2_only();
            } else {
                builder = builder.http1_only();
            }
            let _ = builder
                .serve_connection(
                    TokioIo::new(FlushIo {
                        inner: server,
                        tracker,
                    }),
                    service,
                )
                .await;
        });
        let (mut body, client_task) = if h2 {
            let (mut sender, conn) = hyper::client::conn::http2::Builder::new(TokioExecutor::new())
                .initial_stream_window_size(1)
                .adaptive_window(false)
                .handshake::<_, Empty<Bytes>>(TokioIo::new(client))
                .await
                .unwrap();
            let task = tokio::spawn(async move {
                let _ = conn.await;
            });
            let response = sender
                .send_request(
                    Request::builder()
                        .uri("https://localhost/")
                        .body(Empty::new())
                        .unwrap(),
                )
                .await
                .unwrap();
            (response.into_body(), task)
        } else {
            let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(client))
                .await
                .unwrap();
            let task = tokio::spawn(async move {
                let _ = conn.await;
            });
            let response = sender
                .send_request(
                    Request::builder()
                        .uri("/")
                        .body(Empty::<Bytes>::new())
                        .unwrap(),
                )
                .await
                .unwrap();
            (response.into_body(), task)
        };
        let mut bytes = body
            .frame()
            .await
            .unwrap()
            .unwrap()
            .into_data()
            .unwrap()
            .to_vec();
        assert!(bytes.len() < expected.len());
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let read = async {
            while let Some(frame) = body.frame().await {
                match frame {
                    Ok(frame) => {
                        if let Ok(data) = frame.into_data() {
                            bytes.extend_from_slice(&data);
                        }
                    }
                    Err(_) => return true,
                }
            }
            false
        };
        assert!(
            timeout(std::time::Duration::from_secs(5), read)
                .await
                .unwrap(),
            "body must fail"
        );
        assert_eq!(bytes, expected.as_bytes());
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["sql"]["streamError"]["code"], "58030");
        client_task.abort();
        server_task.abort();
        let _ = client_task.await;
        let _ = server_task.await;
    }
    #[tokio::test]
    async fn h2_diagnostic_survives_stalled_window_before_body_error() {
        diagnostic_stall(true).await;
    }
    #[tokio::test]
    async fn h1_diagnostic_survives_stalled_transport_before_body_error() {
        diagnostic_stall(false).await;
    }
}
