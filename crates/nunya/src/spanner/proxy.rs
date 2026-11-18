//! Authenticated, fail-closed proxy for the native Spanner API.
// tonic::Status is large by design.
#![allow(clippy::result_large_err)]
use super::{
    auth::{self, Authenticated},
    redact::{nunya_status, redact_status},
    server::{SpannerServer, SpannerService},
    stream::{self, Event, Reassembler},
    values,
};
use crate::{
    dialect::{
        googlesql::lower_gsql,
        spanner_struct::{self, ValuePosition},
    },
    error::{NyCode, Refusal},
    ir::{InputRewrite, RewritePlan, StatementKind, ValueSource, check::check},
    policy::{DomainName, Policy, TokenizeSetting, Treatment},
    readlog::{LifecycleRecord, LifecycleKind, LifecycleActor, Outcome, ReadLog, ReadLogRecord, statement_fingerprint},
    schema::Snapshot,
    state::authority::{AdmitError, Operation, TokenizeError, TokenizeLimits},
    token::{SessionId, StandardCodec, TokenCodec, keys::RootKey},
    value::ColumnType,
};
use futures::{Stream, StreamExt};
use googleapis_tonic_google_spanner_v1::google::spanner::v1::{spanner_client::SpannerClient, *};
use prost_types::{ListValue, Value};
use std::{
    collections::HashMap,
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};
use tonic::{
    Request, Response, Status,
    service::{Interceptor, interceptor::InterceptedService},
    transport::{Channel, ClientTlsConfig, Endpoint},
};

/// Trusted upstream transport configuration.
pub enum SpannerUpstream {
    /// Plaintext emulator without Google credentials.
    Emulator {
        /// Plaintext trusted emulator URL.
        endpoint: String,
    },
    /// TLS Google service using application default credentials.
    Google {
        /// HTTPS trusted Google service URL.
        endpoint: String,
    },
}
/// Shared resources for native Spanner requests.
pub struct SpannerContext {
    /// Shared durable session authority.
    pub authority: Arc<crate::state::authority::Authority>,
    /// Shared live connection registry.
    pub registry: crate::state::runtime::Registry,
    /// Validated route identifier.
    pub route: crate::state::model::RouteId,
    /// Reloadable policy.
    pub policy: RwLock<Arc<Policy>>,
    /// Revision of the currently installed policy.
    pub policy_revision: RwLock<Option<String>>,
    /// Reloadable schema.
    pub snapshot: RwLock<Arc<Snapshot>>,
    /// Root token key.
    pub root_key: RootKey,
    /// Mandatory read audit sink.
    pub readlog: Box<dyn ReadLog>,
    /// Typed token codec.
    pub codec: StandardCodec,
    /// Only permitted upstream database.
    pub database: String,
    /// Millisecond Unix clock.
    pub clock: Arc<dyn Fn() -> u64 + Send + Sync>,
    /// Trusted upstream connection.
    pub upstream: SpannerUpstream,
}
/// Inserts only the gateway's Google token, never downstream credentials.
#[derive(Clone, Default)]
pub struct AuthInterceptor {
    /// Gateway-only bearer token.
    pub(super) token: Option<tonic::metadata::MetadataValue<tonic::metadata::Ascii>>,
}
fn tokenize_type_code(ty: &ColumnType) -> Result<TypeCode, NyCode> {
    match ty {
        ColumnType::Text | ColumnType::Varchar(_) | ColumnType::Bpchar(_) => Ok(TypeCode::String),
        ColumnType::Int8 => Ok(TypeCode::Int64),
        ColumnType::Date => Ok(TypeCode::Date),
        ColumnType::Timestamptz => Ok(TypeCode::Timestamp),
        ColumnType::Int2 | ColumnType::Int4 | ColumnType::Uuid | ColumnType::Timestamp
        | ColumnType::Bool | ColumnType::Numeric | ColumnType::Bytea | ColumnType::Json
        | ColumnType::Jsonb | ColumnType::Array(_) | ColumnType::Enum(_)
        | ColumnType::Other(_) => Err(NyCode::NotAnalysable),
    }
}

fn tokenize_refusal(error: TokenizeError) -> NyCode {
    match error {
        TokenizeError::LimitReached => NyCode::TokenizeLimit,
        TokenizeError::Ended => NyCode::SessionEnded,
        TokenizeError::Store => NyCode::ReadLogUnavailable,
    }
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        req.metadata_mut().remove("authorization");
        if let Some(token) = &self.token {
            req.metadata_mut().insert("authorization", token.clone());
        }
        Ok(req)
    }
}
/// Trusted intercepted upstream client.
pub(super) type Client = SpannerClient<InterceptedService<Channel, AuthInterceptor>>;
// One marker belongs to one RPC, including all generated-client clones.
#[derive(Clone, Default)]
struct DispatchMarker(Arc<std::sync::atomic::AtomicBool>);
impl DispatchMarker {
    fn dispatched(&self) -> bool {
        self.0.load(std::sync::atomic::Ordering::Acquire)
    }
    fn classify(&self, error: Status, write: bool) -> Status {
        if write && (!self.dispatched() || connection_refused(&error)) {
            upstream_not_ready()
        } else {
            forwarded_transport(error, write)
        }
    }
}
#[derive(Clone)]
struct DispatchService<S> {
    inner: S,
    marker: DispatchMarker,
}
impl<S, R> tower::Service<R> for DispatchService<S>
where
    S: tower::Service<R>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;
    fn poll_ready(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }
    fn call(&mut self, request: R) -> Self::Future {
        self.marker.0.store(true, std::sync::atomic::Ordering::Release);
        self.inner.call(request)
    }
}
type WriteClient = SpannerClient<InterceptedService<DispatchService<Channel>, AuthInterceptor>>;
type ConfigurationSnapshot = (Arc<Policy>, Arc<Snapshot>, Option<String>);
type ResultStream = Pin<Box<dyn Stream<Item = Result<PartialResultSet, Status>> + Send + 'static>>;
/// Native Spanner service and its gateway-created session ownership registry.
pub struct NunyaSpanner {
    ctx: Arc<SpannerContext>,
    channel: Channel,
    sessions: Arc<Mutex<HashMap<String, SessionId>>>,
    watchers: Arc<Mutex<HashMap<SessionId, SessionWatcher>>>,
    transactions: Arc<Mutex<Transactions>>,
    provider: Option<Arc<dyn gcp_auth::TokenProvider>>,
}
type Transactions = HashMap<(String, Vec<u8>), TrackedTransaction>;
#[derive(Clone, Copy, PartialEq, Eq)]
enum TxnState {
    Clean,
    Dml,
}
#[derive(Clone, Copy)]
struct TrackedTransaction {
    owner: SessionId,
    state: TxnState,
}
fn read_write(options: Option<&TransactionOptions>) -> bool {
    matches!(
        options.and_then(|o| o.mode.as_ref()),
        Some(transaction_options::Mode::ReadWrite(_))
    )
}
fn inline_read_write(selector: Option<&TransactionSelector>) -> bool {
    matches!(selector.and_then(|s| s.selector.as_ref()), Some(transaction_selector::Selector::Begin(o)) if read_write(Some(o)))
}
fn track_transaction(
    transactions: &Mutex<Transactions>,
    owner: SessionId,
    session: &str,
    id: &[u8],
    state: TxnState,
) -> Result<(), Status> {
    if id.is_empty() {
        return Ok(());
    }
    let mut transactions = transactions.lock().map_err(|_| invalid())?;
    let key = (session.to_owned(), id.to_vec());
    // Globally cap tracking at 4096 entries; evicted transactions require write admission.
    if !transactions.contains_key(&key)
        && transactions.len() >= 4096
        && let Some(key) = transactions.keys().next().cloned()
    {
        transactions.remove(&key);
    }
    transactions
        .entry(key)
        .and_modify(|entry| {
            if state == TxnState::Dml {
                entry.state = state;
            }
        })
        .or_insert(TrackedTransaction { owner, state });
    Ok(())
}
async fn best_effort_rollback(
    channel: Channel,
    provider: Option<Arc<dyn gcp_auth::TokenProvider>>,
    session: String,
    transaction_id: Vec<u8>,
) {
    let result = tokio::time::timeout(Duration::from_secs(2), async {
        upstream_client(&channel, &provider)
            .await?
            .rollback(RollbackRequest {
                session,
                transaction_id,
            })
            .await
    })
    .await;
    if !matches!(result, Ok(Ok(_))) {
        eprintln!("spanner: best-effort rollback after refused commit failed");
    }
}
struct SessionWatcher {
    connection: u64,
    stop: tokio::sync::oneshot::Sender<()>,
    ended: tokio::sync::watch::Receiver<bool>,
}
// Captured before forwarding: watcher cleanup must not erase the connection id.
#[derive(Clone)]
struct WriteAudit {
    ctx: Arc<SpannerContext>,
    who: Authenticated,
    connection: Option<u64>,
}
impl WriteAudit {
    fn new(service: &NunyaSpanner, who: &Authenticated) -> Self {
        Self {
            ctx: service.ctx.clone(), who: who.clone(),
            connection: service.watchers.lock().ok().and_then(|w| w.get(&who.session).map(|w| w.connection)),
        }
    }
    fn record(&self, reason: &'static str) {
        if self.ctx.readlog.write_lifecycle(&LifecycleRecord {
            kind: LifecycleKind::WriteOutcomeUnknown,
            at_ms: (self.ctx.clock)(), principal: self.who.principal.to_hex(),
            session: Some(self.who.session.to_hex()), route: Some(self.ctx.route.as_str().into()),
            connection: self.connection, actor: LifecycleActor::System, reason,
        }).is_err() {
            eprintln!("audit: write outcome unknown record unavailable");
        }
    }
    fn finish(&self, error: Status, reason: UnknownCause) -> Status {
        let reason = match reason {
            UnknownCause::UpstreamLost => "upstream_lost",
            UnknownCause::NoCommitTimestamp => "no_commit_timestamp",
            // The end signal carries only a bool. Read authority only for an
            // actual cancellation, never to infer the cause of transport loss.
            UnknownCause::SessionEnded => {
                if self.ctx.authority.session(self.who.session).is_some_and(|row| {
                    matches!(row.revoke_reason, Some(crate::state::model::EndReason::Expired | crate::state::model::EndReason::IdleTimeout))
                        || (row.revoke_reason.is_none() && row.expires_ms <= (self.ctx.clock)())
                }) { "expired" } else { "revoked" }
            }
        };
        self.record(reason);
        // Do not expose internal error sources to the downstream caller.
        Status::new(error.code(), error.message())
    }
    fn finish_error(&self, error: Status) -> Status {
        use std::error::Error;
        let reason = error.source().and_then(|source| source.downcast_ref::<UnknownCause>()).copied();
        match reason {
            Some(reason) => self.finish(error, reason),
            None => error,
        }
    }
}
// A typed, data-free cause travels with the internal status until the audit
// boundary. Every NY012 constructor must supply a cause.
#[derive(Clone, Copy, Debug)]
enum UnknownCause { UpstreamLost, SessionEnded, NoCommitTimestamp }
impl std::fmt::Display for UnknownCause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("write outcome unknown")
    }
}
impl std::error::Error for UnknownCause {}
fn unknown_status(reason: UnknownCause) -> Status {
    let mut error = nunya_status(NyCode::WriteOutcomeUnknown);
    error.set_source(Arc::new(reason));
    error
}
fn upstream_not_ready() -> Status {
    Status::unavailable(nunya_status(NyCode::ProtocolNotAllowed).message())
}
async fn ready_for_write(channel: &Channel, write: bool) -> Result<(), Status> {
    if write {
        tower::ServiceExt::ready(&mut channel.clone()).await.map_err(|_| upstream_not_ready())?;
    }
    Ok(())
}
// A wire-decoded Status has no io::Error source. A local TCP refusal proves
// no send, but only before any response part has been received.
fn connection_refused(error: &Status) -> bool {
    use std::error::Error;
    let mut source = error.source();
    while let Some(error) = source {
        if error.downcast_ref::<std::io::Error>().is_some_and(|e| e.kind() == std::io::ErrorKind::ConnectionRefused) {
            return true;
        }
        source = error.source();
    }
    false
}
fn forwarded_transport(error: Status, write: bool) -> Status {
    if write && matches!(error.code(), tonic::Code::Unavailable | tonic::Code::Unknown | tonic::Code::Cancelled | tonic::Code::DeadlineExceeded) {
        unknown_status(UnknownCause::UpstreamLost)
    } else {
        remote(error)
    }
}
fn ended_status(write: bool) -> Status {
    if write { unknown_status(UnknownCause::SessionEnded) }
    else { nunya_status(NyCode::SessionEnded) }
}
fn after_forward(error: Status, write: bool) -> Status {
    if write {
        let reason = if error.message() == nunya_status(NyCode::SessionEnded).message() {
            UnknownCause::SessionEnded
        } else { UnknownCause::UpstreamLost };
        unknown_status(reason)
    } else { error }
}
async fn ended(signal: &mut tokio::sync::watch::Receiver<bool>) {
    loop {
        if *signal.borrow_and_update() {
            return;
        }
        if signal.changed().await.is_err() {
            return;
        }
    }
}
async fn cancellable<T>(
    signal: &mut tokio::sync::watch::Receiver<bool>,
    write: bool,
    future: impl std::future::Future<Output = Result<T, Status>>,
) -> Result<T, Status> {
    tokio::select! {
        biased;
        _ = ended(signal) => Err(ended_status(write)),
        result = future => result.map_err(|error| forwarded_transport(error, write)),
    }
}
async fn cancellable_write<T>(
    signal: &mut tokio::sync::watch::Receiver<bool>,
    write: bool,
    marker: &DispatchMarker,
    future: impl std::future::Future<Output = Result<T, Status>>,
) -> Result<T, Status> {
    tokio::select! {
        biased;
        // Cancellation drops the RPC future, so an unset marker proves it
        // cannot have sent this write or dispatch it later.
        _ = ended(signal) => Err(ended_status(write && marker.dispatched())),
        result = future => result.map_err(|error| marker.classify(error, write)),
    }
}
// The receiver is discarded immediately on revocation, including queued results.
struct EndCheckedStream {
    receiver: Option<tokio::sync::mpsc::Receiver<Result<PartialResultSet, Status>>>,
    signal: tokio::sync::watch::Receiver<bool>,
    wake: Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
    write: bool,
    audit: Option<WriteAudit>,
}
impl EndCheckedStream {
    fn new(
        receiver: tokio::sync::mpsc::Receiver<Result<PartialResultSet, Status>>,
        signal: tokio::sync::watch::Receiver<bool>,
        write: bool,
    ) -> Self {
        let mut wake_signal = signal.clone();
        Self {
            receiver: Some(receiver),
            signal,
            wake: Box::pin(async move { ended(&mut wake_signal).await }),
            write,
            audit: None,
        }
    }
    fn error(&mut self, error: Status) -> Status {
        match self.audit.take() {
            Some(audit) => audit.finish_error(error),
            None => error,
        }
    }
}
impl Stream for EndCheckedStream {
    type Item = Result<PartialResultSet, Status>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.receiver.is_none() {
            return std::task::Poll::Ready(None);
        }
        if *this.signal.borrow() || this.wake.as_mut().poll(cx).is_ready() {
            this.receiver.take();
            return std::task::Poll::Ready(Some(Err(this.error(ended_status(this.write)))));
        }
        let result = match this.receiver.as_mut() {
            Some(receiver) => receiver.poll_recv(cx),
            None => return std::task::Poll::Ready(None),
        };
        if *this.signal.borrow() {
            this.receiver.take();
            return std::task::Poll::Ready(Some(Err(this.error(ended_status(this.write)))));
        }
        let result = match result {
            std::task::Poll::Ready(Some(Err(error))) => {
                this.receiver.take();
                std::task::Poll::Ready(Some(Err(this.error(error))))
            }
            other => other,
        };
        if matches!(result, std::task::Poll::Ready(None)) {
            this.receiver.take();
        }
        result
    }
}
fn refusal(code: NyCode) -> Refusal {
    Refusal::new(code, "Spanner request refused")
}
fn status(error: Refusal) -> Status {
    nunya_status(error.code)
}
fn invalid() -> Status {
    nunya_status(NyCode::NotAnalysable)
}
fn denied() -> Status {
    nunya_status(NyCode::StatementNotAllowed)
}
fn remote(error: Status) -> Status {
    redact_status(error.code())
}
fn admission_status(error: AdmitError) -> Status {
    nunya_status(match error {
        AdmitError::ReadOnly => NyCode::StatementNotAllowed,
        AdmitError::Unknown | AdmitError::Ended => NyCode::SessionEnded,
    })
}
async fn upstream_client(
    channel: &Channel,
    provider: &Option<Arc<dyn gcp_auth::TokenProvider>>,
) -> Result<Client, Status> {
    Ok(SpannerClient::with_interceptor(channel.clone(), upstream_interceptor(provider).await?))
}
async fn upstream_interceptor(
    provider: &Option<Arc<dyn gcp_auth::TokenProvider>>,
) -> Result<AuthInterceptor, Status> {
    if let Some(provider) = provider {
        let token = provider
            .token(&["https://www.googleapis.com/auth/spanner.data"])
            .await
            .map_err(|_| nunya_status(NyCode::TokenNotValid))?;
        let token = format!("Bearer {}", token.as_str())
            .parse()
            .map_err(|_| nunya_status(NyCode::TokenNotValid))?;
        Ok(AuthInterceptor { token: Some(token) })
    } else {
        Ok(AuthInterceptor::default())
    }
}
/// Connect to the configured upstream; downstream credentials never leave Nunya.
pub async fn build(ctx: Arc<SpannerContext>) -> Result<NunyaSpanner, Refusal> {
    let (channel, provider) = connect(&ctx.upstream).await?;
    Ok(NunyaSpanner {
        ctx,
        channel,
        sessions: Arc::new(Mutex::new(HashMap::new())),
        watchers: Arc::new(Mutex::new(HashMap::new())),
        transactions: Arc::new(Mutex::new(HashMap::new())),
        provider,
    })
}
/// Connect with gateway credentials and configured transport security.
pub(super) async fn connect(
    upstream: &SpannerUpstream,
) -> Result<(Channel, Option<Arc<dyn gcp_auth::TokenProvider>>), Refusal> {
    let (endpoint, provider) = match upstream {
        SpannerUpstream::Emulator { endpoint } => {
            if !endpoint.starts_with("http://") {
                return Err(refusal(NyCode::StatementNotAllowed));
            }
            (
                Endpoint::from_shared(endpoint.clone())
                    .map_err(|_| refusal(NyCode::StatementNotAllowed))?,
                None,
            )
        }
        SpannerUpstream::Google { endpoint } => {
            if !endpoint.starts_with("https://") {
                return Err(refusal(NyCode::StatementNotAllowed));
            }
            let ep = Endpoint::from_shared(endpoint.clone())
                .map_err(|_| refusal(NyCode::StatementNotAllowed))?
                .tls_config(ClientTlsConfig::new().with_webpki_roots())
                .map_err(|_| refusal(NyCode::StatementNotAllowed))?;
            let provider = gcp_auth::provider()
                .await
                .map_err(|_| refusal(NyCode::TokenNotValid))?;
            (ep, Some(provider))
        }
    };
    let channel = endpoint
        .connect()
        .await
        .map_err(|_| refusal(NyCode::StatementNotAllowed))?;
    Ok((channel, provider))
}
/// Run the downstream service with optional configured transport TLS.
pub async fn serve(
    addr: SocketAddr,
    svc: NunyaSpanner,
    tls: Option<crate::pgwire::context::ReloadableTls>,
) -> Result<(), Refusal> {
    let mut builder = tonic::transport::Server::builder();
    if let Some(tls) = tls {
        let listener = tokio::net::TcpListener::bind(addr).await
            .map_err(|_| refusal(NyCode::StatementNotAllowed))?;
        let incoming = tls_incoming(listener, tls);
        return builder.add_service(SpannerServer::new(svc))
            .serve_with_incoming(incoming).await
            .map_err(|_| refusal(NyCode::StatementNotAllowed));
    }
    builder
        .add_service(SpannerServer::new(svc))
        .serve(addr)
        .await
        .map_err(|_| refusal(NyCode::StatementNotAllowed))
}
const TLS_PENDING_CAP: usize = 1024;
const TLS_HANDSHAKE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);

// JoinSet owns handshake tasks, aborting unfinished work when the incoming stream is dropped.
fn tls_incoming(listener: tokio::net::TcpListener, tls: crate::pgwire::context::ReloadableTls)
    -> impl futures::Stream<Item = std::io::Result<tokio_rustls::server::TlsStream<tokio::net::TcpStream>>> {
    tls_incoming_with(listener, tls, TLS_PENDING_CAP, TLS_HANDSHAKE_TIMEOUT)
}
fn tls_incoming_with(
    listener: tokio::net::TcpListener,
    tls: crate::pgwire::context::ReloadableTls,
    cap: usize,
    timeout: std::time::Duration,
) -> impl futures::Stream<Item = std::io::Result<tokio_rustls::server::TlsStream<tokio::net::TcpStream>>> {
    let fifo = std::collections::VecDeque::<tokio::task::AbortHandle>::new();
    futures::stream::unfold((listener, tokio::task::JoinSet::new(), fifo, std::collections::VecDeque::new()), move |(listener, mut pending, mut fifo, mut ready)| {
        let tls = tls.clone();
        async move {
            loop {
                if let Some(stream) = ready.pop_front() {
                    return Some((Ok(stream), (listener, pending, fifo, ready)));
                }
                // Reap cancelled/completed tasks before accepting more work. Successful
                // handshakes are yielded, never discarded when clearing the pending set.
                fifo.retain(|handle| !handle.is_finished());
                while let Some(result) = pending.try_join_next() {
                    if let Ok(Some(stream)) = result {
                        return Some((Ok(stream), (listener, pending, fifo, ready)));
                    }
                }
                tokio::select! {
                    accepted = listener.accept() => {
                        let (tcp, _) = match accepted {
                            Ok(pair) => pair,
                            Err(error) => {
                                // Tonic continues polling after errors; back off to avoid spinning.
                                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                                return Some((Err(error), (listener, pending, fifo, ready)));
                            }
                        };
                        let config = tls.read().unwrap_or_else(std::sync::PoisonError::into_inner).clone();
                        if let Some(config) = config {
                            fifo.retain(|handle| !handle.is_finished());
                            if fifo.len() >= cap && let Some(oldest) = fifo.pop_front() {
                                oldest.abort();
                            }
                            // Aborted tasks still count in JoinSet until joined. Drain enough
                            // to enforce its cap too, preserving any completed TLS sessions.
                            while pending.len() >= cap {
                                if let Some(Ok(Some(stream))) = pending.join_next().await {
                                    ready.push_back(stream);
                                }
                            }
                            fifo.retain(|handle| !handle.is_finished());
                            fifo.push_back(pending.spawn(async move {
                                tokio::time::timeout(timeout,
                                    tokio_rustls::TlsAcceptor::from(config).accept(tcp)).await.ok()?.ok()
                            }));
                        }
                    }
                    result = pending.join_next(), if !pending.is_empty() => {
                        if let Some(Ok(Some(stream))) = result {
                            return Some((Ok(stream), (listener, pending, fifo, ready)));
                        }
                    }
                }
            }
        }
    })
}

impl NunyaSpanner {
    fn authenticate<T>(&self, req: &Request<T>) -> Result<Authenticated, Status> {
        auth::authenticate(
            req.metadata(),
            &self.ctx.authority,
            &self.ctx.route,
            &self.ctx.root_key,
        )
        .map_err(status)
    }
    fn database(&self, database: &str) -> Result<(), Status> {
        if database == self.ctx.database {
            Ok(())
        } else {
            Err(denied())
        }
    }
    fn session_database(&self, name: &str) -> Result<(), Status> {
        let suffix = name
            .strip_prefix(&self.ctx.database)
            .and_then(|v| v.strip_prefix("/sessions/"))
            .ok_or_else(denied)?;
        if suffix.is_empty() || suffix.contains('/') {
            Err(denied())
        } else {
            Ok(())
        }
    }
    fn session(&self, name: &str, who: &Authenticated) -> Result<(), Status> {
        self.session_database(name)?;
        let sessions = self.sessions.lock().map_err(|_| invalid())?;
        owned(&sessions, name, &who.session).map_err(status)
    }
    fn record_session(&self, name: &str, who: &Authenticated) -> Result<(), Status> {
        self.session_database(name)?;
        let mut sessions = self.sessions.lock().map_err(|_| invalid())?;
        if sessions.get(name).is_some_and(|p| p != &who.session) {
            return Err(nunya_status(NyCode::TokenNotValid));
        }
        let mut watchers = self.watchers.lock().map_err(|_| invalid())?;
        if let std::collections::hash_map::Entry::Vacant(entry) = watchers.entry(who.session) {
            let row = self
                .ctx
                .authority
                .session(who.session)
                .ok_or_else(|| nunya_status(NyCode::SessionEnded))?;
            let deadline = tokio::time::Instant::now()
                + Duration::from_millis(row.expires_ms.saturating_sub(crate::serve::now()));
            let (guard, mut end) = self.ctx.registry.register(
                who.session,
                who.principal,
                self.ctx.route.as_str(),
                Some(deadline),
            );
            // Register before rechecking admission so a concurrent revocation cannot be missed.
            self.ctx
                .authority
                .admit(who.session, Operation::Read)
                .map_err(admission_status)?;
            let (stop, stopped) = tokio::sync::oneshot::channel();
            let (ended_tx, ended) = tokio::sync::watch::channel(false);
            entry.insert(SessionWatcher { connection: guard.id().get(), stop, ended });
            let sessions = self.sessions.clone();
            let transactions = self.transactions.clone();
            let watchers = Arc::downgrade(&self.watchers);
            let channel = self.channel.clone();
            let provider = self.provider.clone();
            let session = who.session;
            let principal = who.principal;
            let ctx = self.ctx.clone();
            tokio::spawn(async move {
                let _guard = guard;
                let reason = tokio::select! {
                    biased;
                    _ = stopped => return,
                    reason = end.ended() => reason,
                };
                ended_tx.send_replace(true);
                if ctx.readlog.write_lifecycle(&LifecycleRecord {
                    kind: LifecycleKind::ConnectionEnded,
                    at_ms: (ctx.clock)(),
                    principal: principal.to_hex(),
                    session: Some(session.to_hex()),
                    route: Some(ctx.route.as_str().into()),
                    connection: Some(_guard.id().get()),
                    actor: LifecycleActor::System,
                    reason: match reason {
                        crate::state::registry::EndReason::Revoked => "revoked",
                        crate::state::registry::EndReason::Expired => "expired",
                    },
                }).is_err() {
                    eprintln!("audit: lifecycle record unavailable");
                }
                let cleanup = async {
                    let names = {
                        let mut sessions = sessions.lock().map_err(|_| invalid())?;
                        let names: Vec<_> = sessions
                            .iter()
                            .filter(|(_, owner)| **owner == session)
                            .map(|(name, _)| name.clone())
                            .collect();
                        for name in &names {
                            sessions.remove(name);
                        }
                        if let Some(watchers) = watchers.upgrade() {
                            watchers.lock().map_err(|_| invalid())?.remove(&session);
                        }
                        names
                    };
                    let tracked: Vec<_> = transactions.lock().map_err(|_| invalid())?
                        .iter().filter(|(_, txn)| txn.owner == session)
                        .map(|(key, _)| key.clone()).collect();
                    for (name, id) in &tracked {
                        best_effort_rollback(channel.clone(), provider.clone(), name.clone(), id.clone()).await;
                    }
                    transactions.lock().map_err(|_| invalid())?.retain(|_, txn| txn.owner != session);
                    for name in names {
                        let _ = tokio::time::timeout(Duration::from_secs(5), async {
                            upstream_client(&channel, &provider)
                                .await?
                                .delete_session(DeleteSessionRequest { name })
                                .await
                        })
                        .await;
                    }
                    Ok::<(), Status>(())
                };
                let _ = cleanup.await;
            });
        } else {
            self.ctx
                .authority
                .admit(who.session, Operation::Read)
                .map_err(admission_status)?;
        }
        sessions.insert(name.into(), who.session);
        Ok(())
    }
    fn end_signal(
        &self,
        who: &Authenticated,
    ) -> Result<tokio::sync::watch::Receiver<bool>, Status> {
        let signal = self
            .watchers
            .lock()
            .map_err(|_| ended_status(false))?
            .get(&who.session)
            .map(|watcher| watcher.ended.clone())
            .ok_or_else(|| ended_status(false))?;
        if *signal.borrow() {
            return Err(ended_status(false));
        }
        self.ctx
            .authority
            .admit(who.session, Operation::Read)
            .map_err(|_| ended_status(false))?;
        Ok(signal)
    }
    fn admit_write(&self, who: &Authenticated) -> Result<(), Status> {
        self.ctx
            .authority
            .admit(who.session, Operation::Write)
            .map(|_| ())
            .map_err(admission_status)
    }
    fn check_response(
        &self,
        who: &Authenticated,
        signal: &tokio::sync::watch::Receiver<bool>,
        write: bool,
    ) -> Result<(), Status> {
        if *signal.borrow()
            || self
                .ctx
                .authority
                .admit(who.session, Operation::Read)
                .is_err()
        {
            return Err(ended_status(write));
        }
        Ok(())
    }
    fn mark_dml(&self, session: &str, id: &[u8], owner: SessionId) -> Result<(), Status> {
        track_transaction(&self.transactions, owner, session, id, TxnState::Dml)
    }
    fn mark_selector_dml(
        &self,
        session: &str,
        selector: Option<&TransactionSelector>,
        owner: SessionId,
    ) -> Result<(), Status> {
        if let Some(transaction_selector::Selector::Id(id)) =
            selector.and_then(|s| s.selector.as_ref())
        {
            self.mark_dml(session, id, owner)?;
        }
        Ok(())
    }
    fn record_inline(
        &self,
        session: &str,
        owner: SessionId,
        inline: bool,
        write: bool,
        metadata: Option<&ResultSetMetadata>,
    ) -> Result<(), Status> {
        if inline && let Some(transaction) = metadata.and_then(|m| m.transaction.as_ref()) {
            track_transaction(
                &self.transactions,
                owner,
                session,
                &transaction.id,
                if write {
                    TxnState::Dml
                } else {
                    TxnState::Clean
                },
            )?;
        }
        Ok(())
    }
    fn refused_commit_rollback(&self, req: &CommitRequest) {
        if let Some(commit_request::Transaction::TransactionId(id)) = &req.transaction {
            tokio::spawn(best_effort_rollback(
                self.channel.clone(),
                self.provider.clone(),
                req.session.clone(),
                id.clone(),
            ));
        }
    }
    async fn delete_unrecorded(&self, name: &str) {
        let _ = tokio::time::timeout(Duration::from_secs(5), async {
            self.client()
                .await?
                .delete_session(DeleteSessionRequest { name: name.into() })
                .await
        })
        .await;
    }
    async fn client(&self) -> Result<Client, Status> {
        upstream_client(&self.channel, &self.provider).await
    }
    async fn write_client(&self) -> Result<(WriteClient, DispatchMarker), Status> {
        let marker = DispatchMarker::default();
        let service = DispatchService { inner: self.channel.clone(), marker: marker.clone() };
        Ok((SpannerClient::with_interceptor(service, upstream_interceptor(&self.provider).await?), marker))
    }
    fn configuration(&self) -> Result<ConfigurationSnapshot, Status> {
        // Lock order: policy, then revision; writers retain the policy write guard.
        let policy = self.ctx.policy.read().map_err(|_| invalid())?;
        let revision = self.ctx.policy_revision.read().map_err(|_| invalid())?.clone();
        let snapshot = self.ctx.snapshot.read().map_err(|_| invalid())?.clone();
        if (self.ctx.clock)().saturating_sub(snapshot.taken_at_unix_ms) > policy.snapshot_max_age_ms
        {
            return Err(nunya_status(NyCode::SchemaStale));
        }
        Ok((policy.clone(), snapshot, revision))
    }
    #[allow(clippy::too_many_arguments)]
    fn audit(
        ctx: &SpannerContext,
        watchers: &Mutex<HashMap<SessionId, SessionWatcher>>,
        who: &Authenticated,
        revision: Option<&str>,
        label: &str,
        rows: u64,
        tokens: Vec<String>,
    ) -> Result<(), Status> {
        Self::audit_outcome(ctx, watchers, who, revision, label, rows, tokens, Outcome::Allowed)
    }
    #[allow(clippy::too_many_arguments)]
    fn audit_outcome(
        ctx: &SpannerContext,
        watchers: &Mutex<HashMap<SessionId, SessionWatcher>>,
        who: &Authenticated,
        revision: Option<&str>,
        label: &str,
        rows: u64,
        tokens: Vec<String>,
        outcome: Outcome,
    ) -> Result<(), Status> {
        // Fingerprint the original agent statement; log no literals, tokens, or upstream values.
        ctx.readlog
            .write(&ReadLogRecord {
                ts_unix_ms: (ctx.clock)(),
                principal: who.principal,
                session: who.session,
                scope: who.scope,
                credential_id: Some(who.credential),
                connection: watchers.lock().map_err(|_| nunya_status(NyCode::ReadLogUnavailable))?.get(&who.session).map(|w| w.connection),
                route: ctx.route.as_str().into(),
                policy_revision: revision.map(str::to_owned),
                config_revision: None,
                statement_fingerprint: statement_fingerprint(
                    &crate::readlog::redact_tokenize_args(label),
                ),
                statement: "Spanner operation".into(),
                outcome,
                rows,
                tokens_released: tokens,
            })
            .map_err(|_| nunya_status(NyCode::ReadLogUnavailable))
    }
    fn refuse_tokenize(&self, who: &Authenticated, revision: Option<&str>, code: NyCode) -> Status {
        match Self::audit_outcome(
            &self.ctx, &self.watchers, who, revision, "Spanner nunya_tokenize", 0, vec![],
            Outcome::Refused(code),
        ) {
            Ok(()) => nunya_status(code),
            Err(error) => error,
        }
    }
    fn convert(
        &self,
        who: &Authenticated,
        policy: &Policy,
        domain: &DomainName,
        ty: &ColumnType,
        value: &Value,
        decode: bool,
    ) -> Result<Value, Status> {
        // Spanner has one STRING wire representation for all textual column types.
        let wire_ty = if ty.is_textual() { &ColumnType::Text } else { ty };
        let value = values::from_proto(wire_ty, value).map_err(status)?;
        let keys = &who.keys;
        let range = policy.domains.get(domain).and_then(|d| d.range.as_ref());
        let result = if decode {
            self.ctx.codec.decode(keys, domain, ty, range, &value)
        } else {
            self.ctx.codec.encode(keys, domain, ty, range, &value)
        }
        .map_err(status)?;
        values::to_proto(wire_ty, &result).map_err(status)
    }
    #[allow(clippy::too_many_arguments)]
    fn sql(
        &self,
        who: &Authenticated,
        revision: Option<&str>,
        sql: &str,
        params: &mut Option<prost_types::Struct>,
        policy: &Policy,
        snapshot: &Snapshot,
    ) -> Result<(RewritePlan, Vec<String>, StatementKind), Status> {
        let refusal = |error: Refusal| {
            if sql.to_ascii_lowercase().contains("nunya_tokenize") {
                self.refuse_tokenize(who, revision, error.code)
            } else {
                status(error)
            }
        };
        let (stmt, names) = lower_gsql(sql, snapshot, policy).map_err(refusal)?;
        let plan = check(&stmt, policy, snapshot).map_err(refusal)?;

        if plan.tokenize.is_none()
            && let Some(params) = params.as_ref()
        {
            for (name, value) in &params.fields {
                let consumed = plan.inputs.iter().any(|input| match input {
                    InputRewrite::DecryptParam { index, .. } => {
                        index.checked_sub(1).and_then(|i| names.get(i as usize)) == Some(name)
                    }
                    InputRewrite::DecryptLiteral { .. } => false,
                });
                if !consumed {
                    refuse_token_value(value, policy)?;
                }
            }
        }
        for input in &plan.inputs {
            match input {
                InputRewrite::DecryptParam { index, domain, ty } => {
                    let name = index
                        .checked_sub(1)
                        .and_then(|i| names.get(i as usize))
                        .ok_or_else(invalid)?;
                    let value = params
                        .as_mut()
                        .and_then(|p| p.fields.get_mut(name))
                        .ok_or_else(invalid)?;
                    *value = self.convert(who, policy, domain, ty, value, true)?;
                }
                // GoogleSQL literal rendering differs from PostgreSQL: refuse rather than interpolate.
                InputRewrite::DecryptLiteral { .. } => return Err(invalid()),
            }
        }

        Ok((plan, names, stmt.kind))
    }
    fn rewrite(
        ctx: &SpannerContext,
        who: &Authenticated,
        policy: &Policy,
        plan: &RewritePlan,
        types: &[ColumnType],
        row: &mut [Value],
    ) -> Result<Vec<String>, Status> {
        let keys = &who.keys;
        stream::rewrite_row(row, &plan.outputs, types, &mut |domain, ty, value| {
            ctx.codec.encode(
                keys,
                domain,
                ty,
                policy.domains.get(domain).and_then(|d| d.range.as_ref()),
                value,
            )
        })
        .map_err(status)
    }
    fn types(
        metadata: Option<&ResultSetMetadata>,
        plan: &RewritePlan,
    ) -> Result<Vec<ColumnType>, Status> {
        let fields = metadata
            .and_then(|m| m.row_type.as_ref())
            .map(|t| t.fields.as_slice())
            .unwrap_or(&[]);
        if fields.len() != plan.outputs.len() {
            return Err(invalid());
        }
        fields
            .iter()
            .map(|f| {
                f.r#type
                    .as_ref()
                    .map(values::column_type)
                    .ok_or_else(invalid)
            })
            .collect()
    }
    fn rows(
        &self,
        who: &Authenticated,
        policy: &Policy,
        plan: &RewritePlan,
        result: &mut ResultSet,
    ) -> Result<Vec<String>, Status> {
        let types = Self::types(result.metadata.as_ref(), plan)?;
        let mut tokens = vec![];
        for row in &mut result.rows {
            tokens.extend(Self::rewrite(
                &self.ctx,
                who,
                policy,
                plan,
                &types,
                &mut row.values,
            )?);
        }
        if let Some(stats) = &mut result.stats {
            stats.query_plan = None;
            stats.query_stats = None;
        }
        Ok(tokens)
    }
    #[allow(clippy::too_many_arguments)]
    fn local_tokenize(
        &self,
        who: &Authenticated,
        revision: Option<&str>,
        policy: &Policy,
        snapshot: &Snapshot,
        plan: &RewritePlan,
        names: &[String],
        req: &ExecuteSqlRequest,
    ) -> Result<Option<ResultSet>, Status> {
        let Some((domain, arg)) = &plan.tokenize else {
            return Ok(None);
        };
        self.local_tokenize_inner(who, policy, snapshot, (domain, arg), names, req)
            .map(Some)
            .map_err(|code| self.refuse_tokenize(who, revision, code))
    }
    fn local_tokenize_inner(
        &self,
        who: &Authenticated,
        policy: &Policy,
        snapshot: &Snapshot,
        tokenize: (&DomainName, &ValueSource),
        names: &[String],
        req: &ExecuteSqlRequest,
    ) -> Result<ResultSet, NyCode> {
        let (domain, arg) = tokenize;
        if !req.resume_token.is_empty() {
            return Err(NyCode::NotAnalysable);
        }
        let selector = match &req.transaction {
            None => None,
            Some(transaction) => Some(transaction.selector.as_ref().ok_or(NyCode::NotAnalysable)?),
        };
        match selector {
            None => {}
            Some(transaction_selector::Selector::SingleUse(options)) => match &options.mode {
                Some(transaction_options::Mode::ReadOnly(_)) => {}
                Some(transaction_options::Mode::ReadWrite(_))
                | Some(transaction_options::Mode::PartitionedDml(_))
                | None => return Err(NyCode::NotAnalysable),
            },
            Some(transaction_selector::Selector::Begin(_)) => return Err(NyCode::NotAnalysable),
            Some(transaction_selector::Selector::Id(id)) if id.is_empty() => {
                return Err(NyCode::NotAnalysable);
            }
            Some(transaction_selector::Selector::Id(_)) => {}
        }
        let setting = policy.domains.get(domain).ok_or(NyCode::StatementNotAllowed)?;
        let limits = match setting.tokenize {
            TokenizeSetting::On {
                limit_per_session,
                limit_per_principal,
                principal_window,
            } => TokenizeLimits {
                per_session: u64::from(limit_per_session),
                per_principal: u64::from(limit_per_principal),
                principal_window_ms: u64::try_from(principal_window.as_millis())
                    .unwrap_or(u64::MAX),
            },
            TokenizeSetting::Off => return Err(NyCode::StatementNotAllowed),
        };
        let ty = policy.columns.iter().find_map(|(column, treatment)| match treatment {
            Treatment::Blind { domain: d } if d == domain => {
                snapshot.column(column).map(|column| column.ty.clone())
            }
            _ => None,
        }).ok_or(NyCode::NotAnalysable)?;
        let code = tokenize_type_code(&ty)?;
        let value = match arg {
            ValueSource::Param(index) => {
                let name = index
                    .checked_sub(1)
                    .and_then(|i| names.get(i as usize))
                    .ok_or(NyCode::NotAnalysable)?;
                let value = req.params
                    .as_ref()
                    .and_then(|p| p.fields.get(name))
                    .ok_or(NyCode::NotAnalysable)?
                    .clone();
                if let Some(declared) = req.param_types.get(name) {
                    let declared = values::column_type(declared);
                    if declared != ty && !(declared.is_textual() && ty.is_textual()) {
                        return Err(NyCode::NotAnalysable);
                    }
                }
                value
            }
            ValueSource::Literal { text, .. } => Value {
                kind: Some(prost_types::value::Kind::StringValue(text.clone())),
            },
            ValueSource::Column { .. } | ValueSource::Null | ValueSource::Computed(_) => {
                return Err(NyCode::NotAnalysable);
            }
        };
        if matches!(value.kind, None | Some(prost_types::value::Kind::NullValue(_))) {
            return Err(NyCode::NotAnalysable);
        }
        let wire_ty = if ty.is_textual() { &ColumnType::Text } else { &ty };
        let value = values::from_proto(wire_ty, &value).map_err(|e| e.code)?;
        let value = self.ctx.codec.encode(
            &who.keys, domain, &ty, setting.range.as_ref(), &value,
        ).map_err(|e| e.code)?;
        let value = values::to_proto(wire_ty, &value).map_err(|e| e.code)?;
        self.ctx.authority.record_tokenization(who.session, domain, &limits)
            .map_err(tokenize_refusal)?;
        Ok(ResultSet {
            metadata: Some(ResultSetMetadata {
                row_type: Some(StructType {
                    fields: vec![struct_type::Field {
                        name: "nunya_tokenize".into(),
                        r#type: Some(Type {
                            code: code as i32,
                            ..Default::default()
                        }),
                    }],
                }),
                ..Default::default()
            }),
            rows: vec![ListValue {
                values: vec![value],
            }],
            ..Default::default()
        })
    }
    fn structured<'a>(
        &self,
        who: &Authenticated,
        policy: &Policy,
        plan: &RewritePlan,
        positions: &[ValuePosition],
        lookup: impl Fn(&ValuePosition) -> Option<&'a Value>,
    ) -> Result<Vec<(usize, Value)>, Status> {
        let mut result = vec![];
        for (i, pos) in positions.iter().enumerate() {
            let consumed = plan.inputs.iter().any(|input| {
                matches!(input,
                InputRewrite::DecryptParam { index, .. } if *index as usize == i + 1)
            });
            if !consumed {
                refuse_token_value(lookup(pos).ok_or_else(invalid)?, policy)?;
            }
        }
        for input in &plan.inputs {
            match input {
                InputRewrite::DecryptParam { index, domain, ty } => {
                    let index = index.checked_sub(1).ok_or_else(invalid)? as usize;
                    let pos = positions.get(index).ok_or_else(invalid)?;
                    let value = lookup(pos).ok_or_else(invalid)?;
                    result.push((index, self.convert(who, policy, domain, ty, value, true)?));
                }
                InputRewrite::DecryptLiteral { .. } => return Err(invalid()),
            }
        }
        Ok(result)
    }
    fn prepare_mutation(
        &self,
        who: &Authenticated,
        policy: &Policy,
        snapshot: &Snapshot,
        mutation: &mut Mutation,
    ) -> Result<(), Status> {
        let lowered =
            spanner_struct::lower_mutation(mutation, snapshot, policy).map_err(status)?;
        let plan = check(&lowered.stmt, policy, snapshot).map_err(status)?;
        if let Some(mutation::Operation::Delete(delete)) = &mutation.operation {
            check_key_tokens(
                delete.key_set.as_ref(),
                policy,
                &plan,
                &lowered.positions,
                true,
            )?;
        }
        let rewrites = self.structured(who, policy, &plan, &lowered.positions, |p| {
            mutation_value(mutation, p)
        })?;
        for (index, value) in rewrites {
            *mutation_value_mut(mutation, &lowered.positions[index]).ok_or_else(invalid)? =
                value;
        }
        Ok(())
    }
    #[allow(clippy::too_many_arguments)]
    fn stream(
        &self,
        who: Authenticated,
        policy: Arc<Policy>,
        revision: Option<String>,
        plan: RewritePlan,
        label: String,
        mut upstream: tonic::Streaming<PartialResultSet>,
        mut signal: tokio::sync::watch::Receiver<bool>,
        write: bool,
        mut inline_session: Option<String>,
        outcome_audit: WriteAudit,
    ) -> ResultStream {
        let ctx = self.ctx.clone();
        let watchers = self.watchers.clone();
        let transactions = self.transactions.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        let client_signal = signal.clone();
        tokio::spawn(async move {
            let mut count = 0;
            let mut tokens = vec![];
            let work = async {
                let mut reassembler = Reassembler::new();
                let mut precommit = stream::Precommit::default();
                let mut types = None;
                let mut received_part = false;
                loop {
                    let part = tokio::select! {
                        biased;
                        _ = ended(&mut signal) => return Err(ended_status(write)),
                        part = upstream.next() => part,
                    };
                    let Some(part) = part else {
                        break;
                    };
                    let mut part = part.map_err(|error| {
                        if write && !received_part && connection_refused(&error) {
                            upstream_not_ready()
                        } else {
                            forwarded_transport(error, write)
                        }
                    })?;
                    received_part = true;
                    ctx.authority
                        .admit(who.session, Operation::Read)
                        .map_err(|_| ended_status(write))?;
                    if let Some(session) = inline_session.as_ref()
                        && let Some(transaction) = part.metadata.as_ref().and_then(|m| m.transaction.as_ref()) {
                        track_transaction(&transactions, who.session, session, &transaction.id, if write { TxnState::Dml } else { TxnState::Clean })?;
                        inline_session = None;
                    }
                    precommit.observe(&mut part);
                    let (events, resume) =
                        reassembler.push(part).map_err(|e| after_forward(status(e), write))?;
                    let mut chunk_tokens = vec![];
                    let mut metadata = None;
                    let mut rows = vec![];
                    let mut stats = None;
                    for event in events {
                        match event {
                            Event::Metadata(m) => {
                                types = Some(Self::types(Some(&m), &plan).map_err(|e| after_forward(e, write))?);
                                metadata = Some(m);
                            }
                            Event::Row(mut row) => {
                                chunk_tokens.extend(Self::rewrite(
                                    &ctx,
                                    &who,
                                    &policy,
                                    &plan,
                                    types.as_deref().ok_or_else(|| after_forward(invalid(), write))?,
                                    &mut row,
                                ).map_err(|e| after_forward(e, write))?);
                                rows.push(row);
                            }
                            Event::Stats(mut s) => {
                                s.query_plan = None;
                                s.query_stats = None;
                                stats = Some(s);
                            }
                        }
                    }
                    if metadata.is_some() || !rows.is_empty() || stats.is_some() || resume.is_some()
                    {
                        let row_count = rows.len() as u64;
                        let mut output = stream::emit(metadata, rows, resume, stats);
                        precommit.attach(&mut output);
                        tokio::select! {
                            biased;
                            _ = ended(&mut signal) => return Err(ended_status(write)),
                            result = tx.send(Ok(output)) => {
                                result.map_err(|_| after_forward(Status::cancelled("stream closed"), write))?;
                            }
                        }
                        count += row_count;
                        tokens.extend(chunk_tokens);
                    }
                }
                reassembler.finish().map_err(|e| after_forward(status(e), write))?;
                if let Some(output) = precommit.finish() {
                    ctx.authority
                        .admit(who.session, Operation::Read)
                        .map_err(|_| ended_status(write))?;
                    tokio::select! {
                        biased;
                        _ = ended(&mut signal) => return Err(ended_status(write)),
                        result = tx.send(Ok(output)) => {
                            result.map_err(|_| after_forward(Status::cancelled("stream closed"), write))?;
                        }
                    }
                }
                Ok::<(), Status>(())
            }
            .await;
            drop(upstream);
            let audit =
                Self::audit(&ctx, &watchers, &who, revision.as_deref(), &label, count, tokens).map_err(|e| after_forward(e, write));
            // A failed audit after a forwarded write is indeterminate even if the
            // upstream also failed. For reads, preserve an NY011 cancellation.
            let work = if write {
                audit.and(work)
            } else {
                work.and(audit)
            };
            if let Err(error) = work {
                let _ = tx.send(Err(error)).await;
            }
        });
        let mut stream = EndCheckedStream::new(rx, client_signal, write);
        stream.audit = Some(outcome_audit);
        Box::pin(stream)
    }
}
fn owned(
    sessions: &HashMap<String, SessionId>,
    session: &str,
    access_session: &SessionId,
) -> Result<(), Refusal> {
    if sessions
        .get(session)
        .is_some_and(|owner| owner == access_session)
    {
        Ok(())
    } else {
        Err(refusal(NyCode::TokenNotValid))
    }
}

#[tonic::async_trait]
impl SpannerService for NunyaSpanner {
    type ExecuteStreamingSqlStream = ResultStream;
    type StreamingReadStream = ResultStream;
    type BatchWriteStream =
        Pin<Box<dyn Stream<Item = Result<BatchWriteResponse, Status>> + Send + 'static>>;
    async fn create_session(
        &self,
        req: Request<CreateSessionRequest>,
    ) -> Result<Response<Session>, Status> {
        let who = self.authenticate(&req)?;
        let mut req = req.into_inner();
        self.database(&req.database)?;
        if let Some(s) = &mut req.session {
            s.creator_role.clear();
            if !s.name.is_empty() {
                self.session_database(&s.name)?;
            }
        }
        let response = self
            .client()
            .await?
            .create_session(req)
            .await
            .map_err(remote)?
            .into_inner();
        if let Err(error) = self.record_session(&response.name, &who) {
            self.delete_unrecorded(&response.name).await;
            return Err(error);
        }
        Ok(Response::new(response))
    }
    async fn batch_create_sessions(
        &self,
        req: Request<BatchCreateSessionsRequest>,
    ) -> Result<Response<BatchCreateSessionsResponse>, Status> {
        let who = self.authenticate(&req)?;
        let mut req = req.into_inner();
        self.database(&req.database)?;
        if let Some(s) = &mut req.session_template {
            s.creator_role.clear();
            if !s.name.is_empty() {
                self.session_database(&s.name)?;
            }
        }
        let response = self
            .client()
            .await?
            .batch_create_sessions(req)
            .await
            .map_err(remote)?
            .into_inner();
        for (index, session) in response.session.iter().enumerate() {
            if let Err(error) = self.record_session(&session.name, &who) {
                for unrecorded in &response.session[index..] {
                    self.delete_unrecorded(&unrecorded.name).await;
                }
                return Err(error);
            }
        }
        Ok(Response::new(response))
    }
    async fn get_session(
        &self,
        req: Request<GetSessionRequest>,
    ) -> Result<Response<Session>, Status> {
        let who = self.authenticate(&req)?;
        let req = req.into_inner();
        self.session(&req.name, &who)?;
        let response = self
            .client()
            .await?
            .get_session(req.clone())
            .await
            .map_err(remote)?
            .into_inner();
        if response.name != req.name {
            return Err(invalid());
        }
        Ok(Response::new(response))
    }
    async fn list_sessions(
        &self,
        req: Request<ListSessionsRequest>,
    ) -> Result<Response<ListSessionsResponse>, Status> {
        let who = self.authenticate(&req)?;
        let req = req.into_inner();
        self.database(&req.database)?;
        let mut response = self
            .client()
            .await?
            .list_sessions(req)
            .await
            .map_err(remote)?
            .into_inner();
        response
            .sessions
            .retain(|s| self.session(&s.name, &who).is_ok());
        Ok(Response::new(response))
    }
    async fn delete_session(
        &self,
        req: Request<DeleteSessionRequest>,
    ) -> Result<Response<()>, Status> {
        let who = self.authenticate(&req)?;
        let req = req.into_inner();
        self.session(&req.name, &who)?;
        self.client()
            .await?
            .delete_session(req.clone())
            .await
            .map_err(remote)?;
        let mut sessions = self.sessions.lock().map_err(|_| invalid())?;
        let mut watchers = self.watchers.lock().map_err(|_| invalid())?;
        sessions.remove(&req.name);
        self.transactions.lock().map_err(|_| invalid())?.retain(|(name, _), _| name != &req.name);
        if !sessions.values().any(|owner| *owner == who.session)
            && let Some(stop) = watchers.remove(&who.session)
        {
            let _ = stop.stop.send(());
        }
        Ok(Response::new(()))
    }
    async fn execute_sql(
        &self,
        req: Request<ExecuteSqlRequest>,
    ) -> Result<Response<ResultSet>, Status> {
        let who = self.authenticate(&req)?;
        let mut req = req.into_inner();
        self.session(&req.session, &who)?;
        let mut signal = self.end_signal(&who)?;
        sql_features(&req)?;
        let (policy, snapshot, revision) = self.configuration()?;
        let label = req.sql.clone();
        let (plan, names, kind) = self.sql(&who, revision.as_deref(), &req.sql, &mut req.params, &policy, &snapshot)?;
        let write = matches!(
            kind,
            StatementKind::Insert | StatementKind::Update | StatementKind::Delete
        );
        match kind {
            StatementKind::Insert | StatementKind::Update | StatementKind::Delete => {
                self.mark_selector_dml(&req.session, req.transaction.as_ref(), who.session)?;
                self.admit_write(&who)?
            }
            StatementKind::Select | StatementKind::Tokenize { .. } => {}
            StatementKind::Begin
            | StatementKind::Commit
            | StatementKind::Rollback
            | StatementKind::Savepoint
            | StatementKind::ReleaseSavepoint
            | StatementKind::RollbackToSavepoint
            | StatementKind::SessionNoop { .. }
            | StatementKind::Refused(_) => return Err(denied()),
        }
        let label = if plan.tokenize.is_some() {
            "Spanner nunya_tokenize".to_owned()
        } else {
            label
        };
        if plan.tokenize.is_none() {
            Self::audit(&self.ctx, &self.watchers, &who, revision.as_deref(), &label, 0, vec![])?;
        }
        if let Some(result) =
            self.local_tokenize(&who, revision.as_deref(), &policy, &snapshot, &plan, &names, &req)?
        {
            Self::audit(&self.ctx, &self.watchers, &who, revision.as_deref(), &label, 1, local_tokens(&result)?)?;
            self.check_response(&who, &signal, false)?;
            return Ok(Response::new(result));
        }
        let (mut client, marker) = self.write_client().await?;
        ready_for_write(&self.channel, write).await?;
        let outcome_audit = WriteAudit::new(self, &who);
        let result = async {
            let inline = inline_read_write(req.transaction.as_ref());
            let session = req.session.clone();
            let mut result = cancellable_write(&mut signal, write, &marker, client.execute_sql(req))
                .await?
                .into_inner();
            self.record_inline(&session, who.session, inline, write, result.metadata.as_ref())?;
            let tokens = self
                .rows(&who, &policy, &plan, &mut result)
                .map_err(|e| after_forward(e, write))?;
            Self::audit(&self.ctx, &self.watchers, &who, revision.as_deref(), &label, result.rows.len() as u64, tokens)
                .map_err(|e| after_forward(e, write))?;
            self.check_response(&who, &signal, write)?;
            Ok(Response::new(result))
        }.await;
        result.map_err(|error| outcome_audit.finish_error(error))
    }
    async fn execute_streaming_sql(
        &self,
        req: Request<ExecuteSqlRequest>,
    ) -> Result<Response<Self::ExecuteStreamingSqlStream>, Status> {
        let who = self.authenticate(&req)?;
        let mut req = req.into_inner();
        self.session(&req.session, &who)?;
        let mut signal = self.end_signal(&who)?;
        sql_features(&req)?;
        let (policy, snapshot, revision) = self.configuration()?;
        let label = req.sql.clone();
        let (plan, names, kind) = self.sql(&who, revision.as_deref(), &req.sql, &mut req.params, &policy, &snapshot)?;
        let write = matches!(
            kind,
            StatementKind::Insert | StatementKind::Update | StatementKind::Delete
        );
        match kind {
            StatementKind::Insert | StatementKind::Update | StatementKind::Delete => {
                self.mark_selector_dml(&req.session, req.transaction.as_ref(), who.session)?;
                self.admit_write(&who)?
            }
            StatementKind::Select | StatementKind::Tokenize { .. } => {}
            StatementKind::Begin
            | StatementKind::Commit
            | StatementKind::Rollback
            | StatementKind::Savepoint
            | StatementKind::ReleaseSavepoint
            | StatementKind::RollbackToSavepoint
            | StatementKind::SessionNoop { .. }
            | StatementKind::Refused(_) => return Err(denied()),
        }
        let label = if plan.tokenize.is_some() {
            "Spanner nunya_tokenize".to_owned()
        } else {
            label
        };
        if plan.tokenize.is_none() {
            Self::audit(&self.ctx, &self.watchers, &who, revision.as_deref(), &label, 0, vec![])?;
        }
        if let Some(result) =
            self.local_tokenize(&who, revision.as_deref(), &policy, &snapshot, &plan, &names, &req)?
        {
            Self::audit(&self.ctx, &self.watchers, &who, revision.as_deref(), &label, 1, local_tokens(&result)?)?;
            self.check_response(&who, &signal, false)?;
            return Ok(Response::new(Box::pin(tokio_stream::iter(vec![Ok(
                PartialResultSet {
                    metadata: result.metadata,
                    values: result.rows.into_iter().flat_map(|r| r.values).collect(),
                    ..Default::default()
                },
            )]))));
        }
        let (mut client, marker) = self.write_client().await?;
        let inline = inline_read_write(req.transaction.as_ref());
        let session = req.session.clone();
        ready_for_write(&self.channel, write).await?;
        let outcome_audit = WriteAudit::new(self, &who);
        let upstream = cancellable_write(&mut signal, write, &marker, client.execute_streaming_sql(req))
            .await.map_err(|error| outcome_audit.finish_error(error))?
            .into_inner();
        Ok(Response::new(
            self.stream(who, policy, revision, plan, label, upstream, signal, write, inline.then_some(session), outcome_audit),
        ))
    }
    async fn execute_batch_dml(
        &self,
        req: Request<ExecuteBatchDmlRequest>,
    ) -> Result<Response<ExecuteBatchDmlResponse>, Status> {
        let who = self.authenticate(&req)?;
        let mut req = req.into_inner();
        self.session(&req.session, &who)?;
        let mut signal = self.end_signal(&who)?;
        let (policy, snapshot, revision) = self.configuration()?;
        let mut plans = vec![];
        let mut labels = vec![];
        for statement in &mut req.statements {
            labels.push(statement.sql.clone());
            let (plan, _, kind) = self.sql(
                &who,
                revision.as_deref(),
                &statement.sql,
                &mut statement.params,
                &policy,
                &snapshot,
            )?;
            match kind {
                StatementKind::Insert | StatementKind::Update | StatementKind::Delete => {}
                StatementKind::Tokenize { .. } => {
                    return Err(self.refuse_tokenize(&who, revision.as_deref(), NyCode::NotAnalysable));
                }
                StatementKind::Select
                | StatementKind::Begin
                | StatementKind::Commit
                | StatementKind::Rollback
                | StatementKind::Savepoint
                | StatementKind::ReleaseSavepoint
                | StatementKind::RollbackToSavepoint
                | StatementKind::SessionNoop { .. }
                | StatementKind::Refused(_) => return Err(denied()),
            }
            plans.push(plan);
        }
        self.mark_selector_dml(&req.session, req.transaction.as_ref(), who.session)?;
        self.admit_write(&who)?;
        for label in &labels {
            Self::audit(&self.ctx, &self.watchers, &who, revision.as_deref(), label, 0, vec![])?;
        }
        let (mut client, marker) = self.write_client().await?;
        ready_for_write(&self.channel, true).await?;
        let outcome_audit = WriteAudit::new(self, &who);
        let result = async {
            let inline = inline_read_write(req.transaction.as_ref());
            let session = req.session.clone();
            let mut result = cancellable_write(&mut signal, true, &marker, client.execute_batch_dml(req))
                .await?
                .into_inner();
            self.record_inline(&session, who.session, inline, true, result.result_sets.first().and_then(|set| set.metadata.as_ref()))?;
            if result.result_sets.len() > plans.len() {
                return Err(unknown_status(UnknownCause::UpstreamLost));
            }
            for (i, set) in result.result_sets.iter_mut().enumerate() {
                let tokens = self
                    .rows(&who, &policy, &plans[i], set)
                    .map_err(|e| after_forward(e, true))?;
                Self::audit(&self.ctx, &self.watchers, &who, revision.as_deref(), &labels[i], set.rows.len() as u64, tokens)
                    .map_err(|e| after_forward(e, true))?;
            }
            if let Some(s) = &mut result.status {
                s.message = if s.code == 0 {
                    String::new()
                } else {
                    redact_status(tonic::Code::from_i32(s.code))
                        .message()
                        .to_owned()
                };
                s.details.clear();
            }
            self.check_response(&who, &signal, true)?;
            Ok(Response::new(result))
        }.await;
        result.map_err(|error| outcome_audit.finish_error(error))
    }
    async fn read(&self, req: Request<ReadRequest>) -> Result<Response<ResultSet>, Status> {
        let who = self.authenticate(&req)?;
        let mut req = req.into_inner();
        self.session(&req.session, &who)?;
        let mut signal = self.end_signal(&who)?;
        read_features(&req)?;
        let (policy, snapshot, revision) = self.configuration()?;
        let lowered = spanner_struct::lower_read(&req, &snapshot, &policy).map_err(status)?;
        let plan = check(&lowered.stmt, &policy, &snapshot).map_err(status)?;
        check_key_tokens(
            req.key_set.as_ref(),
            &policy,
            &plan,
            &lowered.positions,
            false,
        )?;
        let rewrites = self.structured(&who, &policy, &plan, &lowered.positions, |p| {
            read_value(&req, p)
        })?;
        for (index, value) in rewrites {
            *read_value_mut(&mut req, &lowered.positions[index]).ok_or_else(invalid)? = value;
        }
        Self::audit(&self.ctx, &self.watchers, &who, revision.as_deref(), "Read", 0, vec![])?;
        let inline = inline_read_write(req.transaction.as_ref());
        let session = req.session.clone();
        let mut result = cancellable(&mut signal, false, self.client().await?.read(req))
            .await?
            .into_inner();
        self.record_inline(&session, who.session, inline, false, result.metadata.as_ref())?;
        let tokens = self.rows(&who, &policy, &plan, &mut result)?;
        Self::audit(&self.ctx, &self.watchers, &who, revision.as_deref(), "Read", result.rows.len() as u64, tokens)?;
        self.check_response(&who, &signal, false)?;
        Ok(Response::new(result))
    }
    async fn streaming_read(
        &self,
        req: Request<ReadRequest>,
    ) -> Result<Response<Self::StreamingReadStream>, Status> {
        let who = self.authenticate(&req)?;
        let mut req = req.into_inner();
        self.session(&req.session, &who)?;
        let mut signal = self.end_signal(&who)?;
        read_features(&req)?;
        let (policy, snapshot, revision) = self.configuration()?;
        let lowered = spanner_struct::lower_read(&req, &snapshot, &policy).map_err(status)?;
        let plan = check(&lowered.stmt, &policy, &snapshot).map_err(status)?;
        check_key_tokens(
            req.key_set.as_ref(),
            &policy,
            &plan,
            &lowered.positions,
            false,
        )?;
        let rewrites = self.structured(&who, &policy, &plan, &lowered.positions, |p| {
            read_value(&req, p)
        })?;
        for (index, value) in rewrites {
            *read_value_mut(&mut req, &lowered.positions[index]).ok_or_else(invalid)? = value;
        }
        Self::audit(&self.ctx, &self.watchers, &who, revision.as_deref(), "Read", 0, vec![])?;
        let inline = inline_read_write(req.transaction.as_ref());
        let session = req.session.clone();
        let upstream = cancellable(&mut signal, false, self.client().await?.streaming_read(req))
            .await?
            .into_inner();
        let outcome_audit = WriteAudit::new(self, &who);
        Ok(Response::new(self.stream(
            who,
            policy,
            revision,
            plan,
            "Read".into(),
            upstream,
            signal,
            false,
            inline.then_some(session),
            outcome_audit,
        )))
    }
    async fn begin_transaction(
        &self,
        req: Request<BeginTransactionRequest>,
    ) -> Result<Response<Transaction>, Status> {
        let who = self.authenticate(&req)?;
        let mut req = req.into_inner();
        self.session(&req.session, &who)?;
        let mut signal = self.end_signal(&who)?;
        if let Some(mutation) = &mut req.mutation_key {
            self.admit_write(&who)?;
            let (policy, snapshot, _) = self.configuration()?;
            self.prepare_mutation(&who, &policy, &snapshot, mutation)?;
        }
        let track = read_write(req.options.as_ref());
        let session = req.session.clone();
        let transaction = cancellable(
            &mut signal,
            false,
            self.client().await?.begin_transaction(req),
        )
        .await?
        .into_inner();
        if track {
            track_transaction(
                &self.transactions,
                who.session,
                &session,
                &transaction.id,
                TxnState::Clean,
            )?;
        }
        Ok(Response::new(transaction))
    }

    async fn rollback(&self, req: Request<RollbackRequest>) -> Result<Response<()>, Status> {
        let who = self.authenticate(&req)?;
        let req = req.into_inner();
        self.session(&req.session, &who)?;
        let mut signal = self.end_signal(&who)?;
        let mut client = self.client().await?;
        let key = (req.session.clone(), req.transaction_id.clone());
        let result = cancellable(&mut signal, false, client.rollback(req)).await;
        self.transactions
            .lock()
            .map_err(|_| invalid())?
            .remove(&key);
        result?;
        Ok(Response::new(()))
    }
    async fn commit(
        &self,
        req: Request<CommitRequest>,
    ) -> Result<Response<CommitResponse>, Status> {
        let who = self.authenticate(&req)?;
        let mut req = req.into_inner();
        self.session(&req.session, &who)?;
        let mut signal = self
            .end_signal(&who)
            .inspect_err(|_| self.refused_commit_rollback(&req))?;
        let (policy, snapshot, revision) = self.configuration()?;
        let key = match &req.transaction {
            Some(commit_request::Transaction::TransactionId(id)) => {
                Some((req.session.clone(), id.clone()))
            }
            Some(commit_request::Transaction::SingleUseTransaction(_)) => None,
            None => None,
        };
        let state = if let Some(key) = &key {
            self.transactions.lock().map_err(|_| invalid())?.remove(key)
        } else {
            None
        };
        let clean = match &req.transaction {
            Some(commit_request::Transaction::TransactionId(_)) => {
                state.is_some_and(|t| t.state == TxnState::Clean)
            }
            Some(commit_request::Transaction::SingleUseTransaction(_)) => true,
            None => false,
        };
        if !req.mutations.is_empty() || !clean {
            self.admit_write(&who)
                .inspect_err(|_| self.refused_commit_rollback(&req))?;
        }
        for mutation in &mut req.mutations {
            self.prepare_mutation(&who, &policy, &snapshot, mutation)?;
        }
        Self::audit(
            &self.ctx,
            &self.watchers,
            &who,
            revision.as_deref(),
            "Commit",
            0,
            vec![],
        )?;
        let (mut client, marker) = self.write_client().await?;
        ready_for_write(&self.channel, true).await?;
        let outcome_audit = WriteAudit::new(self, &who);
        let result = async {
            let result = cancellable_write(&mut signal, true, &marker, client.commit(req)).await;
            let retry = result.as_ref().is_ok_and(|response| {
                matches!(
                    response.get_ref().multiplexed_session_retry,
                    Some(commit_response::MultiplexedSessionRetry::PrecommitToken(_))
                )
            });
            if retry && let (Some((session, id)), Some(state)) = (key, state) {
                track_transaction(&self.transactions, state.owner, &session, &id, state.state)
                    .map_err(|e| after_forward(e, true))?;
            }
            let response = result?.into_inner();
            if !retry && response.commit_timestamp.is_none() {
                return Err(unknown_status(UnknownCause::NoCommitTimestamp));
            }
            Ok(Response::new(response))
        }.await;
        result.map_err(|error| outcome_audit.finish_error(error))
    }
    async fn partition_query(
        &self,
        req: Request<PartitionQueryRequest>,
    ) -> Result<Response<PartitionResponse>, Status> {
        let who = self.authenticate(&req)?;
        self.session(&req.get_ref().session, &who)?;
        Err(denied())
    }
    async fn partition_read(
        &self,
        req: Request<PartitionReadRequest>,
    ) -> Result<Response<PartitionResponse>, Status> {
        let who = self.authenticate(&req)?;
        self.session(&req.get_ref().session, &who)?;
        Err(denied())
    }
    async fn batch_write(
        &self,
        req: Request<BatchWriteRequest>,
    ) -> Result<Response<Self::BatchWriteStream>, Status> {
        let who = self.authenticate(&req)?;
        self.session(&req.get_ref().session, &who)?;
        self.admit_write(&who)?;
        Err(denied())
    }
}
fn check_key_tokens(
    set: Option<&googleapis_tonic_google_spanner_v1::google::spanner::v1::KeySet>,
    policy: &Policy,
    plan: &RewritePlan,
    positions: &[ValuePosition],
    delete: bool,
) -> Result<(), Status> {
    use googleapis_tonic_google_spanner_v1::google::spanner::v1::key_range::{EndKeyType, StartKeyType};
    let set = set.ok_or_else(invalid)?;
    for (key, values) in set.keys.iter().enumerate() {
        for (part, value) in values.values.iter().enumerate() {
            let pos = if delete {
                ValuePosition::DeleteKey { key, part }
            } else {
                ValuePosition::Key { key, part }
            };
            let consumed = plan.inputs.iter().any(|input| match input {
                InputRewrite::DecryptParam { index, .. } => {
                    index.checked_sub(1).and_then(|i| positions.get(i as usize)) == Some(&pos)
                }
                InputRewrite::DecryptLiteral { .. } => false,
            });
            if !consumed {
                refuse_token_value(value, policy)?;
            }
        }
    }
    for range in &set.ranges {
        let start = match &range.start_key_type {
            Some(StartKeyType::StartClosed(v) | StartKeyType::StartOpen(v)) => v,
            None => return Err(invalid()),
        };
        let end = match &range.end_key_type {
            Some(EndKeyType::EndClosed(v) | EndKeyType::EndOpen(v)) => v,
            None => return Err(invalid()),
        };
        for value in start.values.iter().chain(&end.values) {
            refuse_token_value(value, policy)?;
        }
    }
    Ok(())
}
fn refuse_token_value(value: &Value, policy: &Policy) -> Result<(), Status> {
    if let Some(prost_types::value::Kind::StringValue(text)) = &value.kind
        && crate::token::detect::looks_like_token(text, policy)
    {
        return Err(nunya_status(NyCode::TokenOutsideDomain));
    }
    Ok(())
}
fn local_tokens(result: &ResultSet) -> Result<Vec<String>, Status> {
    if result.rows.len() != 1 || result.rows[0].values.len() != 1 {
        return Err(invalid());
    }
    match &result.rows[0].values[0].kind {
        Some(prost_types::value::Kind::StringValue(value)) => Ok(vec![value.clone()]),
        Some(prost_types::value::Kind::NullValue(_)) => Ok(vec![]),
        _ => Err(invalid()),
    }
}
fn sql_features(req: &ExecuteSqlRequest) -> Result<(), Status> {
    if !req.partition_token.is_empty() || req.data_boost_enabled || req.query_mode != 0 {
        Err(denied())
    } else {
        Ok(())
    }
}
fn read_features(req: &ReadRequest) -> Result<(), Status> {
    if !req.partition_token.is_empty() || req.data_boost_enabled {
        Err(denied())
    } else {
        Ok(())
    }
}
fn read_value<'a>(req: &'a ReadRequest, pos: &ValuePosition) -> Option<&'a Value> {
    match pos {
        ValuePosition::Key { key, part } => req.key_set.as_ref()?.keys.get(*key)?.values.get(*part),
        ValuePosition::MutationValue { .. } | ValuePosition::DeleteKey { .. } => None,
    }
}
fn read_value_mut<'a>(req: &'a mut ReadRequest, pos: &ValuePosition) -> Option<&'a mut Value> {
    match pos {
        ValuePosition::Key { key, part } => req
            .key_set
            .as_mut()?
            .keys
            .get_mut(*key)?
            .values
            .get_mut(*part),
        ValuePosition::MutationValue { .. } | ValuePosition::DeleteKey { .. } => None,
    }
}
fn mutation_value<'a>(m: &'a Mutation, pos: &ValuePosition) -> Option<&'a Value> {
    match (m.operation.as_ref()?, pos) {
        (
            mutation::Operation::Insert(w)
            | mutation::Operation::Update(w)
            | mutation::Operation::InsertOrUpdate(w)
            | mutation::Operation::Replace(w),
            ValuePosition::MutationValue { row, column },
        ) => w.values.get(*row)?.values.get(*column),
        (mutation::Operation::Delete(d), ValuePosition::DeleteKey { key, part }) => {
            d.key_set.as_ref()?.keys.get(*key)?.values.get(*part)
        }
        _ => None,
    }
}
fn mutation_value_mut<'a>(m: &'a mut Mutation, pos: &ValuePosition) -> Option<&'a mut Value> {
    match (m.operation.as_mut()?, pos) {
        (
            mutation::Operation::Insert(w)
            | mutation::Operation::Update(w)
            | mutation::Operation::InsertOrUpdate(w)
            | mutation::Operation::Replace(w),
            ValuePosition::MutationValue { row, column },
        ) => w.values.get_mut(*row)?.values.get_mut(*column),
        (mutation::Operation::Delete(d), ValuePosition::DeleteKey { key, part }) => d
            .key_set
            .as_mut()?
            .keys
            .get_mut(*key)?
            .values
            .get_mut(*part),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        pgwire::auth::ScramVerifier,
        state::{
            authority::AcquireRequest,
            model::{Mode, PrincipalKind, Verifier},
        },
        token::PrincipalId,
    };
    use crate::{
        policy::{DomainSettings, ReadLogSink, Treatment},
        readlog::StdoutLog,
        schema::{Column, ColumnRef, Relation, RelationKind},
    };
    use sha2::{Digest, Sha256};
    use std::collections::BTreeMap;
    #[test]
    fn session_ownership_is_exact() {
        let sessions = HashMap::from([
            ("db/sessions/a".into(), SessionId([1; 16])),
            ("db/sessions/b".into(), SessionId([2; 16])),
        ]);
        assert!(owned(&sessions, "db/sessions/a", &SessionId([1; 16])).is_ok());
        assert_eq!(
            owned(&sessions, "db/sessions/a", &SessionId([2; 16]))
                .unwrap_err()
                .code,
            NyCode::TokenNotValid
        );
        assert_eq!(
            owned(&sessions, "db/sessions/missing", &SessionId([1; 16]))
                .unwrap_err()
                .code,
            NyCode::TokenNotValid
        );
        assert_eq!(
            owned(&sessions, "db/sessions/a/extra", &SessionId([1; 16]))
                .unwrap_err()
                .code,
            NyCode::TokenNotValid
        );
    }
    #[test]
    fn deleted_session_is_not_owned() {
        let mut sessions = HashMap::from([("db/sessions/a".into(), SessionId([1; 16]))]);
        sessions.remove("db/sessions/a");
        assert!(owned(&sessions, "db/sessions/a", &SessionId([1; 16])).is_err());
    }
    #[test]
    fn local_token_audit_keeps_tokens_and_omits_nulls() {
        let result = |value| ResultSet {
            rows: vec![ListValue {
                values: vec![value],
            }],
            ..Default::default()
        };
        assert_eq!(
            local_tokens(&result(text("tok_email_test"))).unwrap(),
            vec!["tok_email_test"]
        );
        assert!(
            local_tokens(&result(Value {
                kind: Some(prost_types::value::Kind::NullValue(0))
            }))
            .unwrap()
            .is_empty()
        );
    }
    include!("proxy/transport_tests.rs");
    #[derive(Default)]
    struct Fake {
        result_token: Mutex<Option<MultiplexedSessionPrecommitToken>>,
        next_commit: Mutex<Option<CommitResponse>>,
        commit_status: Mutex<Option<Status>>,
        sql_status: Mutex<Option<Status>>,
        stream_error_after_first: Mutex<Option<Status>>,
        commit_error: Mutex<Option<tonic::Code>>,
        sql_error: Mutex<Option<tonic::Code>>,
        begins: Mutex<Vec<BeginTransactionRequest>>,
        rollbacks: Mutex<Vec<(String, Vec<u8>)>>,
        next_transaction: std::sync::atomic::AtomicUsize,
        hold_rollback: std::sync::atomic::AtomicBool,
        parts: Mutex<Option<Vec<PartialResultSet>>>,
        seen: Mutex<Vec<ExecuteSqlRequest>>,
        commits: Mutex<Vec<CommitRequest>>,
        deleted: Mutex<Vec<String>>,
        batches: Mutex<Vec<ExecuteBatchDmlRequest>>,
        streaming: std::sync::atomic::AtomicUsize,
        next_session: std::sync::atomic::AtomicUsize,
        roles: Mutex<Vec<String>>,
        failure: std::sync::atomic::AtomicBool,
        hold_sql: std::sync::atomic::AtomicBool,
        hold_commit: std::sync::atomic::AtomicBool,
        hold_stream: std::sync::atomic::AtomicBool,
        release: tokio::sync::Notify,
        burst_stream: std::sync::atomic::AtomicBool,
        emitted: Arc<std::sync::atomic::AtomicUsize>,
        wrong_database: std::sync::atomic::AtomicBool,
    }
    impl Fake {
        fn transaction(&self) -> Transaction {
            Transaction {
                id: self
                    .next_transaction
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                    .to_be_bytes()
                    .to_vec(),
                ..Default::default()
            }
        }
        fn inline_transaction(&self, selector: Option<&TransactionSelector>) -> Option<Transaction> {
            inline_read_write(selector).then(|| self.transaction())
        }
    }
    #[tonic::async_trait]
    impl SpannerService for Fake {
        type ExecuteStreamingSqlStream = ResultStream;
        type StreamingReadStream = ResultStream;
        type BatchWriteStream =
            Pin<Box<dyn Stream<Item = Result<BatchWriteResponse, Status>> + Send + 'static>>;
        async fn create_session(
            &self,
            req: Request<CreateSessionRequest>,
        ) -> Result<Response<Session>, Status> {
            if self.failure.load(std::sync::atomic::Ordering::Relaxed) {
                return Err(Status::already_exists("private@example.com"));
            }
            let req = req.into_inner();
            self.roles
                .lock()
                .unwrap()
                .push(req.session.unwrap_or_default().creator_role);
            Ok(Response::new(Session {
                name: format!(
                    "{}/sessions/fake-{}",
                    if self
                        .wrong_database
                        .load(std::sync::atomic::Ordering::Relaxed)
                    {
                        "projects/p/instances/i/databases/other".into()
                    } else {
                        req.database
                    },
                    self.next_session
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                ),
                ..Default::default()
            }))
        }
        async fn batch_create_sessions(
            &self,
            req: Request<BatchCreateSessionsRequest>,
        ) -> Result<Response<BatchCreateSessionsResponse>, Status> {
            let req = req.into_inner();
            self.roles
                .lock()
                .unwrap()
                .push(req.session_template.unwrap_or_default().creator_role);
            Ok(Response::new(BatchCreateSessionsResponse {
                session: if self
                    .wrong_database
                    .load(std::sync::atomic::Ordering::Relaxed)
                {
                    vec![
                        Session {
                            name: format!("{}/sessions/recorded", req.database),
                            ..Default::default()
                        },
                        Session {
                            name: "projects/p/instances/i/databases/other/sessions/bad".into(),
                            ..Default::default()
                        },
                        Session {
                            name: format!("{}/sessions/unrecorded", req.database),
                            ..Default::default()
                        },
                    ]
                } else {
                    vec![Session {
                        name: format!("{}/sessions/batch", req.database),
                        ..Default::default()
                    }]
                },
            }))
        }
        async fn execute_streaming_sql(
            &self,
            req: Request<ExecuteSqlRequest>,
        ) -> Result<Response<ResultStream>, Status> {
            if let Some(error) = self.stream_error_after_first.lock().unwrap().take() {
                return Ok(Response::new(Box::pin(futures::stream::iter([
                    Ok(PartialResultSet {
                        metadata: Some(ResultSetMetadata::default()),
                        stats: Some(ResultSetStats::default()), ..Default::default()
                    }),
                    Err(error),
                ]))));
            }
            if let Some(parts) = self.parts.lock().unwrap().take() {
                return Ok(Response::new(Box::pin(futures::stream::iter(parts.into_iter().map(Ok)))));
            }
            self.streaming
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let transaction = self.inline_transaction(req.get_ref().transaction.as_ref());
            let hold = self.hold_stream.load(std::sync::atomic::Ordering::Relaxed);
            let burst = self.burst_stream.load(std::sync::atomic::Ordering::Relaxed);
            if hold || burst {
                let mut first = PartialResultSet {
                    metadata: Some(ResultSetMetadata {
                        transaction,
                        row_type: Some(StructType {
                            fields: vec![struct_type::Field {
                                name: "email".into(),
                                r#type: Some(Type {
                                    code: TypeCode::String as i32,
                                    ..Default::default()
                                }),
                            }],
                        }),
                        ..Default::default()
                    }),
                    values: vec![text("private@example.com")],
                    ..Default::default()
                };
                if burst {
                    if req.get_ref().sql.starts_with("UPDATE ") {
                        first = PartialResultSet {
                            metadata: Some(ResultSetMetadata::default()),
                            stats: Some(ResultSetStats::default()),
                            ..Default::default()
                        };
                    }
                    let emitted = self.emitted.clone();
                    return Ok(Response::new(Box::pin(
                        futures::stream::iter((0..20).map(move |index| {
                            let mut part = first.clone();
                            if index != 0 {
                                part.metadata = None;
                            }
                            Ok(part)
                        }))
                        .inspect(move |_| {
                            emitted.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }),
                    )));
                }
                return Ok(Response::new(Box::pin(
                    futures::stream::iter([Ok(first)]).chain(futures::stream::pending()),
                )));
            }
            if req.get_ref().sql.starts_with("UPDATE ") {
                return Ok(Response::new(Box::pin(futures::stream::iter([Ok(PartialResultSet {
                    metadata: Some(ResultSetMetadata { transaction, ..Default::default() }),
                    stats: Some(ResultSetStats::default()), ..Default::default()
                })]))));
            }
            Ok(Response::new(Box::pin(futures::stream::iter(vec![
                Ok(PartialResultSet {
                    metadata: Some(ResultSetMetadata {
                        transaction,
                        row_type: Some(StructType {
                            fields: vec![struct_type::Field {
                                name: "email".into(),
                                r#type: Some(Type {
                                    code: TypeCode::String as i32,
                                    ..Default::default()
                                }),
                            }],
                        }),
                        ..Default::default()
                    }),
                    values: vec![text("private@")],
                    chunked_value: true,
                    ..Default::default()
                }),
                Ok(PartialResultSet {
                    values: vec![text("example.com")],
                    ..Default::default()
                }),
            ]))))
        }
        async fn delete_session(
            &self,
            req: Request<DeleteSessionRequest>,
        ) -> Result<Response<()>, Status> {
            self.deleted.lock().unwrap().push(req.into_inner().name);
            Ok(Response::new(()))
        }
        async fn execute_sql(
            &self,
            req: Request<ExecuteSqlRequest>,
        ) -> Result<Response<ResultSet>, Status> {
            let req = req.into_inner();
            let transaction = self.inline_transaction(req.transaction.as_ref());
            let dml = req.sql.starts_with("UPDATE ")
                || req.sql.starts_with("INSERT ")
                || req.sql.starts_with("DELETE ");
            self.seen.lock().unwrap().push(req);
            if self.hold_sql.load(std::sync::atomic::Ordering::Relaxed) {
                self.release.notified().await;
            }
            if let Some(error) = self.sql_status.lock().unwrap().take() {
                return Err(error);
            }
            if let Some(code) = self.sql_error.lock().unwrap().take() {
                return Err(Status::new(code, "upstream error"));
            }
            if dml {
                return Ok(Response::new(ResultSet { metadata: transaction.map(|transaction| ResultSetMetadata { transaction: Some(transaction), ..Default::default() }), precommit_token: self.result_token.lock().unwrap().clone(), ..Default::default() }));
            }
            Ok(Response::new(ResultSet {
                metadata: Some(ResultSetMetadata {
                    transaction,
                    row_type: Some(StructType {
                        fields: vec![struct_type::Field {
                            name: "email".into(),
                            r#type: Some(Type {
                                code: TypeCode::String as i32,
                                ..Default::default()
                            }),
                        }],
                    }),
                    ..Default::default()
                }),
                precommit_token: self.result_token.lock().unwrap().clone(),
                rows: vec![ListValue {
                    values: vec![text("private@example.com")],
                }],
                ..Default::default()
            }))
        }
        async fn execute_batch_dml(
            &self,
            req: Request<ExecuteBatchDmlRequest>,
        ) -> Result<Response<ExecuteBatchDmlResponse>, Status> {
            let req = req.into_inner();
            let transaction = self.inline_transaction(req.transaction.as_ref());
            self.batches.lock().unwrap().push(req);
            Ok(Response::new(ExecuteBatchDmlResponse {
                result_sets: transaction.map(|transaction| ResultSet {
                    metadata: Some(ResultSetMetadata { transaction: Some(transaction), ..Default::default() }), ..Default::default()
                }).into_iter().collect(),
                precommit_token: self.result_token.lock().unwrap().clone(), ..Default::default()
            }))
        }
        async fn begin_transaction(&self, req: Request<BeginTransactionRequest>) -> Result<Response<Transaction>, Status> {
            self.begins.lock().unwrap().push(req.into_inner());
            Ok(Response::new(Transaction { precommit_token: self.result_token.lock().unwrap().clone(), ..self.transaction() }))
        }
        async fn read(&self, req: Request<ReadRequest>) -> Result<Response<ResultSet>, Status> {
            self.execute_sql(Request::new(ExecuteSqlRequest { transaction: req.into_inner().transaction, ..Default::default() })).await
        }
        async fn streaming_read(&self, req: Request<ReadRequest>) -> Result<Response<ResultStream>, Status> {
            self.execute_streaming_sql(Request::new(ExecuteSqlRequest { transaction: req.into_inner().transaction, ..Default::default() })).await
        }
        async fn rollback(&self, req: Request<RollbackRequest>) -> Result<Response<()>, Status> {
            let req = req.into_inner();
            self.rollbacks.lock().unwrap().push((req.session, req.transaction_id));
            if self.hold_rollback.load(std::sync::atomic::Ordering::Relaxed) {
                std::future::pending::<()>().await;
            }
            Ok(Response::new(()))
        }
        async fn commit(
            &self,
            req: Request<CommitRequest>,
        ) -> Result<Response<CommitResponse>, Status> {
            self.commits.lock().unwrap().push(req.into_inner());
            if self.hold_commit.load(std::sync::atomic::Ordering::Relaxed) {
                self.release.notified().await;
            }
            if let Some(error) = self.commit_status.lock().unwrap().take() {
                return Err(error);
            }
            if let Some(code) = self.commit_error.lock().unwrap().take() {
                return Err(Status::new(code, "upstream error"));
            }
            Ok(Response::new(self.next_commit.lock().unwrap().take().unwrap_or(CommitResponse {
                commit_timestamp: Some(prost_types::Timestamp { seconds: 1, nanos: 0 }),
                ..Default::default()
            })))
        }
    }
    fn text(s: &str) -> Value {
        Value {
            kind: Some(prost_types::value::Kind::StringValue(s.into())),
        }
    }
    fn domain() -> DomainName {
        DomainName("email".into())
    }
    fn session_request<T>(value: T, id: SessionId, secret: &str) -> Request<T> {
        let mut req = Request::new(value);
        req.metadata_mut().insert(
            "authorization",
            format!("Bearer nunya.session:{}.{}", id.to_hex(), secret)
                .parse()
                .unwrap(),
        );
        req
    }
    fn acquire(a: &crate::state::authority::Authority, principal: PrincipalId) -> SessionId {
        a.acquire_session(
            principal,
            AcquireRequest {
                route: crate::state::model::RouteId::new("spanner-native").unwrap(),
                mode: None,
                lifetime_ms: None,
                isolated: false,
                label: None,
                idempotency_key: SessionId::random().to_hex(),
                verifier: Verifier(ScramVerifier::from_password("password", b"salt", 2)),
                secret_sha256: Sha256::digest(b"password").into(),
            },
        )
        .unwrap()
        .id
    }
    fn provision(a: &crate::state::authority::Authority, name: &str) -> SessionId {
        let p = a
            .create_principal(name, PrincipalKind::Agent, false, name)
            .unwrap();
        a.set_grants(
            p.id,
            vec![(
                crate::state::model::RouteId::new("spanner-native").unwrap(),
                Mode::ReadWrite,
            )],
        )
        .unwrap();
        a.add_principal_credential(
            p.id,
            Verifier(ScramVerifier::from_password("password", b"salt", 2)),
            None,
        )
        .unwrap();
        acquire(a, p.id)
    }
    struct Fixture {
        svc: NunyaSpanner,
        fake: Arc<Fake>,
        server: tokio::task::JoinHandle<()>,
        dir: std::path::PathBuf,
        alice: SessionId,
        bob: SessionId,
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            self.server.abort();
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }
    impl Fixture {
        fn request<T>(&self, value: T, user: &str) -> Request<T> {
            let id = match user {
                "alice" => self.alice,
                "bob" => self.bob,
                _ => panic!("unknown fixture identity"),
            };
            session_request(value, id, "password")
        }
        async fn new() -> Self {
            Self::with_log(Box::new(StdoutLog)).await
        }
        async fn with_log(readlog: Box<dyn ReadLog>) -> Self {
            let fake = Arc::new(Fake::default());
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let incoming = futures::stream::unfold(listener, |listener| async move {
                let result = listener.accept().await.map(|(socket, _)| socket);
                Some((result, listener))
            });
            let upstream = fake.clone();
            let server = tokio::spawn(async move {
                tonic::transport::Server::builder()
                    .add_service(SpannerServer::from_arc(upstream))
                    .serve_with_incoming(incoming)
                    .await
                    .unwrap();
            });
            let dir = std::env::temp_dir().join(format!(
                "spanner-proxy-{}-{}",
                std::process::id(),
                addr.port()
            ));
            let col = ColumnRef {
                schema: "".into(),
                table: "customers".into(),
                column: "email".into(),
            };
            let policy = Policy {
                columns: BTreeMap::from([(col, Treatment::Blind { domain: domain() })]),
                domains: BTreeMap::from([(
                    domain(),
                    DomainSettings {
                        tokenize: TokenizeSetting::On {
                            limit_per_session: 10,
                            limit_per_principal: 100,
                            principal_window: std::time::Duration::from_secs(86400),
                        },
                        range: None,
                    },
                )]),
                snapshot_max_age_ms: 1000,
                readlog: ReadLogSink::Stdout,
            };
            let snapshot = Snapshot::new(
                vec![Relation {
                    schema: "".into(),
                    name: "customers".into(),
                    kind: RelationKind::Table,
                    columns: vec![Column {
                        name: "email".into(),
                        ty: ColumnType::Text,
                        nullable: true,
                        generated: None,
                        default: None,
                    }],
                    view_definition: None,
                    primary_key: vec!["email".into()],
                }],
                vec![],
                vec!["".into()],
                0,
            );
            let runtime = crate::state::runtime::StateRuntime::open(
                &dir.join("state"),
                &RootKey::from_hex(&"00".repeat(32)).unwrap(),
                &crate::config::SessionSettings::default(),
                &[],
            )
            .unwrap();
            let ctx = Arc::new(SpannerContext {
                authority: runtime.authority.clone(),
                registry: runtime.registry.clone(),
                route: crate::state::model::RouteId::new("spanner-native").unwrap(),
                policy: RwLock::new(Arc::new(policy)),
                policy_revision: RwLock::new(None),
                snapshot: RwLock::new(Arc::new(snapshot)),
                root_key: RootKey::from_hex(&"00".repeat(32)).unwrap(),
                readlog,
                codec: StandardCodec,
                database: "projects/p/instances/i/databases/d".into(),
                clock: Arc::new(|| 0),
                upstream: SpannerUpstream::Emulator {
                    endpoint: format!("http://{addr}"),
                },
            });
            let alice = provision(&runtime.authority, "alice");
            let bob = provision(&runtime.authority, "bob");
            let svc = build(ctx).await.unwrap();
            Self {
                svc,
                alice,
                bob,
                fake,
                server,
                dir,
            }
        }
        async fn session(&self) -> String {
            self.svc
                .create_session(self.request(
                    CreateSessionRequest {
                        database: self.svc.ctx.database.clone(),
                        ..Default::default()
                    },
                    "alice",
                ))
                .await
                .unwrap()
                .into_inner()
                .name
        }
        async fn query(&self, sql: &str) -> ExecuteSqlRequest {
            ExecuteSqlRequest {
                session: self.session().await,
                sql: sql.into(),
                ..Default::default()
            }
        }
        fn token(&self) -> Value {
            let auth = self.svc.authenticate(&self.request((), "alice")).unwrap();
            let policy = self.svc.ctx.policy.read().unwrap().clone();
            self.svc
                .convert(
                    &auth,
                    &policy,
                    &domain(),
                    &ColumnType::Text,
                    &text("private@example.com"),
                    false,
                )
                .unwrap()
        }
    }
    struct CaptureLog(Arc<Mutex<Vec<ReadLogRecord>>>, Arc<Mutex<Vec<LifecycleRecord>>>);
    impl ReadLog for CaptureLog {
        fn write_lifecycle(&self, record: &LifecycleRecord) -> Result<(), Refusal> {
            self.1.lock().unwrap().push(record.clone());
            Ok(())
        }
        fn write(&self, record: &ReadLogRecord) -> Result<(), Refusal> {
            self.0.lock().unwrap().push(record.clone());
            Ok(())
        }
    }
    #[tokio::test]
    async fn statement_snapshot_retains_revision_across_reload() {
        let records = Arc::new(Mutex::new(vec![]));
        let f = Fixture::with_log(Box::new(CaptureLog(
            records.clone(), Arc::new(Mutex::new(vec![])),
        ))).await;
        let install = |revision: &str| {
            let mut policy = f.svc.ctx.policy.write().unwrap();
            *f.svc.ctx.policy_revision.write().unwrap() = Some(revision.into());
            *policy = Arc::new((**policy).clone());
        };
        install("R1");
        let req = f.query("SELECT email FROM customers").await;
        f.fake.hold_sql.store(true, std::sync::atomic::Ordering::Relaxed);
        let rpc = f.svc.execute_sql(f.request(req, "alice"));
        let reload = async {
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                while f.fake.seen.lock().unwrap().is_empty() {
                    tokio::task::yield_now().await;
                }
            }).await.unwrap();
            install("R2");
            f.fake.release.notify_one();
        };
        let (result, ()) = tokio::join!(rpc, reload);
        result.unwrap();
        {
            let records = records.lock().unwrap();
            assert_eq!(records.len(), 2);
            assert!(records.iter().all(|r| r.policy_revision.as_deref() == Some("R1")));
        }
        records.lock().unwrap().clear();
        f.fake.hold_sql.store(false, std::sync::atomic::Ordering::Relaxed);
        let req = f.query("SELECT email FROM customers").await;
        f.svc.execute_sql(f.request(req, "alice")).await.unwrap();
        {
            let records = records.lock().unwrap();
            assert_eq!(records.len(), 2);
            assert!(records.iter().all(|r| r.policy_revision.as_deref() == Some("R2")));
        }

        // Local tokenize has no await between configuration and audit. Exercise
        // those same helpers directly to install a revision at that boundary.
        records.lock().unwrap().clear();
        install("R1");
        let mut req = f.query("SELECT nunya_tokenize('email', 'private@example.com')").await;
        let who = f.svc.authenticate(&f.request((), "alice")).unwrap();
        let (policy, snapshot, revision) = f.svc.configuration().unwrap();
        let (plan, names, _) = f.svc.sql(
            &who, revision.as_deref(), &req.sql, &mut req.params, &policy, &snapshot,
        ).unwrap();
        install("R2");
        let result = f.svc.local_tokenize(
            &who, revision.as_deref(), &policy, &snapshot, &plan, &names, &req,
        ).unwrap().unwrap();
        NunyaSpanner::audit(
            &f.svc.ctx, &f.svc.watchers, &who, revision.as_deref(),
            "Spanner nunya_tokenize", 1, local_tokens(&result).unwrap(),
        ).unwrap();
        assert_eq!(records.lock().unwrap()[0].policy_revision.as_deref(), Some("R1"));
        records.lock().unwrap().clear();
        req.resume_token = vec![1];
        f.svc.local_tokenize(
            &who, revision.as_deref(), &policy, &snapshot, &plan, &names, &req,
        ).unwrap_err();
        {
            let records = records.lock().unwrap();
            assert_eq!(records.len(), 1);
            assert!(matches!(records[0].outcome, Outcome::Refused(NyCode::NotAnalysable)));
            assert_eq!(records[0].policy_revision.as_deref(), Some("R1"));
        }
        records.lock().unwrap().clear();
        req.resume_token.clear();
        f.svc.execute_sql(f.request(req, "alice")).await.unwrap();
        let records = records.lock().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].policy_revision.as_deref(), Some("R2"));
    }

    #[tokio::test]
    async fn revocation_audits_watcher_connection() {
        let records = Arc::new(Mutex::new(vec![]));
        let lifecycle = Arc::new(Mutex::new(vec![]));
        let f = Fixture::with_log(Box::new(CaptureLog(records.clone(), lifecycle.clone()))).await;
        let _name = f.session().await;
        let connection = f.svc.watchers.lock().unwrap()[&f.alice].connection;
        let row = f.svc.ctx.authority.session(f.alice).unwrap();
        revoke(&f);
        wait_for(|| !lifecycle.lock().unwrap().is_empty()).await;
        let lifecycle = lifecycle.lock().unwrap();
        assert_eq!(lifecycle.len(), 1);
        let record = &lifecycle[0];
        assert_eq!(record.kind, LifecycleKind::ConnectionEnded);
        assert_eq!(record.actor, LifecycleActor::System);
        assert_eq!(record.reason, "revoked");
        assert_eq!(record.connection, Some(connection));
        assert_eq!(record.session, Some(f.alice.to_hex()));
        assert_eq!(record.principal, row.principal.to_hex());
        assert_eq!(record.route.as_deref(), Some(f.svc.ctx.route.as_str()));
        assert!(records.lock().unwrap().is_empty());
    }
    #[tokio::test]
    async fn audit_records_bearer_identity_and_watcher_connection() {
        let records = Arc::new(Mutex::new(vec![]));
        let f = Fixture::with_log(Box::new(CaptureLog(records.clone(), Arc::new(Mutex::new(vec![]))))).await;
        let name = f.session().await;
        let authority = &f.svc.ctx.authority;
        let row = authority.session(f.alice).unwrap();
        authority.renew(row.principal, row.id, None, Some((
            Verifier(ScramVerifier::from_password("rotated", b"salt", 2)),
            Sha256::digest(b"rotated").into(),
        ))).unwrap();
        for secret in ["password", "rotated"] {
            let request = session_request((), f.alice, secret);
            let who = f.svc.authenticate(&request).unwrap();
            f.svc.record_session(&name, &who).unwrap();
            NunyaSpanner::audit(&f.svc.ctx, &f.svc.watchers, &who, None, "Read", 0, vec![]).unwrap();
            let records = records.lock().unwrap();
            let record = records.last().unwrap();
            assert_eq!(record.principal, row.principal);
            assert_eq!(record.session, row.id);
            assert_eq!(record.scope, row.scope);
            assert_eq!(record.route, f.svc.ctx.route.as_str());
            assert_eq!(record.credential_id, authority.session_secret_credential(row.id, secret.as_bytes()));
            assert_eq!(record.connection, Some(f.svc.watchers.lock().unwrap()[&row.id].connection));
        }
    }
    #[tokio::test]
    async fn plain_token_parameter_and_structured_values_are_refused() {
        use googleapis_tonic_google_spanner_v1::google::spanner::v1::{KeyRange, KeySet, key_range};
        let f = Fixture::new().await;
        let mut policy = (**f.svc.ctx.policy.read().unwrap()).clone();
        policy.columns.clear();
        let snapshot = f.svc.ctx.snapshot.read().unwrap().clone();
        let policy = crate::policy::effective::legacy_policy(policy, &snapshot);
        *f.svc.ctx.policy.write().unwrap() = Arc::new(policy);
        let mut req = f.query("UPDATE customers SET email = @v").await;
        req.params = Some(prost_types::Struct {
            fields: [("v".into(), text("tok_email_a"))].into(),
        });
        assert!(
            f.svc
                .execute_sql(f.request(req.clone(), "alice"))
                .await
                .unwrap_err()
                .message()
                .contains("NY001")
        );
        assert!(
            f.svc
                .execute_streaming_sql(f.request(req, "alice"))
                .await
                .err()
                .unwrap()
                .message()
                .contains("NY001")
        );
        assert!(f.fake.seen.lock().unwrap().is_empty());
        let session = f.session().await;
        let values = ListValue {
            values: vec![text("tok_email_a")],
        };
        let sets = [
            KeySet {
                keys: vec![values.clone()],
                ..Default::default()
            },
            KeySet {
                ranges: vec![KeyRange {
                    start_key_type: Some(key_range::StartKeyType::StartClosed(values.clone())),
                    end_key_type: Some(key_range::EndKeyType::EndOpen(ListValue {
                        values: vec![],
                    })),
                }],
                ..Default::default()
            },
        ];
        for set in sets {
            let req = ReadRequest {
                session: session.clone(),
                table: "customers".into(),
                columns: vec!["email".into()],
                key_set: Some(set.clone()),
                ..Default::default()
            };
            assert!(
                f.svc
                    .read(f.request(req, "alice"))
                    .await
                    .unwrap_err()
                    .message()
                    .contains("NY001")
            );
            let req = CommitRequest {
                session: session.clone(),
                mutations: vec![Mutation {
                    operation: Some(mutation::Operation::Delete(mutation::Delete {
                        table: "customers".into(),
                        key_set: Some(set),
                    })),
                }],
                ..Default::default()
            };
            assert!(
                f.svc
                    .commit(f.request(req, "alice"))
                    .await
                    .unwrap_err()
                    .message()
                    .contains("NY001")
            );
        }
        let req = CommitRequest {
            session,
            mutations: vec![Mutation {
                operation: Some(mutation::Operation::Insert(mutation::Write {
                    table: "customers".into(),
                    columns: vec!["email".into()],
                    values: vec![values],
                })),
            }],
            ..Default::default()
        };
        assert!(
            f.svc
                .commit(f.request(req, "alice"))
                .await
                .unwrap_err()
                .message()
                .contains("NY001")
        );
        assert!(f.fake.commits.lock().unwrap().is_empty());
    }
    #[tokio::test]
    async fn blind_string_is_tokenized() {
        let f = Fixture::new().await;
        let req = f.query("SELECT email FROM customers").await;
        let result = f
            .svc
            .execute_sql(f.request(req, "alice"))
            .await
            .unwrap()
            .into_inner();
        match &result.rows[0].values[0].kind {
            Some(prost_types::value::Kind::StringValue(s)) => assert!(s.starts_with("tok_email_")),
            _ => panic!("expected token"),
        }
    }
    #[tokio::test]
    async fn token_parameter_is_decrypted() {
        let f = Fixture::new().await;
        let mut req = f
            .query("SELECT email FROM customers WHERE email = @v")
            .await;
        req.params = Some(prost_types::Struct {
            fields: BTreeMap::from([("v".into(), f.token())]),
        });
        f.svc.execute_sql(f.request(req, "alice")).await.unwrap();
        assert_eq!(
            f.fake.seen.lock().unwrap()[0]
                .params
                .as_ref()
                .unwrap()
                .fields["v"],
            text("private@example.com")
        );
    }
    #[tokio::test]
    async fn like_is_refused_before_upstream() {
        let f = Fixture::new().await;
        let req = f
            .query("SELECT email FROM customers WHERE email LIKE '%example%'")
            .await;
        assert!(f.svc.execute_sql(f.request(req, "alice")).await.is_err());
        assert!(f.fake.seen.lock().unwrap().is_empty());
    }
    #[tokio::test]
    async fn wrong_bearer_is_ny003() {
        let f = Fixture::new().await;
        for principal in [false, true] {
            let value = CreateSessionRequest {
                database: f.svc.ctx.database.clone(),
                ..Default::default()
            };
            let mut req = session_request(value, f.alice, "wrong");
            if principal {
                req.metadata_mut().insert(
                    "authorization",
                    "Bearer nunya.alice.password".parse().unwrap(),
                );
            }
            assert!(
                f.svc
                    .create_session(req)
                    .await
                    .unwrap_err()
                    .message()
                    .contains("NY003")
            );
        }
    }
    #[tokio::test]
    async fn another_principals_session_is_refused() {
        let f = Fixture::new().await;
        let req = f.query("SELECT email FROM customers").await;
        assert!(
            f.svc
                .execute_sql(f.request(req, "bob"))
                .await
                .unwrap_err()
                .message()
                .contains("NY003")
        );
        assert!(f.fake.seen.lock().unwrap().is_empty());
    }
    #[tokio::test]
    async fn partition_query_is_refused() {
        let f = Fixture::new().await;
        let session = f.session().await;
        let result = f
            .svc
            .partition_query(f.request(
                PartitionQueryRequest {
                    session,
                    ..Default::default()
                },
                "alice",
            ))
            .await
            .unwrap_err();
        assert!(result.message().contains("NY006"));
    }
    #[tokio::test]
    async fn upstream_status_is_redacted() {
        let f = Fixture::new().await;
        f.fake
            .failure
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let result = f
            .svc
            .create_session(f.request(
                CreateSessionRequest {
                    database: f.svc.ctx.database.clone(),
                    ..Default::default()
                },
                "alice",
            ))
            .await
            .unwrap_err();
        assert_eq!(result.code(), tonic::Code::AlreadyExists);
        assert!(!result.message().contains("private@example.com"));
        assert!(result.details().is_empty());
    }
    fn precommit(seq_num: i32) -> MultiplexedSessionPrecommitToken {
        MultiplexedSessionPrecommitToken {
            precommit_token: vec![0, 255, seq_num as u8],
            seq_num,
        }
    }
    async fn customer_read(f: &Fixture) -> ReadRequest {
        ReadRequest {
            session: f.session().await,
            table: "customers".into(),
            columns: vec!["email".into()],
            key_set: Some(KeySet {
                all: true,
                ..Default::default()
            }),
            ..Default::default()
        }
    }
    #[tokio::test]
    async fn unary_precommit_tokens_pass_through() {
        let f = Fixture::new().await;
        let token = Some(precommit(7));
        *f.fake.result_token.lock().unwrap() = token.clone();
        let query = f.query("SELECT email FROM customers").await;
        let result = f
            .svc
            .execute_sql(f.request(query, "alice"))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(result.precommit_token, token);
        let result = f
            .svc
            .read(f.request(customer_read(&f).await, "alice"))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(result.precommit_token, token);
        let mut query = f.query("UPDATE customers SET email = @v").await;
        query.params = Some(prost_types::Struct {
            fields: [("v".into(), f.token())].into(),
        });
        let result = f
            .svc
            .execute_batch_dml(f.request(
                ExecuteBatchDmlRequest {
                    session: query.session,
                    statements: vec![execute_batch_dml_request::Statement {
                        sql: query.sql,
                        params: query.params,
                        param_types: query.param_types,
                    }],
                    ..Default::default()
                },
                "alice",
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(result.precommit_token, token);
    }
    #[tokio::test]
    async fn commit_precommit_retry_and_unknown_outcome() {
        let f = Fixture::new().await;
        let session = f.session().await;
        f.svc.mark_dml(&session, b"txn", f.alice).unwrap();
        let retry = CommitResponse {
            multiplexed_session_retry: Some(
                commit_response::MultiplexedSessionRetry::PrecommitToken(precommit(8)),
            ),
            ..Default::default()
        };
        *f.fake.next_commit.lock().unwrap() = Some(retry.clone());
        let request = CommitRequest {
                        transaction: Some(commit_request::Transaction::TransactionId(b"txn".to_vec())),
            session: session.clone(),
            precommit_token: Some(precommit(7)),
            ..Default::default()
        };
        let result = f
            .svc
            .commit(f.request(request, "alice"))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(result, retry);
        assert_eq!(
            f.fake.commits.lock().unwrap()[0].precommit_token,
            Some(precommit(7))
        );
        assert!(f.svc.transactions.lock().unwrap().contains_key(&(session.clone(), b"txn".to_vec())));
        reduce_grant(&f);
        assert_read_only(
            f.svc
                .commit(f.request(
                    CommitRequest {
                        transaction: Some(commit_request::Transaction::TransactionId(b"txn".to_vec())),
                        session,
                        ..Default::default()
                    },
                    "alice",
                ))
                .await
                .unwrap_err(),
        );
        assert_eq!(f.fake.commits.lock().unwrap().len(), 1);

        let f = Fixture::new().await;
        let session = f.session().await;
        f.svc.mark_dml(&session, b"txn", f.alice).unwrap();
        *f.fake.next_commit.lock().unwrap() = Some(CommitResponse::default());
        let error = f
            .svc
            .commit(f.request(
                CommitRequest {
                        transaction: Some(commit_request::Transaction::TransactionId(b"txn".to_vec())),
                    session: session.clone(),
                    ..Default::default()
                },
                "alice",
            ))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::Unknown);
        assert!(error.message().contains("NY012"));
        assert!(!f.svc.transactions.lock().unwrap().contains_key(&(session.clone(), b"txn".to_vec())));
        assert!(
            f.svc
                .commit(f.request(
                    CommitRequest {
                        transaction: Some(commit_request::Transaction::TransactionId(b"txn".to_vec())),
                        session,
                        ..Default::default()
                    },
                    "alice"
                ))
                .await
                .unwrap()
                .get_ref()
                .commit_timestamp
                .is_some()
        );
    }
    #[tokio::test]
    async fn begin_mutation_key_is_checked_and_decrypted() {
        let f = Fixture::new().await;
        let session = f.session().await;
        *f.fake.result_token.lock().unwrap() = Some(precommit(7));
        let mutation = Mutation {
            operation: Some(mutation::Operation::Insert(mutation::Write {
                table: "customers".into(),
                columns: vec!["email".into()],
                values: vec![ListValue {
                    values: vec![f.token()],
                }],
            })),
        };
        let result = f
            .svc
            .begin_transaction(f.request(
                BeginTransactionRequest {
                    session: session.clone(),
                    mutation_key: Some(mutation),
                    ..Default::default()
                },
                "alice",
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(result.precommit_token, Some(precommit(7)));
        assert_eq!(
            mutation_value(
                f.fake.begins.lock().unwrap()[0]
                    .mutation_key
                    .as_ref()
                    .unwrap(),
                &ValuePosition::MutationValue { row: 0, column: 0 }
            ),
            Some(&text("private@example.com"))
        );
        assert!(
            f.svc
                .begin_transaction(f.request(
                    BeginTransactionRequest {
                        session,
                        mutation_key: Some(Mutation::default()),
                        ..Default::default()
                    },
                    "alice"
                ))
                .await
                .is_err()
        );
        assert_eq!(f.fake.begins.lock().unwrap().len(), 1);
    }
    #[tokio::test]
    async fn begin_mutation_key_refuses_downgraded_grant() {
        let f = Fixture::new().await;
        let session = f.session().await;
        let mutation = Mutation {
            operation: Some(mutation::Operation::Insert(mutation::Write {
                table: "customers".into(),
                columns: vec!["email".into()],
                values: vec![ListValue { values: vec![f.token()] }],
            })),
        };
        reduce_grant(&f);
        let error = f.svc.begin_transaction(f.request(BeginTransactionRequest {
            session, mutation_key: Some(mutation), ..Default::default()
        }, "alice")).await.unwrap_err();
        assert_read_only(error);
        assert!(f.fake.begins.lock().unwrap().is_empty());
    }
    #[tokio::test]
    async fn begin_mutation_key_refuses_token_in_unprotected_column() {
        let f = Fixture::new().await;
        let session = f.session().await;
        // The base fixture protects its only column; add an unprotected text column.
        let mut snapshot = (**f.svc.ctx.snapshot.read().unwrap()).clone();
        snapshot.relations[0].columns.push(Column {
            name: "display_name".into(), ty: ColumnType::Text, nullable: true,
            generated: None, default: None,
        });
        *f.svc.ctx.snapshot.write().unwrap() = Arc::new(snapshot);
        let mutation = Mutation {
            operation: Some(mutation::Operation::Insert(mutation::Write {
                table: "customers".into(), columns: vec!["display_name".into()],
                values: vec![ListValue { values: vec![f.token()] }],
            })),
        };
        let error = f.svc.begin_transaction(f.request(BeginTransactionRequest {
            session, mutation_key: Some(mutation), ..Default::default()
        }, "alice")).await.unwrap_err();
        assert_eq!(error.code(), nunya_status(NyCode::TokenOutsideDomain).code());
        assert!(error.message().contains("NY001"));
        assert!(f.fake.begins.lock().unwrap().is_empty());
    }
    #[tokio::test]
    async fn revocation_before_trailing_precommit_flush_refuses_output() {
        let f = Fixture::new().await;
        use prost::Message;
        use tonic::codec::Codec;
        let frame = |part: PartialResultSet| {
            let encoded = part.encode_to_vec();
            let mut data = vec![0];
            data.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
            data.extend_from_slice(&encoded);
            Ok::<_, Status>(hyper::body::Frame::data(bytes::Bytes::from(data)))
        };
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let authority = f.svc.ctx.authority.clone();
        let session = f.alice;
        // Revoke synchronously as EOF is polled, after the final control part's
        // admission check and before finish(). No watcher scheduling is needed.
        let mut at_eof = false;
        let body = http_body_util::StreamBody::new(
            tokio_stream::wrappers::ReceiverStream::new(rx).chain(futures::stream::poll_fn(move |_| {
                if !at_eof {
                    at_eof = true;
                    authority.revoke(session, crate::state::model::EndReason::AdminRevoked).unwrap();
                }
                std::task::Poll::Ready(None)
            })),
        );
        let mut codec = tonic::codec::ProstCodec::<PartialResultSet, PartialResultSet>::default();
        let upstream = tonic::Streaming::new_response(
            codec.decoder(), body, http::StatusCode::OK, None, None,
        );
        tx.send(frame(PartialResultSet {
            metadata: Some(ResultSetMetadata {
                row_type: Some(StructType { fields: vec![struct_type::Field {
                    name: "email".into(), r#type: Some(Type {
                        code: TypeCode::String as i32, ..Default::default()
                    }),
                }] }), ..Default::default()
            }),
            values: vec![text("private@example.com")], ..Default::default()
        })).await.unwrap();
        let req = f.query("SELECT email FROM customers").await;
        let who = f.svc.authenticate(&f.request((), "alice")).unwrap();
        let (policy, snapshot, revision) = f.svc.configuration().unwrap();
        let (plan, _, _) = f.svc.sql(
            &who, revision.as_deref(), &req.sql, &mut None, &policy, &snapshot,
        ).unwrap();
        // Keep the watcher false to exercise the authoritative recheck itself.
        let (_signal_tx, signal) = tokio::sync::watch::channel(false);
        let outcome_audit = WriteAudit::new(&f.svc, &who);
        let mut stream = f.svc.stream(
            who, policy, revision, plan, req.sql, upstream, signal, false, None, outcome_audit,
        );
        let row = stream.next().await.unwrap().unwrap();
        assert_eq!(row.values.len(), 1);
        assert!(row.precommit_token.is_none());
        tx.send(frame(PartialResultSet {
            precommit_token: Some(precommit(9)), last: true, ..Default::default()
        })).await.unwrap();
        drop(tx);
        assert_ended(stream.next().await.unwrap().unwrap_err(), false);
        assert!(stream.next().await.is_none());
    }
    #[tokio::test]
    async fn streaming_precommit_tokens_and_last_survive_reassembly() {
        for read in [false, true] {
            let f = Fixture::new().await;
            let metadata = ResultSetMetadata {
                row_type: Some(StructType {
                    fields: vec![struct_type::Field {
                        name: "email".into(),
                        r#type: Some(Type {
                            code: TypeCode::String as i32,
                            ..Default::default()
                        }),
                    }],
                }),
                ..Default::default()
            };
            *f.fake.parts.lock().unwrap() = Some(vec![
                PartialResultSet {
                    metadata: Some(metadata),
                    values: vec![text("private@example.com")],
                    precommit_token: Some(precommit(7)),
                    ..Default::default()
                },
                PartialResultSet {
                    values: vec![text("private@")],
                    chunked_value: true,
                    precommit_token: Some(precommit(8)),
                    ..Default::default()
                },
                PartialResultSet {
                    values: vec![text("example.com")],
                    ..Default::default()
                },
                PartialResultSet {
                    precommit_token: Some(precommit(9)),
                    last: true,
                    ..Default::default()
                },
            ]);
            let mut stream = if read {
                f.svc
                    .streaming_read(f.request(customer_read(&f).await, "alice"))
                    .await
                    .unwrap()
                    .into_inner()
            } else {
                f.svc
                    .execute_streaming_sql(
                        f.request(f.query("SELECT email FROM customers").await, "alice"),
                    )
                    .await
                    .unwrap()
                    .into_inner()
            };
            let mut tokens = vec![];
            let mut values = vec![];
            let mut last = false;
            while let Some(part) = stream.next().await {
                let part = part.unwrap();
                assert!(!last);
                if let Some(token) = part.precommit_token {
                    tokens.push(token);
                }
                values.extend(part.values);
                last = part.last;
            }
            assert_eq!(tokens, vec![precommit(7), precommit(8), precommit(9)]);
            assert_eq!(values, vec![f.token(), f.token()]);
            assert!(last);
        }
    }
    #[tokio::test]
    async fn commit_blind_insert_is_decrypted() {
        let f = Fixture::new().await;
        let session = f.session().await;
        let mutation = Mutation {
            operation: Some(mutation::Operation::Insert(mutation::Write {
                table: "customers".into(),
                columns: vec!["email".into()],
                values: vec![ListValue {
                    values: vec![f.token()],
                }],
            })),
        };
        f.svc
            .commit(f.request(
                CommitRequest {
                    session,
                    mutations: vec![mutation],
                    ..Default::default()
                },
                "alice",
            ))
            .await
            .unwrap();
        let commits = f.fake.commits.lock().unwrap();
        assert_eq!(
            mutation_value(
                &commits[0].mutations[0],
                &ValuePosition::MutationValue { row: 0, column: 0 }
            ),
            Some(&text("private@example.com"))
        );
    }
    #[tokio::test]
    async fn creator_roles_are_never_forwarded() {
        let f = Fixture::new().await;
        f.svc
            .create_session(f.request(
                CreateSessionRequest {
                    database: f.svc.ctx.database.clone(),
                    session: Some(Session {
                        creator_role: "admin".into(),
                        ..Default::default()
                    }),
                },
                "alice",
            ))
            .await
            .unwrap();
        f.svc
            .batch_create_sessions(f.request(
                BatchCreateSessionsRequest {
                    database: f.svc.ctx.database.clone(),
                    session_template: Some(Session {
                        creator_role: "admin".into(),
                        ..Default::default()
                    }),
                    session_count: 1,
                },
                "alice",
            ))
            .await
            .unwrap();
        assert_eq!(*f.fake.roles.lock().unwrap(), vec!["", ""]);
    }
    #[tokio::test]
    async fn chunked_blind_string_is_one_complete_token() {
        let f = Fixture::new().await;
        let req = f.query("SELECT email FROM customers").await;
        let mut stream = f
            .svc
            .execute_streaming_sql(f.request(req, "alice"))
            .await
            .unwrap()
            .into_inner();
        let mut values = vec![];
        while let Some(part) = stream.next().await {
            let part = part.unwrap();
            assert!(!part.chunked_value);
            for value in part.values {
                if let Some(prost_types::value::Kind::StringValue(s)) = &value.kind {
                    assert!(!s.contains("private@"));
                    assert!(!s.contains("example.com"));
                }
                values.push(value);
            }
        }
        assert_eq!(values, vec![f.token()]);
    }

    #[tokio::test]
    async fn catalog_uses_strong_read_only_and_sanitizes_errors() {
        use crate::schema::spanner_gsql::SqlRunner;
        let f = Fixture::new().await;
        let runner = super::super::catalog::CatalogRunner::new(
            &f.svc.ctx.upstream,
            f.svc.ctx.database.clone(),
        )
        .await
        .unwrap();
        assert_eq!(
            runner.query("SELECT catalog").await.unwrap(),
            vec![vec![Some("private@example.com".into())]]
        );
        {
            let seen = f.fake.seen.lock().unwrap();
            assert!(matches!(
                seen[0]
                    .transaction
                    .as_ref()
                    .and_then(|t| t.selector.as_ref()),
                Some(transaction_selector::Selector::SingleUse(
                    TransactionOptions {
                        mode: Some(transaction_options::Mode::ReadOnly(
                            transaction_options::ReadOnly {
                                timestamp_bound: Some(
                                    transaction_options::read_only::TimestampBound::Strong(true)
                                ),
                                ..
                            }
                        )),
                        ..
                    }
                ))
            ));
        }
        f.fake
            .failure
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let error = runner.query("SELECT catalog").await.unwrap_err();
        assert_eq!(error.code, NyCode::SchemaStale);
        assert!(!format!("{error:?}").contains("private@example.com"));
    }
    #[tokio::test]
    async fn another_session_of_same_principal_is_refused() {
        let f = Fixture::new().await;
        let req = f.query("SELECT email FROM customers").await;
        let first = f.svc.authenticate(&f.request((), "alice")).unwrap();
        let second = acquire(&f.svc.ctx.authority, first.principal);
        assert_ne!(first.session, second);
        assert!(
            f.svc
                .execute_sql(session_request(req, second, "password"))
                .await
                .unwrap_err()
                .message()
                .contains("NY003")
        );
        assert!(f.fake.seen.lock().unwrap().is_empty());
    }
    #[test]
    fn tokenization_store_refuses_as_unavailable() {
        assert_eq!(tokenize_refusal(TokenizeError::Ended), NyCode::SessionEnded);
        let code = tokenize_refusal(TokenizeError::Store);
        assert_eq!(code, NyCode::ReadLogUnavailable);
        assert_eq!(nunya_status(code).code(), tonic::Code::FailedPrecondition);
    }
    #[tokio::test]
    async fn tokenization_uses_durable_session_counter() {
        let records = Arc::new(Mutex::new(vec![]));
        let f = Fixture::with_log(Box::new(CaptureLog(
            records.clone(),
            Arc::new(Mutex::new(vec![])),
        ))).await;
        let who = f.svc.authenticate(&f.request((), "alice")).unwrap();
        let policy = f.svc.ctx.policy.read().unwrap().clone();
        let snapshot = f.svc.ctx.snapshot.read().unwrap().clone();
        let plan = RewritePlan {
            outputs: vec![],
            inputs: vec![],
            tokenize: Some((
                domain(),
                ValueSource::Literal {
                    span: crate::ir::Span { start: 0, end: 0 },
                    text: "private@example.com".into(),
                },
            )),
        };
        let tokenize = || {
            f.svc
                .local_tokenize(&who, None, &policy, &snapshot, &plan, &[], &ExecuteSqlRequest::default())
        };
        for _ in 0..10 {
            assert!(tokenize().unwrap().is_some());
        }
        assert_eq!(
            f.svc
                .ctx
                .authority
                .session_tokenizations(f.alice, &domain())
                .unwrap(),
            10
        );
        records.lock().unwrap().clear();
        assert!(tokenize().unwrap_err().message().contains("NY007"));
        {
            let records = records.lock().unwrap();
            assert_eq!(records.len(), 1);
            assert!(matches!(records[0].outcome, Outcome::Refused(NyCode::TokenizeLimit)));
        }
        assert_eq!(
            f.svc
                .ctx
                .authority
                .session_tokenizations(f.alice, &domain())
                .unwrap(),
            10
        );
        assert!(f.fake.seen.lock().unwrap().is_empty());
    }
    // Keep schema changes local to each test; the shared fixture remains textual.
    fn tokenize_column_type(f: &Fixture, ty: ColumnType) {
        *f.svc.ctx.snapshot.write().unwrap() = Arc::new(Snapshot::new(
            vec![Relation {
                schema: "".into(),
                name: "customers".into(),
                kind: RelationKind::Table,
                columns: vec![Column {
                    name: "email".into(), ty, nullable: true,
                    generated: None, default: None,
                }],
                view_definition: None,
                primary_key: vec!["email".into()],
            }],
            vec![], vec!["".into()], 0,
        ));
    }
    fn tokenize_count(f: &Fixture) -> u64 {
        f.svc.ctx.authority.session_tokenizations(f.alice, &domain()).unwrap()
    }
    fn assert_tokenize_error(error: Status, code: NyCode) {
        assert_eq!(error.code(), nunya_status(code).code());
        assert!(error.message().contains(code.sqlstate()));
    }
    async fn tokenize_rpc(
        f: &Fixture, req: ExecuteSqlRequest, streaming: bool,
    ) -> Result<ResultSet, Status> {
        if streaming {
            let mut stream = f.svc.execute_streaming_sql(f.request(req, "alice"))
                .await?.into_inner();
            let part = stream.next().await.unwrap()?;
            assert!(!part.chunked_value);
            assert!(stream.next().await.is_none());
            Ok(ResultSet {
                metadata: part.metadata,
                rows: vec![ListValue { values: part.values }],
                ..Default::default()
            })
        } else {
            Ok(f.svc.execute_sql(f.request(req, "alice")).await?.into_inner())
        }
    }
    fn assert_no_tokenize_upstream(f: &Fixture) {
        assert!(f.fake.seen.lock().unwrap().is_empty());
        assert_eq!(f.fake.streaming.load(std::sync::atomic::Ordering::Relaxed), 0);
        assert!(f.fake.batches.lock().unwrap().is_empty());
        assert!(f.fake.commits.lock().unwrap().is_empty());
    }
    #[tokio::test]
    async fn tokenize_rpcs_match_blind_read() {
        let f = Fixture::new().await;
        // Read once for the same session, then clear the query log to detect forwarding.
        let read = f.query("SELECT email FROM customers").await;
        let expected = f.svc.execute_sql(f.request(read, "alice")).await.unwrap()
            .into_inner().rows[0].values[0].clone();
        f.fake.seen.lock().unwrap().clear();
        for streaming in [false, true] {
            for literal in [false, true] {
                let sql = if literal {
                    "SELECT nunya_tokenize('email', 'private@example.com')"
                } else {
                    "SELECT nunya_tokenize('email', @v)"
                };
                let mut req = f.query(sql).await;
                if !literal {
                    req.params = Some(prost_types::Struct {
                        fields: BTreeMap::from([("v".into(), text("private@example.com"))]),
                    });
                    req.param_types.insert("v".into(), Type {
                        code: TypeCode::String as i32, ..Default::default()
                    });
                }
                let result = tokenize_rpc(&f, req, streaming).await.unwrap();
                assert_eq!(result.rows.len(), 1);
                assert_eq!(result.rows[0].values, vec![expected.clone()]);
                let fields = &result.metadata.unwrap().row_type.unwrap().fields;
                assert_eq!(fields.len(), 1);
                assert_eq!(fields[0].name, "nunya_tokenize");
                assert_eq!(fields[0].r#type.as_ref().unwrap().code, TypeCode::String as i32);
            }
        }
        assert_eq!(tokenize_count(&f), 4);
        assert_no_tokenize_upstream(&f);
    }
    #[tokio::test]
    async fn tokenize_rpcs_use_snapshot_type() {
        for (ty, code, input) in [
            (ColumnType::Int8, TypeCode::Int64, "42"),
            (ColumnType::Date, TypeCode::Date, "2020-01-01"),
            (ColumnType::Timestamptz, TypeCode::Timestamp, "2020-01-01T12:34:56Z"),
            (ColumnType::Varchar(None), TypeCode::String, "private@example.com"),
            (ColumnType::Bpchar(None), TypeCode::String, "private@example.com"),
        ] {
            let f = Fixture::new().await;
            tokenize_column_type(&f, ty.clone());
            let who = f.svc.authenticate(&f.request((), "alice")).unwrap();
            let wire_ty = if ty.is_textual() { &ColumnType::Text } else { &ty };
            let plain = values::from_proto(wire_ty, &text(input)).unwrap();
            let encoded = f.svc.ctx.codec.encode(&who.keys, &domain(), &ty, None, &plain).unwrap();
            let expected = values::to_proto(wire_ty, &encoded).unwrap();
            for streaming in [false, true] {
                // Literal, explicitly typed param, and omitted param type all use the column type.
                for mode in 0..3 {
                    let sql = if mode == 0 {
                        format!("SELECT nunya_tokenize('email', '{input}')")
                    } else {
                        "SELECT nunya_tokenize('email', @v)".into()
                    };
                    let mut req = f.query(&sql).await;
                    if mode != 0 {
                        req.params = Some(prost_types::Struct {
                            fields: BTreeMap::from([("v".into(), text(input))]),
                        });
                        if mode == 1 {
                            req.param_types.insert("v".into(), Type {
                                code: code as i32, ..Default::default()
                            });
                        }
                    }
                    let result = tokenize_rpc(&f, req, streaming).await.unwrap();
                    assert_eq!(result.rows.len(), 1);
                    assert_eq!(result.rows[0].values, vec![expected.clone()]);
                    let fields = result.metadata.unwrap().row_type.unwrap().fields;
                    assert_eq!(fields.len(), 1);
                    assert_eq!(fields[0].name, "nunya_tokenize");
                    assert_eq!(fields[0].r#type.as_ref().unwrap().code, code as i32);
                }
            }
            assert_eq!(tokenize_count(&f), 6);
            assert_no_tokenize_upstream(&f);
        }
    }
    #[tokio::test]
    async fn tokenize_type_and_conversion_refusals_preserve_quota() {
        let f = Fixture::new().await;
        tokenize_column_type(&f, ColumnType::Int8);
        for streaming in [false, true] {
            let mut req = f.query("SELECT nunya_tokenize('email', @v)").await;
            req.params = Some(prost_types::Struct {
                fields: BTreeMap::from([("v".into(), text("42"))]),
            });
            req.param_types.insert("v".into(), Type {
                code: TypeCode::String as i32, ..Default::default()
            });
            assert_tokenize_error(tokenize_rpc(&f, req, streaming).await.unwrap_err(), NyCode::NotAnalysable);
            let req = f.query("SELECT nunya_tokenize('email', 'abc')").await;
            assert_tokenize_error(tokenize_rpc(&f, req, streaming).await.unwrap_err(), NyCode::NotAnalysable);
            assert_eq!(tokenize_count(&f), 0);
        }
        // The policy still names a blind column, but the snapshot cannot resolve it.
        *f.svc.ctx.snapshot.write().unwrap() = Arc::new(Snapshot::new(vec![], vec![], vec!["".into()], 0));
        for streaming in [false, true] {
            let req = f.query("SELECT nunya_tokenize('email', '42')").await;
            assert_tokenize_error(tokenize_rpc(&f, req, streaming).await.unwrap_err(), NyCode::NotAnalysable);
        }
        assert_eq!(tokenize_count(&f), 0);
        assert_no_tokenize_upstream(&f);
    }
    #[tokio::test]
    async fn tokenize_disabled_and_revoked_preserve_quota() {
        let f = Fixture::new().await;
        let mut policy = (*f.svc.ctx.policy.read().unwrap().clone()).clone();
        policy.domains.get_mut(&domain()).unwrap().tokenize = TokenizeSetting::Off;
        *f.svc.ctx.policy.write().unwrap() = Arc::new(policy);
        for streaming in [false, true] {
            let req = f.query("SELECT nunya_tokenize('email', 'private@example.com')").await;
            assert_tokenize_error(tokenize_rpc(&f, req, streaming).await.unwrap_err(), NyCode::StatementNotAllowed);
        }
        assert_eq!(tokenize_count(&f), 0);
        let req = f.query("SELECT nunya_tokenize('email', 'private@example.com')").await;
        revoke(&f);
        for streaming in [false, true] {
            assert_tokenize_error(tokenize_rpc(&f, req.clone(), streaming).await.unwrap_err(), NyCode::TokenNotValid);
        }
        assert_eq!(tokenize_count(&f), 0);
        assert_no_tokenize_upstream(&f);
    }
    type CounterObservation = (ReadLogRecord, u64);
    struct CounterLog {
        authority: Arc<Mutex<Option<Arc<crate::state::authority::Authority>>>>,
        records: Arc<Mutex<Vec<CounterObservation>>>,
    }
    impl ReadLog for CounterLog {
        fn write(&self, record: &ReadLogRecord) -> Result<(), Refusal> {
            let count = self.authority.lock().unwrap().as_ref().unwrap()
                .session_tokenizations(record.session, &domain()).unwrap();
            self.records.lock().unwrap().push((record.clone(), count));
            Ok(())
        }
        fn write_lifecycle(&self, _: &LifecycleRecord) -> Result<(), Refusal> { Ok(()) }
    }
    #[tokio::test]
    async fn tokenize_rpc_limit_audits_after_counter() {
        let authority = Arc::new(Mutex::new(None));
        let records = Arc::new(Mutex::new(vec![]));
        let f = Fixture::with_log(Box::new(CounterLog {
            authority: authority.clone(), records: records.clone(),
        })).await;
        *authority.lock().unwrap() = Some(f.svc.ctx.authority.clone());
        let req = f.query("SELECT nunya_tokenize('email', 'private@example.com')").await;
        for i in 1..=10 {
            tokenize_rpc(&f, req.clone(), i % 2 == 0).await.unwrap();
            assert_eq!(tokenize_count(&f), i);
            let records = records.lock().unwrap();
            assert_eq!(records.len(), i as usize);
            let (record, observed_count) = records.last().unwrap();
            assert!(matches!(record.outcome, Outcome::Allowed));
            assert_eq!(record.rows, 1);
            assert_eq!(record.tokens_released.len(), 1);
            assert_eq!(*observed_count, i);
        }
        assert_tokenize_error(tokenize_rpc(&f, req, true).await.unwrap_err(), NyCode::TokenizeLimit);
        let records = records.lock().unwrap();
        assert_eq!(records.len(), 11);
        assert_eq!(records.iter().filter(|(r, _)| matches!(r.outcome, Outcome::Allowed)).count(), 10);
        let (refused, count) = &records[10];
        assert!(matches!(refused.outcome, Outcome::Refused(NyCode::TokenizeLimit)));
        assert!(refused.tokens_released.is_empty());
        assert_eq!(refused.rows, 0);
        assert_eq!(*count, 10);
        assert_eq!(tokenize_count(&f), 10);
        assert_no_tokenize_upstream(&f);
    }
    async fn tokenize_selector_case(selector: Option<transaction_selector::Selector>, accepted: bool) {
        for streaming in [false, true] {
            let records = Arc::new(Mutex::new(vec![]));
            let f = Fixture::with_log(Box::new(CaptureLog(
                records.clone(), Arc::new(Mutex::new(vec![])),
            ))).await;
            let mut req = f.query("SELECT nunya_tokenize('email', 'private@example.com')").await;
            req.transaction = selector.clone().map(|selector| TransactionSelector { selector: Some(selector) });
            let result = tokenize_rpc(&f, req, streaming).await;
            if accepted {
                let result = result.unwrap();
                assert_eq!(local_tokens(&result).unwrap().len(), 1);
                assert_eq!(tokenize_count(&f), 1);
                let records = records.lock().unwrap();
                assert_eq!(records.len(), 1);
                assert!(matches!(records[0].outcome, Outcome::Allowed));
            } else {
                assert_tokenize_error(result.unwrap_err(), NyCode::NotAnalysable);
                assert_eq!(tokenize_count(&f), 0);
                assert_tokenize_refused(&records, NyCode::NotAnalysable);
            }
            assert_no_tokenize_upstream(&f);
        }
    }
    fn assert_tokenize_refused(records: &Mutex<Vec<ReadLogRecord>>, code: NyCode) {
        let records = records.lock().unwrap();
        assert_eq!(records.len(), 1);
        let record = &records[0];
        assert!(matches!(record.outcome, Outcome::Refused(c) if c == code));
        assert_eq!(record.rows, 0);
        assert!(record.tokens_released.is_empty());
        assert_eq!(record.statement_fingerprint, statement_fingerprint("Spanner nunya_tokenize"));
        assert!(!record.statement.contains("private@example.com"));
        assert!(!record.statement.contains("email"));
    }
    fn single_use(mode: Option<transaction_options::Mode>) -> transaction_selector::Selector {
        transaction_selector::Selector::SingleUse(TransactionOptions { mode, ..Default::default() })
    }
    #[tokio::test]
    async fn tokenize_selector_none() { tokenize_selector_case(None, true).await; }
    #[tokio::test]
    async fn tokenize_selector_single_read_only() {
        tokenize_selector_case(Some(single_use(Some(transaction_options::Mode::ReadOnly(Default::default())))), true).await;
    }
    #[tokio::test]
    async fn tokenize_selector_single_read_write() {
        tokenize_selector_case(Some(single_use(Some(transaction_options::Mode::ReadWrite(Default::default())))), false).await;
    }
    #[tokio::test]
    async fn tokenize_selector_single_partitioned() {
        tokenize_selector_case(Some(single_use(Some(transaction_options::Mode::PartitionedDml(Default::default())))), false).await;
    }
    #[tokio::test]
    async fn tokenize_selector_single_missing_mode() {
        tokenize_selector_case(Some(single_use(None)), false).await;
    }
    #[tokio::test]
    async fn tokenize_selector_begin_read_only() {
        tokenize_selector_case(Some(transaction_selector::Selector::Begin(TransactionOptions {
            mode: Some(transaction_options::Mode::ReadOnly(Default::default())), ..Default::default()
        })), false).await;
    }
    #[tokio::test]
    async fn tokenize_selector_begin_read_write() {
        tokenize_selector_case(Some(transaction_selector::Selector::Begin(TransactionOptions {
            mode: Some(transaction_options::Mode::ReadWrite(Default::default())), ..Default::default()
        })), false).await;
    }
    #[tokio::test]
    async fn tokenize_selector_id() {
        tokenize_selector_case(Some(transaction_selector::Selector::Id(vec![1])), true).await;
    }
    #[tokio::test]
    async fn tokenize_selector_empty_id() {
        tokenize_selector_case(Some(transaction_selector::Selector::Id(vec![])), false).await;
    }
    #[tokio::test]
    async fn tokenize_resume_and_null_refuse_before_counter() {
        for streaming in [false, true] {
            for mode in 0..5 {
                let records = Arc::new(Mutex::new(vec![]));
                let f = Fixture::with_log(Box::new(CaptureLog(
                    records.clone(), Arc::new(Mutex::new(vec![])),
                ))).await;
                let mut req = f.query("SELECT nunya_tokenize('email', @v)").await;
                req.params = Some(prost_types::Struct { fields: BTreeMap::from([(
                    "v".into(), match mode {
                        1 => Value { kind: Some(prost_types::value::Kind::NullValue(0)) },
                        2 => Value { kind: None },
                        _ => text("private@example.com"),
                    },
                )]) });
                if mode == 0 { req.resume_token = vec![1]; }
                if mode == 3 { req.transaction = Some(TransactionSelector { selector: None }); }
                if mode == 4 { req.params = None; }
                assert_tokenize_error(tokenize_rpc(&f, req, streaming).await.unwrap_err(), NyCode::NotAnalysable);
                assert_eq!(tokenize_count(&f), 0);
                assert_tokenize_refused(&records, NyCode::NotAnalysable);
                assert_no_tokenize_upstream(&f);
            }
        }
    }
    #[tokio::test]
    async fn tokenize_analysis_refusals_audit_once() {
        for streaming in [false, true] {
            for mode in 0..3 {
                let records = Arc::new(Mutex::new(vec![]));
                let f = Fixture::with_log(Box::new(CaptureLog(
                    records.clone(), Arc::new(Mutex::new(vec![])),
                ))).await;
                let sql = if mode == 1 {
                    "SELECT NUNYA_TOKENIZE('email', 'private@example.com', 'extra')"
                } else { "SELECT nunya_tokenize('email', @v)" };
                let mut req = f.query(sql).await;
                req.params = Some(prost_types::Struct { fields: BTreeMap::from([("v".into(), text("private@example.com"))]) });
                let code = if mode == 0 {
                    let mut policy = (**f.svc.ctx.policy.read().unwrap()).clone();
                    policy.domains.get_mut(&domain()).unwrap().tokenize = TokenizeSetting::Off;
                    *f.svc.ctx.policy.write().unwrap() = Arc::new(policy);
                    NyCode::StatementNotAllowed
                } else {
                    if mode == 2 { req.param_types.insert("v".into(), Type { code: TypeCode::Int64 as i32, ..Default::default() }); }
                    NyCode::NotAnalysable
                };
                assert_tokenize_error(tokenize_rpc(&f, req, streaming).await.unwrap_err(), code);
                assert_tokenize_refused(&records, code);
                assert_eq!(tokenize_count(&f), 0);
                assert_no_tokenize_upstream(&f);
            }
        }
    }
    #[tokio::test]
    async fn tokenize_batch_refusal_audits() {
        let records = Arc::new(Mutex::new(vec![]));
        let f = Fixture::with_log(Box::new(CaptureLog(records.clone(), Arc::new(Mutex::new(vec![]))))).await;
        let query = f.query("SELECT nunya_tokenize('email', 'private@example.com')").await;
        let req = ExecuteBatchDmlRequest {
            session: query.session,
            statements: vec![execute_batch_dml_request::Statement { sql: query.sql, ..Default::default() }],
            ..Default::default()
        };
        assert_tokenize_error(f.svc.execute_batch_dml(f.request(req, "alice")).await.unwrap_err(), NyCode::NotAnalysable);
        assert_tokenize_refused(&records, NyCode::NotAnalysable);
        assert_eq!(tokenize_count(&f), 0);
        assert_no_tokenize_upstream(&f);
    }
    struct EndOnAllowedLog {
        authority: Arc<Mutex<Option<Arc<crate::state::authority::Authority>>>>,
        records: Arc<Mutex<Vec<ReadLogRecord>>>,
    }
    impl ReadLog for EndOnAllowedLog {
        fn write(&self, record: &ReadLogRecord) -> Result<(), Refusal> {
            self.records.lock().unwrap().push(record.clone());
            if matches!(record.outcome, Outcome::Allowed) {
                let guard = self.authority.lock().unwrap();
                let authority = guard.as_ref().unwrap();
                assert_eq!(authority.session_tokenizations(record.session, &domain()).unwrap(), 1);
                authority.revoke(record.session, crate::state::model::EndReason::AdminRevoked).unwrap();
            }
            Ok(())
        }
        fn write_lifecycle(&self, _: &LifecycleRecord) -> Result<(), Refusal> { Ok(()) }
    }
    #[tokio::test]
    async fn tokenize_rechecks_revocation_after_allowed_audit() {
        for streaming in [false, true] {
            let authority = Arc::new(Mutex::new(None));
            let records = Arc::new(Mutex::new(vec![]));
            let f = Fixture::with_log(Box::new(EndOnAllowedLog {
                authority: authority.clone(), records: records.clone(),
            })).await;
            *authority.lock().unwrap() = Some(f.svc.ctx.authority.clone());
            let req = f.query("SELECT nunya_tokenize('email', 'private@example.com')").await;
            assert_tokenize_error(tokenize_rpc(&f, req, streaming).await.unwrap_err(), NyCode::SessionEnded);
            assert_eq!(tokenize_count(&f), 1);
            let records = records.lock().unwrap();
            assert_eq!(records.len(), 1);
            assert!(matches!(records[0].outcome, Outcome::Allowed));
            assert_no_tokenize_upstream(&f);
        }
    }
    #[test]
    fn tokenize_result_type_is_explicit() {
        for ty in [
            ColumnType::Int2, ColumnType::Int4, ColumnType::Uuid, ColumnType::Timestamp,
            ColumnType::Bool, ColumnType::Numeric, ColumnType::Bytea, ColumnType::Json,
            ColumnType::Jsonb, ColumnType::Array(Box::new(ColumnType::Text)),
            ColumnType::Enum("e".into()), ColumnType::Other("unknown".into()),
        ] {
            assert_eq!(tokenize_type_code(&ty).unwrap_err(), NyCode::NotAnalysable);
        }
    }
    async fn wait_for(mut ready: impl FnMut() -> bool) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while !ready() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }
    fn reduce_grant(f: &Fixture) {
        let who = f.svc.authenticate(&f.request((), "alice")).unwrap();
        f.svc
            .ctx
            .authority
            .set_grants(
                who.principal,
                vec![(f.svc.ctx.route.clone(), Mode::ReadOnly)],
            )
            .unwrap();
    }
    fn assert_read_only(error: Status) {
        assert_eq!(
            error.code(),
            nunya_status(NyCode::StatementNotAllowed).code()
        );
        assert!(error.message().contains("NY006"));
    }
    fn revoke(f: &Fixture) {
        f.svc
            .ctx
            .authority
            .revoke(f.alice, crate::state::model::EndReason::AdminRevoked)
            .unwrap();
    }
    fn assert_ended(error: Status, write: bool) {
        assert_eq!(
            error.code(),
            if write {
                tonic::Code::Unknown
            } else {
                tonic::Code::PermissionDenied
            }
        );
        assert!(
            error
                .message()
                .contains(if write { "NY012" } else { "NY011" })
        );
    }
    #[tokio::test]
    async fn buffered_stream_is_discarded_after_revocation() {
        for write in [false, true] {
            let f = Fixture::new().await;
            f.fake
                .burst_stream
                .store(true, std::sync::atomic::Ordering::Relaxed);
            let mut req = f
                .query(if write {
                    "UPDATE customers SET email = @v"
                } else {
                    "SELECT email FROM customers"
                })
                .await;
            if write {
                req.params = Some(prost_types::Struct {
                    fields: [("v".into(), f.token())].into(),
                });
            }
            let who = f.svc.authenticate(&f.request((), "alice")).unwrap();
            let mut signal = f.svc.end_signal(&who).unwrap();
            let mut stream = f
                .svc
                .execute_streaming_sql(f.request(req, "alice"))
                .await
                .unwrap()
                .into_inner();
            wait_for(|| f.fake.emitted.load(std::sync::atomic::Ordering::Relaxed) >= 9).await;
            // No downstream polls: allow the producer to fill all eight slots.
            tokio::time::sleep(Duration::from_millis(100)).await;
            revoke(&f);
            tokio::time::timeout(Duration::from_secs(2), ended(&mut signal))
                .await
                .unwrap();
            assert_ended(stream.next().await.unwrap().unwrap_err(), write);
            assert!(stream.next().await.is_none());
            assert!(stream.next().await.is_none());
        }
    }
    #[tokio::test]
    async fn failed_session_recording_deletes_upstream_sessions() {
        let f = Fixture::new().await;
        f.fake
            .wrong_database
            .store(true, std::sync::atomic::Ordering::Relaxed);
        assert!(
            f.svc
                .create_session(f.request(
                    CreateSessionRequest {
                        database: f.svc.ctx.database.clone(),
                        ..Default::default()
                    },
                    "alice"
                ))
                .await
                .is_err()
        );
        assert_eq!(
            *f.fake.deleted.lock().unwrap(),
            vec!["projects/p/instances/i/databases/other/sessions/fake-0"]
        );
        f.fake.deleted.lock().unwrap().clear();
        assert!(
            f.svc
                .batch_create_sessions(f.request(
                    BatchCreateSessionsRequest {
                        database: f.svc.ctx.database.clone(),
                        session_count: 3,
                        ..Default::default()
                    },
                    "alice"
                ))
                .await
                .is_err()
        );
        assert_eq!(
            *f.fake.deleted.lock().unwrap(),
            vec![
                "projects/p/instances/i/databases/other/sessions/bad",
                "projects/p/instances/i/databases/d/sessions/unrecorded",
            ]
        );
        assert!(
            f.svc
                .sessions
                .lock()
                .unwrap()
                .contains_key("projects/p/instances/i/databases/d/sessions/recorded")
        );
    }
    fn rw_options() -> TransactionOptions {
        TransactionOptions {
            mode: Some(transaction_options::Mode::ReadWrite(Default::default())),
            ..Default::default()
        }
    }
    fn id_selector(id: &[u8]) -> TransactionSelector {
        TransactionSelector {
            selector: Some(transaction_selector::Selector::Id(id.to_vec())),
        }
    }
    async fn begin_rw(f: &Fixture, session: &str) -> Vec<u8> {
        f.svc
            .begin_transaction(f.request(
                BeginTransactionRequest {
                    session: session.into(),
                    options: Some(rw_options()),
                    ..Default::default()
                },
                "alice",
            ))
            .await
            .unwrap()
            .into_inner()
            .id
    }
    async fn txn_dml(f: &Fixture, session: &str, id: &[u8]) {
        let mut req = f.query("UPDATE customers SET email = @v").await;
        req.session = session.into();
        req.transaction = Some(id_selector(id));
        req.params = Some(prost_types::Struct {
            fields: [("v".into(), f.token())].into(),
        });
        f.svc.execute_sql(f.request(req, "alice")).await.unwrap();
    }
    fn txn_commit(session: &str, id: &[u8]) -> CommitRequest {
        CommitRequest {
            session: session.into(),
            transaction: Some(commit_request::Transaction::TransactionId(id.to_vec())),
            ..Default::default()
        }
    }
    async fn other_transaction_ends(commit_b: bool) {
        let f = Fixture::new().await;
        let session = f.session().await;
        let a = begin_rw(&f, &session).await;
        txn_dml(&f, &session, &a).await;
        let b = begin_rw(&f, &session).await;
        assert_ne!(a, b);
        if commit_b {
            f.svc
                .commit(f.request(txn_commit(&session, &b), "alice"))
                .await
                .unwrap();
        } else {
            f.svc
                .rollback(f.request(
                    RollbackRequest {
                        session: session.clone(),
                        transaction_id: b,
                    },
                    "alice",
                ))
                .await
                .unwrap();
        }
        reduce_grant(&f);
        assert_read_only(
            f.svc
                .commit(f.request(txn_commit(&session, &a), "alice"))
                .await
                .unwrap_err(),
        );
        assert!(
            f.fake
                .commits
                .lock()
                .unwrap()
                .iter()
                .all(|c| c.transaction
                    != Some(commit_request::Transaction::TransactionId(a.clone())))
        );
        wait_for(|| {
            f.fake
                .rollbacks
                .lock()
                .unwrap()
                .contains(&(session.clone(), a.clone()))
        })
        .await;
    }
    #[tokio::test]
    async fn multiplexed_rollback_of_b_keeps_dml_of_a() {
        other_transaction_ends(false).await;
    }
    #[tokio::test]
    async fn empty_commit_of_b_keeps_dml_of_a() {
        other_transaction_ends(true).await;
    }
    #[tokio::test]
    async fn clean_transaction_commits_after_downgrade() {
        let f = Fixture::new().await;
        let mut req = f.query("SELECT email FROM customers").await;
        let session = req.session.clone();
        let id = begin_rw(&f, &session).await;
        req.transaction = Some(id_selector(&id));
        f.svc.execute_sql(f.request(req, "alice")).await.unwrap();
        reduce_grant(&f);
        f.svc
            .commit(f.request(txn_commit(&session, &id), "alice"))
            .await
            .unwrap();
        assert_eq!(f.fake.commits.lock().unwrap().len(), 1);
    }
    #[tokio::test]
    async fn unknown_transaction_commit_requires_write() {
        let f = Fixture::new().await;
        let session = f.session().await;
        reduce_grant(&f);
        assert_read_only(
            f.svc
                .commit(f.request(txn_commit(&session, b"unknown"), "alice"))
                .await
                .unwrap_err(),
        );
        assert!(f.fake.commits.lock().unwrap().is_empty());
    }
    #[tokio::test]
    async fn inline_begin_dml_is_tracked() {
        for streaming in [false, true] {
            let f = Fixture::new().await;
            let mut req = f.query("UPDATE customers SET email = @v").await;
            let session = req.session.clone();
            req.transaction = Some(TransactionSelector {
                selector: Some(transaction_selector::Selector::Begin(rw_options())),
            });
            req.params = Some(prost_types::Struct {
                fields: [("v".into(), f.token())].into(),
            });
            let id = if streaming {
                let mut stream = f
                    .svc
                    .execute_streaming_sql(f.request(req, "alice"))
                    .await
                    .unwrap()
                    .into_inner();
                let part = stream.next().await.unwrap().unwrap();
                part.metadata.unwrap().transaction.unwrap().id
            } else {
                f.svc
                    .execute_sql(f.request(req, "alice"))
                    .await
                    .unwrap()
                    .into_inner()
                    .metadata
                    .unwrap()
                    .transaction
                    .unwrap()
                    .id
            };
            reduce_grant(&f);
            assert_read_only(
                f.svc
                    .commit(f.request(txn_commit(&session, &id), "alice"))
                    .await
                    .unwrap_err(),
            );
            assert!(f.fake.commits.lock().unwrap().is_empty());
        }
    }
    #[tokio::test]
    async fn revocation_rolls_back_tracked_transactions() {
        let f = Fixture::new().await;
        let session = f.session().await;
        let id = begin_rw(&f, &session).await;
        txn_dml(&f, &session, &id).await;
        let clean = begin_rw(&f, &session).await;
        revoke(&f);
        wait_for(|| {
            f.fake
                .rollbacks
                .lock()
                .unwrap()
                .contains(&(session.clone(), id.clone()))
        })
        .await;
        wait_for(|| {
            f.fake
                .rollbacks
                .lock()
                .unwrap()
                .contains(&(session.clone(), clean.clone()))
        })
        .await;
        wait_for(|| f.fake.deleted.lock().unwrap().contains(&session)).await;
        assert!(f.svc.transactions.lock().unwrap().is_empty());
    }
    #[tokio::test]
    async fn refused_commit_does_not_wait_for_rollback() {
        let f = Fixture::new().await;
        let session = f.session().await;
        let id = begin_rw(&f, &session).await;
        txn_dml(&f, &session, &id).await;
        f.fake
            .hold_rollback
            .store(true, std::sync::atomic::Ordering::Relaxed);
        reduce_grant(&f);
        let error = tokio::time::timeout(
            Duration::from_secs(1),
            f.svc.commit(f.request(txn_commit(&session, &id), "alice")),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert_read_only(error);
        wait_for(|| {
            f.fake
                .rollbacks
                .lock()
                .unwrap()
                .contains(&(session.clone(), id.clone()))
        })
        .await;
        assert!(f.fake.commits.lock().unwrap().is_empty());
    }
    #[tokio::test]
    async fn commit_after_dml_requires_write_admission() {
        for write in [false, true] {
            let f = Fixture::new().await;
            let mut req = f
                .query(if write {
                    "UPDATE customers SET email = @v"
                } else {
                    "SELECT email FROM customers"
                })
                .await;
            let session = req.session.clone();
            let id = begin_rw(&f, &session).await;
            req.transaction = Some(id_selector(&id));
            if write {
                req.params = Some(prost_types::Struct {
                    fields: [("v".into(), f.token())].into(),
                });
            }
            f.svc.execute_sql(f.request(req, "alice")).await.unwrap();
            reduce_grant(&f);
            let result = f
                .svc
                .commit(f.request(
                    CommitRequest {
                        transaction: Some(commit_request::Transaction::TransactionId(id)),
                        session,
                        ..Default::default()
                    },
                    "alice",
                ))
                .await;
            if write {
                assert_read_only(result.unwrap_err());
            } else {
                result.unwrap();
            }
            assert_eq!(f.fake.commits.lock().unwrap().len(), usize::from(!write));
        }
    }
    #[tokio::test]
    async fn select_reply_after_revocation_has_no_rows() {
        let f = Fixture::new().await;
        let req = f.query("SELECT email FROM customers").await;
        f.fake
            .hold_sql
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let call = f.svc.execute_sql(f.request(req, "alice"));
        let cancel = async {
            wait_for(|| f.fake.seen.lock().unwrap().len() == 1).await;
            revoke(&f);
            f.fake.release.notify_one();
        };
        let (response, ()) =
            tokio::time::timeout(Duration::from_secs(3), async { tokio::join!(call, cancel) })
                .await
                .unwrap();
        assert_ended(response.unwrap_err(), false);
    }
    struct RevokePostAudit {
        target: Mutex<Option<(Arc<crate::state::authority::Authority>, SessionId)>>,
        count: std::sync::atomic::AtomicUsize,
    }
    impl ReadLog for RevokePostAudit {
        fn write_lifecycle(&self, _: &LifecycleRecord) -> Result<(), Refusal> {
            Ok(())
        }
        fn write(&self, _: &ReadLogRecord) -> Result<(), Refusal> {
            if self
                .count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                == 1
                && let Some((authority, session)) = self.target.lock().unwrap().as_ref()
            {
                authority
                    .revoke(*session, crate::state::model::EndReason::AdminRevoked)
                    .unwrap();
            }
            Ok(())
        }
    }
    #[tokio::test]
    async fn select_rechecks_authority_after_audit() {
        let log = Arc::new(RevokePostAudit {
            target: Mutex::new(None),
            count: std::sync::atomic::AtomicUsize::new(0),
        });
        // Use an adapter because the audit sink owns its logger.
        struct Log(Arc<RevokePostAudit>);
        impl ReadLog for Log {
            fn write_lifecycle(&self, _: &LifecycleRecord) -> Result<(), Refusal> {
                Ok(())
            }
            fn write(&self, record: &ReadLogRecord) -> Result<(), Refusal> {
                self.0.write(record)
            }
        }
        let f = Fixture::with_log(Box::new(Log(log.clone()))).await;
        let req = f.query("SELECT email FROM customers").await;
        *log.target.lock().unwrap() = Some((f.svc.ctx.authority.clone(), f.alice));
        assert_ended(
            f.svc
                .execute_sql(f.request(req, "alice"))
                .await
                .unwrap_err(),
            false,
        );
    }
    #[tokio::test]
    async fn stream_stops_after_revocation() {
        let f = Fixture::new().await;
        f.fake
            .hold_stream
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let req = f.query("SELECT email FROM customers").await;
        let mut stream = f
            .svc
            .execute_streaming_sql(f.request(req, "alice"))
            .await
            .unwrap()
            .into_inner();
        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(first.values, vec![f.token()]);
        revoke(&f);
        let error = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert_ended(error, false);
        assert!(stream.next().await.is_none());
        assert_eq!(
            f.fake.streaming.load(std::sync::atomic::Ordering::Relaxed),
            1
        );
    }
    #[tokio::test]
    async fn upstream_commit_loss_and_missing_timestamp_are_audited() {
        for (upstream, expected_reason) in [
            (Some(tonic::Code::Unavailable), Some("upstream_lost")),
            (Some(tonic::Code::Aborted), None),
            (None, Some("no_commit_timestamp")),
        ] {
            let records = Arc::new(Mutex::new(vec![]));
            let lifecycle = Arc::new(Mutex::new(vec![]));
            let f = Fixture::with_log(Box::new(CaptureLog(records, lifecycle.clone()))).await;
            let session = f.session().await;
            let connection = f.svc.watchers.lock().unwrap()[&f.alice].connection;
            *f.fake.commit_error.lock().unwrap() = upstream;
            *f.fake.next_commit.lock().unwrap() = Some(CommitResponse::default());
            let error = f.svc.commit(f.request(CommitRequest { session, ..Default::default() }, "alice")).await.unwrap_err();
            if expected_reason.is_some() { assert_ended(error, true); }
            else { assert_eq!(error.code(), tonic::Code::Aborted); }
            assert_eq!(f.fake.commits.lock().unwrap().len(), 1);
            let audit = lifecycle.lock().unwrap();
            assert_eq!(audit.len(), usize::from(expected_reason.is_some()));
            if let Some(reason) = expected_reason {
                assert_eq!(audit[0].kind, LifecycleKind::WriteOutcomeUnknown);
                assert_eq!(audit[0].reason, reason);
                assert_eq!(audit[0].connection, Some(connection));
            }
        }
    }
    #[tokio::test]
    async fn upstream_dml_loss_is_unknown_but_read_is_unchanged() {
        for write in [false, true] {
            let records = Arc::new(Mutex::new(vec![]));
            let lifecycle = Arc::new(Mutex::new(vec![]));
            let f = Fixture::with_log(Box::new(CaptureLog(records, lifecycle.clone()))).await;
            let mut req = f.query(if write { "UPDATE customers SET email = @v" } else { "SELECT email FROM customers" }).await;
            if write {
                req.transaction = Some(TransactionSelector {
                    selector: Some(transaction_selector::Selector::Begin(rw_options())),
                });
                req.params = Some(prost_types::Struct { fields: [("v".into(), f.token())].into() });
            }
            *f.fake.sql_error.lock().unwrap() = Some(tonic::Code::Unavailable);
            let error = f.svc.execute_sql(f.request(req, "alice")).await.unwrap_err();
            if write { assert_ended(error, true); }
            else { assert_eq!(error.code(), tonic::Code::Unavailable); }
            assert_eq!(f.fake.seen.lock().unwrap().len(), 1);
            let audit = lifecycle.lock().unwrap();
            assert_eq!(audit.len(), usize::from(write));
            if write {
                assert_eq!(audit[0].kind, LifecycleKind::WriteOutcomeUnknown);
                assert_eq!(audit[0].reason, "upstream_lost");
            }
        }
    }
    #[tokio::test]
    async fn commit_in_flight_is_ny012_after_revocation() {
        let f = Fixture::new().await;
        let session = f.session().await;
        f.fake
            .hold_commit
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let call = f.svc.commit(f.request(
            CommitRequest {
                session,
                ..Default::default()
            },
            "alice",
        ));
        let cancel = async {
            wait_for(|| f.fake.commits.lock().unwrap().len() == 1).await;
            revoke(&f);
        };
        let (response, ()) =
            tokio::time::timeout(Duration::from_secs(3), async { tokio::join!(call, cancel) })
                .await
                .unwrap();
        assert_ended(response.unwrap_err(), true);
        assert_eq!(f.fake.commits.lock().unwrap().len(), 1);
    }
    #[tokio::test]
    async fn dml_in_flight_is_ny012_and_select_is_ny011() {
        for write in [true, false] {
            let f = Fixture::new().await;
            let mut req = f
                .query(if write {
                    "UPDATE customers SET email = @v"
                } else {
                    "SELECT email FROM customers"
                })
                .await;
            if write {
                req.params = Some(prost_types::Struct {
                    fields: [("v".into(), f.token())].into(),
                });
            }
            f.fake
                .hold_sql
                .store(true, std::sync::atomic::Ordering::Relaxed);
            let call = f.svc.execute_sql(f.request(req, "alice"));
            let cancel = async {
                wait_for(|| f.fake.seen.lock().unwrap().len() == 1).await;
                revoke(&f);
            };
            let (response, ()) =
                tokio::time::timeout(Duration::from_secs(3), async { tokio::join!(call, cancel) })
                    .await
                    .unwrap();
            assert_ended(response.unwrap_err(), write);
            assert_eq!(f.fake.seen.lock().unwrap().len(), 1);
        }
    }
    struct FailPostAudit(std::sync::atomic::AtomicUsize);
    impl ReadLog for FailPostAudit {
        fn write_lifecycle(&self, _: &LifecycleRecord) -> Result<(), Refusal> {
            self.next()
        }
        fn write(&self, _: &ReadLogRecord) -> Result<(), Refusal> {
            self.next()
        }
    }
    impl FailPostAudit {
        fn next(&self) -> Result<(), Refusal> {
            if self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed) == 0 {
                Ok(())
            } else {
                Err(refusal(NyCode::ReadLogUnavailable))
            }
        }
    }
    #[tokio::test]
    async fn audit_failure_after_forwarded_dml_is_ny012() {
        let f = Fixture::with_log(Box::new(FailPostAudit(
            std::sync::atomic::AtomicUsize::new(0),
        )))
        .await;
        let mut req = f.query("UPDATE customers SET email = @v").await;
        req.params = Some(prost_types::Struct {
            fields: [("v".into(), f.token())].into(),
        });
        assert_ended(
            f.svc
                .execute_sql(f.request(req, "alice"))
                .await
                .unwrap_err(),
            true,
        );
        assert_eq!(f.fake.seen.lock().unwrap().len(), 1);
    }
    #[tokio::test]
    async fn revocation_deletes_upstream_sessions() {
        let f = Fixture::new().await;
        let first = f.session().await;
        let second = f.session().await;
        let bob = f
            .svc
            .create_session(f.request(
                CreateSessionRequest {
                    database: f.svc.ctx.database.clone(),
                    ..Default::default()
                },
                "bob",
            ))
            .await
            .unwrap()
            .into_inner()
            .name;
        assert_eq!(f.svc.ctx.registry.list_session(&f.alice).len(), 1);
        f.svc
            .ctx
            .authority
            .revoke(f.alice, crate::state::model::EndReason::AdminRevoked)
            .unwrap();
        wait_for(|| {
            f.fake.deleted.lock().unwrap().len() == 2
                && f.svc.ctx.registry.list_session(&f.alice).is_empty()
        })
        .await;
        let deleted = f.fake.deleted.lock().unwrap().clone();
        assert!(deleted.contains(&first));
        assert!(deleted.contains(&second));
        assert!(!deleted.contains(&bob));
        assert!(
            !f.svc
                .sessions
                .lock()
                .unwrap()
                .values()
                .any(|s| *s == f.alice)
        );
        let who = f.svc.authenticate(&f.request((), "bob")).unwrap();
        assert!(f.svc.session(&first, &who).is_err());
        f.svc
            .execute_sql(f.request(
                ExecuteSqlRequest {
                    session: bob,
                    sql: "SELECT email FROM customers".into(),
                    ..Default::default()
                },
                "bob",
            ))
            .await
            .unwrap();
    }
    #[tokio::test]
    async fn revoked_session_is_refused_on_next_rpc() {
        let f = Fixture::new().await;
        let req = f.query("SELECT email FROM customers").await;
        f.svc
            .execute_sql(f.request(req.clone(), "alice"))
            .await
            .unwrap();
        f.svc
            .ctx
            .authority
            .revoke(f.alice, crate::state::model::EndReason::AdminRevoked)
            .unwrap();
        let error = f
            .svc
            .execute_sql(f.request(req, "alice"))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::PermissionDenied);
        assert!(error.message().contains("NY003"));
        assert_eq!(f.fake.seen.lock().unwrap().len(), 1);
    }
    #[tokio::test]
    async fn delete_last_session_stops_watcher() {
        let f = Fixture::new().await;
        let name = f.session().await;
        assert_eq!(f.svc.ctx.registry.list_session(&f.alice).len(), 1);
        f.svc
            .delete_session(f.request(DeleteSessionRequest { name: name.clone() }, "alice"))
            .await
            .unwrap();
        wait_for(|| f.svc.ctx.registry.list_session(&f.alice).is_empty()).await;
        assert_eq!(*f.fake.deleted.lock().unwrap(), vec![name]);
        assert!(f.svc.watchers.lock().unwrap().is_empty());
    }
    #[tokio::test]
    async fn read_only_session_refuses_writes() {
        let f = Fixture::new().await;
        reduce_grant(&f);
        let select = f.query("SELECT email FROM customers").await;
        let session = select.session.clone();
        f.svc.execute_sql(f.request(select, "alice")).await.unwrap();
        // A valid token lets SQL lowering reach admission for the blind column.
        let dml = ExecuteSqlRequest {
            session: session.clone(),
            sql: "UPDATE customers SET email = @v".into(),
            params: Some(prost_types::Struct {
                fields: [("v".into(), f.token())].into(),
            }),
            ..Default::default()
        };
        assert_read_only(
            f.svc
                .execute_sql(f.request(dml.clone(), "alice"))
                .await
                .unwrap_err(),
        );
        assert_read_only(
            f.svc
                .execute_streaming_sql(f.request(dml.clone(), "alice"))
                .await
                .err()
                .unwrap(),
        );
        assert_read_only(
            f.svc
                .execute_batch_dml(f.request(
                    ExecuteBatchDmlRequest {
                        session: session.clone(),
                        statements: vec![execute_batch_dml_request::Statement {
                            sql: dml.sql,
                            params: dml.params,
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                    "alice",
                ))
                .await
                .unwrap_err(),
        );
        assert_read_only(
            f.svc
                .commit(f.request(
                    CommitRequest {
                        session: session.clone(),
                        mutations: vec![Mutation::default()],
                        ..Default::default()
                    },
                    "alice",
                ))
                .await
                .unwrap_err(),
        );
        assert_read_only(
            f.svc
                .batch_write(f.request(
                    BatchWriteRequest {
                        session: session.clone(),
                        ..Default::default()
                    },
                    "alice",
                ))
                .await
                .err()
                .unwrap(),
        );
        assert_eq!(f.fake.seen.lock().unwrap().len(), 1);
        assert_eq!(
            f.fake.streaming.load(std::sync::atomic::Ordering::Relaxed),
            0
        );
        assert!(f.fake.batches.lock().unwrap().is_empty());
        assert!(f.fake.commits.lock().unwrap().is_empty());
        f.svc
            .commit(f.request(
                CommitRequest {
                    transaction: Some(commit_request::Transaction::SingleUseTransaction(rw_options())),
                    session,
                    ..Default::default()
                },
                "alice",
            ))
            .await
            .unwrap();
        assert_eq!(f.fake.commits.lock().unwrap().len(), 1);
    }
    #[tokio::test]
    async fn grant_reduced_mid_session_refuses_dml() {
        let f = Fixture::new().await;
        let mut req = f.query("UPDATE customers SET email = @v").await;
        req.params = Some(prost_types::Struct {
            fields: [("v".into(), f.token())].into(),
        });
        f.svc
            .execute_sql(f.request(req.clone(), "alice"))
            .await
            .unwrap();
        reduce_grant(&f);
        assert_read_only(
            f.svc
                .execute_sql(f.request(req, "alice"))
                .await
                .unwrap_err(),
        );
        assert_eq!(f.fake.seen.lock().unwrap().len(), 1);
    }
}

#[cfg(test)]
mod tls_rotation_tests {
    use super::*;
    use crate::config_reload::{tests::{Fixture, certificate, connector, connect_tls}, ConfigReloader, Transports};
    use futures::StreamExt;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    async fn stalled_clients_do_not_block(cap: usize, count: usize, check_oldest: bool) {
        let f = Fixture::new();
        let cert = certificate(&f);
        let config = f.load(&f.text);
        let transports = Transports::new(&config).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let incoming = tls_incoming_with(listener, transports.spanner.clone(), cap, TLS_HANDSHAKE_TIMEOUT);
        let server = tokio::spawn(async move {
            futures::pin_mut!(incoming);
            while let Some(Ok(mut io)) = incoming.next().await {
                let mut byte = [0];
                if io.read_exact(&mut byte).await.is_ok() {
                    let _ = io.write_all(&byte).await;
                }
            }
        });
        let mut stalled = Vec::new();
        for _ in 0..count {
            stalled.push(tokio::net::TcpStream::connect(addr).await.unwrap());
        }
        let client = connector(&[cert], true);
        let result = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            let mut io = connect_tls(addr, &client).await;
            io.write_all(b"x").await.unwrap();
            let mut response = [0];
            io.read_exact(&mut response).await.unwrap();
            assert_eq!(response, *b"x");
            if check_oldest {
                let result = stalled[0].read(&mut response).await;
                assert!(matches!(result, Ok(0)) || result.is_err());
                // FIFO eviction preserves the other stalled socket.
                assert!(matches!(stalled[1].try_read(&mut response), Err(e)
                    if e.kind() == std::io::ErrorKind::WouldBlock));
            }
        }).await;
        server.abort();
        let _ = server.await;
        assert!(result.is_ok(), "TLS client was blocked by stalled handshakes");
    }
    #[tokio::test]
    async fn seventy_stalled_connections_allow_prompt_tls_echo() {
        stalled_clients_do_not_block(TLS_PENDING_CAP, 70, false).await;
    }
    #[tokio::test]
    async fn handshake_cap_evicts_oldest_stalled_connection() {
        stalled_clients_do_not_block(2, 2, true).await;
    }
    #[tokio::test]
    async fn incoming_rotates_certificates_with_h2_and_independent_handshakes() {
        let f = Fixture::new(); let a = certificate(&f); let hub = f.hub();
        let config = f.load(&f.text);
        let transports = Transports::new(&config).unwrap();
        let reload = ConfigReloader::new(config, Arc::downgrade(&hub)); reload.attach_transports(transports.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let incoming = tls_incoming(listener, transports.spanner.clone());
        let server = tokio::spawn(async move {
            futures::pin_mut!(incoming);
            let mut sessions = tokio::task::JoinSet::new();
            while let Some(Ok(mut io)) = incoming.next().await {
                sessions.spawn(async move {
                    let mut byte = [0]; io.read_exact(&mut byte).await.unwrap(); io.write_all(&byte).await.unwrap();
                });
            }
        });
        let b = certificate(&f); let client = connector(&[a.clone(), b.clone()], true);
        let stalled = tokio::net::TcpStream::connect(addr).await.unwrap();
        let mut old = connect_tls(addr, &client).await;
        assert_eq!(old.get_ref().1.peer_certificates().unwrap()[0], a);
        assert_eq!(old.get_ref().1.alpn_protocol(), Some(&b"h2"[..]));
        reload.reload().await; assert_eq!(reload.last_reload(), "applied");
        let new = connect_tls(addr, &client).await;
        assert_eq!(new.get_ref().1.peer_certificates().unwrap()[0], b);
        assert_eq!(new.get_ref().1.alpn_protocol(), Some(&b"h2"[..]));
        old.write_all(b"x").await.unwrap(); let mut response = [0]; old.read_exact(&mut response).await.unwrap(); assert_eq!(response, *b"x");
        drop(stalled); server.abort(); let _ = server.await;
    }
}
