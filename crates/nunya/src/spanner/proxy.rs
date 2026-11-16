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
    policy::{DomainName, Policy, TokenizeSetting},
    readlog::{LifecycleRecord, LifecycleKind, LifecycleActor, Outcome, ReadLog, ReadLogRecord, statement_fingerprint},
    schema::Snapshot,
    state::authority::{AdmitError, Operation, TokenizeError, TokenizeLimits},
    token::{SessionId, StandardCodec, TokenCodec, keys::RootKey},
    value::ColumnType,
};
use futures::{Stream, StreamExt};
use google_cloud_googleapis::spanner::v1::{spanner_client::SpannerClient, *};
use prost_types::{ListValue, Value};
use std::{
    collections::{HashMap, HashSet},
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
type ResultStream = Pin<Box<dyn Stream<Item = Result<PartialResultSet, Status>> + Send + 'static>>;
/// Native Spanner service and its gateway-created session ownership registry.
pub struct NunyaSpanner {
    ctx: Arc<SpannerContext>,
    channel: Channel,
    sessions: Arc<Mutex<HashMap<String, SessionId>>>,
    watchers: Arc<Mutex<HashMap<SessionId, SessionWatcher>>>,
    dml_sessions: Arc<Mutex<HashSet<String>>>,
    provider: Option<Arc<dyn gcp_auth::TokenProvider>>,
}
struct SessionWatcher {
    connection: u64,
    stop: tokio::sync::oneshot::Sender<()>,
    ended: tokio::sync::watch::Receiver<bool>,
}
fn ended_status(write: bool) -> Status {
    nunya_status(if write {
        NyCode::WriteOutcomeUnknown
    } else {
        NyCode::SessionEnded
    })
}
fn after_forward(error: Status, write: bool) -> Status {
    if write { ended_status(true) } else { error }
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
        result = future => result.map_err(remote),
    }
}
// The receiver is discarded immediately on revocation, including queued results.
struct EndCheckedStream {
    receiver: Option<tokio::sync::mpsc::Receiver<Result<PartialResultSet, Status>>>,
    signal: tokio::sync::watch::Receiver<bool>,
    wake: Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
    write: bool,
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
            return std::task::Poll::Ready(Some(Err(ended_status(this.write))));
        }
        let result = match this.receiver.as_mut() {
            Some(receiver) => receiver.poll_recv(cx),
            None => return std::task::Poll::Ready(None),
        };
        if *this.signal.borrow() {
            this.receiver.take();
            return std::task::Poll::Ready(Some(Err(ended_status(this.write))));
        }
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
    if let Some(provider) = provider {
        let token = provider
            .token(&["https://www.googleapis.com/auth/spanner.data"])
            .await
            .map_err(|_| nunya_status(NyCode::TokenNotValid))?;
        let token = format!("Bearer {}", token.as_str())
            .parse()
            .map_err(|_| nunya_status(NyCode::TokenNotValid))?;
        // Reuse the channel and replace only its interceptor token.
        Ok(SpannerClient::with_interceptor(
            channel.clone(),
            AuthInterceptor { token: Some(token) },
        ))
    } else {
        Ok(SpannerClient::with_interceptor(
            channel.clone(),
            AuthInterceptor::default(),
        ))
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
        dml_sessions: Arc::new(Mutex::new(HashSet::new())),
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
    tls: Option<tonic::transport::ServerTlsConfig>,
) -> Result<(), Refusal> {
    let mut builder = tonic::transport::Server::builder();
    if let Some(tls) = tls {
        builder = builder
            .tls_config(tls)
            .map_err(|_| refusal(NyCode::StatementNotAllowed))?;
    }
    builder
        .add_service(SpannerServer::new(svc))
        .serve(addr)
        .await
        .map_err(|_| refusal(NyCode::StatementNotAllowed))
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
            let dml_sessions = self.dml_sessions.clone();
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
                        dml_sessions
                            .lock()
                            .map_err(|_| invalid())?
                            .retain(|name| !names.contains(name));
                        if let Some(watchers) = watchers.upgrade() {
                            watchers.lock().map_err(|_| invalid())?.remove(&session);
                        }
                        names
                    };
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
    fn mark_dml(&self, session: &str) -> Result<(), Status> {
        self.dml_sessions
            .lock()
            .map_err(|_| invalid())?
            .insert(session.into());
        Ok(())
    }
    fn clear_dml(&self, session: &str) -> Result<(), Status> {
        self.dml_sessions
            .lock()
            .map_err(|_| invalid())?
            .remove(session);
        Ok(())
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
    fn configuration(&self) -> Result<(Arc<Policy>, Arc<Snapshot>), Status> {
        let policy = self.ctx.policy.read().map_err(|_| invalid())?.clone();
        let snapshot = self.ctx.snapshot.read().map_err(|_| invalid())?.clone();
        if (self.ctx.clock)().saturating_sub(snapshot.taken_at_unix_ms) > policy.snapshot_max_age_ms
        {
            return Err(nunya_status(NyCode::SchemaStale));
        }
        Ok((policy, snapshot))
    }
    fn audit(
        ctx: &SpannerContext,
        watchers: &Mutex<HashMap<SessionId, SessionWatcher>>,
        who: &Authenticated,
        label: &str,
        rows: u64,
        tokens: Vec<String>,
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
                policy_revision: None,
                config_revision: None,
                statement_fingerprint: statement_fingerprint(
                    &crate::readlog::redact_tokenize_args(label),
                ),
                statement: "Spanner operation".into(),
                outcome: Outcome::Allowed,
                rows,
                tokens_released: tokens,
            })
            .map_err(|_| nunya_status(NyCode::ReadLogUnavailable))
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
        let value = values::from_proto(ty, value).map_err(status)?;
        let keys = &who.keys;
        let range = policy.domains.get(domain).and_then(|d| d.range.as_ref());
        let result = if decode {
            self.ctx.codec.decode(keys, domain, ty, range, &value)
        } else {
            self.ctx.codec.encode(keys, domain, ty, range, &value)
        }
        .map_err(status)?;
        values::to_proto(ty, &result).map_err(status)
    }
    fn sql(
        &self,
        who: &Authenticated,
        sql: &str,
        params: &mut Option<prost_types::Struct>,
        policy: &Policy,
        snapshot: &Snapshot,
    ) -> Result<(RewritePlan, Vec<String>, StatementKind), Status> {
        let (stmt, names) = lower_gsql(sql, snapshot, policy).map_err(status)?;
        let plan = check(&stmt, policy, snapshot).map_err(status)?;

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
    fn local_tokenize(
        &self,
        who: &Authenticated,
        policy: &Policy,
        plan: &RewritePlan,
        names: &[String],
        params: &Option<prost_types::Struct>,
        param_types: &HashMap<String, Type>,
    ) -> Result<Option<ResultSet>, Status> {
        let Some((domain, arg)) = &plan.tokenize else {
            return Ok(None);
        };
        let setting = policy.domains.get(domain).ok_or_else(denied)?;
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
            TokenizeSetting::Off => return Err(denied()),
        };
        let (ty, value) = match arg {
            ValueSource::Param(index) => {
                let name = index
                    .checked_sub(1)
                    .and_then(|i| names.get(i as usize))
                    .ok_or_else(invalid)?;
                let value = params
                    .as_ref()
                    .and_then(|p| p.fields.get(name))
                    .ok_or_else(invalid)?
                    .clone();
                let ty = param_types
                    .get(name)
                    .map(values::column_type)
                    .unwrap_or(ColumnType::Text);
                (ty, value)
            }
            ValueSource::Literal { text, .. } => (
                ColumnType::Text,
                Value {
                    kind: Some(prost_types::value::Kind::StringValue(text.clone())),
                },
            ),
            ValueSource::Column { .. } | ValueSource::Null | ValueSource::Computed(_) => {
                return Err(invalid());
            }
        };
        if ty != ColumnType::Int8 && !ty.is_textual() {
            return Err(invalid());
        }
        let value = self.convert(who, policy, domain, &ty, &value, false)?;
        self.ctx
            .authority
            .record_tokenization(who.session, domain, &limits)
            .map_err(|error| {
                nunya_status(match error {
                    TokenizeError::LimitReached => NyCode::TokenizeLimit,
                    TokenizeError::Ended | TokenizeError::Store => NyCode::SessionEnded,
                })
            })?;
        let code = if ty == ColumnType::Int8 {
            TypeCode::Int64
        } else {
            TypeCode::String
        };
        Ok(Some(ResultSet {
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
        }))
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
    #[allow(clippy::too_many_arguments)]
    fn stream(
        &self,
        who: Authenticated,
        policy: Arc<Policy>,
        plan: RewritePlan,
        label: String,
        mut upstream: tonic::Streaming<PartialResultSet>,
        mut signal: tokio::sync::watch::Receiver<bool>,
        write: bool,
    ) -> ResultStream {
        let ctx = self.ctx.clone();
        let watchers = self.watchers.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        let client_signal = signal.clone();
        tokio::spawn(async move {
            let mut count = 0;
            let mut tokens = vec![];
            let work = async {
                let mut reassembler = Reassembler::new();
                let mut types = None;
                loop {
                    let part = tokio::select! {
                        biased;
                        _ = ended(&mut signal) => return Err(ended_status(write)),
                        part = upstream.next() => part,
                    };
                    let Some(part) = part else {
                        break;
                    };
                    ctx.authority
                        .admit(who.session, Operation::Read)
                        .map_err(|_| ended_status(write))?;
                    let (events, resume) =
                        reassembler.push(part.map_err(remote)?).map_err(|e| after_forward(status(e), write))?;
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
                        tokio::select! {
                            biased;
                            _ = ended(&mut signal) => return Err(ended_status(write)),
                            result = tx.send(Ok(stream::emit(metadata, rows, resume, stats))) => {
                                result.map_err(|_| after_forward(Status::cancelled("stream closed"), write))?;
                            }
                        }
                        count += row_count;
                        tokens.extend(chunk_tokens);
                    }
                }
                reassembler.finish().map_err(|e| after_forward(status(e), write))?;
                Ok::<(), Status>(())
            }
            .await;
            drop(upstream);
            let audit =
                Self::audit(&ctx, &watchers, &who, &label, count, tokens).map_err(|e| after_forward(e, write));
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
        Box::pin(EndCheckedStream::new(rx, client_signal, write))
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
        self.clear_dml(&req.name)?;
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
        let (policy, snapshot) = self.configuration()?;
        let label = req.sql.clone();
        let (plan, names, kind) = self.sql(&who, &req.sql, &mut req.params, &policy, &snapshot)?;
        let write = matches!(
            kind,
            StatementKind::Insert | StatementKind::Update | StatementKind::Delete
        );
        match kind {
            StatementKind::Insert | StatementKind::Update | StatementKind::Delete => {
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
        Self::audit(&self.ctx, &self.watchers, &who, &label, 0, vec![])?;
        if let Some(result) =
            self.local_tokenize(&who, &policy, &plan, &names, &req.params, &req.param_types)?
        {
            Self::audit(&self.ctx, &self.watchers, &who, &label, 1, local_tokens(&result)?)?;
            return Ok(Response::new(result));
        }
        let mut client = self.client().await?;
        if write {
            self.mark_dml(&req.session)?;
        }
        let mut result = cancellable(&mut signal, write, client.execute_sql(req))
            .await?
            .into_inner();
        let tokens = self
            .rows(&who, &policy, &plan, &mut result)
            .map_err(|e| after_forward(e, write))?;
        Self::audit(&self.ctx, &self.watchers, &who, &label, result.rows.len() as u64, tokens)
            .map_err(|e| after_forward(e, write))?;
        self.check_response(&who, &signal, write)?;
        Ok(Response::new(result))
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
        let (policy, snapshot) = self.configuration()?;
        let label = req.sql.clone();
        let (plan, names, kind) = self.sql(&who, &req.sql, &mut req.params, &policy, &snapshot)?;
        let write = matches!(
            kind,
            StatementKind::Insert | StatementKind::Update | StatementKind::Delete
        );
        match kind {
            StatementKind::Insert | StatementKind::Update | StatementKind::Delete => {
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
        Self::audit(&self.ctx, &self.watchers, &who, &label, 0, vec![])?;
        if let Some(result) =
            self.local_tokenize(&who, &policy, &plan, &names, &req.params, &req.param_types)?
        {
            Self::audit(&self.ctx, &self.watchers, &who, &label, 1, local_tokens(&result)?)?;
            return Ok(Response::new(Box::pin(tokio_stream::iter(vec![Ok(
                PartialResultSet {
                    metadata: result.metadata,
                    values: result.rows.into_iter().flat_map(|r| r.values).collect(),
                    ..Default::default()
                },
            )]))));
        }
        let mut client = self.client().await?;
        if write {
            self.mark_dml(&req.session)?;
        }
        let upstream = cancellable(&mut signal, write, client.execute_streaming_sql(req))
            .await?
            .into_inner();
        Ok(Response::new(
            self.stream(who, policy, plan, label, upstream, signal, write),
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
        let (policy, snapshot) = self.configuration()?;
        let mut plans = vec![];
        let mut labels = vec![];
        for statement in &mut req.statements {
            labels.push(statement.sql.clone());
            let (plan, _, kind) = self.sql(
                &who,
                &statement.sql,
                &mut statement.params,
                &policy,
                &snapshot,
            )?;
            match kind {
                StatementKind::Insert | StatementKind::Update | StatementKind::Delete => {}
                StatementKind::Select
                | StatementKind::Begin
                | StatementKind::Commit
                | StatementKind::Rollback
                | StatementKind::Savepoint
                | StatementKind::ReleaseSavepoint
                | StatementKind::RollbackToSavepoint
                | StatementKind::SessionNoop { .. }
                | StatementKind::Tokenize { .. }
                | StatementKind::Refused(_) => return Err(denied()),
            }
            plans.push(plan);
        }
        self.admit_write(&who)?;
        for label in &labels {
            Self::audit(&self.ctx, &self.watchers, &who, label, 0, vec![])?;
        }
        let mut client = self.client().await?;
        self.mark_dml(&req.session)?;
        let mut result = cancellable(&mut signal, true, client.execute_batch_dml(req))
            .await?
            .into_inner();
        if result.result_sets.len() > plans.len() {
            return Err(ended_status(true));
        }
        for (i, set) in result.result_sets.iter_mut().enumerate() {
            let tokens = self
                .rows(&who, &policy, &plans[i], set)
                .map_err(|e| after_forward(e, true))?;
            Self::audit(&self.ctx, &self.watchers, &who, &labels[i], set.rows.len() as u64, tokens)
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
    }
    async fn read(&self, req: Request<ReadRequest>) -> Result<Response<ResultSet>, Status> {
        let who = self.authenticate(&req)?;
        let mut req = req.into_inner();
        self.session(&req.session, &who)?;
        let mut signal = self.end_signal(&who)?;
        read_features(&req)?;
        let (policy, snapshot) = self.configuration()?;
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
        Self::audit(&self.ctx, &self.watchers, &who, "Read", 0, vec![])?;
        let mut result = cancellable(&mut signal, false, self.client().await?.read(req))
            .await?
            .into_inner();
        let tokens = self.rows(&who, &policy, &plan, &mut result)?;
        Self::audit(&self.ctx, &self.watchers, &who, "Read", result.rows.len() as u64, tokens)?;
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
        let (policy, snapshot) = self.configuration()?;
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
        Self::audit(&self.ctx, &self.watchers, &who, "Read", 0, vec![])?;
        let upstream = cancellable(&mut signal, false, self.client().await?.streaming_read(req))
            .await?
            .into_inner();
        Ok(Response::new(self.stream(
            who,
            policy,
            plan,
            "Read".into(),
            upstream,
            signal,
            false,
        )))
    }
    async fn begin_transaction(
        &self,
        req: Request<BeginTransactionRequest>,
    ) -> Result<Response<Transaction>, Status> {
        let who = self.authenticate(&req)?;
        let req = req.into_inner();
        self.session(&req.session, &who)?;
        let mut signal = self.end_signal(&who)?;
        Ok(Response::new(
            cancellable(
                &mut signal,
                false,
                self.client().await?.begin_transaction(req),
            )
            .await?
            .into_inner(),
        ))
    }
    async fn rollback(&self, req: Request<RollbackRequest>) -> Result<Response<()>, Status> {
        let who = self.authenticate(&req)?;
        let req = req.into_inner();
        self.session(&req.session, &who)?;
        let mut signal = self.end_signal(&who)?;
        let mut client = self.client().await?;
        let session = req.session.clone();
        let result = cancellable(&mut signal, false, client.rollback(req)).await;
        self.clear_dml(&session)?;
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
        let mut signal = self.end_signal(&who)?;
        let (policy, snapshot) = self.configuration()?;
        let dml_forwarded = self
            .dml_sessions
            .lock()
            .map_err(|_| invalid())?
            .contains(&req.session);
        if !req.mutations.is_empty() || dml_forwarded {
            self.admit_write(&who)?;
        }
        for mutation in &mut req.mutations {
            let lowered =
                spanner_struct::lower_mutation(mutation, &snapshot, &policy).map_err(status)?;
            let plan = check(&lowered.stmt, &policy, &snapshot).map_err(status)?;
            if let Some(mutation::Operation::Delete(delete)) = &mutation.operation {
                check_key_tokens(
                    delete.key_set.as_ref(),
                    &policy,
                    &plan,
                    &lowered.positions,
                    true,
                )?;
            }
            let rewrites = self.structured(&who, &policy, &plan, &lowered.positions, |p| {
                mutation_value(mutation, p)
            })?;
            for (index, value) in rewrites {
                *mutation_value_mut(mutation, &lowered.positions[index]).ok_or_else(invalid)? =
                    value;
            }
        }
        Self::audit(&self.ctx, &self.watchers, &who, "Commit", 0, vec![])?;
        let mut client = self.client().await?;
        let session = req.session.clone();
        let result = cancellable(&mut signal, true, client.commit(req)).await;
        self.clear_dml(&session)
            .map_err(|e| after_forward(e, true))?;
        Ok(Response::new(result?.into_inner()))
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
    set: Option<&google_cloud_googleapis::spanner::v1::KeySet>,
    policy: &Policy,
    plan: &RewritePlan,
    positions: &[ValuePosition],
    delete: bool,
) -> Result<(), Status> {
    use google_cloud_googleapis::spanner::v1::key_range::{EndKeyType, StartKeyType};
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
    #[derive(Default)]
    struct Fake {
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
            self.streaming
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let hold = self.hold_stream.load(std::sync::atomic::Ordering::Relaxed);
            let burst = self.burst_stream.load(std::sync::atomic::Ordering::Relaxed);
            if hold || burst {
                let mut first = PartialResultSet {
                    metadata: Some(ResultSetMetadata {
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
            Ok(Response::new(Box::pin(futures::stream::iter(vec![
                Ok(PartialResultSet {
                    metadata: Some(ResultSetMetadata {
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
            let dml = req.sql.starts_with("UPDATE ")
                || req.sql.starts_with("INSERT ")
                || req.sql.starts_with("DELETE ");
            self.seen.lock().unwrap().push(req);
            if self.hold_sql.load(std::sync::atomic::Ordering::Relaxed) {
                self.release.notified().await;
            }
            if dml {
                return Ok(Response::new(ResultSet::default()));
            }
            Ok(Response::new(ResultSet {
                metadata: Some(ResultSetMetadata {
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
            self.batches.lock().unwrap().push(req.into_inner());
            Ok(Response::new(ExecuteBatchDmlResponse::default()))
        }
        async fn commit(
            &self,
            req: Request<CommitRequest>,
        ) -> Result<Response<CommitResponse>, Status> {
            self.commits.lock().unwrap().push(req.into_inner());
            if self.hold_commit.load(std::sync::atomic::Ordering::Relaxed) {
                self.release.notified().await;
            }
            Ok(Response::new(CommitResponse::default()))
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
            NunyaSpanner::audit(&f.svc.ctx, &f.svc.watchers, &who, "Read", 0, vec![]).unwrap();
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
        use google_cloud_googleapis::spanner::v1::{KeyRange, KeySet, key_range};
        let f = Fixture::new().await;
        let mut policy = (**f.svc.ctx.policy.read().unwrap()).clone();
        policy.columns.clear();
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
    #[tokio::test]
    async fn tokenization_uses_durable_session_counter() {
        let f = Fixture::new().await;
        let who = f.svc.authenticate(&f.request((), "alice")).unwrap();
        let policy = f.svc.ctx.policy.read().unwrap().clone();
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
                .local_tokenize(&who, &policy, &plan, &[], &None, &HashMap::new())
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
        assert!(tokenize().unwrap_err().message().contains("NY007"));
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
