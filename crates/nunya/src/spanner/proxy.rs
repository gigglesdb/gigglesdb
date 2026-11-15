//! Authenticated, fail-closed proxy for the native Spanner API.
// tonic::Status is large by design.
#![allow(clippy::result_large_err)]
use super::{
    auth::{Authenticated, Authenticator},
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
    pgwire::auth::ScramVerifier,
    policy::{DomainName, Policy, TokenizeSetting},
    readlog::{Outcome, ReadLog, ReadLogRecord, statement_fingerprint},
    schema::Snapshot,
    task::TaskStore,
    token::{
        StandardCodec, TokenCodec,
        keys::{RootKey, derive_scope},
    },
    value::ColumnType,
};
use futures::{Stream, StreamExt};
use google_cloud_googleapis::spanner::v1::{spanner_client::SpannerClient, *};
use prost_types::{ListValue, Value};
use std::{
    collections::{BTreeMap, HashMap},
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, Mutex, RwLock},
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
    /// Reloadable policy.
    pub policy: RwLock<Arc<Policy>>,
    /// Reloadable schema.
    pub snapshot: RwLock<Arc<Snapshot>>,
    /// Root token key.
    pub root_key: RootKey,
    /// Persistent task scopes.
    pub tasks: TaskStore,
    /// Mandatory read audit sink.
    pub readlog: Box<dyn ReadLog>,
    /// Standing credential verifiers.
    pub standing: BTreeMap<String, ScramVerifier>,
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
    client: Client,
    channel: Channel,
    sessions: Mutex<HashMap<String, String>>,
    auth: Authenticator,
    provider: Option<Arc<dyn gcp_auth::TokenProvider>>,
    tokenize_counts: Mutex<HashMap<(String, DomainName), u32>>,
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
/// Connect to the configured upstream; downstream credentials never leave Nunya.
pub async fn build(ctx: Arc<SpannerContext>) -> Result<NunyaSpanner, Refusal> {
    let (channel, provider) = connect(&ctx.upstream).await?;
    Ok(NunyaSpanner {
        ctx,
        client: SpannerClient::with_interceptor(channel.clone(), AuthInterceptor::default()),
        channel,
        sessions: Mutex::new(HashMap::new()),
        auth: Authenticator::default(),
        provider,
        tokenize_counts: Mutex::new(HashMap::new()),
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
        self.auth
            .authenticate(
                req.metadata(),
                &self.ctx.tasks,
                &self.ctx.standing,
                (self.ctx.clock)(),
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
        owned(&sessions, name, &who.principal).map_err(status)
    }
    fn record_session(&self, name: &str, who: &Authenticated) -> Result<(), Status> {
        self.session_database(name)?;
        let mut sessions = self.sessions.lock().map_err(|_| invalid())?;
        if sessions.get(name).is_some_and(|p| p != &who.principal) {
            return Err(nunya_status(NyCode::TokenNotValid));
        }
        sessions.insert(name.into(), who.principal.clone());
        Ok(())
    }
    async fn client(&self) -> Result<Client, Status> {
        if let Some(provider) = &self.provider {
            let token = provider
                .token(&["https://www.googleapis.com/auth/spanner.data"])
                .await
                .map_err(|_| nunya_status(NyCode::TokenNotValid))?;
            let token = format!("Bearer {}", token.as_str())
                .parse()
                .map_err(|_| nunya_status(NyCode::TokenNotValid))?;
            // Reuse the channel and replace only its interceptor token.
            Ok(SpannerClient::with_interceptor(
                self.channel.clone(),
                AuthInterceptor { token: Some(token) },
            ))
        } else {
            Ok(self.client.clone())
        }
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
        who: &Authenticated,
        label: &str,
        rows: u64,
        tokens: Vec<String>,
    ) -> Result<(), Status> {
        // Fingerprint the original agent statement; log no literals, tokens, or upstream values.
        ctx.readlog
            .write(&ReadLogRecord {
                ts_unix_ms: (ctx.clock)(),
                task: who.task_id,
                credential: who.principal.clone(),
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
        let keys = derive_scope(&self.ctx.root_key, &who.salt);
        let range = policy.domains.get(domain).and_then(|d| d.range.as_ref());
        let result = if decode {
            self.ctx.codec.decode(&keys, domain, ty, range, &value)
        } else {
            self.ctx.codec.encode(&keys, domain, ty, range, &value)
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
        let keys = derive_scope(&ctx.root_key, &who.salt);
        stream::rewrite_row(row, &plan.outputs, types, &mut |domain, ty, value| {
            ctx.codec.encode(
                &keys,
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
        let limit = match setting.tokenize {
            TokenizeSetting::On { limit_per_task } => limit_per_task,
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
        if let Some(id) = who.task_id {
            self.ctx
                .tasks
                .count_tokenize(&id, domain, limit)
                .map_err(status)?;
        } else {
            let mut counts = self.tokenize_counts.lock().map_err(|_| invalid())?;
            let count = counts
                .entry((who.principal.clone(), domain.clone()))
                .or_default();
            *count = count
                .checked_add(1)
                .filter(|v| *v <= limit)
                .ok_or_else(|| nunya_status(NyCode::TokenizeLimit))?;
        }
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
    fn stream(
        &self,
        who: Authenticated,
        policy: Arc<Policy>,
        plan: RewritePlan,
        label: String,
        mut upstream: tonic::Streaming<PartialResultSet>,
    ) -> ResultStream {
        let ctx = self.ctx.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        tokio::spawn(async move {
            let mut count = 0;
            let mut tokens = vec![];
            let work = async {
                let mut reassembler = Reassembler::new();
                let mut types = None;
                while let Some(part) = upstream.next().await {
                    let (events, resume) =
                        reassembler.push(part.map_err(remote)?).map_err(status)?;
                    let mut metadata = None;
                    let mut rows = vec![];
                    let mut stats = None;
                    for event in events {
                        match event {
                            Event::Metadata(m) => {
                                types = Some(Self::types(Some(&m), &plan)?);
                                metadata = Some(m);
                            }
                            Event::Row(mut row) => {
                                tokens.extend(Self::rewrite(
                                    &ctx,
                                    &who,
                                    &policy,
                                    &plan,
                                    types.as_deref().ok_or_else(invalid)?,
                                    &mut row,
                                )?);
                                count += 1;
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
                        tx.send(Ok(stream::emit(metadata, rows, resume, stats)))
                            .await
                            .map_err(|_| Status::cancelled("stream closed"))?;
                    }
                }
                reassembler.finish().map_err(status)?;
                Ok::<(), Status>(())
            }
            .await;
            let audit = Self::audit(&ctx, &who, &label, count, tokens);
            let work = audit.and(work);
            if let Err(error) = work {
                let _ = tx.send(Err(error)).await;
            }
        });
        Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx))
    }
}
fn owned(
    sessions: &HashMap<String, String>,
    session: &str,
    principal: &str,
) -> Result<(), Refusal> {
    if sessions
        .get(session)
        .is_some_and(|owner| owner == principal)
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
        self.record_session(&response.name, &who)?;
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
        for session in &response.session {
            self.session_database(&session.name)?;
        }
        for session in &response.session {
            self.record_session(&session.name, &who)?;
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
        self.sessions
            .lock()
            .map_err(|_| invalid())?
            .remove(&req.name);
        Ok(Response::new(()))
    }
    async fn execute_sql(
        &self,
        req: Request<ExecuteSqlRequest>,
    ) -> Result<Response<ResultSet>, Status> {
        let who = self.authenticate(&req)?;
        let mut req = req.into_inner();
        self.session(&req.session, &who)?;
        sql_features(&req)?;
        let (policy, snapshot) = self.configuration()?;
        let label = req.sql.clone();
        let (plan, names, _) = self.sql(&who, &req.sql, &mut req.params, &policy, &snapshot)?;
        let label = if plan.tokenize.is_some() {
            "Spanner nunya_tokenize".to_owned()
        } else {
            label
        };
        Self::audit(&self.ctx, &who, &label, 0, vec![])?;
        if let Some(result) =
            self.local_tokenize(&who, &policy, &plan, &names, &req.params, &req.param_types)?
        {
            Self::audit(&self.ctx, &who, &label, 1, local_tokens(&result)?)?;
            return Ok(Response::new(result));
        }
        let mut result = self
            .client()
            .await?
            .execute_sql(req)
            .await
            .map_err(remote)?
            .into_inner();
        let tokens = self.rows(&who, &policy, &plan, &mut result)?;
        Self::audit(&self.ctx, &who, &label, result.rows.len() as u64, tokens)?;
        Ok(Response::new(result))
    }
    async fn execute_streaming_sql(
        &self,
        req: Request<ExecuteSqlRequest>,
    ) -> Result<Response<Self::ExecuteStreamingSqlStream>, Status> {
        let who = self.authenticate(&req)?;
        let mut req = req.into_inner();
        self.session(&req.session, &who)?;
        sql_features(&req)?;
        let (policy, snapshot) = self.configuration()?;
        let label = req.sql.clone();
        let (plan, names, _) = self.sql(&who, &req.sql, &mut req.params, &policy, &snapshot)?;
        let label = if plan.tokenize.is_some() {
            "Spanner nunya_tokenize".to_owned()
        } else {
            label
        };
        Self::audit(&self.ctx, &who, &label, 0, vec![])?;
        if let Some(result) =
            self.local_tokenize(&who, &policy, &plan, &names, &req.params, &req.param_types)?
        {
            Self::audit(&self.ctx, &who, &label, 1, local_tokens(&result)?)?;
            return Ok(Response::new(Box::pin(tokio_stream::iter(vec![Ok(
                PartialResultSet {
                    metadata: result.metadata,
                    values: result.rows.into_iter().flat_map(|r| r.values).collect(),
                    ..Default::default()
                },
            )]))));
        }
        let upstream = self
            .client()
            .await?
            .execute_streaming_sql(req)
            .await
            .map_err(remote)?
            .into_inner();
        Ok(Response::new(
            self.stream(who, policy, plan, label, upstream),
        ))
    }
    async fn execute_batch_dml(
        &self,
        req: Request<ExecuteBatchDmlRequest>,
    ) -> Result<Response<ExecuteBatchDmlResponse>, Status> {
        let who = self.authenticate(&req)?;
        let mut req = req.into_inner();
        self.session(&req.session, &who)?;
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
        for label in &labels {
            Self::audit(&self.ctx, &who, label, 0, vec![])?;
        }
        let mut result = self
            .client()
            .await?
            .execute_batch_dml(req)
            .await
            .map_err(remote)?
            .into_inner();
        if result.result_sets.len() > plans.len() {
            return Err(invalid());
        }
        for (i, set) in result.result_sets.iter_mut().enumerate() {
            let tokens = self.rows(&who, &policy, &plans[i], set)?;
            Self::audit(&self.ctx, &who, &labels[i], set.rows.len() as u64, tokens)?;
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
        Ok(Response::new(result))
    }
    async fn read(&self, req: Request<ReadRequest>) -> Result<Response<ResultSet>, Status> {
        let who = self.authenticate(&req)?;
        let mut req = req.into_inner();
        self.session(&req.session, &who)?;
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
        Self::audit(&self.ctx, &who, "Read", 0, vec![])?;
        let mut result = self
            .client()
            .await?
            .read(req)
            .await
            .map_err(remote)?
            .into_inner();
        let tokens = self.rows(&who, &policy, &plan, &mut result)?;
        Self::audit(&self.ctx, &who, "Read", result.rows.len() as u64, tokens)?;
        Ok(Response::new(result))
    }
    async fn streaming_read(
        &self,
        req: Request<ReadRequest>,
    ) -> Result<Response<Self::StreamingReadStream>, Status> {
        let who = self.authenticate(&req)?;
        let mut req = req.into_inner();
        self.session(&req.session, &who)?;
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
        Self::audit(&self.ctx, &who, "Read", 0, vec![])?;
        let upstream = self
            .client()
            .await?
            .streaming_read(req)
            .await
            .map_err(remote)?
            .into_inner();
        Ok(Response::new(self.stream(
            who,
            policy,
            plan,
            "Read".into(),
            upstream,
        )))
    }
    async fn begin_transaction(
        &self,
        req: Request<BeginTransactionRequest>,
    ) -> Result<Response<Transaction>, Status> {
        let who = self.authenticate(&req)?;
        let req = req.into_inner();
        self.session(&req.session, &who)?;
        Ok(Response::new(
            self.client()
                .await?
                .begin_transaction(req)
                .await
                .map_err(remote)?
                .into_inner(),
        ))
    }
    async fn rollback(&self, req: Request<RollbackRequest>) -> Result<Response<()>, Status> {
        let who = self.authenticate(&req)?;
        let req = req.into_inner();
        self.session(&req.session, &who)?;
        self.client()
            .await?
            .rollback(req)
            .await
            .map_err(remote)?
            .into_inner();
        Ok(Response::new(()))
    }
    async fn commit(
        &self,
        req: Request<CommitRequest>,
    ) -> Result<Response<CommitResponse>, Status> {
        let who = self.authenticate(&req)?;
        let mut req = req.into_inner();
        self.session(&req.session, &who)?;
        let (policy, snapshot) = self.configuration()?;
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
        Self::audit(&self.ctx, &who, "Commit", 0, vec![])?;
        Ok(Response::new(
            self.client()
                .await?
                .commit(req)
                .await
                .map_err(remote)?
                .into_inner(),
        ))
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
        policy::{DomainSettings, ReadLogSink, Treatment},
        readlog::StdoutLog,
        schema::{Column, ColumnRef, Relation, RelationKind},
    };
    #[test]
    fn session_ownership_is_exact() {
        let sessions = HashMap::from([
            ("db/sessions/a".into(), "alice".into()),
            ("db/sessions/b".into(), "bob".into()),
        ]);
        assert!(owned(&sessions, "db/sessions/a", "alice").is_ok());
        assert_eq!(
            owned(&sessions, "db/sessions/a", "bob").unwrap_err().code,
            NyCode::TokenNotValid
        );
        assert_eq!(
            owned(&sessions, "db/sessions/missing", "alice")
                .unwrap_err()
                .code,
            NyCode::TokenNotValid
        );
        assert_eq!(
            owned(&sessions, "db/sessions/a/extra", "alice")
                .unwrap_err()
                .code,
            NyCode::TokenNotValid
        );
    }
    #[test]
    fn deleted_session_is_not_owned() {
        let mut sessions = HashMap::from([("db/sessions/a".into(), "alice".into())]);
        sessions.remove("db/sessions/a");
        assert!(owned(&sessions, "db/sessions/a", "alice").is_err());
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
        roles: Mutex<Vec<String>>,
        failure: std::sync::atomic::AtomicBool,
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
                name: format!("{}/sessions/fake", req.database),
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
                session: vec![Session {
                    name: format!("{}/sessions/batch", req.database),
                    ..Default::default()
                }],
            }))
        }
        async fn execute_streaming_sql(
            &self,
            _req: Request<ExecuteSqlRequest>,
        ) -> Result<Response<ResultStream>, Status> {
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
            _req: Request<DeleteSessionRequest>,
        ) -> Result<Response<()>, Status> {
            Ok(Response::new(()))
        }
        async fn execute_sql(
            &self,
            req: Request<ExecuteSqlRequest>,
        ) -> Result<Response<ResultSet>, Status> {
            self.seen.lock().unwrap().push(req.into_inner());
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
        async fn commit(
            &self,
            req: Request<CommitRequest>,
        ) -> Result<Response<CommitResponse>, Status> {
            self.commits.lock().unwrap().push(req.into_inner());
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
    fn request<T>(value: T, user: &str) -> Request<T> {
        let mut r = Request::new(value);
        r.metadata_mut().insert(
            "authorization",
            format!("Bearer nunya.{user}.password").parse().unwrap(),
        );
        r
    }
    struct Fixture {
        svc: NunyaSpanner,
        fake: Arc<Fake>,
        server: tokio::task::JoinHandle<()>,
        dir: std::path::PathBuf,
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            self.server.abort();
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }
    impl Fixture {
        async fn new() -> Self {
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
                        tokenize: TokenizeSetting::On { limit_per_task: 10 },
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
            let ctx = Arc::new(SpannerContext {
                policy: RwLock::new(Arc::new(policy)),
                snapshot: RwLock::new(Arc::new(snapshot)),
                root_key: RootKey::from_hex(&"00".repeat(32)).unwrap(),
                tasks: TaskStore::open_dir(&dir).unwrap(),
                readlog: Box::new(StdoutLog),
                standing: BTreeMap::from([
                    (
                        "alice".into(),
                        ScramVerifier::from_password("password", b"salt", 4096),
                    ),
                    (
                        "bob".into(),
                        ScramVerifier::from_password("password", b"salt", 4096),
                    ),
                ]),
                codec: StandardCodec,
                database: "projects/p/instances/i/databases/d".into(),
                clock: Arc::new(|| 0),
                upstream: SpannerUpstream::Emulator {
                    endpoint: format!("http://{addr}"),
                },
            });
            let svc = build(ctx).await.unwrap();
            Self {
                svc,
                fake,
                server,
                dir,
            }
        }
        async fn session(&self) -> String {
            self.svc
                .create_session(request(
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
            let auth = self.svc.authenticate(&request((), "alice")).unwrap();
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
                .execute_sql(request(req.clone(), "alice"))
                .await
                .unwrap_err()
                .message()
                .contains("NY001")
        );
        assert!(
            f.svc
                .execute_streaming_sql(request(req, "alice"))
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
                    .read(request(req, "alice"))
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
                    .commit(request(req, "alice"))
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
                .commit(request(req, "alice"))
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
            .execute_sql(request(req, "alice"))
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
        f.svc.execute_sql(request(req, "alice")).await.unwrap();
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
        assert!(f.svc.execute_sql(request(req, "alice")).await.is_err());
        assert!(f.fake.seen.lock().unwrap().is_empty());
    }
    #[tokio::test]
    async fn wrong_bearer_is_ny003() {
        let f = Fixture::new().await;
        let mut req = request(
            CreateSessionRequest {
                database: f.svc.ctx.database.clone(),
                ..Default::default()
            },
            "alice",
        );
        req.metadata_mut()
            .insert("authorization", "Bearer nunya.alice.wrong".parse().unwrap());
        assert!(
            f.svc
                .create_session(req)
                .await
                .unwrap_err()
                .message()
                .contains("NY003")
        );
    }
    #[tokio::test]
    async fn another_principals_session_is_refused() {
        let f = Fixture::new().await;
        let req = f.query("SELECT email FROM customers").await;
        assert!(
            f.svc
                .execute_sql(request(req, "bob"))
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
            .partition_query(request(
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
            .create_session(request(
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
            .commit(request(
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
            .create_session(request(
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
            .batch_create_sessions(request(
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
            .execute_streaming_sql(request(req, "alice"))
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
}
