use super::{
    codec::{self, Invocation},
    error::ApiError,
    identity::ConnectionIdentity,
    json,
    limits::*,
    native, native_codec, proto,
    session::{EntryHandle, Registry, SessionLease},
};
pub(crate) use super::stream::{Receiver as SpillChunks, grpc as spill_chunks};
use crate::{Response, Service, Session, agent::statement::OperationId};
use giggles_core::{Cancellation, relational::spill::{self, SpilledRows}};
use prost::Message;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    sync::{Mutex, OwnedSemaphorePermit, mpsc, oneshot, watch},
    task::{JoinHandle, JoinSet},
};

/// One service-wide API call and the physical socket owner. Kept through worker
/// completion and response delivery even if the request future is dropped.
pub struct CallOwnership {
    _call: OwnedSemaphorePermit,
    socket: Arc<OwnedSemaphorePermit>,
}
impl std::fmt::Debug for CallOwnership {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CallOwnership([bounded])")
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Encoding {
    Json,
    Protobuf,
}

pub(crate) struct WireSession {
    pub(crate) entry: EntryHandle,
    pub(crate) identity: ConnectionIdentity,
    bindings: watch::Receiver<()>,
    always_check: bool,
}

impl WireSession {
    pub(crate) fn binding_changes(&self) -> watch::Receiver<()> {
        self.bindings.clone()
    }
}

pub(crate) struct WireCall {
    pub request: Invocation,
    pub operation_id: Option<OperationId>,
    #[allow(dead_code)] // Informational, since cancel already carries the budget.
    pub budget_ms: Option<u32>,
    pub received: Instant,
    pub cancel: Cancellation,
}

/// Immutable wire reply plus the unique live-session owner for its response body.
pub struct Completion {
    pub reply: proto::ExecuteReply,
    pub json: Option<Vec<u8>>,
    pub spill: Option<SpilledRows>,
    pub lease: ResponseLease,
}

pub struct ResponseLease {
    session: SessionLease,
    _ownership: Arc<CallOwnership>,
}
impl ResponseLease {
    pub fn check_release(&self) -> Result<(), ApiError> {
        self.session.check_release()
    }
}

struct Job {
    lease: SessionLease,
    request: Invocation,
    operation_id: Option<crate::agent::statement::OperationId>,
    context: Cancellation,
    received: Instant,
    encoding: Encoding,
    stream: bool,
    ownership: Arc<CallOwnership>,
    reply: oneshot::Sender<Result<Completion, ApiError>>,
    #[cfg(test)]
    hook: Option<tests::WorkerHook>,
}

/// Shared persistent sessions for the new listeners. Construct once per server
/// and pass the same Arc to HTTP and gRPC; handles are local to this runtime.
/// Which listeners a runtime serves. Each kind has its own runtime and
/// registry, so a session token from one is `SESSION_NOT_FOUND` on the other.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ListenerKind {
    Ordinary,
    /// GD-0061 R3-208: agent HTTP and gRPC; certificates from the agent map
    /// only, and every session is a task session.
    Agent,
    Role,
}

pub struct ApiRuntime {
    role_listener: Option<Arc<crate::auth::RoleApiListener>>,
    kind: ListenerKind,
    service: Arc<Service>,
    registry: Arc<Registry>,
    jobs: mpsc::Sender<Job>,
    stopping: AtomicBool,
    stop: watch::Sender<bool>,
    supervisor: Mutex<Option<JoinHandle<()>>>,
    #[cfg(feature = "test-hooks")]
    pub(crate) wire_stats: crate::wire::listen::WireStats,
    #[cfg(test)]
    hook: std::sync::Mutex<Option<tests::WorkerHook>>,
}

impl ApiRuntime {
    pub fn new(service: Arc<Service>, limits: ApiLimits) -> Result<Arc<Self>, ApiError> {
        Self::with_kind(service, limits, ListenerKind::Ordinary, None)
    }

    /// The runtime for the agent HTTP and gRPC listeners, built with
    /// `agent.api_limits`.
    pub fn new_agent(service: Arc<Service>, limits: ApiLimits) -> Result<Arc<Self>, ApiError> {
        Self::with_kind(service, limits, ListenerKind::Agent, None)
    }

    pub fn new_role(
        service: Arc<Service>,
        limits: ApiLimits,
        listener: Arc<crate::auth::RoleApiListener>,
    ) -> Result<Arc<Self>, ApiError> {
        Self::with_kind(service, limits, ListenerKind::Role, Some(listener))
    }

    fn with_kind(
        service: Arc<Service>,
        limits: ApiLimits,
        kind: ListenerKind,
        role_listener: Option<Arc<crate::auth::RoleApiListener>>,
    ) -> Result<Arc<Self>, ApiError> {
        tokio::runtime::Handle::try_current().map_err(|_| ApiError::Unavailable)?;
        let limits = limits.validate()?;
        let registry = if let Some(listener) = &role_listener {
            Registry::new_role(
                service.clone(),
                limits,
                Duration::from_secs(listener.idle_timeout_seconds.into()),
                Duration::from_secs(listener.max_age_seconds.into()),
            )
        } else {
            Registry::new(service.clone(), limits)
        };
        let (jobs, receiver) = mpsc::channel(MAX_CALLS);
        let (stop, stopped) = watch::channel(false);
        let supervisor = tokio::spawn(supervise(
            service.clone(),
            registry.clone(),
            receiver,
            stopped,
        ));
        Ok(Arc::new(Self {
            #[cfg(feature = "test-hooks")]
            wire_stats: Default::default(),
            kind,
            role_listener,
            service,
            registry,
            jobs,
            stopping: AtomicBool::new(false),
            stop,
            supervisor: Mutex::new(Some(supervisor)),
            #[cfg(test)]
            hook: std::sync::Mutex::new(None),
        }))
    }

    pub(crate) fn service(&self) -> &Arc<Service> {
        &self.service
    }

    /// Only crate transport code can call this, after verifying the TLS leaf.
    pub(crate) fn authenticate(&self, fingerprint: &str) -> Result<ConnectionIdentity, ApiError> {
        match self.kind {
            ListenerKind::Ordinary => ConnectionIdentity::verified(&self.service, fingerprint),
            ListenerKind::Role => Err(ApiError::Unauthenticated),
            ListenerKind::Agent => ConnectionIdentity::verified_agent(&self.service, fingerprint),
        }
    }

    pub fn kind(&self) -> ListenerKind {
        self.kind
    }

    pub(crate) fn admit_call(
        &self,
        socket: Arc<OwnedSemaphorePermit>,
    ) -> Result<Arc<CallOwnership>, ApiError> {
        if self.stopping.load(Ordering::Acquire) {
            return Err(ApiError::Unavailable);
        }
        let call = self
            .service
            .admission
            .api_call()
            .ok_or(ApiError::ResourceExhausted)?;
        Ok(Arc::new(CallOwnership {
            _call: call,
            socket,
        }))
    }

    /// Test shorthand for an open without a task.
    #[cfg(test)]
    pub(crate) fn open(
        &self,
        identity: &ConnectionIdentity,
    ) -> Result<proto::OpenSessionReply, ApiError> {
        self.open_task(identity, None)
    }

    /// Validate the complete open request before touching the session registry.
    pub(crate) fn open_request(
        &self,
        identity: &ConnectionIdentity,
        request: &proto::OpenSessionRequest,
    ) -> Result<proto::OpenSessionReply, ApiError> {
        let open = super::open::classify_open(request)?;
        if let Some(listener) = &self.role_listener {
            use super::open::{Credential, OpenKind};
            use crate::auth::scram::{DEFAULT_ITERATIONS, MECHANISM, ServerExchange, Verifier};
            use crate::auth::signin::{self, Method, SigninPolicy};
            let connection = identity
                .connection
                .as_ref()
                .ok_or(ApiError::Unauthenticated)?;
            let mut server_final = None;
            let (role, database) = match open {
                OpenKind::Binding { .. } => return Err(ApiError::InvalidArgument),
                OpenKind::ScramContinue {
                    exchange,
                    client_final,
                } => {
                    // Consume before proof validation: failures are single-use too.
                    let mut pending = self.registry.take_scram(&exchange)?;
                    let final_bytes = pending
                        .exchange
                        .client_final(client_final.as_bytes())
                        .map_err(|_| ApiError::Unauthenticated)?;
                    server_final =
                        Some(String::from_utf8(final_bytes).map_err(|_| ApiError::Internal)?);
                    (pending.role, pending.database)
                }
                OpenKind::RoleSignin {
                    role,
                    database,
                    credential,
                } => {
                    // Reject plaintext before lookup or any password derivation, including trust.
                    if matches!(credential, Credential::Password(_)) && !connection.tls {
                        return Err(ApiError::Unauthenticated);
                    }
                    let method = signin::choose(
                        SigninPolicy {
                            certificates: &listener.certificates,
                            loopback_trust: listener.loopback_trust,
                        },
                        connection.peer,
                        connection.certificate.as_deref(),
                        &role,
                        &self.service,
                    );
                    match (credential, method) {
                        (Credential::ScramStart { client_first }, Method::Password(verifier)) => {
                            let mut exchange = ServerExchange::new(&role, verifier);
                            let first = exchange
                                .client_first(MECHANISM, client_first.as_bytes())
                                .map_err(|_| ApiError::Unauthenticated)?;
                            let handle =
                                self.registry.insert_scram(super::session::PendingScram {
                                    exchange,
                                    role,
                                    database,
                                    created: self.registry.now(),
                                    _peer: connection.peer,
                                })?;
                            return Ok(proto::OpenSessionReply {
                                scram_challenge: Some(proto::ScramChallenge {
                                    exchange: handle,
                                    server_first: String::from_utf8(first)
                                        .map_err(|_| ApiError::Internal)?,
                                }),
                                ..Default::default()
                            });
                        }
                        (Credential::ScramStart { .. }, _) => {
                            return Err(ApiError::InvalidArgument);
                        }
                        (Credential::Password(password), Method::Password(verifier)) => {
                            // No registry lock is held during the CPU derivation. A missing
                            // verifier performs the same default work as a generated verifier.
                            let valid = if let Some(verifier) = verifier {
                                verifier.verify_password(&password)
                            } else {
                                let dummy = Verifier::derive(
                                    &password,
                                    b"api-dummy-salt!!",
                                    DEFAULT_ITERATIONS,
                                );
                                std::hint::black_box(dummy);
                                false
                            };
                            if !valid {
                                return Err(ApiError::Unauthenticated);
                            }
                        }
                        (Credential::None, Method::Password(_)) => {
                            return Err(ApiError::Unauthenticated);
                        }
                        (_, Method::Certificate | Method::LoopbackTrust) => {}
                    }
                    (role, database)
                }
            };
            if !listener.databases.contains(&database) {
                return Err(ApiError::Unauthenticated);
            }
            let mut session = signin::open_role_session(&self.service, &role)
                .map_err(|_| ApiError::Unauthenticated)?;
            session.database = Some(database);
            let owner = ConnectionIdentity::role_signed_in(&session, listener.clone());
            let session_id = self.registry.open_role(&owner, session)?;
            return Ok(proto::OpenSessionReply {
                session_id,
                scram_server_final: server_final,
                idle_timeout_seconds: listener.idle_timeout_seconds,
                max_age_seconds: listener.max_age_seconds,
                transaction_timeout_seconds: TRANSACTION_TIMEOUT.as_secs() as u32,
                ..Default::default()
            });
        }
        match open {
            super::open::OpenKind::Binding { task } => self.open_task(identity, task.as_deref()),
            _ => Err(ApiError::InvalidArgument),
        }
    }

    /// Opens a session. On the agent runtime it is a task session for `task`
    /// (`Service::agent_session`); a refusal is `Unauthenticated`. A `task` on
    /// the ordinary runtime is `InvalidArgument`.
    pub(crate) fn open_task(
        &self,
        identity: &ConnectionIdentity,
        task: Option<&str>,
    ) -> Result<proto::OpenSessionReply, ApiError> {
        if task.is_some() && self.kind == ListenerKind::Ordinary {
            return Err(ApiError::InvalidArgument);
        }
        if (self.kind == ListenerKind::Agent) != identity.agent {
            return Err(ApiError::Unauthenticated);
        }
        let session_id = self.registry.open_task(identity, task)?;
        Ok(proto::OpenSessionReply {
            session_id,
            idle_timeout_seconds: IDLE_TIMEOUT.as_secs() as u32,
            max_age_seconds: MAX_AGE.as_secs() as u32,
            transaction_timeout_seconds: TRANSACTION_TIMEOUT.as_secs() as u32,
            ..Default::default()
        })
    }

    pub(crate) fn open_delivery_owner(
        &self,
        identity: &ConnectionIdentity,
        reply: &proto::OpenSessionReply,
    ) -> Result<Option<ConnectionIdentity>, ApiError> {
        if self.kind == ListenerKind::Role && !reply.session_id.is_empty() {
            self.registry
                .session_owner(&reply.session_id, identity)
                .map(Some)
        } else {
            Ok(None)
        }
    }

    pub(crate) fn close(&self, identity: &ConnectionIdentity, token: &str) -> Result<(), ApiError> {
        self.registry.close(token, identity)
    }

    pub(crate) fn open_wire(
        &self,
        identity: &ConnectionIdentity,
        request: &proto::OpenSessionRequest,
    ) -> Result<(proto::OpenSessionReply, Option<WireSession>), ApiError> {
        let bindings = self.service.binding_changes.subscribe();
        let reply = self.open_request(identity, request)?;
        if reply.session_id.is_empty() {
            return Ok((reply, None));
        }
        // A failure after the session exists must not leave it open until it idles out.
        let mut owner = match if self.kind == ListenerKind::Role {
            self.open_delivery_owner(identity, &reply)
                .and_then(|owner| owner.ok_or(ApiError::Internal))
        } else {
            Ok(identity.clone())
        } {
            Ok(owner) => owner,
            Err(error) => {
                self.registry.discard(&reply.session_id);
                return Err(error);
            }
        };
        let always_check = identity.credential_secret.is_some();
        owner.forget_secret();
        let identity = owner;
        let entry = match self.registry.entry_handle(&reply.session_id, &identity) {
            Ok(entry) => entry,
            Err(error) => {
                self.registry.discard(&reply.session_id);
                return Err(error);
            }
        };
        Ok((
            reply,
            Some(WireSession {
                entry,
                identity,
                bindings,
                always_check,
            }),
        ))
    }

    pub(crate) fn wire_cancellation(
        &self,
        received: Instant,
        budget_ms: Option<u32>,
    ) -> Result<Cancellation, ApiError> {
        let budget = budget_ms.unwrap_or(10_000);
        if budget > 10_000 {
            return Err(ApiError::InvalidArgument);
        }
        let deadline = received
            .checked_add(Duration::from_millis(u64::from(budget)))
            .ok_or(ApiError::InvalidArgument)?;
        Ok(Cancellation::new(Some(deadline)))
    }

    /// Certificate and role identities skip re-checking while the bindings watch
    /// is unchanged. Changes to bindings, agent bindings, role aliases, or task
    /// revocation all notify `service.binding_changes`. Credential identities
    /// are time-dependent and are checked on every call.
    pub(crate) fn wire_recheck(&self, session: &mut WireSession) -> Result<(), ApiError> {
        if session.always_check || session.bindings.has_changed().unwrap_or(true) {
            session.bindings.borrow_and_update();
            if let Err(error) = session.identity.check(&self.service) {
                self.registry.invalidate_entry(&session.entry);
                return Err(error);
            }
        }
        Ok(())
    }

    pub(crate) fn wire_session_alive(&self, session: &WireSession) -> bool {
        self.registry.entry_alive(&session.entry)
    }

    pub(crate) async fn execute_wire(
        &self,
        session: &mut WireSession,
        call: WireCall,
        ownership: Arc<CallOwnership>,
    ) -> Result<Completion, ApiError> {
        if self.stopping.load(Ordering::Acquire) {
            return Err(ApiError::Unavailable);
        }
        // The wire decoder parses bounded request data before the identity
        // check. The connection also checks identity before returning a
        // decode error. A parsed request still needs a live lease and the
        // worker's authority check before dispatch.
        self.wire_recheck(session)?;
        self.refuse_off_agent_forms(&call.request, &call.operation_id)?;
        let lease =
            self.registry
                .lease_entry(&session.entry, &session.identity, call.cancel.clone())?;
        self.execute_invocation(
            lease,
            call.request,
            call.operation_id,
            call.cancel,
            call.received,
            Encoding::Protobuf,
            true,
            ownership,
        )
        .await
    }

    pub(crate) fn close_wire(&self, session: WireSession) {
        self.registry.close_entry(&session.entry);
    }

    /// Decode before leasing, so malformed requests never modify transaction state.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn execute(
        &self,
        identity: &ConnectionIdentity,
        token: &str,
        request: proto::ExecuteRequest,
        received: Instant,
        grpc_deadline: Option<Instant>,
        ownership: Arc<CallOwnership>,
        encoding: Encoding,
        stream: bool,
    ) -> Result<Completion, ApiError> {
        if self.stopping.load(Ordering::Acquire) {
            return Err(ApiError::Unavailable);
        }
        // Bare role connections pass here; registry.lease checks the stored owner.
        identity.check(&self.service)?;
        let decoded = codec::decode_execute(request)?;
        self.refuse_off_agent_forms(&decoded.request, &decoded.operation_id)?;
        let deadline = received
            .checked_add(Duration::from_millis(u64::from(decoded.budget_ms)))
            .ok_or(ApiError::InvalidArgument)?;
        let deadline = grpc_deadline.map_or(deadline, |other| other.min(deadline));
        let context = Cancellation::new(Some(deadline));
        let lease = self.registry.lease(token, identity, context.clone())?;
        self.execute_invocation(
            lease,
            decoded.request,
            decoded.operation_id,
            context,
            received,
            encoding,
            stream,
            ownership,
        )
        .await
    }

    fn refuse_off_agent_forms(
        &self,
        request: &Invocation,
        operation_id: &Option<OperationId>,
    ) -> Result<(), ApiError> {
        // GD-0061 R3-209: operation IDs and the protected forms belong to
        // task sessions, which only the agent runtime opens. The ordinary and
        // SQL-role runtimes refuse them as malformed, before any lease.
        if self.kind != ListenerKind::Agent
            && (operation_id.is_some()
                || matches!(
                    request,
                    Invocation::TaskPreview { .. }
                        | Invocation::TaskApply { .. }
                        | Invocation::TaskOutcome
                ))
        {
            return Err(ApiError::InvalidArgument);
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute_invocation(
        &self,
        lease: SessionLease,
        request: Invocation,
        operation_id: Option<OperationId>,
        context: Cancellation,
        received: Instant,
        encoding: Encoding,
        stream: bool,
        ownership: Arc<CallOwnership>,
    ) -> Result<Completion, ApiError> {
        // Zero/expired budgets can never reach a business worker.
        context.check().map_err(|_| ApiError::DeadlineExceeded)?;
        let (reply, receiver) = oneshot::channel();
        let job = Job {
            lease,
            request,
            operation_id,
            context,
            received,
            encoding,
            stream,
            ownership,
            reply,
            #[cfg(test)]
            hook: self.hook.lock().unwrap().take(),
        };
        self.jobs.try_send(job).map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => ApiError::ResourceExhausted,
            mpsc::error::TrySendError::Closed(_) => ApiError::Unavailable,
        })?;
        receiver.await.map_err(|_| ApiError::Internal)?
    }

    /// Called after an atomic binding reload. Service notifications also cover
    /// trusted library reloads that do not pass through the executable.
    pub fn invalidate_bindings(&self) {
        self.registry.sweep();
    }

    #[cfg(feature = "test-hooks")]
    pub fn test_registry_entry_count(&self) -> usize {
        self.registry.entry_count()
    }

    #[cfg(feature = "test-hooks")]
    pub fn test_wire_connection(
        &self,
        peer: std::net::SocketAddr,
    ) -> Option<Arc<crate::wire::listen::ConnectionStats>> {
        self.wire_stats.connection(peer)
    }

    pub async fn shutdown(&self) {
        self.stopping.store(true, Ordering::Release);
        self.registry.stop();
        let _ = self.stop.send(true);
        if let Some(task) = self.supervisor.lock().await.take()
            && tokio::time::timeout(crate::transport::IO_TIMEOUT, task).await.is_err()
        {
            eprintln!("API shutdown supervisor drain timed out");
        }
    }
}

impl Drop for ApiRuntime {
    fn drop(&mut self) {
        self.stopping.store(true, Ordering::Release);
        self.registry.stop();
        let _ = self.stop.send(true);
        // The supervisor owns no Arc<ApiRuntime>; it drains started jobs after
        // this signal even if explicit asynchronous shutdown was not called.
    }
}

async fn supervise(
    service: Arc<Service>,
    registry: Arc<Registry>,
    mut jobs: mpsc::Receiver<Job>,
    mut stop: watch::Receiver<bool>,
) {
    let mut binding_changes = service.binding_changes.subscribe();
    // Cover a reload between runtime construction and this task's first poll.
    registry.sweep();
    let mut workers = JoinSet::new();
    let mut sweep = tokio::time::interval(Duration::from_secs(1));
    sweep.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            biased;
            changed = stop.changed() => {
                if changed.is_err() || *stop.borrow() { break; }
            }
            Some(_) = workers.join_next(), if !workers.is_empty() => {}
            _ = binding_changes.changed() => registry.sweep(),
            _ = sweep.tick() => registry.sweep(),
            job = jobs.recv() => match job {
                Some(job) => { workers.spawn(run_job(service.clone(), job)); }
                None => break,
            }
        }
    }
    registry.stop();
    jobs.close();
    while let Ok(job) = jobs.try_recv() {
        let _ = job.reply.send(Err(ApiError::Unavailable));
    }
    // Never abort admitted blocking work. It owns cleanup and its permits.
    while workers.join_next().await.is_some() {}
}

async fn run_job(service: Arc<Service>, mut job: Job) {
    // GD-0061 R3-403: an agent call takes an agent slot before it waits for a
    // blocking worker; a full admission waits briefly before refusing.
    let agent_slot = if job.lease.session().agent {
        match tokio::select! {
            biased;
            _ = job.reply.closed() => return,
            r = service.agent_admission.admit() => r,
        } {
            Ok(slot) => Some(slot),
            Err(error) => return refuse_agent_call(job, &error),
        }
    } else {
        None
    };
    // GD-0075 R5-103: per-role statement admission before the blocking pool.
    let role = job.lease.session().role.clone();
    let exempt = job.lease.session().is_aborted() || match &job.request {
        Invocation::Commit | Invocation::Rollback | Invocation::Savepoint(_)
        | Invocation::RollbackTo(_) | Invocation::Release(_) => true,
        Invocation::Legacy(request) => crate::admission::request_exempt(request),
        _ => false,
    };
    let role_permit = if exempt {
        None
    } else { tokio::select! {
        biased;
        _ = job.reply.closed() => return,
        admitted = service.admission.statement(&role, Some(&job.context)) => match admitted {
            Ok(permit) => Some(permit),
            Err(refusal) => {
                let error = match refusal {
                    crate::admission::StatementRefusal::Core(_) => ApiError::DeadlineExceeded,
                    _ => ApiError::ResourceExhausted,
                };
                let _ = job.reply.send(Err(error));
                return;
            }
        }
    }};
    let permit = tokio::select! {
        biased;
        _ = job.reply.closed() => return,
        admitted = service.blocking.admit(&job.context) => match admitted {
            Ok(permit) => permit,
            Err(_) => { let _ = job.reply.send(Err(ApiError::DeadlineExceeded)); return; }
        }
    };
    if job.reply.is_closed() {
        return;
    }
    let Job {
        mut lease,
        request,
        operation_id,
        context,
        received,
        encoding,
        stream,
        ownership,
        reply,
        #[cfg(test)]
        hook,
    } = job;
    #[cfg(test)]
    let (before, after, finished) = tests::WorkerHook::split(hook);
    let result = service
        .blocking
        .run_admitted(permit, move || {
            // Drop order: role permit first, then the agent slot (reverse of admission).
            let _agent_slot = agent_slot;
            let _role_permit = role_permit;
            #[cfg(test)]
            if let Some(barrier) = before {
                barrier.wait();
            }
            lease.session().connection = Some(ownership.socket.clone());
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                context.scope(|| {
                    lease.session().authority_scope(|session| {
                        // GD-0061 R3-310/313/314: a task session releases its rows before
                        // encoding, so it never takes the streaming path. Agent sessions are
                        // `Role::Database` with `agent` set (Service::agent_session), so the
                        // role check alone does not cover them.
                        let stream = stream
                            && !session.agent
                            && session.task().is_none();
                        #[cfg(feature = "legacy-task")]
                        let stream = stream && !matches!(session.role, crate::Role::Task { .. });
                        let (result, spill) = spill::bounded(|| {
                            if stream {
                                spill::streaming(|| execute_reply(session, request, operation_id, received, encoding))
                            } else {
                                (execute_reply(session, request, operation_id, received, encoding), None)
                            }
                        });
                        Ok(result.and_then(|(mut reply, json)| {
                            if let Some(rows) = &spill {
                                if let Some(proto::execute_reply::NativeResult::OperationOutcome(outcome)) = &mut reply.native_result {
                                    outcome.matched = rows.len();
                                }
                                let state = proto::TransactionState::try_from(reply.transaction_state)
                                    .map_err(|_| ApiError::Internal)?;
                                let (reply, json) = prepare_encoded(Ok(reply), state, encoding)?;
                                Ok((reply, json, spill))
                            } else {
                                Ok((reply, json, None))
                            }
                        }))
                    })
                })
            }));
            if matches!(&result, Ok(Ok(_))) {
                lease.completed();
            }
            #[cfg(test)]
            if let Some(barrier) = after {
                barrier.wait();
            }
            match result {
                Ok(Ok(Ok((reply, json, spill)))) => Ok(Completion {
                    reply,
                    json,
                    spill,
                    lease: ResponseLease {
                        session: lease,
                        _ownership: ownership,
                    },
                }),
                Ok(Ok(Err(error))) => Err(error),
                Ok(Err(error)) => {
                    // Authority failed before the dispatcher ran. Never reconstruct
                    // sensitive task fields or call the engine a second time.
                    if matches!(error, giggles_core::CoreError::Cancelled) {
                        Err(ApiError::DeadlineExceeded)
                    } else {
                        lease.invalidate();
                        Err(ApiError::Unauthenticated)
                    }
                }
                Err(_) => {
                    lease.invalidate();
                    Err(ApiError::Internal)
                }
            }
        })
        .await;
    let _ = reply.send(result.unwrap_or(Err(ApiError::Internal)));
    #[cfg(test)]
    if let Some(finished) = finished {
        let _ = finished.send(());
    }
}

/// The reply to an agent call refused at agent admission: the statement never
/// ran. As on the PostgreSQL listener, the refusal aborts an open transaction.
fn refuse_agent_call(job: Job, error: &giggles_core::CoreError) {
    let Job {
        mut lease,
        encoding,
        ownership,
        reply,
        ..
    } = job;
    let session = lease.session();
    session.abort();
    let state = if session.is_aborted() {
        proto::TransactionState::Aborted
    } else if session.in_transaction() {
        proto::TransactionState::InTransaction
    } else {
        proto::TransactionState::Idle
    };
    let result = prepare_encoded(codec::encode_error(error, state), state, encoding).map(|(reply, json)| {
        lease.completed();
        Completion {
            reply,
            json,
            spill: None,
            lease: ResponseLease {
                session: lease,
                _ownership: ownership,
            },
        }
    });
    let _ = reply.send(result);
}

fn prepare_reply(
    response: Response,
    state: proto::TransactionState,
    encoding: Encoding,
) -> Result<(proto::ExecuteReply, Option<Vec<u8>>), ApiError> {
    prepare_encoded(codec::encode_reply(response, state), state, encoding)
}

fn execute_reply(
    session: &mut Session,
    request: Invocation,
    operation_id: Option<crate::agent::statement::OperationId>,
    received: Instant,
    encoding: Encoding,
) -> Result<(proto::ExecuteReply, Option<Vec<u8>>), ApiError> {
    // GD-0061 R3-309/R3-313: the transport a task statement arrived on (HTTP
    // carries JSON only; gRPC carries protobuf). No-op without a task.
    session.sql.set_task_entry(match encoding {
        Encoding::Json => giggles_core::task::readlog::ENTRY_HTTP,
        Encoding::Protobuf => giggles_core::task::readlog::ENTRY_GRPC,
    });
    let reply = execute_released(session, request, operation_id, received, encoding);
    // After the reply is encoded: charges any rows_read no release charged
    // and finalizes the statement's read-log record (no-op without a task).
    session.sql.finish_task_statement(match &reply {
        Ok(_) => Ok(()),
        Err(_) => Err(&giggles_core::CoreError::Limit),
    });
    reply
}

/// `execute_reply` after the entry point is set: runs the request, whose
/// core rows are released inside `native::run` or `Request::Sql` before
/// anything below encodes them (GD-0061 R3-313).
fn execute_released(
    session: &mut Session,
    request: Invocation,
    operation_id: Option<crate::agent::statement::OperationId>,
    received: Instant,
    encoding: Encoding,
) -> Result<(proto::ExecuteReply, Option<Vec<u8>>), ApiError> {
    enum Executed {
        Legacy(Response),
        Native(Result<native::NativeResult, giggles_core::CoreError>),
    }
    let was_transaction = session.sql.in_transaction();
    let was_aborted = session.is_aborted();
    let prior_path = session.search_path.clone();
    let native_commit = matches!(&request, Invocation::Commit);
    let path_control = match &request {
        Invocation::Savepoint(n) => Some((0, n.clone())),
        Invocation::RollbackTo(n) => Some((1, n.clone())),
        Invocation::Release(n) => Some((2, n.clone())),
        _ => None,
    };
    session.api_path.sql_committed = false;
    let executed = match request {
        // GD-0061 R3-209: on a task session the legacy surface admits only
        // `Ping` (and `Sql`, whose statement is admitted on the SQL route).
        // A refusal is a native error reply, so it carries its task fields.
        Invocation::Legacy(request) => match native::admit_legacy(session, &request) {
            Ok(()) => Executed::Legacy(session.handle_received(request, received)),
            Err(error) => {
                session.abort();
                Executed::Native(Err(error))
            }
        },
        request => {
            #[cfg(test)]
            let outcome = giggles_core::relational::with_sql_parser_forbidden(|| {
                native::run(session, request, operation_id)
            });
            #[cfg(not(test))]
            let outcome = native::run(session, request, operation_id);
            Executed::Native(outcome)
        }
    };
    if matches!(
        &executed,
        Executed::Native(Ok(native::NativeResult::Control))
    ) && let Some((kind, mut name)) = path_control
    {
        let mut end = name.len().min(63);
        while !name.is_char_boundary(end) {
            end -= 1;
        }
        name.truncate(end);
        if kind == 0 {
            session.api_path.savepoints.push((name, prior_path.clone()));
        } else if let Some(i) = session
            .api_path
            .savepoints
            .iter()
            .rposition(|(n, _)| n == &name)
        {
            if kind == 1 {
                let path = session.api_path.savepoints[i].1.clone();
                session.set_search_path(if path == "\"\"" { "" } else { &path });
            }
            session
                .api_path
                .savepoints
                .truncate(i + usize::from(kind == 1));
        }
    }
    if !was_transaction && session.sql.in_transaction() {
        session.api_path.transaction_start = Some(prior_path);
    } else if was_transaction && !session.sql.in_transaction() {
        session.api_path.savepoints.clear();
        let committed = !was_aborted
            && match &executed {
                Executed::Native(Ok(native::NativeResult::Control)) => native_commit,
                Executed::Legacy(response) => {
                    response.code == "ok" && session.api_path.sql_committed
                }
                _ => false,
            };
        if let Some(path) = session.api_path.transaction_start.take()
            && !committed
        {
            session.set_search_path(if path == "\"\"" { "" } else { &path });
        }
    }
    let state = if session.is_aborted() {
        proto::TransactionState::Aborted
    } else if session.in_transaction() {
        proto::TransactionState::InTransaction
    } else {
        proto::TransactionState::Idle
    };
    let native = match executed {
        Executed::Legacy(response) => return prepare_reply(response, state, encoding),
        Executed::Native(result) => result,
    };
    let result = (|| match native {
        Err(error) => codec::encode_error(&error, state),
        Ok(outcome) => {
            let mut reply = codec::encode_reply(Response::code("ok"), state)?;
            use proto::execute_reply::NativeResult as P;
            reply.native_result = match outcome {
                native::NativeResult::Control => None,
                native::NativeResult::Catalog(outcome, table) => Some(P::CatalogOutcome(
                    native_codec::encode_catalog_outcome(outcome, table),
                )),
                native::NativeResult::SequenceCatalog(catalog_epoch, table) => {
                    Some(P::CatalogOutcome(proto::NativeCatalogOutcome {
                        catalog_epoch,
                        table,
                        columns: vec![],
                        primary_key: vec![],
                        index_id: None,
                    }))
                }
                native::NativeResult::Sql(sql) => {
                    reply.sql = codec::encode_reply(Response::released_sql(sql), state)?.sql;
                    None
                }
                native::NativeResult::Operation(outcome) => {
                    Some(P::OperationOutcome(native_codec::encode_outcome(outcome)?))
                }
                native::NativeResult::Page(page) => {
                    Some(P::OperationOutcome(native_codec::encode_page(page)?))
                }
                native::NativeResult::Transaction(outcome) => Some(P::TransactionOutcome(
                    native_codec::encode_transaction_outcome(outcome)?,
                )),
                native::NativeResult::Error(giggles_core::operations::Error::Task(error)) => {
                    return codec::encode_error(&giggles_core::CoreError::Task(error), state);
                }
                native::NativeResult::Error(error) => {
                    reply.code = if error == giggles_core::operations::Error::Unknown {
                        "08007"
                    } else {
                        "native_error"
                    }
                    .into();
                    Some(P::NativeError(native_codec::encode_error(error)))
                }
            };
            Ok(reply)
        }
    })();
    prepare_encoded(result, state, encoding)
}

fn prepare_encoded(
    result: Result<proto::ExecuteReply, ApiError>,
    state: proto::TransactionState,
    encoding: Encoding,
) -> Result<(proto::ExecuteReply, Option<Vec<u8>>), ApiError> {
    fn encode(
        reply: proto::ExecuteReply,
        encoding: Encoding,
    ) -> Result<(proto::ExecuteReply, Option<Vec<u8>>), ApiError> {
        if reply.encoded_len() > crate::transport::MAX_RESPONSE {
            return Err(ApiError::PayloadTooLarge);
        }
        let json = match encoding {
            Encoding::Json => Some(json::encode_reply(&reply)?),
            Encoding::Protobuf => None,
        };
        Ok((reply, json))
    }
    match result.and_then(|reply| encode(reply, encoding)) {
        Ok(result) => Ok(result),
        // Execution already happened; a serialization failure is not a rollback.
        Err(_) => encode(
            codec::encode_reply(Response::code("08007"), state)?,
            encoding,
        ),
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod wire_seam_tests;

#[cfg(all(test, feature = "engine-rocksdb"))]
mod native_release_tests;
