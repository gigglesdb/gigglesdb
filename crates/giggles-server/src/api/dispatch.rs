use super::{
    codec::{self, Invocation},
    error::ApiError,
    identity::ConnectionIdentity,
    json,
    limits::*,
    native, native_codec, proto,
    session::{Registry, SessionLease},
};
use crate::{Response, Service, Session};
use giggles_core::Cancellation;
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

/// Immutable wire reply plus the unique live-session owner for its response body.
pub struct Completion {
    pub reply: proto::ExecuteReply,
    pub json: Option<Vec<u8>>,
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
    context: Cancellation,
    received: Instant,
    encoding: Encoding,
    ownership: Arc<CallOwnership>,
    reply: oneshot::Sender<Result<Completion, ApiError>>,
    #[cfg(test)]
    hook: Option<tests::WorkerHook>,
}

/// Shared persistent sessions for the new listeners. Construct once per server
/// and pass the same Arc to HTTP and gRPC; handles are local to this runtime.
pub struct ApiRuntime {
    service: Arc<Service>,
    registry: Arc<Registry>,
    jobs: mpsc::Sender<Job>,
    stopping: AtomicBool,
    stop: watch::Sender<bool>,
    supervisor: Mutex<Option<JoinHandle<()>>>,
    #[cfg(test)]
    hook: std::sync::Mutex<Option<tests::WorkerHook>>,
}

impl ApiRuntime {
    pub fn new(service: Arc<Service>, limits: ApiLimits) -> Result<Arc<Self>, ApiError> {
        tokio::runtime::Handle::try_current().map_err(|_| ApiError::Unavailable)?;
        let limits = limits.validate()?;
        let registry = Registry::new(service.clone(), limits);
        let (jobs, receiver) = mpsc::channel(MAX_CALLS);
        let (stop, stopped) = watch::channel(false);
        let supervisor = tokio::spawn(supervise(
            service.clone(),
            registry.clone(),
            receiver,
            stopped,
        ));
        Ok(Arc::new(Self {
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
        ConnectionIdentity::verified(&self.service, fingerprint)
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

    pub(crate) fn open(
        &self,
        identity: &ConnectionIdentity,
    ) -> Result<proto::OpenSessionReply, ApiError> {
        let session_id = self.registry.open(identity)?;
        Ok(proto::OpenSessionReply {
            session_id,
            idle_timeout_seconds: IDLE_TIMEOUT.as_secs() as u32,
            max_age_seconds: MAX_AGE.as_secs() as u32,
            transaction_timeout_seconds: TRANSACTION_TIMEOUT.as_secs() as u32,
        })
    }

    pub(crate) fn close(&self, identity: &ConnectionIdentity, token: &str) -> Result<(), ApiError> {
        self.registry.close(token, identity)
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
    ) -> Result<Completion, ApiError> {
        if self.stopping.load(Ordering::Acquire) {
            return Err(ApiError::Unavailable);
        }
        identity.check(&self.service)?;
        let decoded = codec::decode_execute(request)?;
        let deadline = received
            .checked_add(Duration::from_millis(u64::from(decoded.budget_ms)))
            .ok_or(ApiError::InvalidArgument)?;
        let deadline = grpc_deadline.map_or(deadline, |other| other.min(deadline));
        let context = Cancellation::new(Some(deadline));
        let lease = self.registry.lease(token, identity, context.clone())?;
        // Zero/expired budgets can never reach a business worker.
        context.check().map_err(|_| ApiError::DeadlineExceeded)?;
        let (reply, receiver) = oneshot::channel();
        let job = Job {
            lease,
            request: decoded.request,
            context,
            received,
            encoding,
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

    pub async fn shutdown(&self) {
        self.stopping.store(true, Ordering::Release);
        self.registry.stop();
        let _ = self.stop.send(true);
        if let Some(task) = self.supervisor.lock().await.take() {
            let _ = task.await;
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
        context,
        received,
        encoding,
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
            #[cfg(test)]
            if let Some(barrier) = before {
                barrier.wait();
            }
            lease.session().connection = Some(ownership.socket.clone());
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                context.scope(|| {
                    lease.session().authority_scope(|session| {
                        // Immutable response construction remains within the lease.
                        Ok(execute_reply(session, request, received, encoding))
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
                Ok(Ok(Ok((reply, json)))) => Ok(Completion {
                    reply,
                    json,
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
    received: Instant,
    encoding: Encoding,
) -> Result<(proto::ExecuteReply, Option<Vec<u8>>), ApiError> {
    enum Executed {
        Legacy(Response),
        Native(Result<native::NativeResult, giggles_core::CoreError>),
    }
    let executed = match request {
        Invocation::Legacy(request) => Executed::Legacy(session.handle_received(request, received)),
        request => {
            #[cfg(test)]
            let outcome = giggles_core::relational::with_sql_parser_forbidden(|| {
                native::run(session, request)
            });
            #[cfg(not(test))]
            let outcome = native::run(session, request);
            Executed::Native(outcome)
        }
    };
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
        Err(error) => codec::encode_reply(Response::error(&error), state),
        Ok(outcome) => {
            let mut reply = codec::encode_reply(Response::code("ok"), state)?;
            use proto::execute_reply::NativeResult as P;
            reply.native_result = match outcome {
                native::NativeResult::Control => None,
                native::NativeResult::Operation(outcome) => {
                    Some(P::OperationOutcome(native_codec::encode_outcome(outcome)?))
                }
                native::NativeResult::Transaction(outcome) => Some(P::TransactionOutcome(
                    native_codec::encode_transaction_outcome(outcome)?,
                )),
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
