use super::*;
#[path = "native_tests.rs"]
mod native_tests;
use crate::{Binding, Role, clock::Clock};
use giggles_core::{Admin, Limits};
use std::{
    collections::BTreeSet,
    future::Future,
    task::Poll,
    time::{SystemTime, UNIX_EPOCH},
};

static NEXT_FIXTURE: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

// Test-only synchronization is private to this module. Production calls cannot
// select barriers or hold a worker outside normal engine execution.
pub(super) struct WorkerHook {
    barrier: Barrier,
    after: bool,
    finished: oneshot::Sender<()>,
}
pub(super) struct Barrier {
    entered: oneshot::Sender<()>,
    release: std::sync::mpsc::Receiver<()>,
    panic: bool,
}
impl Barrier {
    pub(super) fn wait(self) {
        let _ = self.entered.send(());
        self.release
            .recv_timeout(Duration::from_secs(10))
            .expect("test releases worker");
        assert!(!self.panic, "injected API worker panic");
    }
}
impl WorkerHook {
    pub(super) fn split(
        hook: Option<Self>,
    ) -> (
        Option<Barrier>,
        Option<Barrier>,
        Option<oneshot::Sender<()>>,
    ) {
        match hook {
            Some(Self {
                barrier,
                after: true,
                finished,
            }) => (None, Some(barrier), Some(finished)),
            Some(Self {
                barrier,
                after: false,
                finished,
            }) => (Some(barrier), None, Some(finished)),
            None => (None, None, None),
        }
    }
}
pub(super) struct Gate {
    pub(super) entered: oneshot::Receiver<()>,
    pub(super) release: std::sync::mpsc::Sender<()>,
    pub(super) finished: oneshot::Receiver<()>,
}
pub(super) fn gate(runtime: &ApiRuntime, after: bool) -> Gate {
    let (entered, observed) = oneshot::channel();
    let (release, waiting) = std::sync::mpsc::channel();
    let (finished, done) = oneshot::channel();
    *runtime.hook.lock().unwrap() = Some(WorkerHook {
        barrier: Barrier {
            entered,
            release: waiting,
            panic: false,
        },
        after,
        finished,
    });
    Gate {
        entered: observed,
        release,
        finished: done,
    }
}
pub(super) async fn signal(receiver: oneshot::Receiver<()>) {
    tokio::time::timeout(Duration::from_secs(5), receiver)
        .await
        .unwrap()
        .unwrap();
}
async fn pending<F: Future>(mut future: std::pin::Pin<&mut F>) {
    std::future::poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
}

struct Fixture {
    runtime: Arc<ApiRuntime>,
    owner: ConnectionIdentity,
    token: String,
    socket: Arc<OwnedSemaphorePermit>,
}
impl Fixture {
    fn new() -> Self {
        let path = crate::test_support::fixture_root().join(format!(
            "api-dispatch-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).unwrap();
        let admin = Admin::create(path.join("db.redb"), Limits::default()).unwrap();
        admin
            .sql_session()
            .execute("CREATE TABLE app (id BIGINT PRIMARY KEY)", &[], None)
            .unwrap();
        let fingerprint = "11".repeat(32);
        let service = Service::new(
            admin.clone(),
            vec![Binding {
                fingerprint: fingerprint.clone(),
                role: Role::Application {
                    domain: admin.authority_metadata().unwrap().0.to_string(),
                    tables: BTreeSet::from(["app".into()]),
                },
            }],
            Clock::new(0, 3600).unwrap(),
        )
        .unwrap();
        let runtime = ApiRuntime::new(service.clone(), ApiLimits::default()).unwrap();
        let owner = runtime.authenticate(&fingerprint).unwrap();
        let token = runtime.open(&owner).unwrap().session_id;
        let socket = service.admission.connect().unwrap();
        Self {
            runtime,
            owner,
            token,
            socket,
        }
    }
    fn call(&self) -> Arc<CallOwnership> {
        self.runtime.admit_call(self.socket.clone()).unwrap()
    }
    async fn execute(&self, request: proto::ExecuteRequest) -> Result<Completion, ApiError> {
        self.runtime
            .execute(
                &self.owner,
                &self.token,
                request,
                Instant::now(),
                None,
                self.call(),
                Encoding::Json,
                false,
            )
            .await
    }
    fn rows(&self) -> usize {
        self.runtime
            .service
            .admin
            .sql_session()
            .execute("SELECT id FROM app LIMIT 10", &[], None)
            .unwrap()
            .rows
            .len()
    }
}
fn sql(text: &str) -> proto::ExecuteRequest {
    proto::ExecuteRequest {
        budget_ms: None, operation_id: None,
        request: Some(proto::execute_request::Request::Sql(proto::SqlRequest {
            sql: text.into(),
            params: vec![],
        })),
    }
}
fn ping() -> proto::ExecuteRequest {
    proto::ExecuteRequest {
        budget_ms: None, operation_id: None,
        request: Some(proto::execute_request::Request::Ping(proto::PingRequest {})),
    }
}
fn launch(f: &Fixture, request: proto::ExecuteRequest) -> JoinHandle<Result<Completion, ApiError>> {
    let runtime = f.runtime.clone();
    let owner = f.owner.clone();
    let token = f.token.clone();
    let call = f.call();
    tokio::spawn(async move {
        runtime
            .execute(
                &owner,
                &token,
                request,
                Instant::now(),
                None,
                call,
                Encoding::Json,
                false,
            )
            .await
    })
}

#[tokio::test(flavor = "current_thread")]
async fn response_drop_restores_same_transaction_and_busy_spans_delivery() {
    let f = Fixture::new();
    let begin = f.execute(sql("BEGIN")).await.unwrap();
    assert_eq!(
        begin.reply.transaction_state,
        proto::TransactionState::InTransaction as i32
    );
    assert_eq!(f.execute(ping()).await.err(), Some(ApiError::SessionBusy));
    assert!(begin.lease.check_release().is_ok());
    drop(begin);
    drop(
        f.execute(sql("INSERT INTO app (id) VALUES (1)"))
            .await
            .unwrap(),
    );
    assert_eq!(f.rows(), 0);
    let commit = f.execute(sql("COMMIT")).await.unwrap();
    assert_eq!(commit.reply.code, "ok");
    assert_eq!(
        commit.reply.transaction_state,
        proto::TransactionState::Idle as i32
    );
    drop(commit);
    assert_eq!(f.rows(), 1);
    f.runtime.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn submitted_worker_survives_receiver_drop_and_restores_uncommitted_transaction() {
    let f = Fixture::new();
    drop(f.execute(sql("BEGIN")).await.unwrap());
    let Gate {
        entered,
        release,
        finished,
    } = gate(&f.runtime, false);
    let work = launch(&f, sql("INSERT INTO app (id) VALUES (2)"));
    signal(entered).await;
    work.abort();
    assert!(work.await.err().unwrap().is_cancelled());
    assert_eq!(f.execute(ping()).await.err(), Some(ApiError::SessionBusy));
    // The detached worker retains both API admission and the physical socket.
    let held: Vec<_> = (0..MAX_CALLS - 1).map(|_| f.call()).collect();
    assert_eq!(
        f.runtime.admit_call(f.socket.clone()).err(),
        Some(ApiError::ResourceExhausted)
    );
    assert!(Arc::strong_count(&f.socket) > 1);
    let mut worker_permits = Vec::new();
    for _ in 0..crate::blocking::MAX_BLOCKING - 1 {
        worker_permits.push(
            f.runtime
                .service
                .blocking
                .admit(&Cancellation::default())
                .await
                .unwrap(),
        );
    }
    let cancellation = Cancellation::default();
    let mut final_worker = Box::pin(f.runtime.service.blocking.admit(&cancellation));
    pending(final_worker.as_mut()).await;
    release.send(()).unwrap();
    signal(finished).await;
    let final_worker = final_worker.await.unwrap();
    drop(final_worker);
    drop(worker_permits);
    assert!(f.runtime.admit_call(f.socket.clone()).is_ok());
    drop(held);
    assert_eq!(f.rows(), 0);
    let query = f.execute(sql("SELECT id FROM app LIMIT 10")).await.unwrap();
    assert_eq!(query.reply.sql.as_ref().unwrap().rows.len(), 1);
    drop(query);
    drop(f.execute(sql("COMMIT")).await.unwrap());
    assert_eq!(f.rows(), 1);
    f.runtime.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn queued_receiver_drop_and_expired_deadline_never_dispatch() {
    let f = Fixture::new();
    let mut workers = Vec::new();
    for _ in 0..crate::blocking::MAX_BLOCKING {
        workers.push(
            f.runtime
                .service
                .blocking
                .admit(&Cancellation::default())
                .await
                .unwrap(),
        );
    }
    let call = f.call();
    let mut request = Box::pin(f.runtime.execute(
        &f.owner,
        &f.token,
        sql("INSERT INTO app (id) VALUES (3)"),
        Instant::now(),
        None,
        call,
        Encoding::Json,
        false,
    ));
    pending(request.as_mut()).await;
    drop(request);
    // Shutdown drains the queued owner without submitting work or waiting for a
    // worker permit (all 32 remain held here).
    tokio::time::timeout(Duration::from_secs(2), f.runtime.shutdown())
        .await
        .unwrap();
    assert_eq!(f.rows(), 0);
    drop(workers);

    let g = Fixture::new();
    let result = g
        .runtime
        .execute(
            &g.owner,
            &g.token,
            sql("INSERT INTO app (id) VALUES (4)"),
            Instant::now() - Duration::from_secs(11),
            None,
            g.call(),
            Encoding::Json,
            false,
        )
        .await;
    assert_eq!(result.err(), Some(ApiError::DeadlineExceeded));
    assert_eq!(g.rows(), 0);
    drop(g.execute(ping()).await.unwrap());
    g.runtime.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn shutdown_waits_for_started_worker_and_cancels_before_dispatch() {
    let f = Fixture::new();
    let Gate {
        entered,
        release,
        finished,
    } = gate(&f.runtime, false);
    let work = launch(&f, sql("INSERT INTO app (id) VALUES (5)"));
    signal(entered).await;
    let mut shutdown = Box::pin(f.runtime.shutdown());
    pending(shutdown.as_mut()).await;
    assert_eq!(f.runtime.open(&f.owner).err(), Some(ApiError::Unavailable));
    release.send(()).unwrap();
    signal(finished).await;
    assert_eq!(work.await.unwrap().err(), Some(ApiError::DeadlineExceeded));
    shutdown.await;
    assert_eq!(f.rows(), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn close_after_commit_fence_hides_response_without_rolling_back_durable_work() {
    let f = Fixture::new();
    let Gate {
        entered,
        release,
        finished,
    } = gate(&f.runtime, true);
    let work = launch(&f, sql("INSERT INTO app (id) VALUES (6)"));
    signal(entered).await;
    assert_eq!(f.rows(), 1);
    f.runtime.close(&f.owner, &f.token).unwrap();
    release.send(()).unwrap();
    let completion = work.await.unwrap().unwrap();
    signal(finished).await;
    assert_eq!(completion.reply.code, "ok");
    assert_eq!(
        completion.lease.check_release(),
        Err(ApiError::SessionNotFound)
    );
    drop(completion);
    assert_eq!(f.rows(), 1);
    assert_eq!(
        f.execute(ping()).await.err(),
        Some(ApiError::SessionNotFound)
    );
    f.runtime.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn response_release_rechecks_binding_epoch_and_runtime_has_no_arc_cycle() {
    let f = Fixture::new();
    let response = f.execute(ping()).await.unwrap();
    f.runtime.service.reload_bindings(vec![]).unwrap();
    assert_eq!(
        response.lease.check_release(),
        Err(ApiError::Unauthenticated)
    );
    drop(response);
    let weak = Arc::downgrade(&f.runtime);
    drop(f);
    assert!(weak.upgrade().is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn api_capacity_is_shared_by_all_runtimes_and_response_bodies() {
    let f = Fixture::new();
    let other = ApiRuntime::new(f.runtime.service.clone(), ApiLimits::default()).unwrap();
    let response = f.execute(ping()).await.unwrap();
    let calls: Vec<_> = (0..MAX_CALLS - 1)
        .map(|_| other.admit_call(f.socket.clone()).unwrap())
        .collect();
    assert_eq!(
        f.runtime.admit_call(f.socket.clone()).err(),
        Some(ApiError::ResourceExhausted)
    );
    assert_eq!(
        other.admit_call(f.socket.clone()).err(),
        Some(ApiError::ResourceExhausted)
    );
    // Close uses the same admission path as Execute, so a full call pool cannot
    // acquire a special bypass permit or silently close the retained session.
    let close = f
        .runtime
        .admit_call(f.socket.clone())
        .and_then(|_call| f.runtime.close(&f.owner, &f.token));
    assert_eq!(close, Err(ApiError::ResourceExhausted));
    assert!(response.lease.check_release().is_ok());
    drop(response);
    let recovered = other.admit_call(f.socket.clone()).unwrap();
    assert_eq!(
        f.runtime.admit_call(f.socket.clone()).err(),
        Some(ApiError::ResourceExhausted)
    );
    drop(recovered);
    drop(calls);
    other.shutdown().await;
    f.runtime.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn queued_deadline_and_close_cancel_before_worker_submission() {
    let f = Fixture::new();
    let mut permits = Vec::new();
    for _ in 0..crate::blocking::MAX_BLOCKING {
        permits.push(
            f.runtime
                .service
                .blocking
                .admit(&Cancellation::default())
                .await
                .unwrap(),
        );
    }
    let Gate {
        mut entered,
        release,
        finished,
    } = gate(&f.runtime, false);
    let mut request = sql("INSERT INTO app (id) VALUES (8)");
    request.budget_ms = Some(50);
    let mut waiting = Box::pin(f.execute(request));
    pending(waiting.as_mut()).await;
    let result = tokio::time::timeout(Duration::from_secs(2), waiting)
        .await
        .unwrap();
    assert_eq!(result.err(), Some(ApiError::DeadlineExceeded));
    assert_eq!(
        entered.try_recv(),
        Err(oneshot::error::TryRecvError::Closed)
    );
    drop((release, finished));
    assert_eq!(f.rows(), 0);

    let mut waiting = Box::pin(f.execute(sql("INSERT INTO app (id) VALUES (9)")));
    pending(waiting.as_mut()).await;
    f.runtime.close(&f.owner, &f.token).unwrap();
    assert_eq!(
        f.runtime.close(&f.owner, &f.token),
        Err(ApiError::SessionNotFound)
    );
    let result = tokio::time::timeout(Duration::from_secs(2), waiting)
        .await
        .unwrap();
    assert_eq!(result.err(), Some(ApiError::DeadlineExceeded));
    assert_eq!(f.rows(), 0);
    drop(permits);
    f.runtime.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn invalid_and_zero_budget_requests_leave_transaction_and_fresh_context_usable() {
    let f = Fixture::new();
    drop(f.execute(sql("BEGIN")).await.unwrap());
    drop(
        f.execute(sql("INSERT INTO app (id) VALUES (10)"))
            .await
            .unwrap(),
    );
    let invalid = proto::ExecuteRequest {
        request: None,
        budget_ms: None, operation_id: None,
    };
    assert_eq!(
        f.execute(invalid).await.err(),
        Some(ApiError::InvalidArgument)
    );
    let mut zero = sql("COMMIT");
    zero.budget_ms = Some(0);
    assert_eq!(
        f.execute(zero).await.err(),
        Some(ApiError::DeadlineExceeded)
    );
    let expired_grpc = f
        .runtime
        .execute(
            &f.owner,
            &f.token,
            sql("COMMIT"),
            Instant::now(),
            Some(Instant::now() - Duration::from_secs(1)),
            f.call(),
            Encoding::Protobuf,
            false,
        )
        .await;
    assert_eq!(expired_grpc.err(), Some(ApiError::DeadlineExceeded));
    let query = f.execute(sql("SELECT id FROM app LIMIT 10")).await.unwrap();
    assert_eq!(query.reply.sql.as_ref().unwrap().rows.len(), 1);
    assert_eq!(
        query.reply.transaction_state,
        proto::TransactionState::InTransaction as i32
    );
    drop(query);
    assert_eq!(f.rows(), 0);
    drop(f.execute(sql("COMMIT")).await.unwrap());
    assert_eq!(f.rows(), 1);
    // A sealed cancellation context from COMMIT cannot leak into the next call.
    let mut zero = sql("INSERT INTO app (id) VALUES (11)");
    zero.budget_ms = Some(0);
    assert_eq!(
        f.execute(zero).await.err(),
        Some(ApiError::DeadlineExceeded)
    );
    assert_eq!(f.rows(), 1);
    f.runtime.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn worker_panic_invalidates_session_and_recovers_all_ownership() {
    let f = Fixture::new();
    let Gate {
        entered,
        release,
        finished,
    } = gate(&f.runtime, false);
    f.runtime
        .hook
        .lock()
        .unwrap()
        .as_mut()
        .unwrap()
        .barrier
        .panic = true;
    let work = launch(&f, sql("INSERT INTO app (id) VALUES (12)"));
    signal(entered).await;
    release.send(()).unwrap();
    assert_eq!(work.await.unwrap().err(), Some(ApiError::Internal));
    signal(finished).await;
    assert_eq!(
        f.runtime.close(&f.owner, &f.token),
        Err(ApiError::SessionNotFound)
    );
    assert_eq!(f.rows(), 0);
    let all_calls: Vec<_> = (0..MAX_CALLS).map(|_| f.call()).collect();
    drop(all_calls);
    assert_eq!(Arc::strong_count(&f.socket), 1);
    let replacement = f.runtime.open(&f.owner).unwrap().session_id;
    let completion = f
        .runtime
        .execute(
            &f.owner,
            &replacement,
            ping(),
            Instant::now(),
            None,
            f.call(),
            Encoding::Json,
            false,
        )
        .await
        .unwrap();
    assert_eq!(completion.reply.code, "ok");
    drop(completion);
    f.runtime.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn reload_before_authority_scope_prevents_dispatch_and_destroys_old_handle() {
    let f = Fixture::new();
    let Gate {
        entered,
        release,
        finished,
    } = gate(&f.runtime, false);
    let work = launch(&f, sql("INSERT INTO app (id) VALUES (13)"));
    signal(entered).await;
    f.runtime.service.reload_bindings(vec![]).unwrap();
    // Match the executable's reload path. Explicit invalidation makes the
    // cancellation win before the worker checks authority, independent of
    // whether the periodic sweep happens to run while the barrier is held.
    f.runtime.invalidate_bindings();
    release.send(()).unwrap();
    assert_eq!(work.await.unwrap().err(), Some(ApiError::DeadlineExceeded));
    signal(finished).await;
    assert_eq!(f.rows(), 0);
    f.runtime
        .service
        .reload_bindings(vec![Binding {
            fingerprint: f.owner.fingerprint.clone(),
            role: f.owner.role.clone(),
        }])
        .unwrap();
    let replacement = f.runtime.authenticate(&f.owner.fingerprint).unwrap();
    assert_eq!(
        f.runtime.close(&replacement, &f.token),
        Err(ApiError::SessionNotFound)
    );
    assert_eq!(
        f.runtime.open(&f.owner).err(),
        Some(ApiError::Unauthenticated)
    );
    assert!(f.runtime.open(&replacement).is_ok());
    f.runtime.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
#[allow(clippy::await_holding_lock)] // The held writer is the deliberate race barrier.
#[allow(clippy::readonly_write_lock)] // A reader would allow the worker to enter.
async fn registry_calls_do_not_wait_for_worker_authority_and_close_cancels_wait() {
    let f = Fixture::new();
    let Gate {
        entered,
        release,
        finished,
    } = gate(&f.runtime, false);
    let work = launch(&f, sql("INSERT INTO app (id) VALUES (14)"));
    signal(entered).await;
    let service = f.runtime.service.clone();
    let authority = service.authority_gate.write().unwrap();
    release.send(()).unwrap();
    // Registry operations never acquire authority while holding the map lock.
    // The write guard remains held until the blocked worker has cancelled.
    let unrelated = f.runtime.open(&f.owner).unwrap().session_id;
    f.runtime.close(&f.owner, &unrelated).unwrap();
    f.runtime.close(&f.owner, &f.token).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(2), work)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.err(), Some(ApiError::DeadlineExceeded));
    signal(finished).await;
    drop(authority);
    assert_eq!(f.rows(), 0);
    f.runtime.shutdown().await;
}

#[test]
fn oversized_post_decision_reply_is_ambiguous_and_preserves_transaction_state() {
    // This conversion seam runs only after the authoritative native dispatcher.
    // An oversized result must never claim the preceding decision rolled back.
    for encoding in [Encoding::Json, Encoding::Protobuf] {
        let response = Response {
            digest: Some("x".repeat(crate::transport::MAX_RESPONSE + 1)),
            operation: Some(giggles_core::OperationId(21)),
            ..Response::code("committed")
        };
        let (reply, json) =
            prepare_reply(response, proto::TransactionState::InTransaction, encoding).unwrap();
        assert_eq!(reply.code, "08007");
        assert_eq!(
            reply.transaction_state,
            proto::TransactionState::InTransaction as i32
        );
        assert!(reply.operation.is_none());
        assert!(reply.digest.is_none());
        assert!(reply.sql.is_none());
        assert!(reply.encoded_len() < 128);
        if let Some(json) = json {
            assert!(json.len() < 128);
        }
    }
}

fn decision_gate(
    admin: &Admin,
    at: giggles_core::DecisionPoint,
) -> (oneshot::Receiver<Cancellation>, std::sync::mpsc::Sender<()>) {
    let (entered, observed) = oneshot::channel();
    let entered = std::sync::Mutex::new(Some(entered));
    let (release, waiting) = std::sync::mpsc::channel();
    let waiting = std::sync::Mutex::new(waiting);
    admin.set_decision_hook(Some(Arc::new(move |point| {
        if point == at
            && let Some(entered) = entered.lock().unwrap().take()
        {
            let _ = entered.send(Cancellation::current().expect("worker cancellation scope"));
            waiting
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(10))
                .expect("release decision fence");
        }
    })));
    (observed, release)
}

#[tokio::test(flavor = "current_thread")]
async fn exact_commit_fences_make_deadline_and_close_have_one_durable_winner() {
    use giggles_core::DecisionPoint;
    for explicit in [false, true] {
        for close in [false, true] {
            for point in [
                DecisionPoint::BeforeDecision,
                DecisionPoint::Decided,
                DecisionPoint::Committed,
            ] {
                let f = Fixture::new();
                if explicit {
                    drop(f.execute(sql("BEGIN")).await.unwrap());
                    drop(
                        f.execute(sql("INSERT INTO app (id) VALUES (15)"))
                            .await
                            .unwrap(),
                    );
                }
                let (entered, release) = decision_gate(&f.runtime.service.admin, point);
                let request = if explicit {
                    sql("COMMIT")
                } else {
                    sql("INSERT INTO app (id) VALUES (15)")
                };
                let work = launch(&f, request);
                let context = tokio::time::timeout(Duration::from_secs(5), entered)
                    .await
                    .unwrap()
                    .unwrap();
                if close {
                    f.runtime.close(&f.owner, &f.token).unwrap();
                } else {
                    context.deadline(Instant::now() - Duration::from_secs(1));
                }
                let cancelled = point == DecisionPoint::BeforeDecision;
                assert_eq!(context.check().is_err(), cancelled);
                release.send(()).unwrap();
                let completion = work.await.unwrap().unwrap();
                f.runtime.service.admin.set_decision_hook(None);
                assert_eq!(
                    completion.reply.code,
                    if cancelled { "57014" } else { "ok" }
                );
                if close {
                    assert_eq!(
                        completion.lease.check_release(),
                        Err(ApiError::SessionNotFound)
                    );
                } else {
                    assert!(completion.lease.check_release().is_ok());
                }
                assert_eq!(f.rows(), usize::from(!cancelled));
                drop(completion);
                if !close {
                    // A cancelled explicit COMMIT can be recovered only with
                    // ROLLBACK; a sealed decision retains its durable outcome.
                    drop(f.execute(sql("ROLLBACK")).await.unwrap());
                    assert_eq!(f.rows(), usize::from(!cancelled));
                }
                f.runtime.shutdown().await;
            }
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn exact_commit_fence_receiver_drop_retains_durable_work_and_restores_session() {
    let f = Fixture::new();
    let Gate {
        entered: completed,
        release: finish,
        finished,
    } = gate(&f.runtime, true);
    let (entered, release) = decision_gate(
        &f.runtime.service.admin,
        giggles_core::DecisionPoint::Decided,
    );
    let work = launch(&f, sql("INSERT INTO app (id) VALUES (16)"));
    let context = tokio::time::timeout(Duration::from_secs(5), entered)
        .await
        .unwrap()
        .unwrap();
    work.abort();
    assert!(work.await.err().unwrap().is_cancelled());
    assert!(context.check().is_ok());
    release.send(()).unwrap();
    signal(completed).await;
    f.runtime.service.admin.set_decision_hook(None);
    assert_eq!(f.rows(), 1);
    assert_eq!(f.execute(ping()).await.err(), Some(ApiError::SessionBusy));
    finish.send(()).unwrap();
    signal(finished).await;
    let response = f.execute(sql("SELECT id FROM app LIMIT 10")).await.unwrap();
    assert_eq!(response.reply.sql.as_ref().unwrap().rows.len(), 1);
    drop(response);
    f.runtime.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn reload_waits_for_exact_commit_scope_then_withholds_unreleased_result() {
    for point in [
        giggles_core::DecisionPoint::BeforeDecision,
        giggles_core::DecisionPoint::Decided,
    ] {
        let f = Fixture::new();
        let Gate {
            entered: completed,
            release: finish,
            finished,
        } = gate(&f.runtime, true);
        let (entered, release) = decision_gate(&f.runtime.service.admin, point);
        let work = launch(&f, sql("INSERT INTO app (id) VALUES (17)"));
        let context = tokio::time::timeout(Duration::from_secs(5), entered)
            .await
            .unwrap()
            .unwrap();
        let service = f.runtime.service.clone();
        let reload = tokio::task::spawn_blocking(move || service.reload_bindings(vec![]));
        assert!(
            !reload.is_finished(),
            "worker owns authority until immutable response is constructed"
        );
        assert!(context.check().is_ok());
        release.send(()).unwrap();
        signal(completed).await;
        tokio::time::timeout(Duration::from_secs(5), reload)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        f.runtime.service.admin.set_decision_hook(None);
        finish.send(()).unwrap();
        let completion = work.await.unwrap().unwrap();
        signal(finished).await;
        assert_eq!(completion.reply.code, "ok");
        assert_eq!(
            completion.lease.check_release(),
            Err(ApiError::Unauthenticated)
        );
        assert_eq!(f.rows(), 1);
        drop(completion);
        f.runtime.shutdown().await;
    }
}

#[tokio::test]
async fn existing_listeners_reject_role_signin_before_identity_or_registry() {
    let fixture = Fixture::new();
    let agent =
        ApiRuntime::new_agent(fixture.runtime.service.clone(), ApiLimits::default()).unwrap();
    let request = json::decode_open(br#"{"role":"r","database":"d","password":"p"}"#).unwrap();
    for runtime in [&fixture.runtime, &agent] {
        let error = runtime.open_request(&fixture.owner, &request).unwrap_err();
        assert_eq!(error, ApiError::InvalidArgument);
        assert_eq!(error.http_status(), 400);
        assert_eq!(error.grpc_status().code(), tonic::Code::InvalidArgument);
    }
    agent.shutdown().await;
    fixture.runtime.shutdown().await;
}
