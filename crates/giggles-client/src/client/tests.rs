use super::*;
use crate::{
    Limits, OperationId,
    transport::{Exchange, Transport},
};
use giggles_consensus::native::Response;
use giggles_core::local::Reply;
use std::{
    collections::VecDeque,
    net::SocketAddr,
    sync::{Mutex, atomic::AtomicUsize},
    time::Duration,
};

fn config() -> Config {
    Config {
        endpoints: vec![
            "127.0.0.1:10101".parse().unwrap(),
            "127.0.0.1:10102".parse().unwrap(),
        ],
        ca: "secret-ca".into(),
        certificate: "secret-cert".into(),
        key: "private-key".into(),
        server_name: "localhost".into(),
        limits: Limits::default(),
    }
}
fn operation() -> Operation {
    Operation::from_request(giggles_consensus::native::Request {
        version: 1,
        id: 44,
        deadline_ms: 5000,
        action: giggles_consensus::TaskAction::Execute(
            giggles_core::Request {
                operation: OperationId(77),
                selection: giggles_core::Selection::Keys(vec![]),
                column: giggles_core::Column::Status,
                after: giggles_core::Status::Queued,
            },
            None,
        ),
    })
    .unwrap()
}
#[derive(Clone, Copy)]
enum Step {
    Before,
    Lost,
    Timeout,
    Code(&'static str),
    Reply(&'static str),
    Withheld(&'static str),
    Wait(bool),
    CommitError(&'static str),
    Resolve,
}
#[derive(Default)]
struct Script {
    steps: Mutex<VecDeque<Step>>,
    calls: Mutex<Vec<(SocketAddr, Vec<u8>)>>,
    dropped: AtomicUsize,
    budgets: Mutex<Vec<(Duration, Duration)>>,
    committed: AtomicBool,
}
impl Script {
    fn new(steps: Vec<Step>) -> Arc<Self> {
        Arc::new(Self {
            steps: Mutex::new(steps.into()),
            ..Self::default()
        })
    }
}
struct DropGuard<'a>(&'a AtomicUsize);
impl Drop for DropGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
impl Transport for Script {
    fn exchange<'a>(
        &'a self,
        ep: SocketAddr,
        op: &'a Operation,
        deadline: Instant,
        connect: Duration,
        sent: &'a AtomicBool,
    ) -> Exchange<'a> {
        Box::pin(async move {
            self.calls
                .lock()
                .unwrap()
                .push((ep, op.wire_bytes().to_vec()));
            self.budgets
                .lock()
                .unwrap()
                .push((deadline.saturating_duration_since(Instant::now()), connect));
            let step = self.steps.lock().unwrap().pop_front().expect("extra send");
            let _guard = DropGuard(&self.dropped);
            match step {
                Step::CommitError(code) => {
                    sent.store(true, Ordering::Release);
                    self.committed.store(true, Ordering::Release);
                    Ok(Response {
                        version: 1,
                        id: op.id,
                        error: Some(code.into()),
                        reply: None,
                    })
                }
                Step::Resolve => {
                    assert!(self.committed.load(Ordering::Acquire));
                    assert!(!op.execute, "resolution must be status");
                    Ok(Response {
                        version: 1,
                        id: op.id,
                        error: None,
                        reply: Some(Reply::ok(
                            serde_json::json!({"version":1,"code":"committed","operation":77}),
                        )),
                    })
                }
                Step::Before => Err(Failure::BeforeSend),
                Step::Lost => {
                    sent.store(true, Ordering::Release);
                    Err(Failure::AfterSend)
                }
                Step::Timeout => Err(Failure::Timeout),
                Step::Wait(write) => {
                    sent.store(write, Ordering::Release);
                    std::future::pending().await
                }
                Step::Code(code) => {
                    sent.store(true, Ordering::Release);
                    Ok(Response {
                        version: 1,
                        id: op.id,
                        error: Some(code.into()),
                        reply: None,
                    })
                }
                Step::Reply(code) => {
                    sent.store(true, Ordering::Release);
                    Ok(Response {
                        version: 1,
                        id: op.id,
                        error: None,
                        reply: Some(Reply::ok(
                            serde_json::json!({"version":1,"code":code,"operation":77}),
                        )),
                    })
                }
                Step::Withheld(code) => {
                    sent.store(true, Ordering::Release);
                    Ok(Response {
                        version: 1,
                        id: op.id,
                        error: None,
                        reply: Some(Reply::ok(
                            serde_json::json!({"version":1,"code":code,"operation":null}),
                        )),
                    })
                }
            }
        })
    }
}

#[tokio::test(start_paused = true)]
async fn postcommit_error_strings_require_explicit_status_without_retry() {
    for code in [
        "unavailable",
        "capacity",
        "not_leader",
        "authentication",
        "indeterminate",
        "42501",
        "55000",
        "57014",
    ] {
        let script = Script::new(vec![Step::CommitError(code), Step::Resolve]);
        let client = Client::with_transport(config(), script.clone());
        let result = client.send(&operation()).await;
        assert!(
            matches!(
                result,
                Outcome::Unknown {
                    operation_id: OperationId(77),
                    ..
                }
            ),
            "{code}"
        );
        assert_eq!(
            script.calls.lock().unwrap().len(),
            1,
            "{code} retried execute"
        );
        let status = result.resolution(45, 5000).unwrap().unwrap();
        assert!(
            matches!(client.send(&status).await,Outcome::Recorded(r) if r.task_result().operation==Some(OperationId(77)) && r.is_committed())
        );
        assert_eq!(script.calls.lock().unwrap().len(), 2);
    }
}

#[tokio::test(start_paused = true)]
async fn exact_independent_request_connect_and_whole_budgets() {
    for (request, whole, wire, expected) in [(50, 30, 5000, 30), (10, 30, 5000, 10), (50, 30, 2, 2)]
    {
        let mut cfg = config();
        cfg.limits.request = Duration::from_millis(request);
        cfg.limits.operation = Duration::from_millis(whole);
        cfg.limits.connect = Duration::from_millis(7);
        let script = Script::new(vec![Step::Wait(true)]);
        let client = Client::with_transport(cfg, script.clone());
        let op = Operation::status(1, OperationId(77), wire).unwrap();
        assert!(matches!(
            client.send(&op).await,
            Outcome::Unknown {
                reason: UnknownReason::Timeout,
                ..
            }
        ));
        assert_eq!(
            *script.budgets.lock().unwrap(),
            vec![(Duration::from_millis(expected), Duration::from_millis(7))]
        );
    }
}

#[tokio::test(start_paused = true)]
async fn status_stale_leader_retry_preserves_exact_request() {
    let script = Script::new(vec![
        Step::Code("unavailable"),
        Step::Code("capacity"),
        Step::Reply("absent"),
    ]);
    let client = Client::with_transport(config(), script.clone());
    let op = Operation::status(1, OperationId(77), 5000).unwrap();
    assert!(
        matches!(client.send(&op).await,Outcome::Recorded(r) if r.task_result().code=="absent")
    );
    let calls = script.calls.lock().unwrap();
    assert_eq!(calls.len(), 3);
    assert!(calls.iter().all(|(_, bytes)| bytes == op.wire_bytes()));
}

#[tokio::test(start_paused = true)]
async fn all_beforewrite_timeouts_report_no_send() {
    let script = Script::new(vec![Step::Timeout; 3]);
    let client = Client::with_transport(config(), script.clone());
    assert!(matches!(
        client.send(&operation()).await,
        Outcome::TimeoutBeforeSend
    ));
    assert_eq!(script.calls.lock().unwrap().len(), 3);
}

#[tokio::test(start_paused = true)]
async fn status_refusal_then_budget_exhaustion_never_claims_no_send() {
    let script = Script::new(vec![Step::Code("unavailable")]);
    let mut cfg = config();
    cfg.limits.operation = Duration::from_millis(10);
    cfg.limits.backoff = Duration::from_millis(20);
    let client = Client::with_transport(cfg, script.clone());
    assert!(matches!(
        client
            .send(&Operation::status(1, OperationId(77), 5000).unwrap())
            .await,
        Outcome::Unknown {
            reason: UnknownReason::Timeout,
            ..
        }
    ));
    assert_eq!(script.calls.lock().unwrap().len(), 1);
}
#[test]
fn config_rejects_empty_duplicate_zero_port() {
    let mut c = config();
    c.validate().unwrap();
    c.endpoints.clear();
    assert_eq!(c.validate(), Err(ClientError::InvalidConfig));
    c = config();
    c.endpoints[1] = c.endpoints[0];
    assert!(c.validate().is_err());
    c = config();
    c.endpoints[0].set_port(0);
    assert!(c.validate().is_err());
    c = config();
    c.endpoints.push("127.0.0.1:2".parse().unwrap());
    c.endpoints.push("127.0.0.1:3".parse().unwrap());
    assert!(c.validate().is_err());
}
#[test]
fn independent_deadlines_and_limits() {
    let mut c = config();
    c.limits.operation = Duration::from_millis(1);
    c.validate().unwrap();
    for n in [0, 17, usize::MAX] {
        c = config();
        c.limits.max_attempts = n;
        assert!(c.validate().is_err());
    }
    for n in [0, 33, usize::MAX] {
        c = config();
        c.limits.max_in_flight = n;
        assert!(c.validate().is_err());
    }
    c = config();
    c.limits.request = Duration::ZERO;
    assert!(c.validate().is_err());
    c = config();
    c.server_name = "bad name".into();
    assert!(c.validate().is_err());
}
#[test]
fn debug_omits_credentials_request_and_reply() {
    assert!(!format!("{:?}", config()).contains("private-key"));
    assert!(!format!("{:?}", operation()).contains("Queued"));
    assert!(
        !format!(
            "{:?}",
            Recorded(Reply::ok(serde_json::json!({"secret":"unique-sensitive"})))
        )
        .contains("unique-sensitive")
    );
}
#[test]
fn immutable_wire_preserves_id_and_bounds() {
    let op = operation();
    assert_eq!(op.wire_bytes(), op.clone().wire_bytes());
    assert_eq!(op.operation_id(), OperationId(77));
    assert!(Operation::status(1, OperationId(1), 0).is_err());
    assert!(Operation::status(1, OperationId(1), 5001).is_err());
    let request = giggles_core::Request {
        operation: OperationId(77),
        selection: giggles_core::Selection::Keys(
            (0..10000)
                .map(|i| giggles_core::Key {
                    tenant: giggles_core::TenantId(42),
                    row: giggles_core::RowId(i),
                })
                .collect(),
        ),
        column: giggles_core::Column::Status,
        after: giggles_core::Status::Queued,
    };
    assert!(matches!(
        Operation::execute(1, request, None, 5000),
        Err(ClientError::OversizeRequest)
    ));
}
#[tokio::test(start_paused = true)]
async fn before_send_failover_exact_bytes() {
    let s = Script::new(vec![Step::Before, Step::Before, Step::Reply("committed")]);
    let c = Client::with_transport(config(), s.clone());
    assert!(matches!(c.send(&operation()).await,Outcome::Recorded(r) if r.is_committed()));
    let calls = s.calls.lock().unwrap();
    assert_eq!(calls.len(), 3);
    assert!(calls.iter().all(|(_, b)| b == operation().wire_bytes()));
    assert_ne!(calls[0].0, calls[1].0);
    assert_eq!(calls[0].0, calls[2].0);
}
#[tokio::test(start_paused = true)]
async fn after_commit_loss_one_send_and_status_identity() {
    let s = Script::new(vec![Step::Lost]);
    let c = Client::with_transport(config(), s.clone());
    let out = c.send(&operation()).await;
    assert!(matches!(
        out,
        Outcome::Unknown {
            operation_id: OperationId(77),
            reason: UnknownReason::Transport
        }
    ));
    assert_eq!(
        out.resolution(91, 100).unwrap().unwrap().operation_id(),
        OperationId(77)
    );
    assert_eq!(s.calls.lock().unwrap().len(), 1);
}
#[tokio::test(start_paused = true)]
async fn denial_indeterminate_and_unknown_error_never_retry() {
    for step in [
        Step::Code("authentication"),
        Step::Code("revoked"),
        Step::Code("indeterminate"),
        Step::Withheld("42501"),
        Step::Withheld("55000"),
        Step::Withheld("57014"),
        Step::Code("arbitrary malicious error"),
        Step::Code("unavailable"),
        Step::Code("capacity"),
        Step::Code("not_leader"),
    ] {
        let s = Script::new(vec![step]);
        let c = Client::with_transport(config(), s.clone());
        assert!(matches!(
            c.send(&operation()).await,
            Outcome::Denied(_) | Outcome::Unknown { .. }
        ));
        assert_eq!(s.calls.lock().unwrap().len(), 1);
    }
}
#[tokio::test(start_paused = true)]
async fn retry_attempt_bound_and_exhausted_accounting() {
    let s = Script::new(vec![Step::Before; 3]);
    let c = Client::with_transport(config(), s.clone());
    assert!(matches!(c.send(&operation()).await, Outcome::Unavailable));
    assert_eq!(s.calls.lock().unwrap().len(), 3);
    let s = Script::new(vec![Step::Reply("54000")]);
    let c = Client::with_transport(config(), s.clone());
    assert!(matches!(c.send(&operation()).await, Outcome::Recorded(_)));
    assert_eq!(s.calls.lock().unwrap().len(), 1);
}
#[tokio::test(start_paused = true)]
async fn overall_budget_includes_backoff() {
    let mut cfg = config();
    cfg.limits.operation = Duration::from_millis(30);
    cfg.limits.backoff = Duration::from_millis(20);
    let s = Script::new(vec![Step::Before; 3]);
    let c = Client::with_transport(cfg, s.clone());
    let start = Instant::now();
    assert!(matches!(
        c.send(&operation()).await,
        Outcome::TimeoutBeforeSend
    ));
    assert_eq!(s.calls.lock().unwrap().len(), 2);
    assert_eq!(start.elapsed(), Duration::from_millis(20));
}
#[tokio::test(start_paused = true)]
async fn send_timeout_is_unknown_and_stops_transport() {
    let s = Script::new(vec![Step::Wait(true)]);
    let mut cfg = config();
    cfg.limits.request = Duration::from_millis(5);
    let c = Client::with_transport(cfg, s.clone());
    assert!(matches!(
        c.send(&operation()).await,
        Outcome::Unknown {
            reason: UnknownReason::Timeout,
            ..
        }
    ));
    assert_eq!(s.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(c.permits.available_permits(), 8);
}
#[tokio::test(start_paused = true)]
async fn admission_timeout_no_send() {
    let s = Script::new(vec![]);
    let mut cfg = config();
    cfg.limits.max_in_flight = 1;
    let c = Client::with_transport(cfg, s.clone());
    let _held = c.permits.acquire().await.unwrap();
    assert!(matches!(
        c.send(&operation()).await,
        Outcome::TimeoutBeforeSend
    ));
    assert_eq!(s.calls.lock().unwrap().len(), 0);
}
#[tokio::test]
async fn cancellation_drops_transport_and_releases_permit() {
    let s = Script::new(vec![Step::Wait(true)]);
    let mut cfg = config();
    cfg.limits.max_in_flight = 1;
    let c = Client::with_transport(cfg, s.clone());
    let op = operation();
    let mut future = Box::pin(c.send(&op));
    tokio::select! { biased; _=&mut future=>panic!("completed"), _=tokio::task::yield_now()=>{} }
    assert_eq!(c.permits.available_permits(), 0);
    drop(future);
    assert_eq!(s.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(c.permits.available_permits(), 1);
}
#[tokio::test(start_paused = true)]
async fn model_all_safe_prefixes_keep_one_id() {
    for length in 0..=12 {
        for mode in 0..2 {
            let mut cfg = config();
            cfg.limits.max_attempts = 16;
            let mut steps = vec![
                if mode == 0 {
                    Step::Before
                } else {
                    Step::Timeout
                };
                length
            ];
            steps.push(Step::Lost);
            let s = Script::new(steps);
            let c = Client::with_transport(cfg, s.clone());
            assert!(matches!(
                c.send(&operation()).await,
                Outcome::Unknown {
                    operation_id: OperationId(77),
                    ..
                }
            ));
            let calls = s.calls.lock().unwrap();
            assert_eq!(calls.len(), length + 1);
            assert!(
                calls
                    .iter()
                    .all(|(_, bytes)| bytes == operation().wire_bytes())
            );
        }
    }
}
