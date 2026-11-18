#![cfg(feature = "engine-rocksdb")]
mod agent_support;

use agent_support::parity::{
    APP, APP_PW, APPROVER, APPROVER_PW, CONTROLLER, CONTROLLER_PW, Case, EXECUTOR, Observed,
    ParityFixture, TASK, credential_http_call, inc_436_sql, observe_native, task_error_name,
    with_session,
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use giggles_core::task::{
    Budget, OperationId, Outcome, PreviewId, ProtectedPoint, Receipt, StatementClass, TaskError,
    definition::{TaskObjectId, WallClockMs},
    inspect_outcome,
    statements::{TaskStatement, parse},
};
use giggles_server::api::{json as wire_json, proto};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Barrier;
use tokio_postgres::{Client, SimpleQueryMessage, Statement, config::SslMode};
use tokio_postgres_rustls::MakeRustlsConnect;
use tonic::transport::{Certificate, Channel, ClientTlsConfig};

fn write(op: &str, lo: usize, hi: usize) -> Case {
    Case::write(
        op,
        &format!(
            "UPDATE shipments SET status = 'queued' WHERE id BETWEEN {lo} AND {hi} AND status = 'failed'"
        ),
        format!(
            "update many shipments where id >= {lo} and id <= {hi} and status = \"failed\" set status = \"queued\" at most 25"
        ),
    )
}
fn success(o: &Observed, n: u64) {
    assert_eq!(o.sqlstate, None, "{o:?}");
    assert_eq!(o.transport, None, "{o:?}");
    assert_eq!(o.affected, Some(n), "{o:?}");
}
fn refusal(o: &Observed, e: TaskError) {
    assert_eq!(o.sqlstate.as_deref(), Some(e.sqlstate()), "{o:?}");
    assert_eq!(o.task_error.as_deref(), e.name(), "{o:?}");
    assert_eq!(o.message.as_deref(), Some(e.message()), "{o:?}");
    assert!(
        o.rows.is_empty() && o.affected.is_none() && o.transport.is_none(),
        "{o:?}"
    );
}
fn pg_error(e: tokio_postgres::Error) -> Observed {
    let e = e.as_db_error().expect("database error");
    Observed {
        sqlstate: Some(e.code().code().into()),
        message: Some(e.message().into()),
        task_error: task_error_name(e.code().code(), e.message()).map(str::to_owned),
        ..Observed::default()
    }
}
async fn simple(c: &Client, sql: &str) -> Observed {
    match c.simple_query(sql).await {
        Err(e) => pg_error(e),
        Ok(messages) => {
            let mut o = Observed::default();
            for m in messages {
                match m {
                    SimpleQueryMessage::Row(r) => o
                        .rows
                        .push((0..r.len()).map(|i| r.get(i).map(str::to_owned)).collect()),
                    SimpleQueryMessage::CommandComplete(n) => o.affected = Some(n),
                    _ => {}
                }
            }
            o
        }
    }
}
async fn credential(f: &ParityFixture, task: &str) -> String {
    f.password(CONTROLLER, CONTROLLER_PW)
        .await
        .unwrap()
        .query_one(&format!("ISSUE CREDENTIAL FOR TASK \"{task}\""), &[])
        .await
        .unwrap()
        .get(2)
}
async fn pg(f: &ParityFixture, task: &str, secret: &str) -> (Client, tokio::task::JoinHandle<()>) {
    let (c, connection) = tokio_postgres::Config::new()
        .host("localhost")
        .hostaddr(f.agent_pg.address.ip())
        .port(f.agent_pg.address.port())
        .user(EXECUTOR)
        .dbname("giggles")
        .options(format!("-c giggles.task={task}"))
        .password(secret)
        .ssl_mode(SslMode::Require)
        .connect(MakeRustlsConnect::new(agent_support::no_cert_client(&f.ca)))
        .await
        .unwrap();
    (
        c,
        tokio::spawn(async move {
            let _ = connection.await;
        }),
    )
}
fn task_id(f: &ParityFixture, task: &str) -> TaskObjectId {
    f.admin.tasks().get(task).unwrap().unwrap().id
}
fn outcome(f: &ParityFixture, task: &str, op: &str) -> Option<Outcome> {
    inspect_outcome(
        &f.admin.sql_session(),
        task_id(f, task),
        &OperationId::new(op).unwrap(),
    )
    .unwrap()
}
fn receipt(f: &ParityFixture, task: &str, op: &str) -> Receipt {
    let Some(Outcome::Committed(r)) = outcome(f, task, op) else {
        panic!("missing committed receipt {op}")
    };
    r
}
fn count(f: &ParityFixture, predicate: &str) -> usize {
    f.sql(&format!("SELECT id FROM shipments WHERE {predicate}"))
        .len()
}
async fn counters(f: &ParityFixture, task: &str, n: &str) {
    let c = f.password(CONTROLLER, CONTROLLER_PW).await.unwrap();
    let o = simple(&c, &format!("SHOW TASK EXPOSURE \"{task}\"")).await;
    assert_eq!(o.sqlstate, None, "{o:?}");
    let changed: Vec<_> = o
        .rows
        .iter()
        .filter(|r| r[0].as_deref() == Some("changed"))
        .collect();
    assert_eq!(changed.len(), 3, "{o:?}");
    // The changed rows report distinct rows, transitions, and operations.
    assert_eq!(changed[0][3].as_deref(), Some(n), "{o:?}");
    assert_eq!(changed[1][3].as_deref(), Some(n), "{o:?}");
}
fn create(f: &ParityFixture, name: &str, read_cap: Option<usize>) {
    let mut sql = inc_436_sql(&agent_support::timestamptz(f.expires_ms))
        .replace(TASK, name)
        .replace("concurrent_statements = 2", "concurrent_statements = 32");
    if let Some(cap) = read_cap {
        sql = sql
            .replace("1000 ROWS READ", &format!("{cap} ROWS READ"))
            .replace(
                "VISIBLE ROWS WHERE (merchant_id = 42)",
                "VISIBLE ROWS WHERE (merchant_id = 42 AND id = 1)",
            );
    } else {
        sql = sql.replace(
            "25 DISTINCT ROWS AND 25 TRANSITIONS",
            "1 DISTINCT ROWS AND 1 TRANSITIONS",
        );
    }
    let Some(TaskStatement::Create(new)) = parse(&sql) else {
        panic!("invalid task {sql}")
    };
    let issuer = f.admin.roles().get(CONTROLLER).unwrap().unwrap();
    f.admin
        .tasks()
        .create(&issuer, *new, WallClockMs(agent_support::now_ms()))
        .unwrap();
}
async fn http(
    f: &ParityFixture,
    secret: &str,
    method: &str,
    path: &str,
    token: Option<&str>,
    body: Option<&Value>,
) -> Value {
    let (status, v) = credential_http_call(
        f.agent_http.address,
        &f.ca,
        secret,
        method,
        path,
        token,
        body,
    )
    .await;
    assert!(matches!(status, 200 | 201 | 204), "HTTP {status}: {v}");
    v
}
fn auth<T>(mut r: tonic::Request<T>, secret: &str) -> tonic::Request<T> {
    r.metadata_mut()
        .insert("authorization", format!("Bearer {secret}").parse().unwrap());
    r
}
enum Session {
    Pg(Client, tokio::task::JoinHandle<()>, Option<Statement>),
    Http(String, String),
    Grpc(
        proto::giggles_client::GigglesClient<Channel>,
        String,
        String,
    ),
}
impl Session {
    async fn open(f: &ParityFixture, task: &str, kind: usize, case: &Case) -> Self {
        let secret = credential(f, task).await;
        Self::open_with_credential(f, task, kind, case, secret).await
    }
    async fn open_with_credential(
        f: &ParityFixture,
        task: &str,
        kind: usize,
        case: &Case,
        secret: String,
    ) -> Self {
        match kind {
            0 | 1 => {
                let (c, h) = pg(f, task, &secret).await;
                let prepared = if kind == 1 {
                    Some(c.prepare(&case.sql).await.unwrap())
                } else {
                    None
                };
                Self::Pg(c, h, prepared)
            }
            2 => {
                let v = http(
                    f,
                    &secret,
                    "POST",
                    "/v1/sessions",
                    None,
                    Some(&json!({"task":task})),
                )
                .await;
                Self::Http(secret, v["sessionId"].as_str().unwrap().into())
            }
            _ => {
                let channel = Channel::from_shared(format!(
                    "https://127.0.0.1:{}",
                    f.agent_grpc.address.port()
                ))
                .unwrap()
                .tls_config(
                    ClientTlsConfig::new()
                        .domain_name("localhost")
                        .ca_certificate(Certificate::from_pem(f.ca.cert.pem())),
                )
                .unwrap()
                .connect()
                .await
                .unwrap();
                let mut c = proto::giggles_client::GigglesClient::new(channel);
                let token = c
                    .open_session(auth(
                        tonic::Request::new(proto::OpenSessionRequest {
                            task: Some(task.into()),
                            ..Default::default()
                        }),
                        &secret,
                    ))
                    .await
                    .unwrap()
                    .into_inner()
                    .session_id;
                Self::Grpc(c, secret, token)
            }
        }
    }
    async fn execute(&mut self, f: &ParityFixture, case: &Case) -> Observed {
        match self {
            Self::Pg(c, _, None) => simple(c, &case.sql).await,
            Self::Pg(c, _, Some(s)) => match c.execute(s, &[]).await {
                Ok(n) => Observed {
                    affected: Some(n),
                    ..Observed::default()
                },
                Err(e) => pg_error(e),
            },
            Self::Http(secret, token) => observe_native(
                &http(
                    f,
                    secret,
                    "POST",
                    "/v1/session/execute",
                    Some(token),
                    Some(&case.native_body()),
                )
                .await,
            ),
            Self::Grpc(c, secret, token) => {
                let request =
                    wire_json::decode_execute(&serde_json::to_vec(&case.native_body()).unwrap())
                        .unwrap();
                let reply = c
                    .execute(auth(with_session(token, request), secret))
                    .await
                    .unwrap()
                    .into_inner();
                observe_native(
                    &serde_json::from_slice(&wire_json::encode_reply(&reply).unwrap()).unwrap(),
                )
            }
        }
    }
    async fn retry(&mut self, f: &ParityFixture, case: &Case) -> Observed {
        for _ in 0..50 {
            let o = self.execute(f, case).await;
            if o.sqlstate.as_deref() != Some(TaskError::Retry.sqlstate()) {
                return o;
            }
            tokio::task::yield_now().await;
        }
        panic!("50 retries exhausted: {}", case.sql)
    }
    async fn close(self, f: &ParityFixture) {
        match self {
            Self::Pg(c, h, _) => {
                drop(c);
                h.abort();
            }
            Self::Http(secret, token) => {
                http(f, &secret, "DELETE", "/v1/session", Some(&token), None).await;
            }
            Self::Grpc(mut c, secret, token) => {
                c.close_session(auth(
                    with_session(&token, proto::CloseSessionRequest {}),
                    &secret,
                ))
                .await
                .unwrap();
            }
        }
    }
}
async fn last_slot(mixed: bool) {
    let f = Arc::new(ParityFixture::start_with_credential().await);
    f.service
        .set_agent_limits(giggles_server::agent::AgentLimits {
            max_concurrent_statements: 16,
            ..f.service.agent_limits()
        })
        .unwrap();
    let name = "last-slot";
    create(&f, name, None);
    let n = if mixed { 12 } else { 10 };
    let barrier = Arc::new(Barrier::new(n));
    let mut racers = Vec::new();
    let secret = credential(&f, name).await;
    for i in 0..n {
        let w = write(&format!("r{i}"), i + 1, i + 1);
        let kind = if !mixed {
            0
        } else {
            match i % 3 {
                0 => 2 + (i / 3) % 2,
                1 => 0,
                _ => 1,
            }
        };
        let mut s = Session::open_with_credential(&f, name, kind, &w, secret.clone()).await;
        let f = f.clone();
        let barrier = barrier.clone();
        racers.push(tokio::spawn(async move {
            barrier.wait().await;
            let o = s.retry(&f, &w).await;
            s.close(&f).await;
            (i, o)
        }));
    }
    let mut winners = 0;
    for racer in racers {
        let (i, o) = racer.await.unwrap();
        let op = format!("r{i}");
        if o.sqlstate.is_none() {
            success(&o, 1);
            winners += 1;
            assert_eq!(receipt(&f, name, &op).rows_affected, 1);
        } else {
            refusal(&o, TaskError::BudgetExceeded(Budget::DistinctRows));
            assert_eq!(
                outcome(&f, name, &op),
                Some(Outcome::BudgetExceeded(Budget::DistinctRows))
            );
        }
    }
    assert_eq!(winners, 1);
    assert_eq!(count(&f, "status = 'queued'"), 1);
    counters(&f, name, "1").await;
    Arc::try_unwrap(f).ok().unwrap().stop().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn ten_agents_race_for_last_change_one_wins() {
    last_slot(false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn native_simple_extended_race_for_last_slot_one_wins() {
    last_slot(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn native_and_sql_race_same_rows_equal_effects_counters_receipts() {
    let f = ParityFixture::start_with_credential().await;
    let other = ParityFixture::start_with_credential().await;
    let secret = credential(&f, TASK).await;
    let other_secret = credential(&other, TASK).await;
    for round in 0..5 {
        let lo = round * 5 + 1;
        let hi = lo + 4;
        let a = write(&format!("sql.{round}"), lo, hi);
        let b = write(&format!("native.{round}"), lo, hi);
        let mut sql = Session::open_with_credential(&f, TASK, round % 2, &a, secret.clone()).await;
        let mut native =
            Session::open_with_credential(&f, TASK, 2 + round % 2, &b, secret.clone()).await;
        let barrier = Barrier::new(2);
        let (oa, ob) = tokio::join!(
            async {
                barrier.wait().await;
                sql.retry(&f, &a).await
            },
            async {
                barrier.wait().await;
                native.retry(&f, &b).await
            }
        );
        let (winner, loser, case, kind) = if oa.affected == Some(5) {
            success(&oa, 5);
            success(&ob, 0);
            (&a, &b, &a, 2 + round % 2)
        } else {
            success(&oa, 0);
            success(&ob, 5);
            (&b, &a, &b, round % 2)
        };
        let r = receipt(&f, TASK, winner.operation_id.as_ref().unwrap());
        let z = receipt(&f, TASK, loser.operation_id.as_ref().unwrap());
        assert_eq!(
            (r.rows_affected, r.distinct_charged, r.transitions_charged),
            (5, 5, 5)
        );
        assert_eq!(
            (z.rows_affected, z.distinct_charged, z.transitions_charged),
            (0, 0, 0)
        );
        let mut alone =
            Session::open_with_credential(&other, TASK, kind, case, other_secret.clone()).await;
        success(&alone.retry(&other, case).await, 5);
        let rr = receipt(&other, TASK, case.operation_id.as_ref().unwrap());
        // D-G fingerprints exact text and row intent; SQL and native-language
        // forms intentionally differ even when their row effects are equal.
        assert_ne!(r.request_fp, rr.request_fp);
        assert_eq!(
            (r.rows_affected, r.distinct_charged, r.transitions_charged),
            (
                rr.rows_affected,
                rr.distinct_charged,
                rr.transitions_charged
            )
        );
        // effects_digest includes random salted payload hashes and database row
        // versions (receipt.rs::digest_preimage); compare row counts and charges.
        alone.close(&other).await;
        sql.close(&f).await;
        native.close(&f).await;
    }
    assert_eq!(count(&f, "merchant_id = 42 AND status = 'queued'"), 25);
    assert_eq!(count(&f, "merchant_id = 43 AND status = 'failed'"), 40);
    counters(&f, TASK, "25").await;
    f.stop().await;
    other.stop().await;
}

async fn preview(c: &Client, sql: &str) -> (u64, String) {
    let o = simple(c, &format!("PREVIEW {sql}")).await;
    assert_eq!(o.sqlstate, None, "{o:?}");
    assert_eq!(o.rows.len(), 1, "{o:?}");
    (
        o.rows[0][0].as_ref().unwrap().parse().unwrap(),
        o.rows[0][1].clone().unwrap(),
    )
}
fn approve(id: u64, digest: &str) -> String {
    format!("APPROVE CHANGE {id} OF TASK \"{TASK}\" DIGEST '{digest}'")
}
fn apply(op: &str, id: u64, digest: &str) -> Case {
    Case {
        sql: format!("OPERATION '{op}' APPLY CHANGE {id} DIGEST '{digest}'"),
        native: agent_support::parity::Native::Request(
            json!({"taskApply":{"changeId":id.to_string(),"digest":STANDARD.encode(hex::decode(digest).unwrap())}}),
        ),
        operation_id: Some(op.into()),
    }
}
const SIX: &str = "UPDATE shipments SET status = 'queued' WHERE status = 'failed' AND id >= 26";
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn stale_approval_rejected_and_extra_matching_row_rejected() {
    let f = ParityFixture::start_with_credential().await;
    let secret = credential(&f, TASK).await;
    let (c, h) = pg(&f, TASK, &secret).await;
    let approver = f.password(APPROVER, APPROVER_PW).await.unwrap();
    let app = f.password(APP, APP_PW).await.unwrap();
    let mut native = Session::open(&f, TASK, 2, &write("unused", 1, 1)).await;
    for extra in [false, true] {
        let (id, digest) = preview(&c, SIX).await;
        assert_eq!(
            simple(&approver, &approve(id, &digest)).await.sqlstate,
            None
        );
        let before = f
            .admin
            .task_approval(task_id(&f, TASK), PreviewId(id))
            .unwrap();
        if extra {
            app.simple_query("INSERT INTO shipments VALUES (42, 32, 'failed', '2026-11-05 17:06:00+00', 1, 'new@example.com', 'tok_new')").await.unwrap();
        } else {
            app.simple_query("UPDATE shipments SET attempt_count = attempt_count + 1 WHERE merchant_id = 42 AND id = 26").await.unwrap();
        }
        let w = apply(if extra { "extra" } else { "stale" }, id, &digest);
        refusal(&simple(&c, &w.sql).await, TaskError::ApprovalStale);
        refusal(&native.execute(&f, &w).await, TaskError::ApprovalStale);
        assert_eq!(count(&f, "status = 'queued'"), 0);
        assert!(
            simple(&c, &Case::outcome(w.operation_id.as_ref().unwrap()).sql)
                .await
                .rows
                .is_empty()
        );
        assert!(outcome(&f, TASK, w.operation_id.as_ref().unwrap()).is_none());
        assert_eq!(
            f.admin
                .task_approval(task_id(&f, TASK), PreviewId(id))
                .unwrap(),
            before
        );
    }
    // Keep the fresh final preview at six rows after the extra-match check.
    app.simple_query("UPDATE shipments SET status = 'pending' WHERE merchant_id = 42 AND id = 32")
        .await
        .unwrap();
    let (id, digest) = preview(&c, SIX).await;
    let wrong = if digest == "0".repeat(64) {
        "1".repeat(64)
    } else {
        "0".repeat(64)
    };
    refusal(
        &simple(&approver, &approve(id, &wrong)).await,
        TaskError::ApprovalStale,
    );
    assert!(
        f.admin
            .task_approval(task_id(&f, TASK), PreviewId(id))
            .unwrap()
            .is_none()
    );
    assert_eq!(
        simple(&approver, &approve(id, &digest)).await.sqlstate,
        None
    );
    success(&simple(&c, &apply("fresh", id, &digest).sql).await, 6);
    assert_eq!(receipt(&f, TASK, "fresh").rows_affected, 6);
    native.close(&f).await;
    drop(c);
    h.abort();
    f.stop().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn separate_approver_and_executor_are_required() {
    let f = ParityFixture::start_with_credential().await;
    let controller = f.password(CONTROLLER, CONTROLLER_PW).await.unwrap();
    let app = f.password(APP, APP_PW).await.unwrap();
    let approver = f.password(APPROVER, APPROVER_PW).await.unwrap();
    let secret = credential(&f, TASK).await;
    let (c, h) = pg(&f, TASK, &secret).await;
    let create = inc_436_sql(&agent_support::timestamptz(f.expires_ms))
        .replace(TASK, "bad-approver")
        .replace("APPROVER oncall_lead", "APPROVER repair_agent");
    let o = simple(&controller, &create).await;
    assert_eq!(o.sqlstate.as_deref(), Some("22023"), "{o:?}");
    assert!(f.admin.tasks().get("bad-approver").unwrap().is_none());
    let (id, digest) = preview(&c, SIX).await;
    let a = approve(id, &digest);
    refusal(
        &simple(&c, &a).await,
        TaskError::NotAllowedInTaskSession(StatementClass::Other),
    );
    for caller in [&controller, &app] {
        let o = simple(caller, &a).await;
        assert_eq!(o.sqlstate.as_deref(), Some("42501"), "{o:?}");
        assert!(
            f.admin
                .task_approval(task_id(&f, TASK), PreviewId(id))
                .unwrap()
                .is_none()
        );
    }
    let w = apply("needs-approval", id, &digest);
    refusal(&simple(&c, &w.sql).await, TaskError::ApprovalRequired);
    assert!(outcome(&f, TASK, "needs-approval").is_none());
    assert!(
        simple(&c, "SHOW OPERATION 'needs-approval'")
            .await
            .rows
            .is_empty()
    );
    assert_eq!(simple(&approver, &a).await.sqlstate, None);
    success(&simple(&c, &w.sql).await, 6);
    drop(c);
    h.abort();
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn lost_response_recovered_over_another_connection() {
    let f = Arc::new(ParityFixture::start_with_credential().await);
    for native in [false, true] {
        let op = if native { "lost-http" } else { "lost-1" };
        let id = if native { 2 } else { 1 };
        let w = write(op, id, id);
        let (paused_tx, mut paused) = tokio::sync::mpsc::unbounded_channel();
        let (go, go_rx) = std::sync::mpsc::channel();
        let go_rx = Mutex::new(go_rx);
        let hits = AtomicUsize::new(0);
        f.admin
            .set_protected_hook(Some(Arc::new(move |point| {
                // There is no after-commit point: pause immediately before commit.
                if point == ProtectedPoint::AfterDecision
                    && hits.fetch_add(1, Ordering::SeqCst) == 0
                {
                    let _ = paused_tx.send(());
                    go_rx
                        .lock()
                        .unwrap()
                        .recv_timeout(Duration::from_secs(15))
                        .unwrap();
                }
            })))
            .unwrap();
        if native {
            // Own the HTTP socket and its driver so aborting the driver drops
            // the connection before commit, so its eventual reply is lost.
            let secret = credential(&f, TASK).await;
            let v = http(
                &f,
                &secret,
                "POST",
                "/v1/sessions",
                None,
                Some(&json!({"task":TASK})),
            )
            .await;
            let token = v["sessionId"].as_str().unwrap();
            let mut tls = agent_support::no_cert_client(&f.ca);
            tls.alpn_protocols = vec![b"http/1.1".to_vec()];
            let socket = tokio::net::TcpStream::connect(f.agent_http.address)
                .await
                .unwrap();
            let stream = tokio_rustls::TlsConnector::from(Arc::new(tls))
                .connect(
                    rustls::pki_types::ServerName::try_from("localhost").unwrap(),
                    socket,
                )
                .await
                .unwrap();
            let (mut sender, connection) =
                hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(stream))
                    .await
                    .unwrap();
            let driver = tokio::spawn(async move {
                let _ = connection.await;
            });
            let request = hyper::Request::builder()
                .method("POST")
                .uri("/v1/session/execute")
                .header("host", "localhost")
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {secret}"))
                .header("x-giggles-session", token)
                .body(http_body_util::Full::new(bytes::Bytes::from(
                    serde_json::to_vec(&w.native_body()).unwrap(),
                )))
                .unwrap();
            let sent = tokio::spawn(async move { sender.send_request(request).await });
            tokio::time::timeout(Duration::from_secs(20), paused.recv())
                .await
                .unwrap()
                .unwrap();
            driver.abort();
            sent.abort();
            let _ = driver.await;
            let _ = sent.await;
            go.send(()).unwrap();
            http(&f, &secret, "DELETE", "/v1/session", Some(token), None).await;
        } else {
            let secret = credential(&f, TASK).await;
            let (c, connection) = pg(&f, TASK, &secret).await;
            let sql = w.sql.clone();
            let sent = tokio::spawn(async move { c.simple_query(&sql).await });
            tokio::time::timeout(Duration::from_secs(20), paused.recv())
                .await
                .unwrap()
                .unwrap();
            connection.abort();
            sent.abort();
            let _ = connection.await;
            let _ = sent.await;
            go.send(()).unwrap();
        }
        f.admin.set_protected_hook(None).unwrap();
        let mut pg = Session::open(&f, TASK, 0, &w).await;
        let mut http = Session::open(&f, TASK, 2, &w).await;
        let lookup = Case::outcome(op);
        let (a, b) = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let a = pg.execute(&f, &lookup).await;
                let b = http.execute(&f, &lookup).await;
                if [&a, &b].iter().all(|o| {
                    o.sqlstate.is_none()
                        && o.rows.len() == 1
                        && o.rows[0][1].as_deref() == Some("committed")
                }) {
                    break (a, b);
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("lost operation must commit within the recovery deadline");
        for o in [&a, &b] {
            assert_eq!(o.sqlstate, None, "{o:?}");
            assert_eq!(o.rows.len(), 1, "{o:?}");
            assert_eq!(o.rows[0][1].as_deref(), Some("committed"));
        }
        let before = receipt(&f, TASK, op);
        // Fingerprints include exact text and row intent (D-G), so replay
        // on the original surface; both new surfaces recover its outcome.
        let replay = if native {
            http.retry(&f, &w).await
        } else {
            pg.retry(&f, &w).await
        };
        success(&replay, 1);
        assert_eq!(receipt(&f, TASK, op), before);
        assert_eq!(pg.execute(&f, &lookup).await, a);
        assert_eq!(http.execute(&f, &lookup).await, b);
        let different = write(op, 10, 10);
        refusal(
            &pg.execute(&f, &different).await,
            TaskError::OperationConflict,
        );
        refusal(
            &http.execute(&f, &different).await,
            TaskError::OperationConflict,
        );
        assert_eq!(count(&f, "status = 'queued'"), id);
        counters(&f, TASK, &id.to_string()).await;
        pg.close(&f).await;
        http.close(&f).await;
    }
    Arc::try_unwrap(f).ok().unwrap().stop().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn two_readers_race_for_last_disclosure_page_one_gets_it() {
    let f = ParityFixture::start_with_credential().await;
    for round in 0..6 {
        let name = format!("read-{round}");
        create(&f, &name, Some(2));
        let read = Case::new(
            "SELECT id FROM shipments WHERE id = 1",
            "shipments where id = 1 select id",
        );
        let mut sql = Session::open(&f, &name, 0, &read).await;
        let mut native = Session::open(&f, &name, 2, &read).await;
        let burn = if round % 2 == 0 {
            sql.execute(&f, &read).await
        } else {
            native.execute(&f, &read).await
        };
        assert_eq!(burn.sqlstate, None, "{burn:?}");
        assert_eq!(burn.rows.len(), 1, "{burn:?}");
        assert_eq!(
            f.admin
                .task_disclosure(task_id(&f, &name))
                .unwrap()
                .rows_read,
            1
        );
        let barrier = Barrier::new(2);
        let (a, b) = tokio::join!(
            async {
                barrier.wait().await;
                sql.retry(&f, &read).await
            },
            async {
                barrier.wait().await;
                native.retry(&f, &read).await
            }
        );
        let (winner, loser) = if a.sqlstate.is_none() {
            (&a, &b)
        } else {
            (&b, &a)
        };
        assert_eq!(winner.rows.len(), 1, "{winner:?}");
        refusal(loser, TaskError::BudgetExceeded(Budget::RowsRead));
        let disclosure = f.admin.task_disclosure(task_id(&f, &name)).unwrap();
        assert_eq!(disclosure.rows_read, 2);
        assert_eq!(disclosure.rows_returned, 2);
        sql.close(&f).await;
        native.close(&f).await;
    }
    f.stop().await;
}
