#![cfg(feature = "engine-rocksdb")]
mod agent_support;

use agent_support::parity::{APP, APPROVER, ParityFixture, http_call};
use giggles_core::SqlSession;
use giggles_server::{
    admission::RoleStatementLimits,
    api::{ApiLimits, ApiRuntime, error::ApiError, proto, proto::execute_request::Request},
    auth::RoleApiConfig,
    transport::Running,
    wire::{
        client::{Client, Credential},
        listen::serve_role_wire_bound,
        messages::WireConnError,
    },
};
use serde_json::json;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    net::TcpListener,
    sync::watch,
    time::{sleep, timeout},
};

const STALL_LIMIT: Duration = Duration::from_secs(5);
const WAIT_LIMIT: Duration = Duration::from_secs(3);
const LOCK_BUDGET_MS: u32 = 10_000;
const CANCEL_BUDGET_MS: u32 = 5_000;
const BLOCKING_CAP: usize = 32;
const CONNECTIONS: usize = 40;
const UPDATE_ROW: &str =
    "UPDATE shipments SET attempt_count=attempt_count+1 WHERE merchant_id=42 AND id=1";

fn execute(request: Request) -> proto::ExecuteRequest {
    proto::ExecuteRequest {
        request: Some(request),
        ..Default::default()
    }
}

fn sql(statement: &str, budget_ms: u32) -> proto::ExecuteRequest {
    proto::ExecuteRequest {
        budget_ms: Some(budget_ms),
        ..execute(Request::Sql(proto::SqlRequest {
            sql: statement.into(),
            ..Default::default()
        }))
    }
}

fn ping() -> proto::ExecuteRequest {
    execute(Request::Ping(proto::PingRequest {}))
}

async fn ok(client: &mut Client, request: proto::ExecuteRequest) -> proto::ExecuteReply {
    let reply = client.execute(request).await.unwrap();
    assert_eq!(reply.code, "ok", "{reply:?}");
    reply
}

async fn start_holder(f: &ParityFixture) -> Client {
    let mut holder = f.wire_client_for(&f.sql_app).await;
    ok(&mut holder, execute(Request::Begin(proto::BeginRequest {}))).await;
    ok(&mut holder, sql(UPDATE_ROW, LOCK_BUDGET_MS)).await;
    holder
}

async fn release_holder(holder: &mut Client) {
    ok(
        holder,
        execute(Request::Rollback(proto::RollbackRequest {})),
    )
    .await;
}

async fn wait_for(limit: Duration, description: &str, mut condition: impl FnMut() -> bool) {
    timeout(limit, async {
        while !condition() {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {description}"));
}

async fn reply(client: &mut Client, id: u32) -> Result<proto::ExecuteReply, WireConnError> {
    let (received, reply) = client.recv().await.unwrap();
    assert_eq!(received, id);
    reply
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocked_requests_do_not_stall_other_connections() {
    let f = ParityFixture::start().await;
    let mut holder = start_holder(&f).await;
    let mut first = f.wire_client_for(&f.sql_app).await;
    let mut second = f.wire_client_for(&f.sql_app).await;
    let mut a = f.wire_client(false).await;
    let mut b = f.wire_client(false).await;
    // One lock waiter per Tokio worker makes the test sensitive to accidental
    // blocking on those workers.
    let first_id = first
        .send_execute(sql(UPDATE_ROW, LOCK_BUDGET_MS))
        .await
        .unwrap();
    let second_id = second
        .send_execute(sql(UPDATE_ROW, LOCK_BUDGET_MS))
        .await
        .unwrap();
    wait_for(WAIT_LIMIT, "both row-lock waiters", || {
        f.admin.waiting_lock_count() == 2
    })
    .await;

    timeout(STALL_LIMIT, async {
        tokio::join!(ok(&mut a, ping()), ok(&mut b, ping()));
    })
    .await
    .expect("other connections completed while both requests were blocked");
    assert_eq!(f.admin.waiting_lock_count(), 2);
    release_holder(&mut holder).await;
    timeout(WAIT_LIMIT, async {
        assert_eq!(reply(&mut first, first_id).await.unwrap().code, "ok");
        assert_eq!(reply(&mut second, second_id).await.unwrap().code, "ok");
    })
    .await
    .expect("both requests finished after lock release");
    f.stop().await;
}

struct SaturatedPool {
    holder: SqlSession,
    listener: Running,
    runtime: Arc<ApiRuntime>,
    waiters: Vec<(Client, u32)>,
}

impl SaturatedPool {
    async fn start(f: &ParityFixture) -> Self {
        const LOCK_ROW: &str =
            "UPDATE shipments SET status='lock wait' WHERE merchant_id=42 AND id=1";
        f.service
            .set_role_statement_limits(RoleStatementLimits {
                concurrent: CONNECTIONS / 2,
                queue: 0,
                ..Default::default()
            })
            .unwrap();
        // Two roles can together fill the shared blocking cap.
        let config: RoleApiConfig = serde_json::from_value(json!({
            "wire": "127.0.0.1:0",
            "tls": "disabled",
            "loopback_trust": true,
            "connections": CONNECTIONS,
        }))
        .unwrap();
        let settings = Arc::new(config.build().unwrap());
        let listener = TcpListener::bind(settings.wire.unwrap()).await.unwrap();
        let runtime = ApiRuntime::new_role(
            f.service.clone(),
            ApiLimits {
                sessions: CONNECTIONS,
            },
            settings.clone(),
        )
        .unwrap();
        let (_start, gate) = watch::channel(true);
        let listener = serve_role_wire_bound(listener, runtime.clone(), settings, gate)
            .await
            .unwrap();
        let mut holder = f.admin.sql_session();
        holder.execute("BEGIN", &[], None).unwrap();
        holder.execute(LOCK_ROW, &[], None).unwrap();

        let mut waiters = Vec::with_capacity(CONNECTIONS);
        for role in std::iter::repeat_n(APP, CONNECTIONS / 2)
            .chain(std::iter::repeat_n(APPROVER, CONNECTIONS / 2))
        {
            let (client, _) = Client::connect(
                listener.address,
                None,
                proto::OpenSessionRequest {
                    role: Some(role.into()),
                    database: Some("postgres".into()),
                    ..Default::default()
                },
                Credential::None,
            )
            .await
            .unwrap();
            waiters.push(client);
        }
        let mut ids = Vec::with_capacity(CONNECTIONS);
        for waiter in &mut waiters {
            ids.push(
                waiter
                    .send_execute(sql(LOCK_ROW, LOCK_BUDGET_MS))
                    .await
                    .unwrap(),
            );
        }

        let waiters = waiters.into_iter().zip(ids).collect();
        let pool = Self {
            holder,
            listener,
            runtime,
            waiters,
        };
        wait_for(
            WAIT_LIMIT,
            "32 blocking permits, 32 row-lock waits and 40 role permits",
            || {
                let role_permits: usize = f
                    .service
                    .role_pool_counts()
                    .iter()
                    .map(|(available, capacity, _)| capacity - available)
                    .sum();
                let waiting = f.admin.waiting_lock_count();
                assert!(waiting <= BLOCKING_CAP, "observed {waiting} row-lock waits");
                pool.blocking_permits() == BLOCKING_CAP
                    && waiting == BLOCKING_CAP
                    && role_permits == CONNECTIONS
            },
        )
        .await;
        pool
    }

    fn blocking_permits(&self) -> usize {
        assert_blocking_cap(&self.runtime)
    }

    async fn release(self, f: &ParityFixture) {
        let Self {
            holder,
            listener,
            runtime,
            waiters,
        } = self;
        // Drop rolls back the holder without waiting for another SQL admission slot.
        drop(holder);
        timeout(WAIT_LIMIT, async {
            let drain = async {
                for (mut waiter, id) in waiters {
                    let result = reply(&mut waiter, id).await.unwrap();
                    assert_eq!(result.code, "ok", "{result:?}");
                }
            };
            tokio::pin!(drain);
            loop {
                assert_blocking_cap(&runtime);
                tokio::select! {
                    () = &mut drain => break,
                    () = sleep(Duration::from_millis(5)) => {}
                }
            }
        })
        .await
        .expect("all requests finished after lock release");
        assert_eq!(assert_blocking_cap(&runtime), 0);
        assert_eq!(f.admin.waiting_lock_count(), 0);
        listener.shutdown().await.unwrap();
        runtime.shutdown().await;
    }
}

fn assert_blocking_cap(runtime: &ApiRuntime) -> usize {
    let held = runtime.test_blocking_permits_in_use();
    assert!(held <= BLOCKING_CAP, "observed {held} blocking permits");
    held
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocking_cap_holds() {
    let f = ParityFixture::start().await;
    SaturatedPool::start(&f).await.release(&f).await;
    f.stop().await;
}

async fn cancel_waiter(f: &ParityFixture, waiter: &mut Client, limit: Duration) {
    let started = Instant::now();
    let id = waiter
        .send_execute(sql(UPDATE_ROW, CANCEL_BUDGET_MS))
        .await
        .unwrap();
    wait_for(WAIT_LIMIT, "the row-lock wait", || {
        f.admin.waiting_lock_count() == 1
    })
    .await;

    let result = timeout(limit, async {
        waiter.send_cancel(id).await.unwrap();
        reply(waiter, id).await
    })
    .await
    .expect("CANCEL reached the running request");
    assert!(
        started.elapsed() < Duration::from_millis(u64::from(CANCEL_BUDGET_MS) - 500),
        "the reply must arrive well before the request budget"
    );
    match result {
        Ok(reply) => assert_eq!(reply.code, "57014"),
        Err(error) => assert_eq!(error.code, ApiError::DeadlineExceeded.code()),
    }
    assert_eq!(f.admin.waiting_lock_count(), 0);
    assert_eq!(f.ordinary_api.test_blocking_permits_in_use(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_reaches_running_request() {
    let f = ParityFixture::start().await;
    let mut holder = start_holder(&f).await;
    let mut waiter = f.wire_client_for(&f.sql_app).await;
    for _ in 0..20 {
        cancel_waiter(&f, &mut waiter, Duration::from_secs(1)).await;
    }
    release_holder(&mut holder).await;
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_of_row_lock_wait_replies_promptly() {
    let f = ParityFixture::start().await;
    let mut holder = start_holder(&f).await;
    let mut waiter = f.wire_client_for(&f.sql_app).await;
    cancel_waiter(&f, &mut waiter, Duration::from_secs(1)).await;
    release_holder(&mut holder).await;
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disconnect_during_lock_wait_releases_promptly() {
    const UPDATE_SECOND_ROW: &str = "UPDATE shipments SET attempt_count=attempt_count+1 WHERE merchant_id=42 AND id=2 \
         RETURNING attempt_count";

    let f = ParityFixture::start().await;
    let mut holder = start_holder(&f).await;
    let mut waiter = f.wire_client_for(&f.sql_app).await;
    ok(&mut waiter, execute(Request::Begin(proto::BeginRequest {}))).await;
    let before_disconnect = ok(&mut waiter, sql(UPDATE_SECOND_ROW, LOCK_BUDGET_MS))
        .await
        .sql
        .unwrap();
    assert_eq!(before_disconnect.rows.len(), 1);
    waiter
        .send_execute(sql(UPDATE_ROW, LOCK_BUDGET_MS))
        .await
        .unwrap();
    wait_for(WAIT_LIMIT, "the row-lock wait", || {
        f.admin.waiting_lock_count() == 1 && f.ordinary_api.test_blocking_permits_in_use() == 1
    })
    .await;
    assert_eq!(f.ordinary_api.test_registry_entry_count(), 2);
    drop(waiter);
    wait_for(WAIT_LIMIT, "disconnect cleanup", || {
        f.ordinary_api.test_blocking_permits_in_use() == 0
            && f.admin.waiting_lock_count() == 0
            && f.ordinary_api.test_registry_entry_count() == 1
    })
    .await;

    let mut replacement = f.wire_client_for(&f.sql_app).await;
    let updated = timeout(
        WAIT_LIMIT,
        ok(&mut replacement, sql(UPDATE_SECOND_ROW, LOCK_BUDGET_MS)),
    )
    .await
    .expect("the disconnected transaction released its lock on row 2");
    let after_disconnect = updated.sql.unwrap();
    assert_eq!(after_disconnect.rows.len(), 1);
    assert_eq!(
        after_disconnect.rows[0].values,
        before_disconnect.rows[0].values
    );
    release_holder(&mut holder).await;
    f.stop().await;
}

async fn open_http(f: &ParityFixture) -> String {
    let (status, body) = http_call(
        f.http.address,
        (&f.ca, &f.app),
        "POST",
        "/v1/sessions",
        None,
        Some(&json!({})),
    )
    .await;
    assert_eq!(status, 201, "{body}");
    body["sessionId"].as_str().unwrap().to_owned()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn panic_in_request_is_isolated() {
    let f = ParityFixture::start_with_api_limits(ApiLimits { sessions: 2 }).await;
    let mut survivor = f.wire_client(false).await;
    let token = open_http(&f).await;
    f.ordinary_api.test_panic_next_request();
    let request = json!({"ping": {}});
    let (status, body) = http_call(
        f.http.address,
        (&f.ca, &f.app),
        "POST",
        "/v1/session/execute",
        Some(&token),
        Some(&request),
    )
    .await;
    assert_eq!(status, ApiError::Internal.http_status(), "{body}");
    assert_eq!(body["error"]["code"], ApiError::Internal.code());
    assert_eq!(f.ordinary_api.test_blocking_permits_in_use(), 0);
    assert_eq!(f.ordinary_api.test_registry_entry_count(), 1);
    let (status, reply_body) = http_call(
        f.http.address,
        (&f.ca, &f.app),
        "POST",
        "/v1/session/execute",
        Some(&token),
        Some(&request),
    )
    .await;
    assert_eq!(
        status,
        ApiError::SessionNotFound.http_status(),
        "{reply_body}"
    );
    assert_eq!(
        reply_body["error"]["code"],
        ApiError::SessionNotFound.code()
    );
    ok(&mut survivor, ping()).await;

    for inject_panic in [
        ApiRuntime::test_panic_next_request,
        ApiRuntime::test_panic_next_request_worker,
    ] {
        let mut victim = f.wire_client_for(&f.sql_app).await;
        assert_eq!(f.ordinary_api.test_registry_entry_count(), 2);
        inject_panic(&f.ordinary_api);

        let id = victim.send_execute(ping()).await.unwrap();
        let error = timeout(WAIT_LIMIT, reply(&mut victim, id))
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code, body["error"]["code"].as_str().unwrap());
        assert_eq!(error.message, body["error"]["message"].as_str().unwrap());
        assert_eq!(f.ordinary_api.test_blocking_permits_in_use(), 0);
        assert_eq!(f.ordinary_api.test_registry_entry_count(), 1);
        ok(&mut survivor, ping()).await;
        let mut replacement = f.wire_client_for(&f.sql_app).await;
        ok(&mut replacement, ping()).await;
        drop(replacement);
        wait_for(WAIT_LIMIT, "replacement session cleanup", || {
            f.ordinary_api.test_registry_entry_count() == 1
        })
        .await;
    }
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admission_order_unchanged() {
    let f = ParityFixture::start().await;
    let token = open_http(&f).await;
    let mut wire = f.wire_client(false).await;
    // All APP role permits and all blocking permits are held while both
    // listeners try to execute another APP request.
    let pool = SaturatedPool::start(&f).await;

    for (budget, expected) in [
        (0, ApiError::DeadlineExceeded),
        (1_000, ApiError::ResourceExhausted),
    ] {
        let (status, body) = http_call(
            f.http.address,
            (&f.ca, &f.app),
            "POST",
            "/v1/session/execute",
            Some(&token),
            Some(&json!({"ping": {}, "budgetMs": budget})),
        )
        .await;
        assert_eq!(status, expected.http_status(), "{body}");
        assert_eq!(body["error"]["code"], expected.code());
        let id = wire
            .send_execute(proto::ExecuteRequest {
                budget_ms: Some(budget),
                ..ping()
            })
            .await
            .unwrap();
        let error = timeout(WAIT_LIMIT, reply(&mut wire, id))
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code, body["error"]["code"].as_str().unwrap());
        assert_eq!(error.message, body["error"]["message"].as_str().unwrap());
        assert_eq!(pool.blocking_permits(), BLOCKING_CAP);
        assert_eq!(f.admin.waiting_lock_count(), BLOCKING_CAP);
    }
    pool.release(&f).await;
    ok(&mut wire, ping()).await;
    f.stop().await;
}
