//! R3-209 (GD-0061 design-2 sections 4(c), 5 and 6): the native statement
//! profile of an agent HTTP or gRPC session. Every request is classed before
//! any table lookup (`api::native::class`) and admitted as on the agent
//! pgwire path; a native update needs `operationId` and runs the protected
//! commit, as do `taskPreview`, `taskApply` and `taskOutcome`. A task refusal
//! is HTTP 200 / gRPC OK with `code` (the SQLSTATE), `taskError` (its name)
//! and `message` (its fixed text). Each test runs over HTTP JSON and over
//! gRPC (protobuf), on the agent listeners of one service.
#![cfg(feature = "engine-rocksdb")]
mod agent_support;

use agent_support::{DAY_MS, TestCert, binding, certificate, now_ms, scratch, timestamptz};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use bytes::Bytes;
use giggles_core::{
    Admin, Engine, Limits,
    relational::Value as SqlValue,
    roles::{NewRole, TablePrivilege},
    task::{
        DenyRule, StatementClass, TaskError,
        definition::{
            AllowedWrite, DisclosureCaps, LoadLimits, MutationCaps, NewTask, PredicateText,
            WallClockMs,
        },
    },
};
use giggles_server::{
    Binding, Role, Running, Service,
    admission::ServiceLimits,
    api::{
        ApiLimits, ApiRuntime,
        codec::{self, Invocation},
        json as wire_json,
        net::{serve_grpc_bound, serve_http_bound},
        proto,
    },
    clock::Clock,
    tls,
};
use http_body_util::{BodyExt, Full};
use hyper_util::rt::TokioIo;
use rustls::pki_types::{PrivatePkcs8KeyDer, ServerName};
use serde_json::{Value, json};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::watch,
};
use tokio_rustls::TlsConnector;
use tonic::{
    Code,
    transport::{Certificate as CaCertificate, Channel, ClientTlsConfig, Identity},
};

/// The worked example with a writable task: `inc-436` may set `status` from
/// 'failed' to 'queued' on merchant 42's rows (1-5; row 5 is delivered), at
/// most 25 rows. Row 6 belongs to merchant 7.
fn writable_admin() -> Admin {
    let path = scratch("agent-api-profile");
    let admin = Admin::create_engine(path.join("db.redb"), Limits::default(), Engine::V2).unwrap();
    for statement in [
        "CREATE TABLE shipments (id bigint primary key, merchant_id bigint, status text)",
        "INSERT INTO shipments VALUES (1, 42, 'failed'), (2, 42, 'failed'), (3, 42, 'failed'), \
         (4, 42, 'failed'), (5, 42, 'delivered'), (6, 7, 'failed')",
    ] {
        admin.sql_session().execute(statement, &[], None).unwrap();
    }
    let roles = admin.roles();
    for (name, task_only) in [("incident_controller", false), ("agent_exec", true)] {
        roles
            .create(NewRole {
                name: name.into(),
                login: true,
                ..NewRole::default()
            })
            .unwrap();
        if task_only {
            roles.set_task_only(name, true).unwrap();
        }
    }
    for name in ["incident_controller", "agent_exec"] {
        roles
            .grant_table(
                "shipments",
                name,
                &[TablePrivilege::Select, TablePrivilege::Update],
            )
            .unwrap();
    }
    let issuer = roles.get("incident_controller").unwrap().unwrap();
    admin
        .tasks()
        .create(
            &issuer,
            NewTask {
                name: "inc-436".into(),
                executor: "agent_exec".into(),
                approver: None,
                schema: None,
                table: "shipments".into(),
                visible_rows: PredicateText("merchant_id = 42".into()),
                readable: ["id", "merchant_id", "status"].map(String::from).to_vec(),
                masked: Vec::new(),
                untrusted_text: Vec::new(),
                allowed_write: Some(AllowedWrite {
                    columns: ["status".to_string()].into_iter().collect(),
                    old_row: PredicateText("status = 'failed'".into()),
                    new_row: PredicateText("status = 'queued'".into()),
                }),
                mutation_caps: MutationCaps {
                    distinct_rows: 25,
                    transitions: 25,
                },
                disclosure_caps: DisclosureCaps {
                    rows_read: 1000,
                    rows_returned: 1000,
                    bytes_returned: 2_097_152,
                },
                limits: LoadLimits::default(),
                approval_threshold: 25,
                expires: timestamptz(now_ms() + DAY_MS),
            },
            WallClockMs(now_ms()),
        )
        .unwrap();
    admin
}

/// How a call ended: a reply (HTTP 200 / gRPC OK) or a transport status.
#[derive(Debug, PartialEq)]
enum Called {
    Reply(Value),
    InvalidArgument,
    Other(String),
}

impl Called {
    #[track_caller]
    fn reply(self) -> Value {
        match self {
            Called::Reply(reply) => reply,
            other => panic!("expected a reply, got {other:?}"),
        }
    }
}

/// Agent HTTP and gRPC listeners and an ordinary HTTP listener, over one
/// service and one CA.
struct Fixture {
    admin: Admin,
    ca: TestCert,
    ordinary_api: Arc<ApiRuntime>,
    agent_api: Arc<ApiRuntime>,
    http: Running,
    agent_http: Running,
    agent_grpc: Running,
    ordinary: TestCert,
    agent: TestCert,
    grpc: bool,
}

impl Fixture {
    async fn start(grpc: bool) -> Self {
        let admin = writable_admin();
        let ca = certificate(None, false);
        let server = certificate(Some(&ca), true);
        let ordinary = certificate(Some(&ca), false);
        let agent = certificate(Some(&ca), false);
        let service = Service::with_agent(
            admin.clone(),
            vec![Binding {
                fingerprint: ordinary.fingerprint(),
                role: Role::Database {
                    name: "incident_controller".into(),
                },
            }],
            vec![binding(&agent, "agent_exec", &["inc-436"])],
            Clock::new(0, 3600).unwrap(),
            ServiceLimits::default(),
        )
        .unwrap();
        let config = tls::server(
            vec![server.cert.der().clone()],
            PrivatePkcs8KeyDer::from(server.key.serialize_der()).into(),
            vec![ca.cert.der().clone()],
        )
        .unwrap();
        let ordinary_api = ApiRuntime::new(service.clone(), ApiLimits::default()).unwrap();
        let agent_api = ApiRuntime::new_agent(service, ApiLimits { sessions: 4 }).unwrap();
        let (_, gate) = watch::channel(true);
        let bind = || async { TcpListener::bind("127.0.0.1:0").await.unwrap() };
        let http = serve_http_bound(bind().await, ordinary_api.clone(), config.clone(), 8, gate.clone())
            .await
            .unwrap();
        let agent_http =
            serve_http_bound(bind().await, agent_api.clone(), config.clone(), 8, gate.clone())
                .await
                .unwrap();
        let agent_grpc = serve_grpc_bound(bind().await, agent_api.clone(), config, 8, gate)
            .await
            .unwrap();
        Self {
            admin,
            ca,
            ordinary_api,
            agent_api,
            http,
            agent_http,
            agent_grpc,
            ordinary,
            agent,
            grpc,
        }
    }

    async fn stop(self) {
        for listener in [self.http, self.agent_http, self.agent_grpc] {
            listener.shutdown().await.unwrap();
        }
        self.ordinary_api.shutdown().await;
        self.agent_api.shutdown().await;
    }

    /// One HTTP/1.1 JSON call; the status and the JSON body.
    async fn http(
        &self,
        address: SocketAddr,
        cert: &TestCert,
        path: &str,
        token: Option<&str>,
        body: &Value,
    ) -> (u16, Value) {
        let body = serde_json::to_vec(body).unwrap();
        tokio::time::timeout(Duration::from_secs(15), async {
            let config = tls::client(
                vec![cert.cert.der().clone()],
                PrivatePkcs8KeyDer::from(cert.key.serialize_der()).into(),
                vec![self.ca.cert.der().clone()],
            )
            .unwrap();
            let mut config = (*config).clone();
            config.alpn_protocols = vec![b"http/1.1".to_vec()];
            let socket = TcpStream::connect(address).await.unwrap();
            let stream = TlsConnector::from(Arc::new(config))
                .connect(ServerName::try_from("localhost").unwrap(), socket)
                .await
                .unwrap();
            let mut request = hyper::Request::builder()
                .method("POST")
                .uri(format!("https://localhost:{}{path}", address.port()))
                .header("host", "localhost")
                .header("content-type", "application/json");
            if let Some(token) = token {
                request = request.header("x-giggles-session", token);
            }
            let request = request.body(Full::new(Bytes::from(body))).unwrap();
            let (mut sender, connection) =
                hyper::client::conn::http1::handshake(TokioIo::new(stream)).await.unwrap();
            let task = tokio::spawn(async move {
                let _ = connection.await;
            });
            let response = sender.send_request(request).await.unwrap();
            let status = response.status().as_u16();
            let bytes = response.into_body().collect().await.unwrap().to_bytes();
            task.abort();
            let _ = task.await;
            (status, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
        })
        .await
        .expect("HTTP call finished")
    }

    async fn client(&self) -> proto::giggles_client::GigglesClient<Channel> {
        let channel = Channel::from_shared(format!(
            "https://127.0.0.1:{}",
            self.agent_grpc.address.port()
        ))
        .unwrap()
        .tls_config(
            ClientTlsConfig::new()
                .domain_name("localhost")
                .ca_certificate(CaCertificate::from_pem(self.ca.cert.pem()))
                .identity(Identity::from_pem(self.agent.cert.pem(), self.agent.key.serialize_pem())),
        )
        .unwrap()
        .connect_timeout(Duration::from_secs(10))
        .connect()
        .await
        .unwrap();
        proto::giggles_client::GigglesClient::new(channel)
            .max_decoding_message_size(giggles_server::MAX_RESPONSE)
    }

    /// A task session for `inc-436` on this fixture's agent transport.
    async fn open(&self) -> String {
        if self.grpc {
            self.client()
                .await
                .open_session(proto::OpenSessionRequest {
                    task: Some("inc-436".into()),
                    ..Default::default()
                })
                .await
                .unwrap()
                .into_inner()
                .session_id
        } else {
            let (status, reply) = self
                .http(self.agent_http.address, &self.agent, "/v1/sessions", None, &json!({"task": "inc-436"}))
                .await;
            assert_eq!(status, 201, "{reply}");
            reply["sessionId"].as_str().unwrap().to_owned()
        }
    }

    /// One request, written as the HTTP JSON body. Over gRPC it is converted
    /// with the server's own JSON decoder and the reply is shown as JSON.
    async fn call(&self, token: &str, request: &Value) -> Called {
        if self.grpc {
            let wire = match wire_json::decode_execute(&serde_json::to_vec(request).unwrap()) {
                Ok(wire) => wire,
                // Retired arms cannot be represented by the generated default
                // gRPC request. Preserve the JSON decoder's refusal here.
                Err(giggles_server::api::error::ApiError::LegacyTaskRetired) => return Called::InvalidArgument,
                Err(error) => panic!("{request}: {error:?}"),
            };
            let mut call = tonic::Request::new(wire);
            call.metadata_mut()
                .insert("x-giggles-session", token.parse().unwrap());
            match self.client().await.execute(call).await {
                Ok(reply) => Called::Reply(
                    serde_json::from_slice(&wire_json::encode_reply(&reply.into_inner()).unwrap())
                        .unwrap(),
                ),
                Err(status) if status.code() == Code::InvalidArgument => Called::InvalidArgument,
                Err(status) => Called::Other(format!("{:?}", status.code())),
            }
        } else {
            let (status, reply) = self
                .http(self.agent_http.address, &self.agent, "/v1/session/execute", Some(token), request)
                .await;
            match status {
                200 => Called::Reply(reply),
                400 => {
                    assert_eq!(reply["error"]["code"], "INVALID_ARGUMENT", "{reply}");
                    Called::InvalidArgument
                }
                other => Called::Other(format!("{other}: {reply}")),
            }
        }
    }

    fn statuses(&self) -> Vec<String> {
        self.admin
            .sql_session()
            .execute("SELECT status FROM shipments ORDER BY id", &[], None)
            .unwrap()
            .rows
            .into_iter()
            .map(|row| match &row.values[0] {
                SqlValue::Text(text) => text.clone(),
                other => panic!("{other:?}"),
            })
            .collect()
    }
}

const BEFORE: [&str; 6] = ["failed", "failed", "failed", "failed", "delivered", "failed"];
const AFTER: [&str; 6] = ["queued", "queued", "queued", "queued", "delivered", "failed"];
const REQUEUE: &str =
    "update many shipments where merchant_id = 42 and status = \"failed\" set status = \"queued\" at most 25";

fn text(source: &str) -> Value {
    json!({"text": {"text": source, "languageVersion": "0.16"}})
}

fn with_operation(mut request: Value, id: &str) -> Value {
    request["operationId"] = json!(id);
    request
}

fn int(v: i64) -> Value {
    json!({"literal": {"intValue": v.to_string()}})
}

fn id_is(v: i64) -> Value {
    json!({"compare": {"left": {"field": "id"}, "comparison": "NATIVE_COMPARISON_EQ", "right": {"value": int(v)}}})
}

/// A typed native update of row 1 (`intent one`).
fn typed_update(table: &str) -> Value {
    json!({"operation": {"request": {"update": {"table": table, "filter": id_is(1),
        "values": [{"column": "status", "value": {"literal": {"textValue": "queued"}}}],
        "intent": {"one": {}}}}}})
}

fn join(table: &str) -> Value {
    let column = |source: &str| json!({"column": {"source": source, "name": "id"}});
    json!({"typedRead": {"table": "shipments", "join": {"table": table, "leftAlias": "a",
        "rightAlias": "b", "left": false,
        "on": {"compare": {"left": column("a"), "comparison": "NATIVE_COMPARISON_EQ", "right": column("b")}}},
        "projection": [{"expression": column("a"), "alias": "l"}]}})
}

/// A task refusal: HTTP 200 / gRPC OK, `code`, `taskError` and `message`, and
/// the session left idle (the abort after a refusal is harmless).
#[track_caller]
fn assert_task_error(reply: &Value, error: &TaskError, request: &Value) {
    assert_eq!(reply["code"], error.sqlstate(), "{request}: {reply}");
    assert_eq!(reply["taskError"], error.name().unwrap(), "{request}: {reply}");
    assert_eq!(reply["message"], error.message(), "{request}: {reply}");
    assert_eq!(reply["transactionState"], "TRANSACTION_STATE_IDLE", "{request}: {reply}");
    for field in ["sql", "operationOutcome", "transactionOutcome", "nativeError"] {
        assert!(reply.get(field).is_none(), "{request}: {reply}");
    }
}

fn not_allowed(class: StatementClass) -> TaskError {
    TaskError::NotAllowedInTaskSession(class)
}

/// The `affected` count of a protected write's reply.
#[track_caller]
fn affected(reply: &Value) -> String {
    assert_eq!(reply["code"], "ok", "{reply}");
    assert!(reply.get("taskError").is_none() && reply.get("message").is_none(), "{reply}");
    reply["sql"]["affected"].as_str().unwrap().to_owned()
}

/// The single row of a `sql` reply, its values as JSON.
#[track_caller]
fn single_row(reply: &Value) -> Vec<Value> {
    assert_eq!(reply["code"], "ok", "{reply}");
    let rows = reply["sql"]["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1, "{reply}");
    rows[0]["values"].as_array().unwrap().clone()
}

#[tokio::test]
async fn begin_commit_rollback_refused() {
    for grpc in [false, true] {
        let f = Fixture::start(grpc).await;
        let token = f.open().await;
        for request in [json!({"begin": {}}), json!({"commit": {}}), json!({"rollback": {}})] {
            let reply = f.call(&token, &request).await.reply();
            assert_task_error(&reply, &not_allowed(StatementClass::TransactionControl), &request);
        }
        for request in [
            json!({"savepoint": {"name": "s"}}),
            json!({"rollbackTo": {"name": "s"}}),
            json!({"release": {"name": "s"}}),
        ] {
            let reply = f.call(&token, &request).await.reply();
            assert_task_error(&reply, &not_allowed(StatementClass::Savepoint), &request);
        }
        // The session still works after every refusal: nothing was left open.
        let reply = f.call(&token, &json!({"ping": {}})).await.reply();
        assert_eq!(reply["code"], "ok");
        assert_eq!(f.statuses(), BEFORE);
        f.stop().await;
    }
}

#[tokio::test]
async fn transaction_block_refused() {
    for grpc in [false, true] {
        let f = Fixture::start(grpc).await;
        let token = f.open().await;
        let typed = json!({"transaction": {"transaction": {"readOnly": false,
            "steps": [{"execute": typed_update("shipments")["operation"]["request"].clone()}]}}});
        for request in [
            typed,
            text("transaction { update shipments where id=1 set status=\"queued\" }"),
            text(&format!("transaction {{ {REQUEUE}; shipments where id=1 select id }}")),
            text("transaction { shipments where id=1 select id }"),
        ] {
            let reply = f.call(&token, &request).await.reply();
            assert_task_error(&reply, &not_allowed(StatementClass::TransactionControl), &request);
            // An operation ID does not make a transaction a protected write.
            let tagged = with_operation(request.clone(), "t1");
            assert_eq!(f.call(&token, &tagged).await, Called::InvalidArgument, "{tagged}");
        }
        assert_eq!(f.statuses(), BEFORE);
        f.stop().await;
    }
}

#[tokio::test]
async fn join_and_legacy_sql_refused() {
    for grpc in [false, true] {
        let f = Fixture::start(grpc).await;
        let token = f.open().await;
        let task = json!({"operation": "1", "selection": {"keys": {"items": [{"tenant": "1", "row": "1"}]}},
            "column": "COLUMN_STATUS", "after": "DELIVERY_STATUS_QUEUED"});
        for request in [
            join("shipments"),
            // Sequences are not a task statement either.
            json!({"sequence": {"nextval": {"name": "s"}}}),
        ] {
            let reply = f.call(&token, &request).await.reply();
            assert_task_error(&reply, &not_allowed(StatementClass::Other), &request);
        }
        for request in [
            json!({"preview": {"request": task.clone()}}),
            json!({"execute": {"request": task.clone()}}),
            json!({"repairSql": {"operation": "1", "sql": "UPDATE x SET y = 1"}}),
            json!({"status": {"operation": "1"}}),
        ] {
            #[cfg(feature = "legacy-task")]
            {
                let reply = f.call(&token, &request).await.reply();
                assert_task_error(&reply, &not_allowed(StatementClass::Other), &request);
            }
            #[cfg(not(feature = "legacy-task"))]
            assert_eq!(f.call(&token, &request).await, Called::InvalidArgument, "{request}");
        }
        // Legacy SQL reaches the same statement admission as pgwire.
        for (sql, error) in [
            ("BEGIN", not_allowed(StatementClass::TransactionControl)),
            ("INSERT INTO shipments VALUES (9, 42, 'new')", not_allowed(StatementClass::Insert)),
            ("DELETE FROM shipments WHERE id = 1", not_allowed(StatementClass::Delete)),
            ("UPDATE shipments SET status = 'queued' WHERE id = 1", TaskError::OperationIdRequired),
        ] {
            let request = json!({"sql": {"sql": sql}});
            let reply = f.call(&token, &request).await.reply();
            assert_eq!(reply["code"], error.sqlstate(), "{sql}: {reply}");
        }
        assert_eq!(f.statuses(), BEFORE);
        f.stop().await;
    }
}

#[tokio::test]
async fn insert_delete_upsert_refused() {
    for grpc in [false, true] {
        let f = Fixture::start(grpc).await;
        let token = f.open().await;
        let insert = json!({"operation": {"request": {"insert": {"table": "shipments",
            "values": [{"column": "id", "value": int(9)}]}}}});
        let delete = json!({"operation": {"request": {"delete": {"table": "shipments", "filter": id_is(1)}}}});
        for (request, class) in [
            (insert, StatementClass::Insert),
            (text("insert shipments set id=9, merchant_id=42, status=\"new\""), StatementClass::Insert),
            (text("insert shipments set id=9 return id"), StatementClass::Insert),
            (
                text("insert into shipments(id) values(1) on conflict(id) do update set status=\"queued\""),
                StatementClass::Insert,
            ),
            (text("insert into shipments(id) values(1) on conflict(id) do nothing"), StatementClass::Insert),
            (delete, StatementClass::Delete),
            (text("delete shipments where id=1"), StatementClass::Delete),
            (text("delete shipments where id=1 return *"), StatementClass::Delete),
        ] {
            let reply = f.call(&token, &request).await.reply();
            assert_task_error(&reply, &not_allowed(class), &request);
            // Not a protected write: an operation ID is a malformed request.
            let tagged = with_operation(request.clone(), "w1");
            assert_eq!(f.call(&token, &tagged).await, Called::InvalidArgument, "{tagged}");
        }
        assert_eq!(f.statuses(), BEFORE);
        let count = f
            .admin
            .sql_session()
            .execute("SELECT count(*) FROM shipments", &[], None)
            .unwrap();
        assert_eq!(count.rows[0].values[0], SqlValue::Int(6));
        f.stop().await;
    }
}

#[tokio::test]
async fn ping_allowed() {
    for grpc in [false, true] {
        let f = Fixture::start(grpc).await;
        let token = f.open().await;
        for request in [json!({"ping": {}}), json!({"searchPath": {}})] {
            let reply = f.call(&token, &request).await.reply();
            assert_eq!(reply["code"], "ok", "{request}: {reply}");
            assert!(reply.get("taskError").is_none() && reply.get("message").is_none(), "{reply}");
            assert_eq!(reply["transactionState"], "TRANSACTION_STATE_IDLE");
        }
        f.stop().await;
    }
}

#[tokio::test]
#[allow(non_snake_case, reason = "the SQLSTATE in the name, as the item names it")]
async fn update_without_operation_id_gives_42T04() {
    for grpc in [false, true] {
        let f = Fixture::start(grpc).await;
        let token = f.open().await;
        for request in [
            typed_update("shipments"),
            text("update shipments where id=1 set status=\"queued\""),
            text(REQUEUE),
            text("update shipments where id=1 set status=\"queued\" return id"),
        ] {
            let reply = f.call(&token, &request).await.reply();
            assert_task_error(&reply, &TaskError::OperationIdRequired, &request);
            assert_eq!(reply["code"], "42T04");
        }
        assert_eq!(f.statuses(), BEFORE);
        f.stop().await;
    }
}

#[tokio::test]
async fn operation_id_on_read_is_invalid_argument() {
    for grpc in [false, true] {
        let f = Fixture::start(grpc).await;
        let token = f.open().await;
        for request in [
            json!({"ping": {}}),
            json!({"searchPath": {}}),
            text("shipments where id=1 select id"),
            json!({"operation": {"request": {"read": {"table": "shipments", "filter": id_is(1), "projection": ["id"]}}}}),
            json!({"typedRead": {"table": "shipments", "projection": [{"expression": {"column": {"name": "id"}}}]}}),
            json!({"taskPreview": {"text": {"text": REQUEUE, "languageVersion": "0.16"}}}),
            json!({"sql": {"sql": "SELECT 1"}}),
        ] {
            let tagged = with_operation(request.clone(), "r1");
            assert_eq!(f.call(&token, &tagged).await, Called::InvalidArgument, "{tagged}");
        }
        // A malformed ID is a bad request even where an ID belongs.
        let too_long = "a".repeat(129);
        for id in ["", "a b", "a'b", "é", too_long.as_str()] {
            let tagged = with_operation(text(REQUEUE), id);
            assert_eq!(f.call(&token, &tagged).await, Called::InvalidArgument, "{id:?}");
        }
        // Apply and outcome need one; apply needs a 32-byte digest.
        let digest = STANDARD.encode([0u8; 32]);
        for request in [
            json!({"taskOutcome": {}}),
            json!({"taskApply": {"changeId": "1", "digest": digest}}),
            with_operation(json!({"taskApply": {"changeId": "1", "digest": STANDARD.encode([0u8; 31])}}), "a1"),
        ] {
            assert_eq!(f.call(&token, &request).await, Called::InvalidArgument, "{request}");
        }
        // The ordinary listener refuses the field and the task forms outright.
        let (status, ordinary) = f
            .http(f.http.address, &f.ordinary, "/v1/sessions", None, &json!({}))
            .await;
        assert_eq!(status, 201, "{ordinary}");
        let ordinary = ordinary["sessionId"].as_str().unwrap().to_owned();
        for request in [
            with_operation(text(REQUEUE), "o1"),
            with_operation(json!({"taskOutcome": {}}), "o1"),
            json!({"taskPreview": {"text": {"text": REQUEUE, "languageVersion": "0.16"}}}),
        ] {
            let (status, reply) = f
                .http(f.http.address, &f.ordinary, "/v1/session/execute", Some(&ordinary), &request)
                .await;
            assert_eq!(status, 400, "{request}: {reply}");
        }
        // Ordinary sessions behave as before.
        let (status, reply) = f
            .http(f.http.address, &f.ordinary, "/v1/session/execute", Some(&ordinary), &text(REQUEUE))
            .await;
        assert_eq!((status, reply["code"].as_str()), (200, Some("ok")), "{reply}");
        f.stop().await;
    }
}

#[tokio::test]
async fn task_errors_are_http_200_with_code_name_message() {
    for grpc in [false, true] {
        let f = Fixture::start(grpc).await;
        let token = f.open().await;
        // 0A000 not_allowed, 42T04 operation_id_required.
        let reply = f.call(&token, &json!({"begin": {}})).await.reply();
        assert_task_error(&reply, &not_allowed(StatementClass::TransactionControl), &json!("begin"));
        assert_eq!(reply["taskError"], "not_allowed");
        assert_eq!(reply["message"], "explicit transactions are not allowed in a task session");
        let reply = f.call(&token, &text(REQUEUE)).await.reply();
        assert_task_error(&reply, &TaskError::OperationIdRequired, &json!(REQUEUE));
        // 42501 denied: a write outside the allowed write.
        let outside = with_operation(text("update shipments where id=1 set status=\"lost\""), "d1");
        let reply = f.call(&token, &outside).await.reply();
        assert_eq!(reply["code"], "42501", "{reply}");
        assert_eq!(reply["taskError"], "denied", "{reply}");
        let rule = [
            DenyRule::OutsideAllowedWrite,
            DenyRule::ColumnNotWritable,
            DenyRule::RolePrivilege,
        ]
        .into_iter()
        .find(|rule| reply["message"] == rule.message());
        assert!(rule.is_some(), "{reply}");
        // 42T03 operation_conflict: the same ID for another request.
        assert_eq!(affected(&f.call(&token, &with_operation(text(REQUEUE), "a1")).await.reply()), "4");
        let other = with_operation(text("update shipments where id=1 set status=\"queued\""), "a1");
        let reply = f.call(&token, &other).await.reply();
        assert_task_error(&reply, &TaskError::OperationConflict, &other);
        // A refused apply of an unknown preview is a task error too, never
        // a transport status.
        let unknown = with_operation(
            json!({"taskApply": {"changeId": "999", "digest": STANDARD.encode([7u8; 32])}}),
            "a2",
        );
        let reply = f.call(&token, &unknown).await.reply();
        assert_ne!(reply["code"], "ok", "{reply}");
        assert_eq!(reply["transactionState"], "TRANSACTION_STATE_IDLE", "{reply}");
        if let Some(name) = reply.get("taskError") {
            assert!(reply["message"].is_string(), "{reply}");
            assert!(name.is_string(), "{reply}");
        }
        // No message carries a value, a key or a count.
        let message = reply["message"].as_str().unwrap_or("");
        assert!(!message.contains("999") && !message.contains("a2"), "{reply}");
        assert_eq!(f.statuses(), AFTER);
        f.stop().await;
    }
}

#[tokio::test]
async fn single_statement_text_lowers_to_operation() {
    // The unverified point of design-2 4(c): one native statement never lowers
    // to a transaction, so no single statement is refused as one.
    let bindings = Default::default();
    for source in [
        "shipments where id=1 select id",
        "update shipments where id=1 set status=\"queued\"",
        REQUEUE,
        "update shipments where id=1 set status=\"queued\" return id",
        "insert shipments set id=9",
        "insert into shipments(id) values(1) on conflict(id) do nothing",
        "delete shipments where id=1",
        "delete shipments where id=1 return *",
    ] {
        let lowered = giggles_language::lower_text(source, &bindings, giggles_language::LanguageVersion::Embedded016)
            .unwrap_or_else(|error| panic!("{source}: {error:?}"));
        assert!(matches!(lowered, giggles_language::Lowered::Operation { .. }), "{source}");
        let wire = wire_json::decode_execute(&serde_json::to_vec(&text(source)).unwrap()).unwrap();
        assert!(
            matches!(codec::decode_execute(wire).unwrap().request, Invocation::Operation { .. }),
            "{source}"
        );
    }
    let block = "transaction { update shipments where id=1 set status=\"queued\" }";
    let wire = wire_json::decode_execute(&serde_json::to_vec(&text(block)).unwrap()).unwrap();
    assert!(matches!(codec::decode_execute(wire).unwrap().request, Invocation::Transaction { .. }));

    // Over the wire, a single-statement text update with an ID is one
    // protected commit; the resend replays; the outcome is stored.
    for grpc in [false, true] {
        let f = Fixture::start(grpc).await;
        let token = f.open().await;
        let request = with_operation(text(REQUEUE), "inc-436:requeue.1");
        assert_eq!(affected(&f.call(&token, &request).await.reply()), "4");
        assert_eq!(f.statuses(), AFTER);
        f.admin
            .sql_session()
            .execute("UPDATE shipments SET status = 'failed' WHERE id = 1", &[], None)
            .unwrap();
        assert_eq!(affected(&f.call(&token, &request).await.reply()), "4");
        assert_eq!(f.statuses()[0], "failed");
        let outcome = with_operation(json!({"taskOutcome": {}}), "inc-436:requeue.1");
        let row = single_row(&f.call(&token, &outcome).await.reply());
        assert_eq!(row[0]["textValue"], "inc-436:requeue.1");
        assert_eq!(row[1]["textValue"], "committed");
        assert_eq!(row[2]["intValue"], "4");
        // An unknown ID has no row.
        let reply = f
            .call(&token, &with_operation(json!({"taskOutcome": {}}), "never"))
            .await
            .reply();
        assert_eq!(reply["code"], "ok", "{reply}");
        assert!(reply["sql"]["rows"].as_array().is_none_or(|rows| rows.is_empty()), "{reply}");
        // The typed form of the same kind of write commits as well.
        assert_eq!(
            affected(&f.call(&token, &with_operation(typed_update("shipments"), "typed.1")).await.reply()),
            "1"
        );
        assert_eq!(f.statuses(), AFTER);
        f.stop().await;
    }
}

#[tokio::test]
async fn preview_then_apply_over_the_api() {
    for grpc in [false, true] {
        let f = Fixture::start(grpc).await;
        let token = f.open().await;
        for (n, preview) in [
            json!({"taskPreview": {"text": {"text": REQUEUE, "languageVersion": "0.16"}}}),
            json!({"taskPreview": {"operation": typed_update("shipments")["operation"].clone()}}),
        ]
        .into_iter()
        .enumerate()
        {
            let row = single_row(&f.call(&token, &preview).await.reply());
            let change: u64 = row[0]["intValue"].as_str().unwrap().parse().unwrap();
            let digest = hex::decode(row[1]["textValue"].as_str().unwrap()).unwrap();
            assert_eq!(digest.len(), 32);
            assert_eq!(row[2]["intValue"], if n == 0 { "4" } else { "1" });
            assert_eq!(f.statuses(), BEFORE, "a preview writes nothing");
            // The wrong digest writes nothing.
            let wrong = with_operation(
                json!({"taskApply": {"changeId": change.to_string(), "digest": STANDARD.encode([0u8; 32])}}),
                &format!("bad.{n}"),
            );
            let reply = f.call(&token, &wrong).await.reply();
            assert_ne!(reply["code"], "ok", "{reply}");
            assert_eq!(f.statuses(), BEFORE);
            let apply = with_operation(
                json!({"taskApply": {"changeId": change.to_string(), "digest": STANDARD.encode(&digest)}}),
                &format!("apply.{n}"),
            );
            let expected = if n == 0 { "4" } else { "1" };
            assert_eq!(affected(&f.call(&token, &apply).await.reply()), expected);
            if n == 0 {
                assert_eq!(f.statuses(), AFTER);
                f.admin
                    .sql_session()
                    .execute("UPDATE shipments SET status = 'failed' WHERE id < 5", &[], None)
                    .unwrap();
            }
        }
        // A preview of anything but an update is refused before it runs.
        let read = json!({"taskPreview": {"text": {"text": "shipments where id=1 select id", "languageVersion": "0.16"}}});
        let reply = f.call(&token, &read).await.reply();
        assert_eq!(reply["code"], "0A000", "{reply}");
        f.stop().await;
    }
}

#[tokio::test]
async fn refusal_precedes_table_lookup() {
    for grpc in [false, true] {
        let f = Fixture::start(grpc).await;
        let token = f.open().await;
        let insert = |table: &str| text(&format!("insert {table} set id=9"));
        let delete = |table: &str| text(&format!("delete {table} where id=1"));
        let update = |table: &str| text(&format!("update {table} where id=1 set status=\"queued\""));
        let typed_insert = |table: &str| {
            json!({"operation": {"request": {"insert": {"table": table, "values": [{"column": "id", "value": int(9)}]}}}})
        };
        for (known, unknown) in [
            (insert("shipments"), insert("nosuch")),
            (delete("shipments"), delete("nosuch")),
            (update("shipments"), update("nosuch")),
            (typed_insert("shipments"), typed_insert("nosuch")),
            (typed_update("shipments"), typed_update("nosuch")),
            (join("shipments"), join("nosuch")),
            (
                json!({"catalog": {"dropTable": {"table": "shipments"}}}),
                json!({"catalog": {"dropTable": {"table": "nosuch"}}}),
            ),
        ] {
            let known_reply = f.call(&token, &known).await.reply();
            let unknown_reply = f.call(&token, &unknown).await.reply();
            assert_eq!(known_reply, unknown_reply, "{known} / {unknown}");
            assert!(known_reply["taskError"].is_string(), "{known_reply}");
            assert_ne!(known_reply["code"], "42P01");
        }
        assert_eq!(f.statuses(), BEFORE);
        f.stop().await;
    }
}

/// The native intent bound of a refused protected update: a native error,
/// with nothing committed or recorded.
#[track_caller]
fn assert_intent_refused(reply: &Value, kind: &str) {
    assert_eq!(reply["code"], "native_error", "{reply}");
    assert_eq!(reply["nativeError"]["kind"], kind, "{reply}");
    assert!(reply.get("taskError").is_none() && reply.get("sql").is_none(), "{reply}");
}

/// No outcome is stored under `id` (a refusal that was not recorded).
async fn no_outcome(f: &Fixture, token: &str, id: &str) {
    let reply = f
        .call(token, &with_operation(json!({"taskOutcome": {}}), id))
        .await
        .reply();
    assert_eq!(reply["code"], "ok", "{reply}");
    assert!(reply["sql"]["rows"].as_array().is_none_or(|rows| rows.is_empty()), "{id}: {reply}");
}

#[tokio::test]
async fn native_intent_bounds_the_protected_commit() {
    // Four rows (1-4) match; the intent is checked on the real affected count
    // at commit time, before anything is staged.
    let one = "update shipments where merchant_id = 42 and status = \"failed\" set status = \"queued\"";
    let at_most = |n: u32| {
        format!(
            "update many shipments where merchant_id = 42 and status = \"failed\" \
             set status = \"queued\" at most {n}"
        )
    };
    for grpc in [false, true] {
        let f = Fixture::start(grpc).await;
        let token = f.open().await;
        // At most 3 with 4 matches: refused, nothing recorded.
        let request = with_operation(text(&at_most(3)), "many.3");
        let reply = f.call(&token, &request).await.reply();
        assert_intent_refused(&reply, "NATIVE_ERROR_CLASS_TOO_MANY_MATCHES");
        assert_eq!(f.statuses(), BEFORE);
        no_outcome(&f, &token, "many.3").await;
        // At most 4 with exactly 4 matches commits.
        let request = with_operation(text(&at_most(4)), "many.4");
        assert_eq!(affected(&f.call(&token, &request).await.reply()), "4");
        assert_eq!(f.statuses(), AFTER);
        f.admin
            .sql_session()
            .execute("UPDATE shipments SET status = 'failed' WHERE id < 5", &[], None)
            .unwrap();
        // Intent one with 4 matches: refused, nothing recorded; a preview of
        // it returns no change ID either.
        let request = with_operation(text(one), "one.1");
        let reply = f.call(&token, &request).await.reply();
        assert_intent_refused(&reply, "NATIVE_ERROR_CLASS_AMBIGUOUS_MATCH");
        assert_eq!(f.statuses(), BEFORE);
        no_outcome(&f, &token, "one.1").await;
        let preview = json!({"taskPreview": {"text": {"text": one, "languageVersion": "0.16"}}});
        let reply = f.call(&token, &preview).await.reply();
        assert_intent_refused(&reply, "NATIVE_ERROR_CLASS_AMBIGUOUS_MATCH");
        // A refused attempt is not recorded, so its ID stays free: the same
        // request is refused again while 4 rows match, and commits once one
        // row matches.
        let reply = f.call(&token, &request).await.reply();
        assert_intent_refused(&reply, "NATIVE_ERROR_CLASS_AMBIGUOUS_MATCH");
        assert_eq!(f.statuses(), BEFORE);
        no_outcome(&f, &token, "one.1").await;
        f.admin
            .sql_session()
            .execute("UPDATE shipments SET status = 'delivered' WHERE id IN (2, 3, 4)", &[], None)
            .unwrap();
        assert_eq!(affected(&f.call(&token, &request).await.reply()), "1");
        let outcome = with_operation(json!({"taskOutcome": {}}), "one.1");
        let row = single_row(&f.call(&token, &outcome).await.reply());
        assert_eq!(row[1]["textValue"], "committed");
        assert_eq!(row[2]["intValue"], "1");
        assert_eq!(f.statuses()[..5], ["queued", "delivered", "delivered", "delivered", "delivered"]);
        f.stop().await;
    }
}

#[tokio::test]
async fn native_intent_resend_conflicts_when_bound_changes() {
    for grpc in [false, true] {
        let f = Fixture::start(grpc).await;
        let token = f.open().await;
        let native = |n| {
            format!(
                "update many shipments where merchant_id = 42 and status = \"failed\" set status = \"queued\" at most {n}"
            )
        };
        let request = with_operation(text(&native(4)), "intent-resend");
        assert_eq!(affected(&f.call(&token, &request).await.reply()), "4");
        assert_eq!(f.statuses(), AFTER);
        assert_eq!(affected(&f.call(&token, &request).await.reply()), "4");
        assert_eq!(f.statuses(), AFTER);
        let changed = with_operation(text(&native(5)), "intent-resend");
        let reply = f.call(&token, &changed).await.reply();
        assert_task_error(&reply, &TaskError::OperationConflict, &changed);
        assert_eq!(f.statuses(), AFTER);
        f.stop().await;
    }
}
