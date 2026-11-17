//! R3-208 (GD-0061 design-2 sections 1 and 3): the agent HTTP and gRPC
//! listeners. They run on their own `ApiRuntime` (its own registry), accept only
//! certificates in the agent map, open task sessions, and every statement on
//! such a session reaches the same task admission as the agent pgwire path.
#![cfg(feature = "engine-rocksdb")]
mod agent_support;

use agent_support::{TestCert, admin, binding, certificate, create_task, revoke};
use bytes::Bytes;
use giggles_server::{
    Binding, Role, Running, Service,
    admission::ServiceLimits,
    api::{
        ApiLimits, ApiRuntime, ListenerKind,
        net::{serve_grpc_bound, serve_http_bound},
        proto,
    },
    clock::Clock,
    tls,
};
use http_body_util::{BodyExt, Full};
use hyper_util::rt::{TokioExecutor, TokioIo};
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

/// Ordinary and agent HTTP and gRPC listeners over one service and one CA.
struct Fixture {
    admin: giggles_core::Admin,
    ca: TestCert,
    service: Arc<Service>,
    ordinary_api: Arc<ApiRuntime>,
    agent_api: Arc<ApiRuntime>,
    http: Running,
    grpc: Running,
    agent_http: Running,
    agent_grpc: Running,
    /// Bound to `incident_controller` in the ordinary map.
    ordinary: TestCert,
    /// Bound to `agent_exec` for `inc-436` in the agent map.
    agent: TestCert,
}

impl Fixture {
    async fn start(tasks: &[&str]) -> Self {
        let admin = admin();
        for task in ["inc-436", "inc-437"] {
            create_task(&admin, task, "agent_exec");
        }
        let ca = certificate(None, false);
        let server = certificate(Some(&ca), true);
        let ordinary = certificate(Some(&ca), false);
        let agent = certificate(Some(&ca), false);
        let agents = vec![binding(&agent, "agent_exec", tasks)];
        let service = Service::with_agent(
            admin.clone(),
            vec![Binding {
                fingerprint: ordinary.fingerprint(),
                role: Role::Database {
                    name: "incident_controller".into(),
                },
            }],
            agents,
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
        let agent_api =
            ApiRuntime::new_agent(service.clone(), ApiLimits { sessions: 4 }).unwrap();
        assert_eq!(ordinary_api.kind(), ListenerKind::Ordinary);
        assert_eq!(agent_api.kind(), ListenerKind::Agent);
        let (_, gate) = watch::channel(true);
        let bind = || async { TcpListener::bind("127.0.0.1:0").await.unwrap() };
        let http = serve_http_bound(bind().await, ordinary_api.clone(), config.clone(), 8, gate.clone())
            .await
            .unwrap();
        let grpc = serve_grpc_bound(bind().await, ordinary_api.clone(), config.clone(), 8, gate.clone())
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
            service,
            ordinary_api,
            agent_api,
            http,
            grpc,
            agent_http,
            agent_grpc,
            ordinary,
            agent,
        }
    }

    async fn stop(self) {
        for listener in [self.http, self.grpc, self.agent_http, self.agent_grpc] {
            listener.shutdown().await.unwrap();
        }
        self.ordinary_api.shutdown().await;
        self.agent_api.shutdown().await;
    }

    #[allow(clippy::too_many_arguments)]
    async fn http(
        &self,
        address: SocketAddr,
        cert: &TestCert,
        method: &str,
        path: &str,
        token: Option<&str>,
        body: Value,
        h2: bool,
    ) -> (u16, Value) {
        let body = serde_json::to_vec(&body).unwrap();
        tokio::time::timeout(Duration::from_secs(15), async {
            let config = tls::client(
                vec![cert.cert.der().clone()],
                PrivatePkcs8KeyDer::from(cert.key.serialize_der()).into(),
                vec![self.ca.cert.der().clone()],
            )
            .unwrap();
            let mut config = (*config).clone();
            config.alpn_protocols = vec![if h2 { b"h2".to_vec() } else { b"http/1.1".to_vec() }];
            let socket = TcpStream::connect(address).await.unwrap();
            let stream = TlsConnector::from(Arc::new(config))
                .connect(ServerName::try_from("localhost").unwrap(), socket)
                .await
                .unwrap();
            let mut request = hyper::Request::builder()
                .method(method)
                .uri(format!("https://localhost:{}{path}", address.port()))
                .header("host", "localhost")
                .header("content-type", "application/json");
            if let Some(token) = token {
                request = request.header("x-giggles-session", token);
            }
            let request = request.body(Full::new(Bytes::from(body))).unwrap();
            let (response, task) = if h2 {
                let (mut sender, connection) =
                    hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(stream))
                        .await
                        .unwrap();
                let task = tokio::spawn(async move {
                    let _ = connection.await;
                });
                (sender.send_request(request).await.unwrap(), task)
            } else {
                let (mut sender, connection) =
                    hyper::client::conn::http1::handshake(TokioIo::new(stream)).await.unwrap();
                let task = tokio::spawn(async move {
                    let _ = connection.await;
                });
                (sender.send_request(request).await.unwrap(), task)
            };
            let status = response.status().as_u16();
            let bytes = response.into_body().collect().await.unwrap().to_bytes();
            task.abort();
            let _ = task.await;
            (status, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
        })
        .await
        .expect("HTTP call finished")
    }

    async fn open_http(&self, address: SocketAddr, cert: &TestCert, body: Value, h2: bool) -> (u16, Value) {
        self.http(address, cert, "POST", "/v1/sessions", None, body, h2).await
    }

    async fn exec_http(
        &self,
        address: SocketAddr,
        cert: &TestCert,
        token: &str,
        body: Value,
    ) -> (u16, Value) {
        self.http(address, cert, "POST", "/v1/session/execute", Some(token), body, false)
            .await
    }

    async fn grpc(
        &self,
        address: SocketAddr,
        cert: &TestCert,
    ) -> proto::giggles_client::GigglesClient<Channel> {
        let channel = Channel::from_shared(format!("https://127.0.0.1:{}", address.port()))
            .unwrap()
            .tls_config(
                ClientTlsConfig::new()
                    .domain_name("localhost")
                    .ca_certificate(CaCertificate::from_pem(self.ca.cert.pem()))
                    .identity(Identity::from_pem(cert.cert.pem(), cert.key.serialize_pem())),
            )
            .unwrap()
            .connect_timeout(Duration::from_secs(10))
            .connect()
            .await
            .unwrap();
        proto::giggles_client::GigglesClient::new(channel)
            .max_decoding_message_size(giggles_server::MAX_RESPONSE)
    }

    async fn open_grpc(
        &self,
        address: SocketAddr,
        cert: &TestCert,
        task: Option<&str>,
    ) -> Result<String, tonic::Status> {
        self.grpc(address, cert)
            .await
            .open_session(proto::OpenSessionRequest {
                task: task.map(str::to_owned),
                ..Default::default()
            })
            .await
            .map(|reply| reply.into_inner().session_id)
    }
}

fn rpc<T>(token: &str, value: T) -> tonic::Request<T> {
    let mut request = tonic::Request::new(value);
    request
        .metadata_mut()
        .insert("x-giggles-session", token.parse().unwrap());
    request
}

fn grpc_sql(sql: &str) -> proto::ExecuteRequest {
    proto::ExecuteRequest {
        request: Some(proto::execute_request::Request::Sql(proto::SqlRequest {
            sql: sql.into(),
            params: vec![],
        })),
        budget_ms: None, operation_id: None,
    }
}

fn grpc_text(text: &str) -> proto::ExecuteRequest {
    proto::ExecuteRequest {
        request: Some(proto::execute_request::Request::Text(proto::NativeTextRequest {
            text: text.into(),
            bindings: vec![],
            language_version: None,
        })),
        budget_ms: None, operation_id: None,
    }
}

fn grpc_ping() -> proto::ExecuteRequest {
    proto::ExecuteRequest {
        request: Some(proto::execute_request::Request::Ping(proto::PingRequest {})),
        budget_ms: None, operation_id: None,
    }
}

fn session_id(open: &(u16, Value)) -> String {
    assert_eq!(open.0, 201, "{}", open.1);
    open.1["sessionId"].as_str().unwrap().to_owned()
}

const UPDATE_SQL: &str = "UPDATE shipments SET status = 'x' WHERE id = 1";
const UPDATE_TEXT: &str = "update shipments where id=1 set status=\"x\"";

#[tokio::test]
async fn open_with_task_http_json_http_protobuf_grpc() {
    let f = Fixture::start(&["inc-436"]).await;
    let address = f.agent_http.address;
    // HTTP/1.1 JSON with the task named; HTTP/2 JSON with it implicit (one task).
    for (body, h2) in [(json!({"task": "inc-436"}), false), (json!({}), true)] {
        let token = session_id(&f.open_http(address, &f.agent, body, h2).await);
        let (status, reply) = f.exec_http(address, &f.agent, &token, json!({"ping": {}})).await;
        assert_eq!((status, reply["code"].as_str()), (200, Some("ok")), "{reply}");
        // A Show-class native request is admitted.
        let (status, reply) =
            f.exec_http(address, &f.agent, &token, json!({"searchPath": {}})).await;
        assert_eq!((status, reply["code"].as_str()), (200, Some("ok")), "{reply}");
    }
    // gRPC (protobuf) with the task field.
    let token = f.open_grpc(f.agent_grpc.address, &f.agent, Some("inc-436")).await.unwrap();
    let mut client = f.grpc(f.agent_grpc.address, &f.agent).await;
    let reply = client.execute(rpc(&token, grpc_ping())).await.unwrap().into_inner();
    assert_eq!(reply.code, "ok");
    f.stop().await;
}

/// The hard requirement: no statement on an agent API session gets around the
/// task admission the pgwire path uses.
#[tokio::test]
async fn agent_api_statements_reach_admission() {
    let f = Fixture::start(&["inc-436"]).await;
    let address = f.agent_http.address;
    let token = session_id(&f.open_http(address, &f.agent, json!({"task": "inc-436"}), false).await);
    for (body, code) in [
        // `Select` is admitted since R3-315; the executor holds no SELECT
        // grant here, so the privilege intersection refuses it.
        (json!({"sql": {"sql": "SELECT id FROM shipments"}}), "42501"),
        (json!({"sql": {"sql": "BEGIN"}}), "0A000"),
        (json!({"sql": {"sql": "INSERT INTO shipments VALUES (1, 42, 'new')"}}), "0A000"),
        (json!({"sql": {"sql": UPDATE_SQL}}), "42T04"),
        (json!({"text": {"text": UPDATE_TEXT}}), "42T04"),
        // A native text read is a `Select`: admitted, then refused by the
        // privilege intersection (no SELECT grant here).
        (json!({"text": {"text": "shipments where id=1"}}), "42501"),
        (json!({"begin": {}}), "0A000"),
        (json!({"searchPath": {"set": true, "schemas": ["public"]}}), "ok"),
    ] {
        let (status, reply) = f.exec_http(address, &f.agent, &token, body.clone()).await;
        assert_eq!((status, reply["code"].as_str()), (200, Some(code)), "{body}: {reply}");
    }
    let grpc_address = f.agent_grpc.address;
    let token = f.open_grpc(grpc_address, &f.agent, None).await.unwrap();
    let mut client = f.grpc(grpc_address, &f.agent).await;
    for (request, code) in [
        // A relation-free probe is a `Select` and is admitted (R3-315).
        (grpc_sql("SELECT 1"), "ok"),
        (grpc_sql(UPDATE_SQL), "42T04"),
        (grpc_text(UPDATE_TEXT), "42T04"),
    ] {
        let reply = client.execute(rpc(&token, request)).await.unwrap().into_inner();
        assert_eq!(reply.code, code);
    }
    // No refused statement changed anything.
    let rows = f
        .admin
        .sql_session()
        .execute("SELECT count(*) FROM shipments", &[], None)
        .unwrap();
    assert_eq!(rows.rows[0].values[0], giggles_core::relational::Value::Int(0));
    f.stop().await;
}

#[tokio::test]
async fn open_refused_unknown_cert_unlisted_task_revoked() {
    let f = Fixture::start(&["inc-436", "inc-437"]).await;
    let address = f.agent_http.address;
    let grpc_address = f.agent_grpc.address;
    let unknown = certificate(Some(&f.ca), false);
    let (status, reply) = f.open_http(address, &unknown, json!({"task": "inc-436"}), false).await;
    assert_eq!((status, reply["error"]["code"].as_str()), (401, Some("UNAUTHENTICATED")));
    let error = f.open_grpc(grpc_address, &unknown, Some("inc-436")).await.unwrap_err();
    assert_eq!(error.code(), Code::Unauthenticated);
    // A task the binding does not list, and no task when it lists two.
    for body in [json!({"task": "inc-999"}), json!({})] {
        let (status, _) = f.open_http(address, &f.agent, body, false).await;
        assert_eq!(status, 401);
    }
    for task in [Some("inc-999"), None] {
        let error = f.open_grpc(grpc_address, &f.agent, task).await.unwrap_err();
        assert_eq!(error.code(), Code::Unauthenticated);
    }
    // A malformed task field is a bad request, not a refusal.
    for body in [json!({"task": ""}), json!({"task": "bad name"}), json!({"task": 1}), json!({"other": "x"})] {
        let (status, _) = f.open_http(address, &f.agent, body.clone(), false).await;
        assert_eq!(status, 400, "{body}");
    }
    let error = f.open_grpc(grpc_address, &f.agent, Some("")).await.unwrap_err();
    assert_eq!(error.code(), Code::InvalidArgument);
    // A revoked task is refused; the other task still opens.
    revoke(&f.admin, "inc-437");
    let (status, _) = f.open_http(address, &f.agent, json!({"task": "inc-437"}), false).await;
    assert_eq!(status, 401);
    let error = f.open_grpc(grpc_address, &f.agent, Some("inc-437")).await.unwrap_err();
    assert_eq!(error.code(), Code::Unauthenticated);
    session_id(&f.open_http(address, &f.agent, json!({"task": "inc-436"}), false).await);
    f.stop().await;
}

#[tokio::test]
async fn ordinary_cert_refused_on_agent_http_and_grpc() {
    let f = Fixture::start(&["inc-436"]).await;
    // The ordinary certificate works on the ordinary listeners.
    session_id(&f.open_http(f.http.address, &f.ordinary, json!({}), false).await);
    f.open_grpc(f.grpc.address, &f.ordinary, None).await.unwrap();
    for body in [json!({}), json!({"task": "inc-436"})] {
        let (status, _) = f.open_http(f.agent_http.address, &f.ordinary, body, false).await;
        assert_eq!(status, 401);
    }
    for task in [None, Some("inc-436")] {
        let error = f.open_grpc(f.agent_grpc.address, &f.ordinary, task).await.unwrap_err();
        assert_eq!(error.code(), Code::Unauthenticated);
    }
    f.stop().await;
}

#[tokio::test]
async fn agent_cert_refused_on_ordinary_http_and_grpc() {
    let f = Fixture::start(&["inc-436"]).await;
    for body in [json!({}), json!({"task": "inc-436"})] {
        let (status, _) = f.open_http(f.http.address, &f.agent, body, false).await;
        assert_eq!(status, 401);
    }
    for task in [None, Some("inc-436")] {
        let error = f.open_grpc(f.grpc.address, &f.agent, task).await.unwrap_err();
        assert_eq!(error.code(), Code::Unauthenticated);
    }
    // A task field on the ordinary runtime is a bad request.
    let (status, _) = f
        .open_http(f.http.address, &f.ordinary, json!({"task": "inc-436"}), false)
        .await;
    assert_eq!(status, 400);
    let error = f
        .open_grpc(f.grpc.address, &f.ordinary, Some("inc-436"))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::InvalidArgument);
    f.stop().await;
}

#[tokio::test]
async fn session_token_not_valid_across_runtimes() {
    let f = Fixture::start(&["inc-436"]).await;
    let agent_token =
        session_id(&f.open_http(f.agent_http.address, &f.agent, json!({}), false).await);
    let ordinary_token = session_id(&f.open_http(f.http.address, &f.ordinary, json!({}), false).await);
    // Each certificate is valid on its own listener, but the other runtime's
    // token is not in this runtime's registry.
    let (status, reply) = f
        .exec_http(f.agent_http.address, &f.agent, &ordinary_token, json!({"ping": {}}))
        .await;
    assert_eq!((status, reply["error"]["code"].as_str()), (404, Some("SESSION_NOT_FOUND")));
    let (status, reply) = f
        .exec_http(f.http.address, &f.ordinary, &agent_token, json!({"ping": {}}))
        .await;
    assert_eq!((status, reply["error"]["code"].as_str()), (404, Some("SESSION_NOT_FOUND")));
    let mut agent = f.grpc(f.agent_grpc.address, &f.agent).await;
    let error = agent.execute(rpc(&ordinary_token, grpc_ping())).await.unwrap_err();
    assert_eq!(error.code(), Code::NotFound);
    let mut ordinary = f.grpc(f.grpc.address, &f.ordinary).await;
    let error = ordinary.execute(rpc(&agent_token, grpc_ping())).await.unwrap_err();
    assert_eq!(error.code(), Code::NotFound);
    // Both tokens still work where they were issued.
    let (status, _) = f
        .exec_http(f.agent_http.address, &f.agent, &agent_token, json!({"ping": {}}))
        .await;
    assert_eq!(status, 200);
    let reply = ordinary.execute(rpc(&ordinary_token, grpc_ping())).await.unwrap();
    assert_eq!(reply.into_inner().code, "ok");
    f.stop().await;
}

#[tokio::test]
async fn reload_cuts_open_api_session() {
    let f = Fixture::start(&["inc-436", "inc-437"]).await;
    let http_token = session_id(
        &f.open_http(f.agent_http.address, &f.agent, json!({"task": "inc-436"}), false).await,
    );
    let grpc_token = f
        .open_grpc(f.agent_grpc.address, &f.agent, Some("inc-436"))
        .await
        .unwrap();
    let mut client = f.grpc(f.agent_grpc.address, &f.agent).await;
    assert_eq!(
        client.execute(rpc(&grpc_token, grpc_ping())).await.unwrap().into_inner().code,
        "ok"
    );
    // Changing the entry's task list gives it a new epoch, which cuts every
    // session the certificate opened.
    f.service
        .reload_all_bindings(
            vec![Binding {
                fingerprint: f.ordinary.fingerprint(),
                role: Role::Database {
                    name: "incident_controller".into(),
                },
            }],
            vec![binding(&f.agent, "agent_exec", &["inc-436"])],
        )
        .unwrap();
    f.agent_api.invalidate_bindings();
    // The live gRPC connection still carries the old epoch: 401.
    let error = client.execute(rpc(&grpc_token, grpc_ping())).await.unwrap_err();
    assert_eq!(error.code(), Code::Unauthenticated);
    // Each HTTP call here is a new TLS connection, verified under the new
    // epoch; the cut session is gone from the registry.
    let (status, reply) = f
        .exec_http(f.agent_http.address, &f.agent, &http_token, json!({"ping": {}}))
        .await;
    assert_eq!((status, reply["error"]["code"].as_str()), (404, Some("SESSION_NOT_FOUND")));
    // A fresh connection under the new binding opens again.
    let token = session_id(&f.open_http(f.agent_http.address, &f.agent, json!({}), false).await);
    let (status, _) = f
        .exec_http(f.agent_http.address, &f.agent, &token, json!({"ping": {}}))
        .await;
    assert_eq!(status, 200);
    // Removing the binding refuses the next open outright.
    f.service
        .reload_all_bindings(
            vec![Binding {
                fingerprint: f.ordinary.fingerprint(),
                role: Role::Database {
                    name: "incident_controller".into(),
                },
            }],
            vec![],
        )
        .unwrap();
    let (status, _) = f.open_http(f.agent_http.address, &f.agent, json!({}), false).await;
    assert_eq!(status, 401);
    f.stop().await;
}
