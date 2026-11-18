#![cfg(feature = "engine-rocksdb")]
mod agent_support;

use agent_support::{TestCert, admin, binding, certificate, create_task};
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

/// An ordinary SQL issuer and agent HTTP/gRPC listeners sharing one service.
struct Fixture {
    ca: TestCert,
    agent_api: Arc<ApiRuntime>,
    http: Running,
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
        admin
            .roles()
            .grant_table(
                "shipments",
                "agent_exec",
                &[giggles_core::roles::TablePrivilege::Select],
            )
            .unwrap();
        admin
            .sql_session()
            .execute(
                "INSERT INTO shipments VALUES (1,42,'pending'),(2,7,'hidden')",
                &[],
                None,
            )
            .unwrap();
        let agents = if tasks.is_empty() {
            vec![]
        } else {
            vec![binding(&agent, "agent_exec", tasks)]
        };
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
        let config = tls::server_optional_client(
            vec![server.cert.der().clone()],
            PrivatePkcs8KeyDer::from(server.key.serialize_der()).into(),
            Some(vec![ca.cert.der().clone()]),
        )
        .unwrap();
        let agent_api = ApiRuntime::new_agent(service.clone(), ApiLimits { sessions: 4 }).unwrap();
        assert_eq!(agent_api.kind(), ListenerKind::Agent);
        let (_, gate) = watch::channel(true);
        let bind = || async { TcpListener::bind("127.0.0.1:0").await.unwrap() };
        let http = giggles_server::serve_pg_bound(
            bind().await,
            service.clone(),
            config.clone(),
            8,
            gate.clone(),
        )
        .await
        .unwrap();
        let agent_http = serve_http_bound(
            bind().await,
            agent_api.clone(),
            config.clone(),
            8,
            gate.clone(),
        )
        .await
        .unwrap();
        let agent_grpc = serve_grpc_bound(bind().await, agent_api.clone(), config, 8, gate)
            .await
            .unwrap();
        Self {
            ca,
            agent_api,
            http,
            agent_http,
            agent_grpc,
            ordinary,
            agent,
        }
    }

    async fn stop(self) {
        for listener in [self.http, self.agent_http, self.agent_grpc] {
            listener.shutdown().await.unwrap();
        }
        self.agent_api.shutdown().await;
    }

    #[allow(clippy::too_many_arguments)]
    async fn http(
        &self,
        address: SocketAddr,
        cert: Option<&TestCert>,
        authorization: &[&str],
        method: &str,
        path: &str,
        token: Option<&str>,
        body: Value,
        h2: bool,
    ) -> (u16, Value) {
        let body = serde_json::to_vec(&body).unwrap();
        tokio::time::timeout(Duration::from_secs(15), async {
            let config = cert.map_or_else(
                || agent_support::no_cert_client(&self.ca),
                |cert| cert.client(&self.ca),
            );
            let mut config = config;
            config.alpn_protocols = vec![if h2 {
                b"h2".to_vec()
            } else {
                b"http/1.1".to_vec()
            }];
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
            for value in authorization {
                request = request.header("authorization", *value);
            }
            if let Some(token) = token {
                request = request.header("x-giggles-session", token);
            }
            let request = request.body(Full::new(Bytes::from(body))).unwrap();
            let (response, task) = if h2 {
                let (mut sender, connection) = hyper::client::conn::http2::handshake(
                    TokioExecutor::new(),
                    TokioIo::new(stream),
                )
                .await
                .unwrap();
                let task = tokio::spawn(async move {
                    let _ = connection.await;
                });
                (sender.send_request(request).await.unwrap(), task)
            } else {
                let (mut sender, connection) =
                    hyper::client::conn::http1::handshake(TokioIo::new(stream))
                        .await
                        .unwrap();
                let task = tokio::spawn(async move {
                    let _ = connection.await;
                });
                (sender.send_request(request).await.unwrap(), task)
            };
            let status = response.status().as_u16();
            let bytes = response.into_body().collect().await.unwrap().to_bytes();
            task.abort();
            let _ = task.await;
            (
                status,
                serde_json::from_slice(&bytes).unwrap_or(Value::Null),
            )
        })
        .await
        .expect("HTTP call finished")
    }

    async fn open_http(
        &self,
        address: SocketAddr,
        cert: &TestCert,
        body: Value,
        h2: bool,
    ) -> (u16, Value) {
        self.http(
            address,
            Some(cert),
            &[],
            "POST",
            "/v1/sessions",
            None,
            body,
            h2,
        )
        .await
    }

    async fn grpc(
        &self,
        address: SocketAddr,
        cert: Option<&TestCert>,
    ) -> proto::giggles_client::GigglesClient<Channel> {
        let mut tls = ClientTlsConfig::new()
            .domain_name("localhost")
            .ca_certificate(CaCertificate::from_pem(self.ca.cert.pem()));
        if let Some(cert) = cert {
            tls = tls.identity(Identity::from_pem(
                cert.cert.pem(),
                cert.key.serialize_pem(),
            ));
        }
        let channel = Channel::from_shared(format!("https://127.0.0.1:{}", address.port()))
            .unwrap()
            .tls_config(tls)
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
        self.grpc(address, Some(cert))
            .await
            .open_session(proto::OpenSessionRequest {
                task: task.map(str::to_owned),
                ..Default::default()
            })
            .await
            .map(|reply| reply.into_inner().session_id)
    }
}

fn grpc_select() -> proto::ExecuteRequest {
    proto::ExecuteRequest {
        request: Some(proto::execute_request::Request::Sql(proto::SqlRequest {
            sql: "SELECT id FROM shipments".into(),
            params: vec![],
        })),
        budget_ms: None,
        operation_id: None,
    }
}

fn grpc_ping() -> proto::ExecuteRequest {
    proto::ExecuteRequest {
        request: Some(proto::execute_request::Request::Ping(proto::PingRequest {})),
        budget_ms: None,
        operation_id: None,
    }
}

fn session_id(open: &(u16, Value)) -> String {
    assert_eq!(open.0, 201, "{}", open.1);
    open.1["sessionId"].as_str().unwrap().to_owned()
}

impl Fixture {
    async fn ordinary_sql(&self, sql: &str) -> Vec<agent_support::Msg> {
        let mut wire = agent_support::connect_raw(
            self.http.address,
            &self.ca,
            &self.ordinary,
            &[("user", "giggles"), ("database", "giggles")],
        )
        .await;
        let replies = wire.simple(sql).await;
        assert!(
            !replies
                .iter()
                .any(|m| matches!(m, agent_support::Msg::Error { .. }))
        );
        replies
    }
    async fn issue(&self) -> (String, String) {
        let replies = self
            .ordinary_sql("ISSUE CREDENTIAL FOR TASK \"inc-436\"")
            .await;
        let values = replies
            .iter()
            .find_map(|m| match m {
                agent_support::Msg::Row(row) => Some(row),
                _ => None,
            })
            .unwrap();
        (values[0].clone().unwrap(), values[2].clone().unwrap())
    }
    async fn revoke(&self, id: &str) {
        self.ordinary_sql(&format!("REVOKE CREDENTIAL {id}")).await;
    }
    async fn bearer_http(
        &self,
        secret: Option<&str>,
        token: Option<&str>,
        body: Value,
    ) -> (u16, Value) {
        let auth = secret.map(|s| format!("Bearer {s}"));
        let values: Vec<&str> = auth.as_deref().into_iter().collect();
        let path = if token.is_some() {
            "/v1/session/execute"
        } else {
            "/v1/sessions"
        };
        self.http(
            self.agent_http.address,
            None,
            &values,
            "POST",
            path,
            token,
            body,
            false,
        )
        .await
    }
}
fn bearer<T>(secret: &str, token: Option<&str>, value: T) -> tonic::Request<T> {
    let mut request = tonic::Request::new(value);
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {secret}").parse().unwrap());
    if let Some(token) = token {
        request
            .metadata_mut()
            .insert("x-giggles-session", token.parse().unwrap());
    }
    request
}
#[tokio::test]
async fn http_open_select_revoke_then_401() {
    let f = Fixture::start(&[]).await;
    let (id, secret) = f.issue().await;
    let token = session_id(&f.bearer_http(Some(&secret), None, json!({})).await);
    let (status, reply) = f
        .bearer_http(
            Some(&secret),
            Some(&token),
            json!({"sql":{"sql":"SELECT id FROM shipments"}}),
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(reply["code"], "ok", "{reply}");
    assert_eq!(reply["sql"]["rows"].as_array().unwrap().len(), 1);
    f.revoke(&id).await;
    assert_eq!(
        f.bearer_http(Some(&secret), Some(&token), json!({"ping":{}}))
            .await
            .0,
        401
    );
    assert_eq!(f.bearer_http(Some(&secret), None, json!({})).await.0, 401);
    f.stop().await;
}
#[tokio::test]
async fn grpc_open_select_revoke_then_unauthenticated() {
    let f = Fixture::start(&[]).await;
    let (id, secret) = f.issue().await;
    let mut client = f.grpc(f.agent_grpc.address, None).await;
    let token = client
        .open_session(bearer(&secret, None, proto::OpenSessionRequest::default()))
        .await
        .unwrap()
        .into_inner()
        .session_id;
    let reply = client
        .execute(bearer(&secret, Some(&token), grpc_select()))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(reply.code, "ok");
    assert_eq!(reply.sql.unwrap().rows.len(), 1);
    f.revoke(&id).await;
    assert_eq!(
        client
            .execute(bearer(&secret, Some(&token), grpc_ping()))
            .await
            .unwrap_err()
            .code(),
        Code::Unauthenticated
    );
    assert_eq!(
        client
            .open_session(bearer(&secret, None, proto::OpenSessionRequest::default()))
            .await
            .unwrap_err()
            .code(),
        Code::Unauthenticated
    );
    f.stop().await;
}
#[tokio::test]
async fn wrong_secret_is_401() {
    let f = Fixture::start(&[]).await;
    let (_, mut secret) = f.issue().await;
    let last = secret.pop().unwrap();
    secret.push(if last == '0' { '1' } else { '0' });
    assert_eq!(f.bearer_http(Some(&secret), None, json!({})).await.0, 401);
    let mut client = f.grpc(f.agent_grpc.address, None).await;
    assert_eq!(
        client
            .open_session(bearer(&secret, None, proto::OpenSessionRequest::default()))
            .await
            .unwrap_err()
            .code(),
        Code::Unauthenticated
    );
    f.stop().await;
}
#[tokio::test]
async fn missing_header_is_401() {
    let f = Fixture::start(&[]).await;
    assert_eq!(f.bearer_http(None, None, json!({})).await.0, 401);
    f.stop().await;
}
#[tokio::test]
async fn lease_from_credential_a_refused_with_credential_b() {
    let f = Fixture::start(&[]).await;
    let (_, a) = f.issue().await;
    let (_, b) = f.issue().await;
    let token = session_id(&f.bearer_http(Some(&a), None, json!({})).await);
    assert!(
        [401, 404].contains(
            &f.bearer_http(Some(&b), Some(&token), json!({"ping":{}}))
                .await
                .0
        )
    );
    let mut client = f.grpc(f.agent_grpc.address, None).await;
    let token = client
        .open_session(bearer(&a, None, proto::OpenSessionRequest::default()))
        .await
        .unwrap()
        .into_inner()
        .session_id;
    assert!(
        [Code::Unauthenticated, Code::NotFound].contains(
            &client
                .execute(bearer(&b, Some(&token), grpc_ping()))
                .await
                .unwrap_err()
                .code()
        )
    );
    f.stop().await;
}
#[tokio::test]
async fn task_field_mismatch_refused() {
    let f = Fixture::start(&[]).await;
    let (_, secret) = f.issue().await;
    assert_eq!(
        f.bearer_http(Some(&secret), None, json!({"task":"inc-437"}))
            .await
            .0,
        401
    );
    let mut client = f.grpc(f.agent_grpc.address, None).await;
    let request = proto::OpenSessionRequest {
        task: Some("inc-437".into()),
        ..Default::default()
    };
    assert_eq!(
        client
            .open_session(bearer(&secret, None, request))
            .await
            .unwrap_err()
            .code(),
        Code::Unauthenticated
    );
    f.stop().await;
}
#[tokio::test]
async fn cert_identity_still_works() {
    let f = Fixture::start(&["inc-436"]).await;
    session_id(
        &f.open_http(f.agent_http.address, &f.agent, json!({}), false)
            .await,
    );
    f.open_grpc(f.agent_grpc.address, &f.agent, None)
        .await
        .unwrap();
    f.stop().await;
}
#[tokio::test]
async fn malformed_and_duplicate_authorization_refused() {
    let f = Fixture::start(&[]).await;
    let (_, secret) = f.issue().await;
    let good = format!("Bearer {secret}");
    for headers in [
        vec![good.as_str(), good.as_str()],
        vec!["Basic secret"],
        vec!["Bearer  secret"],
        vec!["Bearer"],
        vec!["Bearer "],
        vec![&"a".repeat(257)],
    ] {
        assert_eq!(
            f.http(
                f.agent_http.address,
                None,
                &headers,
                "POST",
                "/v1/sessions",
                None,
                json!({}),
                false
            )
            .await
            .0,
            401
        );
    }
    let unknown = certificate(Some(&f.ca), false);
    assert_eq!(
        f.http(
            f.agent_http.address,
            Some(&unknown),
            &[&format!("bEaReR {secret}")],
            "POST",
            "/v1/sessions",
            None,
            json!({}),
            false
        )
        .await
        .0,
        201
    );
    f.stop().await;
}
