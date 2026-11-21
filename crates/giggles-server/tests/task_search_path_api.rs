//! Task search-path mutation refusals agree across SQL and native APIs.
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
use tonic::transport::{Certificate as CaCertificate, Channel, ClientTlsConfig, Identity};

/// Ordinary and agent HTTP and gRPC listeners over one service and one CA.
struct Fixture {
    ca: TestCert,
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
        let agent_api = ApiRuntime::new_agent(service.clone(), ApiLimits { sessions: 4 }).unwrap();
        assert_eq!(ordinary_api.kind(), ListenerKind::Ordinary);
        assert_eq!(agent_api.kind(), ListenerKind::Agent);
        let (_, gate) = watch::channel(true);
        let bind = || async { TcpListener::bind("127.0.0.1:0").await.unwrap() };
        let http = serve_http_bound(
            bind().await,
            ordinary_api.clone(),
            config.clone(),
            8,
            gate.clone(),
        )
        .await
        .unwrap();
        let grpc = serve_grpc_bound(
            bind().await,
            ordinary_api.clone(),
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
            let reply = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
            (status, reply)
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
        self.http(address, cert, "POST", "/v1/sessions", None, body, h2)
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
                    .identity(Identity::from_pem(
                        cert.cert.pem(),
                        cert.key.serialize_pem(),
                    )),
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

const SQL: [&str; 6] = [
    "SET search_path = s",
    "SET search_path TO DEFAULT",
    "RESET search_path",
    "SET LOCAL search_path = s",
    "SELECT set_config('search_path','s',false)",
    "SELECT set_config('search_path','s',true)",
];

async fn pg_refusals() -> Vec<(String, String)> {
    let (server, _agent, _ordinary, mut wire) = agent_support::agent_wire().await;
    let mut errors = vec![];
    for sql in SQL {
        let error = agent_support::error_of(&wire.simple(sql).await).expect(sql);
        assert_eq!(error.0, "0A000", "{sql}");
        assert_eq!(
            error.1,
            giggles_core::task::TaskError::NotAllowedInTaskSession(
                giggles_core::task::StatementClass::Set
            )
            .message(),
            "{sql}"
        );
        errors.push(error);
    }
    drop(wire);
    server.agent.shutdown().await.unwrap();
    errors
}

fn bodies(errors: &[(String, String)]) -> Vec<(Value, (String, String))> {
    let mut bodies = vec![];
    for schemas in [
        json!(["s", "public"]),
        json!(["public"]),
        json!(["$user", "public"]),
    ] {
        bodies.push((
            json!({"searchPath": {"set": true, "schemas": schemas}}),
            errors[0].clone(),
        ));
    }
    for (sql, error) in SQL.into_iter().zip(errors) {
        bodies.push((json!({"sql": {"sql": sql}}), error.clone()));
    }
    bodies
}

fn refusal(reply: &Value, expected: &(String, String)) {
    assert_eq!(reply["code"], expected.0, "{reply}");
    assert_eq!(reply["taskError"], "not_allowed", "{reply}");
    assert_eq!(reply["message"], expected.1, "{reply}");
}

#[tokio::test]
async fn pgwire_search_path_twins() {
    pg_refusals().await;
}

#[tokio::test]
async fn http_search_path_refusals() {
    let errors = pg_refusals().await;
    let f = Fixture::start(&["inc-436"]).await;
    for h2 in [false, true] {
        let address = f.agent_http.address;
        let token = session_id(&f.open_http(address, &f.agent, json!({}), h2).await);
        let execute = |body| {
            f.http(
                address,
                &f.agent,
                "POST",
                "/v1/session/execute",
                Some(&token),
                body,
                h2,
            )
        };
        let original = execute(json!({"searchPath": {}})).await;
        assert_eq!(original.0, 200);
        assert_eq!(original.1["code"], "ok");
        for (body, expected) in bodies(&errors) {
            let (status, reply) = execute(body.clone()).await;
            assert_eq!(status, 200, "{body}: {reply}");
            refusal(&reply, &expected);
            assert_eq!(execute(json!({"searchPath": {}})).await, original);
            assert_eq!(execute(json!({"ping": {}})).await.1["code"], "ok");
        }
        f.http(
            address,
            &f.agent,
            "DELETE",
            "/v1/session",
            Some(&token),
            json!({}),
            h2,
        )
        .await;
        let token = session_id(
            &f.open_http(f.http.address, &f.ordinary, json!({}), h2)
                .await,
        );
        let execute = |body| {
            f.http(
                f.http.address,
                &f.ordinary,
                "POST",
                "/v1/session/execute",
                Some(&token),
                body,
                h2,
            )
        };
        assert_eq!(
            execute(json!({"searchPath": {"set": true, "schemas": ["s", "public"]}}))
                .await
                .1["code"],
            "ok"
        );
        let shown = execute(json!({"searchPath": {}})).await.1;
        assert_eq!(shown["code"], "ok");
        assert!(shown.to_string().contains("s, public"), "{shown}");
    }
    f.stop().await;
}

#[tokio::test]
async fn grpc_search_path_refusals() {
    let errors = pg_refusals().await;
    let f = Fixture::start(&["inc-436"]).await;
    let token = f
        .open_grpc(f.agent_grpc.address, &f.agent, None)
        .await
        .unwrap();
    let mut client = f.grpc(f.agent_grpc.address, &f.agent).await;
    let decode = |body: Value| {
        giggles_server::api::json::decode_execute(&serde_json::to_vec(&body).unwrap()).unwrap()
    };
    let show = decode(json!({"searchPath": {}}));
    let original = client
        .execute(rpc(&token, show.clone()))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(original.code, "ok");
    for (body, expected) in bodies(&errors) {
        let reply = client
            .execute(rpc(&token, decode(body)))
            .await
            .unwrap()
            .into_inner();
        let json = giggles_server::api::json::encode_reply(&reply).unwrap();
        refusal(&serde_json::from_slice(&json).unwrap(), &expected);
        assert_eq!(
            client
                .execute(rpc(&token, show.clone()))
                .await
                .unwrap()
                .into_inner(),
            original
        );
        assert_eq!(
            client
                .execute(rpc(&token, decode(json!({"ping": {}}))))
                .await
                .unwrap()
                .into_inner()
                .code,
            "ok"
        );
    }
    let token = f
        .open_grpc(f.grpc.address, &f.ordinary, None)
        .await
        .unwrap();
    let mut client = f.grpc(f.grpc.address, &f.ordinary).await;
    assert_eq!(
        client
            .execute(rpc(
                &token,
                decode(json!({"searchPath": {"set": true, "schemas": ["s", "public"]}}))
            ))
            .await
            .unwrap()
            .into_inner()
            .code,
        "ok"
    );
    let reply = client
        .execute(rpc(&token, show))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(reply.code, "ok");
    assert!(format!("{reply:?}").contains("s, public"), "{reply:?}");
    f.stop().await;
}

fn session_id(open: &(u16, Value)) -> String {
    assert_eq!(open.0, 201, "{}", open.1);
    open.1["sessionId"].as_str().unwrap().to_owned()
}

#[tokio::test]
async fn settings_recognition_twins() {
    let cases = [
        "SET SESSION search_path TO s",
        "SET \"SEARCH_PATH\" TO s",
        "SET search_path FROM CURRENT",
        "SET /*comment*/ search_path TO s",
        "/*comment*/ SET search_path TO s",
        "SET search_path TO s; SELECT 1",
        "SELECT set_config('SEARCH_PATH','s',false)",
        "SELECT pg_catalog.set_config('search_path','s',false)",
        "SELECT set_config('search_path','s',false), 1",
        "SELECT (SELECT set_config('search_path','s',false))",
        "WITH x AS (SELECT set_config('search_path','s',false)) SELECT * FROM x",
        "SELECT CASE WHEN true THEN set_config('search_path','s',false) ELSE '' END",
        "SELECT set_config($1,'s',false)",
        "SELECT /*comment*/ set_config('search_path','s',false)",
        "RESET ALL",
        "SET role TO agent_exec",
        "SET LOCAL application_name TO x",
        "SET SESSION AUTHORIZATION agent_exec",
        "SET giggles.task TO x",
        "SELECT set_config('search_path', $1, false)",
        "SET application_name TO x",
        "SELECT set_config('application_name','x',false)",
    ];
    let (server, _a, _o, mut wire) = agent_support::agent_wire().await;
    let f = Fixture::start(&["inc-436"]).await;
    let address = f.agent_http.address;
    let token = session_id(&f.open_http(address, &f.agent, json!({}), false).await);
    let grpc_token = f
        .open_grpc(f.agent_grpc.address, &f.agent, None)
        .await
        .unwrap();
    let mut client = f.grpc(f.agent_grpc.address, &f.agent).await;
    let decode = |body: Value| {
        giggles_server::api::json::decode_execute(&serde_json::to_vec(&body).unwrap()).unwrap()
    };
    let execute = |body| {
        f.http(
            address,
            &f.agent,
            "POST",
            "/v1/session/execute",
            Some(&token),
            body,
            false,
        )
    };
    let original = execute(json!({"searchPath":{}})).await;
    let grpc_original = client
        .execute(rpc(&grpc_token, decode(json!({"searchPath":{}}))))
        .await
        .unwrap()
        .into_inner();
    let pg_original = wire.simple("SHOW search_path").await;
    let mut failures = vec![];
    for sql in cases {
        let pg = agent_support::error_of(&wire.simple(sql).await);
        let params = if sql == "SELECT set_config('search_path', $1, false)" {
            json!([{"textValue":"s"}])
        } else {
            json!([])
        };
        let body = json!({"sql":{"sql":sql,"params":params}});
        let (status, http) = execute(body.clone()).await;
        assert_eq!(status, 200);
        let grpc = client
            .execute(rpc(&grpc_token, decode(body)))
            .await
            .unwrap()
            .into_inner();
        let grpc: Value =
            serde_json::from_slice(&giggles_server::api::json::encode_reply(&grpc).unwrap())
                .unwrap();
        println!("FORM {sql} PG={pg:?} HTTP={http} GRPC={grpc}");
        for (surface, reply) in [("HTTP", &http), ("GRPC", &grpc)] {
            let matches = match pg.as_ref() {
                Some((code, message))
                    if code == "0A000"
                        && message
                            == giggles_core::task::TaskError::NotAllowedInTaskSession(
                                giggles_core::task::StatementClass::Set,
                            )
                            .message() =>
                {
                    reply["code"] == code.as_str()
                        && reply["message"] == message.as_str()
                        && reply["taskError"] == "not_allowed"
                }
                Some((code, message))
                    if code == "0A000"
                        && [
                            giggles_core::task::StatementClass::Other,
                            giggles_core::task::StatementClass::TransactionControl,
                        ]
                        .iter()
                        .any(|class| {
                            message
                                == giggles_core::task::TaskError::NotAllowedInTaskSession(*class)
                                    .message()
                        }) =>
                {
                    reply["code"] == "0A000"
                        && reply.get("message").is_none()
                        && reply.get("taskError").is_none()
                }
                _ => reply["code"].as_str().is_some_and(|code| code != "ok"),
            };
            if !matches {
                failures.push(format!("{sql} {surface}: {reply}, pgwire {pg:?}"));
            }
        }
        assert_eq!(execute(json!({"searchPath":{}})).await, original, "{sql}");
        assert_eq!(
            client
                .execute(rpc(&grpc_token, decode(json!({"searchPath":{}}))))
                .await
                .unwrap()
                .into_inner(),
            grpc_original,
            "{sql}"
        );
        assert_eq!(wire.simple("SHOW search_path").await, pg_original, "{sql}");
    }
    let (_, begin) = execute(json!({"sql":{"sql":"BEGIN"}})).await;
    assert_eq!(begin["code"], "0A000");
    assert!(begin.get("message").is_none(), "{begin}");
    assert!(begin.get("taskError").is_none(), "{begin}");
    drop(wire);
    server.agent.shutdown().await.unwrap();
    f.stop().await;
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// Syntax errors must reach the split/control detector before GIGGLES refusal.
async fn giggles_detector_order(extended: bool) {
    use giggles_core::task::readlog::OUTCOME_DENIED;

    let (server, _, _, mut wire) = agent_support::agent_wire().await;
    let id = server.admin.tasks().get("inc-436").unwrap().unwrap().id;
    let mut failures = vec![];
    for (sql, expected_code, expected_records) in [
        ("GIGGLES '", "42601", 0),
        ("GIGGLES /*", "42601", 0),
        ("GIGGLES $$", "42601", 0),
        ("GIGGLES something", "0A000", 1),
    ] {
        let before = server.admin.task_read_log(id).unwrap().len();
        let messages = if extended {
            wire.extended(sql, &[], 0).await
        } else {
            wire.simple(sql).await
        };
        let error = agent_support::error_of(&messages).expect(sql);
        let records = server.admin.task_read_log(id).unwrap();
        let added = &records[before..];
        println!(
            "GIGGLES extended={extended} sql={sql:?} code={} new_records={} refused={}",
            error.0,
            added.len(),
            added.iter().filter(|r| r.outcome == OUTCOME_DENIED).count()
        );
        if error.0 != expected_code
            || added.len() != expected_records
            || added.iter().any(|r| r.outcome != OUTCOME_DENIED)
        {
            failures.push(format!(
                "{sql:?}: code={}, new records={}, expected {expected_code}/{expected_records}",
                error.0, added.len()
            ));
        }
    }
    drop(wire);
    server.agent.shutdown().await.unwrap();
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
async fn giggles_detector_order_simple() {
    giggles_detector_order(false).await;
}

#[tokio::test]
async fn giggles_detector_order_extended() {
    giggles_detector_order(true).await;
}
