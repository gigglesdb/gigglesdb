//! GD-0061 R3-313: the native API, typed reads and the SQL reply through the
//! release protocol, over the real agent and ordinary HTTP and gRPC
//! listeners. `Select` is not yet in `task::admit::ENABLED`, so on an agent
//! listener every read is refused by admission today: these tests check that
//! such a refusal is logged once (with its entry point and text), charges
//! nothing and puts no row value on the wire. The released paths themselves
//! (masking, debits, one page per read) are proven by lib tests of the same
//! names in `src/api/dispatch/native_release_tests.rs`, which admit `Select`
//! through the cfg(test) route. `non_task_native_unchanged` compares an
//! ordinary session's replies byte-for-byte with bytes recorded on BASE
//! ac5b6466, before this item.
#![cfg(feature = "engine-rocksdb")]
mod agent_support;

use agent_support::{TestCert, admin, binding, certificate, now_ms, timestamptz, DAY_MS};
use bytes::Bytes;
use giggles_core::{
    relational::Value as CoreValue,
    roles::TablePrivilege,
    task::{
        StatementClass,
        definition::{
            DisclosureCaps, LoadLimits, MutationCaps, NewTask, PredicateText, WallClockMs,
        },
        readlog::{ENTRY_GRPC, ENTRY_HTTP, class_code},
    },
};
use giggles_server::{
    Binding, Role, Running, Service,
    admission::ServiceLimits,
    api::{
        ApiLimits, ApiRuntime,
        json as wire_json,
        net::{serve_grpc_bound, serve_http_bound},
        proto,
    },
    clock::Clock,
    tls,
};
use http_body_util::{BodyExt, Full};
use hyper_util::rt::{TokioExecutor, TokioIo};
use prost::Message;
use rustls::pki_types::{PrivatePkcs8KeyDer, ServerName};
use serde_json::{Value, json};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::watch,
};
use tokio_rustls::TlsConnector;
use tonic::transport::{Certificate as CaCertificate, Channel, ClientTlsConfig, Identity};

const SQL: &str = "SELECT id, status FROM shipments ORDER BY id";

/// Ordinary and agent HTTP and gRPC listeners over one service. `shipments`
/// holds three rows of merchant 42 (status `failed`) and one of merchant 7
/// (status `secret`); task `inc-436` sees merchant 42, reads `id` and
/// `merchant_id` and gets `status` masked.
struct Fixture {
    admin: giggles_core::Admin,
    ca: TestCert,
    ordinary_api: Arc<ApiRuntime>,
    agent_api: Arc<ApiRuntime>,
    http: Running,
    grpc: Running,
    agent_http: Running,
    agent_grpc: Running,
    ordinary: TestCert,
    agent: TestCert,
}

impl Fixture {
    async fn start() -> Self {
        let admin = admin();
        admin
            .sql_session()
            .execute(
                "INSERT INTO shipments VALUES (1, 42, 'failed'), (2, 42, 'failed'), \
                 (3, 42, 'failed'), (4, 7, 'secret')",
                &[],
                None,
            )
            .unwrap();
        admin
            .roles()
            .grant_table("shipments", "incident_controller", &[TablePrivilege::Insert])
            .unwrap();
        let issuer = admin.roles().get("incident_controller").unwrap().unwrap();
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
                    readable: vec!["id".into(), "merchant_id".into()],
                    masked: vec![("status".into(), Some(CoreValue::Text("***".into())))],
                    untrusted_text: Vec::new(),
                    allowed_write: None,
                    mutation_caps: MutationCaps::default(),
                    disclosure_caps: DisclosureCaps {
                        rows_read: 100,
                        rows_returned: 100,
                        bytes_returned: 65_536,
                    },
                    limits: LoadLimits::default(),
                    approval_threshold: 0,
                    expires: timestamptz(now_ms() + DAY_MS),
                },
                WallClockMs(now_ms()),
            )
            .unwrap();
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
        let agent_api = ApiRuntime::new_agent(service.clone(), ApiLimits { sessions: 4 }).unwrap();
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

    fn task(&self) -> giggles_core::task::definition::TaskObjectId {
        self.admin.tasks().get("inc-436").unwrap().unwrap().id
    }

    /// One HTTP call; the status and the raw response body.
    async fn http(
        &self,
        address: SocketAddr,
        cert: &TestCert,
        path: &str,
        token: Option<&str>,
        body: &Value,
        h2: bool,
    ) -> (u16, Vec<u8>) {
        let body = serde_json::to_vec(body).unwrap();
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
                .method("POST")
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
            (status, bytes.to_vec())
        })
        .await
        .expect("HTTP call finished")
    }

    async fn open_http(&self, address: SocketAddr, cert: &TestCert) -> String {
        let (status, body) = self.http(address, cert, "/v1/sessions", None, &json!({}), false).await;
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(status, 201, "{body}");
        body["sessionId"].as_str().unwrap().to_owned()
    }

    /// One execute over HTTP; the raw JSON reply body.
    async fn exec_http(
        &self,
        address: SocketAddr,
        cert: &TestCert,
        token: &str,
        request: &Value,
        h2: bool,
    ) -> Vec<u8> {
        let (status, body) = self
            .http(address, cert, "/v1/session/execute", Some(token), request, h2)
            .await;
        assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
        body
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
        client: &mut proto::giggles_client::GigglesClient<Channel>,
    ) -> String {
        client
            .open_session(proto::OpenSessionRequest::default())
            .await
            .unwrap()
            .into_inner()
            .session_id
    }
}

/// One execute over gRPC, from the JSON form of the request.
async fn exec_grpc(
    client: &mut proto::giggles_client::GigglesClient<Channel>,
    token: &str,
    request: &Value,
) -> proto::ExecuteReply {
    let request = wire_json::decode_execute(&serde_json::to_vec(request).unwrap()).unwrap();
    let mut request = tonic::Request::new(request);
    request
        .metadata_mut()
        .insert("x-giggles-session", token.parse().unwrap());
    client.execute(request).await.unwrap().into_inner()
}

fn column(name: &str) -> Value {
    json!({"column": {"name": name}})
}

fn typed_read() -> Value {
    json!({"typedRead": {"table": "shipments",
        "projection": [{"expression": column("id"), "alias": "id"},
            {"expression": column("status"), "alias": "status"}],
        "orderBy": [{"expression": column("id"), "descending": false}], "limit": "100"}})
}

fn native_read_request() -> Value {
    json!({"read": {"table": "shipments", "order": [{"field": "id", "descending": false}],
        "limit": {"literal": {"intValue": "100"}}, "projection": ["id", "status"]}})
}

fn native_read() -> Value {
    json!({"operation": {"request": native_read_request(), "bindings": []}})
}

fn native_insert_returning(id: i64) -> Value {
    json!({"operation": {"request": {"returning": {
        "request": {"insert": {"table": "shipments", "values": [
            {"column": "id", "value": {"literal": {"intValue": id.to_string()}}},
            {"column": "merchant_id", "value": {"literal": {"intValue": "42"}}},
            {"column": "status", "value": {"literal": {"textValue": "new"}}}]}},
        "projection": [{"expression": {"field": "id"}, "alias": "id"},
            {"expression": {"field": "status"}, "alias": "status"}]}}, "bindings": []}})
}

fn sql(text: &str) -> Value {
    json!({"sql": {"sql": text}})
}

fn code(body: &[u8]) -> String {
    let body: Value = serde_json::from_slice(body).unwrap();
    body["code"].as_str().unwrap_or_default().to_owned()
}

/// No row value of `shipments` (masked or out of scope) is in `bytes`.
fn no_row_values(bytes: &[u8]) {
    let text = String::from_utf8_lossy(bytes);
    for value in ["failed", "secret", "***"] {
        assert!(!text.contains(value), "{value} on the wire: {text}");
    }
}

/// Nothing was charged to the task.
fn nothing_charged(f: &Fixture) {
    let disclosure = f.admin.task_disclosure(f.task()).unwrap();
    assert_eq!(
        (disclosure.rows_returned, disclosure.bytes_returned, disclosure.debits.len()),
        (0, 0, 0),
        "{disclosure:?}"
    );
}

/// A typed read on the agent listener is refused by admission before it
/// runs: one read-log record (HTTP entry, Select, its text), no page,
/// nothing charged, no row value in the reply.
#[tokio::test]
async fn typed_read_released_once() {
    let f = Fixture::start().await;
    let address = f.agent_http.address;
    let token = f.open_http(address, &f.agent).await;
    let body = f.exec_http(address, &f.agent, &token, &typed_read(), false).await;
    assert_eq!(code(&body), "0A000", "{}", String::from_utf8_lossy(&body));
    no_row_values(&body);
    let records = f.admin.task_read_log(f.task()).unwrap();
    assert_eq!(records.len(), 1, "{records:?}");
    let record = &records[0];
    assert_eq!(record.entry, ENTRY_HTTP);
    assert_eq!(record.class, class_code(StatementClass::Select));
    assert_eq!((record.pages, record.released_rows), (0, 0));
    assert_eq!(record.sqlstate_text(), "0A000");
    assert!(record.text.contains("shipments"), "{}", record.text);
    nothing_charged(&f);
    f.stop().await;
}

/// A native read and a native INSERT ... RETURNING on the agent listener:
/// both refused by admission, each logged once with its class, nothing
/// charged, no row inserted and no row value in either reply.
#[tokio::test]
async fn native_returning_masked_and_charged() {
    let f = Fixture::start().await;
    let address = f.agent_http.address;
    let token = f.open_http(address, &f.agent).await;
    for request in [native_read(), native_insert_returning(50)] {
        let body = f.exec_http(address, &f.agent, &token, &request, false).await;
        assert_eq!(code(&body), "0A000", "{}", String::from_utf8_lossy(&body));
        no_row_values(&body);
    }
    let records = f.admin.task_read_log(f.task()).unwrap();
    assert_eq!(
        records.iter().map(|r| (r.class, r.entry, r.pages)).collect::<Vec<_>>(),
        [
            (class_code(StatementClass::Select), ENTRY_HTTP, 0),
            (class_code(StatementClass::Insert), ENTRY_HTTP, 0),
        ],
        "{records:?}"
    );
    assert!(records[1].text.contains("Returning"), "{}", records[1].text);
    nothing_charged(&f);
    let count = f
        .admin
        .sql_session()
        .execute("SELECT count(*) FROM shipments", &[], None)
        .unwrap();
    assert_eq!(count.rows[0].values[0], CoreValue::Int(4));
    f.stop().await;
}

/// The same refused request over HTTP/1.1, HTTP/2 and gRPC: the same reply
/// code, one record per call with its transport's entry point, and the same
/// (zero) charge.
#[tokio::test]
async fn http_and_grpc_same_charge() {
    let f = Fixture::start().await;
    let address = f.agent_http.address;
    let token = f.open_http(address, &f.agent).await;
    for h2 in [false, true] {
        let body = f.exec_http(address, &f.agent, &token, &native_read(), h2).await;
        assert_eq!(code(&body), "0A000");
        no_row_values(&body);
    }
    let mut client = f.grpc(f.agent_grpc.address, &f.agent).await;
    let token = f.open_grpc(&mut client).await;
    let reply = exec_grpc(&mut client, &token, &native_read()).await;
    assert_eq!(reply.code, "0A000");
    assert!(reply.native_result.is_none(), "{reply:?}");
    let records = f.admin.task_read_log(f.task()).unwrap();
    assert_eq!(
        records.iter().map(|r| r.entry).collect::<Vec<_>>(),
        [ENTRY_HTTP, ENTRY_HTTP, ENTRY_GRPC],
        "{records:?}"
    );
    assert!(records.iter().all(|r| r.text == records[0].text && r.charged_rows_returned == 0));
    nothing_charged(&f);
    f.stop().await;
}

/// SQL over the agent API (the framed `Request::Sql` reply): a SELECT is
/// refused by admission and logged once with its text and class, over HTTP
/// and gRPC; a statement that does not parse is refused and logged once too;
/// nothing is charged.
#[tokio::test]
async fn framed_sql_reply_released() {
    let f = Fixture::start().await;
    let address = f.agent_http.address;
    let token = f.open_http(address, &f.agent).await;
    let body = f.exec_http(address, &f.agent, &token, &sql(SQL), false).await;
    assert_eq!(code(&body), "0A000");
    no_row_values(&body);
    let body = f.exec_http(address, &f.agent, &token, &sql("SELEC id"), false).await;
    // Admission (by its leading word) answers before the parser does.
    assert_eq!(code(&body), "0A000", "{}", String::from_utf8_lossy(&body));
    let mut client = f.grpc(f.agent_grpc.address, &f.agent).await;
    let token = f.open_grpc(&mut client).await;
    let reply = exec_grpc(&mut client, &token, &sql(SQL)).await;
    assert_eq!(reply.code, "0A000");
    assert!(reply.sql.is_none_or(|sql| sql.rows.is_empty()));
    let records = f.admin.task_read_log(f.task()).unwrap();
    assert_eq!(
        records
            .iter()
            .map(|r| (r.text.as_str(), r.entry, r.sqlstate_text()))
            .collect::<Vec<_>>(),
        [
            (SQL, ENTRY_HTTP, "0A000".to_owned()),
            ("SELEC id", ENTRY_HTTP, "0A000".to_owned()),
            (SQL, ENTRY_GRPC, "0A000".to_owned()),
        ],
        "{records:?}"
    );
    assert_eq!(
        (records[0].class, records[2].class, records[0].pages),
        (class_code(StatementClass::Select), class_code(StatementClass::Select), 0),
        "{records:?}"
    );
    nothing_charged(&f);
    f.stop().await;
}

/// The ordinary listeners' replies are byte-for-byte what BASE ac5b6466
/// answered: HTTP/1.1 JSON bodies and gRPC protobuf replies for typed
/// reads, native reads and pages, SQL, a native transaction block, and a
/// native INSERT ... RETURNING inside a rolled-back transaction.
#[tokio::test]
async fn non_task_native_unchanged() {
    let f = Fixture::start().await;
    let requests = [
        typed_read(),
        native_read(),
        json!({"operation": {"request": native_read_request(), "bindings": [], "page": {"after": []}}}),
        sql(SQL),
        json!({"transaction": {"transaction": {"readOnly": true,
            "steps": [{"letBinding": {"name": "saved", "request": native_read_request()}}],
            "emit": {"values": [{"rows": "saved"}, {"scalar": {"local": {"count": "saved"}}}]}},
            "bindings": []}}),
        json!({"begin": {}}),
        native_insert_returning(50),
        json!({"rollback": {}}),
        json!({"searchPath": {}}),
    ];
    let mut answers = Vec::new();
    let token = f.open_http(f.http.address, &f.ordinary).await;
    for request in &requests {
        let body = f.exec_http(f.http.address, &f.ordinary, &token, request, false).await;
        answers.push(hex(&body));
    }
    let mut client = f.grpc(f.grpc.address, &f.ordinary).await;
    let token = f.open_grpc(&mut client).await;
    for request in &requests {
        answers.push(hex(&exec_grpc(&mut client, &token, request).await.encode_to_vec()));
    }
    if std::env::var_os("R3_313_RECORD").is_some() {
        println!("RECORDED = {answers:#?}");
    }
    assert_eq!(answers.len(), RECORDED.len());
    for (i, (answer, recorded)) in answers.iter().zip(RECORDED).enumerate() {
        assert_eq!(answer, recorded, "exchange {i}; all answers: {answers:#?}");
    }
    f.stop().await;
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Recorded on BASE ac5b6466 (before R3-313).
const RECORDED: &[&str] = &[
    "7b2276657273696f6e223a312c22636f6465223a226f6b222c226f7065726174696f6e4f7574636f6d65223a7b22636f6c756d6e73223a5b226964222c22737461747573225d2c22726f7773223a5b7b2276616c756573223a5b7b22696e7456616c7565223a2231227d2c7b227465787456616c7565223a226661696c6564227d5d2c2276657273696f6e223a2231227d2c7b2276616c756573223a5b7b22696e7456616c7565223a2232227d2c7b227465787456616c7565223a226661696c6564227d5d2c2276657273696f6e223a2232227d2c7b2276616c756573223a5b7b22696e7456616c7565223a2233227d2c7b227465787456616c7565223a226661696c6564227d5d2c2276657273696f6e223a2233227d2c7b2276616c756573223a5b7b22696e7456616c7565223a2234227d2c7b227465787456616c7565223a22736563726574227d5d2c2276657273696f6e223a2234227d5d2c226d617463686564223a2234222c226166666563746564223a2230222c226368616e676564223a2230222c22636f6d706c657465223a747275657d2c227472616e73616374696f6e5374617465223a225452414e53414354494f4e5f53544154455f49444c45227d",
    "7b2276657273696f6e223a312c22636f6465223a226f6b222c226f7065726174696f6e4f7574636f6d65223a7b22636f6c756d6e73223a5b226964222c22737461747573225d2c22726f7773223a5b7b2276616c756573223a5b7b22696e7456616c7565223a2231227d2c7b227465787456616c7565223a226661696c6564227d5d2c2276657273696f6e223a2231227d2c7b2276616c756573223a5b7b22696e7456616c7565223a2232227d2c7b227465787456616c7565223a226661696c6564227d5d2c2276657273696f6e223a2232227d2c7b2276616c756573223a5b7b22696e7456616c7565223a2233227d2c7b227465787456616c7565223a226661696c6564227d5d2c2276657273696f6e223a2233227d2c7b2276616c756573223a5b7b22696e7456616c7565223a2234227d2c7b227465787456616c7565223a22736563726574227d5d2c2276657273696f6e223a2234227d5d2c226d617463686564223a2234222c226166666563746564223a2230222c226368616e676564223a2230222c22636f6d706c657465223a747275657d2c227472616e73616374696f6e5374617465223a225452414e53414354494f4e5f53544154455f49444c45227d",
    "7b2276657273696f6e223a312c22636f6465223a226f6b222c226f7065726174696f6e4f7574636f6d65223a7b22636f6c756d6e73223a5b226964222c22737461747573225d2c22726f7773223a5b7b2276616c756573223a5b7b22696e7456616c7565223a2231227d2c7b227465787456616c7565223a226661696c6564227d5d2c2276657273696f6e223a2231227d2c7b2276616c756573223a5b7b22696e7456616c7565223a2232227d2c7b227465787456616c7565223a226661696c6564227d5d2c2276657273696f6e223a2232227d2c7b2276616c756573223a5b7b22696e7456616c7565223a2233227d2c7b227465787456616c7565223a226661696c6564227d5d2c2276657273696f6e223a2233227d2c7b2276616c756573223a5b7b22696e7456616c7565223a2234227d2c7b227465787456616c7565223a22736563726574227d5d2c2276657273696f6e223a2234227d5d2c226d617463686564223a2234222c226166666563746564223a2230222c226368616e676564223a2230222c22636f6d706c657465223a747275657d2c227472616e73616374696f6e5374617465223a225452414e53414354494f4e5f53544154455f49444c45227d",
    "7b2276657273696f6e223a312c22636f6465223a223432353031222c227472616e73616374696f6e5374617465223a225452414e53414354494f4e5f53544154455f49444c45227d",
    "7b2276657273696f6e223a312c22636f6465223a226f6b222c227472616e73616374696f6e4f7574636f6d65223a7b2273756d6d6172696573223a5b7b226974656d496e646578223a2230222c226d617463686564223a2234222c226166666563746564223a2230222c226368616e676564223a2230222c22636f6d706c657465223a747275657d5d2c22656d6974746564223a5b7b22726f7773223a7b22636f6c756d6e73223a5b7b226964223a2232222c226e616d65223a226964222c226461746154797065223a224e41544956455f444154415f545950455f4249475f494e54222c226e6f744e756c6c223a747275657d2c7b226964223a2234222c226e616d65223a22737461747573222c226461746154797065223a224e41544956455f444154415f545950455f54455854222c226e6f744e756c6c223a66616c73657d5d2c22726f7773223a5b7b2276616c756573223a5b7b22696e7456616c7565223a2231227d2c7b227465787456616c7565223a226661696c6564227d5d2c2276657273696f6e223a2231227d2c7b2276616c756573223a5b7b22696e7456616c7565223a2232227d2c7b227465787456616c7565223a226661696c6564227d5d2c2276657273696f6e223a2232227d2c7b2276616c756573223a5b7b22696e7456616c7565223a2233227d2c7b227465787456616c7565223a226661696c6564227d5d2c2276657273696f6e223a2233227d2c7b2276616c756573223a5b7b22696e7456616c7565223a2234227d2c7b227465787456616c7565223a22736563726574227d5d2c2276657273696f6e223a2234227d5d2c22636f6d706c657465223a747275657d7d2c7b227363616c6172223a7b22696e7456616c7565223a2234227d7d5d7d2c227472616e73616374696f6e5374617465223a225452414e53414354494f4e5f53544154455f49444c45227d",
    "7b2276657273696f6e223a312c22636f6465223a226f6b222c227472616e73616374696f6e5374617465223a225452414e53414354494f4e5f53544154455f494e5f5452414e53414354494f4e227d",
    "7b2276657273696f6e223a312c22636f6465223a226f6b222c226f7065726174696f6e4f7574636f6d65223a7b22636f6c756d6e73223a5b226964222c22737461747573225d2c22726f7773223a5b7b2276616c756573223a5b7b22696e7456616c7565223a223530227d2c7b227465787456616c7565223a226e6577227d5d2c2276657273696f6e223a2231303030303032227d5d2c226d617463686564223a2231222c226166666563746564223a2231222c226368616e676564223a2231222c22636f6d706c657465223a747275657d2c227472616e73616374696f6e5374617465223a225452414e53414354494f4e5f53544154455f494e5f5452414e53414354494f4e227d",
    "7b2276657273696f6e223a312c22636f6465223a226f6b222c227472616e73616374696f6e5374617465223a225452414e53414354494f4e5f53544154455f49444c45227d",
    "7b2276657273696f6e223a312c22636f6465223a226f6b222c2273716c223a7b22636f6c756d6e73223a5b227365617263685f70617468225d2c22726f7773223a5b7b2276616c756573223a5b7b227465787456616c7565223a225c2224757365725c222c207075626c6963227d5d2c2276657273696f6e223a2230227d5d2c226166666563746564223a2230227d2c227472616e73616374696f6e5374617465223a225452414e53414354494f4e5f53544154455f49444c45227d",
    "080112026f6b380142580a0269640a0673746174757312100a0218020a0822066661696c6564100112100a0218040a0822066661696c6564100212100a0218060a0822066661696c6564100312100a0218080a082206736563726574100418043001",
    "080112026f6b380142580a0269640a0673746174757312100a0218020a0822066661696c6564100112100a0218040a0822066661696c6564100212100a0218060a0822066661696c6564100312100a0218080a082206736563726574100418043001",
    "080112026f6b380142580a0269640a0673746174757312100a0218020a0822066661696c6564100112100a0218040a0822066661696c6564100212100a0218060a0822066661696c6564100312100a0218080a082206736563726574100418043001",
    "0801120534323530313801",
    "080112026f6b38014a740a041004280112661a640a0a080212026964180420010a0c08041206737461747573180512100a0218020a0822066661696c6564100112100a0218040a0822066661696c6564100212100a0218060a0822066661696c6564100312100a0218080a0822067365637265741004180112040a021808",
    "080112026f6b3802",
    "080112026f6b380242250a0269640a06737461747573120f0a0218640a0522036e65771083897a1801200128013001",
    "080112026f6b3801",
    "080112026f6b32220a0b7365617263685f7061746812130a11220f222475736572222c207075626c69633801",
];
