//! SQL credentials exercised against the shipped server process, with no agent bindings.
#![cfg(feature = "engine-rocksdb")]
mod agent_support;

use agent_support::{TestCert, certificate, no_cert_client, now_ms, scratch, timestamptz};
use bytes::Bytes;
use giggles_core::{Admin, Engine, Limits, roles::NewRole};
use giggles_server::{api::proto, auth::scram::Verifier};
use http_body_util::{BodyExt, Full};
use hyper_util::rt::TokioIo;
use rustls::pki_types::ServerName;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap, net::SocketAddr, path::PathBuf, process::Stdio, sync::Arc,
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::TcpStream,
    process::{Child, Command},
};
use tokio_postgres::{Client, NoTls, SimpleQueryMessage, config::SslMode};
use tokio_postgres_rustls::MakeRustlsConnect;
use tokio_rustls::TlsConnector;
use tonic::{
    Code,
    transport::{Certificate, Channel, ClientTlsConfig},
};

const SELECT: &str = "SELECT id,status FROM shipments";

struct Fixture {
    path: PathBuf,
    ca: TestCert,
    child: Child,
    addresses: BTreeMap<String, SocketAddr>,
    output: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        // kill_on_drop also protects startup panics; explicit kill covers fixture panics.
        let _ = self.child.start_kill();
    }
}

impl Fixture {
    async fn start() -> Self {
        Self::start_with_api_limit(32).await
    }
    async fn start_with_api_limit(sessions: usize) -> Self {
        let path = scratch("credential-lifecycle");
        let ca = certificate(None, false);
        let server = certificate(Some(&ca), true);
        for (name, bytes) in [
            ("ca.pem", ca.cert.pem()),
            ("server.pem", server.cert.pem()),
            ("server.key.TEST-ONLY", server.key.serialize_pem()),
        ] {
            std::fs::write(path.join(name), bytes).unwrap();
        }
        // Bootstrap only the operator identity; all feature setup uses ordinary SQL.
        let admin =
            Admin::create_engine(path.join("db.redb"), Limits::default(), Engine::V2).unwrap();
        admin
            .roles()
            .create(NewRole {
                name: "owner".into(),
                login: true,
                superuser: true,
                password: Some(Verifier::generate("owner-pw")),
                ..NewRole::default()
            })
            .unwrap();
        drop(admin);
        let mut config = json!({
            "database":path.join("db.redb"), "native":"127.0.0.1:0", "postgres":"127.0.0.1:0",
            "postgresql":{"listen":"127.0.0.1:0","tls":"disabled"},
            "certificate":path.join("server.pem"),"private_key":path.join("server.key.TEST-ONLY"),
            "client_ca":path.join("ca.pem"),"bindings":[],"connections":32,
            "skew_seconds":0,"max_pause_seconds":3600,
            "agent":{"postgres":"127.0.0.1:0","http":"127.0.0.1:0","grpc":"127.0.0.1:0"}
        });
        if sessions != 32 {
            config["agent"]["api_limits"] = json!({"sessions":sessions});
        }
        std::fs::write(
            path.join("config.json"),
            serde_json::to_vec_pretty(&config).unwrap(),
        )
        .unwrap();
        let child = Self::spawn(&path, "first");
        let mut f = Self {
            path,
            ca,
            child,
            addresses: BTreeMap::new(),
            output: None,
        };
        f.ready("first").await;
        let owner = f.ordinary("owner").await;
        for sql in [
            "CREATE TABLE shipments (id bigint primary key, merchant_id bigint, status text)",
            "INSERT INTO shipments VALUES (1,42,'pending'),(2,7,'hidden'),(3,42,'queued')",
            "CREATE ROLE incident_controller LOGIN CREATEROLE PASSWORD 'incident_controller-pw'",
            "CREATE ROLE agent_exec LOGIN TASKONLY",
            "GRANT SELECT ON shipments TO incident_controller",
            "GRANT SELECT ON shipments TO agent_exec",
        ] {
            ok(&owner, sql).await;
        }
        let issuer = f.ordinary("incident_controller").await;
        for task in ["inc-436", "inc-437"] {
            ok(&issuer, &format!(
                "CREATE TASK \"{task}\" EXECUTOR agent_exec ON shipments VISIBLE ROWS WHERE (merchant_id = 42) \
                 READABLE (id,status) DISCLOSE AT MOST 1000 ROWS READ, 2097152 BYTES RETURNED EXPIRES '{}'",
                timestamptz(now_ms() + agent_support::DAY_MS)
            )).await;
        }
        f
    }
    fn spawn(path: &std::path::Path, label: &str) -> Child {
        Command::new(env!("CARGO_BIN_EXE_giggles-server"))
            .arg(path.join("config.json"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(std::fs::File::create(path.join(format!("{label}.stderr.log"))).unwrap())
            .kill_on_drop(true)
            .spawn()
            .unwrap()
    }
    async fn ready(&mut self, label: &str) {
        let stdout = self.child.stdout.take().unwrap();
        let mut output = BufReader::new(stdout);
        let mut line = String::new();
        tokio::time::timeout(Duration::from_secs(30), output.read_line(&mut line))
            .await
            .unwrap()
            .unwrap();
        assert!(
            line.contains("preview=single-host"),
            "readiness: {line}; stderr: {}",
            std::fs::read_to_string(self.path.join(format!("{label}.stderr.log"))).unwrap()
        );
        self.addresses = line
            .split_whitespace()
            .filter_map(|field| {
                let (key, value) = field.split_once('=')?;
                Some((key.into(), value.parse().ok()?))
            })
            .collect();
        // Drain stdout continuously, retaining every byte for the secret-leak assertion.
        let file = std::fs::File::create(self.path.join(format!("{label}.stdout.log"))).unwrap();
        std::fs::write(self.path.join(format!("{label}.ready.log")), line).unwrap();
        self.output = Some(tokio::spawn(async move {
            let mut file = tokio::fs::File::from_std(file);
            let _ = tokio::io::copy(&mut output, &mut file).await;
        }));
    }
    async fn ordinary(&self, user: &str) -> Client {
        let address = self.addresses["postgresql"];
        let (client, connection) = tokio_postgres::Config::new()
            .host("127.0.0.1")
            .port(address.port())
            .user(user)
            .password(format!("{user}-pw"))
            .dbname("postgres")
            .ssl_mode(SslMode::Disable)
            .connect(NoTls)
            .await
            .unwrap();
        tokio::spawn(connection);
        client
    }
    async fn issue(&self, expiry: Option<i64>) -> (String, String) {
        let issuer = self.ordinary("incident_controller").await;
        let mut sql = "ISSUE CREDENTIAL FOR TASK \"inc-436\"".to_owned();
        if let Some(expiry) = expiry {
            sql.push_str(&format!(" EXPIRES '{}'", timestamptz(expiry)));
        }
        let rows = ok(&issuer, &sql).await;
        let row = rows
            .iter()
            .find_map(|m| {
                if let SimpleQueryMessage::Row(r) = m {
                    Some(r)
                } else {
                    None
                }
            })
            .unwrap();
        assert_eq!(
            row.columns().iter().map(|c| c.name()).collect::<Vec<_>>(),
            ["credential_id", "task", "secret", "expires_at"]
        );
        (row.get(0).unwrap().into(), row.get(2).unwrap().into())
    }
    async fn pg(&self, secret: &str, task: Option<&str>) -> Result<Client, tokio_postgres::Error> {
        let mut config = tokio_postgres::Config::new();
        config
            .host("localhost")
            .hostaddr(self.addresses["agent_postgres"].ip())
            .port(self.addresses["agent_postgres"].port())
            .user("agent_exec")
            .password(secret)
            .dbname("giggles")
            .ssl_mode(SslMode::Require)
            .connect_timeout(Duration::from_secs(5));
        if let Some(task) = task {
            config.options(format!("-c giggles.task={task}"));
        }
        let (client, connection) = config
            .connect(MakeRustlsConnect::new(no_cert_client(&self.ca)))
            .await?;
        tokio::spawn(connection);
        Ok(client)
    }
    async fn refused(&self, secret: &str, task: Option<&str>) {
        let error = self.pg(secret, task).await.expect_err("refuse agent login");
        assert_eq!(error.code().unwrap().code(), "28P01");
    }
    async fn http(&self, secret: &str, token: Option<&str>, body: Value) -> (u16, Value) {
        lifecycle_http(&self.ca, self.addresses["agent_http"], secret, token, body).await
    }
    async fn grpc(&self) -> proto::giggles_client::GigglesClient<Channel> {
        lifecycle_grpc(&self.ca, self.addresses["agent_grpc"]).await
    }
    async fn revoke(&self, sql: &str) {
        ok(&self.ordinary("incident_controller").await, sql).await;
    }
    async fn stop(&mut self) {
        self.child.kill().await.unwrap();
        if let Some(output) = self.output.take() {
            tokio::time::timeout(Duration::from_secs(5), output)
                .await
                .unwrap()
                .unwrap();
        }
    }
}

async fn ok(client: &Client, sql: &str) -> Vec<SimpleQueryMessage> {
    client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
}
async fn selected(client: &Client) {
    let rows = ok(client, SELECT).await;
    let mut visible: Vec<_> = rows
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some((r.get(0).unwrap(), r.get(1).unwrap())),
            _ => None,
        })
        .collect();
    visible.sort_unstable();
    assert_eq!(visible, [("1", "pending"), ("3", "queued")]);
}
async fn closed(client: &Client) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !client.is_closed() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("idle pg connection cut within five seconds");
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
fn grpc_select() -> proto::ExecuteRequest {
    proto::ExecuteRequest {
        request: Some(proto::execute_request::Request::Sql(proto::SqlRequest {
            sql: SELECT.into(),
            params: vec![],
        })),
        ..Default::default()
    }
}
fn session(reply: (u16, Value)) -> String {
    assert_eq!(reply.0, 201, "{}", reply.1);
    reply.1["sessionId"].as_str().unwrap().into()
}

#[tokio::test]
async fn zero_per_agent_config() {
    let mut f = Fixture::start().await;
    let config: Value =
        serde_json::from_slice(&std::fs::read(f.path.join("config.json")).unwrap()).unwrap();
    assert_eq!(
        config["agent"]
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["grpc", "http", "postgres"]
    );
    let (_, secret) = f.issue(None).await;
    selected(&f.pg(&secret, None).await.unwrap()).await;
    let token = session(f.http(&secret, None, json!({})).await);
    let (status, reply) = f
        .http(&secret, Some(&token), json!({"sql":{"sql":SELECT}}))
        .await;
    assert_eq!(status, 200);
    assert_eq!(reply["code"], "ok");
    let mut ids: Vec<_> = reply["sql"]["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["values"][0]["intValue"].as_str().unwrap())
        .collect();
    ids.sort_unstable();
    assert_eq!(ids, ["1", "3"]);
    let mut grpc = f.grpc().await;
    let token = grpc
        .open_session(bearer(&secret, None, proto::OpenSessionRequest::default()))
        .await
        .unwrap()
        .into_inner()
        .session_id;
    let reply = grpc
        .execute(bearer(&secret, Some(&token), grpc_select()))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(reply.code, "ok");
    let rows = reply.sql.unwrap().rows;
    assert_eq!(rows.len(), 2);
    let mut ids: Vec<_> = rows
        .iter()
        .map(|r| match &r.values[0].value {
            Some(proto::value::Value::IntValue(id)) => *id,
            other => panic!("unexpected id: {other:?}"),
        })
        .collect();
    ids.sort_unstable();
    assert_eq!(ids, [1, 3]);
    f.stop().await;
}

#[tokio::test]
async fn revoke_cuts_all_three_listeners() {
    let mut f = Fixture::start().await;
    let (id, secret) = f.issue(None).await;
    let pg = f.pg(&secret, None).await.unwrap();
    let http = session(f.http(&secret, None, json!({})).await);
    let mut grpc = f.grpc().await;
    let token = grpc
        .open_session(bearer(&secret, None, proto::OpenSessionRequest::default()))
        .await
        .unwrap()
        .into_inner()
        .session_id;
    f.revoke(&format!("REVOKE CREDENTIAL {id}")).await;
    closed(&pg).await;
    assert_eq!(
        f.http(&secret, Some(&http), json!({"ping":{}})).await.0,
        401
    );
    assert_eq!(
        grpc.execute(bearer(&secret, Some(&token), grpc_select()))
            .await
            .unwrap_err()
            .code(),
        Code::Unauthenticated
    );
    f.refused(&secret, None).await;
    assert_eq!(f.http(&secret, None, json!({})).await.0, 401);
    assert_eq!(
        grpc.open_session(bearer(&secret, None, proto::OpenSessionRequest::default()))
            .await
            .unwrap_err()
            .code(),
        Code::Unauthenticated
    );
    f.stop().await;
}

#[tokio::test]
async fn expiry_cuts_and_refuses() {
    let mut f = Fixture::start().await;
    let expiry = now_ms() + 3000;
    let (_, secret) = f.issue(Some(expiry)).await;
    let pg = f.pg(&secret, None).await.unwrap();
    selected(&pg).await;
    let token = session(f.http(&secret, None, json!({})).await);
    tokio::time::sleep(Duration::from_millis(
        (expiry - now_ms()).max(0) as u64 + 150,
    ))
    .await;
    assert!(pg.simple_query(SELECT).await.is_err());
    f.refused(&secret, None).await;
    assert_eq!(
        f.http(&secret, Some(&token), json!({"ping":{}})).await.0,
        401
    );
    assert_eq!(f.http(&secret, None, json!({})).await.0, 401);
    f.stop().await;
}

#[tokio::test]
async fn credential_survives_restart() {
    let mut f = Fixture::start().await;
    let (_, secret) = f.issue(None).await;
    selected(&f.pg(&secret, None).await.unwrap()).await;
    f.stop().await;
    f.child = Fixture::spawn(&f.path, "restart");
    f.ready("restart").await;
    selected(&f.pg(&secret, None).await.unwrap()).await;
    f.stop().await;
}

#[tokio::test]
async fn revoke_task_ends_credentials() {
    let mut f = Fixture::start().await;
    let (_, secret) = f.issue(None).await;
    let pg = f.pg(&secret, None).await.unwrap();
    f.revoke("REVOKE TASK \"inc-436\"").await;
    closed(&pg).await;
    f.refused(&secret, None).await;
    f.stop().await;
}

#[tokio::test]
async fn cap_of_8_and_show() {
    let mut f = Fixture::start().await;
    let mut credentials = Vec::new();
    for _ in 0..8 {
        credentials.push(f.issue(None).await);
    }
    let issuer = f.ordinary("incident_controller").await;
    let error = issuer
        .simple_query("ISSUE CREDENTIAL FOR TASK \"inc-436\"")
        .await
        .unwrap_err();
    assert_eq!(error.code().unwrap().code(), "54000");
    let show = ok(&issuer, "SHOW TASK \"inc-436\"").await;
    let row = show
        .iter()
        .find_map(|m| {
            if let SimpleQueryMessage::Row(r) = m {
                Some(r)
            } else {
                None
            }
        })
        .unwrap();
    let ids = row.get("credentials").expect("credentials column");
    assert_eq!(ids.split("; ").count(), 8);
    assert!(!ids.contains("ggtc1_"));
    for (id, _) in &credentials {
        assert!(ids.contains(id));
    }
    let exposure = ok(&issuer, "SHOW TASK EXPOSURE \"inc-436\"").await;
    let cells: Vec<_> = exposure
        .iter()
        .filter_map(|m| {
            if let SimpleQueryMessage::Row(r) = m {
                Some(r)
            } else {
                None
            }
        })
        .flat_map(|r| (0..r.len()).filter_map(|i| r.get(i)))
        .collect();
    assert!(cells.iter().all(|cell| !cell.contains("ggtc1_")));
    for (id, _) in &credentials {
        assert!(
            cells
                .iter()
                .any(|cell| cell.contains(&format!("sql {id} as agent_exec expires ")))
        );
    }
    f.stop().await;
    // Read after process termination, including its complete stdout pipe.
    for entry in std::fs::read_dir(&f.path).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_some_and(|e| e == "log") {
            let log = std::fs::read_to_string(path).unwrap();
            for (_, secret) in &credentials {
                assert!(!log.contains(secret), "secret leaked to server output");
            }
        }
    }
}

#[tokio::test]
async fn issue_refused_in_task_session() {
    let mut f = Fixture::start().await;
    let (_, secret) = f.issue(None).await;
    let pg = f.pg(&secret, None).await.unwrap();
    let error = pg
        .simple_query("ISSUE CREDENTIAL FOR TASK \"inc-436\"")
        .await
        .unwrap_err();
    assert_eq!(error.code().unwrap().code(), "0A000");
    f.stop().await;
}

#[tokio::test]
async fn wrong_secret_and_cross_task() {
    let mut f = Fixture::start().await;
    let (_, secret) = f.issue(None).await;
    let mut wrong = secret.clone();
    let last = wrong.pop().unwrap();
    wrong.push(if last == '0' { '1' } else { '0' });
    f.refused(&wrong, None).await;
    assert_eq!(f.http(&wrong, None, json!({})).await.0, 401);
    f.refused(&secret, Some("inc-437")).await;
    assert_eq!(
        f.http(&secret, None, json!({"task":"inc-437"})).await.0,
        401
    );
    f.stop().await;
}

async fn lifecycle_http(
    ca: &TestCert,
    address: SocketAddr,
    secret: &str,
    token: Option<&str>,
    body: Value,
) -> (u16, Value) {
    let stream = TlsConnector::from(Arc::new(no_cert_client(ca)))
        .connect(
            ServerName::try_from("localhost").unwrap(),
            TcpStream::connect(address).await.unwrap(),
        )
        .await
        .unwrap();
    let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .unwrap();
    let task = tokio::spawn(connection);
    let mut request = hyper::Request::builder()
        .method("POST")
        .uri(if token.is_some() {
            "/v1/session/execute"
        } else {
            "/v1/sessions"
        })
        .header("host", "localhost")
        .header("content-type", "application/json")
        .header("authorization", format!("Bearer {secret}"));
    if let Some(token) = token {
        request = request.header("x-giggles-session", token);
    }
    let response = sender
        .send_request(
            request
                .body(Full::new(Bytes::from(serde_json::to_vec(&body).unwrap())))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status().as_u16();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    task.abort();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

async fn lifecycle_grpc(
    ca: &TestCert,
    address: SocketAddr,
) -> proto::giggles_client::GigglesClient<Channel> {
    let channel = Channel::from_shared(format!("https://{}", address))
        .unwrap()
        .tls_config(
            ClientTlsConfig::new()
                .domain_name("localhost")
                .ca_certificate(Certificate::from_pem(ca.cert.pem())),
        )
        .unwrap()
        .connect_timeout(Duration::from_secs(5))
        .connect()
        .await
        .unwrap();
    proto::giggles_client::GigglesClient::new(channel)
}

// In-process listeners make outcome retention configurable without changing the
// shipped-process fixture above. Credentials are still issued over ordinary SQL.
struct Lifecycle {
    f: agent_support::parity::ParityFixture,
    listeners: Vec<giggles_server::Running>,
    addresses: [SocketAddr; 3],
}

impl Lifecycle {
    async fn start() -> Self {
        use giggles_server::{
            agent::serve_agent_pg_bound,
            api::net::{serve_grpc_bound, serve_http_bound},
        };
        let f = agent_support::parity::ParityFixture::start().await;
        let tls = giggles_server::tls::server_optional_client(
            vec![f.server_cert.cert.der().clone()],
            rustls::pki_types::PrivatePkcs8KeyDer::from(f.server_cert.key.serialize_der()).into(),
            Some(vec![f.ca.cert.der().clone()]),
        )
        .unwrap();
        let (_, gate) = tokio::sync::watch::channel(true);
        let pg = serve_agent_pg_bound(
            tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap(),
            f.service.clone(),
            tls.clone(),
            16,
            vec!["giggles".into()],
            gate.clone(),
        )
        .await
        .unwrap();
        let http = serve_http_bound(
            tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap(),
            f.agent_api.clone(),
            tls.clone(),
            16,
            gate.clone(),
        )
        .await
        .unwrap();
        let grpc = serve_grpc_bound(
            tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap(),
            f.agent_api.clone(),
            tls,
            16,
            gate,
        )
        .await
        .unwrap();
        let addresses = [pg.address, http.address, grpc.address];
        Self {
            f,
            listeners: vec![pg, http, grpc],
            addresses,
        }
    }
    async fn issuer(&self) -> Client {
        use agent_support::parity::{CONTROLLER, CONTROLLER_PW};
        self.f.password(CONTROLLER, CONTROLLER_PW).await.unwrap()
    }
    async fn task(&self, name: &str, expiry: i64, caps: u64) {
        ok(&self.issuer().await, &format!(
            "CREATE TASK \"{name}\" EXECUTOR repair_agent ON shipments \
             VISIBLE ROWS WHERE (merchant_id = 42) READABLE (id,status) \
             ALLOW UPDATE (status) OLD ROW WHERE (status = 'failed') NEW ROW WHERE (status = 'queued') \
             AT MOST 25 DISTINCT ROWS AND 25 TRANSITIONS \
             DISCLOSE AT MOST {caps} ROWS READ, 2097152 BYTES RETURNED EXPIRES '{}'",
            timestamptz(expiry))).await;
    }
    async fn issue(&self, name: &str) -> String {
        let rows = ok(
            &self.issuer().await,
            &format!("ISSUE CREDENTIAL FOR TASK \"{name}\""),
        )
        .await;
        rows.iter()
            .find_map(|m| match m {
                SimpleQueryMessage::Row(r) => Some(r.get("secret").unwrap().to_owned()),
                _ => None,
            })
            .unwrap()
    }
    async fn pg(&self, secret: &str) -> Result<Client, tokio_postgres::Error> {
        let (client, connection) = tokio_postgres::Config::new()
            .host("localhost")
            .hostaddr(self.addresses[0].ip())
            .port(self.addresses[0].port())
            .user("repair_agent")
            .password(secret)
            .dbname("giggles")
            .ssl_mode(SslMode::Require)
            .connect(MakeRustlsConnect::new(no_cert_client(&self.f.ca)))
            .await?;
        tokio::spawn(connection);
        Ok(client)
    }
    async fn http(&self, secret: &str, token: Option<&str>, sql: Option<&str>) -> (u16, Value) {
        lifecycle_http(
            &self.f.ca,
            self.addresses[1],
            secret,
            token,
            sql.map_or(json!({}), lifecycle_body),
        )
        .await
    }
    async fn grpc(&self) -> proto::giggles_client::GigglesClient<Channel> {
        lifecycle_grpc(&self.f.ca, self.addresses[2]).await
    }
    async fn stop(self) {
        for listener in self.listeners {
            listener.shutdown().await.unwrap();
        }
        self.f.stop().await;
    }
}

fn lifecycle_body(sql: &str) -> Value {
    use agent_support::parity::Case;
    if let Some(rest) = sql.strip_prefix("OPERATION '") {
        let (op, statement) = rest.split_once("' ").unwrap();
        let id = statement.rsplit_once(" = ").unwrap().1;
        return Case::write(
            op,
            statement,
            format!("update shipments where id = {id} set status = \"queued\""),
        )
        .native_body();
    }
    if let Some(op) = sql
        .strip_prefix("SHOW OPERATION '")
        .and_then(|s| s.strip_suffix("'"))
    {
        return Case::outcome(op).native_body();
    }
    json!({"sql":{"sql":sql}})
}

fn grpc_sql(sql: &str) -> proto::ExecuteRequest {
    if sql.starts_with("OPERATION '") || sql.starts_with("SHOW OPERATION '") {
        return giggles_server::api::json::decode_execute(
            &serde_json::to_vec(&lifecycle_body(sql)).unwrap(),
        )
        .unwrap();
    }
    proto::ExecuteRequest {
        request: Some(proto::execute_request::Request::Sql(proto::SqlRequest {
            sql: sql.into(),
            params: vec![],
        })),
        ..Default::default()
    }
}

async fn past(deadline: i64) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while now_ms() <= deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("short test deadline reached");
}

#[tokio::test]
#[allow(non_snake_case)]
async fn expiry_mid_session_new_write_28T02_but_outcome_lookup_works() {
    let h = Lifecycle::start().await;
    let expiry = now_ms() + 4000;
    h.task("short", expiry, 1000).await;
    let secret = h.issue("short").await;
    let pg = h.pg(&secret).await.unwrap();
    let http = session(h.http(&secret, None, None).await);
    let mut grpc = h.grpc().await;
    let token = grpc
        .open_session(bearer(&secret, None, proto::OpenSessionRequest::default()))
        .await
        .unwrap()
        .into_inner()
        .session_id;
    // Each protocol commits its own operation before the expiry boundary.
    let write = |op: &str, id| {
        format!("OPERATION '{op}' UPDATE shipments SET status = 'queued' WHERE id = {id}")
    };
    ok(&pg, &write("expiry-pg", 1)).await;
    assert_eq!(
        h.http(&secret, Some(&http), Some(&write("expiry-http", 2)))
            .await
            .1["code"],
        "ok"
    );
    assert_eq!(
        grpc.execute(bearer(
            &secret,
            Some(&token),
            grpc_sql(&write("expiry-grpc", 3))
        ))
        .await
        .unwrap()
        .into_inner()
        .code,
        "ok"
    );
    past(expiry).await;
    let new_write = write("too-late", 4);
    assert_eq!(
        pg.simple_query(&new_write)
            .await
            .unwrap_err()
            .code()
            .unwrap()
            .code(),
        "28T02"
    );
    assert_eq!(
        h.http(&secret, Some(&http), Some(&new_write)).await.1["code"],
        "28T02"
    );
    assert_eq!(
        grpc.execute(bearer(&secret, Some(&token), grpc_sql(&new_write)))
            .await
            .unwrap()
            .into_inner()
            .code,
        "28T02"
    );
    // Existing and newly authenticated sessions retain the committed outcomes.
    for fresh in [false, true] {
        let other_pg;
        let p = if fresh {
            other_pg = h.pg(&secret).await.unwrap();
            &other_pg
        } else {
            &pg
        };
        assert!(
            ok(p, "SHOW OPERATION 'expiry-pg'")
                .await
                .iter()
                .any(|r| matches!(r, SimpleQueryMessage::Row(_)))
        );
        let t = if fresh {
            session(h.http(&secret, None, None).await)
        } else {
            http.clone()
        };
        assert_eq!(
            h.http(&secret, Some(&t), Some("SHOW OPERATION 'expiry-http'"))
                .await
                .1["code"],
            "ok"
        );
        let t = if fresh {
            grpc.open_session(bearer(&secret, None, proto::OpenSessionRequest::default()))
                .await
                .unwrap()
                .into_inner()
                .session_id
        } else {
            token.clone()
        };
        assert_eq!(
            grpc.execute(bearer(
                &secret,
                Some(&t),
                grpc_sql("SHOW OPERATION 'expiry-grpc'")
            ))
            .await
            .unwrap()
            .into_inner()
            .code,
            "ok"
        );
    }
    h.stop().await;
}

#[tokio::test]
#[allow(non_snake_case)]
async fn revoke_mid_session_everything_28T01_including_outcome() {
    let mut f = Fixture::start().await;
    let (_, secret) = f.issue(None).await;
    let pg = f.pg(&secret, None).await.unwrap();
    let http = session(f.http(&secret, None, json!({})).await);
    let mut grpc = f.grpc().await;
    let token = grpc
        .open_session(bearer(&secret, None, proto::OpenSessionRequest::default()))
        .await
        .unwrap()
        .into_inner()
        .session_id;
    f.revoke("REVOKE TASK \"inc-436\"").await;
    // SQL-credential identity revalidation precedes class admission. Revocation
    // cuts the connection/lease even for SHOW OPERATION (design R3-1301 section 4).
    closed(&pg).await;
    for sql in [
        SELECT,
        "SHOW TASK",
        "SET application_name = 'test'",
        "SHOW OPERATION 'missing'",
        "OPERATION 'revoked' UPDATE shipments SET status = 'queued' WHERE id = 1",
    ] {
        assert!(pg.simple_query(sql).await.is_err());
        let (status, body) = f
            .http(&secret, Some(&http), json!({"sql":{"sql":sql}}))
            .await;
        assert_eq!(status, 401, "{body}");
        assert_eq!(body["error"]["code"], "UNAUTHENTICATED");
        assert_eq!(
            grpc.execute(bearer(&secret, Some(&token), grpc_sql(sql)))
                .await
                .unwrap_err()
                .code(),
            Code::Unauthenticated
        );
    }
    f.stop().await;
    revoke_mid_cert_session_28t01().await;
}

/// A certificate binding still lists the revoked task, so identity
/// revalidation passes and `task_standing` refuses every class, including
/// outcome lookup, with 28T01 (design-2 section 3, step 3).
async fn revoke_mid_cert_session_28t01() {
    use agent_support::parity::{self, ParityFixture};
    let f = ParityFixture::start().await;
    let (pg, connection) = tokio_postgres::Config::new()
        .host("localhost")
        .hostaddr(f.agent_pg.address.ip())
        .port(f.agent_pg.address.port())
        .user(parity::EXECUTOR)
        .dbname("giggles")
        .options("-c giggles.task=inc-436")
        .ssl_mode(SslMode::Require)
        .connect(MakeRustlsConnect::new(f.agent.client(&f.ca)))
        .await
        .unwrap();
    tokio::spawn(connection);
    let http = f.open_http().await.unwrap();
    let token = f.open_grpc().await.unwrap();
    let mut grpc = parity::grpc_client(f.agent_grpc.address, &f.ca, &f.agent).await;
    let issuer = f
        .password(parity::CONTROLLER, parity::CONTROLLER_PW)
        .await
        .unwrap();
    ok(&issuer, "REVOKE TASK \"inc-436\"").await;
    for sql in [
        SELECT,
        "SHOW OPERATION 'missing'",
        "OPERATION 'revoked' UPDATE shipments SET status = 'queued' WHERE id = 1",
    ] {
        let error = pg.simple_query(sql).await.unwrap_err();
        assert_eq!(error.code().unwrap().code(), "28T01", "pg: {sql}: {error:?}");
        let (status, body) = parity::http_call(
            f.agent_http.address,
            (&f.ca, &f.agent),
            "POST",
            "/v1/session/execute",
            Some(&http),
            Some(&lifecycle_body(sql)),
        )
        .await;
        assert_eq!(status, 200, "http: {sql}: {body}");
        assert_eq!(body["code"], "28T01", "http: {sql}: {body}");
        let reply = grpc
            .execute(parity::with_session(&token, grpc_sql(sql)))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.code, "28T01", "grpc: {sql}");
    }
    drop((pg, issuer));
    f.stop().await;
}

async fn refused_three(f: &Fixture, secret: &str) {
    let error = f
        .pg(secret, None)
        .await
        .expect_err("credential must be refused");
    let db = error.as_db_error().unwrap();
    assert_eq!(db.code().code(), "28P01");
    assert_eq!(
        db.message(),
        "password authentication failed for user \"agent_exec\""
    );
    let (status, body) = f.http(secret, None, json!({})).await;
    assert_eq!(status, 401);
    assert_eq!(body["error"]["code"], "UNAUTHENTICATED");
    assert_eq!(
        f.grpc()
            .await
            .open_session(bearer(secret, None, proto::OpenSessionRequest::default()))
            .await
            .unwrap_err()
            .code(),
        Code::Unauthenticated
    );
}

#[tokio::test]
async fn revoked_task_refused_at_connect_on_three_protocols() {
    let mut f = Fixture::start().await;
    let (id, revoked) = f.issue(None).await;
    let expiry = now_ms() + 1500;
    let (_, expired) = f.issue(Some(expiry)).await;
    let (_, task) = f.issue(None).await;
    f.revoke(&format!("REVOKE CREDENTIAL {id}")).await;
    refused_three(&f, &revoked).await;
    past(expiry).await;
    refused_three(&f, &expired).await;
    f.revoke("REVOKE TASK \"inc-436\"").await;
    refused_three(&f, &task).await;
    f.stop().await;
}

#[tokio::test]
async fn after_retention_connect_refused() {
    let h = Lifecycle::start().await;
    h.f.service
        .set_outcome_retention(Duration::from_millis(250));
    let expiry = now_ms() + 1500;
    h.task("retention", expiry, 1000).await;
    let secret = h.issue("retention").await;
    h.pg(&secret).await.unwrap();
    session(h.http(&secret, None, None).await);
    h.grpc()
        .await
        .open_session(bearer(&secret, None, proto::OpenSessionRequest::default()))
        .await
        .unwrap();
    past(expiry + 250).await;
    let error = h.pg(&secret).await.unwrap_err();
    assert_eq!(error.code().unwrap().code(), "28P01");
    assert_eq!(
        error.as_db_error().unwrap().message(),
        "password authentication failed for user \"repair_agent\""
    );
    assert_eq!(h.http(&secret, None, None).await.0, 401);
    assert_eq!(
        h.grpc()
            .await
            .open_session(bearer(&secret, None, proto::OpenSessionRequest::default()))
            .await
            .unwrap_err()
            .code(),
        Code::Unauthenticated
    );
    h.stop().await;
}

async fn binding_cut(rebind: bool) {
    use agent_support::parity::{self, ParityFixture};
    let f = ParityFixture::start().await;
    // A tokio-postgres client exposes server-initiated closure without a query.
    let (pg, connection) = tokio_postgres::Config::new()
        .host("localhost")
        .hostaddr(f.agent_pg.address.ip())
        .port(f.agent_pg.address.port())
        .user(parity::EXECUTOR)
        .dbname("giggles")
        .options("-c giggles.task=inc-436")
        .ssl_mode(SslMode::Require)
        .connect(MakeRustlsConnect::new(f.agent.client(&f.ca)))
        .await
        .unwrap();
    tokio::spawn(connection);
    let http = f.open_http().await.unwrap();
    let token = f.open_grpc().await.unwrap();
    let mut grpc = parity::grpc_client(f.agent_grpc.address, &f.ca, &f.agent).await;
    let bindings = if rebind {
        let issuer = f
            .password(parity::CONTROLLER, parity::CONTROLLER_PW)
            .await
            .unwrap();
        ok(
            &issuer,
            &parity::inc_436_sql(&timestamptz(now_ms() + agent_support::DAY_MS)).replacen(
                "inc-436",
                "inc-other",
                1,
            ),
        )
        .await;
        vec![agent_support::binding(
            &f.agent,
            parity::EXECUTOR,
            &["inc-other"],
        )]
    } else {
        vec![]
    };
    f.service.reload_all_bindings(vec![], bindings).unwrap();
    closed(&pg).await;
    for _ in 0..2 {
        let (status, body) = parity::http_call(
            f.agent_http.address,
            (&f.ca, &f.agent),
            "POST",
            "/v1/session/execute",
            Some(&http),
            Some(&json!({"sql":{"sql":SELECT}})),
        )
        .await;
        // A removed binding fails identity (401). A rebound certificate still
        // authenticates, so the old lease is either refused by check_binding
        // (401) or already swept by the binding-change registry sweep (404
        // SESSION_NOT_FOUND); either way it can never run again.
        let code = grpc
            .execute(parity::with_session(&token, grpc_select()))
            .await
            .unwrap_err()
            .code();
        if rebind {
            assert!(
                (status == 401 && body["error"]["code"] == "UNAUTHENTICATED")
                    || (status == 404 && body["error"]["code"] == "SESSION_NOT_FOUND"),
                "{status} {body}"
            );
            assert!(
                matches!(code, Code::Unauthenticated | Code::NotFound),
                "{code:?}"
            );
        } else {
            assert_eq!(status, 401, "{body}");
            assert_eq!(code, Code::Unauthenticated);
        }
    }
    if rebind {
        // The same certificate can open the new task, but never reuse old leases.
        let (_, body) = parity::http_call(
            f.agent_http.address,
            (&f.ca, &f.agent),
            "POST",
            "/v1/sessions",
            None,
            Some(&json!({"task":"inc-other"})),
        )
        .await;
        assert!(body["sessionId"].is_string(), "{body}");
    } else {
        assert!(f.open_http().await.is_err());
        assert!(f.open_grpc().await.is_err());
    }
    f.stop().await;
}

#[tokio::test]
async fn rebinding_fingerprint_to_other_task_cuts_old_session_on_next_call() {
    binding_cut(true).await;
}

#[tokio::test]
async fn removed_binding_cuts_session_and_api_lease() {
    binding_cut(false).await;
}

#[tokio::test]
#[ignore = "R3-1005: no live task-definition ALTER DDL/API; only REVOKE bumps def_version"]
async fn definition_change_requires_reconnect() {
    // The production API cannot construct a non-revoked definition change yet.
    // Keep the required regression visible, rather than simulate a reconnect
    // with REVOKE (which must return Revoked instead of DefinitionChanged).
    assert_eq!(
        giggles_server::sqlstate(&giggles_core::CoreError::Task(
            giggles_core::task::TaskError::Denied(giggles_core::task::DenyRule::DefinitionChanged)
        )),
        "42501"
    );
    panic!("exercise old/new sessions once live task definition changes are supported");
}

#[tokio::test]
async fn two_sessions_same_cert_share_budgets() {
    use agent_support::parity::{self, ParityFixture};
    let f = ParityFixture::start().await;
    f.service
        .set_agent_limits(giggles_server::agent::AgentLimits {
            disclosure_chunk_rows: 0,
            disclosure_chunk_bytes: 0,
            ..Default::default()
        })
        .unwrap();
    let issuer = f
        .password(parity::CONTROLLER, parity::CONTROLLER_PW)
        .await
        .unwrap();
    let sql = parity::inc_436_sql(&timestamptz(now_ms() + agent_support::DAY_MS))
        .replacen("inc-436", "budget", 1)
        .replace("1000 ROWS READ", "2 ROWS READ")
        .replace(
            "READABLE (id, status, failed_at, attempt_count)",
            "READABLE (merchant_id, id, status, failed_at, attempt_count)",
        );
    ok(&issuer, &sql).await;
    f.service
        .reload_all_bindings(
            vec![],
            vec![agent_support::binding(
                &f.agent,
                parity::EXECUTOR,
                &["budget"],
            )],
        )
        .unwrap();
    let mut a = agent_support::connect_raw(
        f.agent_pg.address,
        &f.ca,
        &f.agent,
        &[
            ("user", parity::EXECUTOR),
            ("database", "giggles"),
            ("giggles.task", "budget"),
        ],
    )
    .await;
    let mut b = agent_support::connect_raw(
        f.agent_pg.address,
        &f.ca,
        &f.agent,
        &[
            ("user", parity::EXECUTOR),
            ("database", "giggles"),
            ("giggles.task", "budget"),
        ],
    )
    .await;
    let read = "SELECT id FROM shipments WHERE merchant_id = 42 AND id = 1";
    assert_eq!(parity::observe_pg(&a.simple(read).await).sqlstate, None);
    assert_eq!(parity::observe_pg(&b.simple(read).await).sqlstate, None);
    let denied = parity::observe_pg(&a.simple(read).await);
    assert_eq!(denied.sqlstate.as_deref(), Some("54T01"), "{denied:?}");
    drop(a);
    let mut c = agent_support::connect_raw(
        f.agent_pg.address,
        &f.ca,
        &f.agent,
        &[
            ("user", parity::EXECUTOR),
            ("database", "giggles"),
            ("giggles.task", "budget"),
        ],
    )
    .await;
    assert_eq!(
        parity::observe_pg(&c.simple(read).await)
            .sqlstate
            .as_deref(),
        Some("54T01")
    );
    // Switching transport does not reset the task's counters either.
    let (status, reply) = parity::http_call(
        f.agent_http.address,
        (&f.ca, &f.agent),
        "POST",
        "/v1/sessions",
        None,
        Some(&json!({"task":"budget"})),
    )
    .await;
    assert_eq!(status, 201);
    let (_, reply) = parity::http_call(
        f.agent_http.address,
        (&f.ca, &f.agent),
        "POST",
        "/v1/session/execute",
        Some(reply["sessionId"].as_str().unwrap()),
        Some(&json!({"sql":{"sql":read}})),
    )
    .await;
    assert_eq!(reply["code"], "54T01");
    let mut grpc = parity::grpc_client(f.agent_grpc.address, &f.ca, &f.agent).await;
    let token = grpc
        .open_session(proto::OpenSessionRequest {
            task: Some("budget".into()),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner()
        .session_id;
    assert_eq!(
        grpc.execute(parity::with_session(&token, grpc_sql(read)))
            .await
            .unwrap()
            .into_inner()
            .code,
        "54T01"
    );
    drop(b);
    drop(c);
    f.stop().await;
}

#[tokio::test]
async fn sql_credential_lifecycle_three_protocols() {
    let mut f = Fixture::start().await;
    let mut credentials = Vec::new();
    for _ in 0..8 {
        credentials.push(f.issue(None).await);
    }
    let issuer = f.ordinary("incident_controller").await;
    assert_eq!(
        issuer
            .simple_query("ISSUE CREDENTIAL FOR TASK \"inc-436\"")
            .await
            .unwrap_err()
            .code()
            .unwrap()
            .code(),
        "54000"
    );
    f.revoke(&format!("REVOKE CREDENTIAL {}", credentials[0].0))
        .await;
    refused_three(&f, &credentials[0].1).await;
    let (_, secret) = f.issue(None).await; // Revocation frees the eighth slot.
    let pg = f.pg(&secret, None).await.unwrap();
    let http = session(f.http(&secret, None, json!({})).await);
    let mut grpc = f.grpc().await;
    let token = grpc
        .open_session(bearer(&secret, None, proto::OpenSessionRequest::default()))
        .await
        .unwrap()
        .into_inner()
        .session_id;
    selected(&pg).await;
    assert_eq!(
        f.http(&secret, Some(&http), json!({"sql":{"sql":SELECT}}))
            .await
            .1["code"],
        "ok"
    );
    assert_eq!(
        grpc.execute(bearer(&secret, Some(&token), grpc_select()))
            .await
            .unwrap()
            .into_inner()
            .code,
        "ok"
    );
    ok(&f.ordinary("owner").await, "ALTER ROLE agent_exec NOLOGIN").await;
    assert!(pg.simple_query(SELECT).await.is_err());
    assert_eq!(
        f.http(&secret, Some(&http), json!({"sql":{"sql":SELECT}}))
            .await
            .0,
        401
    );
    assert_eq!(
        grpc.execute(bearer(&secret, Some(&token), grpc_select()))
            .await
            .unwrap_err()
            .code(),
        Code::Unauthenticated
    );
    refused_three(&f, &secret).await;
    f.stop().await;
}

#[tokio::test]
async fn idle_session_cut_off_at_next_call() {
    for revoke_task in [false, true] {
        let mut f = Fixture::start_with_api_limit(2).await;
        let (id, secret) = f.issue(None).await;
        let pg = f.pg(&secret, None).await.unwrap();
        let http = session(f.http(&secret, None, json!({})).await);
        let mut grpc = f.grpc().await;
        let token = grpc
            .open_session(bearer(&secret, None, proto::OpenSessionRequest::default()))
            .await
            .unwrap()
            .into_inner()
            .session_id;
        let sql = if revoke_task {
            "REVOKE TASK \"inc-436\"".into()
        } else {
            format!("REVOKE CREDENTIAL {id}")
        };
        f.revoke(&sql).await;
        // No traffic is sent on any agent session before observing closure.
        closed(&pg).await;
        // Both API slots were occupied by idle leases. Authenticate using a
        // different task without touching either old lease: capacity can only
        // return if the notification/periodic sweeper reclaimed them.
        let rows = ok(
            &f.ordinary("incident_controller").await,
            "ISSUE CREDENTIAL FOR TASK \"inc-437\"",
        )
        .await;
        let replacement = rows
            .iter()
            .find_map(|m| match m {
                SimpleQueryMessage::Row(r) => Some(r.get("secret").unwrap().to_owned()),
                _ => None,
            })
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let (status, body) = f.http(&replacement, None, json!({})).await;
                if status == 201 {
                    break;
                }
                assert_eq!(status, 429, "waiting for idle lease capacity: {body}");
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("idle HTTP lease swept without a call on it");
        grpc.open_session(bearer(
            &replacement,
            None,
            proto::OpenSessionRequest::default(),
        ))
        .await
        .expect("idle gRPC lease also swept");
        assert!(pg.simple_query(SELECT).await.is_err());
        for _ in 0..2 {
            assert_eq!(
                f.http(&secret, Some(&http), json!({"ping":{}})).await.0,
                401
            );
            assert_eq!(
                grpc.execute(bearer(&secret, Some(&token), grpc_select()))
                    .await
                    .unwrap_err()
                    .code(),
                Code::Unauthenticated
            );
        }
        f.stop().await;
    }
}
