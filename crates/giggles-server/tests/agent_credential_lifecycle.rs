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
        let config = json!({
            "database":path.join("db.redb"), "native":"127.0.0.1:0", "postgres":"127.0.0.1:0",
            "postgresql":{"listen":"127.0.0.1:0","tls":"disabled"},
            "certificate":path.join("server.pem"),"private_key":path.join("server.key.TEST-ONLY"),
            "client_ca":path.join("ca.pem"),"bindings":[],"connections":32,
            "skew_seconds":0,"max_pause_seconds":3600,
            "agent":{"postgres":"127.0.0.1:0","http":"127.0.0.1:0","grpc":"127.0.0.1:0"}
        });
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
        let error = self
            .pg(secret, task)
            .await
            .expect_err("refuse agent login");
        assert_eq!(error.code().unwrap().code(), "28P01");
    }
    async fn http(&self, secret: &str, token: Option<&str>, body: Value) -> (u16, Value) {
        let stream = TlsConnector::from(Arc::new(no_cert_client(&self.ca)))
            .connect(
                ServerName::try_from("localhost").unwrap(),
                TcpStream::connect(self.addresses["agent_http"])
                    .await
                    .unwrap(),
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
    async fn grpc(&self) -> proto::giggles_client::GigglesClient<Channel> {
        let channel = Channel::from_shared(format!("https://{}", self.addresses["agent_grpc"]))
            .unwrap()
            .tls_config(
                ClientTlsConfig::new()
                    .domain_name("localhost")
                    .ca_certificate(Certificate::from_pem(self.ca.cert.pem())),
            )
            .unwrap()
            .connect_timeout(Duration::from_secs(5))
            .connect()
            .await
            .unwrap();
        proto::giggles_client::GigglesClient::new(channel)
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
