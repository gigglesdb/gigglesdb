//! Shared fixture for agent-listener tests (GD-0061 R3-205; grown in R3-1001):
//! test certificates, the worked-example database, an agent pgwire listener over
//! real TLS and a raw startup client that can send any startup parameter.
#![allow(dead_code)]
use giggles_core::{
    Admin, Engine, Limits,
    roles::{NewRole, TablePrivilege},
    task::definition::{
        DisclosureCaps, LoadLimits, MutationCaps, NewTask, PredicateText, WallClockMs,
    },
};
use giggles_server::{
    AgentBinding, Binding, Running, Service, admission::ServiceLimits,
    agent::serve_agent_pg_bound, clock::Clock, tls,
};
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair,
};
use rustls::{
    ClientConfig, ServerConfig,
    pki_types::{PrivatePkcs8KeyDer, ServerName},
};
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::watch,
};
use tokio_rustls::TlsConnector;

static SERIAL: AtomicU64 = AtomicU64::new(0);
pub const DAY_MS: i64 = 86_400_000;

pub struct TestCert {
    pub cert: Certificate,
    pub key: KeyPair,
}

impl TestCert {
    pub fn fingerprint(&self) -> String {
        tls::fingerprint(self.cert.der())
    }
    /// A rustls client config presenting this certificate and trusting `ca`.
    pub fn client(&self, ca: &TestCert) -> ClientConfig {
        ClientConfig::builder()
            .with_root_certificates(tls::roots(vec![ca.cert.der().clone()]).unwrap())
            .with_client_auth_cert(
                vec![self.cert.der().clone()],
                PrivatePkcs8KeyDer::from(self.key.serialize_der()).into(),
            )
            .unwrap()
    }
}

/// A TLS client trusting the fixture CA without presenting a certificate.
pub fn no_cert_client(ca: &TestCert) -> ClientConfig {
    ClientConfig::builder()
        .with_root_certificates(tls::roots(vec![ca.cert.der().clone()]).unwrap())
        .with_no_client_auth()
}

/// A CA (no `ca`), or a server or client certificate signed by `ca`.
pub fn certificate(ca: Option<&TestCert>, server: bool) -> TestCert {
    let key = KeyPair::generate().unwrap();
    let mut p = CertificateParams::new(vec!["localhost".into()]).unwrap();
    p.distinguished_name.push(
        rcgen::DnType::CommonName,
        match (ca.is_none(), server) {
            (true, _) => "Giggles agent test CA",
            (false, true) => "localhost",
            (false, false) => "Giggles agent test client",
        },
    );
    if ca.is_none() {
        p.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    } else {
        p.extended_key_usages = vec![if server {
            ExtendedKeyUsagePurpose::ServerAuth
        } else {
            ExtendedKeyUsagePurpose::ClientAuth
        }];
    }
    let cert = match ca {
        Some(ca) => p.signed_by(&key, &ca.cert, &ca.key).unwrap(),
        None => p.self_signed(&key).unwrap(),
    };
    TestCert { cert, key }
}

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// Unix milliseconds as a PostgreSQL timestamptz literal in UTC.
pub fn timestamptz(ms: i64) -> String {
    let days = ms.div_euclid(86_400_000);
    let rem = ms.rem_euclid(86_400_000);
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02} {:02}:{:02}:{:02}.{:03}+00",
        rem / 3_600_000,
        rem / 60_000 % 60,
        rem / 1000 % 60,
        rem % 1000
    )
}

/// A scratch directory under the task TEMP.
pub fn scratch(prefix: &str) -> PathBuf {
    let root = PathBuf::from(std::env::var_os("TEMP").expect("set task TEMP"));
    let path = root.join(format!(
        "{prefix}-{}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        SERIAL.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&path).unwrap();
    path
}

/// The worked example: `shipments` (`deliveries` is reserved), an issuer, an
/// ordinary login, and two task-only executors.
pub fn admin() -> Admin {
    let path = scratch("agent-pg");
    let admin = Admin::create_engine(path.join("db.redb"), Limits::default(), Engine::V2).unwrap();
    admin
        .sql_session()
        .execute(
            "CREATE TABLE shipments (id bigint primary key, merchant_id bigint, status text)",
            &[],
            None,
        )
        .unwrap();
    let roles = admin.roles();
    for (name, task_only) in [
        ("incident_controller", false),
        ("oncall_lead", false),
        ("agent_exec", true),
        ("other_exec", true),
    ] {
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
    roles
        .grant_table("shipments", "incident_controller", &[TablePrivilege::Select])
        .unwrap();
    admin
}

pub fn create_task(admin: &Admin, name: &str, executor: &str) {
    let issuer = admin.roles().get("incident_controller").unwrap().unwrap();
    admin
        .tasks()
        .create(
            &issuer,
            NewTask {
                name: name.into(),
                executor: executor.into(),
                approver: None,
                schema: None,
                table: "shipments".into(),
                visible_rows: PredicateText("merchant_id = 42".into()),
                readable: vec!["id".into(), "status".into()],
                masked: Vec::new(),
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
}

pub fn revoke(admin: &Admin, name: &str) {
    let issuer = admin.roles().get("incident_controller").unwrap().unwrap();
    admin.tasks().revoke(&issuer, name).unwrap();
}

pub fn binding(cert: &TestCert, executor: &str, tasks: &[&str]) -> AgentBinding {
    AgentBinding {
        fingerprint: cert.fingerprint(),
        executor: executor.into(),
        tasks: tasks.iter().map(|t| t.to_string()).collect(),
    }
}

/// One CA, a server certificate, the service and a running agent listener.
pub struct AgentServer {
    pub admin: Admin,
    pub ca: TestCert,
    pub server: TestCert,
    pub service: Arc<Service>,
    pub tls: Arc<ServerConfig>,
    pub agent: Running,
}

impl AgentServer {
    pub async fn start(
        admin: Admin,
        ca: TestCert,
        ordinary: Vec<Binding>,
        agents: Vec<AgentBinding>,
    ) -> Self {
        let server = certificate(Some(&ca), true);
        let service = Service::with_agent(
            admin.clone(),
            ordinary,
            agents,
            Clock::new(0, 3600).unwrap(),
            ServiceLimits::default(),
        )
        .unwrap();
        let tls = tls::server_optional_client(
            vec![server.cert.der().clone()],
            PrivatePkcs8KeyDer::from(server.key.serialize_der()).into(),
            Some(vec![ca.cert.der().clone()]),
        )
        .unwrap();
        let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (_, start) = watch::channel(true);
        let agent = serve_agent_pg_bound(
            socket,
            service.clone(),
            tls.clone(),
            8,
            vec!["giggles".into()],
            start,
        )
        .await
        .unwrap();
        Self {
            admin,
            ca,
            server,
            service,
            tls,
            agent,
        }
    }
}

/// What the server answered to one startup packet.
#[derive(Debug, PartialEq, Eq)]
pub enum Startup {
    /// AuthenticationOk ... ReadyForQuery; the ParameterStatus pairs.
    Ready(Vec<(String, String)>),
    /// An ErrorResponse: severity, SQLSTATE, message and its raw body.
    Error {
        severity: String,
        code: String,
        message: String,
        body: Vec<u8>,
    },
    /// The server closed the connection (or TLS failed) without a message.
    Dropped,
}

/// SSLRequest, TLS with `cert`, then a startup packet with `params`.
pub async fn raw_startup(
    address: std::net::SocketAddr,
    ca: &TestCert,
    cert: &TestCert,
    params: &[(&str, &str)],
) -> Startup {
    tokio::time::timeout(Duration::from_secs(15), async {
        let mut socket = TcpStream::connect(address).await.unwrap();
        socket
            .write_all(&[0, 0, 0, 8, 0x04, 0xd2, 0x16, 0x2f])
            .await
            .unwrap();
        let mut answer = [0u8; 1];
        if socket.read_exact(&mut answer).await.is_err() || answer[0] != b'S' {
            return Startup::Dropped;
        }
        let connector = TlsConnector::from(Arc::new(cert.client(ca)));
        let Ok(mut stream) = connector
            .connect(ServerName::try_from("localhost").unwrap(), socket)
            .await
        else {
            return Startup::Dropped;
        };
        let mut body = 196608u32.to_be_bytes().to_vec();
        for (key, value) in params {
            body.extend_from_slice(key.as_bytes());
            body.push(0);
            body.extend_from_slice(value.as_bytes());
            body.push(0);
        }
        body.push(0);
        let mut packet = ((body.len() + 4) as u32).to_be_bytes().to_vec();
        packet.extend_from_slice(&body);
        if stream.write_all(&packet).await.is_err() || stream.flush().await.is_err() {
            return Startup::Dropped;
        }
        let mut statuses = Vec::new();
        loop {
            let Ok(tag) = stream.read_u8().await else {
                return Startup::Dropped;
            };
            let Ok(length) = stream.read_u32().await else {
                return Startup::Dropped;
            };
            assert!((4..65536).contains(&length));
            let mut body = vec![0; length as usize - 4];
            if stream.read_exact(&mut body).await.is_err() {
                return Startup::Dropped;
            }
            match tag {
                b'R' if body == 3u32.to_be_bytes() => {
                    // Raw startup probes answer a password challenge with an empty password.
                    stream.write_all(&[b'p', 0, 0, 0, 5, 0]).await.unwrap();
                }
                b'E' => return error_fields(body),
                b'S' => {
                    let mut parts = body.split(|b| *b == 0);
                    let key = String::from_utf8_lossy(parts.next().unwrap()).into_owned();
                    let value = String::from_utf8_lossy(parts.next().unwrap()).into_owned();
                    statuses.push((key, value));
                }
                b'Z' => return Startup::Ready(statuses),
                _ => {}
            }
        }
    })
    .await
    .expect("startup timed out")
}

fn error_fields(body: Vec<u8>) -> Startup {
    let (mut severity, mut code, mut message) = (String::new(), String::new(), String::new());
    for field in body.split(|b| *b == 0).filter(|f| !f.is_empty()) {
        let value = String::from_utf8_lossy(&field[1..]).into_owned();
        match field[0] {
            b'S' => severity = value,
            b'C' => code = value,
            b'M' => message = value,
            _ => {}
        }
    }
    Startup::Error {
        severity,
        code,
        message,
        body,
    }
}

impl Startup {
    pub fn status(&self, key: &str) -> Option<&str> {
        match self {
            Startup::Ready(statuses) => statuses
                .iter()
                .find(|(k, _)| k == key)
                .map(|(_, v)| v.as_str()),
            _ => None,
        }
    }
    pub fn code(&self) -> Option<&str> {
        match self {
            Startup::Error { code, .. } => Some(code),
            _ => None,
        }
    }
}

/// One backend message, decoded for assertions (R3-206).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Msg {
    ParseComplete,
    BindComplete,
    NoData,
    Empty,
    Suspended,
    Parameters(Vec<u32>),
    Columns(Vec<String>),
    Row(Vec<Option<String>>),
    Complete(String),
    Error { code: String, message: String },
    Ready(u8),
    Other(u8),
}

/// A raw protocol-v3 session over TLS, after startup.
pub struct Wire {
    stream: tokio_rustls::client::TlsStream<TcpStream>,
}

/// SSLRequest, TLS with `cert`, startup with `params`; panics unless the
/// server answers ReadyForQuery.
pub async fn connect_raw(
    address: std::net::SocketAddr,
    ca: &TestCert,
    cert: &TestCert,
    params: &[(&str, &str)],
) -> Wire {
    let mut socket = TcpStream::connect(address).await.unwrap();
    socket
        .write_all(&[0, 0, 0, 8, 0x04, 0xd2, 0x16, 0x2f])
        .await
        .unwrap();
    let mut answer = [0u8; 1];
    socket.read_exact(&mut answer).await.unwrap();
    assert_eq!(answer[0], b'S');
    let stream = TlsConnector::from(Arc::new(cert.client(ca)))
        .connect(ServerName::try_from("localhost").unwrap(), socket)
        .await
        .unwrap();
    let mut wire = Wire { stream };
    let mut body = 196608u32.to_be_bytes().to_vec();
    for (key, value) in params {
        body.extend_from_slice(key.as_bytes());
        body.push(0);
        body.extend_from_slice(value.as_bytes());
        body.push(0);
    }
    body.push(0);
    let mut packet = ((body.len() + 4) as u32).to_be_bytes().to_vec();
    packet.extend_from_slice(&body);
    wire.stream.write_all(&packet).await.unwrap();
    let messages = wire.until_ready().await;
    assert!(
        matches!(messages.last(), Some(Msg::Ready(b'I'))),
        "startup: {messages:?}"
    );
    wire
}

fn message(tag: u8, body: &[u8], out: &mut Vec<u8>) {
    out.push(tag);
    out.extend(((body.len() + 4) as u32).to_be_bytes());
    out.extend_from_slice(body);
}

fn cstr(text: &str, out: &mut Vec<u8>) {
    out.extend_from_slice(text.as_bytes());
    out.push(0);
}

impl Wire {
    async fn read(&mut self) -> Msg {
        let tag = self.stream.read_u8().await.unwrap();
        let length = self.stream.read_u32().await.unwrap() as usize;
        let mut body = vec![0; length - 4];
        self.stream.read_exact(&mut body).await.unwrap();
        let text = |b: &[u8]| String::from_utf8_lossy(b).into_owned();
        match tag {
            b'1' => Msg::ParseComplete,
            b'2' => Msg::BindComplete,
            b'n' => Msg::NoData,
            b'I' => Msg::Empty,
            b's' => Msg::Suspended,
            b'Z' => Msg::Ready(body[0]),
            b'C' => Msg::Complete(text(&body[..body.len() - 1])),
            b't' => Msg::Parameters(
                body[2..]
                    .chunks(4)
                    .map(|c| u32::from_be_bytes(c.try_into().unwrap()))
                    .collect(),
            ),
            b'T' => {
                let count = u16::from_be_bytes([body[0], body[1]]) as usize;
                let mut at = 2;
                let mut names = Vec::new();
                for _ in 0..count {
                    let end = at + body[at..].iter().position(|b| *b == 0).unwrap();
                    names.push(text(&body[at..end]));
                    at = end + 1 + 18;
                }
                Msg::Columns(names)
            }
            b'D' => {
                let count = u16::from_be_bytes([body[0], body[1]]) as usize;
                let mut at = 2;
                let mut values = Vec::new();
                for _ in 0..count {
                    let n = i32::from_be_bytes(body[at..at + 4].try_into().unwrap());
                    at += 4;
                    if n < 0 {
                        values.push(None);
                    } else {
                        values.push(Some(text(&body[at..at + n as usize])));
                        at += n as usize;
                    }
                }
                Msg::Row(values)
            }
            b'E' => {
                let (mut code, mut message) = (String::new(), String::new());
                for field in body.split(|b| *b == 0).filter(|f| !f.is_empty()) {
                    match field[0] {
                        b'C' => code = text(&field[1..]),
                        b'M' => message = text(&field[1..]),
                        _ => {}
                    }
                }
                Msg::Error { code, message }
            }
            other => Msg::Other(other),
        }
    }

    /// Messages up to and including ReadyForQuery, without notices,
    /// ParameterStatus or the startup's authentication and key data.
    pub async fn until_ready(&mut self) -> Vec<Msg> {
        tokio::time::timeout(Duration::from_secs(15), async {
            let mut messages = Vec::new();
            loop {
                let next = self.read().await;
                let done = matches!(next, Msg::Ready(_));
                if !matches!(next, Msg::Other(b'S' | b'N' | b'K' | b'R')) {
                    messages.push(next);
                }
                if done {
                    return messages;
                }
            }
        })
        .await
        .expect("no ReadyForQuery")
    }

    async fn send(&mut self, bytes: &[u8]) {
        self.stream.write_all(bytes).await.unwrap();
        self.stream.flush().await.unwrap();
    }

    /// Sends `bytes` as they are and returns every byte the server answered,
    /// up to and including ReadyForQuery (GD-0061 R3-310 recorded exchanges).
    pub async fn exchange_raw(&mut self, bytes: &[u8]) -> Vec<u8> {
        self.send(bytes).await;
        tokio::time::timeout(Duration::from_secs(15), async {
            let mut answer = Vec::new();
            loop {
                let tag = self.stream.read_u8().await.unwrap();
                let length = self.stream.read_u32().await.unwrap();
                let mut body = vec![0; length as usize - 4];
                self.stream.read_exact(&mut body).await.unwrap();
                answer.push(tag);
                answer.extend(length.to_be_bytes());
                answer.extend(body);
                if tag == b'Z' {
                    return answer;
                }
            }
        })
        .await
        .expect("no ReadyForQuery")
    }

    /// A simple Query.
    pub async fn simple(&mut self, sql: &str) -> Vec<Msg> {
        let mut body = Vec::new();
        cstr(sql, &mut body);
        let mut out = Vec::new();
        message(b'Q', &body, &mut out);
        self.send(&out).await;
        self.until_ready().await
    }

    /// Parse, Bind (text parameters), Describe portal, Execute with
    /// `max_rows`, Sync, all on the unnamed statement and portal.
    pub async fn extended(
        &mut self,
        sql: &str,
        params: &[Option<&str>],
        max_rows: u32,
    ) -> Vec<Msg> {
        let mut out = Vec::new();
        let mut body = Vec::new();
        cstr("", &mut body);
        cstr(sql, &mut body);
        body.extend(0u16.to_be_bytes());
        message(b'P', &body, &mut out);
        let mut body = Vec::new();
        cstr("", &mut body);
        cstr("", &mut body);
        body.extend(0u16.to_be_bytes());
        body.extend((params.len() as u16).to_be_bytes());
        for param in params {
            match param {
                None => body.extend((-1i32).to_be_bytes()),
                Some(value) => {
                    body.extend((value.len() as i32).to_be_bytes());
                    body.extend_from_slice(value.as_bytes());
                }
            }
        }
        body.extend(0u16.to_be_bytes());
        message(b'B', &body, &mut out);
        let mut body = vec![b'P'];
        cstr("", &mut body);
        message(b'D', &body, &mut out);
        let mut body = Vec::new();
        cstr("", &mut body);
        body.extend(max_rows.to_be_bytes());
        message(b'E', &body, &mut out);
        message(b'S', &[], &mut out);
        self.send(&out).await;
        self.until_ready().await
    }

    /// Parse with declared parameter OIDs, Describe statement, Sync.
    pub async fn parse_describe(&mut self, sql: &str, oids: &[u32]) -> Vec<Msg> {
        let mut out = Vec::new();
        let mut body = Vec::new();
        cstr("", &mut body);
        cstr(sql, &mut body);
        body.extend((oids.len() as u16).to_be_bytes());
        for oid in oids {
            body.extend(oid.to_be_bytes());
        }
        message(b'P', &body, &mut out);
        let mut body = vec![b'S'];
        cstr("", &mut body);
        message(b'D', &body, &mut out);
        message(b'S', &[], &mut out);
        self.send(&out).await;
        self.until_ready().await
    }
}

/// The first error in `messages`, as (SQLSTATE, message).
pub fn error_of(messages: &[Msg]) -> Option<(String, String)> {
    messages.iter().find_map(|m| match m {
        Msg::Error { code, message } => Some((code.clone(), message.clone())),
        _ => None,
    })
}

/// The startup parameters of the worked example's agent connection.
pub const AGENT_STARTUP: &[(&str, &str)] = &[
    ("user", "agent_exec"),
    ("database", "giggles"),
    ("giggles.task", "inc-436"),
];

/// A started agent server with task `inc-436` bound to one certificate, an
/// ordinary certificate bound to `oncall_lead`, and a raw session on the agent
/// listener.
pub async fn agent_wire() -> (AgentServer, TestCert, TestCert, Wire) {
    let admin = admin();
    create_task(&admin, "inc-436", "agent_exec");
    let ca = certificate(None, false);
    let agent = certificate(Some(&ca), false);
    let ordinary = certificate(Some(&ca), false);
    let server = AgentServer::start(
        admin,
        ca,
        vec![giggles_server::Binding {
            fingerprint: ordinary.fingerprint(),
            role: giggles_server::Role::Database {
                name: "oncall_lead".into(),
            },
        }],
        vec![binding(&agent, "agent_exec", &["inc-436"])],
    )
    .await;
    let wire = connect_raw(server.agent.address, &server.ca, &agent, AGENT_STARTUP).await;
    (server, agent, ordinary, wire)
}

/// A raw session for `ordinary` on a certificate PostgreSQL listener of the
/// same service: the ordinary map, no task.
pub async fn ordinary_wire(
    server: &AgentServer,
    ordinary: &TestCert,
) -> (giggles_server::Running, Wire) {
    let (_open, gate) = watch::channel(true);
    let listener = giggles_server::serve_pg_bound(
        TcpListener::bind("127.0.0.1:0").await.unwrap(),
        server.service.clone(),
        server.tls.clone(),
        8,
        gate,
    )
    .await
    .unwrap();
    let wire = connect_raw(
        listener.address,
        &server.ca,
        ordinary,
        &[("user", "giggles"), ("database", "giggles")],
    )
    .await;
    (listener, wire)
}

/// The parity harness (GD-0061 R3-1001): one worked-example server with every
/// listener up, and one request run on four agent surfaces (pgwire simple,
/// pgwire extended, HTTP JSON and gRPC), each answer reduced to [`Observed`].
pub mod parity {
    use super::{
        Msg, TestCert, Wire, binding, certificate, connect_raw, now_ms, scratch, timestamptz,
    };
    use bytes::Bytes;
    use giggles_core::{
        Admin, Engine, Limits,
        relational::Value as SqlValue,
        roles::{NewRole, TablePrivilege},
        task::{
            Budget, DenyRule, StatementClass, TaskError,
            definition::WallClockMs,
            statements::{TaskStatement, parse},
        },
    };
    use giggles_server::{
        Binding, Role, Running, Service,
        admission::ServiceLimits,
        agent::serve_agent_pg_bound,
        api::{
            ApiLimits, ApiRuntime, json as wire_json,
            net::{serve_grpc_bound, serve_http_bound},
            proto,
        },
        auth::{PostgresqlConfig, TlsMode, scram::Verifier, serve_postgresql},
        clock::Clock,
        serve_bound, serve_pg_bound, tls,
    };
    use http_body_util::{BodyExt, Full};
    use hyper_util::rt::TokioIo;
    use rustls::{
        ServerConfig,
        pki_types::{PrivatePkcs8KeyDer, ServerName},
    };
    use serde_json::{Value, json};
    use std::{collections::BTreeMap, net::SocketAddr, sync::Arc, time::Duration};
    use tokio::{
        net::{TcpListener, TcpStream},
        sync::watch,
    };
    use tokio_postgres::{Client, NoTls, config::SslMode};
    use tokio_rustls::TlsConnector;
    use tonic::transport::{Certificate as CaCertificate, Channel, ClientTlsConfig, Identity};

    pub const TASK: &str = "inc-436";
    pub const TABLE: &str = "shipments";
    pub const CONTROLLER: &str = "incident_controller";
    pub const EXECUTOR: &str = "repair_agent";
    pub const APPROVER: &str = "oncall_lead";
    pub const APP: &str = "delivery_app";
    pub const CONTROLLER_PW: &str = "controller-pw";
    pub const EXECUTOR_PW: &str = "agent-pw";
    pub const APPROVER_PW: &str = "lead-pw";
    pub const APP_PW: &str = "app-pw";
    /// The native text language version the cases are written in.
    pub const LANGUAGE: &str = "0.16";
    /// The spec's incident window (section 3, allowed write old row).
    pub const INCIDENT_START: &str = "2026-11-05 17:00:00+00";
    pub const INCIDENT_END: &str = "2026-11-05 17:07:00+00";
    const INCIDENT_START_MS: i64 = 1_793_898_000_000;
    /// Merchant 42 has failed rows inside the window (ids 1..=31); merchant
    /// 43 has failed rows (ids 1..=40) outside the task's visible rows.
    pub const MERCHANT_42_ROWS: usize = 31;
    pub const MERCHANT_43_ROWS: usize = 40;

    /// The worked-example schema (`deliveries` is reserved, so `shipments`).
    const SCHEMA: &str = "CREATE TABLE shipments (merchant_id bigint, id bigint, status text, \
         failed_at timestamptz, attempt_count int, customer_email text, card_token text, \
         PRIMARY KEY (merchant_id, id))";

    /// Spec section 3's values as a CREATE TASK statement.
    pub fn inc_436_sql(expires: &str) -> String {
        format!(
            "CREATE TASK \"{TASK}\" \
               EXECUTOR {EXECUTOR} APPROVER {APPROVER} \
               ON {TABLE} \
               VISIBLE ROWS WHERE (merchant_id = 42) \
               READABLE (id, status, failed_at, attempt_count) \
               MASKED (customer_email) \
               ALLOW UPDATE (status) \
                 OLD ROW WHERE (status = 'failed' AND failed_at >= '{INCIDENT_START}' \
                                AND failed_at < '{INCIDENT_END}') \
                 NEW ROW WHERE (status = 'queued') \
                 AT MOST 25 DISTINCT ROWS AND 25 TRANSITIONS \
                 APPROVE ABOVE 5 \
               DISCLOSE AT MOST 1000 ROWS READ, 2097152 BYTES RETURNED \
               WITH (statement_ms = 2000, rows_scanned = 100000, concurrent_statements = 2) \
               EXPIRES '{expires}'"
        )
    }

    /// The next 17:30Z at least an hour after `now` (spec section 3).
    pub fn next_1730z(now: i64) -> i64 {
        const DAY: i64 = 86_400_000;
        let at = now.div_euclid(DAY) * DAY + 17 * 3_600_000 + 30 * 60_000;
        if at < now + 3_600_000 { at + DAY } else { at }
    }

    fn seed() -> String {
        let mut rows = Vec::new();
        for i in 0..MERCHANT_42_ROWS {
            let id = i as i64 + 1;
            rows.push(format!(
                "(42, {id}, 'failed', '{}', {}, 'customer{id}@example.com', 'tok_42_{id}')",
                timestamptz(INCIDENT_START_MS + i as i64 * 10_000),
                1 + i % 3
            ));
        }
        for i in 0..MERCHANT_43_ROWS {
            let id = i as i64 + 1;
            rows.push(format!(
                "(43, {id}, 'failed', '{}', 1, 'buyer{id}@example.com', 'tok_43_{id}')",
                timestamptz(INCIDENT_START_MS + (i % MERCHANT_42_ROWS) as i64 * 10_000)
            ));
        }
        format!("INSERT INTO {TABLE} VALUES {}", rows.join(", "))
    }

    /// The four agent surfaces of one request.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum Surface {
        PgSimple,
        PgExtended,
        HttpText,
        GrpcText,
    }

    impl Surface {
        pub const ALL: [Surface; 4] = [
            Surface::PgSimple,
            Surface::PgExtended,
            Surface::HttpText,
            Surface::GrpcText,
        ];
    }

    /// The native form of a case: native text, or a whole request body for
    /// the forms native text has no words for (such as `taskOutcome`).
    #[derive(Clone, Debug)]
    pub enum Native {
        Text(String),
        Request(Value),
    }

    /// One request in its SQL form and its native form. `operation_id` is
    /// sent as `operationId` on HTTP and gRPC; the SQL form carries its ID in
    /// its own text (`OPERATION '<id>' ...`, `SHOW OPERATION '<id>'`).
    #[derive(Clone, Debug)]
    pub struct Case {
        pub sql: String,
        pub native: Native,
        pub operation_id: Option<String>,
    }

    impl Case {
        /// The same request as SQL and as native text, with no operation ID.
        pub fn new(sql: impl Into<String>, text: impl Into<String>) -> Self {
            Self {
                sql: sql.into(),
                native: Native::Text(text.into()),
                operation_id: None,
            }
        }

        /// A protected write under `op`: `OPERATION '<op>' <sql>`, and the
        /// native text with `operationId`.
        pub fn write(op: &str, sql: &str, text: impl Into<String>) -> Self {
            Self {
                sql: format!("OPERATION '{op}' {sql}"),
                native: Native::Text(text.into()),
                operation_id: Some(op.into()),
            }
        }

        /// The outcome lookup of `op`: `SHOW OPERATION '<op>'` and
        /// `taskOutcome`.
        pub fn outcome(op: &str) -> Self {
            Self {
                sql: format!("SHOW OPERATION '{op}'"),
                native: Native::Request(json!({"taskOutcome": {}})),
                operation_id: Some(op.into()),
            }
        }

        /// The HTTP JSON body of the native form. gRPC sends the same
        /// request, converted by the server's own JSON decoder.
        pub fn native_body(&self) -> Value {
            let mut body = match &self.native {
                Native::Text(text) => {
                    json!({"text": {"text": text, "languageVersion": LANGUAGE}})
                }
                Native::Request(request) => request.clone(),
            };
            if let Some(op) = &self.operation_id {
                body["operationId"] = json!(op);
            }
            body
        }
    }

    /// What one surface answered, reduced to what every surface can say.
    ///
    /// - `sqlstate`: `None` on success.
    /// - `task_error`, `message`: set for a task refusal only. pgwire has no
    ///   name field: its name is that of the `TaskError` whose SQLSTATE and
    ///   fixed message the ErrorResponse carries, and its message is kept
    ///   only then (native sends `message` for task refusals only).
    /// - `rows`: values as PostgreSQL text (`None` is NULL).
    /// - `affected`: the row count of a write that returns no columns
    ///   (`UPDATE n`); `None` for any result with columns (a read, SHOW or
    ///   RETURNING, whose rows say it) and for errors. Native fills its
    ///   `affected` for row results too, so only this reading is common.
    /// - `transport`: an HTTP status other than 200/201, or a gRPC status
    ///   other than OK (pgwire reports through SQLSTATE instead).
    #[derive(Clone, Debug, Default, PartialEq, Eq)]
    pub struct Observed {
        pub sqlstate: Option<String>,
        pub task_error: Option<String>,
        pub message: Option<String>,
        pub columns: Vec<String>,
        pub rows: Vec<Vec<Option<String>>>,
        pub affected: Option<u64>,
        pub transport: Option<String>,
    }

    /// Every named task error, for reading a pgwire ErrorResponse back.
    fn task_errors() -> Vec<TaskError> {
        let mut all = vec![
            TaskError::ApprovalRequired,
            TaskError::ApprovalStale,
            TaskError::Revoked,
            TaskError::Expired,
            TaskError::OperationConflict,
            TaskError::OperationIdRequired,
            TaskError::Retry,
        ];
        all.extend(DenyRule::ALL.map(TaskError::Denied));
        all.extend(Budget::ALL.map(TaskError::BudgetExceeded));
        all.extend(StatementClass::ALL.map(TaskError::NotAllowedInTaskSession));
        all
    }

    /// The task error name a SQLSTATE and message identify, if any.
    pub fn task_error_name(sqlstate: &str, message: &str) -> Option<&'static str> {
        task_errors()
            .into_iter()
            .find(|e| e.sqlstate() == sqlstate && e.message() == message)
            .and_then(|e| e.name())
    }

    /// The row count of a write's CommandComplete tag.
    fn tag_count(tag: &str) -> Option<u64> {
        let mut words = tag.split_whitespace();
        match words.next()? {
            "UPDATE" | "DELETE" | "INSERT" | "MERGE" => words.last()?.parse().ok(),
            _ => None,
        }
    }

    /// A pgwire exchange as [`Observed`].
    pub fn observe_pg(messages: &[Msg]) -> Observed {
        let mut observed = Observed::default();
        for message in messages {
            match message {
                Msg::Columns(names) => observed.columns = names.clone(),
                Msg::Row(values) => observed.rows.push(values.clone()),
                Msg::Complete(tag) => observed.affected = tag_count(tag),
                Msg::Error { code, message } if observed.sqlstate.is_none() => {
                    observed.sqlstate = Some(code.clone());
                    if let Some(name) = task_error_name(code, message) {
                        observed.task_error = Some(name.into());
                        observed.message = Some(message.clone());
                    }
                }
                _ => {}
            }
        }
        if observed.sqlstate.is_some() || !observed.columns.is_empty() {
            observed.affected = None;
        }
        observed
    }

    /// A native value as PostgreSQL text.
    fn native_text(value: &Value) -> Option<String> {
        let (kind, inner) = value
            .as_object()
            .and_then(|o| o.iter().next())
            .unwrap_or_else(|| panic!("native value {value}"));
        match kind.as_str() {
            "nullValue" => None,
            "boolValue" => Some(if inner.as_bool().unwrap() { "t" } else { "f" }.into()),
            "intValue" | "textValue" | "numericValue" => Some(
                inner
                    .as_str()
                    .map_or_else(|| inner.to_string(), str::to_owned),
            ),
            "bytesValue" => {
                use base64::{Engine as _, engine::general_purpose::STANDARD};
                let bytes = STANDARD.decode(inner.as_str().unwrap()).unwrap();
                Some(format!(
                    "\\x{}",
                    bytes.iter().map(|b| format!("{b:02x}")).collect::<String>()
                ))
            }
            other => panic!("the harness does not render native {other} yet: {value}"),
        }
    }

    /// A native reply (HTTP JSON, or gRPC shown as JSON) as [`Observed`].
    pub fn observe_native(reply: &Value) -> Observed {
        let code = reply["code"].as_str().unwrap_or_else(|| panic!("{reply}"));
        let text = |key: &str| reply.get(key).and_then(Value::as_str).map(str::to_owned);
        let mut observed = Observed {
            sqlstate: (code != "ok").then(|| code.to_owned()),
            task_error: text("taskError"),
            message: text("message"),
            ..Observed::default()
        };
        if observed.sqlstate.is_some() {
            return observed;
        }
        let Some(result) = reply.get("sql").or_else(|| reply.get("operationOutcome")) else {
            return observed;
        };
        observed.columns = result["columns"]
            .as_array()
            .map(|c| c.iter().map(|c| c.as_str().unwrap().to_owned()).collect())
            .unwrap_or_default();
        observed.rows = result["rows"]
            .as_array()
            .map(|rows| {
                rows.iter()
                    .map(|row| {
                        row["values"]
                            .as_array()
                            .map(|v| v.iter().map(native_text).collect())
                            .unwrap_or_default()
                    })
                    .collect()
            })
            .unwrap_or_default();
        // proto3 JSON leaves out a zero count.
        if observed.columns.is_empty() {
            observed.affected = Some(result.get("affected").map_or(0, |n| {
                n.as_str()
                    .map_or_else(|| n.as_u64(), |n| n.parse().ok())
                    .unwrap_or_else(|| panic!("affected {n}"))
            }));
        }
        observed
    }

    /// One HTTP/1.1 call over mTLS: the status and the JSON body (`Null`
    /// when there is none).
    pub async fn http_call(
        address: SocketAddr,
        (ca, cert): (&TestCert, &TestCert),
        method: &str,
        path: &str,
        token: Option<&str>,
        body: Option<&Value>,
    ) -> (u16, Value) {
        tokio::time::timeout(Duration::from_secs(15), async {
            let config = tls::client(
                vec![cert.cert.der().clone()],
                PrivatePkcs8KeyDer::from(cert.key.serialize_der()).into(),
                vec![ca.cert.der().clone()],
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
                .method(method)
                .uri(format!("https://localhost:{}{path}", address.port()))
                .header("host", "localhost");
            if body.is_some() {
                request = request.header("content-type", "application/json");
            }
            if let Some(token) = token {
                request = request.header("x-giggles-session", token);
            }
            let bytes = body.map_or_else(Vec::new, |b| serde_json::to_vec(b).unwrap());
            let request = request.body(Full::new(Bytes::from(bytes))).unwrap();
            let (mut sender, connection) =
                hyper::client::conn::http1::handshake(TokioIo::new(stream))
                    .await
                    .unwrap();
            let task = tokio::spawn(async move {
                let _ = connection.await;
            });
            let response = sender.send_request(request).await.unwrap();
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

    /// A gRPC client over mTLS presenting `cert`.
    pub async fn grpc_client(
        address: SocketAddr,
        ca: &TestCert,
        cert: &TestCert,
    ) -> proto::giggles_client::GigglesClient<Channel> {
        let channel = Channel::from_shared(format!("https://127.0.0.1:{}", address.port()))
            .unwrap()
            .tls_config(
                ClientTlsConfig::new()
                    .domain_name("localhost")
                    .ca_certificate(CaCertificate::from_pem(ca.cert.pem()))
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

    /// A gRPC request carrying the session token.
    pub fn with_session<T>(token: &str, message: T) -> tonic::Request<T> {
        let mut request = tonic::Request::new(message);
        request
            .metadata_mut()
            .insert("x-giggles-session", token.parse().unwrap());
        request
    }

    /// A transport status as [`Observed`].
    fn refused(transport: String) -> Observed {
        Observed {
            transport: Some(transport),
            ..Observed::default()
        }
    }

    /// `user` with `password` on a PostgreSQL password listener.
    pub async fn connect_password(
        address: SocketAddr,
        user: &str,
        password: &str,
    ) -> Result<Client, tokio_postgres::Error> {
        let (client, connection) = tokio_postgres::Config::new()
            .host("localhost")
            .hostaddr(address.ip())
            .port(address.port())
            .user(user)
            .password(password)
            .dbname("postgres")
            .ssl_mode(SslMode::Disable)
            .connect_timeout(Duration::from_secs(5))
            .connect(NoTls)
            .await?;
        tokio::spawn(connection);
        Ok(client)
    }

    /// The startup parameters of `repair_agent`'s task session.
    pub const AGENT_STARTUP: &[(&str, &str)] = &[
        ("user", EXECUTOR),
        ("database", "giggles"),
        ("giggles.task", TASK),
    ];

    /// The parity fixture: a v2 database with the worked example, and every
    /// listener of one service.
    ///
    /// - `postgresql`: the PostgreSQL password listener (SCRAM, no TLS) for
    ///   `incident_controller`, `oncall_lead` and `delivery_app`;
    /// - `legacy_pg`, `native`, `http`, `grpc`: the certificate listeners,
    ///   where `app` is bound to `delivery_app`;
    /// - `agent_pg`, `agent_http`, `agent_grpc`: the agent listeners, where
    ///   `agent` is bound to `repair_agent` for `inc-436`.
    pub struct ParityFixture {
        pub admin: Admin,
        pub service: Arc<Service>,
        pub ca: TestCert,
        pub server_cert: TestCert,
        pub agent: TestCert,
        pub app: TestCert,
        pub tls: Arc<ServerConfig>,
        pub ordinary_api: Arc<ApiRuntime>,
        pub agent_api: Arc<ApiRuntime>,
        pub postgresql: Running,
        pub legacy_pg: Running,
        pub native: Running,
        pub http: Running,
        pub grpc: Running,
        pub agent_pg: Running,
        pub agent_http: Running,
        pub agent_grpc: Running,
        /// `inc-436`'s expiry, Unix ms.
        pub expires_ms: i64,
    }

    impl ParityFixture {
        /// The database, roles and rows, every listener, then `inc-436`.
        pub async fn start() -> Self {
            let path = scratch("agent-parity").join("db");
            let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
            let mut session = admin.sql_session();
            session.execute(SCHEMA, &[], None).unwrap();
            session.execute(&seed(), &[], None).unwrap();
            drop(session);
            let roles = admin.roles();
            for (name, task_only, password) in [
                (CONTROLLER, false, CONTROLLER_PW),
                (APPROVER, false, APPROVER_PW),
                (APP, false, APP_PW),
                (EXECUTOR, true, EXECUTOR_PW),
            ] {
                roles
                    .create(NewRole {
                        name: name.into(),
                        login: true,
                        password: Some(Verifier::generate(password)),
                        ..NewRole::default()
                    })
                    .unwrap();
                if task_only {
                    roles.set_task_only(name, true).unwrap();
                }
            }
            use TablePrivilege::{Insert, Select, Update};
            for (name, privileges) in [
                (CONTROLLER, &[Select, Update][..]),
                // Spec section 14: SELECT and UPDATE (status).
                (EXECUTOR, &[Select, Update][..]),
                (APPROVER, &[Select, Update][..]),
                (APP, &[Select, Insert, Update][..]),
            ] {
                roles.grant_table(TABLE, name, privileges).unwrap();
            }

            let ca = certificate(None, false);
            let server_cert = certificate(Some(&ca), true);
            let agent = certificate(Some(&ca), false);
            let app = certificate(Some(&ca), false);
            let tls = tls::server(
                vec![server_cert.cert.der().clone()],
                PrivatePkcs8KeyDer::from(server_cert.key.serialize_der()).into(),
                vec![ca.cert.der().clone()],
            )
            .unwrap();
            let service = Service::with_agent(
                admin.clone(),
                vec![Binding {
                    fingerprint: app.fingerprint(),
                    role: Role::Database { name: APP.into() },
                }],
                vec![binding(&agent, EXECUTOR, &[TASK])],
                Clock::new(0, 3600).unwrap(),
                ServiceLimits::default(),
            )
            .unwrap();

            let config = PostgresqlConfig {
                listen: "127.0.0.1:0".parse().unwrap(),
                tls: TlsMode::Disabled,
                certificate: None,
                private_key: None,
                client_ca: None,
                certificates: BTreeMap::new(),
                databases: vec!["postgres".into()],
                loopback_trust: false,
                connections: 32,
            };
            let listener = config.build().unwrap();
            let postgresql = serve_postgresql(config.listen, service.clone(), Arc::new(listener))
                .await
                .unwrap();
            let (_open, gate) = watch::channel(true);
            let bind = || async { TcpListener::bind("127.0.0.1:0").await.unwrap() };
            let legacy_pg =
                serve_pg_bound(bind().await, service.clone(), tls.clone(), 16, gate.clone())
                    .await
                    .unwrap();
            let native = serve_bound(bind().await, service.clone(), tls.clone(), 16, gate.clone())
                .await
                .unwrap();
            let ordinary_api = ApiRuntime::new(service.clone(), ApiLimits::default()).unwrap();
            let agent_api = ApiRuntime::new_agent(service.clone(), ApiLimits::default()).unwrap();
            let http = serve_http_bound(
                bind().await,
                ordinary_api.clone(),
                tls.clone(),
                16,
                gate.clone(),
            )
            .await
            .unwrap();
            let grpc = serve_grpc_bound(
                bind().await,
                ordinary_api.clone(),
                tls.clone(),
                16,
                gate.clone(),
            )
            .await
            .unwrap();
            let agent_pg = serve_agent_pg_bound(
                bind().await,
                service.clone(),
                tls.clone(),
                16,
                vec!["giggles".into()],
                gate.clone(),
            )
            .await
            .unwrap();
            let agent_http = serve_http_bound(
                bind().await,
                agent_api.clone(),
                tls.clone(),
                16,
                gate.clone(),
            )
            .await
            .unwrap();
            let agent_grpc =
                serve_grpc_bound(bind().await, agent_api.clone(), tls.clone(), 16, gate)
                    .await
                    .unwrap();
            let fixture = Self {
                admin,
                service,
                ca,
                server_cert,
                agent,
                app,
                tls,
                ordinary_api,
                agent_api,
                postgresql,
                legacy_pg,
                native,
                http,
                grpc,
                agent_pg,
                agent_http,
                agent_grpc,
                expires_ms: next_1730z(now_ms()),
            };
            fixture.create_inc_436();
            fixture
        }

        /// `CREATE TASK "inc-436"` as `incident_controller`. The PostgreSQL
        /// listener has no route for CREATE TASK yet, so the statement goes
        /// through core's task parser and `Admin::tasks()`, as R3-901 does.
        pub fn create_inc_436(&self) {
            let sql = inc_436_sql(&timestamptz(self.expires_ms));
            let Some(TaskStatement::Create(new)) = parse(&sql) else {
                panic!("CREATE TASK did not parse: {sql}");
            };
            let issuer = self.admin.roles().get(CONTROLLER).unwrap().unwrap();
            self.admin
                .tasks()
                .create(&issuer, *new, WallClockMs(now_ms()))
                .unwrap();
        }

        pub async fn stop(self) {
            for listener in [
                self.postgresql,
                self.legacy_pg,
                self.native,
                self.http,
                self.grpc,
                self.agent_pg,
                self.agent_http,
                self.agent_grpc,
            ] {
                listener.shutdown().await.unwrap();
            }
            self.ordinary_api.shutdown().await;
            self.agent_api.shutdown().await;
        }

        /// Runs `sql` as the administrator and returns its rows.
        pub fn sql(&self, sql: &str) -> Vec<Vec<SqlValue>> {
            self.admin
                .sql_session()
                .execute(sql, &[], None)
                .unwrap()
                .rows
                .into_iter()
                .map(|row| row.values)
                .collect()
        }

        /// `user` with `password` on the PostgreSQL password listener.
        pub async fn password(
            &self,
            user: &str,
            password: &str,
        ) -> Result<Client, tokio_postgres::Error> {
            connect_password(self.postgresql.address, user, password).await
        }

        /// A raw task session on the agent pgwire listener.
        pub async fn agent_wire(&self) -> Wire {
            connect_raw(self.agent_pg.address, &self.ca, &self.agent, AGENT_STARTUP).await
        }

        /// A raw session for `app` on the legacy certificate pgwire listener.
        pub async fn legacy_wire(&self) -> Wire {
            connect_raw(
                self.legacy_pg.address,
                &self.ca,
                &self.app,
                &[("user", "giggles"), ("database", "giggles")],
            )
            .await
        }

        /// Opens a task session on the agent HTTP listener: its token, or
        /// the refusal's transport status (`http 401`).
        pub async fn open_http(&self) -> Result<String, String> {
            let (status, reply) = http_call(
                self.agent_http.address,
                (&self.ca, &self.agent),
                "POST",
                "/v1/sessions",
                None,
                Some(&json!({"task": TASK})),
            )
            .await;
            match status {
                201 => Ok(reply["sessionId"].as_str().unwrap().to_owned()),
                other => Err(format!("http {other}")),
            }
        }

        /// Opens a task session on the agent gRPC listener: its token, or the
        /// refusal's transport status (`grpc Unauthenticated`).
        pub async fn open_grpc(&self) -> Result<String, String> {
            let opened = grpc_client(self.agent_grpc.address, &self.ca, &self.agent)
                .await
                .open_session(proto::OpenSessionRequest {
                    task: Some(TASK.into()),
                    ..Default::default()
                })
                .await;
            match opened {
                Ok(reply) => Ok(reply.into_inner().session_id),
                Err(status) => Err(format!("grpc {:?}", status.code())),
            }
        }

        /// `case` on `surface`, in a new task session that is closed after
        /// the call.
        pub async fn run(&self, surface: Surface, case: &Case) -> Observed {
            match surface {
                Surface::PgSimple => observe_pg(&self.agent_wire().await.simple(&case.sql).await),
                Surface::PgExtended => {
                    observe_pg(&self.agent_wire().await.extended(&case.sql, &[], 0).await)
                }
                Surface::HttpText => {
                    let token = match self.open_http().await {
                        Ok(token) => token,
                        Err(status) => return refused(status),
                    };
                    let peer = (&self.ca, &self.agent);
                    let (status, reply) = http_call(
                        self.agent_http.address,
                        peer,
                        "POST",
                        "/v1/session/execute",
                        Some(&token),
                        Some(&case.native_body()),
                    )
                    .await;
                    let observed = match status {
                        200 => observe_native(&reply),
                        other => refused(format!("http {other}")),
                    };
                    let address = self.agent_http.address;
                    http_call(address, peer, "DELETE", "/v1/session", Some(&token), None).await;
                    observed
                }
                Surface::GrpcText => {
                    let token = match self.open_grpc().await {
                        Ok(token) => token,
                        Err(status) => return refused(status),
                    };
                    let body = serde_json::to_vec(&case.native_body()).unwrap();
                    let request = wire_json::decode_execute(&body)
                        .unwrap_or_else(|error| panic!("{case:?}: {error:?}"));
                    let mut client =
                        grpc_client(self.agent_grpc.address, &self.ca, &self.agent).await;
                    let observed = match client.execute(with_session(&token, request)).await {
                        Ok(reply) => {
                            let json = wire_json::encode_reply(&reply.into_inner()).unwrap();
                            observe_native(&serde_json::from_slice(&json).unwrap())
                        }
                        Err(status) => refused(format!("grpc {:?}", status.code())),
                    };
                    let close = proto::CloseSessionRequest {};
                    let _ = client.close_session(with_session(&token, close)).await;
                    observed
                }
            }
        }

        /// `case` on every surface, in [`Surface::ALL`] order.
        pub async fn run_all(&self, case: &Case) -> Vec<(Surface, Observed)> {
            let mut all = Vec::new();
            for surface in Surface::ALL {
                all.push((surface, self.run(surface, case).await));
            }
            all
        }
    }

    /// Asserts that every surface observed the same, and returns it.
    #[track_caller]
    pub fn agree(observed: &[(Surface, Observed)]) -> Observed {
        let (first, expected) = &observed[0];
        for (surface, seen) in &observed[1..] {
            assert_eq!(seen, expected, "{surface:?} differs from {first:?}");
        }
        expected.clone()
    }
}
