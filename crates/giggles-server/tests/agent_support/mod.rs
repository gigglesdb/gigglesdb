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
        let tls = tls::server(
            vec![server.cert.der().clone()],
            PrivatePkcs8KeyDer::from(server.key.serialize_der()).into(),
            vec![ca.cert.der().clone()],
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
