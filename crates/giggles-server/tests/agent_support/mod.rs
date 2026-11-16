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
