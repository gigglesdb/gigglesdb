#![allow(dead_code)]
use bytes::Bytes;
use giggles_core::{
    Admin, Column, Grant, Key, Limits, Policy, Principal, Row, RowId, Status, TaskId, TenantId,
};
use giggles_server::{
    Binding, Role, Running, Service,
    api::{
        ApiLimits, ApiRuntime,
        net::{serve_grpc_bound, serve_http_bound},
        proto,
    },
    clock::Clock,
    tls,
};
use http_body_util::{BodyExt, Full};
use hyper_util::rt::{TokioExecutor, TokioIo};
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair,
};
use rustls::pki_types::{PrivatePkcs8KeyDer, ServerName};
use std::{
    collections::BTreeSet,
    io,
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::watch,
};
use tokio_rustls::TlsConnector;
use tonic::transport::{Certificate as CaCertificate, Channel, ClientTlsConfig, Identity};

pub struct TestCert {
    pub cert: Certificate,
    pub key: KeyPair,
}
pub fn certificate(ca: Option<&TestCert>, server: bool) -> TestCert {
    let key = KeyPair::generate().unwrap();
    let mut p = CertificateParams::new(vec!["localhost".into()]).unwrap();
    p.distinguished_name.push(
        rcgen::DnType::CommonName,
        if ca.is_none() {
            "Giggles test CA"
        } else if server {
            "localhost"
        } else {
            "Giggles test client"
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
pub fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}
pub struct Fixture {
    pub path: PathBuf,
    pub admin: Admin,
    pub service: Arc<Service>,
    pub runtime: Arc<ApiRuntime>,
    pub ca: TestCert,
    pub server: TestCert,
    pub app: TestCert,
    pub other: TestCert,
    pub task: TestCert,
    pub unbound: TestCert,
    pub bindings: Vec<Binding>,
    pub http: Running,
    pub grpc: Running,
}
impl Fixture {
    pub async fn new() -> Self {
        Self::with_limits(ApiLimits::default()).await
    }
    pub async fn catalog_empty() -> Self {
        Self::catalog_empty_engine(giggles_core::Engine::Legacy).await
    }
    #[allow(dead_code)]
    pub async fn catalog_empty_engine(engine: giggles_core::Engine) -> Self {
        Self::build_engine(ApiLimits::default(), giggles_server::admission::ServiceLimits::default(),
            giggles_core::TaskResources { read_rows: 100000, disclosed_rows: 100000, encoded_bytes: 100000000 }, false, engine).await
    }
    pub async fn with_limits(limits: ApiLimits) -> Self {
        Self::with_service_limits(limits, giggles_server::admission::ServiceLimits::default()).await
    }
    pub async fn with_service_limits(
        limits: ApiLimits,
        service_limits: giggles_server::admission::ServiceLimits,
    ) -> Self {
        Self::build(
            limits,
            service_limits,
            giggles_core::TaskResources {
                read_rows: 100000,
                disclosed_rows: 100000,
                encoded_bytes: 100000000,
            },
            true,
        )
        .await
    }
    pub async fn with_task_resources(resources: giggles_core::TaskResources) -> Self {
        Self::build(
            ApiLimits::default(),
            giggles_server::admission::ServiceLimits::default(),
            resources,
            true,
        )
        .await
    }
    async fn build(
        limits: ApiLimits,
        service_limits: giggles_server::admission::ServiceLimits,
        resources: giggles_core::TaskResources,
        seed_rows: bool,
    ) -> Self {
        Self::build_engine(limits, service_limits, resources, seed_rows, giggles_core::Engine::Legacy).await
    }
    async fn build_engine(
        limits: ApiLimits,
        service_limits: giggles_server::admission::ServiceLimits,
        resources: giggles_core::TaskResources,
        seed_rows: bool,
        engine: giggles_core::Engine,
    ) -> Self {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let path = std::env::var_os("GIGGLES_TEST_ARTIFACTS")
            .map(PathBuf::from).unwrap_or_else(std::env::temp_dir).join(format!(
            "api-fixture-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).unwrap();
        let ca = certificate(None, false);
        let server = certificate(Some(&ca), true);
        let app = certificate(Some(&ca), false);
        let other = certificate(Some(&ca), false);
        let task = certificate(Some(&ca), false);
        let unbound = certificate(Some(&ca), false);
        for (name, c) in [
            ("ca", &ca),
            ("server", &server),
            ("app", &app),
            ("other", &other),
            ("task", &task),
            ("unbound", &unbound),
        ] {
            std::fs::write(path.join(format!("{name}.pem")), c.cert.pem()).unwrap();
            std::fs::write(
                path.join(format!("{name}.key.TEST-ONLY")),
                c.key.serialize_pem(),
            )
            .unwrap();
        }
        let admin = if engine == giggles_core::Engine::Legacy {
            Admin::create(path.join("db.redb"), Limits::default()).unwrap()
        } else {
            Admin::create_engine(path.join("db.redb"), Limits::default(), engine).unwrap()
        };
        let mut sql = admin.sql_session();
        sql.execute(
            "CREATE TABLE app (id BIGINT PRIMARY KEY, body TEXT, flag BOOLEAN, data BYTEA)",
            &[],
            None,
        )
        .unwrap();
        sql.execute("CREATE TABLE other (id BIGINT PRIMARY KEY)", &[], None)
            .unwrap();
        drop(sql);
        if seed_rows {
        admin
            .grant(Grant {
                task: TaskId(1),
                issuer: Principal(1),
                executor: Principal(2),
                approver: Principal(3),
                policy: Policy {
                    tenant: TenantId(42),
                    columns: BTreeSet::from([Column::Status]),
                    transitions: BTreeSet::from([(Status::Failed, Status::Queued)]),
                    incident: (100, 107),
                    distinct_limit: 20,
                    transition_limit: 20,
                    approval_above: 10,
                    valid_from: 1,
                    expires: now() + 7200,
                },
            })
            .unwrap();
        admin
            .provision_task_resources(TaskId(1), resources)
            .unwrap();
        for row in 0..4 {
            admin
                .put(
                    Key {
                        tenant: TenantId(42),
                        row: RowId(row),
                    },
                    Row {
                        status: Status::Failed,
                        failed_at: 103,
                        attempt_count: 0,
                        version: 0,
                    },
                )
                .unwrap();
        }
        }
        let domain = admin.authority_metadata().unwrap().0.to_string();
        let bindings = vec![
            Binding {
                fingerprint: tls::fingerprint(app.cert.der()),
                role: Role::Application {
                    domain: domain.clone(),
                    tables: BTreeSet::from(["app".into()]),
                },
            },
            Binding {
                fingerprint: tls::fingerprint(other.cert.der()),
                role: Role::Application {
                    domain: domain.clone(),
                    tables: BTreeSet::from(["app".into()]),
                },
            },
            Binding {
                fingerprint: tls::fingerprint(task.cert.der()),
                role: Role::Task {
                    domain,
                    task: TaskId(1),
                    principal: Principal(2),
                },
            },
        ];
        let service = Service::with_limits(
            admin.clone(),
            bindings.clone(),
            Clock::new(0, 3600).unwrap(),
            service_limits,
        )
        .unwrap();
        let runtime = ApiRuntime::new(service.clone(), limits).unwrap();
        let config = tls::server(
            vec![server.cert.der().clone()],
            PrivatePkcs8KeyDer::from(server.key.serialize_der()).into(),
            vec![ca.cert.der().clone()],
        )
        .unwrap();
        let (_, gate) = watch::channel(true);
        let http = serve_http_bound(
            TcpListener::bind("127.0.0.1:0").await.unwrap(),
            runtime.clone(),
            config.clone(),
            32,
            gate.clone(),
        )
        .await
        .unwrap();
        let grpc = serve_grpc_bound(
            TcpListener::bind("127.0.0.1:0").await.unwrap(),
            runtime.clone(),
            config,
            32,
            gate,
        )
        .await
        .unwrap();
        println!("retained_fixture={}", path.display());
        Self {
            path,
            admin,
            service,
            runtime,
            ca,
            server,
            app,
            other,
            task,
            unbound,
            bindings,
            http,
            grpc,
        }
    }
    pub fn client_config(&self, c: &TestCert, h2: bool) -> Arc<rustls::ClientConfig> {
        let cfg = tls::client(
            vec![c.cert.der().clone()],
            PrivatePkcs8KeyDer::from(c.key.serialize_der()).into(),
            vec![self.ca.cert.der().clone()],
        )
        .unwrap();
        let mut cfg = (*cfg).clone();
        cfg.alpn_protocols = vec![if h2 {
            b"h2".to_vec()
        } else {
            b"http/1.1".to_vec()
        }];
        Arc::new(cfg)
    }
    pub async fn http(
        &self,
        c: &TestCert,
        method: &str,
        path: &str,
        token: Option<&str>,
        body: Vec<u8>,
        h2: bool,
    ) -> io::Result<(u16, Vec<u8>)> {
        self.http_headers(c, method, path, token, body, h2, &[])
            .await
    }
    #[allow(clippy::too_many_arguments)]
    pub async fn http_headers(
        &self,
        c: &TestCert,
        method: &str,
        path: &str,
        token: Option<&str>,
        body: Vec<u8>,
        h2: bool,
        headers: &[(&str, &str)],
    ) -> io::Result<(u16, Vec<u8>)> {
        tokio::time::timeout(Duration::from_secs(15), async {
            let socket = TcpStream::connect(self.http.address).await?;
            let stream = TlsConnector::from(self.client_config(c, h2))
                .connect(ServerName::try_from("localhost").unwrap(), socket)
                .await?;
            let mut request = hyper::Request::builder()
                .method(method)
                .uri(format!(
                    "https://localhost:{}{path}",
                    self.http.address.port()
                ))
                .header("host", "localhost");
            if !body.is_empty() {
                request = request.header("content-type", "application/json");
            }
            if let Some(token) = token {
                request = request.header("x-giggles-session", token);
            }
            for (k, v) in headers {
                request = request.header(*k, *v);
            }
            let request = request
                .body(Full::new(Bytes::from(body)))
                .map_err(io::Error::other)?;
            let (response, task) = if h2 {
                let (mut sender, connection) = hyper::client::conn::http2::handshake(
                    TokioExecutor::new(),
                    TokioIo::new(stream),
                )
                .await
                .map_err(io::Error::other)?;
                let task = tokio::spawn(async move {
                    let _ = connection.await;
                });
                let response = sender
                    .send_request(request)
                    .await
                    .map_err(io::Error::other)?;
                (response, task)
            } else {
                let (mut sender, connection) =
                    hyper::client::conn::http1::handshake(TokioIo::new(stream))
                        .await
                        .map_err(io::Error::other)?;
                let task = tokio::spawn(async move {
                    let _ = connection.await;
                });
                let response = sender
                    .send_request(request)
                    .await
                    .map_err(io::Error::other)?;
                (response, task)
            };
            let status = response.status().as_u16();
            let data = response
                .into_body()
                .collect()
                .await
                .map_err(io::Error::other)?
                .to_bytes()
                .to_vec();
            task.abort();
            let _ = task.await;
            Ok((status, data))
        })
        .await
        .map_err(io::Error::other)?
    }
    pub async fn open_http(&self, c: &TestCert, h2: bool) -> String {
        let (status, b) = self
            .http(c, "POST", "/v1/sessions", None, b"{}".to_vec(), h2)
            .await
            .unwrap();
        assert_eq!(status, 201, "{}", String::from_utf8_lossy(&b));
        serde_json::from_slice::<serde_json::Value>(&b).unwrap()["sessionId"]
            .as_str()
            .unwrap()
            .to_owned()
    }
    pub async fn exec_http(
        &self,
        c: &TestCert,
        token: &str,
        body: serde_json::Value,
        h2: bool,
    ) -> serde_json::Value {
        let (status, b) = self
            .http(
                c,
                "POST",
                "/v1/session/execute",
                Some(token),
                serde_json::to_vec(&body).unwrap(),
                h2,
            )
            .await
            .unwrap();
        assert_eq!(status, 200, "{}", String::from_utf8_lossy(&b));
        serde_json::from_slice(&b).unwrap()
    }
    pub async fn grpc_client(&self, c: &TestCert) -> proto::giggles_client::GigglesClient<Channel> {
        let channel =
            Channel::from_shared(format!("https://127.0.0.1:{}", self.grpc.address.port()))
                .unwrap()
                .tls_config(
                    ClientTlsConfig::new()
                        .domain_name("localhost")
                        .ca_certificate(CaCertificate::from_pem(self.ca.cert.pem()))
                        .identity(Identity::from_pem(c.cert.pem(), c.key.serialize_pem())),
                )
                .unwrap()
                .connect_timeout(Duration::from_secs(10))
                .connect()
                .await
                .unwrap();
        proto::giggles_client::GigglesClient::new(channel)
            .max_decoding_message_size(giggles_server::MAX_RESPONSE)
    }
    pub async fn stop(self) {
        let (h, g) = tokio::join!(self.http.shutdown(), self.grpc.shutdown());
        h.unwrap();
        g.unwrap();
        self.runtime.shutdown().await;
    }
}
pub fn rpc<T>(token: &str, value: T) -> tonic::Request<T> {
    let mut request = tonic::Request::new(value);
    request
        .metadata_mut()
        .insert("x-giggles-session", token.parse().unwrap());
    request
}
pub fn sql(query: &str) -> proto::ExecuteRequest {
    proto::ExecuteRequest {
        request: Some(proto::execute_request::Request::Sql(proto::SqlRequest {
            sql: query.into(),
            params: vec![],
        })),
        budget_ms: None,
    }
}
