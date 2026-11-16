//! PostgreSQL gateway lifecycle and schema transport.
use crate::{
    config::{Config, UpstreamDialect},
    pgwire::{
        context::{GatewayContext, Metrics},
        upstream::{UpstreamConfig, UpstreamTls},
    },
    policy::{Policy, file::PolicyError},
    schema::Snapshot,
};
use rustls::{
    DigitallySignedStruct, SignatureScheme,
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    pki_types::{CertificateDer, ServerName, UnixTime},
};
use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, Mutex, RwLock},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::AsyncWriteExt,
    sync::{Semaphore, mpsc},
    task::{JoinHandle, JoinSet},
};

// Require intentionally skips certificate trust and hostname validation, but verifies
// handshake signatures with the configured crypto provider.
#[derive(Debug)]
struct RequireVerifier;
impl ServerCertVerifier for RequireVerifier {
    fn verify_server_cert(
        &self,
        _: &CertificateDer<'_>,
        _: &[CertificateDer<'_>],
        _: &ServerName<'_>,
        _: &[u8],
        _: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &rustls::crypto::ring::default_provider().signature_verification_algorithms,
        )
    }
    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &rustls::crypto::ring::default_provider().signature_verification_algorithms,
        )
    }
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

struct AbortTask<T>(JoinHandle<T>);
impl<T> Drop for AbortTask<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}
#[derive(Clone)]
pub(crate) struct ReloadableLog(pub(crate) Arc<RwLock<Box<dyn crate::readlog::ReadLog>>>);
impl crate::readlog::ReadLog for ReloadableLog {
    fn write(&self, record: &crate::readlog::ReadLogRecord) -> Result<(), crate::error::Refusal> {
        self.0
            .read()
            .map_err(|_| {
                crate::error::Refusal::new(
                    crate::error::NyCode::ReadLogUnavailable,
                    "read log unavailable",
                )
            })?
            .write(record)
    }
}
/// Owned schema connection driver, aborted on every exit path.
pub(crate) struct SchemaClient {
    pub(crate) client: tokio_postgres::Client,
    driver: JoinHandle<()>,
}
impl Drop for SchemaClient {
    fn drop(&mut self) {
        self.driver.abort();
    }
}
/// Open the exclusive schema connection using the configured TLS policy.
pub(crate) async fn connect_schema(cfg: &UpstreamConfig) -> Result<SchemaClient, String> {
    tokio::time::timeout(
        Duration::from_millis(cfg.connect_timeout_ms),
        connect_schema_inner(cfg),
    )
    .await
    .map_err(|_| "upstream schema connection timed out".to_owned())?
}
async fn connect_schema_inner(cfg: &UpstreamConfig) -> Result<SchemaClient, String> {
    let mut pg = tokio_postgres::Config::new();
    pg.host(&cfg.host)
        .port(cfg.port)
        .user(&cfg.user)
        .password(cfg.password.as_bytes())
        .dbname(&cfg.dbname)
        .application_name(&cfg.application_name)
        .connect_timeout(Duration::from_millis(cfg.connect_timeout_ms));
    let fail = || "upstream schema connection failed".to_owned();
    if matches!(cfg.tls, UpstreamTls::Disable) {
        pg.ssl_mode(tokio_postgres::config::SslMode::Disable);
        let (client, connection) = pg
            .connect(tokio_postgres::NoTls)
            .await
            .map_err(|_| fail())?;
        return Ok(SchemaClient {
            client,
            driver: tokio::spawn(async move {
                let _ = connection.await;
            }),
        });
    }
    pg.ssl_mode(tokio_postgres::config::SslMode::Require);
    let builder = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|_| fail())?;
    let tls = match &cfg.tls {
        UpstreamTls::Require => builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(RequireVerifier))
            .with_no_client_auth(),
        UpstreamTls::VerifyFull { ca_file } => {
            let pem = std::fs::read(ca_file).map_err(|_| fail())?;
            let mut roots = rustls::RootCertStore::empty();
            for cert in rustls_pemfile::certs(&mut pem.as_slice()) {
                roots.add(cert.map_err(|_| fail())?).map_err(|_| fail())?;
            }
            if roots.is_empty() {
                return Err(fail());
            }
            builder.with_root_certificates(roots).with_no_client_auth()
        }
        UpstreamTls::Disable => return Err(fail()),
    };
    let (client, connection) = pg
        .connect(tokio_postgres_rustls::MakeRustlsConnect::new(tls))
        .await
        .map_err(|_| fail())?;
    Ok(SchemaClient {
        client,
        driver: tokio::spawn(async move {
            let _ = connection.await;
        }),
    })
}
pub(crate) fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|d| u64::try_from(d.as_millis()).ok())
        .unwrap_or(u64::MAX)
}
/// Load the configured catalog dialect on an exclusive client.
pub(crate) async fn snapshot(
    client: &tokio_postgres::Client,
    cfg: &Config,
) -> Result<Snapshot, String> {
    let result = match cfg.dialect {
        UpstreamDialect::Postgres => {
            crate::schema::pg::load_pg_snapshot(
                client,
                &crate::schema::pg::PgSchemaOptions {
                    schemas: cfg.schemas.clone(),
                    search_path: cfg.search_path.clone(),
                },
                now(),
            )
            .await
        }
        UpstreamDialect::SpannerPg => {
            crate::schema::spanner_pg::load_spanner_pg_snapshot(
                client,
                &cfg.schemas,
                &cfg.search_path,
                now(),
            )
            .await
        }
    };
    result.map_err(|_| "upstream schema snapshot unavailable".into())
}
/// Parse policy bytes and resolve its audit path relative to the policy file.
pub(crate) fn parse_policy_bytes(path: &Path, bytes: &[u8]) -> Result<Policy, Vec<PolicyError>> {
    let text = std::str::from_utf8(bytes).map_err(|_| {
        vec![PolicyError {
            message: "policy file is not UTF-8".into(),
        }]
    })?;
    let mut policy = crate::policy::file::parse_policy(text)?;
    if let crate::policy::ReadLogSink::File { path: log, .. } = &mut policy.readlog
        && log.is_relative()
    {
        *log = path.parent().unwrap_or(Path::new(".")).join(&*log);
    }
    Ok(policy)
}
#[cfg(test)]
fn reload_decision(
    candidate: Result<Policy, Vec<PolicyError>>,
    snapshot: &Snapshot,
) -> Result<Policy, Vec<PolicyError>> {
    let policy = candidate?;
    crate::policy::validate::validate(&policy, snapshot)?;
    Ok(policy)
}
pub(crate) fn errors(errors: Vec<PolicyError>) -> String {
    errors
        .into_iter()
        .map(|e| e.to_string().replace(['\r', '\n'], " "))
        .collect::<Vec<_>>()
        .join("; ")
}
#[cfg(unix)]
async fn terminate() {
    if let Ok(mut signal) =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
    {
        signal.recv().await;
    } else {
        std::future::pending::<()>().await;
    }
}
#[cfg(not(unix))]
async fn terminate() {
    std::future::pending::<()>().await;
}
#[cfg(unix)]
pub(crate) async fn hangup() {
    if let Ok(mut signal) = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup()) {
        signal.recv().await;
    } else {
        std::future::pending::<()>().await;
    }
}
#[cfg(not(unix))]
pub(crate) async fn hangup() {
    std::future::pending::<()>().await;
}

/// Serve agents until a shutdown signal, draining active sessions for thirty seconds.
pub async fn run(mut config: Config) -> Result<(), String> {
    let root_hex = zeroize::Zeroizing::new(data_encoding::HEXLOWER.encode(&*config.root_key));
    let root_key =
        crate::token::keys::RootKey::from_hex(&root_hex).map_err(|_| "root key unavailable")?;
    let runtime = crate::state::runtime::StateRuntime::open(
        &config.state_dir,
        &root_key,
        &config.session,
        &config.standing,
    )?;
    let maintenance = AbortTask(tokio::spawn(runtime.clone().run_maintenance(
        crate::state::runtime::MAINTENANCE_INTERVAL,
    )));
    // GD-0081 integration: start mgmt::server here (not started yet; lead 5 builds it).
    let postgres_route = if config.upstream.is_some() {
        Some(
            config
                .postgres_route_id
                .as_deref()
                .and_then(crate::state::model::RouteId::new)
                .ok_or("PostgreSQL route id unavailable")?,
        )
    } else {
        None
    };
    let spanner_route = if config.spanner_native.is_some() {
        Some(
            config
                .spanner_route_id
                .as_deref()
                .and_then(crate::state::model::RouteId::new)
                .ok_or("Spanner route id unavailable")?,
        )
    } else {
        None
    };
    let hub = crate::reload::Hub::new(&config.policy, config.policy_watch_ms)?;
    let policy = hub.policy();
    let (reload_tx, reload_rx) = mpsc::channel(1);
    let admin_hub = hub.clone();
    let status_hub = hub.clone();
    let mut admin = AbortTask(tokio::spawn(crate::admin::serve_admin(
        config.admin_listen,
        Arc::new(move || admin_hub.metrics()),
        reload_tx,
        Arc::new(move || status_hub.status()),
    )));
    let reload_hub = hub.clone();
    let reload_path = config.policy.clone();
    let _reload = AbortTask(tokio::spawn(async move {
        reload_hub.run(reload_path, reload_rx).await
    }));
    let (shutdown, rx) = tokio::sync::watch::channel(false);
    let mut routes = JoinSet::new();
    if let Some(section) = config.spanner_native.take() {
        let state = crate::spanner_serve::RouteState {
            authority: runtime.authority.clone(),
            registry: runtime.registry.clone(),
            route: spanner_route.ok_or("Spanner route id unavailable")?,
        };
        let readlog = Box::new(hub.log.clone());
        let tls = config
            .tls
            .as_ref()
            .map(|tls| {
                let cert = std::fs::read(&tls.cert_file)
                    .map_err(|_| "tls: cannot load certificate and private key")?;
                let key = zeroize::Zeroizing::new(
                    std::fs::read(&tls.key_file)
                        .map_err(|_| "tls: cannot load certificate and private key")?,
                );
                Ok::<_, String>(
                    tonic::transport::ServerTlsConfig::new()
                        .identity(tonic::transport::Identity::from_pem(cert, key.as_slice())),
                )
            })
            .transpose()?;
        let root_hex = zeroize::Zeroizing::new(data_encoding::HEXLOWER.encode(&*config.root_key));
        let root_key =
            crate::token::keys::RootKey::from_hex(&root_hex).map_err(|_| "root key unavailable")?;
        let policy = policy.clone();
        let route_hub = hub.clone();
        let rx = rx.clone();
        routes.spawn(async move {
            crate::spanner_serve::run_spanner_with_hub(
                &section,
                policy,
                root_key,
                state,
                readlog,
                Some(route_hub),
                tls,
                rx,
            )
            .await
        });
    }
    if let Some(route) = postgres_route {
        let runtime = runtime.clone();
        let rx = rx.clone();
        routes.spawn(async move {
            let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
            let work = run_postgres(
                config,
                policy,
                rx.clone(),
                ready_tx,
                hub,
                (runtime, route),
            );
            tokio::pin!(work);
            tokio::select! {
                biased;
                result = &mut work => result,
                _ = ready_rx => work.await,
                _ = stopped(rx) => Ok(()),
            }
        });
    }
    let supervision = supervise(routes, shutdown.clone());
    tokio::pin!(supervision);
    let result = tokio::select! {
        result = &mut supervision => result,
        _ = &mut admin.0 => {
            let _ = shutdown.send(true);
            let _ = supervision.await;
            Err("admin listener stopped".into())
        }
    };
    // Abort maintenance before releasing the runtime after all routes have drained.
    drop(maintenance);
    // Retain exclusive state ownership until all routes have drained.
    drop(runtime);
    result
}
async fn stopped(mut rx: tokio::sync::watch::Receiver<bool>) {
    loop {
        if *rx.borrow_and_update() {
            return;
        }
        if rx.changed().await.is_err() {
            return;
        }
    }
}
async fn supervise(
    mut routes: JoinSet<Result<(), String>>,
    shutdown: tokio::sync::watch::Sender<bool>,
) -> Result<(), String> {
    let result = tokio::select! {
        _ = async { tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = terminate() => {} } } => Ok(()),
        result = routes.join_next() => match result {
            Some(Ok(result)) => result,
            Some(Err(_)) => Err("route task stopped".into()),
            None => Err("no routes configured".into()),
        },
    };
    let _ = shutdown.send(true);
    while routes.join_next().await.is_some() {}
    result
}
async fn run_postgres(
    config: Config,
    policy: Arc<Policy>,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ready: tokio::sync::oneshot::Sender<()>,
    hub: Arc<crate::reload::Hub>,
    state: (
        Arc<crate::state::runtime::StateRuntime>,
        crate::state::model::RouteId,
    ),
) -> Result<(), String> {
    let log = hub.log.clone();
    let upstream = config
        .upstream_config()
        .ok_or("PostgreSQL route unavailable")?;
    let schema = connect_schema(&upstream).await?;
    let first = tokio::time::timeout(
        Duration::from_millis(upstream.connect_timeout_ms),
        snapshot(&schema.client, &config),
    )
    .await
    .map_err(|_| "upstream schema snapshot timed out")??;

    let tls = config
        .tls
        .as_ref()
        .map(crate::config::load_tls)
        .transpose()?;
    let root_hex = zeroize::Zeroizing::new(data_encoding::HEXLOWER.encode(&*config.root_key));
    let root_key =
        crate::token::keys::RootKey::from_hex(&root_hex).map_err(|_| "root key unavailable")?;
    let ctx = Arc::new(GatewayContext {
        authority: state.0.authority.clone(),
        registry: state.0.registry.clone(),
        route: state.1,
        policy: RwLock::new(policy),
        snapshot: RwLock::new(Arc::new(first)),
        root_key,
        readlog: Box::new(log.clone()),
        upstream: config
            .upstream_config()
            .ok_or("PostgreSQL route unavailable")?,
        search_path: config.search_path.clone(),
        lowering: crate::dialect::pg::PgLowering,
        codec: crate::token::StandardCodec,
        clock: Arc::new(now),
        cancel_map: Mutex::new(HashMap::new()),
        metrics: Metrics::default(),
        tls,
        allow_plaintext: config.allow_plaintext,
    });
    let listener =
        tokio::net::TcpListener::bind(config.listen.ok_or("agent listener unavailable")?)
            .await
            .map_err(|_| "agent listener unavailable")?;
    hub.register(crate::reload::Route::Postgres(ctx.clone()))?;
    let config = Arc::new(config);
    let refresh_config = config.clone();
    let (snapshot_tx, mut snapshot_rx) = mpsc::channel(1);
    let _refresher = AbortTask(tokio::spawn(async move {
        let mut schema = schema;
        let mut interval =
            tokio::time::interval(Duration::from_millis(refresh_config.snapshot_refresh_ms));
        interval.tick().await;
        loop {
            interval.tick().await;
            if schema.client.is_closed() {
                match connect_schema(&upstream).await {
                    Ok(new) => schema = new,
                    Err(_) => {
                        eprintln!("serve: schema reconnect failed");
                        continue;
                    }
                }
            }
            let candidate = tokio::time::timeout(
                Duration::from_millis(upstream.connect_timeout_ms),
                snapshot(&schema.client, &refresh_config),
            )
            .await;
            match candidate {
                Ok(Ok(new)) => {
                    if snapshot_tx.send(new).await.is_err() {
                        break;
                    }
                }
                Ok(Err(_)) => eprintln!("serve: snapshot refresh failed"),
                Err(_) => {
                    eprintln!("serve: snapshot refresh timed out");
                    // Dropping and reconnecting also cancels any unfinished catalog transaction.
                    match connect_schema(&upstream).await {
                        Ok(new) => schema = new,
                        Err(_) => {
                            schema.driver.abort();
                        }
                    }
                }
            }
        }
    }));
    let permits = Arc::new(Semaphore::new(config.max_connections));
    let mut sessions = JoinSet::new();
    let shutdown = stopped(shutdown_rx);
    tokio::pin!(shutdown);
    let _ = ready.send(());
    eprintln!("serve: listening");
    let outcome = loop {
        tokio::select! {
            _ = &mut shutdown => break Ok(()),
            accepted = listener.accept() => {
                let (mut tcp, _) = match accepted { Ok(pair) => pair, Err(_) => break Err("agent listener failed".into()) };
                if tcp.set_nodelay(true).is_err() { continue; }
                match permits.clone().try_acquire_owned() {
                    Ok(permit) => { let ctx = ctx.clone(); sessions.spawn(async move { let _permit = permit; crate::pgwire::session::serve_tcp(tcp, ctx).await; }); }
                    Err(_) => {
                        let mut bytes = bytes::BytesMut::new();
                        crate::pgwire::codec::encode_backend(&crate::pgwire::codec::Backend::ErrorResponse(crate::pgwire::redact::nunya_error(crate::error::NyCode::ProtocolNotAllowed)), &mut bytes);
                        let _ = tokio::time::timeout(Duration::from_secs(1), tcp.write_all(&bytes)).await;
                    }
                }
            }
            _ = sessions.join_next(), if !sessions.is_empty() => {},
            Some(new) = snapshot_rx.recv() => {
                if let Err(e) = hub.refresh(&crate::reload::Route::Postgres(ctx.clone()), new) {
                    eprintln!("serve: refresh validation failed: {e}");
                }
            }
        }
    };
    eprintln!("serve: shutting down");
    drop(listener);
    drop(_refresher);
    if tokio::time::timeout(Duration::from_secs(30), async {
        while sessions.join_next().await.is_some() {}
    })
    .await
    .is_err()
    {
        sessions.abort_all();
        while sessions.join_next().await.is_some() {}
    }
    eprintln!("serve: stopped");
    outcome
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    #[tokio::test]
    async fn route_failure_cancels_initializing_sibling() {
        let (tx, rx) = tokio::sync::watch::channel(false);
        let stopped_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = stopped_flag.clone();
        let mut routes = JoinSet::new();
        routes.spawn(async { Err("startup failed".into()) });
        routes.spawn(async move {
            stopped(rx).await;
            flag.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        });
        let result = supervise(routes, tx).await;
        assert_eq!(result, Err("startup failed".into()));
        assert!(stopped_flag.load(std::sync::atomic::Ordering::SeqCst));
    }
    fn policy() -> Policy {
        Policy {
            columns: BTreeMap::new(),
            domains: BTreeMap::new(),
            snapshot_max_age_ms: 60000,
            readlog: crate::policy::ReadLogSink::Stdout,
        }
    }
    #[test]
    fn captured_policy_bytes_resolve_relative_readlog() {
        let policy = parse_policy_bytes(
            Path::new("config/policy.toml"),
            b"[readlog]\nsink = 'file'\npath = 'audit.jsonl'\n",
        )
        .unwrap();
        assert!(
            matches!(policy.readlog, crate::policy::ReadLogSink::File { path, .. } if path == Path::new("config/audit.jsonl"))
        );
        assert!(parse_policy_bytes(Path::new("unused"), &[255]).is_err());
    }
    #[test]
    fn reload_accepts_valid_and_refuses_parse_or_schema_errors() {
        let snapshot = Snapshot::new(vec![], vec![], vec![], 0);
        assert!(reload_decision(Ok(policy()), &snapshot).is_ok());
        assert!(
            reload_decision(
                Err(vec![PolicyError {
                    message: "invalid policy".into()
                }]),
                &snapshot
            )
            .is_err()
        );
        let mut invalid = policy();
        invalid.columns.insert(
            crate::schema::ColumnRef {
                schema: "public".into(),
                table: "missing".into(),
                column: "secret".into(),
            },
            crate::policy::Treatment::Masked { placeholder: None },
        );
        assert!(reload_decision(Ok(invalid), &snapshot).is_err());
    }
}
