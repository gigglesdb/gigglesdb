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
    sync::{Arc, Mutex, RwLock, atomic::{AtomicBool, Ordering}},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::AsyncWriteExt,
    sync::mpsc,
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
// Sending shutdown on every exit lets the election loop release its Lease even
// when startup fails. Dropping its JoinHandle intentionally does not abort it.
#[cfg(feature = "kubernetes")]
struct ElectionTask {
    shutdown: tokio::sync::watch::Sender<bool>,
    task: JoinHandle<Result<(), crate::policy::kube_leader::ElectionError>>,
}
#[cfg(feature = "kubernetes")]
impl Drop for ElectionTask {
    fn drop(&mut self) { let _ = self.shutdown.send(true); }
}
#[derive(Default)]
pub(crate) struct Revisions {
    pub(crate) policy: String,
    pub(crate) config: String,
}
#[derive(Clone)]
pub(crate) struct ReloadableLog(
    pub(crate) Arc<RwLock<Box<dyn crate::readlog::ReadLog>>>,
    pub(crate) Arc<RwLock<Revisions>>,
    pub(crate) Arc<AtomicBool>,
);
impl ReloadableLog {
    /// Whether the last audit write succeeded, initially true.
    pub(crate) fn last_write_ok(&self) -> bool { self.2.load(Ordering::Relaxed) }
}
impl crate::readlog::ReadLog for ReloadableLog {
    fn write_lifecycle(&self, record: &crate::readlog::LifecycleRecord) -> Result<(), crate::error::Refusal> {
        let result = (|| {
            self.0.read().map_err(|_| crate::error::Refusal::new(
                crate::error::NyCode::ReadLogUnavailable, "read log unavailable"))?.write_lifecycle(record)
        })();
        self.2.store(result.is_ok(), Ordering::Relaxed);
        result
    }
    fn write(&self, record: &crate::readlog::ReadLogRecord) -> Result<(), crate::error::Refusal> {
        let result = (|| {
            let sink = self.0.read().map_err(|_| crate::error::Refusal::new(
                crate::error::NyCode::ReadLogUnavailable, "read log unavailable"))?;
            let revisions = self.1.read().map_err(|_| crate::error::Refusal::new(
                crate::error::NyCode::ReadLogUnavailable, "read log unavailable"))?;
            let mut stamped = record.clone();
            stamped.policy_revision.get_or_insert_with(|| revisions.policy.clone());
            stamped.config_revision.get_or_insert_with(|| revisions.config.clone());
            sink.write(&stamped)
        })();
        self.2.store(result.is_ok(), Ordering::Relaxed);
        result
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
    .map_err(|_| schema_failure("network"))?
}
fn schema_failure(class: &str) -> String {
    format!("upstream schema connection failed: {class}")
}
fn login_class(code: &str) -> String {
    if code.len() == 5 && code.bytes().all(|b| b.is_ascii_alphanumeric()) {
        format!("server refused login sqlstate {code}")
    } else {
        "other".into()
    }
}
fn schema_error_class(mut error: &(dyn std::error::Error + 'static)) -> String {
    let mut network = false;
    loop {
        if let Some(error) = error.downcast_ref::<rustls::Error>() {
            return match error {
                rustls::Error::InvalidCertificate(
                    rustls::CertificateError::NotValidForName
                    | rustls::CertificateError::NotValidForNameContext { .. },
                ) => "tls hostname mismatch",
                rustls::Error::InvalidCertificate(_) => "tls certificate not trusted",
                _ => "tls other",
            }.into();
        }
        if let Some(error) = error.downcast_ref::<tokio_postgres::Error>()
            && let Some(code) = error.code()
        {
            return login_class(code.code());
        }
        if let Some(io) = error.downcast_ref::<std::io::Error>() {
            network = true;
            if let Some(inner) = io.get_ref() {
                error = inner;
                continue;
            }
        }
        match error.source() {
            Some(source) => error = source,
            None => return if network { "network" } else { "other" }.into(),
        }
    }
}
#[cfg(test)]
mod schema_connection_tests {
    use super::*;
    #[test]
    fn startup_failure_classes_are_data_free() {
        for (error, expected) in [
            (rustls::Error::InvalidCertificate(rustls::CertificateError::UnknownIssuer), "tls certificate not trusted"),
            (rustls::Error::InvalidCertificate(rustls::CertificateError::BadSignature), "tls certificate not trusted"),
            (rustls::Error::InvalidCertificate(rustls::CertificateError::NotValidForName), "tls hostname mismatch"),
            (rustls::Error::General("private detail".into()), "tls other"),
        ] {
            let wrapped = std::io::Error::other(error);
            assert_eq!(schema_error_class(&wrapped), expected);
        }
        assert_eq!(schema_error_class(&std::io::Error::from(std::io::ErrorKind::ConnectionRefused)), "network");
        assert_eq!(login_class("28000"), "server refused login sqlstate 28000");
        assert_eq!(login_class("28P01"), "server refused login sqlstate 28P01");
        for invalid in ["", "280000", "28\n01", "\u{e9}001"] {
            assert_eq!(login_class(invalid), "other");
        }
    }
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
    let fail = || schema_failure("other");
    if matches!(cfg.tls, UpstreamTls::Disable) {
        pg.ssl_mode(tokio_postgres::config::SslMode::Disable);
        let (client, connection) = pg
            .connect(tokio_postgres::NoTls)
            .await
            .map_err(|error| schema_failure(&schema_error_class(&error)))?;
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
            let pem = std::fs::read(ca_file).map_err(|_| schema_failure("ca file unreadable"))?;
            let mut roots = rustls::RootCertStore::empty();
            for cert in rustls_pemfile::certs(&mut pem.as_slice()) {
                roots.add(cert.map_err(|_| schema_failure("ca file invalid"))?)
                    .map_err(|_| schema_failure("ca file invalid"))?;
            }
            if roots.is_empty() {
                return Err(schema_failure("ca file invalid"));
            }
            builder.with_root_certificates(roots).with_no_client_auth()
        }
        UpstreamTls::Disable => return Err(fail()),
    };
    let (client, connection) = pg
        .connect(tokio_postgres_rustls::MakeRustlsConnect::new(tls))
        .await
        .map_err(|error| schema_failure(&schema_error_class(&error)))?;
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
pub(crate) fn parse_policy_bytes(path: &Path, bytes: &[u8], service: &crate::policy::ServicePolicySettings) -> Result<crate::policy::PolicyDocument, Vec<PolicyError>> {
    let text = std::str::from_utf8(bytes).map_err(|_| {
        vec![PolicyError {
            message: "policy file is not UTF-8".into(),
        }]
    })?;
    let mut document = crate::policy::file::parse_policy_document(text)?;
    if let Some(crate::policy::ReadLogSink::File { path: log, .. }) = &mut document.file_readlog
        && log.is_relative()
    {
        *log = path.parent().unwrap_or(Path::new(".")).join(&*log);
    }
    let document = crate::policy::file::resolve(document, service)?;
    Ok(document)
}
pub(crate) use crate::policy::file::readlog_key;
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

struct ManagementConnections(crate::state::runtime::Registry);
impl crate::mgmt::routes::ConnectionSource for ManagementConnections {
    fn list(&self) -> Vec<crate::mgmt::routes::ConnectionView> {
        self.0.list_all().into_iter().filter(|snap| snap.ended.is_none()).map(|snap| {
            crate::mgmt::routes::ConnectionView { id: snap.id.get(), session: snap.session.to_hex(),
                principal: snap.principal.to_hex(), route: snap.route }
        }).collect()
    }
}
struct ManagementStatus {
    config_reload: Option<Arc<crate::config_reload::ConfigReloader>>,
    hub: Arc<crate::reload::Hub>,
    routes: Vec<crate::state::model::RouteId>,
    spanner_route: Option<crate::state::model::RouteId>,
}
impl crate::mgmt::routes::StatusSource for ManagementStatus {
    fn policy_revision(&self, route: &str) -> Option<String> {
        let route = self.routes.iter().find(|id| id.as_str() == route)?;
        self.hub.policy_revision_for(route)
    }
    fn config_revision(&self) -> Option<String> {
        self.hub.log.1.read().ok().map(|r| r.config.clone())
    }
    fn config_restart_pending(&self) -> Vec<String> {
        self.config_reload.as_ref().map(|r| r.restart_pending()).unwrap_or_default()
    }
    fn config_last_reload(&self) -> Option<String> {
        self.config_reload.as_ref().map(|r| r.last_reload())
    }
    fn route_kind(&self, route: &str) -> Option<&'static str> {
        let id = self.routes.iter().find(|id| id.as_str() == route)?;
        Some(if self.spanner_route.as_ref() == Some(id) { "spanner-native" } else { "postgres" })
    }
    fn route_ids(&self) -> Vec<String> { self.routes.iter().map(|r| r.as_str().into()).collect() }
}

// Bound acknowledgement latency; status continues reporting any older releases.
const ACTIVATION_WAIT: std::time::Duration = std::time::Duration::from_secs(2);

struct ManagementPolicy {
    rejections: std::sync::Mutex<std::collections::BTreeMap<crate::state::model::RouteId, crate::mgmt::policy::PolicyRejection>>,
    writes: std::sync::Mutex<()>,
    hub: Arc<crate::reload::Hub>,
    store: Arc<dyn crate::state::store::Store>,
}
impl ManagementPolicy {
    fn activation_after_write(&self, route: &crate::state::model::RouteId) -> crate::mgmt::policy::Activation {
        crate::mgmt::policy::Activation::from_pending(self.hub.fence_for(route)
            .map_or(0, |fence| fence.wait_released(fence.epoch(), ACTIVATION_WAIT)))
    }
    fn write(
        &self,
        route: &crate::state::model::RouteId,
        operation: impl FnOnce() -> Result<crate::policy::PolicyRevision, crate::reload::PolicyOpError>,
    ) -> Result<crate::mgmt::policy::PolicyWrite, crate::mgmt::policy::PolicyError> {
        // Serialize adapter mutations so the pre-operation revision determines replay
        // even when identical management requests arrive concurrently.
        let _guard = self
            .writes
            .lock()
            .map_err(|_| crate::mgmt::policy::PolicyError::Code(503, "policy_unavailable"))?;
        let before = self.hub.active_revision(route)?.map(|r| r.number);
        let revision = operation()?;
        let replayed = before.is_some_and(|n| revision.number <= n);
        // Hub publication has returned; neither the Hub nor adapter write lock is held
        // while releases finish (their slow path may read the route policy).
        drop(_guard);
        let activation = self.activation_after_write(route);
        Ok(crate::mgmt::policy::PolicyWrite { revision, replayed, activation })
    }
}
#[cfg(test)]
mod management_policy_tests {
    use super::*;
    use crate::mgmt::policy::{Activation, PolicySource};

    #[test]
    fn write_waits_without_adapter_lock_and_replay_reads_live_fence() {
        let fixture = crate::pgwire::session::tests::Fixture::new();
        let dir = std::env::temp_dir().join(format!("activation-{}", crate::token::SessionId::random().to_hex()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("policy.toml");
        std::fs::write(&path, "[readlog]\nsink='stdout'\n").unwrap();
        let route = crate::state::model::RouteId::new("postgres").unwrap();
        let store = Arc::new(crate::state::store::RedbStore::open(&dir.join("state"), &crate::token::keys::RootKey::from_hex(&"01".repeat(32)).unwrap()).unwrap());
        let hub = crate::reload::Hub::new(&[(route.clone(),path)],None,Default::default()).unwrap();
        hub.set_store(store.clone()).unwrap();
        hub.set_revision_store(Arc::new(crate::policy::LocalRevisionStore::new(store.clone()))).unwrap();
        hub.register(crate::reload::Route::Postgres(fixture.ctx.clone())).unwrap();
        let adapter = ManagementPolicy { writes: Default::default(), rejections: Default::default(), hub, store };
        assert_eq!(adapter.import(&route,"admin","import".into()).unwrap().activation, Activation::Active);
        let fence = adapter.hub.fence_for(&route).unwrap();
        let slot = fence.register(); slot.begin(&fence);
        let fp = adapter.hub.current_catalog_fingerprint(&route).unwrap();
        let masked = "[readlog]\nsink='stdout'\n[[column]]\ntable='public.items'\nname='value'\nmasked=true\n";
        let result = adapter.apply(&route,"admin",masked,1,&fp,"apply".into()).unwrap();
        assert_eq!(result.activation, Activation::Pending { statements: 1 });
        assert!(!result.replayed);
        assert_eq!(adapter.activation(&route), Activation::Pending { statements: 1 });
        let replay = adapter.apply(&route,"admin",masked,1,&fp,"apply".into()).unwrap();
        assert!(replay.replayed);
        assert_eq!(replay.activation, Activation::Pending { statements: 1 });
        std::thread::scope(|scope| {
            scope.spawn(|| {
                std::thread::sleep(std::time::Duration::from_millis(30));
                let _guard = adapter.writes.try_lock().expect("wait must release adapter lock");
                let _policy = fixture.ctx.policy.read().unwrap();
                slot.end();
            });
            let replay = adapter.apply(&route,"admin",masked,1,&fp,"apply".into()).unwrap();
            assert!(replay.replayed); assert_eq!(replay.activation, Activation::Active);
        });
        assert_eq!(adapter.activation(&route), Activation::Active);
        let unknown = crate::state::model::RouteId::new("unknown").unwrap();
        assert_eq!(adapter.activation_after_write(&unknown), Activation::Active);
        drop(adapter); drop(fixture);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
#[allow(clippy::result_large_err)] // Internal policy errors retain the stale revision for the API.
impl crate::mgmt::policy::PolicySource for ManagementPolicy {
    fn activation(&self, r: &crate::state::model::RouteId) -> crate::mgmt::policy::Activation {
        crate::mgmt::policy::Activation::from_pending(self.hub.fence_for(r)
            .map_or(0, |fence| fence.lagging(fence.epoch())))
    }
    fn record_rejection(&self, r: &crate::state::model::RouteId, rejection: crate::mgmt::policy::PolicyRejection) {
        self.rejections.lock().unwrap_or_else(|e| e.into_inner()).insert(r.clone(), rejection);
    }
    fn last_rejected(&self, r: &crate::state::model::RouteId) -> Option<crate::mgmt::policy::PolicyRejection> {
        self.rejections.lock().unwrap_or_else(|e| e.into_inner()).get(r).cloned()
    }
    fn published(&self, r: &crate::state::model::RouteId) -> Result<Option<u64>, crate::mgmt::policy::PolicyError> { self.hub.published_revision(r).map_err(Into::into) }
    fn validate(&self, r: &crate::state::model::RouteId, toml: &str) -> Result<crate::policy::PolicyDocument, crate::mgmt::policy::PolicyError> { self.hub.validate_draft(r, toml).map_err(Into::into) }
    fn owner(
        &self,
        r: &crate::state::model::RouteId,
    ) -> Result<crate::policy::Owner, crate::mgmt::policy::PolicyError> {
        self.hub.owner(r).map_err(Into::into)
    }
    fn active(
        &self,
        r: &crate::state::model::RouteId,
    ) -> Result<Option<crate::policy::PolicyRevision>, crate::mgmt::policy::PolicyError> {
        self.hub.active_revision(r).map_err(Into::into)
    }
    fn history(
        &self,
        r: &crate::state::model::RouteId,
    ) -> Result<Vec<crate::policy::RevisionSummary>, crate::mgmt::policy::PolicyError> {
        self.hub.revisions(r).map_err(Into::into)
    }
    fn get(
        &self,
        r: &crate::state::model::RouteId,
        n: u64,
    ) -> Result<Option<crate::policy::PolicyRevision>, crate::mgmt::policy::PolicyError> {
        self.hub.revision(r, n).map_err(Into::into)
    }
    fn effective(
        &self,
        r: &crate::state::model::RouteId,
    ) -> Result<
        (
            Arc<crate::policy::PolicyDocument>,
            Arc<crate::schema::Snapshot>,
            crate::policy::bindings::RouteBindings,
        ),
        crate::mgmt::policy::PolicyError,
    > {
        let (d, s) = self.hub.effective_document(r)?;
        let b = crate::policy::bindings::load(self.store.as_ref(), r)
            .map_err(|_| crate::mgmt::policy::PolicyError::Code(503, "policy_unavailable"))?;
        Ok((d, s, b))
    }
    fn fingerprint(
        &self,
        r: &crate::state::model::RouteId,
    ) -> Result<String, crate::mgmt::policy::PolicyError> {
        self.hub.current_catalog_fingerprint(r).map_err(Into::into)
    }
    fn export(
        &self,
        r: &crate::state::model::RouteId,
    ) -> Result<String, crate::mgmt::policy::PolicyError> {
        self.hub.policy_source(r).map_err(Into::into)
    }
    fn import(
        &self,
        r: &crate::state::model::RouteId,
        a: &str,
        k: String,
    ) -> Result<crate::mgmt::policy::PolicyWrite, crate::mgmt::policy::PolicyError> {
        self.write(r, || self.hub.import_policy(r, a, Some(k)))
    }
    fn apply(
        &self,
        r: &crate::state::model::RouteId,
        a: &str,
        t: &str,
        e: u64,
        f: &str,
        k: String,
    ) -> Result<crate::mgmt::policy::PolicyWrite, crate::mgmt::policy::PolicyError> {
        self.write(r, || self.hub.apply_policy(r, a, t, e, f, Some(k)))
    }
    fn rollback(
        &self,
        r: &crate::state::model::RouteId,
        a: &str,
        t: u64,
        e: u64,
        k: String,
    ) -> Result<crate::mgmt::policy::PolicyWrite, crate::mgmt::policy::PolicyError> {
        self.write(r, || self.hub.rollback_policy(r, a, t, e, Some(k)))
    }
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
    let mut policy_routes = Vec::new();
    for (route, path) in [(&postgres_route, &config.postgres_policy), (&spanner_route, &config.spanner_policy)] {
        if let Some(route) = route {
            policy_routes.push((route.clone(), path.clone().ok_or("route policy unavailable")?));
        }
    }
    #[cfg(feature = "kubernetes")]
    let elector = if config.policy_store == crate::config::PolicyStore::Kubernetes {
        let k = config.policy_store_kubernetes.as_ref().ok_or("Kubernetes settings unavailable")?;
        let client = kube::Client::try_default().await.map_err(|_| "Kubernetes client unavailable")?;
        Some((client.clone(), crate::policy::kube_leader::LeaderElector::new(
            client, &k.namespace, &k.lease_name, &k.pod_name, &k.management_url,
            crate::policy::kube_leader::Timings {
                lease_duration: Duration::from_secs(k.lease_duration_secs),
                renew_deadline: Duration::from_secs(k.renew_deadline_secs),
                retry_period: Duration::from_secs(k.retry_period_secs),
            }).map_err(|_| "Kubernetes election configuration invalid")?))
    } else { None };
    let leadership: Option<Arc<dyn crate::mgmt::routes::Leadership>>;
    let revision_store: Arc<dyn crate::policy::RevisionStore>;
    #[cfg(feature = "kubernetes")]
    if let Some((client, candidate)) = &elector {
        let k = config.policy_store_kubernetes.as_ref().ok_or("Kubernetes settings unavailable")?;
        let state = candidate.state();
        leadership = Some(Arc::new(state.clone()));
        revision_store = Arc::new(crate::policy::kube_store::KubernetesRevisionStore::new(
            client.clone(), &k.namespace, tokio::runtime::Handle::current()).with_leader(state));
    } else {
        leadership = None;
        revision_store = Arc::new(crate::policy::LocalRevisionStore::new(runtime.store()));
    }
    #[cfg(not(feature = "kubernetes"))]
    {
        leadership = None;
        revision_store = Arc::new(crate::policy::LocalRevisionStore::new(runtime.store()));
    }
    let (shutdown, rx) = tokio::sync::watch::channel(false);
    #[cfg(feature = "kubernetes")]
    let election_task = if let Some((_, candidate)) = elector {
        // Make one attempt before startup ownership writes; followers still fail closed.
        let candidate = tokio::task::spawn_blocking(move || {
            let _ = candidate.tick();
            candidate
        }).await.map_err(|_| "Kubernetes election unavailable")?;
        Some(ElectionTask { shutdown: shutdown.clone(), task: tokio::spawn(candidate.run(rx.clone())) })
    } else { None };
    let hub = if config.policy_store == crate::config::PolicyStore::Kubernetes {
        let store = revision_store.clone();
        let routes = policy_routes.clone();
        let file_owners = config.file_policy_routes.clone();
        let service = config.policy_settings.clone();
        let watch = config.policy_watch_ms;
        // Keep the client transport's runtime free while the synchronous bridge reads.
        tokio::task::spawn_blocking(move || {
            crate::reload::Hub::release_file_owners(store.as_ref(), &file_owners)?;
            crate::reload::Hub::new_with_revision_store(&routes, watch, service, Some(store.as_ref()))
        }).await.map_err(|_| "service policy unavailable")??
    } else {
        crate::reload::Hub::release_file_owners(revision_store.as_ref(), &config.file_policy_routes)?;
        crate::reload::Hub::new_with_revision_store(&policy_routes, config.policy_watch_ms, config.policy_settings.clone(), Some(revision_store.as_ref()))?
    };
    hub.set_store(runtime.store())?;
    hub.set_file_owners(&config.file_policy_routes)?;
    hub.set_revision_store(revision_store)?;
    hub.set_authority(Arc::downgrade(&runtime.authority))?;
    let admins: crate::mgmt::routes::Admins = Arc::new(std::sync::RwLock::new(config.management.as_ref().map(|m| {
        m.admins.iter().map(|a| (a.name.clone(), crate::pgwire::auth::ScramVerifier {
            salt: a.verifier.salt.clone(), iterations: a.verifier.iterations,
            stored_key: a.verifier.stored_key, server_key: a.verifier.server_key,
        })).collect()
    }).unwrap_or_default()));
    if admins.read().unwrap_or_else(std::sync::PoisonError::into_inner).iter().any(|(name, _)| {
        runtime.authority.principal_by_name(name).is_some_and(|p| p.source == crate::state::model::PrincipalSource::Api)
    }) { return Err("management admin name taken".into()); }
    let config_reload = Arc::new(crate::config_reload::ConfigReloader::new(
        crate::config_reload::copy_config(&config), Arc::downgrade(&hub),
    ));
    let transports = crate::config_reload::Transports::new(&config)?;
    config_reload.attach_transports(transports.clone());
    config_reload.attach(runtime.authority.clone(), admins.clone());
    runtime.set_lifecycle_log(Arc::new(hub.log.clone()));
    hub.set_config_revision(&config.revision)?;
    let (reload_tx, reload_rx) = mpsc::channel(1);
    let metrics_config = config_reload.clone();
    let status_config = config_reload.clone();
    let admin_hub = hub.clone();
    let status_hub = hub.clone();
    let metrics_runtime = runtime.clone();
    let ready_runtime = runtime.clone();
    let ready_hub = hub.clone();
    let mut admin = AbortTask(tokio::spawn(crate::admin::serve_admin(
        config.admin_listen,
        Arc::new(move || {
            let mut metrics = admin_hub.metrics();
            metrics.extend(metrics_runtime.metrics());
            metrics.extend(metrics_config.metrics());
            metrics
        }),
        reload_tx,
        Arc::new(move || format!("{}{}", status_hub.status(), status_config.status())),
        Arc::new(move || ready_hub.readiness(ready_hub.log.last_write_ok(), ready_runtime.store_last_write_ok())),
    )));
    let reload_config = config_reload.clone();
    let reload_hub = hub.clone();
    let _reload = AbortTask(tokio::spawn(async move {
        reload_hub.run(reload_rx, Some(reload_config)).await
    }));
    let management = if config.management.is_some() || cfg!(unix) {
        let ctx = crate::mgmt::routes::MgmtContext { leadership,
            policy: Some(Arc::new(ManagementPolicy { rejections: Default::default(), hub: hub.clone(), store: runtime.store(), writes: std::sync::Mutex::new(()) })),
            authority: runtime.authority.clone(), admins,
            connections: Arc::new(ManagementConnections(runtime.registry.clone())),
            status: Arc::new(ManagementStatus {
                config_reload: Some(config_reload.clone()),
                hub: hub.clone(),
                spanner_route: spanner_route.clone(),
                routes: postgres_route.iter().chain(spanner_route.iter()).cloned().collect(),
            }),
            lifecycle: Arc::new(crate::mgmt::audit::ReadLogSink(hub.log.clone())),
        };
        Some(crate::mgmt::server::spawn_with_tls(Arc::new(ctx), config.management.as_ref(),
            &config.state_dir, rx.clone(), transports.management.clone()).await.map_err(|err| err.to_string())?)
    } else { None };

    let mut routes = JoinSet::new();
    if let Some(section) = config.spanner_native.take() {
        let state = crate::spanner_serve::RouteState {
            authority: runtime.authority.clone(),
            registry: runtime.registry.clone(),
            route: spanner_route.ok_or("Spanner route id unavailable")?,
        };
        let readlog = Box::new(hub.route_log(&state.route)?);
        let policy = hub.policy_for(&state.route)?;
        let tls = config.tls.as_ref().map(|_| transports.spanner.clone());
        let root_hex = zeroize::Zeroizing::new(data_encoding::HEXLOWER.encode(&*config.root_key));
        let root_key =
            crate::token::keys::RootKey::from_hex(&root_hex).map_err(|_| "root key unavailable")?;
        let route_hub = hub.clone();
        let rx = rx.clone();
        let refresh_period = transports.spanner_period.subscribe();
        routes.spawn(async move {
            crate::spanner_serve::run_spanner_with_hub(
                &section,
                policy,
                root_key,
                state,
                readlog,
                Some(route_hub),
                Some(refresh_period),
                tls,
                rx,
            )
            .await
        });
    }
    if let Some(route) = postgres_route {
        let policy = hub.policy_for(&route)?;
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
                transports,
                config_reload,
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
    if let Some(management) = management { management.wait().await; }
    #[cfg(feature = "kubernetes")]
    if let Some(mut task) = election_task { let _ = (&mut task.task).await; }
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
#[allow(clippy::too_many_arguments)]
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
    transports: Arc<crate::config_reload::Transports>,
    config_reload: Arc<crate::config_reload::ConfigReloader>,
) -> Result<(), String> {
    let log = hub.route_log(&state.1)?;
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

    let tls = transports.postgres.clone();
    let root_hex = zeroize::Zeroizing::new(data_encoding::HEXLOWER.encode(&*config.root_key));
    let root_key =
        crate::token::keys::RootKey::from_hex(&root_hex).map_err(|_| "root key unavailable")?;
    let ctx = Arc::new(GatewayContext {
        fence: Default::default(),
        authority: state.0.authority.clone(),
        registry: state.0.registry.clone(),
        route: state.1,
        policy_revision: RwLock::new(None),
        policy: RwLock::new(policy),
        snapshot: RwLock::new(Arc::new(first)),
        root_key,
        readlog: Box::new(log.clone()),
        upstream: config
            .upstream_config()
            .ok_or("PostgreSQL route unavailable")?,
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
    config_reload.attach_catalog(ctx.clone(), Arc::new(|cfg| Box::pin(async move {
        let upstream = cfg.upstream_config().ok_or("snapshot unavailable")?;
        let schema = connect_schema(&upstream).await.map_err(|_| "snapshot unavailable")?;
        tokio::time::timeout(Duration::from_millis(upstream.connect_timeout_ms), snapshot(&schema.client, &cfg))
            .await.map_err(|_| "snapshot unavailable")?.map_err(|_| "snapshot unavailable")
    })));
    let config = Arc::new(config);
    let refresh_config = config.clone();
    let mut settings_rx = transports.refresh.subscribe();
    let (snapshot_tx, mut snapshot_rx) = mpsc::channel(1);
    let _refresher = AbortTask(tokio::spawn(async move {
        let mut schema = schema;
        loop {
            crate::config_reload::refresh_tick(&mut settings_rx, |s| s.refresh_ms).await;
            let settings = settings_rx.borrow().clone();
            let mut capture_config = crate::config_reload::copy_config(&refresh_config);
            capture_config.schemas = settings.schemas;
            capture_config.search_path = settings.search_path;
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
                snapshot(&schema.client, &capture_config),
            )
            .await;
            match candidate {
                Ok(Ok(new)) => {
                    if snapshot_tx.send((settings.generation, new)).await.is_err() {
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
    let permits = transports.connections.clone();
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
                match permits.acquire() {
                    Some(permit) => { let ctx = ctx.clone(); sessions.spawn(async move { crate::pgwire::session::serve_tcp_admitted(tcp, ctx, permit).await; }); }
                    None => {
                        let mut bytes = bytes::BytesMut::new();
                        crate::pgwire::codec::encode_backend(&crate::pgwire::codec::Backend::ErrorResponse(crate::pgwire::redact::nunya_error(crate::error::NyCode::ProtocolNotAllowed)), &mut bytes);
                        let _ = tokio::time::timeout(Duration::from_secs(1), tcp.write_all(&bytes)).await;
                    }
                }
            }
            _ = sessions.join_next(), if !sessions.is_empty() => {},
            Some((generation, new)) = snapshot_rx.recv() => {
                if let Err(e) = hub.refresh_if(&crate::reload::Route::Postgres(ctx.clone()), new, || transports.refresh.borrow().generation == generation) {
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
    #[test]
    fn management_status_tracks_live_revisions_and_refuses_poisoned_lock() {
        use crate::mgmt::routes::StatusSource;
        let mut nonce = [0; 16];
        getrandom::fill(&mut nonce).unwrap();
        let dir = std::env::temp_dir().join(format!("nunya-management-status-{}", data_encoding::HEXLOWER.encode(&nonce)));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("policy.toml");
        std::fs::write(&path, "[readlog]\nsink='stdout'\n").unwrap();
        let route = crate::state::model::RouteId::new("postgres").unwrap();
        let hub = crate::reload::Hub::new(&[(route.clone(), path)], None, crate::policy::ServicePolicySettings::default()).unwrap();
        hub.set_config_revision("config").unwrap();
        let revisions = hub.route_log(&route).unwrap().1;
        let status = ManagementStatus { config_reload: None, hub: hub.clone(), spanner_route: None, routes: vec![route] };
        let first = status.policy_revision("postgres").unwrap();
        revisions.write().unwrap().policy = "reloaded".into();
        assert_ne!(first, "reloaded");
        assert_eq!(status.policy_revision("postgres").as_deref(), Some("reloaded"));
        assert_eq!(status.config_revision().as_deref(), Some("config"));
        assert_eq!(status.route_ids(), ["postgres"]);
        let _ = std::thread::spawn(move || {
            let _guard = revisions.write().unwrap();
            panic!("poison revision lock");
        }).join();
        assert_eq!(status.policy_revision("postgres"), None);
        let service_revisions = hub.log.1.clone();
        let _ = std::thread::spawn(move || {
            let _guard = service_revisions.write().unwrap();
            panic!("poison revision lock");
        }).join();
        assert_eq!(status.config_revision(), None);
        drop(status);
        drop(hub);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn management_status_returns_each_routes_revision() {
        use crate::mgmt::routes::StatusSource;
        let mut nonce = [0; 16];
        getrandom::fill(&mut nonce).unwrap();
        let dir = std::env::temp_dir().join(format!("nunya-management-units-{}", data_encoding::HEXLOWER.encode(&nonce)));
        std::fs::create_dir_all(&dir).unwrap();
        let a = dir.join("a.toml");
        let b = dir.join("b.toml");
        std::fs::write(&a, "[readlog]\nsink='stdout'\n").unwrap();
        std::fs::write(&b, "[readlog]\nsink='stdout'\n").unwrap();
        let routes = vec![crate::state::model::RouteId::new("a").unwrap(), crate::state::model::RouteId::new("b").unwrap()];
        let hub = crate::reload::Hub::new(&[(routes[0].clone(), a), (routes[1].clone(), b)], None, crate::policy::ServicePolicySettings::default()).unwrap();
        hub.set_config_revision("config").unwrap();
        let status = ManagementStatus { config_reload: None, hub: hub.clone(), spanner_route: None, routes: routes.clone() };
        let first = status.policy_revision("a").unwrap();
        assert_eq!(status.policy_revision("b"), Some(first.clone()));
        hub.route_log(&routes[1]).unwrap().1.write().unwrap().policy = "other".into();
        assert_eq!(status.policy_revision("a"), Some(first));
        assert_eq!(status.policy_revision("b").as_deref(), Some("other"));
        assert_eq!(status.policy_revision("unknown"), None);
        assert_eq!(status.config_revision().as_deref(), Some("config"));
        assert_eq!(status.route_ids(), ["a", "b"]);
        drop(status);
        drop(hub);
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn management_connections_excludes_ended_snapshots() {
        use crate::mgmt::routes::ConnectionSource;
        use crate::token::{SessionId, PrincipalId};
        let registry = crate::state::runtime::Registry::default();
        let session = SessionId([1; 16]);
        let principal = PrincipalId([2; 16]);
        let (guard, _signal) = registry.register(session, principal, "postgres", None);
        let source = ManagementConnections(registry.clone());
        let connections = source.list();
        assert_eq!(connections.len(), 1);
        assert_eq!(connections[0].id, guard.id().get());
        assert_eq!(connections[0].session, session.to_hex());
        assert_eq!(connections[0].principal, principal.to_hex());
        assert_eq!(connections[0].route, "postgres");
        registry.signal_session(&session, crate::state::registry::EndReason::Revoked);
        assert!(source.list().is_empty());
    }
    #[test]
    fn audit_availability_tracks_both_write_kinds_and_recovers() {
        use crate::readlog::{ReadLog, ReadLogRecord, LifecycleRecord, LifecycleKind, LifecycleActor};
        struct Sink(Arc<AtomicBool>);
        impl ReadLog for Sink {
            fn write(&self, _: &ReadLogRecord) -> Result<(), crate::error::Refusal> {
                if self.0.load(Ordering::Relaxed) { Ok(()) } else {
                    Err(crate::error::Refusal::new(crate::error::NyCode::ReadLogUnavailable, "read log unavailable"))
                }
            }
            fn write_lifecycle(&self, _: &LifecycleRecord) -> Result<(), crate::error::Refusal> {
                self.write(&record())
            }
        }
        fn record() -> ReadLogRecord {
            ReadLogRecord {
                ts_unix_ms: 1, principal: crate::token::PrincipalId([1; 16]),
                session: crate::token::SessionId([2; 16]), scope: crate::token::ScopeId([3; 16]),
                connection: None, credential_id: None, route: "postgres".into(),
                policy_revision: None, config_revision: None, statement_fingerprint: "test".into(),
                statement: "test".into(), outcome: crate::readlog::Outcome::Allowed, rows: 0, tokens_released: vec![],
            }
        }
        let available = Arc::new(AtomicBool::new(true));
        let log = ReloadableLog(Arc::new(RwLock::new(Box::new(Sink(available.clone())))),
            Arc::new(RwLock::new(Revisions::default())), Arc::new(AtomicBool::new(true)));
        let lifecycle = LifecycleRecord { policy: None,
            kind: LifecycleKind::SessionEnded, at_ms: 1, principal: String::new(), session: None,
            route: None, connection: None, actor: LifecycleActor::System, reason: "requested",
        };
        assert!(log.last_write_ok());
        for lifecycle_write in [false, true] {
            for ok in [false, true] {
                available.store(ok, Ordering::Relaxed);
                let result = if lifecycle_write { log.write_lifecycle(&lifecycle) } else { log.write(&record()) };
                assert_eq!(result.is_ok(), ok);
                assert_eq!(log.last_write_ok(), ok);
            }
        }
    }
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
            retired_domains: Default::default(),
            json_filters: Default::default(),
            columns: BTreeMap::new(),
            domains: BTreeMap::new(),
            snapshot_max_age_ms: 60000,
            readlog: crate::policy::ReadLogSink::Stdout,
        }
    }
    #[test]
    fn readlog_comparison_normalizes_existing_and_missing_files() {
        use crate::policy::ReadLogSink;
        let dir = std::env::temp_dir().join(format!(
            "readlog-key-{}",
            crate::token::SessionId::random().to_hex()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let sink = |path| ReadLogSink::File {
            path,
            max_bytes: 10,
        };
        let plain = sink(dir.as_path().join("read.log"));
        let dotted = sink(dir.as_path().join("./read.log"));
        assert_eq!(readlog_key(&plain), readlog_key(&dotted));
        std::fs::write(dir.as_path().join("read.log"), "").unwrap();
        assert_eq!(readlog_key(&plain), readlog_key(&dotted));
        let missing = sink(dir.as_path().join("missing/read.log"));
        assert_eq!(readlog_key(&missing), missing);
        assert_ne!(
            readlog_key(&plain),
            readlog_key(&ReadLogSink::File {
                path: dir.as_path().join("read.log"),
                max_bytes: 11,
            })
        );
        assert_eq!(readlog_key(&ReadLogSink::Stdout), ReadLogSink::Stdout);
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn captured_policy_bytes_resolve_relative_readlog() {
        let errors =
            parse_policy_bytes(Path::new("unused"), b"version = 2\ndefault = 'show'\n", &crate::policy::ServicePolicySettings::default())
                .unwrap_err();
        assert_eq!(
            errors,
            vec![PolicyError {
                message: "readlog: missing table".into()
            }]
        );
        let policy = parse_policy_bytes(
            Path::new("config/policy.toml"),
            b"[readlog]\nsink = 'file'\npath = 'audit.jsonl'\n", &crate::policy::ServicePolicySettings::default()
        )
        .unwrap();
        assert!(
            matches!(policy.policy.readlog, crate::policy::ReadLogSink::File { path, .. } if path == Path::new("config/audit.jsonl"))
        );
        assert!(parse_policy_bytes(Path::new("unused"), &[255], &crate::policy::ServicePolicySettings::default()).is_err());
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

    #[test]
    fn service_settings_resolve_before_serving_gates() {
        use crate::policy::{ReadLogSink, ServicePolicySettings, Treatment};
        let service = ServicePolicySettings { readlog: Some(ReadLogSink::Stdout), snapshot_max_age_ms: Some(123) };
        let path = Path::new("policy.toml");
        let legacy = parse_policy_bytes(path, b"", &service).unwrap();
        assert_eq!(legacy.policy.readlog, ReadLogSink::Stdout);
        assert_eq!(legacy.policy.snapshot_max_age_ms, 123);
        assert!(parse_policy_bytes(path, b"version=2\ndefault='tokenize'", &service).is_ok());
        let show = b"version=2\ndefault='show'\n[[column]]\ntable='public.t'\nname='plain'\nshow=true\n";
        let policy = parse_policy_bytes(path, show, &service).unwrap();
        assert_eq!(policy.policy.readlog, ReadLogSink::Stdout);
        assert!(policy.policy.columns.values().all(|t| *t == Treatment::Show));
        assert!(parse_policy_bytes(path, b"version=2\ndefault='show'\n[[json_path]]\ntable='public.t'\nname='data'\npath='$.a'\nmasked=true", &service).is_ok());
        assert_eq!(parse_policy_bytes(path, b"[readlog]\nsink='file'\npath='audit.log'", &service).unwrap_err()[0].message,
            "readlog: policy file and service config disagree");
        assert_eq!(parse_policy_bytes(path, show, &ServicePolicySettings::default()).unwrap_err()[0].message, "readlog: missing table");
    }


    #[test]
    fn file_and_service_readlog_paths_resolve_before_comparison() {
        use crate::policy::{ReadLogSink, ServicePolicySettings};
        let base = std::env::temp_dir();
        let service = ServicePolicySettings {
            readlog: Some(ReadLogSink::File {
                path: std::fs::canonicalize(&base).unwrap().join("gd88-audit.log"),
                max_bytes: 100 * 1024 * 1024,
            }),
            ..Default::default()
        };
        let bytes = b"[readlog]\nsink='file'\npath='./gd88-audit.log'";
        let policy = parse_policy_bytes(&base.join("policy.toml"), bytes, &service).unwrap();
        assert_eq!(readlog_key(&policy.policy.readlog), service.readlog.unwrap());
    }
    #[test]
    fn tokenize_default_accepts_scalars_and_json_paths() {
        let service = crate::policy::ServicePolicySettings {
            readlog: Some(crate::policy::ReadLogSink::Stdout), ..Default::default()
        };
        let path = Path::new("policy.toml");
        let scalar = parse_policy_bytes(path, b"version=2\ndefault='tokenize'", &service).unwrap();
        assert_eq!(scalar.route_default, crate::policy::RouteDefault::Tokenize);
        assert!(parse_policy_bytes(path,
            b"version=2\ndefault='tokenize'\n[[json_path]]\ntable='public.t'\nname='data'\npath='$.a'\nmasked=true", &service).is_ok());
        // Stack 4b serves [[link]] rules.
        let linked = parse_policy_bytes(path,
            b"version=2\ndefault='tokenize'\n[[link]]\nfields=['public.t.a','public.t.b']", &service).unwrap();
        assert_eq!(linked.links.len(), 1);
    }

}
