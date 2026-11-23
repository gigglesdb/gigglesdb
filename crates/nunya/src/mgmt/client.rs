//! Bounded, secret-redacting management API client.
use crate::{pgwire::auth::ScramVerifier, state::model::*};
use bytes::Bytes;
use http::{Method, Request, Uri};
use http_body_util::{BodyExt, Full, Limited};
use hyper_util::rt::TokioIo;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{fmt, net::IpAddr, path::Path, str::FromStr, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
    time::{sleep, timeout},
};
use zeroize::Zeroizing;

/// Sanitized management client failure.
#[derive(Debug, PartialEq, Eq)]
pub enum ClientError {
    /// Endpoint, connection or HTTP transport failure.
    Transport,
    /// Trust configuration or TLS handshake failure.
    Tls,
    /// Connection or request deadline elapsed.
    Timeout,
    /// Invalid JSON or oversized response.
    Decode,
    /// Leader redirect requiring explicit opt-in and HTTPS.
    LeaderRedirect {
        /// Advertised leader URL.
        location: String,
    },
    /// Stale policy precondition with value-free server metadata.
    Stale {
        /// Current durable revision.
        current: Box<crate::policy::RevisionSummary>,
        /// Proposed changes.
        diff: Box<PolicyDiffView>,
    },
    /// Server refusal with its public error code.
    Api {
        /// HTTP response status.
        status: u16,
        /// Server-provided public refusal code.
        code: String,
    },
}
impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stale { current, diff } => write!(f, "stale policy revision; current {}; {}", current.number, diff.summary),
            Self::LeaderRedirect { location } => write!(f, "leader at {location}; following requires --follow-leader and HTTPS on both endpoints"),
            Self::Transport => f.write_str("management transport unavailable"),
            Self::Tls => f.write_str("management TLS unavailable"),
            Self::Timeout => f.write_str("management request timed out"),
            Self::Decode => f.write_str("management response invalid"),
            Self::Api { status, code } => write!(f, "management API {status}: {code}"),
        }
    }
}
impl std::error::Error for ClientError {}

/// Validated management destination; construct by parsing a string.
#[derive(Clone)]
pub struct Endpoint(Destination);
#[derive(Clone)]
enum Destination {
    Tcp {
        host: String,
        authority: String,
        port: u16,
        tls: bool,
    },
    #[cfg(unix)]
    Unix(std::path::PathBuf),
}
impl FromStr for Endpoint {
    type Err = ClientError;
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        if let Some(path) = text.strip_prefix("unix:") {
            #[cfg(unix)]
            if Path::new(path).is_absolute() {
                return Ok(Self(Destination::Unix(path.into())));
            }
            #[cfg(not(unix))]
            let _ = path;
            return Err(ClientError::Transport);
        }
        let uri: Uri = text.parse().map_err(|_| ClientError::Transport)?;
        let tls = match uri.scheme_str() {
            Some("https") => true,
            Some("http") => false,
            _ => return Err(ClientError::Transport),
        };
        if uri.path_and_query().is_some_and(|p| p.as_str() != "/") {
            return Err(ClientError::Transport);
        }
        let authority = uri.authority().ok_or(ClientError::Transport)?;
        if authority.as_str().contains('@') || text.contains('#') {
            return Err(ClientError::Transport);
        }
        let host = uri
            .host()
            .ok_or(ClientError::Transport)?
            .trim_matches(['[', ']']);
        let port = authority.port_u16().ok_or(ClientError::Transport)?;
        if host.is_empty()
            || (!tls
                && !host.eq_ignore_ascii_case("localhost")
                && !host.parse::<IpAddr>().is_ok_and(|a| a.is_loopback()))
        {
            return Err(ClientError::Transport);
        }
        Ok(Self(Destination::Tcp {
            host: host.to_owned(),
            authority: authority.to_string(),
            port,
            tls,
        }))
    }
}
impl fmt::Debug for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Endpoint { [REDACTED] }")
    }
}
/// Management authentication material.
pub enum Credentials {
    /// Named bearer credential, kept out of diagnostics.
    Bearer {
        /// Principal name.
        name: String,
        /// Plaintext credential protected on drop.
        secret: Zeroizing<String>,
    },
    /// Local Unix socket authentication.
    None,
}
impl fmt::Debug for Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Credentials { [REDACTED] }")
    }
}
/// Locally generated session credential and server-safe derivations.
pub struct SessionSecret {
    /// 32 random bytes encoded as 64 lowercase hex characters.
    pub secret: Zeroizing<String>,
    /// PostgreSQL SCRAM verifier using the generated or supplied public parameters.
    pub verifier: String,
    /// Lowercase SHA-256 of the secret's UTF-8 bytes.
    pub sha256_hex: String,
    /// Standard base64 SCRAM salt used to derive this credential.
    pub scram_salt: String,
    /// SCRAM iteration count used to derive this credential.
    pub scram_iterations: u32,
}
impl SessionSecret {
    /// Generate a fresh credential using existing public SCRAM parameters.
    pub fn generate_with(salt: &[u8], iterations: u32) -> Result<Self, ClientError> {
        if salt.is_empty() || iterations == 0 {
            return Err(ClientError::Decode);
        }
        let mut bytes = Zeroizing::new([0; 32]);
        getrandom::fill(bytes.as_mut()).map_err(|_| ClientError::Transport)?;
        let secret = Zeroizing::new(data_encoding::HEXLOWER.encode(bytes.as_ref()));
        let verifier = ScramVerifier::from_password(&secret, salt, iterations).to_pg_string();
        let sha256_hex = data_encoding::HEXLOWER.encode(&Sha256::digest(secret.as_bytes()));
        Ok(Self {
            secret,
            verifier,
            sha256_hex,
            scram_salt: data_encoding::BASE64.encode(salt),
            scram_iterations: iterations,
        })
    }
    /// Generate a fresh credential; abort with a fixed message if OS entropy fails.
    pub fn generate() -> Self {
        let mut bytes = Zeroizing::new([0; 32]);
        let mut salt = [0; 16];
        getrandom::fill(bytes.as_mut()).expect("session entropy unavailable");
        getrandom::fill(&mut salt).expect("session entropy unavailable");
        let secret = Zeroizing::new(data_encoding::HEXLOWER.encode(bytes.as_ref()));
        let verifier = ScramVerifier::from_password(&secret, &salt, crate::pgwire::auth::SESSION_SCRAM_ITERATIONS).to_pg_string();
        let sha256_hex = data_encoding::HEXLOWER.encode(&Sha256::digest(secret.as_bytes()));
        Self {
            secret,
            verifier,
            sha256_hex,
            scram_salt: data_encoding::BASE64.encode(&salt),
            scram_iterations: crate::pgwire::auth::SESSION_SCRAM_ITERATIONS,
        }
    }
}
impl fmt::Debug for SessionSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SessionSecret { [REDACTED] }")
    }
}
/// Acquisition input containing only derived credential material.
#[derive(Serialize)]
pub struct AcquireParams {
    /// Requested route.
    pub route: String,
    /// Requested mode, or the route default.
    pub mode: Option<Mode>,
    /// Requested lifetime in duration syntax.
    pub lifetime: Option<String>,
    /// Whether to create an isolated scope.
    pub isolated: bool,
    /// Optional session label.
    pub label: Option<String>,
    /// Locally computed SCRAM verifier.
    pub verifier: String,
    /// Locally computed SHA-256 hex digest.
    pub secret_sha256: String,
}
/// Additional derived session credential for renewal.
#[derive(Serialize)]
pub struct SessionCredential {
    /// SCRAM verifier.
    pub verifier: String,
    /// SHA-256 hex digest.
    pub secret_sha256: String,
}
/// Advertised, non-secret connection details for a session route.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum ConnectInfo {
    /// PostgreSQL wire protocol, including the PGAdapter route.
    Postgres {
        /// Gateway host.
        host: String,
        /// Gateway port.
        port: u16,
        /// Database name.
        database: String,
    },
    /// Native Cloud Spanner protocol.
    SpannerNative {
        /// Gateway host and port.
        endpoint: String,
        /// Fully qualified database path.
        database: String,
    },
}
/// Public session projection.
#[derive(Debug, Deserialize)]
pub struct SessionView {
    /// Current policy revision.
    #[serde(default)]
    pub policy_revision: Option<String>,
    /// Current configuration revision.
    #[serde(default)]
    pub config_revision: Option<String>,
    /// Session hex identifier.
    pub id: String,
    /// Principal hex identifier.
    pub principal: String,
    /// Route identifier.
    pub route: String,
    /// Configured route protocol kind, absent on older servers.
    #[serde(default)]
    pub route_kind: Option<String>,
    /// Advertised connection details, absent on older servers or unknown routes.
    #[serde(default)]
    pub connect: Option<ConnectInfo>,
    /// Effective access mode.
    pub mode: Mode,
    /// Lifecycle state.
    pub state: SessionState,
    /// Creation timestamp.
    pub created_ms: u64,
    /// Expiration timestamp.
    pub expires_ms: u64,
    /// Optional idle deadline interval.
    pub idle_timeout_ms: Option<u64>,
    /// Most recent activity timestamp.
    pub last_used_ms: u64,
    /// Optional end reason.
    pub revoke_reason: Option<EndReason>,
    /// Optional label.
    pub label: Option<String>,
    /// Scope isolation flag.
    pub isolated: bool,
    /// Scope hex identifier.
    pub scope: String,
    /// Whether the scope has continuity.
    pub scope_continuity: bool,
}
/// Principal projection; list responses omit grants and credentials.
#[derive(Debug, Deserialize)]
pub struct PrincipalView {
    /// Principal hex identifier.
    pub id: String,
    /// Principal name.
    pub name: String,
    /// Principal kind.
    pub kind: PrincipalKind,
    /// Authorization state.
    pub state: PrincipalState,
    /// Provisioning source.
    pub source: PrincipalSource,
    /// Administrator flag.
    pub administrator: bool,
    /// Record revision.
    pub revision: u64,
    /// Route grants, present on detail responses.
    #[serde(default)]
    pub grants: Vec<GrantView>,
    /// Credential summaries, present on detail responses.
    #[serde(default)]
    pub credentials: Vec<CredentialSummary>,
}
/// Public route grant.
#[derive(Debug, Deserialize)]
pub struct GrantView {
    /// Route identifier.
    pub route: String,
    /// Maximum access mode.
    pub mode: Mode,
}
/// Non-secret credential projection.
#[derive(Debug, Deserialize)]
pub struct CredentialSummary {
    /// Credential hex identifier.
    pub id: String,
    /// Lifecycle state.
    pub state: CredentialState,
    /// Optional expiration timestamp.
    pub expires_ms: Option<u64>,
}
/// Active connection projection.
#[derive(Debug, Deserialize)]
pub struct ConnectionView {
    /// Connection identifier.
    pub id: u64,
    /// Session identifier.
    pub session: String,
    /// Principal identifier.
    pub principal: String,
    /// Route identifier.
    pub route: String,
}
/// Server status projection.
#[derive(Debug, Deserialize)]
pub struct StatusView {
    /// Deployment hex identifier.
    pub deployment_id: String,
    /// Persisted format revision.
    pub store_format: u32,
    /// Configured route identifiers.
    pub route_ids: Vec<String>,
    /// Optional policy revisions by configured route identifier.
    #[serde(default)]
    pub policy_revisions: std::collections::BTreeMap<String, Option<String>>,
    /// Legacy single revision.
    #[serde(default)]
    pub policy_revision: Option<String>,
    /// Optional configuration revision.
    pub config_revision: Option<String>,
}
/// Live release acknowledgement from a single replica.
#[derive(Debug, Serialize, Deserialize)]
pub struct PolicyActivationView {
    /// Active or pending.
    pub state: String,
    /// Older releases still in flight.
    pub pending_statements: usize,
}
/// Route policy ownership and active revision.
#[derive(Debug, Serialize, Deserialize)]
pub struct PolicyStatusView {
    /// Live release status, absent on older servers.
    #[serde(default)]
    pub activation: Option<PolicyActivationView>,
    /// Last value-free refusal, absent on older servers.
    #[serde(default)]
    pub last_rejected: Option<super::policy::PolicyRejection>,
    /// Revision actually enforced by the route.
    #[serde(default)]
    pub published_revision: Option<u64>,
    /// File or service ownership.
    pub owner: String,
    /// Active durable revision.
    pub active_revision: Option<crate::policy::RevisionSummary>,
    /// Captured catalog fingerprint.
    pub catalog_fingerprint: String,
}
/// Value-free change metadata retained as server JSON.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyDiffView {
    /// Change counts.
    pub summary: serde_json::Value,
    /// Detailed changes and preview metadata.
    #[serde(flatten)]
    pub changes: std::collections::BTreeMap<String, serde_json::Value>,
}
/// Effective column metadata.
#[derive(Debug, Serialize, Deserialize)]
pub struct PolicyEffectiveView {
    /// Value-free column projections.
    pub columns: Vec<serde_json::Value>,
}
/// Successful policy validation.
#[derive(Debug, Serialize, Deserialize)]
pub struct PolicyValidationView {
    /// Validation outcome.
    pub valid: bool,
    /// Catalog used for validation.
    pub catalog_fingerprint: String,
}
/// Policy publication outcome.
#[derive(Debug, Serialize, Deserialize)]
pub struct PolicyMutationView {
    /// Active or pending, absent on older servers.
    #[serde(default)]
    pub activation: Option<String>,
    /// Older releases still in flight, absent on older servers.
    #[serde(default)]
    pub pending_statements: Option<usize>,
    /// Published revision summary.
    pub revision: crate::policy::RevisionSummary,
    /// Whether a prior request was replayed.
    pub replayed: bool,
}
/// Historical source document with flattened metadata.
#[derive(Serialize, Deserialize)]
pub struct PolicyRevisionView {
    /// Public revision metadata.
    #[serde(flatten)]
    pub revision: crate::policy::RevisionSummary,
    /// Exported source, excluded from diagnostics.
    pub policy_toml: String,
}
/// Exported source and optional durable revision.
#[derive(Serialize, Deserialize)]
pub struct PolicyExportView {
    /// Optional durable revision summary.
    pub revision: Option<crate::policy::RevisionSummary>,
    /// Source document, excluded from diagnostics.
    pub policy_toml: String,
}
/// One-connection-per-request management client.
pub struct MgmtClient {
    endpoint: Endpoint,
    authorization: Option<Zeroizing<String>>,
    tls: Option<Arc<rustls::ClientConfig>>,
    follow_leader_redirect: bool,
}
impl fmt::Debug for MgmtClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("MgmtClient { [REDACTED] }")
    }
}
impl MgmtClient {
    /// Validate authentication and load TLS trust anchors.
    pub fn new(
        endpoint: Endpoint,
        ca_file: Option<&Path>,
        credentials: Credentials,
    ) -> Result<Self, ClientError> {
        let network = matches!(endpoint.0, Destination::Tcp { .. });
        let authorization = match credentials {
            Credentials::Bearer { name, secret } if network => {
                if name.is_empty()
                    || name.len() > 64
                    || !name
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
                {
                    return Err(ClientError::Transport);
                }
                let value = Zeroizing::new(format!("Bearer nunya.{name}.{}", secret.as_str()));
                http::HeaderValue::from_str(&value).map_err(|_| ClientError::Transport)?;
                Some(value)
            }
            Credentials::None if !network => None,
            _ => return Err(ClientError::Transport),
        };
        let tls = if matches!(endpoint.0, Destination::Tcp { tls: true, .. }) {
            let mut roots = rustls::RootCertStore::empty();
            if let Some(path) = ca_file {
                let pem = std::fs::read(path).map_err(|_| ClientError::Tls)?;
                for cert in rustls_pemfile::certs(&mut pem.as_slice()) {
                    roots
                        .add(cert.map_err(|_| ClientError::Tls)?)
                        .map_err(|_| ClientError::Tls)?;
                }
            } else {
                let native = rustls_native_certs::load_native_certs();
                if !native.errors.is_empty() {
                    return Err(ClientError::Tls);
                }
                for cert in native.certs {
                    roots.add(cert).map_err(|_| ClientError::Tls)?;
                }
            }
            if roots.is_empty() {
                return Err(ClientError::Tls);
            }
            Some(Arc::new(
                rustls::ClientConfig::builder_with_provider(Arc::new(
                    rustls::crypto::ring::default_provider(),
                ))
                .with_safe_default_protocol_versions()
                .map_err(|_| ClientError::Tls)?
                .with_root_certificates(roots)
                .with_no_client_auth(),
            ))
        } else {
            None
        };
        Ok(Self {
            endpoint,
            authorization,
            tls,
            follow_leader_redirect: false,
        })
    }
    /// Explicitly permit one HTTPS leader redirect using the original TLS trust roots.
    pub fn follow_leader_redirect(mut self, enabled: bool) -> Self {
        self.follow_leader_redirect = enabled;
        self
    }
    async fn exchange<I>(
        &self,
        io: I,
        request: Request<Full<Bytes>>,
    ) -> Result<(u16, Bytes, Option<String>), ClientError>
    where
        I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(io))
            .await
            .map_err(|_| ClientError::Transport)?;
        // Dropping the guard aborts the driver even on timeout or a malformed body.
        struct Driver(tokio::task::JoinHandle<()>);
        impl Drop for Driver {
            fn drop(&mut self) {
                self.0.abort();
            }
        }
        let _driver = Driver(tokio::spawn(async move {
            let _ = connection.await;
        }));
        let response = sender
            .send_request(request)
            .await
            .map_err(|_| ClientError::Transport)?;
        let status = response.status().as_u16();
        let location = response.headers().get(http::header::LOCATION).and_then(|v| v.to_str().ok()).map(str::to_owned);
        let body = Limited::new(response.into_body(), 1024 * 1024)
            .collect()
            .await
            .map_err(|e| {
                if e.is::<http_body_util::LengthLimitError>() {
                    ClientError::Decode
                } else {
                    ClientError::Transport
                }
            })?
            .to_bytes();
        Ok((status, body, location))
    }
    async fn send_once(
        &self,
        method: &Method,
        path: &str,
        body: Bytes,
        key: Option<&str>,
    ) -> Result<(u16, Bytes, Option<String>), ClientError> {
        let mut request = Request::builder()
            .method(method.clone())
            .uri(path)
            .header("Content-Type", "application/json")
            .header("Connection", "close");
        if let Some(auth) = &self.authorization {
            request = request.header("Authorization", auth.as_str());
        }
        if let Some(key) = key {
            request = request.header("Idempotency-Key", key);
        }
        match &self.endpoint.0 {
            Destination::Tcp {
                host,
                authority,
                port,
                tls,
            } => {
                let request = request
                    .header("Host", authority)
                    .body(Full::new(body))
                    .map_err(|_| ClientError::Transport)?;
                let io = timeout(
                    Duration::from_secs(10),
                    TcpStream::connect((host.as_str(), *port)),
                )
                .await
                .map_err(|_| ClientError::Timeout)?
                .map_err(|_| ClientError::Transport)?;
                if *tls {
                    let name = rustls::pki_types::ServerName::try_from(host.clone())
                        .map_err(|_| ClientError::Tls)?;
                    let connector =
                        tokio_rustls::TlsConnector::from(self.tls.clone().ok_or(ClientError::Tls)?);
                    let io = timeout(Duration::from_secs(10), connector.connect(name, io))
                        .await
                        .map_err(|_| ClientError::Timeout)?
                        .map_err(|_| ClientError::Tls)?;
                    timeout(Duration::from_secs(30), self.exchange(io, request))
                        .await
                        .map_err(|_| ClientError::Timeout)?
                } else {
                    timeout(Duration::from_secs(30), self.exchange(io, request))
                        .await
                        .map_err(|_| ClientError::Timeout)?
                }
            }
            #[cfg(unix)]
            Destination::Unix(path) => {
                let request = request
                    .header("Host", "localhost")
                    .body(Full::new(body))
                    .map_err(|_| ClientError::Transport)?;
                let io = timeout(
                    Duration::from_secs(10),
                    tokio::net::UnixStream::connect(path),
                )
                .await
                .map_err(|_| ClientError::Timeout)?
                .map_err(|_| ClientError::Transport)?;
                timeout(Duration::from_secs(30), self.exchange(io, request))
                    .await
                    .map_err(|_| ClientError::Timeout)?
            }
        }
    }
    fn redirect_endpoint(&self, location: &str) -> Result<(Endpoint, String), ClientError> {
        let uri: Uri = location.parse().map_err(|_| ClientError::Decode)?;
        let scheme = uri.scheme_str().ok_or(ClientError::Decode)?;
        if scheme != "https" || !matches!(self.endpoint.0, Destination::Tcp { tls: true, .. }) {
            return Err(ClientError::LeaderRedirect { location: location.into() });
        }
        let authority = uri.authority().ok_or(ClientError::Decode)?;
        let authority = if authority.port_u16().is_none() { format!("{}:{}", authority, if scheme == "https" {443} else {80}) } else { authority.to_string() };
        if location.contains('#') { return Err(ClientError::Decode); }
        let endpoint = format!("{scheme}://{authority}").parse()?;
        Ok((endpoint, uri.path_and_query().map_or("/", |p| p.as_str()).to_owned()))
    }
    async fn send_with_retry(&self, method: &Method, path: &str, body: Bytes, key: Option<&str>) -> Result<(u16, Bytes, Option<String>), ClientError> {
        let mut attempt = 0;
        loop {
            match self.send_once(method, path, body.clone(), key).await {
                Err(ClientError::Transport | ClientError::Timeout) if method != Method::GET && key.is_some() && attempt < 2 => {
                    sleep(Duration::from_millis(if attempt == 0 { 100 } else { 400 })).await;
                    attempt += 1;
                }
                result => return result,
            }
        }
    }
    async fn once(&self, method: &Method, path: &str, body: Bytes, key: Option<&str>) -> Result<(u16, Bytes), ClientError> {
        let (status, response, location) = self.send_with_retry(method, path, body.clone(), key).await?;
        if status != 307 { return Ok((status, response)); }
        let location = location.ok_or(ClientError::Decode)?;
        if !self.follow_leader_redirect {
            return Err(ClientError::LeaderRedirect { location });
        }
        let (endpoint, path) = self.redirect_endpoint(&location)?;
        let redirected = Self { endpoint, authorization: self.authorization.clone(), tls: self.tls.clone(), follow_leader_redirect: false };
        // Retries stay on this leader with identical bytes/key, without another redirect.
        let (status, response, _) = redirected.send_with_retry(method, &path, body, key).await?;
        if status == 307 { return Err(ClientError::Decode); }
        Ok((status, response))
    }
    async fn call<T: DeserializeOwned>(
        &self,
        method: Method,
        path: String,
        body: serde_json::Value,
    ) -> Result<T, ClientError> {
        self.call_key(method, path, body, None).await
    }
    async fn call_key<T: DeserializeOwned>(&self, method: Method, path: String,
        body: serde_json::Value, supplied_key: Option<String>) -> Result<T, ClientError> {
        let mutation = method != Method::GET;
        let key = if supplied_key.is_some() { supplied_key } else if mutation {
            let mut nonce = [0; 16];
            getrandom::fill(&mut nonce).map_err(|_| ClientError::Transport)?;
            Some(data_encoding::HEXLOWER.encode(&nonce))
        } else {
            None
        };
        let body = Bytes::from(serde_json::to_vec(&body).map_err(|_| ClientError::Decode)?);
        let (status, body) = self.once(&method, &path, body, key.as_deref()).await?;
        if !(200..300).contains(&status) {
            let code = serde_json::from_slice::<serde_json::Value>(&body)
                .ok()
                .and_then(|v| v.get("error").and_then(|e| e.as_str()).map(str::to_owned))
                .unwrap_or_else(|| "unknown".into());
            if status == 409 && code == "stale_revision" {
                #[derive(Deserialize)]
                struct Stale { current: crate::policy::RevisionSummary, diff: PolicyDiffView }
                let stale: Stale = serde_json::from_slice(&body).map_err(|_| ClientError::Decode)?;
                return Err(ClientError::Stale { current: Box::new(stale.current), diff: Box::new(stale.diff) });
            }
            return Err(ClientError::Api { status, code });
        }
        serde_json::from_slice(&body).map_err(|_| ClientError::Decode)
    }
    /// Acquire a session using locally derived credentials.
    pub async fn acquire(&self, params: AcquireParams) -> Result<SessionView, ClientError> {
        self.call(
            Method::POST,
            "/v1/sessions".into(),
            serde_json::to_value(params).map_err(|_| ClientError::Decode)?,
        )
        .await
    }
    /// Read one session.
    pub async fn session(&self, id: &str) -> Result<SessionView, ClientError> {
        self.call(Method::GET, item("sessions", id)?, json!({}))
            .await
    }
    /// List visible sessions.
    pub async fn sessions(&self) -> Result<Vec<SessionView>, ClientError> {
        self.call(Method::GET, "/v1/sessions".into(), json!({}))
            .await
    }
    /// Renew a session, optionally adding a derived credential.
    pub async fn renew(
        &self,
        id: &str,
        mode: Option<Mode>,
        add_credential: Option<SessionCredential>,
    ) -> Result<SessionView, ClientError> {
        self.call(
            Method::POST,
            format!("{}/renew", item("sessions", id)?),
            json!({"mode":mode,"add_credential":add_credential}),
        )
        .await
    }
    /// Revoke a session.
    pub async fn revoke(&self, id: &str) -> Result<SessionView, ClientError> {
        self.call(
            Method::POST,
            format!("{}/revoke", item("sessions", id)?),
            json!({}),
        )
        .await
    }
    /// Create a principal with an optional initial verifier.
    pub async fn create_principal(
        &self,
        name: &str,
        kind: PrincipalKind,
        admin: bool,
        verifier: Option<&str>,
    ) -> Result<PrincipalView, ClientError> {
        self.call(
            Method::POST,
            "/v1/principals".into(),
            json!({"name":name,"kind":kind,"admin":admin,"verifier":verifier}),
        )
        .await
    }
    /// List principal records.
    pub async fn principals(&self) -> Result<Vec<PrincipalView>, ClientError> {
        self.call(Method::GET, "/v1/principals".into(), json!({}))
            .await
    }
    /// Read a principal including grants and credentials.
    pub async fn principal(&self, id: &str) -> Result<PrincipalView, ClientError> {
        self.call(Method::GET, item("principals", id)?, json!({}))
            .await
    }
    /// Disable a principal.
    pub async fn disable(&self, id: &str) -> Result<PrincipalView, ClientError> {
        self.call(
            Method::POST,
            format!("{}/disable", item("principals", id)?),
            json!({}),
        )
        .await
    }
    /// Enable a principal.
    pub async fn enable(&self, id: &str) -> Result<PrincipalView, ClientError> {
        self.call(
            Method::POST,
            format!("{}/enable", item("principals", id)?),
            json!({}),
        )
        .await
    }
    /// Add a principal verifier with an optional expiration duration.
    pub async fn add_credential(
        &self,
        id: &str,
        verifier: &str,
        expires: Option<&str>,
    ) -> Result<CredentialSummary, ClientError> {
        self.call(
            Method::POST,
            format!("{}/credentials", item("principals", id)?),
            json!({"verifier":verifier,"expires":expires}),
        )
        .await
    }
    /// Revoke a principal credential.
    pub async fn delete_credential(
        &self,
        id: &str,
        cid: &str,
    ) -> Result<CredentialSummary, ClientError> {
        valid_id(cid)?;
        self.call(
            Method::DELETE,
            format!("{}/credentials/{cid}", item("principals", id)?),
            json!({}),
        )
        .await
    }
    /// Replace a principal's route grants.
    pub async fn set_grants(
        &self,
        id: &str,
        grants: &[(&str, Mode)],
    ) -> Result<PrincipalView, ClientError> {
        let grants: Vec<_> = grants
            .iter()
            .map(|(route, mode)| json!({"route":route,"mode":mode}))
            .collect();
        self.call(
            Method::PUT,
            format!("{}/grants", item("principals", id)?),
            json!({"grants":grants}),
        )
        .await
    }
    /// List active connections.
    pub async fn connections(&self) -> Result<Vec<ConnectionView>, ClientError> {
        self.call(Method::GET, "/v1/connections".into(), json!({}))
            .await
    }
    /// Read policy status.
    pub async fn policy_status(&self, route: &str) -> Result<PolicyStatusView, ClientError> {
        self.call(Method::GET, policy_path(route)?, json!({})).await
    }
    /// Read policy effective.
    pub async fn policy_effective(&self, route: &str) -> Result<PolicyEffectiveView, ClientError> {
        self.call(Method::GET, format!("{}/effective", policy_path(route)?), json!({})).await
    }
    /// Read policy revisions.
    pub async fn policy_revisions(&self, route: &str) -> Result<Vec<crate::policy::RevisionSummary>, ClientError> {
        self.call(Method::GET, format!("{}/revisions", policy_path(route)?), json!({})).await
    }
    /// Read policy export.
    pub async fn policy_export(&self, route: &str) -> Result<PolicyExportView, ClientError> {
        self.call(Method::GET, format!("{}/export", policy_path(route)?), json!({})).await
    }
    /// Validate a draft without publication.
    pub async fn policy_validate(&self, route: &str, toml: &str) -> Result<PolicyValidationView, ClientError> {
        self.call(Method::POST, format!("{}/validate", policy_path(route)?), json!({"policy_toml":toml})).await
    }
    /// Preview a draft without publication.
    pub async fn policy_preview(&self, route: &str, toml: &str) -> Result<PolicyDiffView, ClientError> {
        self.call(Method::POST, format!("{}/preview", policy_path(route)?), json!({"policy_toml":toml})).await
    }
    /// Read one historical revision.
    pub async fn policy_revision(&self, route: &str, n: u64) -> Result<PolicyRevisionView, ClientError> {
        self.call(Method::GET, format!("{}/revisions/{n}", policy_path(route)?), json!({})).await
    }
    /// Publish a draft, returning the retry key alongside its outcome.
    pub async fn policy_apply(&self, route: &str, toml: &str, expected_revision: u64,
        catalog_fingerprint: &str, idempotency_key: Option<String>) -> Result<(PolicyMutationView, String), ClientError> {
        self.policy_mutation(route, "apply", json!({"policy_toml":toml,"expected_revision":expected_revision,"catalog_fingerprint":catalog_fingerprint}), idempotency_key).await
    }
    /// Restore a revision with an optional retry key.
    pub async fn policy_rollback(&self, route: &str, to: u64, expected_revision: u64, key: Option<String>) -> Result<(PolicyMutationView, String), ClientError> {
        self.policy_mutation(route, "rollback", json!({"to":to,"expected_revision":expected_revision}), key).await
    }
    /// Import the configured file into service ownership.
    pub async fn policy_import(&self, route: &str, key: Option<String>) -> Result<(PolicyMutationView, String), ClientError> {
        self.policy_mutation(route, "import", json!({}), key).await
    }
    async fn policy_mutation(&self, route: &str, op: &str, body: serde_json::Value, key: Option<String>) -> Result<(PolicyMutationView, String), ClientError> {
        let key = match key { Some(key) => key, None => {
            let mut nonce = [0;16];
            getrandom::fill(&mut nonce).map_err(|_| ClientError::Transport)?;
            data_encoding::HEXLOWER.encode(&nonce)
        }};
        let result = self.call_key(Method::POST, format!("{}/{op}", policy_path(route)?), body, Some(key.clone())).await?;
        Ok((result, key))
    }
    /// Read server status.
    pub async fn status(&self) -> Result<StatusView, ClientError> {
        self.call(Method::GET, "/v1/status".into(), json!({})).await
    }
}
fn valid_id(id: &str) -> Result<(), ClientError> {
    if id.len() == 32 && id.bytes().all(|b| b.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err(ClientError::Transport)
    }
}
fn item(collection: &str, id: &str) -> Result<String, ClientError> {
    valid_id(id)?;
    Ok(format!("/v1/{collection}/{id}"))
}

fn policy_path(route: &str) -> Result<String, ClientError> {
    crate::state::model::RouteId::new(route).ok_or(ClientError::Transport)?;
    Ok(format!("/v1/routes/{route}/policy"))
}

#[cfg(test)]
mod tests {
    #[test]
    fn generated_rotation_parameters() -> Result<(), super::ClientError> {
        use super::*;
        assert_eq!(
            SessionSecret::generate_with(b"", 1).unwrap_err(),
            ClientError::Decode
        );
        assert_eq!(
            SessionSecret::generate_with(b"salt", 0).unwrap_err(),
            ClientError::Decode
        );
        let secret = SessionSecret::generate_with(b"reused-salt", 2)?;
        let verifier = ScramVerifier::parse(&secret.verifier).ok_or(ClientError::Decode)?;
        assert_eq!(verifier.salt, b"reused-salt");
        assert_eq!(verifier.iterations, 2);
        Ok(())
    }

    use super::*;
    #[test]
    fn endpoint_parsing() {
        for accepted in [
            "http://127.0.0.1:80",
            "http://[::1]:80",
            "http://localhost:9",
            "https://example.org:443",
        ] {
            assert!(accepted.parse::<Endpoint>().is_ok());
        }
        for refused in [
            "http://10.0.0.1:1",
            "http://example.org:80",
            "https://host",
            "https://host:443/path",
            "https://user@host:443",
            "ftp://localhost:5",
            "https://host:443/?x=y",
            "https://host:443/#x",
            "unix:relative",
        ] {
            let e = refused.parse::<Endpoint>().unwrap_err();
            assert_eq!(e.to_string(), "management transport unavailable");
        }
        assert_eq!(
            "unix:/tmp/nunya.sock".parse::<Endpoint>().is_ok(),
            cfg!(unix)
        );
    }
    async fn read_request<I: AsyncRead + Unpin>(io: &mut I) -> Vec<u8> {
        use tokio::io::AsyncReadExt;
        let mut request = Vec::new();
        let mut byte = [0];
        while !request.ends_with(b"\r\n\r\n") {
            assert_eq!(io.read(&mut byte).await.unwrap(), 1);
            request.push(byte[0]);
        }
        let headers = String::from_utf8(request.clone()).unwrap();
        let length: usize = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse().unwrap())
            })
            .unwrap_or(0);
        let mut body = vec![0; length];
        io.read_exact(&mut body).await.unwrap();
        request.extend(body);
        request
    }
    fn test_client(addr: std::net::SocketAddr) -> MgmtClient {
        MgmtClient::new(
            format!("http://{addr}").parse().unwrap(),
            None,
            Credentials::Bearer {
                name: "test".into(),
                secret: Zeroizing::new("private".into()),
            },
        )
        .unwrap()
    }
    fn tls_pair() -> (Arc<rustls::ServerConfig>, Arc<rustls::ClientConfig>) {
        let certified = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let cert = certified.cert.der().clone();
        let key = rustls::pki_types::PrivatePkcs8KeyDer::from(certified.key_pair.serialize_der());
        let server = rustls::ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions().unwrap().with_no_client_auth()
            .with_single_cert(vec![cert.clone()], key.into()).unwrap();
        let mut roots = rustls::RootCertStore::empty();
        roots.add(cert).unwrap();
        let client = rustls::ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions().unwrap().with_root_certificates(roots).with_no_client_auth();
        (Arc::new(server), Arc::new(client))
    }
    #[tokio::test]
    async fn redirects_default_refuses_without_contacting_leader() {
        use tokio::{io::AsyncWriteExt, net::TcpListener};
        let first = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let second = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = test_client(first.local_addr().unwrap());
        let target = format!("https://localhost:{}/new", second.local_addr().unwrap().port());
        let location = target.clone();
        let server = tokio::spawn(async move {
            let (mut io, _) = first.accept().await.unwrap();
            read_request(&mut io).await;
            io.write_all(format!("HTTP/1.1 307 Temporary Redirect\r\nLocation: {location}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
        });
        let result = client.call::<serde_json::Value>(Method::GET, "/old".into(), json!({})).await;
        assert_eq!(result, Err(ClientError::LeaderRedirect { location: target }));
        assert!(timeout(Duration::from_millis(50), second.accept()).await.is_err());
        server.await.unwrap();
    }
    #[tokio::test]
    async fn redirects_https_same_roots_once_and_retry_leader_with_same_key() {
        use tokio::{io::AsyncWriteExt, net::TcpListener};
        for (second_redirect, retry) in [(false, false), (true, false), (false, true)] {
            let first = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let second = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let (server_tls, client_tls) = tls_pair();
            let mut client = test_client(first.local_addr().unwrap()).follow_leader_redirect(true);
            client.endpoint = format!("https://localhost:{}", first.local_addr().unwrap().port()).parse().unwrap();
            client.tls = Some(client_tls);
            let target = format!("https://localhost:{}/new?x=1", second.local_addr().unwrap().port());
            let server = tokio::spawn(async move {
                let acceptor = tokio_rustls::TlsAcceptor::from(server_tls);
                let (io, _) = first.accept().await.unwrap();
                let mut io = acceptor.accept(io).await.unwrap();
                let original = read_request(&mut io).await;
                io.write_all(format!("HTTP/1.1 307 Temporary Redirect\r\nLocation: {target}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
                io.flush().await.unwrap();
                let mut requests = Vec::new();
                for attempt in 0..=usize::from(retry) {
                    let (io, _) = second.accept().await.unwrap();
                    let mut io = acceptor.accept(io).await.unwrap();
                    requests.push(read_request(&mut io).await);
                    if retry && attempt == 0 { continue; }
                    let response = if second_redirect { format!("HTTP/1.1 307 Temporary Redirect\r\nLocation: {target}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n") } else { "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}".into() };
                    io.write_all(response.as_bytes()).await.unwrap();
                    io.flush().await.unwrap();
                }
                (original, requests)
            });
            let result = client.call_key::<serde_json::Value>(Method::POST, "/old".into(), json!({"payload":1}), Some("retry-key".into())).await;
            if second_redirect { assert_eq!(result, Err(ClientError::Decode)); } else { assert!(result.is_ok()); }
            let (original, requests) = server.await.unwrap();
            for redirected in &requests {
                let redirected = String::from_utf8(redirected.clone()).unwrap();
                assert!(redirected.starts_with("POST /new?x=1 HTTP/1.1"));
                assert_eq!(String::from_utf8(original.clone()).unwrap().split_once("\r\n\r\n").unwrap().1, redirected.split_once("\r\n\r\n").unwrap().1);
                assert!(redirected.contains("idempotency-key: retry-key"));
                assert!(redirected.contains("authorization: Bearer nunya.test.private"));
            }
            if retry { assert_eq!(requests[0], requests[1]); }
        }
    }
    #[test]
    fn redirect_schemes_are_validated() {
        let mut client = test_client("127.0.0.1:1".parse().unwrap()).follow_leader_redirect(true);
        assert!(client.redirect_endpoint("https://leader/v1/status").is_err());
        assert!(client.redirect_endpoint("http://127.0.0.1:2/v1/status").is_err());
        client.endpoint = "https://leader:443".parse().unwrap();
        assert!(client.redirect_endpoint("https://leader/v1/status").is_ok());
        for target in ["http://127.0.0.1:2/v1/status", "ftp://leader/v1/status", "/relative", "https://user@leader/v1/status"] {
            assert!(client.redirect_endpoint(target).is_err());
        }
    }
    #[tokio::test]
    async fn mutations_retry_identical_bytes_and_stop_after_three_attempts() {
        use tokio::{io::AsyncWriteExt, net::TcpListener};
        for succeed in [false, true] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let client = test_client(listener.local_addr().unwrap());
            let server = tokio::spawn(async move {
                let mut requests = Vec::new();
                for attempt in 0..3 {
                    let (mut io, _) = listener.accept().await.unwrap();
                    requests.push(read_request(&mut io).await);
                    if succeed && attempt == 2 {
                        io.write_all(
                            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}",
                        )
                        .await
                        .unwrap();
                    }
                }
                requests
            });
            let result = client
                .call::<serde_json::Value>(
                    Method::POST,
                    "/v1/sessions".into(),
                    json!({"verifier":"derived"}),
                )
                .await;
            assert_eq!(result.is_ok(), succeed);
            let requests = server.await.unwrap();
            assert_eq!(requests[0], requests[1]);
            assert_eq!(requests[1], requests[2]);
            let request = String::from_utf8(requests[0].clone()).unwrap();
            let key = request
                .lines()
                .find_map(|l| l.strip_prefix("idempotency-key: "))
                .unwrap();
            assert_eq!(key.len(), 32);
            assert!(key.bytes().all(|b| b.is_ascii_hexdigit()));
        }
    }
    #[tokio::test]
    async fn api_and_decode_failures_are_not_retried_and_bodies_are_bounded() {
        use tokio::{io::AsyncWriteExt, net::TcpListener};
        for (response, expected) in [
            (
                b"HTTP/1.1 403 Forbidden\r\nContent-Length: 20\r\n\r\n{\"error\":\"no_grant\"}"
                    .as_slice(),
                ClientError::Api {
                    status: 403,
                    code: "no_grant".into(),
                },
            ),
            (
                b"HTTP/1.1 503 Unavailable\r\nContent-Length: 2\r\n\r\n{}".as_slice(),
                ClientError::Api {
                    status: 503,
                    code: "unknown".into(),
                },
            ),
            (
                b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\nx".as_slice(),
                ClientError::Decode,
            ),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let client = test_client(listener.local_addr().unwrap());
            let server = tokio::spawn(async move {
                let (mut io, _) = listener.accept().await.unwrap();
                read_request(&mut io).await;
                io.write_all(response).await.unwrap();
                drop(io);
                assert!(
                    timeout(Duration::from_millis(700), listener.accept())
                        .await
                        .is_err()
                );
            });
            assert_eq!(
                client
                    .call::<serde_json::Value>(Method::POST, "/v1/sessions".into(), json!({}))
                    .await
                    .unwrap_err(),
                expected
            );
            server.await.unwrap();
        }
        // A streaming body larger than the cap is refused without buffering it all.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = test_client(listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut io, _) = listener.accept().await.unwrap();
            read_request(&mut io).await;
            io.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1048577\r\n\r\n")
                .await
                .unwrap();
            let _ = io.write_all(&vec![b' '; 1048577]).await;
        });
        assert_eq!(client.status().await.unwrap_err(), ClientError::Decode);
        server.await.unwrap();
    }
    #[tokio::test]
    async fn policy_apply_replay_and_typed_stale() {
        use tokio::{net::TcpListener, io::AsyncWriteExt};
        let revision = json!({"number":2,"route":"pg","created_unix_ms":1,"policy_sha256":"hash","catalog_fingerprint":"catalog","actor":"admin","kind":"Apply","idempotency_key":"retry","request_digest":null});
        let diff = json!({"summary":{"newly_visible":1},"newly_visible":[{"schema":"public","table":"t","column":"c"}]});
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = test_client(listener.local_addr().unwrap());
        let expected_revision = revision.clone(); let expected_diff = diff.clone();
        let server = tokio::spawn(async move {
            let mut previous = None;
            for attempt in 0..4 {
                let (mut io, _) = listener.accept().await.unwrap();
                let request = read_request(&mut io).await;
                let text = String::from_utf8(request.clone()).unwrap();
                assert!(text.starts_with("POST /v1/routes/pg/policy/apply "));
                assert!(text.contains("idempotency-key: retry"));
                if let Some(previous) = &previous { assert_eq!(&request, previous); }
                previous = Some(request);
                let body = if attempt == 3 { json!({"error":"file_owner_configured"}) } else if attempt == 2 { json!({"error":"stale_revision","current":revision,"diff":diff}) }
                    else if attempt == 1 { json!({"revision":revision,"replayed":true,"activation":"pending","pending_statements":1}) }
                    else { json!({"revision":revision,"replayed":false}) }.to_string();
                let status = if attempt >= 2 { "409 Conflict" } else { "200 OK" };
                let response = format!("HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
                io.write_all(response.as_bytes()).await.unwrap();
            }
        });
        for replayed in [false, true] {
            let (result, key) = client.policy_apply("pg", "default='show'", 1, "catalog", Some("retry".into())).await.unwrap();
            assert_eq!(key, "retry"); assert_eq!(result.replayed, replayed); assert_eq!(result.revision.number, 2);
            assert_eq!(result.activation.as_deref(), if replayed { Some("pending") } else { None });
            assert_eq!(result.pending_statements, if replayed { Some(1) } else { None });
        }
        match client.policy_apply("pg", "default='show'", 1, "catalog", Some("retry".into())).await.unwrap_err() {
            ClientError::Stale { current, diff } => {
                assert_eq!(serde_json::to_value(current).unwrap(), expected_revision);
                assert_eq!(serde_json::to_value(diff).unwrap(), expected_diff);
            }
            _ => panic!("expected typed stale error"),
        }
        assert_eq!(client.policy_apply("pg", "default='show'", 1, "catalog", Some("retry".into())).await.unwrap_err(), ClientError::Api { status: 409, code: "file_owner_configured".into() });
        server.await.unwrap();
    }
    #[test]
    fn policy_status_accepts_old_and_new_activation_fields() {
        let old = json!({"owner":"service","active_revision":null,"catalog_fingerprint":"fp"});
        let status: PolicyStatusView = serde_json::from_value(old.clone()).unwrap();
        assert!(status.activation.is_none()); assert!(status.last_rejected.is_none());
        let mut new = old;
        new["activation"] = json!({"state":"pending","pending_statements":1});
        new["last_rejected"] = json!({"at_ms":123,"operation":"apply","code":"invalid_policy","expected_revision":2});
        let status: PolicyStatusView = serde_json::from_value(new).unwrap();
        assert_eq!(status.activation.unwrap().pending_statements, 1);
        assert_eq!(status.last_rejected.unwrap().code, "invalid_policy");
    }
    #[test]
    fn status_accepts_legacy_and_route_revision_shapes() {
        let old: StatusView = serde_json::from_value(json!({
            "deployment_id":"id", "store_format":1, "route_ids":["pg"],
            "policy_revision":"R1", "config_revision":"C1"
        })).unwrap();
        assert!(old.policy_revisions.is_empty());
        assert_eq!(old.policy_revision.as_deref(), Some("R1"));
        let new: StatusView = serde_json::from_value(json!({
            "deployment_id":"id", "store_format":1, "route_ids":["pg"],
            "policy_revisions":{"pg":"R2"}, "config_revision":"C1"
        })).unwrap();
        assert_eq!(new.policy_revisions["pg"].as_deref(), Some("R2"));
        assert_eq!(new.policy_revision, None);
    }

}

