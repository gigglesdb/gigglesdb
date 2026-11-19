//! Strict configuration loading with data-free diagnostics.
use crate::pgwire::auth::ScramVerifier;
use serde::{Deserialize, de::DeserializeOwned};
use std::{
    fmt,
    io::BufReader,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use toml::{Value, map::Map};
use zeroize::Zeroizing;

/// Upstream encryption mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum UpstreamTls {
    /// Plaintext transport.
    Disable,
    /// Encryption without peer verification.
    Require,
    /// Encryption with CA and hostname verification.
    VerifyFull,
}
/// Upstream SQL catalog dialect.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum UpstreamDialect {
    /// PostgreSQL catalog.
    #[default]
    Postgres,
    /// Spanner PostgreSQL catalog through PGAdapter.
    SpannerPg,
}
/// Inbound TLS files.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsSection {
    /// Resolved certificate chain path.
    pub cert_file: PathBuf,
    /// Resolved private key path.
    pub key_file: PathBuf,
}
/// Standing authenticated identity.
#[derive(Debug)]
pub struct StandingSection {
    /// Login name.
    pub name: String,
    /// Parsed SCRAM verifier.
    pub verifier: ScramVerifier,
}
/// Parse a positive integer duration with a single seconds, minutes, hours or days unit.
pub fn parse_duration(text: &str) -> Option<Duration> {
    let (unit, digits) = text.as_bytes().split_last()?;
    if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }
    let multiplier = match unit {
        b's' => 1,
        b'm' => 60,
        b'h' => 3600,
        b'd' => 86400,
        _ => return None,
    };
    let seconds = std::str::from_utf8(digits)
        .ok()?
        .parse::<u64>()
        .ok()?
        .checked_mul(multiplier)?;
    (seconds != 0).then(|| Duration::from_secs(seconds))
}
/// Session expiration policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionSettings {
    /// Maximum permitted session lifetime.
    pub max_lifetime: Duration,
    /// Idle expiration interval; absent when disabled.
    pub idle_timeout: Option<Duration>,
}
impl Default for SessionSettings {
    fn default() -> Self {
        Self {
            max_lifetime: Duration::from_secs(8 * 3600),
            idle_timeout: Some(Duration::from_secs(3600)),
        }
    }
}
/// Reason a requested session lifetime was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifetimeError {
    /// A lifetime must be positive.
    Zero,
    /// A lifetime must not exceed the configured maximum.
    AboveMaximum,
}
impl fmt::Display for LifetimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Zero => "session lifetime must be positive",
            Self::AboveMaximum => "session lifetime exceeds maximum",
        })
    }
}
impl SessionSettings {
    /// Validate a requested lifetime or select the configured maximum.
    pub fn lifetime_for(&self, requested: Option<Duration>) -> Result<Duration, LifetimeError> {
        match requested {
            None => Ok(self.max_lifetime),
            Some(d) if d.is_zero() => Err(LifetimeError::Zero),
            Some(d) if d > self.max_lifetime => Err(LifetimeError::AboveMaximum),
            Some(d) => Ok(d),
        }
    }
}
/// Management listener settings.
pub struct ManagementSection {
    /// Validated listener address.
    pub listen: SocketAddr,
    /// Resolved and validated TLS files.
    pub tls: Option<TlsSection>,
    /// Management identities.
    pub admins: Vec<AdminSection>,
}
impl fmt::Debug for ManagementSection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ManagementSection { [REDACTED] }")
    }
}
/// Authenticated management identity.
pub struct AdminSection {
    /// Login name.
    pub name: String,
    /// Parsed SCRAM verifier.
    pub verifier: ScramVerifier,
}
impl fmt::Debug for AdminSection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AdminSection { [REDACTED] }")
    }
}
/// Policy revision backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum PolicyStore {
    /// Local durable state.
    #[default]
    Local,
    /// Kubernetes custom resources.
    Kubernetes,
}
/// Kubernetes policy backend and election settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KubernetesPolicyStore {
    /// Namespace containing policy resources and Lease.
    pub namespace: String,
    /// Election Lease name.
    pub lease_name: String,
    /// Unique candidate pod name.
    pub pod_name: String,
    /// Advertised management base URL.
    pub management_url: String,
    /// Lease lifetime in seconds.
    pub lease_duration_secs: u64,
    /// Local renewal deadline in seconds.
    pub renew_deadline_secs: u64,
    /// Election retry interval in seconds.
    pub retry_period_secs: u64,
}
/// Validated configuration with zeroizing secrets.
pub struct Config {
    /// Revision backend selection.
    pub policy_store: PolicyStore,
    /// Kubernetes backend settings.
    pub policy_store_kubernetes: Option<KubernetesPolicyStore>,
    /// Routes explicitly returned to file ownership at startup.
    pub file_policy_routes: Vec<String>,
    /// Service settings resolved with each route policy.
    pub policy_settings: crate::policy::ServicePolicySettings,
    /// Configuration path passed to the loader, as given.
    pub path: PathBuf,
    /// SHA-256 of the startup config file bytes.
    pub revision: String,
    /// Agent listener.
    pub listen: Option<SocketAddr>,
    /// Administration listener.
    pub admin_listen: SocketAddr,
    /// Effective PostgreSQL route policy path, when configured.
    pub postgres_policy: Option<PathBuf>,
    /// Effective native Spanner route policy path, when configured.
    pub spanner_policy: Option<PathBuf>,
    /// Root key bytes erased on drop.
    pub root_key: Zeroizing<[u8; 32]>,
    /// Resolved state directory.
    pub state_dir: PathBuf,
    /// Selected schemas.
    pub schemas: Vec<String>,
    /// Name resolution search path.
    pub search_path: Vec<String>,
    /// Refresh interval in milliseconds.
    pub snapshot_refresh_ms: u64,
    /// Optional policy polling interval in milliseconds.
    pub policy_watch_ms: Option<u64>,
    /// Permit inbound plaintext.
    pub allow_plaintext: bool,
    /// Inbound TLS configuration.
    pub tls: Option<TlsSection>,
    /// Upstream database settings.
    pub upstream: Option<crate::pgwire::upstream::UpstreamConfig>,
    /// Optional native Spanner listener.
    pub spanner_native: Option<crate::spanner_serve::SpannerNativeSection>,
    /// Catalog dialect.
    pub dialect: UpstreamDialect,
    /// Maximum concurrent agent sessions.
    pub max_connections: usize,
    /// Session expiration settings.
    pub session: SessionSettings,
    /// Optional management listener settings.
    pub management: Option<ManagementSection>,
    /// PostgreSQL route identity, when configured.
    pub postgres_route_id: Option<String>,
    /// Native Spanner route identity, when configured.
    pub spanner_route_id: Option<String>,
    /// Standing identities.
    pub standing: Vec<StandingSection>,
}
impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Config { [REDACTED] }")
    }
}
impl Config {
    /// Read and validate a configuration file.
    pub fn load(path: &Path) -> Result<Self, Vec<String>> {
        load(path)
    }
    /// Copy settings into the session upstream configuration.
    pub fn upstream_config(&self) -> Option<crate::pgwire::upstream::UpstreamConfig> {
        use crate::pgwire::upstream::{UpstreamConfig, UpstreamTls};
        let u = self.upstream.as_ref()?;
        Some(UpstreamConfig {
            host: u.host.clone(),
            port: u.port,
            user: u.user.clone(),
            password: u.password.clone(),
            dbname: u.dbname.clone(),
            application_name: u.application_name.clone(),
            connect_timeout_ms: u.connect_timeout_ms,
            tls: match &u.tls {
                UpstreamTls::Disable => UpstreamTls::Disable,
                UpstreamTls::Require => UpstreamTls::Require,
                UpstreamTls::VerifyFull { ca_file } => UpstreamTls::VerifyFull {
                    ca_file: ca_file.clone(),
                },
            },
        })
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawStanding {
    name: String,
    verifier: String,
}
macro_rules! strict_section {
    ($name:ident, $($field:ident => $key:literal),+ $(,)?) => {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct $name { $(#[serde(rename = $key)] $field: Option<Value>),+ }
    };
}
strict_section!(RawConfig,
    _store=>"policy_store", _kube=>"policy_store_kubernetes",
    _readlog=>"readlog", _age=>"snapshot_max_age_ms",
    _watch=>"policy_watch_ms", _max=>"max_connections", _listen=>"listen", _admin=>"admin_listen", _policy=>"policy",
    _root=>"root_key_file", _state=>"state_dir", _schemas=>"schemas",
    _search=>"search_path", _refresh=>"snapshot_refresh_ms",
    _plaintext=>"allow_plaintext", _tls=>"tls", _upstream=>"upstream", _standing=>"standing", _spanner=>"spanner_native", _management=>"management", _session=>"session");
strict_section!(RawUpstream,
    _dialect=>"dialect", _host=>"host", _port=>"port", _user=>"user", _password=>"password_file",
    _dbname=>"dbname", _tls=>"tls", _ca=>"ca_file", _timeout=>"connect_timeout_ms",
    _application=>"application_name", _route=>"route_id", _policy=>"policy");
fn err(e: &mut Vec<String>, field: &str, why: &str) {
    e.push(format!("{field}: {why}"));
}
fn table(v: Value, keys: &[&str], field: &str, e: &mut Vec<String>) -> Map<String, Value> {
    let Value::Table(mut m) = v else {
        err(e, field, "expected table");
        return Map::new();
    };
    let bad: Vec<_> = m
        .keys()
        .filter(|k| !keys.contains(&k.as_str()))
        .cloned()
        .collect();
    for k in bad {
        m.remove(&k);
        err(e, field, "unknown field");
    }
    m
}
fn get<T: DeserializeOwned>(
    m: &mut Map<String, Value>,
    key: &str,
    e: &mut Vec<String>,
) -> Option<T> {
    m.remove(key).and_then(|v| match v.try_into() {
        Ok(v) => Some(v),
        Err(_) => {
            err(e, key, "invalid value or type");
            None
        }
    })
}
fn req<T: DeserializeOwned + Default>(
    m: &mut Map<String, Value>,
    key: &str,
    e: &mut Vec<String>,
) -> T {
    if !m.contains_key(key) {
        err(e, key, "required field missing");
    }
    get(m, key, e).unwrap_or_default()
}
fn resolve(base: &Path, p: PathBuf) -> PathBuf {
    if p.is_absolute() { p } else { base.join(p) }
}
fn path_field(m: &mut Map<String, Value>, field: &str, e: &mut Vec<String>) -> Option<PathBuf> {
    get::<PathBuf>(m, field, e).and_then(|p| {
        if p.as_os_str().is_empty() {
            err(e, field, "path must be nonempty");
            None
        } else {
            Some(p)
        }
    })
}
fn secret(p: Option<PathBuf>, env: &str) -> Option<Zeroizing<Vec<u8>>> {
    match p {
        Some(p) => std::fs::read(p).ok().map(Zeroizing::new),
        None => std::env::var(env)
            .ok()
            .map(|s| Zeroizing::new(s.into_bytes())),
    }
}
fn key(bytes: &[u8]) -> Option<Zeroizing<[u8; 32]>> {
    let mut out = Zeroizing::new([0; 32]);
    if bytes.len() == 32 {
        out.copy_from_slice(bytes);
        return Some(out);
    }
    let text = std::str::from_utf8(bytes).ok()?.trim();
    if text.len() != 64 {
        return None;
    }
    for (slot, pair) in out.iter_mut().zip(text.as_bytes().as_chunks::<2>().0) {
        let digit = |b: u8| match b {
            b'0'..=b'9' => Some(b - b'0'),
            b'a'..=b'f' => Some(b - b'a' + 10),
            b'A'..=b'F' => Some(b - b'A' + 10),
            _ => None,
        };
        *slot = digit(pair[0])? * 16 + digit(pair[1])?;
    }
    Some(out)
}
/// Include search-path schemas in explicit capture sets, preserving order and all-schema capture.
fn effective_schemas(schemas: Vec<String>, search_path: &[String]) -> Vec<String> {
    if schemas.is_empty() {
        return schemas;
    }
    let mut effective = Vec::new();
    for schema in schemas.iter().chain(search_path) {
        if !effective.contains(schema) {
            effective.push(schema.clone());
        }
    }
    effective
}

/// Read settings and collect independent validation failures without source snippets.
pub fn load(path: &Path) -> Result<Config, Vec<String>> {
    let text = Zeroizing::new(
        std::fs::read_to_string(path).map_err(|_| vec!["config: cannot read file".into()])?,
    );
    let v = toml::from_str::<Value>(&text).map_err(|_| vec!["config: invalid TOML".into()])?;
    let mut e = Vec::new();
    let mut m = table(
        v,
        &[
            "policy_store",
            "policy_store_kubernetes",
            "readlog",
            "snapshot_max_age_ms",
            "policy_watch_ms",
            "max_connections",
            "listen",
            "admin_listen",
            "policy",
            "root_key_file",
            "state_dir",
            "schemas",
            "search_path",
            "snapshot_refresh_ms",
            "allow_plaintext",
            "tls",
            "upstream",
            "standing",
            "spanner_native",
            "management",
            "session",
        ],
        "config",
        &mut e,
    );
    if Value::Table(m.clone()).try_into::<RawConfig>().is_err() {
        err(&mut e, "config", "invalid fields");
    }
    let policy_store = get(&mut m, "policy_store", &mut e).unwrap_or_default();
    let policy_store_kubernetes = m.remove("policy_store_kubernetes").map(|v| parse_kubernetes_store(v, &mut e));
    if policy_store == PolicyStore::Kubernetes {
        if policy_store_kubernetes.is_none() {
            err(&mut e, "policy_store_kubernetes", "required for kubernetes policy store");
        }
        #[cfg(not(feature = "kubernetes"))]
        err(&mut e, "policy_store", "built without the kubernetes feature");
    } else if policy_store_kubernetes.is_some() {
        err(&mut e, "policy_store_kubernetes", "only valid with kubernetes policy store");
    }
    let base = path.parent().unwrap_or_else(|| Path::new("."));
    let mut policy_errors = Vec::new();
    let mut readlog = m.remove("readlog").map(|value|
        crate::policy::file::parse_log(value, &mut policy_errors));
    e.extend(policy_errors.into_iter().map(|error| error.message));
    if let Some(crate::policy::ReadLogSink::File { path, .. }) = &mut readlog {
        *path = resolve(base, path.clone());
    }
    let snapshot_max_age_ms = m.remove("snapshot_max_age_ms").and_then(|value| {
        let age = value.as_integer().and_then(|age| u64::try_from(age).ok());
        if age.is_none() {
            err(&mut e, "snapshot_max_age_ms", "must be a nonnegative integer");
        }
        age
    });
    let policy_settings = crate::policy::ServicePolicySettings { readlog, snapshot_max_age_ms };
    let has_upstream = m.contains_key("upstream");
    let listen = get(&mut m, "listen", &mut e);
    if has_upstream && listen.is_none() {
        err(&mut e, "listen", "valid address required for upstream");
    }
    let has_spanner = m.contains_key("spanner_native");
    let mut spanner_file_owner = false;
    let mut spanner_policy = None;
    let mut spanner_route_id = None;
    let spanner_native = m.remove("spanner_native").and_then(|mut v| {
        if let Value::Table(t) = &mut v {
            spanner_file_owner = file_owner(t, "spanner_native.policy_owner", &mut e);
            spanner_policy = route_policy(t, "spanner_native.policy", base, &mut e);
            spanner_route_id = Some(route_id(
                t.remove("route_id"),
                "spanner-native",
                "spanner_native.route_id",
                &mut e,
            ));
        }
        match crate::spanner_serve::parse_section(&v) {
            Ok(section) => {
                let parts: Vec<_> = section.database.split('/').collect();
                if parts.len() != 6
                    || parts[0] != "projects"
                    || parts[2] != "instances"
                    || parts[4] != "databases"
                    || [1, 3, 5].iter().any(|&i| parts[i].is_empty())
                {
                    err(&mut e, "spanner_native", "invalid database path");
                }
                Some(section)
            }
            Err(_) => {
                err(&mut e, "spanner_native", "invalid configuration");
                None
            }
        }
    });
    if !has_upstream && spanner_native.is_none() {
        err(&mut e, "config", "at least one route is required");
    }
    let admin_listen =
        get(&mut m, "admin_listen", &mut e).unwrap_or(SocketAddr::from(([127, 0, 0, 1], 6480)));
    let policy = path_field(&mut m, "policy", &mut e).map(|p| resolve(base, p));
    let state_dir = resolve(
        base,
        path_field(&mut m, "state_dir", &mut e).unwrap_or_else(|| {
            err(&mut e, "state_dir", "required path missing");
            PathBuf::new()
        }),
    );
    let rp = path_field(&mut m, "root_key_file", &mut e).map(|p| resolve(base, p));
    let root_key = secret(rp, "NUNYA_ROOT_KEY")
        .and_then(|s| key(&s))
        .unwrap_or_else(|| {
            err(
                &mut e,
                "root key",
                "missing, unreadable or invalid credential",
            );
            Zeroizing::new([0; 32])
        });
    let schemas = get(&mut m, "schemas", &mut e).unwrap_or_else(|| vec!["public".into()]);
    let search_path: Vec<String> = get(&mut m, "search_path", &mut e).unwrap_or_else(|| vec!["public".into()]);
    let schemas = effective_schemas(schemas, &search_path);
    let snapshot_refresh_ms = get(&mut m, "snapshot_refresh_ms", &mut e).unwrap_or(30000);
    if snapshot_refresh_ms == 0 {
        err(&mut e, "snapshot_refresh_ms", "must be positive");
    }
    let policy_watch_ms: Option<u64> = get(&mut m, "policy_watch_ms", &mut e);
    if policy_watch_ms.is_some_and(|ms| !(100..=3600000).contains(&ms)) {
        err(&mut e, "policy_watch_ms", "must be from 100 to 3600000");
    }
    let max_connections = get(&mut m, "max_connections", &mut e).unwrap_or(256usize);
    if !(1..=10000).contains(&max_connections) {
        err(&mut e, "max_connections", "must be from 1 to 10000");
    }
    let allow_plaintext = get(&mut m, "allow_plaintext", &mut e).unwrap_or(false);
    let tls = m
        .remove("tls")
        .and_then(|v| parse_tls(v, base, "tls", &mut e));
    if !allow_plaintext && tls.is_none() {
        err(&mut e, "tls", "required unless allow_plaintext is true");
    }
    let mut postgres_file_owner = false;
    let mut postgres_policy = None;
    let mut postgres_route_id = None;
    let mut dialect = UpstreamDialect::default();
    let upstream = m.remove("upstream").map(|uv| {
        let mut u = table(
            uv,
            &[
                "policy_owner",
                "route_id",
                "policy",
                "dialect",
                "host",
                "port",
                "user",
                "password_file",
                "dbname",
                "tls",
                "ca_file",
                "connect_timeout_ms",
                "application_name",
            ],
            "upstream",
            &mut e,
        );
        postgres_file_owner = file_owner(&mut u, "upstream.policy_owner", &mut e);
        if Value::Table(u.clone()).try_into::<RawUpstream>().is_err() {
            err(&mut e, "upstream", "invalid fields");
        }
        postgres_policy = route_policy(&mut u, "upstream.policy", base, &mut e);
        postgres_route_id = Some(route_id(
            u.remove("route_id"),
            "postgres",
            "upstream.route_id",
            &mut e,
        ));
        dialect = get(&mut u, "dialect", &mut e).unwrap_or_default();
        let host: String = req(&mut u, "host", &mut e);
        let user: String = req(&mut u, "user", &mut e);
        let dbname: String = req(&mut u, "dbname", &mut e);
        if host.is_empty() || user.is_empty() || dbname.is_empty() {
            err(&mut e, "upstream", "host, user and dbname must be nonempty");
        }
        let port = get(&mut u, "port", &mut e).unwrap_or(5432);
        if port == 0 {
            err(&mut e, "upstream.port", "must be positive");
        }
        let tls_mode = get(&mut u, "tls", &mut e).unwrap_or(UpstreamTls::Disable);
        let ca_file = path_field(&mut u, "ca_file", &mut e).map(|p| resolve(base, p));
        if tls_mode == UpstreamTls::VerifyFull && ca_file.is_none() {
            err(&mut e, "upstream.ca_file", "required for verify-full");
        }
        if let Some(p) = &ca_file
            && !valid_ca(p)
        {
            err(&mut e, "upstream.ca_file", "cannot load CA certificates");
        }
        let connect_timeout_ms = get(&mut u, "connect_timeout_ms", &mut e).unwrap_or(5000);
        if connect_timeout_ms == 0 {
            err(&mut e, "connect_timeout_ms", "must be positive");
        }
        let application_name =
            get(&mut u, "application_name", &mut e).unwrap_or_else(|| "nunya".into());
        let pp = path_field(&mut u, "password_file", &mut e).map(|p| resolve(base, p));
        let password = secret(pp, "NUNYA_UPSTREAM_PASSWORD")
            .and_then(|b| {
                std::str::from_utf8(&b)
                    .ok()
                    .map(|s| Zeroizing::new(s.trim_end_matches(['\r', '\n']).to_owned()))
            })
            .unwrap_or_else(|| {
                err(
                    &mut e,
                    "upstream password",
                    "missing, unreadable or invalid credential",
                );
                Zeroizing::new(String::new())
            });
        crate::pgwire::upstream::UpstreamConfig {
            host,
            port,
            user,
            password,
            dbname,
            tls: match tls_mode {
                UpstreamTls::Disable => crate::pgwire::upstream::UpstreamTls::Disable,
                UpstreamTls::Require => crate::pgwire::upstream::UpstreamTls::Require,
                UpstreamTls::VerifyFull => crate::pgwire::upstream::UpstreamTls::VerifyFull {
                    ca_file: ca_file.unwrap_or_default(),
                },
            },
            connect_timeout_ms,
            application_name,
        }
    });
    for (configured, effective, field) in [
        (has_upstream, &mut postgres_policy, "upstream.policy"),
        (has_spanner, &mut spanner_policy, "spanner_native.policy"),
    ] {
        if configured && effective.is_none() {
            *effective = policy.clone();
            if effective.is_none() {
                err(&mut e, field, "required path missing (no service-level policy)");
            }
        }
    }
    let mut standing = Vec::new();
    let mut names = std::collections::BTreeSet::new();
    if let Some(v) = m.remove("standing") {
        if let Value::Array(entries) = v {
            for v in entries {
                let t = table(v, &["name", "verifier"], "standing", &mut e);
                match Value::Table(t).try_into::<RawStanding>() {
                    Ok(raw) => {
                        let s = Zeroizing::new(raw.verifier);
                        match ScramVerifier::parse(&s) {
                            Some(verifier)
                                if valid_principal_name(&raw.name)
                                    && names.insert(raw.name.clone()) =>
                            {
                                standing.push(StandingSection {
                                    name: raw.name,
                                    verifier,
                                })
                            }
                            _ => err(&mut e, "standing", "invalid or duplicate name or verifier"),
                        }
                    }
                    Err(_) => err(&mut e, "standing", "name and verifier required"),
                }
            }
        } else {
            err(&mut e, "standing", "expected array of tables");
        }
    }
    if postgres_route_id.is_some() && postgres_route_id == spanner_route_id {
        err(&mut e, "route_id", "routes must have distinct ids");
    }
    let session = m
        .remove("session")
        .map(|v| parse_session(v, &mut e))
        .unwrap_or_default();
    let management = m.remove("management").and_then(|v| {
        let mut t = table(v, &["listen", "tls", "admin"], "management", &mut e);
        let address = t
            .remove("listen")
            .and_then(|v| v.as_str().and_then(|s| s.parse::<SocketAddr>().ok()));
        let tls = t
            .remove("tls")
            .and_then(|v| parse_tls(v, base, "management.tls", &mut e));
        let mut admins = Vec::new();
        let mut names = std::collections::BTreeSet::new();
        let invalid = "management: invalid admin configuration";
        if let Some(v) = t.remove("admin") {
            if let Value::Array(entries) = v {
                for v in entries {
                    match v.try_into::<RawStanding>() {
                        Ok(raw) => {
                            let secret = Zeroizing::new(raw.verifier);
                            match ScramVerifier::parse(&secret) {
                                Some(verifier)
                                    if !raw.name.is_empty() && names.insert(raw.name.clone()) =>
                                {
                                    admins.push(AdminSection {
                                        name: raw.name,
                                        verifier,
                                    });
                                }
                                _ => e.push(invalid.into()),
                            }
                        }
                        Err(_) => e.push(invalid.into()),
                    }
                }
            } else {
                e.push(invalid.into());
            }
        }
        let Some(address) = address else {
            err(&mut e, "management", "valid listen address required");
            return None;
        };
        if !address.ip().is_loopback() && tls.is_none() {
            err(&mut e, "management", "non-loopback listen requires tls");
        }
        let native_address = spanner_native
            .as_ref()
            .and_then(|s| s.listen.parse::<SocketAddr>().ok());
        if Some(address) == listen || address == admin_listen || Some(address) == native_address {
            err(
                &mut e,
                "management",
                "listen conflicts with another listener",
            );
        }
        Some(ManagementSection {
            listen: address,
            tls,
            admins,
        })
    });
    if management.as_ref().is_some_and(|m| {
        m.admins
            .iter()
            .any(|a| standing.iter().any(|p| p.name == a.name))
    }) {
        err(
            &mut e,
            "management",
            "admin name conflicts with a standing principal",
        );
    }
    if !e.is_empty() {
        return Err(e);
    }
    use sha2::{Digest, Sha256};
    Ok(Config {
        policy_store,
        policy_store_kubernetes,
        policy_settings,
        path: path.to_path_buf(),
        revision: data_encoding::HEXLOWER.encode(&Sha256::digest(text.as_bytes())),
        session,
        management,
        postgres_route_id: postgres_route_id.clone(),
        spanner_route_id: spanner_route_id.clone(),
        listen,
        admin_listen,
        file_policy_routes: [(postgres_file_owner, &postgres_route_id), (spanner_file_owner, &spanner_route_id)].into_iter().filter_map(|(release, route)| if release { route.clone() } else { None }).collect(),
        postgres_policy,
        spanner_policy,
        state_dir,
        root_key,
        schemas,
        search_path,
        snapshot_refresh_ms,
        policy_watch_ms,
        allow_plaintext,
        tls,
        standing,
        dialect,
        max_connections,
        upstream,
        spanner_native,
    })
}
fn dns_label(s: &str) -> bool {
    !s.is_empty() && s.len() <= 63
        && s.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        && s.as_bytes()[0].is_ascii_alphanumeric()
        && s.as_bytes()[s.len() - 1].is_ascii_alphanumeric()
}
fn parse_kubernetes_store(v: Value, e: &mut Vec<String>) -> KubernetesPolicyStore {
    let mut m = table(v, &["namespace", "lease_name", "pod_name", "management_url",
        "lease_duration_secs", "renew_deadline_secs", "retry_period_secs"], "policy_store_kubernetes", e);
    let namespace: String = req(&mut m, "namespace", e);
    let lease_name: String = get(&mut m, "lease_name", e).unwrap_or_else(|| "nunya-leader".into());
    let pod_name: String = req(&mut m, "pod_name", e);
    let management_url: String = req(&mut m, "management_url", e);
    for (field, value) in [("namespace", &namespace), ("lease_name", &lease_name)] {
        if !dns_label(value) { err(e, field, "expected DNS label"); }
    }
    if pod_name.is_empty() || pod_name.trim() != pod_name { err(e, "pod_name", "expected nonempty pod name"); }
    let valid_url = management_url.parse::<http::Uri>().ok().is_some_and(|u|
        matches!(u.scheme_str(), Some("http" | "https"))
        && u.authority().is_some_and(|a| !a.as_str().contains('@'))
        && !management_url.contains('#')
        && u.path_and_query().is_none_or(|p| p.as_str() == "/"));
    if !valid_url { err(e, "management_url", "expected http/https base URL without path or userinfo"); }
    let lease_duration_secs = get(&mut m, "lease_duration_secs", e).unwrap_or(15);
    let renew_deadline_secs = get(&mut m, "renew_deadline_secs", e).unwrap_or(10);
    let retry_period_secs = get(&mut m, "retry_period_secs", e).unwrap_or(2);
    if retry_period_secs == 0 || retry_period_secs >= renew_deadline_secs
        || renew_deadline_secs >= lease_duration_secs || i32::try_from(lease_duration_secs).is_err() {
        err(e, "policy_store_kubernetes", "invalid election timings");
    }
    KubernetesPolicyStore { namespace, lease_name, pod_name, management_url,
        lease_duration_secs, renew_deadline_secs, retry_period_secs }
}
fn route_policy(
    table: &mut Map<String, Value>,
    field: &str,
    base: &Path,
    e: &mut Vec<String>,
) -> Option<PathBuf> {
    let value = table.remove("policy")?;
    let mut field_table = Map::new();
    field_table.insert(field.into(), value);
    path_field(&mut field_table, field, e).map(|p| resolve(base, p))
}
fn route_id(value: Option<Value>, default: &str, field: &str, e: &mut Vec<String>) -> String {
    let id = match value {
        None => return default.into(),
        Some(Value::String(s)) => s,
        Some(_) => {
            err(e, field, "invalid route id");
            return default.into();
        }
    };
    let valid = !id.is_empty()
        && id.len() <= 63
        && id
            .as_bytes()
            .first()
            .is_some_and(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
        && id.bytes().all(|b| {
            b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'.' | b'_' | b'-')
        });
    if !valid {
        err(e, field, "invalid route id");
    }
    id
}
fn parse_session(v: Value, e: &mut Vec<String>) -> SessionSettings {
    let mut t = table(v, &["max_lifetime", "idle_timeout"], "session", e);
    let mut settings = SessionSettings::default();
    if let Some(v) = t.remove("max_lifetime") {
        match v.as_str().and_then(parse_duration) {
            Some(d) if (Duration::from_secs(60)..=Duration::from_secs(30 * 86400)).contains(&d) => {
                settings.max_lifetime = d
            }
            _ => err(e, "session", "invalid max_lifetime"),
        }
    }
    if let Some(v) = t.remove("idle_timeout") {
        if v.as_str() == Some("off") {
            settings.idle_timeout = None;
        } else {
            match v.as_str().and_then(parse_duration) {
                Some(d) => settings.idle_timeout = Some(d),
                None => err(e, "session", "invalid idle_timeout"),
            }
        }
    }
    if settings
        .idle_timeout
        .is_some_and(|d| d < Duration::from_secs(60) || d > settings.max_lifetime)
    {
        err(e, "session", "invalid idle_timeout");
    }
    settings
}
fn parse_tls(v: Value, base: &Path, field: &str, e: &mut Vec<String>) -> Option<TlsSection> {
    let t = table(v, &["cert_file", "key_file"], field, e);
    match Value::Table(t).try_into::<TlsSection>() {
        Ok(mut t) => {
            if t.cert_file.as_os_str().is_empty() || t.key_file.as_os_str().is_empty() {
                err(e, field, "paths must be nonempty");
            }
            t.cert_file = resolve(base, t.cert_file);
            t.key_file = resolve(base, t.key_file);
            if load_tls(&t).is_err() {
                err(e, field, "cannot load certificate and key");
            }
            Some(t)
        }
        Err(_) => {
            err(e, field, "certificate and key paths required");
            None
        }
    }
}
fn valid_ca(path: &Path) -> bool {
    let Ok(file) = std::fs::File::open(path) else {
        return false;
    };
    let mut store = rustls::RootCertStore::empty();
    for cert in rustls_pemfile::certs(&mut BufReader::new(file)) {
        let Ok(cert) = cert else {
            return false;
        };
        if store.add(cert).is_err() {
            return false;
        }
    }
    !store.is_empty()
}
/// Load a server certificate chain and matching key with sanitized diagnostics.
pub fn load_tls(cfg: &TlsSection) -> Result<Arc<rustls::ServerConfig>, String> {
    let fail = || "tls: cannot load certificate and private key".to_owned();
    let cert = std::fs::File::open(&cfg.cert_file).map_err(|_| fail())?;
    let certs = rustls_pemfile::certs(&mut BufReader::new(cert))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| fail())?;
    let key = std::fs::File::open(&cfg.key_file).map_err(|_| fail())?;
    let key = rustls_pemfile::private_key(&mut BufReader::new(key))
        .map_err(|_| fail())?
        .ok_or_else(fail)?;
    let config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|_| fail())?
    .with_no_client_auth()
    .with_single_cert(certs, key)
    .map_err(|_| fail())?;
    Ok(Arc::new(config))
}

fn valid_principal_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

fn file_owner(table: &mut Map<String, Value>, field: &str, errors: &mut Vec<String>) -> bool {
    match table.remove("policy_owner") {
        None => false,
        Some(Value::String(owner)) if owner == "file" => true,
        Some(_) => { err(errors, field, "must be file"); false }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    struct Fixture(PathBuf);
    impl Fixture {
        fn new() -> Self {
            let mut nonce = [0u8; 16];
            getrandom::fill(&mut nonce).unwrap();
            let name = data_encoding::HEXLOWER.encode(&nonce);
            let dir = std::env::temp_dir().join(format!("config-{name}"));
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("root.key"), "ab".repeat(32)).unwrap();
            std::fs::write(dir.join("password"), "private-password\n").unwrap();
            Self(dir)
        }
        fn load(&self, text: &str) -> Result<Config, Vec<String>> {
            let path = self.0.join("nunya.toml");
            std::fs::write(&path, text).unwrap();
            load(&path)
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    const EXAMPLE: &str = r#"
listen = "127.0.0.1:6432"
policy = "policy.toml"
root_key_file = "root.key"
state_dir = "state"
allow_plaintext = true
[upstream]
host = "localhost"
user = "nunya"
dbname = "app"
password_file = "password"
tls = "disable"
"#;
    const NATIVE: &str = "\n[spanner_native]\nlisten='127.0.0.1:6433'\ndatabase='projects/p/instances/i/databases/d'\n";
    const KUBE: &str = "[policy_store_kubernetes]\nnamespace='default'\npod_name='nunya-abc'\nmanagement_url='http://10.0.0.1:6481'\n";
    #[test]
    fn policy_store_defaults_and_unknown_selection() {
        let f = Fixture::new();
        let c = f.load(EXAMPLE).unwrap();
        assert_eq!(c.policy_store, PolicyStore::Local);
        assert!(c.policy_store_kubernetes.is_none());
        for selection in ["'other'", "42", "[]"] {
            assert!(f.load(&format!("policy_store={selection}\n{EXAMPLE}")).is_err());
        }
        assert!(f.load(&format!("{EXAMPLE}{KUBE}")).is_err());
        assert!(f.load(&format!("policy_store='kubernetes'\n{EXAMPLE}")).is_err());
    }
    #[test]
    fn kubernetes_settings_validation() {
        let mut errors = Vec::new();
        let v: Value = toml::from_str(KUBE).unwrap();
        let k = parse_kubernetes_store(v["policy_store_kubernetes"].clone(), &mut errors);
        assert!(errors.is_empty());
        assert_eq!(k.lease_name, "nunya-leader");
        assert_eq!((k.lease_duration_secs, k.renew_deadline_secs, k.retry_period_secs), (15, 10, 2));
        for (from, to) in [
            ("namespace='default'", "namespace='UPPER'"),
            ("namespace='default'", "namespace='-bad'"),
            ("namespace='default'", "namespace='bad-'"),
            ("namespace='default'", "namespace=''"),
            ("namespace='default'", "namespace=42"),
            ("namespace='default'", ""),
            ("pod_name='nunya-abc'", ""),
            ("pod_name='nunya-abc'", "pod_name=''"),
            ("pod_name='nunya-abc'", "pod_name=42"),
            ("management_url='http://10.0.0.1:6481'", ""),
            ("management_url='http://10.0.0.1:6481'", "management_url=42"),
            ("management_url='http://10.0.0.1:6481'", "management_url='ftp://pod'"),
            ("management_url='http://10.0.0.1:6481'", "management_url='http://pod/path'"),
            ("management_url='http://10.0.0.1:6481'", "management_url='http://u@pod'"),
            ("management_url='http://10.0.0.1:6481'", "management_url='http://pod?x'"),
            ("management_url='http://10.0.0.1:6481'", "management_url='http://pod/#x'"),
            ("namespace='default'", "namespace='default'\nlease_name='bad.name'"),
            ("namespace='default'", "namespace='default'\nlease_name=42"),
            ("namespace='default'", "namespace='default'\nunknown=true"),
            ("namespace='default'", "namespace='default'\nretry_period_secs=0"),
            ("namespace='default'", "namespace='default'\nretry_period_secs=10"),
            ("namespace='default'", "namespace='default'\nrenew_deadline_secs=15"),
            ("namespace='default'", "namespace='default'\nlease_duration_secs=2147483648"),
            ("namespace='default'", "namespace='default'\nlease_duration_secs=-1"),
            ("namespace='default'", "namespace='default'\nrenew_deadline_secs='10'"),
            ("namespace='default'", "namespace='default'\nretry_period_secs=[]"),
        ] {
            let text = KUBE.replace(from, to);
            let v: Value = toml::from_str(&text).unwrap();
            let mut errors = Vec::new();
            parse_kubernetes_store(v["policy_store_kubernetes"].clone(), &mut errors);
            assert!(!errors.is_empty(), "refusal case {from} -> {to}");
        }
        let mut errors = Vec::new();
        parse_kubernetes_store(Value::String("invalid".into()), &mut errors);
        assert!(!errors.is_empty());
    }
    #[cfg(feature = "kubernetes")]
    #[test]
    fn kubernetes_config_loads() {
        let c = Fixture::new().load(&format!("policy_store='kubernetes'\n{EXAMPLE}{KUBE}")).unwrap();
        assert_eq!(c.policy_store, PolicyStore::Kubernetes);
    }
    #[cfg(not(feature = "kubernetes"))]
    #[test]
    fn kubernetes_requires_feature() {
        let errors = Fixture::new().load(&format!("policy_store='kubernetes'\n{EXAMPLE}{KUBE}")).unwrap_err();
        assert!(errors.iter().any(|s| s.contains("built without the kubernetes feature")));
    }
    #[test]
    fn search_path_schemas_are_captured_in_order() {
        let f = Fixture::new();
        let text = format!("schemas=['public']\nsearch_path=['other','public']\n{EXAMPLE}");
        let c = f.load(&text).unwrap();
        assert_eq!(c.schemas, ["public", "other"]);
        assert_eq!(c.search_path, ["other", "public"]);
    }
    #[test]
    fn effective_capture_set_has_no_duplicates_and_compares_exactly() {
        let f = Fixture::new();
        let text = format!("schemas=['other','public','other']\nsearch_path=['public','other','Other','Other']\n{EXAMPLE}");
        let c = f.load(&text).unwrap();
        assert_eq!(c.schemas, ["other", "public", "Other"]);
        assert_eq!(c.search_path, ["public", "other", "Other", "Other"]);
    }
    #[test]
    fn empty_capture_set_keeps_all_non_system_schemas() {
        let f = Fixture::new();
        let text = format!("schemas=[]\nsearch_path=['other','public']\n{EXAMPLE}");
        let c = f.load(&text).unwrap();
        assert!(c.schemas.is_empty());
        assert_eq!(c.search_path, ["other", "public"]);
    }
    #[test]
    fn default_capture_set_and_search_path_stay_public() {
        let f = Fixture::new();
        let c = f.load(EXAMPLE).unwrap();
        assert_eq!(c.schemas, ["public"]);
        assert_eq!(c.search_path, ["public"]);
    }
    #[test]
    fn policy_owner_is_strict_and_per_route() {
        let f = Fixture::new();
        let c = f.load(&EXAMPLE.replace("[upstream]", "[upstream]\npolicy_owner='file'")).unwrap();
        assert_eq!(c.file_policy_routes, ["postgres"]);
        assert!(f.load(EXAMPLE).unwrap().file_policy_routes.is_empty());
        for value in ["'service'", "true", "42", "''"] {
            assert!(f.load(&EXAMPLE.replace("[upstream]", &format!("[upstream]\npolicy_owner={value}"))).is_err());
        }
        let c = f.load(&format!("{EXAMPLE}{NATIVE}policy_owner='file'\n")).unwrap();
        assert_eq!(c.file_policy_routes, ["spanner-native"]);
    }
    #[test]
    fn per_route_policy_keys_resolve_against_config_dir() {
        let f = Fixture::new();
        let text = EXAMPLE.replace("policy = \"policy.toml\"\n", "")
            .replace("[upstream]", "[upstream]\npolicy='pg.toml'");
        let c = f.load(&format!("{text}{NATIVE}policy='spanner.toml'\n")).unwrap();
        assert_eq!(c.postgres_policy, Some(f.0.join("pg.toml")));
        assert_eq!(c.spanner_policy, Some(f.0.join("spanner.toml")));
        let native_only = format!("{}{NATIVE}policy='native.toml'\n", text.split("[upstream]").next().unwrap());
        let c = f.load(&native_only).unwrap();
        assert_eq!(c.postgres_policy, None);
        assert_eq!(c.spanner_policy, Some(f.0.join("native.toml")));
    }
    #[test]
    fn route_without_own_policy_falls_back_to_service_level() {
        let f = Fixture::new();
        let text = EXAMPLE.replace("[upstream]", "[upstream]\npolicy='pg.toml'");
        let c = f.load(&format!("{text}{NATIVE}")).unwrap();
        assert_eq!(c.postgres_policy, Some(f.0.join("pg.toml")));
        assert_eq!(c.spanner_policy, Some(f.0.join("policy.toml")));
        let c = f.load(&format!("{EXAMPLE}{NATIVE}")).unwrap();
        assert_eq!(c.postgres_policy, Some(f.0.join("policy.toml")));
        assert_eq!(c.spanner_policy, c.postgres_policy);
    }
    #[test]
    fn missing_route_policy_is_a_clear_error() {
        let f = Fixture::new();
        let text = EXAMPLE.replace("policy = \"policy.toml\"\n", "");
        let pg_error = "upstream.policy: required path missing (no service-level policy)";
        let spanner_error = "spanner_native.policy: required path missing (no service-level policy)";
        assert_eq!(f.load(&text).unwrap_err(), vec![pg_error]);
        assert_eq!(f.load(&format!("{text}{NATIVE}policy='s.toml'\n")).unwrap_err(), vec![pg_error]);
        let pg_own = text.replace("[upstream]", "[upstream]\npolicy='p.toml'");
        assert_eq!(f.load(&format!("{pg_own}{NATIVE}")).unwrap_err(), vec![spanner_error]);
        let native_only = format!("{}{NATIVE}", text.split("[upstream]").next().unwrap());
        assert_eq!(f.load(&native_only).unwrap_err(), vec![spanner_error]);
    }
    #[test]
    fn route_policy_key_invalid_or_unknown_neighbour_refused() {
        let f = Fixture::new();
        for value in ["5", "''", "['DO_NOT_LEAK']"] {
            for (text, field) in [
                (EXAMPLE.replace("[upstream]", &format!("[upstream]\npolicy={value}")), "upstream.policy"),
                (format!("{EXAMPLE}{NATIVE}policy={value}\n"), "spanner_native.policy"),
            ] {
                let errors = f.load(&text).unwrap_err();
                assert!(errors.iter().any(|e| e.starts_with(&format!("{field}:"))));
                assert!(!errors.join(";").contains("DO_NOT_LEAK"));
            }
        }
        for text in [
            EXAMPLE.replace("[upstream]", "[upstream]\npolicy='pg.toml'\nunknown='DO_NOT_LEAK'"),
            format!("{EXAMPLE}{NATIVE}policy='s.toml'\nunknown='DO_NOT_LEAK'\n"),
            EXAMPLE.replace("[upstream]", "[upstream]\npolicy_watch_ms=100"),
            format!("{EXAMPLE}{NATIVE}policy_watch_ms=100\n"),
        ] {
            let errors = f.load(&text).unwrap_err();
            assert!(!errors.join(";").contains("DO_NOT_LEAK"));
        }
    }
    #[test]
    fn startup_revision_hashes_exact_config_bytes() {
        use sha2::{Digest, Sha256};
        let f = Fixture::new();
        let text = EXAMPLE.replace("\n", "\r\n");
        let config = f.load(&text).unwrap();
        assert_eq!(config.revision, data_encoding::HEXLOWER.encode(&Sha256::digest(text.as_bytes())));
    }
    #[test]
    fn duration_grammar_and_overflow() {
        for (text, secs) in [
            ("1s", 1),
            ("60m", 3600),
            ("8h", 28800),
            ("30d", 2592000),
            ("001s", 1),
            ("18446744073709551615s", u64::MAX),
        ] {
            assert_eq!(parse_duration(text), Some(Duration::from_secs(secs)));
        }
        for text in [
            "",
            "s",
            "0h",
            "0s",
            "1",
            "1ms",
            "1S",
            "-1h",
            "+1h",
            "8 h",
            " 8h",
            "8h ",
            "1.5h",
            "\u{ff11}h",
            "18446744073709551616s",
            "18446744073709551615m",
        ] {
            assert_eq!(parse_duration(text), None);
        }
    }
    #[test]
    fn session_defaults_bounds_and_requested_lifetimes() {
        let f = Fixture::new();
        let c = f.load(EXAMPLE).unwrap();
        assert_eq!(c.session, SessionSettings::default());
        assert_eq!(c.session.max_lifetime, Duration::from_secs(28800));
        assert_eq!(c.session.idle_timeout, Some(Duration::from_secs(3600)));
        assert!(c.management.is_none());
        assert_eq!(c.session.lifetime_for(None), Ok(c.session.max_lifetime));
        assert_eq!(
            c.session.lifetime_for(Some(Duration::from_nanos(1))),
            Ok(Duration::from_nanos(1))
        );
        assert_eq!(
            c.session.lifetime_for(Some(c.session.max_lifetime)),
            Ok(c.session.max_lifetime)
        );
        assert_eq!(
            c.session.lifetime_for(Some(Duration::ZERO)),
            Err(LifetimeError::Zero)
        );
        assert_eq!(
            c.session.lifetime_for(Some(Duration::from_secs(28801))),
            Err(LifetimeError::AboveMaximum)
        );
        for section in [
            "",
            "max_lifetime='30d'",
            "max_lifetime='60s'\nidle_timeout='60s'",
            "idle_timeout='off'",
            "max_lifetime='60s'\nidle_timeout='off'",
        ] {
            let cfg = f.load(&format!("{EXAMPLE}\n[session]\n{section}")).unwrap();
            if section.contains("'off'") {
                assert!(cfg.session.idle_timeout.is_none());
            }
        }
        for section in [
            "max_lifetime='0h'",
            "max_lifetime='31d'",
            "max_lifetime='59s'",
            "max_lifetime='8 h'",
            "max_lifetime='-1h'",
            "max_lifetime=8",
            "idle_timeout='9h'",
            "idle_timeout='0h'",
            "idle_timeout='59s'",
            "idle_timeout=1",
            "idle_timeout='OFF'",
            "max_lifetime='60s'",
            "unknown='PRIVATE'",
        ] {
            let errors = f
                .load(&format!("{EXAMPLE}\n[session]\n{section}"))
                .unwrap_err();
            assert!(errors.iter().all(|s| s.starts_with("session:")));
            assert!(!format!("{errors:?}").contains("PRIVATE"));
        }
        assert!(
            f.load(&EXAMPLE.replace("[upstream]", "session = 'bad'\n[upstream]"))
                .is_err()
        );
    }
    #[test]
    fn route_id_defaults_overrides_and_refusals() {
        let f = Fixture::new();
        let c = f.load(EXAMPLE).unwrap();
        assert_eq!(c.postgres_route_id.as_deref(), Some("postgres"));
        assert_eq!(c.spanner_route_id, None);
        let c = f.load(&format!("{EXAMPLE}{NATIVE}")).unwrap();
        assert_eq!(c.spanner_route_id.as_deref(), Some("spanner-native"));
        let native_only = format!("{}{NATIVE}", EXAMPLE.split("[upstream]").next().unwrap());
        assert_eq!(f.load(&native_only).unwrap().postgres_route_id, None);
        for id in ["custom.route_1-2", "0", &"x".repeat(63)] {
            let text = EXAMPLE.replace("[upstream]", &format!("[upstream]\nroute_id='{id}'"));
            assert_eq!(
                f.load(&text).unwrap().postgres_route_id.as_deref(),
                Some(id)
            );
            assert_eq!(
                f.load(&format!("{native_only}route_id='{id}'"))
                    .unwrap()
                    .spanner_route_id
                    .as_deref(),
                Some(id)
            );
        }
        for id in ["Upper", "", &"x".repeat(64), "a/b", "_a", ".a", "-a", "\u{e9}"] {
            let errors = f
                .load(&EXAMPLE.replace("[upstream]", &format!("[upstream]\nroute_id='{id}'")))
                .unwrap_err();
            assert!(errors.contains(&"upstream.route_id: invalid route id".into()));
            let errors = f
                .load(&format!("{native_only}route_id='{id}'"))
                .unwrap_err();
            assert!(errors.contains(&"spanner_native.route_id: invalid route id".into()));
        }
        for text in [
            EXAMPLE.replace("[upstream]", "[upstream]\nroute_id=1"),
            format!("{native_only}route_id=1"),
        ] {
            assert!(f.load(&text).is_err());
        }
        let errors = f
            .load(&format!("{EXAMPLE}{NATIVE}route_id='postgres'"))
            .unwrap_err();
        assert!(errors.contains(&"route_id: routes must have distinct ids".into()));
        assert!(
            f.load(&format!("{EXAMPLE}{NATIVE}route_id='valid'\nunknown=true"))
                .is_err()
        );
    }
    #[test]
    fn management_addresses_and_strict_fields() {
        let f = Fixture::new();
        for ip in ["127.0.0.1", "[::1]"] {
            let c = f
                .load(&format!("{EXAMPLE}\n[management]\nlisten='{ip}:6481'"))
                .unwrap();
            let m = c.management.unwrap();
            assert!(m.tls.is_none());
            assert!(m.admins.is_empty());
        }
        for ip in ["10.0.0.5", "0.0.0.0", "[::]"] {
            let errors = f
                .load(&format!("{EXAMPLE}\n[management]\nlisten='{ip}:6481'"))
                .unwrap_err();
            assert!(errors.contains(&"management: non-loopback listen requires tls".into()));
        }
        for section in [
            "",
            "listen='bad'",
            "listen=1",
            "listen='127.0.0.1:6481'\nunknown='PRIVATE'",
        ] {
            let errors = f
                .load(&format!("{EXAMPLE}\n[management]\n{section}"))
                .unwrap_err();
            assert!(errors.iter().all(|s| s.starts_with("management:")));
            assert!(!format!("{errors:?}").contains("PRIVATE"));
        }
        for port in [6432, 6433, 6480] {
            let errors = f
                .load(&format!(
                    "{EXAMPLE}{NATIVE}\n[management]\nlisten='127.0.0.1:{port}'"
                ))
                .unwrap_err();
            assert!(errors.contains(&"management: listen conflicts with another listener".into()));
        }
    }
    #[test]
    fn management_admins_redacted_and_validated() {
        let f = Fixture::new();
        let verifier = ScramVerifier::from_password("secret", b"salt", 1).to_pg_string();
        let base = format!("{EXAMPLE}\n[management]\nlisten='127.0.0.1:6481'");
        let entry = format!("\n[[management.admin]]\nname='PRIVATE_ADMIN'\nverifier='{verifier}'");
        let c = f.load(&format!("{base}{entry}")).unwrap();
        let m = c.management.unwrap();
        assert_eq!(m.admins.len(), 1);
        assert_eq!(m.admins[0].name, "PRIVATE_ADMIN");
        assert_eq!(m.admins[0].verifier.to_pg_string(), verifier);
        for debug in [format!("{m:?}"), format!("{:?}", m.admins[0])] {
            assert!(!debug.contains(&verifier));
            assert!(!debug.contains("PRIVATE_ADMIN"));
        }
        for text in [
            format!("{base}{entry}{entry}"),
            format!("{base}{entry}\nextra='PRIVATE'"),
            format!("{base}{}", entry.replace("PRIVATE_ADMIN", "")),
            format!("{base}{}", entry.replace(&verifier, "PRIVATE_VERIFIER")),
            format!("{base}\n[[management.admin]]\nname='PRIVATE_ADMIN'"),
            format!("{base}\nadmin='PRIVATE'"),
        ] {
            let errors = f.load(&text).unwrap_err();
            assert_eq!(errors, vec!["management: invalid admin configuration"]);
        }
    }
    #[test]
    fn management_admin_name_conflicts_with_standing() {
        let f = Fixture::new();
        let verifier = ScramVerifier::from_password("secret", b"salt", 1).to_pg_string();
        let text = format!(
            "{EXAMPLE}\n[[standing]]\nname='same'\nverifier='{verifier}'\n[management]\nlisten='127.0.0.1:6481'\n[[management.admin]]\nname='same'\nverifier='{verifier}'"
        );
        assert_eq!(
            f.load(&text).unwrap_err(),
            vec!["management: admin name conflicts with a standing principal"]
        );
    }
    #[test]
    fn management_tls_rejects_invalid_files_and_fields() {
        let f = Fixture::new();
        let base = format!("{EXAMPLE}\n[management]\nlisten='10.0.0.5:6481'");
        for invalid in [
            "tls={cert_file='missing',key_file='mgmt.key'}",
            "tls={cert_file='mgmt.crt'}",
            "tls={cert_file='',key_file=''}",
            "tls={cert_file='mgmt.crt',key_file='mgmt.key',unknown='PRIVATE'}",
            "tls='PRIVATE'",
        ] {
            let errors = f.load(&format!("{base}\n{invalid}")).unwrap_err();
            assert!(errors.iter().any(|s| s.starts_with("management.tls:")));
            assert!(!format!("{errors:?}").contains("PRIVATE"));
        }
    }
    #[test]
    fn policy_watch_interval_bounds() {
        let f = Fixture::new();
        assert_eq!(f.load(EXAMPLE).unwrap().policy_watch_ms, None);
        for ms in [100, 500, 3600000] {
            let text =
                EXAMPLE.replace("[upstream]", &format!("policy_watch_ms = {ms}\n[upstream]"));
            assert_eq!(f.load(&text).unwrap().policy_watch_ms, Some(ms));
        }
        for ms in [99, 3600001] {
            let text =
                EXAMPLE.replace("[upstream]", &format!("policy_watch_ms = {ms}\n[upstream]"));
            assert!(
                f.load(&text)
                    .unwrap_err()
                    .contains(&"policy_watch_ms: must be from 100 to 3600000".to_owned())
            );
        }
    }
    #[test]
    fn optional_routes_and_strict_native_settings() {
        let f = Fixture::new();
        let base = EXAMPLE
            .split("[upstream]")
            .next()
            .unwrap()
            .replace("listen = \"127.0.0.1:6432\"", "");
        let native = format!(
            "{base}\n[spanner_native]\nlisten = '127.0.0.1:6433'\ndatabase = 'projects/p/instances/i/databases/d'\n"
        );
        let config = f.load(&native).unwrap();
        assert!(config.upstream.is_none());
        assert!(config.listen.is_none());
        assert!(config.spanner_native.is_some());
        assert!(f.load(&base).is_err());
        assert!(
            f.load(&EXAMPLE.replace("listen = \"127.0.0.1:6432\"", ""))
                .is_err()
        );
        for invalid in [
            native.replace(
                "projects/p/instances/i/databases/d",
                "projects/p/instances/i/databases/",
            ),
            native.replace("projects/p/instances/i/databases/d", "bad"),
            native.replace("127.0.0.1:6433", "bad"),
            format!("{native}unknown = true\n"),
        ] {
            assert!(f.load(&invalid).is_err());
        }
        assert!(f.load(&format!("{EXAMPLE}\n[spanner_native]\nlisten='127.0.0.1:6433'\ndatabase='projects/p/instances/i/databases/d'")).is_ok());
    }
    #[test]
    fn example_parses_paths_resolve_and_debug_redacts() {
        let f = Fixture::new();
        let c = f.load(EXAMPLE).unwrap();
        assert_eq!(c.postgres_policy, Some(f.0.join("policy.toml")));
        assert_eq!(c.spanner_policy, None);
        assert_eq!(c.state_dir, f.0.join("state"));
        assert_eq!(
            c.upstream.as_ref().unwrap().password.as_str(),
            "private-password"
        );
        assert_eq!(c.admin_listen, SocketAddr::from(([127, 0, 0, 1], 6480)));
        for debug in [format!("{c:?}"), format!("{:?}", c.upstream_config())] {
            assert!(!debug.contains("private-password"));
            assert!(!debug.contains(&"ab".repeat(32)));
        }
    }
    #[test]
    fn dialect_limits_and_upstream_mapping() {
        let f = Fixture::new();
        let defaults = f.load(EXAMPLE).unwrap();
        assert_eq!(defaults.dialect, UpstreamDialect::Postgres);
        assert_eq!(defaults.max_connections, 256);
        let text = EXAMPLE.replace(
            "[upstream]",
            "max_connections = 10000\n[upstream]\ndialect = 'spanner-pg'",
        );
        let mut cfg = f.load(&text).unwrap();
        assert_eq!(cfg.dialect, UpstreamDialect::SpannerPg);
        assert_eq!(cfg.max_connections, 10000);
        let upstream = cfg.upstream_config().unwrap();
        assert_eq!(upstream.host, "localhost");
        assert_eq!(upstream.port, 5432);
        assert_eq!(upstream.user, "nunya");
        assert_eq!(upstream.dbname, "app");
        assert_eq!(upstream.application_name, "nunya");
        assert_eq!(upstream.connect_timeout_ms, 5000);
        assert!(matches!(
            upstream.tls,
            crate::pgwire::upstream::UpstreamTls::Disable
        ));
        let require = f
            .load(&EXAMPLE.replace("tls = \"disable\"", "tls = \"require\""))
            .unwrap();
        assert!(matches!(
            require.upstream_config().unwrap().tls,
            crate::pgwire::upstream::UpstreamTls::Require
        ));
        let ca = f.0.join("ca.pem");
        cfg.upstream.as_mut().unwrap().tls = crate::pgwire::upstream::UpstreamTls::VerifyFull {
            ca_file: ca.clone(),
        };
        assert!(
            matches!(cfg.upstream_config().unwrap().tls, crate::pgwire::upstream::UpstreamTls::VerifyFull { ca_file } if ca_file == ca)
        );
        let explicit = f
            .load(&EXAMPLE.replace(
                "[upstream]",
                "max_connections = 1\n[upstream]\ndialect = 'postgres'",
            ))
            .unwrap();
        assert_eq!(explicit.max_connections, 1);
        assert_eq!(explicit.dialect, UpstreamDialect::Postgres);
        for limit in ["0", "10001", "-1", "'bad'"] {
            assert!(
                f.load(&EXAMPLE.replace(
                    "[upstream]",
                    &format!("max_connections = {limit}\n[upstream]")
                ))
                .is_err()
            );
        }
        assert!(
            f.load(&EXAMPLE.replace("[upstream]", "[upstream]\ndialect = 'unknown'"))
                .is_err()
        );
    }
    #[test]
    fn shipped_example_parses_with_file_credentials() {
        let f = Fixture::new();
        let example = include_str!("../examples/nunya.toml")
            .replace(
                "state_dir = \"state\"",
                "state_dir = \"state\"\nroot_key_file = \"root.key\"",
            )
            .replace("[upstream]", "[upstream]\npassword_file = \"password\"");
        assert!(f.load(&example).is_ok());
    }
    #[test]
    fn unknown_keys_and_independent_errors_are_collected() {
        let f = Fixture::new();
        let text = format!(
            "unknown_secret='DO_NOT_LEAK'\n{}",
            EXAMPLE
                .replace("allow_plaintext = true", "allow_plaintext = false")
                .replace(
                    "tls = \"disable\"",
                    "tls = \"bogus\"\nextra_secret='DO_NOT_LEAK'"
                )
        );
        let errors = f.load(&text).unwrap_err();
        assert!(errors.len() >= 4);
        assert!(errors.iter().any(|e| e.contains("unknown field")));
        assert!(errors.iter().any(|e| e.contains("required unless")));
        assert!(!format!("{errors:?}").contains("DO_NOT_LEAK"));
    }
    #[test]
    fn standing_names_follow_principal_name_rule() {
        let f = Fixture::new();
        let verifier = ScramVerifier::from_password("secret", b"salt", 1).to_pg_string();
        for name in ["ops", "agent_1", "A-b", &"x".repeat(64)] {
            let text = format!("{EXAMPLE}\n[[standing]]\nname='{name}'\nverifier='{verifier}'");
            assert_eq!(f.load(&text).unwrap().standing[0].name, name);
        }
        for name in [
            "", &"x".repeat(65), "task:x", "a.b", "a b", "\u{e9}", "\u{c3}\u{a9}",
        ] {
            let text = format!("{EXAMPLE}\n[[standing]]\nname='{name}'\nverifier='{verifier}'");
            let errors = f.load(&text).unwrap_err();
            assert_eq!(errors, vec!["standing: invalid or duplicate name or verifier"]);
            if !name.is_empty() {
                assert!(!errors.join(";").contains(name));
            }
        }
    }

    #[test]
    fn verifier_validation_and_tls_requirements() {
        let f = Fixture::new();
        let text = format!(
            "{}\n[[standing]]\nname='agent'\nverifier='SECRET_BAD_VERIFIER'",
            EXAMPLE.replace("tls = \"disable\"", "tls = \"verify-full\"")
        );
        let errors = f.load(&text).unwrap_err();
        assert!(errors.iter().any(|e| e.contains("ca_file")));
        assert!(errors.iter().any(|e| e.contains("standing")));
        assert!(!format!("{errors:?}").contains("SECRET_BAD_VERIFIER"));
        let verifier = ScramVerifier::from_password("secret", b"salt", 1).to_pg_string();
        let text = format!("{EXAMPLE}\n[[standing]]\nname='agent'\nverifier='{verifier}'");
        assert_eq!(f.load(&text).unwrap().standing.len(), 1);
    }
    #[test]
    fn invalid_tls_and_root_key_are_sanitized() {
        let f = Fixture::new();
        std::fs::write(f.0.join("root.key"), "secret-invalid-key").unwrap();
        let errors = f.load(EXAMPLE).unwrap_err();
        assert!(!format!("{errors:?}").contains("secret-invalid-key"));
        assert!(
            load_tls(&TlsSection {
                cert_file: f.0.join("missing"),
                key_file: f.0.join("missing")
            })
            .is_err()
        );
    }

    #[test]
    fn service_policy_settings_parse_and_validate() {
        let f = Fixture::new();
        let c = f.load(EXAMPLE).unwrap();
        assert_eq!(c.policy_settings, crate::policy::ServicePolicySettings::default());
        let text = format!("snapshot_max_age_ms=0\n{EXAMPLE}\n[readlog]\nsink='file'\npath='audit.log'\nmax_bytes=12\n");
        let c = f.load(&text).unwrap();
        assert_eq!(c.policy_settings.snapshot_max_age_ms, Some(0));
        assert_eq!(c.policy_settings.readlog, Some(crate::policy::ReadLogSink::File { path: f.0.join("audit.log"), max_bytes: 12 }));
        for setting in ["snapshot_max_age_ms=-1", "snapshot_max_age_ms=1.5", "snapshot_max_age_ms='1'"] {
            assert!(f.load(&format!("{setting}\n{EXAMPLE}")).is_err());
        }
        for setting in ["sink='bad'", "sink='file'", "sink='stdout'\nmax_bytes=0", "sink='stdout'\nunknown=true"] {
            assert!(f.load(&format!("{EXAMPLE}\n[readlog]\n{setting}")).is_err());
        }
        assert_eq!(f.load(&format!("{EXAMPLE}\n[readlog]\nsink='stdout'")).unwrap().policy_settings.readlog, Some(crate::policy::ReadLogSink::Stdout));
    }

}

