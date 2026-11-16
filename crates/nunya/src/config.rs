//! Strict configuration loading with data-free diagnostics.
use crate::pgwire::auth::ScramVerifier;
use serde::{Deserialize, de::DeserializeOwned};
use std::{
    fmt,
    io::BufReader,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
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
/// Validated configuration with zeroizing secrets.
pub struct Config {
    /// Agent listener.
    pub listen: Option<SocketAddr>,
    /// Administration listener.
    pub admin_listen: SocketAddr,
    /// Resolved policy path.
    pub policy: PathBuf,
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
    _watch=>"policy_watch_ms", _max=>"max_connections", _listen=>"listen", _admin=>"admin_listen", _policy=>"policy",
    _root=>"root_key_file", _state=>"state_dir", _schemas=>"schemas",
    _search=>"search_path", _refresh=>"snapshot_refresh_ms",
    _plaintext=>"allow_plaintext", _tls=>"tls", _upstream=>"upstream", _standing=>"standing", _spanner=>"spanner_native");
strict_section!(RawUpstream,
    _dialect=>"dialect", _host=>"host", _port=>"port", _user=>"user", _password=>"password_file",
    _dbname=>"dbname", _tls=>"tls", _ca=>"ca_file", _timeout=>"connect_timeout_ms",
    _application=>"application_name");
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
        ],
        "config",
        &mut e,
    );
    if Value::Table(m.clone()).try_into::<RawConfig>().is_err() {
        err(&mut e, "config", "invalid fields");
    }
    let base = path.parent().unwrap_or_else(|| Path::new("."));
    let has_upstream = m.contains_key("upstream");
    let listen = get(&mut m, "listen", &mut e);
    if has_upstream && listen.is_none() {
        err(&mut e, "listen", "valid address required for upstream");
    }
    let spanner_native =
        m.remove("spanner_native")
            .and_then(|v| match crate::spanner_serve::parse_section(&v) {
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
            });
    if !has_upstream && spanner_native.is_none() {
        err(&mut e, "config", "at least one route is required");
    }
    let admin_listen =
        get(&mut m, "admin_listen", &mut e).unwrap_or(SocketAddr::from(([127, 0, 0, 1], 6480)));
    let policy = resolve(
        base,
        path_field(&mut m, "policy", &mut e).unwrap_or_else(|| {
            err(&mut e, "policy", "required path missing");
            PathBuf::new()
        }),
    );
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
    let search_path = get(&mut m, "search_path", &mut e).unwrap_or_else(|| vec!["public".into()]);
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
    let tls = m.remove("tls").and_then(|v| {
        let t = table(v, &["cert_file", "key_file"], "tls", &mut e);
        match Value::Table(t).try_into::<TlsSection>() {
            Ok(mut t) => {
                if t.cert_file.as_os_str().is_empty() || t.key_file.as_os_str().is_empty() {
                    err(&mut e, "tls", "paths must be nonempty");
                }
                t.cert_file = resolve(base, t.cert_file);
                t.key_file = resolve(base, t.key_file);
                if load_tls(&t).is_err() {
                    err(&mut e, "tls", "cannot load certificate and key");
                }
                Some(t)
            }
            Err(_) => {
                err(&mut e, "tls", "certificate and key paths required");
                None
            }
        }
    });
    if !allow_plaintext && tls.is_none() {
        err(&mut e, "tls", "required unless allow_plaintext is true");
    }
    let mut dialect = UpstreamDialect::default();
    let upstream = m.remove("upstream").map(|uv| {
        let mut u = table(
            uv,
            &[
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
        if Value::Table(u.clone()).try_into::<RawUpstream>().is_err() {
            err(&mut e, "upstream", "invalid fields");
        }
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
                                if !raw.name.is_empty()
                                    && !raw.name.starts_with("task:")
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
    if !e.is_empty() {
        return Err(e);
    }
    Ok(Config {
        listen,
        admin_listen,
        policy,
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
        assert_eq!(c.policy, f.0.join("policy.toml"));
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
}
